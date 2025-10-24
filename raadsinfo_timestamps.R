# ============================
# PDF → DuckDB bulk import (definitieve versie)
# ============================
# Vereiste packages:
# install.packages(c("pdftools","DBI","duckdb","fs","digest","withr","progress"))
# Voor OCR (optioneel): install.packages("tesseract")

library(pdftools)
library(DBI)
library(duckdb)
library(fs)
library(digest)
library(withr)
library(progress)

# === CONFIG ===
pdf_root_dir   <- "docs_openoverheid"   # bronmap
duckdb_path    <- "azc.db"              # database
compute_hash   <- FALSE                 # TRUE = SHA256 berekenen
build_fts      <- TRUE                  # probeer full-text index
recurse_dirs   <- TRUE
pages_chunk    <- 2000
commit_every   <- 25
enable_ocr     <- FALSE                 # TRUE = OCR voor scans (traag!)

# === CONNECT ===
con <- dbConnect(duckdb::duckdb(), duckdb_path)

try({
  cores <- parallel::detectCores(logical = TRUE)
  if (is.finite(cores) && cores > 0) dbExecute(con, sprintf("PRAGMA threads=%d;", cores))
}, silent = TRUE)

# === SCHEMA ===
dbExecute(con, "
CREATE TABLE IF NOT EXISTS openoverheid_files (
  path       TEXT PRIMARY KEY,
  filename   TEXT,
  size_bytes BIGINT,
  mtime      TIMESTAMP,
  sha256     TEXT
);
")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS openoverheid_meta (
  file_path  TEXT REFERENCES openoverheid_files(path),
  version    TEXT,
  pages      INTEGER,
  encrypted  BOOLEAN,
  linearized BOOLEAN,
  tagged     BOOLEAN,
  title      TEXT,
  author     TEXT,
  subject    TEXT,
  keywords   TEXT,
  creator    TEXT,
  producer   TEXT,
  created    TIMESTAMP,
  moddate    TIMESTAMP,
  trapped    TEXT
);
")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS openoverheid_text (
  file_path TEXT REFERENCES openoverheid_files(path),
  page      INTEGER,
  text      TEXT
);
")

dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_files_path ON openoverheid_files(path);")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_text_file_page ON openoverheid_text(file_path, page);")

# === HELPERS ===
safe_pdf_info <- function(path) {
  tryCatch({
    out <- pdf_info(path)
    to_chr <- function(x) if (is.null(x)) NA_character_ else as.character(x)
    to_int <- function(x) if (is.null(x)) NA_integer_ else as.integer(x)
    to_lgl <- function(x) if (is.null(x)) NA else as.logical(x)
    list(
      version    = to_chr(out$version),
      pages      = to_int(out$pages),
      encrypted  = to_lgl(out$encrypted),
      linearized = to_lgl(out$linearized),
      tagged     = to_lgl(out$tagged),
      title      = to_chr(out$keys$Title),
      author     = to_chr(out$keys$Author),
      subject    = to_chr(out$keys$Subject),
      keywords   = to_chr(out$keys$Keywords),
      creator    = to_chr(out$keys$Creator),
      producer   = to_chr(out$keys$Producer),
      created    = to_chr(out$keys$CreationDate),
      moddate    = to_chr(out$keys$ModDate),
      trapped    = to_chr(out$keys$Trapped)
    )
  }, error = function(e) NULL)
}

safe_pdf_text <- function(path) {
  tryCatch(pdf_text(path), error = function(e) NULL)
}

# OCR fallback voor gescande PDF's (langzaam)
ocr_pdf_text <- function(path) {
  if (!enable_ocr) return(NULL)
  if (!requireNamespace("tesseract", quietly = TRUE)) return(NULL)
  tmpdir <- tempfile("pdfimgs_"); dir.create(tmpdir)
  on.exit(unlink(tmpdir, recursive = TRUE), add = TRUE)
  imgs <- tryCatch(pdf_convert(path, dpi = 300, filenames = file.path(tmpdir, "p%04d.png")),
                   error = function(e) character(0))
  if (length(imgs) == 0) return(NULL)
  txt <- vapply(imgs, function(im) tesseract::ocr(im), FUN.VALUE = character(1))
  txt
}

already_in_files <- function(path) {
  n <- dbGetQuery(con, "SELECT COUNT(*) n FROM openoverheid_files WHERE path = ?;", params = list(path))$n
  n > 0
}
have_meta <- function(path) {
  n <- dbGetQuery(con, "SELECT COUNT(*) n FROM openoverheid_meta WHERE file_path = ?;", params = list(path))$n
  n > 0
}
have_text <- function(path) {
  n <- dbGetQuery(con, "SELECT COUNT(*) n FROM openoverheid_text WHERE file_path = ?;", params = list(path))$n
  n > 0
}

file_stat_info <- function(path) {
  info <- file_info(path)
  list(
    path = path,
    filename = path_file(path),
    size_bytes = as.numeric(info$size),
    mtime = as.POSIXct(info$modification_time, tz = "UTC")
  )
}

insert_file_row <- function(fi) {
  sha <- NA_character_
  if (compute_hash) {
    sha <- tryCatch(digest::digest(fi$path, algo = "sha256", file = TRUE),
                    error = function(e) NA_character_)
  }
  dbExecute(
    con,
    "INSERT INTO openoverheid_files(path, filename, size_bytes, mtime, sha256)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT (path) DO NOTHING;",
    params = list(fi$path, fi$filename, fi$size_bytes, fi$mtime, sha)
  )
  fi$path
}

insert_meta_row <- function(file_path, meta) {
  dbExecute(
    con,
    "INSERT INTO openoverheid_meta(
       file_path, version, pages, encrypted, linearized, tagged, title, author,
       subject, keywords, creator, producer, created, moddate, trapped
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
    params = list(
      file_path, meta$version, meta$pages, meta$encrypted, meta$linearized, meta$tagged,
      meta$title, meta$author, meta$subject, meta$keywords, meta$creator, meta$producer,
      meta$created, meta$moddate, meta$trapped
    )
  )
}

# chunk-veilig + lengtes gelijk
insert_text_pages <- function(file_path, pages_vec, chunk = 2000) {
  if (is.null(pages_vec)) return(invisible())
  n <- length(pages_vec); if (n == 0) return(invisible())
  
  starts <- seq(1, n, by = chunk)
  for (s in starts) {
    e   <- min(s + chunk - 1, n)
    idx <- s:e
    df <- data.frame(
      file_path = rep(file_path, length(idx)),
      page      = idx,
      text      = pages_vec[idx],
      stringsAsFactors = FALSE
    )
    if (nrow(df) != length(idx)) {
      df$text <- rep_len(df$text, length(idx))
      df$file_path <- rep_len(file_path, length(idx))
      df$page <- idx
    }
    dbAppendTable(con, "openoverheid_text", df)
  }
}

# === BESTANDEN ===
pdf_paths <- dir_ls(pdf_root_dir, recurse = recurse_dirs, type = "file", glob = "*.pdf")
if (length(pdf_paths) == 0) message("Geen PDF-bestanden gevonden in: ", pdf_root_dir)

pb <- progress_bar$new(
  total = length(pdf_paths),
  format = "Importeren [:bar] :current/:total (:percent) | :elapsed | huidig: :message"
)

processed <- 0
errors    <- list()

dbExecute(con, "BEGIN;")

for (path in pdf_paths) {
  pb$message(path_file(path)); pb$tick()
  
  fi <- file_stat_info(path)
  key <- if (already_in_files(path)) path else insert_file_row(fi)
  
  # Metadata
  if (!have_meta(key)) {
    meta <- safe_pdf_info(path)
    if (!is.null(meta)) insert_meta_row(key, meta)
  }
  
  # Tekst
  if (!have_text(key)) {
    txt <- safe_pdf_text(path)
    if (is.null(txt) || length(txt) == 0 || all(nchar(txt) == 0)) {
      txt <- ocr_pdf_text(path)
    }
    if (!is.null(txt) && length(txt) > 0) insert_text_pages(key, txt, chunk = pages_chunk)
  }
  
  processed <- processed + 1
  if (processed %% commit_every == 0) { dbExecute(con, "COMMIT;"); dbExecute(con, "BEGIN;") }
}

dbExecute(con, "COMMIT;")

# === FTS (best effort) ===
if (build_fts) {
  try(dbExecute(con, "INSTALL fts;"), silent = TRUE)
  try(dbExecute(con, "LOAD fts;"), silent = TRUE)
  try(dbExecute(con, "CREATE INDEX IF NOT EXISTS fts_openoverheid_text ON openoverheid_text USING FTS(text);"), silent = TRUE)
}

cat("\nKlaar.\nBestanden verwerkt: ", processed, "\n", sep = "")
print(dbGetQuery(con, "SELECT COUNT(*) AS files FROM openoverheid_files;"))
print(dbGetQuery(con, "SELECT COUNT(*) AS pages FROM openoverheid_text;"))

dbDisconnect(con, shutdown = TRUE)