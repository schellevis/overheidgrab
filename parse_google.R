# ============================
# GOOGLE DOCS → DuckDB bulk import (alle typen)
# ============================
# install.packages(c(
#   "DBI","duckdb","fs","digest","withr","progress",
#   "pdftools","xml2","rvest","docxtractr","readr","readxl","mime"
# ))

suppressPackageStartupMessages({
  library(DBI); library(duckdb); library(fs); library(digest)
  library(withr); library(progress)
  library(pdftools); library(xml2); library(rvest)
  library(docxtractr); library(readr); library(readxl); library(mime)
})

# === CONFIG ===
root_dir       <- "docs_google"
duckdb_path    <- "azc.db"
compute_hash   <- FALSE
commit_every   <- 50
build_fts      <- TRUE
enable_ocr     <- FALSE     # OCR alleen voor PDF-gevallen (scans); zet TRUE indien gewenst

store_doc_text <- TRUE      # 1 rij per bestand met samengevoegde tekst
store_page_text <- FALSE    # alleen van toepassing bij PDF

# === CONNECT ===
con <- dbConnect(duckdb::duckdb(), duckdb_path)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

# Threads (best effort)
try({
  cores <- parallel::detectCores(logical = TRUE)
  if (is.finite(cores) && cores > 8) dbExecute(con, sprintf("PRAGMA threads=%d;", cores))
}, silent = TRUE)

# === SCHEMA (google_ prefix) ===
dbExecute(con, "
CREATE TABLE IF NOT EXISTS google_files (
  path       TEXT PRIMARY KEY,
  filename   TEXT,
  size_bytes BIGINT,
  mtime      TIMESTAMP,
  sha256     TEXT,
  mime       TEXT,
  fileformat TEXT
);
")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS google_meta (
  file_path  TEXT REFERENCES google_files(path),
  -- PDF-meta indien beschikbaar; anders NULL
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
CREATE TABLE IF NOT EXISTS google_text_doc (
  file_path TEXT PRIMARY KEY REFERENCES google_files(path),
  pages     INTEGER, -- voor PDF; anders NULL
  text      TEXT
);
")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS google_text_pages (
  file_path TEXT REFERENCES google_files(path),
  page      INTEGER,
  text      TEXT
);
")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS google_import_log (
  file_path TEXT,
  step      TEXT,
  ok        BOOLEAN,
  message   TEXT,
  ts        TIMESTAMP DEFAULT now()
);
")

# Indexen
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_google_files_path      ON google_files(path);")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_google_textdoc_path   ON google_text_doc(file_path);")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_google_textpages_path ON google_text_pages(file_path, page);")

# === HELPERS ===
file_stat_info <- function(path) {
  info <- fs::file_info(path)
  list(
    path = path,
    filename = fs::path_file(path),
    size_bytes = as.numeric(info$size %||% NA_real_),
    mtime = as.POSIXct(info$modification_time, tz = "UTC")
  )
}
`%||%` <- function(a,b) if (is.null(a)) b else a

insert_file_row <- function(fi, mime = NULL, fileformat = NULL, sha = NA_character_) {
  if (isTRUE(compute_hash)) {
    sha <- tryCatch(digest::digest(fi$path, algo = "sha256", file = TRUE),
                    error = function(e) NA_character_)
  }
  dbExecute(con,"
    INSERT INTO google_files(path, filename, size_bytes, mtime, sha256, mime, fileformat)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (path) DO UPDATE SET
      size_bytes=excluded.size_bytes,
      mtime=excluded.mtime,
      sha256=COALESCE(excluded.sha256, google_files.sha256),
      mime=COALESCE(excluded.mime, google_files.mime),
      fileformat=COALESCE(excluded.fileformat, google_files.fileformat);
  ", params = list(fi$path, fi$filename, fi$size_bytes, fi$mtime, sha, mime, fileformat))
}

have_meta       <- function(path) dbGetQuery(con, "SELECT COUNT(*) n FROM google_meta      WHERE file_path=?;", list(path))$n > 0
have_text_doc   <- function(path) dbGetQuery(con, "SELECT COUNT(*) n FROM google_text_doc WHERE file_path=?;", list(path))$n > 0
have_text_pages <- function(path) dbGetQuery(con, "SELECT COUNT(*) n FROM google_text_pages WHERE file_path=?;", list(path))$n > 0

log_step <- function(path, step, ok, message=NULL) {
  dbExecute(con, "INSERT INTO google_import_log(file_path, step, ok, message) VALUES (?, ?, ?, ?);",
            params = list(path, step, isTRUE(ok), as.character(message)))
}

# Detect MIME:
# 1) probeer via DuckDB google_download (filename match)
# 2) anders via mime::guess_type (op bestandsnaam; extensieloos → vaak NA)
# 3) heel ruwe header checks (PDF/ZIP/HTML)
detect_mime <- function(fi) {
  mime_db <- tryCatch({
    dbGetQuery(con, "
      SELECT mime, fileformat
      FROM google_download
      WHERE filename = ? 
      LIMIT 1;", params = list(fi$filename))
  }, error = function(e) NULL)
  
  mime_val <- NULL; fileformat_val <- NULL
  if (!is.null(mime_db) && nrow(mime_db) == 1) {
    mime_val <- mime_db$mime[[1]]; fileformat_val <- mime_db$fileformat[[1]]
  }
  
  if (is.null(mime_val) || is.na(mime_val) || nzchar(trimws(mime_val)) == FALSE) {
    mime_guess <- mime::guess_type(fi$filename) %||% NA_character_
    if (!is.na(mime_guess) && nzchar(mime_guess)) mime_val <- mime_guess
  }
  
  # header-based fallbacks
  if (is.null(mime_val) || is.na(mime_val) || mime_val == "application/octet-stream") {
    conx <- file(fi$path, "rb"); on.exit(close(conx), add = TRUE)
    raw5 <- tryCatch(readBin(conx, "raw", n = 5), error = function(e) raw(0))
    sig  <- tryCatch(rawToChar(raw5), error = function(e) "")
    if (startsWith(sig, "%PDF-")) mime_val <- "application/pdf"
    # ZIP-based office docs (DOCX/XLSX/PPTX) beginnen met PK..
    if (length(raw5) >= 2 && raw5[1] == as.raw(0x50) && raw5[2] == as.raw(0x4B)) {
      # we kunnen niet altijd onderscheiden, maar 'application/zip' is al nuttig
      if (is.null(mime_val) || is.na(mime_val)) mime_val <- "application/zip"
    }
    # simpele HTML check
    raw256 <- tryCatch({ seek(conx, 0); readBin(conx, "raw", n = 256) }, error=function(e) raw(0))
    headstr <- tolower(suppressWarnings(rawToChar(raw256)))
    if (grepl("<html", headstr, fixed = TRUE) || grepl("<!doctype html", headstr, fixed = TRUE)) {
      mime_val <- "text/html"
    }
  }
  
  list(mime = mime_val %||% NA_character_, fileformat = fileformat_val %||% NA_character_)
}

# === Extractors ===
extract_pdf <- function(path) {
  info <- tryCatch(pdf_info(path), error = function(e) NULL)
  pages <- NA_integer_; meta_list <- NULL
  if (!is.null(info)) {
    pages <- as.integer(info$pages %||% NA_integer_)
    to_chr <- function(x) if (is.null(x)) NA_character_ else as.character(x)
    to_lgl <- function(x) if (is.null(x)) NA else as.logical(x)
    meta_list <- list(
      version    = to_chr(info$version),
      pages      = pages,
      encrypted  = to_lgl(info$encrypted),
      linearized = to_lgl(info$linearized),
      tagged     = to_lgl(info$tagged),
      title      = to_chr(info$keys$Title),
      author     = to_chr(info$keys$Author),
      subject    = to_chr(info$keys$Subject),
      keywords   = to_chr(info$keys$Keywords),
      creator    = to_chr(info$keys$Creator),
      producer   = to_chr(info$keys$Producer),
      created    = to_chr(info$keys$CreationDate),
      moddate    = to_chr(info$keys$ModDate),
      trapped    = to_chr(info$keys$Trapped)
    )
  }
  txt <- tryCatch(pdf_text(path), error = function(e) NULL)
  if ((is.null(txt) || length(txt) == 0 || all(nchar(txt) == 0)) && enable_ocr) {
    if (requireNamespace("tesseract", quietly = TRUE)) {
      tmpdir <- tempfile("pdfimgs_"); dir.create(tmpdir)
      on.exit(unlink(tmpdir, recursive = TRUE), add = TRUE)
      imgs <- tryCatch(pdf_convert(path, dpi = 300, filenames = file.path(tmpdir, "p%04d.png")),
                       error = function(e) character(0))
      if (length(imgs) > 0) {
        txt <- vapply(imgs, function(im) tesseract::ocr(im), FUN.VALUE = character(1))
      }
    }
  }
  list(text = if (is.null(txt)) NULL else paste(txt, collapse = "\n"),
       pages = if (is.null(info)) NA_integer_ else pages,
       meta  = meta_list)
}

extract_html <- function(path) {
  # ruwe best effort: strip script/style en pak body-text
  doc <- tryCatch(read_html(path), error = function(e) NULL)
  if (is.null(doc)) return(NULL)
  # rvest::html_text2 verwijdert whitespace netter
  txt <- tryCatch(html_text2(xml2::xml_find_first(doc, "//body")), error = function(e) NULL)
  if (is.null(txt)) txt <- tryCatch(html_text2(doc), error = function(e) NULL)
  txt
}

extract_docx <- function(path) {
  d <- tryCatch(read_docx(path), error = function(e) NULL)
  if (is.null(d)) return(NULL)
  # docxtractr: paragrafen samenvoegen
  tryCatch(paste(docx_extract_all(d), collapse = "\n"), error = function(e) NULL)
}

extract_xlsx <- function(path) {
  # lees alle sheets en concat cellen
  sheets <- tryCatch(readxl::excel_sheets(path), error = function(e) character(0))
  if (!length(sheets)) return(NULL)
  parts <- lapply(sheets, function(s) {
    df <- tryCatch(readxl::read_excel(path, sheet = s, .name_repair = "minimal"), error = function(e) NULL)
    if (is.null(df)) return(NULL)
    # naar tekst
    paste(c(paste0("[Sheet] ", s),
            apply(as.matrix(format(df)), 1, function(row) paste(row, collapse = " | "))),
          collapse = "\n")
  })
  parts <- parts[!vapply(parts, is.null, logical(1))]
  if (!length(parts)) return(NULL)
  paste(parts, collapse = "\n\n")
}

extract_csv <- function(path) {
  # limiet om memory te sparen bij reusachtige CSV's
  conx <- file(path, "rt"); on.exit(close(conx), add = TRUE)
  lines <- tryCatch(readr::read_lines(conx, n_max = 100000, progress = FALSE), error = function(e) character(0))
  if (!length(lines)) return(NULL)
  paste(lines, collapse = "\n")
}

extract_txt <- function(path) {
  conx <- file(path, "rt"); on.exit(close(conx), add = TRUE)
  lines <- tryCatch(readr::read_lines(conx, progress = FALSE), error = function(e) character(0))
  if (!length(lines)) return(NULL)
  paste(lines, collapse = "\n")
}

# === LOOP ===
all_paths <- dir_ls(root_dir, recurse = TRUE, type = "file")
if (!length(all_paths)) stop("Geen bestanden gevonden in: ", root_dir)

pb <- progress_bar$new(total = length(all_paths),
                       format = "Importeren [:bar] :current/:total (:percent) | :elapsed | huidig: :message")

processed <- 0L
dbExecute(con, "BEGIN;")

for (p in all_paths) {
  pb$message(path_file(p)); pb$tick()
  fi <- file_stat_info(p)
  
  # detecteer mime/fileformat
  det <- tryCatch(detect_mime(fi), error = function(e) list(mime = NA_character_, fileformat = NA_character_))
  mime_val <- det$mime; fileformat_val <- det$fileformat
  
  # registreer file
  try(log_step(p, "discover", TRUE, paste0("mime=", mime_val %||% "", " fileformat=", fileformat_val %||% "")), silent = TRUE)
  insert_file_row(fi, mime = mime_val, fileformat = fileformat_val)
  
  # sla tekst over als al aanwezig
  need_doc <- store_doc_text && !have_text_doc(p)
  need_pages <- store_page_text && !have_text_pages(p)
  if (!need_doc && !need_pages && have_meta(p)) {
    processed <- processed + 1L
    if (processed %% commit_every == 0L) { dbExecute(con, "COMMIT;"); dbExecute(con, "BEGIN;") }
    next
  }
  
  text_out <- NULL; pages_out <- NA_integer_; meta_out <- NULL
  mime_lc <- tolower(mime_val %||% "")
  
  # Router per type
  if (startsWith(mime_lc, "application/pdf")) {
    res <- tryCatch(extract_pdf(p), error = function(e) { log_step(p, "extract_pdf", FALSE, e$message); NULL })
    if (!is.null(res)) { text_out <- res$text; pages_out <- res$pages; meta_out <- res$meta }
  } else if (grepl("html", mime_lc, fixed = TRUE) || grepl("text/html", mime_lc, fixed = TRUE)) {
    text_out <- tryCatch(extract_html(p), error = function(e) { log_step(p, "extract_html", FALSE, e$message); NULL })
  } else if (grepl("word", mime_lc, fixed = TRUE) || grepl("officedocument.wordprocessingml", mime_lc, fixed = TRUE) ||
             mime_lc == "application/zip") {
    # zip kan ook docx/xlsx zijn; we proberen eerst docx
    text_out <- tryCatch(extract_docx(p), error = function(e) NULL)
    if (is.null(text_out)) text_out <- tryCatch(extract_xlsx(p), error = function(e) NULL) # misschien xlsx
  } else if (grepl("spreadsheetml", mime_lc, fixed = TRUE)) {
    text_out <- tryCatch(extract_xlsx(p), error = function(e) { log_step(p, "extract_xlsx", FALSE, e$message); NULL })
  } else if (grepl("csv", mime_lc, fixed = TRUE)) {
    text_out <- tryCatch(extract_csv(p), error = function(e) { log_step(p, "extract_csv", FALSE, e$message); NULL })
  } else if (grepl("^text/", mime_lc)) {
    text_out <- tryCatch(extract_txt(p), error = function(e) { log_step(p, "extract_txt", FALSE, e$message); NULL })
  } else {
    # Fallback: probeer als tekst
    text_out <- tryCatch(extract_txt(p), error = function(e) NULL)
  }
  
  # schrijf meta voor PDF
  if (!have_meta(p) && !is.null(meta_out)) {
    dbExecute(con, "
      INSERT INTO google_meta(
        file_path, version, pages, encrypted, linearized, tagged, title, author,
        subject, keywords, creator, producer, created, moddate, trapped
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
              params = list(
                p, meta_out$version, meta_out$pages, meta_out$encrypted, meta_out$linearized, meta_out$tagged,
                meta_out$title, meta_out$author, meta_out$subject, meta_out$keywords, meta_out$creator,
                meta_out$producer, meta_out$created, meta_out$moddate, meta_out$trapped
              )
    )
  }
  
  # schrijf tekst
  if (store_doc_text && !have_text_doc(p) && !is.null(text_out) && nzchar(text_out)) {
    dbExecute(con, "
      INSERT INTO google_text_doc(file_path, pages, text)
      VALUES (?, ?, ?)
      ON CONFLICT (file_path) DO UPDATE SET pages=excluded.pages, text=excluded.text;",
              params = list(p, if (is.na(pages_out)) NULL else pages_out, text_out)
    )
    log_step(p, "write_text_doc", TRUE, paste0("chars=", nchar(text_out)))
  } else if (is.null(text_out) || !nzchar(text_out)) {
    log_step(p, "skip_no_text", FALSE, "Geen tekst geëxtraheerd")
  }
  
  processed <- processed + 1L
  if (processed %% commit_every == 0L) { dbExecute(con, "COMMIT;"); dbExecute(con, "BEGIN;") }
}

dbExecute(con, "COMMIT;")

# === FTS-index ===
if (build_fts) {
  try(dbExecute(con, "INSTALL fts;"), silent = TRUE)
  try(dbExecute(con, "LOAD fts;"), silent = TRUE)
  try(dbExecute(con, "CREATE INDEX IF NOT EXISTS fts_google_text_doc ON google_text_doc USING FTS(text);"), silent = TRUE)
}

# === Samenvatting ===
cat("\nKlaar. Bestanden gezien: ", length(all_paths), " | Verwerkt: ", processed, "\n", sep = "")
print(dbGetQuery(con, "SELECT COUNT(*) AS files FROM google_files;"))
print(dbGetQuery(con, "SELECT COUNT(*) AS docs  FROM google_text_doc;"))

# Handige checks
cat("\nPer MIME-type in google_files:\n")
print(dbGetQuery(con, "
  SELECT COALESCE(mime,'(unknown)') AS mime, COUNT(*) n
  FROM google_files
  GROUP BY 1 ORDER BY n DESC;
"))

cat("\nImport-resultaten (laatste 20):\n")
print(dbGetQuery(con, "
  SELECT * FROM google_import_log
  ORDER BY ts DESC
  LIMIT 20;
"))