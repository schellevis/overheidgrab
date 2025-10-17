# === Packages ===
# install.packages(c("jsonlite", "duckdb", "DBI"))
library(jsonlite)
library(DBI)
library(duckdb)

# === Instellingen ===
json_dir   <- "cache_raadsinfo"                # map met je JSON-bestanden
pattern    <- "\\.json(nd)?$"            # .json of .jsonl/.ndjson
db_path    <- "azc.db"              # DuckDB-bestand
table_name <- "raadsinfo"           # naam van de tabel


# === Helper: alleen hits.hits eruit, _source.* naar boven, list->JSON ===
extract_hits_df <- function(file) {
  x <- tryCatch(fromJSON(file, flatten = TRUE), error = function(e) NULL)
  if (is.null(x)) return(NULL)
  
  # Pak expliciet de array met documenten
  df <- tryCatch(x$hits$hits, error = function(e) NULL)
  if (is.null(df) || NROW(df) == 0) return(NULL)
  
  # _source.* naar topniveau en prefix verwijderen
  src_cols <- grep("^_source\\.", names(df), value = TRUE)
  if (length(src_cols)) {
    names(df)[match(src_cols, names(df))] <- sub("^_source\\.", "", src_cols)
  }
  
  # Maak 'id' kolom als die nog niet bestaat
  if (!("id" %in% names(df))) {
    if ("_id" %in% names(df)) {
      df$id <- df[["_id"]]
    } else if ("DC_identifier" %in% names(df)) {
      df$id <- df[["DC_identifier"]]
    } else {
      # laatste redmiddel: rij-hash (voorkomt echte duplicates inladen)
      df$id <- vapply(seq_len(NROW(df)), function(i) digest::digest(df[i, , drop = FALSE]), "")
    }
  }
  
  # Lists/arrays -> JSON string (houdt schema strak)
  is_list <- vapply(df, is.list, logical(1))
  if (any(is_list)) {
    df[is_list] <- lapply(df[is_list], function(col)
      vapply(col, function(x) if (length(x)) toJSON(x, auto_unbox = TRUE) else NA_character_, "")
    )
  }
  
  df
}

# === Start ===
con <- dbConnect(duckdb::duckdb(), dbdir = db_path)

# Drop de tabel zodat je opnieuw kunt draaien
if (dbExistsTable(con, table_name)) {
  dbExecute(con, paste0("DROP TABLE ", table_name))
}

# Verzamel bestanden
files <- list.files(json_dir, pattern = "\\.json(nd)?$",
                    full.names = TRUE, ignore.case = TRUE)
if (!length(files)) stop("Geen JSON-bestanden gevonden in: ", json_dir)

# Stream/append per bestand (met kolomharmonisatie)
for (f in files) {
  message("Verwerken: ", f)
  df <- extract_hits_df(f)
  if (is.null(df) || !NROW(df)) next
  
  if (!dbExistsTable(con, table_name)) {
    dbWriteTable(con, table_name, df, temporary = FALSE)
  } else {
    # kolommen harmoniseren
    tbl_cols <- dbGetQuery(con, sprintf("PRAGMA table_info(%s);", table_name))$name
    df_cols  <- names(df)
    missing_in_df  <- setdiff(tbl_cols, df_cols)
    missing_in_tbl <- setdiff(df_cols, tbl_cols)
    
    if (length(missing_in_df))  for (nm in missing_in_df)  df[[nm]] <- NA
    if (length(missing_in_tbl)) for (nm in missing_in_tbl)
      dbExecute(con, sprintf('ALTER TABLE %s ADD COLUMN "%s" VARCHAR', table_name, nm))
    
    # volgorde gelijk trekken en append
    order_cols <- dbGetQuery(con, sprintf("PRAGMA table_info(%s);", table_name))$name
    df <- df[, order_cols[order_cols %in% names(df)], drop = FALSE]
    dbWriteTable(con, table_name, df, append = TRUE)
  }
}

# Ontdubbelen op id
if (dbExistsTable(con, table_name)) {
  cols <- dbGetQuery(con, sprintf("PRAGMA table_info(%s);", table_name))$name
  if ("id" %in% cols) {
    dbExecute(con, sprintf("
      CREATE TABLE tmp AS
      SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
        FROM %s
      ) WHERE rn = 1;
      DROP TABLE %s;
      ALTER TABLE tmp RENAME TO %s;
    ", table_name, table_name, table_name))
  }
}

# Snel sanity check


dbDisconnect(con, shutdown = TRUE)
cat("✅ Klaar. Alleen hits.hits geladen, tabel gedropt & ontdubbeld op id.\n")