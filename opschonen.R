#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

# ---- CLI-argumenten ----
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {
  cat("Gebruik:\n")
  cat("  ./dedup.R <tabelnaam> <identifier_kolom>\n\n")
  cat("Voorbeeld:\n")
  cat("  ./dedup.R gecombineerd hash\n")
  quit(status = 1)
}

table_name <- args[1]
id_column  <- args[2]

db_path <- "azc.db"

if (!file.exists(db_path)) {
  stop(sprintf("Databasebestand '%s' niet gevonden in %s", db_path, getwd()))
}

con <- dbConnect(duckdb::duckdb(), db_path)

cat("▶️  Ontdubbelen van tabel:", table_name, "op kolom:", id_column, "\n")

tmp_table <- paste0(table_name, "_dedup_tmp")

# 1) Maak tijdelijke ontdubbelde tabel
query_create_tmp <- sprintf("
  CREATE TABLE \"%s\" AS
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY \"%s\" ORDER BY \"%s\") AS rn
    FROM \"%s\"
  )
  WHERE rn = 1
", tmp_table, id_column, id_column, table_name)

tryCatch(
  {
    dbExecute(con, query_create_tmp)
  },
  error = function(e) {
    cat("\n❌ Fout bij maken van dedup-tabel:\n", conditionMessage(e), "\n")
    dbDisconnect(con)
    quit(status = 1)
  }
)

# 2) Vervang origineel door ontdubbelde tabel
dbExecute(con, sprintf("DROP TABLE \"%s\"", table_name))
dbExecute(con, sprintf("ALTER TABLE \"%s\" RENAME TO \"%s\"", tmp_table, table_name))

cat("✅ Ontdubbeld. Tabel", table_name, "bevat nu max. 1 rij per", id_column, "\n")

dbDisconnect(con)