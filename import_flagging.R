library(DBI)
library(duckdb)
library(dplyr)

# Pad naar je DuckDB
db_path <- "azc.db"

# Map met CSV's
csv_dir <- "flagging"

# Alle CSV-bestanden in de map 'flagging'
csv_files <- list.files(csv_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(csv_files) == 0) {
  stop("Geen CSV-bestanden gevonden in de map 'flagging'.")
}

# Kolommen die we willen hebben
needed_cols <- c("hash", "onduidelijk_twijfelgeval", "relevant", "aanpassing_verlenging")

read_flagging_csv <- function(path) {
  # LET OP: sep = ";" omdat dit Excel-achtige CSV's zijn
  df <- read.csv(
    path,
    sep = ";",
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  
  # Eventueel spaties rond kolomnamen weghalen
  names(df) <- trimws(names(df))
  
  # Voeg missende kolommen toe als NA
  missing <- setdiff(needed_cols, names(df))
  if (length(missing) > 0) {
    for (m in missing) {
      df[[m]] <- NA
    }
  }
  
  # Selecteer en reorder alleen de gewenste kolommen
  df[needed_cols]
}

# Alle CSV's inlezen en onder elkaar plakken
all_flagging <- csv_files |>
  lapply(read_flagging_csv) |>
  bind_rows()

# Verbinding met DuckDB
con <- dbConnect(duckdb::duckdb(), dbdir = db_path)

# Tabel flagging droppen als hij bestaat
dbExecute(con, "DROP TABLE IF EXISTS flagging;")

# Nieuwe tabel aanmaken en vullen met de data
dbWriteTable(con, "flagging", all_flagging)

# Even checken
print(dbGetQuery(con, "SELECT COUNT(*) AS n FROM flagging;"))
print(dbGetQuery(con, "SELECT hash, onduidelijk_twijfelgeval, relevant, aanpassing_verlenging FROM flagging LIMIT 10;"))

dbDisconnect(con, shutdown = TRUE)