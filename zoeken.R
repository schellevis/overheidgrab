suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

# ---- Instellingen ----
db_path    <- "azc.db"          # pad naar je DuckDB database
table_name <- "gecombineerd"    # tabelnaam
ts_column  <- "ts"              # kolom waarop je 'laatste' baseert

# ---- Verbinden ----
con <- dbConnect(duckdb::duckdb(), db_path)

# ---- Laatste 1000 rijen ophalen ----
query <- sprintf("
  SELECT *
  FROM %s
  ORDER BY %s DESC
  LIMIT 30
  OFFSET 1000
", table_name, ts_column)

df_last_1000 <- dbGetQuery(con, query)

# ---- Data Viewer openen in RStudio ----
View(df_last_1000, title = sprintf("Laatste 1000 uit %s", table_name))

# ---- Optioneel: verbinding sluiten ----
dbDisconnect(con)