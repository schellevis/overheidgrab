library(DBI)
library(duckdb)

con <- dbConnect(duckdb::duckdb(), "azc.db")

dbGetQuery(con, "PRAGMA show_tables;")
dbGetQuery(con, "SELECT COUNT(*) FROM raadsinfo;")
dbGetQuery(con, "SELECT * FROM raadsinfo LIMIT 20;")

dbDisconnect(con, shutdown = TRUE)