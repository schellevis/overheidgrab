# Installeer DuckDB package (eenmalig nodig)
#install.packages("duckdb")

library(duckdb)

# Connectie maken met je duck.db
con <- dbConnect(duckdb::duckdb(), "azc.db")

# Hele tabel uitlezen
df <- dbGetQuery(
  con,
  "
  
  SELECT *
   FROM raadsinfo LIMIT 1000
  "
)



# Naar CSV wegschrijven
write.csv(df, "raadsinfo.csv", row.names = FALSE)

# Verbinding netjes sluiten
dbDisconnect(con, shutdown = TRUE)