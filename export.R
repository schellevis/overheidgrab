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
   FROM gecombineerd
     WHERE source='openoverheid'
  ORDER BY ts DESC
  LIMIT 1000
  "
)



# Naar CSV wegschrijven
write.csv(df, "besluiten.csv", row.names = FALSE)

# Verbinding netjes sluiten
dbDisconnect(con, shutdown = TRUE)