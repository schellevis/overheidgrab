# Installeer DuckDB package (eenmalig nodig)
#install.packages("duckdb")

library(duckdb)

# Connectie maken met je duck.db
con <- dbConnect(duckdb::duckdb(), "azc.db")

# Hele tabel uitlezen
df <- dbGetQuery(
  con,
  "
  
  SELECT gecombineerd.id,koppeling.incident,koppeling.identifier, 
  CAST(strftime('%Y-%m-%d', gecombineerd.ts) AS TEXT) AS datum,
  gecombineerd.gemeente, flagging.onduidelijk_twijfelgeval,flagging.relevant,
  flagging.aanpassing_verlenging, gecombineerd.titel, gecombineerd.source,gecombineerd.hash
   FROM gecombineerd
   LEFT JOIN koppeling ON koppeling.hash = gecombineerd.hash
   LEFT JOIN flagging ON flagging.identifier = koppeling.identifier
  where flagging.relevant = 'ja' or flagging.onduidelijk_twijfelgeval is not null
  ORDER BY gecombineerd.ts desc
  "
)



# Naar CSV wegschrijven
write.csv(df, "relevant.csv", row.names = FALSE)

# Verbinding netjes sluiten
dbDisconnect(con, shutdown = TRUE)