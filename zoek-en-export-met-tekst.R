# Installeer DuckDB package (eenmalig nodig)
#install.packages("duckdb")

library(duckdb)

# Connectie maken met je duck.db
con <- dbConnect(duckdb::duckdb(), "azc.db")

# Hele tabel uitlezen
df <- dbGetQuery(
  con,
  "
  
  SELECT koppeling.incident,koppeling.identifier, gecombineerd.ts, gecombineerd.gemeente, flagging.onduidelijk_twijfelgeval,flagging.relevant,
  flagging.aanpassing_verlenging, gecombineerd.titel, gecombineerd.source,gecombineerd.hash
   FROM gecombineerd
   LEFT JOIN koppeling ON koppeling.hash = gecombineerd.hash
   LEFT JOIN flagging ON flagging.hash = gecombineerd.hash
  where (((titel ilike '%noodverordening' or (titel ilike '%risico%' and titel ilike '%veiligheid%' and titel ilike '%gebied%'))
  and titel not ilike '%intrekken%' and titel not ilike '%intrekking%' and titel not ilike '%Woo-besluit%' and titel not ilike '%Woo-verzoek%')
  OR ((tekst ilike '%aanwijzing%' and tekst ilike '%veiligheidsrisicogebied%') AND titel not ilike '%algemene plaatselijke verordening%' and titel not ilike '%apv%')
  AND gemeente not ilike '%Tweede Kamer%' and gemeente not ilike '%ministerie%' and titel not ilike '%lijst van ingekomen stukken%')
  ORDER BY ts desc
  "
)



# Naar CSV wegschrijven
write.csv(df, "besluiten3.csv", row.names = FALSE)

# Verbinding netjes sluiten
dbDisconnect(con, shutdown = TRUE)