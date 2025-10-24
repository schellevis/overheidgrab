library(DBI)
library(duckdb)

con <- dbConnect(duckdb::duckdb(), "azc.db")

# Query: aantal niet-PDF-bestanden
df_counts <- dbGetQuery(con, "
  SELECT
    bestandstype,
    COUNT(*) AS aantal
  FROM openoverheid_extrameta
  WHERE bestandstype IS NOT NULL
  GROUP BY bestandstype
  ORDER BY aantal DESC;
")

print(df_counts)


dbDisconnect(con, shutdown = TRUE)