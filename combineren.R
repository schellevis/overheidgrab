library(DBI)
library(duckdb)

con <- dbConnect(duckdb::duckdb(), dbdir = "azc.db")

sql <- "
DROP TABLE IF EXISTS gecombineerd;

CREATE TABLE gecombineerd AS
WITH
bk AS (
  SELECT
    CAST(DC_identifier AS VARCHAR)  AS id,
    CAST(timestamp      AS TIMESTAMP) AS ts,
    CAST(DCTERMS_publisher AS VARCHAR) AS gemeente,
    CAST(DC_title       AS VARCHAR) AS titel,
    'bekendmakingen'                AS source,
    CAST(broodtekst     AS VARCHAR) AS tekst
  FROM bekendmakingen
),
ri AS (
  SELECT
    CAST(id          AS VARCHAR)   AS id,
    CAST(timestamp   AS TIMESTAMP) AS ts,
    CAST(gemeente    AS VARCHAR)   AS gemeente,
    CAST(COALESCE(name) AS VARCHAR) AS titel,
    'raadsinformatie'              AS source,
    CAST(text        AS VARCHAR)   AS tekst
  FROM raadsinfo
)
SELECT * FROM bk
UNION ALL
SELECT * FROM ri;
"

dbExecute(con, sql)

# Snelle check
print(dbGetQuery(con, "SELECT source, COUNT(*) AS n FROM gecombineerd GROUP BY source;"))
print(dbGetQuery(con, "SELECT * FROM gecombineerd LIMIT 10;"))

dbDisconnect(con, shutdown = TRUE)
