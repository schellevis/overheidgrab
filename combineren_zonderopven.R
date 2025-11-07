library(DBI)
library(duckdb)

con <- dbConnect(duckdb::duckdb(), dbdir = "azc.db")

sql <- "
DROP TABLE IF EXISTS gecombineerd;

CREATE TABLE gecombineerd AS
WITH
bk AS (
  SELECT
    CAST(DC_identifier AS VARCHAR)            AS id,
    CAST(\"timestamp\" AS TIMESTAMP)          AS ts,
    CAST(DC_title AS VARCHAR)                 AS titel,
    CAST('bekendmakingen' AS VARCHAR)         AS source,
    CAST(NULL AS VARCHAR)                     AS openoverheid_source,
    CAST(broodtekst AS VARCHAR)               AS tekst
  FROM bekendmakingen
),
ri AS (
  SELECT
    CAST(id AS VARCHAR)                       AS id,
    CAST(\"timestamp\" AS TIMESTAMP)          AS ts,
    CAST(COALESCE(name) AS VARCHAR)           AS titel,
    CAST('raadsinformatie' AS VARCHAR)        AS source,
    CAST(NULL AS VARCHAR)                     AS openoverheid_source,
    CAST(\"text\" AS VARCHAR)                 AS tekst
  FROM raadsinfo
)

SELECT
  *,
  md5(COALESCE(id,'')) AS hash
FROM (
  SELECT * FROM bk
  UNION ALL
  SELECT * FROM ri
);
"

dbExecute(con, sql)

dbGetQuery(con, "
  SELECT source, COUNT(*) AS n
  FROM gecombineerd
  GROUP BY source
  ORDER BY source;
") |> print()

dbGetQuery(con, "
  SELECT hash, id, ts, titel, LEFT(tekst, 160) AS snippet
  FROM gecombineerd
  WHERE source = 'openoverheid'
  ORDER BY ts DESC
  LIMIT 10;
") |> print()

dbDisconnect(con, shutdown = TRUE)