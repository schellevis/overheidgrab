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
    CAST(DCTERMS_publisher AS VARCHAR)        AS gemeente,
    CAST(DC_title AS VARCHAR)                 AS titel,
    CAST('bekendmakingen' AS VARCHAR)         AS source,
    CAST(NULL AS VARCHAR)                     AS openoverheid_source
  FROM bekendmakingen
),
ri AS (
  SELECT
    CAST(id AS VARCHAR)                       AS id,
    CAST(\"timestamp\" AS TIMESTAMP)          AS ts,
    CAST(gemeente AS VARCHAR)                 AS gemeente,
    CAST(COALESCE(name) AS VARCHAR)           AS titel,
    CAST('raadsinformatie' AS VARCHAR)        AS source,
    CAST(NULL AS VARCHAR)                     AS openoverheid_source
  FROM raadsinfo
),
oo AS (
  SELECT
    CAST(COALESCE(em.weblocatie, em.pid) AS VARCHAR) AS id,
    COALESCE(
      CAST(em.openbaarmakingsdatum AS TIMESTAMP),
      CAST(f.mtime AS TIMESTAMP)
    ) AS ts,
    CAST(m.author AS VARCHAR)                     AS gemeente,
    CAST(COALESCE(em.titel, m.title) AS VARCHAR)  AS titel,
    CAST('openoverheid' AS VARCHAR)               AS source,
    CAST(em.aanbieder AS VARCHAR)                 AS openoverheid_source
  FROM openoverheid_text_doc t
  LEFT JOIN openoverheid_files      f  ON f.path      = t.file_path
  LEFT JOIN openoverheid_meta       m  ON m.file_path = t.file_path
  LEFT JOIN openoverheid_extrameta  em ON em.filename = f.path
  WHERE f.path IS NOT NULL
)

SELECT
  *,
  md5(COALESCE(id,'') || '|' || COALESCE(CAST(ts AS VARCHAR),'') || '|' || COALESCE(gemeente,'')) AS hash
FROM (
  SELECT * FROM bk
  UNION ALL
  SELECT * FROM ri
  UNION ALL
  SELECT * FROM oo
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
  SELECT hash, id, ts
  FROM gecombineerd
  WHERE source = 'openoverheid'
  ORDER BY ts DESC
  LIMIT 10;
") |> print()

dbDisconnect(con, shutdown = TRUE)