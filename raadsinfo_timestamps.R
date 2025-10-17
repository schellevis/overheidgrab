# === Packages ===
library(DBI)
library(duckdb)

db_path    <- "azc.db"
table_name <- "main.raadsinfo"  # schema.tabel
col_raw    <- '"last_discussed_at"'  # bevat een punt -> altijd quoten
col_ts     <- "timestamp"

con <- dbConnect(duckdb::duckdb(), dbdir = db_path)

# 1) Diagnose: bekijk ruwe waarde, genormaliseerde string en een eerste parse
diag_sql <- sprintf("
  SELECT
    id,
    %s                                        AS raw,
    TRIM(
      REPLACE(
        REPLACE(
          REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$', ''),
        'T',' '),
      '/','-')
    )                                         AS norm,
    TRY_CAST(
      TRIM(
        REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$', ''),'T',' '), '/','-')
      ) AS TIMESTAMP
    )                                         AS try_cast_norm
  FROM %s
  WHERE %s IS NOT NULL
  LIMIT 20;
", col_raw, col_raw, col_raw, table_name, col_raw)

cat("— Diagnose voorbeeld (20 rijen):\n")
print(dbGetQuery(con, diag_sql))

# 2) Voeg doelkolom toe (eenmalig, veilig)
add_col_sql <- sprintf("
  ALTER TABLE %s
  ADD COLUMN IF NOT EXISTS %s TIMESTAMP;
", table_name, col_ts)
dbExecute(con, add_col_sql)

# 3) Robuuste UPDATE: normaliseer + probeer meerdere formaten
update_sql <- sprintf("
  UPDATE %s
  SET %s = COALESCE(
    -- 0) directe CAST op genormaliseerde string
    TRY_CAST(
      TRIM(REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$',''), 'T',' '), '/','-'))
      AS TIMESTAMP
    ),

    -- 1) dd-mm-YYYY HH:MM:SS
    TRY_STRPTIME(
      TRIM(REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$',''), 'T',' '), '/','-')),
      '%%d-%%m-%%Y %%H:%%M:%%S'
    ),

    -- 2) dd-mm-YYYY HH:MM
    TRY_STRPTIME(
      TRIM(REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$',''), 'T',' '), '/','-')),
      '%%d-%%m-%%Y %%H:%%M'
    ),

    -- 3) dd-mm-YYYY (tijd 00:00:00)
    TRY_STRPTIME(
      TRIM(REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$',''), 'T',' '), '/','-')),
      '%%d-%%m-%%Y'
    ),

    -- 4) YYYY-mm-dd HH:MM:SS
    TRY_STRPTIME(
      TRIM(REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$',''), 'T',' '), '/','-')),
      '%%Y-%%m-%%d %%H:%%M:%%S'
    ),

    -- 5) YYYY-mm-dd HH:MM
    TRY_STRPTIME(
      TRIM(REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$',''), 'T',' '), '/','-')),
      '%%Y-%%m-%%d %%H:%%M'
    ),

    -- 6) YYYY-mm-dd
    TRY_STRPTIME(
      TRIM(REPLACE(REPLACE(REGEXP_REPLACE(%s, '(Z|\\+\\d{2}:?\\d{2})$',''), 'T',' '), '/','-')),
      '%%Y-%%m-%%d'
    )
  );
", table_name, col_ts, col_raw, col_raw, col_raw, col_raw, col_raw, col_raw, col_raw)

rows <- dbExecute(con, update_sql)
cat(sprintf("— UPDATE gedaan; rijen geraakt (mogelijk inclusief NULL->NULL): %d\n", rows))

# 4) Checks
cat("— Aantal gevulde timestamps:\n")
print(dbGetQuery(con, sprintf("SELECT COUNT(*) AS gevuld FROM %s WHERE %s IS NOT NULL;", table_name, col_ts)))

cat("— Voorbeeld met resultaat:\n")
print(dbGetQuery(con, sprintf("
  SELECT id, %s AS raw, %s AS ts
  FROM %s
  WHERE %s IS NOT NULL
  LIMIT 10;
", col_raw, col_ts, table_name, col_ts)))

dbDisconnect(con, shutdown = TRUE)