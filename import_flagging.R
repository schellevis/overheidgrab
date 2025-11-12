library(DBI)
library(duckdb)
library(dplyr)

## === PAD NAAR JE DATABASE EN CSV-DIR ===
db_path  <- "~/overheidgrab/azc.db"        # pas aan als nodig
project_dir <- "~/overheidgrab"
csv_dir  <- file.path(project_dir, "flagging")

## === CSV-BESTANDEN ZOEKEN ===
csv_files <- list.files(csv_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(csv_files) == 0) {
  stop("Geen CSV-bestanden gevonden in de map 'flagging'.")
}

## We willen deze kolommen uiteindelijk hebben
needed_cols <- c(
  "identifier",
  "ts",
  "hash",
  "onduidelijk_twijfelgeval",
  "relevant",
  "aanpassing_verlenging"
)

## === HULPFUNCTIE: DELIMIETER RADDEN (TAB / ; / ,) ===
guess_sep <- function(path) {
  first_line <- tryCatch(
    readLines(path, n = 1, warn = FALSE),
    error = function(e) ""
  )
  if (length(first_line) == 0) return(";")  # default
  
  if (grepl("\t", first_line, fixed = TRUE)) {
    return("\t")
  } else if (grepl(";", first_line, fixed = TRUE)) {
    return(";")
  } else if (grepl(",", first_line, fixed = TRUE)) {
    return(",")
  } else {
    return(";")  # fallback
  }
}

read_flagging_csv <- function(path) {
  message("Inlezen: ", path)
  
  sep <- guess_sep(path)
  message("  -> gedetecteerde separator: '", ifelse(sep == "\t", "\\t", sep), "'")
  
  df <- tryCatch(
    read.csv(
      path,
      sep = sep,
      stringsAsFactors = FALSE,
      check.names = FALSE
    ),
    error = function(e) {
      warning("Kon niet lezen: ", path, " - ", e$message)
      return(NULL)
    }
  )
  
  if (is.null(df)) {
    return(NULL)
  }
  
  # Kolomnamen opschonen
  names(df) <- trimws(names(df))
  # Eventuele BOM/rare prefix op 'hash' normaliseren
  names(df) <- sub("^ï.*hash$", "hash", names(df), ignore.case = TRUE)
  
  # Missende kolommen toevoegen als NA
  missing <- setdiff(needed_cols, names(df))
  if (length(missing) > 0) {
    for (m in missing) {
      df[[m]] <- NA
    }
  }
  
  # >>> HIER: alle needed_cols forceren naar character <<<
  df[needed_cols] <- lapply(df[needed_cols], as.character)
  
  # Alleen de kolommen die we willen, in de juiste volgorde
  df[needed_cols]
}

## === ALLE CSV'S INLEZEN EN STACKEN ===
all_flagging <- csv_files |>
  lapply(read_flagging_csv) |>
  bind_rows()

cat("Rijen vóór ontdubbelen:", nrow(all_flagging), "\n")

## === OPSCHONEN EN ONTDUBBELEN ===
all_flagging <- all_flagging |>
  mutate(
    identifier = trimws(identifier),
    ts         = trimws(ts),
    hash       = trimws(hash)
  )

# Info over hoe vaak identifier+ts gevuld zijn
valid_combo <- sum(
  !is.na(all_flagging$identifier) & all_flagging$identifier != "" &
    !is.na(all_flagging$ts)         & all_flagging$ts != ""
)
cat("Rijen met geldige (niet-lege) identifier+ts:", valid_combo, "\n")
cat("Rijen met lege/NA identifier of ts:", nrow(all_flagging) - valid_combo, "\n")

# NIET filteren op leeg/NA, alleen ontdubbelen op combinatie identifier+ts
all_flagging <- all_flagging |>
  distinct(identifier, ts, .keep_all = TRUE)

cat("Rijen ná ontdubbelen (op identifier+ts):", nrow(all_flagging), "\n")

str(all_flagging)  # even checken in R

## === NAAR DUCKDB SCHRIJVEN ===
con <- dbConnect(duckdb::duckdb(), dbdir = db_path)

dbExecute(con, "DROP TABLE IF EXISTS flagging;")

dbWriteTable(con, "flagging", all_flagging)

# Aantal rijen in de nieuwe tabel
print(dbGetQuery(con, "SELECT COUNT(*) AS n FROM flagging;"))

# Check op dubbele combinaties identifier+ts (zou 0 moeten zijn)
print(dbGetQuery(con, "
  SELECT identifier, ts, COUNT(*) AS n
  FROM flagging
  GROUP BY identifier, ts
  HAVING COUNT(*) > 1
  ORDER BY n DESC
  LIMIT 20;
"))

dbDisconnect(con, shutdown = TRUE)