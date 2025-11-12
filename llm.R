library(tidyverse)
library(duckdb)
library(ellmer)


con <- dbConnect(duckdb::duckdb(), "azc.db")

incidenten <- as_tibble(dbGetQuery(con,"SELECT DISTINCT koppeling.incident
  FROM gecombineerd
  LEFT JOIN koppeling ON koppeling.hash = gecombineerd.hash
  LEFT JOIN flagging  ON flagging.identifier = koppeling.identifier
  WHERE flagging.relevant = 'ja'
  ORDER BY koppeling.incident LIMIT 100"))


lijst <- incidenten %>%
  pull(incident)

for (nummer in lijst) {
  cat("Incident",nummer)
  
  sql <- glue::glue("SELECT gecombineerd.hash,gecombineerd.tekst,gecombineerd.id,gecombineerd.ts,gecombineerd.gemeente FROM koppeling
    LEFT JOIN gecombineerd ON koppeling.hash = gecombineerd.hash
    WHERE incident = '{nummer}' LIMIT 1000")
  
  result <- as_tibble(dbGetQuery(con,sql))
  
  tekst <- result %>%
    pull(tekst) %>%
    paste(collapse = " ")
  
  
    cat("\n")
}