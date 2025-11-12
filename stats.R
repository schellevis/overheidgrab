library(tidyverse)
library(duckdb)



con <- dbConnect(duckdb::duckdb(), "azc.db")

relevant <- dbGetQuery(
  con,
  "
  
  SELECT koppeling.incident,
  CAST(strftime('%Y-%m-%d', gecombineerd.ts) AS TEXT) AS datum,
  flagging.relevant
   FROM gecombineerd
   LEFT JOIN koppeling ON koppeling.hash = gecombineerd.hash
   LEFT JOIN flagging ON flagging.identifier = koppeling.identifier
  where flagging.relevant = 'ja' or flagging.onduidelijk_twijfelgeval is not null
  ORDER BY gecombineerd.ts desc
  ")


samenvatting <- relevant %>%
  mutate(jaar = year(datum),maand = month(datum)) %>%
  group_by(jaar) %>%
  summarise(aantal = n_distinct(incident))

plot(samenvatting,type='l')

print(samenvatting)