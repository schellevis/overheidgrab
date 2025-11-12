library(dplyr)

ongevallen <- read.csv("ongevallen.txt",header = TRUE)

ongevallen %>%
  group_by(JAAR_VKL) %>%
  summarise(aantal = n())