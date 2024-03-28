library(dplyr)

# pasta com dados de GPS
source <- "F:/Dados/SMTR"

# lendo dados
gtfs <- readr::read_rds("data/gtfs.rds")

gps <- readr::read_csv(file.path(source, "gps_sample.csv"))

# projetando a posição