library(dplyr)

# pasta com dados de GPS
source <- "F:/Dados/SMTR"

# lendo dados
gtfs <- readr::read_rds("data/gtfs.rds")

gps <- readr::read_csv(file.path(source, "gps_sample.csv"))

# projetando a posição de gps na shapefile:
# obter a distancia total que o ônibus já percorreu

# por essa distância, detectar a parada anterior e a próxima do ônibus

# medir em quanto tempo o ônibus chegou na próxima parada
# (quando a próxima parada se tornou parada anterior)