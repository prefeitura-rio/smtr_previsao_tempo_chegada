library(dplyr)

# pasta com dados de GPS
source <- "F:/Dados/SMTR"

# lendo dados
gtfs_stops <- readr::read_rds("data/gtfs_stops.rds")

gtfs_shapes <- readr::read_rds("data/gtfs_shapes.rds")

gps <- readr::read_csv(file.path(source, "gps_sample.csv"))

# transformando em objeto sf

gps <- gps %>%
    sf::st_as_sf(
        coords = c("latitude", "longitude"),
        crs = "WGS84"
    )

# projetando a posição de gps na shapefile:
# obter a distancia total que o ônibus já percorreu

gps <- gps %>%
    left_join(gtfs_shapes) %>%
    mutate(
        nearest = sf::st_nearest_points(geometry, shape)
    )

# por essa distância, detectar a parada anterior e a próxima do ônibus

# medir em quanto tempo o ônibus chegou na próxima parada
# (quando a próxima parada se tornou parada anterior)