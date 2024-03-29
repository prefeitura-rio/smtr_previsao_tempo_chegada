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
    arrange(servico) # ordenando por servico

servicos <- unique(gps$servico)

gps <- purrr::imap_dfr(
    servicos,
    function(serv, iter) {
        message(
            paste(iter, length(servicos), sep = "/")
        )
        
        # matcheando apenas linhas de mesmo servico 
        
        df <- gps %>%
            filter(servico == serv)
        
        aux <- gtfs_shapes %>%
            filter(servico == servico)
        
        # calcula o ponto do gtfs mais perto de cada obs. do gps
        
        nearest <- sf::st_nearest_feature(
            df,
            aux
        )
        
        # atribui distancias percorridas
        df$shape_dist_traveled <- aux$shape_dist_traveled[nearest]
    }
)

# por essa distância, detectar a parada anterior e a próxima do ônibus

# anterior:

gtfs_stops <- gtfs_stops %>%
    rename(
        "previous_stop" = stop_sequence,
        "distance_previous" = shape_dist_traveled
        )

by_cond <- join_by(
    servico, closest(shape_dist_traveled >= distance_previous)
) # join com a linha que contém maior distância abaixo da atual

gps <- gps %>%
    left_join(
        gtfs_stops, by = by_cond
    )

# seguinte

gtfs_stops <- gtfs_stops %>%
    rename(
        "next_stop" = previous_stop,
        "distance_next" = distance_previous
    )

by_cond <- join_by(
    servico, closest(shape_dist_traveled < distance_next)
) # join com a linha que contém menor distância acima da atual

gps <- gps %>%
    left_join(
        gtfs_stops, by = by_cond
    )

# medir em quanto tempo o ônibus chegou na próxima parada
# (quando a próxima parada se tornou parada anterior)