# código que constrói a base de validação:
# base do gps acrescentada da variável y,
# dizendo quanto tempo o ônibus demorou para chegar no próx. ponto

# como demora bastante, fazemos em formato de função:
# roda apenas para um serviço por vez

library(dplyr)

# pasta com dados de GPS
source <- "F:/Dados/SMTR"

# escolhe um serviço

# lendo dados
gtfs_stops <- readr::read_rds("data/gtfs_stops.rds")

gtfs_shapes <- readr::read_rds("data/gtfs_shapes.rds")

gps <- readr::read_csv(file.path(source, "gps_sample.csv"))

# filtrando para o serviço escolhido:

gps <- gps %>%
    filter(servico == servico)

# transformando em objeto sf

gps <- gps %>%
    sf::st_as_sf(
        coords = c("latitude", "longitude"),
        crs = "WGS84"
    )

# projetando a posição de gps na shapefile:
# obter a distancia total que o ônibus já percorreu

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

# tentando criar uma id única de viagem:
# cada viagem só passa um vez no mesmo ponto

# conta o número de vezes que saiu do ponto inicial

gps <- gps %>%
    mutate(previous_stop = tidyr::replace_na(previous_stop, 0)) %>%
    mutate(
        trip_id = cumsum(previous_stop == 1),
        .by = c("servico", "id_veiculo")
    )

# criando variável que indica a hora em que o ônibus chegou a um ponto

gps <- gps %>%
    mutate(
        arrival_time = ifelse(
            previous_stop = dplyr::lead(next_stop, order_by = "timestamp_gps"),
            timestamp_gps,
            NA
        ),
        .by = "servico"
    )

# estendendo o tempo de chegada no ponto seguinte para outras obs.

gps <- gps %>%
    group_by(servico, id_veiculo, trip_id) %>%
    tidyr::fill(
        arrival_time, .direction = "up"
    )

# calculando intervalo de tempo

gps <- gps %>%
    mutate(
        time_until_next = lubridate::time_length(arrival_time - timestamp_gps, unit = "minute")
    )

# salvando

readr::write_rds(gps, "data/base_validacao.rds")