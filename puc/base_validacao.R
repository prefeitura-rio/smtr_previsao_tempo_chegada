# código que constrói a base de validação:
# base do gps acrescentada da variável y,
# dizendo quanto tempo o ônibus demorou para chegar no próx. ponto

# como demora bastante, fazemos em formato de função:
# roda apenas para um serviço por vez

base_validacao <- function(serv, gps, gtfs_stops) {
  # filtrando para o serviço escolhido:

  gps <- gps %>%
    filter(servico == serv)

  # transformando em objeto sf

  gps <- gps %>%
    sf::st_as_sf(
      coords = c("latitude", "longitude"),
      crs = "WGS84"
    )

  # projetando a posição de gps na shapefile:
  # obter a distancia total que o ônibus já percorreu

  gtfs_stops <- gtfs_stops %>%
    filter(servico == serv) # pontos deste serviço

  # calcula o ponto mais próximo de cada observação
  nearest <- sf::st_nearest_feature(
    gps,
    gtfs_stops
  )

  # atribui distancias percorridas
  gps$shape_dist_traveled <- gtfs_stops$shape_dist_traveled[nearest]
  
  # removendo geometria
  
  gtfs_stops <- gtfs_stops %>%
      sf::st_drop_geometry()

  # por essa distância, detectar a parada anterior e a próxima do ônibus
  
  gtfs_stops <- gtfs_stops %>%
      arrange(stop_sequence)

  gtfs_stops <- gtfs_stops %>%
      mutate(
          previous_stop = stop_sequence,
          next_stop = previous_stop + 1,
          distance_previous = shape_dist_traveled
      ) %>%
      mutate(
          distance_next = dplyr::lead(shape_dist_traveled),
          .by = shape_id
      )
  
  by_cond <- join_by(
      between(shape_dist_traveled, distance_previous, distance_next),
      closest(shape_dist_traveled >= distance_previous)
  )
  
  gps <- gps %>%
      left_join(
          gtfs_stops %>% 
              select(
                  previous_stop, distance_previous, next_stop, distance_next
                  ),
          by = by_cond
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
      .by = c("id_veiculo")
    )

  # criando variável que indica a hora em que o ônibus chegou a um ponto

  gps <- gps %>%
    mutate(
      arrival_time = ifelse(
        previous_stop == dplyr::lead(next_stop, order_by = timestamp_gps),
        timestamp_gps,
        NA
      ),
      .by = c("id_veiculo", "trip_id")
    ) %>%
      mutate(
          arrival_time = as.POSIXct(arrival_time)
      )

  # estendendo o tempo de chegada no ponto seguinte para outras obs.

  gps <- gps %>%
    group_by(id_veiculo, trip_id) %>%
    tidyr::fill(
      arrival_time,
      .direction = "up"
    )

  # tirando NAs remanescentes
  
  gps <- gps %>%
      tidyr::drop_na(timestamp_gps, arrival_time)
  
  # calculando intervalo de tempo

  gps <- gps %>%
    mutate(
      time_until_next = lubridate::time_length(arrival_time - timestamp_gps, unit = "minute")
    )
  
  return(gps)
}
