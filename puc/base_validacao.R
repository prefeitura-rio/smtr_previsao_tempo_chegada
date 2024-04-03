# código que constrói a base de validação:
# base do gps acrescentada da variável y,
# dizendo quanto tempo o ônibus demorou para chegar no próx. ponto

# como demora bastante, fazemos em formato de função:
# roda apenas para um serviço por vez

base_validacao <- function(serv) {
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

  gtfs_shapes <- gtfs_shapes %>%
    filter(servico == serv) # pontos deste serviço
  
  gtfs_shapes <- gtfs_shapes %>%
      arrange(shape_pt_sequence) # ordenando pela ordem dos pontos
    
  # a principio nao sabemos qual shape_id o onibus esta seguindo:
  
  # sigo o método de
  # https://github.com/prefeitura-rio/queries-rj-smtr/blob/master/models/projeto_subsidio_sppo/README.md
  # crio a variável status_viagem segundo esses critérios
  
  # como aproximação vemos isso passo-a-passo:
  # em cada observação o ônibus é visto próximo a um ponto do shape.
  # se, na obs seguinte, ele seguiu o caminho previsto, está neste shape
  
  # para cada shape_id, calcula o ponto mais próximo
  # de cada observação
  
  purrr::map_dfr(
      unique(gtfs_shapes_geom$shape_id),
      function(shape) {
        points <- gtfs_shapes_geom %>%
            filter(shape_id == shape)
      }
      
  )
  
  nearest <- purrr::map(
      unique(gtfs_shapes$shape_id),
      function(shape) {
          points <- gtfs_shapes
          
          sf::st_geometry(points)[points$shape_id != shape] <- NULL
          
          sf::st_nearest_feature(
              gps,
              points
          )
      }
  ) %>%
      unlist() %>%
      matrix(., ncol = length(unique(gtfs_shapes$shape_id)))
  
  # calcula as primeiras diferenças de cada sequência de pontos
  nearest_diff <- diff(nearest)
  
  # escolhe a menor diferença não negativas (sem saltos):
  nearest_diff[nearest_diff < 0] <- Inf
  nearest_diff_min <- apply(nearest_diff, 1, min)
  
  cond <- nearest_diff == nearest_diff_min
  
  # se há empate, deixa vazio
  
  cond[rowSums(cond) > 1, ] <- c(NA, rep(FALSE, ncol(cond) - 1))
  
  nearest <- nearest[cond]
  
  # atribui distancias percorridas
  gps$shape_dist_traveled <- c(gtfs_stops$shape_dist_traveled[nearest], NA)
  
  # não deixando transbordar entre veículos
  gps <- gps %>%
      mutate(
          shape_dist_traveled = ifelse(
              id_veiculo == dplyr::lead(id_veiculo),
              shape_dist_traveled,
              NA
          )
      )
  
  # removendo geometria
  
  gtfs_stops <- gtfs_stops %>%
      sf::st_drop_geometry()

  # por essa distância, detectar a parada anterior e a próxima do ônibus
  
  gtfs_stops <- gtfs_stops %>%
      filter(servico == serv) # pontos deste serviço
  
  gtfs_stops <- gtfs_stops %>%
      arrange(stop_sequence) # ordenando pela ordem dos pontos

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
