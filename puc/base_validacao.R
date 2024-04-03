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

  gtfs_shapes_geom <- gtfs_shapes_geom %>%
      filter(servico == serv)
  
  gtfs_shapes <- gtfs_shapes %>%
    filter(servico == serv) # pontos deste serviço
  
  gtfs_shapes <- gtfs_shapes %>%
      arrange(shape_pt_sequence) # ordenando pela ordem dos pontos
    
  # a principio nao sabemos qual shape_id o onibus esta seguindo:
  
  # sigo o método de
  # https://github.com/prefeitura-rio/queries-rj-smtr/blob/master/models/projeto_subsidio_sppo/README.md
  # crio a variável status_viagem segundo esses critérios
  
  gps <- purrr::map_dfr(
      unique(gtfs_shapes_geom$shape_id),
      function(shape) {
          points <- gtfs_shapes_geom %>%
              filter(shape_id == shape)
          
          # distancia a pontos de inicio e fim
          
          start_point <- points$start_pt %>%
              sf::st_as_sfc()
          
          end_point <- points$end_pt %>%
              sf::as_sfc()
          
          near_start <- sf::st_distance(gps, start_point) <= 500
          
          near_end <- sf::st_distance(gps, start_point) <= 500
          
          # distancia da shape como um todo
          
          near_shape <- nngeo::st_nn(gps, points, returnDist = TRUE)
          
          near_shape <- unlist(near_shape)
          
          near_middle <- near_shape <= 500
          
          # criando colunas
          
          gps <- gps %>%
              mutate(shape_id = shape) %>%
              mutate(
                  status_viagem = case_when(
                      near_start ~ "start",
                      near_end ~ "end",
                      near_middle ~ "middle",
                      .default = "out"
                  )
              )
          
          gps
      }
  )
  
  # marco status de movimento startmiddle e middleend
  
  gps <- gps %>%
      mutate(
          status_viagem = case_when(
              dplyr::lag(status_viagem, order_by = "timestamp_gps") == "start" & status_viagem == "middle" ~ "startmiddle",
              dplyr::lag(status_viagem, order_by = "timestamp_gps") == "middle" & status_viagem == "end" ~ "middleend",
              .default = status_viagem
          ),
          .by = c("id_veiculo", "shape_id")
      )
  
  # cada startmiddle marca o início de uma viagem, middleend marca o fim
  
  gps <- gps %>%
      mutate( # criando id_viagem nas linhas de início
          id_viagem = ifelse(
              status_viagem == "startmiddle",
              row_number(),
              NA
          )
      )
  
  # preenchendo de cima para baixo
  
  gps <- gps %>%
      group_by(id_veiculo, shape_id) %>%
      tidyr::fill(id_viagem, .direction = "down") %>%
      ungroup()
  
  # dropando linhas sem viagem identificada
  
  gps <- gps %>%
      tidyr::drop_na(id_viagem)
  
  # agora que sabemos as shape_ids, calculamos quando o ônibus chega a um ponto:
  # quando passa a 500m dele
  
  gps <- purrr::map_dfr(
      unique(gps$shape_id),
      function(shape){
          gps <- gps %>%
              filter(shape_id == shape)
          
          stops <- gtfs_stops %>%
              filter(shape_id == shape)
          
          nearest_stops <- nngeo::st_nn(gps, stops, returnDist = TRUE)
          
          gps$stop <- stops$stop_id[unlist(nearest_stops$nn)]
          
          gps <- gps %>%
              mutate(
                  stop = case_when(
                      unlist(nearest_stops$dist) <= 500 ~ stop,
                      .default = "none"
                  )
              )
          
          gps
      }
  )

  # criando variável que indica a hora em que o ônibus chegou a um ponto

  gps <- gps %>%
    mutate(
      arrival_time = ifelse(
        stop != dplyr::lag(stop, order_by = timestamp_gps),
        timestamp_gps,
        NA
      ),
      .by = c("id_veiculo")
    ) %>%
      mutate(
          arrival_time = as.POSIXct(arrival_time)
      )

  # estendendo o tempo de chegada no ponto seguinte para outras obs.

  gps <- gps %>%
    group_by(id_veiculo) %>%
    tidyr::fill(
      arrival_time,
      .direction = "up"
    ) %>%
      ungroup()

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
