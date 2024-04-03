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
      coords = c("longitude", "latitude"),
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

  message("Identificando status da viagem\n")
  
  n_shapes <- length(unique(gtfs_shapes_geom$shape_id))
  
  gps <- purrr::imap_dfr(
    unique(gtfs_shapes_geom$shape_id),
    function(shape_code, iter) {
      message(paste(iter, n_shapes, sep = "/"))
        
      points <- gtfs_shapes_geom %>%
        filter(shape_id == shape_code)

      # distancia a pontos de inicio e fim

      start_point <- points$start_pt %>%
        sf::st_as_sfc(crs = "WGS84")

      end_point <- points$end_pt %>%
        sf::st_as_sfc(crs = "WGS84")
      
      near_start <- sf::st_is_within_distance(
          gps, start_point,
          dist = units::as_units(500, "m"),
          sparse = FALSE
      )
      
      near_end <- sf::st_is_within_distance(
          gps, end_point,
          dist = units::as_units(500, "m"),
          sparse = FALSE
      )

      # distancia da shape como um todo
      
      near_middle <- sf::st_is_within_distance(
          gps, points,
          dist = units::as_units(500, "m"),
          sparse = FALSE
      )

      # criando colunas

      gps <- gps %>%
        mutate(shape_id = shape_code) %>%
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
      mudanca = paste0(dplyr::lag(status_viagem, order_by = timestamp_gps), status_viagem),
      .by = c("id_veiculo", "shape_id")
    )
  
  # cada startmiddle marca o início de uma viagem, middleend marca o fim

  gps <- gps %>%
    mutate( # criando id_viagem nas linhas de início
      id_viagem = case_when(
          mudanca == "startmiddle" ~ row_number(),
          mudanca == "endend" ~ 0 
      )
    )

  # preenchendo de cima para baixo, parando nos 0s

  gps <- gps %>%
    group_by(id_veiculo, shape_id) %>%
    tidyr::fill(id_viagem, .direction = "down")

  # dropando linhas sem viagem identificada

  gps <- gps %>%
    tidyr::drop_na(id_viagem) %>%
    filter(id_viagem != 0)

  # agora que sabemos as shape_ids, calculamos quando o ônibus chega a um ponto:
  # quando passa a 500m dele

  message("\nIdentificando paradas dos ônibus\n")
  
  n_shapes <- length(unique(gps$shape_id))
  
  gps <- purrr::imap_dfr(
    unique(gps$shape_id),
    function(shape, iter) {
      message(paste(iter, n_shapes, sep = "/"))    
        
      gps <- gps %>%
        filter(shape_id == shape)

      stops <- gtfs_stops %>%
        filter(shape_id == shape)

      nearest_stops <- nngeo::st_nn(gps, stops, returnDist = TRUE)

      gps$stop <- stops$stop_id[unlist(nearest_stops$nn)]
      
      gps$nearest_dist <- unlist(nearest_stops$dist)

      gps <- gps %>%
        mutate(
          stop = case_when(
            nearest_dist <= 500 ~ stop,
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
        stop != dplyr::lag(stop, order_by = timestamp_gps) & stop != "none",
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
