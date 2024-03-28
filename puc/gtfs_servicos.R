library(dplyr)

# lendo dados
stops <- readr::read_csv("data-raw/stops_sample.csv")

stop_times <- readr::read_csv("data-raw/stop_times_sample.csv")

trips <- readr::read_csv("data-raw/trips_sample.csv")

shapes_geom <- readr::read_csv("data-raw/shapes_geom_sample.csv")

routes <- readr::read_csv("data-raw/routes_sample.csv")

# mantendo apenas algumas colunas

stops <- stops %>%
    select(
        stop_id, stop_lat, stop_lon
    )

stop_times <- stop_times %>%
    select(
        trip_id, stop_id, stop_sequence, shape_dist_traveled
    )

trips <- trips %>%
    select(
        trip_id, route_id, direction_id, shape_id
    )

shapes_geom <- shapes_geom %>%
    select(
        shape_id, shape_distance, start_pt, end_pt
    )

routes <- routes %>%
    select(
        route_id, "servico" = route_short_name
    )

# juntando todas as informacoes do gtfs:
# objetivo é uma base a nivel de serviço, para
# mergear com gps

# servico de cada trip

gtfs <- trips %>%
    left_join(routes, by = "route_id")

# paradas de cada trip

gtfs <- gtfs %>%
    right_join(stop_times, by = "trip_id")

# agregando para serviço

gtfs <- gtfs %>%
    select(-trip_id) %>%
    distinct()

# adicionando coordenadas dos pontos

gtfs <- gtfs %>%
    left_join(stops, by = "stop_id")

# adicionando geometria das linhas

gtfs <- gtfs %>%
    left_join(shapes_geom, by = "shape_id")

# transformando em objeto sf

gtfs <- gtfs %>%
    sf::st_as_sf(
        coords = c("stop_lat", "stop_lon"),
        crs = "WGS84"
    )

# salvando

readr::write_rds(gtfs, "data/gtfs.rds")