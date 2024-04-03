library(dplyr)

# lendo dados
stops <- readr::read_csv("data-raw/stops_sample.csv")

stop_times <- readr::read_csv("data-raw/stop_times_sample.csv")

trips <- readr::read_csv("data-raw/trips_sample.csv")

shapes_geom <- readr::read_csv("data-raw/shapes_geom_sample.csv")

shapes <- readr::read_csv("data-raw/shapes_sample.csv")

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
        shape_id, shape_distance, shape, start_pt, end_pt
    )

routes <- routes %>%
    select(
        route_id, "servico" = route_short_name
    )

shapes <- shapes %>%
    select(
        shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled
    )

# juntando todas as informacoes do gtfs:
# objetivo é uma base a nivel de serviço, para
# mergear com gps

## Primeiro, uma base com os pontos por linha

gtfs_stops <- trips %>%
    left_join(stop_times, by = "trip_id")

# agregando por serviço

gtfs_stops <- gtfs_stops %>%
    select(-trip_id) %>%
    distinct()

# adicionando nome dos serviços

gtfs_stops <- gtfs_stops %>%
    left_join(routes, by = "route_id")

# adicionando coordenadas dos pontos

gtfs_stops <- gtfs_stops %>%
    left_join(stops, by = "stop_id")

# transformando em sf

gtfs_stops <- gtfs_stops %>%
    sf::st_as_sf(
        coords = c("stop_lat", "stop_lon"),
        crs = "WGS84"
    )

# salvando

readr::write_rds(gtfs_stops, "data/gtfs_stops.rds")

## Agora, base com os itinerários de cada linha

gtfs_shapes <- trips %>%
    left_join(shapes, by = "shape_id", relationship = "many-to-many")

# agregando a nível das linhas

gtfs_shapes <- gtfs_shapes %>%
    select(-trip_id) %>%
    distinct()

# servico de cada linha

gtfs_shapes <- gtfs_shapes %>%
    left_join(routes, by = "route_id")

# transformando em objeto sf

gtfs_shapes <- gtfs_shapes %>%
    sf::st_as_sf(
        coords = c("shape_pt_lat", "shape_pt_lon"),
        crs = "WGS84"
    )

# salvando

readr::write_rds(gtfs_shapes, "data/gtfs_shapes.rds")

## E a geometria das shapes

gtfs_shapes_geom <- trips %>%
    left_join(shapes_geom, by = "shape_id", relationship = "many-to-many")

# agregando a nível das linhas

gtfs_shapes_geom <- gtfs_shapes_geom %>%
    select(-trip_id) %>%
    distinct()

# servico de cada linha

gtfs_shapes_geom <- gtfs_shapes_geom %>%
    left_join(routes, by = "route_id")

# geometria

gtfs_shapes_geom <- gtfs_shapes_geom %>%
    sf::st_as_sf(
        wkt = "shape",
        crs = "WGS84"
    )

# salvando

readr::write_rds(gtfs_shapes, "data/gtfs_shapes_geom.rds")
