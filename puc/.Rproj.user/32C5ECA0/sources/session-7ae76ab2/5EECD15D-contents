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
        route_id, route_short_name
    )

# juntando todas as informacoes do gtfs:
# objetivo é uma base a nivel de serviço, para
# mergear com gps

gtfs <- routes %>%
    right_join(trips, by = "route_id") %>%
    right_join(stop_times, by = "trip_id")