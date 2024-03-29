library(basedosdados)

set_billing_id("absolute-text-417919") # projeto google cloud

# define path onde salvar os arquivos: são pesados

source <- "F:/Dados/SMTR"

## dados de GPS deste ano (cada dia pesa uns 1.3GB)

# duas terças-feiras consecutivas
query_gps <- paste("SELECT timestamp_gps, data, hora, servico, latitude, longitude, flag_em_movimento,",
    "tipo_parada, flag_trajeto_correto, velocidade_instantanea, velocidade_estimada_10_min,",
    "distancia, flag_em_operacao",
    "FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`",
    "ORDER BY servico, data, timestamp_gps",
    "WHERE (data = \"2024-03-19\" OR data = \"2024-03-26\")",
    "AND flag_em_operacao = TRUE")

download(query_gps, path = file.path(source, "gps_sample.csv"))

## dados do GTFS

# pontos (stop_id)
query_stops <- "SELECT * FROM `rj-smtr.gtfs.stops` WHERE feed_start_date = \"2024-03-18\""

download(query_stops, path = "data-raw/stops_sample.csv")

# tempo das paradas (trip_id e stop_id)
query_stop_times <- "SELECT * FROM `rj-smtr.gtfs.stop_times` WHERE feed_start_date = \"2024-03-18\""

download(query_stop_times, path = "data-raw/stop_times_sample.csv")

# viagens (trip_id e shape_id)
query_trips <- "SELECT * FROM `rj-smtr.gtfs.trips` WHERE feed_start_date = \"2024-03-18\""

download(query_trips, path = "data-raw/trips_sample.csv")

# shapes (shape_id)
query_shapes <- "SELECT * FROM `rj-smtr.gtfs.shapes_geom` WHERE feed_start_date = \"2024-03-18\""

download(query_shapes, path = "data-raw/shapes_geom_sample.csv")

# shapes (shape_id)
query_shapes <- "SELECT * FROM `rj-smtr.gtfs.shapes` WHERE feed_start_date = \"2024-03-18\""

download(query_shapes, path = "data-raw/shapes_sample.csv")

# routes (route_id, service)
query_routes <- "SELECT * FROM `rj-smtr.gtfs.routes` WHERE feed_start_date = \"2024-03-18\""

download(query_routes, path = "data-raw/routes_sample.csv")
