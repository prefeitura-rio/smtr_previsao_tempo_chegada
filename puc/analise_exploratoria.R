library(dplyr)
library(ggplot2)

# caminho para os dados mais pesados
source <- "F:/Dados/SMTR"

# serviços de interesse, para ficar mais leve
lista_servicos <- list(
    "309"
)

# antes deste script, deve-se rodar:
# data-raw/download_from_datalake.R -> baixa os dados
# gtfs_servicos.R -> use as infos do gtfs

###########################
## 1) Bases de validação ##
###########################

# para cada serviço, une à base de gps uma coluna com o tempo
# que o ônibus efetivamente levou até o próximo ponto

# lendo os dados:

# coordenadas de cada ponto, para cada servico e shape_id
gtfs_stops <- readr::read_rds("data/gtfs_stops.rds")

# pontinhos ao longo de cada shape, por servico e shape_id
gtfs_shapes <- readr::read_rds("data/gtfs_shapes.rds")

gps <- readr::read_csv(file.path(source, "gps_sample.csv"))

# carrega função que monta a base de validação:
source("base_validacao.R")

# para cada serviço da lista, monta a base
dat <- purrr::map(
    lista_servicos,
    ~ base_validacao(., gps, gtfs_stops)
)

###########################
## 2) Testes de sanidade ##
###########################

gps <- dat[[1]]

# as viagens têm que ser identificadas corretamente:
# ônibus têm que ir subindo no tempo

trips <- gps %>%
    sf::st_drop_geometry() %>%
    ungroup() %>%
    select(previous_stop, id_veiculo, trip_id) %>%
    distinct() %>%
    mutate(
        time = row_number(),
        .by = c("id_veiculo", "trip_id")
    )

ggplot(trips, aes(x = time, y = previous_stop)) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1) +
    theme_minimal()

# estamos com bastante erro de identificação


