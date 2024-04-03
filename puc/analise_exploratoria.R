library(dplyr)
library(ggplot2)
library(sf)
library(nngeo)

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

# geometria + pontos de inicio e fim
gtfs_shapes_geom <- readr::read_rds("data/gtfs_shapes_geom.rds")

gps <- readr::read_csv(file.path(source, "gps_sample.csv"))

# carrega função que monta a base de validação:
source("base_validacao.R")

# para cada serviço da lista, monta a base
dat <- purrr::map_dfr(
    lista_servicos,
    base_validacao
)

readr::write_rds(dat, "data/base_validacao.rds")

###########################
## 2) Testes de sanidade ##
###########################

dat <- readr::readr_rds("data/base_validacao.rds")


