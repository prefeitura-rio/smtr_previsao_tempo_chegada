library(dplyr)
library(ggplot2)
library(sf)
library(nngeo)
library(basedosdados)

# projeto google cloud
set_billing_id("absolute-text-417919") 

# caminho para os dados mais pesados
source <- "F:/Dados/SMTR"

###########################
## 1) Bases de validação ##
###########################

# código SQL que puxa os dados de gps,
# identifica as viagens,
# identifica os pontos,
# e calcula tempos de chegada realizados

dates <- read_sql(
    "select distinct feed_start_date, feed_end_date from `rj-smtr.gtfs.stops`"
)

query <- readr::read_file("identificacao_pontos.sql")

start_date <- "\"2024-03-26\""

end_date <- "\"2024-03-27\""

query <- query %>%
    gsub("\\{start_date\\}", start_date, .) %>%
    gsub("\\{end_date\\}", end_date, .)

dat <- read_sql(query)

download(query, file.path(source, "gps_sample.csv"))

###########################
## 2) Testes de sanidade ##
###########################

dat <- readr::read_rds("data/base_validacao.rds")


