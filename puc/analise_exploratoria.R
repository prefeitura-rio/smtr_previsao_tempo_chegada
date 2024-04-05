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

query <- readr::read_file("identificacao_pontos.sql")

date <- "\"2024-03-26\""

feed_date <- "\"2024-03-18\""

query <- query %>%
    gsub("\\{date\\}", date, .) %>%
    gsub("\\{feed_start\\}", feed_date, .)

dat <- read_sql(query)

###########################
## 2) Testes de sanidade ##
###########################

dat <- readr::read_rds("data/base_validacao.rds")


