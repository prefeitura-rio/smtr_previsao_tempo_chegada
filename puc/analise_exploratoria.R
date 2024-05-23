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

query <- readr::read_file("projecao.sql")

start_date <- "\"2024-03-12\""

end_date <- "\"2024-03-12\""

query <- query %>%
    gsub("\\{start_date\\}", start_date, .) %>%
    gsub("\\{end_date\\}", end_date, .)

#dat <- read_sql(query)

download(query, path = file.path(source, "gps_test_1_10.csv"))

###########################
## 2) Testes de sanidade ##
###########################

## Ver quantos pontos são pulados

##########################
## 3) Modelo Newtoniano ##
##########################

dat <- readr::read_csv(file.path(source, "gps_sample.csv"))

dat %>%
    mutate(
        est_arrival_time = 60/1000 * dist_to_stop/velocidade_estimada_10_min,
        error = (arrival_time - est_arrival_time)
    ) %>%
    summarise(
        RMSE = sqrt(mean(error^2, na.rm = TRUE)),
        MAE = mean(abs(error), na.rm = TRUE),
        MAD = median(abs(error - median(error, na.rm = TRUE)), na.rm = TRUE)
    )

dat %>%
    mutate(
        est_arrival_time = 1/1000 * dist_next_stop/velocidade_estimada_10_min,
        error = (arrival_time - est_arrival_time)
    ) %>%
    {.$error} %>%
    hist()

reg <- lm(
    log(arrival_time) ~ log(dist_next_stop) + log(velocidade_estimada_10_min),
    data = dat %>% filter(dist_next_stop > 0, arrival_time > 0)
)
