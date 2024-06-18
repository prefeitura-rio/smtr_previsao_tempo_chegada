library(dplyr)
library(ggplot2)
library(ranger)
library(deepviz)
library(basedosdados)

source <- "F:/Dados/SMTR"

# projeto google cloud
set_billing_id("absolute-text-417919") 

#######################################
## 1) Tabela de resumo das previsões ##
#######################################

predictions <- bind_rows(
    data.table::fread("output/historical_avg.csv"),
    data.table::fread("output/random_forest.csv"),
    data.table::fread("output/rede_neural.csv")
)

# tirando a média ponderada das previsões

predictions_summ <- predictions %>%
    filter(`Ordem do ponto` == "Total") %>%
    summarise(
        across(c("RMSE", "MAE", "MAPE", "MAD"), ~ sum(. * num_obs, na.rm = TRUE)/sum(num_obs * (!is.na(.)), na.rm = TRUE)),
        .by = c("Modelo")
    )

readr::write_rds(predictions_summ, "output/predictions_summ.rds")

##############################
## 2) Gráfico do desempenho ##
##############################

predictions_plot <- predictions %>%
    filter(`Ordem do ponto` != "Total") %>%
    summarise(
        across(c("RMSE", "MAE", "MAPE", "MAD"), ~ sum(. * num_obs, na.rm = TRUE)/sum(num_obs * (!is.na(.)), na.rm = TRUE)),
        .by = c("Modelo", "Ordem do ponto")
    ) %>%
    mutate(across(`Ordem do ponto`, as.numeric))

ggplot(predictions_plot, aes(x = `Ordem do ponto`, y = RMSE, color = Modelo)) +
    geom_line() +
    theme_minimal()

ggsave("output/plot_performance_rmse.png")

ggplot(predictions_plot, aes(x = `Ordem do ponto`, y = MAE, color = Modelo)) +
    geom_line() +
    theme_minimal()

ggsave("output/plot_performance_mae.png")

ggplot(predictions_plot %>% filter(Modelo != "Neural Network"), aes(x = `Ordem do ponto`, y = MAPE, color = Modelo)) +
    geom_line() +
    theme_minimal() +
    scale_y_continuous(labels = scales::percent)

ggsave("output/plot_performance_mape.png")

#################################
## 3) Histograma das previsões ##
#################################

predictions_hist <- predictions %>%
    filter(`Ordem do ponto` == "Total")

ggplot(predictions_hist, aes(x = RMSE, y = after_stat(density))) +
    geom_histogram(fill = "darkblue", color = "white") +
    scale_x_continuous(transform = "log10") +
    theme_minimal() +
    ylab("Densidade das linhas") +
    facet_wrap(~ Modelo, nrow = 2)

ggsave("output/plot_histogram_rmse.png")

#############################
## 4) Observações perdidas ##
#############################

num_obs <- readr::read_csv("output/aux_num_obs.csv")

num_obs_orig <- read_sql(
    "select servico, count(*) as nrows_orig from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` where data between \"2024-05-01\" and \"2024-05-31\" and flag_em_operacao = TRUE and tipo_parada is null and flag_em_movimento = TRUE group by servico"
)

table_obs <- data.frame(
    "Número de observações" = c("Serviços", "Observações"),
    "Dados de GPS" = c(nrow(num_obs_orig), sum(num_obs_orig$nrows_orig) %>% as.integer()),
    "Dados identificados" = c(nrow(num_obs), sum(num_obs$num_obs) %>% as.integer())
)

readr::write_rds(table_obs, "output/table_obs.rds")

################################
## 5) Descrição das variáveis ##
################################

df <- data.table::fread(file.path(source, "linhas", "309.csv"))

names(df)

table_vars <- data.frame(
    vars1 = c(
        "Data", "Serviço", "Posição do ônibus", "Velocidade instantânea",
        "Velocidade média", "Número do ponto", "Distância ao ponto"
    ),
    vars2 = c(
        "Distância viajada", "Quantos pontos à frente", "Tempo de chegada ao ponto",
        "ID do Itinerário", "Hora", "Dia da semana", ""
    )
)

readr::write_rds(table_vars, "output/table_vars.rds")

############################
## 6) Plotando os modelos ##
############################

