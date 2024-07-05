library(dplyr)
library(ggplot2)
library(ranger)
library(deepviz)
library(basedosdados)
library(rpart)
library(rpart.plot)
library(ISLR)

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
    nome = c(
        "Data", "Serviço", "Posição do ônibus", "Velocidade instantânea",
        "Velocidade média", "Número do ponto", "Distância ao ponto",
        "Distância viajada", "Quantos pontos à frente", "Tempo de chegada ao ponto",
        "ID do Itinerário", "Hora", "Dia da semana"
    ),
    desc = c(
        "data", "código da linha", "longitude e latitude", "km/h", 
        "km/h, média dos últimos 10mins", "de 1, ..., N", "m",
        "m", "de 1 a 20", "quantos mins até chegar", "código shape_id",
        "12h, 13h, ...", "de 1 a 7"
    )
)

readr::write_rds(table_vars, "output/table_vars.rds")

############################
## 6) Plotando os modelos ##
############################

## estimando rf para o 309

df <- data.table::fread(file.path(source, "linhas", "309.csv"))

train <- df %>%
    filter(data <= "2024-05-21")

test <- df %>%
    filter(data > "2024-05-21")

rm(df)

# encoding as factors

train$shape_code <- as.factor(train$shape_code)

test$shape_code <- as.factor(test$shape_code)

###################
## Random Forest ##
###################

rf <- ranger(
    arrival_time ~ hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
        dist_traveled_shape + dist_to_stop + stop_order + stop_sequence +
        day_of_week + shape_code,
    data = train,
    num.trees = 300,
    max.depth = 0,
    mtry = 4,
    importance = "impurity"
)

# salvando importancia

imp <- rf$variable.importance

var_importance <- data.frame(
    "Variável" = names(imp),
    "Importância" = imp
)

var_importance <- var_importance %>%
    mutate(Variável = case_match(
        Variável,
        "hora" ~ "Hora",
        "latitude" ~ "Latitude",
        "longitude" ~ "Longitude",
        "velocidade_instantanea" ~ "Velocidade instantânea",
        "velocidade_estimada_10_min" ~ "Velocidade média",
        "dist_traveled_shape" ~ "Distância viajada",
        "dist_to_stop" ~ "Distância ao ponto",
        "stop_order" ~ "Quantos pontos à frente",
        "stop_sequence" ~ "Número do ponto",
        "day_of_week" ~ "Dia da semana",
        "shape_code" ~ "ID do Itinerário"
    ))

var_importance <- var_importance %>%
    arrange(-Importância) %>%
    mutate(Variável = reorder(Variável, Importância, mean))

## Gráfico de variable importance

ggplot(var_importance, aes(x = Importância, y = Variável)) +
    geom_col(fill = "darkblue", color = "white") +
    theme_minimal()

ggsave("output/plot_variable_importance.png")

# gerando previsoes

prediction <- test %>%
    filter(stop_order %in% c(1,5,10,20), shape_code == 1)

prediction <- prediction %>%
    bind_cols("est_arrival_time" = predict(rf, prediction)$predictions)

prediction <- prediction %>%
    mutate(error = arrival_time - est_arrival_time)

ggplot(prediction, aes(x = error, y = after_stat(density))) +
    geom_histogram(fill = "darkblue", color = "white") +
    theme_minimal() +
    facet_wrap(~ stop_order, labeller = labeller(stop_order = ~ paste(., "Pontos à frente")), scales = "free_y") +
    xlab("Erros de previsão") + ylab("Densidade")

ggsave("output/plot_prediction.png")

#######################
## Árvore de decisão ##
#######################

tree <- rpart(
    arrival_time ~ hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
        dist_traveled_shape + dist_to_stop + stop_order + stop_sequence +
        day_of_week + shape_code,
    data = train,
    control=rpart.control(cp=.0001)
)

#produce a pruned tree based on the best cp value
pruned_tree <- prune(tree, cp=0.01)

#plot the pruned tree
prp(pruned_tree)

