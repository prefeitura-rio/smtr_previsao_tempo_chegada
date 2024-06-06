library(dplyr)
library(lubridate)
library(basedosdados)

# projeto google cloud
set_billing_id("absolute-text-417919") 

# caminho para os dados mais pesados
source <- "F:/Dados/SMTR"

training_cutoff <- "2024-05-21"

# function for model evaluation

prediction_errors <- function(df, modelo, serv) {
    by_stop <- df %>%
        mutate(error = arrival_time - est_arrival_time) %>%
        summarise(
            Modelo = modelo,
            Servico = serv,
            RMSE = sqrt(mean(error^2, na.rm = TRUE)),
            MAE = mean(abs(error), na.rm = TRUE),
            MAPE = mean(abs(error)/arrival_time, na.rm = TRUE),
            MAD = median(abs(error - median(error, na.rm = TRUE)), na.rm = TRUE),
            .by = "stop_order"
        ) %>%
        arrange(stop_order) %>%
        rename("Ordem do ponto" = stop_order) %>%
        mutate(across(`Ordem do ponto`, as.character))
    
    total <- df %>%
        mutate(error = arrival_time - est_arrival_time) %>%
        summarise(
            `Ordem do ponto` = "Total",
            Modelo = modelo,
            Servico = serv,
            RMSE = sqrt(mean(error^2, na.rm = TRUE)),
            MAE = mean(abs(error), na.rm = TRUE),
            MAPE = mean(abs(error)/arrival_time, na.rm = TRUE),
            MAD = median(abs(error - median(error, na.rm = TRUE)), na.rm = TRUE)
        )
    
    bind_rows(total, by_stop)
}

# data frame para armazenar previsoes

predictions <- data.frame(
    Servico = character(),
    `Ordem do ponto` = character(),
    Modelo = character(),
    num_obs = double(),
    RMSE = double(),
    MAE = double(),
    MAPE = double(),
    MAD = double()
)

# lendo arquivos de cada servico

files <- list.files(
    file.path(source, "linhas"),
    pattern = "*.csv",
    full.names = TRUE
)

for (file in files) {
    message(file)
    
    df <- data.table::fread(file)
    
    train <- df %>%
        filter(data <= "2024-05-21")
    
    test <- df %>%
        filter(data > "2024-05-20")
    
    rm(df)
    
    nrows <- nrow(test)
    
    #######################
    ## Médias históricas ##
    #######################
    
    # train the historical averages model as a benchmark
    
    historical_avg <- train %>%
        summarise(
            est_arrival_time = mean(arrival_time, na.rm = TRUE),
            .by = c("stop_order", "stop_id", "hora", "day_of_week")
        )
    
    # making predictions
    
    historical_avg <- test %>%
        left_join(
            historical_avg,
            by = c("stop_order", "stop_id", "hora", "day_of_week")
        )
    
    # evaluating

    historical_avg <- historical_avg %>%
        prediction_errors(
            "Médias históricas",
            file
        ) %>%
        mutate(num_obs = nrows)
        
    ###################
    ## Random Forest ##
    ###################
    
    rf <- ranger(
        arrival_time ~ hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
            dist_traveled_shape + dist_to_stop + stop_order + stop_sequence + day_of_week,
        data = train,
        num.trees = 300,
        max.depth = 20
    )
    
    # gerando previsoes
    
    rf <- test %>%
        bind_cols("est_arrival_time" = predict(rf, test_data)$predictions) %>%
        prediction_errors("Random Forest", file) %>%
        mutate(num_obs = nrows)
    
    #################
    ## Rede Neural ##
    #################
    
    # selecionando variáveis
    
    y_train <- train$arrival_time
    
    x_train <- train %>%
        select(
            hora, latitude, longitude, velocidade_instantanea, velocidade_estimada_10_min,
            dist_traveled_shape, dist_to_stop, stop_sequence, stop_order, day_of_week
        ) %>%
        as.matrix()
    
    y_test <- test$arrival_time
    
    x_test <- test %>%
        select(
            hora, latitude, longitude, velocidade_instantanea, velocidade_estimada_10_min,
            dist_traveled_shape, dist_to_stop, stop_sequence, stop_order, day_of_week
        ) %>%
        as.matrix()
    
    # computando medias e desvios padrão do treino para normalizar
    
    y_mean <- mean(y_train)
    
    y_sd <- sd(y_train)
    
    x_mean <- colMeans(x_train)
    
    x_sd <- apply(x_train, 2, sd)
    
    # normalizando bases
    
    y_train <- (y_train - y_mean)/y_sd
    
    x_train <- t((t(x_train) - x_mean)/x_sd)
    
    y_test <- (y_test - y_mean)/y_sd
    
    x_test <- t((t(x_test) - x_mean)/x_sd)
    
    # convertendo de volta em data frame
    
    train <- data.frame(
        "arrival_time" = y_train, x_train
    )
    
    test <- data.frame(
        "arrival_time" = y_test, x_test
    )
    
    # treinando a rede
    
    nn <- neuralnet(
        arrival_time ~ hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
            dist_traveled_shape + dist_to_stop + stop_order + stop_sequence + day_of_week,
        data = train,
        hidden = 5,
        lifesign = "full",
        stepmax = 1e+06,
        threshold = 0.1
    )
    
    # previsões
    # desnormalizando
    
    pred <- predict(nn, test) * y_sd + y_mean
    
    nn <- test %>%
        bind_cols("est_arrival_time" = pred) %>%
        mutate(stop_order = round(stop_order * x_sd["stop_order"] + x_mean["stop_order"])) %>%
        mutate(arrival_time = arrival_time * y_sd + y_mean) %>%
        prediction_errors("Neural Network", file) %>%
        mutate(num_obs = nrows)
    
    ############
    ## Resumo ##
    ############
    
    # appendo as previsões ao data frame geral
    
    predictions <- predictions %>%
        bind_rows(historical_avg, rf, nn)
}

# tirando a média ponderada das previsões

predictions <- predictions %>%
    summarise(
        across(c("RMSE", "MAE", "MAPE", "MAD"), ~ sum(. * num_obs, na.rm = TRUE)/sum(num_obs * (!is.na(.)), na.rm = TRUE)),
        .by = c("Modelo", "Ordem do ponto")
    )

predictions <- predictions %>%
    arrange(Modelo, `Ordem do ponto`)