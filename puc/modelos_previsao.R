library(dplyr)
library(lubridate)
library(ranger)
library(keras)

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

# data frame para armazenar tempos de execução

tempo_execucao <- data.frame(
    Modelo = character(),
    Tempo = character()
)

readr::write_csv(tempo_execucao, "output/tempo_execucao.csv")


# lendo arquivos de cada servico

files <- list.files(
    file.path(source, "linhas"),
    pattern = "*.csv",
    full.names = TRUE
)

start_time <- Sys.time()

for (file in files) {
    message(file)
    
    df <- data.table::fread(file)
    
    if (nrow(df) < 1000) next
    
    colnames(df) <- c( "data", "servico", "latitude", "longitude", "velocidade_instantanea",
                       "velocidade_estimada_10_min", "stop_sequence", "dist_to_stop",
                       "dist_traveled_shape", "stop_order", "arrival_time", "shape_code",
                       "hora", "day_of_week")
    
    train <- df %>%
        filter(data <= "2024-05-21")
    
    test <- df %>%
        filter(data > "2024-05-21")
    
    rm(df)
    
    nrows <- nrow(test)
    
    #######################
    ## Médias históricas ##
    #######################
    
    # train the historical averages model as a benchmark
    
    historical_avg <- train %>%
        summarise(
            est_arrival_time = mean(arrival_time, na.rm = TRUE),
            .by = c("stop_order", "stop_sequence", "shape_code", "hora", "day_of_week")
        )
    
    # making predictions
    
    historical_avg <- test %>%
        left_join(
            historical_avg,
            by = c("stop_order", "stop_sequence", "shape_code", "hora", "day_of_week")
        )
    
    # evaluating

    historical_avg <- historical_avg %>%
        prediction_errors(
            "Médias históricas",
            file
        ) %>%
        mutate(num_obs = nrows)
    
    # appendo as previsões ao data frame geral
    
    cn <- ifelse(file == files[1], TRUE, FALSE)
    
    readr::write_csv(historical_avg, "output/historical_avg.csv", col_names = cn, append = TRUE)
}

end_time <- Sys.time()

interval <- difftime(end_time, start_time, units="mins") %>%
    as.numeric() %>%
    round()

readr::write_csv(
    data.frame("Médias históricas", interval), "output/tempo_execucao.csv", append = TRUE
)

## Random Forest

start_time <- Sys.time()

for (file in files) {
    message(file)
    
    df <- data.table::fread(file)
    
    if (nrow(df) < 1000) next
    
    colnames(df) <- c( "data", "servico", "latitude", "longitude", "velocidade_instantanea",
                       "velocidade_estimada_10_min", "stop_sequence", "dist_to_stop",
                       "dist_traveled_shape", "stop_order", "arrival_time", "shape_code",
                       "hora", "day_of_week")
    
    train <- df %>%
        filter(data <= "2024-05-21")
    
    test <- df %>%
        filter(data > "2024-05-21")
    
    rm(df)
    
    nrows <- nrow(test)
    
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
        mtry = 4
    )
    
    # gerando previsoes
    
    rf <- test %>%
        bind_cols("est_arrival_time" = predict(rf, test)$predictions) %>%
        prediction_errors("Random Forest", file) %>%
        mutate(num_obs = nrows)
    
    # appendo as previsões ao data frame geral
    
    cn <- ifelse(file == files[1], TRUE, FALSE)
    
    readr::write_csv(rf, "output/random_forest.csv", col_names = cn, append = TRUE)
}

end_time <- Sys.time()

interval <- difftime(end_time, start_time, units="mins") %>%
    as.numeric() %>%
    round()

readr::write_csv(
    data.frame("Random Forest", interval), "output/tempo_execucao.csv", append = TRUE
)

## Rede Neural com uma camada

start_time <- Sys.time()

for (file in files) {
    message(file)
    
    df <- data.table::fread(file)
    
    if (nrow(df) < 1000) next
    
    colnames(df) <- c( "data", "servico", "latitude", "longitude", "velocidade_instantanea",
                       "velocidade_estimada_10_min", "stop_sequence", "dist_to_stop",
                       "dist_traveled_shape", "stop_order", "arrival_time", "shape_code",
                       "hora", "day_of_week")
    
    train <- df %>%
        filter(data <= "2024-05-21")
    
    test <- df %>%
        filter(data > "2024-05-21")
    
    rm(df)
    
    nrows <- nrow(test)
    
    #################
    ## Rede Neural ##
    #################
    
    # níveis de shape_code
    # para evitar discrepâncias entre níveis no treino e teste
    
    shape_levels <- union(
        unique(train$shape_code),
        unique(test$shape_code)
    )
    
    # debugando factors com apenas um nivel
    
    if (length(shape_levels) > 1) {
    test$shape_code <- factor(test$shape_code, levels = shape_levels)
    
    train$shape_code <- factor(train$shape_code, levels = shape_levels)
    }
    
    # selecionando variáveis
    
    y_train <- train$arrival_time
    
    x_train <- train %>%
        model.matrix(
            ~ 0 + hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
                dist_traveled_shape + dist_to_stop + stop_order + stop_sequence +
                day_of_week + shape_code,
            .
        )
    
    y_test <- test$arrival_time
    
    x_test <- test %>%
        model.matrix(
            ~  0 + hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
                dist_traveled_shape + dist_to_stop + stop_order + stop_sequence +
                day_of_week + shape_code,
            .
        )
    
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
    
    test <- data.frame(
        "arrival_time" = y_test, x_test
    )
    
    # treinando a rede
    
    nn <- keras_model_sequential() %>%
        layer_dense(units = 6, activation = "relu", input_shape = ncol(x_test)) %>%
        layer_dense(units = 1, activation = "linear")
    
    nn %>%
        compile(
            optimizer = "adam",
            loss = "mean_squared_error",
            metrics = "mae"
        )
    
    early_stopping <- callback_early_stopping(
        monitor = "val_loss",
        patience = 5,
        mode = "min"
    )
    
    nn %>%
        fit(
            x_train, y_train,
            epochs = 100,
            batch_size = 16,
            validation_split = 0.9,
            callbacks = list(early_stopping)
        )
    
    # previsões
    # desnormalizando
    
    pred <- predict(nn, x_test) * y_sd + y_mean
    
    nn <- test %>%
        bind_cols("est_arrival_time" = pred) %>%
        mutate(stop_order = round(stop_order * x_sd["stop_order"] + x_mean["stop_order"])) %>%
        mutate(arrival_time = arrival_time * y_sd + y_mean) %>%
        prediction_errors("Neural Network", file) %>%
        mutate(num_obs = nrows)
    
    # appendo as previsões ao data frame geral
    
    cn <- ifelse(file == files[1], TRUE, FALSE)
    
    readr::write_csv(nn, "output/rede_neural.csv", col_names = cn, append = TRUE)
}

end_time <- Sys.time()

interval <- difftime(end_time, start_time, units="mins") %>%
    as.numeric() %>%
    round()

readr::write_csv(
    data.frame("Rede Neural", interval), "output/tempo_execucao.csv", append = TRUE
)

## Rede Neural com duas camadas ocultas

start_time <- Sys.time()

for (file in files) {
    message(file)
    
    df <- data.table::fread(file)
    
    if (nrow(df) < 1000) next
    
    colnames(df) <- c( "data", "servico", "latitude", "longitude", "velocidade_instantanea",
                       "velocidade_estimada_10_min", "stop_sequence", "dist_to_stop",
                       "dist_traveled_shape", "stop_order", "arrival_time", "shape_code",
                       "hora", "day_of_week")
    
    train <- df %>%
        filter(data <= "2024-05-21")
    
    test <- df %>%
        filter(data > "2024-05-21")
    
    rm(df)
    
    nrows <- nrow(test)
    
    #################
    ## Rede Neural ##
    #################
    
    # níveis de shape_code
    # para evitar discrepâncias entre níveis no treino e teste
    
    shape_levels <- union(
        unique(train$shape_code),
        unique(test$shape_code)
    )
    
    # debugando factors com apenas um nivel
    
    if (length(shape_levels) > 1) {
        test$shape_code <- factor(test$shape_code, levels = shape_levels)
        
        train$shape_code <- factor(train$shape_code, levels = shape_levels)
    }
    
    # selecionando variáveis
    
    y_train <- train$arrival_time
    
    x_train <- train %>%
        model.matrix(
            ~ 0 + hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
                dist_traveled_shape + dist_to_stop + stop_order + stop_sequence +
                day_of_week + shape_code,
            .
        )
    
    y_test <- test$arrival_time
    
    x_test <- test %>%
        model.matrix(
            ~  0 + hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
                dist_traveled_shape + dist_to_stop + stop_order + stop_sequence +
                day_of_week + shape_code,
            .
        )
    
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
    
    test <- data.frame(
        "arrival_time" = y_test, x_test
    )
    
    # treinando a rede
    
    nn <- keras_model_sequential() %>%
        layer_dense(units = 6, activation = "relu", input_shape = ncol(x_test)) %>%
        layer_dense(units = 3, activation = "relu") %>%
        layer_dense(units = 1, activation = "linear")
    
    nn %>%
        compile(
            optimizer = "adam",
            loss = "mean_squared_error",
            metrics = "mae"
        )
    
    early_stopping <- callback_early_stopping(
        monitor = "val_loss",
        patience = 5,
        mode = "min"
    )
    
    nn %>%
        fit(
            x_train, y_train,
            epochs = 100,
            batch_size = 16,
            validation_split = 0.9,
            callbacks = list(early_stopping)
        )
    
    # previsões
    # desnormalizando
    
    pred <- predict(nn, x_test) * y_sd + y_mean
    
    nn <- test %>%
        bind_cols("est_arrival_time" = pred) %>%
        mutate(stop_order = round(stop_order * x_sd["stop_order"] + x_mean["stop_order"])) %>%
        mutate(arrival_time = arrival_time * y_sd + y_mean) %>%
        prediction_errors("Deep Neural Network", file) %>%
        mutate(num_obs = nrows)
    
    # appendo as previsões ao data frame geral
    
    cn <- ifelse(file == files[1], TRUE, FALSE)
    
    readr::write_csv(nn, "output/rede_neural_prof.csv", col_names = cn, append = TRUE)
}

end_time <- Sys.time()

interval <- difftime(end_time, start_time, units="mins") %>%
    as.numeric() %>%
    round()

readr::write_csv(
    data.frame("Rede Neural profunda", interval), "output/tempo_execucao.csv", append = TRUE
)

## Random Forest único

start_time <- Sys.time()

file <- file.path(source, "base_validacao.csv")

message(file)

df <- data.table::fread(file)

colnames(df) <- c( "data", "servico", "latitude", "longitude", "velocidade_instantanea",
                   "velocidade_estimada_10_min", "stop_sequence", "dist_to_stop",
                   "dist_traveled_shape", "stop_order", "arrival_time", "shape_code",
                   "hora", "day_of_week")

train <- df %>%
    filter(data <= "2024-05-21")

test <- df %>%
    filter(data > "2024-05-20")

rm(df)

nrows <- nrow(test)

# encoding as factors

train$shape_code <- as.factor(train$shape_code)

test$shape_code <- as.factor(test$shape_code)

train$servico <- as.factor(train$servico)

test$servico <- as.factor(test$servico)

###################
## Random Forest ##
###################

rf <- ranger(
    arrival_time ~ servico + hora + latitude + longitude + velocidade_instantanea + velocidade_estimada_10_min +
        dist_traveled_shape + dist_to_stop + stop_order + stop_sequence +
        day_of_week + shape_code,
    data = train,
    num.trees = 300,
    max.depth = 0
)

# gerando previsoes

rf <- test %>%
    bind_cols("est_arrival_time" = predict(rf, test)$predictions) %>%
    prediction_errors("Random Forest Único", file) %>%
    mutate(num_obs = nrows)

# appendo as previsões ao data frame geral

cn <- ifelse(file == files[1], TRUE, FALSE)

readr::write_csv(rf, "output/random_forest_unico.csv", col_names = TRUE)

end_time <- Sys.time()

interval <- difftime(end_time, start_time, units="mins") %>%
    as.numeric() %>%
    round()

readr::write_csv(
    data.frame("Random Forest Único", interval), "output/tempo_execucao.csv", append = TRUE
)