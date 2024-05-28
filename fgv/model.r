library(mgcv)
library(readr)
library(dplyr)
library(ggplot2)
library(gridExtra)


# Function to filter "in_route" == TRUE and "trip" != 0 and convert columns to the correct data types
treat <- function(gps_data, trip_counter) {
    gps_data <- gps_data %>% filter(in_route == TRUE)
    gps_data <- gps_data %>% filter(direction != -1)

    gps_data$time_elapsed <- as.numeric(gps_data$time_elapsed)
    gps_data$distance_traveled <- as.numeric(gps_data$distance_traveled)
    gps_data$velocidade_estimada_10_min <- as.numeric(gps_data$velocidade_estimada_10_min)
    gps_data$trip <- as.factor(gps_data$trip_id + trip_counter)

    trip_counter <- max(as.numeric(gps_data$trip)) 
    gps_data$hora <- as.POSIXct(gps_data$hora, format = "%H:%M:%S")

    # Subtract distance traveled from all values the start of each trip
    gps_data$distance_traveled <- (gps_data %>% group_by(trip) %>% mutate(distance_traveled_min_trip = distance_traveled - min(distance_traveled)))$distance_traveled_min_trip

    # Subtract time elapsed from all values the start of each trip
    gps_data$time_elapsed <- (gps_data %>% group_by(trip) %>% mutate(time_elapsed_min_trip = time_elapsed - first(time_elapsed)))$time_elapsed_min_trip

    # If the time_elapsed is 0, substitute it by 1
    gps_data$time_elapsed <- ifelse(gps_data$time_elapsed == 0, 1, gps_data$time_elapsed)

    # Create a categorical variable for the time period of the day (early morning, morning, early afternoon, late afternoon, evening, night)  
    gps_data$periodo_dia <- cut(as.numeric(format(gps_data$hora, "%H")), breaks = c(0, 6, 12, 14, 18, 20, 24), labels = c("early_morning", "morning", "early_afternoon", "late_afternoon", "evening", "night"))

    # Create a column with the previus distance_traveled
    gps_data$prev_distance_traveled <- c(0, gps_data$distance_traveled[-nrow(gps_data)])

    # Create a column with the difference between the distance_travelcdaed and the previus distance_traveled (if the time_elapsed is 1, the difference is 0)
    gps_data$distance_traveled_diff <- ifelse(gps_data$time_elapsed == 1, 0, gps_data$distance_traveled - gps_data$prev_distance_traveled)

    # Create a column with the previus time_elapsed
    gps_data$prev_time_elapsed <- c(0, gps_data$time_elapsed[-nrow(gps_data)])

    # Create a column with the difference between the time_elapsed and the previus time_elapsed (if the time_elapsed is 1, the difference is 0)
    gps_data$time_elapsed_diff <- ifelse(gps_data$time_elapsed == 1, 0, gps_data$time_elapsed - gps_data$prev_time_elapsed)

    # Calculate percentage of max distance traveled in the trip
    gps_data$distance_traveled_percentage <- gps_data$distance_traveled / max(gps_data$distance_traveled)

    gps_data$distance_traveled_squared <- gps_data$distance_traveled^2

    return(list(gps_data, trip_counter))
}

treat_validation <- function(gps_data, trip_counter) {
    # If the df is empty, return it
    if (nrow(gps_data) == 0) {
        return(list(gps_data, trip_counter))
    }

    # Add trip_id based on direction change
    gps_data$trip_id <- cumsum(gps_data$direction != lag(gps_data$direction, default = 0))

    gps_data$trip <- as.factor(gps_data$trip_id + trip_counter)
    trip_counter <- max(as.numeric(gps_data$trip))

    # Add time_elapsed based in timestamp
    gps_data$hora <- as.POSIXct(gps_data$hora, format = "%H:%M:%S")

    gps_data$time_elapsed <- (gps_data %>% group_by(trip) %>% mutate(time_elapsed_min_trip = hora - min(hora)))$time_elapsed_min_trip

    # Start distance_traveled from 0
    gps_data$distance_traveled <- 0

    # While in the same trip, add next_stop_distance to distance_traveled
    for (i in 1:(nrow(gps_data) - 1)) {
        if (gps_data$trip[i] == gps_data$trip[i + 1]) {
            gps_data$distance_traveled[i + 1] <- gps_data$distance_traveled[i] + gps_data$next_stop_distance[i]
        }
    }

    gps_data$periodo_dia <- cut(as.numeric(format(gps_data$hora, "%H")), breaks = c(-1, 6, 12, 14, 18, 20, 24), labels = c("early_morning", "morning", "early_afternoon", "late_afternoon", "evening", "night"))

    return(list(gps_data, trip_counter))
}

# Function to create a csv with all the data in the folder processed
create_csv <- function(source, type = "processed") {
    files <- list.files(path = source, pattern = "*.csv", full.names = TRUE)
    processed_data <- data.frame()
    trip_counter <- 0

    for (file in files) {
        print(paste("Processing file:", file))
        data <- read_csv(file)
        if (type == "processed") {
            treated_list <- treat(data, trip_counter)
        } else {
            treated_list <- treat_validation(data, trip_counter)
        }
        data <- treated_list[[1]]
        trip_counter <- treated_list[[2]]

        processed_data <- rbind(processed_data, data)
    }

    return(processed_data)
}


# Function to split the data into training and test sets randomly
split_data_random <- function(data, train_size = 0.8, seed = 0) {
    set.seed(seed)
    train_idx <- sample(1:nrow(data), train_size * nrow(data))
    train_data <- data[train_idx,]
    test_data <- data[-train_idx,]

    return(list(train_data, test_data))
}

# Function to split the data into training and test sets by date
split_data_by_date <- function(data, train_size = 0.8, seed = 0) {
    set.seed(seed)
    dates <- unique(data$data)
    train_idx <- sort(sample(dates, train_size * length(dates)))
    train_data <- data[data$data %in% train_idx,]
    test_data <- data[!data$data %in% train_idx,]

    # Print the dates chosen for training and test separated by commas
    print(paste("Dates chosen for training:", paste(train_idx, collapse = ", ")))
    print(paste("Dates chosen for test:", paste(dates[!dates %in% train_idx], collapse = ", ")))

    return(list(train_data, test_data))
}

# Function to split the data into training and test sets by trip
split_data_by_trip <- function(data, train_size = 0.8, seed = 0) {
    set.seed(seed)
    trips <- unique(data$trip)
    train_idx <- sort(sample(trips, train_size * length(trips)))
    train_data <- data[data$trip %in% train_idx,]
    test_data <- data[!data$trip %in% train_idx,]

    # Print the trips chosen for training and test separated by commas
    print(paste("Trips chosen for training:", paste(train_idx, collapse = ", ")))
    print(paste("Trips chosen for test:", paste(trips[!trips %in% train_idx], collapse = ", ")))

    return(list(train_data, test_data))
}

# Function to filter the data by a list of routes
filter_routes <- function(processed_data, validation_data, routes) {
    processed_data <- processed_data %>% filter(servico %in% range_servico)

    train_data <- data.frame()
    for (route in unique(processed_data$servico)) {
        print(paste("Route:", route))
        for (direction in unique(processed_data$direction)) {
            route_data <- processed_data[processed_data$servico == route & processed_data$direction == direction,]
            route_data <- route_data %>% filter(distance_traveled > 100)
            route_data$time_elapsed <- (route_data %>% group_by(trip) %>% mutate(time_elapsed_min_trip = time_elapsed - min(time_elapsed)))$time_elapsed_min_trip
            route_data$distance_traveled <- (route_data %>% group_by(trip) %>% mutate(distance_traveled_min_trip = distance_traveled - min(distance_traveled)))$distance_traveled_min_trip
            route_data <- route_data %>% filter(time_elapsed > 1)
            train_data <- rbind(train_data, route_data)
        }
    }

    routes_with_data <- unique(train_data$servico)

    validation_data <- validation_data[validation_data$servico %in% routes_with_data,]

    # APAGARR
    validation_data <- validation_data %>% 
        mutate(periodo_dia = cut(as.numeric(format(hora, "%H")), breaks = c(-1, 6, 12, 14, 18, 20, 24), labels = c("early_morning", "morning", "early_afternoon", "late_afternoon", "evening", "night")))
    
    train_data <- train_data %>% 
        mutate(periodo_dia = cut(as.numeric(format(hora, "%H")), breaks = c(-1, 6, 12, 14, 18, 20, 24), labels = c("early_morning", "morning", "early_afternoon", "late_afternoon", "evening", "night")))

    return(list(train_data, validation_data))
}

# Function to calculate RMSE
calculate_rmse <- function(residuals) {
  sqrt(mean(residuals^2))
}

# Function to calculate MAE
calculate_mae <- function(residuals) {
  mean(abs(residuals))
}


# ===================== Data Preparation =====================

# Check if the processed data and validation data csv files already exist
# If they do, read the data from the csv files
# If they don't, create the csv files from the raw data
if (file.exists("fgv/data/processed_data.csv")) {
    processed_data <- read_csv("fgv/data/processed_data.csv")
} else {
    source <- 'fgv/data/processed_data'
    processed_data <- create_csv(source)
    write.csv(processed_data, "fgv/data/processed_data.csv", row.names = FALSE)
}

if (file.exists("fgv/data/validation_data.csv")) {
    validation_data <- read_csv("fgv/data/validation_data.csv")
} else {
    source <- 'fgv/data/validation_data'
    validation_data <- create_csv(source, type = "validation")
    write.csv(validation_data, "fgv/data/validation_data.csv", row.names = FALSE)
}


# ===================== Data Processing =====================
# Select the routes between 400 and 600
range_servico <- 400:600
data_list <- filter_routes(processed_data, validation_data, range_servico)
train_data <- data_list[[1]]
validation_data <- data_list[[2]]


# ===================== Train =====================
# Plot the relationship between time_elapsed and distance_traveled colored by servico
plot(train_data$distance_traveled, train_data$time_elapsed, col = train_data$servico, xlab = "Distance Traveled", ylab = "Time Elapsed")

# Fit a GLM model to the data
glm_model_gaussian_route <- glm(time_elapsed ~ distance_traveled:periodo_dia + distance_traveled:servico:direction + mean_speed_1_min:servico:direction, data = train_data, family = gaussian)
summary(glm_model_gaussian_route)

# Predict the time_elapsed using the GLM model
pred_gaussian_route <- predict(glm_model_gaussian_route, train_data, se.fit = TRUE, type = "response")
train_data$time_elapsed_gaussian_route_estimated <- pred_gaussian_route$fit
train_data$gaussian_route_residuals <- train_data$time_elapsed_gaussian_route_estimated - train_data$time_elapsed

# Calculate the errors for the GLM model
glm_rmse_gaussian_route <- calculate_rmse(train_data$gaussian_route_residuals)
glm_mae_gaussian_route <- calculate_mae(train_data$gaussian_route_residuals)
print(paste("RMSE Gaussian:", glm_rmse_gaussian_route, " - MAE Gaussian:", glm_mae_gaussian_route))


glm_model_gaussian_general <- glm(time_elapsed ~ distance_traveled:periodo_dia + mean_speed_1_min, data = train_data, family = gaussian)
summary(glm_model_gaussian_general)

pred_gaussian_general <- predict(glm_model_gaussian_general, train_data, se.fit = TRUE, type = "response")
train_data$time_elapsed_gaussian_general_estimated <- pred_gaussian_general$fit
train_data$gaussian_residuals_general <- train_data$time_elapsed_gaussian_general_estimated - train_data$time_elapsed

glm_rmse_gaussian_general <- calculate_rmse(train_data$gaussian_residuals_general)
glm_mae_gaussian_general <- calculate_mae(train_data$gaussian_residuals_general)
print(paste("RMSE Gaussian General:", glm_rmse_gaussian_general, " - MAE Gaussian General:", glm_mae_gaussian_general))


# Plot predicted vs observed time_elapsed for all models with an alpha of 0.5
plot(train_data$time_elapsed, train_data$time_elapsed_gaussian_route_estimated, col = rgb(0, 0, 1, 0.25), xlab = "Observed time_elapsed", ylab = "Predicted time_elapsed")
points(train_data$time_elapsed, train_data$time_elapsed_gaussian_general_estimated, col = rgb(1, 0, 0, 0.25))
abline(a = 0, b = 1, col = "black") # Add a line indicating the perfect prediction
legend("topleft", legend = c("Gaussian Route", "Gaussian General"), col = c("red", "blue"), pch = 1)


# Discretize the observed time_elapsed into bins with 500m intervals
train_data$time_elapsed_bins <- cut(train_data$time_elapsed, breaks = seq(0, max(train_data$time_elapsed), by = 500))

# Plot do modelo Gaussian
plot_gaussian <- ggplot(train_data, aes(x = time_elapsed_bins, y = time_elapsed_gaussian_route_estimated)) +
    geom_boxplot(fill = "red") +
    geom_hline(yintercept = 0, linetype = "dotted") +
    stat_summary(fun = mean, geom = "point", shape = 23, size = 3, fill = "white") +
    stat_summary(fun = mean, geom = "text", vjust = -1, aes(label = round(..y.., 2)), size = 3, color = "black") +
    ggtitle("Gaussian Model (Per Route)") +
    xlab("Time Elapsed") +
    ylab("Residuals")

# Plot do modelo Gaussian General
plot_gaussian_general <- ggplot(train_data, aes(x = time_elapsed_bins, y = time_elapsed_gaussian_general_estimated)) +
    geom_boxplot(fill = "blue") +
    geom_hline(yintercept = 0, linetype = "dotted") +
    stat_summary(fun = mean, geom = "point", shape = 23, size = 3, fill = "white") +
    stat_summary(fun = mean, geom = "text", vjust = -1, aes(label = round(..y.., 2)), size = 3, color = "black") +
    ggtitle("Gaussian General Model") +
    xlab("Time Elapsed") +
    ylab("Residuals")

# Mostrar os gráficos
grid.arrange(plot_gaussian, plot_gaussian_general, nrow = 2)


# ===================== Validation =====================

# Do the same for the validation data
pred_data_gaussian_route_validation <- predict(glm_model_gaussian_route, validation_data, se.fit = TRUE, type = "response")
validation_data$time_elapsed_estimated_gaussian_route <- pred_data_gaussian_route_validation$fit
validation_data$glm_model_residuals_gaussian_route <- validation_data$time_elapsed_estimated_gaussian_route - validation_data$time_elapsed
print(paste("Mean Standard Error Gaussian per Route:", mean(pred_data_gaussian_route_validation$se.fit)))


pred_data_gaussian_general_validation <- predict(glm_model_gaussian_general, validation_data, se.fit = TRUE, type = "response")
validation_data$time_elapsed_estimated_gaussian_general <- pred_data_gaussian_general_validation$fit
validation_data$glm_model_residuals_gaussian_general <- validation_data$time_elapsed_estimated_gaussian_general - validation_data$time_elapsed
print(paste("Mean Standard Error Gaussian General:", mean(pred_data_gaussian_general_validation$se.fit)))


first_10_stops <- validation_data %>% filter(current_stop_index <= 10)

# Plot residuals for gaussian model in a multi-panel plot (separated by current_stop_index) and color by servico and limit the y axis to -500 to 500 with mean written over each boxplot
ggplot(first_10_stops, aes(x = factor(current_stop_index), y = glm_model_residuals_gaussian_route, color = servico)) +
    geom_boxplot() +
    stat_summary(fun = mean, geom = "point", shape = 23, size = 3, fill = "white") +
    stat_summary(fun = mean, geom = "text", vjust = -1, aes(label = round(..y.., 2)), size = 3, color = "black") +
    xlab("Stop Index") +
    ylab("Residuals") +
    ylim(-500, 500) +
    facet_wrap(~servico) +
    ggtitle("Gaussian Model (Per Route)") +
    theme_minimal() +
    geom_hline(yintercept = 0, linetype = "dashed")

# Plot residuals for poisson model in a multi-panel plot (separated by current_stop_index) and color by servico and limit the y axis to -500 to 500 with mean written over each boxplot
ggplot(first_10_stops, aes(x = factor(current_stop_index), y = glm_model_residuals_gaussian_general, color = servico)) +
    geom_boxplot() +
    stat_summary(fun = mean, geom = "point", shape = 23, size = 3, fill = "white") +
    stat_summary(fun = mean, geom = "text", vjust = -1, aes(label = round(..y.., 2)), size = 3, color = "black") +
    xlab("Stop Index") +
    ylab("Residuals") +
    ylim(-500, 500) +
    facet_wrap(~servico) +
    ggtitle("Gaussian Model General") +
    theme_minimal() +
    geom_hline(yintercept = 0, linetype = "dashed")