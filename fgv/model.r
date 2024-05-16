library(mgcv)
library(readr)
library(dplyr)


# Function to filter "in_route" == TRUE and "trip" != 0 and convert columns to the correct data types
treat <- function(gps_data) {
    gps_data <- gps_data %>% filter(in_route == TRUE)
    gps_data <- gps_data %>% filter(trip != 0)

    gps_data$time_elapsed <- as.numeric(gps_data$time_elapsed)
    gps_data$distance_traveled <- as.numeric(gps_data$distance_traveled)
    gps_data$velocidade_estimada_10_min <- as.numeric(gps_data$velocidade_estimada_10_min)
    gps_data$trip <- as.factor(gps_data$trip)
    gps_data$hora <- as.POSIXct(gps_data$hora, format = "%H:%M:%S")

    # Create a categorical variable for the time period of the day (early morning, morning, early afternoon, late afternoon, evening, night)  
    gps_data$periodo_dia <- cut(as.numeric(format(gps_data$hora, "%H")), breaks = c(0, 6, 12, 14, 18, 20, 24), labels = c("early_morning", "morning", "early_afternoon", "late_afternoon", "evening", "night"))

    # Filter to "direction" == 0
    gps_data <- gps_data %>% filter(direction == 1)

    return(gps_data)
}


# Function to create a csv with all the data in the folder processed
create_csv <- function(source) {
    files <- list.files(path = source, pattern = "*.csv", full.names = TRUE)
    processed_data <- data.frame()

    for (file in files) {
        data <- read_csv(file)
        data <- treat(data)
        processed_data <- rbind(processed_data, data)
    }

    return(processed_data)
}


# Function to split the data into training and test sets
split_data <- function(data, seed = 0) {
    set.seed(seed)
    train_idx <- sample(1:nrow(data), 0.8 * nrow(data))
    train_data <- data[train_idx,]
    test_data <- data[-train_idx,]

    return(list(train_data, test_data))
}


# Function to create columns with the estimated time_elapsed and the residuals for a model
create_residuals <- function(data, model) {
    colunm_name <- paste(deparse(substitute(model)), "_time_elapsed_estimated", sep = "")
    residuals_name <- paste(deparse(substitute(model)), "_residuals", sep = "")
    data[, colunm_name] <- predict(model, data)
    data[, residuals_name] <- data$time_elapsed - data[, colunm_name]

    return(data)
}



# ===================== Data Preparation =====================

# Get data source file path
source <- 'smtr_previsao_tempo_chegada/fgv/data/'

# Get all data from the source folder
processed_data <- create_csv(source)

# Split data into training and test sets
data_list <- split_data(processed_data)
train_data <- data_list[[1]]
test_data <- data_list[[2]]



# ===================== Train =====================

# Plot the relationship between time_elapsed and distance_traveled
plot(train_data$distance_traveled, train_data$time_elapsed, xlab = "Distance Traveled", ylab = "Time Elapsed")

# Fit a GLM and a GAM model to the training data using the distance_traveled and velocidade_estimada_10_min and periodo_dia
glm_model <- glm(time_elapsed ~ velocidade_estimada_10_min + periodo_dia:distance_traveled, data = train_data)
gam_model <- gam(time_elapsed ~ velocidade_estimada_10_min + periodo_dia + s(distance_traveled), data = train_data)

summary(glm_model)
summary(gam_model)

# Add the estimated time_elapsed and residuals to the training data
train_data = create_residuals(train_data, glm_model)
train_data = create_residuals(train_data, gam_model)

# Plot predicted vs observed time_elapsed for both models
plot(train_data$time_elapsed, train_data$glm_model_time_elapsed_estimated, col = "red", xlab = "Observed time_elapsed", ylab = "Predicted time_elapsed")
points(train_data$time_elapsed, train_data$gam_model_time_elapsed_estimated, col = "blue", pch = 10)
abline(a = 0, b = 1, col = "black") # Plot a line indicating the perfect prediction
legend("topleft", legend = c("GLM", "GAM", "Perfect Prediction"), col = c("red", "blue", "black"), pch = 1)



# ===================== Test =====================

# Add the estimated time_elapsed and residuals to the test data
test_data = create_residuals(test_data, glm_model)
test_data = create_residuals(test_data, gam_model)

# Plot predicted vs observed time_elapsed for both models
plot(test_data$time_elapsed, test_data$glm_model_time_elapsed_estimated, col = "red", xlab = "Observed time_elapsed", ylab = "Predicted time_elapsed")
points(test_data$time_elapsed, test_data$gam_model_time_elapsed_estimated, col = "blue", pch = 10)
abline(a = 0, b = 1, col = "black") # Plot a line indicating the perfect prediction
legend("topleft", legend = c("GLM", "GAM", "Perfect Prediction"), col = c("red", "blue", "black"), pch = 1)
