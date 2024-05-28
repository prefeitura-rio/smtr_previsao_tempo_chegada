library(mgcv)
library(readr)
library(dplyr)


# Function to filter "in_route" == TRUE and "trip" != 0 and convert columns to the correct data types
treat <- function(gps_data, trip_counter) {
    gps_data <- gps_data %>% filter(in_route == TRUE)
    gps_data <- gps_data %>% filter(trip != 0)

    gps_data$time_elapsed <- as.numeric(gps_data$time_elapsed)
    
    # If the time_elapsed is 0, substitute it by 1
    gps_data$time_elapsed <- ifelse(gps_data$time_elapsed == 0, 1, gps_data$time_elapsed)

    gps_data$distance_traveled <- as.numeric(gps_data$distance_traveled)
    gps_data$velocidade_estimada_10_min <- as.numeric(gps_data$velocidade_estimada_10_min)
    gps_data$trip <- as.factor(gps_data$trip + trip_counter)
    trip_counter <- max(as.numeric(gps_data$trip)) 
    gps_data$hora <- as.POSIXct(gps_data$hora, format = "%H:%M:%S")

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

    return(list(gps_data, trip_counter))
}


# Function to create a csv with all the data in the folder processed
create_csv <- function(source) {
    files <- list.files(path = source, pattern = "*.csv", full.names = TRUE)
    processed_data <- data.frame()
    trip_counter <- 0

    for (file in files) {
        data <- read_csv(file)
        treated_list <- treat(data, trip_counter)
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
source <- 'fgv/data/'

# Get all data from the source folder
processed_data <- create_csv(source)

# Select only the values wtih direction == 0
processed_data <- processed_data %>% filter(direction == 0)

# Split data into training and test sets
data_list <- split_data_by_trip(processed_data, train_size = 0.8, seed = 0)
train_data <- data_list[[1]]
test_data <- data_list[[2]]



# ===================== Train =====================

# Plot the relationship between time_elapsed and distance_traveled
plot(train_data$distance_traveled, train_data$time_elapsed, xlab = "Distance Traveled", ylab = "Time Elapsed")

# Fit a GLM model to the data
glm_model_gaussian <- glm(time_elapsed ~ distance_traveled:periodo_dia + velocidade_estimada_10_min, data = train_data, family = gaussian)
glm_model_poisson <- glm(time_elapsed ~ distance_traveled:periodo_dia + velocidade_estimada_10_min, data = train_data, family = poisson)
glm_model_gamma <- glm(time_elapsed ~ distance_traveled:periodo_dia + velocidade_estimada_10_min, data = train_data, family = Gamma(link = "log"))

# Summarize the models
summary(glm_model_gaussian)
summary(glm_model_poisson)
summary(glm_model_gamma)


# Predict the time_elapsed using the GLM model
pred_data_gaussian <- predict(glm_model_gaussian, train_data, se.fit = TRUE, type = "response")
pred_data_poisson <- predict(glm_model_poisson, train_data, se.fit = TRUE, type = "response")
pred_data_gamma <- predict(glm_model_gamma, train_data, se.fit = TRUE, type = "response")


# Add the estimated time_elapsed and residuals to the training data
train_data$time_elapsed_estimated_gaussian <- pred_data_gaussian$fit
train_data$time_elapsed_estimated_poisson <- pred_data_poisson$fit
train_data$time_elapsed_estimated_gamma <- pred_data_gamma$fit

# Calculate the residuals for the GLM model
train_data$glm_model_residuals_gaussian <- train_data$time_elapsed_estimated_gaussian - train_data$time_elapsed
train_data$glm_model_residuals_poisson <- train_data$time_elapsed_estimated_poisson - train_data$time_elapsed 
train_data$glm_model_residuals_gamma <- train_data$time_elapsed_estimated_gamma - train_data$time_elapsed


# Calculate the root mean squared error for the GLM model
glm_rmse_gaussian <- sqrt(mean(train_data$glm_model_residuals_gaussian^2))
glm_rmse_poisson <- sqrt(mean(train_data$glm_model_residuals_poisson^2))
glm_rmse_gamma <- sqrt(mean(train_data$glm_model_residuals_gamma^2))


# Calculate the mean absolute error for both models
glm_mae_gaussian <- mean(abs(train_data$glm_model_residuals_gaussian))
glm_mae_poisson <- mean(abs(train_data$glm_model_residuals_poisson))
glm_mae_gamma <- mean(abs(train_data$glm_model_residuals_gamma))


# Print the root mean squared error and mean absolute error for the GLM models
print(paste("RMSE Gaussian:", glm_rmse_gaussian, " - MAE Gaussian:", glm_mae_gaussian))
print(paste("RMSE Poisson:", glm_rmse_poisson, " - MAE Poisson:", glm_mae_poisson))
print(paste("RMSE Gamma:", glm_rmse_gamma, " - MAE Gamma:", glm_mae_gamma))


# Plot predicted vs observed time_elapsed for all models with an alpha of 0.5
plot(train_data$time_elapsed, train_data$time_elapsed_estimated_gaussian, col = rgb(1, 0, 0, 0.4), xlab = "Observed time_elapsed", ylab = "Predicted time_elapsed")
points(train_data$time_elapsed, train_data$time_elapsed_estimated_poisson, col = rgb(0, 0, 1, 0.4))
points(train_data$time_elapsed, train_data$time_elapsed_estimated_gamma, col = rgb(0, 1, 0, 0.4))
abline(a = 0, b = 1, col = "black") # Add a line indicating the perfect prediction
legend("topleft", legend = c("Gaussian", "Poisson", "Gamma"), col = c("red", "blue", "green"), pch = 1) # Add a legend to the plot


# Discretize the observed time_elapsed into bins with 500m intervals
train_data$time_elapsed_bins <- cut(train_data$time_elapsed, breaks = seq(0, max(train_data$time_elapsed), by = 200))

# Plot a boxplot of the residuals of each model for each time_elapsed bin in a multi-panel plot
par(mfrow = c(3, 1))

boxplot(train_data$glm_model_residuals_gaussian ~ train_data$time_elapsed_bins, col = "red", xlab = "Time Elapsed", ylab = "Residuals", main = "Gaussian Model")
abline(h = 0, lty = 2) # dotted line at 0
rect(0.5, -10000, 10.5, 10000, density = 10) # add a rectangle to highlight the residuals that are outside the range of -10000 to 10000

# Calculate the RMSE and MAE for the highlighted time_elapsed bins
highlighted_data <- train_data[train_data$time_elapsed_bins %in% levels(train_data$time_elapsed_bins)[1:10],]
rmse_gaussian_highlighted <- sqrt(mean(highlighted_data$glm_model_residuals_gaussian^2))
mae_gaussian_highlighted <- mean(abs(highlighted_data$glm_model_residuals_gaussian))

# Plot the RMSE and MAPE over the highlighted time_elapsed bins
max_y <- max(train_data$glm_model_residuals_gaussian)
text(5, max_y - max_y/10, paste("RMSE:", round(rmse_gaussian_highlighted, 2)), font = 2, cex = 1.5)
text(5, max_y - max_y/5, paste("MAE:", round(mae_gaussian_highlighted, 2)), font = 2, cex = 1.5)


boxplot(train_data$glm_model_residuals_poisson ~ train_data$time_elapsed_bins, col = "blue", xlab = "Time Elapsed", ylab = "Residuals", main = "Poisson Model")
abline(h = 0, lty = 2) # dotted line at 0
rect(0.5, -10000, 10.5, 10000, density = 10) # add a rectangle to highlight the residuals that are outside the range of -10000 to 10000

# Calculate the RMSE and MAE for the highlighted time_elapsed bins
highlighted_data <- train_data[train_data$time_elapsed_bins %in% levels(train_data$time_elapsed_bins)[1:10],]
rmse_poisson_highlighted <- sqrt(mean(highlighted_data$glm_model_residuals_poisson^2))
mae_poisson_highlighted <- mean(abs(highlighted_data$glm_model_residuals_poisson))

# Plot the RMSE and MAPE over the highlighted time_elapsed bins
max_y <- max(train_data$glm_model_residuals_poisson)
text(5, max_y - max_y/10, paste("RMSE:", round(rmse_poisson_highlighted, 2)), font = 2, cex = 1.5)
text(5, max_y - max_y/5, paste("MAE:", round(mae_poisson_highlighted, 2)), font = 2, cex = 1.5)


boxplot(train_data$glm_model_residuals_gamma ~ train_data$time_elapsed_bins, col = "green", xlab = "Time Elapsed", ylab = "Residuals", main = "Gamma Model")
abline(h = 0, lty = 2) # dotted line at 0
rect(0.5, -10000, 10.5, 10000, density = 10) # add a rectangle to highlight the residuals that are outside the range of -10000 to 10000

# Calculate the RMSE and MAE for the highlighted time_elapsed bins
highlighted_data <- train_data[train_data$time_elapsed_bins %in% levels(train_data$time_elapsed_bins)[1:10],]
rmse_gamma_highlighted <- sqrt(mean(highlighted_data$glm_model_residuals_gamma^2))
mae_gamma_highlighted <- mean(abs(highlighted_data$glm_model_residuals_gamma))

# Plot the RMSE and MAPE over the highlighted time_elapsed bins
max_y <- max(train_data$glm_model_residuals_gamma)
text(5, max_y - max_y/10, paste("RMSE:", round(rmse_gamma_highlighted, 2)), font = 2, cex = 1.5)
text(5, max_y - max_y/5, paste("MAE:", round(mae_gamma_highlighted, 2)), font = 2, cex = 1.5)

# Reset the plotting layout
par(mfrow = c(1, 1))


# ===================== Test =====================

# Do the same for the test data
pred_data_gaussian_test <- predict(glm_model_gaussian, test_data, se.fit = TRUE, type = "response")
pred_data_poisson_test <- predict(glm_model_poisson, test_data, se.fit = TRUE, type = "response")
pred_data_gamma_test <- predict(glm_model_gamma, test_data, se.fit = TRUE, type = "response")

test_data$time_elapsed_estimated_gaussian <- pred_data_gaussian_test$fit
test_data$time_elapsed_estimated_poisson <- pred_data_poisson_test$fit
test_data$time_elapsed_estimated_gamma <- pred_data_gamma_test$fit

test_data$glm_model_residuals_gaussian <- test_data$time_elapsed_estimated_gaussian - test_data$time_elapsed
test_data$glm_model_residuals_poisson <- test_data$time_elapsed_estimated_poisson - test_data$time_elapsed
test_data$glm_model_residuals_gamma <- test_data$time_elapsed_estimated_gamma - test_data$time_elapsed

glm_rmse_gaussian_test <- sqrt(mean(test_data$glm_model_residuals_gaussian^2))
glm_rmse_poisson_test <- sqrt(mean(test_data$glm_model_residuals_poisson^2))
glm_rmse_gamma_test <- sqrt(mean(test_data$glm_model_residuals_gamma^2))

glm_mae_gaussian_test <- median(abs(test_data$glm_model_residuals_gaussian))
glm_mae_poisson_test <- median(abs(test_data$glm_model_residuals_poisson))
glm_mae_gamma_test <- median(abs(test_data$glm_model_residuals_gamma))

print(paste("RMSE Gaussian Test:", glm_rmse_gaussian_test, " - MAE Gaussian Test:", glm_mae_gaussian_test))
print(paste("RMSE Poisson Test:", glm_rmse_poisson_test, " - MAE Poisson Test:", glm_mae_poisson_test))
print(paste("RMSE Gamma Test:", glm_rmse_gamma_test, " - MAE Gamma Test:", glm_mae_gamma_test))

plot(test_data$time_elapsed, test_data$time_elapsed_estimated_gaussian, col = rgb(1, 0, 0, 0.4), xlab = "Observed time_elapsed", ylab = "Predicted time_elapsed")
points(test_data$time_elapsed, test_data$time_elapsed_estimated_poisson, col = rgb(0, 0, 1, 0.4))
points(test_data$time_elapsed, test_data$time_elapsed_estimated_gamma, col = rgb(0, 1, 0, 0.4))
abline(a = 0, b = 1, col = "black")
legend("topleft", legend = c("Gaussian", "Poisson", "Gamma"), col = c("red", "blue", "green"), pch = 1)

test_data$time_elapsed_bins <- cut(test_data$time_elapsed, breaks = seq(0, max(test_data$time_elapsed), by = 200))

par(mfrow = c(3, 1))

boxplot(test_data$glm_model_residuals_gaussian ~ test_data$time_elapsed_bins, col = "red", xlab = "Time Elapsed", ylab = "Residuals", main = "Gaussian Model")
abline(h = 0, lty = 2)
rect(0.5, -10000, 10.5, 10000, density = 10)
highlighted_data <- test_data[test_data$time_elapsed_bins %in% levels(test_data$time_elapsed_bins)[1:10],]
rmse_gaussian_highlighted_test <- sqrt(mean(highlighted_data$glm_model_residuals_gaussian^2))
mae_gaussian_highlighted_test <- mean(abs(highlighted_data$glm_model_residuals_gaussian))
max_y <- max(test_data$glm_model_residuals_gaussian)
text(5, max_y - max_y/10, paste("RMSE:", round(rmse_gaussian_highlighted_test, 2)), font = 2, cex = 1.5)
text(5, max_y - max_y/5, paste("MAE:", round(mae_gaussian_highlighted_test, 2)), font = 2, cex = 1.5)

boxplot(test_data$glm_model_residuals_poisson ~ test_data$time_elapsed_bins, col = "blue", xlab = "Time Elapsed", ylab = "Residuals", main = "Poisson Model")
abline(h = 0, lty = 2)
rect(0.5, -10000, 10.5, 10000, density = 10)
highlighted_data <- test_data[test_data$time_elapsed_bins %in% levels(test_data$time_elapsed_bins)[1:10],]
rmse_poisson_highlighted_test <- sqrt(mean(highlighted_data$glm_model_residuals_poisson^2))
mae_poisson_highlighted_test <- mean(abs(highlighted_data$glm_model_residuals_poisson))
max_y <- max(test_data$glm_model_residuals_poisson)
text(5, max_y - max_y/10, paste("RMSE:", round(rmse_poisson_highlighted_test, 2)), font = 2, cex = 1.5)
text(5, max_y - max_y/5, paste("MAE:", round(mae_poisson_highlighted_test, 2)), font = 2, cex = 1.5)

boxplot(test_data$glm_model_residuals_gamma ~ test_data$time_elapsed_bins, col = "green", xlab = "Time Elapsed", ylab = "Residuals", main = "Gamma Model")
abline(h = 0, lty = 2)
rect(0.5, -10000, 10.5, 10000, density = 10)
highlighted_data <- test_data[test_data$time_elapsed_bins %in% levels(test_data$time_elapsed_bins)[1:10],]
rmse_gamma_highlighted_test <- sqrt(mean(highlighted_data$glm_model_residuals_gamma^2))
mae_gamma_highlighted_test <- mean(abs(highlighted_data$glm_model_residuals_gamma))
max_y <- max(test_data$glm_model_residuals_gamma)
text(5, max_y - max_y/10, paste("RMSE:", round(rmse_gamma_highlighted_test, 2)), font = 2, cex = 1.5)
text(5, max_y - max_y/5, paste("MAE:", round(mae_gamma_highlighted_test, 2)), font = 2, cex = 1.5)

par(mfrow = c(1, 1))