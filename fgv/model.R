library(tidyverse)

treat_data <- function(data_path, route) {
#' @title Treat data
#' @description This function treats the data set and returns the treated data, saving some plots of removed data
#' @param data_path The path to the data set
#' @param route The route number
#' @return The treated data set

  # Load the data
  data <- read.csv(data_path)

  # Select only the columns that are necessary for the analysis ('data', 'hora', 'id_veiculo', 'servico', 'velocidade_estimada_10_min', 'in_route', 'direction', 'distance_traveled', 'cumulative_time_traveled', 'mean_speed_1_min', 'mean_speed_3_min', 'mean_speed_5_min'
  data <- data %>% select(data, hora, id_veiculo, servico, velocidade_estimada_10_min, in_route, direction, distance_traveled, time_traveled, mean_speed_1_min, mean_speed_3_min, mean_speed_5_min)

  # Transform the 'hora' column to a time format
  data$hora <- as.POSIXct(data$hora, format = "%H:%M:%S")

  # Make the "data" column have the correct format
  data$data <- as.Date(data$data)

  # Make the "id_veiculo" column a factor
  data$id_veiculo <- as.factor(data$id_veiculo)

  # Make the "servico" column a factor
  data$servico <- as.factor(data$servico)

  # Make the "in_route" column a boolean
  data$in_route <- as.logical(data$in_route)

  # Add week day column
  data$week_day <- weekdays(data$data)

  # Make the week_day column a factor with the levels in the correct order
  data$week_day <- factor(data$week_day, levels = c("domingo", "segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado"))

  # Add is_weekend column
  data$is_weekend <- ifelse(data$week_day == "sábado" | data$week_day == "domingo", TRUE, FALSE)

  # Add time_period column
  data$time_period <- cut(as.numeric(format(as.POSIXct(data$hora, tz = "UTC"), "%H")), breaks = c(0, 6, 12, 18, 24), labels = c("Dawn", "Morning", "Afternoon", "Night"))

  print(paste("Data loaded for route", route, "with", nrow(data), "data points"))

  # Remove all the data that is not in the route
  data <- data %>% filter(in_route == TRUE)

  print(paste("Data filtered for route", route, "with", nrow(data), "data points in the route"))

  # Remove all data that the direction is not 0 or 1
  data <- data %>% filter(direction == 0 | direction == 1)

  # Remove all the data that the vehicle has not traveled more than 100 meters
  filtered_data <- data %>% filter(distance_traveled > 100)

  # Create a new column called 'trip_id' that will be incremented each time the direction changes group by vehicle or more than 5 minutes have passed
  filtered_data <- filtered_data %>% group_by(id_veiculo, data) %>% mutate(trip_id = cumsum(direction != lag(direction, default = first(direction)) | time_traveled > 600))

  # Remove trips that have less than 10 data points
  filtered_data <- filtered_data %>% group_by(id_veiculo, trip_id, data) %>% filter(n() > 10)

  # Recalculate trip_id after removing trips with less than 10 data points
  filtered_data <- filtered_data %>% group_by(id_veiculo, data) %>% mutate(trip_id = cumsum(direction != lag(direction, default = first(direction))))

  # Get all data that the dstance traveled was decreased from the previous data point (with a 100 meters threshold)
  wrong_direction_data <- filtered_data %>% group_by(id_veiculo, trip_id, data) %>% filter(distance_traveled < lag(distance_traveled, default = first(distance_traveled)) - 100)

  # Filter wrong direction data to remove trips with more than 5 wrong direction data points
  wrong_direction_count <- 5
  wrong_direction_data <- wrong_direction_data %>% group_by(id_veiculo, trip_id, data) %>% filter(n() > wrong_direction_count)

  # Get the trip_id of the wrong direction data
  wrong_direction_trip_id <- wrong_direction_data %>% select(id_veiculo, trip_id, data) %>% distinct()

  # Remove all the wrong direction data
  filtered_data <- filtered_data %>% anti_join(wrong_direction_trip_id, by = c("id_veiculo", "trip_id", "data"))

  # Add cumulative time traveled column for each trip
  filtered_data <- filtered_data %>% group_by(id_veiculo, trip_id, data) %>% mutate(time_traveled_trip = cumsum(time_traveled))

  # Recalculate trip_id after removing wrong direction data
  filtered_data <- filtered_data %>% group_by(id_veiculo, data) %>% mutate(trip_id = cumsum(direction != lag(direction, default = first(direction)) | hora - lag(hora, default = first(hora)) > 300))

  # Normalize the time traveled to start at 0 for each trip
  filtered_data <- filtered_data %>% group_by(id_veiculo, trip_id, data) %>% mutate(time_traveled_trip = time_traveled_trip - min(time_traveled_trip))

  print(paste("Data treated for route", route, "with", nrow(filtered_data), "data points in correct direction"))

  # Get trips that the distance traveled was almost the same for more than 20 data points (10 minutes)
  dead_trips_count <- 20
  dead_trips_threshold <- 100
  dead_trips <- filtered_data
  for (i in 1:dead_trips_count) {
    dead_trips <- dead_trips %>% group_by(id_veiculo, trip_id, data) %>% filter(abs(lead(distance_traveled, i) - distance_traveled) < dead_trips_threshold)
  }
  dead_trips <- dead_trips %>% select(id_veiculo, trip_id, data) %>% distinct()

  print(paste(nrow(dead_trips), "dead trips found for route", route))

  # Get all the dead trips data
  dead_trips_data <- filtered_data %>% inner_join(dead_trips, by = c("id_veiculo", "trip_id", "data"))

  # Plot a scatter plot of time traveled vs distance traveled with low opacity
  plot_dead_trips <- dead_trips_data %>% ggplot(aes(x = time_traveled_trip, y = distance_traveled, color = as.factor(direction))) +
    geom_point(alpha = 0.2) +
    labs(title = "Tempo de viagem vs Distância percorrida", x = "Tempo de viagem (s)", y = "Distância percorrida (m)")

  # Get trips that the beggining (first minute) of the trip has a distance traveld greater than 2000 meters
  max_distance_traveled <- 2000
  middle_trips <- filtered_data %>% group_by(id_veiculo, trip_id, data) %>% filter(time_traveled_trip < 60 & distance_traveled > max_distance_traveled) %>% select(id_veiculo, trip_id, data) %>% distinct()

  # Get all the middle trips data
  middle_trips_data <- filtered_data %>% inner_join(middle_trips, by = c("id_veiculo", "trip_id", "data"))

  # Plot a scatter plot of time traveled vs distance traveled for the middle trips data
  plot_middle_trips <- middle_trips_data %>% ggplot(aes(x = time_traveled_trip, y = distance_traveled, color = as.factor(direction))) +
    geom_point(alpha = 0.2) +
    labs(title = "Tempo de viagem vs Distância percorrida", x = "Tempo de viagem (s)", y = "Distância percorrida (m)")

  print(paste(nrow(middle_trips), "middle trips found for route", route))

  # Remove all the dead trips data
  filtered_data <- filtered_data %>% anti_join(dead_trips, by = c("id_veiculo", "trip_id", "data"))

  # Remove all the middle trips data
  filtered_data <- filtered_data %>% anti_join(middle_trips, by = c("id_veiculo", "trip_id", "data"))

  print(paste("In the end, there are", nrow(filtered_data), "data points for route", route))

  # Plot a scatter plot of time traveled vs distance traveled
  plot_time_distance <- filtered_data %>% ggplot(aes(x = time_traveled_trip, y = distance_traveled, color = as.factor(direction))) +
    geom_point(alpha = 0.2) +
    labs(title = "Tempo de viagem vs Distância percorrida", x = "Tempo de viagem (s)", y = "Distância percorrida (m)")

  # Clip the mean speed at 1 minute between 0 and 60 km/h
  filtered_data$mean_speed_1_min <- pmin(pmax(filtered_data$mean_speed_1_min, 0), 60)

  # Clip the mean speed at 3 minutes between 0 and 60 km/h
  filtered_data$mean_speed_3_min <- pmin(pmax(filtered_data$mean_speed_3_min, 0), 60)

  # Clip the mean speed at 5 minutes between 0 and 60 km/h
  filtered_data$mean_speed_5_min <- pmin(pmax(filtered_data$mean_speed_5_min, 0), 60)

  # Save plots in a file
  ggsave(paste0("plots/plot_dead_trips/", route, ".png"), plot_dead_trips)
  ggsave(paste0("plots/plot_middle_trips/", route, ".png"), plot_middle_trips)
  ggsave(paste0("plots/plot_time_distance/", route, ".png"), plot_time_distance)

  return(filtered_data)
}


evaluate_model <- function(data_path, route) {
#' @title Evaluate model
#' @description This function evaluates a generalized linear model with a normal distribution to predict the time traveled based on the distance traveled, the direction, the mean speed at 10 minutes, if it is weekend or not, and the time period
#' @param data_path The path to the data set
#' @param route The route number
#' @return The AIC and RMSE of the model

  # Load the data
  data <- read.csv(data_path)

  # Order "week_day" as a factor
  data$week_day <- factor(data$week_day, levels = c("domingo", "segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado"))

  # Fit a generalized linear model with a normal distribution to predict the time traveled based on the distance traveled, the direction, the mean speed at 10 minutes, if it is weekend or not, and the time period
  time_period_model <- lm(time_traveled_trip ~ distance_traveled:as.factor(direction):as.factor(is_weekend) + distance_traveled:as.factor(direction):mean_speed_5_min, data = data)
  # Calculate the errors of the time period model
  data$predicted_time_traveled <- predict(time_period_model, newdata = data)
  data$error_time_period <- data$time_traveled_trip - data$predicted_time_traveled

  # Plot the errors of the time period model for each day of the week
  plot_errors_time_period_week_day <- data %>% ggplot(aes(x = week_day, y = error_time_period)) +
    geom_boxplot(fill = "blue", color = "black") +
    labs(title = "Erros do modelo de período de tempo para cada dia da semana", x = "Dia da semana", y = "Erro (s)") + ylim(-300, 300) +
    facet_wrap(~time_period) + theme_minimal()

  # Plot the errors of the time period model for each day of the week and each interval of 500 meters traveled
  plot_errors_week_day_distance <- data %>% filter(distance_traveled <= 5000) %>% ggplot(aes(x = cut(distance_traveled, breaks = seq(0, 5000, by = 500)), y = error_time_period)) +
    geom_boxplot(fill = "blue", color = "black") +
    labs(title = "Erros do modelo de período de tempo para cada dia da semana e intervalo de 500 metros percorridos", x = "Distância percorrida (m)", y = "Erro (s)") + ylim(-300, 300) +
    scale_x_discrete(labels = c("0-500", "500-1000", "1000-1500", "1500-2000", "2000-2500", "2500-3000", "3000-3500", "3500-4000", "4000-4500", "4500-5000")) +
    facet_wrap(~week_day) + theme_minimal()

  # Plot predicted time vs real time
  plot_predicted_time_vs_observed <- data %>% ggplot(aes(x = time_traveled_trip, y = predicted_time_traveled)) +
    geom_point(color = "blue") +
    geom_abline(intercept = 0, slope = 1, color = "red") +
    labs(title = "Tempo de viagem previsto vs real", x = "Tempo de viagem real (s)", y = "Tempo de viagem previsto (s)") +
    theme_minimal()

  # Save the plots
  ggsave(paste0("plots/plot_errors_time_period_week_day/", route, ".png"), plot_errors_time_period_week_day, height = 8, width = 18)
  ggsave(paste0("plots/plot_errors_week_day_distance/", route, ".png"), plot_errors_week_day_distance, height = 8, width = 20)
  ggsave(paste0("plots/plot_predicted_time_vs_observed/", route, ".png"), plot_predicted_time_vs_observed, height = 8, width = 8)

  # Get model AIC
  aic_time_period <- AIC(time_period_model)

  # Calculate the RMSE of the time period model
  rmse_time_period <- modelr::rmse(time_period_model, data)

  return(c(aic_time_period, rmse_time_period))
}


#  --------------------------- Data treatment ---------------------------

# Create the treated data folder if it does not exist
if (!dir.exists("plots")) {
  dir.create("plots")
}

# Get all file names in the data folder
SOURCE_DATA_DIR <- "data/results_march_2024/train"
file_names <- list.files(SOURCE_DATA_DIR, full.names = TRUE)

# Treat all the data from the data folder
TREATED_DATA_DIR <- "data/treated/train"
for (file_name in file_names) {
  route <- strsplit(strsplit(file_name, "/")[[1]][4], "_")[[1]][1]
  filtered_data <- treat_data(file_name, route)
  write.csv(filtered_data, paste0(TREATED_DATA_DIR, route, "_train_data_treated.csv"), row.names = FALSE)
}


# --------------------------- Model evaluation ---------------------------

# Get all file names in the processed data folder
processed_file_names <- list.files(TREATED_DATA_DIR, full.names = TRUE)
evaluate_model(processed_file_names[1], "409")

# List to store the metrics of the models
metrics_list <- list()

# Iterate over all the processed files and evaluate the models
for (file_name in processed_file_names) {
  route <- strsplit(strsplit(file_name, "/")[[1]][4], "_")[[1]][1]
  options(warn=-1)
  metrics <- evaluate_model(file_name, route)
  print(paste("Metrics for route", route, "are AIC:", metrics[1], "RMSE:", metrics[2]))
  metrics_list[[route]] <- metrics
}

# Convert the list to a data frame
metrics_df <- do.call(rbind, metrics_list)

# Change the column names
colnames(metrics_df) <- c("AIC", "RMSE")

# Order the data frame by RMSE
metrics_df <- metrics_df[order(metrics_df[, "RMSE"]), ]

# Print the data frame
print(metrics_df)