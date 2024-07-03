import src.utils as utils
import src.gtfs_handler as gtfs_handler
import src.gps_handler as gps_handler

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import os
import re
import shutil

from numba import jit
from tqdm import tqdm

import time

def define_output_path(base_path, folder_name):
    # Define the output path for the file
    file_output_path = base_path + folder_name + "/"
    # Create folder if it doesn't exist
    if not os.path.exists(file_output_path):
        os.makedirs(file_output_path)
        return file_output_path
    else:
        # If the path already exists, skip the file
        return None

# Get the start time of the loading process
loading_start_time = time.time()

# Define if the output must be overwritten or if the data must be appended to the existing files
OVERWRITE = True

# Define min and max boundaries for number of points in a route for a bus to be considered
MIN_POINTS = 300
MAX_POINTS = 2880

# Define the paths to the GTFS and GPS data
GTFS_FOLDER = "./data/gtfs_data"
GPS_FOLDER = "./data/gps_data"

# Define the path to the output folder
OUTPUT_FOLDER = "./data/output/"

# If the output folder exists and the OVERWRITE flag is set to True, delete the folder
if OVERWRITE:
    if os.path.exists(OUTPUT_FOLDER):
        print("Deleting the output folder...")
        shutil.rmtree(OUTPUT_FOLDER)

# Load the GTFS data
print("Loading GTFS data...")
gtfs = gtfs_handler.GTFSHandler(GTFS_FOLDER)

# Load the GPS data
print("Loading GPS data...")
gps = gps_handler.GPSHandler(GPS_FOLDER)

# Check if the GPS data files are in the correct format (YYYY-MM-DD.csv)
# If not, split the files into files with the same date and remove the original file
original_files = os.listdir(GPS_FOLDER)
files = []
print("Files in the GPS data folder:")
for file in original_files:
    print(file)
    # If the filename is not in the format "YYYY-MM-DD.csv", split into files with the same date and delete the original file
    if not re.match(r"\d{4}-\d{2}-\d{2}.csv", file) and file.endswith(".csv"):
        print(f"File {file} does not match the format 'YYYY-MM-DD.csv'. Splitting the file...")
        gps.split_file(GPS_FOLDER, file)

    # Update the list of files in the folder
    files = os.listdir(GPS_FOLDER)

# Get the number of files in the folder
num_files = len(files)

# Get the end time
loading_end_time = time.time()

print(f"Data loaded in {loading_end_time - loading_start_time:.2f} seconds")


print("Starting the data processing...")

# Iterate over the GPS data files
for file_counter, file in enumerate(files, start=1):

    # Skip files that are not csv files
    if not file.endswith(".csv"):
        print("Skipping file {file}...")
        continue

    # Load the file data
    gps.load_file_data(file)

    # Get the routes in the file
    file_routes = gps.show_routes()
    num_routes = len(file_routes)

    # Define the output path for the file and create the folder if it doesn't exist
    file_output_path = define_output_path(OUTPUT_FOLDER, file.split(".")[0])

    # Iterate over the routes
    for route_counter, route in enumerate(file_routes, start=1):
        
        # Define the output path for the route
        route_output_path = define_output_path(file_output_path, str(route))
        if route_output_path is None:
            print(f"Route {route} already processed. Skipping...")
            continue

        try:
            # Get the route data and filter the GTFS data according to the route
            gps.get_route_data(route)
            gtfs.filter_by_route(str(route))
        except Exception as e:
            # If an error occurs, skip the route
            print(f"Error filtering the GTFS data for the route {route}: {e}")
            continue

        # Plot route, directions and stops
        gtfs.plot_route(title=f"Route {route}", save_path=route_output_path + f"route_{route}.png") 

        # Get the buses in the route that have a number of points between the defined boundaries
        route_buses = gps.show_buses(route, filter_min=MIN_POINTS, filter_max=MAX_POINTS)
        num_buses = len(route_buses)

        # Create a progress bar for the buses of that specific route and file (day)
        bus_progress_bar = tqdm(total=num_buses, position=0, leave=True)

        # Iterate over the buses
        for bus_counter, bus in enumerate(route_buses, start=1):
                
            # Define the output path for the bus
            bus_output_path = define_output_path(route_output_path, str(bus))
            if bus_output_path is None:
                print(f"Bus {bus} already processed. Skipping...")
                bus_progress_bar.update(1)
                continue

            # Get the bus data
            gps.get_bus_data(bus)            

            print(f"File: {file}({file_counter}/{num_files}) - Route: {route}({route_counter}/{num_routes}) - Bus: {bus} ({bus_counter}/{num_buses}): {gps.gps_df.shape[0]} points")

            try:
                # Process the bus data
                utils.process_bus_data(gps, gtfs, bus, route, bus_output_path)
            except Exception as e:
                # If an error occurs, skip the bus data
                print(f"Error processing the data for the bus {bus}: {e}")
                # Update the progress bar
                bus_progress_bar.update(1)
                continue

            # Append the training and validation data
            training_append_output_path = OUTPUT_FOLDER + f"{route}_train_data.csv"
            validation_append_output_path = OUTPUT_FOLDER + f"{route}_val_data.csv"

            gps.gps_df.to_csv(training_append_output_path, mode='a', index=False, header=not os.path.exists(training_append_output_path))
            gps.validation_df.to_csv(validation_append_output_path, mode='a', index=False, header=not os.path.exists(validation_append_output_path))

            # Update the progress bar
            bus_progress_bar.update(1)

