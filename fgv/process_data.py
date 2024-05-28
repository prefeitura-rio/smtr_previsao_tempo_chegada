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

# Define if the output must be overwritten
OVERWRITE = True

# Define min and max boundaries for number of points in a route for a bus
MIN_POINTS = 1000
MAX_POINTS = 2880

# Define the paths to the GTFS and GPS data
GTFS_FOLDER = "./data/gtfs_data"
GPS_FOLDER = "./data/gps_data"

# Load the GTFS data
print("Loading GTFS data...")
gtfs = gtfs_handler.GTFSHandler(GTFS_FOLDER)

# Load the GPS data
print("Loading GPS data...")
gps = gps_handler.GPSHandler(GPS_FOLDER)

original_files = os.listdir(GPS_FOLDER)
files = []
print("Files in the GPS data folder:")
for file in original_files:
    print(file)

    # If the filename is not in the format "YYYY-MM-DD.csv", split into files with the same date and delete the original file
    if not re.match(r"\d{4}-\d{2}-\d{2}.csv", file):
        print(f"File {file} does not match the format 'YYYY-MM-DD.csv'. Splitting the file...")

        gps.split_file(GPS_FOLDER, file)

        files = os.listdir(GPS_FOLDER)

num_files = len(files)

output_path = "./data/output/"

if OVERWRITE:
    # Delete the output folder if it exists
    if os.path.exists(output_path):
        print("Deleting the output folder...")
        shutil.rmtree(output_path)

file_counter = 0
# Iterate over the GPS data files
for file in files:
    gps.load_file_data(file)

    file_routes = gps.show_routes()
    num_routes = len(file_routes)
    file_counter += 1

    file_output_path = output_path + file.split(".")[0] + "/"
    # Create folder if it doesn't exist
    if not os.path.exists(file_output_path):
        os.makedirs(file_output_path)

    route_counter = 0
    # Iterate over the routes
    for route in file_routes:

        route_output_path = file_output_path + str(route) + "/"
        # Create folder if it doesn't exist
        if not os.path.exists(route_output_path):
            os.makedirs(route_output_path)
        else:
            # If the path already exists, skip the route
            print(f"Route {route} already processed. Skipping...")
            route_counter += 1
            continue
        
        try:
            # Get the route data
            gps.get_route_data(route)
            gtfs.filter_by_route(str(route))
        except Exception as e:
            print(f"Error filtering the GTFS data for the route {route}: {e}")
            route_counter += 1
            continue

        # Plot route, directions and stops
        gtfs.plot_route(title=f"Route {route}", save_path=route_output_path + f"route_{route}.png") 

        route_buses = gps.show_buses(route, filter_min=MIN_POINTS, filter_max=MAX_POINTS)
        num_buses = len(route_buses)
        route_counter += 1

        bus_progress_bar = tqdm(total=num_buses, position=0, leave=True)

        bus_counter = 0
        # Iterate over the buses
        for bus in route_buses:

            bus_output_path = route_output_path + str(bus) + "/"
            # Create folder if it doesn't exist
            if not os.path.exists(bus_output_path):
                os.makedirs(bus_output_path)
            else:
                # If the path already exists, skip the bus
                print(f"Bus {bus} already processed. Skipping...")
                bus_counter += 1
                continue
                
            # Get the bus data
            gps.get_bus_data(bus)            

            # print(f"File: {file} - Route: {route} - Bus: {bus}: {gps.gps_df.shape[0]} points")
            print(f"File: {file}({file_counter}/{num_files}) - Route: {route}({route_counter}/{num_routes}) - Bus: {bus} ({bus_counter}/{num_buses}): {gps.gps_df.shape[0]} points")

            # Iterate over the bus data
            utils.process_bus_data(gps, gtfs, bus, route, bus_output_path)

            # Append the training data
            training_append_output_path = output_path + f"{route}_train_data.csv"
            gps.gps_df.to_csv(training_append_output_path, mode='a', index=False, header=not os.path.exists(training_append_output_path))
            # Append the validation data
            validation_append_output_path = output_path + f"{route}_val_data.csv"
            gps.validation_df.to_csv(validation_append_output_path, mode='a', index=False, header=not os.path.exists(validation_append_output_path))

            bus_counter += 1
            # Update the progress bar
            bus_progress_bar.update(1)

