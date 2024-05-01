import os
import src.utils as utils

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

class GPSHandler:

    def __init__(self, gps_folder_path):
        self.gps_folder_path = gps_folder_path

        self.gps_df = pd.DataFrame()
        self.load_data()

    def load_data(self):

        # Get all the files in the folder
        directory_files = os.listdir(self.gps_folder_path)

        print(f"Loading GPS data from {len(directory_files)} CSV files...")

        # Iterate through all the files in the folder
        for file in directory_files:
            # If the file is a CSV file, read it and concatenate it to the main dataframe
            if file.endswith(".csv"):
                df = pd.read_csv(f"{self.gps_folder_path}/{file}")
                self.gps_df = pd.concat([self.gps_df, df])

        # Sort the dataframe by timestamp_gps
        self.gps_df = self.gps_df.sort_values(by='timestamp_gps')

        print("GPS data loaded successfully!")

    def show_buses(self):
        # Print the value counts of the buses
        print(self.gps_df['id_veiculo'].value_counts())

    def get_bus_data(self, bus_id):
        # Get the data for a specific bus
        self.gps_df = self.gps_df[self.gps_df['id_veiculo'] == bus_id]
        return self.gps_df

    # def filter_by_bus(): # FILTER according to the amount/frequency/time window of the data collected by each bus

    def plot_gps_data(self, data=None, route=None, title='GPS Data'):

        if data is None:
            data = self.gps_df

        fig, ax = plt.subplots(1, 1, figsize=(10, 8))

        # Plot the GPS data
        data.plot(x='longitude', y='latitude', ax=ax, color='blue', alpha=0.3, kind='scatter')

        # Plot the route segments as a line (if available)
        if route is not None:
            for segment in route:
                x, y = zip(*segment)
                ax.plot(x, y, color='black')

        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_aspect('equal')

        plt.title(title)
        plt.grid()

        plt.show()

    def filter_gps_coordinates(self, gtfs, tolerance_meters=100):

        # Get the route directions
        route_directions = gtfs.route_trips['direction_id'].unique()

        # Get the route segments set for each direction
        route_segments_by_direction = [gtfs.get_route_segments_by_direction(direction) for direction in route_directions]

        # Iterate over each direction and the corresponding route segments
        for direction, route_segments in zip(route_directions, route_segments_by_direction):

            # Get the minimum distance from each point to the route
            min_distances, closest_segment_indexes = utils.closest_projection(self.gps_df[['longitude', 'latitude']].values, route_segments)

            # Store the minimum distance and the closest segment index on the dataframe
            self.gps_df[f'min_distance_{direction}'] = min_distances
            self.gps_df[f'closest_segment_index_{direction}'] = closest_segment_indexes

        # Get the mean latitude for distance conversion
        mean_latitude = self.gps_df['latitude'].mean()

        # Iterate over the directions, converting the values obtained to meters
        for direction in route_directions:
            min_distances = np.array(self.gps_df[f'min_distance_{direction}'])
            min_distances = np.sqrt(min_distances) # Take the sqrt cause "closest_projection" reports the squared distance for optimization
            
            # Convert the minimum distances measures from degress to meters
            self.gps_df[f'min_distance_{direction}'] = utils.degrees_to_meters(min_distances, mean_latitude)

        # For each gps point, take the mimimum distance from the route among all directions
        min_distances = self.gps_df[[f'min_distance_{direction}' for direction in route_directions]].min(axis=1)

        # Assign the flgag 'on_route' column based on the tolerance distance
        self.gps_df['on_route'] = min_distances < tolerance_meters

        return self.gps_df





