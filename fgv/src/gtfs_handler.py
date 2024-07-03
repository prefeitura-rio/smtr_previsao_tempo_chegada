import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import src.utils as utils

class GTFSHandler:
    def __init__(self, gtfs_folder_path):
        """
        Initialize the GTFSHandler with the path to the folder containing GTFS data files.

        Args:
            gtfs_folder_path (str): Path to the folder containing GTFS data files.
        """

        self.gtfs_folder_path = gtfs_folder_path

        self.load_data()

    def load_data(self):
        """
        Load GTFS data from the specified folder.
        """

        self.agencies = pd.read_csv(f"{self.gtfs_folder_path}/agency.txt")
        self.calendar = pd.read_csv(f"{self.gtfs_folder_path}/calendar.txt")
        self.calendar_dates = pd.read_csv(f"{self.gtfs_folder_path}/calendar_dates.txt")
        self.fare_tributtes = pd.read_csv(f"{self.gtfs_folder_path}/fare_attributes.txt")
        self.fare_rules = pd.read_csv(f"{self.gtfs_folder_path}/fare_rules.txt")
        self.feed_info = pd.read_csv(f"{self.gtfs_folder_path}/feed_info.txt")
        self.frequencies = pd.read_csv(f"{self.gtfs_folder_path}/frequencies.txt")
        self.routes = pd.read_csv(f"{self.gtfs_folder_path}/routes.txt")
        self.shapes = pd.read_csv(f"{self.gtfs_folder_path}/shapes.txt")
        self.stop_times = pd.read_csv(f"{self.gtfs_folder_path}/stop_times.txt")
        self.stops = pd.read_csv(f"{self.gtfs_folder_path}/stops.txt")
        self.trips = pd.read_csv(f"{self.gtfs_folder_path}/trips.txt")

        print("GTFS data loaded successfully!")

    def filter_by_route(self, route_short_name):
        """
        Filter the GTFS data by the specified route short name.

        Args:
            route_short_name (str): Short name of the route to filter by.
        """

        print(f"Filtering the data by the route {route_short_name}...")
        print("DF ROUTE ID HEAD:", self.routes[self.routes['route_short_name'] == route_short_name].head())

        # Get the route id
        self.route_id = self.routes[self.routes['route_short_name'] == route_short_name]['route_id'].values[0]

        # Filter the trips by the route id
        self.route_trips = self.trips[self.trips['route_id'] == self.route_id]

        # Filter the stops data by the route trips
        self.route_stop_times = self.stop_times[self.stop_times['trip_id'].isin(self.route_trips['trip_id'])]
        self.route_stop_ids = self.route_stop_times['stop_id'].unique()
        self.route_stops = self.stops[self.stops['stop_id'].isin(self.route_stop_ids)]

        # Get a summary of the stops data merging the stop times and trips
        merged_df = pd.merge(self.route_stop_times, self.route_trips[["trip_id", "direction_id", "shape_id"]], on='trip_id')
        merged_df = pd.merge(merged_df, self.route_stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]], on='stop_id')
        # Drop duplicates and unnecessary columns
        merged_df = merged_df.drop(columns=["trip_id", "arrival_time", "departure_time", "stop_headsign", "timepoint"])
        # Rename the shape_dist_traveled column
        merged_df = merged_df.rename(columns={"shape_dist_traveled": "stop_distance"})
        merged_df = merged_df.drop_duplicates()
        # Sort by direction_id and stop_sequence
        merged_df = merged_df.sort_values(by=["direction_id", "stop_sequence"])
        # Update the route stops data with the merged data as geopandas dataframe
        self.route_stops = gpd.GeoDataFrame(merged_df, geometry=gpd.points_from_xy(merged_df.stop_lon, merged_df.stop_lat))

        # Filter the shapes data by the route trips
        self.route_shape_ids = self.route_trips['shape_id'].unique()

        assert len(self.route_shape_ids) >= 1, "The route must have at least one shape!"

        self.route_shapes = self.shapes[self.shapes['shape_id'].isin(self.route_shape_ids)]
        self.route_shape_points = [tuple(x) for x in self.route_shapes[['shape_pt_lon', 'shape_pt_lat']].values]
        self.route_shape_segments = [tuple(self.route_shape_points[i:i+2]) for i in range(len(self.route_shape_points)-1)]

    def get_route_segments_by_direction(self, direction_id):
        """
        Get the route segments for the specified direction.

        Args:
            direction_id (int): Identifier of the direction.

        Returns:
            list: List of route segments for the specified direction.
        """

        assert len(self.route_shape_ids) >= 1, "Please filter the data by a route first!"

        # Get the shape ids for the direction
        shape_ids = self.route_trips[self.route_trips['direction_id'] == direction_id]['shape_id'].unique()

        # Get the shape points for the direction
        shape_points = [tuple(x) for x in self.shapes[self.shapes['shape_id'].isin(shape_ids)][['shape_pt_lon', 'shape_pt_lat']].values]

        # Get the shape segments for the direction
        shape_segments = [tuple(shape_points[i:i+2]) for i in range(len(shape_points)-1)]

        return shape_segments
    
    def get_shape_by_direction(self, direction_id):
        """
        Get the shape for the specified direction.

        Args:
            direction_id (int): Identifier of the direction.

        Returns:
            pandas.DataFrame: Dataframe containing shape points for the specified direction.
        """

        assert len(self.route_shape_ids) >= 1, "Please filter the data by a route first!"

        # Get the shape ids for the direction
        shape_ids = self.route_trips[self.route_trips['direction_id'] == direction_id]['shape_id'].unique()

        # Get the shape for the direction
        return self.shapes[self.shapes['shape_id'].isin(shape_ids)]

    def plot_route(self, title='Route', save_path=None):
        """
        Plot the route on a map, optionally saving the plot.

        Args:
            title (str, optional): Title of the plot. Defaults to 'Route'.
            save_path (str, optional): Path to save the plot image. Defaults to None.
        """
        
        assert len(self.route_shape_ids) >= 1, "Please filter the data by a route first!"

        fig, ax = plt.subplots(1, 1, figsize=(10, 8))
        
        for direction_id in self.route_trips['direction_id'].unique():
            route_segments = self.get_route_segments_by_direction(direction_id)
            color = 'orange' if direction_id == 0 else 'green'
            for segment in route_segments:
                x, y = zip(*segment)
                ax.plot(x, y, color=color)

        # Plot the stops, colored according to the direction id
        self.route_stops[self.route_stops['direction_id'] == 0].plot(ax=ax, color='orange', label='Direction 0')
        self.route_stops[self.route_stops['direction_id'] == 1].plot(ax=ax, color='green', label='Direction 1')

        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.set_aspect('equal')

        plt.title(title)
        plt.legend()
        plt.grid()

        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()

        # Close the plot
        plt.close()

    def get_stops_by_direction(self):
        """
        Get the stops and distances by direction for the filtered route.
        """

        # Get the route directions
        route_directions = self.route_stops['direction_id'].unique()

        # Initialize the lists to store the stops and distances by direction
        self.stops_by_direction = []
        self.stops_distances_by_direction = []

        # Iterate over the directions
        for direction in route_directions:

            # Get the stops for each direction
            self.stops_by_direction.append(self.route_stops[self.route_stops['direction_id'] == direction])
            # Get a np array with the stop_distance for each stop and each direction
            self.stops_distances_by_direction.append(np.array(self.stops_by_direction[direction]['stop_distance'].values))