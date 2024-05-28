import os
import src.utils as utils

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

class GPSHandler:

    def __init__(self, gps_folder_path):
        self.gps_folder_path = gps_folder_path

        self.gps_all_df = pd.DataFrame()

        self.gps_df = pd.DataFrame()

    def load_file_data(self, filename):
    
        print(f"Loading GPS data from the file {filename} CSV files...")

        if filename.endswith(".csv"):
            self.gps_all_df = pd.read_csv(f"{self.gps_folder_path}/{filename}")

        # Drop unnecessary columns
        unnecessary_columns = ['modo', 'flag_em_operacao', 'flag_linha_existe_sigmob', 'flag_trajeto_correto', 'flag_trajeto_correto_hist', 'versao']
        self.gps_all_df = self.gps_all_df.drop(columns=unnecessary_columns)

        # Sort the dataframe by timestamp_gps
        self.gps_all_df = self.gps_all_df.sort_values(by='timestamp_gps')

        # Re-index the dataframe
        self.gps_all_df = self.gps_all_df.reset_index(drop=True)

        print("GPS data loaded successfully!")

    def load_data(self):

        # Get all the files in the folder
        directory_files = os.listdir(self.gps_folder_path)

        print(f"Loading GPS data from {len(directory_files)} CSV files...")

        self.gps_all_df = pd.DataFrame()
        self.gps_df = pd.DataFrame()

        # Iterate through all the files in the folder
        for file in directory_files:
            # If the file is a CSV file, read it and concatenate it to the main dataframe
            if file.endswith(".csv"):
                df = pd.read_csv(f"{self.gps_folder_path}/{file}")
                self.gps_all_df = pd.concat([self.gps_all_df, df])

        # Drop unnecessary columns
        unnecessary_columns = ['modo', 'flag_em_operacao', 'flag_linha_existe_sigmob', 'flag_trajeto_correto', 'flag_trajeto_correto_hist', 'versao']
        self.gps_all_df = self.gps_all_df.drop(columns=unnecessary_columns)

        # Sort the dataframe by timestamp_gps
        self.gps_all_df = self.gps_all_df.sort_values(by='timestamp_gps')

        # Re-index the dataframe
        self.gps_all_df = self.gps_all_df.reset_index(drop=True)

        print("GPS data loaded successfully!")

    def get_routes(self):
        # Get the unique routes
        return self.gps_all_df['servico'].unique()
    
    def get_routes_count(self):
        # Get the unique routes
        return self.gps_all_df['servico'].value_counts()
    
    def show_routes(self):
        # Print the value counts of the routes
        print(self.get_routes_count())

        # Get the unique routes
        routes = self.get_routes()

        print(f"Found {len(routes)} routes:")
        print(routes)

        return routes
    
    def get_buses(self):
        # Get the unique buses
        buses = self.gps_all_df['id_veiculo'].unique()

        return buses

    def show_buses(self, route_id, filter_min=None, filter_max=None):
        # Get the buses for the route
        buses = self.gps_df['id_veiculo'].unique()
        buses_value_counts = self.gps_df['id_veiculo'].value_counts()

        total_num_buses = len(buses)

        # Filter the buses by the minimum and maximum values
        if filter_min is not None:
            buses = [bus for bus in buses if buses_value_counts[bus] >= filter_min]
        
        if filter_max is not None:
            buses = [bus for bus in buses if buses_value_counts[bus] <= filter_max]

        print(f"Route {route_id} has {len(buses)}/{total_num_buses} elegible buses:")
        print(buses)

        return buses        

    def get_bus_data(self, bus_id):
        # Get the data for a specific bus
        self.gps_df = self.gps_all_df[self.gps_all_df['id_veiculo'] == bus_id]

        # Re-index the dataframe
        self.gps_df = self.gps_df.reset_index(drop=True)

        return self.gps_df
    
    def get_route_data(self, route_id):
        # Get the data for a specific route
        self.gps_df = self.gps_all_df[self.gps_all_df['servico'] == route_id]

        # Re-index the dataframe
        self.gps_df = self.gps_df.reset_index(drop=True)

        return self.gps_df

    # def filter_by_bus(): # FILTER according to the amount/frequency/time window of the data collected by each bus

    def plot_gps_data(self, data=None, route=None, title='GPS Data', save_path=None):

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

        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()

        # Close the plot
        plt.close()

    def filter_gps_coordinates(self, gtfs, tolerance_meters=100):

        # Get the route directions
        self.route_directions = gtfs.route_stops['direction_id'].unique()

        # Get the route segments set for each direction
        route_segments_by_direction = [gtfs.get_route_segments_by_direction(direction) for direction in self.route_directions]

        # Iterate over each direction and the corresponding route segments
        for direction, route_segments in zip(self.route_directions, route_segments_by_direction):

            # Get the minimum distance from each point to the route
            min_distances, closest_segment_indexes = utils.closest_projection(self.gps_df[['longitude', 'latitude']].values, route_segments)

            # Store the minimum distance and the closest segment index on the dataframe
            self.gps_df[f'min_distance_{direction}'] = min_distances
            self.gps_df[f'closest_segment_index_{direction}'] = closest_segment_indexes

        # Get the mean latitude for distance conversion
        mean_latitude = self.gps_df['latitude'].mean()

        # Iterate over the directions, converting the values obtained to meters
        for direction in self.route_directions:
            min_distances = np.array(self.gps_df[f'min_distance_{direction}'])
            min_distances = np.sqrt(min_distances) # Take the sqrt cause "closest_projection" reports the squared distance for optimization
            
            # Convert the minimum distances measures from degress to meters
            self.gps_df[f'min_distance_{direction}'] = utils.degrees_to_meters(min_distances, mean_latitude)

        # For each gps point, take the mimimum distance from the route among all directions
        min_distances = self.gps_df[[f'min_distance_{direction}' for direction in self.route_directions]].min(axis=1)

        # Assign the flgag 'in_route' column based on the tolerance distance
        self.gps_df['in_route'] = min_distances < tolerance_meters

        return self.gps_df

    def get_distance_from_start(self, gtfs):

        # Iterate over each direction and the corresponding route segments
        for direction in self.route_directions:

            # Get the shape for the direction
            route_shape = gtfs.get_shape_by_direction(direction)

            # Get the shape points and distances
            # route_points = np.array(route_shape[['shape_pt_lon', 'shape_pt_lat']])
            # route_distances = np.array(route_shape['shape_dist_traveled'])
            # # Craft the route segments
            # route_segments = np.array([tuple(route_points[i:i+2]) for i in range(len(route_points)-1)])

            # coord_x, coord_y, distance
            route_points_array = np.array(route_shape[['shape_pt_lon', 'shape_pt_lat', 'shape_dist_traveled']], dtype=np.float32)

            # Get the GPS point coordinates and the closest segment index for each GPS point
            gps_points = np.array(self.gps_df[['longitude', 'latitude', f'closest_segment_index_{direction}']])

            results = np.zeros(gps_points.shape[0], dtype=np.float32)

            for i, point in enumerate(gps_points):

                closest_segment_index = int(point[2])
                closest_segment_start = route_points_array[closest_segment_index]
                closest_segment_end = route_points_array[closest_segment_index + 1]

                # Get the distance from the start of the segment
                results[i] = utils.distance_travelled(point[0], point[1],
                                                    closest_segment_start[0], closest_segment_start[1], closest_segment_start[2],
                                                    closest_segment_end[0], closest_segment_end[1], closest_segment_end[2])
                
            self.gps_df[f'distance_from_start_{direction}'] = results

    def split_file(self, file_path, file_name):
        # Split the file into the differente dates "YYYY-MM-DD" and delete the original file
        raw_file = pd.read_csv(f"{file_path}/{file_name}")

        # Get the unique dates
        dates = raw_file['data'].unique()

        # Iterate over the dates
        for date in dates:
            date_file = raw_file[raw_file['data'] == date]

            # Save the file
            date_file.to_csv(f"{file_path}/{date}.csv", index=False)

        # Delete the original file
        os.remove(f"{file_path}/{file_name}")



