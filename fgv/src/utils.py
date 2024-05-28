import numpy as np
import pandas as pd

from numba import jit

@jit(nopython=True)
def project_point_on_segment(px, py, ax, ay, bx, by):
    """
    Projects a point (px, py) onto a line segment defined by two points (ax, ay) and (bx, by).
    
    Args:
        px (float): x-coordinate of the point to be projected.
        py (float): y-coordinate of the point to be projected.
        ax (float): x-coordinate of the first point defining the line segment.
        ay (float): y-coordinate of the first point defining the line segment.
        bx (float): x-coordinate of the second point defining the line segment.
        by (float): y-coordinate of the second point defining the line segment.
    
    Returns:
        tuple: The coordinates of the closest point on the line segment to the given point.
    """
    
    # Vector from A to P
    apx, apy = px - ax, py - ay
    # Vector from A to B
    abx, aby = bx - ax, by - ay
    
    # Magnitude of AB vector (squared)
    ab2 = abx * abx + aby * aby
    
    if ab2 == 0:
        # A and B are the same points, no valid segment
        return ax, ay
    
    # Project AP onto AB to find the closest point
    ap_dot_ab = apx * abx + apy * aby
    t = ap_dot_ab / ab2

    if t < 0.0:
        # Closest to A
        return ax, ay
    elif t > 1.0:
        # Closest to B
        return bx, by
    else:
        # Projection point is on the segment
        closestx = ax + t * abx
        closesty = ay + t * aby
        return closestx, closesty

@jit(nopython=True)
def squared_distance_to_segment(px, py, ax, ay, bx, by):
    """
    Calculates the squared distance between a point (px, py) and a line segment defined by two points (ax, ay) and (bx, by).

    Args:
        px (float): x-coordinate of the point
        py (float): y-coordinate of the point
        ax (float): x-coordinate of the first point of the line segment
        ay (float): y-coordinate of the first point of the line segment
        bx (float): x-coordinate of the second point of the line segment
        by (float): y-coordinate of the second point of the line segment

    Returns:
        float: The squared distance between the point and the line segment.
    """
    
    # Get the closest point on the segment
    closestx, closesty = project_point_on_segment(px, py, ax, ay, bx, by)
    
    # Get the difference between the point and the closest point
    dx, dy = px - closestx, py - closesty

    # Return the squared distance
    return dx * dx + dy * dy

@jit(nopython=True)
def closest_projection(points, route_segments):
    """
    Finds the closest projection of each point onto a set of route segments.

    Args:
        points (np.array): Array of points with shape (N, 2), where N is the number of points.
        route_segments (np.array): Array of route segments with shape (M, 2, 2), where M is the number of segments.
    
    Returns:
        min_squared_distances (np.array): Array of minimum squared distances from each point to its closest segment.
        closest_segment_indexes (np.array): Array of indexes of the closest segment for each point.
    """
    
    # Convert the numpy arrays to store data as np.float32
    points = points.astype(np.float32)
    route_segments = np.array(route_segments).astype(np.float32)

    # Get the number of points and segments to iterate over
    num_points = points.shape[0]
    num_segments = route_segments.shape[0]

    # Initialize with infinity
    min_squared_distances = np.full(num_points, np.inf)
    closest_segment_indexes = np.zeros(num_points, np.int32)

    # Iterate over each point and each segment
    for i in range(num_points):
        for j in range(num_segments):
            # Get the squared distance from the point to the segment
            distance = squared_distance_to_segment(points[i, 0], points[i, 1],
                                                   route_segments[j][0][0], route_segments[j][0][1],
                                                   route_segments[j][1][0], route_segments[j][1][1])
            # Save the minimum distance and the closest segment index
            if distance < min_squared_distances[i]:
                min_squared_distances[i] = distance
                closest_segment_indexes[i] = j

    return min_squared_distances, closest_segment_indexes

@jit(cache=True)
def get_latitude_meters_coefficient(latitude):
    """
    Get the conversion rate of meters to latitude degrees based on the latitude.

    Args:
        latitude (float): The latitude in degrees.
    
    Returns:
        float: The conversion rate of meters to latitude degrees.
    """

    # Conversion rate of meters to latitude degrees at the equator
    meters_per_degree = 111320
    
    # Adjust the conversion rate based on latitude
    latitude_adjusted_meters_per_degree = meters_per_degree * (1 - 0.00669438 * ((latitude * 0.0174533) ** 2))
    
    return latitude_adjusted_meters_per_degree

@jit(nopython=True)
def meters_to_degrees(meters, latitude):
    """
    Convert distance in meters to degrees of latitude.

    Args:
        meters (float): The distance in meters.
        latitude (float): The latitude in degrees.

    Returns:
        float: The change in latitude in degrees.
    """
    # Calculate the change in latitude from meters
    return meters / get_latitude_meters_coefficient(latitude)

@jit(nopython=True)
def degrees_to_meters(degrees, latitude):
    """
    Converts degrees to meters based on the latitude.

    Args:
        degrees (float): The value in degrees to be converted.
        latitude (float): The latitude in degrees.

    Returns:
        float: The converted value in meters.
    """

    # Calculate the change in meters from latitude
    return degrees * get_latitude_meters_coefficient(latitude)

@jit(nopython=True)
def distance_travelled(px, py, ax, ay, da, bx, by, db):
    """
    Calculates the distance traveled on a segment given a projected point.

    Args:
        px (float): The x-coordinate of the projected point.
        py (float): The y-coordinate of the projected point.
        ax (float): The x-coordinate of the segment start point.
        ay (float): The y-coordinate of the segment start point.
        da (float): The distance traveled from the route start to the segment start point.
        bx (float): The x-coordinate of the segment end point.
        by (float): The y-coordinate of the segment end point.
        db (float): The distance traveled from the route start to the segment end point.

    Returns:
        float: The distance traveled on the segment until the projected point.
    """

    # Get the projected point on the segment
    px_hat, py_hat = project_point_on_segment(px, py, ax, ay, bx, by)

    # Get the distance traveled until the projected point
    
    # If projected point = segment start
    if px_hat == ax and py_hat == ay:
        distance_traveled = da
    # If projected point = segment end
    elif px_hat == bx and py_hat == by:
        distance_traveled = db
    # Otherwise, sum the distance traveled from the segment start until the projected point
    else: 
        # Compare the x values to get the distance traveled
        if bx - ax == 0:
            distance_traveled = da + (py_hat - ay) / (by - ay) * (db - da)
        # Compare the y values if the x values are the same (to avoid division by zero)
        else:
            distance_traveled = da + (px_hat - ax) / (bx - ax) * (db - da)

    return distance_traveled

@jit(nopython=True)
def infer_bus_direction(distance_traveled_inbound, distance_traveled_outbound, tolerance=100):
    """
    Infers the bus direction based on the distance traveled for inbound and outbound routes.

    Args:
        distance_traveled_inbound (np.array): The distance traveled on the inbound route during last records.
        distance_traveled_outbound (np.array): The distance traveled on the outbound route during last records.

    Returns:
        int: The inferred bus direction (0 for inbound, 1 for outbound).
    """

    # Evaluate the mean of the distances traveled among the last records in each direction
    mean_dist_traveled_inbound = np.mean(distance_traveled_inbound) - distance_traveled_inbound[0]
    mean_dist_traveled_outbound = np.mean(distance_traveled_outbound) - distance_traveled_outbound[0]

    # Get the difference between the mean of the distances
    diff_mean_distance_traveled = mean_dist_traveled_inbound - mean_dist_traveled_outbound

    if abs(diff_mean_distance_traveled) > tolerance:
        if diff_mean_distance_traveled > 0:
            return 0
        else:
            return 1
    else:
        return -1
    
@jit(nopython=True)
def assign_direction(gps_in_route, gps_distance_dir_0, gps_distance_dir_1, N=5, terminal_tolerance=500):
    """
    Assigns the direction to a GPS point based on the distance from the start of the route.
    
    Parameters:
    gps_in_route (np.array): Array of boolean values indicating if the GPS point is in the route.
    gps_distance_dir_0 (np.array): Array of distances traveled on the inbound route.
    gps_distance_dir_1 (np.array): Array of distances traveled on the outbound route.

    Returns:
    np.array: Array of inferred directions for each GPS point (-1 for unknown, 0 for inbound, 1 for outbound)
    """

    # Initialize the result array with -1 for unknown direction
    result = np.full(len(gps_in_route), -1)
    directly_infered = np.full(len(gps_in_route), False)

    # Iterate over the GPS points, starting from the N-th point
    for i in range(N, len(gps_in_route)):
        # If the GPS point is not in the route, skip
        if gps_in_route[i] == False:
            continue

        # If the GPS point is too far from a terminal, don't allow direction changes
        if i > 1 and result[i-1] != -1 and (gps_distance_dir_0[i] > terminal_tolerance and gps_distance_dir_1[i] > terminal_tolerance):
            result[i] = result[i-1]
            continue

        # Get the last N elements of each direction
        result[i] = infer_bus_direction(gps_distance_dir_0[i-N:i], gps_distance_dir_1[i-N:i])
        if result[i] != -1:
            directly_infered[i] = True

        # If the last datapoints are in route and the directions are still unknown, assign the direction infered
        for j in range(i, 0, -1):
            if result[j] != -1 and result[j-1] == -1 and gps_in_route[j-1] == True:
                result[j-1] = result[j]
            else:
                break

    # Return the array with the infered directions and the method used to infer them
    return result, directly_infered

@jit(nopython=True)
def assign_distance_traveled(gps_timestamp, gps_in_route, gps_direction, gps_distance_dir_0, gps_distance_dir_1):
    """
    Assigns the distance traveled to each GPS point based on the infered direction.

    Args:
        gps_in_route (np.array): Array of boolean values indicating if the GPS point is in the route.
        gps_direction (np.array): Array of inferred directions for each GPS point (-1 for unknown, 0 for inbound, 1 for outbound).
        gps_distance_dir_0 (np.array): Array of distances traveled on the inbound route.
        gps_distance_dir_1 (np.array): Array of distances traveled on the outbound route.

    Returns:
        tuple: Arrays of distances traveled and cumulative distances traveled for each GPS point.
    """

    # Initialize arrays to store the distance traveled and the cumulative distance traveled
    distance_traveled = np.zeros(len(gps_in_route))
    cumulative_distance_traveled = np.zeros(len(gps_in_route))

    # Initialize arrays to store the time spent and the cumulative time spent
    time_spent = np.zeros(len(gps_in_route), dtype=np.int64)
    cumulative_time_spent = np.zeros(len(gps_in_route), dtype=np.int64)

    # Initialize the distance offset (to compensate direction changes)
    distance_offset = 0

    # Iterate over the GPS points
    for i in range(len(gps_in_route)):

        # Detect direction changes, and update the distance and time offset
        if i > 0:
            time_spent[i] = (gps_timestamp[i] - gps_timestamp[i-1])

            if gps_direction[i] != gps_direction[i-1]:
                distance_offset += distance_traveled[i-1]
                cumulative_time_spent[i] = time_spent[i]
            else:
                cumulative_time_spent[i] = cumulative_time_spent[i-1] + time_spent[i]

        # Reset the values if the day changed
        if i > 0 and gps_timestamp[i] // 86400 != gps_timestamp[i-1] // 86400:
            distance_offset = 0

        # If the GPS point is not in the route, pass (default distance is 0)
        if gps_in_route[i] == False:
            pass
        # Otherwise, set the distance traveled based on the direction (or 0, if the direction is unknown)
        else:
            if gps_direction[i] == 0:
                distance_traveled[i] = gps_distance_dir_0[i]
            elif gps_direction[i] == 1:
                distance_traveled[i] = gps_distance_dir_1[i]
            else:
                distance_traveled[i] = 0

        
        # Update the cumulative distance traveled
        cumulative_distance_traveled[i] = distance_traveled[i] + distance_offset

    # Return the distance arrays
    return distance_traveled, cumulative_distance_traveled, time_spent, cumulative_time_spent

@jit(nopython=True)
def assign_mean_speed(gps_in_route, gps_timestamps, gps_cumulative_distance, N=3):
    """
    Assigns the mean speed to each of the last N minutes of GPS data.

    Args:
        gps_timestamps (np.array): Array of timestamps for each GPS point.
        gps_in_route (np.array): Array of boolean values indicating if the GPS point is in the route.
        gps_cumulative_distance (np.array): Array of cumulative distances traveled for each GPS point.
        N (int, optional): Number of last minutes to calculate the mean speed. Defaults to 3.

    Returns:
        np.array: Array of mean speeds for each GPS point.
    """

    # Initialize the arrays to store the mean speeds for each minute and the accumulated mean speeds
    mean_speeds = np.zeros(len(gps_timestamps), dtype=np.float64)

    # Iterate over the gps data
    for i in range(1, len(gps_timestamps)):
        # Skip the points that are not in the route
        if gps_in_route[i] == False or gps_in_route[i-1] == False:
            continue

        # Get the current timestamp and distance
        current_timestamp = gps_timestamps[i]
        current_distance = gps_cumulative_distance[i]

        # Iterate backwards over the last N minutes of data, looking for the initial point
        for j in range(i - 1, -1, -1):

            # Get the timestamps and distances of the current and next points in the past to be compared
            past_timestamp = gps_timestamps[j]
            past_distance = gps_cumulative_distance[j]

            # If the index is the first or the difference between the timestamps exceeds N minutes, evaluate the mean speed
            if j == 0 or current_timestamp - past_timestamp >= 60 * N:
                # Evaluate the time and distance differences
                time_diff = current_timestamp - past_timestamp
                distance_diff = current_distance - past_distance

                # Ensure time_diff is not zero to avoid division by zero
                if time_diff > 0:
                    # Calculate the mean speed (in km/h)
                    mean_speeds[i] = distance_diff / time_diff * 3.6

                break
            

    # Return the array with the mean speeds
    return mean_speeds

@jit(nopython=True)
def get_closest_stop(gps_distance, stop_distances, mode="next"):
    """
    Get the index of the closest stop to the GPS distance.

    Args:
        gps_distance (float): The GPS distance traveled
        stop_distances (np.array): The distance traveled to each bus stop
        mode (str, optional): The mode to get the closest stop. Defaults to "next".

    Raises:
        ValueError: If an invalid mode is provided.

    Returns:
        int: The index of the closest stop.
    """

    match mode:
        # Get the next stop index
        case "next":
            # Get the smaller index where the stop distance is greater than the GPS distance
            return min(np.searchsorted(stop_distances, gps_distance, side="right"), len(stop_distances) - 1)

        # Get the last stop index
        case "last":
            # Get the largest index where the stop distance is smaller than the GPS distance
            return max(np.searchsorted(stop_distances, gps_distance, side="left"), 0)

        # Raise an error for invalid mode
        case _:
            raise ValueError("Invalid mode. Choose 'next' or 'last'.")

@jit(nopython=True)
def get_last_stop(gps_distance, stop_distances):
    """
    Get the index of the last stop before the GPS distance.

    Args:
        gps_distance (float): The GPS distance traveled
        stop_distances (np.array): The distance traveled to each bus stop

    Returns:
        int: The index of the last stop.
    """
    return get_closest_stop(gps_distance, stop_distances, mode="last")

@jit(nopython=True)
def get_next_stop(gps_distance, stop_distances):
    """
    Get the index of the next stop after the GPS distance.

    Args:
        gps_distance (float): The GPS distance traveled
        stop_distances (np.array): The distance traveled to each bus stop

    Returns:
        int: The index of the next stop.
    """
    return get_closest_stop(gps_distance, stop_distances, mode="next")

@ jit(nopython=True)
def assign_stops(gps_in_route, gps_direction, gps_distance, stops_distances_by_direction, stop_tolerance=20):
    """
    Assigns the stops to the GPS data based on the direction and distance from the start of the route.
    """
    # By default, the first direction is the first one in the list
    stop_distances = stops_distances_by_direction[gps_direction[0]]

    # Initialize the lists of last and next stops with -1, by default
    last_stops = np.full(len(gps_in_route), -1, dtype=np.int32)
    next_stops = np.full(len(gps_in_route), -1, dtype=np.int32)
    distance_to_last_stop = np.full(len(gps_in_route), -1, dtype=np.float32)
    distance_to_next_stop = np.full(len(gps_in_route), -1, dtype=np.float32)

    # Iterate over the GPS data
    for i in range(len(gps_in_route)):
        # Skip the points that are not in the route
        if gps_in_route[i] == False:
            continue

        # Update the list of stops by direction if it has changed
        if i > 0 and gps_direction[i] != gps_direction[i-1]:
            stop_distances = stops_distances_by_direction[gps_direction[i]]

        # Assign the indexes of the last and next stops
        next_stops[i] = get_next_stop(gps_distance[i], stop_distances)
        # last_stops[i] = utils.get_last_stop(gps_distance[i], stop_distances)
        last_stops[i] = max(0, next_stops[i] - 1) # To avoid unnecessary calculations

        # Assign the distances to the last and next stops
        distance_to_next_stop[i] = max(stop_distances[next_stops[i]] - gps_distance[i], 0)
        distance_to_last_stop[i] = max(abs(stop_distances[last_stops[i]] - gps_distance[i]), 0)

    # Return the lists of last and next stops
    return last_stops, next_stops, distance_to_last_stop, distance_to_next_stop

@jit(nopython=True)
def map_distance_into_timestamp(current_distance, initial_distance, final_distance, initial_timestamp, final_timestamp):
    """
    Map a distance into a timestamp using a linear interpolation
    
    Args:
        current_distance (float): The distance to be mapped
        initial_distance (float): The initial distance
        final_distance (float): The final distance
        initial_timestamp (int): The initial timestamp
        final_timestamp (int): The final timestamp

    Returns:
        float: The mapped timestamp
    """
    return (current_distance - initial_distance) * (final_timestamp - initial_timestamp) / (final_distance - initial_distance) + initial_timestamp

def generate_virtual_point(initial_distance, final_distance, initial_cumulative_distance, initial_timestamp, final_timestamp, stop_num, stop_distances, direction):

    # Get the timestamp and distance of the virtual datapoint
    virtual_timestamp = map_distance_into_timestamp(stop_distances[stop_num], initial_distance, final_distance, initial_timestamp, final_timestamp)
    virtual_distance = stop_distances[stop_num]

    try:

        # Assert if both values are valid
        assert virtual_timestamp >= initial_timestamp and virtual_timestamp <= final_timestamp, f"TIMESTAMP ERROR: {virtual_timestamp} {initial_timestamp} {final_timestamp}"
        assert virtual_distance >= initial_distance and virtual_distance <= final_distance, f"DISTANCE ERROR: {virtual_distance} {initial_distance} {final_distance}"

        next_stop_index = min(stop_num + 1, len(stop_distances) - 1)

        # Append the virtual datapoints to alist
        return [virtual_timestamp, # timestamp
                virtual_distance, # distance_traveled
                initial_cumulative_distance + (virtual_distance - initial_distance), # cumulative_distance_traveled
                direction, # direction
                stop_num, # current_stop_index
                # next_stop_index, # next_stop_index (to avoid out of bounds error)
                stop_distances[next_stop_index] - stop_distances[stop_num]] # next_stop_distance
    
    except AssertionError as e:
        print(e)
        return None

def virtualize_stop_points(gps_timestamps, gps_in_route, gps_direction, gps_last_stop_index, gps_next_stop_index, gps_distances, gps_cumulative_distances, mean_speeds, stops_distances_by_direction, vehicle_id, service_id):

    # By default, the first direction is the first one in the list
    current_direction = gps_direction[0]
    stop_distances = stops_distances_by_direction[current_direction]
    
    # Initialize the list of virtual datapoints
    virtual_datapoints = list()

    # Iterate over the gps data
    for i in range(1, len(gps_timestamps)):

        # Ensure that the last and current datapoint are in the route
        if gps_in_route[i-1] == False or gps_in_route[i] == False:
            continue

        # If the direction changes
        if gps_direction[i] != current_direction:

            # TODO: Generate a virtual datapoint to represent the final station
            # # As the direction changes, sum the distance of the last station with the amount ran after the direction transition
            # initial_distance = stop_distances[len(stop_distances) - 1]
            # virtual_final_distance = stop_distances[len(stop_distances) - 1] + gps_distances[i]
            # virtual_datapoint = generate_virtual_point(gps_distances[i-1], virtual_final_distance, gps_cumulative_distances[i-1], gps_timestamps[i-1], gps_timestamps[i], len(stop_distances) - 1, stop_distances, current_direction)

            # virtual_datapoints.append(virtual_datapoint)   




            current_direction = gps_direction[i] # Update the current direction
            stop_distances = stops_distances_by_direction[current_direction] # Update the stop distances



            # TODO: Generate a virtual datapoint for the initial station
            # if gps_direction[i] != 0 and gps_direction[i] != 1:
            
            #     virtual_initial_distance = - [stop_distances[len(stop_distances) - 1] - gps_distances[i-1]] # As the direction changes, sum the distance of the last station with the amount ran before the direction transition
            #     virtual_datapoint = generate_virtual_point(virtual_initial_distance, gps_distances[i], gps_cumulative_distances[i-1], gps_timestamps[i-1], gps_timestamps[i], 0, stop_distances, current_direction)

            #     virtual_datapoints.append(virtual_datapoint)    



            continue # To ensure that we have 2 consecutive datapoints with the same direction

        # Check if the bus went through a bus stop between the last and current datapoints
        if gps_last_stop_index[i-1] != gps_last_stop_index[i] or gps_next_stop_index[i-1] != gps_next_stop_index[i]:

            # Get the indexes of the stops to be generated
            initial_stop_index = gps_next_stop_index[i-1]
            final_stop_index = gps_last_stop_index[i]

            if initial_stop_index >= len(stop_distances) or final_stop_index >= len(stop_distances):
                # print(f"ERROR: Stop index out of bounds: {initial_stop_index} {final_stop_index}")
                continue

            # Get the distances of the last and current datapoint, to be used in the interpolation
            initial_distance = gps_distances[i-1]
            final_distance = gps_distances[i]

            initial_timestamp = gps_timestamps[i-1]
            final_timestamp = gps_timestamps[i]

            initial_cumulative_distance = gps_cumulative_distances[i-1]

            # Iteratate over the stops to be generated
            for stop_num in range(initial_stop_index, final_stop_index + 1):
                # Generate a virtual datapoint for each stop
                virtual_datapoint = generate_virtual_point(initial_distance, final_distance, initial_cumulative_distance, initial_timestamp, final_timestamp, stop_num, stop_distances, gps_direction[i])

                # Check if the virtual datapoint is valid
                if virtual_datapoint is None:
                    continue

                # Append the mean and accumulated mean speeds to the virtual datapoint (the same as the current datapoint)
                for j in range(mean_speeds.shape[1]):
                    virtual_datapoint.append(mean_speeds[i, j])

                # Append the virtual datapoints to a list
                virtual_datapoints.append(virtual_datapoint)                

    # Set the name of the columns
    columns = ['timestamp_gps', 'distance_traveled', 'cumulative_distance_traveled', 'direction', 'current_stop_index',  'next_stop_distance']
    # Add the name of the mean and accumulated mean speeds to the columns
    for i in range(1, 5+1, 2):
        columns.append(f'mean_speed_{i}_min')

    # Convert the list into a pandas dataframe
    virtual_df = pd.DataFrame(virtual_datapoints, columns=columns)

    virtual_df['timestamp_gps'] = pd.to_datetime(virtual_df['timestamp_gps'])

    # Split day and hour
    virtual_df['data'] = virtual_df['timestamp_gps'].dt.date
    virtual_df['hora'] = virtual_df['timestamp_gps'].dt.time

    # Time must be shown as HH:MM:SS
    virtual_df['hora'] = virtual_df['hora'].apply(lambda x: x.strftime('%H:%M:%S'))

    # Insert columns again, getting the first value of the original dataframe
    virtual_df['id_veiculo'] = vehicle_id
    virtual_df['servico'] = service_id

    # Reorder the columns
    virtual_df = virtual_df[['timestamp_gps', 'data', 'hora', 'id_veiculo', 'servico', 'direction', 'cumulative_distance_traveled', 'current_stop_index', 'next_stop_distance', 'mean_speed_1_min', 'mean_speed_3_min', 'mean_speed_5_min']]

    # Return the dataframe that contains the virtual datapoints
    return virtual_df

def process_bus_data(gps, gtfs, vehicle, route, bus_output_path):
    # Plot bus data
    gps.plot_gps_data(title=f"GPS data from bus {vehicle} (route {route})", save_path=bus_output_path + "gps_data.png")
    
    # Filter gps coordinates
    gps.filter_gps_coordinates(gtfs)

    # Plot fitered bus data
    gps.plot_gps_data(gps.gps_df[gps.gps_df["in_route"] == True], gtfs.route_shape_segments, title=f"Filtered GPS data from bus {vehicle} (route {route})", save_path=bus_output_path + "filtered_gps_data.png")

    # Assign the distances from the route start
    gps.get_distance_from_start(gtfs)

    # Plot the histogram with the distances from the start

    # Assign the direction and direction inference to each GPS point
    gps.gps_df['direction'], gps.gps_df['direction_directly_infered'] = assign_direction(gps.gps_df['in_route'].to_numpy(), gps.gps_df['distance_from_start_0'].to_numpy(), gps.gps_df['distance_from_start_1'].to_numpy(), N=3)

    # Plot the distances/directions infered

    # Assign the distance traveled to each GPS point based on the inferred direction
    gps.gps_df['distance_traveled'], gps.gps_df['cumulative_distance_traveled'] = assign_distance_traveled(gps.gps_df['in_route'].to_numpy(), gps.gps_df['direction'].to_numpy(), gps.gps_df['distance_from_start_0'].to_numpy(), gps.gps_df['distance_from_start_1'].to_numpy())

    # Plot a histogram of distance traveled and cumulative distance traveled

    # Get stops by direction
    gtfs.get_stops_by_direction()

    # Assign stops to the GPS data
    gps.gps_df['last_stop_index'], gps.gps_df['next_stop_index'], gps.gps_df['last_stop_distance'], gps.gps_df['next_stop_distance'] = assign_stops(gps.gps_df['in_route'].to_numpy(), gps.gps_df['direction'].to_numpy(), gps.gps_df['distance_traveled'].to_numpy(), gtfs.stops_distances_by_direction)

    # Plot a histogram of the last_stop_distance and next_stop_distance

    # Convert the 'timestamp_gps' to datetime
    gps.gps_df['timestamp_gps'] = pd.to_datetime(gps.gps_df['timestamp_gps'])

    # Assign the mean speed
    timestamp_array = gps.gps_df['timestamp_gps'].to_numpy()
    timestamp_seconds_array = (timestamp_array).astype('int64') // 10**9

    gps.gps_df['mean_speed_1_min'] = assign_mean_speed(gps.gps_df['in_route'].to_numpy(), timestamp_seconds_array, gps.gps_df['cumulative_distance_traveled'].to_numpy(), N=1)
    gps.gps_df['mean_speed_3_min'] = assign_mean_speed(gps.gps_df['in_route'].to_numpy(), timestamp_seconds_array, gps.gps_df['cumulative_distance_traveled'].to_numpy(), N=3)
    gps.gps_df['mean_speed_5_min'] = assign_mean_speed(gps.gps_df['in_route'].to_numpy(), timestamp_seconds_array, gps.gps_df['cumulative_distance_traveled'].to_numpy(), N=5)

    # Plot a histogram of the mean speeds

    # Generate the validation dataset with virtual/interpolated datapoints
    gps.validation_df = virtualize_stop_points(gps.gps_df['timestamp_gps'].to_numpy(), gps.gps_df['in_route'].to_numpy(), gps.gps_df['direction'].to_numpy(), gps.gps_df['last_stop_index'].to_numpy(), gps.gps_df['next_stop_index'].to_numpy(), gps.gps_df['distance_traveled'].to_numpy(), gps.gps_df['cumulative_distance_traveled'].to_numpy(), gps.gps_df[['mean_speed_1_min', 'mean_speed_3_min', 'mean_speed_5_min']].to_numpy(), gtfs.stops_distances_by_direction, vehicle, route)

    print(f"PROCESSED: {vehicle} {route} -> {len(gps.gps_df)} / {len(gps.validation_df)}")

    # Save the results
    gps.gps_df.to_csv(bus_output_path + "raw_processed_gps_data.csv", index=False)
    gps.gps_df[gps.gps_df['in_route'] == True].to_csv(bus_output_path + "processed_gps_data.csv", index=False)
    gps.validation_df.to_csv(bus_output_path + "validation_data.csv", index=False)



