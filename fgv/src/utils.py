import numpy as np
import pandas as pd

from numba import jit

@jit(nopython=True)
def squared_distance_to_segment(px:np.float32, py:np.float32, ax:np.float32, ay:np.float32, bx:np.float32, by:np.float32):
    # Vector from A to P
    apx, apy = px - ax, py - ay
    # Vector from A to B
    abx, aby = bx - ax, by - ay
    
    # Magnitude of AB vector (squared)
    ab2 = abx * abx + aby * aby
    
    if ab2 == 0:
        # A and B are the same points, no valid segment
        return apx * apx + apy * apy
    
    # Project AP onto AB to find the closest point
    ap_dot_ab = apx * abx + apy * aby
    t = ap_dot_ab / ab2
    
    if t < 0.0:
        # Closest to A
        closestx, closesty = ax, ay
    elif t > 1.0:
        # Closest to B
        closestx, closesty = bx, by
    else:
        # Projection point is on the segment
        closestx = ax + t * abx
        closesty = ay + t * aby
    
    # Distance from point to the closest point on the segment (squared)
    dx, dy = px - closestx, py - closesty
    return dx * dx + dy * dy

@jit(nopython=True)
def closest_projection(points, route_segments):

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
            distance = squared_distance_to_segment(points[i, 0], points[i, 1],
                                                   route_segments[j][0][0], route_segments[j][0][1],
                                                   route_segments[j][1][0], route_segments[j][1][1])
            if distance < min_squared_distances[i]:
                min_squared_distances[i] = distance
                closest_segment_indexes[i] = j

    return min_squared_distances, closest_segment_indexes

@jit(cache=True)
def get_latitude_meters_coefficient(latitude):
    # Conversion rate of meters to latitude degrees at the equator
    meters_per_degree = 111320
    
    # Adjust the conversion rate based on latitude
    latitude_adjusted_meters_per_degree = meters_per_degree * (1 - 0.00669438 * ((latitude * 0.0174533) ** 2))
    
    return latitude_adjusted_meters_per_degree

@jit(nopython=True)
def meters_to_degrees(meters, latitude):

    # Calculate the change in latitude from meters
    return meters / get_latitude_meters_coefficient(latitude)

@jit(nopython=True)
def degrees_to_meters(degrees, latitude):

    # Calculate the change in meters from latitude
    return degrees * get_latitude_meters_coefficient(latitude)
