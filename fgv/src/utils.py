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