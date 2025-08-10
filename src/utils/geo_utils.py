"""
Geospatial utility functions.
"""
import math

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on Earth given their
    latitude and longitude using the Haversine formula.
    
    Args:
        lat1 (float): Latitude of the first point in decimal degrees
        lon1 (float): Longitude of the first point in decimal degrees
        lat2 (float): Latitude of the second point in decimal degrees
        lon2 (float): Longitude of the second point in decimal degrees
        
    Returns:
        float: Distance between the points in meters
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    r = 6371000  # Radius of Earth in meters
    
    return r * c

def has_train_arrived(train_lat, train_lon, stop_lat, stop_lon, threshold=100):
    """
    Determine if a train has arrived at a stop based on its proximity.
    
    Args:
        train_lat (float): Latitude of the train
        train_lon (float): Longitude of the train
        stop_lat (float): Latitude of the stop
        stop_lon (float): Longitude of the stop
        threshold (float): Distance threshold in meters to consider arrival
        
    Returns:
        bool: True if the train is within the threshold distance of the stop
    """
    distance = haversine(train_lat, train_lon, stop_lat, stop_lon)
    return distance <= threshold
