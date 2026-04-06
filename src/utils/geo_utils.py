"""
Geospatial utility functions.
"""
import math
import csv
import os

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


_shape_cache = {}

def load_shape_points(gtfs_dir="gtfs_data", shape_id=None):
    """
    Load shape polyline points from shapes.txt. Returns the longest full-line
    shape if shape_id is not specified.

    Args:
        gtfs_dir: path to GTFS directory containing shapes.txt
        shape_id: specific shape_id to load, or None for longest

    Returns:
        list of (lat, lon, dist_meters) tuples sorted by sequence
    """
    cache_key = (gtfs_dir, shape_id)
    if cache_key in _shape_cache:
        return _shape_cache[cache_key]

    shapes_path = os.path.join(gtfs_dir, "shapes.txt")
    # Group points by shape_id
    shapes = {}
    with open(shapes_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = row["shape_id"]
            shapes.setdefault(sid, []).append((
                float(row["shape_pt_lat"]),
                float(row["shape_pt_lon"]),
                int(row["shape_pt_sequence"]),
                float(row["shape_dist_traveled"]),
            ))

    if shape_id is None:
        # Pick the shape with the greatest max distance (full-line)
        shape_id = max(shapes, key=lambda s: max(p[3] for p in shapes[s]))

    pts = shapes[shape_id]
    pts.sort(key=lambda p: p[2])  # sort by sequence
    result = [(lat, lon, dist) for lat, lon, _seq, dist in pts]

    _shape_cache[cache_key] = result
    return result


def project_to_route(lat, lon, shape_points):
    """
    Project a GPS point onto the route polyline.

    Finds the nearest segment on the shape polyline and interpolates the
    distance along the route.

    Args:
        lat: GPS latitude
        lon: GPS longitude
        shape_points: list of (lat, lon, dist_meters) from load_shape_points()

    Returns:
        distance in meters along the route (float)
    """
    best_dist_to_track = float("inf")
    best_route_dist = shape_points[0][2]

    for i in range(len(shape_points) - 1):
        lat1, lon1, d1 = shape_points[i]
        lat2, lon2, d2 = shape_points[i + 1]

        # Project point onto segment using simple fraction
        # Vector from p1 to p2
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        seg_len_sq = dlat * dlat + dlon * dlon

        if seg_len_sq < 1e-14:
            # Degenerate segment
            t = 0.0
        else:
            t = ((lat - lat1) * dlat + (lon - lon1) * dlon) / seg_len_sq
            t = max(0.0, min(1.0, t))

        # Closest point on segment
        proj_lat = lat1 + t * dlat
        proj_lon = lon1 + t * dlon

        dist_to_track = haversine(lat, lon, proj_lat, proj_lon)

        if dist_to_track < best_dist_to_track:
            best_dist_to_track = dist_to_track
            best_route_dist = d1 + t * (d2 - d1)

    return best_route_dist
