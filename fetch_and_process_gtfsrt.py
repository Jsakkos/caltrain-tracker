import requests
import sqlite3
import os
import json
from datetime import datetime, timedelta
import time
import pytz

# Get the API key from environment variables
API_KEY = os.environ.get('API_KEY')

# Path to the SQLite database where train location data will be stored
DB_PATH = 'data/caltrain_lat_long.db'

# URL to fetch real-time vehicle monitoring data from the GTFS-RT API
GTFS_URL = f"https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency=CT"  # Caltrain

def get_db_connection():
    """
    Establishes a connection to the SQLite database.
    
    Returns:
        conn (sqlite3.Connection): A connection object to the database.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def create_table(conn):
    """
    Creates the 'train_locations' table if it doesn't already exist in the database.
    This table stores data about train locations.

    Args:
        conn (sqlite3.Connection): A connection object to the SQLite database.
    """
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS train_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trip_id TEXT,
            stop_id TEXT,
            vehicle_lat FLOAT,
            vehicle_lon FLOAT,
            timestamp TEXT,
            UNIQUE(timestamp, trip_id, stop_id)
        )
    ''')
    conn.commit()

def table_exists(conn, table_name):
    """
    Checks whether a table exists in the database.

    Args:
        conn (sqlite3.Connection): A connection object to the SQLite database.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return cursor.fetchone() is not None

def insert_arrival(conn, train_locations):
    """
    Inserts a new record of train location data into the database if it doesn't already exist.
    
    Args:
        conn (sqlite3.Connection): A connection object to the SQLite database.
        train_locations (tuple): A tuple containing the trip ID, stop ID, vehicle latitude, 
                                 vehicle longitude, and timestamp.
    """
    cursor = conn.cursor()
   
    # Ensure the table exists before inserting data
    if not table_exists(conn, 'train_locations'):
        print("Table 'train_locations' does not exist. Creating it now.")
        create_table(conn)

    # Check if the record already exists to avoid duplicates
    cursor.execute('''
        SELECT id FROM train_locations
        WHERE timestamp = ? AND trip_id = ? AND stop_id = ?
    ''', (train_locations[4], train_locations[0], train_locations[1]))
   
    if cursor.fetchone() is None:
        # Insert the new train location data if no duplicate is found
        cursor.execute('''
            INSERT INTO train_locations
            (trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', train_locations)
        conn.commit()
        print(f"Inserted new record: {train_locations}")
    else:
        print(f"Duplicate record not inserted: {train_locations}")

def fetch_and_process_data():
    """
    Fetches real-time vehicle monitoring data from the GTFS-RT API, processes it, 
    and stores relevant train location data in the SQLite database.
    """
    conn = get_db_connection()
    create_table(conn)
    
    try:
        # Fetch data from the GTFS-RT API
        response = requests.get(GTFS_URL)
        response.raise_for_status()  # Check for HTTP errors
        
        # Decode the data using 'utf-8-sig' to handle potential BOM characters
        data = response.content
        data_str = data.decode('utf-8-sig')
        json_data = json.loads(data_str)
        
        # Extract vehicle activity from the JSON data
        vehicle_activities = json_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']
        
        # Process each vehicle activity (train) from the API response
        for activity in vehicle_activities:
            journey = activity['MonitoredVehicleJourney']
            trip_id = journey['VehicleRef']
            vehicle_lat = float(journey['VehicleLocation']['Latitude'])
            vehicle_lon = float(journey['VehicleLocation']['Longitude'])
            monitored_call = journey['MonitoredCall']
            stop_id = monitored_call['StopPointRef']
            timestamp = activity['RecordedAtTime']
            timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z')
            local_tz = pytz.timezone('America/Los_Angeles')
            timestamp = timestamp.astimezone(local_tz)
            # Store with timezone information
            lat_lon_data = (trip_id, stop_id, vehicle_lat, vehicle_lon, 
                            timestamp.isoformat(timespec='seconds'))
            insert_arrival(conn, lat_lon_data)
    
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
    except KeyError as e:
        print(f"Error accessing JSON data: {e}")
    finally:
        # Close the database connection after processing
        conn.close()

if __name__ == "__main__":
    # Run the data fetching and processing function when the script is executed
    fetch_and_process_data()