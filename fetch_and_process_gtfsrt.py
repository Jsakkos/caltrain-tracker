import requests
import sqlite3
import os
import json
from datetime import datetime, timedelta
import time
API_KEY = os.environ.get('API_KEY')
DB_PATH = os.environ.get('DB_PATH', '/data/caltrain_lat_long.db')
GTFS_URL = f"https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency=CT"  # Caltrain

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def create_table(conn):
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

def fetch_and_process_data():
    conn = get_db_connection()
    create_table(conn)
    
    try:
        response = requests.get(GTFS_URL)
        response.raise_for_status()
        data = response.json()
        
        vehicle_activities = data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']
        
        for activity in vehicle_activities:
            journey = activity['MonitoredVehicleJourney']
            trip_id = journey['VehicleRef']
            stop_id = journey['MonitoredCall']['StopPointRef']
            vehicle_lat = float(journey['VehicleLocation']['Latitude'])
            vehicle_lon = float(journey['VehicleLocation']['Longitude'])
            timestamp = datetime.strptime(activity['RecordedAtTime'], '%Y-%m-%dT%H:%M:%S%z') - timedelta(hours=7)
            timestamp = timestamp.replace(tzinfo=None).isoformat()

            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO train_locations (trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp))
        
        conn.commit()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
    except KeyError as e:
        print(f"Error processing data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    fetch_and_process_data()
