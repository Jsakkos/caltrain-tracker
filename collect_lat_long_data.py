
from flask import Flask, render_template, jsonify
import sqlite3
import requests

import pandas as pd
import math
import json
import requests
import json
import sqlite3
import pandas as pd
from datetime import datetime, time, timedelta
import time as time_module  # Rename the time module to avoid conflicts
# app = Flask(__name__)

API_KEY = "afec7635-a79b-4ccb-b87b-5c8d9cf5b36c"
OPERATOR = 'CT'  # Caltrain operator ID
import sqlite3
import requests
import json
from datetime import datetime, timedelta
import time as time_module

# Assuming API_KEY and OPERATOR are defined elsewhere

def get_db_connection():
    conn = sqlite3.connect('caltrain_lat_long.db')
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

def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return cursor.fetchone() is not None

def insert_arrival(conn, train_locations):
    cursor = conn.cursor()
   
    if not table_exists(conn, 'train_locations'):
        print("Table 'train_locations' does not exist. Creating it now.")
        create_table(conn)

    # Check if the record already exists
    cursor.execute('''
        SELECT id FROM train_locations
        WHERE timestamp = ? AND trip_id = ? AND stop_id = ?
    ''', (train_locations[4], train_locations[0], train_locations[1]))
   
    if cursor.fetchone() is None:
        # Record doesn't exist, insert it
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
    conn = get_db_connection()
    url = f'https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency={OPERATOR}'
    
    try:
        # Fetch the GTFS-RT data
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the JSON data
        data = response.content
        data_str = data.decode('utf-8-sig')
        json_data = json.loads(data_str)
        
        # Extracting the relevant information
        vehicle_activities = json_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']
        
        # Process each vehicle activity
        for activity in vehicle_activities:
            journey = activity['MonitoredVehicleJourney']
            trip_id = journey['VehicleRef']
            vehicle_lat = float(journey['VehicleLocation']['Latitude'])
            vehicle_lon = float(journey['VehicleLocation']['Longitude'])
            monitored_call = journey['MonitoredCall']
            stop_id = monitored_call['StopPointRef']
            timestamp = activity['RecordedAtTime']
            timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z') - timedelta(hours=7)
            timestamp = timestamp.replace(tzinfo=None)
            
            lat_lon_data = (trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp.isoformat())
            insert_arrival(conn, lat_lon_data)
    
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
    except KeyError as e:
        print(f"Error accessing JSON data: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    while True:
        fetch_and_process_data()
        time_module.sleep(60)