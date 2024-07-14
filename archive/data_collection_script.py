import requests
import time
import zipfile
import io
import pandas as pd
import math
import json
from datetime import datetime, timedelta
import sqlite3

API_KEY = "afec7635-a79b-4ccb-b87b-5c8d9cf5b36c"
OPERATOR = 'CT'  # Caltrain operator ID

def download_gtfs_data():
    url = f'http://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={OPERATOR}'
    response = requests.get(url)
    response.raise_for_status()
    
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    zip_file.extractall('gtfs_data')
    print("GTFS data downloaded and unzipped successfully.")

def load_stops_data():
    stops_df = pd.read_csv('gtfs_data/stops.txt')
    stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
    return stops_df

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    r = 6371000  # Radius of Earth in meters
    return r * c

def has_train_arrived(train_lat, train_lon, stop_lat, stop_lon, threshold=100):
    distance = haversine(train_lat, train_lon, stop_lat, stop_lon)
    return distance <= threshold

def create_connection():
    conn = sqlite3.connect('caltrain_performance.db')
    return conn

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS train_arrivals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            trip_id TEXT,
            stop_id TEXT,
            stop_name TEXT,
            scheduled_arrival TEXT,
            actual_arrival TEXT,
            delay INTEGER,
            UNIQUE(date, trip_id, stop_id)
        )
    ''')
    conn.commit()
    print("Table 'train_arrivals' created or already exists.") 
def insert_arrival(conn, arrival_data):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO train_arrivals 
        (date, trip_id, stop_id, stop_name, scheduled_arrival, actual_arrival, delay)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', arrival_data)
    conn.commit()

def fetch_and_process_data(stops_df):
    conn = create_connection()
    create_table(conn)

    url = f'https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency={OPERATOR}'
    response = requests.get(url)
    response.raise_for_status()

    data_str = response.content.decode('utf-8-sig')
    json_data = json.loads(data_str)
    vehicle_activities = json_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']

    for activity in vehicle_activities:
        journey = activity['MonitoredVehicleJourney']
        trip_id = journey['VehicleRef']
        vehicle_lat = float(journey['VehicleLocation']['Latitude'])
        vehicle_lon = float(journey['VehicleLocation']['Longitude'])

        monitored_call = journey['MonitoredCall']
        stop_id = monitored_call['StopPointRef']
        stop = stops_df[stops_df['stop_id'] == stop_id]
        
        if not stop.empty:
            stop_lat = float(stop['stop_lat'].values[0])
            stop_lon = float(stop['stop_lon'].values[0])

            if has_train_arrived(vehicle_lat, vehicle_lon, stop_lat, stop_lon):
                expected_arrival_time = datetime.strptime(monitored_call['ExpectedArrivalTime'], '%Y-%m-%dT%H:%M:%S%z') - timedelta(hours=7)
                expected_arrival_time = expected_arrival_time.replace(tzinfo=None)
                actual_arrival_time = datetime.now()
                delay = (actual_arrival_time - expected_arrival_time).seconds // 60

                arrival_data = (
                    actual_arrival_time.date().isoformat(),
                    trip_id,
                    stop_id,
                    monitored_call['StopPointName'],
                    expected_arrival_time.time().isoformat(),
                    actual_arrival_time.time().isoformat(),
                    delay
                )
                insert_arrival(conn, arrival_data)

    conn.close()

if __name__ == "__main__":
    download_gtfs_data()  # Download GTFS data once at the start
    stops_df = load_stops_data()
    
    while True:
        fetch_and_process_data(stops_df)
        time.sleep(60)  # Wait for 1 minute before the next request