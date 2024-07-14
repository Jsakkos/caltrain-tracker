import threading
import time
from flask import Flask, render_template, jsonify
import sqlite3
import requests
import zipfile
import io
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

def get_db_connection():
    conn = sqlite3.connect('caltrain_location.db')
    conn.row_factory = sqlite3.Row
    return conn

# @app.route('/')
# def index():
#     return render_template('index.html')

@app.route('/api/delay_stats')
def delay_stats():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='train_arrivals'")
    if cursor.fetchone() is None:
        # Table doesn't exist, return empty data
        return jsonify({
            'overall': {'total': 0, 'delayed': 0},
            'morning': {'total': 0, 'delayed': 0},
            'evening': {'total': 0, 'delayed': 0}
        })

    # Overall delay stats
    cursor.execute('''
        SELECT 
            COUNT(*) as total_trips,
            SUM(CASE WHEN delay > 0 THEN 1 ELSE 0 END) as delayed_trips
        FROM train_arrivals
    ''')
    overall_stats = cursor.fetchone()

    # Morning commute stats (6 AM to 10 AM)
    cursor.execute('''
        SELECT 
            COUNT(*) as total_trips,
            SUM(CASE WHEN delay > 0 THEN 1 ELSE 0 END) as delayed_trips
        FROM train_arrivals
        WHERE time(scheduled_arrival) BETWEEN time('06:00:00') AND time('10:00:00')
    ''')
    morning_stats = cursor.fetchone()

    # Evening commute stats (4 PM to 8 PM)
    cursor.execute('''
        SELECT 
            COUNT(*) as total_trips,
            SUM(CASE WHEN delay > 0 THEN 1 ELSE 0 END) as delayed_trips
        FROM train_arrivals
        WHERE time(scheduled_arrival) BETWEEN time('16:00:00') AND time('20:00:00')
    ''')
    evening_stats = cursor.fetchone()

    conn.close()

    return jsonify({
        'overall': {
            'total': overall_stats['total_trips'],
            'delayed': overall_stats['delayed_trips']
        },
        'morning': {
            'total': morning_stats['total_trips'],
            'delayed': morning_stats['delayed_trips']
        },
        'evening': {
            'total': evening_stats['total_trips'],
            'delayed': evening_stats['delayed_trips']
        }
    })

# Data collection functions
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

def has_train_arrived(train_lat, train_lon, stop_lat, stop_lon, threshold=500):
    distance = haversine(train_lat, train_lon, stop_lat, stop_lon)
    return distance <= threshold

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
    # print("Table 'train_arrivals' created or already exists.")

def insert_arrival(conn, arrival_data):
    cursor = conn.cursor()
    
    # Check if the record already exists
    cursor.execute('''
        SELECT id FROM train_arrivals 
        WHERE date = ? AND trip_id = ? AND stop_id = ?
    ''', arrival_data[:3])
    
    if cursor.fetchone() is None:
        # Record doesn't exist, insert it
        cursor.execute('''
            INSERT INTO train_arrivals 
            (date, trip_id, stop_id, stop_name, scheduled_arrival, actual_arrival, delay)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', arrival_data)
        conn.commit()
        print(f"Inserted new record: {arrival_data[:3]}")
    else:
        print(f"Duplicate record not inserted: {arrival_data[:3]}")



def convert_schedule_time(time_str, date):
    # Parse the time string
    hours, minutes, seconds = map(int, time_str.split(':'))
    
    # Handle times past midnight
    if hours >= 24:
        hours -= 24
        date += timedelta(days=1)
    
    # Create a time object
    time_obj = time(hour=hours, minute=minutes, second=seconds)
    
    # Combine the date and time
    return datetime.combine(date, time_obj)
def fetch_and_process_data(stops_df, stop_times_df):
    conn = get_db_connection()
    create_table(conn)

    url = f'https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency={OPERATOR}'
    response = requests.get(url)
    response.raise_for_status()

    data_str = response.content.decode('utf-8-sig')
    json_data = json.loads(data_str)
    vehicle_activities = json_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']

    current_time = datetime.now()
    current_date = current_time.date()

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
                # Get the scheduled arrival time from stop_times.txt
                scheduled_time_str = stop_times_df.loc[
                    (stop_times_df.trip_id == int(trip_id)) & 
                    (stop_times_df.stop_id == int(stop_id)),
                    'arrival_time'
                ].values[0]
                
                # Convert the scheduled time to a datetime object
                scheduled_arrival_time = convert_schedule_time(scheduled_time_str, current_date)
                
                # Use the current time as the actual arrival time
                actual_arrival_time = current_time

                # Only process if the train has actually arrived (not waiting ahead of schedule)
                if actual_arrival_time >= scheduled_arrival_time - timedelta(minutes=5):
                        delay = (actual_arrival_time - scheduled_arrival_time).seconds // 60
                        if delay < 0:
                            delay =0
                        arrival_data = (
                            actual_arrival_time.date().isoformat(),
                            trip_id,
                            stop_id,
                            monitored_call['StopPointName'],
                            scheduled_arrival_time.time().isoformat(),
                            actual_arrival_time.time().isoformat(),
                            delay
                        )
                        insert_arrival(conn, arrival_data)
                else:
                    print(f"Skipping insertion: Train {trip_id} is waiting ahead of schedule at stop {stop_id}")

    conn.close()
def load_gtfs_data():
    stops_df = pd.read_csv('gtfs_data/stops.txt')
    stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
    
    stop_times_df = pd.read_csv('gtfs_data/stop_times.txt')
    return stops_df, stop_times_df
def data_collection_thread():
    download_gtfs_data()
    stops_df, stop_times_df = load_gtfs_data()
    while True:
        try:
            fetch_and_process_data(stops_df, stop_times_df)
        except Exception as e:
            print(e)
        time_module.sleep(60) # Wait for 1 minute before the next request

if __name__ == '__main__':
    # Start the data collection thread
    data_thread = threading.Thread(target=data_collection_thread)
    data_thread.daemon = True  # This ensures the thread will exit when the main program does
    data_thread.start()

    # Run the Flask app
    app.run(debug=True, use_reloader=False)