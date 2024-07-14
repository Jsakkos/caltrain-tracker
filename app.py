import threading
import time as time_module
from dash import Dash, html, dcc, callback, Output, Input
import dash
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sqlite3
import math
from datetime import datetime, timedelta,time
import dash_bootstrap_components as dbc
import requests
import json
import os

API_KEY = os.environ.get('API_KEY')
DB_PATH = os.environ.get('DB_PATH', '/data/caltrain_lat_long.db')
GTFS_PATH = os.environ.get('GTFS_PATH', '/data/gtfs_data')
OPERATOR = 'CT'  # Caltrain operator ID

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
def data_collection_loop():
    while True:
        fetch_and_process_data()
        time_module.sleep(60)

def load_data():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM train_locations"
    df = pd.read_sql_query(query, conn)
    conn.close()

    df['stop_id'] = df['stop_id'].astype(int)
    df['trip_id'] = df['trip_id'].astype(int)

    stops_df = load_stops_data()
    stop_times_df = load_stop_times_data()
    stops_df['stop_id'] = stops_df['stop_id'].astype(int)

    df2 = pd.merge(df, stop_times_df[['trip_id', 'stop_id', 'arrival_time']], on=['trip_id', 'stop_id'])
    df2 = pd.merge(df2,stops_df[['stop_id','stop_name','parent_station','stop_lat','stop_lon']],on=['stop_id'])

    # Apply the Haversine function to calculate distance for each row
    df2['distance'] = df2.apply(lambda row: haversine(row['vehicle_lat'], row['vehicle_lon'], row['stop_lat'], row['stop_lon']), axis=1)
    df2['timestamp'] = pd.to_datetime(df2['timestamp'])
    df2['date'] = df2['timestamp'].dt.date
    
    df2['arrival_time'] = df2['arrival_time'].apply(normalize_time)
    df2['arrival_time'] = pd.to_datetime(df2['arrival_time'], format='%H:%M:%S').dt.time
        
    # Find the minimum distance for each combination of trip_id, stop_id, and date
    min_distances = df2.groupby(['trip_id', 'stop_id', 'date'])['distance'].min().reset_index()

    # Merge the minimum distances back to the original dataframe
    merged_df = pd.merge(df2, min_distances, on=['trip_id', 'stop_id', 'date', 'distance'])

    arrival_times = merged_df.groupby(['trip_id', 'stop_id', 'date']).first().reset_index()
    arrival_times = arrival_times[['trip_id', 'stop_id', 'date', 'timestamp']]
    arrival_times.rename(columns={'timestamp': 'actual_arrival_time'}, inplace=True)

    # Merge with the original dataframe to get the scheduled arrival time
    comparison_df = pd.merge(arrival_times, df2[['trip_id', 'stop_id', 'stop_name','parent_station','date', 'arrival_time']], on=['trip_id', 'stop_id', 'date'])
    # Calculate the delay in minutes
    comparison_df['delay_minutes'] = comparison_df.apply(
        lambda row: calculate_time_difference(row['actual_arrival_time'].time(), row['arrival_time']), axis=1
    )
    comparison_df.loc[comparison_df.delay_minutes > 500,'delay_minutes'] = 0.0
    comparison_df.loc[comparison_df.delay_minutes < -100,'delay_minutes'] = 0.0
    # Determine if the train is delayed
    comparison_df['is_delayed'] = comparison_df['delay_minutes'] > 4

    # Calculate the overall on-time performance based on unique trip counts
    unique_trips = comparison_df.drop_duplicates(subset=['trip_id', 'stop_id', 'date'])
    total_trips = len(unique_trips)
    on_time_trips = len(unique_trips[unique_trips['is_delayed'] == False])
    on_time_performance = (on_time_trips / total_trips) * 100
    unique_trips.loc[(unique_trips.delay_minutes >4) & (unique_trips.delay_minutes <=15),'delay_severity'] = 'Minor'
    unique_trips.loc[(unique_trips.delay_minutes >15),'delay_severity'] = 'Major'
    unique_trips['delay_severity'].fillna('On Time', inplace=True)
    unique_trips.loc[unique_trips.delay_minutes < 0,'delay_minutes']=0
    # Calculate percentage of delays by severity
    delay_severity_counts = unique_trips['delay_severity'].value_counts(normalize=True) * 100
    delay_severity_counts = delay_severity_counts.reset_index()
    delay_severity_counts.columns = ['delay_severity', 'percentage']
    unique_trips['commute_period'] =unique_trips['actual_arrival_time'].apply(categorize_commute_time)

    # Filter for Morning and Evening commutes
    filtered_trips = unique_trips[unique_trips['commute_period'].isin(['Morning', 'Evening'])]

    # Calculate total trips for each commute period
    total_commute_period_trips = filtered_trips.groupby('commute_period').size().reset_index(name='total_counts')

    # Calculate counts of delays by commute period and severity
    commute_delay_counts = filtered_trips.groupby(['commute_period', 'delay_severity']).size().reset_index(name='counts')

    # Merge to get total counts for each commute period
    commute_delay_counts = pd.merge(commute_delay_counts, total_commute_period_trips, on='commute_period')

    # Calculate percentage of delays by commute period and severity
    commute_delay_counts['percentage'] = (commute_delay_counts['counts'] / commute_delay_counts['total_counts']) * 100
    return df, stops_df, stop_times_df, unique_trips, on_time_performance, delay_severity_counts

def load_stops_data():
    stops_df = pd.read_csv(os.path.join(GTFS_PATH, 'stops.txt'))
    stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
    return stops_df

def load_stop_times_data():
    stop_times_df = pd.read_csv(os.path.join(GTFS_PATH, 'stop_times.txt'))
    return stop_times_df

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


# Normalize the arrival times
def normalize_time(t):
    if int(t.split(":")[0]) >= 24:
        return "00" + t[2:]
    return t


# Function to calculate time difference in minutes
def calculate_time_difference(time1, time2):
    datetime1 = datetime.combine(datetime.today(), time1)
    datetime2 = datetime.combine(datetime.today(), time2)
    time_diff = datetime2 - datetime1
    return time_diff.total_seconds() / 60  # Return the difference in minutes


# Categorize commute time
def categorize_commute_time(timestamp):
    if timestamp.weekday() >= 5:  # Saturday (5) and Sunday (6)
        return 'Weekend'
    
    morning_start = time(6, 0)
    morning_end = time(9, 0)
    evening_start = time(15, 30)
    evening_end = time(19, 30)
    
    commute_time = timestamp.time()
    
    if morning_start <= commute_time <= morning_end:
        return 'Morning'
    elif evening_start <= commute_time <= evening_end:
        return 'Evening'
    else:
        return 'Other'
def create_figures(df, unique_trips, on_time_performance, delay_severity_counts):
    # Calculate commute_delay_counts
    filtered_trips = unique_trips[unique_trips['commute_period'].isin(['Morning', 'Evening'])]
    total_commute_period_trips = filtered_trips.groupby('commute_period').size().reset_index(name='total_counts')
    commute_delay_counts = filtered_trips.groupby(['commute_period', 'delay_severity']).size().reset_index(name='counts')
    commute_delay_counts = pd.merge(commute_delay_counts, total_commute_period_trips, on='commute_period')
    commute_delay_counts['percentage'] = (commute_delay_counts['counts'] / commute_delay_counts['total_counts']) * 100

    # Create the figures
    fig_commute_delay = px.bar(commute_delay_counts, x='commute_period', y='percentage', color='delay_severity',
                            title="Percentage of Morning and Evening Commutes with Delays by Severity",
                            labels={'commute_period': 'Commute Period', 'percentage': 'Percentage', 'delay_severity': 'Delay Severity'})
    for trace in fig_commute_delay.data:
        if trace.name == 'On Time':
            trace.visible = 'legendonly'

    # Calculate percentage of delays by severity
    daily_summary = unique_trips.groupby('date')['delay_severity'].value_counts(normalize=True).unstack() * 100

    # Reset index to have date as a column
    daily_summary = daily_summary.reset_index()

    # Melt the DataFrame for Plotly
    daily_summary_melted = daily_summary.melt(id_vars='date', value_vars=['Major', 'Minor', 'On Time'], var_name='Status', value_name='Percentage')

    # Define the order of the Status items
    status_order = ['On Time', 'Minor Delay', 'Major Delay']
    daily_summary_melted.loc[daily_summary_melted.Status == 'Major','Status']='Major Delay'
    daily_summary_melted.loc[daily_summary_melted.Status == 'Minor','Status']='Minor Delay'
    # Define custom colors for each Status
    status_colors = {
        'On Time': '#00CC96',
        'Minor Delay': '#FECB52',
        'Major Delay': '#EF553B'
    }

    # Create the stacked bar plot
    fig = px.bar(daily_summary_melted, x='date', y='Percentage', color='Status', 
                title='On-time performance by date',
                category_orders={'Status': status_order},
                color_discrete_map=status_colors,labels={'date': 'Date', 'percentage': 'Percentage','Minor':'Minor Delay'})

    fig_delay_minutes = px.histogram(unique_trips.loc[unique_trips.delay_minutes >=5], x='delay_minutes', color="commute_period", barmode='overlay',
                                    title="Trip delay durations",
                                    labels={'commute_period': 'Commute Period','delay_minutes': 'Trip delay (mins)','count': "Number of trips"})

    # Create heatmap
    unique_trips['parent_station'] = unique_trips['parent_station'].apply(clean_station_name)
    heatmap_data = unique_trips.pivot_table(index='trip_id', columns='stop_id', values='delay_minutes', aggfunc='mean', sort=False)
    stop_id_to_parent_station = unique_trips[['stop_id', 'parent_station']].drop_duplicates().set_index('stop_id')['parent_station'].to_dict()
    heatmap_data = heatmap_data[sorted(heatmap_data.columns)]
    heatmap_data.columns = [stop_id_to_parent_station[stop_id] for stop_id in heatmap_data.columns]
    heatmap_data.index = heatmap_data.index.astype(str)
    
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=heatmap_data.columns,
        y=heatmap_data.index,
        colorscale='Viridis',
    ))
    fig_heatmap.update_layout(
        title="Heatmap of Delays by Stop and Train Number",
        xaxis_title="Stop",
        yaxis_title="Train Number",
        height = 1000,
    )

    return fig, fig_commute_delay, fig_delay_minutes, fig_heatmap

def clean_station_name(name):
    if name == 'place_MLBR':
        return 'Millbrae'
    name = name.replace('_', ' ')
    return name.title()

# Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# the style arguments for the sidebar. We use position:fixed and a fixed width
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

sidebar = html.Div(
    [
        html.H2("Caltrain OTP", className="display-4"),
        html.Hr(),
        html.P(
            "On-time performance analysis of Caltrain", className="lead"
        ),
        dbc.Nav(
            [
                dbc.NavLink("Home", href="/", active="exact"),
                dbc.NavLink("Methodology", href="/methods", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)


content = html.Div(id="page-content", style=CONTENT_STYLE)

app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    html.Div(id="page-content", style=CONTENT_STYLE),
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # in milliseconds, update every 60 seconds
        n_intervals=0
    )
])
home_layout = html.Div([
    html.H1("Caltrain On-Time Performance"),
    html.Div(id="on-time-performance"),
    html.Div(id="delay-severity-graph-container"),
    html.Div(id="commute-delay-graph-container"),
    html.Div(id="delay-minutes-graph-container"),
    html.Div(id="heatmap-graph-container")
])

methodology_layout = html.Div([
    html.H1("Methodology"),
    html.P(""),
    html.H3("Data collection"),
    html.P('All data was gathered from the 511.org transit API.' ),
    html.P('The list of stops and stop times were downloaded from the GTFS API here: http://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={OPERATOR}'),
    html.P('Historical train position data was collected in every minute (per API restrictions) from the GTFS-RT Vehicle Monitoring API: https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency={OPERATOR}'),
    html.P('The GTFS-RT feed was parsed by vehicle to get train number, stop number, latitude, longitude, and the timestamp of when the data was collected, which was inserted into an SQLite database.'),
    html.H3("Train arrival detection"),
    html.P("Since the raw data only contains the location of each train and the stop it's travelling towards, we need to determine when the trains arrive. The distance to each stop was calculated using the Haversine formula on the train lat/long and the arriving stop lat/long. Since the data is relatively sparse, to determine when a train had arrived, the row with the minimum distance to the stop for each train ID, date, and stop ID was used to indicate train arrival."),
    html.H3("Calculation of on-time performance"),
    html.Ul([
        html.Li("Definition of delays"),
        html.Li("Any assumptions made in the analysis")
    ])
])

@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname == "/":
        return home_layout
    elif pathname == "/methods":
        return methodology_layout
    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )

@app.callback(
    [Output("on-time-performance", "children"),
     Output("delay-severity-graph-container", "children"),
     Output("commute-delay-graph-container", "children"),
     Output("delay-minutes-graph-container", "children"),
     Output("heatmap-graph-container", "children")],
    [Input('interval-component', 'n_intervals'),
     Input("url", "pathname")]
)
def update_graphs(n, pathname):
    if pathname != "/":
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    df, stops_df, stop_times_df, unique_trips, on_time_performance, delay_severity_counts = load_data()
    fig, fig_commute_delay, fig_delay_minutes, fig_heatmap = create_figures(df, unique_trips, on_time_performance, delay_severity_counts)
    
    return (
        f"{on_time_performance:.2f}%",
        dcc.Graph(figure=fig),
        dcc.Graph(figure=fig_commute_delay),
        dcc.Graph(figure=fig_delay_minutes),
        dcc.Graph(figure=fig_heatmap)
    )
# Main function to run both the data collection and Dash app
def main():
    # Start the data collection in a separate thread
    data_thread = threading.Thread(target=data_collection_loop)
    data_thread.daemon = True
    data_thread.start()

    # Run the Dash app
    app.run_server(debug=True, host='0.0.0.0')

if __name__ == '__main__':
    main()