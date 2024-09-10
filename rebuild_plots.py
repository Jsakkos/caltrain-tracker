import sqlite3
import plotly.express as px
import pandas as pd
import os
import math
import time as time_module
import plotly.graph_objects as go
import pandas as pd
import sqlite3
import math
from datetime import datetime, timedelta,time
import json
import os
DB_PATH = r'data/caltrain_lat_long.db'
# Define custom colors for each Status
STATUS_COLORS = {
    'On Time': '#00CC96',
    'Minor': '#FECB52',
    'Major': '#EF553B',
    'Minor Delay': '#FECB52',
    'Major Delay': '#EF553B'
}
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def load_data():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM train_locations", conn)
    conn.close()
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def load_stops_data():
    stops_df = pd.read_csv(os.path.join('gtfs_data', 'stops.txt'))
    stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
    return stops_df

def load_stop_times_data():
    stop_times_df = pd.read_csv(os.path.join('gtfs_data', 'stop_times.txt'))
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
def process_data(df):
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
        lambda row: calculate_time_difference( row['arrival_time'],row['actual_arrival_time'].time(),), axis=1
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
    unique_trips['hour'] = pd.to_datetime(unique_trips['actual_arrival_time']).dt.hour
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

    # Calculate percentage of delays by severity
    daily_summary = unique_trips.groupby('date')['delay_severity'].value_counts(normalize=True).unstack() * 100

    # Reset index to have date as a column
    daily_summary = daily_summary.reset_index()

    # Melt the DataFrame for Plotly
    daily_summary_melted = daily_summary.melt(id_vars='date', value_vars=['Major', 'Minor', 'On Time'], var_name='Status', value_name='Percentage')


    daily_summary_melted.loc[daily_summary_melted.Status == 'Major','Status']='Major Delay'
    daily_summary_melted.loc[daily_summary_melted.Status == 'Minor','Status']='Minor Delay'
    start_date = unique_trips.date.min().strftime('%m/%d/%Y')
    stop_date = unique_trips.date.max().strftime('%m/%d/%Y')
    n_datapoints = len(unique_trips)
    return on_time_performance,daily_summary_melted,commute_delay_counts,unique_trips,start_date,stop_date,n_datapoints
def generate_daily_stats_plot(daily_summary_melted):
    # Define the order of the Status items
    status_order = ['On Time', 'Minor Delay', 'Major Delay']
    # Create the stacked bar plot
    fig = px.bar(daily_summary_melted, x='date', y='Percentage', color='Status', 
            title='On-time performance by date',
            category_orders={'Status': status_order},
            color_discrete_map=STATUS_COLORS,labels={'date': 'Date', 'percentage': 'Percentage','Minor':'Minor Delay'})
    fig.update_layout(plot_bgcolor='#f4f4f4',paper_bgcolor='#f4f4f4', autosize=True,
    height=600,              # Set a default height
    margin=dict(l=20, r=20, t=50, b=20),
    title_font_size=24)
    fig.write_html("app/docs/static/daily_stats.html",include_plotlyjs='cdn')
def generate_commute_delay_plot(commute_delay_counts):
    fig_commute_delay = px.bar(commute_delay_counts, x='commute_period', y='percentage', color='delay_severity',
                        title="Percentage of Morning and Evening Commutes with Delays by Severity",
                        labels={'commute_period': 'Commute Period', 'percentage': 'Percentage', 'delay_severity': 'Delay Severity'},
                        color_discrete_map=STATUS_COLORS,category_orders={'Commute Period': ['Morning', 'Evening']})
    for trace in fig_commute_delay.data:
        if trace.name == 'On Time':
            trace.visible = 'legendonly'
    fig_commute_delay.update_layout(plot_bgcolor='#f4f4f4',paper_bgcolor='#f4f4f4', autosize=True,
    height=600,              # Set a default height
    margin=dict(l=20, r=20, t=50, b=20),
    title_font_size=24)
    fig_commute_delay.write_html("app/docs/static/commute_delay.html",include_plotlyjs='cdn')
def generate_delay_minutes_plot(unique_trips):
    fig_delay_minutes = px.histogram(unique_trips.loc[unique_trips.delay_minutes >=1],x='delay_minutes', color="commute_period",barmode='overlay',marginal="box",log_x=True,
                        hover_data=unique_trips.columns,
                        title="Trip delay durations",
                        labels={'commute_period': 'Commute Period','delay_minutes': 'Trip delay (mins)','count': "Number of trips"})
    fig_delay_minutes.update_layout(plot_bgcolor='#f4f4f4',paper_bgcolor='#f4f4f4', autosize=True,
    height=600,              # Set a default height
    margin=dict(l=20, r=20, t=50, b=20),
    title_font_size=24)
    fig_delay_minutes.write_html("app/docs/static/delay_minutes.html",include_plotlyjs='cdn')
def save_graphs(df):
    # Generate and save the graphs
    on_time_percentage,daily_summary_melted,commute_delay_counts,unique_trips,start_date,stop_date,n_datapoints = process_data(df)
    generate_daily_stats_plot(daily_summary_melted)
    generate_commute_delay_plot(commute_delay_counts)
    generate_delay_minutes_plot(unique_trips)
    
    return on_time_percentage,start_date,stop_date,n_datapoints

def main():
    df = load_data()
    on_time_percentage,start_date,stop_date,n_datapoints = save_graphs(df)
    
    with open("app/docs/static/on_time_percentage.txt", "w") as f:
        f.write(f"{on_time_percentage:.2f}%")
    with open("app/docs/static/stop_date.txt", "w") as f:
        f.write(f"{stop_date}")
    with open("app/docs/static/start_date.txt", "w") as f:
        f.write(f"{start_date}")
    with open("app/docs/static/n_datapoints.txt", "w") as f:
        f.write(f"{n_datapoints}")    
if __name__ == "__main__":
    main()
