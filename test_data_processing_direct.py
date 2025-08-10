#!/usr/bin/env python3
"""
Standalone script to test data processing without Prefect dependencies.
This script replicates the core functionality of data_processing.py but without Prefect.
"""
import os
import logging
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, time
import math

from src.config import SQLITE_DB_PATH, STATIC_CONTENT_PATH
from src.utils.time_utils import calculate_time_difference, categorize_commute_time, normalize_time
from src.utils.geo_utils import haversine

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define custom colors for each Status
STATUS_COLORS = {
    'On Time': '#00CC96',
    'Minor': '#FECB52',
    'Major': '#EF553B',
    'Minor Delay': '#FECB52',
    'Major Delay': '#EF553B'
}

def load_raw_data():
    """
    Load raw train location data from the SQLite database.
    
    Returns:
        pd.DataFrame: DataFrame containing train location data
    """
    logger.info(f"Loading raw data from SQLite database: {SQLITE_DB_PATH}")
    logger.info(f"Database exists: {os.path.exists(SQLITE_DB_PATH)}")
    
    conn = sqlite3.connect(SQLITE_DB_PATH)
    try:
        # First, let's check the table structure
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
        logger.info(f"Tables in database: {tables['name'].tolist()}")
        
        # Get table schema to understand column types
        if 'train_locations' in tables['name'].tolist():
            schema = pd.read_sql_query("PRAGMA table_info(train_locations)", conn)
            logger.info(f"Table schema: {schema[['name', 'type']].to_dict('records')}")
            
            # Query train locations directly using pandas but without parsing timestamps yet
            df = pd.read_sql_query("SELECT * FROM train_locations", conn)
            
            # Ensure we have the expected columns
            if df.empty:
                logger.warning("No train location data found in database")
                return pd.DataFrame()
                
            # Convert timestamp to datetime with error handling
            if 'timestamp' in df.columns:
                try:
                    # Try parsing with different formats
                    logger.info(f"Sample timestamp value: {df['timestamp'].iloc[0] if not df.empty else 'N/A'}")
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    # Drop rows with invalid timestamps
                    df = df.dropna(subset=['timestamp'])
                    logger.info(f"Successfully parsed timestamps, {len(df)} valid records")
                except Exception as e:
                    logger.error(f"Error parsing timestamps: {e}")
                    # Try a different approach - parse as strings first
                    try:
                        logger.info("Trying alternative timestamp parsing approach...")
                        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce')
                        df = df.dropna(subset=['timestamp'])
                        logger.info(f"Alternative parsing successful, {len(df)} valid records")
                    except Exception as e2:
                        logger.error(f"Alternative parsing also failed: {e2}")
            
            logger.info(f"Loaded {len(df)} raw train location records from database")
            logger.info(f"Raw data columns: {df.columns.tolist()}")
            if not df.empty:
                logger.info(f"Raw data sample: {df.head(1).to_dict('records')}")
            return df
        else:
            logger.error("train_locations table not found in database")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading train location data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def load_gtfs_data():
    """
    Load GTFS static data (stops and stop times) from CSV files.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Tuple of DataFrames (stops_df, stop_times_df)
    """
    # Load stops from CSV file
    stops_df = pd.read_csv(os.path.join('gtfs_data', 'stops.txt'))
    stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
    
    # Load stop times from CSV file
    stop_times_df = pd.read_csv(os.path.join('gtfs_data', 'stop_times.txt'))
    
    logger.info(f"Loaded {len(stops_df)} stops and {len(stop_times_df)} stop times from CSV files")
    return stops_df, stop_times_df

def process_arrival_data(raw_df, stops_df, stop_times_df):
    """
    Process raw train location data to calculate arrival metrics.
    
    Args:
        raw_df (pd.DataFrame): Raw train location data
        stops_df (pd.DataFrame): GTFS stops data
        stop_times_df (pd.DataFrame): GTFS stop times data
        
    Returns:
        pd.DataFrame: Processed arrival data
    """
    # Ensure we have data to process
    if raw_df.empty or stops_df.empty or stop_times_df.empty:
        logger.warning("One or more input DataFrames are empty. Cannot process arrival data.")
        return pd.DataFrame()  # Return empty DataFrame
        
    # Check if stop_id exists in the raw_df
    if 'stop_id' not in raw_df.columns:
        logger.error("'stop_id' column missing from raw data")
        logger.debug(f"Available columns: {raw_df.columns.tolist()}")
        return pd.DataFrame()  # Return empty DataFrame
        
    # Ensure consistent data types for joining
    raw_df['stop_id'] = raw_df['stop_id'].astype(int)
    raw_df['trip_id'] = raw_df['trip_id'].astype(int)
    stops_df['stop_id'] = stops_df['stop_id'].astype(int)
    stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(int)
    stop_times_df['trip_id'] = stop_times_df['trip_id'].astype(int)

    # Merge datasets
    logger.info("Merging datasets...")
    df2 = pd.merge(raw_df, stop_times_df[['trip_id', 'stop_id', 'arrival_time']], on=['trip_id', 'stop_id'])
    df2 = pd.merge(df2, stops_df[['stop_id', 'stop_name', 'parent_station', 'stop_lat', 'stop_lon']], on=['stop_id'])

    # Calculate distance between train and stop
    logger.info("Calculating distances...")
    df2['distance'] = df2.apply(lambda row: haversine(
        row['vehicle_lat'], row['vehicle_lon'], row['stop_lat'], row['stop_lon']
    ), axis=1)
    
    # Convert timestamp
    df2['timestamp'] = pd.to_datetime(df2['timestamp'])
    df2['date'] = df2['timestamp'].dt.date
    
    # Normalize arrival times
    df2['arrival_time'] = df2['arrival_time'].apply(normalize_time)
    df2['arrival_time'] = pd.to_datetime(df2['arrival_time'], format='%H:%M:%S').dt.time
        
    # Find the minimum distance for each trip-stop-date combination
    logger.info("Finding minimum distances...")
    min_distances = df2.groupby(['trip_id', 'stop_id', 'date'])['distance'].min().reset_index()
    
    # Merge to get records with minimum distances
    merged_df = pd.merge(df2, min_distances, on=['trip_id', 'stop_id', 'date', 'distance'])
    
    # Get the first timestamp for each trip-stop-date (closest approach)
    logger.info("Calculating arrival times...")
    arrival_times = merged_df.groupby(['trip_id', 'stop_id', 'date']).first().reset_index()
    arrival_times = arrival_times[['trip_id', 'stop_id', 'date', 'timestamp']]
    arrival_times.rename(columns={'timestamp': 'actual_arrival_time'}, inplace=True)
    
    # Merge to get scheduled arrival time
    comparison_df = pd.merge(
        arrival_times, 
        df2[['trip_id', 'stop_id', 'stop_name', 'parent_station', 'date', 'arrival_time']], 
        on=['trip_id', 'stop_id', 'date']
    )
    
    # Calculate delay in minutes
    logger.info("Calculating delays...")
    comparison_df['delay_minutes'] = comparison_df.apply(
        lambda row: calculate_time_difference(row['arrival_time'], row['actual_arrival_time'].time()), 
        axis=1
    )
    
    # Clean up unrealistic delays
    comparison_df.loc[comparison_df.delay_minutes > 500, 'delay_minutes'] = 0.0
    comparison_df.loc[comparison_df.delay_minutes < -100, 'delay_minutes'] = 0.0
    
    # Determine if delayed and delay severity
    comparison_df['is_delayed'] = comparison_df['delay_minutes'] > 4
    comparison_df.loc[(comparison_df.delay_minutes > 4) & (comparison_df.delay_minutes <= 15), 'delay_severity'] = 'Minor'
    comparison_df.loc[comparison_df.delay_minutes > 15, 'delay_severity'] = 'Major'
    comparison_df['delay_severity'].fillna('On Time', inplace=True)
    comparison_df.loc[comparison_df.delay_minutes < 0, 'delay_minutes'] = 0
    
    # Categorize commute period
    comparison_df['commute_period'] = comparison_df['actual_arrival_time'].apply(categorize_commute_time)
    comparison_df['hour'] = pd.to_datetime(comparison_df['actual_arrival_time']).dt.hour
    
    logger.info(f"Processed {len(comparison_df)} arrival records")
    return comparison_df

def save_processed_data(processed_df):
    """
    Save processed arrival data to a CSV file for persistence.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        int: Number of records saved
    """
    if processed_df.empty:
        logger.warning("No processed data to save")
        return 0
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'data'), exist_ok=True)
        output_path = os.path.join(STATIC_CONTENT_PATH, 'data', 'processed_arrivals.csv')
        
        # Save to CSV
        processed_df.to_csv(output_path, index=False)
        
        logger.info(f"Saved {len(processed_df)} processed arrival records to {output_path}")
        return len(processed_df)
    except Exception as e:
        logger.error(f"Error saving processed data: {e}")
        return 0

def generate_daily_stats_plot(processed_df):
    """
    Generate daily statistics plot.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        str: Path to the saved plot
    """
    # Ensure directory exists
    os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'plots'), exist_ok=True)
    output_path = os.path.join(STATIC_CONTENT_PATH, 'plots', 'daily_stats.html')
    
    # Check if DataFrame is empty
    if processed_df.empty:
        logger.warning("Empty DataFrame provided to generate_daily_stats_plot. Creating empty plot.")
        # Create an empty figure with a message
        fig = go.Figure()
        fig.add_annotation(
            text="No data available for the selected period",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        fig.update_layout(
            title="On-time performance by date",
            plot_bgcolor='#f4f4f4',
            paper_bgcolor='#f4f4f4', 
            autosize=True,
            height=600,
            margin=dict(l=20, r=20, t=50, b=20),
            title_font_size=24
        )
        fig.write_html(output_path, include_plotlyjs='cdn')
        return output_path
    
    # Calculate percentage of delays by severity
    daily_summary = processed_df.groupby('date')['delay_severity'].value_counts(normalize=True).unstack() * 100

    # Reset index to have date as a column
    daily_summary = daily_summary.reset_index()

    # Melt the DataFrame for Plotly
    daily_summary_melted = daily_summary.melt(
        id_vars='date', 
        value_vars=['Major', 'Minor', 'On Time'], 
        var_name='Status', 
        value_name='Percentage'
    )

    # Rename for better display
    daily_summary_melted.loc[daily_summary_melted.Status == 'Major', 'Status'] = 'Major Delay'
    daily_summary_melted.loc[daily_summary_melted.Status == 'Minor', 'Status'] = 'Minor Delay'
    
    # Define the order of the Status items
    status_order = ['On Time', 'Minor Delay', 'Major Delay']
    
    # Create the stacked bar plot
    fig = px.bar(
        daily_summary_melted, 
        x='date', 
        y='Percentage', 
        color='Status', 
        title='On-time performance by date',
        category_orders={'Status': status_order},
        color_discrete_map=STATUS_COLORS,
        labels={'date': 'Date', 'percentage': 'Percentage'}
    )
    
    fig.update_layout(
        plot_bgcolor='#f4f4f4',
        paper_bgcolor='#f4f4f4', 
        autosize=True,
        height=600,
        margin=dict(l=20, r=20, t=50, b=20),
        title_font_size=24
    )
    
    fig.write_html(output_path, include_plotlyjs='cdn')
    logger.info(f"Daily stats plot saved to {output_path}")
    return output_path

def generate_commute_delay_plot(processed_df):
    """
    Generate commute delay plot.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        str: Path to the saved plot
    """
    # Ensure directory exists
    os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'plots'), exist_ok=True)
    output_path = os.path.join(STATIC_CONTENT_PATH, 'plots', 'commute_delay.html')
    
    # Check if DataFrame is empty
    if processed_df.empty:
        logger.warning("Empty DataFrame provided to generate_commute_delay_plot. Creating empty plot.")
        # Create an empty figure with a message
        fig = go.Figure()
        fig.add_annotation(
            text="No data available for the selected period",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        fig.update_layout(
            title="Delays by commute period",
            plot_bgcolor='#f4f4f4',
            paper_bgcolor='#f4f4f4', 
            autosize=True,
            height=600,
            margin=dict(l=20, r=20, t=50, b=20),
            title_font_size=24
        )
        fig.write_html(output_path, include_plotlyjs='cdn')
        return output_path
    
    # Filter for Morning and Evening commutes
    filtered_trips = processed_df[processed_df['commute_period'].isin(['Morning', 'Evening'])]
    
    if filtered_trips.empty:
        logger.warning("No commute data available. Creating empty plot.")
        fig = go.Figure()
        fig.add_annotation(
            text="No commute data available for the selected period",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        fig.update_layout(
            title="Delays by commute period",
            plot_bgcolor='#f4f4f4',
            paper_bgcolor='#f4f4f4', 
            autosize=True,
            height=600,
            margin=dict(l=20, r=20, t=50, b=20),
            title_font_size=24
        )
        fig.write_html(output_path, include_plotlyjs='cdn')
        return output_path
    
    # Calculate total trips for each commute period
    total_commute_period_trips = filtered_trips.groupby('commute_period').size().reset_index(name='total_counts')
    
    # Calculate counts of delays by commute period and severity
    commute_delay_counts = filtered_trips.groupby(['commute_period', 'delay_severity']).size().reset_index(name='counts')
    
    # Merge to get total counts for each commute period
    commute_delay_counts = pd.merge(commute_delay_counts, total_commute_period_trips, on='commute_period')
    
    # Calculate percentage of delays by commute period and severity
    commute_delay_counts['percentage'] = (commute_delay_counts['counts'] / commute_delay_counts['total_counts']) * 100
    
    # Create the grouped bar chart
    fig = px.bar(
        commute_delay_counts, 
        x='commute_period', 
        y='percentage', 
        color='delay_severity',
        title='Delays by commute period',
        color_discrete_map=STATUS_COLORS,
        labels={
            'commute_period': 'Commute Period', 
            'percentage': 'Percentage of Trips', 
            'delay_severity': 'Delay Status'
        },
        barmode='group'
    )
    
    fig.update_layout(
        plot_bgcolor='#f4f4f4',
        paper_bgcolor='#f4f4f4', 
        autosize=True,
        height=600,
        margin=dict(l=20, r=20, t=50, b=20),
        title_font_size=24
    )
    
    fig.write_html(output_path, include_plotlyjs='cdn')
    logger.info(f"Commute delay plot saved to {output_path}")
    return output_path

def generate_summary_stats(processed_df):
    """
    Generate summary statistics of the processed data.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        dict: Summary statistics
    """
    if processed_df.empty:
        logger.warning("Empty DataFrame provided to generate_summary_stats")
        return {
            'error': 'No data available',
            'on_time_performance': 0,
            'total_trips': 0
        }
    
    # Calculate the overall on-time performance based on unique trip counts
    unique_trips = processed_df.drop_duplicates(subset=['trip_id', 'stop_id', 'date'])
    total_trips = len(unique_trips)
    on_time_trips = len(unique_trips[unique_trips['is_delayed'] == False])
    on_time_performance = (on_time_trips / total_trips) * 100 if total_trips > 0 else 0
    
    # Calculate delay severity counts
    delay_severity_counts = unique_trips['delay_severity'].value_counts().to_dict()
    
    # Calculate average delay
    avg_delay = unique_trips['delay_minutes'].mean()
    
    # Get date range
    start_date = unique_trips['date'].min().strftime('%Y-%m-%d') if not unique_trips.empty else 'N/A'
    end_date = unique_trips['date'].max().strftime('%Y-%m-%d') if not unique_trips.empty else 'N/A'
    
    summary = {
        'on_time_performance': round(on_time_performance, 2),
        'total_trips': total_trips,
        'on_time_trips': on_time_trips,
        'delay_severity_counts': delay_severity_counts,
        'avg_delay_minutes': round(avg_delay, 2),
        'date_range': f"{start_date} to {end_date}",
        'data_points': len(processed_df)
    }
    
    logger.info(f"Generated summary stats: {summary}")
    return summary

def main():
    """
    Main function to run the data processing pipeline.
    """
    logger.info("Starting direct data processing test")
    
    # Load data
    raw_df = load_raw_data()
    
    if raw_df.empty:
        logger.error("Raw data is empty. Check the SQLite database connection and data.")
        return
    
    # Load GTFS data
    stops_df, stop_times_df = load_gtfs_data()
    
    if stops_df.empty or stop_times_df.empty:
        logger.error("GTFS data is empty. Check the CSV files in gtfs_data directory.")
        return
    
    # Process data
    processed_df = process_arrival_data(raw_df, stops_df, stop_times_df)
    
    if processed_df.empty:
        logger.error("Failed to process arrival data. Check the data processing logic.")
        return
    
    # Save processed data
    save_processed_data(processed_df)
    
    # Generate visualizations
    daily_stats_plot = generate_daily_stats_plot(processed_df)
    commute_delay_plot = generate_commute_delay_plot(processed_df)
    
    # Generate summary statistics
    summary_stats = generate_summary_stats(processed_df)
    
    logger.info("Data processing completed successfully")
    logger.info(f"Summary stats: {summary_stats}")
    logger.info(f"Plots generated: {daily_stats_plot}, {commute_delay_plot}")

if __name__ == "__main__":
    main()
