"""
Standalone script for data processing and visualization without Prefect dependencies.
"""
import logging
import os
import sys
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Tuple, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the project root to the path for imports
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(BASE_DIR)

# Define utility functions locally instead of importing them
# This ensures we use our own implementations
def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on earth."""
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371 * c
    return km * 1000  # Return in meters

def calculate_time_difference(scheduled_time, actual_time):
    """Calculate the difference between scheduled and actual time in minutes."""
    try:
        # Convert scheduled_time string to a datetime object
        if isinstance(scheduled_time, str):
            # Parse the time string
            scheduled_parts = scheduled_time.split(':')
            scheduled_hours = int(scheduled_parts[0])
            scheduled_minutes = int(scheduled_parts[1])
            scheduled_seconds = int(scheduled_parts[2]) if len(scheduled_parts) > 2 else 0
            
            # Get the date part from actual_time
            date_part = actual_time.date()
            
            # Create a base datetime with the date from actual_time
            base_dt = datetime.combine(date_part, datetime.min.time())
            
            # Add the hours, minutes, seconds as a timedelta
            # This handles times past midnight (e.g., 25:30:00)
            scheduled_dt = base_dt + timedelta(hours=scheduled_hours, minutes=scheduled_minutes, seconds=scheduled_seconds)
            
            # Calculate difference in minutes
            diff = (actual_time - scheduled_dt).total_seconds() / 60
            return diff
        else:
            logger.error(f"Unexpected scheduled_time type: {type(scheduled_time)}")
            return 0  # Default to no delay if we can't parse
    except Exception as e:
        logger.error(f"Error calculating time difference: {e}, scheduled_time={scheduled_time}, actual_time={actual_time}")
        return 0  # Default to no delay on error

def categorize_commute_time(hour):
    """Categorize the time of day into commute periods."""
    if 6 <= hour < 10:
        return "Morning Commute"
    elif 16 <= hour < 20:
        return "Evening Commute"
    else:
        return "Off-Peak"

def normalize_time(time_str):
    """Normalize time string to handle times past midnight."""
    try:
        parts = time_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2]) if len(parts) > 2 else 0
        
        # Return the time as a string in HH:MM:SS format
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except Exception as e:
        logger.error(f"Error normalizing time: {e}")
        return time_str

# Define paths
try:
    from src.config import SQLITE_DB_PATH, STATIC_CONTENT_PATH
except ImportError:
    SQLITE_DB_PATH = os.path.join(BASE_DIR, 'data/caltrain_lat_long.db')
    STATIC_CONTENT_PATH = os.path.join(BASE_DIR, 'static')

# Ensure static content directory exists
os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'plots'), exist_ok=True)
os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'data'), exist_ok=True)

# Define custom colors for each Status
STATUS_COLORS = {
    "On Time": "#4CAF50",  # Green
    "Minor": "#FFC107",    # Amber
    "Major": "#F44336"     # Red
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
    try:
        # Load stops data using the same path as rebuild_plots.py
        stops_path = os.path.join(BASE_DIR, 'gtfs_data', 'stops.txt')
        logger.info(f"Loading stops data from: {stops_path}")
        stops_df = pd.read_csv(stops_path)
        
        # Filter stops to only include numeric stop_ids
        stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
        
        # Load stop times data
        stop_times_path = os.path.join(BASE_DIR, 'gtfs_data', 'stop_times.txt')
        logger.info(f"Loading stop times data from: {stop_times_path}")
        stop_times_df = pd.read_csv(stop_times_path)
        
        logger.info(f"Loaded {len(stops_df)} stops and {len(stop_times_df)} stop times from CSV files")
        return stops_df, stop_times_df
    except Exception as e:
        logger.error(f"Error loading GTFS data: {e}")
        return pd.DataFrame(), pd.DataFrame()

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
    logger.info("Merging datasets...")
    
    # Ensure consistent data types for joining
    try:
        raw_df['stop_id'] = raw_df['stop_id'].astype(int)
        raw_df['trip_id'] = raw_df['trip_id'].astype(int)
        stops_df['stop_id'] = stops_df['stop_id'].astype(int)
        stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(int)
        stop_times_df['trip_id'] = stop_times_df['trip_id'].astype(int)
    except Exception as e:
        logger.error(f"Error converting data types: {e}")
        # Try to continue with string types
        raw_df['stop_id'] = raw_df['stop_id'].astype(str)
        raw_df['trip_id'] = raw_df['trip_id'].astype(str)
        stops_df['stop_id'] = stops_df['stop_id'].astype(str)
        stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)
        stop_times_df['trip_id'] = stop_times_df['trip_id'].astype(str)
    
    # Merge datasets
    try:
        # Merge raw data with stop times
        df2 = pd.merge(raw_df, stop_times_df[['trip_id', 'stop_id', 'arrival_time']], on=['trip_id', 'stop_id'])
        
        # Merge with stops data to get stop coordinates
        df2 = pd.merge(df2, stops_df[['stop_id', 'stop_lat', 'stop_lon', 'stop_name']], on='stop_id')
        
        # Calculate distance from train to stop
        logger.info("Calculating distances...")
        df2['distance'] = df2.apply(lambda row: haversine(row['vehicle_lat'], row['vehicle_lon'], row['stop_lat'], row['stop_lon']), axis=1)
        
        # Find minimum distance for each trip_id, stop_id combination
        logger.info("Finding minimum distances...")
        idx = df2.groupby(['trip_id', 'stop_id'])['distance'].idxmin()
        arrival_df = df2.loc[idx]
        
        # Calculate arrival time (when train is closest to the stop)
        logger.info("Calculating arrival times...")
        arrival_df['arrival_datetime'] = arrival_df['timestamp']
        
        # Calculate delay (difference between scheduled and actual arrival)
        logger.info("Calculating delays...")
        # Use our local calculate_time_difference function, not the one from src.utils.time_utils
        arrival_df['delay_minutes'] = arrival_df.apply(
            lambda row: globals()['calculate_time_difference'](row['arrival_time'], row['arrival_datetime']), 
            axis=1
        )
        
        # Add date column for daily aggregation
        arrival_df['date'] = arrival_df['arrival_datetime'].dt.date
        
        # Add hour column for time-of-day analysis
        arrival_df['hour'] = arrival_df['arrival_datetime'].dt.hour
        
        # Add commute period column
        arrival_df['commute_period'] = arrival_df['hour'].apply(categorize_commute_time)
        
        # Categorize delay severity
        arrival_df['delay_severity'] = arrival_df['delay_minutes'].apply(
            lambda x: 'On Time' if x <= 5 else ('Minor' if x <= 15 else 'Major')
        )
        
        # Fill NaN values in delay_severity with 'On Time'
        arrival_df['delay_severity'].fillna('On Time', inplace=True)
        
        logger.info(f"Processed {len(arrival_df)} arrival records")
        return arrival_df
    except Exception as e:
        logger.error(f"Error processing arrival data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def save_processed_data(processed_df):
    """
    Save processed arrival data to a CSV file for persistence.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        int: Number of records saved
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'data'), exist_ok=True)
        
        # Save to CSV
        output_path = os.path.join(STATIC_CONTENT_PATH, 'data', 'processed_arrivals.csv')
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
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'plots'), exist_ok=True)
        
        # Group by date and calculate metrics
        daily_stats = processed_df.groupby('date').agg(
            avg_delay=('delay_minutes', 'mean'),
            on_time_pct=('delay_severity', lambda x: (x == 'On Time').mean() * 100),
            minor_delay_pct=('delay_severity', lambda x: (x == 'Minor').mean() * 100),
            major_delay_pct=('delay_severity', lambda x: (x == 'Major').mean() * 100),
            total_trips=('trip_id', 'nunique')
        ).reset_index()
        
        # Create figure with secondary y-axis
        fig = go.Figure()
        
        # Add bar chart for delay percentages
        fig.add_trace(go.Bar(
            x=daily_stats['date'],
            y=daily_stats['on_time_pct'],
            name='On Time',
            marker_color=STATUS_COLORS['On Time']
        ))
        
        fig.add_trace(go.Bar(
            x=daily_stats['date'],
            y=daily_stats['minor_delay_pct'],
            name='Minor Delay',
            marker_color=STATUS_COLORS['Minor']
        ))
        
        fig.add_trace(go.Bar(
            x=daily_stats['date'],
            y=daily_stats['major_delay_pct'],
            name='Major Delay',
            marker_color=STATUS_COLORS['Major']
        ))
        
        # Add line chart for average delay
        fig.add_trace(go.Scatter(
            x=daily_stats['date'],
            y=daily_stats['avg_delay'],
            name='Avg Delay (min)',
            yaxis='y2',
            line=dict(color='black', width=2)
        ))
        
        # Add line chart for total trips
        fig.add_trace(go.Scatter(
            x=daily_stats['date'],
            y=daily_stats['total_trips'],
            name='Total Trips',
            yaxis='y3',
            line=dict(color='blue', width=2, dash='dash')
        ))
        
        # Update layout
        fig.update_layout(
            title='Daily Caltrain Performance',
            barmode='stack',
            xaxis=dict(title='Date'),
            yaxis=dict(title='Percentage (%)', side='left', range=[0, 100]),
            yaxis2=dict(title='Avg Delay (min)', side='right', overlaying='y', range=[0, max(daily_stats['avg_delay']) * 1.2]),
            yaxis3=dict(title='Total Trips', side='right', overlaying='y', position=0.9, range=[0, max(daily_stats['total_trips']) * 1.2]),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255, 255, 255, 0.8)'),
            height=600,
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        # Save the plot
        output_path = os.path.join(STATIC_CONTENT_PATH, 'plots', 'daily_stats.html')
        fig.write_html(output_path)
        
        logger.info(f"Daily stats plot saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error generating daily stats plot: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_commute_delay_plot(processed_df):
    """
    Generate commute delay plot.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        str: Path to the saved plot
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'plots'), exist_ok=True)
        
        # Group by hour and commute period
        hourly_stats = processed_df.groupby(['hour', 'commute_period']).agg(
            avg_delay=('delay_minutes', 'mean'),
            on_time_pct=('delay_severity', lambda x: (x == 'On Time').mean() * 100),
            minor_delay_pct=('delay_severity', lambda x: (x == 'Minor').mean() * 100),
            major_delay_pct=('delay_severity', lambda x: (x == 'Major').mean() * 100),
            total_trips=('trip_id', 'nunique')
        ).reset_index()
        
        # Sort by hour
        hourly_stats = hourly_stats.sort_values('hour')
        
        # Create figure with secondary y-axis
        fig = go.Figure()
        
        # Add bar chart for delay percentages
        for period in ['Morning Commute', 'Evening Commute', 'Off-Peak']:
            period_data = hourly_stats[hourly_stats['commute_period'] == period]
            
            # Skip if no data for this period
            if period_data.empty:
                continue
            
            # Add traces for each delay category
            fig.add_trace(go.Bar(
                x=period_data['hour'],
                y=period_data['on_time_pct'],
                name=f'{period} - On Time',
                marker_color=STATUS_COLORS['On Time'],
                legendgroup=period,
                visible=True if period == 'Morning Commute' else 'legendonly'
            ))
            
            fig.add_trace(go.Bar(
                x=period_data['hour'],
                y=period_data['minor_delay_pct'],
                name=f'{period} - Minor Delay',
                marker_color=STATUS_COLORS['Minor'],
                legendgroup=period,
                visible=True if period == 'Morning Commute' else 'legendonly'
            ))
            
            fig.add_trace(go.Bar(
                x=period_data['hour'],
                y=period_data['major_delay_pct'],
                name=f'{period} - Major Delay',
                marker_color=STATUS_COLORS['Major'],
                legendgroup=period,
                visible=True if period == 'Morning Commute' else 'legendonly'
            ))
            
            # Add line chart for average delay
            fig.add_trace(go.Scatter(
                x=period_data['hour'],
                y=period_data['avg_delay'],
                name=f'{period} - Avg Delay (min)',
                yaxis='y2',
                line=dict(color='black', width=2),
                legendgroup=period,
                visible=True if period == 'Morning Commute' else 'legendonly'
            ))
            
            # Add line chart for total trips
            fig.add_trace(go.Scatter(
                x=period_data['hour'],
                y=period_data['total_trips'],
                name=f'{period} - Total Trips',
                yaxis='y3',
                line=dict(color='blue', width=2, dash='dash'),
                legendgroup=period,
                visible=True if period == 'Morning Commute' else 'legendonly'
            ))
        
        # Add buttons to switch between commute periods
        buttons = [
            dict(
                label='Morning Commute',
                method='update',
                args=[
                    {'visible': [True if 'Morning Commute' in trace.name else False for trace in fig.data]},
                    {'title': 'Morning Commute Performance by Hour'}
                ]
            ),
            dict(
                label='Evening Commute',
                method='update',
                args=[
                    {'visible': [True if 'Evening Commute' in trace.name else False for trace in fig.data]},
                    {'title': 'Evening Commute Performance by Hour'}
                ]
            ),
            dict(
                label='Off-Peak',
                method='update',
                args=[
                    {'visible': [True if 'Off-Peak' in trace.name else False for trace in fig.data]},
                    {'title': 'Off-Peak Performance by Hour'}
                ]
            )
        ]
        
        # Update layout
        fig.update_layout(
            title='Morning Commute Performance by Hour',
            barmode='stack',
            xaxis=dict(title='Hour of Day', tickmode='array', tickvals=list(range(24))),
            yaxis=dict(title='Percentage (%)', side='left', range=[0, 100]),
            yaxis2=dict(title='Avg Delay (min)', side='right', overlaying='y', range=[0, max(hourly_stats['avg_delay']) * 1.2]),
            yaxis3=dict(title='Total Trips', side='right', overlaying='y', position=0.9, range=[0, max(hourly_stats['total_trips']) * 1.2]),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255, 255, 255, 0.8)'),
            height=600,
            margin=dict(l=50, r=50, t=80, b=50),
            updatemenus=[dict(
                type='buttons',
                direction='right',
                x=0.5,
                y=1.15,
                xanchor='center',
                yanchor='top',
                buttons=buttons
            )]
        )
        
        # Save the plot
        output_path = os.path.join(STATIC_CONTENT_PATH, 'plots', 'commute_delay.html')
        fig.write_html(output_path)
        
        logger.info(f"Commute delay plot saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error generating commute delay plot: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_summary_stats(processed_df):
    """
    Generate summary statistics of the processed data.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        Dict[str, Any]: Summary statistics
    """
    try:
        # Calculate on-time performance
        on_time_count = len(processed_df[processed_df['delay_severity'] == 'On Time'])
        total_count = len(processed_df)
        on_time_performance = (on_time_count / total_count) * 100 if total_count > 0 else 0
        
        # Calculate average delay
        avg_delay = processed_df['delay_minutes'].mean()
        
        # Count trips by delay severity
        delay_severity_counts = processed_df['delay_severity'].value_counts().to_dict()
        
        # Get date range
        min_date = processed_df['date'].min()
        max_date = processed_df['date'].max()
        date_range = f"{min_date} to {max_date}"
        
        # Count unique trips
        total_trips = processed_df['trip_id'].nunique()
        
        # Count on-time trips
        on_time_trips = processed_df[processed_df['delay_severity'] == 'On Time']['trip_id'].nunique()
        
        # Create summary dictionary
        summary = {
            'on_time_performance': round(on_time_performance, 2),
            'total_trips': total_trips,
            'on_time_trips': on_time_trips,
            'delay_severity_counts': delay_severity_counts,
            'avg_delay_minutes': avg_delay,
            'date_range': date_range,
            'data_points': total_count
        }
        
        logger.info(f"Generated summary stats: {summary}")
        return summary
    except Exception as e:
        logger.error(f"Error generating summary stats: {e}")
        import traceback
        traceback.print_exc()
        return {}

def run_data_processing():
    """
    Run the data processing flow directly without using Prefect's API.
    """
    logger.info("Starting direct data processing")
    
    # Step 1: Load raw data
    logger.info("Loading raw data...")
    raw_df = load_raw_data()
    if raw_df.empty:
        logger.error("Raw data is empty. Check the SQLite database connection and data.")
        return {"error": "Raw data is empty"}
    
    # Step 2: Load GTFS data
    logger.info("Loading GTFS data...")
    stops_df, stop_times_df = load_gtfs_data()
    
    # Check if all required dataframes are non-empty
    if raw_df.empty or stops_df.empty or stop_times_df.empty:
        logger.warning("One or more input DataFrames are empty. Cannot process arrival data.")
        return {"error": "One or more input DataFrames are empty"}
    
    # Step 3: Process arrival data
    logger.info("Processing arrival data...")
    processed_df = process_arrival_data(raw_df, stops_df, stop_times_df)
    if processed_df.empty:
        logger.error("Processed data is empty. Check the processing logic.")
        return {"error": "Processed data is empty"}
    
    # Step 4: Save processed data
    logger.info("Saving processed data...")
    num_saved = save_processed_data(processed_df)
    logger.info(f"Saved {num_saved} processed arrival records")
    
    # Step 5: Generate plots
    logger.info("Generating plots...")
    plots = []
    try:
        daily_stats_plot = generate_daily_stats_plot(processed_df)
        plots.append(daily_stats_plot)
        logger.info(f"Daily stats plot saved to {daily_stats_plot}")
    except Exception as e:
        logger.error(f"Error generating daily stats plot: {e}")
    
    try:
        commute_delay_plot = generate_commute_delay_plot(processed_df)
        plots.append(commute_delay_plot)
        logger.info(f"Commute delay plot saved to {commute_delay_plot}")
    except Exception as e:
        logger.error(f"Error generating commute delay plot: {e}")
    
    # Step 6: Generate summary stats
    logger.info("Generating summary stats...")
    try:
        summary_stats = generate_summary_stats(processed_df)
        logger.info(f"Generated summary stats: {summary_stats}")
    except Exception as e:
        logger.error(f"Error generating summary stats: {e}")
        summary_stats = {"error": str(e)}
    
    logger.info("Data processing completed successfully")
    return {
        "plots": plots,
        "summary": summary_stats,
        "records_processed": len(processed_df),
        "records_saved": num_saved
    }

if __name__ == "__main__":
    try:
        result = run_data_processing()
        print("Data processing completed with result:", result)
        
        # Check if plots were generated successfully
        if result and 'plots' in result and result['plots']:
            print(f"Plots generated: {', '.join(filter(None, result['plots']))}")
            
        # Check if summary stats were generated successfully
        if result and 'summary' in result:
            print(f"Summary stats: {result['summary']}")
    except Exception as e:
        print(f"Error running data processing: {e}")
        import traceback
        traceback.print_exc()
