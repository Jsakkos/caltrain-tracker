"""
Prefect flows for data processing and visualization.
"""
# Removed logging import as we're using print statements with Prefect
import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import select, func
from prefect import task, flow
from typing import Tuple, Dict, List, Any
import sqlite3

# Add the project root to the path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Local imports
try:
    from src.config import SQLITE_DB_PATH, STATIC_CONTENT_PATH
    from src.utils.time_utils import calculate_time_difference, categorize_commute_time, normalize_time
    from src.utils.geo_utils import haversine
except ImportError:
    # If running directly, set these variables manually
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    SQLITE_DB_PATH = os.path.join(BASE_DIR, 'data/caltrain_lat_long.db')
    STATIC_CONTENT_PATH = os.path.join(BASE_DIR, 'static')
    
    # Import utility functions directly
    sys.path.append(os.path.join(BASE_DIR, 'src'))
    from utils.time_utils import calculate_time_difference, categorize_commute_time, normalize_time
    from utils.geo_utils import haversine

# Define custom colors for each Status
STATUS_COLORS = {
    'On Time': '#00CC96',
    'Minor': '#FECB52',
    'Major': '#EF553B',
    'Minor Delay': '#FECB52',
    'Major Delay': '#EF553B'
}

# Removed logger initialization as we're using print statements with Prefect

@task
def load_raw_data() -> pd.DataFrame:
    """
    Load raw train location data from the database.
    
    Returns:
        pd.DataFrame: DataFrame containing train location data
    """
    print(f"Loading raw data from SQLite database: {SQLITE_DB_PATH}")
    print(f"Database exists: {os.path.exists(SQLITE_DB_PATH)}")
    
    conn = sqlite3.connect(SQLITE_DB_PATH)
    try:
        # First, let's check the table structure
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
        print(f"Tables in database: {tables['name'].tolist()}")
        
        # Get table schema to understand column types
        if 'train_locations' in tables['name'].tolist():
            schema = pd.read_sql_query("PRAGMA table_info(train_locations)", conn)
            print(f"Table schema: {schema[['name', 'type']].to_dict('records')}")
            
            # Query train locations directly using pandas but without parsing timestamps yet
            df = pd.read_sql_query("SELECT * FROM train_locations", conn)
            
            # Ensure we have the expected columns
            if df.empty:
                print("WARNING: No train location data found in database")
                return pd.DataFrame()
                
            # Convert timestamp to datetime with error handling
            if 'timestamp' in df.columns:
                try:
                    # Try parsing with different formats
                    print(f"Sample timestamp value: {df['timestamp'].iloc[0] if not df.empty else 'N/A'}")
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    # Drop rows with invalid timestamps
                    df = df.dropna(subset=['timestamp'])
                    print(f"Successfully parsed timestamps, {len(df)} valid records")
                except Exception as e:
                    print(f"ERROR: Error parsing timestamps: {e}")
                    # Try a different approach - parse as strings first
                    try:
                        print("Trying alternative timestamp parsing approach...")
                        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce')
                        df = df.dropna(subset=['timestamp'])
                        print(f"Alternative parsing successful, {len(df)} valid records")
                    except Exception as e2:
                        print(f"ERROR: Alternative parsing also failed: {e2}")
            
            print(f"Loaded {len(df)} raw train location records from database")
            print(f"Raw data columns: {df.columns.tolist()}")
            if not df.empty:
                print(f"Raw data sample: {df.head(1).to_dict('records')}")
            return df
        else:
            print("ERROR: train_locations table not found in database")
            return pd.DataFrame()
    except Exception as e:
        print(f"ERROR: Error loading train location data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

@task
def load_gtfs_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load GTFS static data (stops and stop times) from the database.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Tuple of DataFrames (stops_df, stop_times_df)
    """
    # Load stops from CSV file like in rebuild_plots.py
    stops_df = pd.read_csv(os.path.join('gtfs_data', 'stops.txt'))
    stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
    
    # Load stop times from CSV file
    stop_times_df = pd.read_csv(os.path.join('gtfs_data', 'stop_times.txt'))
    
    print(f"Loaded {len(stops_df)} stops and {len(stop_times_df)} stop times from CSV files")
    return stops_df, stop_times_df

@task
def process_arrival_data(raw_df: pd.DataFrame, stops_df: pd.DataFrame, stop_times_df: pd.DataFrame) -> pd.DataFrame:
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
        print("WARNING: One or more input DataFrames are empty. Cannot process arrival data.")
        return pd.DataFrame()  # Return empty DataFrame
        
    # Check if stop_id exists in the raw_df
    if 'stop_id' not in raw_df.columns:
        print("ERROR: 'stop_id' column missing from raw data")
        print(f"DEBUG: Available columns: {raw_df.columns.tolist()}")
        return pd.DataFrame()  # Return empty DataFrame
        
    # Ensure consistent data types for joining
    try:
        # Try to convert to integers first
        raw_df['stop_id'] = raw_df['stop_id'].astype(int)
        raw_df['trip_id'] = raw_df['trip_id'].astype(int)
        stops_df['stop_id'] = stops_df['stop_id'].astype(int)
        stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(int)
        stop_times_df['trip_id'] = stop_times_df['trip_id'].astype(int)
    except Exception as e:
        print(f"WARNING: Could not convert to integers: {e}")
        # Fall back to string types
        raw_df['stop_id'] = raw_df['stop_id'].astype(str)
        raw_df['trip_id'] = raw_df['trip_id'].astype(str)
        stops_df['stop_id'] = stops_df['stop_id'].astype(str)
        stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)
        stop_times_df['trip_id'] = stop_times_df['trip_id'].astype(str)

    # Merge datasets
    df2 = pd.merge(raw_df, stop_times_df[['trip_id', 'stop_id', 'arrival_time']], on=['trip_id', 'stop_id'])
    df2 = pd.merge(df2, stops_df[['stop_id', 'stop_name', 'parent_station', 'stop_lat', 'stop_lon']], on=['stop_id'])

    # Calculate distance between train and stop
    df2['distance'] = df2.apply(lambda row: haversine(
        row['vehicle_lat'], row['vehicle_lon'], row['stop_lat'], row['stop_lon']
    ), axis=1)
    
    # Convert timestamp
    df2['timestamp'] = pd.to_datetime(df2['timestamp'])
    df2['date'] = df2['timestamp'].dt.date
    
    # Normalize arrival times - keep as string for now
    df2['arrival_time'] = df2['arrival_time'].apply(normalize_time)
        
    # Find the minimum distance for each trip-stop-date combination
    min_distances = df2.groupby(['trip_id', 'stop_id', 'date'])['distance'].min().reset_index()
    
    # Merge to get records with minimum distances
    merged_df = pd.merge(df2, min_distances, on=['trip_id', 'stop_id', 'date', 'distance'])
    
    # Get the first timestamp for each trip-stop-date (closest approach)
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
    # We need to handle the string format of arrival_time
    comparison_df['delay_minutes'] = comparison_df.apply(
        lambda row: calculate_time_difference(row['arrival_time'], row['actual_arrival_time']), 
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
    
    print(f"Processed {len(comparison_df)} arrival records")
    return comparison_df

@task
def save_processed_data(processed_df: pd.DataFrame) -> int:
    """
    Save processed arrival data to a CSV file for persistence.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        int: Number of records saved
    """
    if processed_df.empty:
        print("WARNING: No processed data to save")
        return 0
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'data'), exist_ok=True)
        output_path = os.path.join(STATIC_CONTENT_PATH, 'data', 'processed_arrivals.csv')
        
        # Save to CSV
        processed_df.to_csv(output_path, index=False)
        
        print(f"Saved {len(processed_df)} processed arrival records to {output_path}")
        return len(processed_df)
    except Exception as e:
        print(f"ERROR: Error saving processed data: {e}")
        return 0

@task
def generate_daily_stats_plot(processed_df: pd.DataFrame) -> str:
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
        print("WARNING: Empty DataFrame provided to generate_daily_stats_plot. Creating empty plot.")
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
        print(f"Generated empty daily stats plot: {output_path}")
        return output_path
    
    try:
        # Calculate percentage of delays by severity per day
        daily_summary = processed_df.groupby('date')['delay_severity'].value_counts(normalize=True).unstack() * 100
        daily_summary = daily_summary.reset_index()
        
        # Check if we have all required columns
        required_columns = ['Major', 'Minor', 'On Time']
        missing_columns = [col for col in required_columns if col not in daily_summary.columns]
        
        # Add missing columns with zeros
        for col in missing_columns:
            daily_summary[col] = 0
        
        # Prepare data for plotting
        daily_summary_melted = daily_summary.melt(
            id_vars='date', 
            value_vars=['Major', 'Minor', 'On Time'], 
            var_name='Status', 
            value_name='Percentage'
        )
        
        # Rename status labels
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
            labels={'date': 'Date', 'Percentage': 'Percentage'}
        )
        
        fig.update_layout(
            plot_bgcolor='#f4f4f4',
            paper_bgcolor='#f4f4f4', 
            autosize=True,
            height=600,
            margin=dict(l=20, r=20, t=50, b=20),
            title_font_size=24
        )
        
        # Save the figure
        fig.write_html(output_path, include_plotlyjs='cdn')
        print(f"Generated daily stats plot: {output_path}")
    except Exception as e:
        print(f"ERROR: Error generating daily stats plot: {e}")
        # Create an error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error generating plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="red")
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

@task
def generate_commute_delay_plot(processed_df: pd.DataFrame) -> str:
    """
    Generate commute delay plot.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        str: Path to the saved plot
    """
    # Ensure directory exists
    os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'plots'), exist_ok=True)
    output_path = os.path.join(STATIC_CONTENT_PATH, 'plots', 'commute_delays.html')
    
    # Check if DataFrame is empty
    if processed_df.empty:
        print("WARNING: Empty DataFrame provided to generate_commute_delay_plot. Creating empty plot.")
        # Create an empty figure with a message
        fig = go.Figure()
        fig.add_annotation(
            text="No data available for commute periods",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        fig.update_layout(
            title="Delay Distribution by Commute Period",
            plot_bgcolor='#f4f4f4',
            paper_bgcolor='#f4f4f4', 
            autosize=True,
            height=600,
            margin=dict(l=20, r=20, t=50, b=20),
            title_font_size=24
        )
        fig.write_html(output_path, include_plotlyjs='cdn')
        print(f"Generated empty commute delay plot: {output_path}")
        return output_path
    
    try:
        # Filter for Morning and Evening commutes
        filtered_trips = processed_df[processed_df['commute_period'].isin(['Morning', 'Evening'])]
        
        # Check if filtered data is empty
        if filtered_trips.empty:
            print("WARNING: No commute period data found. Creating empty plot.")
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for Morning or Evening commute periods",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=20)
            )
            fig.update_layout(
                title="Delay Distribution by Commute Period",
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
        
        # Calculate percentage
        commute_delay_counts['percentage'] = (commute_delay_counts['counts'] / commute_delay_counts['total_counts']) * 100
        
        # Define the order of the Status items
        status_order = ['On Time', 'Minor', 'Major']
        
        # Create the grouped bar chart
        fig = px.bar(
            commute_delay_counts, 
            x='commute_period', 
            y='percentage', 
            color='delay_severity',
            barmode='group',
            title='Delay Distribution by Commute Period',
            category_orders={'delay_severity': status_order},
            color_discrete_map=STATUS_COLORS,
            labels={
                'commute_period': 'Commute Period', 
                'percentage': 'Percentage', 
                'delay_severity': 'Delay Status'
            }
        )
        
        fig.update_layout(
            plot_bgcolor='#f4f4f4',
            paper_bgcolor='#f4f4f4', 
            autosize=True,
            height=600,
            margin=dict(l=20, r=20, t=50, b=20),
            title_font_size=24
        )
        
        # Save the figure
        fig.write_html(output_path, include_plotlyjs='cdn')
        print(f"Generated commute delay plot: {output_path}")
    except Exception as e:
        print(f"ERROR: Error generating commute delay plot: {e}")
        # Create an error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error generating plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="red")
        )
        fig.update_layout(
            title="Delay Distribution by Commute Period",
            plot_bgcolor='#f4f4f4',
            paper_bgcolor='#f4f4f4', 
            autosize=True,
            height=600,
            margin=dict(l=20, r=20, t=50, b=20),
            title_font_size=24
        )
        fig.write_html(output_path, include_plotlyjs='cdn')
    
    return output_path

@task
def generate_summary_stats(processed_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics of the processed data.
    
    Args:
        processed_df (pd.DataFrame): Processed arrival data
        
    Returns:
        Dict[str, Any]: Summary statistics
    """
    # Ensure directory exists for output
    os.makedirs(os.path.join(STATIC_CONTENT_PATH, 'data'), exist_ok=True)
    output_path = os.path.join(STATIC_CONTENT_PATH, 'data', 'summary_stats.json')
    
    # Check if DataFrame is empty
    if processed_df.empty:
        print("WARNING: Empty DataFrame provided to generate_summary_stats. Creating empty summary.")
        # Create a default summary with empty/zero values
        summary = {
            'on_time_performance': 0,
            'total_trips': 0,
            'on_time_trips': 0,
            'best_train': {
                'id': 0,
                'avg_delay_minutes': 0
            },
            'worst_train': {
                'id': 0,
                'avg_delay_minutes': 0
            },
            'best_stop': {
                'id': 0,
                'name': 'N/A',
                'avg_delay_minutes': 0
            },
            'worst_stop': {
                'id': 0,
                'name': 'N/A',
                'avg_delay_minutes': 0
            },
            'date_range': {
                'start': 'N/A',
                'end': 'N/A'
            },
            'data_status': 'No data available'
        }
        
        # Save empty summary to JSON
        import json
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"Generated empty summary statistics: {output_path}")
        return summary
    
    try:
        unique_trips = processed_df.drop_duplicates(subset=['trip_id', 'stop_id', 'date'])
        
        # Calculate on-time performance
        total_trips = len(unique_trips)
        on_time_trips = len(unique_trips[unique_trips['is_delayed'] == False])
        on_time_performance = (on_time_trips / total_trips) * 100 if total_trips > 0 else 0
        
        # Calculate the best/worst trains - with error handling
        try:
            best_train = unique_trips.groupby('trip_id')['delay_minutes'].mean().reset_index()
            if not best_train.empty:
                best_train_id = int(best_train.sort_values(by='delay_minutes').reset_index(drop=True).iloc[0, 0])
                best_train_delay_minutes = float(best_train.sort_values(by='delay_minutes').reset_index(drop=True).iloc[0, 1])
            else:
                best_train_id = 0
                best_train_delay_minutes = 0
                
            worst_train = unique_trips.groupby('trip_id')['delay_minutes'].mean().reset_index()
            if not worst_train.empty:
                worst_train_id = int(worst_train.sort_values(by='delay_minutes', ascending=False).reset_index(drop=True).iloc[0, 0])
                worst_train_delay_minutes = float(
                    worst_train.sort_values(by='delay_minutes', ascending=False).reset_index(drop=True).iloc[0, 1]
                )
            else:
                worst_train_id = 0
                worst_train_delay_minutes = 0
        except (IndexError, KeyError) as e:
            print(f"ERROR: Error calculating best/worst trains: {e}")
            best_train_id = 0
            best_train_delay_minutes = 0
            worst_train_id = 0
            worst_train_delay_minutes = 0
        
        # Calculate the best/worst stops - with error handling
        try:
            best_stop = unique_trips.groupby('stop_id')['delay_minutes'].mean().reset_index()
            if not best_stop.empty:
                best_stop_id = int(best_stop.sort_values(by='delay_minutes').reset_index(drop=True).iloc[0, 0])
                best_stop_delay_minutes = float(best_stop.sort_values(by='delay_minutes').reset_index(drop=True).iloc[0, 1])
                best_stop_name = unique_trips.loc[unique_trips.stop_id == best_stop_id, 'stop_name'].reset_index(drop=True).iloc[0]
            else:
                best_stop_id = 0
                best_stop_delay_minutes = 0
                best_stop_name = 'N/A'
                
            worst_stop = unique_trips.groupby('stop_id')['delay_minutes'].mean().reset_index()
            if not worst_stop.empty:
                worst_stop_id = int(worst_stop.sort_values(by='delay_minutes', ascending=False).reset_index(drop=True).iloc[0, 0])
                worst_stop_delay_minutes = float(
                    worst_stop.sort_values(by='delay_minutes', ascending=False).reset_index(drop=True).iloc[0, 1]
                )
                worst_stop_name = unique_trips.loc[
                    unique_trips.stop_id == worst_stop_id, 'stop_name'
                ].reset_index(drop=True).iloc[0]
            else:
                worst_stop_id = 0
                worst_stop_delay_minutes = 0
                worst_stop_name = 'N/A'
        except (IndexError, KeyError) as e:
            print(f"ERROR: Error calculating best/worst stops: {e}")
            best_stop_id = 0
            best_stop_delay_minutes = 0
            best_stop_name = 'N/A'
            worst_stop_id = 0
            worst_stop_delay_minutes = 0
            worst_stop_name = 'N/A'
        
        # Date range - with error handling
        try:
            start_date = unique_trips.date.min().strftime('%m/%d/%Y')
            end_date = unique_trips.date.max().strftime('%m/%d/%Y')
        except (AttributeError, ValueError) as e:
            print(f"ERROR: Error calculating date range: {e}")
            start_date = 'N/A'
            end_date = 'N/A'
        
        # Create summary
        summary = {
            'on_time_performance': on_time_performance,
            'total_trips': total_trips,
            'on_time_trips': on_time_trips,
            'best_train': {
                'id': best_train_id,
                'avg_delay_minutes': best_train_delay_minutes
            },
            'worst_train': {
                'id': worst_train_id,
                'avg_delay_minutes': worst_train_delay_minutes
            },
            'best_stop': {
                'id': best_stop_id,
                'name': best_stop_name,
                'avg_delay_minutes': best_stop_delay_minutes
            },
            'worst_stop': {
                'id': worst_stop_id,
                'name': worst_stop_name,
                'avg_delay_minutes': worst_stop_delay_minutes
            },
            'date_range': {
                'start': start_date,
                'end': end_date
            },
            'data_status': 'Data available'
        }
        
        # Save to JSON file
        import json
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"Generated summary statistics: {output_path}")
        return summary
    except Exception as e:
        print(f"ERROR: Error generating summary statistics: {e}")
        # Create a default summary with empty/zero values
        summary = {
            'on_time_performance': 0,
            'total_trips': 0,
            'on_time_trips': 0,
            'best_train': {'id': 0, 'avg_delay_minutes': 0},
            'worst_train': {'id': 0, 'avg_delay_minutes': 0},
            'best_stop': {'id': 0, 'name': 'N/A', 'avg_delay_minutes': 0},
            'worst_stop': {'id': 0, 'name': 'N/A', 'avg_delay_minutes': 0},
            'date_range': {'start': 'N/A', 'end': 'N/A'},
            'data_status': f'Error: {str(e)}'
        }
        
        # Save error summary to JSON
        import json
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        return summary

@task
def generate_dashboard_data(processed_df: pd.DataFrame) -> Dict[str, str]:
    """
    Generate rich JSON data files for the website dashboard.

    Produces multiple JSON files from the processed DataFrame, each targeting
    a specific dashboard component (daily trends, station rankings, heatmaps, etc.).

    Args:
        processed_df: Processed arrival data with delay calculations

    Returns:
        Dict mapping filename to output path
    """
    import json

    output_dir = os.path.join(STATIC_CONTENT_PATH, 'data')
    os.makedirs(output_dir, exist_ok=True)
    outputs = {}

    if processed_df.empty:
        print("WARNING: Empty DataFrame, generating empty dashboard data")
        return outputs

    # Deduplicate: one record per trip-stop-date
    df = processed_df.drop_duplicates(subset=['trip_id', 'stop_id', 'date']).copy()
    df['date'] = pd.to_datetime(df['date'])
    df['day_of_week'] = df['date'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['day_name'] = df['date'].dt.strftime('%A')
    df['month'] = df['date'].dt.to_period('M').astype(str)
    df['week_start'] = (df['date'] - pd.to_timedelta(df['day_of_week'], unit='D')).dt.strftime('%Y-%m-%d')

    total = len(df)
    on_time = len(df[df['delay_severity'] == 'On Time'])
    minor = len(df[df['delay_severity'] == 'Minor'])
    major = len(df[df['delay_severity'] == 'Major'])

    # --- 1. Enhanced stats.json ---
    last_7d = df[df['date'] >= df['date'].max() - pd.Timedelta(days=7)]
    last_30d = df[df['date'] >= df['date'].max() - pd.Timedelta(days=30)]

    stats = {
        'on_time_percentage': round(on_time / total * 100, 2) if total else 0,
        'minor_delay_percentage': round(minor / total * 100, 2) if total else 0,
        'major_delay_percentage': round(major / total * 100, 2) if total else 0,
        'total_arrivals': total,
        'avg_delay_minutes': round(float(df['delay_minutes'].mean()), 2),
        'median_delay_minutes': round(float(df['delay_minutes'].median()), 2),
        'last_updated': datetime.now().isoformat(),
        'date_range': {
            'start': df['date'].min().strftime('%Y-%m-%d'),
            'end': df['date'].max().strftime('%Y-%m-%d'),
        },
        'days_tracked': int((df['date'].max() - df['date'].min()).days) + 1,
        'rolling_7d_on_time': round(
            len(last_7d[last_7d['delay_severity'] == 'On Time']) / len(last_7d) * 100, 2
        ) if len(last_7d) > 0 else 0,
        'rolling_30d_on_time': round(
            len(last_30d[last_30d['delay_severity'] == 'On Time']) / len(last_30d) * 100, 2
        ) if len(last_30d) > 0 else 0,
    }

    path = os.path.join(output_dir, 'stats.json')
    with open(path, 'w') as f:
        json.dump(stats, f, indent=2)
    outputs['stats.json'] = path
    print(f"Generated stats.json ({total} arrivals)")

    # --- 2. daily_performance.json ---
    daily = df.groupby(df['date'].dt.strftime('%Y-%m-%d')).agg(
        total_trips=('trip_id', 'count'),
        on_time_count=('delay_severity', lambda x: (x == 'On Time').sum()),
        minor_count=('delay_severity', lambda x: (x == 'Minor').sum()),
        major_count=('delay_severity', lambda x: (x == 'Major').sum()),
        avg_delay_min=('delay_minutes', 'mean'),
    ).reset_index()
    daily.columns = ['date', 'total_trips', 'on_time_count', 'minor_count', 'major_count', 'avg_delay_min']
    daily['on_time_pct'] = round(daily['on_time_count'] / daily['total_trips'] * 100, 1)
    daily['minor_pct'] = round(daily['minor_count'] / daily['total_trips'] * 100, 1)
    daily['major_pct'] = round(daily['major_count'] / daily['total_trips'] * 100, 1)
    daily['avg_delay_min'] = round(daily['avg_delay_min'], 2)
    daily = daily.sort_values('date')

    path = os.path.join(output_dir, 'daily_performance.json')
    with open(path, 'w') as f:
        json.dump(daily.to_dict('records'), f)
    outputs['daily_performance.json'] = path
    print(f"Generated daily_performance.json ({len(daily)} days)")

    # --- 3. station_performance.json ---
    station = df.groupby(['stop_id', 'stop_name']).agg(
        total_arrivals=('trip_id', 'count'),
        on_time_count=('delay_severity', lambda x: (x == 'On Time').sum()),
        avg_delay_min=('delay_minutes', 'mean'),
        median_delay_min=('delay_minutes', 'median'),
    ).reset_index()
    station['on_time_pct'] = round(station['on_time_count'] / station['total_arrivals'] * 100, 1)
    station['avg_delay_min'] = round(station['avg_delay_min'], 2)
    station['median_delay_min'] = round(station['median_delay_min'], 2)
    # Add lat/lon from first occurrence if available
    if 'stop_lat' in df.columns and 'stop_lon' in df.columns:
        station_coords = df.drop_duplicates('stop_id')[['stop_id', 'stop_lat', 'stop_lon']]
        station = station.merge(station_coords, on='stop_id', how='left')
    else:
        station['stop_lat'] = None
        station['stop_lon'] = None
    station['stop_id'] = station['stop_id'].astype(int)
    station = station.sort_values('on_time_pct', ascending=False)

    path = os.path.join(output_dir, 'station_performance.json')
    with open(path, 'w') as f:
        json.dump(station.to_dict('records'), f)
    outputs['station_performance.json'] = path
    print(f"Generated station_performance.json ({len(station)} stations)")

    # --- 4. train_performance.json ---
    train = df.groupby('trip_id').agg(
        total_stops=('stop_id', 'count'),
        on_time_count=('delay_severity', lambda x: (x == 'On Time').sum()),
        avg_delay_min=('delay_minutes', 'mean'),
        days_observed=('date', 'nunique'),
    ).reset_index()
    train['on_time_pct'] = round(train['on_time_count'] / train['total_stops'] * 100, 1)
    train['avg_delay_min'] = round(train['avg_delay_min'], 2)
    train['trip_id'] = train['trip_id'].astype(int)
    train = train.sort_values('on_time_pct', ascending=False)

    path = os.path.join(output_dir, 'train_performance.json')
    with open(path, 'w') as f:
        json.dump(train.to_dict('records'), f)
    outputs['train_performance.json'] = path
    print(f"Generated train_performance.json ({len(train)} trains)")

    # --- 5. hourly_heatmap.json ---
    heatmap = df.groupby(['day_of_week', 'day_name', 'hour']).agg(
        total=('trip_id', 'count'),
        on_time_count=('delay_severity', lambda x: (x == 'On Time').sum()),
        avg_delay_min=('delay_minutes', 'mean'),
    ).reset_index()
    heatmap['on_time_pct'] = round(heatmap['on_time_count'] / heatmap['total'] * 100, 1)
    heatmap['avg_delay_min'] = round(heatmap['avg_delay_min'], 2)

    path = os.path.join(output_dir, 'hourly_heatmap.json')
    with open(path, 'w') as f:
        json.dump(heatmap.to_dict('records'), f)
    outputs['hourly_heatmap.json'] = path
    print(f"Generated hourly_heatmap.json ({len(heatmap)} cells)")

    # --- 6. commute_analysis.json ---
    commute = {}
    for period in df['commute_period'].unique():
        subset = df[df['commute_period'] == period]
        period_total = len(subset)
        commute[period] = {
            'total_trips': period_total,
            'on_time_pct': round(len(subset[subset['delay_severity'] == 'On Time']) / period_total * 100, 1) if period_total else 0,
            'minor_pct': round(len(subset[subset['delay_severity'] == 'Minor']) / period_total * 100, 1) if period_total else 0,
            'major_pct': round(len(subset[subset['delay_severity'] == 'Major']) / period_total * 100, 1) if period_total else 0,
            'avg_delay_min': round(float(subset['delay_minutes'].mean()), 2) if period_total else 0,
            'median_delay_min': round(float(subset['delay_minutes'].median()), 2) if period_total else 0,
        }

    path = os.path.join(output_dir, 'commute_analysis.json')
    with open(path, 'w') as f:
        json.dump(commute, f, indent=2)
    outputs['commute_analysis.json'] = path
    print(f"Generated commute_analysis.json ({len(commute)} periods)")

    # --- 7. weekly_summary.json ---
    weekly = df.groupby('week_start').agg(
        total_trips=('trip_id', 'count'),
        on_time_count=('delay_severity', lambda x: (x == 'On Time').sum()),
        avg_delay_min=('delay_minutes', 'mean'),
        days_with_data=('date', 'nunique'),
    ).reset_index()
    weekly['on_time_pct'] = round(weekly['on_time_count'] / weekly['total_trips'] * 100, 1)
    weekly['avg_delay_min'] = round(weekly['avg_delay_min'], 2)
    weekly = weekly.sort_values('week_start')

    path = os.path.join(output_dir, 'weekly_summary.json')
    with open(path, 'w') as f:
        json.dump(weekly.to_dict('records'), f)
    outputs['weekly_summary.json'] = path
    print(f"Generated weekly_summary.json ({len(weekly)} weeks)")

    # --- 8. monthly_summary.json ---
    monthly = df.groupby('month').agg(
        total_trips=('trip_id', 'count'),
        on_time_count=('delay_severity', lambda x: (x == 'On Time').sum()),
        avg_delay_min=('delay_minutes', 'mean'),
        days_with_data=('date', 'nunique'),
    ).reset_index()
    monthly['on_time_pct'] = round(monthly['on_time_count'] / monthly['total_trips'] * 100, 1)
    monthly['avg_delay_min'] = round(monthly['avg_delay_min'], 2)
    monthly = monthly.sort_values('month')

    path = os.path.join(output_dir, 'monthly_summary.json')
    with open(path, 'w') as f:
        json.dump(monthly.to_dict('records'), f)
    outputs['monthly_summary.json'] = path
    print(f"Generated monthly_summary.json ({len(monthly)} months)")

    print(f"Dashboard data generation complete: {len(outputs)} files")
    return outputs


@task
def detect_incidents(processed_df: pd.DataFrame, top_n: int = 25) -> Dict[str, Any]:
    """
    Detect delay incidents and build train trajectories for Marey diagrams.

    Stage 1: Identify candidate incident dates from processed arrivals.
    Stage 2: For each incident date, query raw GPS and project onto route.
    Stage 3: Classify tiers and build trajectory JSON.

    Args:
        processed_df: Processed arrival data with delay_minutes column
        top_n: Number of top incidents to keep

    Returns:
        dict with 'incidents' and 'trajectories' keys
    """
    import json as _json
    from src.utils.geo_utils import load_shape_points, project_to_route

    output_dir = os.path.join(STATIC_CONTENT_PATH, "data")
    os.makedirs(output_dir, exist_ok=True)

    if processed_df.empty or 'delay_minutes' not in processed_df.columns:
        print("WARNING: No processed data available for incident detection")
        # Write empty files
        for fname in ["incidents.json", "incident_trajectories.json"]:
            with open(os.path.join(output_dir, fname), "w") as f:
                _json.dump([] if fname == "incidents.json" else {}, f)
        return {"incidents": [], "trajectories": {}}

    # Load station metadata for labeling
    station_meta_path = os.path.join("data", "station_metadata.json")
    with open(station_meta_path) as f:
        station_meta = _json.load(f)["stations"]

    # Load shape polyline for GPS projection
    shape_points = load_shape_points("gtfs_data")
    max_route_dist = shape_points[-1][2]

    # Build station distance lookup
    station_distances = []
    for s in station_meta:
        d = project_to_route(s["lat"], s["lon"], shape_points)
        # Only include stations within the main route
        if d < max_route_dist * 0.98 or s["order"] <= 26:
            station_distances.append({
                "name": s["name"],
                "distance": round(d / 1000, 2),  # km
                "order": s["order"],
            })
    station_distances.sort(key=lambda x: x["distance"])

    # ── Stage 1: Find candidate incident dates ──
    print("Stage 1: Scanning for delay incidents...")
    df = processed_df.copy()
    # Deduplicate to one record per trip-stop-date
    df = df.drop_duplicates(subset=["trip_id", "stop_id", "date"])
    print(f"  {len(df)} unique trip-stop-date arrivals")
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Find trains with major delays (>= 15 min)
    major = df[df["delay_minutes"] >= 15].copy()
    if major.empty:
        print("No trains with delay >= 15 min found")
        for fname in ["incidents.json", "incident_trajectories.json"]:
            with open(os.path.join(output_dir, fname), "w") as f:
                _json.dump([] if fname == "incidents.json" else {}, f)
        return {"incidents": [], "trajectories": {}}

    # Group by date
    date_groups = major.groupby("date").agg(
        trains_affected=("trip_id", "nunique"),
        max_delay=("delay_minutes", "max"),
        avg_delay=("delay_minutes", "mean"),
    ).reset_index()

    # Also compute overall on-time rate per date
    daily_stats = df.groupby("date").agg(
        total=("trip_id", "count"),
        on_time=("delay_severity", lambda x: (x == "On Time").sum()),
    ).reset_index()
    daily_stats["on_time_pct"] = (daily_stats["on_time"] / daily_stats["total"] * 100).round(1)

    date_groups = date_groups.merge(daily_stats[["date", "on_time_pct", "total"]], on="date", how="left")

    # Classify tiers
    date_groups["tier"] = date_groups["trains_affected"].apply(lambda x: 2 if x >= 3 else 1)

    # Severity score
    date_groups["severity_score"] = (
        date_groups["avg_delay"] * date_groups["trains_affected"]
    ).round(0).astype(int)

    # Sort and take top N
    date_groups = date_groups.sort_values("severity_score", ascending=False).head(top_n)

    print(f"Found {len(date_groups)} incidents ({(date_groups['tier']==2).sum()} system, {(date_groups['tier']==1).sum()} train)")

    # ── Stage 2: Build trajectories for incident dates ──
    print("Stage 2: Building trajectories from raw GPS data...")
    incident_dates = date_groups["date"].tolist()

    # Query raw GPS for incident dates
    conn = sqlite3.connect(SQLITE_DB_PATH)
    incidents_list = []
    trajectories_dict = {}

    for idx, row in date_groups.iterrows():
        inc_date = row["date"]
        date_str = str(inc_date)

        # Query raw GPS for this date
        gps_df = pd.read_sql_query(
            "SELECT trip_id, vehicle_lat, vehicle_lon, timestamp FROM train_locations "
            "WHERE date(timestamp) = ?",
            conn,
            params=(date_str,),
        )

        if gps_df.empty:
            print(f"  No GPS data for {date_str}, skipping")
            continue

        gps_df["timestamp"] = pd.to_datetime(gps_df["timestamp"], errors="coerce")
        gps_df = gps_df.dropna(subset=["timestamp"])
        gps_df["trip_id"] = gps_df["trip_id"].astype(str)

        # Get the delayed trains for this date
        delayed_trains = set(
            major[major["date"] == inc_date]["trip_id"].astype(str).unique()
        )

        # Find root cause: earliest delayed train
        date_major = major[major["date"] == inc_date].copy()
        date_major["trip_id"] = date_major["trip_id"].astype(str)
        if "arrival_datetime" in date_major.columns:
            date_major["arrival_datetime"] = pd.to_datetime(date_major["arrival_datetime"], errors="coerce")
            earliest = date_major.sort_values("arrival_datetime").iloc[0]
        else:
            earliest = date_major.sort_values("delay_minutes", ascending=False).iloc[0]

        root_cause_train = str(earliest["trip_id"])
        root_cause_station = earliest.get("stop_name", "Unknown")
        if isinstance(root_cause_station, str):
            root_cause_station = (root_cause_station
                .replace(" Caltrain Station", "")
                .replace(" Northbound", "")
                .replace(" Southbound", ""))

        # Build incident ID
        if row["tier"] == 2:
            inc_id = f"{date_str}-system"
            summary = f"System Disruption — {row['trains_affected']} trains delayed"
        else:
            inc_id = f"{date_str}-train-{root_cause_train}"
            summary = f"Train {root_cause_train} delayed {int(row['max_delay'])} min at {root_cause_station}"

        # Project GPS points onto route for each train
        train_trajectories = []
        for trip_id, trip_gps in gps_df.groupby("trip_id"):
            trip_gps = trip_gps.sort_values("timestamp")

            # Skip trains with very few points
            if len(trip_gps) < 3:
                continue

            points = []
            for _, p in trip_gps.iterrows():
                d = project_to_route(p["vehicle_lat"], p["vehicle_lon"], shape_points)
                t = p["timestamp"].strftime("%H:%M:%S")
                points.append({"time": t, "distance": round(d / 1000, 2)})

            if not points:
                continue

            # Determine direction from distance trend
            dists = [p["distance"] for p in points]
            direction = "SB" if dists[-1] > dists[0] else "NB"

            # Check if this train is anomalous
            is_anomalous = str(trip_id) in delayed_trains

            # Check for cascading: delayed but not the root cause
            is_cascading = is_anomalous and str(trip_id) != root_cause_train

            # Get max delay for this train
            train_delays = date_major[date_major["trip_id"] == str(trip_id)]
            max_delay = float(train_delays["delay_minutes"].max()) if not train_delays.empty else 0

            # Downsample normal trains to every 5th point for smaller JSON
            if not is_anomalous and len(points) > 10:
                points = points[::5] + [points[-1]]

            train_trajectories.append({
                "trip_id": str(trip_id),
                "direction": direction,
                "points": points,
                "is_anomalous": is_anomalous,
                "is_cascading": is_cascading,
                "max_delay_min": round(max_delay, 1),
            })

        if not train_trajectories:
            print(f"  No valid trajectories for {date_str}, skipping")
            continue

        # Detect held station for root cause train
        held_station = root_cause_station
        for traj in train_trajectories:
            if traj["trip_id"] == root_cause_train and len(traj["points"]) >= 5:
                # Look for flat sections: 5+ points with < 0.3km movement
                pts_d = [p["distance"] for p in traj["points"]]
                for i in range(len(pts_d) - 4):
                    window = pts_d[i:i+5]
                    if max(window) - min(window) < 0.3:
                        # Found a flat section — find nearest station
                        center_dist = sum(window) / len(window)
                        nearest = min(station_distances, key=lambda s: abs(s["distance"] - center_dist))
                        if abs(nearest["distance"] - center_dist) < 0.5:
                            held_station = nearest["name"]
                        break

        incidents_list.append({
            "id": inc_id,
            "date": date_str,
            "tier": int(row["tier"]),
            "summary": summary,
            "trains_affected": int(row["trains_affected"]),
            "max_delay_min": round(float(row["max_delay"]), 1),
            "avg_delay_min": round(float(row["avg_delay"]), 1),
            "on_time_pct": float(row["on_time_pct"]),
            "severity_score": int(row["severity_score"]),
            "root_cause_train": root_cause_train,
            "root_cause_station": held_station,
        })

        trajectories_dict[inc_id] = {
            "incident_id": inc_id,
            "stations": station_distances,
            "trajectories": train_trajectories,
        }

        print(f"  {inc_id}: {len(train_trajectories)} trains, tier {int(row['tier'])}")

    conn.close()

    # Sort incidents by severity
    incidents_list.sort(key=lambda x: x["severity_score"], reverse=True)

    # Write output files
    with open(os.path.join(output_dir, "incidents.json"), "w") as f:
        _json.dump(incidents_list, f, indent=2)
    print(f"Generated incidents.json ({len(incidents_list)} incidents)")

    with open(os.path.join(output_dir, "incident_trajectories.json"), "w") as f:
        _json.dump(trajectories_dict, f)
    print(f"Generated incident_trajectories.json ({len(trajectories_dict)} incident details)")

    return {"incidents": incidents_list, "trajectories": trajectories_dict}


@flow(name="Process Train Data and Generate Visualizations",log_prints=True)
def process_data_flow():
    """
    Main flow to process train data and generate visualizations.
    """
    print("Starting train data processing flow")
    
    # Load data
    raw_df = load_raw_data()
    
    # Debug the raw data
    if raw_df.empty:
        print("ERROR: Raw data is empty. Check the SQLite database connection and data.")
        print(f"Database path: {SQLITE_DB_PATH}")
        print(f"Database exists: {os.path.exists(SQLITE_DB_PATH)}")
        
        # Create empty plots to avoid errors
        daily_stats_plot = generate_daily_stats_plot(pd.DataFrame())
        commute_delay_plot = generate_commute_delay_plot(pd.DataFrame())
        
        return {
            'plots': [daily_stats_plot, commute_delay_plot],
            'summary': {
                'error': 'No data available',
                'on_time_performance': 0,
                'total_trips': 0
            }
        }
    
    # Log raw data info
    print(f"Raw data loaded: {len(raw_df)} records")
    print(f"Raw data columns: {raw_df.columns.tolist()}")
    print(f"Raw data sample: {raw_df.head(1).to_dict('records')}")
    
    # Load GTFS data
    stops_df, stop_times_df = load_gtfs_data()
    
    if stops_df.empty or stop_times_df.empty:
        print("ERROR: GTFS data is empty. Check the CSV files in gtfs_data directory.")
        return {
            'error': 'GTFS data missing',
            'plots': [],
            'summary': {}
        }
    
    # Process data
    processed_df = process_arrival_data(raw_df, stops_df, stop_times_df)
    
    if processed_df.empty:
        print("ERROR: Failed to process arrival data. Check the data processing logic.")
        # Create empty plots to avoid errors
        daily_stats_plot = generate_daily_stats_plot(pd.DataFrame())
        commute_delay_plot = generate_commute_delay_plot(pd.DataFrame())
        
        return {
            'plots': [daily_stats_plot, commute_delay_plot],
            'summary': {
                'error': 'Processing failed',
                'on_time_performance': 0,
                'total_trips': 0
            }
        }
    
    # Log processed data info
    print(f"Processed data: {len(processed_df)} records")
    print(f"Processed data columns: {processed_df.columns.tolist()}")
    
    # Save processed data
    save_processed_data(processed_df)
    
    # Generate visualizations
    daily_stats_plot = generate_daily_stats_plot(processed_df)
    commute_delay_plot = generate_commute_delay_plot(processed_df)
    
    # Generate summary statistics
    summary_stats = generate_summary_stats(processed_df)

    # Generate rich dashboard data files
    dashboard_outputs = generate_dashboard_data(processed_df)

    # Detect incidents and build Marey diagram data
    incident_data = detect_incidents(processed_df)

    print("Train data processing flow completed successfully")
    return {
        'plots': [daily_stats_plot, commute_delay_plot],
        'summary': summary_stats,
        'dashboard_data': dashboard_outputs,
        'incident_data': incident_data
    }

if __name__ == "__main__":
    # Removed logging configuration as we're using print statements with Prefect
    process_data_flow.serve(name="process-data", cron="0 0 * * *")
