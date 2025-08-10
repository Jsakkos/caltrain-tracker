"""
Time-related utility functions.
"""
from datetime import datetime, timedelta, time
import pytz
from src.config import TIMEZONE

# Get the timezone
local_tz = pytz.timezone(TIMEZONE)

def normalize_time(t):
    """
    Normalize GTFS time format which can go beyond 24 hours.
    
    Args:
        t (str): Time string in HH:MM:SS format
        
    Returns:
        str: Normalized time string in HH:MM:SS format
    """
    try:
        hour = int(t.split(":")[0])
        if hour >= 24:
            new_hour = hour % 24
            return f"{new_hour:02d}{t[2:]}"
        return t
    except (ValueError, IndexError):
        return t

def calculate_time_difference(time1, time2):
    """
    Calculate time difference in minutes between two time objects.
    
    Args:
        time1 (str or datetime.time): First time (scheduled time)
        time2 (datetime.datetime): Second time (actual arrival time)
        
    Returns:
        float: Time difference in minutes
    """
    try:
        # Handle string format for time1 (scheduled time from GTFS)
        if isinstance(time1, str):
            # Parse the time string
            parts = time1.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2]) if len(parts) > 2 else 0
            
            # Get the date part from time2
            date_part = time2.date()
            
            # Create a base datetime with the date from time2
            base_dt = datetime.combine(date_part, datetime.min.time())
            
            # Add the hours, minutes, seconds as a timedelta
            # This handles times past midnight (e.g., 25:30:00)
            scheduled_dt = base_dt + timedelta(hours=hours, minutes=minutes, seconds=seconds)
            
            # Calculate difference in minutes
            diff = (time2 - scheduled_dt).total_seconds() / 60
            return diff
        
        # Handle time objects
        elif isinstance(time1, time):
            datetime1 = datetime.combine(datetime.today(), time1)
            
            # If time2 is a datetime, extract the time part
            if isinstance(time2, datetime):
                datetime2 = time2
            else:
                datetime2 = datetime.combine(datetime.today(), time2)
            
            # Handle cases where time2 is after midnight
            if datetime2 < datetime1:
                datetime2 += timedelta(days=1)
            
            return (datetime2 - datetime1).total_seconds() / 60
        
        else:
            # Fallback for unexpected types
            return 0
            
    except Exception as e:
        print(f"Error calculating time difference: {e}, time1={time1}, time2={time2}")
        return 0

def categorize_commute_time(timestamp):
    """
    Categorize a timestamp into commute periods.
    
    Args:
        timestamp (datetime): The timestamp to categorize
        
    Returns:
        str: One of 'Morning', 'Evening', 'Weekend', or 'Other'
    """
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

def utc_to_local(utc_dt):
    """
    Convert UTC datetime to local timezone.
    
    Args:
        utc_dt (datetime): UTC datetime with timezone info
        
    Returns:
        datetime: Local datetime without timezone info
    """
    local_dt = utc_dt.astimezone(local_tz)
    return local_dt.replace(tzinfo=None)

def parse_gtfs_timestamp(timestamp_str):
    """
    Parse a GTFS-RT timestamp string to a datetime object.
    
    Args:
        timestamp_str (str): ISO-format timestamp with timezone
        
    Returns:
        datetime: Local datetime object
    """
    utc_dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S%z')
    return utc_to_local(utc_dt)
