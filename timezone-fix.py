import sqlite3
import pandas as pd
import pytz
from datetime import datetime

def fix_timezones():
    # Connect to database
    conn = sqlite3.connect('data/caltrain_lat_long.db')
    
    # Read existing data
    df = pd.read_sql_query("SELECT * FROM train_locations", conn)
    
    # Create backup table
    df.to_sql('train_locations_backup', conn, if_exists='replace', index=False)
    
    # Convert timestamps to proper timezone
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    pacific = pytz.timezone('America/Los_Angeles')
    
    def fix_timestamp(ts):
        # Assume timestamp was recorded in Pacific time
        naive = pd.Timestamp(ts)
        # Localize to Pacific time
        local_dt = pacific.localize(naive, is_dst=None)
        # Convert to UTC then back to local to handle DST correctly
        utc_dt = local_dt.astimezone(pytz.UTC)
        correct_local = utc_dt.astimezone(pacific)
        return correct_local.replace(tzinfo=None)
    
    # Apply timezone fix
    df['timestamp'] = df['timestamp'].apply(fix_timestamp)
    
    # Drop existing table and recreate with corrected data
    conn.execute("DROP TABLE train_locations")
    df.to_sql('train_locations', conn, if_exists='replace', index=False)
    
    # Create indexes for performance
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON train_locations(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trip ON train_locations(trip_id)")
    
    conn.close()

if __name__ == "__main__":
    fix_timezones()
