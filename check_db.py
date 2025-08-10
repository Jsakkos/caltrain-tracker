"""
Check the SQLite database contents.
"""
import sqlite3
import os
from pathlib import Path

# Get the database path
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.path.join(BASE_DIR, 'data', 'caltrain_lat_long.db')

# Connect to the database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Check if the train_locations table exists and count records
try:
    cursor.execute("SELECT COUNT(*) FROM train_locations")
    count = cursor.fetchone()[0]
    print(f"Found {count} records in train_locations table")
    
    # Get a sample of records
    if count > 0:
        cursor.execute("SELECT * FROM train_locations LIMIT 5")
        columns = [desc[0] for desc in cursor.description]
        print(f"Columns: {columns}")
        
        rows = cursor.fetchall()
        for row in rows:
            print(row)
except sqlite3.OperationalError as e:
    print(f"Error: {e}")

# Close the connection
conn.close()
