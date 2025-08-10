"""
Script to migrate data from SQLite to PostgreSQL.
"""
import os
import logging
import pandas as pd
import sqlite3
from datetime import datetime
from sqlalchemy.orm import Session
import time

from src.db.database import SessionLocal, engine, Base
from src.models.train_data import TrainLocation, ArrivalData
from src.config import SQLITE_DB_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def get_sqlite_connection():
    """
    Get a connection to the SQLite database.
    
    Returns:
        sqlite3.Connection: A connection to the SQLite database
    """
    if not os.path.exists(SQLITE_DB_PATH):
        logger.error(f"SQLite database not found at: {SQLITE_DB_PATH}")
        raise FileNotFoundError(f"SQLite database not found at: {SQLITE_DB_PATH}")
    
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def migrate_train_locations():
    """
    Migrate train location data from SQLite to PostgreSQL.
    
    Returns:
        int: Number of records migrated
    """
    logger.info("Starting migration of train location data")
    
    # Get SQLite connection
    sqlite_conn = get_sqlite_connection()
    
    # Get all train location records from SQLite
    query = "SELECT * FROM train_locations"
    df = pd.read_sql_query(query, sqlite_conn)
    
    # Close SQLite connection
    sqlite_conn.close()
    
    # Get PostgreSQL session
    db = SessionLocal()
    
    try:
        # Migrate records batch by batch
        batch_size = 1000
        total_rows = len(df)
        migrated_count = 0
        
        start_time = time.time()
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i + batch_size]
            
            for _, row in batch_df.iterrows():
                # Check if record already exists in PostgreSQL
                existing = db.query(TrainLocation).filter(
                    TrainLocation.trip_id == str(row['trip_id']),
                    TrainLocation.stop_id == str(row['stop_id']),
                    TrainLocation.timestamp == pd.to_datetime(row['timestamp'])
                ).first()
                
                if not existing:
                    # Create new record
                    train_location = TrainLocation(
                        trip_id=str(row['trip_id']),
                        stop_id=str(row['stop_id']),
                        vehicle_lat=float(row['vehicle_lat']),
                        vehicle_lon=float(row['vehicle_lon']),
                        timestamp=pd.to_datetime(row['timestamp'])
                    )
                    db.add(train_location)
                    migrated_count += 1
            
            # Commit the batch
            db.commit()
            
            elapsed_time = time.time() - start_time
            records_per_second = (i + len(batch_df)) / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(
                f"Processed {min(i + batch_size, total_rows)}/{total_rows} train locations "
                f"({migrated_count} migrated, {records_per_second:.2f} records/sec)"
            )
        
        logger.info(f"Migration complete: {migrated_count} train locations migrated")
        return migrated_count
    except Exception as e:
        db.rollback()
        logger.error(f"Error migrating train locations: {e}", exc_info=True)
        raise
    finally:
        db.close()

def migrate_data():
    """
    Migrate all data from SQLite to PostgreSQL.
    """
    logger.info("Starting data migration from SQLite to PostgreSQL")
    
    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)
    
    # Migrate train locations
    train_locations_count = migrate_train_locations()
    
    logger.info(f"Data migration completed: {train_locations_count} train locations migrated")

if __name__ == "__main__":
    migrate_data()
