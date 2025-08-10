"""
Script to load GTFS static data into the database.
"""
import os
import logging
import pandas as pd
from sqlalchemy.orm import Session
from pathlib import Path

from src.db.database import SessionLocal, engine, Base
from src.models.train_data import Stop, Trip, StopTime
from src.config import GTFS_DATA_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def load_stops(db: Session, gtfs_path: str):
    """
    Load stops data from GTFS static feed into the database.
    
    Args:
        db (Session): SQLAlchemy database session
        gtfs_path (str): Path to GTFS static data directory
    """
    stops_file = os.path.join(gtfs_path, 'stops.txt')
    if not os.path.exists(stops_file):
        logger.error(f"Stops file not found at: {stops_file}")
        return 0
    
    logger.info(f"Loading stops data from: {stops_file}")
    
    # Read stops data from CSV
    stops_df = pd.read_csv(stops_file)
    stops_df = stops_df[stops_df['stop_id'].str.isnumeric()]
    
    # Insert stops into database
    added_count = 0
    for _, row in stops_df.iterrows():
        # Check if stop already exists
        existing_stop = db.query(Stop).filter(Stop.stop_id == str(row['stop_id'])).first()
        if not existing_stop:
            stop = Stop(
                stop_id=str(row['stop_id']),
                stop_name=row['stop_name'],
                stop_lat=float(row['stop_lat']),
                stop_lon=float(row['stop_lon']),
                parent_station=str(row['parent_station']) if pd.notna(row.get('parent_station')) else None
            )
            db.add(stop)
            added_count += 1
    
    db.commit()
    logger.info(f"Added {added_count} new stops to database")
    return added_count

def load_trips(db: Session, gtfs_path: str):
    """
    Load trips data from GTFS static feed into the database.
    
    Args:
        db (Session): SQLAlchemy database session
        gtfs_path (str): Path to GTFS static data directory
    """
    trips_file = os.path.join(gtfs_path, 'trips.txt')
    if not os.path.exists(trips_file):
        logger.error(f"Trips file not found at: {trips_file}")
        return 0
    
    logger.info(f"Loading trips data from: {trips_file}")
    
    # Read trips data from CSV
    trips_df = pd.read_csv(trips_file)
    
    # Insert trips into database
    added_count = 0
    for _, row in trips_df.iterrows():
        # Check if trip already exists
        existing_trip = db.query(Trip).filter(Trip.trip_id == str(row['trip_id'])).first()
        if not existing_trip:
            trip = Trip(
                trip_id=str(row['trip_id']),
                route_id=str(row['route_id']),
                service_id=str(row['service_id']),
                trip_headsign=row['trip_headsign'] if pd.notna(row.get('trip_headsign')) else None,
                direction_id=int(row['direction_id']) if pd.notna(row.get('direction_id')) else None
            )
            db.add(trip)
            added_count += 1
    
    db.commit()
    logger.info(f"Added {added_count} new trips to database")
    return added_count

def load_stop_times(db: Session, gtfs_path: str):
    """
    Load stop times data from GTFS static feed into the database.
    
    Args:
        db (Session): SQLAlchemy database session
        gtfs_path (str): Path to GTFS static data directory
    """
    stop_times_file = os.path.join(gtfs_path, 'stop_times.txt')
    if not os.path.exists(stop_times_file):
        logger.error(f"Stop times file not found at: {stop_times_file}")
        return 0
    
    logger.info(f"Loading stop times data from: {stop_times_file}")
    
    # Read stop times data from CSV
    stop_times_df = pd.read_csv(stop_times_file)
    
    # For efficiency, we'll batch the inserts
    batch_size = 1000
    total_rows = len(stop_times_df)
    added_count = 0
    
    for i in range(0, total_rows, batch_size):
        batch_df = stop_times_df.iloc[i:i + batch_size]
        
        for _, row in batch_df.iterrows():
            # Check if stop time already exists
            trip_id = str(row['trip_id'])
            stop_id = str(row['stop_id'])
            stop_sequence = int(row['stop_sequence'])
            
            existing_stop_time = db.query(StopTime).filter(
                StopTime.trip_id == trip_id,
                StopTime.stop_id == stop_id,
                StopTime.stop_sequence == stop_sequence
            ).first()
            
            if not existing_stop_time:
                stop_time = StopTime(
                    trip_id=trip_id,
                    stop_id=stop_id,
                    arrival_time=row['arrival_time'],
                    departure_time=row['departure_time'],
                    stop_sequence=stop_sequence
                )
                db.add(stop_time)
                added_count += 1
        
        # Commit the batch
        db.commit()
        logger.info(f"Processed {min(i + batch_size, total_rows)}/{total_rows} stop times")
    
    logger.info(f"Added {added_count} new stop times to database")
    return added_count

def load_all_gtfs_data():
    """
    Load all GTFS static data into the database.
    """
    logger.info("Starting GTFS data loading process")
    
    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)
    
    # Get the database session
    db = SessionLocal()
    
    try:
        # Load all GTFS data
        stops_count = load_stops(db, GTFS_DATA_PATH)
        trips_count = load_trips(db, GTFS_DATA_PATH)
        stop_times_count = load_stop_times(db, GTFS_DATA_PATH)
        
        logger.info(f"GTFS data loading completed: {stops_count} stops, {trips_count} trips, {stop_times_count} stop times")
    except Exception as e:
        db.rollback()
        logger.error(f"Error loading GTFS data: {e}", exc_info=True)
        raise
    finally:
        db.close()

if __name__ == "__main__":
    load_all_gtfs_data()
