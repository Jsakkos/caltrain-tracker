"""
Test SQLite database functionality directly without Prefect.
"""
import logging
import pandas as pd
from sqlalchemy import select
from datetime import datetime, timedelta

from src.db.database import SessionLocal
from src.models.train_data import TrainLocation, Stop, StopTime, ArrivalData

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_load_raw_data():
    """
    Load raw train location data from the SQLite database.
    """
    db = SessionLocal()
    try:
        # Query train locations
        query = select(TrainLocation).limit(10)
        result = db.execute(query)
        records = result.scalars().all()
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'id': record.id,
                'trip_id': record.trip_id,
                'stop_id': record.stop_id,
                'vehicle_lat': record.vehicle_lat,
                'vehicle_lon': record.vehicle_lon,
                'timestamp': record.timestamp
            }
            for record in records
        ])
        
        logger.info(f"Loaded {len(df)} raw train location records from database")
        logger.info(f"Sample data: {df.head()}")
        return df
    finally:
        db.close()

def test_database_tables():
    """
    Test if all required database tables exist and contain data.
    """
    db = SessionLocal()
    try:
        # Check TrainLocation table
        train_count = db.query(TrainLocation).count()
        logger.info(f"TrainLocation table has {train_count} records")
        
        # Check Stop table
        stop_count = db.query(Stop).count()
        logger.info(f"Stop table has {stop_count} records")
        
        # Check StopTime table
        stoptime_count = db.query(StopTime).count()
        logger.info(f"StopTime table has {stoptime_count} records")
        
        # Check ArrivalData table
        arrival_count = db.query(ArrivalData).count()
        logger.info(f"ArrivalData table has {arrival_count} records")
        
    finally:
        db.close()

if __name__ == "__main__":
    logger.info("Testing SQLite database connection...")
    test_database_tables()
    test_load_raw_data()
    logger.info("SQLite database test completed")
