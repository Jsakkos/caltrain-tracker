"""
Test direct database access functions without using Prefect flows.
"""
import logging
import json
import requests
from datetime import datetime
from sqlalchemy import select

from src.db.database import SessionLocal
from src.models.train_data import TrainLocation
from src.config import GTFS_RT_URL
from src.utils.time_utils import parse_gtfs_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_vehicle_data():
    """
    Fetch vehicle monitoring data from the GTFS-RT API.
    """
    try:
        logger.info(f"Fetching data from {GTFS_RT_URL}")
        response = requests.get(GTFS_RT_URL)
        response.raise_for_status()
        
        # Decode the data using 'utf-8-sig' to handle potential BOM characters
        data = response.content
        data_str = data.decode('utf-8-sig')
        json_data = json.loads(data_str)
        
        logger.info(f"Fetched vehicle data successfully")
        return json_data
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        raise

def process_vehicle_data(data):
    """
    Process the raw JSON data from the GTFS-RT API.
    """
    try:
        vehicle_activities = data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']
        processed_data = []
        
        for activity in vehicle_activities:
            journey = activity['MonitoredVehicleJourney']
            trip_id = journey['VehicleRef']
            vehicle_lat = float(journey['VehicleLocation']['Latitude'])
            vehicle_lon = float(journey['VehicleLocation']['Longitude'])
            
            # Handle potentially missing MonitoredCall 
            stop_id = None
            if 'MonitoredCall' in journey:
                monitored_call = journey['MonitoredCall']
                stop_id = monitored_call.get('StopPointRef')
            else:
                # Use the first one from OnwardCalls if available
                if 'OnwardCalls' in journey and journey['OnwardCalls'] and 'OnwardCall' in journey['OnwardCalls']:
                    onward_calls = journey['OnwardCalls']['OnwardCall']
                    if onward_calls and len(onward_calls) > 0:
                        stop_id = onward_calls[0].get('StopPointRef')
            
            # If no stop_id was found, use a placeholder
            if not stop_id:
                stop_id = 'unknown'
                logger.warning(f"No stop_id found for vehicle {trip_id}")
                
            timestamp = activity['RecordedAtTime']
            
            # Parse and convert the timestamp to local time
            local_dt = parse_gtfs_timestamp(timestamp)
            
            processed_data.append((trip_id, stop_id, vehicle_lat, vehicle_lon, local_dt))
        
        logger.info(f"Processed {len(processed_data)} vehicle locations")
        return processed_data
    except KeyError as e:
        logger.error(f"Error processing data - missing key: {e}")
        raise

def save_train_locations(train_locations):
    """
    Save the processed train location data to the database.
    """
    db = SessionLocal()
    new_records = 0
    
    try:
        for loc_data in train_locations:
            trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp = loc_data
            
            # Check if the record already exists
            existing = db.query(TrainLocation).filter(
                TrainLocation.trip_id == trip_id,
                TrainLocation.stop_id == stop_id,
                TrainLocation.timestamp == timestamp
            ).first()
            
            if not existing:
                # Create a new record
                train_location = TrainLocation(
                    trip_id=trip_id,
                    stop_id=stop_id,
                    vehicle_lat=vehicle_lat,
                    vehicle_lon=vehicle_lon,
                    timestamp=timestamp
                )
                db.add(train_location)
                new_records += 1
        
        db.commit()
        logger.info(f"Inserted {new_records} new records")
        return new_records
    except Exception as e:
        db.rollback()
        logger.error(f"Error saving data: {e}")
        raise
    finally:
        db.close()

def test_data_collection():
    """
    Test the data collection process with SQLite database.
    """
    logger.info("Starting data collection test...")
    
    # Fetch data from API
    data = fetch_vehicle_data()
    
    # Process the data
    processed_data = process_vehicle_data(data)
    
    # Save to database
    new_records = save_train_locations(processed_data)
    
    logger.info(f"Data collection test completed. Added {new_records} new records.")
    return new_records

if __name__ == "__main__":
    test_data_collection()
