"""
Prefect flows for data collection from GTFS realtime API.
"""
import json
# Removed logging import as we're using print statements with Prefect
from datetime import datetime, timedelta
from prefect import task, flow
from sqlalchemy.orm import Session
import requests
from typing import List, Dict, Any, Tuple

from src.config import GTFS_RT_URL, DATA_COLLECTION_INTERVAL
from src.db.database import SessionLocal
from src.models.train_data import TrainLocation
from src.utils.time_utils import parse_gtfs_timestamp

# Removed logger initialization as we're using print statements with Prefect

@task(retries=3, retry_delay_seconds=5)
def fetch_vehicle_data() -> Dict[str, Any]:
    """
    Fetch vehicle monitoring data from the GTFS-RT API.
    
    Returns:
        Dict[str, Any]: The parsed JSON response
    """
    try:
        print(f"Fetching data from {GTFS_RT_URL}")
        response = requests.get(GTFS_RT_URL)
        response.raise_for_status()
        
        # Decode the data using 'utf-8-sig' to handle potential BOM characters
        data = response.content
        data_str = data.decode('utf-8-sig')
        json_data = json.loads(data_str)
        
        # Log the first portion of the response to help debug the structure
        try:
            print(f"Response structure sample: {json.dumps(json_data)[:500]}...")
        except Exception as log_err:
            print(f"WARNING: Could not log response structure: {log_err}")
        
        print(f"Fetched vehicle data with {len(json_data.get('Siri', {}).get('ServiceDelivery', {}).get('VehicleMonitoringDelivery', {}).get('VehicleActivity', []))} vehicles")
        
        return json_data
    except requests.RequestException as e:
        print(f"ERROR: Error fetching data: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"ERROR: Error parsing JSON: {e}")
        raise

@task
def process_vehicle_data(data: Dict[str, Any]) -> List[Tuple]:
    """
    Process the raw JSON data from the GTFS-RT API.
    
    Args:
        data (Dict[str, Any]): The raw JSON data
        
    Returns:
        List[Tuple]: A list of tuples containing processed train location data
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
                print(f"WARNING: No stop_id found for vehicle {trip_id}")
                
            timestamp = activity['RecordedAtTime']
            
            # Parse and convert the timestamp to local time
            local_dt = parse_gtfs_timestamp(timestamp)
            
            processed_data.append((trip_id, stop_id, vehicle_lat, vehicle_lon, local_dt))
        
        print(f"Processed {len(processed_data)} vehicle locations")
        return processed_data
    except KeyError as e:
        print(f"ERROR: Error processing data - missing key: {e}")
        raise

@task
def save_train_locations(train_locations: List[Tuple]) -> int:
    """
    Save the processed train location data to the database.
    
    Args:
        train_locations (List[Tuple]): A list of tuples containing train location data
        
    Returns:
        int: The number of new records inserted
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
        print(f"Inserted {new_records} new records")
        return new_records
    except Exception as e:
        db.rollback()
        print(f"ERROR: Error saving data: {e}")
        raise
    finally:
        db.close()

@flow(name="Collect Train Location Data",log_prints=True)
def collect_train_data_flow():
    """
    Main flow to collect train location data from the GTFS-RT API.
    """
    print("Starting train data collection flow")
    data = fetch_vehicle_data()
    processed_data = process_vehicle_data(data)
    new_records = save_train_locations(processed_data)
    print(f"Train data collection flow completed with {new_records} new records")
    return new_records

if __name__ == "__main__":
    # Removed logging configuration as we're using print statements with Prefect
    collect_train_data_flow.serve(name="train-data-collection", cron="* * * * *")
