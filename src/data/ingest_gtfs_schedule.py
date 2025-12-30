"""
GTFS Schedule Ingestion Script.

Loads GTFS static schedule data from a folder into the database with version tracking.
"""
import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.database import SessionLocal, engine, Base
from src.models.gtfs_schedule import (
    GtfsScheduleVersion,
    GtfsAgency,
    GtfsStop,
    GtfsRoute,
    GtfsTrip,
    GtfsStopTime,
    GtfsCalendar,
    GtfsCalendarDate,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_csv_file(file_path: Path) -> List[Dict[str, Any]]:
    """Read a GTFS CSV file and return list of dictionaries."""
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return []
    
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        return list(reader)


def parse_feed_info(gtfs_path: Path) -> Optional[Dict[str, str]]:
    """Parse feed_info.txt to extract version metadata."""
    feed_info_path = gtfs_path / 'feed_info.txt'
    
    if not feed_info_path.exists():
        logger.error(f"feed_info.txt not found in {gtfs_path}")
        return None
    
    rows = read_csv_file(feed_info_path)
    if not rows:
        logger.error("feed_info.txt is empty")
        return None
    
    return rows[0]


def version_exists(db: Session, feed_version: str) -> bool:
    """Check if a schedule version already exists in the database."""
    existing = db.query(GtfsScheduleVersion).filter_by(feed_version=feed_version).first()
    return existing is not None


def deactivate_old_versions(db: Session):
    """Mark all existing versions as inactive."""
    db.query(GtfsScheduleVersion).update({'is_active': False})


def create_schedule_version(db: Session, feed_info: Dict[str, str]) -> GtfsScheduleVersion:
    """Create a new schedule version record."""
    version = GtfsScheduleVersion(
        feed_version=feed_info.get('feed_version', ''),
        feed_start_date=feed_info.get('feed_start_date'),
        feed_end_date=feed_info.get('feed_end_date'),
        feed_publisher_name=feed_info.get('feed_publisher_name'),
        feed_publisher_url=feed_info.get('feed_publisher_url'),
        feed_lang=feed_info.get('feed_lang'),
        is_active=True,
    )
    db.add(version)
    db.flush()  # Get the ID
    return version


def load_agencies(db: Session, gtfs_path: Path, version_id: int) -> int:
    """Load agency.txt data."""
    rows = read_csv_file(gtfs_path / 'agency.txt')
    count = 0
    for row in rows:
        agency = GtfsAgency(
            schedule_version_id=version_id,
            agency_id=row.get('agency_id', ''),
            agency_name=row.get('agency_name', ''),
            agency_url=row.get('agency_url'),
            agency_timezone=row.get('agency_timezone'),
            agency_lang=row.get('agency_lang'),
            agency_phone=row.get('agency_phone'),
        )
        db.add(agency)
        count += 1
    return count


def load_stops(db: Session, gtfs_path: Path, version_id: int) -> int:
    """Load stops.txt data."""
    rows = read_csv_file(gtfs_path / 'stops.txt')
    count = 0
    for row in rows:
        stop = GtfsStop(
            schedule_version_id=version_id,
            stop_id=row.get('stop_id', ''),
            stop_code=row.get('stop_code'),
            stop_name=row.get('stop_name', ''),
            stop_lat=float(row['stop_lat']) if row.get('stop_lat') else None,
            stop_lon=float(row['stop_lon']) if row.get('stop_lon') else None,
            zone_id=row.get('zone_id'),
            stop_url=row.get('stop_url'),
            location_type=int(row['location_type']) if row.get('location_type') else None,
            parent_station=row.get('parent_station') or None,
            stop_timezone=row.get('stop_timezone'),
            wheelchair_boarding=int(row['wheelchair_boarding']) if row.get('wheelchair_boarding') else None,
            platform_code=row.get('platform_code'),
        )
        db.add(stop)
        count += 1
    return count


def load_routes(db: Session, gtfs_path: Path, version_id: int) -> int:
    """Load routes.txt data."""
    rows = read_csv_file(gtfs_path / 'routes.txt')
    count = 0
    for row in rows:
        route = GtfsRoute(
            schedule_version_id=version_id,
            route_id=row.get('route_id', ''),
            agency_id=row.get('agency_id'),
            route_short_name=row.get('route_short_name'),
            route_long_name=row.get('route_long_name'),
            route_desc=row.get('route_desc'),
            route_type=int(row['route_type']) if row.get('route_type') else None,
            route_url=row.get('route_url'),
            route_color=row.get('route_color'),
            route_text_color=row.get('route_text_color'),
        )
        db.add(route)
        count += 1
    return count


def load_trips(db: Session, gtfs_path: Path, version_id: int) -> int:
    """Load trips.txt data."""
    rows = read_csv_file(gtfs_path / 'trips.txt')
    count = 0
    for row in rows:
        trip = GtfsTrip(
            schedule_version_id=version_id,
            trip_id=row.get('trip_id', ''),
            route_id=row.get('route_id', ''),
            service_id=row.get('service_id', ''),
            trip_headsign=row.get('trip_headsign'),
            trip_short_name=row.get('trip_short_name'),
            direction_id=int(row['direction_id']) if row.get('direction_id') else None,
            block_id=row.get('block_id'),
            shape_id=row.get('shape_id'),
            wheelchair_accessible=int(row['wheelchair_accessible']) if row.get('wheelchair_accessible') else None,
            bikes_allowed=int(row['bikes_allowed']) if row.get('bikes_allowed') else None,
        )
        db.add(trip)
        count += 1
    return count


def load_stop_times(db: Session, gtfs_path: Path, version_id: int) -> int:
    """Load stop_times.txt data."""
    rows = read_csv_file(gtfs_path / 'stop_times.txt')
    count = 0
    batch_size = 1000
    
    for i, row in enumerate(rows):
        stop_time = GtfsStopTime(
            schedule_version_id=version_id,
            trip_id=row.get('trip_id', ''),
            stop_id=row.get('stop_id', ''),
            arrival_time=row.get('arrival_time'),
            departure_time=row.get('departure_time'),
            stop_sequence=int(row.get('stop_sequence', 0)),
            stop_headsign=row.get('stop_headsign'),
            pickup_type=int(row['pickup_type']) if row.get('pickup_type') else None,
            drop_off_type=int(row['drop_off_type']) if row.get('drop_off_type') else None,
            shape_dist_traveled=float(row['shape_dist_traveled']) if row.get('shape_dist_traveled') else None,
            timepoint=int(row['timepoint']) if row.get('timepoint') else None,
        )
        db.add(stop_time)
        count += 1
        
        # Flush in batches to avoid memory issues
        if (i + 1) % batch_size == 0:
            db.flush()
            logger.info(f"  Loaded {i + 1} stop times...")
    
    return count


def load_calendars(db: Session, gtfs_path: Path, version_id: int) -> int:
    """Load calendar.txt data."""
    rows = read_csv_file(gtfs_path / 'calendar.txt')
    count = 0
    for row in rows:
        calendar = GtfsCalendar(
            schedule_version_id=version_id,
            service_id=row.get('service_id', ''),
            monday=int(row.get('monday', 0)),
            tuesday=int(row.get('tuesday', 0)),
            wednesday=int(row.get('wednesday', 0)),
            thursday=int(row.get('thursday', 0)),
            friday=int(row.get('friday', 0)),
            saturday=int(row.get('saturday', 0)),
            sunday=int(row.get('sunday', 0)),
            start_date=row.get('start_date', ''),
            end_date=row.get('end_date', ''),
        )
        db.add(calendar)
        count += 1
    return count


def load_calendar_dates(db: Session, gtfs_path: Path, version_id: int) -> int:
    """Load calendar_dates.txt data."""
    rows = read_csv_file(gtfs_path / 'calendar_dates.txt')
    count = 0
    for row in rows:
        calendar_date = GtfsCalendarDate(
            schedule_version_id=version_id,
            service_id=row.get('service_id', ''),
            date=row.get('date', ''),
            exception_type=int(row.get('exception_type', 1)),
        )
        db.add(calendar_date)
        count += 1
    return count


def ingest_gtfs_schedule(gtfs_path: str, force: bool = False) -> bool:
    """
    Ingest GTFS schedule data from a folder into the database.
    
    Args:
        gtfs_path: Path to the GTFS data folder
        force: If True, re-ingest even if version exists
        
    Returns:
        True if ingestion was successful, False otherwise
    """
    gtfs_path = Path(gtfs_path)
    
    if not gtfs_path.exists():
        logger.error(f"GTFS path does not exist: {gtfs_path}")
        return False
    
    # Parse feed info
    feed_info = parse_feed_info(gtfs_path)
    if not feed_info:
        return False
    
    feed_version = feed_info.get('feed_version', '')
    if not feed_version:
        logger.error("No feed_version found in feed_info.txt")
        return False
    
    logger.info(f"Found GTFS feed version: {feed_version}")
    
    # Create database tables if they don't exist
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    try:
        # Check if version already exists
        if version_exists(db, feed_version):
            if not force:
                logger.info(f"Version {feed_version} already exists in database. Skipping ingestion.")
                logger.info("Use --force to re-ingest this version.")
                return True
            else:
                logger.info(f"Force mode: Deleting existing version {feed_version}")
                existing = db.query(GtfsScheduleVersion).filter_by(feed_version=feed_version).first()
                db.delete(existing)
                db.flush()
        
        # Deactivate old versions and create new version record
        deactivate_old_versions(db)
        version = create_schedule_version(db, feed_info)
        version_id = version.id
        logger.info(f"Created schedule version record with ID: {version_id}")
        
        # Load all GTFS files
        logger.info("Loading agencies...")
        agency_count = load_agencies(db, gtfs_path, version_id)
        logger.info(f"  Loaded {agency_count} agencies")
        
        logger.info("Loading stops...")
        stop_count = load_stops(db, gtfs_path, version_id)
        logger.info(f"  Loaded {stop_count} stops")
        
        logger.info("Loading routes...")
        route_count = load_routes(db, gtfs_path, version_id)
        logger.info(f"  Loaded {route_count} routes")
        
        logger.info("Loading trips...")
        trip_count = load_trips(db, gtfs_path, version_id)
        logger.info(f"  Loaded {trip_count} trips")
        
        logger.info("Loading stop times...")
        stop_time_count = load_stop_times(db, gtfs_path, version_id)
        logger.info(f"  Loaded {stop_time_count} stop times")
        
        logger.info("Loading calendars...")
        calendar_count = load_calendars(db, gtfs_path, version_id)
        logger.info(f"  Loaded {calendar_count} calendars")
        
        logger.info("Loading calendar dates...")
        calendar_date_count = load_calendar_dates(db, gtfs_path, version_id)
        logger.info(f"  Loaded {calendar_date_count} calendar dates")
        
        # Commit all changes
        db.commit()
        logger.info(f"Successfully ingested GTFS version {feed_version}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error ingesting GTFS data: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description='Ingest GTFS schedule data into the database')
    parser.add_argument(
        '--gtfs-path',
        type=str,
        required=True,
        help='Path to the GTFS data folder'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-ingestion even if version already exists'
    )
    
    args = parser.parse_args()
    
    success = ingest_gtfs_schedule(args.gtfs_path, args.force)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
