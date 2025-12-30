"""
Daily GTFS Schedule Update Checker.

Downloads the latest GTFS feed from 511.org and ingests it if it's a new version.
Run this script daily via cron/task scheduler to keep schedule data up to date.
"""
import argparse
import io
import logging
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import API_KEY, CALTRAIN_AGENCY_CODE, GTFS_DATA_PATH, BASE_DIR
from src.data.ingest_gtfs_schedule import ingest_gtfs_schedule, parse_feed_info, version_exists
from src.db.database import SessionLocal

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 511.org GTFS download URL
GTFS_DOWNLOAD_URL = f"https://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={CALTRAIN_AGENCY_CODE}"


def download_gtfs_feed(output_path: Path) -> bool:
    """
    Download the latest GTFS feed from 511.org.
    
    Args:
        output_path: Path to extract the GTFS files to
        
    Returns:
        True if download was successful, False otherwise
    """
    logger.info(f"Downloading GTFS feed from 511.org...")
    
    try:
        response = requests.get(GTFS_DOWNLOAD_URL, timeout=60)
        response.raise_for_status()
        
        # The response is a ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            zf.extractall(output_path)
        
        logger.info(f"Successfully downloaded and extracted GTFS feed to {output_path}")
        return True
        
    except requests.RequestException as e:
        logger.error(f"Failed to download GTFS feed: {e}")
        return False
    except zipfile.BadZipFile as e:
        logger.error(f"Downloaded file is not a valid ZIP: {e}")
        return False


def archive_old_gtfs(source_path: Path, archive_base: Path) -> Optional[Path]:
    """
    Move old GTFS data to the archive folder.
    
    Args:
        source_path: Path to the GTFS data to archive
        archive_base: Base path for archives
        
    Returns:
        Path to the archived folder, or None if archiving failed
    """
    if not source_path.exists():
        return None
    
    # Get the feed version for naming
    feed_info = parse_feed_info(source_path)
    if feed_info:
        version = feed_info.get('feed_version', 'unknown')
    else:
        version = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    archive_path = archive_base / f"gtfs_data_{version}"
    
    # Avoid overwriting existing archives
    if archive_path.exists():
        archive_path = archive_base / f"gtfs_data_{version}_{datetime.now().strftime('%H%M%S')}"
    
    try:
        shutil.move(str(source_path), str(archive_path))
        logger.info(f"Archived old GTFS data to {archive_path}")
        return archive_path
    except Exception as e:
        logger.warning(f"Failed to archive old GTFS data: {e}")
        return None


def check_and_update_gtfs(skip_download: bool = False, gtfs_path: Optional[str] = None) -> bool:
    """
    Check for new GTFS schedule and update if available.
    
    Args:
        skip_download: If True, skip downloading and use existing gtfs_path
        gtfs_path: Path to GTFS data (required if skip_download is True)
        
    Returns:
        True if update was performed or no update was needed, False on error
    """
    if skip_download:
        if not gtfs_path:
            logger.error("gtfs_path is required when skip_download is True")
            return False
        new_gtfs_path = Path(gtfs_path)
        cleanup_temp = False
    else:
        # Download to temp directory
        temp_dir = tempfile.mkdtemp(prefix='gtfs_')
        new_gtfs_path = Path(temp_dir)
        cleanup_temp = True
        
        if not download_gtfs_feed(new_gtfs_path):
            shutil.rmtree(temp_dir, ignore_errors=True)
            return False
    
    try:
        # Parse feed info from downloaded/provided data
        feed_info = parse_feed_info(new_gtfs_path)
        if not feed_info:
            logger.error("Could not parse feed_info.txt from new GTFS data")
            return False
        
        new_version = feed_info.get('feed_version', '')
        if not new_version:
            logger.error("No feed_version found in new GTFS data")
            return False
        
        logger.info(f"New GTFS feed version: {new_version}")
        
        # Check if this version already exists
        db = SessionLocal()
        try:
            if version_exists(db, new_version):
                logger.info(f"Version {new_version} already exists in database. No update needed.")
                return True
        finally:
            db.close()
        
        logger.info(f"New version {new_version} detected. Ingesting...")
        
        # Archive old GTFS data if it exists
        current_gtfs_path = Path(GTFS_DATA_PATH)
        archive_path = BASE_DIR / 'archive'
        archive_path.mkdir(exist_ok=True)
        
        if current_gtfs_path.exists() and not skip_download:
            archive_old_gtfs(current_gtfs_path, archive_path)
        
        # Ingest the new schedule
        if not ingest_gtfs_schedule(str(new_gtfs_path)):
            logger.error("Failed to ingest new GTFS schedule")
            return False
        
        # Move new GTFS data to the standard location (if downloaded)
        if not skip_download:
            if current_gtfs_path.exists():
                shutil.rmtree(current_gtfs_path, ignore_errors=True)
            shutil.move(str(new_gtfs_path), str(current_gtfs_path))
            cleanup_temp = False
            logger.info(f"Moved new GTFS data to {current_gtfs_path}")
        
        logger.info(f"Successfully updated to GTFS version {new_version}")
        return True
        
    finally:
        # Cleanup temp directory if needed
        if cleanup_temp and not skip_download:
            shutil.rmtree(str(new_gtfs_path), ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description='Check for and ingest new GTFS schedule updates'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip downloading and use --gtfs-path instead'
    )
    parser.add_argument(
        '--gtfs-path',
        type=str,
        help='Path to GTFS data folder (used with --skip-download)'
    )
    
    args = parser.parse_args()
    
    success = check_and_update_gtfs(
        skip_download=args.skip_download,
        gtfs_path=args.gtfs_path
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
