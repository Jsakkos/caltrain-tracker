"""
Main application entry point for the Caltrain Tracker.
"""
import os
import logging
import argparse
import uvicorn
import threading
import time
from pathlib import Path

from datetime import timedelta
# No need to import get_client for our direct flow execution approach
from prefect import flow

from src.config import API_HOST, API_PORT, STATIC_CONTENT_PATH, DATA_COLLECTION_INTERVAL, PREFECT_API_URL
from src.db.database import engine, Base
from src.deployments.deploy_flows import deploy as create_deployments
from src.flows.data_collection import collect_train_data_flow
from src.flows.data_processing import process_data_flow
from src.data.gtfs_loader import load_all_gtfs_data
# Import the app from main.py instead of app.py
from src.api.main import app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def setup_database():
    """
    Set up the database by creating all tables.
    """
    try:
        logger.info("Setting up database")
        Base.metadata.create_all(bind=engine)
        logger.info("Database setup complete")
        return True
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False

def setup_static_content():
    """
    Set up static content directories.
    """
    logger.info("Setting up static content directories")
    static_dirs = [
        Path(STATIC_CONTENT_PATH) / "plots",
        Path(STATIC_CONTENT_PATH) / "data",
    ]
    
    for directory in static_dirs:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Caltrain Tracker Application")
    parser.add_argument("--load-gtfs", action="store_true", help="Load GTFS static data into the database")
    parser.add_argument("--prefect-only", action="store_true", help="Run only the Prefect flows without the API server")
    parser.add_argument("--host", type=str, default=API_HOST, help="Host to run the API server on")
    parser.add_argument("--port", type=int, default=API_PORT, help="Port to run the API server on")
    
    return parser.parse_args()

def main():
    """
    Main application entry point.
    """
    logger.info("Starting Caltrain Tracker application")
    
    # Parse command line arguments
    args = parse_arguments()
    
    try:
        # Load GTFS static data if requested
        if args.load_gtfs:
            logger.info("Loading GTFS static data")
            try:
                load_all_gtfs_data()
            except Exception as e:
                logger.error(f"Error loading GTFS data: {e}", exc_info=True)
        
        # Start the API server if not in Prefect-only mode
        if not args.prefect_only:
            logger.info(f"Starting API server at http://{args.host}:{args.port}")
            uvicorn.run(
                "src.api.main:app",  # This is correct, pointing to the main.py app
                host=args.host,
                port=args.port,
                reload=False,  # Set to False in production
                log_level="info"
            )
        else:
            logger.info("Running in Prefect-only mode, API server not started")
            # Keep the main thread alive
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Error in main application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    setup_database()
    setup_static_content()
    # start_prefect_flows()
    # Use the main function which will properly run the app from src.api.main
    main()
