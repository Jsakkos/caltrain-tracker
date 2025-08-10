"""
Configuration module for the Caltrain Tracker application.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Get the absolute path to the project root directory
BASE_DIR = Path(__file__).resolve().parent.parent
dotenv_path = os.path.join(BASE_DIR, '.env')

# Print debug info
print(f"Base directory: {BASE_DIR}")
print(f"Dotenv path: {dotenv_path}")
print(f"Dotenv file exists: {os.path.exists(dotenv_path)}")

# Load environment variables
load_dotenv(dotenv_path)

# API Configuration
API_KEY = os.environ.get('API_KEY','afec7635-a79b-4ccb-b87b-5c8d9cf5b36c')
CALTRAIN_AGENCY_CODE = "CT"  # Caltrain operator ID
GTFS_RT_URL = f"https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency={CALTRAIN_AGENCY_CODE}"

# Database Configuration
# Primary database is now SQLite
SQLITE_DB_PATH = os.environ.get('DB_PATH', str(BASE_DIR / 'data' / 'caltrain_lat_long.db'))

# Keep PostgreSQL configuration for reference but it's not used
DB_USER = os.environ.get('DB_USER', 'postgres')
# PostgreSQL settings (not used anymore, kept for reference)
# DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')
# DB_HOST = os.environ.get('DB_HOST', 'localhost')
# DB_PORT = os.environ.get('DB_PORT', '5432')
# DB_NAME = os.environ.get('DB_NAME', 'caltrain_tracker')
# DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLite settings
DB_URI = f"sqlite:///{SQLITE_DB_PATH}"

# Application settings
DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
TIMEZONE = 'America/Los_Angeles'

# Paths
GTFS_DATA_PATH = os.environ.get('GTFS_PATH', str(BASE_DIR / 'gtfs_data'))
STATIC_CONTENT_PATH = str(BASE_DIR / 'static')

# Prefect settings
PREFECT_API_URL = os.environ.get('PREFECT_API_URL', 'http://127.0.0.1:4200')
# For Prefect 3
PREFECT_API_KEY = os.environ.get('PREFECT_API_KEY', '')
DATA_COLLECTION_INTERVAL = int(os.environ.get('DATA_COLLECTION_INTERVAL', '60'))  # in seconds
VISUALIZATION_UPDATE_INTERVAL = int(os.environ.get('VISUALIZATION_UPDATE_INTERVAL', '3600'))  # in seconds

# FastAPI settings
API_HOST = os.environ.get('API_HOST', '0.0.0.0')
API_PORT = int(os.environ.get('API_PORT', '8000'))
DASH_PORT = int(os.environ.get('DASH_PORT', '8050'))
