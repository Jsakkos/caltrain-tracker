"""
Configuration for scripts that call the 511 API (GTFS schedule updates).

The collector (src/collector.py) and the nightly build (src/pipeline) read their
settings themselves and don't import this module, because it requires API_KEY.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

API_KEY = os.environ.get('API_KEY')
if not API_KEY:
    raise RuntimeError(
        "API_KEY environment variable is not set. "
        "Copy .env.example to .env and add your 511.org API key."
    )
CALTRAIN_AGENCY_CODE = "CT"  # Caltrain operator ID

SQLITE_DB_PATH = os.environ.get('DB_PATH', str(BASE_DIR / 'data' / 'caltrain_lat_long.db'))
TIMEZONE = 'America/Los_Angeles'
