"""
Poll 511's Caltrain vehicle feed once a minute and append positions to SQLite.

Stdlib only and no src.* imports, so the container is python:3.12-slim plus this file.
"""
import logging
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/Los_Angeles")
# The text format SQLAlchemy wrote for existing rows. The unique index on
# (trip_id, stop_id, timestamp) compares text, so this must not change.
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

log = logging.getLogger("collector")


def to_local_text(recorded_at: str) -> str:
    """511's ISO timestamp as Pacific wall time in the stored text format."""
    return datetime.fromisoformat(recorded_at).astimezone(LOCAL_TZ).strftime(TIMESTAMP_FORMAT)


def _next_stop(journey: dict) -> str:
    stop = (journey.get("MonitoredCall") or {}).get("StopPointRef")
    if not stop:
        onward = (journey.get("OnwardCalls") or {}).get("OnwardCall") or []
        stop = onward[0].get("StopPointRef") if onward else None
    return stop or "unknown"


def parse_vehicles(payload: dict) -> list[tuple]:
    """(trip_id, stop_id, lat, lon, timestamp) for each vehicle in a SIRI VehicleMonitoring payload."""
    delivery = payload["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"]
    rows = []
    for activity in delivery.get("VehicleActivity") or []:
        try:
            journey = activity["MonitoredVehicleJourney"]
            location = journey["VehicleLocation"]
            rows.append((
                journey["VehicleRef"],
                _next_stop(journey),
                float(location["Latitude"]),
                float(location["Longitude"]),
                to_local_text(activity["RecordedAtTime"]),
            ))
        except (KeyError, TypeError, ValueError) as e:
            log.warning("Skipping malformed vehicle activity: %r", e)
    return rows


# Matches the table SQLAlchemy created, so a fresh database (tests, a new
# deployment) looks the same as the production one.
SCHEMA = """
CREATE TABLE IF NOT EXISTS train_locations (
    id INTEGER NOT NULL,
    trip_id VARCHAR,
    stop_id VARCHAR,
    vehicle_lat FLOAT,
    vehicle_lon FLOAT,
    timestamp DATETIME,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trip_stop_timestamp ON train_locations (trip_id, stop_id, timestamp);
"""

INSERT = (
    "INSERT OR IGNORE INTO train_locations (trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp) "
    "VALUES (?, ?, ?, ?, ?)"
)


def connect(db_path: str) -> sqlite3.Connection:
    # A long busy timeout: the nightly build and the NAS backup read the same file.
    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA)
    return conn


def save(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    """Insert rows, skipping ones already stored. Returns the number of new rows."""
    before = conn.total_changes
    with conn:
        conn.executemany(INSERT, rows)
    return conn.total_changes - before
