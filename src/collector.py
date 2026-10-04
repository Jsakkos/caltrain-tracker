"""
Poll 511's Caltrain vehicle feed once a minute and append positions to SQLite.

Stdlib only and no src.* imports, so the container is python:3.12-slim plus this file.
"""
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
import urllib.request
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

FEED_URL = "https://api.511.org/transit/VehicleMonitoring?api_key={key}&agency=CT"
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


def fetch(url: str, timeout: float = 30) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        # 511 prefixes its JSON with a UTF-8 byte-order mark.
        return json.loads(response.read().decode("utf-8-sig"))


def run_once(conn: sqlite3.Connection, get_payload: Callable[[], dict]) -> int:
    rows = parse_vehicles(get_payload())
    new = save(conn, rows)
    log.info("%d vehicles, %d new rows", len(rows), new)
    return new


def run_forever(conn: sqlite3.Connection, get_payload: Callable[[], dict],
                stop: threading.Event, interval: float = 60) -> None:
    # No retries within a cycle: 511 allows 60 requests/hour per key, so a
    # retry would push us over the limit. A failed minute is simply skipped.
    while not stop.is_set():
        started = time.monotonic()
        try:
            run_once(conn, get_payload)
        except Exception:
            log.exception("Collection failed; trying again next interval")
        stop.wait(max(0.0, interval - (time.monotonic() - started)))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    url = FEED_URL.format(key=os.environ["API_KEY"])  # never log the URL, it holds the key
    db_path = os.environ.get("DB_PATH", "data/caltrain_lat_long.db")
    interval = int(os.environ.get("COLLECTION_INTERVAL", "60"))
    conn = connect(db_path)

    if "--once" in sys.argv:
        run_once(conn, lambda: fetch(url))
        return

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    log.info("Collecting every %ds into %s", interval, db_path)
    run_forever(conn, lambda: fetch(url), stop, interval)
    conn.close()
    log.info("Stopped")


if __name__ == "__main__":
    main()
