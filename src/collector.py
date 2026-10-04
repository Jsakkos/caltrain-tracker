"""
Poll 511's Caltrain vehicle feed once a minute and append positions to SQLite.

Stdlib only and no src.* imports, so the container is python:3.12-slim plus this file.
"""
import logging
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
