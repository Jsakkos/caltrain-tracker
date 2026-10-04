"""Tiny GTFS feeds and train_locations databases for pipeline tests."""
import csv
from pathlib import Path

from src.collector import connect

# Two real Caltrain platforms, about 2.1 km apart.
SF = ("70011", "San Francisco Caltrain", 37.7764, -122.3943)
TWENTY_SECOND = ("70021", "22nd Street Caltrain", 37.7577, -122.3924)

# (train number, stop_id, arrival_time)
DEFAULT_CALLS = [
    ("101", SF[0], "08:00:00"),
    ("101", TWENTY_SECOND[0], "08:05:00"),
    ("103", SF[0], "24:10:00"),
]


def _write(folder: Path, name: str, header: list[str], rows: list[tuple]) -> None:
    with open(folder / f"{name}.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def make_feed(root: Path, name: str = "v1", version: str = "1", start: str = "20260101",
              end: str = "20261231", calls=DEFAULT_CALLS) -> Path:
    """A one-service GTFS feed (every day) under root/name."""
    folder = root / name
    folder.mkdir(parents=True)
    _write(folder, "feed_info", ["feed_version", "feed_start_date", "feed_end_date"], [(version, start, end)])
    _write(folder, "stops", ["stop_id", "stop_name", "parent_station", "stop_lat", "stop_lon"],
           [(s[0], s[1], "", s[2], s[3]) for s in (SF, TWENTY_SECOND)])
    _write(folder, "calendar",
           ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
            "start_date", "end_date"],
           [("all", 1, 1, 1, 1, 1, 1, 1, start, end)])
    trains = sorted({c[0] for c in calls})
    _write(folder, "trips", ["trip_id", "service_id", "trip_short_name"], [(f"t{t}", "all", t) for t in trains])
    _write(folder, "stop_times", ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
           [(f"t{t}", s, i, a, a) for i, (t, s, a) in enumerate(calls, 1)])
    return folder


def make_db(path: Path):
    return connect(str(path))


def add_pings(conn, *pings) -> None:
    """pings: (trip, stop, lat, lon, 'YYYY-MM-DD HH:MM:SS') tuples, stored like the collector stores them."""
    rows = [(t, s, lat, lon, f"{ts}.000000") for t, s, lat, lon, ts in pings]
    with conn:
        conn.executemany(
            "INSERT INTO train_locations (trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
