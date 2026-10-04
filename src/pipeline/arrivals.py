"""
Arrivals: each train's closest approach to each scheduled stop, one row per service day.

Reads train_locations straight from the collector's SQLite file with DuckDB and
keeps the results in a DuckDB file, recomputing only days that are new, recent,
or whose schedule version changed. The scoring rules match the legacy pandas
flow (src/flows/data_processing.py) except for calls after midnight; see
docs/superpowers/plans/2026-10-03-duckdb-build.md for the list.
"""
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from src.data.gtfs_feeds import FEEDS_DIR, discover_feeds, feed_for_date, load_stops, schedule_for_dates

AFTER_MIDNIGHT_HOURS = 4

SCHEMA = """
CREATE TABLE IF NOT EXISTS arrivals (
    date DATE,
    trip_id VARCHAR,
    stop_id VARCHAR,
    stop_name VARCHAR,
    parent_station VARCHAR,
    arrival_time VARCHAR,
    actual_arrival_time TIMESTAMP,
    delay_minutes DOUBLE,
    is_delayed BOOLEAN,
    delay_severity VARCHAR,
    commute_period VARCHAR,
    hour INTEGER,
    ping_count INTEGER,
    feed_version VARCHAR
);
CREATE TABLE IF NOT EXISTS processed_days (
    date DATE PRIMARY KEY,
    feed_version VARCHAR
);
"""

# One row per (trip, stop, service day): the ping nearest the platform. Ties go
# to the first stored row, like the legacy groupby().first().
#
# A GTFS service day runs past midnight (24:10:00 on D is 00:10 on D+1), so a
# ping can belong to its own calendar day's trips or the previous day's. Each
# ping goes to whichever of the two puts the scheduled call closest in time.
ARRIVALS_SQL = """
WITH sched AS (
    SELECT date AS service_date, trip_id, stop_id, arrival_time, feed_version,
           CAST(date AS TIMESTAMP) + to_seconds(
               CAST(split_part(arrival_time, ':', 1) AS INTEGER) * 3600
               + CAST(split_part(arrival_time, ':', 2) AS INTEGER) * 60
               + COALESCE(TRY_CAST(split_part(arrival_time, ':', 3) AS INTEGER), 0)) AS sched_ts
    FROM schedule
),
assigned AS (
    SELECT p.id, p.trip_id, p.stop_id, p.lat, p.lon, p.ts,
           s.service_date, s.arrival_time, s.feed_version, s.sched_ts
    FROM pings p
    JOIN scan_days USING (date)
    JOIN sched s ON s.trip_id = p.trip_id AND s.stop_id = p.stop_id AND s.service_date IN (p.date, p.date - 1)
    QUALIFY row_number() OVER (PARTITION BY p.id
                               ORDER BY abs(epoch(p.ts) - epoch(s.sched_ts)), s.service_date DESC) = 1
),
located AS (
    SELECT a.id, a.trip_id, a.stop_id, a.service_date AS date, a.ts, a.arrival_time, a.feed_version,
           a.sched_ts, st.stop_name, st.parent_station,
           power(sin(radians(st.stop_lat - a.lat) / 2), 2)
             + cos(radians(a.lat)) * cos(radians(st.stop_lat)) * power(sin(radians(st.stop_lon - a.lon) / 2), 2) AS a
    FROM assigned a
    JOIN todo_days t ON t.date = a.service_date
    JOIN stops st USING (stop_id)
),
ranked AS (
    SELECT *,
           count(*) OVER (PARTITION BY trip_id, stop_id, date) AS ping_count,
           row_number() OVER (PARTITION BY trip_id, stop_id, date
                              ORDER BY 6371000 * 2 * atan2(sqrt(a), sqrt(1 - a)), id) AS rn
    FROM located
),
scored AS (
    SELECT *, (epoch(ts) - epoch(sched_ts)) / 60.0 AS raw_delay
    FROM ranked WHERE rn = 1
),
cleaned AS (
    SELECT *, CASE WHEN raw_delay > 500 OR raw_delay < -100 THEN 0.0 ELSE raw_delay END AS delay
    FROM scored
)
SELECT date, trip_id, stop_id, stop_name, parent_station,
       arrival_time,
       ts AS actual_arrival_time,
       greatest(delay, 0.0) AS delay_minutes,
       delay > 4 AS is_delayed,
       CASE WHEN delay > 15 THEN 'Major' WHEN delay > 4 THEN 'Minor' ELSE 'On Time' END AS delay_severity,
       CASE WHEN isodow(ts) >= 6 THEN 'Weekend'
            WHEN CAST(ts AS TIME) BETWEEN TIME '06:00:00' AND TIME '09:00:00' THEN 'Morning'
            WHEN CAST(ts AS TIME) BETWEEN TIME '15:30:00' AND TIME '19:30:00' THEN 'Evening'
            ELSE 'Other' END AS commute_period,
       hour(ts) AS hour,
       CAST(ping_count AS INTEGER) AS ping_count,
       feed_version
FROM cleaned
"""


def open_store(path: str) -> duckdb.DuckDBPyConnection:
    # Capped so the nightly run stays a polite neighbour on a shared host
    # (DuckDB otherwise takes up to 80% of RAM and every core).
    store = duckdb.connect(path, config={"memory_limit": "1GB", "threads": 4})
    store.execute("INSTALL sqlite; LOAD sqlite; SET sqlite_all_varchar = true")
    store.execute(SCHEMA)
    return store


def attach_pings(store: duckdb.DuckDBPyConnection, sqlite_path: str) -> None:
    """Expose train_locations as the temp view ``pings`` (id, trip_id, stop_id, lat, lon, ts, date)."""
    quoted = str(sqlite_path).replace("'", "''")
    store.execute(f"""
        CREATE OR REPLACE TEMP VIEW pings AS
        SELECT CAST(id AS BIGINT) AS id, trip_id, stop_id,
               CAST(vehicle_lat AS DOUBLE) AS lat, CAST(vehicle_lon AS DOUBLE) AS lon,
               ts, CAST(ts AS DATE) AS date
        FROM (SELECT *, TRY_CAST("timestamp" AS TIMESTAMP) AS ts FROM sqlite_scan('{quoted}', 'train_locations'))
        WHERE ts IS NOT NULL
    """)


def update_arrivals(store: duckdb.DuckDBPyConnection, sqlite_path: str, feeds_root: Path = FEEDS_DIR,
                    full: bool = False, today: date | None = None, batch_days: int = 31) -> list[date]:
    """Recompute arrivals for days that need it. Returns those days.

    Days are scored ``batch_days`` at a time, each batch in its own transaction,
    so memory stays bounded on a full rebuild and an interrupted one resumes.
    """
    attach_pings(store, sqlite_path)
    today = today or date.today()
    # Pings in the small hours can belong to the previous day's trips (the last
    # trains arrive around 01:30), so that day counts as one with data too.
    service_days = [r[0] for r in store.execute(f"""
        SELECT DISTINCT unnest(CASE WHEN hour(ts) < {AFTER_MIDNIGHT_HOURS} THEN [date, date - 1] ELSE [date] END) AS d
        FROM pings ORDER BY d
    """).fetchall()]

    feeds = discover_feeds(feeds_root)
    version = {d: getattr(feed_for_date(d, feeds), "version", None) for d in service_days}
    done = dict(store.execute("SELECT date, feed_version FROM processed_days").fetchall())
    recent = {today, today - timedelta(days=1)}
    todo = {d for d in service_days if full or d in recent or d not in done or done[d] != version[d]}
    # A day's late trains are scored from the next day's early pings, so new or
    # changed pings on a day can move arrivals on the day before.
    todo = sorted(todo | {d - timedelta(days=1) for d in todo if d - timedelta(days=1) in version})

    stops = load_stops(feeds_root)
    stops = stops[stops["stop_id"].str.isnumeric()]
    for start in range(0, len(todo), batch_days):
        _score_days(store, todo[start:start + batch_days], version, feeds_root, stops)
    return todo


def _score_days(store: duckdb.DuckDBPyConnection, days: list[date], version: dict, feeds_root: Path,
                stops: pd.DataFrame) -> None:
    # Pings dated D+1 can belong to D; assigning them needs the D-1..D+1 schedules.
    one = timedelta(days=1)
    schedule = schedule_for_dates({d + k * one for d in days for k in (-1, 0, 1)}, feeds_root)
    schedule = schedule.assign(date=pd.to_datetime(schedule["date"]).dt.date)
    todo_days = pd.DataFrame({"date": days})
    scan_days = pd.DataFrame({"date": sorted(set(days) | {d + one for d in days})})
    processed = pd.DataFrame({"date": days, "feed_version": [version[d] for d in days]})

    store.register("schedule", schedule)
    store.register("stops", stops)
    store.register("todo_days", todo_days)
    store.register("scan_days", scan_days)
    store.register("processed", processed)
    try:
        store.execute("BEGIN")
        store.execute("DELETE FROM arrivals WHERE date IN (SELECT date FROM todo_days)")
        if not schedule.empty:  # an empty frame has untyped columns DuckDB can't join on
            store.execute(f"INSERT INTO arrivals {ARRIVALS_SQL}")
        store.execute("INSERT OR REPLACE INTO processed_days SELECT date, feed_version FROM processed")
        store.execute("COMMIT")
    except Exception:
        store.execute("ROLLBACK")
        raise
    finally:
        for name in ("schedule", "stops", "todo_days", "scan_days", "processed"):
            store.unregister(name)


def load_arrivals(store: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return store.execute("SELECT * FROM arrivals ORDER BY date, trip_id, stop_id").df()
