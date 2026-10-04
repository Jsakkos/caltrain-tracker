"""
Arrivals: each train's closest approach to each scheduled stop, one row per day.

Reads train_locations straight from the collector's SQLite file with DuckDB and
keeps the results in a DuckDB file, recomputing only days that are new, recent,
or whose schedule version changed. The scoring rules deliberately match the
legacy pandas flow (src/flows/data_processing.py); see
docs/superpowers/plans/2026-10-03-duckdb-build.md for the list.
"""
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from src.data.gtfs_feeds import FEEDS_DIR, discover_feeds, feed_for_date, load_stops, schedule_for_dates

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

# One row per (trip, stop, day): the ping nearest the platform. Ties go to the
# first stored row, like the legacy groupby().first().
ARRIVALS_SQL = """
WITH located AS (
    SELECT p.id, p.trip_id, p.stop_id, p.date, p.ts, s.arrival_time, s.feed_version,
           st.stop_name, st.parent_station,
           power(sin(radians(st.stop_lat - p.lat) / 2), 2)
             + cos(radians(p.lat)) * cos(radians(st.stop_lat)) * power(sin(radians(st.stop_lon - p.lon) / 2), 2) AS a
    FROM pings p
    JOIN todo_days USING (date)
    JOIN schedule s USING (date, trip_id, stop_id)
    JOIN stops st USING (stop_id)
),
ranked AS (
    SELECT *,
           6371000 * 2 * atan2(sqrt(a), sqrt(1 - a)) AS distance,
           count(*) OVER (PARTITION BY trip_id, stop_id, date) AS ping_count,
           row_number() OVER (PARTITION BY trip_id, stop_id, date
                              ORDER BY 6371000 * 2 * atan2(sqrt(a), sqrt(1 - a)), id) AS rn
    FROM located
),
timed AS (
    SELECT *,
           -- GTFS hours run past 24; legacy normalize_time wraps them onto the same date.
           CAST(split_part(arrival_time, ':', 1) AS INTEGER) % 24 AS sched_h,
           CAST(split_part(arrival_time, ':', 2) AS INTEGER) AS sched_m,
           COALESCE(TRY_CAST(split_part(arrival_time, ':', 3) AS INTEGER), 0) AS sched_s
    FROM ranked WHERE rn = 1
),
scored AS (
    SELECT *,
           (epoch(ts) - epoch(CAST(date AS TIMESTAMP)) - (sched_h * 3600 + sched_m * 60 + sched_s)) / 60.0 AS raw_delay
    FROM timed
),
cleaned AS (
    SELECT *, CASE WHEN raw_delay > 500 OR raw_delay < -100 THEN 0.0 ELSE raw_delay END AS delay
    FROM scored
)
SELECT date, trip_id, stop_id, stop_name, parent_station,
       lpad(CAST(sched_h AS VARCHAR), 2, '0') || substr(arrival_time, strpos(arrival_time, ':')) AS arrival_time,
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
                    full: bool = False, today: date | None = None) -> list[date]:
    """Recompute arrivals for days that need it. Returns those days."""
    attach_pings(store, sqlite_path)
    today = today or date.today()
    ping_days = [r[0] for r in store.execute("SELECT DISTINCT date FROM pings ORDER BY date").fetchall()]

    feeds = discover_feeds(feeds_root)
    version = {d: getattr(feed_for_date(d, feeds), "version", None) for d in ping_days}
    done = dict(store.execute("SELECT date, feed_version FROM processed_days").fetchall())
    recent = {today, today - timedelta(days=1)}
    todo = [d for d in ping_days if full or d in recent or d not in done or done[d] != version[d]]
    if not todo:
        return []

    schedule = schedule_for_dates(todo, feeds_root)
    schedule = schedule.assign(date=pd.to_datetime(schedule["date"]).dt.date)
    stops = load_stops(feeds_root)
    stops = stops[stops["stop_id"].str.isnumeric()]
    todo_days = pd.DataFrame({"date": todo})
    processed = pd.DataFrame({"date": todo, "feed_version": [version[d] for d in todo]})

    store.register("schedule", schedule)
    store.register("stops", stops)
    store.register("todo_days", todo_days)
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
        for name in ("schedule", "stops", "todo_days", "processed"):
            store.unregister(name)
    return todo


def load_arrivals(store: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return store.execute("SELECT * FROM arrivals ORDER BY date, trip_id, stop_id").df()
