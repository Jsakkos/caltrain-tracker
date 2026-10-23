"""
Build the analysis dataset: analysis/data/analysis.duckdb

Re-derives arrivals from the raw pings (analysis/data/caltrain_lat_long.db, a
snapshot of the server's SQLite) with the extra columns the production table
drops: signed delay, closest-approach distance, stop sequence, route type and
direction. Also writes the per-day scheduled calls, a per-day feed-health table,
and 511's own stop_observations from the monthly archives for validation.

Run:  uv run --group notebooks python analysis/build_dataset.py
"""
from __future__ import annotations

import sys
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.data.gtfs_feeds import (  # noqa: E402
    FEEDS_DIR, _tables, active_service_ids, discover_feeds, feed_for_date, load_stops,
)

DATA = ROOT / "analysis" / "data"
SQLITE = DATA / "caltrain_lat_long.db"
OUT = DATA / "analysis.duckdb"


def schedule_with_meta(days: list[date]) -> pd.DataFrame:
    """Scheduled calls per day with route_id, direction_id, stop_sequence, n_stops."""
    feeds = discover_feeds(FEEDS_DIR)
    parts = []
    for day in days:
        feed = feed_for_date(day, feeds)
        if feed is None:
            continue
        t = _tables(feed.path)
        trips = t["trips"]
        trips = trips[trips.service_id.isin(active_service_ids(feed, day))]
        number = trips.get("trip_short_name", pd.Series("", index=trips.index))
        trips = trips.assign(train=number.where(number != "", trips.trip_id).str.lstrip("M"))
        calls = t["stop_times"].merge(trips[["trip_id", "train", "route_id", "direction_id"]], on="trip_id")
        calls = calls.assign(stop_sequence=calls.stop_sequence.astype(int))
        # Caltrain's stop_sequence has gaps, so rank the calls within each trip (1..n)
        calls = calls.assign(stop_rank=calls.groupby("trip_id")["stop_sequence"].rank(method="first").astype(int))
        n = calls.groupby("trip_id")["stop_id"].transform("size")
        parts.append(pd.DataFrame({
            "date": day, "trip_id": calls.train, "stop_id": calls.stop_id,
            "stop_sequence": calls.stop_sequence, "stop_rank": calls.stop_rank, "n_stops": n,
            "arrival_time": calls.arrival_time, "route_id": calls.route_id,
            "direction_id": calls.direction_id, "feed_version": feed.version,
            "gtfs_trip_id": calls.trip_id,
        }))
    sched = pd.concat(parts, ignore_index=True)
    return sched.drop_duplicates(["date", "trip_id", "stop_id"], ignore_index=True)


ARRIVALS_SQL = """
WITH sched AS (
    SELECT *, CAST(date AS TIMESTAMP) + to_seconds(
               CAST(split_part(arrival_time, ':', 1) AS INTEGER) * 3600
               + CAST(split_part(arrival_time, ':', 2) AS INTEGER) * 60
               + COALESCE(TRY_CAST(split_part(arrival_time, ':', 3) AS INTEGER), 0)) AS sched_ts
    FROM schedule
),
assigned AS (
    SELECT p.id, p.trip_id, p.stop_id, p.lat, p.lon, p.ts,
           s.date AS service_date, s.arrival_time, s.feed_version, s.sched_ts,
           s.stop_sequence, s.stop_rank, s.n_stops, s.route_id, s.direction_id
    FROM pings p
    JOIN sched s ON s.trip_id = p.trip_id AND s.stop_id = p.stop_id AND s.date IN (p.date, p.date - 1)
    QUALIFY row_number() OVER (PARTITION BY p.id
                               ORDER BY abs(epoch(p.ts) - epoch(s.sched_ts)), s.date DESC) = 1
),
located AS (
    SELECT a.*, st.stop_name, st.parent_station,
           6371000 * 2 * atan2(sqrt(h), sqrt(1 - h)) AS dist_m
    FROM (SELECT a.*,
                 power(sin(radians(st.stop_lat - a.lat) / 2), 2)
                   + cos(radians(a.lat)) * cos(radians(st.stop_lat)) * power(sin(radians(st.stop_lon - a.lon) / 2), 2) AS h
          FROM assigned a JOIN stops st USING (stop_id)) a
    JOIN stops st USING (stop_id)
),
ranked AS (
    SELECT *,
           count(*) OVER (PARTITION BY trip_id, stop_id, service_date) AS ping_count,
           min(ts) OVER (PARTITION BY trip_id, stop_id, service_date) AS first_ping,
           max(ts) OVER (PARTITION BY trip_id, stop_id, service_date) AS last_ping,
           row_number() OVER (PARTITION BY trip_id, stop_id, service_date ORDER BY dist_m, id) AS rn
    FROM located
)
SELECT service_date AS date, trip_id, stop_id, stop_name, parent_station,
       lpad(split_part(arrival_time, ':', 1), 2, '0') || substr(arrival_time, strpos(arrival_time, ':')) AS arrival_time,
       sched_ts, ts AS actual_ts,
       (epoch(ts) - epoch(sched_ts)) / 60.0 AS delay_signed,
       dist_m, CAST(ping_count AS INTEGER) AS ping_count, first_ping, last_ping,
       stop_sequence, stop_rank, n_stops, route_id, direction_id, feed_version
FROM ranked WHERE rn = 1
"""

FEED_HEALTH_SQL = """
WITH p AS (
    SELECT *, lag(lat) OVER w AS plat, lag(lon) OVER w AS plon
    FROM pings
    WINDOW w AS (PARTITION BY trip_id, date ORDER BY ts)
)
SELECT date,
       count(*) AS pings,
       count(DISTINCT date_trunc('minute', ts)) AS polls,
       count(*) * 1.0 / count(DISTINCT date_trunc('minute', ts)) AS vehicles_per_poll,
       avg(CASE WHEN plat IS NULL THEN NULL WHEN lat <> plat OR lon <> plon THEN 1.0 ELSE 0.0 END) AS pos_change_share,
       count(DISTINCT trip_id) AS trains,
       min(ts) AS first_ping, max(ts) AS last_ping,
       sum(CASE WHEN lat < 1 THEN 1 ELSE 0 END) AS zero_gps
FROM p
WHERE stop_id <> 'unknown' AND length(trip_id) <= 3
GROUP BY date ORDER BY date
"""


def main() -> None:
    t0 = time.time()
    if OUT.exists():
        OUT.unlink()
    con = duckdb.connect(str(OUT), config={"threads": 8})
    con.execute("INSTALL sqlite; LOAD sqlite; SET sqlite_all_varchar = true")
    con.execute(f"""
        CREATE TEMP VIEW pings AS
        SELECT CAST(id AS BIGINT) AS id, trip_id, stop_id,
               CAST(vehicle_lat AS DOUBLE) AS lat, CAST(vehicle_lon AS DOUBLE) AS lon,
               ts, CAST(ts AS DATE) AS date
        FROM (SELECT *, TRY_CAST("timestamp" AS TIMESTAMP) AS ts
              FROM sqlite_scan('{SQLITE.as_posix()}', 'train_locations'))
        WHERE ts IS NOT NULL
    """)
    days = [r[0] for r in con.execute("SELECT DISTINCT date FROM pings ORDER BY date").fetchall()]
    all_days = sorted({d + timedelta(days=k) for d in days for k in (-1, 0)})
    print(f"{len(days)} ping days; loading schedules for {len(all_days)} days ...", flush=True)

    schedule = schedule_with_meta(all_days)
    stops = load_stops(FEEDS_DIR)
    stops = stops[stops["stop_id"].str.isnumeric()]
    con.register("schedule_df", schedule)
    con.register("stops_df", stops)
    con.execute("CREATE TABLE schedule AS SELECT * FROM schedule_df")
    con.execute("CREATE TABLE stops AS SELECT * FROM stops_df")
    print(f"schedule rows {len(schedule):,}  ({time.time()-t0:.0f}s)", flush=True)

    con.execute(f"CREATE TABLE arrivals AS {ARRIVALS_SQL}")
    n = con.execute("SELECT count(*) FROM arrivals").fetchone()[0]
    print(f"arrivals {n:,}  ({time.time()-t0:.0f}s)", flush=True)

    con.execute(f"CREATE TABLE feed_health AS {FEED_HEALTH_SQL}")
    # Raw pings copied in for trajectory work (only real train numbers, known stops)
    con.execute("""CREATE TABLE pings AS SELECT id, trip_id, stop_id, lat, lon, ts, date FROM pings
                   WHERE stop_id <> 'unknown' AND length(trip_id) <= 3""")
    print(f"feed_health + pings  ({time.time()-t0:.0f}s)", flush=True)

    # 511's observed arrivals from the monthly archives, for validation
    frames = []
    for folder in sorted(FEEDS_DIR.glob("historic-*")):
        f = folder / "stop_observations.txt"
        if f.exists():
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
            frames.append(df.assign(archive=folder.name))
    if frames:
        obs = pd.concat(frames, ignore_index=True)
        obs = obs.assign(train=obs.trip_id.str.split(":").str[0].str.lstrip("M"))
        con.register("obs_df", obs)
        con.execute("CREATE TABLE stop_observations AS SELECT * FROM obs_df")
        print(f"stop_observations {len(obs):,}  ({time.time()-t0:.0f}s)", flush=True)

    con.close()
    print(f"done -> {OUT}  ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
