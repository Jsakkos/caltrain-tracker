"""
Line-wide stops and trips that never ran, found from the minute-by-minute pings.

Ported from analysis/05_network_stops.py (Detector A, Detector B and the event
table). Neither leaves a record in the feed itself, but both leave fingerprints:

  * a network stop is several trains frozen at the same minute, away from the
    terminals, for a quarter of an hour or more. Frozen positions also come from
    feed freezes, so an event only counts as a stop (``is_network_stop``) when at
    least CONFIRM_TRAINS trains then run 15+ minutes late within CONFIRM_HOURS.
  * a never-ran trip is a scheduled train with no pings all day while the feed
    carried other trains around its departure.

Reads the temp views/tables ``pings``, ``clean``, ``feed_days`` and the table
``scheduled_trips`` on the connection.
"""
from __future__ import annotations

import duckdb
import pandas as pd

RUN_MIN_PINGS = 10        # identical consecutive positions that make a stationary run
MIN_TRAINS = 4            # stationary trains in the same minute
MERGE_GAP_MIN = 20        # minutes of quiet that split two events
MIN_HOLD_MIN = 15         # shortest event worth reporting
TERMINAL_RADIUS_M = 600   # a run this close to a terminal is a parked train
TERMINALS = ("San Francisco", "San Jose Diridon", "Tamien", "Gilroy")
CONFIRM_TRAINS = 5        # late trains that turn a freeze into a network stop
CONFIRM_HOURS = 4         # how long after the stop those trains are counted
DIRS_HOURS = 3            # window for the "both directions hit" count
NEVER_RAN_BEFORE_MIN, NEVER_RAN_BEFORE_ALIVE = 30, 20   # live feed minutes needed before departure
NEVER_RAN_AFTER_MIN, NEVER_RAN_AFTER_ALIVE = 60, 45     # ... and after it
NORMAL_LATE_SHARE = 0.15  # an hour with a smaller late share counts as back to normal

COLUMNS = ["date", "t_start", "t_end", "hold_minutes", "max_trains", "held_near", "trains_late",
           "max_delay", "dirs_hit", "cancelled_after", "hours_to_normal", "is_network_stop"]

_TERMINAL_TEST = " OR ".join(f"nearest ILIKE '{name}%'" for name in TERMINALS)

# The pings the analysis reads: positioned trains only, as in analysis/build_dataset.py
# (no 'unknown' stop, no foreign-vehicle ids).
PINGS_SQL = """
CREATE OR REPLACE TEMP VIEW ns_pings AS
SELECT * FROM pings WHERE stop_id <> 'unknown' AND length(trip_id) <= 3
"""

STOPS_RUN_SQL = f"""
CREATE OR REPLACE TEMP VIEW ns_stops_run AS
WITH p AS (
    SELECT p.*, lag(lat) OVER w AS plat, lag(lon) OVER w AS plon
    FROM ns_pings p JOIN feed_days d USING (date)
    WHERE NOT d.bad_feed AND hour(ts) BETWEEN 4 AND 23 AND lat > 1
    WINDOW w AS (PARTITION BY trip_id, date ORDER BY ts)
),
r AS (SELECT *, CASE WHEN plat IS NULL OR lat <> plat OR lon <> plon THEN 1 ELSE 0 END AS moved FROM p),
g AS (SELECT *, sum(moved) OVER (PARTITION BY trip_id, date ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp FROM r),
runs AS (
    SELECT trip_id, date, grp, min(ts) AS t0, max(ts) AS t1, count(*) AS n, any_value(lat) AS lat, any_value(lon) AS lon
    FROM g GROUP BY 1, 2, 3 HAVING count(*) >= {RUN_MIN_PINGS}
),
dd AS (
    SELECT r.*, s.stop_name,
           6371000 * 2 * asin(sqrt(power(sin(radians(s.stop_lat - r.lat) / 2), 2)
               + cos(radians(r.lat)) * cos(radians(s.stop_lat)) * power(sin(radians(s.stop_lon - r.lon) / 2), 2))) AS dist
    FROM runs r, ns_stops s
),
near AS (
    SELECT trip_id, date, grp, t0, t1, n, lat, lon, stop_name AS nearest, dist AS dist_station
    FROM dd QUALIFY row_number() OVER (PARTITION BY trip_id, date, grp ORDER BY dist) = 1
)
SELECT * FROM near WHERE NOT (dist_station < {TERMINAL_RADIUS_M} AND ({_TERMINAL_TEST}))
"""

STOP_MINUTES_SQL = """
CREATE OR REPLACE TEMP VIEW ns_stop_min AS
SELECT date, trip_id, unnest(generate_series(date_trunc('minute', t0), date_trunc('minute', t1), INTERVAL 1 MINUTE)) AS m
FROM ns_stops_run
"""

CANDIDATES_SQL = f"""
WITH mm AS (SELECT date, m, count(DISTINCT trip_id) AS k FROM ns_stop_min GROUP BY 1, 2),
ev AS (
    SELECT date, m, k, m - lag(m) OVER (PARTITION BY date ORDER BY m) AS gap FROM mm WHERE k >= {MIN_TRAINS}
),
e2 AS (
    SELECT *, sum(CASE WHEN gap IS NULL OR gap > INTERVAL {MERGE_GAP_MIN} MINUTE THEN 1 ELSE 0 END)
              OVER (PARTITION BY date ORDER BY m) AS eid
    FROM ev
)
SELECT date, eid, min(m) AS t_start, max(m) AS t_end,
       round(epoch(max(m) - min(m)) / 60) AS hold_minutes, max(k) AS max_trains
FROM e2 GROUP BY 1, 2 HAVING epoch(max(m) - min(m)) / 60 >= {MIN_HOLD_MIN}
"""

# A trip never ran when it has no pings that day on a trusted day while the feed
# was alive around its departure. Only the (few) missing trips are joined against
# the per-minute feed activity.
NEVER_RAN_SQL = f"""
WITH missing AS (
    SELECT s.date, s.trip_id, s.route_id, s.dep_ts
    FROM scheduled_trips s
    JOIN feed_days d USING (date)
    WHERE NOT d.bad_feed
      AND NOT EXISTS (SELECT 1 FROM ns_pings p WHERE p.date = s.date AND p.trip_id = s.trip_id)
),
alive AS (
    SELECT date, date_trunc('minute', ts) AS m, count(*) AS k
    FROM ns_pings WHERE date IN (SELECT date FROM missing) GROUP BY 1, 2
)
SELECT m.date, m.trip_id, m.route_id, m.dep_ts
FROM missing m JOIN alive f ON f.date = m.date
 AND f.m BETWEEN m.dep_ts - INTERVAL {NEVER_RAN_BEFORE_MIN} MINUTE AND m.dep_ts + INTERVAL {NEVER_RAN_AFTER_MIN} MINUTE
GROUP BY m.date, m.trip_id, m.route_id, m.dep_ts
HAVING count(*) FILTER (WHERE f.m <= m.dep_ts) >= {NEVER_RAN_BEFORE_ALIVE}
   AND count(*) FILTER (WHERE f.m >= m.dep_ts) >= {NEVER_RAN_AFTER_ALIVE}
ORDER BY m.date, m.dep_ts, m.trip_id
"""

EVENTS_SQL = f"""
SELECT e.date, e.t_start, e.t_end, e.hold_minutes, e.max_trains,
   (SELECT count(DISTINCT trip_id) FROM clean h
     WHERE h.date = e.date AND h.sched_ts BETWEEN e.t_start AND e.t_end + INTERVAL {CONFIRM_HOURS} HOUR AND h.late15) AS trains_late,
   (SELECT max(delay_signed) FROM clean h
     WHERE h.date = e.date AND h.sched_ts BETWEEN e.t_start AND e.t_end + INTERVAL {CONFIRM_HOURS} HOUR) AS max_delay,
   (SELECT count(DISTINCT direction) FROM clean h
     WHERE h.date = e.date AND h.sched_ts BETWEEN e.t_start AND e.t_end + INTERVAL {DIRS_HOURS} HOUR AND h.late15) AS dirs_hit,
   (SELECT count(*) FROM ns_cancelled c WHERE c.date = e.date AND c.dep_ts >= e.t_start - INTERVAL 1 HOUR) AS cancelled_after,
   (SELECT min(hh) FROM (SELECT date_trunc('hour', sched_ts) AS hh, avg(late4::INT) AS l
                         FROM clean h WHERE h.date = e.date AND h.sched_ts > e.t_end GROUP BY 1) x
     WHERE l < {NORMAL_LATE_SHARE}) AS first_normal_hour,
   (SELECT mode(nearest) FROM ns_stops_run s WHERE s.date = e.date AND s.t1 >= e.t_start AND s.t0 <= e.t_end) AS held_near
FROM ns_candidates e
ORDER BY e.date, e.t_start
"""


def _station_name(name: str | None) -> str | None:
    if name is None:
        return None
    for suffix in (" Caltrain Station", " Northbound", " Southbound"):
        name = name.replace(suffix, "")
    return name


def _none_for_nan(series: pd.Series) -> pd.Series:
    return series.astype(object).where(series.notna(), None)


def never_ran(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Scheduled trips with no pings that day on a trusted day, while the feed carried other trains.

    Needs ``NEVER_RAN_BEFORE_ALIVE`` of the ``NEVER_RAN_BEFORE_MIN`` minutes before
    departure and ``NEVER_RAN_AFTER_ALIVE`` of the ``NEVER_RAN_AFTER_MIN`` after to
    have pings from any train, which rules out collector gaps and late starts.
    """
    con.execute(PINGS_SQL)
    try:
        return _never_ran(con)
    finally:
        con.execute("DROP VIEW IF EXISTS ns_pings")


def _never_ran(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(NEVER_RAN_SQL).df()


def detect_network_stops(con: duckdb.DuckDBPyConnection, stops: pd.DataFrame) -> pd.DataFrame:
    """One row per stretch with several trains frozen at once, with how far the ripple reached.

    ``stops`` is the ``load_stops`` frame (station coordinates). ``is_network_stop``
    separates real stops from feed freezes by whether ``CONFIRM_TRAINS`` trains then
    ran 15+ minutes late. ``hours_to_normal`` is None when no later hour got back
    under the normal late share.
    """
    con.register("ns_stops", stops)
    try:
        con.execute(PINGS_SQL)
        con.execute(STOPS_RUN_SQL)
        con.execute(STOP_MINUTES_SQL)
        candidates = con.execute(CANDIDATES_SQL).df()
        con.register("ns_candidates", candidates)
        con.register("ns_cancelled", _never_ran(con))
        events = con.execute(EVENTS_SQL).df()
    finally:
        for view in ("ns_stop_min", "ns_stops_run", "ns_pings"):
            con.execute(f"DROP VIEW IF EXISTS {view}")
        for name in ("ns_candidates", "ns_cancelled", "ns_stops"):
            con.unregister(name)

    events["held_near"] = events.held_near.map(_station_name)
    hours = (events.first_normal_hour - events.t_start).dt.total_seconds() / 3600
    events["hours_to_normal"] = _none_for_nan(hours.round(1))
    events["max_delay"] = _none_for_nan(events.max_delay.round())
    events["hold_minutes"] = events.hold_minutes.astype(float)
    events["is_network_stop"] = events.trains_late >= CONFIRM_TRAINS
    return events[COLUMNS].reset_index(drop=True)
