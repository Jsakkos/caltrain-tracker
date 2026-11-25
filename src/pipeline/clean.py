"""
One definition of "on time" for the dashboard, the stories page and the analysis notebooks.

``arrivals`` holds every train's closest approach to every scheduled stop, good
or bad. This module decides which of those rows are trustworthy and how late is
late, in one place:

  * the origin stop is dropped (the train sits at the platform, so closest
    approach says nothing about punctuality)
  * calls scheduled at or after 24:00 are dropped (511 drops all vehicles at
    midnight Pacific)
  * the closest ping must be within MAX_DIST_M of the platform (further away
    means stale or garbage positions)
  * only real train numbers (no foreign-vehicle ids)
  * service days where 511's positions were stale are dropped whole (``bad_feed``)

"Late" is more than LATE_MIN minutes behind schedule, "major" more than
MAJOR_MIN; early counts as on time.
"""
from __future__ import annotations

import duckdb
import pandas as pd

# Feed-health rule: a service day is "bad" when 511's positions were stale.
# Thresholds come from the per-day distribution (01_data_quality notebook):
# healthy weekdays have 55-65% of consecutive pings changing position and
# ~78% of non-origin arrivals located within 1 km of the platform.
POS_CHANGE_MIN = 0.45
NEAR_1KM_MIN = 0.60
MAX_DIST_M = 1000
LATE_MIN = 4
MAJOR_MIN = 15
MIN_LEAGUE_RUNS = 80

# One row per service day, from the raw pings view (id, trip_id, stop_id, lat, lon, ts, date)
# restricted to the days in todo_days.
FEED_HEALTH_SQL = """
WITH p AS (
    SELECT *, lag(lat) OVER w AS plat, lag(lon) OVER w AS plon
    FROM pings
    WHERE date IN (SELECT date FROM todo_days)
    WINDOW w AS (PARTITION BY trip_id, date ORDER BY ts)
)
SELECT date,
       count(*) AS pings,
       count(DISTINCT date_trunc('minute', ts)) AS polls,
       count(*) * 1.0 / count(DISTINCT date_trunc('minute', ts)) AS vehicles_per_poll,
       avg(CASE WHEN plat IS NULL THEN NULL WHEN lat <> plat OR lon <> plon THEN 1.0 ELSE 0.0 END) AS pos_change_share,
       count(DISTINCT trip_id) AS trains,
       min(ts) AS first_ping, max(ts) AS last_ping
FROM p
WHERE stop_id <> 'unknown' AND length(trip_id) <= 3
GROUP BY date ORDER BY date
"""

FEED_DAYS_SQL = f"""
CREATE OR REPLACE TEMP VIEW feed_days AS
WITH loc AS (
    SELECT date,
           avg((dist_m < 100)::INT)  AS near100,
           avg((dist_m < {MAX_DIST_M})::INT) AS near1k,
           count(*) AS arrivals
    FROM arrivals WHERE stop_rank > 1 GROUP BY date
)
SELECT f.date, isodow(f.date) AS dow, isodow(f.date) <= 5 AS weekday,
       f.pings, f.polls, f.vehicles_per_poll, f.pos_change_share, f.trains,
       f.first_ping, f.last_ping,
       l.near100, l.near1k, l.arrivals,
       (f.pos_change_share < {POS_CHANGE_MIN} OR l.near1k < {NEAR_1KM_MIN}) AS bad_feed,
       -- partial days: collector started late or stopped early
       (f.polls < 900 AND isodow(f.date) <= 5) OR (f.polls < 800 AND isodow(f.date) > 5) AS partial_day
FROM feed_health f LEFT JOIN loc l USING (date)
"""

_CLEAN_SQL = """
CREATE OR REPLACE TEMP VIEW {name} AS
SELECT a.*,
       CASE WHEN a.stop_id LIKE '%1' THEN 'NB' ELSE 'SB' END AS direction,
       CASE WHEN a.route_id LIKE 'Local%' THEN 'Local' ELSE a.route_id END AS service,
       isodow(a.date) AS dow, isodow(a.date) <= 5 AS weekday,
       hour(a.sched_ts) AS sched_hour,
       a.delay_signed > {late} AS late4, a.delay_signed > {major} AS late15,
       replace(replace(replace(a.stop_name, ' Caltrain Station', ''), ' Northbound', ''), ' Southbound', '') AS station,
       d.bad_feed, d.partial_day
FROM arrivals a JOIN feed_days d USING (date)
WHERE a.stop_rank > 1
  AND a.arrival_time < '24:'
  AND a.dist_m < {max_dist}
  {mask}
  AND length(a.trip_id) <= 3
"""

_RULES = dict(late=LATE_MIN, major=MAJOR_MIN, max_dist=MAX_DIST_M)
CLEAN_SQL = _CLEAN_SQL.format(name="clean", mask="AND NOT d.bad_feed", **_RULES)
CLEAN_ALL_DAYS_SQL = _CLEAN_SQL.format(name="clean_all_days", mask="", **_RULES)


def register_views(con: duckdb.DuckDBPyConnection) -> None:
    """Create the temp views ``feed_days``, ``clean`` and ``clean_all_days`` over ``arrivals`` and ``feed_health``.

    Temp views, so a read-only database file is not modified.
    """
    for sql in (FEED_DAYS_SQL, CLEAN_SQL, CLEAN_ALL_DAYS_SQL):
        con.execute(sql)


def load_clean(con: duckdb.DuckDBPyConnection, include_bad_feed: bool = False) -> pd.DataFrame:
    """Clean arrivals in the dashboard's shape, with signed delays and the scheduled hour.

    The legacy columns are rebuilt from ``delay_signed`` and ``sched_ts`` so early
    trains stay negative and a train scheduled at 08:55 is an 8 o'clock train
    however late it runs. ``dep_ts`` is the train's scheduled departure from its
    origin, when ``scheduled_trips`` has it.
    """
    view = "clean_all_days" if include_bad_feed else "clean"
    return con.execute(f"""
        SELECT c.* REPLACE (
                   c.delay_signed AS delay_minutes,
                   c.late4 AS is_delayed,
                   CASE WHEN c.delay_signed > {MAJOR_MIN} THEN 'Major'
                        WHEN c.delay_signed > {LATE_MIN} THEN 'Minor' ELSE 'On Time' END AS delay_severity,
                   c.sched_hour AS hour,
                   CASE WHEN isodow(c.sched_ts) >= 6 THEN 'Weekend'
                        WHEN CAST(c.sched_ts AS TIME) BETWEEN TIME '06:00:00' AND TIME '09:00:00' THEN 'Morning'
                        WHEN CAST(c.sched_ts AS TIME) BETWEEN TIME '15:30:00' AND TIME '19:30:00' THEN 'Evening'
                        ELSE 'Other' END AS commute_period),
               t.dep_ts
        FROM {view} c LEFT JOIN scheduled_trips t ON t.date = c.date AND t.trip_id = c.trip_id
        ORDER BY c.date, c.trip_id, c.stop_rank
    """).df()


def wilson(p: float, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson score interval for a share ``p`` observed on ``n`` trials (as fractions)."""
    if n <= 0:
        return 0.0, 1.0
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return max(0.0, centre - half), min(1.0, centre + half)


def _most_common(values: pd.Series):
    values = values.dropna()
    return values.mode().iloc[0] if len(values) else None


def last_stop_league(df: pd.DataFrame) -> pd.DataFrame:
    """Per train, how often it is late at its last observed stop, worst first.

    ``df`` is a ``load_clean`` frame. Only weekday runs count, and only trains
    with at least MIN_LEAGUE_RUNS of them. ``departs`` is the most common
    scheduled departure ("HH:MM"), null when the frame has no ``dep_ts``.
    """
    columns = ["trip_id", "service", "direction", "departs", "runs", "late_pct", "ci_low", "ci_high", "last_stop_name"]
    runs = df[df["weekday"]]
    runs = runs.sort_values("stop_rank").groupby(["date", "trip_id"], sort=False).tail(1)
    if "dep_ts" in runs:
        runs = runs.assign(departs=runs["dep_ts"].dt.strftime("%H:%M"))
    else:
        runs = runs.assign(departs=None)

    rows = []
    for (trip_id, service, direction), g in runs.groupby(["trip_id", "service", "direction"]):
        n = len(g)
        if n < MIN_LEAGUE_RUNS:
            continue
        p = float(g["late4"].mean())
        low, high = wilson(p, n)
        rows.append({
            "trip_id": trip_id, "service": service, "direction": direction,
            "departs": _most_common(g["departs"]), "runs": n,
            "late_pct": round(p * 100, 1), "ci_low": round(low * 100, 1), "ci_high": round(high * 100, 1),
            "last_stop_name": _most_common(g["station"]),
        })
    league = pd.DataFrame(rows, columns=columns)
    return league.sort_values(["late_pct", "trip_id"], ascending=[False, True], ignore_index=True)
