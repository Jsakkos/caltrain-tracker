"""
Shared helpers for the analysis notebooks.

    from common import connect, q
    con = connect()          # read-only DuckDB with the `clean` view registered
    df = q("select ... from clean")

`clean` is the subset every notebook starts from:
  * not the origin stop (the train sits at the platform; closest approach is meaningless)
  * scheduled before midnight (511 drops all vehicles at 00:00 Pacific)
  * closest ping within 1 km of the platform (positions further away are stale/garbage)
  * service day not flagged as a bad feed day (see `feed_days`)
  * real train numbers only (no foreign-vehicle ids)
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).resolve().parent
DB = HERE / "data" / "analysis.duckdb"

# Feed-health rule: a service day is "bad" when 511's positions were stale.
# Thresholds come from the per-day distribution (01_data_quality notebook):
# healthy weekdays have 55-65% of consecutive pings changing position and
# ~78% of non-origin arrivals located within 1 km of the platform.
BAD_DAY_SQL = """
CREATE OR REPLACE VIEW feed_days AS
WITH loc AS (
    SELECT date,
           avg((dist_m < 100)::INT)  AS near100,
           avg((dist_m < 1000)::INT) AS near1k,
           count(*) AS arrivals
    FROM arrivals WHERE stop_rank > 1 GROUP BY date
)
SELECT f.date, isodow(f.date) AS dow, isodow(f.date) <= 5 AS weekday,
       f.pings, f.polls, f.vehicles_per_poll, f.pos_change_share, f.trains,
       f.first_ping, f.last_ping,
       l.near100, l.near1k, l.arrivals,
       (f.pos_change_share < 0.45 OR l.near1k < 0.60) AS bad_feed,
       -- partial days: collector started late or stopped early
       (f.polls < 900 AND isodow(f.date) <= 5) OR (f.polls < 800 AND isodow(f.date) > 5) AS partial_day
FROM feed_health f LEFT JOIN loc l USING (date)
"""

CLEAN_SQL = """
CREATE OR REPLACE VIEW clean AS
SELECT a.*,
       CASE WHEN a.stop_id LIKE '%1' THEN 'NB' ELSE 'SB' END AS direction,
       CASE WHEN a.route_id LIKE 'Local%' THEN 'Local' ELSE a.route_id END AS service,
       isodow(a.date) AS dow, isodow(a.date) <= 5 AS weekday,
       hour(a.sched_ts) AS sched_hour,
       a.delay_signed > 4 AS late4, a.delay_signed > 15 AS late15,
       replace(replace(replace(a.stop_name, ' Caltrain Station', ''), ' Northbound', ''), ' Southbound', '') AS station,
       d.bad_feed, d.partial_day
FROM arrivals a JOIN feed_days d USING (date)
WHERE a.stop_rank > 1
  AND a.arrival_time < '24:'
  AND a.dist_m < 1000
  AND NOT d.bad_feed
  AND length(a.trip_id) <= 3
"""


def connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB), read_only=read_only)
    # Views live in a temp schema so the file is not modified.
    con.execute(BAD_DAY_SQL.replace("CREATE OR REPLACE VIEW", "CREATE OR REPLACE TEMP VIEW"))
    con.execute(CLEAN_SQL.replace("CREATE OR REPLACE VIEW", "CREATE OR REPLACE TEMP VIEW"))
    return con


_CON: duckdb.DuckDBPyConnection | None = None


def q(sql: str, con: duckdb.DuckDBPyConnection | None = None) -> pd.DataFrame:
    global _CON
    if con is None:
        if _CON is None:
            _CON = connect()
        con = _CON
    return con.execute(sql).df()


# Station order north -> south (by GTFS stop_id numeric order, which follows the line)
STATION_ORDER = [
    "San Francisco", "22nd Street", "Bayshore", "South San Francisco", "San Bruno", "Millbrae",
    "Broadway", "Burlingame", "San Mateo", "Hayward Park", "Hillsdale", "Belmont", "San Carlos",
    "Redwood City", "Menlo Park", "Palo Alto", "Stanford", "California Avenue", "San Antonio",
    "Mountain View", "Sunnyvale", "Lawrence", "Santa Clara", "College Park", "San Jose Diridon",
    "Tamien", "Capitol", "Blossom Hill", "Morgan Hill", "San Martin", "Gilroy",
]


def wilson_halfwidth(p: pd.Series, n: pd.Series, z: float = 1.96) -> pd.Series:
    """Half-width of a 95% Wilson interval for a share p observed on n trials."""
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return half
