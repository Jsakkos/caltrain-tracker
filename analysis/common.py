"""
Shared helpers for the analysis notebooks.

    from common import connect, q
    con = connect()          # read-only DuckDB with the `clean` view registered
    df = q("select ... from clean")

`clean` is the subset every notebook starts from (rules defined in src/pipeline/clean.py):
  * not the origin stop (the train sits at the platform; closest approach is meaningless)
  * scheduled before midnight (511 drops all vehicles at 00:00 Pacific)
  * closest ping within 1 km of the platform (positions further away are stale/garbage)
  * service day not flagged as a bad feed day (see `feed_days`)
  * real train numbers only (no foreign-vehicle ids)
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).resolve().parent
DB = HERE / "data" / "analysis.duckdb"

# The rules live in the pipeline so the dashboard, the stories page and these notebooks agree;
# notebooks run from analysis/, so put the repo root on the path to import it.
sys.path.insert(0, str(HERE.parent))
from src.pipeline.clean import register_views  # noqa: E402


def connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB), read_only=read_only)
    register_views(con)  # temp views, so the file is not modified
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
