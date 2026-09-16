"""
Versioned GTFS schedule store.

Every Caltrain timetable the tracker uses lives in its own folder under
``gtfs_feeds/``, so arrivals can be scored against the schedule that was in
effect on their service date rather than whichever feed happens to be newest:

- ``v<feed_version>/``   published feeds from 511's ``datafeeds`` endpoint
- ``historic-YYYY-MM/``  Caltrain subsets of 511's monthly regional archives,
                         which list the services that ran on each day of the month

``gtfs_data/`` stays a copy of the newest published feed for code that only needs
the current timetable (stop lists, route shapes).

This module deliberately avoids importing ``src.config`` so notebooks and scripts
can read schedules without an API key.
"""
from __future__ import annotations

import csv
import io
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent.parent.parent
FEEDS_DIR = BASE_DIR / "gtfs_feeds"
CURRENT_DIR = BASE_DIR / "gtfs_data"
DATAFEEDS_URL = "https://api.511.org/transit/datafeeds"

STOP_COLUMNS = ["stop_id", "stop_name", "parent_station", "stop_lat", "stop_lon"]
SCHEDULE_COLUMNS = ["date", "trip_id", "stop_id", "stop_sequence", "arrival_time", "feed_version"]


@dataclass(frozen=True)
class Feed:
    path: Path
    version: str
    start: date
    end: date

    @property
    def historic(self) -> bool:
        return self.path.name.startswith("historic-")

    def covers(self, day: date) -> bool:
        return self.start <= day <= self.end


def _ymd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def read_feed_info(folder: Path) -> Feed | None:
    """Parse ``feed_info.txt``; returns None for folders that aren't usable feeds."""
    info = Path(folder) / "feed_info.txt"
    if not info.exists():
        return None
    with open(info, encoding="utf-8-sig", newline="") as f:
        row = next(csv.DictReader(f), None)
    if not row or not row.get("feed_start_date") or not row.get("feed_end_date"):
        return None
    return Feed(Path(folder), row.get("feed_version") or Path(folder).name,
                _ymd(row["feed_start_date"]), _ymd(row["feed_end_date"]))


def discover_feeds(root: Path = FEEDS_DIR) -> list[Feed]:
    root = Path(root)
    if not root.exists():
        return []
    feeds = (read_feed_info(p) for p in sorted(root.iterdir()) if p.is_dir() and not p.name.startswith("."))
    return [f for f in feeds if f is not None]


def feed_for_date(day: date, feeds: Iterable[Feed]) -> Feed | None:
    """The schedule to score ``day`` against.

    A monthly archive records what actually ran that day, so it wins. Otherwise
    use the published feed whose timetable started most recently: 511 republishes
    feeds with overlapping date ranges, and the newer timetable is the one in service.
    """
    candidates = [f for f in feeds if f.covers(day)]
    return max(candidates, key=lambda f: (f.historic, f.start, f.version), default=None)


@lru_cache(maxsize=None)
def _tables(folder: Path) -> dict[str, pd.DataFrame]:
    def read(name: str) -> pd.DataFrame:
        path = folder / f"{name}.txt"
        return pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else pd.DataFrame()

    return {name: read(name) for name in ("stops", "trips", "stop_times", "calendar", "calendar_dates")}


def active_service_ids(feed: Feed, day: date) -> frozenset[str]:
    """Service IDs running on ``day``: weekday rules from calendar.txt plus calendar_dates exceptions."""
    tables = _tables(feed.path)
    ymd = day.strftime("%Y%m%d")
    active: set[str] = set()

    calendar = tables["calendar"]
    if not calendar.empty:
        weekday = day.strftime("%A").lower()
        runs = (calendar[weekday] == "1") & (calendar.start_date <= ymd) & (calendar.end_date >= ymd)
        active |= set(calendar.loc[runs, "service_id"])

    exceptions = tables["calendar_dates"]
    if not exceptions.empty:
        today = exceptions[exceptions.date == ymd]
        active |= set(today.loc[today.exception_type == "1", "service_id"])
        active -= set(today.loc[today.exception_type == "2", "service_id"])
    return frozenset(active)


@lru_cache(maxsize=256)
def _stop_calls(folder: Path, service_ids: frozenset[str]) -> pd.DataFrame:
    tables = _tables(folder)
    trips = tables["trips"]
    trips = trips[trips.service_id.isin(service_ids)]
    # The realtime feed reports the public train number (VehicleRef), and
    # modified-service trips such as "M101" run as "101".
    number = trips.get("trip_short_name", pd.Series("", index=trips.index))
    trips = trips.assign(train=number.where(number != "", trips.trip_id).str.lstrip("M"))
    calls = tables["stop_times"].merge(trips[["trip_id", "train"]], on="trip_id")
    return pd.DataFrame({
        "trip_id": calls.train,
        "stop_id": calls.stop_id,
        "stop_sequence": calls.stop_sequence.astype(int),
        "arrival_time": calls.arrival_time,
    })


def schedule_for_dates(dates: Iterable[date], root: Path = FEEDS_DIR) -> pd.DataFrame:
    """Scheduled stop-calls for each date, taken from the feed in effect that day.

    Returns columns ``date, trip_id, stop_id, stop_sequence, arrival_time,
    feed_version`` with ``trip_id`` holding the public train number. Dates that
    no feed covers are simply absent.
    """
    feeds = discover_feeds(root)
    parts = []
    for day in sorted(set(dates)):
        feed = feed_for_date(day, feeds)
        if feed is None:
            continue
        calls = _stop_calls(feed.path, active_service_ids(feed, day))
        if not calls.empty:
            parts.append(calls.assign(date=day, feed_version=feed.version))
    if not parts:
        return pd.DataFrame(columns=SCHEDULE_COLUMNS)
    schedule = pd.concat(parts, ignore_index=True)[SCHEDULE_COLUMNS]
    return schedule.drop_duplicates(["date", "trip_id", "stop_id"], ignore_index=True)


def load_stops(root: Path = FEEDS_DIR) -> pd.DataFrame:
    """Every stop across all feeds; when feeds disagree, the newest timetable's definition wins."""
    feeds = sorted(discover_feeds(root), key=lambda f: (f.start, f.historic, f.version))
    frames = [_tables(f.path)["stops"].reindex(columns=STOP_COLUMNS) for f in feeds]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=STOP_COLUMNS)
    stops = pd.concat(frames, ignore_index=True).drop_duplicates("stop_id", keep="last")
    stops = stops[(stops.stop_lat != "") & (stops.stop_lon != "")]
    return stops.astype({"stop_lat": float, "stop_lon": float}).reset_index(drop=True)


def install_feed(source: Path, root: Path = FEEDS_DIR) -> tuple[Feed, bool]:
    """Copy an extracted GTFS folder into the store as ``v<feed_version>``.

    Returns the stored feed and whether it was new. Existing versions are never
    overwritten, so the store is an append-only history.
    """
    info = read_feed_info(source)
    if info is None:
        raise ValueError(f"{source} has no usable feed_info.txt")
    root = Path(root)
    dest = root / f"v{info.version}"
    if dest.exists():
        return read_feed_info(dest), False
    root.mkdir(parents=True, exist_ok=True)
    staging = root / f".{dest.name}.partial"
    shutil.rmtree(staging, ignore_errors=True)
    shutil.copytree(source, staging)
    staging.rename(dest)
    return read_feed_info(dest), True


def replace_current(feed: Feed, current: Path = CURRENT_DIR) -> None:
    """Make ``gtfs_data/`` a copy of ``feed``.

    Files are swapped one at a time with ``os.replace`` rather than renaming the
    directory, because ``gtfs_data`` is a bind mount inside the containers.
    """
    current = Path(current)
    current.mkdir(parents=True, exist_ok=True)
    for src in feed.path.glob("*.txt"):
        tmp = current / f".{src.name}.tmp"
        shutil.copy2(src, tmp)
        os.replace(tmp, current / src.name)
    for stale in current.glob("*.txt"):
        if not (feed.path / stale.name).exists():
            stale.unlink()


def download_current_feed(api_key: str, operator_id: str = "CT", root: Path = FEEDS_DIR,
                          current: Path = CURRENT_DIR) -> tuple[Feed, bool]:
    """Fetch 511's current published feed, add it to the store if new, and refresh ``gtfs_data/``."""
    resp = requests.get(DATAFEEDS_URL, params={"api_key": api_key, "operator_id": operator_id}, timeout=120)
    resp.raise_for_status()
    with tempfile.TemporaryDirectory(prefix="gtfs_") as tmp:
        zipfile.ZipFile(io.BytesIO(resp.content)).extractall(tmp)
        feed, is_new = install_feed(Path(tmp), root)

    in_use = read_feed_info(current)
    if in_use is None or in_use.version != feed.version:
        replace_current(feed, current)
    return feed, is_new
