#!/usr/bin/env python3
"""
Fail loudly if the collector has stopped writing pings.

Run from cron every 15 minutes. Exits 1 when the newest row in train_locations
is more than 10 minutes old while trains are running (05:00-01:00 Pacific).
If HEALTHCHECK_URL is set (e.g. a healthchecks.io check), it is pinged on
success and <url>/fail on failure, so a dead collector sends an email.
"""
import os
import sqlite3
import sys
import urllib.request
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "caltrain_lat_long.db"
LOCAL_TZ = ZoneInfo("America/Los_Angeles")
MAX_AGE = timedelta(minutes=10)
QUIET_START, QUIET_END = time(1, 0), time(5, 0)  # no trains in service


def latest_ping(db_path: str) -> datetime | None:
    # Newest by rowid: instant, unlike max(timestamp), which has no index.
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT timestamp FROM train_locations ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    return datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f") if row else None


def is_stale(latest: datetime | None, now: datetime) -> bool:
    if QUIET_START <= now.time() < QUIET_END:
        return False
    return latest is None or now - latest > MAX_AGE


def _ping(url: str) -> None:
    try:
        urllib.request.urlopen(url, timeout=10).close()
    except OSError as e:
        print(f"Healthcheck ping failed: {e}")


def main() -> int:
    now = datetime.now(timezone.utc).astimezone(LOCAL_TZ).replace(tzinfo=None)
    latest = latest_ping(os.environ.get("DB_PATH", str(DB_PATH)))
    stale = is_stale(latest, now)
    print(f"{now:%Y-%m-%d %H:%M} {'STALE' if stale else 'OK'}: newest ping {latest}")
    url = os.environ.get("HEALTHCHECK_URL")
    if url:
        _ping(url.rstrip("/") + ("/fail" if stale else ""))
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
