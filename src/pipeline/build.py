"""
Nightly build: refresh the arrivals table, then rewrite the website files.

    python -m src.pipeline.build            # incremental (cron)
    python -m src.pipeline.build --full     # recompute every day, e.g. after changing scoring rules

Writes static/data/*.json and static/plots/{daily_stats,commute_delays}.html (git-ignored),
which scripts/export_to_website.py copies to the website at 02:30.
"""
import argparse
import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src.data.gtfs_feeds import FEEDS_DIR
from src.pipeline.arrivals import load_arrivals, open_store, update_arrivals
from src.pipeline.dashboard import write_dashboard, write_json_atomic
from src.pipeline.incidents import detect_incidents
from src.utils.geo_utils import load_shape_points

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_TZ = ZoneInfo("America/Los_Angeles")

log = logging.getLogger("build")


def local_now(utc_now: datetime | None = None) -> datetime:
    """Pacific wall time, like the stored pings. The server clock is UTC, so
    date.today() there would roll over at 17:00 Pacific and skip recomputing
    the previous (partial) Pacific day."""
    return (utc_now or datetime.now(timezone.utc)).astimezone(LOCAL_TZ).replace(tzinfo=None)


def gps_loader(store):
    """GPS pings for the given days, in stored order, from the attached ``pings`` view."""
    def load(days: list[date]) -> pd.DataFrame:
        store.register("incident_days", pd.DataFrame({"date": days}))
        try:
            return store.execute("""
                SELECT trip_id, lat AS vehicle_lat, lon AS vehicle_lon, ts AS timestamp
                FROM pings JOIN incident_days USING (date) ORDER BY id
            """).df()
        finally:
            store.unregister("incident_days")
    return load


def build(db: Path, store_path: Path, out: Path, feeds_root: Path = FEEDS_DIR,
          gtfs_dir: Path = BASE_DIR / "gtfs_data",
          station_meta_path: Path = BASE_DIR / "data" / "station_metadata.json",
          full: bool = False, now: datetime | None = None) -> dict:
    now = now or local_now()
    timings = {}

    started = time.monotonic()
    store = open_store(str(store_path))
    try:
        days = update_arrivals(store, str(db), feeds_root, full=full, today=now.date())
        arrivals = load_arrivals(store)
        timings["arrivals"] = time.monotonic() - started
        log.info("Recomputed %d day(s); %d arrivals in total", len(days), len(arrivals))
        if arrivals.empty:
            log.warning("No arrivals; leaving the existing website files alone")
            return {"days": days, "arrivals": 0, "timings": timings}

        started = time.monotonic()
        write_dashboard(out, arrivals, now)
        timings["dashboard"] = time.monotonic() - started

        started = time.monotonic()
        station_meta = json.loads(Path(station_meta_path).read_text(encoding="utf-8"))["stations"]
        shape = load_shape_points(str(gtfs_dir))
        incidents, trajectories = detect_incidents(arrivals, gps_loader(store), station_meta, shape)
        write_json_atomic(Path(out) / "data" / "incidents.json", incidents, indent=2)
        write_json_atomic(Path(out) / "data" / "incident_trajectories.json", trajectories)
        timings["incidents"] = time.monotonic() - started
        log.info("%d incidents", len(incidents))
    finally:
        store.close()

    log.info("Timings: %s", ", ".join(f"{k} {v:.1f}s" for k, v in timings.items()))
    return {"days": days, "arrivals": len(arrivals), "incidents": len(incidents), "timings": timings}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--full", action="store_true", help="recompute every day")
    parser.add_argument("--db", type=Path, default=BASE_DIR / "data" / "caltrain_lat_long.db")
    parser.add_argument("--store", type=Path, default=BASE_DIR / "data" / "analytics.duckdb")
    parser.add_argument("--out", type=Path, default=BASE_DIR / "static")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    build(args.db, args.store, args.out, full=args.full)


if __name__ == "__main__":
    main()
