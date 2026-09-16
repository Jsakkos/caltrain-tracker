"""
Backfill historical Caltrain schedules from 511.org's monthly GTFS archives.

511's ``datafeeds?historic=YYYY-MM`` endpoint returns the whole Bay Area regional
feed for a month (~600 MB zipped, IDs prefixed by agency, e.g. ``CT:101:20250831``).
Its ``calendar_dates.txt`` lists every date each trip ran, so it records the
schedule actually in service, including holidays and mid-month timetable changes.

This script keeps only the Caltrain rows and writes them to
``gtfs_feeds/historic-YYYY-MM/`` as a normal single-agency GTFS feed (prefixes
stripped, synthesized ``feed_info.txt``). The Caltrain rows of 511's
``stop_observations.txt`` (observed arrival times) are kept as well.

Usage:
    python scripts/backfill_gtfs_history.py --start 2025-08 --end 2026-08
    python scripts/backfill_gtfs_history.py --start 2025-08 --zip-dir /tmp/gtfs_hist --keep-zips
"""
import argparse
import csv
import io
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.gtfs_feeds import BASE_DIR, DATAFEEDS_URL, FEEDS_DIR  # noqa: E402

# Columns holding agency-prefixed IDs, per file
PREFIXED = {
    "agency.txt": ["agency_id"],
    "routes.txt": ["route_id", "agency_id"],
    "directions.txt": ["route_id"],
    "trips.txt": ["route_id", "service_id", "trip_id"],
    "calendar_dates.txt": ["service_id"],
    "stop_times.txt": ["trip_id"],
    "stop_observations.txt": ["trip_id", "route_id", "agency_id"],
}


def month_range(start: str, end: str) -> list[str]:
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def last_full_month() -> str:
    today = date.today()
    y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    return f"{y:04d}-{m:02d}"


def download_archive(api_key: str, month: str, dest: Path) -> bool:
    """Stream one monthly archive to disk. Returns False if 511 has no archive for that month."""
    params = {"api_key": api_key, "operator_id": "CT", "historic": month}
    with requests.get(DATAFEEDS_URL, params=params, stream=True, timeout=600) as resp:
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    return True


def _rows(archive: zipfile.ZipFile, name: str):
    with archive.open(name) as f:
        yield from csv.reader(io.TextIOWrapper(f, "utf-8-sig", newline=""))


def extract_operator(zip_path: Path, dest: Path, month: str, operator: str = "CT") -> dict[str, int]:
    """Write the ``operator`` subset of a regional archive to ``dest`` as a standalone GTFS feed."""
    prefix = f"{operator}:"
    unprefix = lambda v: v[len(prefix):] if v.startswith(prefix) else v
    archive = zipfile.ZipFile(zip_path)
    names = set(archive.namelist())
    dest.mkdir(parents=True)
    counts: dict[str, int] = {}

    def copy(name: str, keep) -> None:
        if name not in names:
            return
        rows = _rows(archive, name)
        header = next(rows)
        idx = {col: header.index(col) for col in PREFIXED.get(name, []) if col in header}
        n = 0
        with open(dest / name, "w", encoding="utf-8", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(header)
            for row in rows:
                if keep(dict(zip(header, row))):
                    for i in idx.values():
                        row[i] = unprefix(row[i])
                    writer.writerow(row)
                    n += 1
        counts[name] = n

    ours = lambda col: (lambda r: r.get(col, "").startswith(prefix))
    copy("agency.txt", lambda r: r["agency_id"] == operator)
    copy("routes.txt", lambda r: r["agency_id"] == operator)
    copy("directions.txt", ours("route_id"))
    copy("trips.txt", ours("route_id"))

    service_dates: list[str] = []
    copy("calendar_dates.txt", lambda r: r["service_id"].startswith(prefix) and not service_dates.append(r["date"]))

    stop_ids: set[str] = set()
    copy("stop_times.txt", lambda r: r["trip_id"].startswith(prefix) and not stop_ids.add(r["stop_id"]))

    # stops.txt is small: keep served platforms plus their parent stations
    rows = _rows(archive, "stops.txt")
    header = next(rows)
    stops = [dict(zip(header, r)) for r in rows]
    parents = {s["parent_station"] for s in stops if s["stop_id"] in stop_ids and s.get("parent_station")}
    with open(dest / "stops.txt", "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=header)
        writer.writeheader()
        kept = [s for s in stops if s["stop_id"] in stop_ids | parents]
        writer.writerows(kept)
    counts["stops.txt"] = len(kept)

    copy("stop_observations.txt", lambda r: r.get("agency_id") == operator)

    if not service_dates:
        raise ValueError(f"no {operator} services found in {zip_path}")
    with open(dest / "feed_info.txt", "w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["feed_publisher_name", "feed_publisher_url", "feed_lang",
                         "feed_start_date", "feed_end_date", "feed_version"])
        writer.writerow(["511 SF Bay (monthly regional archive)", "https://511.org/open-data/transit", "en",
                         min(service_dates), max(service_dates), f"historic-{month}"])
    return counts


def backfill_month(month: str, api_key: str | None, zip_dir: Path | None, keep_zips: bool, force: bool) -> str:
    dest = FEEDS_DIR / f"historic-{month}"
    if dest.exists() and not force:
        return "exists, skipped"

    cached = zip_dir / f"CT_{month}.zip" if zip_dir else None
    workdir = Path(tempfile.mkdtemp(prefix=f"gtfs_{month}_"))
    try:
        if cached and cached.exists():
            zip_path = cached
        else:
            if not api_key:
                raise SystemExit("API_KEY is required to download archives (set it in .env or the environment)")
            zip_path = (zip_dir if keep_zips and zip_dir else workdir) / f"CT_{month}.zip"
            zip_path.parent.mkdir(parents=True, exist_ok=True)
            if not download_archive(api_key, month, zip_path):
                return "no archive published yet"

        staging = FEEDS_DIR / f".historic-{month}.partial"
        shutil.rmtree(staging, ignore_errors=True)
        FEEDS_DIR.mkdir(parents=True, exist_ok=True)
        counts = extract_operator(zip_path, staging, month)
        shutil.rmtree(dest, ignore_errors=True)
        staging.rename(dest)
        if cached and cached.exists() and not keep_zips:
            cached.unlink()
        return ", ".join(f"{k.removesuffix('.txt')}={v:,}" for k, v in counts.items())
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--start", required=True, help="first month, YYYY-MM")
    parser.add_argument("--end", default=last_full_month(), help="last month, YYYY-MM (default: last full month)")
    parser.add_argument("--zip-dir", type=Path, help="reuse archives named CT_YYYY-MM.zip from this folder")
    parser.add_argument("--keep-zips", action="store_true", help="keep downloaded/cached archives")
    parser.add_argument("--force", action="store_true", help="re-extract months that already exist")
    args = parser.parse_args()

    load_dotenv(BASE_DIR / ".env")
    api_key = os.environ.get("API_KEY")
    failures = 0
    for month in month_range(args.start, args.end):
        try:
            print(f"{month}: {backfill_month(month, api_key, args.zip_dir, args.keep_zips, args.force)}", flush=True)
        except Exception as exc:  # keep going; report at the end
            failures += 1
            print(f"{month}: FAILED - {exc}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
