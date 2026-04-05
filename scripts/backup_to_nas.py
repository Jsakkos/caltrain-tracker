#!/usr/bin/env python3
"""
Backup Caltrain SQLite database and GTFS data to NAS.

Uses SQLite's backup API for safe, consistent snapshots even while
the database is being written to. Implements rotation: 7 daily,
4 weekly (Sundays), 6 monthly (1st of month).

Usage:
    python scripts/backup_to_nas.py

Cron (daily at 2 AM):
    0 2 * * * /usr/bin/python3 /home/jsakkos/caltrain-prefect/scripts/backup_to_nas.py
"""

import gzip
import json
import logging
import shutil
import sqlite3
import sys
import tarfile
import tempfile
from datetime import datetime, date
from pathlib import Path

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "caltrain_lat_long.db"
GTFS_DIR = PROJECT_ROOT / "gtfs_data"
NAS_BACKUP_ROOT = Path("/nfs/share/backups/caltrain")

DAILY_DIR = NAS_BACKUP_ROOT / "daily"
WEEKLY_DIR = NAS_BACKUP_ROOT / "weekly"
MONTHLY_DIR = NAS_BACKUP_ROOT / "monthly"
BACKUP_LOG = NAS_BACKUP_ROOT / "backup_log.json"

# Retention policy
MAX_DAILY = 7
MAX_WEEKLY = 4
MAX_MONTHLY = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("caltrain-backup")


def check_nfs_mount() -> bool:
    """Verify NAS is accessible by writing a health check file."""
    try:
        NAS_BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
        healthcheck = NAS_BACKUP_ROOT / ".healthcheck"
        healthcheck.write_text(datetime.now().isoformat())
        healthcheck.unlink()
        return True
    except OSError as e:
        log.error(f"NAS not accessible at {NAS_BACKUP_ROOT}: {e}")
        return False


def backup_sqlite(dest_path: Path) -> int:
    """Backup SQLite database using the safe backup API, then gzip.

    Returns the compressed file size in bytes.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        # Use SQLite backup API for a consistent snapshot
        src = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        dst = sqlite3.connect(str(tmp_path))
        src.backup(dst)
        dst.close()
        src.close()

        # Gzip the backup
        with open(tmp_path, "rb") as f_in:
            with gzip.open(dest_path, "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
    finally:
        tmp_path.unlink(missing_ok=True)

    size = dest_path.stat().st_size
    log.info(f"SQLite backup: {size / 1024 / 1024:.1f} MB -> {dest_path.name}")
    return size


def backup_gtfs(dest_path: Path) -> int:
    """Create a tar.gz archive of the GTFS data directory.

    Returns the compressed file size in bytes.
    """
    if not GTFS_DIR.exists():
        log.warning(f"GTFS directory not found: {GTFS_DIR}, skipping")
        return 0

    with tarfile.open(dest_path, "w:gz") as tar:
        tar.add(GTFS_DIR, arcname="gtfs_data")

    size = dest_path.stat().st_size
    log.info(f"GTFS backup: {size / 1024 / 1024:.1f} MB -> {dest_path.name}")
    return size


def rotate_backups(directory: Path, max_count: int):
    """Remove oldest backups beyond max_count, keeping newest."""
    if not directory.exists():
        return

    # Group files by date stem (e.g., "caltrain_db_2026-04-04")
    files = sorted(directory.glob("caltrain_db_*.db.gz"), reverse=True)
    for old_file in files[max_count:]:
        old_file.unlink()
        # Also remove corresponding GTFS backup if it exists
        gtfs_file = old_file.parent / old_file.name.replace("caltrain_db_", "caltrain_gtfs_").replace(".db.gz", ".tar.gz")
        if gtfs_file.exists():
            gtfs_file.unlink()
        log.info(f"Rotated out: {old_file.name}")


def promote_backup(src_dir: Path, dest_dir: Path, date_str: str):
    """Copy a daily backup to weekly or monthly directory."""
    dest_dir.mkdir(parents=True, exist_ok=True)

    db_src = src_dir / f"caltrain_db_{date_str}.db.gz"
    if db_src.exists():
        shutil.copy2(db_src, dest_dir / db_src.name)
        log.info(f"Promoted {db_src.name} -> {dest_dir.name}/")

    gtfs_src = src_dir / f"caltrain_gtfs_{date_str}.tar.gz"
    if gtfs_src.exists():
        shutil.copy2(gtfs_src, dest_dir / gtfs_src.name)


def update_log(entry: dict):
    """Append an entry to the backup log."""
    entries = []
    if BACKUP_LOG.exists():
        try:
            entries = json.loads(BACKUP_LOG.read_text())
        except (json.JSONDecodeError, OSError):
            entries = []

    entries.append(entry)

    # Keep last 200 entries
    entries = entries[-200:]
    BACKUP_LOG.write_text(json.dumps(entries, indent=2))


def main() -> int:
    start_time = datetime.now()
    today = date.today()
    date_str = today.isoformat()

    log.info("=" * 50)
    log.info("Caltrain Backup Started")
    log.info(f"Database: {DB_PATH} ({DB_PATH.stat().st_size / 1024 / 1024:.0f} MB)")
    log.info("=" * 50)

    # Check NAS accessibility
    if not check_nfs_mount():
        log.error("ABORTING: NAS is not accessible")
        return 1

    # Create directories
    DAILY_DIR.mkdir(parents=True, exist_ok=True)

    log_entry = {
        "date": date_str,
        "timestamp": start_time.isoformat(),
        "status": "started",
    }

    try:
        # Backup SQLite database
        db_dest = DAILY_DIR / f"caltrain_db_{date_str}.db.gz"
        db_size = backup_sqlite(db_dest)
        log_entry["db_size_bytes"] = db_size

        # Backup GTFS data
        gtfs_dest = DAILY_DIR / f"caltrain_gtfs_{date_str}.tar.gz"
        gtfs_size = backup_gtfs(gtfs_dest)
        log_entry["gtfs_size_bytes"] = gtfs_size

        # Promote to weekly (Sunday = 6)
        if today.weekday() == 6:
            promote_backup(DAILY_DIR, WEEKLY_DIR, date_str)
            log_entry["promoted_weekly"] = True

        # Promote to monthly (1st of month)
        if today.day == 1:
            promote_backup(DAILY_DIR, MONTHLY_DIR, date_str)
            log_entry["promoted_monthly"] = True

        # Rotate old backups
        rotate_backups(DAILY_DIR, MAX_DAILY)
        rotate_backups(WEEKLY_DIR, MAX_WEEKLY)
        rotate_backups(MONTHLY_DIR, MAX_MONTHLY)

        elapsed = (datetime.now() - start_time).total_seconds()
        log_entry["status"] = "success"
        log_entry["duration_seconds"] = round(elapsed, 1)

        log.info(f"Backup completed in {elapsed:.1f}s")
        update_log(log_entry)
        return 0

    except Exception as e:
        log.error(f"Backup FAILED: {e}")
        log_entry["status"] = "failed"
        log_entry["error"] = str(e)
        update_log(log_entry)
        return 1


if __name__ == "__main__":
    sys.exit(main())
