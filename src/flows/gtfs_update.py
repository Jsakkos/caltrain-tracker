"""
Prefect flow that keeps the GTFS schedule current.
"""
import os
import sys

from prefect import flow, task

# Add the project root to the path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.config import API_KEY
from src.data.gtfs_feeds import download_current_feed


@task(retries=3, retry_delay_seconds=600)
def refresh_schedule() -> dict:
    """
    Download 511's current Caltrain feed, store it under gtfs_feeds/ if it's a new
    version, and make sure gtfs_data/ matches it.
    """
    feed, is_new = download_current_feed(API_KEY)
    status = "new version stored" if is_new else "already stored"
    print(f"GTFS feed {feed.version} ({feed.start} to {feed.end}): {status}")
    return {"feed_version": feed.version, "is_new": is_new}


@flow(name="Update GTFS Schedule", log_prints=True)
def update_gtfs_schedule_flow():
    """
    Daily check for a new published schedule.
    """
    return refresh_schedule()


if __name__ == "__main__":
    update_gtfs_schedule_flow.serve(name="update-gtfs-schedule", cron="30 23 * * *")
