"""
GTFS Schedule Update Checker.

Downloads 511.org's current Caltrain feed, stores it under gtfs_feeds/v<feed_version>/
if it's a version we haven't seen, and refreshes gtfs_data/ to match.

In production this runs daily as the "Update GTFS Schedule" Prefect flow
(src/flows/gtfs_update.py); this script is for running the same check by hand.
"""
import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import API_KEY, CALTRAIN_AGENCY_CODE
from src.data.gtfs_feeds import download_current_feed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Check for and store a new GTFS schedule version')
    parser.add_argument(
        '--ingest-db',
        action='store_true',
        help='Also load the feed into the versioned gtfs_* database tables'
    )
    args = parser.parse_args()

    try:
        feed, is_new = download_current_feed(API_KEY, CALTRAIN_AGENCY_CODE)
    except Exception as e:
        logger.error(f"GTFS update failed: {e}")
        sys.exit(1)

    logger.info(f"Feed {feed.version} ({feed.start} to {feed.end}): "
                f"{'new version stored' if is_new else 'already stored'} at {feed.path}")

    if args.ingest_db:
        from src.data.ingest_gtfs_schedule import ingest_gtfs_schedule
        if not ingest_gtfs_schedule(str(feed.path)):
            sys.exit(1)


if __name__ == '__main__':
    main()
