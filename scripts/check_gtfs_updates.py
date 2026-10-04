"""
GTFS Schedule Update Checker.

Downloads 511.org's current Caltrain feed, stores it under gtfs_feeds/v<feed_version>/
if it's a version we haven't seen, and refreshes gtfs_data/ to match.

Runs daily from host cron (deploy/crontab.txt), ahead of the nightly build.
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
    argparse.ArgumentParser(description='Check for and store a new GTFS schedule version').parse_args()

    try:
        feed, is_new = download_current_feed(API_KEY, CALTRAIN_AGENCY_CODE)
    except Exception as e:
        logger.error(f"GTFS update failed: {e}")
        sys.exit(1)

    logger.info(f"Feed {feed.version} ({feed.start} to {feed.end}): "
                f"{'new version stored' if is_new else 'already stored'} at {feed.path}")


if __name__ == '__main__':
    main()
