#!/usr/bin/env python3
"""
Export Caltrain data to MyWebsite repository for static deployment.

This script copies generated stats and plots from the caltrain-tracker
static directory to the MyWebsite repo, then commits and pushes changes.

Usage:
    python export_to_website.py

Prerequisites:
    - Clone MyWebsite repo to ~/website-deploy:
      git clone git@github.com:Jsakkos/MyWebsite.git ~/website-deploy
"""

import json
import shutil
import subprocess
import os
from datetime import datetime
from pathlib import Path

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent  # Go up from /scripts to repo root
WEBSITE_REPO_PATH = Path.home() / "website-deploy"
OUTPUT_DIR = WEBSITE_REPO_PATH / "public" / "data" / "caltrain"
SOURCE_PLOTS = PROJECT_ROOT / "static" / "plots"
SOURCE_DATA = PROJECT_ROOT / "static" / "data"
BUILD_PLOTS = ["daily_stats.html", "commute_delay.html"]  # written by src.pipeline.dashboard


def ensure_output_dirs():
    """Create output directories if they don't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "plots").mkdir(exist_ok=True)
    print(f"✓ Output directory ready: {OUTPUT_DIR}")


def export_dashboard_data():
    """Copy all dashboard JSON data files to website."""
    dashboard_files = [
        "stats.json",
        "daily_performance.json",
        "station_performance.json",
        "train_performance.json",
        "hourly_heatmap.json",
        "commute_analysis.json",
        "weekly_summary.json",
        "monthly_summary.json",
        "incidents.json",
        "incident_trajectories.json",
    ]
    count = 0
    for filename in dashboard_files:
        src = SOURCE_DATA / filename
        if src.exists():
            shutil.copy(src, OUTPUT_DIR / filename)
            count += 1
            print(f"  - Copied {filename}")
        else:
            print(f"  ⚠ Missing {filename}")
    print(f"✓ Exported {count}/{len(dashboard_files)} dashboard data files")


def export_plots():
    """Copy the plots the nightly build writes, and remove any others from the website.

    static/plots also holds plots from the retired Prefect flows that are never
    regenerated; exporting them would publish stale charts.
    """
    plots_dir = OUTPUT_DIR / "plots"
    count = 0
    for name in BUILD_PLOTS:
        src = SOURCE_PLOTS / name
        if src.exists():
            shutil.copy(src, plots_dir / name)
            count += 1
            print(f"  - Copied {name}")
        else:
            print(f"  ⚠ Missing {name}")
    for stale in plots_dir.glob("*.html"):
        if stale.name not in BUILD_PLOTS:
            stale.unlink()
            print(f"  - Removed stale {stale.name}")
    print(f"✓ Exported {count}/{len(BUILD_PLOTS)} plot(s)")


def export_metadata():
    """Create metadata file with export timestamp."""
    metadata = {
        "last_updated": datetime.now().isoformat(),
        "source": "caltrain-prefect",
        "version": "1.0"
    }
    (OUTPUT_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2))
    print(f"✓ Created metadata.json")


def git_push():
    """Commit and push changes to website repo."""
    os.chdir(WEBSITE_REPO_PATH)
    
    # Add changes first (before pulling)
    subprocess.run(["git", "add", "public/data/caltrain"], check=True)
    
    # Check if there are changes to commit
    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    
    if result.returncode == 0:
        print("No changes to push")
        return False
    
    # Commit the changes
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    subprocess.run([
        "git", "commit", "-m", f"chore: update Caltrain data - {timestamp}"
    ], check=True)
    
    # Pull with rebase to integrate any remote changes
    print("Pulling latest changes...")
    pull_result = subprocess.run(["git", "pull", "--rebase"])
    
    if pull_result.returncode != 0:
        print("⚠ Pull failed, attempting to push anyway...")
    
    # Push
    subprocess.run(["git", "push"], check=True)
    print(f"✓ Pushed to website repo at {timestamp}")
    return True


def main():
    """Main export workflow."""
    print("=" * 50)
    print("Caltrain Data Export to Website")
    print("=" * 50)
    
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Source plots: {SOURCE_PLOTS}")
    print(f"Source data: {SOURCE_DATA}")
    
    # Check if website repo exists
    if not WEBSITE_REPO_PATH.exists():
        print(f"ERROR: Website repo not found at {WEBSITE_REPO_PATH}")
        print("Please clone the repo first:")
        print(f"  git clone git@github.com:Jsakkos/MyWebsite.git {WEBSITE_REPO_PATH}")
        return False
    
    # Run export steps
    ensure_output_dirs()
    export_dashboard_data()
    export_plots()
    export_metadata()
    
    # Commit and push
    print("\nCommitting and pushing changes...")
    return git_push()


if __name__ == "__main__":
    success = main()
    print("\n" + "=" * 50)
    if success:
        print("Export completed successfully!")
    else:
        print("Export completed (no changes to push)")
