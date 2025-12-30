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
WEBSITE_REPO_PATH = Path.home() / "website-deploy"
OUTPUT_DIR = WEBSITE_REPO_PATH / "public" / "data" / "caltrain"
SOURCE_PLOTS = Path("static/plots")
SOURCE_DATA = Path("static/data")


def ensure_output_dirs():
    """Create output directories if they don't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "plots").mkdir(exist_ok=True)
    print(f"✓ Output directory ready: {OUTPUT_DIR}")


def export_stats():
    """Copy summary_stats.json to website as stats.json."""
    src = SOURCE_DATA / "summary_stats.json"
    if src.exists():
        shutil.copy(src, OUTPUT_DIR / "stats.json")
        print(f"✓ Exported stats.json")
    else:
        print(f"⚠ No summary_stats.json found at {src}")


def export_plots():
    """Copy Plotly HTML plots to website."""
    plots_dir = OUTPUT_DIR / "plots"
    if SOURCE_PLOTS.exists():
        count = 0
        for plot in SOURCE_PLOTS.glob("*.html"):
            shutil.copy(plot, plots_dir / plot.name)
            count += 1
            print(f"  - Copied {plot.name}")
        print(f"✓ Exported {count} plot(s)")
    else:
        print(f"⚠ No plots directory found at {SOURCE_PLOTS}")


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
    
    # Pull latest to avoid conflicts
    print("Pulling latest changes...")
    subprocess.run(["git", "pull", "--rebase"], check=True)
    
    # Add changes
    subprocess.run(["git", "add", "public/data/caltrain"], check=True)
    
    # Check if there are changes to commit
    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    
    if result.returncode != 0:  # There are changes
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        subprocess.run([
            "git", "commit", "-m", f"chore: update Caltrain data - {timestamp}"
        ], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"✓ Pushed to website repo at {timestamp}")
        return True
    else:
        print("No changes to push")
        return False


def main():
    """Main export workflow."""
    print("=" * 50)
    print("Caltrain Data Export to Website")
    print("=" * 50)
    
    # Ensure we're in the caltrain-tracker directory
    script_dir = Path(__file__).parent.resolve()
    os.chdir(script_dir)
    print(f"Working from: {script_dir}")
    
    # Check if website repo exists
    if not WEBSITE_REPO_PATH.exists():
        print(f"ERROR: Website repo not found at {WEBSITE_REPO_PATH}")
        print("Please clone the repo first:")
        print(f"  git clone git@github.com:Jsakkos/MyWebsite.git {WEBSITE_REPO_PATH}")
        return False
    
    # Run export steps
    ensure_output_dirs()
    export_stats()
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
