# Caltrain On-Time Performance Tracker

## Overview

Tracks Caltrain on-time performance. A small collector records every train's GPS position from the 511.org real-time API once a minute; a nightly build turns those pings into arrivals, scores them against the schedule that was in effect that day, and writes the JSON and plots behind the dashboard on the MyWebsite site.

## Architecture

```
511 VehicleMonitoring ──(every 60 s)──> collector container ──> data/caltrain_lat_long.db (SQLite)
                                                                        │
511 datafeeds ──(cron 01:30 UTC)──> scripts/check_gtfs_updates.py ──> gtfs_feeds/, gtfs_data/
                                                                        │
                        cron 02:00 UTC: python -m src.pipeline.build  <─┘
                          · DuckDB reads SQLite directly; arrivals kept in data/analytics.duckdb
                          · writes static/data/*.json and static/plots/{daily_stats,commute_delay}.html
                                                                        │
cron 02:15 UTC: scripts/backup_to_nas.py      cron 02:30 UTC: scripts/export_to_website.py ──> MyWebsite (Netlify)
```

- **Collector** (`src/collector.py`, `Dockerfile.collector`): stdlib-only, ~20 MB of RAM. One request a minute and no retries within a minute, because 511 allows 60 requests per hour per API key. Never run a second poller on the same key.
- **Nightly build** (`src/pipeline/`): `arrivals.py` recomputes only days that are new, the last two Pacific days, or days whose schedule version changed (for example after a monthly archive backfill). `dashboard.py` and `incidents.py` write the website files. A full rebuild of 14 months takes about 15 s.
- **Scheduling**: host cron (`deploy/crontab.txt`). There's no orchestrator.

## Setup (pve-docker)

1. Get a 511.org API key at https://511.org/open-data/token and put it in `.env` (see `.env.example`).
2. Start the collector: `docker compose up -d --build collector`
3. Install Python deps for the cron jobs: `uv sync --no-dev`
4. Build once from scratch: `.venv/bin/python -m src.pipeline.build --full`
5. Install the crontab: `crontab deploy/crontab.txt`

## Development

```bash
uv sync
uv run pytest
```

## Project Structure

```
├── src/
│   ├── collector.py         # 511 poller (runs in the collector container)
│   ├── pipeline/            # nightly DuckDB build: arrivals, dashboard files, incidents
│   ├── data/gtfs_feeds.py   # versioned GTFS schedules, picked per date
│   ├── utils/               # geo (route projection) and time helpers
│   └── config.py            # API key loading for the 511 schedule scripts
├── scripts/                 # GTFS update/backfill, freshness check, NAS backup, website export, parity check
├── deploy/crontab.txt       # every scheduled job
├── static/                  # build output: data/*.json, plots/*.html
├── gtfs_data/               # current GTFS static feed (refreshed daily)
├── gtfs_feeds/              # every GTFS schedule version, matched to arrivals by date
├── tests/                   # pytest suite
└── docker-compose.yaml      # the collector service
```

# Methodology

## Data collection

All data was gathered from the 511.org transit API.

The list of stops and stop times were downloaded from the GTFS API here: http://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={OPERATOR}

### Schedules

Caltrain's timetable changes several times a year, so each arrival is compared against the schedule that was in effect on its date. Every schedule version is kept under `gtfs_feeds/`:

- `v<feed_version>/`: feeds published on the datafeeds endpoint. `scripts/check_gtfs_updates.py` checks for a new version every night (host cron), stores it, and refreshes `gtfs_data/`.
- `historic-YYYY-MM/`: the Caltrain rows of 511's monthly regional archive (`datafeeds?historic=YYYY-MM`), which lists the trips that actually ran on each day, holidays and mid-month changes included. These also keep 511's `stop_observations.txt` rows for Caltrain.

`src/data/gtfs_feeds.py` picks the schedule for a date: a monthly archive if one covers it, otherwise the newest published timetable in effect. Archives appear a few days after each month ends; to add them:

```
python scripts/backfill_gtfs_history.py --start 2025-08
```

Each archive download is roughly 600 MB, and extraction takes a few minutes per month.

Historical train position data was collected in every minute (per API restrictions) from the GTFS-RT Vehicle Monitoring API: https://api.511.org/transit/VehicleMonitoring?api_key={API_KEY}&agency={OPERATOR}

The GTFS-RT feed was parsed by vehicle to get train number, stop number, latitude, longitude, and the timestamp of when the data was collected, which was inserted into an SQLite database.
Train arrival detection

Since the raw data only contains the location of each train and the stop it's travelling towards, we need to determine when the trains arrive. The distance to each stop was calculated using the Haversine formula on the train lat/long and the arriving stop lat/long. Since the data is relatively sparse, to determine when a train had arrived, the row with the minimum distance to the stop for each train ID, date, and stop ID was used to indicate train arrival.
## Calculation of on-time performance

On-time performance was calculated on a per-stop, per-train basis. For each stop on a route, the train status is marked as delayed if the train arrives to the stop more than 4 minutes behind schedule. Minor delays are defined as delays between 5-14 minutes, and major delays are 15+ minutes.

## Commute time windows
### Morning
Morning commute hours were defined as 6-9 am.
### Evening 
Evening commute hours were defined as 3:30-7:30 pm.

# Operations

## Daily schedule (host cron, UTC; Pacific is UTC-7 in summer, UTC-8 in winter)

| UTC | Job | Log |
|---|---|---|
| every minute | `collector` container saves GPS pings to SQLite | `docker compose logs -f collector` |
| every 15 min | `scripts/check_freshness.py`: STALE if no ping for 10 min in service hours; pings `HEALTHCHECK_URL` if set | `~/caltrain-freshness.log` |
| 01:30 | `scripts/check_gtfs_updates.py`: store a new published schedule | `~/caltrain-gtfs.log` |
| 02:00 | `python -m src.pipeline.build`: refresh arrivals, rewrite `static/` | `~/caltrain-build.log` |
| 02:15 | `scripts/backup_to_nas.py` | `~/caltrain-backup.log` |
| 02:30 | `scripts/export_to_website.py`: copy `static/` to MyWebsite and push; Netlify deploys | `~/caltrain-export.log` |

## Manual runs

```bash
.venv/bin/python -m src.pipeline.build           # incremental, what cron runs
.venv/bin/python -m src.pipeline.build --full    # recompute every day (after changing scoring rules)
.venv/bin/python scripts/export_to_website.py     # push the current files to the website
```

## Scoring rules and parity

The build reproduces the legacy pandas flow exactly, including its quirks: GTFS times past 24:00 are wrapped onto the ping's own date, and the two plots weight arrivals by ping count. The rules are listed in `docs/superpowers/plans/2026-10-03-duckdb-build.md`. To check a change against a known-good output directory, run `python scripts/compare_outputs.py OLD/data NEW/data`.

## Dependency pinning

`pandas` is pinned below 3 (3.x turns on copy-on-write by default, which changes chained-assignment behaviour), and `numpy` below 2.5 (pandas 2.3 triggers its timedelta deprecations).
