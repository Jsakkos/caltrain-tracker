# Caltrain On-Time Performance Tracker

## Overview

A modernized application for tracking and analyzing Caltrain performance metrics. This project collects real-time train location data from the 511.org GTFS-RT API, processes it to determine arrival times and delays, and provides a web interface for visualizing the data.

## Key Features

- Real-time tracking of Caltrain locations and arrivals
- Historical analysis of on-time performance
- Visualization of delay patterns by time of day, day of week, and stop location
- REST API for programmatic access to Caltrain performance data
- Automated data collection (a small collector container) and processing (Prefect workflows)

## Architecture

The application is built using the following technologies:

- **FastAPI**: Modern, high-performance web framework for building APIs
- **Collector**: Stdlib-only Python process (`src/collector.py`) that polls 511 once a minute into SQLite
- **Prefect**: Workflow orchestration for the nightly processing and GTFS update flows
- **SQLite**: Primary data store for GPS/arrival data (`data/caltrain_lat_long.db`)
- **PostgreSQL**: Dedicated backend for Prefect's orchestration state (flow runs, task runs, deployments). Runs in its own container; not used for application data.
- **SQLAlchemy**: ORM for database interactions
- **Alembic**: Database migration tool
- **Plotly**: Interactive data visualizations
- **Docker**: Containerization for easy deployment

### Service topology

Four Docker Compose services:

- `collector` — polls the 511 VehicleMonitoring feed every 60 s and appends to `data/caltrain_lat_long.db`. No retries within a minute: 511 allows 60 requests/hour per API key, so never run a second poller on the same key. Logs: `docker compose logs -f collector`.
- `app` — FastAPI application (port 8181)
- `prefect` — Prefect server + worker running the data pipelines (port 4200). Uses the `postgres` service as its state backend via `PREFECT_API_DATABASE_CONNECTION_URL`.
- `postgres` — Postgres 16 for Prefect orchestration state only.

The `src/`, `static/`, and `data/` directories are bind-mounted into the containers, so code edits are live without a rebuild. Only dependency or Dockerfile changes require `docker compose build`.

## Getting Started

### Option 1: Using Docker Compose (Recommended)

1. Fork or clone this repository
2. Get a 511.org API key at https://511.org/open-data/token
3. Create a `.env` file in the root directory with:
   ```
   API_KEY="your-api-key"
   HOST=localhost
   DB_USER=prefect
   DB_PASSWORD=your-strong-password
   ```
4. Build and run the Docker containers:
   ```
   docker compose build
   docker compose up -d
   ```
5. Access the application:
   - Web UI: `http://localhost:8181`
   - API documentation: `http://localhost:8181/docs`
   - Prefect dashboard: `http://localhost:4200`

### Option 2: Local Development Setup

1. Clone the repository
2. Run the setup script to prepare your development environment:
   ```
   ./setup_dev.sh
   ```
3. Edit the `.env` file with your configuration
4. Run the application:
   ```
   python main.py
   ```

GPS data is written to a local SQLite file at `data/caltrain_lat_long.db`; no separate database server is required for application data. For a full containerized setup that also includes the Prefect Postgres backend, use Option 1.

## Project Structure

```
├── src/                     # Application source code
│   ├── api/                 # FastAPI models and endpoints
│   ├── data/                # Data processing utilities
│   ├── db/                  # Database connection and session handling
│   ├── models/              # SQLAlchemy data models
│   ├── pipelines/           # Prefect workflows for data collection and processing
│   ├── utils/               # Utility functions (time, geo, etc.)
│   └── config.py            # Application configuration
├── alembic/                 # Database migrations
├── static/                  # Static content (plots, data files)
│   ├── plots/               # Generated visualizations
│   └── data/                # Generated data files
├── gtfs_data/               # Current GTFS static feed (refreshed daily)
├── gtfs_feeds/              # Every GTFS schedule version, matched to arrivals by date
├── docker-compose.yaml      # Docker Compose configuration
├── Dockerfile               # Docker image definition
├── main.py                  # Application entry point
└── requirements.txt         # Python dependencies
```

# Methodology

## Data collection

All data was gathered from the 511.org transit API.

The list of stops and stop times were downloaded from the GTFS API here: http://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={OPERATOR}

### Schedules

Caltrain's timetable changes several times a year, so each arrival is compared against the schedule that was in effect on its date. Every schedule version is kept under `gtfs_feeds/`:

- `v<feed_version>/`: feeds published on the datafeeds endpoint. The "Update GTFS Schedule" Prefect flow checks for a new version every night at 23:30, stores it, and refreshes `gtfs_data/` (run `python scripts/check_gtfs_updates.py` to do the same by hand).
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

## Automated daily schedule

- Every minute: the `collector` container saves GPS data → SQLite
- 23:30 daily: `update_gtfs_schedule_flow` checks 511 for a new published schedule (Prefect)
- 00:00 daily: `process_data_flow` runs (Prefect schedule in the container) — regenerates processed CSV and the 10 dashboard JSON files under `static/data/`
- 02:15 AM: NAS backup (host cron → `scripts/backup_to_nas.py`)
- 02:30 AM: Website export (host cron → `scripts/export_to_website.py`) — copies JSON + plots to `~/website-deploy/public/data/caltrain/`, commits, and pushes; Netlify auto-deploys

## Manual pipeline runs

```bash
# Regenerate JSON from latest SQLite data
docker exec caltrain-prefect-prefect-1 python -c \
  "from src.flows.data_processing import process_data_flow; process_data_flow()"

# Push regenerated data to the live dashboard
python3 scripts/export_to_website.py
```

## Dependency pinning

`pandas` is pinned to `>=2.0.0,<3.0.0` in both `requirements.txt` and `requirements-prefect.txt`. Pandas 3.x enables Copy-on-Write by default, which silently breaks chained-assignment patterns (e.g. `df['col'].fillna(x, inplace=True)`). Do not remove the upper bound without auditing the codebase for those patterns first.
