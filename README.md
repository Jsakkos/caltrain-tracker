# Caltrain On-Time Performance Tracker

## Overview

A modernized application for tracking and analyzing Caltrain performance metrics. This project collects real-time train location data from the 511.org GTFS-RT API, processes it to determine arrival times and delays, and provides a web interface for visualizing the data.

## Key Features

- Real-time tracking of Caltrain locations and arrivals
- Historical analysis of on-time performance
- Visualization of delay patterns by time of day, day of week, and stop location
- REST API for programmatic access to Caltrain performance data
- Automated data collection and processing using Prefect workflows

## Architecture

The application is built using the following technologies:

- **FastAPI**: Modern, high-performance web framework for building APIs
- **Prefect**: Workflow orchestration for managing data collection and processing tasks
- **PostgreSQL**: Robust database for storing train location and performance data
- **SQLAlchemy**: ORM for database interactions
- **Alembic**: Database migration tool
- **Plotly**: Interactive data visualizations
- **Docker**: Containerization for easy deployment

## Getting Started

### Option 1: Using Docker Compose (Recommended)

1. Fork or clone this repository
2. Get a 511.org API key at https://511.org/open-data/token
3. Create a `.env` file in the root directory with your API key:
   ```
   API_KEY="your-api-key"
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
4. Create the PostgreSQL database (or use Docker for the database only)
5. Run the application:
   ```
   python main.py
   ```

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
├── gtfs_data/               # GTFS static feed data
├── docker-compose.yaml      # Docker Compose configuration
├── Dockerfile               # Docker image definition
├── main.py                  # Application entry point
└── requirements.txt         # Python dependencies
```

# Methodology

## Data collection

All data was gathered from the 511.org transit API.

The list of stops and stop times were downloaded from the GTFS API here: http://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={OPERATOR}

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
