# Caltrain On-Time Performance Tracker
## Overall Caltrain On-Time Performance
82.99% of trains arriving on time
## Using caltrain-tracker to collect your own data
- Fork this repository
- Get a 511.org API key at https://511.org/open-data/token
- On Linux, cd into caltrain-tracker
- Build the Docker image `docker compose build`
- Create a .env file with your API key `API_KEY="your-api-key"`
- Run the Docker image `docker compose up -d`
- Open a web browser to port 8050 to view the dashboard `your-machine-ip-address:8050`
### Folder structure
#### data
Contains an SQLite database with the accumulated GTFS-RT data (about 1 month, 1 min resolution).
#### gtfs
Contains the unzipped GTFS feed data, which is the reference used for the train schedules. Can be updated by downloading again from 511.org (https://511.org/open-data/transit).

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
