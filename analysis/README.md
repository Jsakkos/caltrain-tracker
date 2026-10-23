# Analysis notebooks

Exploratory analysis of 14 months of Caltrain arrivals (Aug 2025 – Oct 2026). The
ranked findings are in [`INSIGHTS.md`](INSIGHTS.md); the notebooks are the evidence.

| File | What it does |
|---|---|
| `build_dataset.py` | Rebuilds `data/analysis.duckdb` from a snapshot of the collector's SQLite file and the versioned GTFS feeds. Keeps signed delay, closest-approach distance, stop rank, route type, direction; adds a per-day feed-health table and 511's own `stop_observations`. ~1 min. |
| `common.py` | DuckDB connection, the `feed_days` and `clean` views (the subset every notebook uses), station order, Wilson intervals. |
| `viz.py` | Matplotlib defaults and the palette (NB blue, SB orange; one-hue sequential; blue/grey/orange diverging). |
| `01_data_quality.py` | Coverage, feed-health episodes, per-arrival quality, validation against 511's observations, the clean subset. |
| `02_when_where_which.py` | Hour × direction × day-of-week, Saturdays, months, NB vs SB, service types, the train league table, station sawtooth, weather. |
| `03_propagation.py` | Segment accrual map, onset stations, knock-on by headway, express sensitivity, recovery after a 10-minute delay, worst days and how long they take to clear. |
| `04_baselines.py` | Forecast baselines on a held-out period (schedule / persistence / train history / persistence + profile / + leader) and a rolling-hour anomaly rule. |

Notebooks are stored as jupytext "percent" scripts (`.py`) with the executed `.ipynb`
alongside. To rerun:

```bash
# 1. copy the server's data files (see memory note pve-docker-caltrain-access)
scp jsakkos@192.168.1.122:~/caltrain-prefect/data/caltrain_lat_long.db analysis/data/
# 2. build the analysis DuckDB
uv run --group notebooks python analysis/build_dataset.py
# 3. execute a notebook (register the kernel once: uv run --group notebooks python -m ipykernel install --user --name caltrain-analysis)
cd analysis && uv run --group notebooks jupytext --to ipynb --set-kernel caltrain-analysis --execute 02_when_where_which.py
```

`data/external/weather_sfo.json` is a daily weather pull from the Open-Meteo archive
(lat 37.60, lon -122.39) used for the weather check; it is committed so the notebook
runs offline.

## The clean subset

Every finding is computed on `clean` unless stated: non-origin stops only, scheduled
before midnight, closest ping within 1 km of the platform, real train numbers, and the
service day not flagged `bad_feed` (position-change share < 45% or fewer than 60% of
arrivals within 1 km). That removes 39 of 419 days and leaves 513k arrivals on 380 days.
"Late" means more than 4 minutes behind schedule. Delay precision is one minute.
