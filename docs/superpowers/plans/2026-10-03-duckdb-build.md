# DuckDB Nightly Build Implementation Plan (Phase 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `src/flows/data_processing.py` (pandas, recomputes all history, ~12 min, 2+ GB RAM) with `python -m src.pipeline.build`, which computes arrivals incrementally in DuckDB and writes the same website files.

**Architecture:** `arrivals.py` reads `train_locations` straight from SQLite via DuckDB's sqlite scanner, joins each day's pings to the schedule in effect that day (existing `schedule_for_dates`), picks the closest approach per trip/stop/day, and stores one row per arrival in `data/analytics.duckdb`. Only days that are new, recent, or whose schedule version changed are recomputed. `dashboard.py` and `incidents.py` turn the (small) arrivals table into the website files. `build.py` wires it together for cron.

**Tech Stack:** DuckDB (+ sqlite extension), pandas, numpy, plotly, pytest. Parent plan: `2026-10-03-pipeline-slimdown.md`.

---

## Legacy semantics to preserve (parity first, fixes later)

Measured from `src/flows/data_processing.py`:

1. **Join keys are strings.** `train_locations.stop_id` contains `'unknown'`, so the legacy `astype(int)` fails and everything falls back to `str`. Join pings ↔ schedule on `(date, trip_id, stop_id)` and ↔ stops on `stop_id` as text. Stops are filtered to numeric `stop_id`.
2. **`date` = the ping's calendar date** (local wall time as stored).
3. **Distance** = haversine with R = 6,371,000 m in the `atan2` form.
4. **Arrival** = the ping with minimum distance per `(trip_id, stop_id, date)`; ties go to the earliest row (`id` order, since `groupby().first()` keeps `SELECT *` order).
5. **Scheduled time** = `date + normalize_time(arrival_time)`, where hours ≥ 24 wrap to `hour % 24` on the *same* date (a known bug, kept for parity).
6. **Delay** = `(actual - scheduled)` in minutes; then `> 500 → 0`, `< -100 → 0`; then `is_delayed = delay > 4`, severity `Major` if > 15, `Minor` if > 4, else `On Time`; then `< 0 → 0`.
7. **Commute period** from the actual arrival: weekend (Sat/Sun) → `Weekend`; `06:00:00 ≤ t ≤ 09:00:00` → `Morning`; `15:30:00 ≤ t ≤ 19:30:00` → `Evening`; else `Other`. `hour` = actual arrival hour.
8. **Ping weighting in plots.** Legacy `processed_df` has one row *per matched ping* (a fan-out merge), the dashboard JSON and incidents deduplicate to one row per arrival, but `daily_stats.html` and `commute_delay.html` do **not**, so they're weighted by ping count. The arrivals table stores `ping_count` so the plots can reproduce this exactly.
9. **`station_performance.json` has `stop_lat`/`stop_lon` = null** (the legacy frame never carries coordinates). Kept.
10. `processed_arrivals.csv` (608 MB per-ping CSV) and `summary_stats.json` are not read by the website (`export_to_website.py` overwrites the latter with `stats.json`). Not produced.

Outputs the website reads (from MyWebsite `src/lib/api.ts`): `stats.json`, `daily_performance.json`, `station_performance.json`, `train_performance.json`, `hourly_heatmap.json`, `commute_analysis.json`, `weekly_summary.json`, `monthly_summary.json`, `incidents.json`, `incident_trajectories.json`, `plots/daily_stats.html` and `plots/commute_delay.html`. (The Prefect flow and the first version of this build wrote `commute_delays.html`, so the site's `commute_delay.html` was a stale copy; the build now writes the name the site links.)

## Files

- Create `src/pipeline/__init__.py` (empty), `src/pipeline/arrivals.py`, `src/pipeline/dashboard.py`, `src/pipeline/incidents.py`, `src/pipeline/build.py`
- Create `scripts/compare_outputs.py`
- Create `tests/pipeline_fixtures.py` (tiny SQLite + GTFS feed builders), `tests/test_arrivals.py`, `tests/test_dashboard.py`, `tests/test_incidents.py`, `tests/test_compare_outputs.py`
- Modify `pyproject.toml` (add `duckdb` is already present; ensure `plotly`, `numpy`, `pandas<3`)
- Modify `src/utils/geo_utils.py`: add vectorized `project_to_route_many`
- None of the `src/pipeline` modules import `src.config` (it requires `API_KEY`).

## Tasks

### Task 2.1: Parity baseline ✅
Run the legacy flow with Prefect decorators stubbed (identity functions) and `STATIC_CONTENT_PATH` pointed at `parity/old/`, on the local DB snapshot (4.33M rows, 2025-08-10 → 2026-09-14). Record runtime and peak memory.

### Task 2.2: Test fixtures
`tests/pipeline_fixtures.py` builds, in `tmp_path`:
- a GTFS feed folder (`feed_info.txt`, `stops.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`) with 2 stops (`70011` at 37.7764,-122.3943; `70021` at 37.7577,-122.3924), one weekday service, train `101` arriving `08:00:00` / `08:05:00` and train `103` with a `24:10:00` call;
- a SQLite DB with the production `train_locations` schema (reuse `src.collector.connect`) and helper `add_ping(conn, trip, stop, lat, lon, ts_text)`.

### Task 2.3: `arrivals.py`, TDD
Tests (each a separate function in `tests/test_arrivals.py`):
- closest ping wins; tie on distance → lower `id` wins
- delay, severity boundaries at 4/15 minutes, negative delay clamped to 0 but severity `On Time`, `> 500` → 0
- 24:xx schedule wraps to the same date (legacy behaviour)
- commute periods incl. the 09:00:00 inclusive edge and weekends
- `'unknown'` stop pings are ignored, not an error
- `ping_count` = matched pings for that arrival
- incremental: second `update_arrivals` with no new data recomputes only the "always" days; adding a ping on an old day that is already processed is ignored unless `full=True`; a changed feed version for a day triggers recompute

API:
```python
def open_store(path: str) -> duckdb.DuckDBPyConnection
def update_arrivals(store, sqlite_path: str, feeds_root: Path, full: bool = False, today: date | None = None) -> list[date]
def load_arrivals(store) -> pd.DataFrame
```

### Task 2.4: `dashboard.py`, TDD
Port `generate_dashboard_data` to `build_dashboard(arrivals: pd.DataFrame, now: datetime) -> dict[str, object]` (filename → JSON payload) with vectorized counts instead of per-group lambdas, same keys/rounding/sort. `write_json_atomic(path, payload, indent=None)` writes to a temp file and `os.replace`s it. Plots: `daily_stats_figure(arrivals)` and `commute_figure(arrivals)` weight rows by `ping_count`. Tests: on a hand-built 6-row arrivals frame, assert exact numbers in `stats.json`, `daily_performance.json`, `commute_analysis.json`, and that every file name above is produced.

### Task 2.5: `incidents.py`, TDD
Add `project_to_route_many(lats, lons, shape_points) -> np.ndarray` to `geo_utils` (test: matches scalar `project_to_route` within 1 m on 500 random points along the shape bbox). Port `detect_incidents` to `detect_incidents(arrivals, gps_for_dates, station_meta, shape_points, top_n=25)` returning `(incidents, trajectories)`. Paths resolved from the repo base dir. GPS for incident dates comes from one DuckDB query. Test: synthetic arrivals with one system-wide day (3 trains ≥ 15 min) and one single-train day produce tier 2 / tier 1 incidents with the expected ids and summary text.

### Task 2.6: `build.py` + `scripts/compare_outputs.py`
CLI `python -m src.pipeline.build [--full] [--db PATH] [--store PATH] [--out DIR]`, defaulting to `data/caltrain_lat_long.db`, `data/analytics.duckdb`, `static`. Logs days recomputed, arrivals count and timing per stage. `compare_outputs.py OLD NEW` compares each JSON file: lists matched by key (`date`, `stop_id`, `trip_id`, `week_start`, `month`, `(day_of_week, hour)`, `id`), numbers within 0.051 (pandas rounds half-to-even), ignoring `last_updated`; exits non-zero on mismatch with a readable diff. Unit test for the comparer.

### Task 2.7: Parity run
`build --full` on the same snapshot into `parity/new/`; run the comparer; explain or fix every difference. Record runtimes (`--full` and a no-op incremental rerun). Commit.

### Task 2.8: Production switch (in phase 3's deploy)
Host venv: `uv sync` (adds duckdb). Run `build --full` once. Install cron. Delete the Prefect processing deployment and `src/flows/data_processing.py` in the same deploy.
