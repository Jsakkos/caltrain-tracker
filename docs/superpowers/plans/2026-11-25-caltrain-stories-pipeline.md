# Caltrain stories: tracker pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the nightly build compute one cleaned on-time measure for every dashboard file, detect line-wide stops, and export the data and claim checks for four delay stories.

**Architecture:** The `arrivals` table gains the columns the analysis notebooks derive (signed delay, distance, stop rank, service, direction), plus per-day `feed_health` and `scheduled_trips` tables. A new `clean` module owns the notebooks' clean rules as SQL views that both the dashboard and a new `stories` step read. A `network_stops` module ports notebook 05's detectors. Two new JSON files ship through the existing export.

**Tech Stack:** Python 3.13, DuckDB, pandas, pytest (existing; no new dependencies).

**Spec:** MyWebsite repo, `docs/superpowers/specs/2026-11-25-caltrain-stories-design.md` (§4 measure, §5 stories, §6 example day, §8 tracker). The site half is a separate plan in MyWebsite, `docs/superpowers/plans/2026-11-25-caltrain-stories-site.md`; the JSON contract in Task 6 is shared with it verbatim.

## Global Constraints

- "Late" means `delay_signed > 4` minutes; "late 15" means `delay_signed > 15`. On time is `delay_signed <= 4`, early arrivals included.
- Clean rules, exactly as `analysis/common.py`: `stop_rank > 1`; `arrival_time < '24:'`; `dist_m < 1000`; `length(trip_id) <= 3`; day not `bad_feed`, where `bad_feed = pos_change_share < 0.45 OR near1k < 0.60` (`near1k` = share of a day's `stop_rank > 1` arrivals with `dist_m < 1000`).
- Weekday means `isodow(date) <= 5`. "Hour" means the scheduled hour (`hour(sched_ts)`).
- Service is `'Local'` for any `route_id LIKE 'Local%'`, else `route_id`; South County is excluded from service comparisons. Direction is `'NB'` when `stop_id` ends in `1`, else `'SB'`.
- The fixed example day is **2025-11-06**.
- Existing JSON keys keep their names (except the incident root-cause keys, Task 5). `stats.json` keeps its legacy fields computed the old way; every other dashboard file switches to clean arrivals in place.
- The nightly build stays within a few minutes under the existing `memory_limit 1GB, threads 4`.
- Public repo: no hostnames, IP addresses, usernames or private paths in anything committed, this plan included.
- `uv run pytest` passes before every commit.

## Review Focus

1. **First nightly run after the server pull**, on a store with the old `arrivals` schema: it must migrate itself (drop derived tables, recompute every day) with no manual `--full`. Test in Task 1.
2. **A day with pings but no clean arrivals** (a bad-feed day): the dashboard still emits its daily row with `bad_feed: true`, and no division by zero or NaN reaches any file. Tests in Tasks 2 and 3.
3. **NaN or infinity in story output** (an empty group, zero stops detected): `stories.json` must be strict JSON, since `JSON.parse` rejects `NaN`. Test in Task 6.
4. **A story computation raising on real data**: the build logs it, keeps the previous `stories*.json` files, and still writes every dashboard file. Test in Task 7.
5. **The example day absent** (fixture databases, or a future purge): `stories.json` is still written, `stories_example_day.json` is skipped, and the export copes with the missing file. Tests in Tasks 6 and 7.

---

### Task 0: Development data (setup, nothing committed)

- [ ] **Step 1: Snapshot the live database on the server.** SSH to the server (address in the private access notes, not here). The file is written every minute, so copy it with Python's backup API using the deployment's venv (the host has no `sqlite3` CLI): open the live DB with `sqlite3.connect("file:<db>?mode=ro", uri=True)` and `.backup()` into `/tmp/caltrain_snapshot.db`.
- [ ] **Step 2: Copy it down** to `data/caltrain_lat_long.db` in this worktree (gitignored), and to `analysis/data/caltrain_lat_long.db` for the notebooks. Delete the server's `/tmp` copy.
- [ ] **Step 3: Verify.** `SELECT count(*), max(timestamp) FROM train_locations` shows more than 4.5 M rows and a max within the last day.

### Task 1: Richer schedule and arrivals, with self-migration

**Files:**
- Modify: `src/data/gtfs_feeds.py` (`SCHEDULE_COLUMNS`, `_stop_calls`)
- Modify: `src/pipeline/arrivals.py` (`SCHEMA`, `ARRIVALS_SQL`, `open_store`, `_score_days`)
- Modify: `tests/pipeline_fixtures.py` (`make_feed` writes `route_id`, `direction_id`)
- Test: `tests/test_arrivals.py`

**Interfaces:**
- Produces: `SCHEDULE_COLUMNS = ["date", "trip_id", "stop_id", "stop_sequence", "arrival_time", "feed_version", "stop_rank", "n_stops", "route_id", "direction_id"]`. `stop_rank` ranks `stop_sequence` within the GTFS trip (method `first`, 1-based, computed before trip ids become train numbers, as `analysis/build_dataset.py` does); `n_stops` is the trip's call count.
- Produces: `arrivals` gains, after `feed_version`: `sched_ts TIMESTAMP, delay_signed DOUBLE, dist_m DOUBLE, stop_rank INTEGER, n_stops INTEGER, route_id VARCHAR, direction_id VARCHAR`. `delay_signed` is the raw `(ts - sched_ts)` in minutes with no clamping. Existing columns are unchanged.
- Produces: table `scheduled_trips(date DATE, trip_id VARCHAR, route_id VARCHAR, direction_id VARCHAR, dep_ts TIMESTAMP, n_stops INTEGER)`, one row per train per scored day, from calls with `arrival_time < '24:'`; `dep_ts` is the earliest scheduled call. Rewritten for each scored day inside the same transaction.
- Produces: `open_store(path)` drops `arrivals`, `processed_days`, `scheduled_trips` and `feed_health` when `arrivals` exists without a `delay_signed` column, so the next `update_arrivals` recomputes every day.

- [ ] **Step 1: Write failing tests** in `tests/test_arrivals.py`:
  - `test_signed_delay_and_distance_are_kept`: ping at `AT_SF` at 07:58 for the 08:00 call → `delay_signed == pytest.approx(-2.0)`, `delay_minutes == 0.0`, `dist_m < 5`; a `NEAR_SF` arrival has `dist_m == pytest.approx(270, abs=15)`.
  - `test_stop_rank_service_and_direction`: with the fixture feed (101 = `("Local Weekday", "1")`), SF row has `stop_rank == 1`, 22nd Street `stop_rank == 2`, both `n_stops == 2`, `route_id == "Local Weekday"`, `direction_id == "1"`.
  - `test_scheduled_trips_skip_after_midnight`: after a run, `scheduled_trips` for `DAY` holds train 101 with `dep_ts == datetime(2026, 3, 4, 8, 0)` and `n_stops == 2`, and no row for 103 (its only call is 24:10).
  - `test_old_store_is_migrated`: create a DuckDB with the legacy `arrivals`/`processed_days` DDL (copy it from `origin/main` into the test), insert one `processed_days` row, call `open_store` → `arrivals` has `delay_signed` and `processed_days` is empty.
- [ ] **Step 2: Run** `uv run pytest tests/test_arrivals.py -v`. Expected: the four new tests FAIL.
- [ ] **Step 3: Implement** the schedule columns, the extra `ARRIVALS_SQL` outputs (carry `sched_ts`, `stop_rank`, `n_stops`, `route_id`, `direction_id` through `assigned`; expose the haversine distance as `dist_m`), the `scheduled_trips` write, and the migration in `open_store`. `make_feed` gains `routes: dict[str, tuple[str, str]] | None` (default `{"101": ("Local Weekday", "1"), "103": ("Express", "1")}`) and writes `route_id, direction_id` into `trips.txt`.
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: all PASS (existing tests unchanged).
- [ ] **Step 5: Commit** `feat(arrivals): keep signed delay, distance, stop rank and service`.

### Task 2: Feed health and one set of clean rules

**Files:**
- Create: `src/pipeline/clean.py`
- Modify: `src/pipeline/arrivals.py` (`_score_days` refreshes `feed_health` for its days)
- Modify: `analysis/common.py` (import the SQL instead of defining it)
- Test: `tests/test_clean.py`, `tests/test_arrivals.py`

**Interfaces:**
- Produces in `clean.py`: constants `POS_CHANGE_MIN = 0.45`, `NEAR_1KM_MIN = 0.60`, `MAX_DIST_M = 1000`, `LATE_MIN = 4`, `MAJOR_MIN = 15`, `MIN_LEAGUE_RUNS = 80`.
- Produces: `FEED_HEALTH_SQL`, a SELECT over the `pings` view for dates in `todo_days` returning `date, pings, polls, vehicles_per_poll, pos_change_share, trains, first_ping, last_ping` (port of `analysis/build_dataset.py` `FEED_HEALTH_SQL`). `_score_days` deletes and re-inserts those days into table `feed_health`.
- Produces: `register_views(con) -> None`, which creates TEMP views `feed_days` and `clean` with the same columns and semantics as `analysis/common.py` (`feed_days` adds `near100, near1k, arrivals, bad_feed, partial_day`; `clean` adds `direction, service, dow, weekday, sched_hour, late4, late15, station, bad_feed, partial_day`), built from the module's constants. Also a view `clean_all_days`: the same rules without the bad-feed mask.
- Produces: `load_clean(con, include_bad_feed: bool = False) -> pd.DataFrame`, the dashboard-ready frame: all `clean` (or `clean_all_days`) columns, with `delay_minutes := delay_signed`, `is_delayed := late4`, `delay_severity` from signed delay (`Major` > 15, `Minor` > 4, else `On Time`), `hour := sched_hour`, `commute_period` from `sched_ts` (Weekend / Morning 06:00–09:00 / Evening 15:30–19:30 / Other, same bounds as `arrivals.py`), and `dep_ts` left-joined from `scheduled_trips` on `(date, trip_id)`. The `clean` view itself stays identical to the notebooks' so `analysis/common.py` can share it.
- Produces: `wilson(p: float, n: int, z: float = 1.96) -> tuple[float, float]` and `last_stop_league(df: pd.DataFrame) -> pd.DataFrame` over weekday rows of a `load_clean` frame. Per `(date, trip_id)` it keeps the highest `stop_rank` row, then per train returns `trip_id, service, direction, departs` ("HH:MM", the most common `dep_ts` time; null if the frame has no `dep_ts`), `runs, late_pct` (×100, 1 dp), `ci_low, ci_high` (Wilson, ×100, 1 dp), `last_stop_name` (most common `station`), sorted by `late_pct` descending. Port of `02_when_where_which §6`.
- Consumes: `arrivals`, `scheduled_trips` (Task 1).

- [ ] **Step 1: Write failing tests.** `tests/test_clean.py` builds an in-memory DuckDB with hand-written `arrivals` and `feed_health` rows (helper `make_store(rows, health)`):
  - `test_clean_drops_origin_after_midnight_far_and_foreign`: rows with `stop_rank 1`, `arrival_time '24:05:00'`, `dist_m 1500`, `trip_id '9001'` are absent from `clean`; a normal row is present.
  - `test_stale_positions_mask_the_day`: `pos_change_share 0.40` → the day's rows are absent from `clean`, present in `clean_all_days` with `bad_feed` true.
  - `test_far_arrivals_mask_the_day`: half a day's non-origin arrivals at `dist_m 1200` → `bad_feed`.
  - `test_load_clean_is_signed_and_scheduled`: `delay_signed -3` → `delay_minutes -3`, `delay_severity "On Time"`; `sched_ts 08:55`, actual 09:20 → `hour 8`, `commute_period "Morning"`.
  - `test_last_stop_league`: 80 weekday runs of train 523 (two stops each), last stop late on 40 → `late_pct 50.0`, `runs 80`, `last_stop_name "22nd Street"`, and `ci_low < 50 < ci_high`.
  - In `tests/test_arrivals.py`, `test_feed_health_per_day`: three pings for one train, two at the same position → `pos_change_share == 0.5`, `pings == 3`.
- [ ] **Step 2: Run** `uv run pytest tests/test_clean.py tests/test_arrivals.py -v`. Expected: FAIL (module missing).
- [ ] **Step 3: Implement** `clean.py` and the `feed_health` refresh. Then point `analysis/common.py` at `register_views` (the analysis DuckDB has the same table and column names), keeping its `connect()` and `q()` signatures.
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(clean): one set of clean rules for the dashboard, stories and notebooks`.

### Task 3: Dashboard files from clean arrivals

**Files:**
- Modify: `src/pipeline/dashboard.py`, `src/pipeline/build.py`
- Test: `tests/test_dashboard.py`, `tests/test_build.py`

**Interfaces:**
- Consumes: `register_views`, `load_clean`, `last_stop_league`, `MIN_LEAGUE_RUNS` (Task 2).
- Produces: `build_dashboard(arrivals, now, legacy=None, daily=None) -> dict`. `arrivals` is the clean frame. `legacy` (default: `arrivals`) feeds only the legacy `stats.json` keys. `daily` (default: `arrivals` with `bad_feed = False`) feeds `daily_performance.json`. `write_dashboard(out_dir, arrivals, now, legacy=None, daily=None)` passes them through.
- Produces, `stats.json`: legacy keys unchanged, plus `"clean": {"on_time_percentage", "minor_delay_percentage", "major_delay_percentage", "total_arrivals", "avg_delay_minutes", "median_delay_minutes", "days_tracked", "bad_feed_days", "rolling_7d_on_time", "rolling_30d_on_time"}`, the same rounding as the legacy keys. `bad_feed_days` counts distinct `bad_feed` dates in `daily`.
- Produces, `daily_performance.json`: each row gains `"bad_feed": bool`; bad-feed days carry their unmasked-but-otherwise-clean figures.
- Produces, `train_performance.json`: each row gains `"last_stop_late_pct": float | null` (null when `runs < 80`), `"last_stop_runs": int` (0 if the train never runs on weekdays), `"last_stop_name": str | null`.
- `build()` uses `load_clean(store)` for `arrivals`, `load_arrivals(store)` for `legacy`, and `load_clean(store, include_bad_feed=True)` for `daily`. If the clean frame is empty, it logs a warning and leaves the website files alone, as it does today for empty arrivals.

- [ ] **Step 1: Update and add tests** in `tests/test_dashboard.py`:
  - `test_stats` keeps its legacy dict and also asserts `stats["clean"]["on_time_percentage"]` for a clean frame that differs from the legacy frame.
  - `test_daily_rows_flag_bad_feed_days`: a `daily` frame with one bad day → that row has `bad_feed: True`, the others `False`, and the bad day's rows are not counted in `stats["clean"]`.
  - `test_train_last_stop_columns`: 80 weekday runs of 523 → `last_stop_late_pct 50.0`; a train with 10 runs → `None`.
  - `test_outputs_are_strict_json`: `json.dumps(payload, allow_nan=False)` succeeds for every file when one commute period and one train have no rows.
- [ ] **Step 2: Run** `uv run pytest tests/test_dashboard.py -v`. Expected: new assertions FAIL.
- [ ] **Step 3: Implement** in `dashboard.py` and wire `build.py`.
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: PASS, `test_build` included.
- [ ] **Step 5: Commit** `feat(dashboard): compute every file from clean arrivals; add clean headline block`.

### Task 4: Network stops and never-run trips

**Files:**
- Create: `src/pipeline/network_stops.py`
- Test: `tests/test_network_stops.py`

**Interfaces:**
- Consumes: views `pings` (from `attach_pings`), `clean`, `feed_days`; table `scheduled_trips`; a `stops` frame (`load_stops`, numeric ids).
- Produces: `detect_network_stops(con, stops: pd.DataFrame) -> pd.DataFrame`, one row per candidate, columns `date, t_start, t_end, hold_minutes, max_trains, held_near, trains_late, max_delay, dirs_hit, cancelled_after, hours_to_normal, is_network_stop`. It ports `analysis/05_network_stops.py` §2 Detector A and §3 as written. Constants: `RUN_MIN_PINGS = 10`, `MIN_TRAINS = 4`, `MERGE_GAP_MIN = 20`, `MIN_HOLD_MIN = 15`, `TERMINAL_RADIUS_M = 600`, `TERMINALS = ("San Francisco", "San Jose Diridon", "Tamien", "Gilroy")`, `CONFIRM_TRAINS = 5`, `CONFIRM_HOURS = 4`. Only pings with hour 4–23, `lat > 1`, on non-bad days. `held_near` is the cleaned station name (no " Caltrain Station"/direction suffix). `trains_late` counts trains ≥ 15 min late in `[t_start, t_end + 4 h]`, and `is_network_stop = trains_late >= 5`. `hours_to_normal` is the hours from `t_start` to the first later scheduled hour with < 15% of arrivals late, else `None`.
- Produces: `never_ran(con) -> pd.DataFrame` (`date, trip_id, route_id, dep_ts`): scheduled trips with no pings that day, on non-bad days, where the feed carried other trains in ≥ 20 of the 30 minutes before `dep_ts` and ≥ 45 of the 60 after (05 Detector B).

- [ ] **Step 1: Write failing tests.** Build an in-memory DuckDB with `pings`, `clean`, `feed_days` and `scheduled_trips` tables, plus a two-station `stops` frame (Palo Alto, San Francisco):
  - `test_frozen_trains_then_late_is_a_network_stop`: 4 trains each with 20 identical pings near Palo Alto from 15:55, and 5 trains late15 in `clean` within 4 h → one row, `is_network_stop`, `held_near == "Palo Alto"`, `hold_minutes >= 15`.
  - `test_freeze_without_late_trains_is_a_feed_freeze`: same pings, no late trains → `is_network_stop` false.
  - `test_trains_parked_at_a_terminal_are_ignored`: 4 trains frozen 100 m from San Francisco → no rows.
  - `test_never_ran_needs_a_live_feed`: a scheduled trip with no pings while another train pings every minute → listed; the same trip on a day with no pings at all → not listed.
- [ ] **Step 2: Run** `uv run pytest tests/test_network_stops.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement** by porting the notebook SQL into module-level constants.
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: PASS.
- [ ] **Step 5: Commit** `feat: detect line-wide stops and never-run trips nightly`.

### Task 5: Incident panel data without root-cause labels

**Files:**
- Modify: `src/pipeline/incidents.py`, `src/pipeline/build.py`
- Test: `tests/test_incidents.py`

**Interfaces:**
- Produces: `detect_incidents(arrivals, gps_for_dates, station_meta, shape_points, top_n=25, network_stops: pd.DataFrame | None = None)`. Incident dicts drop `root_cause_train` and `root_cause_station` and gain `"held_near": str | None`: the `held_near` of that date's earliest `is_network_stop` row, else `None`. Trajectory output is unchanged.
- `build()` passes the clean frame and the Task 4 frame. Order in `build()`: dashboard, network stops, incidents, stories (Task 7).

- [ ] **Step 1: Update tests**: existing incident expectations lose the two root-cause keys; add `test_held_near_comes_from_the_network_stop` (a network-stop row on the incident date → `held_near == "Palo Alto"`; no row → `None`).
- [ ] **Step 2: Run** `uv run pytest tests/test_incidents.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(incidents): replace root-cause guesses with where trains were held`.

### Task 6: Stories

**Files:**
- Create: `src/pipeline/stories.py`
- Test: `tests/test_stories.py`

**Interfaces:**
- Consumes: views `clean`, `feed_days`; `last_stop_league`, `wilson` (Task 2); the Task 4 frames; `station_distances` and `_trajectories`-style projection from `incidents.py` (factor out a public `project_trajectories(gps, shape_points) -> list[dict]` if needed).
- Produces: `build_stories(con, events: pd.DataFrame, cancellations: pd.DataFrame, now: datetime) -> dict`, `validate_stories(payload: dict) -> None` (raises `ValueError` naming the first problem), `REQUIRED_FIGURES: dict[str, str]` (name → unit), and `example_day(con, gps_for_dates, station_meta, shape_points, events, day=date(2025, 11, 6)) -> dict | None`.

**The contract** (`stories.json`; the site plan copies this block):

```text
{
  "schema_version": 1,
  "generated_at": "2026-11-26T02:00:41",   Pacific wall time
  "data_through": "2026-11-25",            max(date) in clean
  "first_day": "2025-08-10",               min(date) in clean
  "clean_days": 412,
  "figures": { name: {"value": number | string, "unit": "pct"|"count"|"minutes"|"hours"|"weeks"|"time"|"text"} },
  "stories": {
    "opener":     {"heatmap": [{"dow": 1-7, "hour": 5-23, "n": int, "late_pct": float}]},
    "recovery":   {"holds": bool, "fan": [{"k": 1-10, "n": int, "p10": f, "p25": f, "median": f, "p75": f, "p90": f, "back_on_time_pct": f}]},
    "express":    {"holds": bool,
                   "league":  [{"train": str, "service": str, "direction": "NB"|"SB", "departs": "HH:MM", "last_stop": str,
                                "runs": int, "late_pct": f, "ci_low": f, "ci_high": f}],      10 least reliable, worst first
                   "groups":  [{"service": "Local"|"Limited"|"Express", "direction": "NB"|"SB", "n": int, "late_pct": f}],
                   "headway": [{"leader": "≤ 0"|"0–4"|"4–10"|"10–20"|"> 20", "headway": "≤ 10"|"11–20"|"21–30"|"31–40",
                                "n": int, "late_pct": f}]},
    "afternoons": {"holds": bool, "events": [{"date": "YYYY-MM-DD", "start": "HH:MM", "hold_minutes": int, "held_near": str,
                                              "trains_late": int, "cancelled": int, "hours_to_normal": f | null}]},   network stops, newest first
    "saturday":   {"holds": bool, "days": [{"date": "YYYY-MM-DD", "type": "weekday"|"Saturday"|"Sunday", "n": int, "late_pct": f}]}
  }
}
```

`stories_example_day.json`:

```text
{"date": "2025-11-06", "window": {"start": "HH:MM", "end": "HH:MM"}, "freeze": {"start": "HH:MM", "end": "HH:MM"} | null,
 "stations": [{"name": str, "distance": km, "order": int}],
 "trains": [{"trip_id": str, "direction": "NB"|"SB", "max_delay_min": f, "points": [{"time": "HH:MM:SS", "distance": km}]}]}
```

The window runs from 1 h before the freeze start to 4 h after the freeze end, clipped to 05:00–23:59; with no network stop that day it's 14:00–20:00 and `freeze` is null. Trains with `max_delay_min >= 15` keep every point; others are thinned, so the file stays ≤ 400 KB. It returns `None` when the day has no clean arrivals or no pings.

**Figures** (`REQUIRED_FIGURES`, all percentages ×100 with 1 dp; "weekday" where noted). Sources are the notebook cells named.
- `on_time_pct` pct: share of `clean` with `NOT late4`, all days.
- `recovery_trips` count: weekday trips that first reach `delay_signed >= 10` at some stop and have a later stop (`03 §4`).
- `recovery_median_change_5` minutes, `recovery_worse_5_pct` pct, `recovery_better_5_pct` pct: delay change ≥ +2 / ≤ −2 at k = 5.
- `recovery_back_on_time_pct` pct: share with `delay_signed <= 4` at k = 10.
- `worst_train` text, `worst_train_departs` time, `worst_train_late_pct` pct, `worst_train_last_stop` text, `worst_train_service` text, `worst_train_direction` text (`"northbound"`/`"southbound"`): `league[0]`.
- `nb_in_worst_25` count: northbound trains among the 25 least reliable with ≥ 80 runs.
- `express_nb_late_pct`, `express_sb_late_pct`, `local_nb_late_pct`, `local_sb_late_pct` pct: weekday per-stop late share by group.
- `headway_close_late_pct`, `headway_far_late_pct` pct: follower late share with leader 10–20 min late at headway ≤ 10 vs 21–30 (`03 §3`, weekday).
- `stops_count` count: rows with `is_network_stop`. `stops_every_weeks` weeks: `clean_days / stops_count / 7` (1 dp; 0 when no stops).
- `stops_late15_share_pct` pct: share of all `late15` clean arrivals with `sched_ts` in `[t_start, t_end + 4 h]` of a network stop on their date.
- `stops_hours_to_normal_median` hours, `stops_longest_hold_minutes` minutes.
- `cancel_express_pct`, `cancel_local_pct` pct: on days with ≥ 3 never-run trips, never-run share of scheduled Express / Local trips (`05 §4`).
- `example_trains_late` count: `trains_late` of the example day's first network stop (0 if none).
- `worst_days_10_share_pct` pct: share of all `late15` arrivals falling on the 10 days with the most (`03 §5`).
- `sat_late_pct`, `weekday_late_pct`, `sun_late_pct` pct: pooled late share by day type. `sat_bad_day_pct`, `weekday_bad_day_pct` pct: share of days of that type with > 15% late. `saturdays` count (`02 §2`).

**Claim checks:** `recovery.holds = recovery_median_change_5 >= 0 and recovery_back_on_time_pct < 5`; `express.holds` = Express NB has the highest `late_pct` in `groups`; `afternoons.holds = stops_late15_share_pct >= 20`; `saturday.holds = sat_late_pct > max(weekday_late_pct, sun_late_pct)`.

`validate_stories` requires: `schema_version == 1`; every `REQUIRED_FIGURES` key present with the listed unit, and a finite number (or a string for `text`/`time`); `holds` booleans; non-empty `heatmap`, `fan`, `league`, `groups`, `headway` and `days` (`events` may be empty).

- [ ] **Step 1: Write failing tests** in `tests/test_stories.py`, with a helper `make_clean(con, rows)` that creates a `clean` table with the Task 2 columns:
  - `test_recovery_holds_when_late_trains_stay_late`: trips reach 10 late at rank 3, then +0.5 per stop → `fan[4]["median"] == 2.5`, `holds` true; `test_recovery_fails_when_trains_recover` (back to 0) → `holds` false.
  - `test_express_league_and_claim`: 80 runs of Express NB 523, late at the last stop on 40 → `league[0]["train"] == "523"`, `figures["worst_train_late_pct"]["value"] == 50.0`, `holds` true. With Local SB the latest group → `holds` false.
  - `test_afternoons_share`: one network-stop window holding 3 of 4 late15 arrivals → `stops_late15_share_pct == 75.0`, `holds` true; with no events → `stops_count == 0`, `stops_every_weeks == 0`, `holds` false, `events == []`.
  - `test_saturday_claim`: Saturday 12% vs weekday 6% → `holds` true.
  - `test_payload_is_strict_json_and_valid`: `json.dumps(payload, allow_nan=False)` succeeds and `validate_stories(payload)` passes.
  - `test_validate_rejects_missing_and_nonfinite`: removing `sat_late_pct` → `ValueError` whose message contains `"sat_late_pct"`; `float("nan")` → `ValueError`.
  - `test_example_day_window_and_absence`: a network stop at 15:55 → `window == {"start": "14:55", "end": ...}` and `freeze["start"] == "15:55"`; a date with no pings → `None`.
- [ ] **Step 2: Run** `uv run pytest tests/test_stories.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement** `stories.py`, porting the notebook SQL named above against the `clean` view.
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: PASS.
- [ ] **Step 5: Commit** `feat: compute the four delay stories and their claim checks`.

### Task 7: Build and export wiring

**Files:**
- Modify: `src/pipeline/build.py`, `scripts/export_to_website.py`, `README.md`
- Test: `tests/test_build.py`, `tests/test_export_to_website.py`

**Interfaces:**
- `build()` runs the stories step after incidents, inside `try/except Exception`. On success it calls `validate_stories`, writes `static/data/stories.json` with `allow_nan=False`, and writes `static/data/stories_example_day.json` when `example_day` returns a dict. On failure it logs `"stories step failed; keeping the previous stories files"` with the traceback and touches neither file. `timings["stories"]` is recorded. `write_json_atomic` gains an `allow_nan: bool = True` parameter.
- The export's file list gains `"stories.json"` and `"stories_example_day.json"`; a missing source file is skipped with a printed warning rather than an error.

- [ ] **Step 1: Write failing tests:**
  - `test_build_keeps_previous_stories_when_the_step_fails`: monkeypatch `build_stories` to raise; a pre-existing `stories.json` is byte-identical afterwards, and every `DASHBOARD_FILES` file exists.
  - `test_build_writes_valid_stories`: monkeypatch `build_stories` to return a valid payload and `example_day` to return `None` → `stories.json` is written and `stories_example_day.json` isn't.
  - `test_export_skips_missing_story_files`: the export runs with only the dashboard files present.
- [ ] **Step 2: Run** `uv run pytest tests/test_build.py tests/test_export_to_website.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement**, and add a "Stories" paragraph to `README.md`'s outputs section listing the two files and pointing to `src/pipeline/clean.py` for the on-time definition.
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(build): export stories nightly; keep the last good file on failure`.

### Task 8: Full run on fresh data (launch gate) and pull request

- [ ] **Step 1: Full build on the snapshot:** `uv run python -m src.pipeline.build --full --db data/caltrain_lat_long.db --store <scratch>/analytics.duckdb --out <scratch>/static`. Record the timings; total must stay within a few minutes.
- [ ] **Step 2: Cross-check against the notebooks.** Rebuild `analysis/data/analysis.duckdb` from the same snapshot (`uv run --group notebooks python analysis/build_dataset.py`), then compare in a scratch script: clean row count and on-time share; `recovery_*`; `league[:3]`; the Saturday/weekday/Sunday shares; the network-stop dates. Notebook definitions and pipeline figures must agree to rounding, so any difference is a porting bug to fix before going on.
- [ ] **Step 3: Gate.** All four `holds` are true, `stories_example_day.json` exists with a non-null `freeze` and is ≤ 400 KB, and `json.loads` reads every output file. If a claim fails on current data, stop and report it to Jonathan with the numbers; don't open the PR.
- [ ] **Step 4: Push and open the PR** against `main`. The description gives the headline before and after, the four claim results, the timings, and the deploy note: "after merging, pull on the server; the first nightly run migrates the store and recomputes every day automatically."
