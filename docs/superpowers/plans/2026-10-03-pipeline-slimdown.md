# Pipeline Slim-Down Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Prefect + Postgres + the FastAPI app with a stdlib collector container, an incremental DuckDB nightly build, and host cron, so the Caltrain tracker produces the same dashboard files with a fraction of the RAM, disk and CPU.

**Architecture:** A single long-running stdlib Python process polls 511 once a minute and appends to the existing SQLite file (raw store stays SQLite). A nightly DuckDB job reads SQLite directly, computes arrivals only for days it has not processed yet, and writes the same 10 dashboard JSON files the website export already copies. Host cron (already used for backup and export) schedules everything; Prefect, its Postgres and the unused `app` container are removed.

**Tech Stack:** Python 3.12/3.13 stdlib (`sqlite3`, `urllib`, `zoneinfo`), DuckDB, numpy, pandas (GTFS schedule lookup only), Docker Compose (collector only), host cron, pytest.

---

## Why (measured 2026-10-03)

| Component | Measured | Useful work |
|---|---|---|
| `prefect` container (server + worker) | 606 MB RAM always on | schedules 3 jobs |
| `postgres` (Prefect state only) | 244 MB RAM, **7.4 GB** disk, 245k flow runs | run history |
| Per-minute collection flow | 10,080 runs/week, **16.6 s avg** (max 1m20s) | 1 HTTP GET + ~15 rows |
| Nightly processing flow | **746 s avg** | recomputes all history every night |
| `app` container (FastAPI, port 8181) | 15 MB | nothing uses it; most endpoints read the empty `arrival_data` table |
| Actual data | 689 MB SQLite, 4.33M rows | |

Local benchmarks on the same DB: pandas `SELECT *` = 22.8 s and 2.2 GB peak; DuckDB `sqlite_scan` full aggregate = 0.3 s; full export to Parquet = 1.4 s / 26 MB.

## Constraints that shape the plan

1. **511 rate limit: 60 requests/hour per API key.** The collector runs once a minute, so two collectors on the same key exceed the limit and both get HTTP 429s. Phase 1 is therefore a *cutover with a rollback path*, not a side-by-side run. The new collector also must not retry within a minute.
2. **Dedup relies on text equality.** `train_locations` has `UNIQUE (trip_id, stop_id, timestamp)` and all three are stored as TEXT. Existing rows look like `('168', '70012', '2026-09-14 21:07:49.000000')`: local America/Los_Angeles wall time, `%Y-%m-%d %H:%M:%S.%f`. The new collector must write exactly that format or dedup silently breaks.
3. **Production config lives only on the server.** `~/caltrain-prefect` on pve-docker (192.168.1.122) is at `3dbae33`, behind `main` by 9 commits, with uncommitted edits that production depends on:
   - `docker-compose.yaml`: a `postgres` service + `PREFECT_API_DATABASE_CONNECTION_URL` (Prefect's backend)
   - `requirements-prefect.txt`: `asyncpg>=0.29.0` (needed for that backend), `pandas<3` pin
   - `requirements.txt`: `pandas<3` pin
   - `src/flows/data_processing.py`: `np.select` replacement for a chained `fillna(inplace=True)`
   - `README.md`: ops notes (daily schedule, manual runs, pandas pin rationale)
   - `src/config.py`: the old hardcoded API-key fallback (must **not** be carried over; PR #2 removed it)
   - `static/data/*`, `static/plots/*`: generated outputs (regenerated nightly, not worth preserving)

   Deploying `main` as-is would drop asyncpg and the Postgres service and break Prefect. Task 1.1 upstreams these first.
4. **Prefect re-registers deployments on container start.** `src/deployments/start_prefect.sh` runs `deploy_flows.py`, which deploys every flow found in `src/flows/`. Deleting the collection deployment is only durable once `src/flows/data_collection.py` is gone from the code too. The existing deployment row in Postgres must also be deleted explicitly (`Collect Train Location Data/collect_train_data_flow`).
5. **Host cron already exists:** `15 2 * * *` NAS backup and `30 2 * * *` website export, both via `~/caltrain-prefect/.venv/bin/python` (Python 3.13.2).
6. **Docker is not installed on the dev machine.** Images are built on pve-docker.

## File map

| Phase | File | Responsibility |
|---|---|---|
| 1 | `src/collector.py` (new) | Stdlib-only poller: parse SIRI payload, write rows, loop once a minute |
| 1 | `tests/test_collector.py` (new) | Unit tests for parsing, timestamp format, dedup, loop error handling |
| 1 | `Dockerfile.collector` (new) | `python:3.12-slim` plus one file |
| 1 | `docker-compose.yaml` | Add `collector`; upstream server's `postgres` service |
| 1 | `.dockerignore` | Keep the 689 MB DB and archives out of build contexts |
| 1 | `requirements*.txt`, `src/flows/data_processing.py`, `README.md` | Upstream server-only fixes |
| 1 | `src/flows/data_collection.py`, `test_data_collection.py` (delete) | Remove Prefect collection |
| 1 | `src/deployments/deploy_flows.py`, `main.py` | Drop references to the deleted flow |
| 1 | `pyproject.toml`, `uv.lock` | pytest dev dependency + config |
| 2 | `src/pipeline/arrivals.py` (new) | DuckDB: raw pings + dated schedule → arrivals, incremental by day |
| 2 | `src/pipeline/dashboard.py` (new) | DuckDB SQL → the 10 dashboard JSON files + 2 plots |
| 2 | `src/pipeline/incidents.py` (new) | Incident detection + vectorized route projection |
| 2 | `src/pipeline/build.py` (new) | CLI entry point (`python -m src.pipeline.build [--full]`) |
| 2 | `scripts/compare_outputs.py` (new) | Parity check: old vs new output directories |
| 2 | `tests/test_pipeline_*.py` (new) | Unit tests on small fixture DBs |
| 3 | `scripts/check_freshness.py` (new) | Cron check that rows are still arriving |
| 3 | `deploy/crontab.txt` (new) | Canonical host crontab, checked in |
| 3 | delete: `Dockerfile.app`, `Dockerfile.prefect`, `requirements*.txt`, `src/deployments/`, `src/flows/`, `src/api/`, `src/db/`, `src/models/train_data.py`, `main.py`, `alembic/`, `alembic.ini`, root `test_*.py`, `check_db.py`, `timezone-fix.py`, `run_data_processing_standalone.py`, `fetch_and_process_gtfsrt.py` | Retire Prefect/FastAPI stack |

---

# Phase 1: Move collection out of Prefect

Exit criteria: the `collector` container is the only thing polling 511; the Prefect collection deployment is deleted; hourly row counts over the first 24 h match the same weekday a week earlier (within ±10%); the nightly Prefect processing flow still runs.

### Task 1.0: Test harness

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock` (via `uv add`)
- Create: `tests/__init__.py` (empty)

- [x] **Step 1: Add pytest as a dev dependency**

Run: `uv add --dev pytest`
Expected: `pyproject.toml` gains a `[dependency-groups] dev = ["pytest>=..."]` block.

- [x] **Step 2: Add pytest config so `src` imports resolve from the repo root**

Append to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

`testpaths` keeps pytest away from the root-level `test_*.py` scripts, which import Prefect and hit the live DB.

- [x] **Step 3: Verify**

Run: `uv run pytest`
Expected: `no tests ran` (exit code 5).

- [x] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock tests/__init__.py
git commit -m "Add pytest harness scoped to tests/"
```

### Task 1.1: Upstream the server-only production fixes

**Files:**
- Modify: `docker-compose.yaml`
- Modify: `requirements-prefect.txt`, `requirements.txt`
- Modify: `src/flows/data_processing.py:220-224`
- Modify: `README.md`

- [x] **Step 1: Add the `postgres` service and Prefect backend URL to `docker-compose.yaml`**

Between `app` and `prefect`:

```yaml
  postgres:
    image: postgres:16-alpine
    environment:
      - POSTGRES_USER=${DB_USER}
      - POSTGRES_PASSWORD=${DB_PASSWORD}
      - POSTGRES_DB=prefect
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER} -d prefect"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: always
```

In `prefect`: add `depends_on: {postgres: {condition: service_healthy}}`, remove the `prefect_data:/root/.prefect` volume, and add `- PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://${DB_USER}:${DB_PASSWORD}@postgres:5432/prefect`. Replace top-level `volumes: prefect_data:` with `postgres_data:`.

Keep the repo's `gtfs_data` and `gtfs_feeds` mounts on `prefect`; the server compose predates them, and the PR #3 code needs them.

- [x] **Step 2: Pin pandas and add asyncpg**

`requirements.txt` and `requirements-prefect.txt`: `pandas>=2.0.0` → `pandas>=2.0.0,<3.0.0`. `requirements-prefect.txt`: add `asyncpg>=0.29.0` under `psycopg2-binary`.

- [x] **Step 3: Replace the chained `fillna(inplace=True)` in `process_arrival_data`**

Add `import numpy as np` beside `import pandas as pd`, and replace the three `delay_severity` lines with:

```python
    comparison_df['delay_severity'] = np.select(
        [comparison_df.delay_minutes > 15, comparison_df.delay_minutes > 4],
        ['Major', 'Minor'],
        default='On Time',
    )
```

- [x] **Step 4: README**: replace "PostgreSQL: Robust database for storing train location..." with the SQLite/Postgres-for-Prefect split, and add the "Operations" section (daily schedule, manual runs, pandas pin rationale) exactly as on the server. Phase 1 Task 1.6 then updates the collection line.

- [x] **Step 5: Verify the flow module still imports**

Run: `uv run python -m py_compile src/flows/data_processing.py`
Expected: no output. (Prefect isn't in the local uv env, so an import check only happens on the server when the image rebuilds.)

- [x] **Step 6: Commit**

```bash
git add docker-compose.yaml requirements.txt requirements-prefect.txt src/flows/data_processing.py README.md
git commit -m "Upstream production-only Postgres backend, pandas<3 pin and np.select fix"
```

### Task 1.2: Payload parsing and timestamp format

**Files:**
- Create: `src/collector.py`
- Test: `tests/test_collector.py`

- [x] **Step 1: Write the failing tests**

```python
from src.collector import parse_vehicles, to_local_text


def activity(vehicle="168", recorded="2026-09-14T04:07:49Z", lat="37.5", lon="-122.3",
             monitored="70012", onward=None):
    journey = {
        "VehicleRef": vehicle,
        "VehicleLocation": {"Latitude": lat, "Longitude": lon},
    }
    if monitored is not None:
        journey["MonitoredCall"] = {"StopPointRef": monitored}
    if onward is not None:
        journey["OnwardCalls"] = {"OnwardCall": [{"StopPointRef": s} for s in onward]}
    return {"RecordedAtTime": recorded, "MonitoredVehicleJourney": journey}


def payload(*activities):
    return {"Siri": {"ServiceDelivery": {"VehicleMonitoringDelivery": {"VehicleActivity": list(activities)}}}}


def test_timestamp_matches_stored_format_in_local_time():
    # Existing rows: '2026-09-14 21:07:49.000000' (PDT wall time, microseconds).
    assert to_local_text("2026-09-15T04:07:49Z") == "2026-09-14 21:07:49.000000"
    assert to_local_text("2026-09-14T21:07:49-07:00") == "2026-09-14 21:07:49.000000"


def test_timestamp_uses_standard_time_in_winter():
    assert to_local_text("2026-01-15T20:00:00Z") == "2026-01-15 12:00:00.000000"


def test_parses_vehicle_with_monitored_call():
    rows = parse_vehicles(payload(activity()))
    assert rows == [("168", "70012", 37.5, -122.3, "2026-09-13 21:07:49.000000")]


def test_falls_back_to_first_onward_call():
    rows = parse_vehicles(payload(activity(monitored=None, onward=["70021", "70031"])))
    assert rows[0][1] == "70021"


def test_unknown_stop_when_no_calls():
    rows = parse_vehicles(payload(activity(monitored=None)))
    assert rows[0][1] == "unknown"


def test_skips_malformed_activity_and_keeps_the_rest():
    bad = {"RecordedAtTime": "2026-09-14T04:07:49Z", "MonitoredVehicleJourney": {"VehicleRef": "1"}}
    rows = parse_vehicles(payload(bad, activity(vehicle="170")))
    assert [r[0] for r in rows] == ["170"]


def test_no_vehicles_overnight():
    empty = {"Siri": {"ServiceDelivery": {"VehicleMonitoringDelivery": {}}}}
    assert parse_vehicles(empty) == []
```

- [x] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_collector.py -v`
Expected: collection error, `ModuleNotFoundError: No module named 'src.collector'`.

- [x] **Step 3: Implement**

```python
"""
Poll 511's Caltrain vehicle feed once a minute and append positions to SQLite.

Stdlib only and no src.* imports, so the container is python:3.12-slim plus this file.
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/Los_Angeles")
# The text format SQLAlchemy wrote for existing rows. The unique index on
# (trip_id, stop_id, timestamp) compares text, so this must not change.
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

log = logging.getLogger("collector")


def to_local_text(recorded_at: str) -> str:
    """511's ISO timestamp as Pacific wall time in the stored text format."""
    return datetime.fromisoformat(recorded_at).astimezone(LOCAL_TZ).strftime(TIMESTAMP_FORMAT)


def _next_stop(journey: dict) -> str:
    stop = (journey.get("MonitoredCall") or {}).get("StopPointRef")
    if not stop:
        onward = (journey.get("OnwardCalls") or {}).get("OnwardCall") or []
        stop = onward[0].get("StopPointRef") if onward else None
    return stop or "unknown"


def parse_vehicles(payload: dict) -> list[tuple]:
    """(trip_id, stop_id, lat, lon, timestamp) for each vehicle in a SIRI VehicleMonitoring payload."""
    delivery = payload["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"]
    rows = []
    for activity in delivery.get("VehicleActivity") or []:
        try:
            journey = activity["MonitoredVehicleJourney"]
            location = journey["VehicleLocation"]
            rows.append((
                journey["VehicleRef"],
                _next_stop(journey),
                float(location["Latitude"]),
                float(location["Longitude"]),
                to_local_text(activity["RecordedAtTime"]),
            ))
        except (KeyError, TypeError, ValueError) as e:
            log.warning("Skipping malformed vehicle activity: %r", e)
    return rows
```

- [x] **Step 4: Run tests**

Run: `uv run pytest tests/test_collector.py -v`
Expected: 7 passed.

- [x] **Step 5: Commit**

```bash
git add src/collector.py tests/test_collector.py
git commit -m "Add stdlib collector payload parsing"
```

### Task 1.3: SQLite writes with dedup

**Files:**
- Modify: `src/collector.py`
- Test: `tests/test_collector.py`

- [x] **Step 1: Write the failing tests**

```python
from src.collector import connect, save

ROW = ("168", "70012", 37.5, -122.3, "2026-09-14 21:07:49.000000")


def test_save_inserts_and_counts(tmp_path):
    conn = connect(str(tmp_path / "t.db"))
    assert save(conn, [ROW, ROW[:4] + ("2026-09-14 21:08:49.000000",)]) == 2
    assert conn.execute("select count(*) from train_locations").fetchone()[0] == 2


def test_save_ignores_duplicates(tmp_path):
    conn = connect(str(tmp_path / "t.db"))
    save(conn, [ROW])
    assert save(conn, [ROW]) == 0
    assert conn.execute("select count(*) from train_locations").fetchone()[0] == 1


def test_connect_keeps_existing_table_and_rows(tmp_path):
    path = str(tmp_path / "t.db")
    save(connect(path), [ROW])
    assert connect(path).execute("select trip_id, timestamp from train_locations").fetchall() == [
        ("168", "2026-09-14 21:07:49.000000")
    ]
```

- [x] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_collector.py -v`
Expected: `ImportError: cannot import name 'connect'`.

- [x] **Step 3: Implement**: add `import sqlite3` and:

```python
# Matches the table SQLAlchemy created, so a fresh database (tests, a new
# deployment) looks the same as the production one.
SCHEMA = """
CREATE TABLE IF NOT EXISTS train_locations (
    id INTEGER NOT NULL,
    trip_id VARCHAR,
    stop_id VARCHAR,
    vehicle_lat FLOAT,
    vehicle_lon FLOAT,
    timestamp DATETIME,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trip_stop_timestamp ON train_locations (trip_id, stop_id, timestamp);
"""

INSERT = (
    "INSERT OR IGNORE INTO train_locations (trip_id, stop_id, vehicle_lat, vehicle_lon, timestamp) "
    "VALUES (?, ?, ?, ?, ?)"
)


def connect(db_path: str) -> sqlite3.Connection:
    # A long busy timeout: the nightly build and the NAS backup read the same file.
    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA)
    return conn


def save(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    """Insert rows, skipping ones already stored. Returns the number of new rows."""
    before = conn.total_changes
    with conn:
        conn.executemany(INSERT, rows)
    return conn.total_changes - before
```

- [x] **Step 4: Run tests**

Run: `uv run pytest tests/test_collector.py -v`
Expected: 10 passed.

- [x] **Step 5: Commit**

```bash
git add src/collector.py tests/test_collector.py
git commit -m "Write collector rows with INSERT OR IGNORE against the existing unique index"
```

### Task 1.4: Fetch, loop and entry point

**Files:**
- Modify: `src/collector.py`
- Test: `tests/test_collector.py`

- [x] **Step 1: Write the failing tests**

```python
import threading

from src.collector import run_forever, run_once


def test_run_once_saves_fetched_rows(tmp_path):
    conn = connect(str(tmp_path / "t.db"))
    assert run_once(conn, lambda: payload(activity(), activity(vehicle="170"))) == 2


def test_run_forever_survives_fetch_errors_and_stops(tmp_path):
    conn = connect(str(tmp_path / "t.db"))
    stop = threading.Event()
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise OSError("HTTP Error 429: Too Many Requests")
        stop.set()
        return payload(activity())

    run_forever(conn, flaky, stop, interval=0)
    assert len(calls) == 2
    assert conn.execute("select count(*) from train_locations").fetchone()[0] == 1
```

- [x] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_collector.py -v`
Expected: `ImportError: cannot import name 'run_forever'`.

- [x] **Step 3: Implement**: add `import json, os, signal, sys, threading, time, urllib.request` and `from typing import Callable`, then:

```python
FEED_URL = "https://api.511.org/transit/VehicleMonitoring?api_key={key}&agency=CT"


def fetch(url: str, timeout: float = 30) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        # 511 prefixes its JSON with a UTF-8 byte-order mark.
        return json.loads(response.read().decode("utf-8-sig"))


def run_once(conn: sqlite3.Connection, get_payload: Callable[[], dict]) -> int:
    rows = parse_vehicles(get_payload())
    new = save(conn, rows)
    log.info("%d vehicles, %d new rows", len(rows), new)
    return new


def run_forever(conn, get_payload, stop: threading.Event, interval: float = 60) -> None:
    # No retries within a cycle: 511 allows 60 requests/hour per key, so a
    # retry would push us over the limit. A failed minute is simply skipped.
    while not stop.is_set():
        started = time.monotonic()
        try:
            run_once(conn, get_payload)
        except Exception:
            log.exception("Collection failed; trying again next interval")
        stop.wait(max(0.0, interval - (time.monotonic() - started)))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    url = FEED_URL.format(key=os.environ["API_KEY"])  # never log the URL, it holds the key
    db_path = os.environ.get("DB_PATH", "data/caltrain_lat_long.db")
    interval = int(os.environ.get("COLLECTION_INTERVAL", "60"))
    conn = connect(db_path)

    if "--once" in sys.argv:
        run_once(conn, lambda: fetch(url))
        return

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    log.info("Collecting every %ds into %s", interval, db_path)
    run_forever(conn, lambda: fetch(url), stop, interval)
    conn.close()
    log.info("Stopped")


if __name__ == "__main__":
    main()
```

- [x] **Step 4: Run tests**

Run: `uv run pytest -v`
Expected: 12 passed.

- [x] **Step 5: Commit**

```bash
git add src/collector.py tests/test_collector.py
git commit -m "Add collector loop and entry point"
```

### Task 1.5: Container

**Files:**
- Create: `Dockerfile.collector`
- Modify: `docker-compose.yaml`
- Modify: `.dockerignore`

- [x] **Step 1: `Dockerfile.collector`**

```dockerfile
# Stdlib only: no pip install. The python image ships tzdata for zoneinfo.
FROM python:3.12-slim
WORKDIR /app
COPY src/collector.py .
CMD ["python", "-u", "collector.py"]
```

- [x] **Step 2: Compose service** (first service in the file):

```yaml
  collector:
    build:
      context: .
      dockerfile: Dockerfile.collector
    environment:
      - API_KEY=${API_KEY}
      - DB_PATH=/app/data/caltrain_lat_long.db
    volumes:
      - ./data:/app/data
    restart: always
    stop_grace_period: 40s  # a fetch in flight can take up to its 30 s timeout
    logging:
      driver: json-file
      options:
        max-size: "5m"
        max-file: "3"
```

- [x] **Step 3: `.dockerignore`**: append `data/`, `archive/`, `notebooks/`, `.venv`. Every service bind-mounts `data/` at runtime; the build context currently uploads the 689 MB DB on every build.

- [x] **Step 4: Validate compose syntax** (on the server, since there's no Docker locally): done in Task 1.7 Step 4 with `docker compose config -q`.

- [x] **Step 5: Commit**

```bash
git add Dockerfile.collector docker-compose.yaml .dockerignore
git commit -m "Run the collector as its own container"
```

### Task 1.6: Remove Prefect collection

**Files:**
- Delete: `src/flows/data_collection.py`, `test_data_collection.py`
- Modify: `src/deployments/deploy_flows.py:18-22` (drop the `data_collection.py` override)
- Modify: `main.py:19` (drop `from src.flows.data_collection import collect_train_data_flow`; it is never called)
- Modify: `README.md` (collection now runs in the `collector` container; Prefect still runs processing and the GTFS update)

- [x] **Step 1: Delete and edit as listed**

- [x] **Step 2: Verify nothing still references it**

Run: `grep -rn "data_collection\|collect_train" --include=*.py --include=*.sh .`
Expected: only `test_direct_db_access.py` (its own unrelated local function).

- [x] **Step 3: Verify the remaining entry points import**

Run: `API_KEY=x uv run python -c "import src.deployments.deploy_flows, src.flows.data_processing, src.flows.gtfs_update"`
Expected: no traceback. (`main.py` imports FastAPI and isn't in the uv env; it gets checked on the server.)

- [x] **Step 4: Run tests**: `uv run pytest`, expected 12 passed.

- [x] **Step 5: Commit**

```bash
git add -A src/flows src/deployments main.py test_data_collection.py README.md
git commit -m "Remove the Prefect collection flow"
```

### Task 1.7: Deploy and cut over (on pve-docker; **confirm with the user before starting**)

All commands run as `ssh jsakkos@192.168.1.122`, in `~/caltrain-prefect`.

- [ ] **Step 1: Snapshot the DB**: `.venv/bin/python scripts/backup_to_nas.py`, then check the new file in `/nfs/share/backups/caltrain/`.
- [ ] **Step 2: Save the server's local edits** (already upstreamed by Task 1.1, but keep a copy): `git diff > ~/caltrain-server-local-2026-10-03.patch` and `cp CLAUDE.md ~/caltrain-server-CLAUDE.md`.
- [ ] **Step 3: Move the checkout to the merged branch**: `git checkout -- . && git fetch && git checkout <branch-or-main> && git pull`. Leave untracked `.claude/`, `.superpowers/`, `CLAUDE.md` alone. Check that `.env` still has `API_KEY`, `DB_USER`, `DB_PASSWORD`, `HOST`.
- [ ] **Step 4: Check config**: `docker compose config -q` (silent on success). Also confirm the gtfs_feeds folders exist (`ls gtfs_feeds | head`); PR #3 processing needs them.
- [ ] **Step 5: Stop Prefect collection first** (one poller per key):
  `docker exec caltrain-prefect-prefect-1 prefect deployment delete "Collect Train Location Data/collect_train_data_flow"`
- [ ] **Step 6: Rebuild Prefect with the new code + asyncpg**, then start the collector:
  `docker compose up -d --build prefect collector`
- [ ] **Step 7: Smoke check**: `docker compose logs -f --tail 20 collector` should show `Collecting every 60s…` then `N vehicles, N new rows` each minute (0 vehicles overnight is normal).
  `docker exec caltrain-prefect-prefect-1 prefect deployment ls` should list only processing and the GTFS update; no collection.
- [ ] **Step 8: Next morning**: the processing flow ran at 00:00 (Prefect UI or `prefect flow-run ls --limit 3`), and the hourly counts match a week earlier:

```bash
.venv/bin/python - <<'EOF'
import sqlite3
c = sqlite3.connect("file:data/caltrain_lat_long.db?mode=ro", uri=True)
q = """select substr(timestamp, 12, 2) h, count(*) from train_locations
       where date(timestamp) = date('now', 'localtime', ?) group by h"""
a, b = dict(c.execute(q, ("-1 day",))), dict(c.execute(q, ("-8 days",)))
for h in sorted(set(a) | set(b)):
    print(h, b.get(h, 0), a.get(h, 0))
EOF
```

**Found during the 2026-10-03 cutover (fixed on the branch):**
- Rebuilding the Prefect image pulled SQLAlchemy 2.1.3, and the Prefect 3.8.7 scheduler failed every minute (`Can't evaluate bulk DML statement`). Pinned `SQLAlchemy<2.1`.
- The `prefect` service had no `API_KEY` in its environment; it only worked through the server's hardcoded key fallback. Without it the GTFS flow failed to deploy. Added `API_KEY=${API_KEY}`.
- Prefect cron schedules run in UTC (see Task 3.1).

**Rollback:** `docker compose stop collector`, `git checkout 3dbae33 && git apply ~/caltrain-server-local-2026-10-03.patch`, then `docker compose up -d --build prefect`. That re-registers the collection deployment from the old code.

---

# Phase 2: Incremental DuckDB nightly build

Exit criteria: `python -m src.pipeline.build` produces the 10 dashboard JSON files plus the 2 plots; `scripts/compare_outputs.py` shows parity with the pandas flow on the same DB snapshot (differences only where explained below); a nightly incremental run takes under 60 s on pve-docker. Phase 2 gets its own step-level plan (`docs/superpowers/plans/<date>-duckdb-build.md`) written once Phase 1 is live, since its parity baseline depends on the Phase 1 code that will be running in production.

### Task 2.1: Parity baseline
Copy a consistent DB snapshot from the server (backup API, see memory notes). Run the current `process_data_flow` locally with `STATIC_CONTENT_PATH` pointed at `parity/old/`, and record its runtime. These outputs are the reference.

### Task 2.2: `src/pipeline/arrivals.py`: arrivals in DuckDB
- DuckDB file `data/analytics.duckdb` with tables `arrivals` (one row per trip/stop/date: `trip_id, stop_id, date, actual_arrival, scheduled_arrival, delay_minutes, delay_severity, commute_period, hour, stop_name, parent_station, stop_lat, stop_lon, feed_version`) and `processed_days(date, feed_version, computed_at)`.
- Days to compute = days with pings but no `processed_days` row, plus days whose `feed_version` from `schedule_for_dates` differs from the stored one (this catches a newly backfilled historic archive), plus "yesterday" and "today" always. `--full` recomputes everything.
- Read pings with `sqlite_scan(db, 'train_locations')` filtered with `timestamp >= ? AND timestamp < ?` (a text range; unlike `date(timestamp)` it can use an index once phase 3 adds one on `timestamp`).
- Schedule: register `schedule_for_dates(days)` (existing pandas function, cached) as a DuckDB relation.
- Distance in SQL (haversine via `radians/sin/cos/asin`); closest approach via `QUALIFY row_number() OVER (PARTITION BY trip_id, stop_id, date ORDER BY distance, timestamp) = 1`. This matches the current "min distance, then first row" semantics.
- Delay semantics must match `calculate_time_difference` + `normalize_time` exactly for the parity run, **including** the current behaviour where GTFS times ≥ 24:00 are normalised to hour % 24 on the ping's date and outliers are clamped (`>500 → 0`, `<-100 → 0`, `<0 → 0`). Fixing that is a separate, deliberate change (Task 2.6).
- Tests: tiny fixture SQLite + a fixture GTFS folder under `tests/fixtures/`: closest-approach selection, tie-break, delay severity boundaries (4, 15), commute periods, incremental rerun touching only new days, feed-version change triggering recompute.

### Task 2.3: `src/pipeline/dashboard.py`: outputs
One SQL query per file over `arrivals`, written with the same keys, rounding and sort order as `generate_dashboard_data`: `stats.json`, `daily_performance.json`, `station_performance.json`, `train_performance.json`, `hourly_heatmap.json`, `commute_analysis.json`, `weekly_summary.json`, `monthly_summary.json`, plus `summary_stats.json` and `processed_arrivals.csv` if `export_to_website.py` still reads them (check first; drop them if not). The two plotly HTML plots are built from the daily/commute aggregates (small frames).

### Task 2.4: `src/pipeline/incidents.py`
Port `detect_incidents`. Get each incident day's GPS through one DuckDB query (`timestamp` range); vectorise `project_to_route` with numpy (points × segments matrix: 398 segments × ~11k pings per day fits easily in memory). Resolve `gtfs_data` and `data/station_metadata.json` from the repo base dir instead of the CWD. Test: projection matches the scalar `project_to_route` within 1 m on 1,000 random points.

### Task 2.5: `src/pipeline/build.py` + parity
CLI: `python -m src.pipeline.build [--full] [--out static/data]`. Writes to a temp dir and renames into place, so the 02:30 export never sees half-written files. `scripts/compare_outputs.py old/ new/` diffs the JSON (numbers within 0.05; lists compared as sets keyed by date/stop/trip). Run `--full` on the snapshot, compare, and explain or fix every difference. Record runtimes for `--full` and incremental runs.

### Task 2.6 (optional, separate PR): fix the after-midnight delay bug
Trains scheduled at `24:xx`/`25:xx` are currently scored against the wrong day. Score them against `service_date + interval` instead. This changes published numbers, so it ships separately with a note.

### Task 2.7: Switch production
On the server: `uv sync` the host venv (adds duckdb), run `build --full` once, and add cron `5 0 * * *` (Phase 3 Task 3.1 format). Delete the Prefect processing deployment and remove `src/flows/data_processing.py` from the code in the same deploy (constraint 4).

---

# Phase 3: Retire Prefect, Postgres and the app container; host cron

Exit criteria: `docker compose ps` shows only `collector`; the host crontab is the checked-in `deploy/crontab.txt`; freed space confirmed; README describes the new stack.

### Task 3.1: `deploy/crontab.txt` (checked in, installed with `crontab deploy/crontab.txt`)

The host clock is **UTC** (`timedatectl`: Etc/UTC), and the Prefect deployments had `timezone: None`, so "midnight" processing actually ran at 17:00 Pacific and the GTFS check at 16:30. Decide with the user which local time they want; the crontab below pins Pacific explicitly with `CRON_TZ` (supported by cronie and Debian cron ≥ 3.0pl1-133; check with `man 5 crontab` first, otherwise convert the times to UTC).

```cron
# Caltrain tracker. Times are America/Los_Angeles; the host clock is UTC.
CRON_TZ=America/Los_Angeles
PY=/home/jsakkos/caltrain-prefect/.venv/bin/python
APP=/home/jsakkos/caltrain-prefect
30 23 * * *  cd $APP && flock -n /tmp/caltrain-gtfs.lock  $PY scripts/check_gtfs_updates.py   >> /home/jsakkos/caltrain-gtfs.log 2>&1
5 0 * * *    cd $APP && flock -n /tmp/caltrain-build.lock $PY -m src.pipeline.build           >> /home/jsakkos/caltrain-build.log 2>&1
15 2 * * *   $PY $APP/scripts/backup_to_nas.py      >> /home/jsakkos/caltrain-backup.log 2>&1
30 2 * * *   $PY $APP/scripts/export_to_website.py  >> /home/jsakkos/caltrain-export.log 2>&1
*/15 * * * * cd $APP && $PY scripts/check_freshness.py >> /home/jsakkos/caltrain-freshness.log 2>&1
```
Check the host timezone first (`timedatectl`); the Prefect container used `TZ=America/Los_Angeles`.

### Task 3.2: `scripts/check_freshness.py`
Exit non-zero and log loudly if there is no row in `train_locations` newer than 10 minutes during service hours (05:00–01:30 local). Optional `HEALTHCHECK_URL` env var: if set, ping it on success (healthchecks.io-style) so failures alert by email. Ask the user whether they want an account for that; otherwise it only logs.

### Task 3.3: Remove services
`docker compose stop prefect postgres app && docker compose rm -f prefect postgres app`. Remove them from `docker-compose.yaml`. **Ask before** `docker volume rm caltrain-prefect_postgres_data` (7.4 GB, Prefect run history only, permanent).

### Task 3.4: Delete dead code and dependencies
Delete the files listed in the File map (phase 3 rows). Collapse dependencies into `pyproject.toml` only (duckdb, numpy, pandas<3, plotly, requests, python-dotenv, pytz); drop sqlalchemy, alembic, prefect, psycopg2, asyncpg, fastapi, uvicorn, dash. Set `requires-python = ">=3.12"` to match the collector image. `src/config.py`: drop the Prefect/FastAPI/Dash/Postgres settings and the debug prints. `.env.example`: drop the Prefect/Postgres/Dash keys.

### Task 3.5: Index for range reads
`CREATE INDEX IF NOT EXISTS idx_train_locations_timestamp ON train_locations (timestamp)`, run once on the server during a quiet minute. Drop the redundant `ix_train_locations_id` (it duplicates the primary key).

### Task 3.6: Docs
README architecture: collector container → SQLite → nightly DuckDB build (cron) → `static/data` → export → MyWebsite/Netlify. Ops section: crontab, logs, manual `--full` rebuild, rollback. Update the memory note about containers on pve-docker.
