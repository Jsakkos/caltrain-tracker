# Data audit: what the Caltrain tracker actually collects

Audited 2026-10-22 against the live deployment on pve-docker (`~/caltrain-prefect`), the repo at commit `affa73f`, and 511's published documentation. No code was changed. Every number below was measured on the production SQLite/DuckDB files or the committed GTFS archives unless marked **uncertain**.

## Summary

**What you have.** One feed, polled once a minute: 511's SIRI VehicleMonitoring for agency CT. Five fields per vehicle per poll are kept: train number, next stop, latitude, longitude, and 511's `RecordedAtTime` as Pacific wall time. That is 4.54 M pings over 419 service days (2025-08-10 to today) in a 723 MB SQLite file, plus a 26 MB DuckDB table of 729 k derived arrivals (one per train, stop, service day). Schedules are versioned: 4 published GTFS feeds and 13 monthly 511 archives (Aug 2025 to Aug 2026) that record exactly which trips ran each day and also carry 511's own observed arrival times.

**What the data supports well.** Per-train, per-stop actual-vs-scheduled times at roughly one-minute precision for about 90% of scheduled stop calls on a healthy day, across 14 months and three timetable changes. That is enough for historical delay analysis, delay-accumulation profiles along a trip, following-train coupling at a stop, and retrospective incident/anomaly detection. The signal is real: delay at consecutive stops of the same trip correlates at 0.95, delay of consecutive trains at the same stop at 0.56, and the delayed share climbs steadily from 2% at 5 am to 10% at 6 pm.

**What it does not support.** Anything real-time (the pipeline is a nightly batch and no predictions or expected times are stored), cancellations (never recorded; a scheduled trip that never appears is indistinguishable from a feed miss), dwell or departure times, trains after midnight (511 drops every vehicle at 00:00 Pacific), and departure punctuality at the origin terminal (a train sitting at the platform for 60 minutes has no meaningful "closest approach").

**The five problems that matter most for a redesign**, in order:

1. **Feed-quality episodes that nothing detects.** In two windows (2026-07-28 to 08-16 and 2026-10-10 to 10-17) 511 kept returning vehicles but with stale positions, so arrivals per weekday fell from ~1,970 to ~1,300 and the closest ping was within 100 m of the platform for only 21% of arrivals instead of 37%. The freshness check saw pings arriving and reported OK. Any analysis that doesn't mask these windows will find them as "anomalies".
2. **Terminal-stop arrivals inflate on-time performance.** 82,562 arrivals (11%) are at San Francisco, San Jose Diridon, Tamien, or Gilroy; 34,958 of them are more than 5 minutes "early" (the train was parked) and are clamped to delay 0, severity On Time.
3. **Only positive delay is published.** 52% of stored arrivals have delay exactly 0 because early arrivals and two clamps (< -100 min, > 500 min) are zeroed. Signed delay is recoverable from stored columns, so this is a pipeline choice, not a data loss.
4. **Gaps.** A 19-day outage (2026-09-14 to 10-03), two days wholly missing (2026-01-31, 02-01), several multi-hour daytime gaps, a nightly blackout from 00:00 to ~03:00, and 104 request timeouts in the last ten days alone (88 of them on 2026-10-21). You would not have been notified of any of them: the healthcheck URL is unset.
5. **The collector throws away most of the feed.** Per 511's specification the same payload carries direction, dated journey reference, destination, bearing, occupancy, "vehicle at stop", and aimed/expected arrival and departure times for the monitored call and every onward call. None of that is stored, and the GTFS-Realtime TripUpdates feed (cancellations, skipped stops, per-stop delays) is not polled at all. Storing the raw payload would cost roughly 1 to 2 MB a day compressed.

Feasibility in one line each: historical delay analysis is possible now; delay propagation is possible now at the descriptive level, with causal attribution needing extra fields; anomaly detection is possible now retrospectively and feasible intra-day with a small collector change; real-time lateness forecasting is not possible now and needs TripUpdates or expected-time capture plus an intra-day job, after which a calibrated short-horizon forecast is realistic.

---

## 1. Data sources

### 1.1 Real-time: 511 SIRI VehicleMonitoring

| Item | Value | Evidence |
|---|---|---|
| Endpoint | `api.511.org/transit/VehicleMonitoring?agency=CT` (JSON) | `src/collector.py` |
| Cadence | every 60 s, one request, no retries | `run_forever` in `src/collector.py`; container log shows one line per minute |
| Rate limit | 60 requests / 3600 s per key (511 default; higher on request by email) | 511 open-data page |
| Headroom | none: the collector uses the whole allowance. A second poller or a manual fetch on the same key costs a minute of data | README warning; arithmetic |
| Fields kept | `VehicleRef` → `trip_id` (public train number, e.g. `104`); `MonitoredCall.StopPointRef`, falling back to first `OnwardCall`, else `'unknown'` → `stop_id`; `VehicleLocation` lat/lon; `RecordedAtTime` converted to Pacific wall-time text | `parse_vehicles` |
| Fields discarded | everything else in `MonitoredVehicleJourney` | same |
| Vehicles per poll | 9.5 to 10.4 on average per month; 1 to 16 within a weekday | `rows_per_minute_by_month`, `vehicles_per_minute_sample` |

What the SIRI VM payload contains per 511's specification (example in the spec is BART; **uncertain** whether Caltrain populates every element, which one captured raw payload would settle): `LineRef`, `DirectionRef`, `FramedVehicleJourneyRef/DatedVehicleJourneyRef` (the GTFS trip id), `OriginRef`, `DestinationRef`, `Monitored`, `VehicleLocation`, `Bearing`, `Occupancy`, `VehicleRef`, `ProgressStatus`, and for `MonitoredCall` and each `OnwardCall`: `StopPointRef`, `VehicleAtStop`, `AimedArrivalTime`, `ExpectedArrivalTime`, `AimedDepartureTime`, `ExpectedDepartureTime`. The spec also shows `PreviousCalls` with `ActualDepartureTime` in the stop-monitoring example.

Two semantics of the stored timestamp that matter for analysis:

- `RecordedAtTime` advances every poll even when the position does not. On a healthy weekday 42 to 49% of consecutive pings for the same train repeat the previous lat/lon exactly; in the degraded October window it was 82%. The seconds component is identical across all of a train's rows for a day (e.g. every arrival of train 104 on 2026-03-10 ends in `:54`–`:57`), so it is a feed clock, not a GPS fix time. **The age of each position is therefore unknown**, and "arrival time" precision is bounded below by however stale the position was when it was closest to the platform.
- Since the unique index is on (trip, stop, timestamp), a poll where 511 repeats the previous `RecordedAtTime` is silently dropped. Counts suggest this is rare (rows per day ≈ minutes × vehicles).

### 1.2 Not polled (available on the same 511 account)

- **GTFS-Realtime TripUpdates** (`/transit/tripupdates?agency=CT`, protobuf): per-trip `schedule_relationship` (SCHEDULED / CANCELED / ADDED), per-stop `stop_time_update` with arrival/departure delay or time and SKIPPED stops. This is the only source that would make cancellations and skipped stops observable.
- **GTFS-Realtime VehiclePositions**: the same positions as VM plus `current_status` (IN_TRANSIT_TO / STOPPED_AT) and a vehicle timestamp.
- **ServiceAlerts** and **StopMonitoring**.

Each would need its own API key (or a raised limit) because the current key is saturated.

### 1.3 Schedules: GTFS, versioned

| Store | Contents | How chosen |
|---|---|---|
| `gtfs_feeds/v<version>/` | 4 published feeds: 20240920, 20251107, 20260611, 20260922 (the current one, valid 2026-01-31 to 2027-01-31) | `scripts/check_gtfs_updates.py` nightly at 01:30 UTC; new versions appended, never overwritten |
| `gtfs_feeds/historic-YYYY-MM/` | 13 monthly 511 regional archives, Caltrain rows only, Aug 2025 to Aug 2026. `calendar_dates.txt` lists exactly which trip version ran each day (holidays, mid-month changes). Also 511's `stop_observations.txt` (observed arrivals) | `scripts/backfill_gtfs_history.py`, manual |
| `gtfs_data/` | copy of the newest published feed (stops, shapes for the Marey projection) | refreshed by the GTFS check |

`src/data/gtfs_feeds.py` picks, per date, a monthly archive if one covers the date, else the published feed with the latest start that covers it. Arrivals from 2026-09-01 onward are scored against published feed 20260922 because the September and October 2026 archives have not been backfilled (September should be available now; **uncertain**, not checked against the API).

Realtime `VehicleRef` is the public train number; schedule trip ids like `M160:20260831` are reduced to `160` (the `M` modified-service prefix is stripped) before joining. Weekday schedules have 112 trips and 2,142 stop calls; weekends 66 trips and ~1,552 calls.

### 1.4 511 `stop_observations.txt` (in the monthly archives)

A second, independent record of arrivals. For Aug 2026: 57,013 rows, 178 trips, 31 days, 1 to 46 calls per trip-day (median 21), `schedule_relationship` always 0, `uncertainty` always 0, 1,974 rows for after-midnight calls (which our GPS data can never observe). At non-origin stops the observed-minus-scheduled delay has median 0.3 min, p90 3.8, p99 16.8, and 9.3% of calls are > 4 min late, close to our 6.6%. At origin stops (`stop_sequence = 1`) every observed time equals the scheduled time, so origin rows carry no information. An earlier check (Aug 2025, recorded in session memory, **not re-verified here**) found 93.8% agreement within 2 minutes against our GPS arrivals and no terminal-stop rows.

### 1.5 Earlier collection eras

- **2024-07-07 to 2024-09-21**: 344,400 pings, 136 trains, in the git-LFS file `archive/data/caltrain_lat_long.db` (34 MB). Same five columns but ISO timestamps without microseconds (`2024-07-07T15:48:54`), so it would need a format shim to merge.
- **2024-11-29 to 2025-03-14**: `docs/static/start_date.txt`, `stop_date.txt` and `n_datapoints.txt` (174,593 points) describe a dashboard built on data from this period. **I could not find that data** in the repo, LFS, the server, or the NAS retention window. Treat it as lost unless you know where it lives.
- **2025-03-14 to 2025-08-10**: nothing found.

## 2. Storage

### 2.1 Raw pings: `data/caltrain_lat_long.db` (SQLite, 723 MB, root-owned, bind-mounted into the collector)

```
train_locations(id INTEGER PK, trip_id VARCHAR, stop_id VARCHAR,
                vehicle_lat FLOAT, vehicle_lon FLOAT, timestamp DATETIME)
UNIQUE (trip_id, stop_id, timestamp); indexes on stop_id, trip_id, id
```

| Measure | Value |
|---|---|
| Rows | 4,543,320 (ids 1 to 4,543,320, contiguous) |
| Range | 2025-08-10 11:14:40 to 2026-10-22 20:04:14 Pacific |
| Days with data | 419 (of 439 calendar days) |
| Weekday / weekend volume | ~12,700 / ~7,800 rows per day; ~1,190 / ~1,040 distinct minutes per day |
| Distinct `trip_id` | 1,097, of which 890 are junk from two polls (see 3.6); ~200 real train numbers |
| Distinct `stop_id` | 58 platforms + `unknown` (8,270 rows, 0.18%) |
| Timestamp text | always 26 chars `YYYY-MM-DD HH:MM:SS.000000`, microseconds always zero |
| Pings per train per day | mean 109, max 785 |
| Pings at the two terminals' platforms | 1.57 M (35%): trains parked at SF or SJ reporting the terminal as their next stop |

Three empty legacy tables (`stops`, `trips`, `arrival_data`) remain from the SQLAlchemy era. Four indexes on a 5-column table are why the file is ~160 bytes per row.

### 2.2 Derived arrivals: `data/analytics.duckdb` (26 MB)

```
arrivals(date, trip_id, stop_id, stop_name, parent_station, arrival_time (GTFS text, may be 24:xx),
         actual_arrival_time, delay_minutes (≥0), is_delayed, delay_severity, commute_period,
         hour, ping_count, feed_version)
processed_days(date PK, feed_version)
```

729,359 rows, 419 service days, 185 train numbers, 58 stops. Recomputed incrementally each night for new days, the last two Pacific days, and any day whose schedule version changed. A full 14-month rebuild takes about 15 s.

Arrival rule: per (train, stop, service day), the ping with the smallest haversine distance to the platform among pings whose reported next stop is that platform; ties go to the earliest row. The service day for a ping is whichever of its calendar date or the previous date puts the scheduled call closest in time. Delay = ping time minus scheduled time; values < -100 or > 500 min are set to 0; negatives are then clipped to 0; severity thresholds > 4 (Minor) and > 15 (Major).

Signed delay (including early) is recoverable as `actual_arrival_time - (date + arrival_time)`; nothing needs re-collecting for that.

### 2.3 Website outputs

Nightly: `static/data/*.json` (stats, daily, station, train, hourly heatmap, commute, weekly, monthly, incidents, incident trajectories) and two Plotly HTML files, copied into the MyWebsite repo and pushed to Netlify at 02:30 UTC. These are aggregates only; the site never receives per-arrival data.

### 2.4 Backups

`scripts/backup_to_nas.py` at 02:15 UTC: SQLite via the backup API, gzipped, plus a GTFS tarball, to `/nfs/share/backups/caltrain/` with 7 daily, 4 weekly, 6 monthly copies. Last run 2026-10-23 02:16 UTC succeeded in 81 s. Retention means a silent corruption older than six months is unrecoverable from backups, but the live file is append-only and has never been rewritten.

## 3. Quality

### 3.1 Outages and gaps in collection

| Window (Pacific) | Length | Cause / note |
|---|---|---|
| 2026-09-14 21:07 → 2026-10-03 21:44 | 19 days | Old Prefect collector died (minutes after a `.env` edit, per session notes); unnoticed until the rewrite. Row ids contiguous, so nothing was deleted. Not backfillable. |
| 2026-01-31, 2026-02-01 | 2 whole days; 02-02 starts 08:15 | No rows at all. Cause unknown. |
| 2026-05-27 19:41 → 05-28 07:44 | 12 h | Unknown. |
| 2025-12-20 14:30 → 21:06 | 6.6 h | Unknown (a Saturday). |
| 2026-05-21 07:35 → 09:22 | 1.8 h | Follows the poll that returned 450 foreign vehicles (3.6). |
| 2025-10-21 19:37 (76 min), 2025-11-04 22:09 (68 min), 2026-06-10 22:24 (76 min), 2026-02-21 05:37 (49 min), 2026-10-16 ~19:49 (61 min, 511 returned 0 vehicles) | ~1 h each | Mix of 511 outages and unknown |
| 2025-10-17 start 07:23, 2026-02-02 start 08:15 | late starts | Unknown |
| ~15 further daytime gaps of 11 to 33 min | | timeouts / 511 hiccups |

In total there are 25 daytime gaps longer than 10 minutes in 14 months, versus a nightly one (next row).

**Nightly blackout.** 511 reports zero vehicles from 00:00 Pacific until the first morning departures (~03:00 weekdays, ~05:55 weekends), even for trains still running. Hour 00 holds 259 rows in total, hours 01 and 02 hold none. Consequence: the 1,316 arrivals for scheduled calls at 24:xx are scored from the last pre-midnight ping and 1,300 of them come out as delay 0. Exclude `arrival_time >= '24:'` from any analysis.

**Request failures.** The container keeps ~10 days of logs (3 × 5 MB). In that window: 104 read timeouts (30 s), one HTTP 500, one HTTP 503; 88 of the timeouts fell between 11:00 and 17:00 on 2026-10-21. Each failure is one missing minute; the collector deliberately does not retry.

### 3.2 Feed-quality episodes (positions arrive but are stale)

This is the finding most likely to bite a deeper analysis, because pings keep flowing and the freshness check stays green.

| Sample weekday | Arrivals / 2,142 scheduled calls | Closest ping within 100 m | Closest ping > 3 km | Stops seen per train (median) | Distinct positions per train (median) | Consecutive pings with position change |
|---|---|---|---|---|---|---|
| 2025-09-10 | 1,966 | 710 | 49 | 21 | 69 | 58% |
| 2026-03-10 | 1,918 | 374 | 352 | 20 | 42 | 51% |
| 2026-08-12 (in 07-28 → 08-16 window) | 1,822 | **47** | 447 | 19 | 38 | n/a |
| 2026-09-10 | 1,966 | 697 | 12 | 21 | 71 | n/a |
| 2026-10-08 | 1,971 | 761 | 32 | 21 | 70 | n/a |
| 2026-10-15 (in 10-10 → 10-17 window) | **1,264** | 266 | 277 | **11.5** | **18** | **18%** |

During 2026-07-28 to 08-16 the feed also reported only ~7 vehicles per poll instead of ~11, then snapped back on 08-17. The 2026-08-12 row shows that arrival counts alone do not reveal degradation: the arrivals exist, they are just kilometres off. During 2026-10-10 to 10-17, weekday arrivals were 1,264 to 1,369 and weekends 898 to 1,040; on 10-19 everything returned to normal (1,975). Both windows span the old and new collector in behaviour, and the collector code stores exactly what 511 sends, so I attribute them to 511; **uncertain** whether Caltrain's own AVL or 511's aggregation was at fault.

Practical rule: compute a per-day feed-health score (vehicles per poll, share of pings whose position changed, median closest-approach distance) and mask days below threshold. Those three statistics are cheap to derive from the existing table.

### 3.3 Arrival-detection artifacts

| Artifact | Size | Effect |
|---|---|---|
| Terminal-stop "arrivals" (SF, SJ Diridon, Tamien, Gilroy) | 82,562 (11.3%); 34,958 of them > 5 min early | Clamped to 0 → counted On Time. SF southbound shows 0.0 min average delay, 0% delayed. Origin departures are unmeasurable with this method. |
| Early > 5 min at non-terminal stops | 802 | Mostly mis-detections (e.g. train 104 on 2026-10-15 "arrived" at Hayward Park at 03:53 for a 06:02 call, from a single ping while parked in SF). |
| Raw delay < -100 min zeroed | 212 | Hidden as On Time. |
| Raw delay > 500 min zeroed | 0 | |
| Single-ping arrivals (`ping_count = 1`) | 18,559 (2.5%) | Least reliable; 6% of them are flagged delayed vs 6.6% overall, so not grossly biased. |
| Arrivals with ≥ 2 pings and signed delay in [-5, 500] min | **92.6%** | A reasonable "clean" subset. |

Signed delay at non-terminal stops (clean subset): p5 -1.2, p25 -0.2, median +0.1, p75 +1.8, p95 +5.8 min. The distribution is sensible and roughly symmetric around zero, which supports the one-minute-precision claim.

Precision: polls are 60 s apart and positions often repeat, so each arrival time is quantised to about ±1 min on good days and worse on stale days. Delays below ~2 min are noise; the 4-minute on-time threshold is comfortably above it.

Coverage: on healthy weekdays 1,920 to 1,978 of 2,142 scheduled calls get an arrival (~91%), weekends ~1,430 of 1,552 (~92%). Missing calls are mostly short inter-station hops where the train passed the stop between polls, and express trains' skipped stops that the schedule lists but the realtime never names.

### 3.4 Timezone handling

- Stored timestamps are naive Pacific wall time, converted from 511's ISO instants with `zoneinfo`. Tests cover PDT and PST.
- The fall-back hour (01:00 to 02:00 on 2025-11-02) and spring-forward hour (02:00 on 2026-03-08) contain no rows because of the nightly blackout, so the ambiguity of naive local time never materialises. If 511 ever starts reporting overnight, the fall-back hour becomes ambiguous; storing UTC alongside would remove the risk.
- Scheduled times are GTFS "time since service-day midnight" added to the service date as naive timestamps. On DST-change nights that is off by an hour for calls after the change, but again no pings exist then.
- Host clock and cron are UTC; the build derives "today" in Pacific. Freshness check compares in Pacific.
- The 2024 archive uses a different timestamp format (ISO `T`, no microseconds).

### 3.5 Duplicates

The unique index prevents exact duplicates. 57 (train, timestamp) pairs appear twice with different `stop_id` (511 changed the reported next stop between two rows with the same `RecordedAtTime`); negligible. No duplicate arrivals by construction.

### 3.6 Bad records

- **Foreign vehicles.** Two single polls, 2026-05-21 07:35:54 and 2026-07-01 22:27:01, returned ~450 vehicles each with four-digit or `9xxA` ids and `unknown` stops, located around San Jose (likely VTA or another agency leaking into `agency=CT`). They never match a schedule so produce no arrivals, but they are why `train_locations` has 1,097 distinct `trip_id` values and why `trains_affected`-style counts from raw pings would be wrong on those two days.
- **Bad coordinates.** 827 rows with latitude ≈ 0.03 (GPS zero), concentrated in 2026-01/02. They lose any closest-approach contest unless they are the only ping.
- **`unknown` stop.** 8,270 rows; ignored by the join.

### 3.7 Cancelled and added trips

Nothing in the pipeline detects either. The proxy "scheduled trip with zero pings all day" yields one trip on most sampled days (train 176 on 2026-03-10 and 2026-08-12; train 643 on 2025-12-25), which could be a real cancellation or simply a train the feed never carried; the data cannot tell them apart. Added or modified trips appear in the monthly archives as `M`-prefixed trip ids and are scored under the base number. Only TripUpdates would make cancellations observable.

### 3.8 Schedule changes over time

Within the audited period: archive-backed schedules for Aug 2025 to Aug 2026, then published feed 20260922 for Sep and Oct 2026. Published feed 20251107 ran 2025-06-16 to 2026-04-01 and 20260611 from 2026-01-31; the archives capture the actual per-day variations (the trip counts per archive range from 179 to 518 trip versions in a month, reflecting mid-month changes and holidays). Every arrival row carries `feed_version`, so a redesign can show which timetable each day was measured against. The 2026-01 archive ends on 2026-01-30 and 2026-02 starts on 02-02, which matches the two missing data days, so those days would score against a published feed if pings existed.

## 4. Granularity

- **Per-train, per-stop actual vs scheduled**: yes, 729 k rows, one-minute precision, ~91% of calls, for arrivals only. Departure times and dwell are not derivable: with one position a minute and frequently stale positions, the difference between arrival and departure at a 30 to 60 s dwell is below the noise floor. Origin departures are not measurable at all.
- **Vehicle positions**: yes, raw GPS tracks at one-minute cadence for every train (109 pings per train-day on average). This is more than arrival prediction: it already drives the Marey diagrams, and it supports speed profiles, held-position detection, and location-of-delay-onset, subject to the staleness caveat.
- **Arrival predictions**: none stored. 511's `ExpectedArrivalTime` values are in the payload but discarded.
- **Train identity**: the public train number only. No vehicle/consist id, no block id (blank in Caltrain's GTFS), no direction field (direction is inferred from stop ids; stop ids ending in 1 are northbound, 2 southbound).
- **Trip identity**: schedule `trip_id` with date suffix is lost; only the number survives, which is fine because the archives make number + date unique.

## 5. Collection reliability

**How it runs.** One Docker container (`caltrain-prefect-collector-1`, `restart: always`, stdlib Python, ~20 MB RAM) appending to SQLite over a bind mount. Host cron (UTC) runs the GTFS check (01:30), build (02:00, ~37 s), NAS backup (02:15), website export (02:30) and a freshness check every 15 minutes. The host was rebooted on 2026-10-18 and the container came back on its own (0 restarts since). Disk: 346 GB free.

**Failure modes observed or latent.**

| Mode | Observed? | Detection today |
|---|---|---|
| 511 request timeout / 5xx | yes, ~10/day baseline, 88 in one afternoon | container log only; no counter persisted |
| 511 returns 0 vehicles during service | yes, 2026-10-16 evening (61 min) | STALE in log after 10 min, no notification |
| 511 returns stale positions | yes, two multi-day windows | **nothing** |
| 511 returns foreign vehicles | yes, two polls | nothing |
| Collector process dies | yes, 19 days in Sep 2026 | STALE in log; no notification |
| `.env` / key change not propagated to container | suspected cause of the Sep outage | nothing |
| Rate-limit 429 from a second consumer of the key | latent | would appear as failed minutes |
| SQLite locked by build/backup | latent; 60 s busy timeout in collector | log |
| Host reboot | yes | restart policy handled it |

**How you would know it broke.** Today: only by reading `~/caltrain-freshness.log` or noticing the website's last date stop moving. `check_freshness.py` supports a healthchecks.io-style `HEALTHCHECK_URL`, but it is not set in the server's `.env`, so STALE lines (43 so far since 2026-10-03) go nowhere. Several of those are false alarms on weekend mornings between 05:00 and ~05:56 when no trains run yet (the quiet window ends at 05:00 regardless of day). Nothing checks that the build, export or backup succeeded either, beyond their logs.

**What is not logged at all.** Per-poll outcome (success / timeout / 0 vehicles / N vehicles) is not persisted, so after the Docker log rotates (~10 days) there is no way to reconstruct whether a quiet period was a 511 outage, a collector outage, or no service. A tiny `polls` table (poll time, status, vehicle count, latency) would fix this retroactively from now on.

## 6. Feasibility

### 6.1 Historical delay analysis

**Possible now.** Fourteen months of per-stop arrivals against the correct timetable for each day. Reasonable analyses with no new collection:

- On-time share and delay distribution by station, direction, hour, weekday, month, train number, service type (local vs express via trip pattern), and timetable version; rolling trends; best/worst days; seasonal and holiday effects (the archives name holidays precisely).
- Delay accrual along the line: for each trip, delay at stop k+1 minus delay at stop k, aggregated by segment and hour. This is where delay is created versus carried.
- Recovery: how much schedule padding trains make up, visible as negative accrual on segments.
- Comparison of our GPS-derived delays with 511's `stop_observations` for Aug 2025 to Aug 2026 as a validation chapter.

Required hygiene (all doable in the build): use signed delay; drop terminal-stop rows or treat terminals as arrival-only for the inbound direction; drop `24:xx` calls; drop `ping_count = 1` rows or weight them down; mask feed-degradation days; exclude the two foreign-vehicle polls.

**Possible with collection changes.** Dwell and departure punctuality (needs `VehicleAtStop`/`ActualDepartureTime` or TripUpdates). Cancellation rates (TripUpdates). Occupancy by train and time (if Caltrain populates `Occupancy`; **uncertain**). Extending history backwards is not possible beyond the 2024 Jul–Sep fragment.

**Not realistic.** Anything about trains after midnight; departure delay at SF/SJ; sub-minute effects; per-car or consist analysis.

### 6.2 Delay propagation along the line

**Possible now (descriptive).** The data show strong propagation structure: within a trip, delay at consecutive stops correlates at 0.953 (n = 606 k pairs); between consecutive same-direction trains at the same stop, 0.56 (n = 627 k). You can already build: delay-accrual heatmaps by segment × hour; "knock-on" curves (given train A is x minutes late at stop S, the delay distribution of the next train at S); Marey diagrams per day (already generated for the 25 worst days, extendable to every day); onset localisation (first stop where a train crossed 5 minutes late, which the held-station heuristic in `incidents.py` approximates from flat GPS stretches).

**Possible with collection changes.** Causal attribution needs more than positions. Opposite-direction interactions on single track south of Tamien and at meet points, and equipment turnarounds at terminals (a late inbound becoming a late outbound), need a consist or block identifier that neither the feed's `VehicleRef` nor Caltrain's GTFS provides; **uncertain** whether `DatedVehicleJourneyRef` or VehiclePositions' `vehicle.id` would expose a physical unit. TripUpdates would add 511's own view of propagation (predicted downstream delays), which is useful to compare against what happened.

**Not realistic.** Distinguishing "held for a late connecting train" from "held for a mechanical issue" with any of 511's feeds; sub-minute headway analysis.

### 6.3 Anomaly detection

**Possible now (retrospective, nightly).** Day-level anomalies (already: incident tiers by number of trains ≥ 15 min late), train-level anomalies versus that train's own history (185 train numbers, each with hundreds of runs), station-level anomalies, and trajectory anomalies (flat GPS segments away from stations). Feed-health anomalies should be a first-class output, both so the site can grey out suspect days and so you learn about degradation while it is happening. Expect the two stale-position windows and the foreign-vehicle polls to dominate any naive detector until masked.

**Possible with collection changes.** Intra-day detection with a few minutes' latency: the SQLite file is live and the arrival query runs in seconds, so a 5-minute cron that scores the current day and publishes a small JSON is feasible without touching the collector. Adding per-poll status and vehicle counts would let it separate "service disruption" from "feed disruption" in real time, which the position data alone cannot.

**Not realistic.** Attributing anomalies to causes (weather, trespasser incidents, mechanical) without joining external sources such as ServiceAlerts or Caltrain's own advisories; ServiceAlerts is one more 511 feed and would be the natural first join.

### 6.4 Real-time lateness forecasting

**Not possible now.** The pipeline is a nightly batch, the site receives aggregates only, and no predictions or expected times are captured, so there is nothing to forecast from or to evaluate against.

**Possible with collection changes.** Two paths, and they stack:

1. Capture 511's predictions (`ExpectedArrivalTime` per onward call from VM, or TripUpdates' per-stop delays) every minute and store them keyed by (poll time, train, stop). Together with the observed arrivals you already compute, this yields a prediction-error dataset within days: how accurate is Caltrain's own ETA at 5, 10, 30 minutes out, by station and time of day. That is a genuinely new analysis and needs no modelling.
2. Build your own short-horizon forecast: current delay plus the historical delay-change profile for the remaining segments at that hour, possibly conditioned on the train ahead. The 606 k within-trip stop pairs are ample training data, and given the 0.95 stop-to-stop correlation a simple additive model will be hard to beat for horizons under 30 minutes. Serving it in near real time needs the intra-day job from 6.3 and a small live endpoint or frequently re-published JSON; the current Netlify static export can refresh every few minutes but was designed for nightly.

Either path needs a second API key (or a raised limit) because the existing key is saturated, and should include storing the raw payload so fields can be added later without re-collecting.

**Not realistic.** Predicting delays before they begin (incident onset); forecasts for after-midnight trains; forecasts that beat Caltrain's own ETA at horizons under a few minutes, since they see the train's AVL directly and you see it a minute late and sometimes stale.

## 7. Open questions and things I could not verify

- Whether Caltrain's VehicleMonitoring actually populates `ExpectedArrivalTime`, `Occupancy`, `VehicleAtStop`, `DirectionRef`, `DatedVehicleJourneyRef`. One raw payload would settle it; I did not fetch one because the key has no spare requests.
- The root cause of the stale-position windows (2026-07-28 to 08-16, 2026-10-10 to 10-17) and whether more exist at smaller scale; a per-day feed-health score over the whole table would show it.
- The whereabouts of the Nov 2024 to Mar 2025 data referenced in `docs/static/`.
- Whether `RecordedAtTime` is per-vehicle or per-response in 511's implementation. The identical seconds across a train's rows per day suggest per-vehicle but set by the aggregator at a fixed phase; it is not a GPS fix time.
- Whether the September 2026 archive is available yet from 511 and whether the earlier 93.8% agreement figure for `stop_observations` holds in other months.
- Why 2026-01-31 and 02-01 have no rows and what happened on 2025-12-20 and 2026-05-27.
