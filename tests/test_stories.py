import json
import math
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from src.pipeline.network_stops import COLUMNS as EVENT_COLUMNS
from src.pipeline.stories import REQUIRED_FIGURES, build_stories, example_day, validate_stories

NOW = datetime(2026, 11, 26, 2, 0, 41)
DAY = "2026-03-04"        # a Wednesday
SATURDAY = "2026-03-07"
SUNDAY = "2026-03-08"

# A straight north-south line: 0.01 degrees of latitude is ~1.11 km (as in test_incidents).
SHAPE = [(37.0 + i * 0.01, -122.0, i * 1111.95) for i in range(11)]
META = [{"name": f"S{i}", "lat": 37.0 + i * 0.01, "lon": -122.0, "order": i + 1} for i in range(0, 11, 2)]


def arrival(day, trip, rank, delay, hhmm, route="Local Weekday", direction="NB", station="22nd Street", n_stops=13):
    """One clean arrival; stop ids end in 1 northbound, 2 southbound, like Caltrain's."""
    return {"date": day, "trip_id": trip, "stop_id": f"{station}|{1 if direction == 'NB' else 2}",
            "station": station, "sched_ts": datetime.fromisoformat(f"{day} {hhmm}:00"),
            "delay_signed": float(delay), "stop_rank": rank, "n_stops": n_stops, "route_id": route}


def make_clean(con, rows):
    """A ``clean`` table with the Task 2 columns, derived from ``rows`` the way the view derives them."""
    df = pd.DataFrame(rows, columns=["date", "trip_id", "stop_id", "station", "sched_ts", "delay_signed",
                                     "stop_rank", "n_stops", "route_id"])
    df["date"] = pd.to_datetime(df["date"])
    df["sched_ts"] = pd.to_datetime(df["sched_ts"])
    con.register("clean_rows", df)
    con.execute("""
        CREATE OR REPLACE TABLE clean AS
        SELECT CAST(date AS DATE) AS date, trip_id, stop_id, station || ' Caltrain Station' AS stop_name, station,
               sched_ts, delay_signed, delay_signed > 4 AS late4, delay_signed > 15 AS late15,
               CAST(stop_rank AS INTEGER) AS stop_rank, CAST(n_stops AS INTEGER) AS n_stops, route_id,
               CASE WHEN route_id LIKE 'Local%' THEN 'Local' ELSE route_id END AS service,
               CASE WHEN stop_id LIKE '%1' THEN 'NB' ELSE 'SB' END AS direction,
               isodow(date) AS dow, isodow(date) <= 5 AS weekday, hour(sched_ts) AS sched_hour, false AS bad_feed
        FROM clean_rows
    """)
    con.unregister("clean_rows")


def add_scheduled(con, day, trip, route, hhmm):
    con.execute("INSERT INTO scheduled_trips VALUES (?, ?, ?, '0', ?, 13)",
                [day, trip, route, datetime.fromisoformat(f"{day} {hhmm}:00")])


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE scheduled_trips (date DATE, trip_id VARCHAR, route_id VARCHAR, "
              "direction_id VARCHAR, dep_ts TIMESTAMP, n_stops INTEGER)")
    return c


def no_events():
    return pd.DataFrame(columns=EVENT_COLUMNS)


def no_cancellations():
    return pd.DataFrame(columns=["date", "trip_id", "route_id", "dep_ts"])


def event(day, start, end, network_stop=True, trains_late=6, held_near="Palo Alto", hours=3.0, cancelled=2):
    t0, t1 = datetime.fromisoformat(f"{day} {start}:00"), datetime.fromisoformat(f"{day} {end}:00")
    return {"date": pd.Timestamp(day), "t_start": t0, "t_end": t1, "hold_minutes": (t1 - t0).seconds / 60,
            "max_trains": 5, "held_near": held_near, "trains_late": trains_late, "max_delay": 40.0, "dirs_hit": 2,
            "cancelled_after": cancelled, "hours_to_normal": hours, "is_network_stop": network_stop}


def events(*rows):
    return pd.DataFrame(list(rows), columns=EVENT_COLUMNS)


def weekdays(n, start="2026-01-05"):
    return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start, periods=n)]


def recovery_rows(recover=False, days=5):
    """Trips that reach 10 min late at rank 3, then either drift +0.5 per stop or get back to time."""
    rows = []
    for day in weekdays(days):
        rows.append(arrival(day, "301", 2, 5.0, "07:00"))
        rows.append(arrival(day, "301", 3, 10.0, "07:05"))
        for r in range(4, 14):
            delay = 0.0 if recover else 10.0 + 0.5 * (r - 3)
            rows.append(arrival(day, "301", r, delay, f"07:{5 * (r - 2):02d}"))
    return rows


def express_rows(local_sb_late=False):
    """Express NB 523 late at 22nd Street on 40 of 80 weekday runs; Local NB 101 always on time."""
    rows = []
    for i, day in enumerate(weekdays(80)):
        rows.append(arrival(day, "523", 2, 0.0, "17:40", "Express", station="Millbrae"))
        rows.append(arrival(day, "523", 3, 10.0 if i < 40 else 1.0, "17:55", "Express"))
        rows.append(arrival(day, "101", 2, 0.0, "08:10", station="Millbrae"))
        rows.append(arrival(day, "101", 3, 0.0, "08:25"))
        if local_sb_late:
            rows.append(arrival(day, "102", 2, 30.0, "09:10", direction="SB", station="Millbrae"))
            rows.append(arrival(day, "102", 3, 30.0, "09:25", direction="SB", station="San Jose"))
    return rows


def day_rows(day, n, late, trip="401", hour=12):
    """``n`` arrivals on ``day``, the first ``late`` of them 8 minutes late."""
    return [arrival(day, f"{trip[:2]}{i // 10}", 2 + i % 10, 8.0 if i < late else 0.0, f"{hour}:{i % 60:02d}")
            for i in range(n)]


def build(con, rows, ev=None, cancellations=None):
    make_clean(con, rows)
    return build_stories(con, no_events() if ev is None else ev,
                         no_cancellations() if cancellations is None else cancellations, NOW)


def figure(payload, name):
    return payload["figures"][name]["value"]


def test_recovery_holds_when_late_trains_stay_late(con):
    payload = build(con, recovery_rows())
    story = payload["stories"]["recovery"]
    assert [p["k"] for p in story["fan"]] == list(range(1, 11))
    assert story["fan"][4]["median"] == 2.5
    assert story["fan"][4]["n"] == 5
    assert story["fan"][9]["back_on_time_pct"] == 0.0
    assert figure(payload, "recovery_trips") == 5
    assert figure(payload, "recovery_median_change_5") == 2.5
    assert figure(payload, "recovery_worse_5_pct") == 100.0
    assert figure(payload, "recovery_better_5_pct") == 0.0
    assert story["holds"] is True


def test_recovery_fails_when_trains_recover(con):
    payload = build(con, recovery_rows(recover=True))
    assert figure(payload, "recovery_back_on_time_pct") == 100.0
    assert figure(payload, "recovery_median_change_5") == -10.0
    assert payload["stories"]["recovery"]["holds"] is False


def test_express_league_and_claim(con):
    for day in weekdays(80):
        add_scheduled(con, day, "523", "Express", "17:22")
    payload = build(con, express_rows())
    story = payload["stories"]["express"]
    top = story["league"][0]
    assert top["train"] == "523"
    assert (top["service"], top["direction"], top["departs"], top["last_stop"], top["runs"]) == \
        ("Express", "NB", "17:22", "22nd Street", 80)
    assert top["ci_low"] < top["late_pct"] < top["ci_high"]
    assert figure(payload, "worst_train") == "523"
    assert figure(payload, "worst_train_late_pct") == 50.0
    assert figure(payload, "worst_train_departs") == "17:22"
    assert figure(payload, "worst_train_last_stop") == "22nd Street"
    assert figure(payload, "worst_train_service") == "Express"
    assert figure(payload, "worst_train_direction") == "northbound"
    assert figure(payload, "nb_in_worst_25") == 2
    assert figure(payload, "express_nb_late_pct") == 25.0
    assert figure(payload, "local_nb_late_pct") == 0.0
    assert {(g["service"], g["direction"]) for g in story["groups"]} == {("Express", "NB"), ("Local", "NB")}
    assert story["holds"] is True

    payload = build(con, express_rows(local_sb_late=True))
    assert figure(payload, "local_sb_late_pct") == 100.0
    assert payload["stories"]["express"]["holds"] is False


def test_headway_follower_late_share(con):
    rows = []
    for day in weekdays(4):
        rows.append(arrival(day, "101", 3, 15.0, "08:00"))                # leader 10-20 late
        rows.append(arrival(day, "103", 3, 6.0, "08:08"))                 # follower 8 min behind: late
        rows.append(arrival(day, "105", 3, 15.0, "09:00"))
        rows.append(arrival(day, "107", 3, 6.0 if day < weekdays(4)[2] else 0.0, "09:25"))   # 25 min: half late
    payload = build(con, rows)
    cells = {(h["leader"], h["headway"]): h for h in payload["stories"]["express"]["headway"]}
    assert cells[("10–20", "≤ 10")]["n"] == 4
    assert figure(payload, "headway_close_late_pct") == 100.0
    assert figure(payload, "headway_far_late_pct") == 50.0


def test_afternoons_share(con):
    rows = [arrival(DAY, "201", 3, 20.0, "16:30"), arrival(DAY, "203", 3, 25.0, "17:00"),
            arrival(DAY, "205", 3, 30.0, "19:00"), arrival(DAY, "207", 3, 18.0, "08:00"),
            arrival(DAY, "209", 3, 1.0, "21:00")]
    ev = events(event(DAY, "15:55", "16:15", hours=3.5),
                event(DAY, "07:50", "08:10", network_stop=False))  # a feed freeze, not a stop
    payload = build(con, rows, ev)
    story = payload["stories"]["afternoons"]
    assert figure(payload, "stops_late15_share_pct") == 75.0
    assert figure(payload, "stops_count") == 1
    assert figure(payload, "stops_every_weeks") == round(1 / 1 / 7, 1)
    assert figure(payload, "stops_hours_to_normal_median") == 3.5
    assert figure(payload, "stops_longest_hold_minutes") == 20
    assert story["holds"] is True
    assert story["events"] == [{"date": DAY, "start": "15:55", "hold_minutes": 20, "held_near": "Palo Alto",
                                "trains_late": 6, "cancelled": 2, "hours_to_normal": 3.5}]

    payload = build(con, rows)
    assert figure(payload, "stops_count") == 0
    assert figure(payload, "stops_every_weeks") == 0
    assert figure(payload, "stops_late15_share_pct") == 0.0
    assert payload["stories"]["afternoons"]["holds"] is False
    assert payload["stories"]["afternoons"]["events"] == []


def test_events_newest_first_and_null_hours(con):
    ev = events(event("2026-03-02", "10:00", "10:30"), event(DAY, "15:55", "16:15", hours=None))
    payload = build(con, [arrival(DAY, "201", 3, 20.0, "16:30")], ev)
    out = payload["stories"]["afternoons"]["events"]
    assert [e["date"] for e in out] == [DAY, "2026-03-02"]
    assert out[0]["hours_to_normal"] is None
    assert figure(payload, "stops_hours_to_normal_median") == 3.0
    json.dumps(payload, allow_nan=False)


def test_cancellation_rates_on_days_with_three_or_more(con):
    for i in range(6):
        add_scheduled(con, DAY, f"5{i}1", "Express", "07:00")
    for i in range(10):
        add_scheduled(con, DAY, f"1{i}1", "Local Weekday", "07:00")
    add_scheduled(con, "2026-03-05", "501", "Express", "07:00")
    cancellations = pd.DataFrame({"date": pd.to_datetime([DAY] * 3 + ["2026-03-05"]),
                                  "trip_id": ["501", "511", "521", "501"],
                                  "route_id": ["Express"] * 4,
                                  "dep_ts": pd.to_datetime([f"{DAY} 07:00"] * 3 + ["2026-03-05 07:00"])})
    payload = build(con, [arrival(DAY, "101", 3, 1.0, "08:00")], cancellations=cancellations)
    assert figure(payload, "cancel_express_pct") == 50.0   # 3 of 6; 2026-03-05 has only one, so it's left out
    assert figure(payload, "cancel_local_pct") == 0.0


def test_worst_days_share(con):
    rows = []
    for i, day in enumerate(weekdays(12)):
        n_late = 3 if i < 10 else 1
        rows += [arrival(day, f"2{j}1", 3, 20.0, "16:00") for j in range(n_late)]
    payload = build(con, rows)
    assert figure(payload, "worst_days_10_share_pct") == round(30 / 32 * 100, 1)


def test_saturday_claim(con):
    rows = day_rows(DAY, 50, 3) + day_rows(SATURDAY, 50, 6)
    payload = build(con, rows)
    assert figure(payload, "weekday_late_pct") == 6.0
    assert figure(payload, "sat_late_pct") == 12.0
    assert figure(payload, "sun_late_pct") == 0.0
    assert figure(payload, "saturdays") == 1
    assert payload["stories"]["saturday"]["holds"] is True
    assert payload["stories"]["saturday"]["days"] == [
        {"date": DAY, "type": "weekday", "n": 50, "late_pct": 6.0},
        {"date": SATURDAY, "type": "Saturday", "n": 50, "late_pct": 12.0},
    ]

    payload = build(con, day_rows(DAY, 50, 3) + day_rows(SATURDAY, 50, 2) + day_rows(SUNDAY, 50, 10))
    assert figure(payload, "sun_late_pct") == 20.0
    assert figure(payload, "sat_bad_day_pct") == 0.0
    assert payload["stories"]["saturday"]["holds"] is False


def rich_rows():
    rows = express_rows() + recovery_rows()
    for day in weekdays(80):
        rows.append(arrival(day, "525", 3, 12.0, "18:05"))   # follows 523 at 22nd Street by 10 minutes
    rows += day_rows(SATURDAY, 30, 5) + day_rows(SUNDAY, 30, 1)
    rows += [arrival(DAY, "201", 3, 20.0, "16:30"), arrival(DAY, "203", 3, 25.0, "17:00")]
    return rows


def test_payload_is_strict_json_and_valid(con):
    for day in weekdays(80):
        add_scheduled(con, day, "523", "Express", "17:22")
    payload = build(con, rich_rows(), events(event(DAY, "15:55", "16:15")))
    json.dumps(payload, allow_nan=False)
    validate_stories(payload)

    assert payload["schema_version"] == 1
    assert payload["generated_at"] == "2026-11-26T02:00:41"
    assert payload["first_day"] == "2026-01-05"
    assert payload["data_through"] == "2026-04-24"
    assert payload["clean_days"] == 82
    assert set(payload["figures"]) == set(REQUIRED_FIGURES)
    for name, unit in REQUIRED_FIGURES.items():
        assert payload["figures"][name]["unit"] == unit
    cell = payload["stories"]["opener"]["heatmap"][0]
    assert set(cell) == {"dow", "hour", "n", "late_pct"}
    assert all(5 <= c["hour"] <= 23 for c in payload["stories"]["opener"]["heatmap"])
    assert len(payload["stories"]["express"]["league"]) <= 10


def test_empty_groups_are_zero_not_nan(con):
    payload = build(con, [arrival(SUNDAY, "101", 3, 1.0, "08:00")])
    json.dumps(payload, allow_nan=False)
    for name, unit in REQUIRED_FIGURES.items():
        value = figure(payload, name)
        assert isinstance(value, str) if unit in ("text", "time") else math.isfinite(value)
    for story in ("recovery", "express", "afternoons", "saturday"):
        assert payload["stories"][story]["holds"] is False
    with pytest.raises(ValueError, match="fan"):
        validate_stories(payload)


def test_validate_rejects_missing_and_nonfinite(con):
    for day in weekdays(80):
        add_scheduled(con, day, "523", "Express", "17:22")
    payload = build(con, rich_rows())
    validate_stories(payload)

    missing = json.loads(json.dumps(payload))
    del missing["figures"]["sat_late_pct"]
    with pytest.raises(ValueError, match="sat_late_pct"):
        validate_stories(missing)

    nan = json.loads(json.dumps(payload))
    nan["figures"]["on_time_pct"]["value"] = float("nan")
    with pytest.raises(ValueError, match="on_time_pct"):
        validate_stories(nan)

    unit = json.loads(json.dumps(payload))
    unit["figures"]["saturdays"]["unit"] = "pct"
    with pytest.raises(ValueError, match="saturdays"):
        validate_stories(unit)

    holds = json.loads(json.dumps(payload))
    holds["stories"]["saturday"]["holds"] = "yes"
    with pytest.raises(ValueError, match="saturday"):
        validate_stories(holds)

    nested = json.loads(json.dumps(payload))
    nested["stories"]["recovery"]["fan"][0]["p10"] = float("inf")
    with pytest.raises(ValueError):
        validate_stories(nested)


EXAMPLE = date(2025, 11, 6)


def example_gps(days):
    """Train 101 runs north from 15:00 for 60 minutes; 103 from 16:30. Nothing on other days."""
    rows = []
    for day in days:
        if day != EXAMPLE:
            continue
        for train, start in (("101", "15:00"), ("103", "16:30")):
            t0 = datetime.fromisoformat(f"{day} {start}:00")
            rows += [(train, 37.0 + i * 0.0015, -122.0, t0 + timedelta(minutes=i)) for i in range(60)]
    return pd.DataFrame(rows, columns=["trip_id", "vehicle_lat", "vehicle_lon", "timestamp"])


def test_example_day_window_and_absence(con):
    day = str(EXAMPLE)
    make_clean(con, [arrival(day, "101", 3, 20.0, "15:40"), arrival(day, "101", 4, 12.0, "15:50"),
                     arrival(day, "103", 3, 1.0, "17:00"),
                     arrival("2025-11-07", "101", 3, 1.0, "15:40")])
    ev = events(event(day, "15:55", "16:20"), event(day, "18:00", "18:30"))

    out = example_day(con, example_gps, META, SHAPE, ev)
    assert out["date"] == day
    assert out["window"] == {"start": "14:55", "end": "20:20"}
    assert out["freeze"] == {"start": "15:55", "end": "16:20"}
    assert [s["name"] for s in out["stations"]] == ["S0", "S2", "S4", "S6", "S8", "S10"]
    trains = {t["trip_id"]: t for t in out["trains"]}
    assert trains["101"]["max_delay_min"] == 20.0
    assert trains["103"]["max_delay_min"] == 1.0
    assert trains["101"]["direction"] == "NB"
    assert len(trains["101"]["points"]) == 60          # late: every point kept
    assert len(trains["103"]["points"]) == 13          # on time: thinned
    assert trains["101"]["points"][0] == {"time": "15:00:00", "distance": 0.0}
    json.dumps(out, allow_nan=False)

    quiet = example_day(con, example_gps, META, SHAPE, no_events())
    assert quiet["window"] == {"start": "14:00", "end": "20:00"}
    assert quiet["freeze"] is None

    assert example_day(con, example_gps, META, SHAPE, ev, day=date(2025, 11, 7)) is None   # no pings
    make_clean(con, [arrival("2025-11-07", "101", 3, 1.0, "15:40")])
    assert example_day(con, example_gps, META, SHAPE, ev) is None                          # no clean arrivals


def test_example_day_window_is_clipped(con):
    day = str(EXAMPLE)
    make_clean(con, [arrival(day, "101", 3, 20.0, "15:40")])
    out = example_day(con, example_gps, META, SHAPE, events(event(day, "05:30", "05:50"), event(day, "21:00", "21:30")))
    assert out["window"] == {"start": "05:00", "end": "09:50"}
    assert out["freeze"] == {"start": "05:30", "end": "05:50"}
    assert out["trains"] == []                         # pings that day, none in the window
    late = example_day(con, example_gps, META, SHAPE, events(event(day, "21:00", "21:30")))
    assert late["window"] == {"start": "20:00", "end": "23:59"}
