from datetime import date, datetime

import pandas as pd
import pytest

from src.pipeline.arrivals import load_arrivals, open_store, update_arrivals
from tests.pipeline_fixtures import SF, TWENTY_SECOND, add_pings, make_db, make_feed

DAY = "2026-03-04"  # a Wednesday
LATER = date(2026, 9, 1)  # "today" far after the test data, so nothing is "recent"
AT_SF = (SF[2], SF[3])
NEAR_SF = (37.7740, -122.3943)  # ~270 m north of the platform
FAR_SF = (37.7700, -122.3943)
AT_22ND = (TWENTY_SECOND[2], TWENTY_SECOND[3])


@pytest.fixture
def env(tmp_path):
    feeds = tmp_path / "feeds"
    make_feed(feeds)
    db_path = tmp_path / "t.db"
    return {"feeds": feeds, "db": db_path, "conn": make_db(db_path),
            "store": open_store(str(tmp_path / "a.duckdb"))}


def run(env, today=LATER, **kw):
    return update_arrivals(env["store"], str(env["db"]), env["feeds"], today=today, **kw)


def ping(trip, stop, where, ts):
    return (trip, stop, where[0], where[1], ts)


def one(env, trip, stop, day=DAY):
    df = load_arrivals(env["store"])
    rows = df[(df.trip_id == trip) & (df.stop_id == stop) & (df.date == pd.Timestamp(day))]
    assert len(rows) == 1, rows
    return rows.iloc[0]


def test_closest_ping_is_the_arrival(env):
    add_pings(env["conn"],
              ping("101", SF[0], FAR_SF, f"{DAY} 07:58:00"),
              ping("101", SF[0], AT_SF, f"{DAY} 08:03:00"),
              ping("101", SF[0], NEAR_SF, f"{DAY} 08:06:00"))
    run(env)
    row = one(env, "101", SF[0])
    assert row.actual_arrival_time == datetime(2026, 3, 4, 8, 3)
    assert row.delay_minutes == pytest.approx(3.0)
    assert row.delay_severity == "On Time"
    assert row.ping_count == 3
    assert row.stop_name == "San Francisco Caltrain"


def test_distance_tie_goes_to_first_stored_row(env):
    # Legacy groupby().first() keeps SELECT * order, i.e. insertion order.
    add_pings(env["conn"],
              ping("101", SF[0], AT_SF, f"{DAY} 08:09:00"),
              ping("101", SF[0], AT_SF, f"{DAY} 08:07:00"))
    run(env)
    assert one(env, "101", SF[0]).actual_arrival_time == datetime(2026, 3, 4, 8, 9)


@pytest.mark.parametrize("actual, delay, severity, delayed", [
    ("08:09:00", 4.0, "On Time", False),
    ("08:09:30", 4.5, "Minor", True),
    ("08:20:00", 15.0, "Minor", True),
    ("08:20:30", 15.5, "Major", True),
    ("07:55:00", 0.0, "On Time", False),   # early: severity from -10, then clamped to 0
    ("17:00:00", 0.0, "On Time", False),   # 535 min: treated as bad data
])
def test_delay_and_severity(env, actual, delay, severity, delayed):
    add_pings(env["conn"], ping("101", TWENTY_SECOND[0], AT_22ND, f"{DAY} {actual}"))
    run(env)
    row = one(env, "101", TWENTY_SECOND[0])
    assert row.delay_minutes == pytest.approx(delay)
    assert row.delay_severity == severity
    assert bool(row.is_delayed) is delayed


def test_after_midnight_schedule_wraps_onto_the_same_date(env):
    # Legacy normalize_time: 24:10:00 -> 00:10:00 on the ping's own date.
    add_pings(env["conn"], ping("103", SF[0], AT_SF, f"{DAY} 00:12:00"))
    run(env)
    row = one(env, "103", SF[0])
    assert row.arrival_time == "00:10:00"
    assert row.delay_minutes == pytest.approx(2.0)


@pytest.mark.parametrize("day, actual, period", [
    (DAY, "09:00:00", "Morning"),
    (DAY, "06:00:00", "Morning"),
    (DAY, "15:30:00", "Evening"),
    (DAY, "12:00:00", "Other"),
    ("2026-03-07", "08:00:00", "Weekend"),  # Saturday
])
def test_commute_period(env, day, actual, period):
    add_pings(env["conn"], ping("101", SF[0], AT_SF, f"{day} {actual}"))
    run(env)
    row = one(env, "101", SF[0], day)
    assert row.commute_period == period
    assert row.hour == int(actual[:2])


def test_unknown_stop_and_unscheduled_pings_are_ignored(env):
    add_pings(env["conn"],
              ping("101", "unknown", AT_SF, f"{DAY} 08:00:00"),
              ping("999", SF[0], AT_SF, f"{DAY} 08:00:00"),
              ping("101", SF[0], AT_SF, f"{DAY} 08:01:00"))
    run(env)
    df = load_arrivals(env["store"])
    assert list(zip(df.trip_id, df.stop_id)) == [("101", SF[0])]


def test_processed_days_are_not_recomputed_unless_full(env):
    add_pings(env["conn"], ping("101", SF[0], NEAR_SF, f"{DAY} 08:03:00"))
    assert run(env) == [date(2026, 3, 4)]

    add_pings(env["conn"], ping("101", SF[0], AT_SF, f"{DAY} 08:06:00"))
    assert run(env) == []
    assert one(env, "101", SF[0]).actual_arrival_time == datetime(2026, 3, 4, 8, 3)

    assert run(env, full=True) == [date(2026, 3, 4)]
    assert one(env, "101", SF[0]).actual_arrival_time == datetime(2026, 3, 4, 8, 6)
    assert len(load_arrivals(env["store"])) == 1


def test_today_and_yesterday_are_always_recomputed(env):
    add_pings(env["conn"], ping("101", SF[0], NEAR_SF, f"{DAY} 08:03:00"))
    run(env, today=date(2026, 3, 5))
    add_pings(env["conn"], ping("101", SF[0], AT_SF, f"{DAY} 08:06:00"))
    assert run(env, today=date(2026, 3, 5)) == [date(2026, 3, 4)]
    assert one(env, "101", SF[0]).actual_arrival_time == datetime(2026, 3, 4, 8, 6)


def test_new_schedule_version_for_a_day_triggers_recompute(env):
    add_pings(env["conn"], ping("101", SF[0], AT_SF, f"{DAY} 08:03:00"))
    run(env)
    assert one(env, "101", SF[0]).feed_version == "1"

    # A monthly archive arrives for March and says 101 was due at 08:01.
    make_feed(env["feeds"], name="historic-2026-03", version="h1", start="20260301", end="20260331",
              calls=[("101", SF[0], "08:01:00")])
    assert run(env) == [date(2026, 3, 4)]
    row = one(env, "101", SF[0])
    assert row.feed_version == "h1"
    assert row.delay_minutes == pytest.approx(2.0)


def test_days_without_a_schedule_wait_for_one(env):
    add_pings(env["conn"], ping("101", SF[0], AT_SF, "2025-06-04 08:03:00"))
    assert run(env) == [date(2025, 6, 4)]
    assert load_arrivals(env["store"]).empty

    make_feed(env["feeds"], name="v0", version="0", start="20250101", end="20251231")
    assert run(env) == [date(2025, 6, 4)]
    assert len(load_arrivals(env["store"])) == 1


def test_batched_days_match_a_single_pass(env, tmp_path):
    for day in ("2026-03-04", "2026-03-05", "2026-03-06"):
        add_pings(env["conn"], ping("101", SF[0], AT_SF, f"{day} 08:03:00"),
                  ping("101", TWENTY_SECOND[0], AT_22ND, f"{day} 08:09:30"))
    run(env)
    single = load_arrivals(env["store"])

    batched_store = open_store(str(tmp_path / "b.duckdb"))
    days = update_arrivals(batched_store, str(env["db"]), env["feeds"], today=LATER, batch_days=1)
    assert len(days) == 3
    pd.testing.assert_frame_equal(load_arrivals(batched_store), single)
