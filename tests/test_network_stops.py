from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pytest

from src.pipeline.network_stops import caltrain_platforms, detect_network_stops, never_ran

DAY = "2026-03-04"
PALO_ALTO = (37.4439, -122.1650)
NEAR_PALO_ALTO = (37.4440, -122.1651)
SF = (37.7764, -122.3948)
NEAR_SF = (37.7771, -122.3948)  # ~80 m from the platform

STOPS = pd.DataFrame({
    "stop_id": ["70171", "70011"],
    "stop_name": ["Palo Alto Caltrain Station Northbound", "San Francisco Caltrain Station Northbound"],
    "parent_station": ["palo_alto", "san_francisco"],
    "stop_lat": [PALO_ALTO[0], SF[0]],
    "stop_lon": [PALO_ALTO[1], SF[1]],
})

# Real coordinates from the GTFS feeds: both Palo Alto platforms, Menlo Park, the two
# San Jose Diridon platforms, and the numeric-id "San Jose Shuttle" stop ~140 m from them.
PA_NB, PA_SB = (37.44344, -122.16466), (37.44340, -122.16470)
MENLO_PARK = (37.45486, -122.18230)
DIRIDON_NB, DIRIDON_SB = (37.32924, -121.90301), (37.32923, -121.90317)
SJ_SHUTTLE = (37.330196, -121.901985)
REAL_STOPS = pd.DataFrame([
    ("70171", "Palo Alto Caltrain Station Northbound", "palo_alto", *PA_NB),
    ("70172", "Palo Alto Caltrain Station Southbound", "palo_alto", *PA_SB),
    ("70161", "Menlo Park Caltrain Station Northbound", "menlo_park", *MENLO_PARK),
    ("70261", "San Jose Diridon Caltrain Station Northbound", "sj_diridon", *DIRIDON_NB),
    ("70262", "San Jose Diridon Caltrain Station Southbound", "sj_diridon", *DIRIDON_SB),
    ("777402", "San Jose Shuttle", "", *SJ_SHUTTLE),
    ("777403", "Tamien Caltrain Station Shuttle", "", 37.311638, -121.883403),
    ("SJD-01-STB", "Elevator: San Jose Diridon Caltrain Station Building", "sj_diridon", 37.329729, -121.902643),
], columns=["stop_id", "stop_name", "parent_station", "stop_lat", "stop_lon"])


def at(day, hhmm):
    return datetime.fromisoformat(f"{day} {hhmm}:00")


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE pings (id BIGINT, trip_id VARCHAR, stop_id VARCHAR, lat DOUBLE, lon DOUBLE, ts TIMESTAMP, date DATE)")
    c.execute("CREATE TABLE clean (date DATE, trip_id VARCHAR, sched_ts TIMESTAMP, delay_signed DOUBLE, "
              "late4 BOOLEAN, late15 BOOLEAN, direction VARCHAR)")
    c.execute("CREATE TABLE feed_days (date DATE, bad_feed BOOLEAN)")
    c.execute("CREATE TABLE scheduled_trips (date DATE, trip_id VARCHAR, route_id VARCHAR, "
              "direction_id VARCHAR, dep_ts TIMESTAMP, n_stops INTEGER)")
    c.execute(f"INSERT INTO feed_days VALUES (DATE '{DAY}', false)")
    return c


def add_pings(con, trip, where, start, minutes, day=DAY, moving=False, stop_id="x"):
    t0 = at(day, start)
    step = 1e-4 if moving else 0.0
    rows = [(trip, stop_id, where[0] + i * step, where[1], t0 + timedelta(minutes=i), day) for i in range(minutes)]
    nid = con.execute("SELECT coalesce(max(id), 0) FROM pings").fetchone()[0]
    con.executemany("INSERT INTO pings VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(nid + i + 1, *r) for i, r in enumerate(rows)])


def add_clean(con, trip, hhmm, delay, direction="NB", day=DAY):
    con.execute("INSERT INTO clean VALUES (?, ?, ?, ?, ?, ?, ?)",
                [day, trip, at(day, hhmm), delay, delay > 4, delay > 15, direction])


def freeze_four_trains(con, where=NEAR_PALO_ALTO):
    for n in range(4):
        add_pings(con, f"10{n}", where, "15:55", 20)


def test_frozen_trains_then_late_is_a_network_stop(con):
    freeze_four_trains(con)
    for n, (hhmm, direction) in enumerate([("16:30", "NB"), ("16:45", "SB"), ("17:00", "NB"),
                                           ("17:15", "SB"), ("17:30", "NB")]):
        add_clean(con, f"20{n}", hhmm, 25.0 + n, direction)
    add_clean(con, "300", "21:30", 1.0)  # back to normal later that evening

    out = detect_network_stops(con, STOPS)

    assert list(out.columns) == ["date", "t_start", "t_end", "hold_minutes", "max_trains", "held_near",
                                 "trains_late", "max_delay", "dirs_hit", "cancelled_after",
                                 "hours_to_normal", "is_network_stop"]
    assert len(out) == 1
    row = out.iloc[0]
    assert row.is_network_stop
    assert row.held_near == "Palo Alto"
    assert row.hold_minutes >= 15
    assert row.max_trains == 4
    assert row.trains_late == 5
    assert row.max_delay == 29.0
    assert row.dirs_hit == 2
    assert row.cancelled_after == 0
    assert row.t_start == pd.Timestamp(at(DAY, "15:55"))
    assert row.hours_to_normal == pytest.approx(5.1, abs=0.05)  # 21:00 hour is the first normal one


def test_freeze_without_late_trains_is_a_feed_freeze(con):
    freeze_four_trains(con)
    add_clean(con, "200", "16:30", 2.0)

    out = detect_network_stops(con, STOPS)

    assert len(out) == 1
    assert not out.iloc[0].is_network_stop
    assert out.iloc[0].trains_late == 0


def test_no_normal_hour_afterwards_gives_none_not_nan(con):
    freeze_four_trains(con)
    for n in range(5):
        add_clean(con, f"20{n}", "17:30", 30.0)

    out = detect_network_stops(con, STOPS)

    assert out.iloc[0].is_network_stop
    assert out.iloc[0].hours_to_normal is None


def test_trains_parked_at_a_terminal_are_ignored(con):
    freeze_four_trains(con, where=NEAR_SF)
    for n in range(5):
        add_clean(con, f"20{n}", "17:30", 30.0)

    out = detect_network_stops(con, STOPS)

    assert len(out) == 0


def test_short_runs_and_bad_feed_days_are_ignored(con):
    for n in range(4):
        add_pings(con, f"10{n}", NEAR_PALO_ALTO, "15:55", 9)  # one ping short of a run
    assert len(detect_network_stops(con, STOPS)) == 0

    con.execute("DELETE FROM pings")
    con.execute(f"UPDATE feed_days SET bad_feed = true WHERE date = DATE '{DAY}'")
    freeze_four_trains(con)
    assert len(detect_network_stops(con, STOPS)) == 0


def test_cancelled_trips_after_the_stop_are_counted(con):
    freeze_four_trains(con)
    for n in range(5):
        add_clean(con, f"20{n}", "17:30", 30.0)
    # a train pinging every minute keeps the feed alive across the stop
    add_pings(con, "900", (37.5, -122.2), "15:00", 300, moving=True)
    con.execute(f"INSERT INTO scheduled_trips VALUES (DATE '{DAY}', '777', 'Express', '0', TIMESTAMP '{DAY} 17:00:00', 20)")

    out = detect_network_stops(con, STOPS)

    assert out.iloc[0].cancelled_after == 1


def test_stops_frame_is_unregistered_afterwards(con):
    freeze_four_trains(con)
    detect_network_stops(con, STOPS)
    for name in ("stops", "ns_stops", "ns_pings", "ns_stops_run"):
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM {name}")


def test_never_ran_needs_a_live_feed(con):
    day2 = "2026-03-05"
    con.execute(f"INSERT INTO feed_days VALUES (DATE '{day2}', false)")
    add_pings(con, "900", (37.5, -122.2), "15:00", 240, moving=True)  # another train pings every minute
    for day in (DAY, day2):
        con.execute(f"INSERT INTO scheduled_trips VALUES (DATE '{day}', '777', 'Express', '0', TIMESTAMP '{day} 17:00:00', 20)")
    con.execute(f"INSERT INTO scheduled_trips VALUES (DATE '{DAY}', '900', 'Local Weekday', '0', TIMESTAMP '{DAY} 15:00:00', 20)")

    out = never_ran(con)

    assert list(out.columns) == ["date", "trip_id", "route_id", "dep_ts"]
    assert out.trip_id.tolist() == ["777"]
    assert out.iloc[0].date == pd.Timestamp(DAY)
    assert out.iloc[0].route_id == "Express"
    assert out.iloc[0].dep_ts == pd.Timestamp(at(DAY, "17:00"))


def test_never_ran_skips_bad_feed_days(con):
    con.execute(f"UPDATE feed_days SET bad_feed = true WHERE date = DATE '{DAY}'")
    add_pings(con, "900", (37.5, -122.2), "15:00", 240, moving=True)
    con.execute(f"INSERT INTO scheduled_trips VALUES (DATE '{DAY}', '777', 'Express', '0', TIMESTAMP '{DAY} 17:00:00', 20)")

    assert never_ran(con).empty


def test_never_ran_ignores_pings_without_a_stop_or_from_foreign_vehicles(con):
    add_pings(con, "900", (37.5, -122.2), "15:00", 240, moving=True)
    # the only pings for 777 have no stop; 'ABCDE' is a foreign vehicle id
    add_pings(con, "777", (37.5, -122.2), "16:00", 30, moving=True, stop_id="unknown")
    con.execute(f"INSERT INTO scheduled_trips VALUES (DATE '{DAY}', '777', 'Express', '0', TIMESTAMP '{DAY} 17:00:00', 20)")
    assert never_ran(con).trip_id.tolist() == ["777"]

    # foreign-vehicle pings do not keep a dead feed "alive"
    con.execute("DELETE FROM pings WHERE trip_id = '900'")
    add_pings(con, "ABCDE", (37.5, -122.2), "15:00", 240, moving=True)
    assert never_ran(con).empty


def test_empty_result_has_the_same_dtypes_as_a_full_one(con):
    empty = detect_network_stops(con, STOPS)
    assert empty.empty and list(empty.columns) == list(detect_network_stops_columns())

    freeze_four_trains(con)
    for n in range(5):
        add_clean(con, f"20{n}", "17:30", 30.0)
    full = detect_network_stops(con, STOPS)
    assert (empty.dtypes == full.dtypes).all(), (empty.dtypes, full.dtypes)


def detect_network_stops_columns():
    from src.pipeline.network_stops import COLUMNS
    return COLUMNS


def make_late(con, n=5):
    for i in range(n):
        add_clean(con, f"20{i}", "17:30", 30.0)


def hold(con, trip, where, start="15:55", minutes=20, split=False):
    """A train frozen for ``minutes``; ``split`` nudges it once halfway, making two stationary runs."""
    if not split:
        add_pings(con, trip, where, start, minutes)
        return
    half = minutes // 2
    add_pings(con, trip, where, start, half)
    t = (at(DAY, start) + timedelta(minutes=half)).strftime("%H:%M")
    add_pings(con, trip, (where[0] + 1e-5, where[1]), t, minutes - half)


def test_caltrain_platforms_keeps_only_numeric_caltrain_platforms():
    kept = caltrain_platforms(REAL_STOPS)
    assert kept["stop_id"].tolist() == ["70171", "70172", "70161", "70261", "70262"]


def test_held_near_ranks_stations_by_trains_not_stationary_runs(con):
    # Menlo Park: 2 trains, each split into 2 runs (4 runs). Palo Alto: 3 trains, 1 run each.
    for n in range(2):
        hold(con, f"10{n}", MENLO_PARK, minutes=20, split=True)
    for n in range(3):
        hold(con, f"11{n}", PA_NB)
    make_late(con)

    out = detect_network_stops(con, REAL_STOPS)

    assert len(out) == 1 and out.iloc[0].held_near == "Palo Alto"


def test_held_near_ties_on_trains_go_to_more_train_minutes(con):
    # 2 trains each; Palo Alto has more stationary runs, Menlo Park more held minutes.
    for n in range(2):
        hold(con, f"10{n}", PA_NB, minutes=20, split=True)
        hold(con, f"11{n}", MENLO_PARK, minutes=30)
    make_late(con)

    out = detect_network_stops(con, REAL_STOPS)

    assert len(out) == 1 and out.iloc[0].held_near == "Menlo Park"


def test_held_near_ties_on_trains_and_minutes_go_to_the_first_name(con):
    # Same trains and train-minutes at both; Palo Alto has more stationary runs.
    for n in range(2):
        hold(con, f"10{n}", PA_NB, minutes=20, split=True)
        hold(con, f"11{n}", MENLO_PARK, minutes=20)
    make_late(con)

    out = detect_network_stops(con, REAL_STOPS)

    assert len(out) == 1 and out.iloc[0].held_near == "Menlo Park"


def test_held_near_counts_both_platforms_of_a_station_together(con):
    # Palo Alto: 2 trains on each platform (4); Menlo Park: 3 trains on its one platform.
    for n in range(2):
        hold(con, f"10{n}", PA_NB)
        hold(con, f"11{n}", PA_SB)
    for n in range(3):
        hold(con, f"12{n}", MENLO_PARK)
    make_late(con)

    out = detect_network_stops(con, REAL_STOPS)

    assert len(out) == 1 and out.iloc[0].held_near == "Palo Alto"


def test_held_near_is_the_same_whatever_the_ping_order(con):
    for n in range(2):
        hold(con, f"10{n}", PA_NB, minutes=20, split=True)
        hold(con, f"11{n}", MENLO_PARK, minutes=20)
    make_late(con)
    first = detect_network_stops(con, REAL_STOPS)
    con.execute("CREATE TABLE shuffled AS SELECT * FROM pings ORDER BY random()")
    con.execute("DROP TABLE pings")
    con.execute("ALTER TABLE shuffled RENAME TO pings")
    for _ in range(5):
        pd.testing.assert_frame_equal(detect_network_stops(con, REAL_STOPS), first)


def test_a_shuttle_stop_does_not_hide_trains_parked_at_a_terminal(con):
    # Parked right on the shuttle stop, ~140 m from the Diridon platforms.
    for n in range(4):
        hold(con, f"10{n}", SJ_SHUTTLE)
    make_late(con)

    assert len(detect_network_stops(con, REAL_STOPS)) == 0


def test_a_shuttle_stop_is_never_named_and_its_trains_are_not_counted(con):
    for n in range(4):
        hold(con, f"10{n}", SJ_SHUTTLE)
        hold(con, f"11{n}", PA_NB)
    make_late(con)

    out = detect_network_stops(con, REAL_STOPS)

    assert len(out) == 1
    assert out.iloc[0].held_near == "Palo Alto"
    assert out.iloc[0].max_trains == 4


def test_given_cancellations_are_used_instead_of_recomputed(con):
    freeze_four_trains(con)
    make_late(con)
    given = pd.DataFrame({"date": [pd.Timestamp(DAY)] * 2, "trip_id": ["777", "778"],
                          "route_id": ["Express", "Express"],
                          "dep_ts": [pd.Timestamp(at(DAY, "17:00")), pd.Timestamp(at(DAY, "17:10"))]})

    out = detect_network_stops(con, STOPS, cancellations=given)

    assert out.iloc[0].cancelled_after == 2  # nothing in scheduled_trips: only the given frame can say 2
