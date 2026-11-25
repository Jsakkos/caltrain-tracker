from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from src.pipeline.arrivals import SCHEMA
from src.pipeline.clean import (
    MIN_LEAGUE_RUNS,
    last_stop_league,
    load_clean,
    register_views,
    wilson,
)

DAY = date(2026, 3, 4)  # a Wednesday


def arrival(**kw) -> dict:
    row = {
        "date": DAY, "trip_id": "101", "stop_id": "70021", "stop_name": "22nd Street Caltrain Station",
        "parent_station": "", "arrival_time": "08:05:00", "actual_arrival_time": datetime(2026, 3, 4, 8, 6),
        "delay_minutes": 1.0, "is_delayed": False, "delay_severity": "On Time", "commute_period": "Morning",
        "hour": 8, "ping_count": 3, "feed_version": "1", "sched_ts": datetime(2026, 3, 4, 8, 5),
        "delay_signed": 1.0, "dist_m": 40.0, "stop_rank": 2, "n_stops": 3,
        "route_id": "Local Weekday", "direction_id": "1",
    }
    return row | kw


def health(day: date = DAY, **kw) -> dict:
    midnight = datetime.combine(day, datetime.min.time())
    row = {"date": day, "pings": 5000, "polls": 1200, "vehicles_per_poll": 4.2, "pos_change_share": 0.6,
           "trains": 90, "first_ping": midnight, "last_ping": midnight + timedelta(hours=23)}
    return row | kw


def insert(con, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    frame = pd.DataFrame(rows)  # noqa: F841 - read by name from the SQL below
    columns = ", ".join(frame.columns)
    con.execute(f"INSERT INTO {table} ({columns}) SELECT {columns} FROM frame")


def make_store(rows: list[dict], health_rows: list[dict] | None = None, trips: list[dict] | None = None):
    con = duckdb.connect(":memory:")
    con.execute(SCHEMA)
    insert(con, "arrivals", rows)
    insert(con, "feed_health", health_rows if health_rows is not None else [health(d) for d in sorted({r["date"] for r in rows})])
    insert(con, "scheduled_trips", trips or [])
    register_views(con)
    return con


def trip_ids(con, view: str = "clean") -> set[str]:
    return {r[0] for r in con.execute(f"SELECT DISTINCT trip_id FROM {view}").fetchall()}


def test_clean_drops_origin_after_midnight_far_and_foreign():
    con = make_store([
        arrival(trip_id="101"),
        arrival(trip_id="102", stop_rank=1),
        arrival(trip_id="103", arrival_time="24:05:00"),
        arrival(trip_id="104", dist_m=1500.0),
        arrival(trip_id="9001"),
    ])
    assert trip_ids(con) == {"101"}


def test_stale_positions_mask_the_day():
    con = make_store([arrival()], [health(pos_change_share=0.40)])
    assert con.execute("SELECT count(*) FROM clean").fetchone()[0] == 0
    assert con.execute("SELECT bad_feed FROM clean_all_days").fetchone() == (True,)


def test_far_arrivals_mask_the_day():
    rows = [arrival(trip_id=str(100 + i), dist_m=1200.0 if i % 2 else 40.0) for i in range(10)]
    con = make_store(rows)
    assert con.execute("SELECT near1k FROM feed_days").fetchone()[0] == pytest.approx(0.5)
    assert con.execute("SELECT count(*) FROM clean").fetchone()[0] == 0
    assert con.execute("SELECT DISTINCT bad_feed FROM clean_all_days").fetchall() == [(True,)]


def test_healthy_day_is_not_masked_and_origin_rows_do_not_count_toward_near1k():
    rows = [arrival(trip_id=str(100 + i)) for i in range(5)] + [arrival(trip_id="200", stop_rank=1, dist_m=5000.0)]
    con = make_store(rows)
    day = con.execute("SELECT near1k, arrivals, bad_feed, weekday, dow FROM feed_days").fetchone()
    assert day == (1.0, 5, False, True, 3)
    assert con.execute("SELECT count(*) FROM clean").fetchone()[0] == 5


def test_clean_view_adds_the_shared_columns():
    con = make_store([
        arrival(trip_id="101", stop_id="70021", route_id="Local Weekday", delay_signed=5.0,
                stop_name="22nd Street Caltrain Station"),
        arrival(trip_id="103", stop_id="70022", route_id="Express", delay_signed=16.0,
                stop_name="Millbrae Caltrain Station Southbound"),
    ])
    df = con.execute("SELECT * FROM clean ORDER BY trip_id").df()
    assert list(df.direction) == ["NB", "SB"]
    assert list(df.service) == ["Local", "Express"]
    assert list(df.station) == ["22nd Street", "Millbrae"]
    assert list(df.late4) == [True, True] and list(df.late15) == [False, True]
    assert set(df.sched_hour) == {8} and set(df.dow) == {3} and set(df.weekday) == {True}
    assert "dep_ts" not in df.columns


def test_early_is_on_time():
    con = make_store([arrival(delay_signed=-3.0), arrival(trip_id="102", delay_signed=4.0)])
    assert con.execute("SELECT count(*) FROM clean WHERE late4").fetchone()[0] == 0


def test_load_clean_is_signed_and_scheduled():
    con = make_store(
        [arrival(delay_signed=-3.0, delay_minutes=0.0, delay_severity="On Time", is_delayed=False,
                 sched_ts=datetime(2026, 3, 4, 8, 55), actual_arrival_time=datetime(2026, 3, 4, 9, 20), hour=9,
                 commute_period="Other"),
         arrival(trip_id="102", delay_signed=16.0, delay_minutes=16.0),
         arrival(trip_id="103", delay_signed=5.0, delay_minutes=5.0)],
        trips=[{"date": DAY, "trip_id": "101", "route_id": "Local Weekday", "direction_id": "1",
                "dep_ts": datetime(2026, 3, 4, 8, 0), "n_stops": 3}],
    )
    df = load_clean(con).set_index("trip_id")
    first = df.loc["101"]
    assert first.delay_minutes == -3.0
    assert first.delay_severity == "On Time"
    assert not first.is_delayed
    assert first.hour == 8
    assert first.commute_period == "Morning"
    assert first.dep_ts == datetime(2026, 3, 4, 8, 0)
    assert df.loc["102"].delay_severity == "Major" and df.loc["102"].is_delayed
    assert df.loc["103"].delay_severity == "Minor"
    assert pd.isna(df.loc["103"].dep_ts)


@pytest.mark.parametrize("day, sched, period", [
    (date(2026, 3, 4), datetime(2026, 3, 4, 9, 0), "Morning"),
    (date(2026, 3, 4), datetime(2026, 3, 4, 9, 1), "Other"),
    (date(2026, 3, 4), datetime(2026, 3, 4, 15, 30), "Evening"),
    (date(2026, 3, 4), datetime(2026, 3, 4, 19, 31), "Other"),
    (date(2026, 3, 7), datetime(2026, 3, 7, 8, 0), "Weekend"),
])
def test_load_clean_commute_period_bounds(day, sched, period):
    con = make_store([arrival(date=day, sched_ts=sched)])
    assert load_clean(con).commute_period.iloc[0] == period


def test_load_clean_can_include_bad_feed_days():
    con = make_store([arrival()], [health(pos_change_share=0.40)])
    assert len(load_clean(con)) == 0
    everything = load_clean(con, include_bad_feed=True)
    assert len(everything) == 1 and bool(everything.bad_feed.iloc[0])


def weekdays(n: int) -> list[date]:
    out, day = [], date(2026, 1, 5)  # a Monday
    while len(out) < n:
        if day.isoweekday() <= 5:
            out.append(day)
        day += timedelta(days=1)
    return out


def league_store(runs: int = MIN_LEAGUE_RUNS, late_runs: int = 40, with_trips: bool = True):
    rows, trips = [], []
    for i, day in enumerate(weekdays(runs)):
        sched = datetime.combine(day, datetime.min.time()) + timedelta(hours=17, minutes=22)
        base = dict(date=day, trip_id="523", route_id="Express", sched_ts=sched)
        rows += [
            arrival(**base, stop_id="70011", stop_rank=1, stop_name="San Francisco Caltrain Station", delay_signed=0.0),
            # a mid-line stop that is always late: the league must look only at the last stop
            arrival(**base, stop_id="70031", stop_rank=2, stop_name="Bayshore Caltrain Station", delay_signed=10.0),
            arrival(**base, stop_id="70021", stop_rank=3, stop_name="22nd Street Caltrain Station",
                    delay_signed=10.0 if i < late_runs else 0.0),
        ]
        trips.append({"date": day, "trip_id": "523", "route_id": "Express", "direction_id": "1", "dep_ts": sched, "n_stops": 3})
    return make_store(rows, trips=trips if with_trips else None)


def test_last_stop_league():
    league = last_stop_league(load_clean(league_store()))
    assert len(league) == 1
    row = league.iloc[0]
    assert row.trip_id == "523"
    assert (row.service, row.direction) == ("Express", "NB")
    assert row.runs == 80
    assert row.late_pct == 50.0
    assert row.last_stop_name == "22nd Street"
    assert row.departs == "17:22"
    assert row.ci_low < 50 < row.ci_high
    assert row.ci_low == round(row.ci_low, 1) and row.ci_high == round(row.ci_high, 1)


def test_league_skips_thin_trains_and_sorts_worst_first():
    store = league_store()
    extra = []
    for i, day in enumerate(weekdays(80)):
        extra.append(arrival(date=day, trip_id="305", stop_rank=3, delay_signed=0.0, stop_id="70022",
                             route_id="Local Weekday"))
        if i < 10:  # too few runs to rank
            extra.append(arrival(date=day, trip_id="777", stop_rank=3, delay_signed=20.0))
    insert(store, "arrivals", extra)
    league = last_stop_league(load_clean(store))
    assert list(league.trip_id) == ["523", "305"]
    assert list(league.direction) == ["NB", "SB"]


def test_league_without_scheduled_trips_has_no_departure():
    league = last_stop_league(load_clean(league_store(with_trips=False)))
    assert league.departs.isna().all()
    assert league.iloc[0].late_pct == 50.0


def test_wilson_interval():
    low, high = wilson(0.5, 80)
    assert low < 0.5 < high
    assert low == pytest.approx(0.393, abs=1e-3) and high == pytest.approx(0.607, abs=1e-3)
    assert 0.0 <= wilson(0.0, 80)[0] and wilson(1.0, 80)[1] <= 1.0
    assert wilson(0.5, 0) == (0.0, 1.0)
