import json
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from src.pipeline.clean import last_stop_league
from src.pipeline.dashboard import (
    DASHBOARD_FILES, build_dashboard, commute_shares, daily_severity_shares, write_dashboard, write_json_atomic,
)


def arrivals():
    rows = [
        # date, trip, stop, name, delay, severity, period, hour, pings
        ("2026-03-02", "101", "70011", "San Francisco", 0.0, "On Time", "Morning", 8, 3),
        ("2026-03-02", "101", "70021", "22nd Street", 6.0, "Minor", "Morning", 8, 1),
        ("2026-03-02", "103", "70011", "San Francisco", 20.0, "Major", "Evening", 17, 2),
        ("2026-03-03", "101", "70011", "San Francisco", 2.0, "On Time", "Morning", 8, 4),
        ("2026-03-03", "103", "70021", "22nd Street", 0.0, "On Time", "Evening", 18, 1),
        ("2026-03-07", "101", "70021", "22nd Street", 10.0, "Minor", "Weekend", 9, 2),
    ]
    df = pd.DataFrame(rows, columns=["date", "trip_id", "stop_id", "stop_name", "delay_minutes",
                                     "delay_severity", "commute_period", "hour", "ping_count"])
    df["date"] = pd.to_datetime(df["date"])
    df["is_delayed"] = df["delay_minutes"] > 4
    df["late4"] = df["is_delayed"]
    df["weekday"] = df["date"].dt.dayofweek < 5
    df["stop_rank"] = 2
    df["station"] = df["stop_name"]
    df["service"] = "Local"
    df["direction"] = "NB"
    df["bad_feed"] = False
    return df


def league_frame(runs_523=80, runs_305=10):
    """Weekday last-stop runs: 523 is late on half of them, 305 has too few runs for the league."""
    out = pd.DataFrame({
        "date": [pd.Timestamp("2026-01-05") + pd.Timedelta(days=i) for trip, n in (("523", runs_523), ("305", runs_305))
                 for i in range(n)],
        "trip_id": [trip for trip, n in (("523", runs_523), ("305", runs_305)) for i in range(n)],
        "late": [i % 2 == 0 for trip, n in (("523", runs_523), ("305", runs_305)) for i in range(n)],
    })
    out["stop_id"] = "70021"
    out["stop_name"] = "22nd Street"
    out["delay_minutes"] = out["late"].map({True: 6.0, False: 0.0})
    out["delay_severity"] = out["late"].map({True: "Minor", False: "On Time"})
    out["commute_period"] = "Morning"
    out["hour"] = 8
    out["ping_count"] = 1
    out["is_delayed"] = out["late"]
    out["late4"] = out["late"]
    out["weekday"] = True
    out["stop_rank"] = 3
    out["station"] = "22nd Street"
    out["service"] = "Local"
    out["direction"] = "NB"
    out["bad_feed"] = False
    return out.drop(columns="late")


NOW = datetime(2026, 3, 8, 0, 5)


def test_produces_every_website_file():
    assert set(build_dashboard(arrivals(), NOW)) == set(DASHBOARD_FILES)


def test_write_dashboard_uses_the_plot_names_the_website_links(tmp_path):
    written = write_dashboard(tmp_path, arrivals(), NOW)
    # MyWebsite src/lib/api.ts links plots/daily_stats.html and plots/commute_delay.html
    assert set(written) == set(DASHBOARD_FILES) | {"daily_stats.html", "commute_delay.html"}
    assert sorted(p.name for p in (tmp_path / "plots").iterdir()) == ["commute_delay.html", "daily_stats.html"]


def test_stats():
    stats = build_dashboard(arrivals(), NOW)["stats.json"]
    assert stats == {
        "on_time_percentage": 50.0,
        "minor_delay_percentage": 33.33,
        "major_delay_percentage": 16.67,
        "total_arrivals": 6,
        "avg_delay_minutes": 6.33,
        "median_delay_minutes": 4.0,
        "last_updated": "2026-03-08T00:05:00",
        "date_range": {"start": "2026-03-02", "end": "2026-03-07"},
        "days_tracked": 3,  # days with arrivals, not the calendar span
        "rolling_7d_on_time": 50.0,
        "rolling_30d_on_time": 50.0,
        "clean": {
            "on_time_percentage": 50.0, "minor_delay_percentage": 33.33, "major_delay_percentage": 16.67,
            "total_arrivals": 6, "avg_delay_minutes": 6.33, "median_delay_minutes": 4.0, "days_tracked": 3,
            "bad_feed_days": 0, "rolling_7d_on_time": 50.0, "rolling_30d_on_time": 50.0,
        },
    }


def test_stats_clean_block_differs_from_legacy():
    clean = arrivals().iloc[[0, 3, 4]]  # On Time, On Time, On Time
    stats = build_dashboard(clean, NOW, legacy=arrivals())["stats.json"]
    assert stats["on_time_percentage"] == 50.0 and stats["total_arrivals"] == 6  # legacy keys use legacy
    assert stats["clean"] == {
        "on_time_percentage": 100.0, "minor_delay_percentage": 0.0, "major_delay_percentage": 0.0,
        "total_arrivals": 3, "avg_delay_minutes": 0.67, "median_delay_minutes": 0.0,
        "days_tracked": 2, "bad_feed_days": 0, "rolling_7d_on_time": 100.0, "rolling_30d_on_time": 100.0,
    }


def test_daily_rows_flag_bad_feed_days():
    clean = arrivals()
    daily = clean.copy()
    daily["bad_feed"] = daily["date"] == pd.Timestamp("2026-03-03")
    out = build_dashboard(clean[clean["date"] != pd.Timestamp("2026-03-03")], NOW, daily=daily)
    rows = {r["date"]: r for r in out["daily_performance.json"]}
    assert [rows[d]["bad_feed"] for d in sorted(rows)] == [False, True, False]
    assert rows["2026-03-03"]["total_trips"] == 2  # unmasked figures
    clean_stats = out["stats.json"]["clean"]
    assert clean_stats["bad_feed_days"] == 1
    assert clean_stats["total_arrivals"] == 4 and clean_stats["days_tracked"] == 2


def test_train_last_stop_columns():
    trains = {t["trip_id"]: t for t in build_dashboard(league_frame(), NOW)["train_performance.json"]}
    assert trains[523]["last_stop_late_pct"] == 50.0
    assert trains[523]["last_stop_runs"] == 80
    assert trains[523]["last_stop_name"] == "22nd Street"
    assert trains[305]["last_stop_late_pct"] is None
    assert trains[305]["last_stop_runs"] == 10  # counted, but too few for a late share


def test_train_without_weekday_runs_has_zero_last_stop_runs():
    df = league_frame()
    df.loc[df["trip_id"] == "305", "weekday"] = False
    trains = {t["trip_id"]: t for t in build_dashboard(df, NOW)["train_performance.json"]}
    assert trains[305]["last_stop_runs"] == 0
    assert trains[305]["last_stop_name"] is None


def test_train_whose_service_changes_has_one_league_row():
    df = league_frame()
    df.loc[(df["trip_id"] == "523") & (df["date"] >= pd.Timestamp("2026-02-05")), "service"] = "Limited"
    out = build_dashboard(df, NOW)["train_performance.json"]
    row = [t for t in out if t["trip_id"] == 523][0]
    assert row["last_stop_runs"] == 80 and row["last_stop_late_pct"] == 50.0
    league = last_stop_league(df)
    assert list(league["trip_id"]).count("523") == 1
    assert league.set_index("trip_id").loc["523", "runs"] == row["last_stop_runs"]


def test_outputs_are_strict_json():
    df = league_frame()
    df.loc[df["trip_id"] == "305", ["weekday", "commute_period"]] = [False, "Weekend"]
    clean = df[df["commute_period"] != "Weekend"]  # no clean Weekend rows; 305 has no clean rows at all
    legacy = df  # ... but it is still in the legacy frame
    out = build_dashboard(clean, NOW, legacy=legacy, daily=df)
    for payload in out.values():
        json.dumps(payload, allow_nan=False)
    assert "Weekend" not in out["commute_analysis.json"]
    assert 305 not in [t["trip_id"] for t in out["train_performance.json"]]
    # an empty clean frame (the build skips it, but the function must still emit strict JSON)
    for payload in build_dashboard(df.iloc[:0], NOW, legacy=legacy, daily=df).values():
        json.dumps(payload, allow_nan=False)


def test_daily_performance():
    daily = build_dashboard(arrivals(), NOW)["daily_performance.json"]
    assert [d["date"] for d in daily] == ["2026-03-02", "2026-03-03", "2026-03-07"]
    assert daily[0] == {"date": "2026-03-02", "total_trips": 3, "on_time_count": 1, "minor_count": 1,
                        "major_count": 1, "avg_delay_min": 8.67, "on_time_pct": 33.3, "minor_pct": 33.3,
                        "major_pct": 33.3, "bad_feed": False}


def test_station_and_train_rankings():
    out = build_dashboard(arrivals(), NOW)
    stations = out["station_performance.json"]
    assert [(s["stop_id"], s["on_time_pct"]) for s in stations] == [(70011, 66.7), (70021, 33.3)]
    assert stations[0]["stop_lat"] is None  # legacy frames never carried coordinates
    trains = {t["trip_id"]: t for t in out["train_performance.json"]}
    assert trains[103] == {"trip_id": 103, "total_stops": 2, "on_time_count": 1, "avg_delay_min": 10.0,
                           "days_observed": 2, "on_time_pct": 50.0, "last_stop_runs": 2,
                           "last_stop_late_pct": None, "last_stop_name": None}


def test_commute_heatmap_week_month():
    out = build_dashboard(arrivals(), NOW)
    assert out["commute_analysis.json"]["Morning"]["on_time_pct"] == 66.7
    assert set(out["commute_analysis.json"]) == {"Morning", "Evening", "Weekend"}
    cell = [c for c in out["hourly_heatmap.json"] if c["day_name"] == "Saturday"][0]
    assert (cell["day_of_week"], cell["hour"], cell["total"]) == (5, 9, 1)
    assert [w["week_start"] for w in out["weekly_summary.json"]] == ["2026-03-02"]
    assert out["monthly_summary.json"][0]["month"] == "2026-03"


def legacy_per_ping(df):
    """The legacy plots ran on one row per matched ping."""
    return df.loc[np.repeat(df.index, df["ping_count"])].reset_index(drop=True)


def test_daily_shares_are_weighted_like_the_legacy_per_ping_frame():
    df = arrivals()
    expected = legacy_per_ping(df).groupby("date")["delay_severity"].value_counts(normalize=True).unstack() * 100
    got = daily_severity_shares(df)
    pd.testing.assert_frame_equal(got[expected.columns], expected.fillna(0), check_names=False)


def test_commute_shares_are_weighted_like_the_legacy_per_ping_frame():
    df = arrivals()
    legacy = legacy_per_ping(df)
    legacy = legacy[legacy.commute_period.isin(["Morning", "Evening"])]
    expected = legacy.groupby(["commute_period", "delay_severity"]).size() / legacy.groupby("commute_period").size() * 100
    got = commute_shares(df).set_index(["commute_period", "delay_severity"])["percentage"]
    pd.testing.assert_series_equal(got.sort_index(), expected.sort_index(), check_names=False)


def test_write_json_atomic_replaces_file(tmp_path):
    path = tmp_path / "x.json"
    path.write_text("old")
    write_json_atomic(path, {"a": 1}, indent=2)
    assert path.read_text() == '{\n  "a": 1\n}'
    assert list(tmp_path.iterdir()) == [path]
