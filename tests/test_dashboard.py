from datetime import datetime

import numpy as np
import pandas as pd
import pytest

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
    return df


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
    }


def test_daily_performance():
    daily = build_dashboard(arrivals(), NOW)["daily_performance.json"]
    assert [d["date"] for d in daily] == ["2026-03-02", "2026-03-03", "2026-03-07"]
    assert daily[0] == {"date": "2026-03-02", "total_trips": 3, "on_time_count": 1, "minor_count": 1,
                        "major_count": 1, "avg_delay_min": 8.67, "on_time_pct": 33.3, "minor_pct": 33.3,
                        "major_pct": 33.3}


def test_station_and_train_rankings():
    out = build_dashboard(arrivals(), NOW)
    stations = out["station_performance.json"]
    assert [(s["stop_id"], s["on_time_pct"]) for s in stations] == [(70011, 66.7), (70021, 33.3)]
    assert stations[0]["stop_lat"] is None  # legacy frames never carried coordinates
    trains = {t["trip_id"]: t for t in out["train_performance.json"]}
    assert trains[103] == {"trip_id": 103, "total_stops": 2, "on_time_count": 1, "avg_delay_min": 10.0,
                           "days_observed": 2, "on_time_pct": 50.0}


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
