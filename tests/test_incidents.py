from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.pipeline.incidents import detect_incidents, station_distances
from src.utils.geo_utils import load_shape_points, project_to_route, project_to_route_many

GTFS = Path(__file__).resolve().parent.parent / "gtfs_data"


def test_vectorized_projection_matches_scalar_on_the_real_shape():
    shape = load_shape_points(str(GTFS))
    rng = np.random.default_rng(0)
    lats = rng.uniform(37.30, 37.78, 500)
    lons = rng.uniform(-122.41, -121.88, 500)
    expected = [project_to_route(a, o, shape) for a, o in zip(lats, lons)]
    np.testing.assert_allclose(project_to_route_many(lats, lons, shape), expected, atol=1.0)


# A straight north-south line: 0.01 degrees of latitude is ~1.11 km.
SHAPE = [(37.0 + i * 0.01, -122.0, i * 1111.95) for i in range(11)]
META = [{"name": f"S{i}", "lat": 37.0 + i * 0.01, "lon": -122.0, "order": i + 1} for i in range(0, 11, 2)]


def arrivals():
    rows = []
    for train, delay in (("101", 20.0), ("103", 16.0), ("105", 18.0), ("107", 0.0)):
        rows.append(("2026-03-04", train, "70011", "S0 Caltrain Station", delay))
    rows.append(("2026-03-05", "201", "70021", "S4 Northbound", 30.0))
    rows.append(("2026-03-05", "203", "70021", "S2 Northbound", 1.0))
    df = pd.DataFrame(rows, columns=["date", "trip_id", "stop_id", "stop_name", "delay_minutes"])
    df["date"] = pd.to_datetime(df["date"])
    df["delay_severity"] = np.select([df.delay_minutes > 15, df.delay_minutes > 4], ["Major", "Minor"], "On Time")
    return df


def gps(dates):
    """Every train runs south to north; 201 is held for 10 minutes at S2 (distance 2.22 km)."""
    rows = []
    for day in dates:
        trains = ["101", "103", "105", "107"] if day == date(2026, 3, 4) else ["201", "203"]
        for train in trains:
            t = datetime.combine(day, datetime.min.time()) + timedelta(hours=8)
            lats = [37.0 + i * 0.002 for i in range(12)]
            if train == "201":
                lats = lats[:6] + [37.02] * 10 + lats[6:]
            for i, lat in enumerate(lats):
                rows.append((train, lat, -122.0, t + timedelta(minutes=i)))
    return pd.DataFrame(rows, columns=["trip_id", "vehicle_lat", "vehicle_lon", "timestamp"])


def test_station_distances_are_sorted_km_along_the_route():
    stations = station_distances(META, SHAPE)
    assert [s["name"] for s in stations] == ["S0", "S2", "S4", "S6", "S8", "S10"]
    assert stations[1]["distance"] == pytest.approx(2.22, abs=0.01)


def test_system_and_single_train_incidents():
    incidents, trajectories = detect_incidents(arrivals(), gps, META, SHAPE)
    by_id = {i["id"]: i for i in incidents}
    assert set(by_id) == {"2026-03-04-system", "2026-03-05-train-201"}

    system = by_id["2026-03-04-system"]
    assert system["tier"] == 2
    assert system["summary"] == "System Disruption — 3 trains delayed"
    assert system["trains_affected"] == 3
    assert system["max_delay_min"] == 20.0
    assert system["avg_delay_min"] == 18.0
    assert system["on_time_pct"] == 25.0
    assert system["severity_score"] == 54
    assert system["root_cause_train"] == "101"
    assert system["root_cause_station"] == "S0"

    train = by_id["2026-03-05-train-201"]
    assert train["tier"] == 1
    assert train["summary"] == "Train 201 delayed 30 min at S4"
    assert train["root_cause_station"] == "S2"  # found from the flat stretch of its trajectory
    assert [i["id"] for i in incidents] == ["2026-03-04-system", "2026-03-05-train-201"]

    trajs = {t["trip_id"]: t for t in trajectories["2026-03-04-system"]["trajectories"]}
    assert trajs["101"]["is_anomalous"] and not trajs["101"]["is_cascading"]
    assert trajs["103"]["is_cascading"]
    assert not trajs["107"]["is_anomalous"]
    assert trajs["107"]["direction"] == "SB"
    assert len(trajs["107"]["points"]) == 4  # normal trains keep every 5th point + the last
    assert len(trajs["101"]["points"]) == 12
    assert trajs["101"]["points"][0] == {"time": "08:00:00", "distance": 0.0}
    assert trajectories["2026-03-04-system"]["stations"][0] == {"name": "S0", "distance": 0.0, "order": 1}


def test_no_major_delays_means_no_incidents():
    df = arrivals()
    df["delay_minutes"] = 0.0
    assert detect_incidents(df, gps, META, SHAPE) == ([], {})
