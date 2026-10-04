"""
Delay incidents and the train trajectories behind the website's Marey diagrams.

A port of the legacy detect_incidents: same tiers, scores, ids and JSON shape,
with route projection vectorized and GPS fetched for all incident days at once.
"""
from datetime import date
from typing import Callable

import pandas as pd

from src.utils.geo_utils import project_to_route_many

GpsLoader = Callable[[list[date]], pd.DataFrame]  # -> trip_id, vehicle_lat, vehicle_lon, timestamp


def station_distances(station_meta: list[dict], shape_points: list[tuple]) -> list[dict]:
    """Stations as km along the route, for the diagram's y axis."""
    max_route_dist = shape_points[-1][2]
    km = project_to_route_many([s["lat"] for s in station_meta], [s["lon"] for s in station_meta], shape_points)
    stations = [
        {"name": s["name"], "distance": round(d / 1000, 2), "order": s["order"]}
        for s, d in zip(station_meta, km)
        if d < max_route_dist * 0.98 or s["order"] <= 26
    ]
    return sorted(stations, key=lambda s: s["distance"])


def _clean_station(name) -> str:
    if not isinstance(name, str):
        return name
    return name.replace(" Caltrain Station", "").replace(" Northbound", "").replace(" Southbound", "")


def _held_station(points: list[dict], stations: list[dict], default: str) -> str:
    """Nearest station to the first stretch of 5 points spanning < 0.3 km, if within 0.5 km."""
    dists = [p["distance"] for p in points]
    for i in range(len(dists) - 4):
        window = dists[i:i + 5]
        if max(window) - min(window) < 0.3:
            center = sum(window) / len(window)
            nearest = min(stations, key=lambda s: abs(s["distance"] - center))
            return nearest["name"] if abs(nearest["distance"] - center) < 0.5 else default
    return default


def _trajectories(gps: pd.DataFrame, shape_points, delayed: set[str], root_cause: str,
                  date_major: pd.DataFrame) -> list[dict]:
    gps = gps.assign(distance=(project_to_route_many(gps["vehicle_lat"], gps["vehicle_lon"], shape_points)
                               / 1000).round(2))
    out = []
    for trip_id, trip_gps in gps.groupby("trip_id"):
        trip_gps = trip_gps.sort_values("timestamp")
        if len(trip_gps) < 3:
            continue
        points = [{"time": t.strftime("%H:%M:%S"), "distance": float(d)}
                  for t, d in zip(trip_gps["timestamp"], trip_gps["distance"])]
        anomalous = trip_id in delayed
        delays = date_major.loc[date_major["trip_id"] == trip_id, "delay_minutes"]
        if not anomalous and len(points) > 10:
            points = points[::5] + [points[-1]]  # keep the JSON small for normal trains
        out.append({
            "trip_id": trip_id,
            "direction": "SB" if points[-1]["distance"] > points[0]["distance"] else "NB",
            "points": points,
            "is_anomalous": anomalous,
            "is_cascading": anomalous and trip_id != root_cause,
            "max_delay_min": round(float(delays.max()) if not delays.empty else 0.0, 1),
        })
    return out


def detect_incidents(arrivals: pd.DataFrame, gps_for_dates: GpsLoader, station_meta: list[dict],
                     shape_points: list[tuple], top_n: int = 25) -> tuple[list[dict], dict[str, dict]]:
    df = arrivals.assign(date=pd.to_datetime(arrivals["date"]).dt.date, trip_id=arrivals["trip_id"].astype(str))
    major = df[df["delay_minutes"] >= 15]
    if major.empty:
        return [], {}

    days = major.groupby("date").agg(
        trains_affected=("trip_id", "nunique"), max_delay=("delay_minutes", "max"),
        avg_delay=("delay_minutes", "mean"),
    ).reset_index()
    daily = df.groupby("date").agg(total=("trip_id", "count"),
                                   on_time=("delay_severity", lambda s: (s == "On Time").sum())).reset_index()
    daily["on_time_pct"] = (daily["on_time"] / daily["total"] * 100).round(1)
    days = days.merge(daily[["date", "on_time_pct", "total"]], on="date", how="left")
    days["tier"] = days["trains_affected"].apply(lambda n: 2 if n >= 3 else 1)
    days["severity_score"] = (days["avg_delay"] * days["trains_affected"]).round(0).astype(int)
    days = days.sort_values("severity_score", ascending=False).head(top_n)

    stations = station_distances(station_meta, shape_points)
    gps_all = gps_for_dates(days["date"].tolist())
    gps_all = gps_all.assign(timestamp=pd.to_datetime(gps_all["timestamp"], errors="coerce"),
                             trip_id=gps_all["trip_id"].astype(str)).dropna(subset=["timestamp"])
    gps_by_day = dict(tuple(gps_all.groupby(gps_all["timestamp"].dt.date)))

    incidents, trajectories = [], {}
    for _, row in days.iterrows():
        day, day_str = row["date"], str(row["date"])
        gps = gps_by_day.get(day)
        if gps is None or gps.empty:
            continue
        date_major = major[major["date"] == day]
        root = date_major.sort_values("delay_minutes", ascending=False).iloc[0]
        root_train, root_station = str(root["trip_id"]), _clean_station(root.get("stop_name", "Unknown"))

        if row["tier"] == 2:
            inc_id = f"{day_str}-system"
            summary = f"System Disruption — {row['trains_affected']} trains delayed"
        else:
            inc_id = f"{day_str}-train-{root_train}"
            summary = f"Train {root_train} delayed {int(row['max_delay'])} min at {root_station}"

        trajs = _trajectories(gps, shape_points, set(date_major["trip_id"]), root_train, date_major)
        if not trajs:
            continue
        held = root_station
        for traj in trajs:
            if traj["trip_id"] == root_train and len(traj["points"]) >= 5:
                held = _held_station(traj["points"], stations, root_station)

        incidents.append({
            "id": inc_id,
            "date": day_str,
            "tier": int(row["tier"]),
            "summary": summary,
            "trains_affected": int(row["trains_affected"]),
            "max_delay_min": round(float(row["max_delay"]), 1),
            "avg_delay_min": round(float(row["avg_delay"]), 1),
            "on_time_pct": float(row["on_time_pct"]),
            "severity_score": int(row["severity_score"]),
            "root_cause_train": root_train,
            "root_cause_station": held,
        })
        trajectories[inc_id] = {"incident_id": inc_id, "stations": stations, "trajectories": trajs}

    incidents.sort(key=lambda i: i["severity_score"], reverse=True)
    return incidents, trajectories
