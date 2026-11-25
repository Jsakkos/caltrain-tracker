"""
The data, quoted figures and claim checks behind the website's four "stories from the data".

Each story ports a cell of the analysis notebooks against the ``clean`` view
(src/pipeline/clean.py), so the page and the notebooks can't drift:

  * opener      hour x day-of-week late share              02 §1
  * recovery    "ten minutes late stays late"              03 §4
  * express     the least reliable trains, by service      02 §5-6, 03 §3
  * afternoons  line-wide stops and their ripple           05, 03 §5
  * saturday    Saturday is the worst day to ride          02 §2

``build_stories`` returns the ``stories.json`` payload; ``example_day`` returns the
``stories_example_day.json`` diagram data. Both are strict JSON: an empty group
gives 0, an empty list and ``holds: false``, never NaN. ``validate_stories`` then
rejects a payload whose charts are empty, so the build keeps the previous file.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, time, timedelta
from typing import Callable

import duckdb
import pandas as pd

from src.pipeline.clean import last_stop_league
from src.pipeline.incidents import project_trajectories, station_distances, thin_points
from src.pipeline.network_stops import CONFIRM_HOURS

SCHEMA_VERSION = 1
EXAMPLE_DAY = date(2025, 11, 6)
LEAGUE_ROWS = 10           # trains in the story's league table
WORST_TRAINS = 25          # "n of the 25 least reliable trains are northbound"
WORST_DAYS = 10            # "ten days hold x% of all 15-minute-late arrivals"
BAD_DAY_SHARE = 0.15       # a day with a larger late share is a bad day
CANCEL_DAY_MIN = 3         # never-run trips that make a cancellation day
SERVICES = ("Local", "Limited", "Express")
DIRECTIONS = ("NB", "SB")
LEADER_BINS = ("≤ 0", "0–4", "4–10", "10–20", "> 20")
HEADWAY_BINS = ("≤ 10", "11–20", "21–30", "31–40")
EXAMPLE_FROZEN_LATE_MIN = 15   # trains at least this late keep every point in the example day

REQUIRED_FIGURES: dict[str, str] = {
    "on_time_pct": "pct",
    "recovery_trips": "count",
    "recovery_median_change_5": "minutes",
    "recovery_worse_5_pct": "pct",
    "recovery_better_5_pct": "pct",
    "recovery_back_on_time_pct": "pct",
    "worst_train": "text",
    "worst_train_departs": "time",
    "worst_train_late_pct": "pct",
    "worst_train_last_stop": "text",
    "worst_train_service": "text",
    "worst_train_direction": "text",
    "nb_in_worst_25": "count",
    "express_nb_late_pct": "pct",
    "express_sb_late_pct": "pct",
    "local_nb_late_pct": "pct",
    "local_sb_late_pct": "pct",
    "headway_close_late_pct": "pct",
    "headway_far_late_pct": "pct",
    "stops_count": "count",
    "stops_every_weeks": "weeks",
    "stops_late15_share_pct": "pct",
    "stops_hours_to_normal_median": "hours",
    "stops_longest_hold_minutes": "minutes",
    "cancel_express_pct": "pct",
    "cancel_local_pct": "pct",
    "example_trains_late": "count",
    "worst_days_10_share_pct": "pct",
    "sat_late_pct": "pct",
    "weekday_late_pct": "pct",
    "sun_late_pct": "pct",
    "sat_bad_day_pct": "pct",
    "weekday_bad_day_pct": "pct",
    "saturdays": "count",
}

HEATMAP_SQL = """
SELECT dow, sched_hour AS hour, count(*) AS n, avg(late4::INT) AS late
FROM clean WHERE sched_hour BETWEEN 5 AND 23 GROUP BY 1, 2 ORDER BY 1, 2
"""

# Per service day: arrivals, late and late-15 counts (02 §2, 03 §5).
DAYS_SQL = """
SELECT date, dow, count(*) AS n, sum(late4::INT) AS late, sum(late15::INT) AS late15
FROM clean GROUP BY 1, 2 ORDER BY 1
"""

# 03 §4: weekday trips from the first stop where they are >= 10 minutes late.
_RECOVERY_CTE = """
WITH t AS (
    SELECT date, trip_id, stop_rank, delay_signed,
           min(CASE WHEN delay_signed >= 10 THEN stop_rank END) OVER (PARTITION BY date, trip_id) AS first10
    FROM clean WHERE weekday
),
base AS (SELECT date, trip_id, delay_signed AS d0, stop_rank AS r0 FROM t WHERE stop_rank = first10),
later AS (
    SELECT t.date, t.trip_id, t.stop_rank - base.r0 AS k, t.delay_signed - base.d0 AS dd, t.delay_signed AS d
    FROM t JOIN base USING (date, trip_id) WHERE t.stop_rank > base.r0
)
"""
RECOVERY_SQL = _RECOVERY_CTE + """
SELECT k, count(*) AS n, quantile_cont(dd, 0.1) AS p10, quantile_cont(dd, 0.25) AS p25, median(dd) AS median,
       quantile_cont(dd, 0.75) AS p75, quantile_cont(dd, 0.9) AS p90,
       avg((dd >= 2)::INT) AS worse, avg((dd <= -2)::INT) AS better, avg((d <= 4)::INT) AS back
FROM later WHERE k <= 10 GROUP BY k ORDER BY k
"""
RECOVERY_TRIPS_SQL = _RECOVERY_CTE + "SELECT count(*) FROM (SELECT DISTINCT date, trip_id FROM later)"

# Each train's last observed weekday stop, for clean.last_stop_league (02 §6).
LAST_STOPS_SQL = """
SELECT c.date, c.trip_id, c.stop_rank, c.weekday, c.late4, c.service, c.direction, c.station, t.dep_ts
FROM clean c LEFT JOIN scheduled_trips t ON t.date = c.date AND t.trip_id = c.trip_id
WHERE c.weekday
QUALIFY row_number() OVER (PARTITION BY c.date, c.trip_id ORDER BY c.stop_rank DESC) = 1
"""

# 02 §5, all hours: weekday per-stop late share by service and direction.
GROUPS_SQL = f"""
SELECT service, direction, count(*) AS n, avg(late4::INT) AS late
FROM clean WHERE weekday AND service IN {SERVICES} GROUP BY 1, 2
"""

# 03 §3: the follower's late share by the delay of the train ahead and the scheduled headway.
HEADWAY_SQL = """
WITH s AS (
    SELECT sched_ts, late4, lag(delay_signed) OVER w AS lead_delay, lag(sched_ts) OVER w AS lead_sched
    FROM clean WHERE weekday
    WINDOW w AS (PARTITION BY date, stop_id ORDER BY sched_ts)
)
SELECT CASE WHEN lead_delay <= 0 THEN '≤ 0' WHEN lead_delay <= 4 THEN '0–4' WHEN lead_delay <= 10 THEN '4–10'
            WHEN lead_delay <= 20 THEN '10–20' ELSE '> 20' END AS leader,
       CASE WHEN hw <= 10 THEN '≤ 10' WHEN hw <= 20 THEN '11–20' WHEN hw <= 30 THEN '21–30' ELSE '31–40' END AS headway,
       count(*) AS n, avg(late4::INT) AS late
FROM (SELECT *, epoch(sched_ts - lead_sched) / 60 AS hw FROM s)
WHERE lead_delay IS NOT NULL AND hw <= 40
GROUP BY 1, 2
"""

# Late-15 arrivals inside a network stop's window: t_start to t_end + CONFIRM_HOURS.
STOP_WINDOWS_SQL = f"""
SELECT count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM st_windows w
           WHERE CAST(w.date AS DATE) = c.date
             AND c.sched_ts BETWEEN w.t_start AND w.t_end + INTERVAL {CONFIRM_HOURS} HOUR)) AS inside,
       count(*) AS total
FROM clean c WHERE c.late15
"""

SCHEDULED_BY_SERVICE_SQL = """
SELECT CASE WHEN route_id LIKE 'Local%' THEN 'Local' ELSE route_id END AS service, count(*) AS n
FROM scheduled_trips WHERE date IN (SELECT CAST(date AS DATE) FROM st_cancel_days) GROUP BY 1
"""


def _num(x, dp: int = 1) -> float:
    """A rounded float, or 0.0 for a missing or non-finite value."""
    if x is None or pd.isna(x) or not math.isfinite(float(x)):
        return 0.0
    return round(float(x), dp)


def _pct(share) -> float:
    return _num(None if share is None or pd.isna(share) else float(share) * 100)


def _share(part, whole) -> float:
    return _pct(part / whole) if whole else 0.0


def _int(x) -> int:
    return 0 if x is None or pd.isna(x) else int(round(float(x)))


def _text(x) -> str:
    return x if isinstance(x, str) else ""


def _network_stops(events: pd.DataFrame | None) -> pd.DataFrame:
    """The ``is_network_stop`` rows, with Python dates and timestamps, earliest first."""
    if events is None or events.empty:
        return pd.DataFrame(columns=["date", "t_start", "t_end", "hold_minutes", "held_near", "trains_late",
                                     "cancelled_after", "hours_to_normal"])
    stops = events[events["is_network_stop"].astype(bool)].copy()
    stops["date"] = pd.to_datetime(stops["date"]).dt.date
    stops["t_start"] = pd.to_datetime(stops["t_start"])
    stops["t_end"] = pd.to_datetime(stops["t_end"])
    return stops.sort_values("t_start", ignore_index=True)


def _with_frame(con: duckdb.DuckDBPyConnection, name: str, frame: pd.DataFrame, sql: str) -> pd.DataFrame:
    con.register(name, frame)
    try:
        return con.execute(sql).df()
    finally:
        con.unregister(name)


def _opener(con) -> dict:
    hm = con.execute(HEATMAP_SQL).df()
    return {"heatmap": [{"dow": int(r.dow), "hour": int(r.hour), "n": int(r.n), "late_pct": _pct(r.late)}
                        for r in hm.itertuples()]}


def _recovery(con, figures: dict) -> dict:
    rec = con.execute(RECOVERY_SQL).df()
    fan = [{"k": int(r.k), "n": int(r.n), "p10": _num(r.p10), "p25": _num(r.p25), "median": _num(r.median),
            "p75": _num(r.p75), "p90": _num(r.p90), "back_on_time_pct": _pct(r.back)} for r in rec.itertuples()]
    at = rec.set_index("k")
    k5 = at.loc[5] if 5 in at.index else None
    k10 = at.loc[10] if 10 in at.index else None
    figures.update(
        recovery_trips=int(con.execute(RECOVERY_TRIPS_SQL).fetchone()[0]),
        recovery_median_change_5=_num(k5["median"]) if k5 is not None else 0.0,
        recovery_worse_5_pct=_pct(k5["worse"]) if k5 is not None else 0.0,
        recovery_better_5_pct=_pct(k5["better"]) if k5 is not None else 0.0,
        recovery_back_on_time_pct=_pct(k10["back"]) if k10 is not None else 0.0,
    )
    holds = (k5 is not None and k10 is not None
             and figures["recovery_median_change_5"] >= 0 and figures["recovery_back_on_time_pct"] < 5)
    return {"holds": bool(holds), "fan": fan}


def _express(con, figures: dict) -> dict:
    league = last_stop_league(con.execute(LAST_STOPS_SQL).df())
    rows = [{"train": str(r.trip_id), "service": _text(r.service), "direction": _text(r.direction),
             "departs": _text(r.departs), "last_stop": _text(r.last_stop_name), "runs": int(r.runs),
             "late_pct": _num(r.late_pct), "ci_low": _num(r.ci_low), "ci_high": _num(r.ci_high)}
            for r in league.head(LEAGUE_ROWS).itertuples()]
    worst = rows[0] if rows else {}
    figures.update(
        worst_train=worst.get("train", ""),
        worst_train_departs=worst.get("departs", ""),
        worst_train_late_pct=worst.get("late_pct", 0.0),
        worst_train_last_stop=worst.get("last_stop", ""),
        worst_train_service=worst.get("service", ""),
        worst_train_direction={"NB": "northbound", "SB": "southbound"}.get(worst.get("direction"), ""),
        nb_in_worst_25=int((league.head(WORST_TRAINS)["direction"] == "NB").sum()),
    )

    found = {(r.service, r.direction): r for r in con.execute(GROUPS_SQL).df().itertuples()}
    groups = [{"service": s, "direction": d, "n": int(found[s, d].n), "late_pct": _pct(found[s, d].late)}
              for s in SERVICES for d in DIRECTIONS if (s, d) in found]
    late = {(g["service"], g["direction"]): g["late_pct"] for g in groups}
    for s in ("Express", "Local"):
        for d in DIRECTIONS:
            figures[f"{s.lower()}_{d.lower()}_late_pct"] = late.get((s, d), 0.0)
    express_nb = late.get(("Express", "NB"))
    holds = express_nb is not None and all(express_nb > v for k, v in late.items() if k != ("Express", "NB"))

    found = {(r.leader, r.headway): r for r in con.execute(HEADWAY_SQL).df().itertuples()}
    headway = [{"leader": lead, "headway": hw, "n": int(found[lead, hw].n), "late_pct": _pct(found[lead, hw].late)}
               for lead in LEADER_BINS for hw in HEADWAY_BINS if (lead, hw) in found]
    cells = {(h["leader"], h["headway"]): h["late_pct"] for h in headway}
    figures["headway_close_late_pct"] = cells.get(("10–20", "≤ 10"), 0.0)
    figures["headway_far_late_pct"] = cells.get(("10–20", "21–30"), 0.0)
    return {"holds": bool(holds), "league": rows, "groups": groups, "headway": headway}


def _afternoons(con, stops: pd.DataFrame, cancellations: pd.DataFrame, clean_days: int, figures: dict) -> dict:
    if stops.empty:
        inside = 0
    else:
        windows = stops[["date", "t_start", "t_end"]].assign(date=pd.to_datetime(stops["date"]))
        inside = int(_with_frame(con, "st_windows", windows, STOP_WINDOWS_SQL)["inside"].iloc[0])
    total = int(con.execute("SELECT count(*) FROM clean WHERE late15").fetchone()[0])
    hours = pd.to_numeric(stops["hours_to_normal"], errors="coerce").dropna()
    example = stops[stops["date"] == EXAMPLE_DAY]
    figures.update(
        stops_count=len(stops),
        stops_every_weeks=_num(clean_days / len(stops) / 7) if len(stops) else 0,
        stops_late15_share_pct=_share(inside, total),
        stops_hours_to_normal_median=_num(hours.median()) if len(hours) else 0.0,
        stops_longest_hold_minutes=_int(pd.to_numeric(stops["hold_minutes"]).max()) if len(stops) else 0,
        example_trains_late=_int(example["trains_late"].iloc[0]) if len(example) else 0,
    )
    figures.update(_cancellation_rates(con, cancellations))

    newest = stops.sort_values("t_start", ascending=False)
    events = [{"date": str(r.date), "start": r.t_start.strftime("%H:%M"), "hold_minutes": _int(r.hold_minutes),
               "held_near": _text(r.held_near), "trains_late": _int(r.trains_late),
               "cancelled": _int(r.cancelled_after),
               "hours_to_normal": None if pd.isna(r.hours_to_normal) else _num(r.hours_to_normal)}
              for r in newest.itertuples()]
    return {"holds": bool(figures["stops_late15_share_pct"] >= 20), "events": events}


def _cancellation_rates(con, cancellations: pd.DataFrame) -> dict:
    """05 §4: on days with CANCEL_DAY_MIN+ never-run trips, the share of scheduled trips that never ran."""
    rates = {"cancel_express_pct": 0.0, "cancel_local_pct": 0.0}
    if cancellations is None or cancellations.empty:
        return rates
    never = cancellations.assign(date=pd.to_datetime(cancellations["date"]).dt.date)
    per_day = never.groupby("date").size()
    days = per_day[per_day >= CANCEL_DAY_MIN].index
    if len(days) == 0:
        return rates
    never = never[never["date"].isin(days)]
    cancelled = never["route_id"].map(lambda r: "Local" if str(r).startswith("Local") else r).value_counts()
    scheduled = _with_frame(con, "st_cancel_days", pd.DataFrame({"date": pd.to_datetime(list(days))}),
                            SCHEDULED_BY_SERVICE_SQL).set_index("service")["n"]
    for service in ("Express", "Local"):
        rates[f"cancel_{service.lower()}_pct"] = _share(cancelled.get(service, 0), scheduled.get(service, 0))
    return rates


def _saturday(days: pd.DataFrame, figures: dict) -> dict:
    days = days.assign(type=days["dow"].map(lambda d: "weekday" if d <= 5 else "Saturday" if d == 6 else "Sunday"),
                       share=days["late"] / days["n"])
    by_type = {t: g for t, g in days.groupby("type")}

    def pooled(t):
        g = by_type.get(t)
        return _share(g["late"].sum(), g["n"].sum()) if g is not None else 0.0

    def bad_days(t):
        g = by_type.get(t)
        return _pct((g["share"] > BAD_DAY_SHARE).mean()) if g is not None else 0.0

    figures.update(sat_late_pct=pooled("Saturday"), weekday_late_pct=pooled("weekday"), sun_late_pct=pooled("Sunday"),
                   sat_bad_day_pct=bad_days("Saturday"), weekday_bad_day_pct=bad_days("weekday"),
                   saturdays=len(by_type.get("Saturday", [])))
    holds = (figures["saturdays"] > 0
             and figures["sat_late_pct"] > max(figures["weekday_late_pct"], figures["sun_late_pct"]))
    rows = [{"date": str(r.date), "type": r.type, "n": int(r.n), "late_pct": _pct(r.share)} for r in days.itertuples()]
    return {"holds": bool(holds), "days": rows}


def build_stories(con: duckdb.DuckDBPyConnection, events: pd.DataFrame, cancellations: pd.DataFrame,
                  now: datetime) -> dict:
    """The ``stories.json`` payload: chart data, quoted figures and a claim check per story.

    Reads ``clean`` and ``scheduled_trips``. ``events`` is the ``detect_network_stops``
    frame (only ``is_network_stop`` rows count), ``cancellations`` the ``never_ran``
    frame, ``now`` Pacific wall time.
    """
    days = con.execute(DAYS_SQL).df()
    days["date"] = pd.to_datetime(days["date"]).dt.date
    n, late, late15 = days["n"].sum(), days["late"].sum(), days["late15"].sum()
    top_days = days["late15"].sort_values(ascending=False).head(WORST_DAYS).sum()
    figures = {
        "on_time_pct": _share(n - late, n),
        "worst_days_10_share_pct": _share(top_days, late15),
    }
    stops = _network_stops(events)
    stories = {
        "opener": _opener(con),
        "recovery": _recovery(con, figures),
        "express": _express(con, figures),
        "afternoons": _afternoons(con, stops, cancellations, len(days), figures),
        "saturday": _saturday(days, figures),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "data_through": str(days["date"].max()) if len(days) else "",
        "first_day": str(days["date"].min()) if len(days) else "",
        "clean_days": len(days),
        "figures": {name: {"value": figures[name], "unit": unit} for name, unit in REQUIRED_FIGURES.items()},
        "stories": stories,
    }


def validate_stories(payload: dict) -> None:
    """Raise ``ValueError`` naming the first problem that should stop ``stories.json`` being published."""
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"schema_version is {payload.get('schema_version')!r}, expected {SCHEMA_VERSION}")
    figures = payload.get("figures")
    if not isinstance(figures, dict):
        raise ValueError("figures is missing")
    for name, unit in REQUIRED_FIGURES.items():
        figure = figures.get(name)
        if not isinstance(figure, dict) or "value" not in figure:
            raise ValueError(f"figure {name} is missing")
        if figure.get("unit") != unit:
            raise ValueError(f"figure {name} has unit {figure.get('unit')!r}, expected {unit!r}")
        value = figure["value"]
        if unit in ("text", "time"):
            if not isinstance(value, str):
                raise ValueError(f"figure {name} is not a string: {value!r}")
        elif isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"figure {name} is not a finite number: {value!r}")

    stories = payload.get("stories")
    if not isinstance(stories, dict):
        raise ValueError("stories is missing")
    for story in ("recovery", "express", "afternoons", "saturday"):
        if not isinstance((stories.get(story) or {}).get("holds"), bool):
            raise ValueError(f"stories.{story}.holds is not a boolean")
    for story, key in (("opener", "heatmap"), ("recovery", "fan"), ("express", "league"), ("express", "groups"),
                       ("express", "headway"), ("saturday", "days")):
        if not (stories.get(story) or {}).get(key):
            raise ValueError(f"stories.{story}.{key} is empty")
    if not isinstance(stories["afternoons"].get("events"), list):
        raise ValueError("stories.afternoons.events is not a list")
    try:
        json.dumps(payload, allow_nan=False)
    except ValueError as e:
        raise ValueError(f"stories payload is not strict JSON: {e}") from e


def example_day(con: duckdb.DuckDBPyConnection, gps_for_dates: Callable[[list[date]], pd.DataFrame],
                station_meta: list[dict], shape_points: list[tuple], events: pd.DataFrame,
                day: date = EXAMPLE_DAY) -> dict | None:
    """The ``stories_example_day.json`` diagram data: every train around the day's first network stop.

    The window runs from an hour before the freeze to four hours after it, clipped
    to 05:00-23:59; 14:00-20:00 with no network stop that day. Trains at least
    EXAMPLE_FROZEN_LATE_MIN late keep every point, the rest are thinned. None when
    the day has no clean arrivals or no pings.
    """
    delays = con.execute("""
        SELECT trip_id, max(delay_signed) AS max_delay, mode(direction) AS direction
        FROM clean WHERE date = ? GROUP BY 1
    """, [day]).df()
    if delays.empty:
        return None
    gps = gps_for_dates([day])
    if gps.empty:
        return None
    gps = gps.assign(timestamp=pd.to_datetime(gps["timestamp"], errors="coerce"),
                     trip_id=gps["trip_id"].astype(str)).dropna(subset=["timestamp"])

    midnight = datetime.combine(day, time())
    stops = _network_stops(events)
    stops = stops[stops["date"] == day]
    if stops.empty:
        freeze, start, end = None, midnight + timedelta(hours=14), midnight + timedelta(hours=20)
    else:
        first = stops.iloc[0]
        freeze = {"start": first.t_start.strftime("%H:%M"), "end": first.t_end.strftime("%H:%M")}
        start = max(first.t_start - timedelta(hours=1), midnight + timedelta(hours=5))
        end = min(first.t_end + timedelta(hours=4), midnight + timedelta(hours=23, minutes=59))

    shown = gps[(gps["timestamp"] >= start) & (gps["timestamp"] < end + timedelta(minutes=1))
                & (gps["vehicle_lat"] > 1) & (gps["trip_id"].str.len() <= 3)]
    max_delay = {t: _num(d) for t, d in zip(delays["trip_id"].astype(str), delays["max_delay"])}
    direction = dict(zip(delays["trip_id"].astype(str), delays["direction"]))
    trains = []
    for trajectory in (project_trajectories(shown, shape_points) if not shown.empty else []):
        trip_id = trajectory["trip_id"]
        delay = max_delay.get(trip_id, 0.0)
        trains.append({
            "trip_id": trip_id,
            "direction": direction.get(trip_id) or trajectory["direction"],
            "max_delay_min": delay,
            "points": trajectory["points"] if delay >= EXAMPLE_FROZEN_LATE_MIN else thin_points(trajectory["points"]),
        })
    return {
        "date": str(day),
        "window": {"start": start.strftime("%H:%M"), "end": end.strftime("%H:%M")},
        "freeze": freeze,
        "stations": station_distances(station_meta, shape_points),
        "trains": trains,
    }
