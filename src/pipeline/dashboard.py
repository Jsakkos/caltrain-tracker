"""
Website files built from the arrivals table (one row per trip/stop/day).

Keys, rounding and sort order match the legacy generate_dashboard_data so the
website (MyWebsite src/lib/api.ts) doesn't notice the switch.
"""
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.pipeline.clean import last_stop_league

DASHBOARD_FILES = [
    "stats.json", "daily_performance.json", "station_performance.json", "train_performance.json",
    "hourly_heatmap.json", "commute_analysis.json", "weekly_summary.json", "monthly_summary.json",
]

STATUS_COLORS = {"On Time": "#00CC96", "Minor": "#FECB52", "Major": "#EF553B",
                 "Minor Delay": "#FECB52", "Major Delay": "#EF553B"}
PLOT_LAYOUT = dict(plot_bgcolor="#f4f4f4", paper_bgcolor="#f4f4f4", autosize=True, height=600,
                   margin=dict(l=20, r=20, t=50, b=20), title_font_size=24)


def _with_calendar(arrivals: pd.DataFrame) -> pd.DataFrame:
    df = arrivals.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["day_of_week"] = df["date"].dt.dayofweek  # 0=Monday
    df["day_name"] = df["date"].dt.strftime("%A")
    df["month"] = df["date"].dt.to_period("M").astype(str)
    df["week_start"] = (df["date"] - pd.to_timedelta(df["day_of_week"], unit="D")).dt.strftime("%Y-%m-%d")
    df["on_time"] = df["delay_severity"] == "On Time"
    df["minor"] = df["delay_severity"] == "Minor"
    df["major"] = df["delay_severity"] == "Major"
    return df


def _pct(part: pd.Series, whole: pd.Series) -> pd.Series:
    return round(part / whole * 100, 1)


def _on_time_share(df: pd.DataFrame) -> float:
    return round(df["on_time"].sum() / len(df) * 100, 2) if len(df) else 0


def _headline(df: pd.DataFrame) -> dict:
    """The headline figures shared by the legacy and clean blocks of stats.json."""
    total = len(df)
    last = df["date"].max()
    return {
        "on_time_percentage": round(df["on_time"].sum() / total * 100, 2) if total else 0,
        "minor_delay_percentage": round(df["minor"].sum() / total * 100, 2) if total else 0,
        "major_delay_percentage": round(df["major"].sum() / total * 100, 2) if total else 0,
        "total_arrivals": total,
        "avg_delay_minutes": round(float(df["delay_minutes"].mean()), 2) if total else 0,
        "median_delay_minutes": round(float(df["delay_minutes"].median()), 2) if total else 0,
        "days_tracked": int(df["date"].nunique()),  # skips days the collector was down
        "rolling_7d_on_time": _on_time_share(df[df["date"] >= last - pd.Timedelta(days=7)]) if total else 0,
        "rolling_30d_on_time": _on_time_share(df[df["date"] >= last - pd.Timedelta(days=30)]) if total else 0,
    }


def _nullable(value):
    return None if pd.isna(value) else value


def build_dashboard(arrivals: pd.DataFrame, now: datetime, legacy: pd.DataFrame | None = None,
                    daily: pd.DataFrame | None = None) -> dict[str, object]:
    """Filename -> JSON payload for every file in DASHBOARD_FILES.

    ``arrivals`` is the clean frame and feeds every file except the legacy keys of
    stats.json (``legacy``, the all-arrivals frame; defaults to ``arrivals``) and
    daily_performance.json (``daily``, clean rows including bad-feed days, flagged
    by a ``bad_feed`` column; defaults to ``arrivals`` with no bad days).
    """
    df = _with_calendar(arrivals)
    old = df if legacy is None else _with_calendar(legacy)
    day_df = _with_calendar(arrivals.assign(bad_feed=False) if daily is None else daily)

    old_stats = _headline(old)
    clean_stats = _headline(df)
    clean_stats["bad_feed_days"] = int(day_df.loc[day_df["bad_feed"], "date"].nunique())
    clean_stats = {k: clean_stats[k] for k in (
        "on_time_percentage", "minor_delay_percentage", "major_delay_percentage", "total_arrivals",
        "avg_delay_minutes", "median_delay_minutes", "days_tracked", "bad_feed_days",
        "rolling_7d_on_time", "rolling_30d_on_time")}
    stats = {
        **{k: old_stats[k] for k in ("on_time_percentage", "minor_delay_percentage", "major_delay_percentage",
                                     "total_arrivals", "avg_delay_minutes", "median_delay_minutes")},
        "last_updated": now.isoformat(),
        "date_range": {"start": old["date"].min().strftime("%Y-%m-%d"), "end": old["date"].max().strftime("%Y-%m-%d")},
        **{k: old_stats[k] for k in ("days_tracked", "rolling_7d_on_time", "rolling_30d_on_time")},
        "clean": clean_stats,
    }

    daily_rows = day_df.groupby(day_df["date"].dt.strftime("%Y-%m-%d")).agg(
        total_trips=("trip_id", "count"), on_time_count=("on_time", "sum"), minor_count=("minor", "sum"),
        major_count=("major", "sum"), avg_delay_min=("delay_minutes", "mean"), bad_feed=("bad_feed", "max"),
    ).reset_index()
    daily_rows["on_time_pct"] = _pct(daily_rows["on_time_count"], daily_rows["total_trips"])
    daily_rows["minor_pct"] = _pct(daily_rows["minor_count"], daily_rows["total_trips"])
    daily_rows["major_pct"] = _pct(daily_rows["major_count"], daily_rows["total_trips"])
    daily_rows["avg_delay_min"] = round(daily_rows["avg_delay_min"], 2)
    daily_rows["bad_feed"] = daily_rows["bad_feed"].astype(bool)
    daily_rows = daily_rows.sort_values("date")

    station = df.groupby(["stop_id", "stop_name"]).agg(
        total_arrivals=("trip_id", "count"), on_time_count=("on_time", "sum"),
        avg_delay_min=("delay_minutes", "mean"), median_delay_min=("delay_minutes", "median"),
    ).reset_index()
    station["on_time_pct"] = _pct(station["on_time_count"], station["total_arrivals"])
    station["avg_delay_min"] = round(station["avg_delay_min"], 2)
    station["median_delay_min"] = round(station["median_delay_min"], 2)
    station["stop_lat"] = None  # the legacy frame never carried coordinates; kept for parity
    station["stop_lon"] = None
    station["stop_id"] = station["stop_id"].astype(int)
    station = station.sort_values("on_time_pct", ascending=False)

    train = df.groupby("trip_id").agg(
        total_stops=("stop_id", "count"), on_time_count=("on_time", "sum"),
        avg_delay_min=("delay_minutes", "mean"), days_observed=("date", "nunique"),
    ).reset_index()
    train["on_time_pct"] = _pct(train["on_time_count"], train["total_stops"])
    train["avg_delay_min"] = round(train["avg_delay_min"], 2)
    train["trip_id"] = train["trip_id"].astype(int)
    train = train.sort_values("on_time_pct", ascending=False)
    league = last_stop_league(df).assign(trip_id=lambda x: x["trip_id"].astype(int)).set_index("trip_id")
    weekday_runs = df[df["weekday"]].groupby("trip_id")["date"].nunique()  # one last stop per train and day
    train["last_stop_runs"] = train["trip_id"].map(weekday_runs.rename(index=int)).fillna(0).astype(int)
    train["last_stop_late_pct"] = train["trip_id"].map(league["late_pct"])
    train["last_stop_name"] = train["trip_id"].map(league["last_stop_name"])
    train_rows = [{k: (_nullable(v) if k in ("last_stop_late_pct", "last_stop_name") else v)
                   for k, v in row.items()} for row in train.to_dict("records")]

    heatmap = df.groupby(["day_of_week", "day_name", "hour"]).agg(
        total=("trip_id", "count"), on_time_count=("on_time", "sum"), avg_delay_min=("delay_minutes", "mean"),
    ).reset_index()
    heatmap["on_time_pct"] = _pct(heatmap["on_time_count"], heatmap["total"])
    heatmap["avg_delay_min"] = round(heatmap["avg_delay_min"], 2)

    commute = {}
    for period in df["commute_period"].unique():
        subset = df[df["commute_period"] == period]
        n = len(subset)
        commute[period] = {
            "total_trips": n,
            "on_time_pct": round(subset["on_time"].sum() / n * 100, 1),
            "minor_pct": round(subset["minor"].sum() / n * 100, 1),
            "major_pct": round(subset["major"].sum() / n * 100, 1),
            "avg_delay_min": round(float(subset["delay_minutes"].mean()), 2),
            "median_delay_min": round(float(subset["delay_minutes"].median()), 2),
        }

    def summary(key: str) -> list[dict]:
        out = df.groupby(key).agg(
            total_trips=("trip_id", "count"), on_time_count=("on_time", "sum"),
            avg_delay_min=("delay_minutes", "mean"), days_with_data=("date", "nunique"),
        ).reset_index()
        out["on_time_pct"] = _pct(out["on_time_count"], out["total_trips"])
        out["avg_delay_min"] = round(out["avg_delay_min"], 2)
        return out.sort_values(key).to_dict("records")

    return {
        "stats.json": stats,
        "daily_performance.json": daily_rows.to_dict("records"),
        "station_performance.json": station.to_dict("records"),
        "train_performance.json": train_rows,
        "hourly_heatmap.json": heatmap.to_dict("records"),
        "commute_analysis.json": commute,
        "weekly_summary.json": summary("week_start"),
        "monthly_summary.json": summary("month"),
    }


# Legacy plots ran on one row per matched ping, so arrivals are weighted by ping_count.

def daily_severity_shares(arrivals: pd.DataFrame) -> pd.DataFrame:
    """Percent of pings per severity, one row per date."""
    weights = arrivals.groupby(["date", "delay_severity"])["ping_count"].sum().unstack(fill_value=0)
    return weights.div(weights.sum(axis=1), axis=0) * 100


def commute_shares(arrivals: pd.DataFrame) -> pd.DataFrame:
    commute = arrivals[arrivals["commute_period"].isin(["Morning", "Evening"])]
    counts = commute.groupby(["commute_period", "delay_severity"])["ping_count"].sum().rename("counts").reset_index()
    totals = counts.groupby("commute_period")["counts"].transform("sum")
    return counts.assign(percentage=counts["counts"] / totals * 100)


def daily_stats_figure(arrivals: pd.DataFrame) -> go.Figure:
    shares = daily_severity_shares(arrivals).reindex(columns=["Major", "Minor", "On Time"], fill_value=0)
    shares.index = pd.to_datetime(shares.index).date
    melted = shares.rename_axis("date").reset_index().melt(
        id_vars="date", value_vars=["Major", "Minor", "On Time"], var_name="Status", value_name="Percentage")
    melted["Status"] = melted["Status"].replace({"Major": "Major Delay", "Minor": "Minor Delay"})
    fig = px.bar(melted, x="date", y="Percentage", color="Status", title="On-time performance by date",
                 category_orders={"Status": ["On Time", "Minor Delay", "Major Delay"]},
                 color_discrete_map=STATUS_COLORS, labels={"date": "Date", "Percentage": "Percentage"})
    fig.update_layout(**PLOT_LAYOUT)
    return fig


def commute_figure(arrivals: pd.DataFrame) -> go.Figure:
    fig = px.bar(commute_shares(arrivals), x="commute_period", y="percentage", color="delay_severity",
                 barmode="group", title="Delay Distribution by Commute Period",
                 category_orders={"delay_severity": ["On Time", "Minor", "Major"]},
                 color_discrete_map=STATUS_COLORS,
                 labels={"commute_period": "Commute Period", "percentage": "Percentage",
                         "delay_severity": "Delay Status"})
    fig.update_layout(**PLOT_LAYOUT)
    return fig


def write_json_atomic(path: Path, payload, indent: int | None = None) -> None:
    """Write via a temp file and rename, so the 02:30 website export never reads half a file."""
    path = Path(path)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            json.dump(payload, f, indent=indent)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def write_html_atomic(path: Path, fig: go.Figure) -> None:
    path = Path(path)
    tmp = path.with_name(f".{path.name}.tmp")
    fig.write_html(tmp, include_plotlyjs="cdn")
    os.replace(tmp, path)


def write_dashboard(out_dir: Path, arrivals: pd.DataFrame, now: datetime, legacy: pd.DataFrame | None = None,
                    daily: pd.DataFrame | None = None) -> list[str]:
    """Write the dashboard JSON (static/data) and the two plots (static/plots). Returns file names."""
    data_dir, plots_dir = Path(out_dir) / "data", Path(out_dir) / "plots"
    data_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)
    pretty = {"stats.json", "commute_analysis.json"}  # indented in the legacy output
    for name, payload in build_dashboard(arrivals, now, legacy, daily).items():
        write_json_atomic(data_dir / name, payload, indent=2 if name in pretty else None)
    write_html_atomic(plots_dir / "daily_stats.html", daily_stats_figure(arrivals))
    write_html_atomic(plots_dir / "commute_delay.html", commute_figure(arrivals))
    return DASHBOARD_FILES + ["daily_stats.html", "commute_delay.html"]
