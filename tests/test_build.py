import json
from datetime import datetime
from pathlib import Path

from src.pipeline.build import build
from src.pipeline.dashboard import DASHBOARD_FILES
from tests.pipeline_fixtures import SF, TWENTY_SECOND, add_pings, make_db, make_feed

REPO = Path(__file__).resolve().parent.parent


def test_build_writes_every_website_file(tmp_path):
    make_feed(tmp_path / "feeds")
    conn = make_db(tmp_path / "t.db")
    add_pings(conn,
              ("101", SF[0], SF[2], SF[3], "2026-03-04 08:30:00"),
              ("101", TWENTY_SECOND[0], 37.7660, -122.3930, "2026-03-04 08:33:00"),
              ("101", TWENTY_SECOND[0], TWENTY_SECOND[2], TWENTY_SECOND[3], "2026-03-04 08:35:00"))
    result = build(tmp_path / "t.db", tmp_path / "a.duckdb", tmp_path / "static", feeds_root=tmp_path / "feeds",
                   gtfs_dir=REPO / "gtfs_data", station_meta_path=REPO / "data" / "station_metadata.json",
                   now=datetime(2026, 3, 5, 0, 5))

    assert result["arrivals"] == 2
    data = tmp_path / "static" / "data"
    for name in DASHBOARD_FILES + ["incidents.json", "incident_trajectories.json"]:
        assert (data / name).exists(), name
    assert json.loads((data / "stats.json").read_text())["major_delay_percentage"] == 100.0
    assert [i["id"] for i in json.loads((data / "incidents.json").read_text())] == ["2026-03-04-train-101"]
    for name in ["daily_stats.html", "commute_delay.html"]:  # the names MyWebsite links
        assert (tmp_path / "static" / "plots" / name).exists(), name


def test_build_without_data_leaves_outputs_alone(tmp_path):
    make_feed(tmp_path / "feeds")
    make_db(tmp_path / "t.db")
    out = tmp_path / "static"
    result = build(tmp_path / "t.db", tmp_path / "a.duckdb", out, feeds_root=tmp_path / "feeds")
    assert result["arrivals"] == 0
    assert not out.exists()


def test_local_now_is_pacific_wall_time_on_a_utc_host():
    from datetime import timezone
    from src.pipeline.build import local_now
    # 02:00 UTC on Oct 4 is still the evening of Oct 3 in California.
    assert local_now(datetime(2026, 10, 4, 2, 0, tzinfo=timezone.utc)) == datetime(2026, 10, 3, 19, 0)
