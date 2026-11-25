import json
from datetime import datetime
from pathlib import Path

import pytest

from src.pipeline import build as build_module
from src.pipeline.build import build
from src.pipeline.dashboard import DASHBOARD_FILES
from src.pipeline.stories import REQUIRED_FIGURES, SCHEMA_VERSION
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

    assert result["arrivals"] == 1  # clean: the origin stop is dropped
    data = tmp_path / "static" / "data"
    for name in DASHBOARD_FILES + ["incidents.json", "incident_trajectories.json"]:
        assert (data / name).exists(), name
    assert json.loads((data / "stats.json").read_text())["major_delay_percentage"] == 100.0
    assert json.loads((data / "stats.json").read_text())["clean"]["total_arrivals"] == 1
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


def _run_build(tmp_path):
    make_feed(tmp_path / "feeds")
    conn = make_db(tmp_path / "t.db")
    add_pings(conn,
              ("101", SF[0], SF[2], SF[3], "2026-03-04 08:30:00"),
              ("101", TWENTY_SECOND[0], 37.7660, -122.3930, "2026-03-04 08:33:00"),
              ("101", TWENTY_SECOND[0], TWENTY_SECOND[2], TWENTY_SECOND[3], "2026-03-04 08:35:00"))
    return build(tmp_path / "t.db", tmp_path / "a.duckdb", tmp_path / "static", feeds_root=tmp_path / "feeds",
                 gtfs_dir=REPO / "gtfs_data", station_meta_path=REPO / "data" / "station_metadata.json",
                 now=datetime(2026, 3, 5, 0, 5))


def test_build_keeps_previous_stories_when_the_step_fails(tmp_path, monkeypatch, caplog):
    data = tmp_path / "static" / "data"
    data.mkdir(parents=True)
    (data / "stories.json").write_bytes(b'{"previous": true}')
    (data / "stories_example_day.json").write_bytes(b'{"previous_day": true}')

    def boom(*args, **kwargs):
        raise RuntimeError("no stories today")
    monkeypatch.setattr(build_module, "build_stories", boom)

    _run_build(tmp_path)

    assert (data / "stories.json").read_bytes() == b'{"previous": true}'
    assert (data / "stories_example_day.json").read_bytes() == b'{"previous_day": true}'
    for name in DASHBOARD_FILES + ["incidents.json", "incident_trajectories.json"]:
        assert (data / name).exists(), name
    assert "stories step failed; keeping the previous stories files" in caplog.text


def test_build_writes_valid_stories(tmp_path, monkeypatch):
    payload = {
        "schema_version": SCHEMA_VERSION,
        "figures": {name: {"value": "x" if unit in ("text", "time") else 1, "unit": unit}
                    for name, unit in REQUIRED_FIGURES.items()},
        "stories": {},
    }
    monkeypatch.setattr(build_module, "build_stories", lambda *a, **k: payload)
    monkeypatch.setattr(build_module, "validate_stories", lambda p: None)
    monkeypatch.setattr(build_module, "example_day", lambda *a, **k: None)

    _run_build(tmp_path)

    data = tmp_path / "static" / "data"
    assert json.loads((data / "stories.json").read_text()) == payload
    assert not (data / "stories_example_day.json").exists()


def test_stories_json_rejects_nan(tmp_path):
    from src.pipeline.dashboard import write_json_atomic
    with pytest.raises(ValueError):
        write_json_atomic(tmp_path / "x.json", {"a": float("nan")}, allow_nan=False)
    assert list(tmp_path.iterdir()) == []


def test_build_keeps_previous_stories_when_the_example_day_has_nan(tmp_path, monkeypatch, caplog):
    data = tmp_path / "static" / "data"
    data.mkdir(parents=True)
    (data / "stories.json").write_bytes(b'{"previous": true}')
    payload = {"schema_version": SCHEMA_VERSION, "figures": {}, "stories": {}}
    monkeypatch.setattr(build_module, "build_stories", lambda *a, **k: payload)
    monkeypatch.setattr(build_module, "validate_stories", lambda p: None)
    monkeypatch.setattr(build_module, "example_day", lambda *a, **k: {"points": [float("nan")]})

    _run_build(tmp_path)

    assert (data / "stories.json").read_bytes() == b'{"previous": true}'
    assert not (data / "stories_example_day.json").exists()
    assert "stories step failed; keeping the previous stories files" in caplog.text


def test_build_calls_the_real_stories_functions_with_matching_arguments(tmp_path, caplog):
    _run_build(tmp_path)  # tiny fixture: unstubbed stories step
    records = [r for r in caplog.records if "stories step failed" in r.getMessage()]
    data = tmp_path / "static" / "data"
    if records:
        exc = records[0].exc_info[1]
        assert type(exc) is ValueError, repr(exc)  # the validator rejecting thin data, not a bad call
        assert "is empty" in str(exc) or "missing" in str(exc) or "finite" in str(exc), str(exc)
        assert not (data / "stories.json").exists()
    else:
        from src.pipeline.stories import validate_stories
        validate_stories(json.loads((data / "stories.json").read_text()))


def test_build_carries_on_without_network_stops_when_the_detector_fails(tmp_path, monkeypatch, caplog):
    from src.pipeline.network_stops import COLUMNS

    def boom(*args, **kwargs):
        raise RuntimeError("detector broke")
    seen = {}
    real_build_stories = build_module.build_stories

    def spy(con, events, cancellations, now):
        seen["events"] = events
        return real_build_stories(con, events, cancellations, now)
    monkeypatch.setattr(build_module, "detect_network_stops", boom)
    monkeypatch.setattr(build_module, "build_stories", spy)

    _run_build(tmp_path)

    data = tmp_path / "static" / "data"
    incidents = json.loads((data / "incidents.json").read_text())
    assert incidents and all(i["held_near"] is None for i in incidents)
    assert list(seen["events"].columns) == COLUMNS and seen["events"].empty  # stories still ran
    failed = [r for r in caplog.records if "network-stop step failed" in r.getMessage()]
    assert failed and failed[0].exc_info is not None  # logged with its traceback


def test_never_ran_is_computed_once_per_build(tmp_path, monkeypatch):
    from src.pipeline import network_stops
    calls = []
    real = network_stops._never_ran

    def counted(con):
        calls.append(1)
        return real(con)
    monkeypatch.setattr(network_stops, "_never_ran", counted)

    _run_build(tmp_path)

    assert len(calls) == 1
