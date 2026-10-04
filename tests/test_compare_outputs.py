import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from compare_outputs import compare_dirs, diff  # noqa: E402


def test_records_match_by_key_regardless_of_order():
    old = [{"stop_id": 1, "pct": 50.0}, {"stop_id": 2, "pct": 70.0}]
    new = [{"stop_id": 2, "pct": 70.04}, {"stop_id": 1, "pct": 50.0}]
    assert diff(old, new) == []


def test_reports_numeric_and_missing_differences():
    problems = diff({"a": 1, "b": 2.0, "last_updated": "x"}, {"a": 2, "b": 2.2, "last_updated": "y"})
    assert problems == [".a: 1 != 2", ".b: 2.0 != 2.2"]
    assert diff([{"date": "d1"}], []) == [": length 1 != 0"]
    assert diff([{"date": "d1"}], [{"date": "d2"}]) == ["[('d1',)]: missing in new", "[('d2',)]: only in new"]


def test_compare_dirs(tmp_path):
    (tmp_path / "old").mkdir()
    (tmp_path / "new").mkdir()
    (tmp_path / "old" / "a.json").write_text(json.dumps({"x": 1}))
    (tmp_path / "new" / "a.json").write_text(json.dumps({"x": 1}))
    (tmp_path / "old" / "b.json").write_text("[]")
    assert compare_dirs(tmp_path / "old", tmp_path / "new") == {"a.json": [], "b.json": ["missing in new"]}
