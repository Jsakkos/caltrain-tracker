import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import export_to_website  # noqa: E402
from src.pipeline.dashboard import write_dashboard  # noqa: E402
from tests.test_dashboard import NOW, arrivals  # noqa: E402


def test_export_plots_copies_build_plots_and_removes_stale_ones(tmp_path, monkeypatch):
    src, out = tmp_path / "plots", tmp_path / "site"
    src.mkdir()
    (out / "plots").mkdir(parents=True)
    for name in ["daily_stats.html", "commute_delay.html", "weekly_trends.html"]:
        (src / name).write_text(name)
    (out / "plots" / "on_time_heatmap.html").write_text("old")
    monkeypatch.setattr(export_to_website, "SOURCE_PLOTS", src)
    monkeypatch.setattr(export_to_website, "OUTPUT_DIR", out)

    export_to_website.export_plots()

    assert sorted(p.name for p in (out / "plots").iterdir()) == ["commute_delay.html", "daily_stats.html"]


def test_build_plots_match_what_the_build_writes(tmp_path):
    write_dashboard(tmp_path, arrivals(), NOW)
    assert sorted(p.name for p in (tmp_path / "plots").iterdir()) == sorted(export_to_website.BUILD_PLOTS)
