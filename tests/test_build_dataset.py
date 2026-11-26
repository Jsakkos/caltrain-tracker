import importlib.util
from pathlib import Path

from src.pipeline import clean

REPO = Path(__file__).resolve().parent.parent


def test_notebook_builder_uses_the_pipeline_feed_health_rule():
    spec = importlib.util.spec_from_file_location("build_dataset", REPO / "analysis" / "build_dataset.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.FEED_HEALTH_SQL is clean.FEED_HEALTH_SQL  # one definition, not a copy
