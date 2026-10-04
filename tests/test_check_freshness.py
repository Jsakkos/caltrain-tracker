import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from check_freshness import is_stale, latest_ping  # noqa: E402
from tests.pipeline_fixtures import add_pings, make_db  # noqa: E402

NOW = datetime(2026, 3, 4, 12, 0)


@pytest.mark.parametrize("latest, now, stale", [
    (datetime(2026, 3, 4, 11, 55), NOW, False),
    (datetime(2026, 3, 4, 11, 45), NOW, True),
    (None, NOW, True),                                                    # nothing collected at all
    (datetime(2026, 3, 4, 0, 40), datetime(2026, 3, 4, 3, 0), False),     # overnight: no trains
    (datetime(2026, 3, 4, 0, 40), datetime(2026, 3, 4, 5, 30), True),     # first trains are running
    (datetime(2026, 3, 3, 23, 30), datetime(2026, 3, 4, 0, 30), True),    # late trains still running
])
def test_is_stale(latest, now, stale):
    assert is_stale(latest, now) is stale


def test_latest_ping_reads_the_newest_row(tmp_path):
    conn = make_db(tmp_path / "t.db")
    assert latest_ping(str(tmp_path / "t.db")) is None
    add_pings(conn, ("101", "70011", 37.0, -122.0, "2026-03-04 11:58:00"),
              ("101", "70011", 37.0, -122.0, "2026-03-04 11:59:00"))
    assert latest_ping(str(tmp_path / "t.db")) == datetime(2026, 3, 4, 11, 59)
