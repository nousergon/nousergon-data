"""close_history states its own per-session coverage (alpha-engine-config-I7218).

On 2026-07-21, 07-22 and 07-31 ``market_data/close_history/consolidated.json``
carried a close for 746-794 of ~957 series, and nothing on the artifact said so: a
forward-return grade anchored on those days silently lost 17-22% of its
cross-section. The artifact now carries ``coverage_by_date`` and names each graded
session short of its trailing median in ``incomplete_sessions``. Every number below
is synthetic.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from collectors import metron_market_data as mmd

RUN_DATE = "2026-10-16"  # a Friday session
DAY = dt.date.fromisoformat(RUN_DATE)
KEY = mmd.CONSOLIDATED_CLOSE_HISTORY_KEY
# 2026-10-05 .. 2026-10-16: ten consecutive NYSE sessions (no holiday in between).
SESSIONS = [
    "2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09",
    "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16",
]
SYMBOLS = [f"S{i:02d}" for i in range(20)]


def _full(days=SESSIONS, skip=()) -> dict[str, list[tuple[str, float]]]:
    """Every symbol carries every day, except (symbol, day) pairs in ``skip``."""
    return {
        sym: [(d, 100.0 + i + k) for k, d in enumerate(days) if (sym, d) not in skip]
        for i, sym in enumerate(SYMBOLS)
    }


# ── the pure rule ───────────────────────────────────────────────────────────


def test_coverage_by_date_counts_every_series_on_every_date():
    series = _full(skip={("S00", "2026-10-14"), ("S01", "2026-10-14")})
    coverage, incomplete = mmd.session_coverage(series, DAY)
    assert list(coverage) == SESSIONS
    assert coverage["2026-10-14"] == 18
    assert all(coverage[d] == 20 for d in SESSIONS if d != "2026-10-14")
    # 18 / 20 = 90%, under the 95% floor of its trailing five-session median (20).
    assert incomplete == {"2026-10-14": {"tickers": 18, "trailing_median": 20, "floor": 19.0}}


def test_a_session_at_the_floor_is_complete():
    series = _full(skip={("S00", "2026-10-14")})  # 19 / 20 = 95%, exactly the floor
    _, incomplete = mmd.session_coverage(series, DAY)
    assert incomplete == {}


def test_a_session_no_series_carries_is_incomplete_at_zero():
    days = [d for d in SESSIONS if d != "2026-10-13"]
    coverage, incomplete = mmd.session_coverage(_full(days), DAY)
    assert "2026-10-13" not in coverage
    assert incomplete["2026-10-13"]["tickers"] == 0


def test_a_short_run_date_is_graded():
    """The newest session is the one most likely to be short, and it is graded."""
    series = _full(skip={(s, RUN_DATE) for s in SYMBOLS[:5]})
    _, incomplete = mmd.session_coverage(series, DAY)
    assert list(incomplete) == [RUN_DATE] and incomplete[RUN_DATE]["tickers"] == 15


def test_a_holiday_is_counted_but_never_graded():
    """A foreign listing trading on a US holiday is not a hole (2026-07-03, observed July 4)."""
    days = ["2026-06-26", "2026-06-29", "2026-06-30", "2026-07-01", "2026-07-02", "2026-07-06"]
    series = _full(days)
    series["S00"].append(("2026-07-03", 1.0))
    coverage, incomplete = mmd.session_coverage(series, dt.date(2026, 7, 6))
    assert coverage["2026-07-03"] == 1
    assert incomplete == {}


def test_sessions_before_the_artifact_has_a_full_trailing_window_are_not_graded():
    """The first five sessions have no median to compare against."""
    series = _full(skip={(s, SESSIONS[0]) for s in SYMBOLS[:10]})
    _, incomplete = mmd.session_coverage(series, DAY)
    assert incomplete == {}


def test_only_the_last_graded_sessions_are_graded():
    series = _full(skip={(s, "2026-10-14") for s in SYMBOLS[:10]})
    _, incomplete = mmd.session_coverage(series, DAY, graded=1)
    assert incomplete == {}
    _, incomplete = mmd.session_coverage(series, DAY, graded=3)
    assert list(incomplete) == ["2026-10-14"]


def test_a_non_session_run_date_anchors_on_the_session_before_it():
    _, incomplete = mmd.session_coverage(_full(skip={("S00", RUN_DATE), ("S01", RUN_DATE)}),
                                         dt.date(2026, 10, 17))  # Saturday
    assert list(incomplete) == [RUN_DATE]


def test_an_empty_artifact_reports_nothing():
    assert mmd.session_coverage({}, DAY) == ({}, {})


# ── wired into collect_history ──────────────────────────────────────────────


@pytest.fixture
def universe(monkeypatch):
    monkeypatch.setattr(
        mmd, "load_price_derived_universe",
        lambda bucket, s3: ([{"yf_symbol": s, "currency": "USD"} for s in SYMBOLS], []),
    )


def _s3_without_previous():
    s3 = MagicMock()
    s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject",
    )
    return s3


def _run(s3, series, **kw):
    return mmd.collect_history(
        bucket="b", run_date=RUN_DATE, s3_client=s3,
        close_history_source=lambda syms: dict(series),
        fx_history_source=lambda ccys: {},
        **kw,
    )


def _consolidated(s3) -> dict:
    puts = {c.kwargs["Key"]: json.loads(c.kwargs["Body"]) for c in s3.put_object.call_args_list}
    return puts[KEY]


def test_a_short_session_is_published_named_and_logged_at_error(universe, caplog):
    """The 2026-07-21 shape: a fifth of the cross-section missing from one session."""
    s3 = _s3_without_previous()
    with caplog.at_level(logging.WARNING, logger=mmd.logger.name):
        result = _run(s3, _full(skip={(s, "2026-10-14") for s in SYMBOLS[:4]}))
    doc = _consolidated(s3)
    assert doc["coverage_by_date"]["2026-10-14"] == 16
    assert doc["incomplete_sessions"] == {
        "2026-10-14": {"tickers": 16, "trailing_median": 20, "floor": 19.0},
    }
    assert result["incomplete_sessions"] == ["2026-10-14"]
    # Recorded, not refused: the artifact is still published and the unit's status
    # is the shrink guard's to decide.
    assert result["status"] == "ok"
    assert any(
        r.levelno == logging.ERROR and "INCOMPLETE SESSIONS" in r.getMessage()
        and "2026-10-14=16/20" in r.getMessage()
        for r in caplog.records
    )


def test_a_whole_artifact_states_its_coverage_and_marks_nothing(universe, caplog):
    s3 = _s3_without_previous()
    with caplog.at_level(logging.WARNING, logger=mmd.logger.name):
        result = _run(s3, _full())
    doc = _consolidated(s3)
    assert doc["coverage_by_date"] == {d: 20 for d in SESSIONS}
    assert doc["incomplete_sessions"] == {}
    assert result["incomplete_sessions"] == [] and result["status"] == "ok"
    assert not any("INCOMPLETE SESSIONS" in r.getMessage() for r in caplog.records)


def test_dry_run_reports_incomplete_sessions_without_writing(universe):
    s3 = _s3_without_previous()
    result = _run(s3, _full(skip={(s, RUN_DATE) for s in SYMBOLS[:4]}), dry_run=True)
    assert result["incomplete_sessions"] == [RUN_DATE]
    s3.put_object.assert_not_called()
