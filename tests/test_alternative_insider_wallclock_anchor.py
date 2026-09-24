"""alpha-engine-config-I11308 — ``collectors/alternative.py::_fetch_insider``'s
90-day Form 4 lookback is CONTENT and takes the same anchor as ``_fetch_news``:
``run_date`` under a declared replay (``shadow.root.active_root()``), the real
UTC calendar date on a live run. ``run_date`` is populated on both paths
(``weekly_collector.py``'s ``args.date or default_run_date()``), so a live
Saturday run used to anchor its window on Friday's session.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from collectors import alternative
from shadow.root import ShadowRoot, activate, deactivate


@pytest.fixture
def declared_replay():
    activate(ShadowRoot(dt.date(2026, 9, 18)))
    try:
        yield
    finally:
        deactivate()


class _FixedNow(datetime):
    _fixed: datetime = datetime(2000, 1, 1, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls._fixed if tz is None else cls._fixed.astimezone(tz)


def _resp(payload):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = payload
    return r


# Form 4 filings on these dates; the 90-day window decides which survive.
_FILING_DATES = ["2026-09-19", "2026-09-18", "2026-06-20", "2026-06-19"]


def _fake_get(url, **_):
    if url.endswith("company_tickers.json"):
        return _resp({"0": {"ticker": "AAPL", "cik_str": 320193}})
    return _resp({"filings": {"recent": {
        "form": ["4"] * len(_FILING_DATES),
        "filingDate": list(_FILING_DATES),
    }}})


def _run(monkeypatch, wall_clock: datetime, run_date: str = "2026-09-18") -> dict:
    _FixedNow._fixed = wall_clock
    monkeypatch.setattr(alternative, "datetime", _FixedNow)
    monkeypatch.setattr(alternative, "get_secret", lambda *a, **k: "test@example.com")
    monkeypatch.setattr(alternative, "_SEC_RATE_DELAY", 0)
    with patch.object(alternative.requests, "get", side_effect=_fake_get):
        return alternative._fetch_insider("AAPL", run_date)


def test_live_run_anchors_on_the_wall_clock_date(monkeypatch):
    """No replay declared: a Saturday 2026-09-19 run with run_date=Friday
    anchors on 2026-09-19, so the window starts 2026-06-21 and the
    2026-06-20 filing falls out. Anchored on run_date it would have stayed."""
    out = _run(monkeypatch, datetime(2026, 9, 19, 10, 48, tzinfo=timezone.utc))
    assert [t["date"] for t in out["transactions"]] == ["2026-09-19", "2026-09-18"]
    assert [t["days_ago"] for t in out["transactions"]] == [0, 1]


def test_live_run_ignores_run_date(monkeypatch):
    wall = datetime(2026, 9, 19, 10, 48, tzinfo=timezone.utc)
    assert _run(monkeypatch, wall, run_date="2026-09-18") == _run(
        monkeypatch, wall, run_date="2026-01-02"
    )


def test_declared_replay_anchors_on_run_date(monkeypatch, declared_replay):
    """Under a replay the wall clock must not move the window: run_date
    2026-09-18 keeps the window at [2026-06-20, 2026-09-18]."""
    a = _run(monkeypatch, datetime(2026, 9, 19, 10, 48, tzinfo=timezone.utc))
    b = _run(monkeypatch, datetime(2026, 12, 25, 3, 0, tzinfo=timezone.utc))
    assert a == b
    dates = [t["date"] for t in a["transactions"]]
    assert "2026-06-20" in dates and "2026-06-19" not in dates
    assert next(t for t in a["transactions"] if t["date"] == "2026-09-18")["days_ago"] == 0
