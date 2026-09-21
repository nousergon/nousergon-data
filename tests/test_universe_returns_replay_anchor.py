"""alpha-engine-config-I11216 deliverable 5: ``collectors/universe_returns.py``
selected CONTENT with the wall clock in two places — the same defect class
fixed in ``metron_market_data.py`` (6a7cfae9):

  * ``collect``'s ``today = date.today()`` drove the whole backfill window
    (``_trading_days_to_process``'s lookback walk + ``_get_existing_dates``'s
    completeness gate) even though ``collect`` already accepted ``run_date``
    — before this fix, ``run_date`` reached only the S3 upload stamp, never
    the date window that decides which rows get computed.
  * ``_build_rows_for_date``'s ``today = date.today()`` independently decided
    which forward windows had "closed" (and were therefore fetchable),
    regardless of what date ``collect`` was asked to replay.

**Correction (alpha-engine-config weekly-sf-first-pass-register-260921 §2.2):**
the first cut of this fix anchored ``collect``'s ``today`` on ``run_date``
UNCONDITIONALLY. ``run_date`` is populated on the LIVE weekly path too
(``weekly_collector.py``'s ``args.date or default_run_date()`` always
resolves to a real date, live or replay), so that anchor made a live
Saturday run believe "today" was Friday — and ``_build_rows_for_date``'s
forward-window-closed gate (``fwd_Nd >= today``) would then treat a forward
date landing exactly on Friday as still open, silently skipping an
otherwise-computable row. The fix now keys off ``shadow.root.active_root()``
— the in-process signal ``python -m shadow run`` sets before a single
collector line runs — not the mere presence of ``run_date``.

A test of this defect has to move the wall clock while holding ``run_date``
fixed and assert on the CONTENT, not the ``as_of``-style stamp — mirrors
``tests/test_replay_anchors_to_run_date.py``'s reasoning for the sibling fix
in ``metron_market_data.py``.
"""

from __future__ import annotations

import datetime as dt
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from collectors import universe_returns as ur
from shadow.root import ShadowRoot, activate, deactivate


@pytest.fixture
def declared_replay():
    """Activates a shadow root for the duration of the test."""
    root = ShadowRoot(dt.date(2026, 9, 18))
    activate(root)
    try:
        yield root
    finally:
        deactivate()


class _FixedToday(date):
    """``date`` subclass whose ``.today()`` returns a controllable fixed
    instant — lets a test move "the wall clock" without touching real time."""

    _fixed: date = date(2000, 1, 1)

    @classmethod
    def today(cls):
        return cls._fixed


# ── _build_rows_for_date: the forward-window-closed gate ────────────────

def test_build_rows_for_date_uses_today_param_not_wall_clock(monkeypatch):
    """Moving the wall clock while holding the explicit ``today`` param fixed
    must not change which forward windows are treated as closed."""
    monkeypatch.setattr(ur, "date", _FixedToday)
    client = MagicMock()
    # eval_date is old enough that every forward window (up to 90d) has
    # closed as of the explicit `today` — the real wall clock must play no
    # role once `today` is passed explicitly.
    fixed_today = date(2026, 9, 18)

    with patch.object(ur, "_grouped_daily_or_empty", return_value={}) as mock_prices:
        _FixedToday._fixed = fixed_today
        ur._build_rows_for_date("2026-03-02", client, sector_map=None, today=fixed_today)
        calls_a = [c.kwargs["today"] for c in mock_prices.call_args_list]

        mock_prices.reset_mock()
        _FixedToday._fixed = date(2026, 12, 25)  # wall clock moved 3+ months
        ur._build_rows_for_date("2026-03-02", client, sector_map=None, today=fixed_today)
        calls_b = [c.kwargs["today"] for c in mock_prices.call_args_list]

    assert calls_a == calls_b == [fixed_today] * len(calls_a)


def test_build_rows_for_date_defaults_to_wall_clock_when_today_omitted(monkeypatch):
    """``today=None`` (the ``prices.py:258`` fallback shape) still reads the
    real wall clock — only a caller with no date of its own hits this path;
    ``collect`` never does (see below)."""
    monkeypatch.setattr(ur, "date", _FixedToday)
    _FixedToday._fixed = date(2026, 9, 18)
    client = MagicMock()

    with patch.object(ur, "_grouped_daily_or_empty", return_value={}) as mock_prices:
        ur._build_rows_for_date("2026-03-02", client, sector_map=None)

    assert mock_prices.call_args.kwargs["today"] == date(2026, 9, 18)


# ── collect(): the backfill window + completeness gate ──────────────────

def test_collect_anchors_backfill_window_on_run_date_under_a_declared_replay(monkeypatch, declared_replay):
    """Moving the wall clock while holding ``run_date`` fixed, UNDER A
    DECLARED REPLAY, must not change the ``today`` that drives
    ``_trading_days_to_process`` and ``_get_existing_dates`` — proves
    ``collect``'s backfill window is anchored on ``run_date`` when
    ``active_root()`` says this is a replay (the fix-not-propagated-to-
    analogous-sites class this issue names: ``run_date`` already existed on
    this signature but, before this fix, reached only the S3 upload stamp).
    """
    monkeypatch.setattr(ur, "date", _FixedToday)
    seen_today: list[date] = []

    def _fake_get_existing(db_path, today=None):
        seen_today.append(today)
        return set()

    def _fake_trading_days(today, max_lookback, existing):
        seen_today.append(today)
        return []  # nothing to process — keeps the test to the anchor only

    with patch("polygon_client.polygon_client", return_value=MagicMock()), \
         patch.object(ur, "_load_sector_map", return_value={}), \
         patch.object(ur, "_ensure_table", MagicMock()), \
         patch.object(ur, "_get_existing_dates", side_effect=_fake_get_existing), \
         patch.object(ur, "_trading_days_to_process", side_effect=_fake_trading_days), \
         patch("boto3.client", return_value=MagicMock()):
        _FixedToday._fixed = date(2026, 9, 18)
        ur.collect("bucket", "db.sqlite", run_date="2026-09-18")
        seen_a = list(seen_today)

        seen_today.clear()
        _FixedToday._fixed = date(2026, 11, 1)  # wall clock moved 6+ weeks
        ur.collect("bucket", "db.sqlite", run_date="2026-09-18")
        seen_b = list(seen_today)

    assert seen_a == seen_b == [date(2026, 9, 18), date(2026, 9, 18)]


def test_collect_uses_wall_clock_on_a_live_run_no_declared_replay(monkeypatch):
    """Outside a declared replay — the live/scheduled path — ``collect``
    must anchor ``today`` on the REAL wall clock, not ``run_date``, even
    though ``run_date`` is populated (``weekly_collector.py`` always passes
    ``args.date or default_run_date()``). A Saturday run's ``run_date`` is
    Friday (``dates.default_run_date()`` resolves to the last closed
    session); anchoring ``today`` on Friday would make
    ``_build_rows_for_date``'s ``fwd_Nd >= today`` gate treat a forward date
    landing exactly on Friday as still open, silently dropping an
    otherwise-computable row on the real Saturday run.
    """
    assert ur.active_root() is None, "no shadow root should be active in this test"

    monkeypatch.setattr(ur, "date", _FixedToday)
    seen_today: list[date] = []

    def _fake_get_existing(db_path, today=None):
        seen_today.append(today)
        return set()

    def _fake_trading_days(today, max_lookback, existing):
        seen_today.append(today)
        return []

    with patch("polygon_client.polygon_client", return_value=MagicMock()), \
         patch.object(ur, "_load_sector_map", return_value={}), \
         patch.object(ur, "_ensure_table", MagicMock()), \
         patch.object(ur, "_get_existing_dates", side_effect=_fake_get_existing), \
         patch.object(ur, "_trading_days_to_process", side_effect=_fake_trading_days), \
         patch("boto3.client", return_value=MagicMock()):
        _FixedToday._fixed = date(2026, 9, 19)  # the real Saturday wall clock
        ur.collect("bucket", "db.sqlite", run_date="2026-09-18")  # run_date = Friday
        seen = list(seen_today)

    assert seen == [date(2026, 9, 19), date(2026, 9, 19)], (
        "a live run must anchor `today` on the real wall clock, not run_date"
    )
