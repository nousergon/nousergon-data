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

A test of this defect has to move the wall clock while holding ``run_date``
fixed and assert on the CONTENT, not the ``as_of``-style stamp — mirrors
``tests/test_replay_anchors_to_run_date.py``'s reasoning for the sibling fix
in ``metron_market_data.py``.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from collectors import universe_returns as ur


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

def test_collect_anchors_backfill_window_on_run_date_not_wall_clock(monkeypatch):
    """Moving the wall clock while holding ``run_date`` fixed must not change
    the ``today`` that drives ``_trading_days_to_process`` and
    ``_get_existing_dates`` — proves ``collect``'s backfill window is
    anchored on ``run_date``, not real time (the fix-not-propagated-to-
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
