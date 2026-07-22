"""Tests for the config#1981 1d/3d/15d universe_returns decay-ladder columns.

Mirrors ``test_universe_returns_60_90d.py``'s pattern for the W3.1 60d/90d
addition. These are the genuinely NEW intermediate-horizon columns added for
the alpha-decay-curve producer-side change (operator ruling "Option A",
2026-07-16, config#1981) — 5d/10d/21d/30d/60d/90d already existed; 10d was
already a raw universe_returns column but was never wired into the
long-format outcome store (see test_signal_returns_decay_curve_policy.py for
that half). 1d/3d/15d fill the gap so the outcome store can carry a genuine
multi-point decay curve rather than the two canonical (5d, 21d) endpoints.
"""
from __future__ import annotations

import math
import os
import sqlite3
import sys
import tempfile
from datetime import date
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.universe_returns import _build_rows_for_date, _ensure_table, _insert_rows

_DECAY_LADDER_COLS = [
    "return_1d", "return_3d", "return_15d",
    "spy_return_1d", "spy_return_3d", "spy_return_15d",
    "beat_spy_1d", "beat_spy_3d", "beat_spy_15d",
    "log_return_1d", "log_return_3d", "log_return_15d",
    "log_spy_return_1d", "log_spy_return_3d", "log_spy_return_15d",
]


def _db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    _ensure_table(path)
    return path


def test_schema_has_decay_ladder_columns():
    path = _db()
    try:
        conn = sqlite3.connect(path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
        conn.close()
        for c in _DECAY_LADDER_COLS:
            assert c in cols, f"{c} missing from universe_returns schema"
    finally:
        os.remove(path)


def test_migration_is_idempotent_on_preexisting_db():
    # _ensure_table must ALTER TABLE without error on a DB that already has
    # the decay-ladder columns (a second call, exactly like the producer's
    # own belt-and-suspenders re-invocation pattern).
    path = _db()
    try:
        _ensure_table(path)  # second call — must not raise
        conn = sqlite3.connect(path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
        conn.close()
        for c in _DECAY_LADDER_COLS:
            assert c in cols
    finally:
        os.remove(path)


def test_decay_ladder_round_trip():
    path = _db()
    try:
        row = {
            "ticker": "AAPL", "eval_date": "2024-01-02", "sector": "Tech",
            "close_price": 100.0,
            "return_5d": 0.01, "return_10d": 0.02, "return_21d": 0.03, "return_30d": 0.04,
            "spy_return_5d": 0.005, "spy_return_10d": 0.01, "spy_return_21d": 0.015, "spy_return_30d": 0.02,
            "beat_spy_5d": 1, "beat_spy_10d": 1, "beat_spy_21d": 1, "beat_spy_30d": 1,
            "log_return_21d": 0.0295, "log_spy_return_21d": 0.0149,
            "sector_etf": "XLK", "sector_etf_return_5d": 0.006, "beat_sector_5d": 1,
            "return_1d": 0.002, "return_3d": 0.006, "return_15d": 0.025,
            "spy_return_1d": 0.001, "spy_return_3d": 0.003, "spy_return_15d": 0.012,
            "beat_spy_1d": 1, "beat_spy_3d": 1, "beat_spy_15d": 1,
            "log_return_1d": 0.002, "log_return_3d": 0.006, "log_return_15d": 0.0247,
            "log_spy_return_1d": 0.001, "log_spy_return_3d": 0.003, "log_spy_return_15d": 0.0119,
        }
        assert _insert_rows(path, [row]) == 1
        conn = sqlite3.connect(path)
        got = conn.execute(
            "SELECT return_1d, return_3d, return_15d, beat_spy_15d, log_return_15d, log_spy_return_1d "
            "FROM universe_returns WHERE ticker='AAPL'"
        ).fetchone()
        conn.close()
        assert got == (0.002, 0.006, 0.025, 1, 0.0247, 0.001)
    finally:
        os.remove(path)


def test_partial_row_without_decay_ladder_still_inserts():
    # Older callers (or a partial-window row where 1d/3d/15d haven't closed
    # yet) that don't set the new keys must still insert (None-filled) — the
    # same graceful-partial contract the 60d/90d addition established.
    path = _db()
    try:
        row = {
            "ticker": "MSFT", "eval_date": "2024-01-02", "sector": "Tech",
            "close_price": 200.0,
            "return_5d": 0.01, "return_10d": None, "return_21d": None, "return_30d": None,
            "spy_return_5d": 0.005, "spy_return_10d": None, "spy_return_21d": None, "spy_return_30d": None,
            "beat_spy_5d": 1, "beat_spy_10d": None, "beat_spy_21d": None, "beat_spy_30d": None,
            "log_return_21d": None, "log_spy_return_21d": None,
            "sector_etf": "XLK", "sector_etf_return_5d": None, "beat_sector_5d": None,
        }
        assert _insert_rows(path, [row]) == 1
        conn = sqlite3.connect(path)
        got = conn.execute(
            "SELECT return_1d, return_3d, return_15d FROM universe_returns WHERE ticker='MSFT'"
        ).fetchone()
        conn.close()
        assert got == (None, None, None)
    finally:
        os.remove(path)


# -- Row build: 1d/3d/15d gating + polygon coverage ---------------------------


def _fake_polygon(prices_by_date: dict[str, dict[str, dict[str, float]]]) -> MagicMock:
    """Return a polygon-shaped client with .get_grouped_daily(date_str) -> bars."""
    client = MagicMock()
    client.get_grouped_daily.side_effect = lambda d: prices_by_date.get(d, {})
    return client


class TestRowBuildDecayLadder:
    def test_decay_ladder_computed_when_windows_closed(self):
        """1d/3d/15d returns + log returns are populated once each forward
        window has closed, using the same _pct_return/_log_return helpers as
        the pre-existing horizons."""
        prices = {
            "2026-03-02": {"AAPL": {"close": 100.0}, "SPY": {"close": 400.0}},
            "2026-03-03": {"AAPL": {"close": 100.5}, "SPY": {"close": 400.4}},  # +1d
            "2026-03-05": {"AAPL": {"close": 101.0}, "SPY": {"close": 400.8}},  # +3d
            "2026-03-09": {"AAPL": {"close": 101.5}, "SPY": {"close": 401.2}},  # +5d
            "2026-03-16": {"AAPL": {"close": 102.0}, "SPY": {"close": 401.6}},  # +10d
            "2026-03-23": {"AAPL": {"close": 103.0}, "SPY": {"close": 402.0}},  # +15d
            "2026-03-31": {"AAPL": {"close": 104.0}, "SPY": {"close": 408.0}},  # +21d
            "2026-04-14": {"AAPL": {"close": 105.0}, "SPY": {"close": 410.0}},  # +30d
        }

        import collectors.universe_returns as ur

        try:
            class _StubDate(date):
                @classmethod
                def today(cls):
                    return cls(2026, 5, 9)
            ur.date = _StubDate
            rows = _build_rows_for_date("2026-03-02", _fake_polygon(prices), sector_map=None)
        finally:
            ur.date = date

        aapl = next(r for r in rows if r["ticker"] == "AAPL")
        assert aapl["return_1d"] == pytest.approx(0.005, abs=1e-4)
        assert aapl["return_3d"] == pytest.approx(0.01, abs=1e-4)
        assert aapl["return_15d"] == pytest.approx(0.03, abs=1e-4)
        assert aapl["log_return_1d"] == pytest.approx(math.log(1.005), abs=1e-5)
        assert aapl["log_return_3d"] == pytest.approx(math.log(1.01), abs=1e-5)
        assert aapl["log_return_15d"] == pytest.approx(math.log(1.03), abs=1e-5)
        assert aapl["spy_return_1d"] is not None
        assert aapl["spy_return_3d"] is not None
        assert aapl["spy_return_15d"] is not None
        assert aapl["beat_spy_1d"] in (0, 1)
        assert aapl["beat_spy_3d"] in (0, 1)
        assert aapl["beat_spy_15d"] in (0, 1)

    def test_decay_ladder_gated_to_null_when_window_unclosed(self):
        """An eval_date whose 5d window has closed (so the row is built) but
        whose 15d forward window has NOT closed yet yields NULL for 15d while
        1d/3d/5d (already closed) are populated — same graceful-partial
        contract as the 21d/60d/90d additions."""
        prices = {
            "2026-05-01": {"AAPL": {"close": 100.0}, "SPY": {"close": 400.0}},
            "2026-05-04": {"AAPL": {"close": 100.2}, "SPY": {"close": 400.1}},  # +1d
            "2026-05-06": {"AAPL": {"close": 100.5}, "SPY": {"close": 400.3}},  # +3d
            "2026-05-08": {"AAPL": {"close": 100.8}, "SPY": {"close": 400.5}},  # +5d
        }

        import collectors.universe_returns as ur

        try:
            class _StubDate(date):
                @classmethod
                def today(cls):
                    return cls(2026, 5, 9)
            ur.date = _StubDate
            rows = _build_rows_for_date("2026-05-01", _fake_polygon(prices), sector_map=None)
        finally:
            ur.date = date

        aapl = next(r for r in rows if r["ticker"] == "AAPL")
        assert aapl["return_1d"] is not None  # +1d window has closed
        assert aapl["return_3d"] is not None  # +3d window has closed
        assert aapl["return_5d"] is not None  # +5d window has closed
        assert aapl["return_15d"] is None      # +15d forward window not yet closed
        assert aapl["log_return_15d"] is None
        assert aapl["spy_return_15d"] is None

    def test_polygon_called_for_t_plus_1_3_15d(self):
        """The collector must fetch grouped-daily for the t+1d/+3d/+15d trading days."""
        prices = {
            "2026-03-02": {"AAPL": {"close": 100.0}, "SPY": {"close": 400.0}},
            "2026-03-03": {"AAPL": {"close": 100.5}, "SPY": {"close": 400.4}},
            "2026-03-05": {"AAPL": {"close": 101.0}, "SPY": {"close": 400.8}},
            "2026-03-09": {"AAPL": {"close": 101.5}, "SPY": {"close": 401.2}},
            "2026-03-23": {"AAPL": {"close": 103.0}, "SPY": {"close": 402.0}},
        }
        client = _fake_polygon(prices)

        import collectors.universe_returns as ur

        try:
            class _StubDate(date):
                @classmethod
                def today(cls):
                    return cls(2026, 5, 9)
            ur.date = _StubDate
            _build_rows_for_date("2026-03-02", client, sector_map=None)
        finally:
            ur.date = date

        called = {c.args[0] for c in client.get_grouped_daily.call_args_list}
        assert "2026-03-03" in called, f"missing t+1d call; called={called}"
        assert "2026-03-05" in called, f"missing t+3d call; called={called}"
        assert "2026-03-23" in called, f"missing t+15d call; called={called}"
