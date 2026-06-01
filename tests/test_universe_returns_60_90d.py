"""Tests for the W3.1 (L4469) 60d/90d universe_returns horizon columns."""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.universe_returns import _ensure_table, _insert_rows

_W3_1_COLS = [
    "return_60d", "return_90d", "spy_return_60d", "spy_return_90d",
    "beat_spy_60d", "beat_spy_90d",
    "log_return_60d", "log_return_90d", "log_spy_return_60d", "log_spy_return_90d",
]


def _db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    _ensure_table(path)
    return path


def test_schema_has_60_90d_columns():
    path = _db()
    try:
        conn = sqlite3.connect(path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
        conn.close()
        for c in _W3_1_COLS:
            assert c in cols, f"{c} missing from universe_returns schema"
    finally:
        os.remove(path)


def test_60_90d_round_trip():
    path = _db()
    try:
        row = {
            "ticker": "AAPL", "eval_date": "2024-01-02", "sector": "Tech",
            "close_price": 100.0,
            "return_5d": 0.01, "return_10d": 0.02, "return_21d": 0.03, "return_30d": 0.04,
            "spy_return_5d": 0.005, "spy_return_10d": 0.01, "spy_return_21d": 0.015, "spy_return_30d": 0.02,
            "beat_spy_5d": 1, "beat_spy_10d": 1, "beat_spy_21d": 1, "beat_spy_30d": 1,
            "log_return_21d": 0.0295, "log_spy_return_21d": 0.0149,
            "return_60d": 0.08, "return_90d": 0.12,
            "spy_return_60d": 0.05, "spy_return_90d": 0.07,
            "beat_spy_60d": 1, "beat_spy_90d": 1,
            "log_return_60d": 0.077, "log_return_90d": 0.1133,
            "log_spy_return_60d": 0.0488, "log_spy_return_90d": 0.0677,
            "sector_etf": "XLK", "sector_etf_return_5d": 0.006, "beat_sector_5d": 1,
        }
        assert _insert_rows(path, [row]) == 1
        conn = sqlite3.connect(path)
        got = conn.execute(
            "SELECT return_60d, return_90d, beat_spy_60d, log_return_90d, log_spy_return_60d "
            "FROM universe_returns WHERE ticker='AAPL'"
        ).fetchone()
        conn.close()
        assert got == (0.08, 0.12, 1, 0.1133, 0.0488)
    finally:
        os.remove(path)


def test_partial_row_without_60_90d_still_inserts():
    # Older callers that don't set the new keys must still insert (None-filled).
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
            "SELECT return_60d, log_spy_return_90d FROM universe_returns WHERE ticker='MSFT'"
        ).fetchone()
        conn.close()
        assert got == (None, None)
    finally:
        os.remove(path)
