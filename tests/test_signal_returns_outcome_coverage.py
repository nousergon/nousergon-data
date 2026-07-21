"""signal_returns outcome-store coverage assertion (config#1860).

Covers ``_check_outcome_store_coverage``:
  - WARNs with counts when a resolved-age score_performance signal date has
    ZERO score_performance_outcomes rows post-run (the exact shape of the
    2026-04-04/04-11/04-12 gap discovered only via months-later forensics
    join in config#1860)
  - does NOT flag a recent date whose primary-horizon forward window hasn't
    closed yet (that's expected lag, not a coverage gap)
  - does NOT flag a date that DOES have outcome rows
  - best-effort: a DB read failure is caught, logged, and returned as
    status:skipped rather than raised (mirrors _emit_context_coverage_metric)
  - deterministic via an injectable "today" so the grace-window boundary is
    testable without wall-clock flakiness
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest import mock

import pytest

from collectors.signal_returns import (
    _backfill_outcome_records,
    _check_outcome_store_coverage,
)


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        yield str(Path(d) / "research.db")


def _seed_score_performance_only(db: str, dates: list[str], symbol: str = "AAPL"):
    """Seed ONLY score_performance rows (no universe_returns / outcomes) —
    simulates a signal date that never made it into the outcome store."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS score_performance (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, score_date TEXT NOT NULL,
                score REAL NOT NULL, price_on_date REAL, UNIQUE(symbol, score_date)
            )
            """
        )
        for d in dates:
            conn.execute(
                "INSERT OR IGNORE INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES (?, ?, ?, ?)",
                (symbol, d, 80.0, 100.0),
            )
        conn.commit()


def _seed_resolved(db: str, score_date: str, symbol: str = "MSFT"):
    """Seed a fully-resolved signal (score_performance + universe_returns)
    and run the real backfill so score_performance_outcomes gets a row."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS score_performance (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, score_date TEXT NOT NULL,
                score REAL NOT NULL, price_on_date REAL, UNIQUE(symbol, score_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_returns (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, eval_date TEXT,
                return_21d REAL, spy_return_21d REAL, beat_spy_21d INTEGER,
                return_5d REAL, spy_return_5d REAL, beat_spy_5d INTEGER,
                log_return_21d REAL, log_spy_return_21d REAL,
                UNIQUE(ticker, eval_date)
            )
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO score_performance (symbol, score_date, score, price_on_date) "
            "VALUES (?, ?, ?, ?)",
            (symbol, score_date, 80.0, 100.0),
        )
        conn.execute(
            "INSERT OR IGNORE INTO universe_returns (ticker, eval_date, return_21d, spy_return_21d, "
            "beat_spy_21d, return_5d, spy_return_5d, beat_spy_5d, log_return_21d, log_spy_return_21d) "
            "VALUES (?, ?, 0.05, 0.02, 1, 0.01, 0.005, 1, 0.049, 0.0198)",
            (symbol, score_date),
        )
        conn.commit()
    _backfill_outcome_records(db, dry_run=False, resolved_at="2026-07-03T16:43:47Z")


# A "today" comfortably past every 21-trading-day window used below.
_TODAY = date(2026, 7, 21)


class TestCoverageGapDetection:
    def test_flags_resolved_age_date_with_zero_outcome_rows(self, tmp_db):
        # Shape of the real config#1860 gap: score_performance has rows for
        # an old date, but score_performance_outcomes has NONE for it.
        _seed_score_performance_only(tmp_db, ["2026-04-04"])
        with mock.patch("collectors.signal_returns.date") as mock_date:
            mock_date.today.return_value = _TODAY
            mock_date.fromisoformat = date.fromisoformat
            summary = _check_outcome_store_coverage(tmp_db)
        assert summary["status"] == "ok"
        assert summary["gap_dates"] == ["2026-04-04"]
        assert summary["gap_counts"] == {"2026-04-04": 1}

    def test_multiple_gap_dates_all_reported(self, tmp_db):
        _seed_score_performance_only(tmp_db, ["2026-04-04", "2026-04-11", "2026-04-12"])
        with mock.patch("collectors.signal_returns.date") as mock_date:
            mock_date.today.return_value = _TODAY
            mock_date.fromisoformat = date.fromisoformat
            summary = _check_outcome_store_coverage(tmp_db)
        assert summary["gap_dates"] == ["2026-04-04", "2026-04-11", "2026-04-12"]
        assert summary["signal_dates_checked"] == 3

    def test_resolved_date_with_outcome_rows_not_flagged(self, tmp_db):
        _seed_resolved(tmp_db, "2026-03-02")
        with mock.patch("collectors.signal_returns.date") as mock_date:
            mock_date.today.return_value = _TODAY
            mock_date.fromisoformat = date.fromisoformat
            summary = _check_outcome_store_coverage(tmp_db)
        assert summary["gap_dates"] == []
        assert summary["signal_dates_checked"] == 1

    def test_recent_date_within_grace_window_not_flagged(self, tmp_db):
        # A signal date whose 21-trading-day primary window has NOT closed
        # yet is expected to have no outcome rows — not a coverage gap.
        recent = "2026-07-20"
        _seed_score_performance_only(tmp_db, [recent])
        with mock.patch("collectors.signal_returns.date") as mock_date:
            mock_date.today.return_value = _TODAY
            mock_date.fromisoformat = date.fromisoformat
            summary = _check_outcome_store_coverage(tmp_db)
        assert summary["gap_dates"] == []
        assert summary["signal_dates_checked"] == 0

    def test_mixed_gap_and_resolved_dates(self, tmp_db):
        _seed_resolved(tmp_db, "2026-03-02", symbol="MSFT")
        _seed_score_performance_only(tmp_db, ["2026-04-04"], symbol="AAPL")
        with mock.patch("collectors.signal_returns.date") as mock_date:
            mock_date.today.return_value = _TODAY
            mock_date.fromisoformat = date.fromisoformat
            summary = _check_outcome_store_coverage(tmp_db)
        assert summary["gap_dates"] == ["2026-04-04"]
        assert summary["signal_dates_checked"] == 2

    def test_no_score_performance_rows_at_all(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(
                "CREATE TABLE score_performance (id INTEGER PRIMARY KEY, symbol TEXT, "
                "score_date TEXT, score REAL, price_on_date REAL)"
            )
            conn.commit()
        summary = _check_outcome_store_coverage(tmp_db)
        assert summary["status"] == "ok"
        assert summary["gap_dates"] == []
        assert summary["signal_dates_checked"] == 0

    def test_db_read_failure_is_best_effort_not_raised(self, tmp_db):
        # No such file / unreadable DB -> caught, not raised.
        summary = _check_outcome_store_coverage("/nonexistent/path/research.db")
        assert summary["status"] == "skipped"
        assert "error" in summary

    def test_gap_count_reflects_multiple_symbols_same_date(self, tmp_db):
        _seed_score_performance_only(tmp_db, ["2026-04-04"], symbol="AAPL")
        _seed_score_performance_only(tmp_db, ["2026-04-04"], symbol="MSFT")
        with mock.patch("collectors.signal_returns.date") as mock_date:
            mock_date.today.return_value = _TODAY
            mock_date.fromisoformat = date.fromisoformat
            summary = _check_outcome_store_coverage(tmp_db)
        assert summary["gap_counts"]["2026-04-04"] == 2
