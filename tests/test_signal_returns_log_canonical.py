"""signal_returns horizon-driven log-domain backfill.

Covers the PR C rewrite of `_backfill_predictor_returns`:
  - reads `log_return_{N}d` / `log_spy_return_{N}d` from universe_returns
  - writes horizon-agnostic columns (actual_log_alpha, horizon_days, correct)
  - parameterizes by `forward_days` (default 21d)
  - guards missing universe_returns columns
  - re-resolves rows with stale `actual_log_alpha=NULL` even when legacy
    actual_5d_return is set
"""
from __future__ import annotations

import math
import sqlite3
import tempfile
from pathlib import Path

import pytest

from collectors.signal_returns import (
    _DEFAULT_FORWARD_DAYS,
    _backfill_predictor_returns,
    _ensure_predictor_outcomes_schema,
)


def _seed_universe_returns(db: str, ticker: str, eval_date: str, *, log_stock: float, log_spy: float):
    """Seed a universe_returns row with the 21d log columns populated."""
    with sqlite3.connect(db) as conn:
        # Emulate post-PR-A schema (subset; only the columns we touch)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS universe_returns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                eval_date TEXT NOT NULL,
                return_21d REAL,
                spy_return_21d REAL,
                log_return_21d REAL,
                log_spy_return_21d REAL,
                UNIQUE(ticker, eval_date)
            )
        """)
        conn.execute(
            "INSERT INTO universe_returns "
            "(ticker, eval_date, return_21d, spy_return_21d, log_return_21d, log_spy_return_21d) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                ticker, eval_date,
                math.exp(log_stock) - 1.0,
                math.exp(log_spy) - 1.0,
                log_stock, log_spy,
            ),
        )
        conn.commit()


def _seed_predictor_outcomes(db: str, ticker: str, pred_date: str, direction: str, *, legacy_5d: float | None = None):
    """Seed a predictor_outcomes row pre-backfill."""
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS predictor_outcomes (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                prediction_date TEXT NOT NULL,
                predicted_direction TEXT,
                prediction_confidence REAL,
                p_up REAL, p_flat REAL, p_down REAL,
                score_modifier_applied REAL DEFAULT 0.0,
                actual_5d_return REAL,
                correct_5d INTEGER,
                UNIQUE(symbol, prediction_date)
            )
        """)
        conn.execute(
            "INSERT INTO predictor_outcomes "
            "(symbol, prediction_date, predicted_direction, actual_5d_return) "
            "VALUES (?, ?, ?, ?)",
            (ticker, pred_date, direction, legacy_5d),
        )
        conn.commit()


# -- Default-horizon constant -------------------------------------------------


def test_default_horizon_is_21d():
    """Production canonical horizon per the predictor's Track A cutover."""
    assert _DEFAULT_FORWARD_DAYS == 21


# -- Schema bridge ------------------------------------------------------------


class TestEnsurePredictorOutcomesSchema:
    def test_adds_missing_columns(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP")
            with sqlite3.connect(db) as conn:
                _ensure_predictor_outcomes_schema(conn)
                cols = {r[1] for r in conn.execute(
                    "PRAGMA table_info(predictor_outcomes)"
                ).fetchall()}
            for col in ("actual_log_alpha", "horizon_days", "correct"):
                assert col in cols

    def test_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP")
            with sqlite3.connect(db) as conn:
                _ensure_predictor_outcomes_schema(conn)
                _ensure_predictor_outcomes_schema(conn)  # no-op


# -- Backfill: writes horizon-agnostic columns --------------------------------


class TestBackfillWritesHorizonAgnosticColumns:
    def test_up_direction_correct_when_log_alpha_positive(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP")
            # Stock log return 0.05, SPY log return 0.01 → log_alpha = 0.04 (UP correct)
            _seed_universe_returns(db, "AAPL", "2026-03-02", log_stock=0.05, log_spy=0.01)

            result = _backfill_predictor_returns(db, dry_run=False, forward_days=21)
            assert result["status"] == "ok"
            assert result["rows_written"] == 1

            with sqlite3.connect(db) as conn:
                row = conn.execute(
                    "SELECT actual_log_alpha, horizon_days, correct, actual_5d_return, correct_5d "
                    "FROM predictor_outcomes WHERE symbol='AAPL' AND prediction_date='2026-03-02'"
                ).fetchone()
            actual_log_alpha, horizon_days, correct, legacy_alpha, legacy_correct = row
            assert actual_log_alpha == pytest.approx(0.04, abs=1e-5)
            assert horizon_days == 21
            assert correct == 1
            # Legacy columns NOT touched (pre-existing NULL stays NULL)
            assert legacy_alpha is None
            assert legacy_correct is None

    def test_down_direction_correct_when_log_alpha_negative(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "DOWN")
            _seed_universe_returns(db, "AAPL", "2026-03-02", log_stock=-0.03, log_spy=0.01)

            _backfill_predictor_returns(db, dry_run=False, forward_days=21)

            with sqlite3.connect(db) as conn:
                row = conn.execute(
                    "SELECT actual_log_alpha, correct FROM predictor_outcomes "
                    "WHERE symbol='AAPL' AND prediction_date='2026-03-02'"
                ).fetchone()
            assert row[0] == pytest.approx(-0.04, abs=1e-5)
            assert row[1] == 1  # DOWN + log_alpha < 0 → correct

    def test_up_direction_incorrect_when_log_alpha_negative(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP")
            _seed_universe_returns(db, "AAPL", "2026-03-02", log_stock=-0.02, log_spy=0.01)

            _backfill_predictor_returns(db, dry_run=False, forward_days=21)

            with sqlite3.connect(db) as conn:
                row = conn.execute(
                    "SELECT correct FROM predictor_outcomes "
                    "WHERE symbol='AAPL' AND prediction_date='2026-03-02'"
                ).fetchone()
            assert row[0] == 0


# -- Re-resolve rows already populated under legacy 5d-only path -------------


class TestReResolveLegacyRows:
    def test_legacy_actual_5d_set_does_not_block_new_resolution(self):
        """Pre-PR-C rows have actual_5d_return populated but actual_log_alpha NULL.
        The new pending-row filter is `actual_log_alpha IS NULL` so they get
        re-resolved at the canonical horizon. Backtester analytics COALESCE
        consumers see the new value preferred over the legacy one."""
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP", legacy_5d=2.5)
            _seed_universe_returns(db, "AAPL", "2026-03-02", log_stock=0.04, log_spy=0.01)

            result = _backfill_predictor_returns(db, dry_run=False, forward_days=21)
            assert result["rows_written"] == 1

            with sqlite3.connect(db) as conn:
                row = conn.execute(
                    "SELECT actual_log_alpha, horizon_days, actual_5d_return "
                    "FROM predictor_outcomes WHERE symbol='AAPL' AND prediction_date='2026-03-02'"
                ).fetchone()
            assert row[0] == pytest.approx(0.03, abs=1e-5)
            assert row[1] == 21
            # Legacy column preserved untouched at its pre-PR-C value
            assert row[2] == pytest.approx(2.5)


# -- Schema-presence guard ----------------------------------------------------


class TestSchemaPresenceGuard:
    def test_missing_log_columns_returns_error(self):
        """Pre-PR-A databases lack log_return_21d. Backfill must fail loudly."""
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            with sqlite3.connect(db) as conn:
                # Pre-PR-A universe_returns: arithmetic only, no log columns
                conn.execute("""
                    CREATE TABLE universe_returns (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ticker TEXT, eval_date TEXT,
                        return_5d REAL, spy_return_5d REAL,
                        UNIQUE(ticker, eval_date)
                    )
                """)
                conn.commit()
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP")

            result = _backfill_predictor_returns(db, dry_run=False, forward_days=21)
            assert result["status"] == "error"
            assert "log_return_21d" in result["error"]
            assert result["rows_written"] == 0


# -- Dry-run behavior ---------------------------------------------------------


class TestDryRun:
    def test_dry_run_does_not_write(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP")
            _seed_universe_returns(db, "AAPL", "2026-03-02", log_stock=0.04, log_spy=0.01)

            result = _backfill_predictor_returns(db, dry_run=True, forward_days=21)
            assert result["status"] == "ok"
            assert result["rows_written"] == 1

            with sqlite3.connect(db) as conn:
                row = conn.execute(
                    "SELECT actual_log_alpha FROM predictor_outcomes "
                    "WHERE symbol='AAPL' AND prediction_date='2026-03-02'"
                ).fetchone()
            assert row[0] is None


# -- Forward-window-unclosed: skip --------------------------------------------


class TestForwardWindowUnclosedSkipped:
    def test_null_log_return_skips_row(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _seed_predictor_outcomes(db, "AAPL", "2026-03-02", "UP")
            with sqlite3.connect(db) as conn:
                conn.execute("""
                    CREATE TABLE universe_returns (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ticker TEXT, eval_date TEXT,
                        return_21d REAL, spy_return_21d REAL,
                        log_return_21d REAL, log_spy_return_21d REAL,
                        UNIQUE(ticker, eval_date)
                    )
                """)
                # 21d window not yet closed → log_return_21d = NULL
                conn.execute(
                    "INSERT INTO universe_returns "
                    "(ticker, eval_date, return_21d, spy_return_21d, "
                    "log_return_21d, log_spy_return_21d) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    ("AAPL", "2026-03-02", None, None, None, None),
                )
                conn.commit()

            result = _backfill_predictor_returns(db, dry_run=False, forward_days=21)
            assert result["status"] == "ok"
            assert result["rows_written"] == 0

            with sqlite3.connect(db) as conn:
                row = conn.execute(
                    "SELECT actual_log_alpha FROM predictor_outcomes "
                    "WHERE symbol='AAPL' AND prediction_date='2026-03-02'"
                ).fetchone()
            assert row[0] is None
