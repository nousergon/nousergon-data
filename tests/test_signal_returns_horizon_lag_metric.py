"""Tests for _emit_horizon_grading_lag_metric (config#2972).

Covers the producer-side CloudWatch gauge that distinguishes an expected
forward-window wait from a genuinely stalled grading pipeline: the exact
false-alarm class a prior groom pass hit when it queried research.db
directly and mistook the natural 21-trading-day lag boundary for a break.
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from collectors.signal_returns import _emit_horizon_grading_lag_metric, _newest_window_closed
from collectors.universe_returns import _ensure_table as _ensure_universe_returns_table


def _make_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    import os
    os.close(fd)
    _ensure_universe_returns_table(path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE predictor_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "symbol TEXT, prediction_date TEXT, horizon_days INTEGER)"
        )
        conn.commit()
    return path


def _seed_universe_returns(db: str, eval_date: str, has_21d: bool) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO universe_returns (ticker, eval_date, return_5d, return_21d, log_return_21d) "
            "VALUES (?, ?, ?, ?, ?)",
            ("AAPL", eval_date, 0.01, 0.02 if has_21d else None, 0.0198 if has_21d else None),
        )
        conn.commit()


def _seed_predictor_outcomes(db: str, prediction_date: str, horizon_days: int | None) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO predictor_outcomes (symbol, prediction_date, horizon_days) VALUES (?, ?, ?)",
            ("AAPL", prediction_date, horizon_days),
        )
        conn.commit()


class TestNewestWindowClosed:
    def test_none_when_table_empty(self):
        assert _newest_window_closed(None, date(2026, 7, 19), 21) is None

    def test_newest_row_already_closed(self):
        # 2026-06-16 + 21 trading days = 2026-07-17, which is < 2026-07-19.
        out = _newest_window_closed("2026-06-16", date(2026, 7, 19), 21)
        assert out == "2026-06-16"

    def test_walks_back_when_newest_row_still_open(self):
        # 2026-06-17 + 21 trading days = 2026-07-20, NOT < 2026-07-19 (still
        # open) — must walk back to 2026-06-16, whose window HAS closed.
        out = _newest_window_closed("2026-06-17", date(2026, 7, 19), 21)
        assert out == "2026-06-16"


class TestEmitHorizonGradingLagMetric:
    def test_healthy_pipeline_emits_zero_lag(self, monkeypatch):
        """The exact config#2972 scenario: rows exist up through a date whose
        21d window hasn't closed yet, and the graded columns are populated
        for every date whose window HAS closed. Lag must be 0 — this is NOT
        a stall, and the metric must not cry wolf on it."""
        db = _make_db()
        # 06-16's window closed 07-17; 06-17's closes 07-20 (still open on 07-19).
        _seed_universe_returns(db, "2026-06-16", has_21d=True)
        _seed_universe_returns(db, "2026-06-17", has_21d=False)
        _seed_predictor_outcomes(db, "2026-06-16", horizon_days=21)
        _seed_predictor_outcomes(db, "2026-06-17", horizon_days=None)

        cw = MagicMock()
        monkeypatch.setattr(
            "collectors.signal_returns.boto3.client",
            lambda svc: cw if svc == "cloudwatch" else MagicMock(),
        )
        monkeypatch.setattr(
            "collectors.signal_returns.date", _stub_today_factory(2026, 7, 19),
        )

        out = _emit_horizon_grading_lag_metric(db, forward_days=21)
        assert out["status"] == "ok"
        assert out["universe_returns_lag_trading_days"] == 0
        assert out["predictor_outcomes_lag_trading_days"] == 0

        cw.put_metric_data.assert_called_once()
        call = cw.put_metric_data.call_args
        assert call.kwargs["Namespace"] == "AlphaEngine/Data"
        names = {m["MetricName"] for m in call.kwargs["MetricData"]}
        assert names == {
            "universe_returns_horizon_grading_lag_trading_days",
            "predictor_outcomes_grading_lag_trading_days",
        }
        for m in call.kwargs["MetricData"]:
            assert m["Value"] == 0.0

    def test_stalled_pipeline_emits_positive_lag(self, monkeypatch):
        """A genuinely stalled grading path: several dates' 21d windows have
        closed but the graded columns never got backfilled for them. Lag
        must be > 0 so the alarm can fire on sustained non-zero lag."""
        db = _make_db()
        # All of these have long since closed by 2026-07-19, but only the
        # oldest got graded — a real stall, not a forward-window wait.
        _seed_universe_returns(db, "2026-05-01", has_21d=True)
        _seed_universe_returns(db, "2026-05-04", has_21d=False)
        _seed_universe_returns(db, "2026-05-05", has_21d=False)
        _seed_predictor_outcomes(db, "2026-05-01", horizon_days=21)
        _seed_predictor_outcomes(db, "2026-05-04", horizon_days=None)
        _seed_predictor_outcomes(db, "2026-05-05", horizon_days=None)

        cw = MagicMock()
        monkeypatch.setattr(
            "collectors.signal_returns.boto3.client",
            lambda svc: cw if svc == "cloudwatch" else MagicMock(),
        )
        monkeypatch.setattr(
            "collectors.signal_returns.date", _stub_today_factory(2026, 7, 19),
        )

        out = _emit_horizon_grading_lag_metric(db, forward_days=21)
        assert out["status"] == "ok"
        assert out["universe_returns_lag_trading_days"] > 0
        assert out["predictor_outcomes_lag_trading_days"] > 0

    def test_db_read_failure_is_non_fatal(self, monkeypatch):
        monkeypatch.setattr(
            "collectors.signal_returns.boto3.client",
            lambda svc: MagicMock(),
        )
        out = _emit_horizon_grading_lag_metric("/nonexistent/path/research.db", forward_days=21)
        assert out["status"] == "skipped"
        assert "error" in out

    def test_empty_tables_emit_zero_not_crash(self, monkeypatch):
        db = _make_db()
        cw = MagicMock()
        monkeypatch.setattr(
            "collectors.signal_returns.boto3.client",
            lambda svc: cw if svc == "cloudwatch" else MagicMock(),
        )
        out = _emit_horizon_grading_lag_metric(db, forward_days=21)
        assert out["status"] == "ok"
        assert out["universe_returns_lag_trading_days"] == 0
        assert out["predictor_outcomes_lag_trading_days"] == 0


def _stub_today_factory(year, month, day):
    """Patches collectors.signal_returns's `date` name so date.today() is
    deterministic, mirroring the pattern already used in
    test_universe_returns_21d_log.py."""
    class _S(date):
        @classmethod
        def today(cls):
            return date(year, month, day)
    return _S
