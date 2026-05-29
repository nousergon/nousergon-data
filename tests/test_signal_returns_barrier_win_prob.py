"""Tests for recording `barrier_win_prob` into predictor_outcomes.

Covers ROADMAP L239: the predictor (alpha-engine-predictor #211) emits an
observe-only López-de-Prado meta-label `barrier_win_prob` — P(upper/profit
barrier touched before lower/stop barrier) — into predictions.json. The
backtester's `barrier_sizing_optimizer` IC gate returns
`barrier_win_prob_column_absent` until alpha-engine-data's predictor-outcomes
seeder ALSO records this field into the `predictor_outcomes` table, mirroring
the existing `p_up` write path.

These tests pin two contracts:
  - `_ensure_predictor_outcomes_schema` adds the nullable `barrier_win_prob`
    column (idempotent ALTER).
  - `_seed_predictor_outcomes` reads `barrier_win_prob` from each prediction
    dict and writes it on the initial INSERT, recording NULL when the
    predictor omitted the field (meta-label classifier not loaded that cycle).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from collectors.signal_returns import (
    _ensure_predictor_outcomes_schema,
    _seed_predictor_outcomes,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db(tmp_path: Path) -> str:
    """An empty predictor_outcomes table — pre-barrier_win_prob schema.

    Mirrors the legacy shape research.db carries before this collector's
    schema-ensure runs (the table is created by the research Lambda; the
    seeder only ALTERs missing columns + inserts rows).
    """
    db = tmp_path / "research.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE predictor_outcomes (
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
            """
        )
        conn.commit()
    return str(db)


def _predictions_payload() -> dict:
    """A predictions.json with one ticker carrying barrier_win_prob and one
    omitting it (the meta-label classifier rides as null when not fitted)."""
    return {
        "date": "2026-05-29",
        "predictions": [
            {
                "ticker": "AAPL",
                "predicted_direction": "UP",
                "prediction_confidence": 0.81,
                "p_up": 0.62, "p_flat": 0.28, "p_down": 0.10,
                "barrier_win_prob": 0.72,
            },
            {
                # Older/observe-gap prediction — classifier not loaded, field absent.
                "ticker": "MSFT",
                "predicted_direction": "FLAT",
                "prediction_confidence": 0.55,
                "p_up": 0.34, "p_flat": 0.40, "p_down": 0.26,
            },
        ],
    }


def _mock_s3_for(payload: dict, *, key: str = "predictor/predictions/2026-05-29.json") -> MagicMock:
    body = MagicMock()
    body.read.return_value = json.dumps(payload).encode()
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {"Contents": [{"Key": key}]}
    s3.get_object.return_value = {"Body": body}
    return s3


# ── Schema bridge ──────────────────────────────────────────────────────────────


def test_schema_ensure_adds_barrier_win_prob_column(tmp_db):
    with sqlite3.connect(tmp_db) as conn:
        _ensure_predictor_outcomes_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(predictor_outcomes)").fetchall()}
    assert "barrier_win_prob" in cols


# ── Seed: records barrier_win_prob from predictions.json ────────────────────────


class TestSeedRecordsBarrierWinProb:

    def test_records_value_when_present(self, tmp_db):
        s3 = _mock_s3_for(_predictions_payload())
        out = _seed_predictor_outcomes(s3, "bucket", tmp_db, dry_run=False)
        assert out["status"] == "ok"
        assert out["rows_written"] == 2

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT barrier_win_prob FROM predictor_outcomes "
                "WHERE symbol='AAPL' AND prediction_date='2026-05-29'"
            ).fetchone()
        assert row[0] == pytest.approx(0.72)

    def test_records_null_when_field_absent(self, tmp_db):
        s3 = _mock_s3_for(_predictions_payload())
        _seed_predictor_outcomes(s3, "bucket", tmp_db, dry_run=False)

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT barrier_win_prob FROM predictor_outcomes "
                "WHERE symbol='MSFT' AND prediction_date='2026-05-29'"
            ).fetchone()
        assert row[0] is None

    def test_seed_auto_creates_column_on_legacy_db(self, tmp_db):
        """The seeder runs schema-ensure before its INSERT, so a research.db
        whose predictor_outcomes predates barrier_win_prob still records it
        without a separate migration step."""
        s3 = _mock_s3_for(_predictions_payload())
        _seed_predictor_outcomes(s3, "bucket", tmp_db, dry_run=False)
        with sqlite3.connect(tmp_db) as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(predictor_outcomes)").fetchall()}
        assert "barrier_win_prob" in cols

    def test_dry_run_writes_nothing(self, tmp_db):
        s3 = _mock_s3_for(_predictions_payload())
        out = _seed_predictor_outcomes(s3, "bucket", tmp_db, dry_run=True)
        # rows_written counts intended inserts; dry_run skips the execute.
        assert out["rows_written"] == 2
        with sqlite3.connect(tmp_db) as conn:
            n = conn.execute("SELECT COUNT(*) FROM predictor_outcomes").fetchone()[0]
        assert n == 0
