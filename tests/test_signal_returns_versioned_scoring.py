"""Tests for champion/challenger Phase 2 versioned outcome scoring (L4469).

Covers: model_version on live rows; the dedicated predictor_outcomes_shadow
table (so the live UNIQUE(symbol,date) invariant + its consumers are untouched);
multi-version coexistence; and the backfill resolving BOTH tables by id so each
version gets its OWN `correct` against the shared realized alpha.
"""
from __future__ import annotations

import json
import sqlite3
from unittest.mock import MagicMock

import pytest

from collectors.signal_returns import (
    _backfill_predictor_returns,
    _ensure_predictor_outcomes_schema,
    _seed_predictor_outcomes,
    _seed_shadow_predictor_outcomes,
)


@pytest.fixture
def tmp_db(tmp_path):
    db = tmp_path / "research.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE predictor_outcomes (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, prediction_date TEXT NOT NULL,
                predicted_direction TEXT, prediction_confidence REAL,
                p_up REAL, p_flat REAL, p_down REAL, score_modifier_applied REAL DEFAULT 0.0,
                actual_5d_return REAL, correct_5d INTEGER,
                actual_log_alpha REAL, horizon_days INTEGER, correct INTEGER,
                UNIQUE(symbol, prediction_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE universe_returns (
                id INTEGER PRIMARY KEY, ticker TEXT NOT NULL, eval_date TEXT NOT NULL,
                return_21d REAL, spy_return_21d REAL,
                log_return_21d REAL, log_spy_return_21d REAL,
                UNIQUE(ticker, eval_date)
            )
            """
        )
        conn.commit()
    return str(db)


def _seed_ur(db, ticker, eval_date, log_stock, log_spy):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO universe_returns (ticker, eval_date, log_return_21d, log_spy_return_21d) "
            "VALUES (?, ?, ?, ?)",
            (ticker, eval_date, log_stock, log_spy),
        )
        conn.commit()


def _s3_live(payload, key="predictor/predictions/2026-03-02.json"):
    body = MagicMock()
    body.read.return_value = json.dumps(payload).encode()
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {"Contents": [{"Key": key}]}
    s3.get_object.return_value = {"Body": body}
    return s3


def _s3_shadow(files: dict):
    s3 = MagicMock()
    pag = MagicMock()
    pag.paginate.return_value = [{"Contents": [{"Key": k} for k in files]}]
    s3.get_paginator.return_value = pag

    def _get(Bucket, Key):
        body = MagicMock()
        body.read.return_value = json.dumps(files[Key]).encode()
        return {"Body": body}

    s3.get_object.side_effect = _get
    return s3


def test_schema_adds_model_version(tmp_db):
    with sqlite3.connect(tmp_db) as conn:
        _ensure_predictor_outcomes_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(predictor_outcomes)")}
    assert "model_version" in cols


def test_live_seed_tags_model_version(tmp_db):
    payload = {
        "date": "2026-03-02", "model_version": "meta-v3.0-8models",
        "predictions": [{"ticker": "AAPL", "predicted_direction": "UP", "p_up": 0.6}],
    }
    _seed_predictor_outcomes(_s3_live(payload), "bkt", tmp_db, dry_run=False)
    with sqlite3.connect(tmp_db) as conn:
        mv = conn.execute(
            "SELECT model_version FROM predictor_outcomes WHERE symbol='AAPL'"
        ).fetchone()[0]
    assert mv == "meta-v3.0-8models"


def test_shadow_seed_creates_dedicated_table_tagged_by_version(tmp_db):
    files = {
        "predictor/predictions_shadow/V1-abc/2026-03-02.json": {
            "date": "2026-03-02", "version_id": "V1-abc",
            "predictions": [{"ticker": "AAPL", "predicted_direction": "DOWN", "p_down": 0.7}],
        }
    }
    out = _seed_shadow_predictor_outcomes(_s3_shadow(files), "bkt", tmp_db, dry_run=False)
    assert out["rows_written"] == 1
    with sqlite3.connect(tmp_db) as conn:
        row = conn.execute(
            "SELECT symbol, model_version, predicted_direction "
            "FROM predictor_outcomes_shadow"
        ).fetchone()
    assert row == ("AAPL", "V1-abc", "DOWN")


def test_shadow_dedup_allows_multiple_versions_same_symbol_date(tmp_db):
    files = {
        "predictor/predictions_shadow/V1/2026-03-02.json": {
            "date": "2026-03-02", "version_id": "V1",
            "predictions": [{"ticker": "AAPL", "predicted_direction": "UP"}],
        },
        "predictor/predictions_shadow/V2/2026-03-02.json": {
            "date": "2026-03-02", "version_id": "V2",
            "predictions": [{"ticker": "AAPL", "predicted_direction": "DOWN"}],
        },
    }
    _seed_shadow_predictor_outcomes(_s3_shadow(files), "bkt", tmp_db, dry_run=False)
    with sqlite3.connect(tmp_db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM predictor_outcomes_shadow "
            "WHERE symbol='AAPL' AND prediction_date='2026-03-02'"
        ).fetchone()[0]
    assert n == 2  # both versions coexist (no UNIQUE(symbol,date) collision)


def test_backfill_resolves_both_tables_with_own_correct(tmp_db):
    # Champion UP + challenger V1 DOWN on the same ticker/date; realized
    # log_alpha = +0.04 (stock beats SPY) → UP is correct, DOWN is wrong.
    _seed_predictor_outcomes(
        _s3_live({"date": "2026-03-02", "model_version": "champ",
                  "predictions": [{"ticker": "AAPL", "predicted_direction": "UP"}]}),
        "bkt", tmp_db, dry_run=False,
    )
    _seed_shadow_predictor_outcomes(
        _s3_shadow({"predictor/predictions_shadow/V1/2026-03-02.json": {
            "date": "2026-03-02", "version_id": "V1",
            "predictions": [{"ticker": "AAPL", "predicted_direction": "DOWN"}]}}),
        "bkt", tmp_db, dry_run=False,
    )
    _seed_ur(tmp_db, "AAPL", "2026-03-02", log_stock=0.05, log_spy=0.01)

    _backfill_predictor_returns(tmp_db, dry_run=False, forward_days=21)

    with sqlite3.connect(tmp_db) as conn:
        live = conn.execute(
            "SELECT actual_log_alpha, correct FROM predictor_outcomes WHERE symbol='AAPL'"
        ).fetchone()
        shadow = conn.execute(
            "SELECT actual_log_alpha, correct FROM predictor_outcomes_shadow WHERE symbol='AAPL'"
        ).fetchone()
    assert abs(live[0] - 0.04) < 1e-6 and live[1] == 1     # champion UP — correct
    assert abs(shadow[0] - 0.04) < 1e-6 and shadow[1] == 0  # challenger DOWN — wrong, own `correct`
