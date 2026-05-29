"""Tests for score_performance calibrator-v1 context seeding + backfill.

Covers the 2026-05-10 producer-side fix: `_seed_score_performance` was
inserting only `(symbol, score_date, score, price_on_date)` and leaving
the 5 canonical context columns (quant_score, qual_score, conviction,
sector_modifier, market_regime) NULL. Saturday 2026-05-09's evaluator
tripped on this when weight_optimizer's downstream lookup expected those
columns post research migration #12.

These tests pin two contracts:
  - Initial INSERT carries all 5 canonical context fields, sourced from
    the same signals.json payload that drives the BUY filter.
  - `_backfill_score_context` repairs legacy rows that were seeded
    before the producer learned to write them. UPDATE-WHERE-NULL means
    re-runs converge to a no-op once every row has at least one source.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from collectors.signal_returns import (
    _CANONICAL_CONTEXT_COLUMNS,
    _DRIFT_EFFECTIVE_DATE,
    _backfill_score_context,
    _emit_context_coverage_metric,
    _ensure_score_performance_schema,
    _extract_signal_context,
    _seed_score_performance,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db(tmp_path: Path) -> str:
    db = tmp_path / "research.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE score_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                score_date TEXT NOT NULL,
                score REAL,
                price_on_date REAL,
                UNIQUE(symbol, score_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE universe_returns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                eval_date TEXT NOT NULL,
                close_price REAL,
                UNIQUE(ticker, eval_date)
            )
            """
        )
        conn.commit()
    return str(db)


def _signals_payload() -> dict:
    """Representative signals.json with sub_scores + context populated."""
    return {
        "date": "2026-05-01",
        "market_regime": "bull",
        "sector_modifiers": {"Technology": 1.05, "Healthcare": 0.95},
        "signals": {
            "AAPL": {
                "score": 78.0, "rating": "BUY", "sector": "Technology",
                "quant_score": 80.0, "qual_score": 72.0, "conviction": "rising",
            },
            "MSFT": {
                "score": 70.0, "rating": "BUY", "sector": "Technology",
                "quant_score": 68.0, "qual_score": 71.0, "conviction": "stable",
            },
            "JNJ": {
                "score": 65.0, "rating": "HOLD", "sector": "Healthcare",
                # HOLD — should be skipped by BUY filter
                "quant_score": 60.0, "qual_score": 65.0, "conviction": "stable",
            },
            "PFE": {
                "score": 76.0, "rating": "BUY", "sector": "Healthcare",
                "quant_score": 75.0, "qual_score": 78.0, "conviction": "declining",
            },
        },
    }


def _mock_s3_for(payload: dict) -> MagicMock:
    body = MagicMock()
    body.read.return_value = json.dumps(payload).encode()
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": body}
    # Single date in the listing
    page = {"CommonPrefixes": [{"Prefix": "signals/2026-05-01/"}]}
    paginator = MagicMock()
    paginator.paginate.return_value = [page]
    s3.get_paginator.return_value = paginator
    return s3


def _seed_universe_close(db: str, ticker: str, eval_date: str, close: float) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO universe_returns (ticker, eval_date, close_price) VALUES (?, ?, ?)",
            (ticker, eval_date, close),
        )
        conn.commit()


# ── _extract_signal_context ───────────────────────────────────────────────────


class TestExtractSignalContext:

    def test_resolves_all_five_fields(self):
        payload = _signals_payload()
        ctx = _extract_signal_context(payload, "AAPL")
        assert ctx == {
            "quant_score": 80.0,
            "qual_score": 72.0,
            "conviction": "rising",
            "sector_modifier": 1.05,
            "market_regime": "bull",
        }

    def test_unknown_ticker_returns_all_none(self):
        ctx = _extract_signal_context(_signals_payload(), "NVDA")
        assert ctx == {
            "quant_score": None, "qual_score": None, "conviction": None,
            "sector_modifier": None, "market_regime": "bull",  # market_regime is payload-level
        }

    def test_missing_sector_modifier_when_sector_absent(self):
        payload = {
            "market_regime": "neutral",
            "sector_modifiers": {"Technology": 1.10},
            "signals": {
                "AAPL": {"quant_score": 70, "qual_score": 60},  # no sector
            },
        }
        ctx = _extract_signal_context(payload, "AAPL")
        assert ctx["sector_modifier"] is None
        assert ctx["market_regime"] == "neutral"


# ── _seed_score_performance — initial INSERT carries canonical context ──────


class TestSeedScorePerformanceCanonicalInsert:

    def test_buy_rows_get_canonical_context_on_insert(self, tmp_db):
        _seed_universe_close(tmp_db, "AAPL", "2026-05-01", 205.50)
        _seed_universe_close(tmp_db, "MSFT", "2026-05-01", 430.25)
        _seed_universe_close(tmp_db, "PFE", "2026-05-01", 28.10)

        s3 = _mock_s3_for(_signals_payload())
        out = _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["status"] == "ok"
        assert out["rows_written"] == 3  # AAPL + MSFT + PFE; JNJ is HOLD

        with sqlite3.connect(tmp_db) as conn:
            rows = conn.execute(
                "SELECT symbol, quant_score, qual_score, conviction, "
                "sector_modifier, market_regime FROM score_performance "
                "ORDER BY symbol"
            ).fetchall()
        by_sym = {r[0]: r[1:] for r in rows}
        assert by_sym["AAPL"] == (80.0, 72.0, "rising", 1.05, "bull")
        assert by_sym["MSFT"] == (68.0, 71.0, "stable", 1.05, "bull")
        assert by_sym["PFE"] == (75.0, 78.0, "declining", 0.95, "bull")

    def test_skips_hold_rated_rows(self, tmp_db):
        _seed_universe_close(tmp_db, "JNJ", "2026-05-01", 152.00)
        s3 = _mock_s3_for(_signals_payload())
        _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        with sqlite3.connect(tmp_db) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM score_performance WHERE symbol='JNJ'"
            ).fetchone()[0] == 0

    def test_existing_rows_are_not_reseeded(self, tmp_db):
        """INSERT OR IGNORE means a re-run doesn't overwrite. Canonical
        context backfill is a separate step (_backfill_score_context)."""
        _seed_universe_close(tmp_db, "AAPL", "2026-05-01", 205.50)
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(
                "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50)"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        out = _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        # Pre-existing row filtered by `existing` set; seeder reports 0 written.
        assert out["rows_written"] == 0

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score, qual_score FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        # Still NULL — that's what _backfill_score_context exists to repair.
        assert row == (None, None)


# ── _backfill_score_context — UPDATE-WHERE-NULL repair for legacy rows ──────


class TestBackfillScoreContext:

    def test_repairs_legacy_null_rows(self, tmp_db):
        """Rows seeded before the canonical-context fix should pick up
        all 5 fields on backfill."""
        with sqlite3.connect(tmp_db) as conn:
            # Add the canonical columns (mirrors prior schema-ensure run)
            _ensure_score_performance_schema(conn)
            conn.execute(
                "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50)"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        out = _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["status"] == "ok"
        assert out["rows_written"] == 1

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score, qual_score, conviction, "
                "sector_modifier, market_regime "
                "FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        assert row == (80.0, 72.0, "rising", 1.05, "bull")

    def test_rerun_is_noop_once_populated(self, tmp_db):
        """Repeat invocations after backfill should converge to 0 updates."""
        _seed_universe_close(tmp_db, "AAPL", "2026-05-01", 205.50)
        s3 = _mock_s3_for(_signals_payload())
        _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        out = _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["rows_written"] == 0
        assert "no NULL context rows" in (out.get("note") or "")

    def test_dry_run_does_not_persist(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            conn.execute(
                "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50)"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        out = _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=True)
        assert out["rows_written"] == 1

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        assert row[0] is None  # not actually written

    def test_partial_null_only_fills_missing(self, tmp_db):
        """A row that already has quant_score should keep it; only NULL
        fields get backfilled."""
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            conn.execute(
                "INSERT INTO score_performance "
                "(symbol, score_date, score, price_on_date, quant_score, conviction) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50, 99.0, 'manual')"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=False)

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score, qual_score, conviction, "
                "sector_modifier, market_regime "
                "FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        # Pre-existing values preserved; NULLs filled.
        assert row == (99.0, 72.0, "manual", 1.05, "bull")


# ── Schema-ensure mirrors migration #12 ──────────────────────────────────────


class TestEnsureScorePerformanceSchema:

    def test_adds_canonical_columns_idempotently(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            _ensure_score_performance_schema(conn)  # second call is a no-op
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(score_performance)"
            ).fetchall()}

        for canonical_col in ("quant_score", "qual_score", "conviction",
                              "sector_modifier", "market_regime"):
            assert canonical_col in cols, f"missing {canonical_col}"


# ── _emit_context_coverage_metric — producer-side drift gate ─────────────────


class TestEmitContextCoverageMetric:
    """The CW gauge AlphaEngine/Data/score_performance_canonical_coverage_pct
    is the runtime drift detector. Contract: every row written with
    score_date >= _DRIFT_EFFECTIVE_DATE must have ALL 5 canonical context
    columns populated. Coverage_pct drops below 100 → alarm fires."""

    def _seed_row(self, db, symbol, score_date, **extras):
        with sqlite3.connect(db) as conn:
            _ensure_score_performance_schema(conn)
            cols = ["symbol", "score_date", "score", "price_on_date", *extras.keys()]
            vals = [symbol, score_date, 78.0, 200.0, *extras.values()]
            placeholders = ", ".join("?" for _ in cols)
            conn.execute(
                f"INSERT INTO score_performance ({', '.join(cols)}) VALUES ({placeholders})",
                vals,
            )
            conn.commit()

    def test_full_coverage_emits_100(self, tmp_db, monkeypatch):
        ctx = dict(quant_score=80.0, qual_score=72.0, conviction="rising",
                   sector_modifier=1.05, market_regime="bull")
        self._seed_row(tmp_db, "AAPL", _DRIFT_EFFECTIVE_DATE, **ctx)
        self._seed_row(tmp_db, "MSFT", _DRIFT_EFFECTIVE_DATE, **ctx)

        cw = MagicMock()
        monkeypatch.setattr("collectors.signal_returns.boto3.client",
                            lambda svc: cw if svc == "cloudwatch" else MagicMock())

        out = _emit_context_coverage_metric(tmp_db)
        assert out["status"] == "ok"
        assert out["coverage_pct"] == 100.0
        assert out["rows_post_cutoff"] == 2
        assert out["rows_fully_populated"] == 2

        cw.put_metric_data.assert_called_once()
        call = cw.put_metric_data.call_args
        assert call.kwargs["Namespace"] == "AlphaEngine/Data"
        metric = call.kwargs["MetricData"][0]
        assert metric["MetricName"] == "score_performance_canonical_coverage_pct"
        assert metric["Value"] == 100.0
        assert metric["Unit"] == "Percent"

    def test_partial_coverage_emits_below_100(self, tmp_db, monkeypatch):
        full_ctx = dict(quant_score=80.0, qual_score=72.0, conviction="rising",
                        sector_modifier=1.05, market_regime="bull")
        partial_ctx = dict(quant_score=80.0)  # missing 4 fields
        self._seed_row(tmp_db, "AAPL", _DRIFT_EFFECTIVE_DATE, **full_ctx)
        self._seed_row(tmp_db, "MSFT", _DRIFT_EFFECTIVE_DATE, **partial_ctx)

        cw = MagicMock()
        monkeypatch.setattr("collectors.signal_returns.boto3.client",
                            lambda svc: cw if svc == "cloudwatch" else MagicMock())

        out = _emit_context_coverage_metric(tmp_db)
        assert out["coverage_pct"] == 50.0
        assert out["rows_post_cutoff"] == 2
        assert out["rows_fully_populated"] == 1

    def test_pre_cutover_rows_excluded_from_gate(self, tmp_db, monkeypatch):
        """Legacy NULL rows seeded before the producer fix must not pollute
        the drift gauge — the gauge is forward-looking."""
        # Row pre-effective-date with all NULLs — must not count against coverage
        self._seed_row(tmp_db, "AAPL", "2026-04-01")  # no canonical kwargs
        # Row post-effective-date with full context
        full_ctx = dict(quant_score=80.0, qual_score=72.0, conviction="rising",
                        sector_modifier=1.05, market_regime="bull")
        self._seed_row(tmp_db, "MSFT", _DRIFT_EFFECTIVE_DATE, **full_ctx)

        cw = MagicMock()
        monkeypatch.setattr("collectors.signal_returns.boto3.client",
                            lambda svc: cw if svc == "cloudwatch" else MagicMock())

        out = _emit_context_coverage_metric(tmp_db)
        assert out["rows_post_cutoff"] == 1
        assert out["coverage_pct"] == 100.0

    def test_empty_post_cutoff_reports_100(self, tmp_db, monkeypatch):
        """No rows past effective_date — coverage undefined, report 100
        so the alarm doesn't fire on a legitimately-empty window."""
        cw = MagicMock()
        monkeypatch.setattr("collectors.signal_returns.boto3.client",
                            lambda svc: cw if svc == "cloudwatch" else MagicMock())

        out = _emit_context_coverage_metric(tmp_db)
        assert out["rows_post_cutoff"] == 0
        assert out["coverage_pct"] == 100.0
        assert "no rows past effective_date" in out["note"]

    def test_metric_emit_failure_is_non_fatal(self, tmp_db, monkeypatch):
        """CW throttling / network errors must not break the collector."""
        full_ctx = dict(quant_score=80.0, qual_score=72.0, conviction="rising",
                        sector_modifier=1.05, market_regime="bull")
        self._seed_row(tmp_db, "AAPL", _DRIFT_EFFECTIVE_DATE, **full_ctx)

        cw = MagicMock()
        cw.put_metric_data.side_effect = RuntimeError("CW throttled")
        monkeypatch.setattr("collectors.signal_returns.boto3.client",
                            lambda svc: cw if svc == "cloudwatch" else MagicMock())

        out = _emit_context_coverage_metric(tmp_db)
        assert out["status"] == "skipped"
        assert "CW throttled" in out["error"]

    def test_canonical_columns_constant_matches_seed_insert(self):
        """Tripwire: if someone adds a 6th canonical column to the seed
        INSERT they must also add it here, or the drift gate becomes
        blind to that field's NULLs."""
        assert set(_CANONICAL_CONTEXT_COLUMNS) == {
            "quant_score", "qual_score", "conviction",
            "sector_modifier", "market_regime",
        }


# ── Stance denormalization (Kimball pattern, 2026-05-11) ──────────────────


def _mock_s3_with_signals_and_predictions(
    signals_payload: dict,
    predictions_payload: dict | None,
) -> MagicMock:
    """S3 mock that returns different bodies for signals.json vs
    predictions.json — needed to test the stance denormalization
    path which reads from BOTH per-date sources.

    ``predictions_payload=None`` simulates a sig_date that predates the
    stance field (predictor#137 shipped 2026-05-11) — get_object on
    the predictions key raises a NoSuchKey-equivalent which the
    extractor catches and returns an empty stance lookup.
    """
    from botocore.exceptions import ClientError

    def _get_object(Bucket, Key):
        body = MagicMock()
        if Key.endswith("signals.json"):
            body.read.return_value = json.dumps(signals_payload).encode()
            return {"Body": body}
        if "predictions" in Key:
            if predictions_payload is None:
                raise ClientError(
                    {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
                    "GetObject",
                )
            body.read.return_value = json.dumps(predictions_payload).encode()
            return {"Body": body}
        raise AssertionError(f"Unexpected S3 key: {Key}")

    s3 = MagicMock()
    s3.get_object.side_effect = _get_object
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "signals/2026-05-01/"}]}
    ]
    s3.get_paginator.return_value = paginator
    return s3


def _predictions_payload(stance_by_ticker: dict[str, str]) -> dict:
    """Minimal predictions.json payload — only the fields our stance
    extractor reads. Real payload has many more per-ticker fields."""
    return {
        "date": "2026-05-01",
        "predictions": [
            {"ticker": t, "stance": s} for t, s in stance_by_ticker.items()
        ],
    }


class TestSeedScorePerformanceStanceColumn:
    """Verifies the stance column on score_performance is populated at
    INSERT from predictions.json — the Kimball denormalization the
    backtester's per-stance attribution depends on."""

    def test_stance_stamped_when_predictions_provides_it(self, tmp_db):
        sigs = _signals_payload()
        preds = _predictions_payload({
            "AAPL": "momentum", "MSFT": "quality", "PFE": "value",
        })
        s3 = _mock_s3_with_signals_and_predictions(sigs, preds)
        for t in ("AAPL", "MSFT", "PFE"):
            _seed_universe_close(tmp_db, t, "2026-05-01", 100.0)

        out = _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["rows_written"] == 3

        with sqlite3.connect(tmp_db) as conn:
            rows = {
                r[0]: r[1]
                for r in conn.execute(
                    "SELECT symbol, stance FROM score_performance"
                ).fetchall()
            }
        assert rows == {"AAPL": "momentum", "MSFT": "quality", "PFE": "value"}

    def test_stance_null_when_predictor_did_not_score_ticker(self, tmp_db):
        """If predictions.json exists but lacks an entry for a ticker
        (e.g., ticker outside predictor's population), stance stays NULL.
        Backtester treats NULL as 'no stance recorded', not a default."""
        sigs = _signals_payload()
        preds = _predictions_payload({"AAPL": "momentum"})  # only AAPL
        s3 = _mock_s3_with_signals_and_predictions(sigs, preds)
        for t in ("AAPL", "MSFT", "PFE"):
            _seed_universe_close(tmp_db, t, "2026-05-01", 100.0)

        _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)

        with sqlite3.connect(tmp_db) as conn:
            rows = {
                r[0]: r[1]
                for r in conn.execute(
                    "SELECT symbol, stance FROM score_performance"
                ).fetchall()
            }
        assert rows["AAPL"] == "momentum"
        assert rows["MSFT"] is None
        assert rows["PFE"] is None

    def test_stance_null_when_predictions_json_missing(self, tmp_db):
        """sig_date predates stance field (predictor#137 shipped
        2026-05-11). predictions.json either doesn't exist or lacks
        the field. Extractor returns empty dict, all rows for that
        date get NULL stance — graceful degrade during the data-layer
        transition."""
        sigs = _signals_payload()
        s3 = _mock_s3_with_signals_and_predictions(sigs, predictions_payload=None)
        for t in ("AAPL", "MSFT", "PFE"):
            _seed_universe_close(tmp_db, t, "2026-05-01", 100.0)

        out = _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["rows_written"] == 3  # rows still get written

        with sqlite3.connect(tmp_db) as conn:
            stances = [
                r[0]
                for r in conn.execute(
                    "SELECT stance FROM score_performance"
                ).fetchall()
            ]
        assert stances == [None, None, None]

    def test_stance_lookup_cached_per_date(self, tmp_db):
        """The stance extractor caches per-date — fetching predictions.json
        once per date, not once per (date, ticker). Pinned via call-count
        on the S3 mock so a future refactor that loses the cache
        immediately surfaces (per-ticker S3 reads would be a real
        regression at scale)."""
        sigs = _signals_payload()
        preds = _predictions_payload({"AAPL": "momentum", "MSFT": "quality", "PFE": "value"})
        s3 = _mock_s3_with_signals_and_predictions(sigs, preds)
        for t in ("AAPL", "MSFT", "PFE"):
            _seed_universe_close(tmp_db, t, "2026-05-01", 100.0)

        _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        # 3 BUY-rated tickers, but only 1 predictions.json fetch
        # (1 unique sig_date) — not 3.
        predictions_calls = [
            c for c in s3.get_object.call_args_list
            if "predictions" in (c.kwargs.get("Key", "") or (c.args[1] if len(c.args) > 1 else ""))
        ]
        assert len(predictions_calls) == 1, (
            f"Expected 1 predictions.json fetch (per-date cache), got "
            f"{len(predictions_calls)}"
        )


class TestLoadStanceLookupForDate:
    """Unit tests for the standalone _load_stance_lookup_for_date helper."""

    def test_returns_ticker_to_stance_mapping(self):
        from collectors.signal_returns import _load_stance_lookup_for_date

        s3 = MagicMock()
        body = MagicMock()
        body.read.return_value = json.dumps({
            "predictions": [
                {"ticker": "AAPL", "stance": "momentum"},
                {"ticker": "WING", "stance": "value"},
            ]
        }).encode()
        s3.get_object.return_value = {"Body": body}

        result = _load_stance_lookup_for_date(s3, "bucket", "2026-05-01")
        assert result == {"AAPL": "momentum", "WING": "value"}

    def test_returns_empty_on_s3_404(self):
        from botocore.exceptions import ClientError
        from collectors.signal_returns import _load_stance_lookup_for_date

        s3 = MagicMock()
        s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
            "GetObject",
        )
        assert _load_stance_lookup_for_date(s3, "bucket", "2026-04-01") == {}

    def test_returns_empty_on_json_parse_error(self):
        from collectors.signal_returns import _load_stance_lookup_for_date

        s3 = MagicMock()
        body = MagicMock()
        body.read.return_value = b"not valid json{"
        s3.get_object.return_value = {"Body": body}
        assert _load_stance_lookup_for_date(s3, "bucket", "2026-05-01") == {}

    def test_skips_entries_without_stance_field(self):
        """Predictions emitted before the stance classifier shipped
        (predictor#137, 2026-05-11) lack the field. Skip those entries
        — don't crash."""
        from collectors.signal_returns import _load_stance_lookup_for_date

        s3 = MagicMock()
        body = MagicMock()
        body.read.return_value = json.dumps({
            "predictions": [
                {"ticker": "AAPL"},  # no stance field
                {"ticker": "WING", "stance": "value"},
            ]
        }).encode()
        s3.get_object.return_value = {"Body": body}

        result = _load_stance_lookup_for_date(s3, "bucket", "2026-05-01")
        assert result == {"WING": "value"}  # AAPL skipped, no error


class TestEnsureScorePerformanceSchemaIncludesStance:
    """Belt-and-suspenders ALTER must include stance — if the
    data-collector Lambda fires against a research.db that hasn't yet
    applied research migration v16, this defensive ALTER catches it."""

    def test_stance_column_added_by_defensive_alter(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            cols = {r[1] for r in conn.execute("PRAGMA table_info(score_performance)").fetchall()}
        assert "stance" in cols


# ── Canonical 21d returns + log-alpha backfill (ROADMAP L480, 2026-05-29) ──
#
# The judge outcome-IC validation correlates judge quality scores against
# realized canonical 21d log-domain market-relative alpha. These tests pin
# that _backfill_score_returns now populates the 21d arithmetic parity
# columns AND log_alpha_21d (= log_return_21d - log_spy_return_21d, the
# same definition the predictor's actual_log_alpha uses).


from collectors.signal_returns import _backfill_score_returns


def _make_21d_db(
    path,
    *,
    ticker="AAPL",
    score_date="2026-05-01",
    entry_price=100.0,
    include_log_cols=True,
    log_21d=0.0488,
    log_spy_21d=0.00995,
):
    """score_performance with one seeded row + a universe_returns carrying
    the full horizon column set (only 21d populated)."""
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE score_performance ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, "
            "score_date TEXT NOT NULL, score REAL, price_on_date REAL, "
            "UNIQUE(symbol, score_date))"
        )
        conn.execute(
            "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
            "VALUES (?, ?, ?, ?)",
            (ticker, score_date, 78.0, entry_price),
        )
        log_cols = (
            ", log_return_21d REAL, log_spy_return_21d REAL"
            if include_log_cols else ""
        )
        conn.execute(
            "CREATE TABLE universe_returns ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, "
            "eval_date TEXT NOT NULL, "
            "return_5d REAL, spy_return_5d REAL, beat_spy_5d INTEGER, "
            "return_10d REAL, spy_return_10d REAL, beat_spy_10d INTEGER, "
            "return_21d REAL, spy_return_21d REAL, beat_spy_21d INTEGER, "
            "return_30d REAL, spy_return_30d REAL, beat_spy_30d INTEGER"
            f"{log_cols}, UNIQUE(ticker, eval_date))"
        )
        if include_log_cols:
            conn.execute(
                "INSERT INTO universe_returns "
                "(ticker, eval_date, return_21d, spy_return_21d, beat_spy_21d, "
                "log_return_21d, log_spy_return_21d) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ticker, score_date, 0.05, 0.01, 1, log_21d, log_spy_21d),
            )
        else:
            conn.execute(
                "INSERT INTO universe_returns "
                "(ticker, eval_date, return_21d, spy_return_21d, beat_spy_21d) "
                "VALUES (?, ?, ?, ?, ?)",
                (ticker, score_date, 0.05, 0.01, 1),
            )
        conn.commit()


class TestBackfillScoreReturns21d:
    def test_populates_21d_arithmetic_and_canonical_log_alpha(self, tmp_path):
        db = str(tmp_path / "research.db")
        _make_21d_db(db, log_21d=0.0488, log_spy_21d=0.00995)

        out = _backfill_score_returns(db, dry_run=False)
        assert out["status"] == "ok"

        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT price_21d, return_21d, spy_21d_return, beat_spy_21d, "
                "log_alpha_21d FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        price_21d, return_21d, spy_21d_return, beat_spy_21d, log_alpha_21d = row
        assert price_21d == pytest.approx(105.0)         # 100 * (1 + 0.05)
        assert return_21d == pytest.approx(5.0)          # stored as percent
        assert spy_21d_return == pytest.approx(1.0)
        assert beat_spy_21d == 1
        # Canonical: log_alpha = log_return_21d - log_spy_return_21d
        assert log_alpha_21d == pytest.approx(0.0488 - 0.00995, abs=1e-6)

    def test_missing_log_columns_warns_but_arithmetic_survives(self, tmp_path, caplog):
        import logging

        db = str(tmp_path / "research.db")
        _make_21d_db(db, include_log_cols=False)

        with caplog.at_level(logging.WARNING, logger="collectors.signal_returns"):
            out = _backfill_score_returns(db, dry_run=False)
        assert out["status"] == "ok"

        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT return_21d, log_alpha_21d FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        # Primary deliverable (arithmetic 21d) survives; canonical alpha NULL.
        assert row[0] == pytest.approx(5.0)
        assert row[1] is None
        assert any(
            "log_alpha_21d" in r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING
        ), "no-silent-fails: missing log columns must emit a WARN"

    def test_dry_run_does_not_write_21d(self, tmp_path):
        db = str(tmp_path / "research.db")
        _make_21d_db(db)

        _backfill_score_returns(db, dry_run=True)

        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT return_21d, log_alpha_21d FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        assert row == (None, None)

    def test_rerun_is_noop_once_populated(self, tmp_path):
        db = str(tmp_path / "research.db")
        _make_21d_db(db)
        _backfill_score_returns(db, dry_run=False)
        out = _backfill_score_returns(db, dry_run=False)
        # Second pass finds nothing NULL → 0 further writes.
        assert out["rows_written"] == 0

    def test_schema_ensure_adds_21d_columns(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(score_performance)"
            ).fetchall()}
        for col in ("price_21d", "return_21d", "spy_21d_return",
                    "beat_spy_21d", "eval_date_21d", "log_alpha_21d"):
            assert col in cols, f"missing {col}"
