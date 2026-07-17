"""signal_returns long-format outcome dual-write (EPIC config#1483 Phase 2).

Covers ``_backfill_outcome_records`` + ``_ensure_score_performance_outcomes_schema``:
  - creates the long-format score_performance_outcomes table (self-sufficient)
  - dual-writes one row per (signal, score_date, HorizonPolicy horizon) from
    the universe_returns DECIMAL source (not the wide percent columns)
  - every emitted record validates against the nousergon_lib outcome_record
    contract (v1); the canonical primary (21d) carries a non-null log_alpha,
    diagnostics (5d) carry null
  - legacy 10d/30d horizons are NOT carried into the canonical store
  - idempotent re-runs; dry-run writes nothing; graceful skip on absent columns
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from collectors.signal_returns import (
    _backfill_outcome_records,
    _ensure_score_performance_outcomes_schema,
)
from nousergon_lib.contracts import conformance_errors
from nousergon_lib.quant.horizons import (
    DEFAULT_POLICY,
    HorizonPolicy,
    PrimaryHorizonMissing,
)

_RESOLVED_AT = "2026-07-01T00:00:00+00:00"


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        yield str(Path(d) / "research.db")


def _seed(db: str, *, with_log=True, with_5d=True, symbol="AAPL", score_date="2026-03-02"):
    """Seed a score_performance row + its resolved universe_returns row."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE score_performance (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, score_date TEXT NOT NULL,
                score REAL NOT NULL, price_on_date REAL, UNIQUE(symbol, score_date)
            )
            """
        )
        cols = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT", "ticker TEXT", "eval_date TEXT",
            "return_21d REAL", "spy_return_21d REAL", "beat_spy_21d INTEGER",
        ]
        if with_5d:
            cols += ["return_5d REAL", "spy_return_5d REAL", "beat_spy_5d INTEGER"]
        if with_log:
            cols += ["log_return_21d REAL", "log_spy_return_21d REAL"]
        conn.execute(f"CREATE TABLE universe_returns ({', '.join(cols)}, UNIQUE(ticker, eval_date))")
        conn.execute(
            "INSERT INTO score_performance (symbol, score_date, score, price_on_date) VALUES (?,?,?,?)",
            (symbol, score_date, 80.0, 100.0),
        )
        ur_cols = ["ticker", "eval_date", "return_21d", "spy_return_21d", "beat_spy_21d"]
        ur_vals = [symbol, score_date, 0.043, 0.021, 1]
        if with_5d:
            ur_cols += ["return_5d", "spy_return_5d", "beat_spy_5d"]
            ur_vals += [0.011, 0.008, 1]
        if with_log:
            ur_cols += ["log_return_21d", "log_spy_return_21d"]
            ur_vals += [0.0421, 0.0208]
        conn.execute(
            f"INSERT INTO universe_returns ({', '.join(ur_cols)}) VALUES ({', '.join('?' * len(ur_vals))})",
            ur_vals,
        )
        conn.commit()


def _fetch(db: str):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return {
            (r["horizon_days"]): dict(r)
            for r in conn.execute("SELECT * FROM score_performance_outcomes").fetchall()
        }


class TestSchemaEnsure:
    def test_creates_table(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_outcomes_schema(conn)
            cols = {r[1] for r in conn.execute("PRAGMA table_info(score_performance_outcomes)")}
        assert {"signal_id", "horizon_days", "log_alpha", "is_primary", "resolved_at"} <= cols

    def test_idempotent(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_outcomes_schema(conn)
            _ensure_score_performance_outcomes_schema(conn)  # no raise


class TestDualWrite:
    def test_writes_primary_and_diagnostic_rows(self, tmp_db):
        _seed(tmp_db)
        res = _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        assert res["status"] == "ok"
        rows = _fetch(tmp_db)
        # Policy default = primary 21 + diagnostic 5
        assert set(rows) == set(DEFAULT_POLICY.all_horizons) == {21, 5}

    def test_primary_row_carries_canonical_log_alpha(self, tmp_db):
        _seed(tmp_db)
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        primary = _fetch(tmp_db)[21]
        assert primary["is_primary"] == 1
        # log_alpha = log_return_21d - log_spy_return_21d
        assert primary["log_alpha"] == pytest.approx(0.0421 - 0.0208, abs=1e-6)
        # decimals, not percent
        assert primary["stock_return"] == pytest.approx(0.043)
        assert primary["spy_return"] == pytest.approx(0.021)
        assert primary["beat_spy"] == 1

    def test_diagnostic_row_has_null_log_alpha(self, tmp_db):
        _seed(tmp_db)
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        diag = _fetch(tmp_db)[5]
        assert diag["is_primary"] == 0
        assert diag["log_alpha"] is None
        assert diag["stock_return"] == pytest.approx(0.011)

    def test_every_row_conforms_to_contract(self, tmp_db):
        _seed(tmp_db)
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        with sqlite3.connect(tmp_db) as conn:
            conn.row_factory = sqlite3.Row
            for r in conn.execute("SELECT * FROM score_performance_outcomes"):
                record = {
                    "schema_version": r["schema_version"],
                    "signal_id": r["signal_id"],
                    "score_date": r["score_date"],
                    "horizon_days": r["horizon_days"],
                    "beat_spy": bool(r["beat_spy"]),
                    "stock_return": r["stock_return"],
                    "spy_return": r["spy_return"],
                    "log_alpha": r["log_alpha"],
                    "resolved_at": r["resolved_at"],
                    "is_primary": bool(r["is_primary"]),
                }
                assert conformance_errors("outcome_record", record) == []

    def test_legacy_horizons_not_written(self, tmp_db):
        # 10d/30d are legacy — the canonical store must not carry them even if
        # universe_returns has those columns.
        _seed(tmp_db)
        with sqlite3.connect(tmp_db) as conn:
            conn.execute("ALTER TABLE universe_returns ADD COLUMN return_10d REAL")
            conn.execute("ALTER TABLE universe_returns ADD COLUMN spy_return_10d REAL")
            conn.execute("ALTER TABLE universe_returns ADD COLUMN beat_spy_10d INTEGER")
            conn.execute("UPDATE universe_returns SET return_10d=0.02, spy_return_10d=0.01, beat_spy_10d=1")
            conn.commit()
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        assert 10 not in _fetch(tmp_db)

    def test_idempotent_rerun(self, tmp_db):
        _seed(tmp_db)
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at="2026-07-09T00:00:00+00:00")
        rows = _fetch(tmp_db)
        assert len(rows) == 2  # not duplicated
        # first-write resolved_at preserved (INSERT OR IGNORE)
        assert rows[21]["resolved_at"] == _RESOLVED_AT

    def test_dry_run_writes_nothing(self, tmp_db):
        _seed(tmp_db)
        res = _backfill_outcome_records(tmp_db, dry_run=True, resolved_at=_RESOLVED_AT)
        assert res["status"] == "ok"
        assert _fetch(tmp_db) == {}

    def test_primary_skipped_when_log_columns_absent(self, tmp_db):
        # No log columns → primary can't carry canonical alpha → skip primary,
        # still write the diagnostic (which needs no log_alpha).
        _seed(tmp_db, with_log=False)
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        rows = _fetch(tmp_db)
        assert 21 not in rows
        assert 5 in rows

    def test_unresolved_row_skipped(self, tmp_db):
        # score_performance row with no matching universe_returns → nothing written.
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(
                """
                CREATE TABLE score_performance (
                    id INTEGER PRIMARY KEY, symbol TEXT, score_date TEXT,
                    score REAL, price_on_date REAL, UNIQUE(symbol, score_date)
                )
                """
            )
            conn.execute(
                "CREATE TABLE universe_returns (id INTEGER PRIMARY KEY, ticker TEXT, eval_date TEXT, "
                "return_21d REAL, spy_return_21d REAL, beat_spy_21d INTEGER, return_5d REAL, "
                "spy_return_5d REAL, beat_spy_5d INTEGER, log_return_21d REAL, log_spy_return_21d REAL, "
                "UNIQUE(ticker, eval_date))"
            )
            conn.execute("INSERT INTO score_performance (symbol, score_date, score, price_on_date) VALUES ('AAPL','2026-03-02',80,100)")
            conn.commit()
        res = _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT)
        assert res["status"] == "ok"
        assert _fetch(tmp_db) == {}


def _add_10d_universe_columns(db: str, *, resolved: bool = True):
    """Add the 10d universe_returns columns as a DATA change (no producer schema
    edit). ``resolved=False`` leaves them absent, simulating an unproduced horizon."""
    with sqlite3.connect(db) as conn:
        for c, t in (("return_10d", "REAL"), ("spy_return_10d", "REAL"), ("beat_spy_10d", "INTEGER")):
            conn.execute(f"ALTER TABLE universe_returns ADD COLUMN {c} {t}")
        if resolved:
            conn.execute("UPDATE universe_returns SET return_10d=0.02, spy_return_10d=0.01, beat_spy_10d=1")
        conn.commit()


def _store_columns(db: str) -> set[str]:
    with sqlite3.connect(db) as conn:
        return {r[1] for r in conn.execute("PRAGMA table_info(score_performance_outcomes)")}


class TestAddAHorizonAcceptance:
    """EPIC config#1483's testable finish line (config#1550): adding an eval
    horizon is a DATA change — one extra long-store ROW per signal, with ZERO
    schema change and no fleet-wide `_Nd` column rename (the config#1456 bug
    class). The horizon is a HorizonPolicy PARAMETER, not a column name."""

    def test_extra_diagnostic_horizon_is_a_data_change(self, tmp_db):
        # Canonical (default policy 5+21) store columns, for the zero-change assert.
        canonical_db = tmp_db + ".canonical"
        _seed(canonical_db)
        _backfill_outcome_records(canonical_db, dry_run=False, resolved_at=_RESOLVED_AT)
        canonical_cols = _store_columns(canonical_db)

        # Same producer, a policy with an EXTRA diagnostic horizon (10), and the
        # 10d data present in universe_returns.
        _seed(tmp_db)
        _add_10d_universe_columns(tmp_db, resolved=True)
        policy = HorizonPolicy(primary_horizon=21, diagnostic_horizons=(5, 10))

        res = _backfill_outcome_records(
            tmp_db, dry_run=False, resolved_at=_RESOLVED_AT, policy=policy
        )
        assert res["status"] == "ok"

        rows = _fetch(tmp_db)
        assert set(rows) == {5, 10, 21}          # the new 10d row appeared
        assert rows[10]["is_primary"] == 0
        assert rows[10]["log_alpha"] is None      # non-primary → null canonical alpha
        assert rows[10]["stock_return"] == pytest.approx(0.02)  # decimals, not percent
        # ZERO schema change: the store's physical columns are identical to the
        # canonical-policy store's — adding a horizon added a ROW, not a COLUMN.
        assert _store_columns(tmp_db) == canonical_cols

    def test_consumer_policy_filtered_read_returns_new_horizon(self, tmp_db):
        _seed(tmp_db)
        _add_10d_universe_columns(tmp_db, resolved=True)
        policy = HorizonPolicy(primary_horizon=21, diagnostic_horizons=(5, 10))
        _backfill_outcome_records(tmp_db, dry_run=False, resolved_at=_RESOLVED_AT, policy=policy)

        # A consumer filters WHERE horizon_days = :h with :h resolved from policy.
        with sqlite3.connect(tmp_db) as conn:
            got = conn.execute(
                "SELECT stock_return FROM score_performance_outcomes WHERE horizon_days = ?",
                (10,),
            ).fetchall()
        assert len(got) == 1 and got[0][0] == pytest.approx(0.02)

    def test_unproduced_horizon_yields_empty_gracefully(self, tmp_db):
        # Policy declares horizon 10 but universe_returns lacks its columns → the
        # producer skips it (graceful-empty), still writes the produced horizons.
        _seed(tmp_db)
        policy = HorizonPolicy(primary_horizon=21, diagnostic_horizons=(5, 10))
        res = _backfill_outcome_records(
            tmp_db, dry_run=False, resolved_at=_RESOLVED_AT, policy=policy
        )
        assert res["status"] == "ok"
        rows = _fetch(tmp_db)
        assert set(rows) == {5, 21}   # 10 gracefully absent, no crash
        assert 10 not in rows

    def test_missing_primary_raises(self):
        # The canonical-label starvation gate: a resolved horizon set lacking the
        # PRIMARY horizon is a producer-starvation bug and must fail loud, never
        # degrade to a diagnostic-only read (nousergon_lib.quant.horizons contract).
        policy = HorizonPolicy(primary_horizon=21, diagnostic_horizons=(5, 10))
        with pytest.raises(PrimaryHorizonMissing):
            policy.require_primary_present([5, 10])
