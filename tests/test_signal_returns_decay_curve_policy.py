"""config#1981 — alpha-decay-curve intermediate-horizon policy wiring.

Operator ruling "Option A" (2026-07-16): add intermediate-horizon columns to
the producer-side return series rather than have the backtester join raw
daily prices itself. The mechanism is a LOCAL ``HorizonPolicy`` override
(``_DECAY_CURVE_POLICY`` in collectors/signal_returns.py) passed into
``_backfill_outcome_records`` — the same override shape
``test_signal_returns_outcome_records.py``'s ``TestAddAHorizonAcceptance``
class already proves works end-to-end (a horizon is a DATA change: one extra
``score_performance_outcomes`` row, zero schema change). This file covers:

  - the module-level policy constant carries the expected primary + the
    decay-curve diagnostic horizons (1/3/5/10/15) alongside the fleet
    DEFAULT_POLICY's primary (21d, unchanged)
  - ``_backfill_outcome_records`` writes ALL of those horizons' rows when
    called with ``_DECAY_CURVE_POLICY`` and universe_returns has their columns
  - a horizon whose universe_returns columns are absent (not yet produced by
    this PR's universe_returns migration on an old row) is gracefully
    skipped, not an error — the HorizonPolicy contract's diagnostic
    graceful-empty behavior
  - the fleet-wide DEFAULT_POLICY itself is untouched by this change (only a
    local override in this producer's call site)
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from collectors.signal_returns import (
    _DECAY_CURVE_DIAGNOSTIC_HORIZONS,
    _DECAY_CURVE_POLICY,
    _backfill_outcome_records,
)
from nousergon_lib.quant.horizons import DEFAULT_POLICY

_RESOLVED_AT = "2026-07-20T00:00:00+00:00"


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        yield str(Path(d) / "research.db")


def _seed_full_ladder(db: str, *, symbol="AAPL", score_date="2026-03-02"):
    """Seed a score_performance row + a universe_returns row carrying the
    full decay-curve ladder (1/3/5/10/15/21d) so every diagnostic horizon in
    _DECAY_CURVE_POLICY resolves."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE score_performance (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, score_date TEXT NOT NULL,
                score REAL NOT NULL, price_on_date REAL, UNIQUE(symbol, score_date)
            )
            """
        )
        cols = ["id INTEGER PRIMARY KEY AUTOINCREMENT", "ticker TEXT", "eval_date TEXT"]
        for h in (1, 3, 5, 10, 15, 21):
            cols += [f"return_{h}d REAL", f"spy_return_{h}d REAL", f"beat_spy_{h}d INTEGER"]
        cols += ["log_return_21d REAL", "log_spy_return_21d REAL"]
        conn.execute(f"CREATE TABLE universe_returns ({', '.join(cols)}, UNIQUE(ticker, eval_date))")

        conn.execute(
            "INSERT INTO score_performance (symbol, score_date, score, price_on_date) VALUES (?,?,?,?)",
            (symbol, score_date, 80.0, 100.0),
        )
        ur_cols = ["ticker", "eval_date"]
        ur_vals = [symbol, score_date]
        # Monotonically increasing returns so a decay-curve assertion has
        # something meaningful to check against.
        per_horizon_return = {1: 0.002, 3: 0.006, 5: 0.011, 10: 0.018, 15: 0.025, 21: 0.043}
        per_horizon_spy = {1: 0.001, 3: 0.003, 5: 0.008, 10: 0.012, 15: 0.016, 21: 0.021}
        for h in (1, 3, 5, 10, 15, 21):
            ur_cols += [f"return_{h}d", f"spy_return_{h}d", f"beat_spy_{h}d"]
            ur_vals += [per_horizon_return[h], per_horizon_spy[h], 1]
        ur_cols += ["log_return_21d", "log_spy_return_21d"]
        ur_vals += [0.0421, 0.0208]
        conn.execute(
            f"INSERT INTO universe_returns ({', '.join(ur_cols)}) VALUES ({', '.join('?' * len(ur_vals))})",
            ur_vals,
        )
        conn.commit()


def _fetch(db: str) -> dict[int, dict]:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return {
            r["horizon_days"]: dict(r)
            for r in conn.execute("SELECT * FROM score_performance_outcomes").fetchall()
        }


class TestDecayCurvePolicyShape:
    def test_diagnostic_horizons_include_the_new_intermediate_points(self):
        assert set(_DECAY_CURVE_DIAGNOSTIC_HORIZONS) == {1, 3, 5, 10, 15}

    def test_primary_horizon_unchanged_from_fleet_default(self):
        assert _DECAY_CURVE_POLICY.primary_horizon == DEFAULT_POLICY.primary_horizon == 21

    def test_fleet_default_policy_itself_is_untouched(self):
        # config#1981 must NOT widen the fleet-wide DEFAULT_POLICY consumed by
        # the predictor/evaluator/executor — only this producer's local call
        # site opts into the wider diagnostic set.
        assert DEFAULT_POLICY.diagnostic_horizons == (5,)

    def test_all_horizons_is_primary_plus_decay_ladder(self):
        assert set(_DECAY_CURVE_POLICY.all_horizons) == {1, 3, 5, 10, 15, 21}


class TestBackfillWithDecayCurvePolicy:
    def test_writes_a_row_for_every_ladder_horizon(self, tmp_db):
        _seed_full_ladder(tmp_db)
        res = _backfill_outcome_records(
            tmp_db, dry_run=False, resolved_at=_RESOLVED_AT, policy=_DECAY_CURVE_POLICY,
        )
        assert res["status"] == "ok"
        rows = _fetch(tmp_db)
        assert set(rows) == {1, 3, 5, 10, 15, 21}

    def test_only_the_primary_horizon_carries_log_alpha(self, tmp_db):
        _seed_full_ladder(tmp_db)
        _backfill_outcome_records(
            tmp_db, dry_run=False, resolved_at=_RESOLVED_AT, policy=_DECAY_CURVE_POLICY,
        )
        rows = _fetch(tmp_db)
        assert rows[21]["is_primary"] == 1
        assert rows[21]["log_alpha"] is not None
        for h in (1, 3, 5, 10, 15):
            assert rows[h]["is_primary"] == 0
            assert rows[h]["log_alpha"] is None

    def test_stock_return_increases_with_horizon_forms_a_curve(self, tmp_db):
        # Not a claim about real market behavior — just verifies the plumbing
        # carries distinct per-horizon values through to the store rather than
        # collapsing them (e.g. via a horizon-column lookup bug), which is
        # what actually makes this a CURVE rather than a flat line.
        _seed_full_ladder(tmp_db)
        _backfill_outcome_records(
            tmp_db, dry_run=False, resolved_at=_RESOLVED_AT, policy=_DECAY_CURVE_POLICY,
        )
        rows = _fetch(tmp_db)
        returns_by_horizon = [rows[h]["stock_return"] for h in (1, 3, 5, 10, 15, 21)]
        assert returns_by_horizon == sorted(returns_by_horizon)
        assert len(set(returns_by_horizon)) == 6  # all distinct — a real curve, not 2 points

    def test_missing_intermediate_horizon_columns_gracefully_skipped(self, tmp_db):
        # Simulates an OLD score_performance row whose universe_returns record
        # predates this PR's 1d/3d/15d columns (10d/5d/21d present, the new
        # ones absent) — the policy still declares them, but the producer
        # gracefully skips what it can't resolve rather than erroring.
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(
                """
                CREATE TABLE score_performance (
                    id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, score_date TEXT NOT NULL,
                    score REAL NOT NULL, price_on_date REAL, UNIQUE(symbol, score_date)
                )
                """
            )
            conn.execute(
                "CREATE TABLE universe_returns (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "ticker TEXT, eval_date TEXT, "
                "return_5d REAL, spy_return_5d REAL, beat_spy_5d INTEGER, "
                "return_10d REAL, spy_return_10d REAL, beat_spy_10d INTEGER, "
                "return_21d REAL, spy_return_21d REAL, beat_spy_21d INTEGER, "
                "log_return_21d REAL, log_spy_return_21d REAL, "
                "UNIQUE(ticker, eval_date))"
            )
            conn.execute(
                "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES ('AAPL','2026-01-05',80,100)"
            )
            conn.execute(
                "INSERT INTO universe_returns (ticker, eval_date, return_5d, spy_return_5d, "
                "beat_spy_5d, return_10d, spy_return_10d, beat_spy_10d, return_21d, "
                "spy_return_21d, beat_spy_21d, log_return_21d, log_spy_return_21d) "
                "VALUES ('AAPL','2026-01-05',0.011,0.008,1,0.018,0.012,1,0.043,0.021,1,0.0421,0.0208)"
            )
            conn.commit()

        res = _backfill_outcome_records(
            tmp_db, dry_run=False, resolved_at=_RESOLVED_AT, policy=_DECAY_CURVE_POLICY,
        )
        assert res["status"] == "ok"
        rows = _fetch(tmp_db)
        # 1d/3d/15d columns don't exist on this legacy-shaped universe_returns
        # row → gracefully absent; 5d/10d/21d (present) still write.
        assert set(rows) == {5, 10, 21}
        assert 1 not in rows
        assert 3 not in rows
        assert 15 not in rows
