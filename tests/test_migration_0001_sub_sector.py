"""Tests for migrations/0001_add_sub_sector_features.py (config#934 re-land).

Covers the RECOMPUTE backfill policy's pure compute logic
(``build_new_columns_fn``) without touching ArcticDB or the network:

  1. Column-set framing: ``COLUMNS_AFTER`` equals the live code-derived
     canonical schema (the exact invariant the chokepoint enforces).
  2. Fallback case: a ticker whose mapped sub-sector ETF equals its sector
     ETF gets its already-persisted ``sector_vs_spy_*`` copied verbatim.
  3. Distinct-ETF case: recompute is BIT-IDENTICAL to what
     ``features.feature_engineer.compute_features`` would have produced for
     the same OHLCV/ETF/SPY history — the strongest correctness argument for
     a RECOMPUTE backfill (the migration must not silently diverge from the
     production formula it mirrors).
  4. Fail-loud (not silent-degrade) when a required ETF's history is missing.

``migrations._base.rewrite_symbols_full``'s ``new_columns_fn`` wiring itself
is exercised end-to-end (against a seeded LMDB ArcticDB library) in
``tests/test_schema_migration_framework.py``, which requires the real
``arcticdb`` package.
"""

from __future__ import annotations

import importlib.util
import os

import numpy as np
import pandas as pd
import pytest

from features.feature_engineer import FEATURES, compute_features
from migrations._base import Migration, MigrationError

# migrations/0001_*.py is a normally-discovered module (migrations.__init__
# imports it via pkgutil at package-import time), but this test file wants
# BOTH the module object (to reach build_new_columns_fn / COLUMNS_AFTER
# directly) and to not force arcticdb to be importable just to read it — its
# module-level code has no arcticdb dependency, so a direct file-load works
# standalone even in an environment without arcticdb installed.
_MODULE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "migrations",
    "0001_add_sub_sector_features.py",
)
_spec = importlib.util.spec_from_file_location("migration_0001_under_test", _MODULE_PATH)
migration_0001 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration_0001)


def _synthetic_ohlcv(n: int = 300, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    daily_returns = rng.normal(0.0004, 0.012, n)
    close = 100.0 * np.exp(np.cumsum(daily_returns))
    idx = pd.date_range("2022-01-03", periods=n, freq="B")
    return pd.DataFrame({"Close": close}, index=idx)


def _series_like(df: pd.DataFrame, seed: int) -> pd.Series:
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0003, 0.010, len(df))
    return pd.Series(50.0 * np.exp(np.cumsum(rets)), index=df.index)


# ── (1) COLUMNS_AFTER framing ────────────────────────────────────────────────


def test_columns_after_matches_live_features_list():
    """COLUMNS_AFTER must equal OHLCV + source + the live FEATURES list —
    the exact invariant tests/test_schema_migration_chokepoint.py enforces.
    A drift here means this migration's frozen declaration is already stale."""
    expected = ("Open", "High", "Low", "Close", "Volume", "VWAP", "source") + tuple(
        FEATURES
    )
    assert migration_0001.COLUMNS_AFTER == expected


def test_new_columns_immediately_follow_sector_vs_spy_20d():
    cols = migration_0001.COLUMNS_AFTER
    i = cols.index("sector_vs_spy_20d")
    assert cols[i + 1 : i + 4] == migration_0001.NEW_COLUMNS


def test_migration_object_is_well_formed():
    mig = migration_0001.MIGRATION
    assert isinstance(mig, Migration)
    assert mig.number == 1
    assert mig.schema_version_before == 0
    assert mig.schema_version_after == 1
    assert "RECOMPUTE" in mig.backfill_policy


# ── (2) fallback case: copy sector_vs_spy_* verbatim ─────────────────────────


def test_fallback_copies_sector_vs_spy_verbatim():
    df = pd.DataFrame(
        {
            "sector_vs_spy_5d": [0.01, 0.02, 0.03],
            "sector_vs_spy_10d": [0.04, 0.05, 0.06],
            "sector_vs_spy_20d": [0.07, 0.08, 0.09],
        },
        index=pd.date_range("2024-01-02", periods=3, freq="B"),
    )
    new_columns_fn = migration_0001.build_new_columns_fn(
        sub_sector_etf_map={"NOSUB": "XLI"},
        sector_etf_map={"NOSUB": "XLI"},  # mapped ETF == sector ETF -> fallback
        etf_close={},
    )
    out = new_columns_fn("NOSUB", df)
    assert set(out) == set(migration_0001.NEW_COLUMNS)
    for new, old in zip(migration_0001.NEW_COLUMNS, migration_0001._SECTOR_COLS):
        pd.testing.assert_series_equal(
            out[new], df[old].astype("float32"), check_names=False
        )


def test_fallback_applies_when_ticker_missing_from_sub_sector_map():
    """A ticker absent from sub_sector_etf_map entirely (e.g. no sector ETF
    at all) is also a fallback — mirrors compute_features's None-series
    branch, where sector_vs_spy_* is already the same neutral value."""
    df = pd.DataFrame(
        {
            "sector_vs_spy_5d": [0.0, 0.0],
            "sector_vs_spy_10d": [0.0, 0.0],
            "sector_vs_spy_20d": [0.0, 0.0],
        },
        index=pd.date_range("2024-01-02", periods=2, freq="B"),
    )
    new_columns_fn = migration_0001.build_new_columns_fn(
        sub_sector_etf_map={}, sector_etf_map={}, etf_close={}
    )
    out = new_columns_fn("GHOST", df)
    for col in migration_0001.NEW_COLUMNS:
        assert (out[col] == 0.0).all()


# ── (3) distinct-ETF recompute matches compute_features exactly ─────────────


def test_distinct_etf_recompute_matches_compute_features_exactly():
    """The migration's own momentum-diff math must be bit-identical to
    feature_engineer.compute_features's sub_sector_vs_benchmark_* output for
    the same OHLCV/ETF/SPY inputs — proving the RECOMPUTE backfill does not
    silently diverge from the production formula it re-derives."""
    ohlcv = _synthetic_ohlcv(seed=10)
    spy = _series_like(ohlcv, seed=11)
    smh = _series_like(ohlcv, seed=12)

    expected = compute_features(
        _full_ohlcv_like(ohlcv, seed=10),
        spy_series=spy,
        sub_sector_etf_series=smh,
    )

    new_columns_fn = migration_0001.build_new_columns_fn(
        sub_sector_etf_map={"NVDA": "SMH"},
        sector_etf_map={"NVDA": "XLK"},
        etf_close={"SPY": spy, "SMH": smh},
    )
    got = new_columns_fn(
        "NVDA",
        pd.DataFrame(
            {
                "sector_vs_spy_5d": 0.0,
                "sector_vs_spy_10d": 0.0,
                "sector_vs_spy_20d": 0.0,
            },
            index=ohlcv.index,
        ),
    )
    for col in migration_0001.NEW_COLUMNS:
        pd.testing.assert_series_equal(
            got[col], expected[col].astype("float32"), check_names=False
        )


def _full_ohlcv_like(close_only: pd.DataFrame, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = len(close_only)
    close = close_only["Close"].to_numpy()
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=close_only.index,
    )


# ── (4) fail-loud on missing required history ────────────────────────────────


def test_distinct_etf_missing_history_fails_loud():
    df = pd.DataFrame(
        {
            "sector_vs_spy_5d": [0.0],
            "sector_vs_spy_10d": [0.0],
            "sector_vs_spy_20d": [0.0],
        },
        index=pd.date_range("2024-01-02", periods=1, freq="B"),
    )
    new_columns_fn = migration_0001.build_new_columns_fn(
        sub_sector_etf_map={"NVDA": "SMH"},
        sector_etf_map={"NVDA": "XLK"},
        etf_close={"SPY": pd.Series([1.0], index=df.index)},  # SMH missing
    )
    with pytest.raises(MigrationError, match="SMH"):
        new_columns_fn("NVDA", df)


def test_missing_spy_history_fails_loud_for_any_non_fallback_symbol():
    df = pd.DataFrame(
        {
            "sector_vs_spy_5d": [0.0],
            "sector_vs_spy_10d": [0.0],
            "sector_vs_spy_20d": [0.0],
        },
        index=pd.date_range("2024-01-02", periods=1, freq="B"),
    )
    new_columns_fn = migration_0001.build_new_columns_fn(
        sub_sector_etf_map={"NVDA": "SMH"},
        sector_etf_map={"NVDA": "XLK"},
        etf_close={"SMH": pd.Series([1.0], index=df.index)},  # SPY missing
    )
    with pytest.raises(MigrationError, match="SPY"):
        new_columns_fn("NVDA", df)
