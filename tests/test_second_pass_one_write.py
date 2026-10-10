"""alpha-engine-config-I11792 — the cross-sectional second pass writes ONCE.

The 2026-10-08..10 `alpha-engine-research` access logs showed every universe
symbol getting three ArcticDB versions per daily_append burst: the per-ticker
write, then one `update_batch` per cross-sectional pass (factor momentum, then
the factor-loading z-scores). `features.second_pass.update_cross_sectional_latest`
writes both passes in one `update_batch`. These tests pin that it is ONE write
per symbol, and that the row it leaves is the row the two sequential writes
left.
"""
from __future__ import annotations

import copy

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("arcticdb")

from features.cross_sectional import (  # noqa: E402
    FACTOR_LOADING_SOURCES,
    factor_loading_columns,
    update_factor_loading_zscores_latest,
)
from features.factor_momentum import (  # noqa: E402
    DEFAULT_FACTOR_LOADINGS,
    update_factor_momentum_latest,
)
from features.second_pass import update_cross_sectional_latest  # noqa: E402


class _Result:
    def __init__(self, data):
        self.data = data


class _CountingLib:
    """read_batch/update_batch over in-memory frames, counting every write
    per symbol — each `update_batch` payload is one new ArcticDB version."""

    def __init__(self, frames: dict[str, pd.DataFrame]) -> None:
        self.store = {t: df.copy() for t, df in frames.items()}
        self.update_batch_calls = 0
        self.versions: dict[str, int] = {t: 0 for t in frames}
        self.fail_update = False

    def read_batch(self, reqs):
        out = []
        for req in reqs:
            df = self.store.get(req.symbol)
            if df is None:
                out.append(_Result(None))
                continue
            lo, hi = req.date_range
            df = df.loc[(df.index >= lo) & (df.index <= hi)]
            cols = getattr(req, "columns", None)
            if cols:
                df = df[[c for c in cols if c in df.columns]]
            out.append(_Result(df.copy()))
        return out

    def update_batch(self, payloads):
        self.update_batch_calls += 1
        if self.fail_update:
            raise RuntimeError("simulated update_batch failure")
        for p in payloads:
            cur = self.store[p.symbol].copy()
            for idx in p.data.index:
                for c in p.data.columns:
                    cur.loc[idx, c] = p.data.loc[idx, c]
            self.store[p.symbol] = cur
            self.versions[p.symbol] += 1


def _frames(n_tickers: int = 30, n_days: int = 420, seed: int = 7):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2024-01-01", periods=n_days)
    raw_cols = sorted(set(DEFAULT_FACTOR_LOADINGS) | set(FACTOR_LOADING_SOURCES))
    out_cols = ["factor_momentum_ratio", *factor_loading_columns()]
    frames = {}
    for i in range(n_tickers):
        df = pd.DataFrame(index=dates)
        df["Close"] = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n_days)))
        for c in raw_cols:
            df[c] = rng.normal(0, 1, n_days) + (5.0 if c == "market_cap_raw" else 0.0)
        for c in out_cols:
            df[c] = np.float32(np.nan)  # daily_append's schema-align placeholder
        frames[f"T{i:02d}"] = df
    return frames, dates[-1], out_cols


def test_both_passes_land_in_one_version_per_symbol():
    frames, as_of, _ = _frames()
    lib = _CountingLib(frames)
    fm, flz = update_cross_sectional_latest(lib, list(frames), as_of)
    assert lib.update_batch_calls == 1
    assert set(lib.versions.values()) == {1}, "each symbol must get exactly one new version"
    assert fm["status"] == "ok" and fm["tickers_written"] == len(frames)
    assert flz["status"] == "ok" and flz["tickers_written"] == len(frames)


def test_the_one_write_leaves_the_row_the_two_writes_left():
    frames, as_of, out_cols = _frames()
    merged = _CountingLib(frames)
    sequential = _CountingLib(copy.deepcopy(frames))
    fm, flz = update_cross_sectional_latest(merged, list(frames), as_of)
    fm_old = update_factor_momentum_latest(sequential, list(frames), as_of)
    flz_old = update_factor_loading_zscores_latest(sequential, list(frames), as_of)

    assert sequential.update_batch_calls == 2
    assert fm == fm_old
    assert flz == flz_old
    for t in frames:
        a = merged.store[t].loc[as_of, out_cols].astype(float).to_numpy()
        b = sequential.store[t].loc[as_of, out_cols].astype(float).to_numpy()
        np.testing.assert_array_equal(a, b)
        assert np.isfinite(a).any()
        # earlier rows untouched
        pd.testing.assert_frame_equal(merged.store[t].iloc[:-1], frames[t].iloc[:-1])


def test_a_disabled_pass_writes_only_the_other():
    frames, as_of, _ = _frames()
    lib = _CountingLib(frames)
    fm, flz = update_cross_sectional_latest(lib, list(frames), as_of, factor_momentum=False)
    assert fm is None
    assert flz["tickers_written"] == len(frames)
    assert lib.update_batch_calls == 1
    assert lib.store["T00"][["factor_momentum_ratio"]].isna().all().all()


def test_both_disabled_reads_and_writes_nothing():
    frames, as_of, _ = _frames(n_days=5)
    lib = _CountingLib(frames)
    assert update_cross_sectional_latest(
        lib, list(frames), as_of, factor_momentum=False, factor_loading_zscores=False
    ) == (None, None)
    assert lib.update_batch_calls == 0


def test_a_failed_write_is_reported_per_pass_and_never_raises():
    frames, as_of, _ = _frames()
    lib = _CountingLib(frames)
    lib.fail_update = True
    fm, flz = update_cross_sectional_latest(lib, list(frames), as_of)
    assert fm["tickers_written"] == 0 and fm["write_fail"] == len(frames)
    assert flz["tickers_written"] == 0 and flz["write_fail"] == len(frames)
