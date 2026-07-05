"""ArcticDB second-pass tests for C.1 factor-loading z-scores.

Mirrors test_factor_momentum.py's mock-lib pattern: materialize rewrites full
history; update_factor_loading_zscores_latest patches only the latest date.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from features.cross_sectional import (
    apply_factor_zscores,
    factor_loading_columns,
    factor_loading_source_columns,
    materialize_factor_loading_zscores,
)


def _cross_section_panel(n_tickers: int = 40, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    tickers = [f"T{i:03d}" for i in range(n_tickers)]
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows = []
    for d in dates:
        for t in tickers:
            rows.append({
                "ticker": t,
                "date": d,
                "Close": 100.0 + rng.normal(),
                "momentum_20d": rng.normal(),
                "return_60d": rng.normal(),
                "beta_60d": rng.normal(1.0, 0.2),
                "idio_vol_60d": abs(rng.normal(0.2, 0.05)),
                "realized_vol_63d": abs(rng.normal(0.25, 0.05)),
                "dist_from_52w_high": rng.uniform(-0.3, 0.0),
                "pe_ratio": abs(rng.normal(20, 5)),
                "roe": rng.normal(0.15, 0.05),
                "market_cap_raw": abs(rng.normal(5e9, 1e9)),
            })
    return pd.DataFrame(rows)


class _MockLib:
    class _Item:
        def __init__(self, data):
            self.data = data

    def __init__(self, frames: dict):
        self._store = {t: df.copy() for t, df in frames.items()}

    def read(self, sym):
        return self._Item(self._store[sym].copy())

    def write(self, sym, df):
        self._store[sym] = df.copy()


class _BatchResult:
    def __init__(self, data):
        self.data = data


class _BatchLib:
    def __init__(self, frames: dict):
        self._store = {t: df.copy() for t, df in frames.items()}

    def read_batch(self, reqs):
        out = []
        for req in reqs:
            sym = req.symbol
            if sym not in self._store:
                out.append(_BatchResult(None))
                continue
            df = self._store[sym]
            dr = getattr(req, "date_range", None)
            if dr is not None:
                lo, hi = dr
                if lo is not None:
                    df = df.loc[df.index >= lo]
                if hi is not None:
                    df = df.loc[df.index <= hi]
            cols = getattr(req, "columns", None)
            if cols:
                df = df[[c for c in cols if c in df.columns]]
            out.append(_BatchResult(df.copy()))
        return out

    def update_batch(self, payloads):
        for p in payloads:
            cur = self._store.get(p.symbol)
            if cur is None:
                self._store[p.symbol] = p.data.copy()
                continue
            cur = cur.copy()
            for idx in p.data.index:
                for c in p.data.columns:
                    cur.loc[idx, c] = p.data.loc[idx, c]
            self._store[p.symbol] = cur


def _panel_to_universe_frames(panel: pd.DataFrame) -> dict:
    src = factor_loading_source_columns()
    frames = {}
    for t, grp in panel.groupby("ticker", sort=False):
        g = grp.sort_values("date").set_index("date")
        frame = {"Close": g["Close"].astype(float)}
        for c in src:
            frame[c] = g[c].astype(float)
        frames[t] = pd.DataFrame(frame)
    return frames


def _expected_latest_zscores(panel: pd.DataFrame, as_of) -> pd.DataFrame:
    latest = panel[panel["date"] == as_of].drop(columns=["date", "Close"])
    return apply_factor_zscores(latest)


class TestMaterializeFactorLoadingZscores:
    def test_writes_zscore_columns_consistent_with_pure_fn(self):
        panel = _cross_section_panel(n_tickers=50, seed=3)
        tickers = sorted(panel["ticker"].unique())
        as_of = panel["date"].max()
        lib = _MockLib(_panel_to_universe_frames(panel))

        result = materialize_factor_loading_zscores(lib, tickers)
        assert result["status"] == "ok"
        assert result["tickers_written"] == len(tickers)

        expected = _expected_latest_zscores(panel, as_of).set_index("ticker")
        dst = factor_loading_columns()
        for t in tickers[:5]:
            df = lib.read(t).data
            for col in dst:
                assert col in df.columns
            got = df.loc[as_of, dst].astype(float)
            exp = expected.loc[t, dst].astype(float)
            np.testing.assert_allclose(got, exp, rtol=1e-5, atol=1e-6)

    def test_write_false_computes_but_does_not_write(self):
        panel = _cross_section_panel(n_tickers=30, seed=4)
        tickers = sorted(panel["ticker"].unique())
        lib = _MockLib(_panel_to_universe_frames(panel))
        result = materialize_factor_loading_zscores(lib, tickers, write=False)
        assert result["status"] == "ok"
        assert result["tickers_written"] == 0
        for t in tickers:
            assert "momentum_20d_zscore" not in lib.read(t).data.columns


class TestUpdateFactorLoadingZscoresLatest:
    def test_writes_latest_value_consistent_with_pure_fn(self):
        pytest = __import__("pytest")
        pytest.importorskip("arcticdb")
        from features.cross_sectional import update_factor_loading_zscores_latest

        panel = _cross_section_panel(n_tickers=50, seed=7)
        tickers = sorted(panel["ticker"].unique())
        frames = _panel_to_universe_frames(panel)
        lib = _BatchLib(frames)
        as_of = max(df.index.max() for df in frames.values())

        result = update_factor_loading_zscores_latest(lib, tickers, as_of)
        assert result["status"] == "ok"
        assert result["tickers_written"] == len(tickers)

        expected = _expected_latest_zscores(panel, as_of).set_index("ticker")
        dst = factor_loading_columns()
        t0 = tickers[0]
        got = lib._store[t0].loc[as_of, dst].astype(float)
        exp = expected.loc[t0, dst].astype(float)
        np.testing.assert_allclose(got, exp, rtol=1e-5, atol=1e-6)

    def test_write_false_computes_but_does_not_write(self):
        pytest = __import__("pytest")
        pytest.importorskip("arcticdb")
        from features.cross_sectional import update_factor_loading_zscores_latest

        panel = _cross_section_panel(n_tickers=30, seed=8)
        tickers = sorted(panel["ticker"].unique())
        lib = _BatchLib(_panel_to_universe_frames(panel))
        as_of = max(df.index.max() for df in lib._store.values())
        result = update_factor_loading_zscores_latest(
            lib, tickers, as_of, write=False,
        )
        assert result["status"] == "ok"
        assert result["tickers_written"] == 0
        assert result["n_computed"] == len(tickers)
        assert "momentum_20d_zscore" not in lib._store[tickers[0]].columns
