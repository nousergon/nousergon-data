"""Tests for the config#934 sub-sector ETF-relative feature slice.

Covers the two new pieces added in the sub-sector forward step:

  1. ``collectors.constituents.GICS_SUBINDUSTRY_TO_ETF`` resolution and the
     sector-ETF FALLBACK (via ``_build_sub_sector_etf_map``) for a
     sub-industry with no liquid proxy — the mapped case resolves to the
     sub-sector ETF (e.g. Semiconductors -> SMH), the unmapped case falls
     back to the ticker's existing sector ETF.

  2. The ``sub_sector_vs_benchmark_{5,10,20}d`` compute in
     ``features.feature_engineer.compute_features``: correct values on a
     synthetic price series, AND equality with ``sector_vs_spy_*`` when the
     sub-sector ETF series IS the sector ETF series (the fallback case the
     map produces for unmapped sub-industries).

Refs nousergon/alpha-engine-config#934.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.constituents import (
    GICS_SUBINDUSTRY_TO_ETF,
    _build_sub_sector_etf_map,
)
from features.feature_engineer import FEATURES, compute_features


# ── (a) sub_sector_etf_map resolution + sector-ETF fallback ──────────────────


def test_gics_subindustry_map_covers_mandated_proxies():
    """The curated map must carry the mandated liquid sub-sector proxies."""
    assert GICS_SUBINDUSTRY_TO_ETF["Semiconductors"] == "SMH"
    assert GICS_SUBINDUSTRY_TO_ETF["Semiconductor Materials & Equipment"] == "SMH"
    assert GICS_SUBINDUSTRY_TO_ETF["Application Software"] == "IGV"
    assert GICS_SUBINDUSTRY_TO_ETF["Systems Software"] == "IGV"
    assert GICS_SUBINDUSTRY_TO_ETF["Biotechnology"] == "XBI"
    assert GICS_SUBINDUSTRY_TO_ETF["Pharmaceuticals"] == "PPH"
    assert GICS_SUBINDUSTRY_TO_ETF["Oil & Gas Exploration & Production"] == "XOP"
    assert GICS_SUBINDUSTRY_TO_ETF["Regional Banks"] == "KRE"
    assert GICS_SUBINDUSTRY_TO_ETF["Aerospace & Defense"] == "ITA"
    assert GICS_SUBINDUSTRY_TO_ETF["Gold"] == "GDX"


def test_build_sub_sector_etf_map_resolution_and_fallback():
    """Mapped sub-industries resolve to their sub-sector ETF; unmapped ones
    (and tickers with no sub-industry captured) fall back to the sector ETF."""
    tickers = ["NVDA", "MSFT", "JPM", "APD", "NOSUB"]
    sector_etf_map = {
        "NVDA": "XLK",   # Information Technology
        "MSFT": "XLK",   # Information Technology
        "JPM": "XLF",    # Financials
        "APD": "XLB",    # Materials (unmapped sub-industry → sector fallback)
        "NOSUB": "XLI",  # no sub-industry captured at all → sector fallback
    }
    sub_industry_map = {
        "NVDA": "Semiconductors",             # → SMH
        "MSFT": "Systems Software",           # → IGV
        "JPM": "Regional Banks",              # → KRE
        "APD": "Industrial Gases",            # unmapped → XLB fallback
        # NOSUB deliberately absent from sub_industry_map
    }

    result = _build_sub_sector_etf_map(tickers, sector_etf_map, sub_industry_map)

    # Mapped sub-industries → sub-sector ETF
    assert result["NVDA"] == "SMH"
    assert result["MSFT"] == "IGV"
    assert result["JPM"] == "KRE"
    # Unmapped sub-industry → sector-ETF fallback (feature == sector-relative)
    assert result["APD"] == "XLB"
    # No sub-industry at all → sector-ETF fallback
    assert result["NOSUB"] == "XLI"


def test_build_sub_sector_etf_map_omits_ticker_with_no_sector_etf():
    """A ticker with neither a sub-industry proxy nor a sector ETF is simply
    omitted (best-effort/additive — never raises)."""
    result = _build_sub_sector_etf_map(
        ["GHOST"], sector_etf_map={}, sub_industry_map={}
    )
    assert "GHOST" not in result
    assert result == {}


# ── (b) sub_sector_vs_benchmark_* compute ────────────────────────────────────


def _synthetic_ohlcv(n: int = 300, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    daily_returns = rng.normal(0.0004, 0.012, n)
    close = 100.0 * np.exp(np.cumsum(daily_returns))
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)
    idx = pd.date_range("2022-01-03", periods=n, freq="B")
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=idx,
    )


def _series_like(df: pd.DataFrame, seed: int) -> pd.Series:
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0003, 0.010, len(df))
    return pd.Series(50.0 * np.exp(np.cumsum(rets)), index=df.index)


def test_sub_sector_features_present_in_emit_list():
    """The three new columns must be registered in the FEATURES emit list."""
    for col in (
        "sub_sector_vs_benchmark_5d",
        "sub_sector_vs_benchmark_10d",
        "sub_sector_vs_benchmark_20d",
    ):
        assert col in FEATURES


def test_sub_sector_vs_benchmark_matches_manual_math():
    """Compute matches the explicit sub_sector_ETF_return − SPY_return math
    over each horizon (mirrors the sector_vs_spy_* definition)."""
    df = _synthetic_ohlcv(seed=1)
    spy = _series_like(df, seed=2)
    subsec = _series_like(df, seed=3)

    out = compute_features(df, spy_series=spy, sub_sector_etf_series=subsec)

    spy_aligned = spy.reindex(df.index)
    sub_aligned = subsec.reindex(df.index)
    for horizon, col in ((5, "5d"), (10, "10d"), (20, "20d")):
        expected = (
            (sub_aligned / sub_aligned.shift(horizon) - 1.0)
            - (spy_aligned / spy_aligned.shift(horizon) - 1.0)
        )
        got = out[f"sub_sector_vs_benchmark_{col}"]
        pd.testing.assert_series_equal(
            got, expected, check_names=False,
        )


def test_sub_sector_equals_sector_when_etf_is_sector_fallback():
    """FALLBACK case: when the sub-sector ETF series IS the sector ETF series
    (what the map produces for an unmapped sub-industry), the
    sub_sector_vs_benchmark_* columns must equal sector_vs_spy_* exactly."""
    df = _synthetic_ohlcv(seed=4)
    spy = _series_like(df, seed=5)
    sector_etf = _series_like(df, seed=6)

    out = compute_features(
        df,
        spy_series=spy,
        sector_etf_series=sector_etf,
        sub_sector_etf_series=sector_etf,  # fallback: same series as sector
    )

    for col in ("5d", "10d", "20d"):
        pd.testing.assert_series_equal(
            out[f"sub_sector_vs_benchmark_{col}"],
            out[f"sector_vs_spy_{col}"],
            check_names=False,
        )


def test_sub_sector_neutral_default_when_series_absent():
    """No sub-sector ETF series → columns neutral-default to 0.0 (never NaN,
    never raises), matching the sector_vs_spy_* None-input branch."""
    df = _synthetic_ohlcv(seed=7)
    spy = _series_like(df, seed=8)

    out = compute_features(df, spy_series=spy, sub_sector_etf_series=None)

    for col in ("5d", "10d", "20d"):
        assert (out[f"sub_sector_vs_benchmark_{col}"] == 0.0).all()
