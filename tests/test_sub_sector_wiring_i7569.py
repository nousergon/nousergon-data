"""alpha-engine-config-I7569: sub_sector_vs_benchmark_{5,10,20}d regression
for the daily S3 feature snapshot (features/compute.py::compute_and_write).

Root cause (measured live 2026-08-17, 901/901 tickers constant 0.0):
``_load_sub_sector_etf_map`` was defined and loaded ``data/sub_sector_etf_map.json``
correctly, but nothing ever called it inside ``compute_and_write`` — the
``compute_features(...)`` call never passed ``sub_sector_etf_series`` at all,
so it always took the function's ``None`` default and hit
``feature_engineer.compute_features``'s constant-0.0 fallback branch for
every ticker, every day. Separately, ``_extract_macro``'s sector-ETF loop
only matched ``stem.startswith("XL")`` — SMH/IGV/XBI/… (config#934) don't
start with "XL", so even a correct per-ticker resolution would have found
nothing in ``macro`` to resolve to.

Mirrors ``test_compute_momentum_columns_i7539.py``'s synthetic-universe +
dry-run + zero-variance-capture pattern.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from features import compute


def _price_frame(n: int, seed: int, drift: float) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    r = drift + rng.normal(0, 0.01, n)
    close = 100.0 * np.exp(np.cumsum(r))
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {
            "Open": close,
            "High": close * 1.001,
            "Low": close * 0.999,
            "Close": close,
            "Volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
        },
        index=idx,
    )


def test_extract_macro_picks_up_non_xl_sub_sector_etfs():
    """Direct regression on the narrower bug: the XL*-only prefix filter."""
    smh = _price_frame(400, seed=1, drift=0.0005)
    xlk = _price_frame(400, seed=2, drift=0.0003)
    aapl = _price_frame(400, seed=3, drift=0.0002)
    slim_data = {"SMH": smh, "XLK": xlk, "AAPL": aapl}

    macro = compute._extract_macro(price_data={}, slim_data=slim_data)

    assert "SMH" in macro, "sub-sector ETF must be surfaced into macro"
    assert "XLK" in macro, "sector ETF (XL* prefix) must still be surfaced"
    assert "AAPL" not in macro, "a plain stock must not leak into macro"


def _synthetic_universe_with_sub_sectors(n_per_group: int = 15, n_rows: int = 400):
    """Two groups of tickers, each mapped to a DIFFERENT sub-sector ETF with
    its own distinct drift — the precondition for real cross-sectional
    variance in sub_sector_vs_benchmark_*."""
    price_data = {}
    sub_sector_map = {}

    for i in range(n_per_group):
        t = f"SEMI{i:02d}"
        price_data[t] = _price_frame(n_rows, seed=100 + i, drift=0.0006)
        sub_sector_map[t] = "SMH"
    for i in range(n_per_group):
        t = f"SOFT{i:02d}"
        price_data[t] = _price_frame(n_rows, seed=200 + i, drift=-0.0002)
        sub_sector_map[t] = "IGV"

    price_data["SMH"] = _price_frame(n_rows, seed=901, drift=0.0004)
    price_data["IGV"] = _price_frame(n_rows, seed=902, drift=-0.0001)
    spy = _price_frame(n_rows, seed=999, drift=0.0002)["Close"]

    # This test's loader mock (below) replaces _load_prices_and_macro
    # wholesale, so it must hand back what the real function's
    # _extract_macro call would have produced — including SMH/IGV, which is
    # exactly the piece the wiring bug dropped. See
    # test_extract_macro_picks_up_non_xl_sub_sector_etfs for the isolated
    # regression on _extract_macro itself.
    macro = {
        "SPY": spy,
        "SMH": price_data["SMH"]["Close"].dropna(),
        "IGV": price_data["IGV"]["Close"].dropna(),
    }

    return price_data, macro, sub_sector_map


def _patch_loaders(monkeypatch, price_data, macro, sub_sector_map):
    monkeypatch.setattr(
        compute, "_load_prices_and_macro",
        lambda s3, bucket, date_str, **_kw: (dict(price_data), dict(macro)),
    )
    monkeypatch.setattr(compute, "_load_sector_map", lambda s3, bucket: {})
    monkeypatch.setattr(
        compute, "_load_sub_sector_etf_map", lambda s3, bucket: dict(sub_sector_map)
    )
    monkeypatch.setattr(
        compute, "_load_cached_fundamentals", lambda s3, bucket, date_str: {}
    )
    monkeypatch.setattr(compute, "_load_cached_alternative", lambda s3, bucket: {})


def test_sub_sector_vs_benchmark_carries_real_variance(monkeypatch):
    """Full pipeline, dry-run. SMH/IGV must reach `macro` via _extract_macro
    (not just be present in price_data), and compute_and_write must resolve
    + pass sub_sector_etf_series per ticker for the value to be non-constant."""
    price_data, macro, sub_sector_map = _synthetic_universe_with_sub_sectors()
    _patch_loaders(monkeypatch, price_data, macro, sub_sector_map)

    captured = {}

    def _capture_guard(features_df, feature_names, **kwargs):
        captured["features_df"] = features_df.copy()

    # Renamed when the guard grew its all-null half (I7539).
    monkeypatch.setattr(compute, "assert_no_dead_feature_columns", _capture_guard)

    result = compute.compute_and_write(
        date_str="2026-08-14", bucket="test-bucket", dry_run=True
    )

    assert result["status"] == "ok"
    assert result["tickers_computed"] >= 20

    df = captured["features_df"]
    for col in (
        "sub_sector_vs_benchmark_5d",
        "sub_sector_vs_benchmark_10d",
        "sub_sector_vs_benchmark_20d",
    ):
        non_null = df[col].dropna()
        assert len(non_null) >= 20, f"{col}: too few non-null values ({len(non_null)})"
        assert non_null.nunique() > 1, f"{col}: still constant — {non_null.unique()[:5]}"
        assert non_null.std() > 0.0, f"{col}: still zero cross-sectional variance"

    # The two groups' means should differ — SMH-mapped and IGV-mapped
    # tickers have deliberately different drift relative to their benchmark.
    semi_mean = df.loc[df["ticker"].str.startswith("SEMI"), "sub_sector_vs_benchmark_5d"].mean()
    soft_mean = df.loc[df["ticker"].str.startswith("SOFT"), "sub_sector_vs_benchmark_5d"].mean()
    assert semi_mean != soft_mean
