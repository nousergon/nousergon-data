"""alpha-engine-config-I7539: end-to-end regression for the daily S3 feature
snapshot (features/compute.py::compute_and_write).

Both root causes were measured on the LIVE 2026-08-14 snapshot:

  - residual_momentum_ratio: computed per-ticker, but _FEATURE_WARMUP_ROWS
    (280) trimmed price history BELOW the 312-row minimum the stacked
    beta_60d(60) -> cum_resid(231) -> shift(21) window needs for the latest
    row to be non-NaN. Every ticker's value was NaN, and the FEATURES-loop
    fallback (`row[f] = ... else 0.0`) turned universe-wide NaN into
    universe-wide constant 0.0.
  - factor_momentum_ratio: a cross-sectional-time-series SECOND PASS
    (features/factor_momentum.py) that nothing in features/compute.py's S3
    daily-snapshot path ever ran — it is only wired into
    builders/backfill.py (full 10y) and builders/daily_append.py's ArcticDB
    go-forward path (features/factor_momentum.py::update_factor_momentum_latest).
    The S3 snapshot writer never read that value back, so the column was
    never in `latest.index` and hit the same 0.0 fallback.

This test builds a synthetic universe with genuinely different per-ticker
drift (so beta/idio-vol/momentum loadings differ across tickers — the
precondition for both columns to carry real cross-sectional variance), runs
the FULL compute_and_write pipeline in dry-run mode (skips only the S3
write itself), and relies on the postflight zero-variance guard
(features/postflight.py, wired into compute_and_write just before the
dry-run branch) to fail the test loud if either column comes back constant.
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


def _synthetic_universe(n_tickers: int = 30, n_rows: int = 400):
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    price_data = {}
    for i, t in enumerate(tickers):
        # Spread of per-ticker drifts -> real cross-sectional dispersion in
        # beta_60d / momentum_20d / idio_vol_60d, the inputs both columns
        # under test are built from.
        drift = 0.0003 * (i - n_tickers / 2)
        price_data[t] = _price_frame(n_rows, seed=i, drift=drift)
    spy = _price_frame(n_rows, seed=999, drift=0.0002)["Close"]
    return price_data, {"SPY": spy}


def _patch_loaders(monkeypatch, price_data, macro):
    monkeypatch.setattr(
        compute, "_load_prices_and_macro",
        lambda s3, bucket, date_str, **_kw: (dict(price_data), dict(macro)),
    )
    monkeypatch.setattr(compute, "_load_sector_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_sub_sector_etf_map", lambda s3, bucket: {})
    monkeypatch.setattr(
        compute, "_load_cached_fundamentals", lambda s3, bucket, date_str: {}
    )
    monkeypatch.setattr(compute, "_load_cached_alternative", lambda s3, bucket: {})


def test_warmup_rows_covers_the_stacked_residual_momentum_window():
    """312 = 60 (beta_60d warmup) + 231 (cum-residual window) + 21 (skip
    shift) - 1 is the derived minimum for the LATEST row to be non-NaN
    (alpha-engine-config-I7539). _FEATURE_WARMUP_ROWS must stay above it."""
    assert compute._FEATURE_WARMUP_ROWS >= 312


def test_residual_and_factor_momentum_carry_real_variance(monkeypatch):
    """Full pipeline, dry-run. This synthetic universe has no sector map,
    fundamentals, or alt data, so those groups legitimately default to a
    shared neutral constant (the real postflight guard would — correctly —
    flag THOSE here too; features.postflight has its own isolated unit
    tests in test_postflight_zero_variance.py). This test isolates the two
    columns this issue is about: capture the assembled features_df by
    intercepting the guard call instead of letting it run against the full
    FEATURES list, and assert directly that residual_momentum_ratio and
    factor_momentum_ratio — the two columns measured constant 0.0 in
    production — carry real cross-sectional variance here."""
    price_data, macro = _synthetic_universe()
    _patch_loaders(monkeypatch, price_data, macro)

    captured = {}

    def _capture_guard(features_df, feature_names, **kwargs):
        captured["features_df"] = features_df.copy()

    # Renamed when the guard grew its all-null half (I7539, second
    # part): one entry point now covers "constant" AND "empty".
    monkeypatch.setattr(compute, "assert_no_dead_feature_columns", _capture_guard)

    result = compute.compute_and_write(
        date_str="2026-08-14", bucket="test-bucket", dry_run=True
    )

    assert result["status"] == "ok"
    assert result["tickers_computed"] >= 20

    df = captured["features_df"]
    for col in ("residual_momentum_ratio", "factor_momentum_ratio"):
        non_null = df[col].dropna()
        assert len(non_null) >= 20, f"{col}: too few non-null values ({len(non_null)})"
        assert non_null.nunique() > 1, f"{col}: still constant — {non_null.unique()}"
        assert non_null.std() > 0.0, f"{col}: still zero cross-sectional variance"


def test_an_uncomputed_factor_momentum_stays_nan_never_a_fabricated_zero(monkeypatch):
    """0.0 is a LEGAL factor-momentum reading, so back-filling it makes an
    uncomputed ticker indistinguishable from one whose tilt really is zero
    (alpha-engine-config-I7539).

    Measured 2026-08-18: `features/2026-08-18/technical.parquet` carried 0.0
    for all 901 tickers while ArcticDB's daily second pass held real values
    for the same date (AAPL -0.049, MSFT +0.069, XOM -0.101). Two stores
    disagreeing about one registered column, with the S3 side fabricating the
    disagreement.

    RED before this change: the `.fillna(0.0)` made every unmapped ticker 0.0.
    """
    price_data, macro = _synthetic_universe()
    _patch_loaders(monkeypatch, price_data, macro)

    captured = {}

    def _capture_guard(features_df, feature_names, **kwargs):
        captured["features_df"] = features_df.copy()

    monkeypatch.setattr(compute, "assert_no_dead_feature_columns", _capture_guard)
    # An empty signal map: the pass ran and resolved nothing, which is the
    # live shape this issue is about.
    monkeypatch.setattr(
        compute, "compute_factor_momentum_feature",
        lambda panel: pd.Series([float("nan")] * len(panel), index=panel.index),
    )

    compute.compute_and_write(date_str="2026-08-14", bucket="test-bucket", dry_run=True)

    col = captured["features_df"]["factor_momentum_ratio"]
    assert col.isna().all(), (
        "an uncomputed factor_momentum_ratio must be absent, not 0.0 — a "
        f"fabricated zero is unfalsifiable downstream. Got: {col.dropna().unique()[:5]}"
    )
