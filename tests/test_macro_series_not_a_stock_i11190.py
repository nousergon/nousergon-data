"""alpha-engine-config-I11190: a macro series computed as a stock emptied factor_momentum_ratio.

`_load_price_source` merges the ArcticDB `macro` library's `_MACRO_SLIM_KEYS`
symbols into the same dict as the equity universe, and the per-ticker compute
excludes only `_SKIP_TICKERS`. HYOAS (#688, config#939) was added to the first
and never to the second, so a FRED credit-spread index was computed as a stock,
published a row in every snapshot, and joined the factor-momentum second pass's
cross-sectional panel. FRED publishes on days the equity market is shut; each
of those dates carries ONE name, fails `compute_daily_factor_returns`'
min_names gate, and becomes a NaN factor return, and
`rolling(231, min_periods=231)` never again sees 231 consecutive dates.
Measured on the 2026-09-24 inputs: 26 such dates, longest clean run 68, and
factor_momentum_ratio 0/903 — 903/903 with HYOAS excluded.

The second half: D31's manifest reason read "no detail reported" because the
degraded result named its columns only under keys `_DegradedRun` does not read.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from features import compute

_END = pd.Timestamp("2026-09-24")


def _stock(n: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.015, n)))
    idx = pd.bdate_range(end=_END, periods=n)
    return pd.DataFrame(
        {
            "Open": close * (1 + rng.normal(0, 0.002, n)),
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
        },
        index=idx,
    )


def _fred_series(n: int) -> pd.DataFrame:
    """HYOAS's live shape: Close only, business days PLUS days the equity
    market is shut (here, every fifth Saturday — ~one per 25 trading days,
    the density measured live: 26 in a 585-row window)."""
    bdays = pd.bdate_range(end=_END, periods=n)
    shut = pd.date_range(bdays[0], _END, freq="W-SAT")[::5]
    assert len(shut) >= n // 30, "fixture must carry shut-market dates"
    idx = bdays.union(shut)
    rng = np.random.default_rng(7)
    return pd.DataFrame({"Close": 3.0 + np.cumsum(rng.normal(0, 0.02, len(idx)))}, index=idx)


def _universe(n_tickers: int = 25, n_rows: int = 700):
    price_data = {f"T{i:02d}": _stock(n_rows, seed=i) for i in range(n_tickers)}
    spy = _stock(n_rows, seed=999)
    hyoas = _fred_series(n_rows)
    # `_load_prices_and_macro` returns the macro-library frames INSIDE
    # price_data (`{**prices, **macro_frames}`) and ALSO as `macro` series.
    price_data["SPY"] = spy
    price_data["HYOAS"] = hyoas
    macro = {"SPY": spy["Close"], "HYOAS": hyoas["Close"]}
    return price_data, macro


def _patch_loaders(monkeypatch, price_data, macro):
    monkeypatch.setattr(
        compute, "_load_prices_and_macro",
        lambda s3, bucket, date_str, **_kw: (dict(price_data), dict(macro)),
    )
    monkeypatch.setattr(compute, "_load_sector_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_sub_sector_etf_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_cached_fundamentals", lambda s3, bucket, date_str: {})
    monkeypatch.setattr(compute, "_load_cached_alternative", lambda s3, bucket: {})


def test_every_macro_series_the_loader_reads_is_skip_protected():
    missing = sorted(set(compute._MACRO_SLIM_KEYS.values()) - compute._SKIP_TICKERS)
    assert not missing, f"macro series computed as stocks: {missing}"
    assert not compute.admits_universe_write("HYOAS")


def test_a_macro_series_is_not_computed_as_a_stock_and_factor_momentum_fills(monkeypatch):
    """RED before the fix: HYOAS got a feature row, and its shut-market dates
    left factor_momentum_ratio NaN for every ticker in the universe."""
    price_data, macro = _universe()
    _patch_loaders(monkeypatch, price_data, macro)

    build = compute.build_feature_frame(_END.date().isoformat(), "test-bucket", s3=None)
    df = build.features_df

    assert "HYOAS" not in set(df["ticker"]), "a FRED index was computed as a stock"
    fm = df.set_index("ticker")["factor_momentum_ratio"]
    assert fm.notna().all(), f"factor_momentum_ratio NaN for {sorted(fm[fm.isna()].index)}"
    assert fm.std() > 0


def test_a_degraded_features_run_names_its_defect_in_the_manifest_reason(monkeypatch):
    """RED before the fix: `_DegradedRun` read error/detail/reason, the result
    carried none, and D31's manifest said "no detail reported"."""
    from weekly_collector import _DegradedRun

    price_data, macro = _universe(n_tickers=3, n_rows=300)
    del price_data["HYOAS"]
    _patch_loaders(monkeypatch, price_data, macro)
    monkeypatch.setattr(
        compute, "find_all_null_columns", lambda df, cols: ["factor_momentum_ratio"],
    )
    monkeypatch.setattr(compute, "find_zero_variance_columns", lambda df, cols: {"beta_60d": 3})

    result = compute.compute_and_write(
        date_str=_END.date().isoformat(), bucket="test-bucket",
        dry_run=True, zero_variance_fatal=False,
    )

    assert result["status"] == "degraded"
    message = str(_DegradedRun("features", result))
    assert "no detail reported" not in message
    assert "factor_momentum_ratio" in message
    assert "beta_60d" in message


def test_a_clean_features_run_carries_no_reason(monkeypatch):
    price_data, macro = _universe(n_tickers=3, n_rows=300)
    del price_data["HYOAS"]
    _patch_loaders(monkeypatch, price_data, macro)
    monkeypatch.setattr(compute, "find_all_null_columns", lambda df, cols: [])
    monkeypatch.setattr(compute, "find_zero_variance_columns", lambda df, cols: {})

    result = compute.compute_and_write(
        date_str=_END.date().isoformat(), bucket="test-bucket",
        dry_run=True, zero_variance_fatal=False,
    )

    assert result["status"] == "ok"
    assert "reason" not in result
