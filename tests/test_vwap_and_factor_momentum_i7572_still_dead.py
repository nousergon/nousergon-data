"""alpha-engine-config-I7572: two of the 8 originally-dead columns d6d87f3f
and a10a95cb/#1426 already "fixed" were STILL broken, measured live
2026-08-19:

  - vwap_divergence_pct: 971c9660 (I7569) fixed VWAP getting dropped from the
    delta merge, but never touched the FEATURES-loop's generic fallback
    (features/compute.py, ``row[f] = float(val) if pd.notna(val) else 0.0``).
    That fallback is the SAME one I7539 already implicated for
    factor_momentum_ratio — it still launders a legitimately-NaN latest-row
    VWAP (daily_closes' EOD pass writes VWAP=None for the current day BY
    DESIGN; morning enrichment fills it the NEXT trading day — see
    collectors/daily_closes.py's module docstring) into a fabricated
    universe-wide constant 0.0, every day, forever.

  - factor_momentum_ratio: a10a95cb (#1426) correctly raised the TRADING-day
    warmup floor to 585, but ``_load_price_source`` never raised the
    CALENDAR-day window it actually reads ArcticDB with —
    ``load_universe_ohlcv`` / ``load_macro_series`` defaulted to
    ``_SLIM_EQUIVALENT_LOOKBACK_DAYS`` = 730 calendar days, ~504 trading days
    (730 * 252/365.25) even before holidays are subtracted. The per-ticker
    trim (``if len(df) > _FEATURE_WARMUP_ROWS: trim``) never fires because
    the source never delivers more than ~504 rows in the first place, so the
    factor-momentum second pass never gets the 585 rows it needs and produces
    0/902 non-null, every daily run, since a10a95cb landed. Measured live
    on ``s3://alpha-engine-research/features/2026-08-19/technical.parquet``.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from features import compute
from features.postflight import ALL_NULL_EXPECTED


# ── vwap_divergence_pct: generic-fallback laundering ───────────────────────

def _price_frame_with_latest_vwap_nan(n: int, seed: int) -> pd.DataFrame:
    """Mirrors the LIVE shape: real, varying VWAP on every closed prior day,
    NaN VWAP on the latest (today, not-yet-enriched) row."""
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    vwap = close * (1.0 + rng.normal(0, 0.002, n))
    vwap[-1] = np.nan  # today's row: not yet known
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {
            "Open": close, "High": close * 1.001, "Low": close * 0.999,
            "Close": close, "VWAP": vwap,
            "Volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
        },
        index=idx,
    )


def _universe_latest_vwap_nan(n_tickers: int = 25, n_rows: int = 400):
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    price_data = {t: _price_frame_with_latest_vwap_nan(n_rows, seed=i) for i, t in enumerate(tickers)}
    spy = _price_frame_with_latest_vwap_nan(n_rows, seed=999)["Close"]
    return price_data, {"SPY": spy}


def _patch_loaders(monkeypatch, price_data, macro):
    monkeypatch.setattr(
        compute, "_load_prices_and_macro",
        lambda s3, bucket, date_str, **_kw: (dict(price_data), dict(macro)),
    )
    monkeypatch.setattr(compute, "_load_sector_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_sub_sector_etf_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_cached_fundamentals", lambda s3, bucket, date_str: {})
    monkeypatch.setattr(compute, "_load_cached_alternative", lambda s3, bucket: {})


def test_a_legitimately_unavailable_latest_vwap_stays_nan_not_zero(monkeypatch):
    """RED before the fix: the generic FEATURES-loop fallback turned every
    ticker's NaN vwap_divergence_pct into a fabricated 0.0. 0.0 is a LEGAL
    divergence reading, so a manufactured zero is indistinguishable from a
    genuinely-flat close==VWAP day."""
    price_data, macro = _universe_latest_vwap_nan()
    _patch_loaders(monkeypatch, price_data, macro)

    captured = {}

    def _capture_guard(features_df, feature_names, **kwargs):
        captured["features_df"] = features_df.copy()

    monkeypatch.setattr(compute, "assert_no_dead_feature_columns", _capture_guard)

    result = compute.compute_and_write(date_str="2026-08-19", bucket="test-bucket", dry_run=True)
    assert result["status"] == "ok"

    col = captured["features_df"]["vwap_divergence_pct"]
    assert col.isna().all(), (
        "a legitimately-unknown latest-row VWAP must produce NaN, not a "
        f"fabricated 0.0. Got: {col.dropna().unique()[:5]}"
    )


def test_all_null_vwap_does_not_trip_the_dead_column_guard(monkeypatch):
    """The all-NaN latest row is the EXPECTED daily shape (postflight.
    ALL_NULL_EXPECTED), not a producer defect — vwap_divergence_pct must
    never appear in the guard's verdict, on either the fatal or non-fatal
    path. (This synthetic universe has no sector map / fundamentals / alt
    data, so OTHER unrelated groups legitimately constant-default here too —
    that is exercised by test_compute_momentum_columns_i7539.py, not this
    test; isolate to vwap_divergence_pct specifically via the same
    capture-the-guard-call technique that file uses.)"""
    price_data, macro = _universe_latest_vwap_nan()
    _patch_loaders(monkeypatch, price_data, macro)

    captured = {}

    def _capture_guard(features_df, feature_names, **kwargs):
        captured["features_df"] = features_df.copy()
        captured["exempt"] = kwargs.get("exempt")

    monkeypatch.setattr(compute, "assert_no_dead_feature_columns", _capture_guard)

    compute.compute_and_write(date_str="2026-08-19", bucket="test-bucket", dry_run=True)

    assert "vwap_divergence_pct" in captured["exempt"], (
        "the fatal-path call must pass an exempt set that includes "
        "vwap_divergence_pct, or a real production run would raise on its "
        "expected-every-day all-NaN latest row"
    )


def test_vwap_divergence_pct_is_registered_as_expected_all_null():
    assert "vwap_divergence_pct" in ALL_NULL_EXPECTED


# ── factor_momentum_ratio: ArcticDB calendar-day lookback starved the warmup ─

def test_the_default_arcticdb_lookback_is_insufficient_for_the_warmup_floor():
    """Pins the measured shape of the defect: 730 calendar days (the
    nousergon_lib.arcticdb default) converts to fewer TRADING days than
    _FEATURE_WARMUP_ROWS requires, even before holidays are subtracted."""
    naive_trading_days = 730 * 252 / 365.25
    assert naive_trading_days < compute._FEATURE_WARMUP_ROWS, (
        "if this no longer holds, the library default alone would have "
        "covered the warmup floor and I7572's second root cause would not "
        "reproduce — re-derive _ARCTICDB_LOOKBACK_DAYS's rationale"
    )


def test_arcticdb_lookback_days_covers_the_warmup_floor_with_holiday_margin():
    """_ARCTICDB_LOOKBACK_DAYS must convert back to AT LEAST
    _FEATURE_WARMUP_ROWS trading days using the same 252/365.25 convention,
    with room left over for market holidays (which the naive weekday-only
    conversion does not account for)."""
    implied_trading_days = compute._ARCTICDB_LOOKBACK_DAYS * 252 / 365.25
    assert implied_trading_days >= compute._FEATURE_WARMUP_ROWS * 1.05, (
        f"_ARCTICDB_LOOKBACK_DAYS={compute._ARCTICDB_LOOKBACK_DAYS} implies "
        f"~{implied_trading_days:.0f} trading days, too tight against the "
        f"{compute._FEATURE_WARMUP_ROWS}-row floor once holidays are counted"
    )


def test_the_old_default_730_days_would_fail_the_floor():
    """The test is not merely agreeing with whatever the constant happens to
    be — assert the PRE-FIX behavior (no override, library default) really
    was insufficient."""
    old_implied_trading_days = math.floor(730 * 252 / 365.25)
    assert old_implied_trading_days < compute._FEATURE_WARMUP_ROWS


def test_load_price_source_passes_the_widened_lookback_to_both_arctic_readers(monkeypatch):
    """RED before the fix: _load_price_source called load_universe_ohlcv and
    load_macro_series with no lookback_days override, so both silently used
    the library's 730-calendar-day default — insufficient for the
    factor-momentum second pass's 585-trading-day floor."""
    captured = {}

    def _fake_load_universe_ohlcv(bucket, **kwargs):
        captured["universe_lookback_days"] = kwargs.get("lookback_days")
        return {}

    def _fake_load_macro_series(bucket, symbols, **kwargs):
        captured["macro_lookback_days"] = kwargs.get("lookback_days")
        return {}

    class _FakeMacroLib:
        def list_symbols(self):
            return []

    monkeypatch.setattr(compute, "load_universe_ohlcv", _fake_load_universe_ohlcv)
    monkeypatch.setattr(compute, "load_macro_series", _fake_load_macro_series)
    monkeypatch.setattr(compute, "open_macro_lib", lambda bucket: _FakeMacroLib())

    compute._load_price_source(s3=None, bucket="test-bucket")

    assert captured["universe_lookback_days"] == compute._ARCTICDB_LOOKBACK_DAYS
    assert captured["macro_lookback_days"] == compute._ARCTICDB_LOOKBACK_DAYS
    assert captured["universe_lookback_days"] is not None, (
        "lookback_days must be explicitly passed, not left to the library "
        "default that starved the factor-momentum warmup"
    )
