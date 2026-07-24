"""migrations/0001_add_sub_sector_features.py — re-land of the sub-sector
ETF-relative features (alpha-engine-config#934), this time WITH the universe
migration that was missing the first time.

``nousergon-data#742`` shipped ``sub_sector_vs_benchmark_{5,10,20}d`` as a
code-only change; the first post-merge ``daily_append`` widened the emitted
column set against the old 94-col descriptor and failed 904/904 with
``StreamDescriptorMismatch`` (config-I3236, 2026-07-21) → reverted
(``nousergon-data#985``). This migration is the missing data half, authored
against the now-shipped framework (``migrations/_base.py``,
alpha-engine-config-I3241/I3238).

``backfill_policy: RECOMPUTE`` — both sides of the ratio (the stock's own
history is already persisted; the sub-sector/sector benchmark ETFs are
liquid, long-listed ETFs) are fully available historically, so history rows
get REAL retro-computed values, not a NaN fill:

  * For a ticker whose ``GICS_SUBINDUSTRY_TO_ETF``-resolved sub-sector ETF
    IS its existing sector ETF (the fallback case — an unmapped sub-industry,
    or no sub-industry captured at all), ``sub_sector_vs_benchmark_*`` is
    mathematically IDENTICAL to the already-persisted ``sector_vs_spy_*`` —
    copied verbatim, no new price data needed.
  * For a ticker mapped to a genuinely distinct sub-sector ETF (SMH / IGV /
    XBI / PPH / XOP / KRE / ITA / GDX), the migration fetches that ETF's full
    historical daily closes (yfinance, mirroring ``collectors/prices.py``'s
    ``_ALWAYS_DOWNLOAD`` convention) and recomputes the exact
    ``feature_engineer.compute_features`` sub-sector-vs-SPY momentum-diff
    formula over the symbol's full history.

A distinct sub-sector ETF whose history fails to fetch is a fail-loud
condition (``MigrationError``), not a silent zero/NaN degrade: those are
precisely the tickers this feature exists to make more informative, so
silently falling back would reintroduce the "some columns quietly wrong"
failure class this framework exists to prevent.
"""

from __future__ import annotations

import logging

import pandas as pd

from migrations._base import (
    Migration,
    MigrationError,
    rewrite_symbols_full,
    verify_additive,
)

log = logging.getLogger(__name__)

# The full frozen canonical column set AFTER this migration: the 94-column
# baseline (migrations.0000_baseline_universe_schema.BASELINE_COLUMNS) with
# the 3 new columns inserted immediately after "sector_vs_spy_20d" — matching
# features.feature_engineer.FEATURES list order (config#934 issue spec).
# Captured literally from store.arctic_store.canonical_universe_columns() on
# this branch (frozen per the chokepoint's anchor design — see _template.py).
COLUMNS_AFTER: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "Volume", "VWAP", "source",
    "rsi_14", "macd_cross", "macd_above_zero", "macd_line_last",
    "price_vs_ma50", "price_vs_ma200", "momentum_20d", "avg_volume_20d",
    "avg_volume_20d_raw", "dist_from_52w_high", "momentum_5d",
    "rel_volume_ratio", "return_vs_spy_5d", "vix_level", "dist_from_52w_low",
    "vol_ratio_10_60", "bollinger_pct", "sector_vs_spy_5d", "sector_vs_spy_10d",
    "sector_vs_spy_20d",
    "sub_sector_vs_benchmark_5d", "sub_sector_vs_benchmark_10d",
    "sub_sector_vs_benchmark_20d",
    "yield_10y", "yield_curve_slope", "gold_mom_5d",
    "oil_mom_5d", "vix_term_slope", "xsect_dispersion", "price_accel",
    "ema_cross_8_21", "atr_14_pct", "realized_vol_20d", "volume_trend",
    "obv_slope_10d", "rsi_slope_5d", "volume_price_div", "mom5d_x_vix",
    "rsi_x_vix", "sector_x_trend", "atr_x_vix", "vol_trend_x_vix",
    "earnings_surprise_pct", "days_since_earnings", "eps_revision_4w",
    "revision_streak", "put_call_ratio", "iv_rank", "iv_vs_rv", "pe_ratio",
    "pb_ratio", "debt_to_equity", "revenue_growth_yoy", "fcf_yield",
    "gross_margin", "roe", "current_ratio", "revenue_growth_3y",
    "eps_growth_3y", "payout_ratio", "dividend_yield", "capex_growth_5y",
    "market_cap_raw", "return_60d", "return_120d", "overnight_return_5d",
    "intraday_return_5d", "dist_from_5d_high", "dist_from_20d_high",
    "beta_60d", "idio_vol_60d", "vol_of_vol_30d", "max_drawdown_60d",
    "realized_vol_63d", "residual_momentum_ratio", "mom_12_1_pct",
    "sector_mom_pct", "factor_momentum_ratio", "momentum_20d_zscore",
    "return_60d_zscore", "beta_60d_zscore", "idio_vol_60d_zscore",
    "realized_vol_63d_zscore", "dist_from_52w_high_zscore", "pe_ratio_zscore",
    "roe_zscore", "size_zscore", "vwap_divergence_pct", "cmf_20_ratio",
    "hy_oas_credit_spread_pct",
)

NEW_COLUMNS: tuple[str, ...] = (
    "sub_sector_vs_benchmark_5d",
    "sub_sector_vs_benchmark_10d",
    "sub_sector_vs_benchmark_20d",
)

_SECTOR_COLS: tuple[str, ...] = (
    "sector_vs_spy_5d",
    "sector_vs_spy_10d",
    "sector_vs_spy_20d",
)

_WINDOWS: tuple[int, ...] = (5, 10, 20)

_SPY_SYMBOL = "SPY"


def _load_json_map(s3, bucket: str, key: str) -> dict[str, str]:
    """Best-effort S3 JSON map load — mirrors features.compute._load_sector_map."""
    import json

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as exc:
        log.warning("migration 0001: failed to load %s: %s", key, exc)
        return {}


def fetch_etf_close_history(
    symbols: list[str], *, period: str = "10y"
) -> dict[str, pd.Series]:
    """Fetch full historical daily Close series for ``symbols`` via yfinance,
    mirroring ``collectors/prices.py``'s ``_ALWAYS_DOWNLOAD`` fetch shape.
    Isolated in its own function (no other module-level side effects) so
    tests can monkeypatch it instead of hitting the network. Returns only the
    symbols that resolved to a non-empty series."""
    import yfinance as yf

    out: dict[str, pd.Series] = {}
    if not symbols:
        return out
    raw = yf.download(
        tickers=symbols if len(symbols) > 1 else symbols[0],
        period=period,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )
    is_multi = isinstance(raw.columns, pd.MultiIndex)
    for sym in symbols:
        try:
            frame = raw[sym] if is_multi else raw
            close = frame["Close"].dropna()
            if close.empty:
                continue
            idx = pd.to_datetime(close.index)
            if idx.tz is not None:
                idx = idx.tz_convert("UTC").tz_localize(None)
            close.index = idx
            out[sym] = close.sort_index()
        except Exception as exc:
            log.warning("migration 0001: no yfinance history for %s: %s", sym, exc)
    return out


def _momentum(series: pd.Series, window: int) -> pd.Series:
    return (series / series.shift(window)) - 1.0


def build_new_columns_fn(
    *,
    sub_sector_etf_map: dict[str, str],
    sector_etf_map: dict[str, str],
    etf_close: dict[str, pd.Series],
):
    """Return a ``rewrite_symbols_full`` ``new_columns_fn(symbol, df)`` that
    implements the RECOMPUTE backfill policy described in this module's
    docstring. Pure function of its inputs — no I/O — so it is fully unit
    testable against synthetic maps/series."""
    spy_close = etf_close.get(_SPY_SYMBOL)

    def _new_columns_fn(symbol: str, df: pd.DataFrame) -> dict[str, pd.Series]:
        etf_sym = sub_sector_etf_map.get(symbol)
        is_fallback = etf_sym is None or etf_sym == sector_etf_map.get(symbol)

        if is_fallback:
            # Mathematically identical to sector_vs_spy_* (same ETF, same
            # formula) — copy the already-correct, already-migrated history
            # verbatim rather than re-deriving it.
            missing = [c for c in _SECTOR_COLS if c not in df.columns]
            if missing:
                raise MigrationError(
                    f"symbol {symbol!r}: fallback case requires "
                    f"{_SECTOR_COLS} to already be present; missing {missing}"
                )
            return {
                new: df[old].astype("float32")
                for new, old in zip(NEW_COLUMNS, _SECTOR_COLS)
            }

        if spy_close is None:
            raise MigrationError(
                "migration 0001: SPY close history is required to recompute "
                "sub_sector_vs_benchmark_* for any non-fallback symbol, but "
                "none was fetched."
            )
        etf_series = etf_close.get(etf_sym)
        if etf_series is None:
            raise MigrationError(
                f"symbol {symbol!r}: mapped sub-sector ETF {etf_sym!r} has no "
                f"fetched history — refusing to silently zero/NaN-fill a "
                f"column this migration's whole point is to make correct "
                f"(RECOMPUTE backfill policy)."
            )

        etf_aligned = etf_series.reindex(df.index)
        spy_aligned = spy_close.reindex(df.index)
        out: dict[str, pd.Series] = {}
        for window, col in zip(_WINDOWS, NEW_COLUMNS):
            mom = _momentum(etf_aligned, window) - _momentum(spy_aligned, window)
            out[col] = mom.astype("float32")
        return out

    return _new_columns_fn


def _run(lib, meta_lib) -> None:
    import os

    import boto3

    from collectors.constituents import _build_sub_sector_etf_map
    from store.arctic_store import DEFAULT_BUCKET
    from store.schema_version import write_schema_version

    # Same bucket every producer (daily_append, weekly_collector) and
    # store.arctic_store use for both the ArcticDB URI and the S3 JSON maps —
    # one bucket, one env var (ARCTIC_BUCKET), matching store.arctic_store's
    # own default resolution.
    bucket = os.environ.get("ARCTIC_BUCKET", DEFAULT_BUCKET)

    s3 = boto3.client("s3")
    sector_etf_map = _load_json_map(s3, bucket, "data/sector_map.json")
    sub_industry_map = _load_json_map(s3, bucket, "data/sub_industry_map.json")
    sub_sector_etf_map = _build_sub_sector_etf_map(
        list(sector_etf_map.keys()), sector_etf_map, sub_industry_map
    )

    distinct_etfs = sorted(
        {
            etf
            for ticker, etf in sub_sector_etf_map.items()
            if etf != sector_etf_map.get(ticker)
        }
    )
    fetch_symbols = sorted({_SPY_SYMBOL, *distinct_etfs})
    log.info(
        "migration 0001: fetching full history for %d symbol(s) (SPY + %d "
        "distinct sub-sector ETF(s)): %s",
        len(fetch_symbols), len(distinct_etfs), fetch_symbols,
    )
    etf_close = fetch_etf_close_history(fetch_symbols)
    missing_etfs = [s for s in fetch_symbols if s not in etf_close]
    if missing_etfs:
        raise MigrationError(
            f"migration 0001: failed to fetch required history for "
            f"{missing_etfs} — aborting rather than silently degrading the "
            f"tickers mapped to them (RECOMPUTE backfill policy)."
        )

    new_columns_fn = build_new_columns_fn(
        sub_sector_etf_map=sub_sector_etf_map,
        sector_etf_map=sector_etf_map,
        etf_close=etf_close,
    )

    rewrite_symbols_full(
        lib, expected_columns=COLUMNS_AFTER, new_columns_fn=new_columns_fn
    )
    # Stamp LAST, only after the rewrite completes.
    write_schema_version(
        meta_lib,
        MIGRATION.schema_version_after,
        migration_number=MIGRATION.number,
        columns_after=COLUMNS_AFTER,
    )


def _verify(lib) -> None:
    verify_additive(lib, expected_columns=COLUMNS_AFTER)


MIGRATION = Migration(
    number=1,
    name="add_sub_sector_features",
    target_library="universe",
    symbol_scope="all universe symbols",
    schema_version_before=0,
    schema_version_after=1,
    columns_after=COLUMNS_AFTER,
    backfill_policy=(
        "RECOMPUTE (not NaN). Both sides of the ratio are fully available "
        "historically: the stock's own history is already persisted, and the "
        "sub-sector/sector benchmark ETFs (SMH/IGV/XBI/PPH/XOP/KRE/ITA/GDX, "
        "plus the existing XL* sector ETFs) are liquid, long-listed ETFs with "
        "years of price history — same as the existing sector_vs_spy_* "
        "benchmarks. Fallback-case tickers (sub-sector ETF == sector ETF) "
        "copy the already-persisted sector_vs_spy_* history verbatim (exact "
        "same formula, exact same ETF); genuinely-distinct-sub-sector-ETF "
        "tickers get their history retro-computed from a fresh yfinance fetch "
        "of that ETF's full history. See build_new_columns_fn/_run in this "
        "module."
    ),
    run=_run,
    verify=_verify,
)
