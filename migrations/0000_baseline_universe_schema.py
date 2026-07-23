"""migrations/0000_baseline_universe_schema.py — the schema ANCHOR
(alpha-engine-config-I3241 / I3238).

This is not a data rewrite. It freezes the ``universe`` library's canonical
column set AS OF the config-I3236 revert (2026-07-21, nousergon-data#985 —
the known-good 94-column schema WITHOUT the reverted
``sub_sector_vs_benchmark_5d/10d/20d`` columns) as schema version 0, and gives
the migration chain and the CI chokepoint their starting point.

Why a FROZEN literal and not ``canonical_universe_columns()`` live:
    The whole point of the chokepoint (config-I3238) is to detect when the
    live, code-derived schema DIVERGES from what the latest migration declares.
    If this anchor derived its columns live, it could never diverge and the
    gate would be inert. So the 94 names below are a hand-frozen snapshot; when
    a feature column is later added, ``canonical_universe_columns()`` grows,
    the chokepoint test goes red, and the author must add migration 0001 with a
    NEW frozen ``columns_after`` (and a real data rewrite). That is the control.

run():  stamps an EXISTING live library to v0 after asserting it already
        conforms (no rewrite — the live universe IS the baseline). On a fresh /
        empty library it simply stamps. Idempotent.
verify(): asserts any present symbols conform to the frozen column set.
"""

from __future__ import annotations

import logging

from migrations._base import Migration, MigrationError

log = logging.getLogger(__name__)

# ── The frozen 94-column canonical universe schema as of the I3236 revert ────
# OHLCV(6) + source(1) + FEATURES(87). Captured from
# store.arctic_store.canonical_universe_columns() at commit 0ebceee.
BASELINE_COLUMNS: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "Volume", "VWAP", "source",
    "rsi_14", "macd_cross", "macd_above_zero", "macd_line_last",
    "price_vs_ma50", "price_vs_ma200", "momentum_20d", "avg_volume_20d",
    "avg_volume_20d_raw", "dist_from_52w_high", "momentum_5d",
    "rel_volume_ratio", "return_vs_spy_5d", "vix_level", "dist_from_52w_low",
    "vol_ratio_10_60", "bollinger_pct", "sector_vs_spy_5d", "sector_vs_spy_10d",
    "sector_vs_spy_20d", "yield_10y", "yield_curve_slope", "gold_mom_5d",
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


def _run(lib, meta_lib) -> None:
    """Stamp an existing/live library to v0 after confirming it conforms.
    No data rewrite — the live universe already IS the baseline schema."""
    from store.schema_version import write_schema_version

    symbols = list(lib.list_symbols())
    for sym in symbols[:5]:
        got = tuple(lib.read(sym).data.columns)
        if got != BASELINE_COLUMNS:
            raise MigrationError(
                f"0000 baseline: symbol {sym!r} persisted columns {got} do NOT "
                f"match the frozen baseline schema ({len(BASELINE_COLUMNS)} "
                f"columns). The library is not at the anchor schema — a real "
                f"forward migration is required, not a baseline stamp."
            )
    write_schema_version(
        meta_lib, 0, migration_number=0, columns_after=BASELINE_COLUMNS
    )
    log.info(
        "0000 baseline: stamped universe schema v0 (%d symbols already "
        "conform, %d columns)",
        len(symbols),
        len(BASELINE_COLUMNS),
    )


def _verify(lib) -> None:
    symbols = list(lib.list_symbols())
    for sym in symbols[:5]:
        got = tuple(lib.read(sym).data.columns)
        if got != BASELINE_COLUMNS:
            raise MigrationError(
                f"0000 baseline verify: symbol {sym!r} columns {got} != frozen "
                f"baseline"
            )


MIGRATION = Migration(
    number=0,
    name="baseline_universe_schema",
    target_library="universe",
    symbol_scope="all universe symbols (anchor only — no rewrite)",
    schema_version_before=None,
    schema_version_after=0,
    columns_after=BASELINE_COLUMNS,
    backfill_policy=(
        "None — this is the anchor for the pre-existing live schema as of the "
        "2026-07-21 config-I3236 revert. No columns added; no history touched."
    ),
    run=_run,
    verify=_verify,
    is_baseline=True,
)
