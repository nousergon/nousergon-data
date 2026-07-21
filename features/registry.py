"""
features/registry.py — Feature catalog and S3 registry.

Defines all features with metadata (group, description, source, refresh frequency).
Generates registry.json for cross-repo consumers that read features from S3.

Self-contained copy from alpha-engine-predictor/feature_store/registry.py.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


#: Sentinel ``compute=`` value for alpha-bearing columns produced by a
#: private feature pack (alpha-engine-config#1032 — private-edge divergence
#: policy, config#1031). A ``FeatureEntry`` carrying this sentinel asserts
#: the column is real (registered, units-suffixed, consumer-documented) but
#: intentionally omits WHAT computes it. See ``features/private_pack.py``
#: for the loading mechanism and ``features/SCHEMA.md`` §3b for the
#: disclosure format consumers see. The schema-contract CI
#: (``tests/test_schema_contract.py``) exempts entries carrying this
#: sentinel from the public-emit-list (``feature_engineer.FEATURES``) sync
#: requirement — everything else (CATALOG registration, units-suffix,
#: SCHEMA.md row, consumer) is still enforced identically to a public
#: column.
PRIVATE_PACK_COMPUTE = "private-pack"


@dataclass(frozen=True)
class FeatureEntry:
    name: str
    group: str          # technical | macro | interaction | alternative | fundamental
    description: str
    dtype: str = "float32"
    source: str = ""    # yfinance | fmp | computed
    refresh: str = ""   # daily | weekly | quarterly
    per_ticker: bool = True  # False for macro features (one row per date)
    # "" (default) for every public column — parity with FEATURES is
    # enforced as before. Set to PRIVATE_PACK_COMPUTE for a column supplied
    # by a private feature pack (features/private_pack.py); such columns
    # are exempt from the FEATURES-emit-list sync check but still require
    # a units-suffixed name, a SCHEMA.md §3 row, and a named consumer —
    # see test_schema_contract.py.
    compute: str = ""


# ── Feature Catalog ──────────────────────────────────────────────────────────
# Authoritative list of all features. Must stay in sync with
# feature_engineer.FEATURES and feature_engineer.compute_features().

CATALOG: list[FeatureEntry] = [
    # ── Technical (29) ────────────────────────────────────────────────────────
    FeatureEntry("rsi_14", "technical", "RSI(14), range 0-100", source="yfinance", refresh="daily"),
    FeatureEntry("macd_cross", "technical", "+1 bullish / -1 bearish / 0 no cross (last 3 days)", source="yfinance", refresh="daily"),
    FeatureEntry("macd_above_zero", "technical", "1 if MACD line > 0, else 0", source="yfinance", refresh="daily"),
    FeatureEntry("macd_line_last", "technical", "MACD line value (fast EMA - slow EMA)", source="yfinance", refresh="daily"),
    FeatureEntry("price_vs_ma50", "technical", "Close / SMA(50) ratio", source="yfinance", refresh="daily"),
    FeatureEntry("price_vs_ma200", "technical", "Close / SMA(200) ratio", source="yfinance", refresh="daily"),
    FeatureEntry("momentum_20d", "technical", "20-day price return", source="yfinance", refresh="daily"),
    FeatureEntry("avg_volume_20d", "technical", "Predictor input: 20-day avg volume / per-ticker global mean (relative liquidity ratio, ~1.0 typical). See features/SCHEMA.md for the bare-name == normalized convention.", source="yfinance", refresh="daily"),
    FeatureEntry("avg_volume_20d_raw", "technical", "Research scanner input: 20-day avg volume in raw shares. Absolute-liquidity gate consumer (MIN_AVG_VOLUME=500_000). The _raw suffix encodes units per features/SCHEMA.md.", source="yfinance", refresh="daily"),
    FeatureEntry("dist_from_52w_high", "technical", "(Close - 52w high) / 52w high", source="yfinance", refresh="daily"),
    FeatureEntry("momentum_5d", "technical", "5-day price return", source="yfinance", refresh="daily"),
    FeatureEntry("rel_volume_ratio", "technical", "Today volume / 20-day avg volume", source="yfinance", refresh="daily"),
    FeatureEntry("return_vs_spy_5d", "technical", "5-day stock return minus SPY return", source="yfinance", refresh="daily"),
    FeatureEntry("dist_from_52w_low", "technical", "(Close - 52w low) / 52w low", source="yfinance", refresh="daily"),
    FeatureEntry("vol_ratio_10_60", "technical", "10-day vol / 60-day vol", source="yfinance", refresh="daily"),
    FeatureEntry("bollinger_pct", "technical", "Position within Bollinger Bands (0-1)", source="yfinance", refresh="daily"),
    FeatureEntry("sector_vs_spy_5d", "technical", "5-day sector ETF return minus SPY return", source="yfinance", refresh="daily"),
    FeatureEntry("sector_vs_spy_10d", "technical", "10-day sector ETF return minus SPY return", source="yfinance", refresh="daily"),
    FeatureEntry("sector_vs_spy_20d", "technical", "20-day sector ETF return minus SPY return", source="yfinance", refresh="daily"),
    FeatureEntry("price_accel", "technical", "Momentum acceleration (5d mom - 20d mom)", source="yfinance", refresh="daily"),
    FeatureEntry("ema_cross_8_21", "technical", "EMA(8) / EMA(21) ratio", source="yfinance", refresh="daily"),
    FeatureEntry("atr_14_pct", "technical", "ATR(14) / Close, normalized volatility", source="yfinance", refresh="daily"),
    FeatureEntry("realized_vol_20d", "technical", "20-day annualized return std dev", source="yfinance", refresh="daily"),
    FeatureEntry("realized_vol_63d", "technical", "63-day (3-month) annualized return std dev — slower vol regime than 20d, pairs as vol-term-structure", source="yfinance", refresh="daily"),
    FeatureEntry("volume_trend", "technical", "5-day avg volume / 20-day avg volume", source="yfinance", refresh="daily"),
    FeatureEntry("obv_slope_10d", "technical", "OBV linear regression slope over 10 days", source="yfinance", refresh="daily"),
    FeatureEntry("rsi_slope_5d", "technical", "5-day RSI slope", source="yfinance", refresh="daily"),
    FeatureEntry("volume_price_div", "technical", "sign(volume_trend-1) * sign(momentum_5d)", source="yfinance", refresh="daily"),
    # config#939 — VWAP divergence: (Close - VWAP) / VWAP. VWAP is a
    # first-class OHLCV column (Polygon `vw`); NaN for yfinance-fallback
    # rows and during the documented 2026-04-17->23 Polygon outage.
    FeatureEntry("vwap_divergence_pct", "technical", "(Close - VWAP) / VWAP, decimal pct — NaN when VWAP is unavailable (yfinance-fallback rows, Polygon outage windows)", source="polygon", refresh="daily"),
    # config#939 — buying/selling pressure: Chaikin Money Flow (CMF-20).
    # Chosen over MFI-14 / Chaikin A/D: bounded ~[-1,1] range (fewest edge
    # cases vs. A/D's unbounded cumulative line), uses only OHLCV already
    # in the feature store (no new data source).
    FeatureEntry("cmf_20_ratio", "technical", "Chaikin Money Flow (20d): rolling_sum(money_flow_multiplier * Volume, 20) / rolling_sum(Volume, 20). Bounded ~[-1, 1] dimensionless ratio; High==Low guarded to NaN.", source="yfinance", refresh="daily"),

    # ── Macro (8) — identical across all tickers on a given day ───────────────
    FeatureEntry("vix_level", "macro", "VIX / 20 (normalized around long-run avg)", source="yfinance", refresh="daily", per_ticker=False),
    FeatureEntry("yield_10y", "macro", "10Y Treasury yield normalized to 0-1", source="yfinance", refresh="daily", per_ticker=False),
    FeatureEntry("yield_curve_slope", "macro", "10Y - 2Y spread, normalized", source="yfinance", refresh="daily", per_ticker=False),
    FeatureEntry("gold_mom_5d", "macro", "5-day gold (GLD) momentum", source="yfinance", refresh="daily", per_ticker=False),
    FeatureEntry("oil_mom_5d", "macro", "5-day oil (USO) momentum", source="yfinance", refresh="daily", per_ticker=False),
    FeatureEntry("vix_term_slope", "macro", "VIX spot vs VIX3M term structure slope, normalized", source="yfinance", refresh="daily", per_ticker=False),
    FeatureEntry("xsect_dispersion", "macro", "Cross-sectional std dev of daily returns across universe", source="computed", refresh="daily", per_ticker=False),
    # config#939 — credit spreads. ICE BofA US HY Index OAS (FRED
    # BAMLH0A0HYM2), percent. License-gated to 2023+ on FRED; pre-2023 /
    # missing rows fall back to the neutral default 0.0 (same pattern as
    # gold_mom_5d / oil_mom_5d), never hard-fail. Deliberately named
    # DISTINCT from crucible-predictor's regime_predictor.py
    # `hy_oas_level` / `hy_oas_change_21d` (a separate market-wide
    # regime-substrate feature family, sourced independently via its own
    # HYOAS.parquet cache and consumed only through cfg.MACRO_NORM_FEATURES
    # — not this feature-store namespace).
    FeatureEntry("hy_oas_credit_spread_pct", "macro", "ICE BofA US HY Index OAS (FRED BAMLH0A0HYM2), percent. License-gated to 2023+ on FRED; falls back to neutral 0.0 when unavailable.", source="fred", refresh="daily", per_ticker=False),

    # ── Regime interactions (5) — macro x ticker-specific signals ─────────────
    FeatureEntry("mom5d_x_vix", "interaction", "momentum_5d * VIX regime indicator", source="computed", refresh="daily"),
    FeatureEntry("rsi_x_vix", "interaction", "RSI deviation from 50 * VIX regime", source="computed", refresh="daily"),
    FeatureEntry("sector_x_trend", "interaction", "Sector-relative return * price trend", source="computed", refresh="daily"),
    FeatureEntry("atr_x_vix", "interaction", "ATR * VIX regime indicator", source="computed", refresh="daily"),
    FeatureEntry("vol_trend_x_vix", "interaction", "Volume trend * VIX regime indicator", source="computed", refresh="daily"),

    # ── Alternative data (7) — O10-O12 signals ───────────────────────────────
    FeatureEntry("earnings_surprise_pct", "alternative", "Most recent quarterly EPS surprise %", source="fmp", refresh="weekly"),
    FeatureEntry("days_since_earnings", "alternative", "Days since last earnings (0-1, capped 90d)", source="fmp", refresh="weekly"),
    FeatureEntry("eps_revision_4w", "alternative", "4-week cumulative EPS revision %", source="fmp", refresh="weekly"),
    FeatureEntry("revision_streak", "alternative", "Consecutive weeks of same-direction revisions", source="fmp", refresh="weekly"),
    FeatureEntry("put_call_ratio", "alternative", "Log-transformed put/call OI ratio", source="yfinance", refresh="weekly"),
    FeatureEntry("iv_rank", "alternative", "IV percentile rank (0-1)", source="yfinance", refresh="weekly"),
    FeatureEntry("iv_vs_rv", "alternative", "Implied vol / realized vol ratio", source="yfinance", refresh="weekly"),

    # ── v3.1 technical additions — horizon + decomposition + reversal-native ──
    FeatureEntry("return_60d", "technical", "60-day price return (Close_t / Close_{t-60} - 1)", source="yfinance", refresh="daily"),
    FeatureEntry("return_120d", "technical", "120-day price return (Close_t / Close_{t-120} - 1)", source="yfinance", refresh="daily"),
    FeatureEntry("overnight_return_5d", "technical", "5d sum of overnight returns (Open_t vs Close_{t-1})", source="yfinance", refresh="daily"),
    FeatureEntry("intraday_return_5d", "technical", "5d sum of intraday returns (Close_t vs Open_t)", source="yfinance", refresh="daily"),
    FeatureEntry("dist_from_5d_high", "technical", "(Close - 5d rolling max High) / 5d rolling max High", source="yfinance", refresh="daily"),
    FeatureEntry("dist_from_20d_high", "technical", "(Close - 20d rolling max High) / 20d rolling max High", source="yfinance", refresh="daily"),

    # ── v3.2 per-ticker risk features (Stage 2 of regime-conditioning rebuild) ──
    FeatureEntry("beta_60d", "technical", "60d rolling regression slope of stock log-returns vs SPY log-returns (systematic market exposure)", source="yfinance", refresh="daily"),
    FeatureEntry("idio_vol_60d", "technical", "60d residual vol after removing beta exposure; std × sqrt(252) (idiosyncratic risk)", source="yfinance", refresh="daily"),
    FeatureEntry("vol_of_vol_30d", "technical", "30d rolling stdev of realized_vol_20d (stability of vol regime)", source="yfinance", refresh="daily"),
    FeatureEntry("max_drawdown_60d", "technical", "Worst peak-to-trough drawdown within trailing 60d window (non-positive decimal pct)", source="yfinance", refresh="daily"),
    # W2 (L4469) — residual/idiosyncratic momentum family. residual_momentum_ratio
    # reuses the beta-residualized log-return series (same as idio_vol_60d), NO
    # beta recompute. Predictor-consumed; observe-gated in the L2 until validated.
    FeatureEntry("residual_momentum_ratio", "technical", "Vol-scaled cumulative residual (idiosyncratic) log-return over the 12-1 skip-month window: ∑resid_ret[t-252,t-21] / (σ_resid·√231) — an information ratio (Blitz/Hanauer residual momentum)", source="yfinance", refresh="daily"),
    FeatureEntry("mom_12_1_pct", "technical", "12-1 skip-month raw price momentum: close.shift(21)/close.shift(252) - 1 (classic momentum factor, skips the recent-month reversal)", source="yfinance", refresh="daily"),
    FeatureEntry("sector_mom_pct", "technical", "The ticker's sector-ETF own 12-1 skip-month momentum (GKX industry momentum — absolute, distinct from sector_vs_spy_* relative features)", source="yfinance", refresh="daily"),
    # W2.3 (L4469) — factor momentum (Gupta-Kelly "Factor Momentum Everywhere").
    # Cross-sectional-time-series projection: Σ_f zscore(loading_{i,f,t}) ×
    # factor_momentum_{f,t}. NOT produced by per-ticker compute_features —
    # materialized in a second pass over the universe lib (factor_momentum.
    # materialize_factor_momentum). Mirrors the post-assembly *_zscore loadings.
    FeatureEntry("factor_momentum_ratio", "technical", "Factor-momentum tilt (Gupta-Kelly): per date, dot the ticker's cross-sectionally z-scored factor loadings with each factor's trailing 12-1 long-short return momentum — Σ_f zscore(loading) × factor_mom. Dimensionless projection; backward-only (loadings at t × factor momentum through t-skip)", source="derived", refresh="daily"),

    # ── Fundamental (13) — quarterly financials ───────────────────────────────
    FeatureEntry("pe_ratio", "fundamental", "Trailing P/E ratio, normalized (PE / 30)", source="fmp", refresh="quarterly"),
    FeatureEntry("pb_ratio", "fundamental", "Price-to-book ratio, normalized (PB / 5)", source="fmp", refresh="quarterly"),
    FeatureEntry("debt_to_equity", "fundamental", "Total debt / total equity, normalized (D/E / 2)", source="fmp", refresh="quarterly"),
    FeatureEntry("revenue_growth_yoy", "fundamental", "Year-over-year revenue growth (decimal)", source="fmp", refresh="quarterly"),
    FeatureEntry("fcf_yield", "fundamental", "Free cash flow / market cap (decimal)", source="fmp", refresh="quarterly"),
    FeatureEntry("gross_margin", "fundamental", "Gross profit / revenue (0-1)", source="fmp", refresh="quarterly"),
    FeatureEntry("roe", "fundamental", "Return on equity (decimal)", source="fmp", refresh="quarterly"),
    FeatureEntry("current_ratio", "fundamental", "Current assets / current liabilities, normalized (CR / 3)", source="fmp", refresh="quarterly"),
    # ── Phase 3a of attractiveness-pillars-260520 — Growth + Stewardship pillar substrate ──
    FeatureEntry("revenue_growth_3y", "fundamental", "3-year revenue CAGR (decimal); Growth pillar input", source="fmp", refresh="quarterly"),
    FeatureEntry("eps_growth_3y", "fundamental", "3-year EPS CAGR (decimal); Growth pillar input", source="fmp", refresh="quarterly"),
    FeatureEntry("payout_ratio", "fundamental", "TTM dividends / net income (0-2 clipped); Stewardship pillar input — retention rate = 1 - payout drives reinvestment", source="fmp", refresh="quarterly"),
    FeatureEntry("dividend_yield", "fundamental", "Indicated annual dividend yield (decimal, 0-0.2 clipped); Stewardship pillar input", source="fmp", refresh="quarterly"),
    FeatureEntry("capex_growth_5y", "fundamental", "5-year CAPEX growth (decimal); Stewardship pillar input — reinvestment intensity proxy", source="fmp", refresh="quarterly"),
    # SIZE pillar substrate (config#1142) — raw market cap, absolute units
    # (the _raw suffix encodes absolute / un-normalized units). Surfaced from
    # Finnhub's already-fetched marketCapitalization; UN-clipped/UN-normalized.
    # Base input to the Barra SIZE loading (size_zscore); scale-invariant.
    FeatureEntry("market_cap_raw", "fundamental", "Raw market capitalization (absolute units, un-normalized) surfaced from Finnhub marketCapitalization. Base input to the Barra SIZE factor loading (size_zscore).", source="finnhub", refresh="quarterly"),

    # ── Factor loadings (9) — C.1 of optimizer-sota-upgrades-260526 ──────────
    # Cross-sectional ±3σ-winsorized z-scores of canonical Barra-style factors.
    # Columns of the factor-loading matrix B for the executor's Σ = B·F·Bᵀ + D
    # risk decomposition (workstream C.3). Computed POST-assembly in
    # features/compute.py via features.cross_sectional.apply_factor_zscores.
    FeatureEntry("momentum_20d_zscore", "factor_loading", "Cross-sectional z-score of momentum_20d (winsorized ±3σ). Barra MOMENTUM (short-horizon) loading.", source="derived", refresh="daily"),
    FeatureEntry("return_60d_zscore", "factor_loading", "Cross-sectional z-score of return_60d (winsorized ±3σ). Barra MOMENTUM (medium-horizon) loading.", source="derived", refresh="daily"),
    FeatureEntry("beta_60d_zscore", "factor_loading", "Cross-sectional z-score of beta_60d (winsorized ±3σ). Barra BETA loading (market sensitivity).", source="derived", refresh="daily"),
    FeatureEntry("idio_vol_60d_zscore", "factor_loading", "Cross-sectional z-score of idio_vol_60d (winsorized ±3σ). Barra RESVOL loading (residual / idiosyncratic risk).", source="derived", refresh="daily"),
    FeatureEntry("realized_vol_63d_zscore", "factor_loading", "Cross-sectional z-score of realized_vol_63d (winsorized ±3σ). Barra VOLATILITY loading (total realized risk).", source="derived", refresh="daily"),
    FeatureEntry("dist_from_52w_high_zscore", "factor_loading", "Cross-sectional z-score of dist_from_52w_high (winsorized ±3σ). Proximity-to-high / reversal-risk loading.", source="derived", refresh="daily"),
    FeatureEntry("pe_ratio_zscore", "factor_loading", "Cross-sectional z-score of pe_ratio (winsorized ±3σ). Barra VALUE loading (proxy via 1/PE direction).", source="derived", refresh="daily"),
    FeatureEntry("roe_zscore", "factor_loading", "Cross-sectional z-score of roe (winsorized ±3σ). Barra QUALITY / profitability loading.", source="derived", refresh="daily"),
    FeatureEntry("size_zscore", "factor_loading", "Cross-sectional z-score of log(market_cap_raw) (winsorized ±3σ). Barra SIZE loading (config#1142) — completes the institutional Barra factor set.", source="derived", refresh="daily"),
]

# Quick lookup by name
_CATALOG_BY_NAME: dict[str, FeatureEntry] = {f.name: f for f in CATALOG}

# Group membership
GROUPS: dict[str, list[str]] = {}
for _f in CATALOG:
    GROUPS.setdefault(_f.group, []).append(_f.name)


def get_feature(name: str) -> FeatureEntry:
    """Look up a feature entry by name."""
    return _CATALOG_BY_NAME[name]


def get_group_features(group: str) -> list[str]:
    """Return feature names for a group."""
    return GROUPS.get(group, [])


def generate_registry_json() -> str:
    """Serialize the full catalog to JSON for S3 consumers."""
    return json.dumps(
        {"features": [asdict(f) for f in CATALOG]},
        indent=2,
    )


def upload_registry(bucket: str, prefix: str = "features/") -> None:
    """Write registry.json to S3."""
    import boto3

    s3 = boto3.client("s3")
    body = generate_registry_json()
    key = f"{prefix}registry.json"
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode(), ContentType="application/json")
    logger.info("Feature registry uploaded to s3://%s/%s (%d features)", bucket, key, len(CATALOG))
