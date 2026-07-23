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
    # ── SCHEMA.md §3 generated-render fields (alpha-engine-config#2590) ──
    # These four fields are the sole source of truth for SCHEMA.md §3's
    # per-field table row; features/gen_schema_md.py renders them
    # mechanically and tests/test_schema_contract.py fails CI if the
    # committed §3 tables drift from a fresh render. NOTE: `formula` is
    # deliberately NOT named `compute` — `compute` (above) is the
    # pre-existing PRIVATE_PACK_COMPUTE sentinel field and means something
    # different (see its docstring); `formula` holds the human-readable
    # Compute-column text (e.g. "Wilder's RSI(14) via EWM(com=13)") shown
    # in SCHEMA.md §3.
    units: str = ""
    formula: str = ""
    consumers: str = ""
    # Sort key for intra-group row order in the generated SCHEMA.md §3
    # table ONLY — does not affect CATALOG's own (load-bearing, S3
    # registry.json cross-repo consumer) list order. Every existing entry
    # was seeded from its CURRENT SCHEMA.md §3 row position within its
    # group at the time of #2590, which is NOT always the same as its
    # CATALOG position (e.g. vwap_divergence_pct / cmf_20_ratio sit near
    # the end of the Technical table but mid-list in CATALOG). New entries
    # default to 0; give new fields an explicit display_order to control
    # where they render.
    display_order: int = 0


# ── Feature Catalog ──────────────────────────────────────────────────────────
# Authoritative list of all features. Must stay in sync with
# feature_engineer.FEATURES and feature_engineer.compute_features().

CATALOG: list[FeatureEntry] = [
    # ── Technical (29) ────────────────────────────────────────────────────────
    FeatureEntry("rsi_14", "technical", "RSI(14), range 0-100", source="yfinance", refresh="daily", units='0–100 score (bare-named = normalized)', formula="Wilder's RSI(14) via EWM(com=13)", consumers='predictor + scanner', display_order=0),
    FeatureEntry("macd_cross", "technical", "+1 bullish / -1 bearish / 0 no cross (last 3 days)", source="yfinance", refresh="daily", units='tri-state (+1 bull / −1 bear / 0 none)', formula='Sign of MACD line crossing 0 within last 3 days', consumers='predictor + scanner', display_order=1),
    FeatureEntry("macd_above_zero", "technical", "1 if MACD line > 0, else 0", source="yfinance", refresh="daily", units='binary (0/1)', formula='`(macd_line > 0).astype(float)`', consumers='predictor + scanner', display_order=2),
    FeatureEntry("macd_line_last", "technical", "MACD line value (fast EMA - slow EMA)", source="yfinance", refresh="daily", units='absolute (price units)', formula='`EMA_fast(close) - EMA_slow(close)`', consumers='predictor', display_order=3),
    FeatureEntry("price_vs_ma50", "technical", "Close / SMA(50) ratio", source="yfinance", refresh="daily", units='decimal pct (bare-named convention)', formula='`(close - SMA50) / SMA50`', consumers='predictor + scanner (gate at −0.05)', display_order=4),
    FeatureEntry("price_vs_ma200", "technical", "Close / SMA(200) ratio", source="yfinance", refresh="daily", units='decimal pct (bare-named convention)', formula='`(close - SMA200) / SMA200`', consumers='predictor + scanner', display_order=5),
    FeatureEntry("momentum_20d", "technical", "20-day price return", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`close / close.shift(20) - 1`', consumers='predictor + scanner', display_order=6),
    FeatureEntry("avg_volume_20d", "technical", "Predictor input: 20-day avg volume / per-ticker global mean (relative liquidity ratio, ~1.0 typical). See features/SCHEMA.md for the bare-name == normalized convention.", source="yfinance", refresh="daily", units='ratio (rolling_20d / per-ticker global mean), ~1.0 typical', formula='`avg_vol_20d / volume.mean()`', consumers='**predictor only** — relative liquidity feature', display_order=7),
    FeatureEntry("avg_volume_20d_raw", "technical", "Research scanner input: 20-day avg volume in raw shares. Absolute-liquidity gate consumer (MIN_AVG_VOLUME=500_000). The _raw suffix encodes units per features/SCHEMA.md.", source="yfinance", refresh="daily", units='**raw shares**', formula='`volume.rolling(20).mean()`', consumers='**scanner only** — absolute liquidity gate vs `MIN_AVG_VOLUME=500_000`', display_order=8),
    FeatureEntry("dist_from_52w_high", "technical", "(Close - 52w high) / 52w high", source="yfinance", refresh="daily", units='decimal pct (bare-named convention)', formula='`(close - rolling_max_252) / rolling_max_252`', consumers='predictor + scanner', display_order=9),
    FeatureEntry("momentum_5d", "technical", "5-day price return", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`close / close.shift(5) - 1`', consumers='predictor + scanner', display_order=10),
    FeatureEntry("rel_volume_ratio", "technical", "Today volume / 20-day avg volume", source="yfinance", refresh="daily", units='ratio (today / rolling_20d)', formula='`volume / volume.rolling(20).mean()`', consumers='predictor', display_order=11),
    FeatureEntry("return_vs_spy_5d", "technical", "5-day stock return minus SPY return", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`momentum_5d - SPY_5d_return`', consumers='predictor', display_order=12),
    FeatureEntry("dist_from_52w_low", "technical", "(Close - 52w low) / 52w low", source="yfinance", refresh="daily", units='decimal pct (bare-named convention)', formula='`(close - rolling_min_252) / rolling_min_252`', consumers='predictor', display_order=13),
    FeatureEntry("vol_ratio_10_60", "technical", "10-day vol / 60-day vol", source="yfinance", refresh="daily", units='ratio', formula='`realized_vol_10d / realized_vol_60d`', consumers='predictor', display_order=14),
    FeatureEntry("bollinger_pct", "technical", "Position within Bollinger Bands (0-1)", source="yfinance", refresh="daily", units='0–1 channel position (bare-named convention)', formula='`(close - lower_bb) / (upper_bb - lower_bb)`', consumers='predictor', display_order=15),
    FeatureEntry("sector_vs_spy_5d", "technical", "5-day sector ETF return minus SPY return", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`sector_etf_5d - SPY_5d`', consumers='predictor', display_order=16),
    FeatureEntry("sector_vs_spy_10d", "technical", "10-day sector ETF return minus SPY return", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`sector_etf_10d - SPY_10d`', consumers='predictor', display_order=17),
    FeatureEntry("sector_vs_spy_20d", "technical", "20-day sector ETF return minus SPY return", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`sector_etf_20d - SPY_20d`', consumers='predictor', display_order=18),
    FeatureEntry("sub_sector_vs_benchmark_5d", "technical", "5-day sub-sector benchmark ETF (SMH/IGV/…) return minus SPY return; falls back to sector ETF for unmapped sub-industries (config#934)", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`sub_sector_etf_5d - SPY_5d`', consumers='predictor', display_order=44),
    FeatureEntry("sub_sector_vs_benchmark_10d", "technical", "10-day sub-sector benchmark ETF (SMH/IGV/…) return minus SPY return; falls back to sector ETF for unmapped sub-industries (config#934)", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`sub_sector_etf_10d - SPY_10d`', consumers='predictor', display_order=45),
    FeatureEntry("sub_sector_vs_benchmark_20d", "technical", "20-day sub-sector benchmark ETF (SMH/IGV/…) return minus SPY return; falls back to sector ETF for unmapped sub-industries (config#934)", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`sub_sector_etf_20d - SPY_20d`', consumers='predictor', display_order=46),
    FeatureEntry("price_accel", "technical", "Momentum acceleration (5d mom - 20d mom)", source="yfinance", refresh="daily", units='decimal (5d return − 20d return)', formula='`momentum_5d - momentum_20d`', consumers='predictor', display_order=19),
    FeatureEntry("ema_cross_8_21", "technical", "EMA(8) / EMA(21) ratio", source="yfinance", refresh="daily", units='ratio (bare-named convention, `EMA8/EMA21 - 1`)', formula='`EMA(8) / EMA(21) - 1`', consumers='predictor', display_order=20),
    FeatureEntry("atr_14_pct", "technical", "ATR(14) / Close, normalized volatility", source="yfinance", refresh="daily", units='decimal pct (`_pct` suffix)', formula='`ATR(14) / close`', consumers='predictor + scanner (consumer ×100 to display %)', display_order=21),
    FeatureEntry("realized_vol_20d", "technical", "20-day annualized return std dev", source="yfinance", refresh="daily", units='annualized vol (decimal)', formula='`std(daily_returns).rolling(20) * sqrt(252)`', consumers='predictor', display_order=22),
    FeatureEntry("realized_vol_63d", "technical", "63-day (3-month) annualized return std dev — slower vol regime than 20d, pairs as vol-term-structure", source="yfinance", refresh="daily", units='annualized vol (decimal)', formula='`std(daily_returns).rolling(63) * sqrt(252)`', consumers='predictor', display_order=23),
    FeatureEntry("volume_trend", "technical", "5-day avg volume / 20-day avg volume", source="yfinance", refresh="daily", units='ratio (5d / 20d avg volume)', formula='`vol_5 / vol_20`', consumers='predictor', display_order=24),
    FeatureEntry("obv_slope_10d", "technical", "OBV linear regression slope over 10 days", source="yfinance", refresh="daily", units='normalized slope (bare-named convention)', formula='`(OBV_fast - OBV_slow) / vol_20`', consumers='predictor', display_order=25),
    FeatureEntry("rsi_slope_5d", "technical", "5-day RSI slope", source="yfinance", refresh="daily", units='bare-named (RSI delta / 5)', formula='`(rsi - rsi.shift(5)) / 5`', consumers='predictor', display_order=26),
    FeatureEntry("volume_price_div", "technical", "sign(volume_trend-1) * sign(momentum_5d)", source="yfinance", refresh="daily", units='tri-state (sign × sign)', formula='`sign(volume_trend - 1) * sign(momentum_5d)`', consumers='predictor', display_order=27),
    # config#939 — VWAP divergence: (Close - VWAP) / VWAP. VWAP is a
    # first-class OHLCV column (Polygon `vw`); NaN for yfinance-fallback
    # rows and during the documented 2026-04-17->23 Polygon outage.
    FeatureEntry("vwap_divergence_pct", "technical", "(Close - VWAP) / VWAP, decimal pct — NaN when VWAP is unavailable (yfinance-fallback rows, Polygon outage windows)", source="polygon", refresh="daily", units='decimal pct (`_pct` suffix)', formula='`(Close - VWAP) / VWAP`', consumers='predictor (config#939 — VWAP divergence). NaN when VWAP is unavailable (yfinance-fallback rows; the documented 2026-04-17→23 Polygon outage) or when VWAP is 0 (guarded via `.replace(0, nan)`)', display_order=42),
    # config#939 — buying/selling pressure: Chaikin Money Flow (CMF-20).
    # Chosen over MFI-14 / Chaikin A/D: bounded ~[-1,1] range (fewest edge
    # cases vs. A/D's unbounded cumulative line), uses only OHLCV already
    # in the feature store (no new data source).
    FeatureEntry("cmf_20_ratio", "technical", "Chaikin Money Flow (20d): rolling_sum(money_flow_multiplier * Volume, 20) / rolling_sum(Volume, 20). Bounded ~[-1, 1] dimensionless ratio; High==Low guarded to NaN.", source="yfinance", refresh="daily", units='dimensionless ratio, bounded ~[-1, 1] (`_ratio` suffix)', formula='Chaikin Money Flow: `rolling_sum(MFM * Volume, 20) / rolling_sum(Volume, 20)` where `MFM = ((Close-Low)-(High-Close))/(High-Low)`', consumers='predictor (config#939 — buying/selling pressure). `High == Low` guarded to NaN via `.replace(0, nan)`, mirroring `volume_trend` / `obv_slope_10d`', display_order=43),

    # ── Macro (8) — identical across all tickers on a given day ───────────────
    FeatureEntry("vix_level", "macro", "VIX / 20 (normalized around long-run avg)", source="yfinance", refresh="daily", per_ticker=False, units='normalized (VIX / 20)', formula='`vix / vix_baseline`', consumers='predictor', display_order=0),
    FeatureEntry("yield_10y", "macro", "10Y Treasury yield normalized to 0-1", source="yfinance", refresh="daily", per_ticker=False, units='normalized (TNX / 10)', formula='`tnx / tnx_normalizer`', consumers='predictor', display_order=1),
    FeatureEntry("yield_curve_slope", "macro", "10Y - 2Y spread, normalized", source="yfinance", refresh="daily", per_ticker=False, units='normalized', formula='`(tnx - irx) / tnx_normalizer`', consumers='predictor', display_order=2),
    FeatureEntry("gold_mom_5d", "macro", "5-day gold (GLD) momentum", source="yfinance", refresh="daily", per_ticker=False, units='decimal return', formula='`gld / gld.shift(5) - 1`', consumers='predictor', display_order=3),
    FeatureEntry("oil_mom_5d", "macro", "5-day oil (USO) momentum", source="yfinance", refresh="daily", per_ticker=False, units='decimal return', formula='`uso / uso.shift(5) - 1`', consumers='predictor', display_order=4),
    FeatureEntry("vix_term_slope", "macro", "VIX spot vs VIX3M term structure slope, normalized", source="yfinance", refresh="daily", per_ticker=False, units='normalized', formula='`(vix - vix3m) / vix_baseline`', consumers='predictor', display_order=5),
    FeatureEntry("xsect_dispersion", "macro", "Cross-sectional std dev of daily returns across universe", source="computed", refresh="daily", per_ticker=False, units='stdev of universe returns', formula='precomputed series', consumers='predictor', display_order=6),
    # config#939 — credit spreads. ICE BofA US HY Index OAS (FRED
    # BAMLH0A0HYM2), percent. License-gated to 2023+ on FRED; pre-2023 /
    # missing rows fall back to the neutral default 0.0 (same pattern as
    # gold_mom_5d / oil_mom_5d), never hard-fail. Deliberately named
    # DISTINCT from crucible-predictor's regime_predictor.py
    # `hy_oas_level` / `hy_oas_change_21d` (a separate market-wide
    # regime-substrate feature family, sourced independently via its own
    # HYOAS.parquet cache and consumed only through cfg.MACRO_NORM_FEATURES
    # — not this feature-store namespace).
    FeatureEntry("hy_oas_credit_spread_pct", "macro", "ICE BofA US HY Index OAS (FRED BAMLH0A0HYM2), percent. License-gated to 2023+ on FRED; falls back to neutral 0.0 when unavailable.", source="fred", refresh="daily", per_ticker=False, units='percent (FRED native units, `_pct` suffix)', formula='ICE BofA US HY Index OAS, FRED series `BAMLH0A0HYM2`, ffilled onto the trading-day index', consumers="predictor (config#939 — credit spreads). License-gated to 2023+ on FRED; pre-2023 / missing rows fall back to neutral `0.0` (same pattern as `gold_mom_5d` / `oil_mom_5d`), never hard-fail. **Distinct from** crucible-predictor's `model/regime_predictor.py` `hy_oas_level` / `hy_oas_change_21d` — that is a separate market-wide regime-substrate feature family (own `HYOAS.parquet` source, consumed only via `cfg.MACRO_NORM_FEATURES`), not a `feature_engineer.FEATURES` / `registry.CATALOG` entry. Same underlying FRED series, deliberately different name/namespace to avoid collision.", display_order=7),

    # ── Regime interactions (5) — macro x ticker-specific signals ─────────────
    FeatureEntry("mom5d_x_vix", "interaction", "momentum_5d * VIX regime indicator", source="computed", refresh="daily", units='bare-named (return × regime)', formula='`momentum_5d * (vix_level - 1)`', consumers='predictor', display_order=0),
    FeatureEntry("rsi_x_vix", "interaction", "RSI deviation from 50 * VIX regime", source="computed", refresh="daily", units='bare-named (centered RSI × regime)', formula='`(rsi - 50) / 50 * (vix_level - 1)`', consumers='predictor', display_order=1),
    FeatureEntry("sector_x_trend", "interaction", "Sector-relative return * price trend", source="computed", refresh="daily", units='bare-named (sector-rel × SPY-trend)', formula='`sector_vs_spy_5d * spy_20d_return`', consumers='predictor', display_order=2),
    FeatureEntry("atr_x_vix", "interaction", "ATR * VIX regime indicator", source="computed", refresh="daily", units='decimal pct × regime', formula='`atr_14_pct * (vix_level - 1)`', consumers='predictor', display_order=3),
    FeatureEntry("vol_trend_x_vix", "interaction", "Volume trend * VIX regime indicator", source="computed", refresh="daily", units='ratio × regime', formula='`(volume_trend - 1) * (vix_level - 1)`', consumers='predictor', display_order=4),

    # ── Alternative data (7) — O10-O12 signals ───────────────────────────────
    FeatureEntry("earnings_surprise_pct", "alternative", "Most recent quarterly EPS surprise %", source="fmp", refresh="weekly", units='decimal pct (`_pct` suffix)', formula='FMP earnings surprise %', consumers='predictor', display_order=0),
    FeatureEntry("days_since_earnings", "alternative", "Days since last earnings (0-1, capped 90d)", source="fmp", refresh="weekly", units='0–1 normalized (days / 90)', formula='`days / 90.0`', consumers='predictor', display_order=1),
    FeatureEntry("eps_revision_4w", "alternative", "4-week cumulative EPS revision %", source="fmp", refresh="weekly", units='decimal pct (bare-named convention)', formula='FMP 4-week EPS revision %', consumers='predictor', display_order=2),
    FeatureEntry("revision_streak", "alternative", "Consecutive weeks of same-direction revisions", source="fmp", refresh="weekly", units='count (bare-named convention — integer count)', formula='Consecutive same-direction-revision weeks', consumers='predictor', display_order=3),
    FeatureEntry("put_call_ratio", "alternative", "Log-transformed put/call OI ratio", source="yfinance", refresh="weekly", units='log-transformed ratio', formula='`log(put_oi / call_oi)`', consumers='predictor', display_order=4),
    FeatureEntry("iv_rank", "alternative", "IV percentile rank (0-1)", source="yfinance", refresh="weekly", units='0–1 percentile rank', formula='IV percentile over 1y window', consumers='predictor', display_order=5),
    FeatureEntry("iv_vs_rv", "alternative", "Implied vol / realized vol ratio", source="yfinance", refresh="weekly", units='ratio', formula='`atm_iv / realized_vol_20d`', consumers='predictor', display_order=6),

    # ── v3.1 technical additions — horizon + decomposition + reversal-native ──
    FeatureEntry("return_60d", "technical", "60-day price return (Close_t / Close_{t-60} - 1)", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`close / close.shift(60) - 1`', consumers='predictor', display_order=28),
    FeatureEntry("return_120d", "technical", "120-day price return (Close_t / Close_{t-120} - 1)", source="yfinance", refresh="daily", units='decimal return (bare-named convention)', formula='`close / close.shift(120) - 1`', consumers='predictor', display_order=29),
    FeatureEntry("overnight_return_5d", "technical", "5d sum of overnight returns (Open_t vs Close_{t-1})", source="yfinance", refresh="daily", units='decimal sum-of-overnight (bare-named convention)', formula='`Σ (open_t / close_{t-1} - 1)` over 5d', consumers='predictor', display_order=30),
    FeatureEntry("intraday_return_5d", "technical", "5d sum of intraday returns (Close_t vs Open_t)", source="yfinance", refresh="daily", units='decimal sum-of-intraday (bare-named convention)', formula='`Σ (close_t / open_t - 1)` over 5d', consumers='predictor', display_order=31),
    FeatureEntry("dist_from_5d_high", "technical", "(Close - 5d rolling max High) / 5d rolling max High", source="yfinance", refresh="daily", units='decimal pct (bare-named convention)', formula='`(close - rolling_max_5) / rolling_max_5`', consumers='predictor', display_order=32),
    FeatureEntry("dist_from_20d_high", "technical", "(Close - 20d rolling max High) / 20d rolling max High", source="yfinance", refresh="daily", units='decimal pct (bare-named convention)', formula='`(close - rolling_max_20) / rolling_max_20`', consumers='predictor', display_order=33),

    # ── v3.2 per-ticker risk features (Stage 2 of regime-conditioning rebuild) ──
    FeatureEntry("beta_60d", "technical", "60d rolling regression slope of stock log-returns vs SPY log-returns (systematic market exposure)", source="yfinance", refresh="daily", units='dimensionless slope (bare-named convention)', formula='`rolling_60d cov(stock, spy) / var(spy)` (log returns)', consumers='predictor', display_order=34),
    FeatureEntry("idio_vol_60d", "technical", "60d residual vol after removing beta exposure; std × sqrt(252) (idiosyncratic risk)", source="yfinance", refresh="daily", units='annualized vol (decimal)', formula='`std(residual_returns).rolling(60) * sqrt(252)` after beta removal', consumers='predictor', display_order=35),
    FeatureEntry("vol_of_vol_30d", "technical", "30d rolling stdev of realized_vol_20d (stability of vol regime)", source="yfinance", refresh="daily", units='stdev of vol', formula='`realized_vol_20d.rolling(30).std()`', consumers='predictor', display_order=36),
    FeatureEntry("max_drawdown_60d", "technical", "Worst peak-to-trough drawdown within trailing 60d window (non-positive decimal pct)", source="yfinance", refresh="daily", units='non-positive decimal pct (bare-named convention)', formula='min of `(close / rolling_max_60 - 1)` over 60d', consumers='predictor', display_order=37),
    # W2 (L4469) — residual/idiosyncratic momentum family. residual_momentum_ratio
    # reuses the beta-residualized log-return series (same as idio_vol_60d), NO
    # beta recompute. Predictor-consumed; observe-gated in the L2 until validated.
    FeatureEntry("residual_momentum_ratio", "technical", "Vol-scaled cumulative residual (idiosyncratic) log-return over the 12-1 skip-month window: ∑resid_ret[t-252,t-21] / (σ_resid·√231) — an information ratio (Blitz/Hanauer residual momentum)", source="yfinance", refresh="daily", units='information ratio (dimensionless)', formula='`sum(residual_returns)[t-252,t-21] / (std(residual_returns).rolling(20) * sqrt(231))` — reuses the beta-residualized log-return (same series as idio_vol_60d)', consumers='predictor (W2 residual-momentum L1, observe-gated)', display_order=38),
    FeatureEntry("mom_12_1_pct", "technical", "12-1 skip-month raw price momentum: close.shift(21)/close.shift(252) - 1 (classic momentum factor, skips the recent-month reversal)", source="yfinance", refresh="daily", units='decimal return', formula='`close.shift(21) / close.shift(252) - 1` (12-1 skip-month momentum)', consumers='predictor (W2)', display_order=39),
    FeatureEntry("sector_mom_pct", "technical", "The ticker's sector-ETF own 12-1 skip-month momentum (GKX industry momentum — absolute, distinct from sector_vs_spy_* relative features)", source="yfinance", refresh="daily", units='decimal return', formula='sector-ETF `close.shift(21) / close.shift(252) - 1` (absolute industry momentum)', consumers='predictor (W2)', display_order=40),
    # W2.3 (L4469) — factor momentum (Gupta-Kelly "Factor Momentum Everywhere").
    # Cross-sectional-time-series projection: Σ_f zscore(loading_{i,f,t}) ×
    # factor_momentum_{f,t}. NOT produced by per-ticker compute_features —
    # materialized in a second pass over the universe lib (factor_momentum.
    # materialize_factor_momentum). Mirrors the post-assembly *_zscore loadings.
    FeatureEntry("factor_momentum_ratio", "technical", "Factor-momentum tilt (Gupta-Kelly): per date, dot the ticker's cross-sectionally z-scored factor loadings with each factor's trailing 12-1 long-short return momentum — Σ_f zscore(loading) × factor_mom. Dimensionless projection; backward-only (loadings at t × factor momentum through t-skip)", source="derived", refresh="daily", units='dimensionless projection', formula='`Σ_f zscore(loading_{i,f,t}) × factor_momentum_{f,t}` (Gupta-Kelly factor momentum) — **second-pass** column materialized over the full universe panel by `factor_momentum.materialize_factor_momentum` (not per-ticker `compute_features`); backward-only', consumers='predictor (W2.3, observe)', display_order=41),

    # ── Fundamental (13) — quarterly financials ───────────────────────────────
    FeatureEntry("pe_ratio", "fundamental", "Trailing P/E ratio, normalized (PE / 30)", source="fmp", refresh="quarterly", units='normalized (PE / 30)', formula='trailing P/E normalized', consumers='predictor', display_order=0),
    FeatureEntry("pb_ratio", "fundamental", "Price-to-book ratio, normalized (PB / 5)", source="fmp", refresh="quarterly", units='normalized (PB / 5)', formula='price-to-book normalized', consumers='predictor', display_order=1),
    FeatureEntry("debt_to_equity", "fundamental", "Total debt / total equity, normalized (D/E / 2)", source="fmp", refresh="quarterly", units='normalized (D/E / 2)', formula='total debt / total equity normalized', consumers='predictor', display_order=2),
    FeatureEntry("revenue_growth_yoy", "fundamental", "Year-over-year revenue growth (decimal)", source="fmp", refresh="quarterly", units='decimal pct (bare-named convention)', formula='year-over-year revenue growth', consumers='predictor', display_order=3),
    FeatureEntry("fcf_yield", "fundamental", "Free cash flow / market cap (decimal)", source="fmp", refresh="quarterly", units='decimal pct (bare-named convention)', formula='FCF / market cap', consumers='predictor', display_order=4),
    FeatureEntry("gross_margin", "fundamental", "Gross profit / revenue (0-1)", source="fmp", refresh="quarterly", units='0–1 fraction (bare-named convention)', formula='gross profit / revenue', consumers='predictor', display_order=5),
    FeatureEntry("roe", "fundamental", "Return on equity (decimal)", source="fmp", refresh="quarterly", units='decimal pct (bare-named convention)', formula='return on equity', consumers='predictor', display_order=6),
    FeatureEntry("current_ratio", "fundamental", "Current assets / current liabilities, normalized (CR / 3)", source="fmp", refresh="quarterly", units='normalized (CR / 3)', formula='current assets / current liabilities normalized', consumers='predictor', display_order=7),
    # ── Phase 3a of attractiveness-pillars-260520 — Growth + Stewardship pillar substrate ──
    FeatureEntry("revenue_growth_3y", "fundamental", "3-year revenue CAGR (decimal); Growth pillar input", source="fmp", refresh="quarterly", units='decimal pct CAGR (bare-named convention)', formula='3y revenue CAGR', consumers='predictor', display_order=8),
    FeatureEntry("eps_growth_3y", "fundamental", "3-year EPS CAGR (decimal); Growth pillar input", source="fmp", refresh="quarterly", units='decimal pct CAGR (bare-named convention)', formula='3y EPS CAGR', consumers='predictor', display_order=9),
    FeatureEntry("payout_ratio", "fundamental", "TTM dividends / net income (0-2 clipped); Stewardship pillar input — retention rate = 1 - payout drives reinvestment", source="fmp", refresh="quarterly", units='0–2 clipped ratio', formula='TTM dividends / net income', consumers='predictor', display_order=10),
    FeatureEntry("dividend_yield", "fundamental", "Indicated annual dividend yield (decimal, 0-0.2 clipped); Stewardship pillar input", source="fmp", refresh="quarterly", units='decimal pct (bare-named convention)', formula='indicated annual dividend yield', consumers='predictor', display_order=11),
    FeatureEntry("capex_growth_5y", "fundamental", "5-year CAPEX growth (decimal); Stewardship pillar input — reinvestment intensity proxy", source="fmp", refresh="quarterly", units='decimal pct (bare-named convention)', formula='5y CAPEX growth', consumers='predictor', display_order=12),
    # SIZE pillar substrate (config#1142) — raw market cap, absolute units
    # (the _raw suffix encodes absolute / un-normalized units). Surfaced from
    # Finnhub's already-fetched marketCapitalization; UN-clipped/UN-normalized.
    # Base input to the Barra SIZE loading (size_zscore); scale-invariant.
    FeatureEntry("market_cap_raw", "fundamental", "Raw market capitalization (absolute units, un-normalized) surfaced from Finnhub marketCapitalization. Base input to the Barra SIZE factor loading (size_zscore).", source="finnhub", refresh="quarterly", units='raw market cap, absolute units (`_raw` suffix)', formula='Finnhub `marketCapitalization`, un-clipped / un-normalized; base input to the Barra SIZE loading `size_zscore` (scale-invariant)', consumers='predictor; research (Barra SIZE loading source for score-neutralization, config#1142); executor risk-model (potential consumer)', display_order=13),

    # ── Factor loadings (9) — C.1 of optimizer-sota-upgrades-260526 ──────────
    # Cross-sectional ±3σ-winsorized z-scores of canonical Barra-style factors.
    # Columns of the factor-loading matrix B for the executor's Σ = B·F·Bᵀ + D
    # risk decomposition (workstream C.3). Computed POST-assembly in
    # features/compute.py via features.cross_sectional.apply_factor_zscores.
    FeatureEntry("momentum_20d_zscore", "factor_loading", "Cross-sectional z-score of momentum_20d (winsorized ±3σ). Barra MOMENTUM (short-horizon) loading.", source="derived", refresh="daily", units='z-score (`_zscore` suffix)', formula='Cross-sectional z of `momentum_20d`, ±3σ winsorized', consumers='executor (Barra MOMENTUM short-horizon, C.3)', display_order=0),
    FeatureEntry("return_60d_zscore", "factor_loading", "Cross-sectional z-score of return_60d (winsorized ±3σ). Barra MOMENTUM (medium-horizon) loading.", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `return_60d`, ±3σ winsorized', consumers='executor (Barra MOMENTUM medium-horizon, C.3)', display_order=1),
    FeatureEntry("beta_60d_zscore", "factor_loading", "Cross-sectional z-score of beta_60d (winsorized ±3σ). Barra BETA loading (market sensitivity).", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `beta_60d`, ±3σ winsorized', consumers='executor (Barra BETA loading — market sensitivity, C.3)', display_order=2),
    FeatureEntry("idio_vol_60d_zscore", "factor_loading", "Cross-sectional z-score of idio_vol_60d (winsorized ±3σ). Barra RESVOL loading (residual / idiosyncratic risk).", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `idio_vol_60d`, ±3σ winsorized', consumers='executor (Barra RESVOL — idiosyncratic risk, C.3)', display_order=3),
    FeatureEntry("realized_vol_63d_zscore", "factor_loading", "Cross-sectional z-score of realized_vol_63d (winsorized ±3σ). Barra VOLATILITY loading (total realized risk).", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `realized_vol_63d`, ±3σ winsorized', consumers='executor (Barra VOLATILITY — total realized risk, C.3)', display_order=4),
    FeatureEntry("dist_from_52w_high_zscore", "factor_loading", "Cross-sectional z-score of dist_from_52w_high (winsorized ±3σ). Proximity-to-high / reversal-risk loading.", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `dist_from_52w_high`, ±3σ winsorized', consumers='executor (proximity-to-high / reversal-risk loading, C.3)', display_order=5),
    FeatureEntry("pe_ratio_zscore", "factor_loading", "Cross-sectional z-score of pe_ratio (winsorized ±3σ). Barra VALUE loading (proxy via 1/PE direction).", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `pe_ratio`, ±3σ winsorized', consumers='executor (Barra VALUE proxy via 1/PE direction, C.3)', display_order=6),
    FeatureEntry("roe_zscore", "factor_loading", "Cross-sectional z-score of roe (winsorized ±3σ). Barra QUALITY / profitability loading.", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `roe`, ±3σ winsorized', consumers='executor (Barra QUALITY — profitability loading, C.3)', display_order=7),
    FeatureEntry("size_zscore", "factor_loading", "Cross-sectional z-score of log(market_cap_raw) (winsorized ±3σ). Barra SIZE loading (config#1142) — completes the institutional Barra factor set.", source="derived", refresh="daily", units='z-score', formula='Cross-sectional z of `log(market_cap_raw)`, ±3σ winsorized (log pre-transform; non-positive cap → NaN, excluded)', consumers='research (Barra SIZE loading for momentum+beta+size score-neutralization, config#1142); executor risk-model (Barra SIZE — potential C.3 consumer)', display_order=8),
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
