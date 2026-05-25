# Feature store — schema contract

**Authoritative reference for every column emitted by
`features/feature_engineer.py::compute_features` and persisted to the
ArcticDB universe library + S3 feature store parquet.**

Status: stable. Updated whenever a feature is added or its semantic
changes. The companion test
`tests/test_schema_contract.py` cross-checks this file against
`features/registry.py::CATALOG` and `features/feature_engineer.FEATURES`
— PRs that add or rename a column without updating SCHEMA.md fail CI.

---

## 1. Naming-convention rule (STANDING DIRECTIVE for new fields)

Every new feature column **MUST** carry an explicit units suffix when
its name alone could be misread by a downstream consumer. Allowed
suffixes:

| Suffix | Units / shape | Examples |
|---|---|---|
| `_raw` | Absolute units (shares, dollars, count). Consumer reads native scale. | `avg_volume_20d_raw` |
| `_ratio` | Dimensionless ratio (typical range 0.5–2.0 or similar bounded band). | `rel_volume_ratio`, `vol_ratio_10_60` |
| `_pct` | Decimal percentage (−1.0 to 1.0 typical). | `atr_14_pct` |
| `_zscore` | Standardized z-score (mean 0, std 1 cross-sectionally). | (none today) |
| `_log_return` | Natural-log return. | (none today) |

**Bare-named fields** (no suffix) inherit one of these semantics by
historical convention — every such field MUST appear in §3 below with
its actual units explicitly documented. The bare-name == normalized /
ratio convention is grandfathered; new bare-named fields are NOT
permitted.

**Rationale.** A column named `avg_volume_20d` was emitted as a
ratio (`rolling_20d / per-ticker global mean`) for predictor input,
while `alpha-engine-research/data/scanner.py` consumed it as raw
shares (compared against `MIN_AVG_VOLUME = 500_000`). The result was
**901/903 feature-store-covered tickers silently failing the Research
scanner liquidity gate** for months, surfaced only when L1995 Phase 1
shipped a standalone scanner Lambda that wrote operator-visible
`candidates.json::scanner_tickers=[]`. The audit recovery, the additive
`avg_volume_20d_raw` emit, and this contract substrate are the
institutional fix.

See `~/Development/alpha-engine-docs/private/feature-store-schema-audit-260525.md`
for the full design doc.

---

## 2. Consumer contract

Two repositories read from the feature store today:

| Consumer | Repo | Read path | Units expected |
|---|---|---|---|
| **Predictor** (training + inference) | alpha-engine-predictor | ArcticDB universe library → `model/meta_model.py` META_FEATURES | All bare-named fields treated as predictor input. Normalized / ratio shape per §3. |
| **Research scanner** | alpha-engine-research | `fetch_data_node` (graph) + `scanner_orchestrator._build_technical_scores_from_feature_store` | `_raw` suffix required for ABSOLUTE-quantity gates (avg_volume_20d_raw for the liquidity gate). Returns / ratios / pcts consumed at native shape. |

A third consumer (executor for trade features? backtester for parity
features?) appearing in the future triggers a per-`[[feedback_lift_invariants_to_chokepoint_after_second_recurrence]]`
lift of this contract from alpha-engine-data into
`alpha_engine_lib`. Filed as P3 follow-up.

---

## 3. Field catalog — units, compute, consumers

Sorted by group, matching `features/registry.py::CATALOG`. Every entry
must be unique with the registry; the schema-contract test enforces
parity.

### Technical (per-ticker, daily refresh)

| Field | Units | Compute | Consumers |
|---|---|---|---|
| `rsi_14` | 0–100 score (bare-named = normalized) | Wilder's RSI(14) via EWM(com=13) | predictor + scanner |
| `macd_cross` | tri-state (+1 bull / −1 bear / 0 none) | Sign of MACD line crossing 0 within last 3 days | predictor + scanner |
| `macd_above_zero` | binary (0/1) | `(macd_line > 0).astype(float)` | predictor + scanner |
| `macd_line_last` | absolute (price units) | `EMA_fast(close) - EMA_slow(close)` | predictor |
| `price_vs_ma50` | decimal pct (bare-named convention) | `(close - SMA50) / SMA50` | predictor + scanner (gate at −0.05) |
| `price_vs_ma200` | decimal pct (bare-named convention) | `(close - SMA200) / SMA200` | predictor + scanner |
| `momentum_20d` | decimal return (bare-named convention) | `close / close.shift(20) - 1` | predictor + scanner |
| `avg_volume_20d` | ratio (rolling_20d / per-ticker global mean), ~1.0 typical | `avg_vol_20d / volume.mean()` | **predictor only** — relative liquidity feature |
| `avg_volume_20d_raw` | **raw shares** | `volume.rolling(20).mean()` | **scanner only** — absolute liquidity gate vs `MIN_AVG_VOLUME=500_000` |
| `dist_from_52w_high` | decimal pct (bare-named convention) | `(close - rolling_max_252) / rolling_max_252` | predictor + scanner |
| `momentum_5d` | decimal return (bare-named convention) | `close / close.shift(5) - 1` | predictor + scanner |
| `rel_volume_ratio` | ratio (today / rolling_20d) | `volume / volume.rolling(20).mean()` | predictor |
| `return_vs_spy_5d` | decimal return (bare-named convention) | `momentum_5d - SPY_5d_return` | predictor |
| `dist_from_52w_low` | decimal pct (bare-named convention) | `(close - rolling_min_252) / rolling_min_252` | predictor |
| `vol_ratio_10_60` | ratio | `realized_vol_10d / realized_vol_60d` | predictor |
| `bollinger_pct` | 0–1 channel position (bare-named convention) | `(close - lower_bb) / (upper_bb - lower_bb)` | predictor |
| `sector_vs_spy_5d` | decimal return (bare-named convention) | `sector_etf_5d - SPY_5d` | predictor |
| `sector_vs_spy_10d` | decimal return (bare-named convention) | `sector_etf_10d - SPY_10d` | predictor |
| `sector_vs_spy_20d` | decimal return (bare-named convention) | `sector_etf_20d - SPY_20d` | predictor |
| `price_accel` | decimal (5d return − 20d return) | `momentum_5d - momentum_20d` | predictor |
| `ema_cross_8_21` | ratio (bare-named convention, `EMA8/EMA21 - 1`) | `EMA(8) / EMA(21) - 1` | predictor |
| `atr_14_pct` | decimal pct (`_pct` suffix) | `ATR(14) / close` | predictor + scanner (consumer ×100 to display %) |
| `realized_vol_20d` | annualized vol (decimal) | `std(daily_returns).rolling(20) * sqrt(252)` | predictor |
| `realized_vol_63d` | annualized vol (decimal) | `std(daily_returns).rolling(63) * sqrt(252)` | predictor |
| `volume_trend` | ratio (5d / 20d avg volume) | `vol_5 / vol_20` | predictor |
| `obv_slope_10d` | normalized slope (bare-named convention) | `(OBV_fast - OBV_slow) / vol_20` | predictor |
| `rsi_slope_5d` | bare-named (RSI delta / 5) | `(rsi - rsi.shift(5)) / 5` | predictor |
| `volume_price_div` | tri-state (sign × sign) | `sign(volume_trend - 1) * sign(momentum_5d)` | predictor |
| `return_60d` | decimal return (bare-named convention) | `close / close.shift(60) - 1` | predictor |
| `return_120d` | decimal return (bare-named convention) | `close / close.shift(120) - 1` | predictor |
| `overnight_return_5d` | decimal sum-of-overnight (bare-named convention) | `Σ (open_t / close_{t-1} - 1)` over 5d | predictor |
| `intraday_return_5d` | decimal sum-of-intraday (bare-named convention) | `Σ (close_t / open_t - 1)` over 5d | predictor |
| `dist_from_5d_high` | decimal pct (bare-named convention) | `(close - rolling_max_5) / rolling_max_5` | predictor |
| `dist_from_20d_high` | decimal pct (bare-named convention) | `(close - rolling_max_20) / rolling_max_20` | predictor |
| `beta_60d` | dimensionless slope (bare-named convention) | `rolling_60d cov(stock, spy) / var(spy)` (log returns) | predictor |
| `idio_vol_60d` | annualized vol (decimal) | `std(residual_returns).rolling(60) * sqrt(252)` after beta removal | predictor |
| `vol_of_vol_30d` | stdev of vol | `realized_vol_20d.rolling(30).std()` | predictor |
| `max_drawdown_60d` | non-positive decimal pct (bare-named convention) | min of `(close / rolling_max_60 - 1)` over 60d | predictor |

### Macro (one row per date — `per_ticker=False`)

| Field | Units | Compute | Consumers |
|---|---|---|---|
| `vix_level` | normalized (VIX / 20) | `vix / vix_baseline` | predictor |
| `yield_10y` | normalized (TNX / 10) | `tnx / tnx_normalizer` | predictor |
| `yield_curve_slope` | normalized | `(tnx - irx) / tnx_normalizer` | predictor |
| `gold_mom_5d` | decimal return | `gld / gld.shift(5) - 1` | predictor |
| `oil_mom_5d` | decimal return | `uso / uso.shift(5) - 1` | predictor |
| `vix_term_slope` | normalized | `(vix - vix3m) / vix_baseline` | predictor |
| `xsect_dispersion` | stdev of universe returns | precomputed series | predictor |

### Regime interactions (per-ticker × macro)

| Field | Units | Compute | Consumers |
|---|---|---|---|
| `mom5d_x_vix` | bare-named (return × regime) | `momentum_5d * (vix_level - 1)` | predictor |
| `rsi_x_vix` | bare-named (centered RSI × regime) | `(rsi - 50) / 50 * (vix_level - 1)` | predictor |
| `sector_x_trend` | bare-named (sector-rel × SPY-trend) | `sector_vs_spy_5d * spy_20d_return` | predictor |
| `atr_x_vix` | decimal pct × regime | `atr_14_pct * (vix_level - 1)` | predictor |
| `vol_trend_x_vix` | ratio × regime | `(volume_trend - 1) * (vix_level - 1)` | predictor |

### Alternative data (weekly refresh)

| Field | Units | Compute | Consumers |
|---|---|---|---|
| `earnings_surprise_pct` | decimal pct (`_pct` suffix) | FMP earnings surprise % | predictor |
| `days_since_earnings` | 0–1 normalized (days / 90) | `days / 90.0` | predictor |
| `eps_revision_4w` | decimal pct (bare-named convention) | FMP 4-week EPS revision % | predictor |
| `revision_streak` | count (bare-named convention — integer count) | Consecutive same-direction-revision weeks | predictor |
| `put_call_ratio` | log-transformed ratio | `log(put_oi / call_oi)` | predictor |
| `iv_rank` | 0–1 percentile rank | IV percentile over 1y window | predictor |
| `iv_vs_rv` | ratio | `atm_iv / realized_vol_20d` | predictor |

### Fundamental (quarterly refresh)

| Field | Units | Compute | Consumers |
|---|---|---|---|
| `pe_ratio` | normalized (PE / 30) | trailing P/E normalized | predictor |
| `pb_ratio` | normalized (PB / 5) | price-to-book normalized | predictor |
| `debt_to_equity` | normalized (D/E / 2) | total debt / total equity normalized | predictor |
| `revenue_growth_yoy` | decimal pct (bare-named convention) | year-over-year revenue growth | predictor |
| `fcf_yield` | decimal pct (bare-named convention) | FCF / market cap | predictor |
| `gross_margin` | 0–1 fraction (bare-named convention) | gross profit / revenue | predictor |
| `roe` | decimal pct (bare-named convention) | return on equity | predictor |
| `current_ratio` | normalized (CR / 3) | current assets / current liabilities normalized | predictor |
| `revenue_growth_3y` | decimal pct CAGR (bare-named convention) | 3y revenue CAGR | predictor |
| `eps_growth_3y` | decimal pct CAGR (bare-named convention) | 3y EPS CAGR | predictor |
| `payout_ratio` | 0–2 clipped ratio | TTM dividends / net income | predictor |
| `dividend_yield` | decimal pct (bare-named convention) | indicated annual dividend yield | predictor |
| `capex_growth_5y` | decimal pct (bare-named convention) | 5y CAPEX growth | predictor |

---

## 4. PR checklist for new features

Before opening a PR that adds a column to `compute_features`:

1. Pick a name with an explicit units suffix from §1, OR justify a
   bare-named exception in the PR body (the test will fail otherwise).
2. Add a `FeatureEntry(...)` to `features/registry.py::CATALOG` with a
   description that names units explicitly. Description must not say
   "20-day avg X" without also stating the units (shares, dollars, %).
3. Add the field to `features/feature_engineer.py::FEATURES` in the
   correct group.
4. Add a row to §3 of this file.
5. If the field has a NEW consumer (e.g., backtester reads it for
   parity), add the consumer to §2 AND add a consumer-contract test in
   the consuming repo that pins the expected units.
6. `pytest tests/test_schema_contract.py` must pass.

---

## 5. Historical reference

| Date | Event |
|---|---|
| 2026-05-25 | L1995 Phase 1 standalone scanner Lambda surfaces `scanner_tickers=[]`; audit reveals `avg_volume_20d` units mismatch with Research scanner consumer; Option E selected as SOTA fix. |
| 2026-05-25 | This SCHEMA.md + additive `avg_volume_20d_raw` + naming-convention rule shipped as the institutional substrate (alpha-engine-data Phase 1). |
