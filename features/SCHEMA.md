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
`nousergon_lib`. Filed as P3 follow-up.

---

## 2a. Price-basis columns (CRSP — NOT feature-catalog entries)

These are price/return LEVEL columns persisted on the universe library
alongside the feature catalog. They are **not** in `registry.py::CATALOG`
or §3 (those are the engineered features); they are the price basis the
features are computed *from*. Canonical column order is the single source
of truth in `store/arctic_store.py` (`OHLCV_COLS` + `TOTAL_RETURN_COL` +
`source` + FEATURES).

| Column | Units | Compute | Consumers |
|---|---|---|---|
| `Close` | absolute price (split-adjusted LEVEL) | polygon-authoritative split restatement (`corporate_actions.apply` → `_split_math.restate_series_for_splits`); changes only on splits | features (basis), executor, research, predictor |
| `total_return_close` | absolute price (split-adjusted + dividend-back-adjusted; total-return axis) | `Close` further back-adjusted by registry dividend events via `corporate_actions.total_return_series` — a SEPARATE series that does NOT mutate `Close` | features (basis at PR7-7c cutover), predictor label (7c) |

**Status (PR7-7a, config#1434):** `total_return_close` is ADDITIVE and,
as of this PR, written ONLY to the OFFLINE scratch CRSP-basis library
(`builders/migrate_universe_crsp_basis.py` → `universe_crsp`). The live
`universe` library does not yet carry it, and the feature/label basis
still reads `Close`. The flip of the feature basis
(`feature_engineer.py` `close_col` default → `total_return_close`), the
ne-data + predictor label flip, and the `backfill`/`daily_append`
dual-writer are GATED to PR7-7c after the shadow-retrain + backtest gate.
Placed immediately after `Close` in the canonical layout so the price-level
and return-basis columns sit adjacent.

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
| `sub_sector_vs_benchmark_5d` | decimal return (bare-named convention) | `sub_sector_etf_5d - SPY_5d` (sub-sector ETF via sub_sector_etf_map, falls back to sector ETF) | predictor |
| `sub_sector_vs_benchmark_10d` | decimal return (bare-named convention) | `sub_sector_etf_10d - SPY_10d` (sub-sector ETF via sub_sector_etf_map, falls back to sector ETF) | predictor |
| `sub_sector_vs_benchmark_20d` | decimal return (bare-named convention) | `sub_sector_etf_20d - SPY_20d` (sub-sector ETF via sub_sector_etf_map, falls back to sector ETF) | predictor |
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
| `residual_momentum_ratio` | information ratio (dimensionless) | `sum(residual_returns)[t-252,t-21] / (std(residual_returns).rolling(20) * sqrt(231))` — reuses the beta-residualized log-return (same series as idio_vol_60d) | predictor (W2 residual-momentum L1, observe-gated) |
| `mom_12_1_pct` | decimal return | `close.shift(21) / close.shift(252) - 1` (12-1 skip-month momentum) | predictor (W2) |
| `sector_mom_pct` | decimal return | sector-ETF `close.shift(21) / close.shift(252) - 1` (absolute industry momentum) | predictor (W2) |
| `factor_momentum_ratio` | dimensionless projection | `Σ_f zscore(loading_{i,f,t}) × factor_momentum_{f,t}` (Gupta-Kelly factor momentum) — **second-pass** column materialized over the full universe panel by `factor_momentum.materialize_factor_momentum` (not per-ticker `compute_features`); backward-only | predictor (W2.3, observe) |
| `vwap_divergence_pct` | decimal pct (`_pct` suffix) | `(Close - VWAP) / VWAP` | predictor (config#939 — VWAP divergence). NaN when VWAP is unavailable (yfinance-fallback rows; the documented 2026-04-17→23 Polygon outage) or when VWAP is 0 (guarded via `.replace(0, nan)`) |
| `cmf_20_ratio` | dimensionless ratio, bounded ~[-1, 1] (`_ratio` suffix) | Chaikin Money Flow: `rolling_sum(MFM * Volume, 20) / rolling_sum(Volume, 20)` where `MFM = ((Close-Low)-(High-Close))/(High-Low)` | predictor (config#939 — buying/selling pressure). `High == Low` guarded to NaN via `.replace(0, nan)`, mirroring `volume_trend` / `obv_slope_10d` |

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
| `hy_oas_credit_spread_pct` | percent (FRED native units, `_pct` suffix) | ICE BofA US HY Index OAS, FRED series `BAMLH0A0HYM2`, ffilled onto the trading-day index | predictor (config#939 — credit spreads). License-gated to 2023+ on FRED; pre-2023 / missing rows fall back to neutral `0.0` (same pattern as `gold_mom_5d` / `oil_mom_5d`), never hard-fail. **Distinct from** crucible-predictor's `model/regime_predictor.py` `hy_oas_level` / `hy_oas_change_21d` — that is a separate market-wide regime-substrate feature family (own `HYOAS.parquet` source, consumed only via `cfg.MACRO_NORM_FEATURES`), not a `feature_engineer.FEATURES` / `registry.CATALOG` entry. Same underlying FRED series, deliberately different name/namespace to avoid collision. |

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
| `market_cap_raw` | raw market cap, absolute units (`_raw` suffix) | Finnhub `marketCapitalization`, un-clipped / un-normalized; base input to the Barra SIZE loading `size_zscore` (scale-invariant) | predictor; research (Barra SIZE loading source for score-neutralization, config#1142); executor risk-model (potential consumer) |

### Factor loadings (cross-sectional, daily refresh)

Columns of the factor-loading matrix **B** consumed by the executor's
structural risk decomposition Σ = B·F·Bᵀ + D (workstream C.3 of
`alpha-engine-docs/private/optimizer-sota-upgrades-260526.md`). Each
column is a cross-sectional ±3σ-winsorized z-score of the named source
column, computed POST-assembly in `features/compute.py` via
`features.cross_sectional.apply_factor_zscores`. Winsorization +
re-standardization follow the Barra USE4 / AQR convention so a single
outlier ticker cannot dominate the downstream factor-return regression
(C.2, alpha-engine-predictor).

**`roe_zscore` known-degenerate (config#1765, open).** Live `roe` is
clip-saturated at its `[-1, 1]` bound for ~98% of the universe (median
`roe` == 1.0), which collapses `roe_zscore` to all-NaN everywhere
(`apply_factor_zscores`'s MAD-degeneracy guard correctly refuses to
fabricate a z-score from a near-constant cross-section — this is NOT a
bug in this module). Root cause traced to `collectors/fundamentals.py`
(`_fetch_single_ticker`): its Finnhub field mapping assumes `roeTTM` /
`roeRfy` (and several other fields — `gross_margin`, `revenue_growth_yoy`,
`revenue_growth_3y`, `eps_growth_3y`, `payout_ratio`, `dividend_yield`
show the same saturation pattern) are 0–1 fractions, but the live
`archive/fundamentals/{date}.json` payloads are consistent with Finnhub
returning those fields as raw percentages (e.g. `62` meaning 62%, not
`0.62`) — every one of them saturates at its declared clip bound for
the bulk of the 903-ticker universe. Fixing the unit scaling needs a
live Finnhub payload to confirm the exact conversion per field (not
done here — out of scope for the config#1765 groom pass, which only
covers the predictor-side symptom). Until this collector bug is fixed,
`alpha-engine-predictor/training/risk_model_persist.py` pins a 7-factor
`FACTOR_LOADING_COLUMNS` set that excludes `roe_zscore` (QUALITY
loading) so the weekly F/D persistence isn't blocked on a permanently
all-NaN column. Re-add `roe_zscore` to that set once `roe` is a real,
non-degenerate cross-section again.

| Field | Units | Compute | Consumers |
|---|---|---|---|
| `momentum_20d_zscore` | z-score (`_zscore` suffix) | Cross-sectional z of `momentum_20d`, ±3σ winsorized | executor (Barra MOMENTUM short-horizon, C.3) |
| `return_60d_zscore` | z-score | Cross-sectional z of `return_60d`, ±3σ winsorized | executor (Barra MOMENTUM medium-horizon, C.3) |
| `beta_60d_zscore` | z-score | Cross-sectional z of `beta_60d`, ±3σ winsorized | executor (Barra BETA loading — market sensitivity, C.3) |
| `idio_vol_60d_zscore` | z-score | Cross-sectional z of `idio_vol_60d`, ±3σ winsorized | executor (Barra RESVOL — idiosyncratic risk, C.3) |
| `realized_vol_63d_zscore` | z-score | Cross-sectional z of `realized_vol_63d`, ±3σ winsorized | executor (Barra VOLATILITY — total realized risk, C.3) |
| `dist_from_52w_high_zscore` | z-score | Cross-sectional z of `dist_from_52w_high`, ±3σ winsorized | executor (proximity-to-high / reversal-risk loading, C.3) |
| `pe_ratio_zscore` | z-score | Cross-sectional z of `pe_ratio`, ±3σ winsorized | executor (Barra VALUE proxy via 1/PE direction, C.3) |
| `roe_zscore` | z-score | Cross-sectional z of `roe`, ±3σ winsorized | executor (Barra QUALITY — profitability loading, C.3) |
| `size_zscore` | z-score | Cross-sectional z of `log(market_cap_raw)`, ±3σ winsorized (log pre-transform; non-positive cap → NaN, excluded) | research (Barra SIZE loading for momentum+beta+size score-neutralization, config#1142); executor risk-model (Barra SIZE — potential C.3 consumer) |

### 3b. Private-pack columns (alpha-engine-config#1032) — disclosure format

Alpha-bearing columns computed by a private feature pack
(`features/private_pack.py`, discovered at runtime via
`NOUSERGON_PRIVATE_FEATURE_PACK`) per the private-edge divergence policy
(config#1031: new alpha-bearing feature recipes land private-first). They
ARE ordinary `registry.CATALOG` entries and DO get a row in this table —
consumers still need name, units, and who reads it. The one thing this
repo does not disclose is HOW the value is computed.

**Disclosure rule:** a private-pack row's `Compute` cell is the literal
sentinel `private pack` — no formula, no source ref, no hint at the
signal. Name (units-suffixed per §1) + units + consumer are the full
public surface. `registry.FeatureEntry.compute` is set to
`registry.PRIVATE_PACK_COMPUTE` for these rows, which is what lets
`test_schema_contract.py` tell a private-pack row apart from an
undocumented public one (a public column with a blank/missing formula is
a bug; a private-pack column with `compute="private pack"` is by design).

There are no private-pack columns registered in this public repo today —
this subsection is the mechanism's contract, exercised end-to-end by
`tests/test_private_feature_pack.py` against a throwaway test fixture
(`tests/fixtures/dummy_private_pack.py`), not by a real entry here. When
the first real alpha-bearing column lands, add a table with the same
four columns as §3 (Field / Units / Compute / Consumers) directly below
this paragraph; the `Field` cell is the backticked column name (e.g. a
hypothetical `some_alpha_signal_zscore`), `Compute` is always the literal
`private pack`, and `Units` / `Consumers` are filled in exactly as they
would be for a public row.

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
6. **Name the universe-library restate step in the PR body** per the
   column-add rollout contract in §6 — widening `FEATURES` does NOT
   migrate the ~900 existing static-schema ArcticDB symbols on its own,
   so the PR that widens the schema must also schedule the full-history
   restate that lays down the new descriptor. Omitting it breaks the
   next `daily_append` per-ticker (config#2006).
7. `pytest tests/test_schema_contract.py
   tests/test_daily_append_schema_evolution_2006.py` must pass.

### 4b. PR checklist for a NEW private-pack column (alpha-engine-config#1032)

Before landing a new alpha-bearing column through the private pack:

1. Pick a units-suffixed name per §1, same as a public column.
2. Add a `FeatureEntry(name, group, description="private pack", ...,
   compute=registry.PRIVATE_PACK_COMPUTE)` to `features/registry.py::CATALOG`.
   The `description` field may say WHO consumes it and WHY at a level
   consistent with §3b's disclosure rule, but must not describe the
   compute.
3. Add a row to §3b of this file (name + units + `private pack` + consumer
   — no formula).
4. Do **NOT** add the column to `features/feature_engineer.py::FEATURES`
   — it must be produced by the private pack's `add_private_features`,
   not by public `compute_features` (`test_private_pack_entries_are_absent_from_public_features`
   enforces this).
5. Implement `add_private_features` + `PRIVATE_FEATURE_NAMES` in the
   private pack module pointed to by `NOUSERGON_PRIVATE_FEATURE_PACK` —
   see `features/private_pack.py` for the contract.
6. `pytest tests/test_schema_contract.py tests/test_private_feature_pack.py`
   must pass.

---

## 5. Historical reference

| Date | Event |
|---|---|
| 2026-05-25 | L1995 Phase 1 standalone scanner Lambda surfaces `scanner_tickers=[]`; audit reveals `avg_volume_20d` units mismatch with Research scanner consumer; Option E selected as SOTA fix. |
| 2026-05-25 | This SCHEMA.md + additive `avg_volume_20d_raw` + naming-convention rule shipped as the institutional substrate (alpha-engine-data Phase 1). |
| 2026-05-26 | C.1 of optimizer-sota-upgrades-260526 — 8 factor-loading `*_zscore` columns added (cross-sectional ±3σ-winsorized z-scores) as substrate for the executor's Σ = B·F·Bᵀ + D risk decomposition. |
| 2026-07-01 | alpha-engine-config#1032 (private-edge divergence policy, config#1031) — private feature-pack loading mechanism (`features/private_pack.py`) + `compute=PRIVATE_PACK_COMPUTE` schema-contract sentinel (§3b) shipped. No alpha-bearing column has landed through it yet; only the throwaway fixture in `tests/fixtures/dummy_private_pack.py` exercises the mechanism. |
| 2026-07-08 | alpha-engine-config#939 — 3 of the 7 originally-listed feature gaps shipped (the other 4 had already landed): `vwap_divergence_pct` (VWAP divergence), `cmf_20_ratio` (Chaikin Money Flow — buying/selling pressure, chosen over MFI-14 / Chaikin A/D for its bounded range and fewest edge cases), `hy_oas_credit_spread_pct` (credit spreads, FRED `BAMLH0A0HYM2` / `HYOAS`, deliberately named distinct from crucible-predictor's separate regime-substrate `hy_oas_level`). All 3 computed from already-ingested data; no new data source. |
| 2026-07-08 | config#2006 — the config#939 widening (92→95 cols) shipped with no restate for the existing static-schema universe symbols; the first live `daily_append` failed per-ticker with `StreamDescriptorMismatch`. Fix: full-history restate + the §6 column-add rollout contract + the `test_daily_append_schema_evolution_2006.py` CI tripwire that reproduces the failure against a real old-schema library and pins the restate recovery. |

---

## 6. Column-add rollout contract (config#2006) — STANDING DIRECTIVE

The universe ArcticDB library is **static-schema**: a symbol's column set
is frozen at write time, and an `update_batch` / `write` whose descriptor
adds a column is rejected with `StreamDescriptorMismatch` until that
symbol is rewritten at the new schema. `to_arctic_canonical`
(`store/arctic_store.py`) makes a `FEATURES` widening a safe *additive
column-ORDER* change at the write chokepoint, **but it does not migrate
already-stored symbols** — the ~900 existing universe symbols keep their
old descriptor until a full-history rewrite lands the new one.

**Contract (option A — chosen, the default going forward):** every PR
that widens `features/feature_engineer.py::FEATURES` (or otherwise changes
the persisted universe column set) MUST, in the same PR body, name the
scheduled **full-history restate** that migrates the existing library to
the new schema, and that restate MUST run before the next `daily_append`
against the affected library. The restate is the existing
`builders/backfill.py` full-symbol rewrite (it recomputes features and
`write_batch`es full history — establishing the new descriptor);
`builders/migrate_universe_crsp_basis.py` is the precedent for a scoped
one-off universe migration. Run it on a data-spot box
(`infrastructure/spot_data_weekly.sh` pattern), never multi-hour compute
on the trading box.

**Why not option B (`dynamic_schema=True`):** flipping the universe
library to dynamic schema would make column additions absorb without a
restate, but it is a **deliberate, non-casual** change — it alters
read-path performance and type-inference behaviour across every consumer
(predictor training + inference read the full panel), and would need its
own shadow-read parity gate + benchmark before cutover. It is recorded
here as the explicit alternative, NOT the current contract; do not flip it
as a shortcut to skip a restate.

**Enforcement.** `tests/test_daily_append_schema_evolution_2006.py` is the
CI tripwire for the bug class: it reproduces the mismatch against a real
old-schema LMDB library through the real `to_arctic_canonical` →
`update_batch` path, asserts the failure is SURFACED (per-symbol
`DataError` → `n_err`, never a silent success or silent column-drop), and
pins that a full-history restate is what makes the append green again. A
future change that routes universe appends through a schema-narrowing
helper (dropping the new column to force a false-green write) turns that
test red — that silent-partial-coverage regression is precisely what the
contract forbids.
