"""
features/feature_engineer.py — Rolling technical feature computation.

Self-contained copy of alpha-engine-predictor/data/feature_engineer.py.
All config references replaced with local constants (no predictor imports).

Mirrors compute_technical_indicators() from alpha-engine-research exactly,
but operates on a full OHLCV DataFrame (rolling window per row) rather than
returning a single snapshot dict. Every row in the output has a complete
feature vector. Rows lacking sufficient price history for any indicator
are dropped after all features are computed.

Feature groups:
  v1.0-v1.5: 34 price/volume/macro features (29 core + 5 regime interactions)
  v2.0 (O10-O12): 7 alternative data features (earnings, revisions, options)
  v3.0: 8 fundamental ratios (quarterly, from FMP)

Effective warmup is 252 rows (dist_from_52w_high / dist_from_52w_low).
Tickers with fewer than ~265 rows should be skipped by the caller.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ── Feature engineering parameters (from predictor.sample.yaml) ──────────────
# These replace `from config import FEATURE_CFG as _FC` in the predictor.
FEATURE_CFG: dict = {
    "rsi_period": 14,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "macd_cross_window": 3,
    "ma_short": 50,
    "ma_long": 200,
    "momentum_short": 5,
    "momentum_long": 20,
    "weeks_52_days": 252,
    "vol_short_window": 10,
    "vol_long_window": 60,
    "bollinger_window": 20,
    "bollinger_std": 2.0,
    "vix_baseline": 20.0,
    "tnx_normalizer": 10.0,
    "ema_fast_span": 8,
    "ema_slow_span": 21,
    "atr_period": 14,
    "realized_vol_window": 20,
    "volume_fast": 5,
    "volume_slow": 20,
    "obv_fast": 5,
    "obv_slow": 20,
    "rsi_slope_window": 5,
    # v3.2 risk feature windows (Stage 2 of regime-conditioning rebuild)
    "beta_window": 60,
    "vol_of_vol_window": 30,
    "max_drawdown_window": 60,
}

_FC = FEATURE_CFG

# ── Feature list (from predictor config.py lines 62-124) ────────────────────
FEATURES = [
    "rsi_14",
    "macd_cross",
    "macd_above_zero",
    "macd_line_last",
    "price_vs_ma50",
    "price_vs_ma200",
    "momentum_20d",
    "avg_volume_20d",
    # v1.1 additions
    "dist_from_52w_high",
    "momentum_5d",
    "rel_volume_ratio",
    "return_vs_spy_5d",
    # v1.2 additions — market context features
    "vix_level",
    "dist_from_52w_low",
    "vol_ratio_10_60",
    "bollinger_pct",
    "sector_vs_spy_5d",
    "sector_vs_spy_10d",
    "sector_vs_spy_20d",
    # v1.3 additions — macro regime features
    "yield_10y",
    "yield_curve_slope",
    "gold_mom_5d",
    "oil_mom_5d",
    # v1.6 additions — investigation upgrades (A2)
    "vix_term_slope",
    "xsect_dispersion",
    # v1.4 additions — design doc Appendix A feature completions
    "price_accel",
    "ema_cross_8_21",
    "atr_14_pct",
    "realized_vol_20d",
    "volume_trend",
    "obv_slope_10d",
    "rsi_slope_5d",
    "volume_price_div",
    # v1.5 additions — regime interaction terms
    "mom5d_x_vix",
    "rsi_x_vix",
    "sector_x_trend",
    "atr_x_vix",
    "vol_trend_x_vix",
    # v2.0 additions — alternative data signals (O10-O12)
    "earnings_surprise_pct",
    "days_since_earnings",
    "eps_revision_4w",
    "revision_streak",
    "put_call_ratio",
    "iv_rank",
    "iv_vs_rv",
    # v3.0 additions — fundamental ratios (quarterly, from FMP)
    "pe_ratio",
    "pb_ratio",
    "debt_to_equity",
    "revenue_growth_yoy",
    "fcf_yield",
    "gross_margin",
    "roe",
    "current_ratio",
    # Phase 3a of attractiveness-pillars-260520 — Growth + Stewardship
    # pillar quant substrate. Surfaced from existing Finnhub metric=all
    # response; no new API integrations.
    "revenue_growth_3y",
    "eps_growth_3y",
    "payout_ratio",
    "dividend_yield",
    "capex_growth_5y",
    # v3.1 additions — longer-horizon + overnight/intraday decomposition +
    # reversal-native signals. Predictor ROADMAP P2: collapse FLAT +
    # test whether 5d is reversal or momentum regime. 2026-04-15: neutral
    # names chosen — meta ridge coefficient sign determines whether the
    # feature behaves as reversal (positive coef) or momentum (negative).
    "return_60d",
    "return_120d",
    "overnight_return_5d",
    "intraday_return_5d",
    "dist_from_5d_high",
    "dist_from_20d_high",
    # v3.2 additions — per-ticker risk features (Stage 2 of regime-
    # conditioning rebuild). Cross-sectionally varying institutional risk
    # metrics that capture distinct dimensions LightGBM trees can split on:
    #   - beta_60d:        market sensitivity (systematic exposure)
    #   - idio_vol_60d:    residual vol after removing market-beta exposure
    #                      (idiosyncratic risk)
    #   - vol_of_vol_30d:  stability of vol regime (regime persistence)
    #   - max_drawdown_60d: worst peak-to-trough drawdown within 60d window
    #                      (left-tail risk; distinct from dist_from_52w_high
    #                      which is current depth-from-rolling-high)
    "beta_60d",
    "idio_vol_60d",
    "vol_of_vol_30d",
    "max_drawdown_60d",
    "realized_vol_63d",
]

MIN_ROWS_FOR_FEATURES = 265  # 252 warmup + buffer


# ── Internal helpers ──────────────────────────────────────────────────────────

def _compute_rsi(close: pd.Series, period: int | None = None) -> pd.Series:
    """
    Wilder's RSI using EWM (com = period - 1).
    Matches research's compute_technical_indicators() exactly.
    """
    if period is None:
        period = _FC["rsi_period"]
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _compute_macd(
    close: pd.Series,
    fast: int | None = None,
    slow: int | None = None,
    signal: int | None = None,
) -> tuple[pd.Series, pd.Series]:
    """Standard MACD. Returns (macd_line, signal_line)."""
    if fast is None:
        fast = _FC["macd_fast"]
    if slow is None:
        slow = _FC["macd_slow"]
    if signal is None:
        signal = _FC["macd_signal"]
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


# ── Public API ────────────────────────────────────────────────────────────────

def compute_features(
    df: pd.DataFrame,
    spy_series: pd.Series | None = None,
    vix_series: pd.Series | None = None,
    sector_etf_series: pd.Series | None = None,
    tnx_series: pd.Series | None = None,
    irx_series: pd.Series | None = None,
    gld_series: pd.Series | None = None,
    uso_series: pd.Series | None = None,
    earnings_data: dict | None = None,
    revision_data: dict | None = None,
    options_data: dict | None = None,
    fundamental_data: dict | None = None,
    vix3m_series: pd.Series | None = None,
    xsect_dispersion: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Compute all technical, macro, and alternative data features for a full OHLCV DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: Close, Volume (Open/High/Low optional).
        Index should be a DatetimeIndex sorted ascending.
    spy_series : SPY Close prices (DatetimeIndex).
    vix_series : VIX Close prices (DatetimeIndex).
    sector_etf_series : Sector ETF Close prices (DatetimeIndex).
    tnx_series : 10Y Treasury yield in percent (DatetimeIndex).
    irx_series : 3M T-bill yield in percent (DatetimeIndex).
    gld_series : GLD Close prices (DatetimeIndex).
    uso_series : USO Close prices (DatetimeIndex).
    earnings_data : dict with keys: surprise_pct, days_since_earnings.
    revision_data : dict with keys: eps_revision_4w, revision_streak.
    options_data : dict with keys: put_call_ratio, iv_rank, atm_iv.
    fundamental_data : dict with keys: pe_ratio, pb_ratio, etc.
    vix3m_series : VIX3M Close prices (DatetimeIndex).
    xsect_dispersion : Cross-sectional dispersion Series (DatetimeIndex).

    Returns
    -------
    pd.DataFrame with original columns plus feature columns. Features
    whose rolling-window warmup exceeds the available history stay NaN
    for the affected rows; no rows are dropped. Callers apply their own
    policy — daily_append writes partial-feature rows with loud
    per-ticker coverage logging; training may dropna at its layer.

    2026-04-21: removed the implicit ``df.dropna(subset=FEATURES)``
    epilogue. It caused every row of short-history tickers (new
    listings, spinoffs — e.g. SNDK with 44 rows) to be dropped because
    252-day features were always NaN, which cascaded into the executor
    crashing at ``load_atr_14_pct`` despite ATR-14 being computable on
    ≥14 rows. First-class support for partial features requires
    returning them.
    """
    if df.empty:
        return df.copy()

    df = df.copy()

    # Ensure index is sorted
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    close = df["Close"].astype(float)
    volume = df["Volume"].astype(float) if "Volume" in df.columns else pd.Series(0.0, index=df.index)

    # ── RSI ─────────────────────────────────────────────────────────────────
    df["rsi_14"] = _compute_rsi(close)

    # ── MACD ──────────────────────────────────────────────────────────────────
    macd_line, signal_line = _compute_macd(close)
    df["macd_line_last"] = macd_line
    df["macd_above_zero"] = (macd_line > 0).astype(float)

    # macd_cross: +1/-1/0 for a cross in the last 3 days
    diff = macd_line - signal_line
    prev_diff = diff.shift(1)
    bullish = (prev_diff < 0) & (diff >= 0)
    bearish = (prev_diff > 0) & (diff <= 0)

    macd_cross = pd.Series(0.0, index=df.index)
    for lag in [2, 1, 0]:  # older to newer so most recent overwrites
        macd_cross[bullish.shift(lag, fill_value=False)] = 1.0
        macd_cross[bearish.shift(lag, fill_value=False)] = -1.0
    df["macd_cross"] = macd_cross

    # ── Moving averages ───────────────────────────────────────────────────────
    _ma_short = _FC["ma_short"]
    _ma_long = _FC["ma_long"]
    ma50 = close.rolling(window=_ma_short, min_periods=_ma_short).mean()
    ma200 = close.rolling(window=_ma_long, min_periods=_ma_long).mean()

    df["price_vs_ma50"] = (close - ma50) / ma50
    df["price_vs_ma200"] = (close - ma200) / ma200

    # ── 20-day momentum ───────────────────────────────────────────────────────
    _mom_long = _FC["momentum_long"]
    df["momentum_20d"] = (close / close.shift(_mom_long)) - 1.0

    # ── Average volume (normalized) ───────────────────────────────────────────
    _vol_slow = _FC["volume_slow"]
    volume_global_mean = volume.mean()
    if volume_global_mean > 0:
        avg_vol_20d = volume.rolling(window=_vol_slow, min_periods=_vol_slow).mean()
        df["avg_volume_20d"] = avg_vol_20d / volume_global_mean
    else:
        df["avg_volume_20d"] = 1.0

    # ── Distance from 52-week high ─────────────────────────────────────────────
    _52w = _FC["weeks_52_days"]
    rolling_max_252 = close.rolling(window=_52w, min_periods=_52w).max()
    df["dist_from_52w_high"] = (close - rolling_max_252) / rolling_max_252

    # ── 5-day momentum ─────────────────────────────────────────────────────────
    _mom_short = _FC["momentum_short"]
    df["momentum_5d"] = (close / close.shift(_mom_short)) - 1.0

    # ── v3.1: Longer-horizon returns ──────────────────────────────────────────
    # ROADMAP P2 diagnostic — test whether 5d is the right label horizon.
    # Neutral naming: meta ridge coefficient sign determines reversal vs
    # momentum regime. Positive coef → reversal (high past returns predict
    # negative future returns). Negative coef → momentum persists at this
    # horizon.
    df["return_60d"] = (close / close.shift(60)) - 1.0
    df["return_120d"] = (close / close.shift(120)) - 1.0

    # ── v3.1: Overnight / intraday decomposition ──────────────────────────────
    # Lou/Polk/Skouras 2019 "A Tug of War": overnight returns
    # (Open_t vs Close_{t-1}) have been historically persistent and positive
    # (earnings, news, macro), while intraday returns (Close_t vs Open_t)
    # have been noisier and often negative (microstructure, flow). Total
    # 5d return = overnight_5d + intraday_5d (approximately — compounding
    # differences are small at 5d horizons and this additive sum is the
    # form used in the source literature).
    if "Open" in df.columns:
        open_ = df["Open"].astype(float)
        overnight_daily = (open_ / close.shift(1)) - 1.0
        intraday_daily = (close / open_) - 1.0
        df["overnight_return_5d"] = overnight_daily.rolling(
            window=_mom_short, min_periods=_mom_short,
        ).sum()
        df["intraday_return_5d"] = intraday_daily.rolling(
            window=_mom_short, min_periods=_mom_short,
        ).sum()
    else:
        # Without Open, these features are undefined — NaN propagates and
        # dropna will exclude the ticker. No silent zero-fill (per
        # feedback_no_silent_fails).
        df["overnight_return_5d"] = float("nan")
        df["intraday_return_5d"] = float("nan")

    # ── v3.1: Distance from recent highs (reversal-native) ────────────────────
    # Distance from recent peak is a cleaner reversal signal than past
    # returns: a stock at its 5d high has nowhere to "continue" in the
    # short-term reversal regime, while a stock pulled back from its 5d
    # high has more room to mean-revert. Negative values always (close
    # cannot exceed max by definition). Closer to zero = near high.
    if "High" in df.columns:
        high_col = df["High"].astype(float)
    else:
        high_col = close
    rolling_max_5 = high_col.rolling(window=5, min_periods=5).max()
    rolling_max_20 = high_col.rolling(window=20, min_periods=20).max()
    df["dist_from_5d_high"] = (close - rolling_max_5) / rolling_max_5
    df["dist_from_20d_high"] = (close - rolling_max_20) / rolling_max_20

    # ── Relative volume ratio ──────────────────────────────────────────────────
    rolling_mean_vol_20 = volume.rolling(window=_vol_slow, min_periods=_vol_slow).mean()
    df["rel_volume_ratio"] = volume / rolling_mean_vol_20.replace(0, float("nan"))
    df["rel_volume_ratio"] = df["rel_volume_ratio"].fillna(1.0)

    # ── Return vs SPY (5-day relative strength) ────────────────────────────────
    spy_mom_5d: pd.Series | None = None
    if spy_series is not None:
        spy_aligned = spy_series.reindex(df.index)
        spy_mom_5d = (spy_aligned / spy_aligned.shift(_mom_short)) - 1.0
        df["return_vs_spy_5d"] = df["momentum_5d"] - spy_mom_5d
    else:
        df["return_vs_spy_5d"] = 0.0

    # ── v1.2 features ──────────────────────────────────────────────────────────

    # VIX level
    if vix_series is not None:
        vix_aligned = vix_series.reindex(df.index, method="ffill")
        df["vix_level"] = vix_aligned / _FC["vix_baseline"]
    else:
        df["vix_level"] = 1.0

    # Distance from 52-week low
    rolling_min_252 = close.rolling(window=_52w, min_periods=_52w).min()
    df["dist_from_52w_low"] = (close - rolling_min_252) / rolling_min_252

    # Historical volatility ratio (10d / 60d)
    _vol_short = _FC["vol_short_window"]
    _vol_long_w = _FC["vol_long_window"]
    log_ret = np.log(close / close.shift(1))
    vol_10d = log_ret.rolling(window=_vol_short, min_periods=_vol_short).std() * np.sqrt(_52w)
    vol_60d = log_ret.rolling(window=_vol_long_w, min_periods=_vol_long_w).std() * np.sqrt(_52w)
    df["vol_ratio_10_60"] = (vol_10d / vol_60d.replace(0, float("nan"))).fillna(1.0)

    # Bollinger band position
    _bb_win = _FC["bollinger_window"]
    _bb_std = _FC["bollinger_std"]
    ma20 = close.rolling(window=_bb_win, min_periods=_bb_win).mean()
    std20 = close.rolling(window=_bb_win, min_periods=_bb_win).std()
    upper_bb = ma20 + _bb_std * std20
    lower_bb = ma20 - _bb_std * std20
    band_width = (upper_bb - lower_bb).replace(0, float("nan"))
    df["bollinger_pct"] = ((close - lower_bb) / band_width).fillna(0.5)

    # Sector ETF vs SPY (5d/10d/20d)
    if sector_etf_series is not None:
        sec_aligned = sector_etf_series.reindex(df.index)
        sec_mom_5d = (sec_aligned / sec_aligned.shift(_mom_short)) - 1.0
        sec_mom_10d = (sec_aligned / sec_aligned.shift(10)) - 1.0
        sec_mom_20d = (sec_aligned / sec_aligned.shift(_mom_long)) - 1.0
        if spy_mom_5d is not None:
            spy_mom_10d = (spy_aligned / spy_aligned.shift(10)) - 1.0
            spy_mom_20d = (spy_aligned / spy_aligned.shift(_mom_long)) - 1.0
            df["sector_vs_spy_5d"] = sec_mom_5d - spy_mom_5d
            df["sector_vs_spy_10d"] = sec_mom_10d - spy_mom_10d
            df["sector_vs_spy_20d"] = sec_mom_20d - spy_mom_20d
        else:
            df["sector_vs_spy_5d"] = sec_mom_5d
            df["sector_vs_spy_10d"] = sec_mom_10d
            df["sector_vs_spy_20d"] = sec_mom_20d
    else:
        df["sector_vs_spy_5d"] = 0.0
        df["sector_vs_spy_10d"] = 0.0
        df["sector_vs_spy_20d"] = 0.0

    # ── v1.3 features — macro regime ──────────────────────────────────────────

    # yield_10y
    if tnx_series is not None:
        _tnx_norm = _FC["tnx_normalizer"]
        tnx_aligned = tnx_series.reindex(df.index, method="ffill")
        df["yield_10y"] = tnx_aligned / _tnx_norm
    else:
        df["yield_10y"] = 0.4

    # yield_curve_slope
    if tnx_series is not None and irx_series is not None:
        irx_aligned = irx_series.reindex(df.index, method="ffill")
        df["yield_curve_slope"] = (tnx_aligned - irx_aligned) / _tnx_norm
    elif tnx_series is not None:
        df["yield_curve_slope"] = tnx_aligned / _tnx_norm
    else:
        df["yield_curve_slope"] = 0.0

    # gold_mom_5d
    if gld_series is not None:
        gld_aligned = gld_series.reindex(df.index, method="ffill")
        df["gold_mom_5d"] = (gld_aligned / gld_aligned.shift(_mom_short)) - 1.0
        df["gold_mom_5d"] = df["gold_mom_5d"].fillna(0.0)
    else:
        df["gold_mom_5d"] = 0.0

    # oil_mom_5d
    if uso_series is not None:
        uso_aligned = uso_series.reindex(df.index, method="ffill")
        df["oil_mom_5d"] = (uso_aligned / uso_aligned.shift(_mom_short)) - 1.0
        df["oil_mom_5d"] = df["oil_mom_5d"].fillna(0.0)
    else:
        df["oil_mom_5d"] = 0.0

    # ── v1.6 features — investigation upgrades (A2) ─────────────────────────

    # vix_term_slope
    if vix_series is not None and vix3m_series is not None:
        vix3m_aligned = vix3m_series.reindex(df.index, method="ffill")
        df["vix_term_slope"] = (vix_aligned - vix3m_aligned) / _FC["vix_baseline"]
        df["vix_term_slope"] = df["vix_term_slope"].fillna(0.0)
    else:
        df["vix_term_slope"] = 0.0

    # xsect_dispersion
    if xsect_dispersion is not None:
        disp_aligned = xsect_dispersion.reindex(df.index, method="ffill")
        df["xsect_dispersion"] = disp_aligned.fillna(0.0)
    else:
        df["xsect_dispersion"] = 0.0

    # ── v1.4 features — design doc Appendix A completions ────────────────────

    # price_accel
    df["price_accel"] = df["momentum_5d"] - df["momentum_20d"]

    # ema_cross_8_21
    ema_8 = close.ewm(span=_FC["ema_fast_span"], adjust=False).mean()
    ema_21 = close.ewm(span=_FC["ema_slow_span"], adjust=False).mean()
    df["ema_cross_8_21"] = ema_8 / ema_21 - 1.0

    # atr_14_pct
    high = df["High"].astype(float) if "High" in df.columns else close
    low = df["Low"].astype(float) if "Low" in df.columns else close
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=_FC["atr_period"], adjust=False).mean()
    df["atr_14_pct"] = atr / close

    # realized_vol_20d
    daily_returns = close.pct_change()
    df["realized_vol_20d"] = daily_returns.rolling(_FC["realized_vol_window"]).std() * np.sqrt(_52w)

    # realized_vol_63d — 3-month realized vol (Stage 2 regime-conditioning).
    # Captures a slower vol regime than 20d. Pair with 20d to give the GBM
    # a vol-term-structure signal (steep upward slope = vol expansion;
    # flat / inverted = mean-revert regime).
    df["realized_vol_63d"] = daily_returns.rolling(63).std() * np.sqrt(_52w)

    # ── v3.2: Per-ticker risk features (Stage 2 regime-conditioning rebuild) ──
    # Four institutional risk dimensions that capture cross-sectional
    # variation distinct from the existing volatility features. Each varies
    # per-ticker on a given date (rank-norm pipeline-compatible) and gives
    # the volatility GBM new splits the existing 6 vol features can't make.
    _beta_w = _FC["beta_window"]
    _vov_w = _FC["vol_of_vol_window"]
    _mdd_w = _FC["max_drawdown_window"]

    # beta_60d: 60d rolling regression slope of stock log-returns vs SPY
    # log-returns. Captures systematic market exposure. NaN when no SPY
    # series available or rolling window is incomplete.
    log_returns = np.log(close / close.shift(1))
    if spy_series is not None:
        spy_aligned_for_beta = spy_series.reindex(df.index).astype(float)
        spy_log_returns = np.log(spy_aligned_for_beta / spy_aligned_for_beta.shift(1))
        # rolling cov(stock, spy) / var(spy)
        rolling_cov = log_returns.rolling(window=_beta_w, min_periods=_beta_w).cov(spy_log_returns)
        rolling_spy_var = spy_log_returns.rolling(window=_beta_w, min_periods=_beta_w).var()
        df["beta_60d"] = (rolling_cov / rolling_spy_var.replace(0, float("nan"))).astype(float)
    else:
        df["beta_60d"] = float("nan")

    # idio_vol_60d: residual vol after removing market-beta exposure.
    # residual = stock_log_return - beta * spy_log_return; std × sqrt(252).
    # Captures idiosyncratic risk.
    if spy_series is not None:
        residual_returns = log_returns - df["beta_60d"] * spy_log_returns
        df["idio_vol_60d"] = (
            residual_returns.rolling(window=_beta_w, min_periods=_beta_w).std()
            * np.sqrt(_52w)
        ).astype(float)
    else:
        df["idio_vol_60d"] = float("nan")

    # vol_of_vol_30d: 30d rolling stdev of realized_vol_20d. Captures the
    # stability of the vol regime — a stock whose realized vol oscillates
    # carries different risk than one whose vol is stable.
    df["vol_of_vol_30d"] = df["realized_vol_20d"].rolling(
        window=_vov_w, min_periods=_vov_w,
    ).std()

    # max_drawdown_60d: worst peak-to-trough drawdown WITHIN the trailing
    # 60d window. Distinct from dist_from_52w_high (current depth from
    # rolling-252-high): captures the deepest historical drawdown that
    # occurred during the recent 60d, even if the stock has since
    # recovered. Always non-positive.
    rolling_max_60 = close.rolling(window=_mdd_w, min_periods=_mdd_w).max()
    drawdown_series = (close / rolling_max_60) - 1.0
    df["max_drawdown_60d"] = drawdown_series.rolling(
        window=_mdd_w, min_periods=_mdd_w,
    ).min()

    # volume_trend
    _vf = _FC["volume_fast"]
    vol_5 = volume.rolling(_vf).mean()
    vol_20 = volume.rolling(_vol_slow).mean()
    df["volume_trend"] = (vol_5 / vol_20.replace(0, float("nan"))).fillna(1.0)

    # obv_slope_10d
    obv_direction = np.sign(close.diff()).fillna(0)
    obv = (obv_direction * volume).cumsum()
    obv_fast = obv.rolling(_FC["obv_fast"]).mean()
    obv_slow = obv.rolling(_FC["obv_slow"]).mean()
    df["obv_slope_10d"] = ((obv_fast - obv_slow) / vol_20.replace(0, float("nan"))).fillna(0.0)

    # rsi_slope_5d
    rsi = df["rsi_14"]
    _rsi_slope_w = _FC["rsi_slope_window"]
    df["rsi_slope_5d"] = (rsi - rsi.shift(_rsi_slope_w)) / float(_rsi_slope_w)

    # volume_price_div
    df["volume_price_div"] = np.sign(df["volume_trend"] - 1.0) * np.sign(df["momentum_5d"])

    # ── v1.5 features — regime interaction terms ───────────────────────────────

    vix_regime = df["vix_level"] - 1.0

    df["mom5d_x_vix"] = df["momentum_5d"] * vix_regime

    rsi_centered = (df["rsi_14"] - 50.0) / 50.0
    df["rsi_x_vix"] = rsi_centered * vix_regime

    if spy_series is not None:
        spy_aligned = spy_series.reindex(df.index, method="ffill")
        spy_trend = (spy_aligned / spy_aligned.shift(20)) - 1.0
        spy_trend = spy_trend.fillna(0.0)
    else:
        spy_trend = pd.Series(0.0, index=df.index)
    df["sector_x_trend"] = df["sector_vs_spy_5d"] * spy_trend

    df["atr_x_vix"] = df["atr_14_pct"] * vix_regime

    df["vol_trend_x_vix"] = (df["volume_trend"] - 1.0) * vix_regime

    # ── v2.0 features — alternative data signals (O10-O12) ────────────────────

    def _safe_float(val, default: float = 0.0) -> float:
        """Coerce to float, treating None as the default."""
        return float(val) if val is not None else default

    # O10: PEAD
    if earnings_data:
        df["earnings_surprise_pct"] = _safe_float(earnings_data.get("surprise_pct"), 0.0)
        days_since = _safe_float(earnings_data.get("days_since_earnings"), 90.0)
        df["days_since_earnings"] = days_since / 90.0
    else:
        df["earnings_surprise_pct"] = 0.0
        df["days_since_earnings"] = 1.0

    # O11: EPS revision momentum
    if revision_data:
        df["eps_revision_4w"] = _safe_float(revision_data.get("eps_revision_4w"), 0.0)
        df["revision_streak"] = _safe_float(revision_data.get("revision_streak"), 0.0)
    else:
        df["eps_revision_4w"] = 0.0
        df["revision_streak"] = 0.0

    # O12: Options-derived signals
    if options_data:
        df["put_call_ratio"] = _safe_float(options_data.get("put_call_ratio"), 0.0)
        df["iv_rank"] = _safe_float(options_data.get("iv_rank"), 0.5)
        atm_iv = _safe_float(options_data.get("atm_iv"), 0.0)
        realized_vol = df["realized_vol_20d"].iloc[-1] if "realized_vol_20d" in df.columns else 0.0
        if realized_vol > 0 and atm_iv > 0:
            df["iv_vs_rv"] = atm_iv / realized_vol
        else:
            df["iv_vs_rv"] = 1.0
    else:
        df["put_call_ratio"] = 0.0
        df["iv_rank"] = 0.5
        df["iv_vs_rv"] = 1.0

    # ── v3.0 features — fundamental ratios (quarterly, from FMP) ─────────────
    if fundamental_data:
        df["pe_ratio"] = _safe_float(fundamental_data.get("pe_ratio"), 0.0)
        df["pb_ratio"] = _safe_float(fundamental_data.get("pb_ratio"), 0.0)
        df["debt_to_equity"] = _safe_float(fundamental_data.get("debt_to_equity"), 0.0)
        df["revenue_growth_yoy"] = _safe_float(fundamental_data.get("revenue_growth_yoy"), 0.0)
        df["fcf_yield"] = _safe_float(fundamental_data.get("fcf_yield"), 0.0)
        df["gross_margin"] = _safe_float(fundamental_data.get("gross_margin"), 0.0)
        df["roe"] = _safe_float(fundamental_data.get("roe"), 0.0)
        df["current_ratio"] = _safe_float(fundamental_data.get("current_ratio"), 0.0)
        # Phase 3a of attractiveness-pillars-260520 — Growth + Stewardship pillar substrate.
        df["revenue_growth_3y"] = _safe_float(fundamental_data.get("revenue_growth_3y"), 0.0)
        df["eps_growth_3y"] = _safe_float(fundamental_data.get("eps_growth_3y"), 0.0)
        df["payout_ratio"] = _safe_float(fundamental_data.get("payout_ratio"), 0.0)
        df["dividend_yield"] = _safe_float(fundamental_data.get("dividend_yield"), 0.0)
        df["capex_growth_5y"] = _safe_float(fundamental_data.get("capex_growth_5y"), 0.0)
    else:
        df["pe_ratio"] = 0.0
        df["pb_ratio"] = 0.0
        df["debt_to_equity"] = 0.0
        df["revenue_growth_yoy"] = 0.0
        df["fcf_yield"] = 0.0
        df["gross_margin"] = 0.0
        df["roe"] = 0.0
        df["current_ratio"] = 0.0
        df["revenue_growth_3y"] = 0.0
        df["eps_growth_3y"] = 0.0
        df["payout_ratio"] = 0.0
        df["dividend_yield"] = 0.0
        df["capex_growth_5y"] = 0.0

    # Rows with NaN features are NOT dropped — see module docstring. A
    # feature whose rolling-window warmup exceeds the available history
    # stays NaN for the affected rows; short-history tickers therefore
    # produce rows with partial feature coverage rather than being
    # filtered out entirely.
    return df


def features_to_array(df: pd.DataFrame) -> np.ndarray:
    """
    Extract all feature columns from a featured DataFrame as a float32 array.
    Shape: (N, len(FEATURES)).
    """
    return df[FEATURES].to_numpy(dtype=np.float32)
