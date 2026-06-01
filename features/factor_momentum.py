"""Factor momentum (W2.3, L4469) — "Factor Momentum Everywhere" (Gupta-Kelly).

Factors that have done well recently tend to keep doing well (factor returns are
positively autocorrelated). This module turns that into a per-(ticker, date)
feature: tilt a stock toward the factors with positive recent momentum, weighted
by the stock's exposure to each factor.

Pipeline (all backward-only / strictly point-in-time):
  1. Daily factor returns — per date d, rank the cross-section by each factor's
     loading **as of d-1** (lagged), form a long(top-quantile)/short(bottom)
     portfolio, and take its realized return ON day d. → factor_return_{f,d}.
  2. Factor momentum — each factor's own trailing 12-1 cumulative return:
     sum of factor_return over (t-window, t-skip]  (skip the most recent month).
  3. Per-ticker projection — signal_{i,t} = Σ_f zscore(loading_{i,f,t}) ×
     factor_momentum_{f,t}, using loadings KNOWN at t and momentum built from
     factor returns realized THROUGH t-skip.

LOOK-AHEAD AUDIT (the load-bearing property — see tests):
  - factor_return_{f,d} uses returns realized on d, ranked by loadings from d-1.
  - factor_momentum_{f,t} sums factor returns through t-skip (< t).
  - the projection uses loadings at t × momentum through t-skip.
  ⇒ the value at t depends only on data ≤ t. Mutating any input AFTER t cannot
    change the feature at or before t.

Designed to run over the FULL ArcticDB universe history (~10y) — the cross-
sectional-time-series construction needs the whole panel, not a per-ticker slice.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# The factor-loading columns this signal tilts across. These are the raw
# per-ticker loadings the feature store already computes; the cross-sectional
# z-score / ranking is done HERE per date (we do not depend on the stored
# *_zscore columns existing over the full history).
DEFAULT_FACTOR_LOADINGS: list[str] = [
    "momentum_20d",
    "return_60d",
    "beta_60d",
    "idio_vol_60d",
    "realized_vol_63d",
    "dist_from_52w_high",
]

_EPS = 1e-9


def _zscore_by_date(values: pd.Series, dates: pd.Series) -> pd.Series:
    """Cross-sectional z-score within each date (NaN-safe; NaN stays NaN)."""
    g = values.groupby(dates)
    mean = g.transform("mean")
    std = g.transform("std")
    return (values - mean) / std.where(std > _EPS)


def compute_daily_factor_returns(
    panel: pd.DataFrame,
    loading_cols: list[str],
    *,
    quantile: float = 0.3,
    min_names: int = 20,
) -> pd.DataFrame:
    """Daily long-short factor returns.

    ``panel`` is long-format with columns ``["ticker", "date", "close", *loading_cols]``
    (one row per ticker per date, ascending date). For each factor and date d:
    rank the cross-section by the factor loading **as of d-1**, long the top
    ``quantile`` / short the bottom ``quantile``, and take the equal-weight mean
    of each name's realized return ON day d. Returns a date-indexed wide frame
    (columns == ``loading_cols``) of daily factor returns.
    """
    df = panel[["ticker", "date", "close", *loading_cols]].copy()
    df = df.sort_values(["ticker", "date"])
    # Realized daily return ON `date` (close_d / close_{d-1} - 1), per ticker.
    df["daily_return"] = df.groupby("ticker", sort=False)["close"].pct_change()
    # Lag the loadings by one row within each ticker so the rank that drives
    # day-d's factor return is known at d-1 (no contemporaneous use).
    for f in loading_cols:
        df[f] = df.groupby("ticker", sort=False)[f].shift(1)

    out: dict[str, pd.Series] = {}
    dates = np.sort(df["date"].unique())
    for f in loading_cols:
        per_date: dict = {}
        sub = df[["date", "daily_return", f]].dropna(subset=[f, "daily_return"])
        for d, grp in sub.groupby("date", sort=True):
            n = len(grp)
            if n < min_names:
                continue
            k = max(int(round(n * quantile)), 1)
            ranked = grp.sort_values(f)
            short_leg = ranked["daily_return"].iloc[:k].mean()
            long_leg = ranked["daily_return"].iloc[-k:].mean()
            per_date[d] = float(long_leg - short_leg)
        out[f] = pd.Series(per_date)
    fr = pd.DataFrame(out).reindex(dates)
    fr.index.name = "date"
    return fr


def compute_factor_momentum_series(
    factor_returns: pd.DataFrame,
    *,
    window: int = 252,
    skip: int = 21,
) -> pd.DataFrame:
    """Each factor's trailing 12-1 cumulative return (skip the recent month).

    ``factor_returns`` is the date-indexed daily-factor-return frame. Returns a
    same-shaped frame whose value at date t is the sum of the factor's daily
    returns over ``(t-window, t-skip]`` — backward-only.
    """
    cum = max(int(window) - int(skip), 1)
    # rolling sum of the most recent `cum` days, then shift by `skip` so the
    # window ends `skip` days before t (12-1 skip-month).
    return factor_returns.rolling(cum, min_periods=cum).sum().shift(skip)


def compute_factor_momentum_feature(
    panel: pd.DataFrame,
    loading_cols: list[str] | None = None,
    *,
    window: int = 252,
    skip: int = 21,
    quantile: float = 0.3,
    min_names: int = 20,
) -> pd.Series:
    """End-to-end per-(ticker, date) factor-momentum signal.

    Returns a Series indexed by the input ``panel``'s row order with the
    ``factor_momentum_ratio`` value: ``Σ_f zscore(loading_{i,f,t}) ×
    factor_momentum_{f,t}``. NaN where the factor-momentum window hasn't warmed
    up or a ticker has no finite loadings at t.
    """
    cols = list(loading_cols) if loading_cols is not None else list(DEFAULT_FACTOR_LOADINGS)
    cols = [c for c in cols if c in panel.columns]
    if not cols:
        return pd.Series(np.nan, index=panel.index, name="factor_momentum_ratio")

    factor_returns = compute_daily_factor_returns(
        panel, cols, quantile=quantile, min_names=min_names,
    )
    factor_mom = compute_factor_momentum_series(factor_returns, window=window, skip=skip)

    # Per-row: dot the date-t cross-sectionally-standardized loadings with the
    # date-t factor momentum. Build a (n_rows, n_factors) standardized-loading
    # matrix and a (n_rows, n_factors) momentum matrix aligned by date, then
    # row-wise nanmean of the product (mean over factors with both finite).
    work = panel[["date"]].copy()
    z = np.column_stack([
        _zscore_by_date(panel[f], panel["date"]).to_numpy(dtype=float) for f in cols
    ])  # loadings at t (no lag — this is the exposure we tilt)
    mom_by_date = factor_mom.reindex(panel["date"].to_numpy())[cols].to_numpy(dtype=float)

    prod = z * mom_by_date
    # Mean over factors with both loading and momentum finite. Done manually
    # (not np.nanmean) to avoid the "Mean of empty slice" warning on all-NaN
    # rows and to force NaN there rather than 0.
    finite = np.isfinite(prod)
    count = finite.sum(axis=1)
    ssum = np.where(finite, prod, 0.0).sum(axis=1)
    signal = np.where(count > 0, ssum / np.maximum(count, 1), np.nan)
    return pd.Series(signal, index=panel.index, name="factor_momentum_ratio")


def materialize_factor_momentum(
    universe_lib,
    tickers,
    *,
    loading_cols: list[str] | None = None,
    window: int = 252,
    skip: int = 21,
    quantile: float = 0.3,
    min_names: int = 20,
    write: bool = True,
    canonical_fn=None,
) -> dict:
    """Second-pass library builder: materialize ``factor_momentum_ratio`` over
    the FULL universe-library history (W2.3, L4469).

    Factor momentum is a cross-sectional-time-series construction — date t's
    value needs EVERY ticker's (close + loadings) history at once (to rank the
    cross-section and build factor-return portfolios). The streaming per-ticker
    writes in ``builders/backfill.py`` / ``builders/daily_append.py`` never hold
    the whole panel in memory, so this runs as a SEPARATE pass AFTER those
    writes:

      * Pass 1 — read back the slim ``(close + loadings)`` panel per ticker
        (the full feature frame is ~630 MB universe-wide; the slim slice is a
        fraction of that), assemble the long panel, and call
        ``compute_factor_momentum_feature`` once over the whole thing.
      * Pass 2 — read-modify-write ``factor_momentum_ratio`` into each ticker's
        stored frame (one frame in memory at a time). ``canonical_fn`` (pass
        ``store.arctic_store.to_arctic_canonical``) re-projects to the
        OHLCV+source+FEATURES column contract so the new column lands in its
        canonical position; ``factor_momentum_ratio`` must be in ``FEATURES`` or
        the canonical projection would drop it.

    Tickers that fail to read/write are skipped with a WARN (the signal is
    OBSERVE-mode and best-effort); an all-NaN ``factor_momentum_ratio`` count is
    logged LOUD so a silently-empty materialization can't pass for success.
    Returns a summary dict.
    """
    cols = list(loading_cols) if loading_cols is not None else list(DEFAULT_FACTOR_LOADINGS)

    # ── Pass 1: assemble the slim long panel (close + loadings) ──────────────
    frames: list[pd.DataFrame] = []
    read_fail = 0
    for t in tickers:
        try:
            df = universe_lib.read(t).data
        except Exception as exc:
            log.warning("factor-momentum: read failed for %s (skipped): %s", t, exc)
            read_fail += 1
            continue
        if df is None or df.empty or "Close" not in df.columns:
            continue
        sub = pd.DataFrame({"close": df["Close"].astype(float)})
        for c in cols:
            if c in df.columns:
                sub[c] = df[c].astype(float)
        sub["ticker"] = t
        sub["date"] = df.index
        frames.append(sub.reset_index(drop=True))

    if not frames:
        log.warning(
            "factor-momentum: no readable tickers (read_fail=%d) — nothing materialized",
            read_fail,
        )
        return {"status": "empty", "tickers_written": 0, "read_fail": read_fail}

    panel = pd.concat(frames, ignore_index=True)
    fm = compute_factor_momentum_feature(
        panel, loading_cols=cols, window=window, skip=skip,
        quantile=quantile, min_names=min_names,
    )
    panel["factor_momentum_ratio"] = fm.to_numpy()

    # ── Pass 2: read-modify-write the column per ticker ──────────────────────
    n_written = 0
    n_all_nan = 0
    write_fail = 0
    n_tickers = panel["ticker"].nunique()
    for t, grp in panel.groupby("ticker", sort=False):
        col = grp.set_index("date")["factor_momentum_ratio"].astype(float)
        if not np.isfinite(col.to_numpy()).any():
            n_all_nan += 1
        if not write:
            continue
        try:
            df = universe_lib.read(t).data
            df["factor_momentum_ratio"] = col.reindex(df.index).astype("float32")
            out = canonical_fn(df) if canonical_fn is not None else df
            universe_lib.write(t, out)
            n_written += 1
        except Exception as exc:
            log.warning("factor-momentum: write failed for %s (skipped): %s", t, exc)
            write_fail += 1

    if n_all_nan:
        # LOUD per [[feedback_no_silent_fails]]: an all-NaN signal is the
        # short-history / no-finite-loadings tail, not a failure — but a HIGH
        # count would mean the panel/loadings were wrong, so surface it.
        log.warning(
            "factor-momentum: %d/%d tickers have an all-NaN factor_momentum_ratio "
            "(short history or no finite loadings → neutral downstream, NOT a "
            "silent zero).",
            n_all_nan, n_tickers,
        )
    log.info(
        "factor-momentum materialized: %d written, %d all-NaN, %d read-fail, "
        "%d write-fail (of %d panel tickers)",
        n_written, n_all_nan, read_fail, write_fail, n_tickers,
    )
    return {
        "status": "ok",
        "tickers_written": n_written,
        "tickers_all_nan": n_all_nan,
        "read_fail": read_fail,
        "write_fail": write_fail,
    }
