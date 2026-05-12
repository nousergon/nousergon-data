"""
validators/price_validator.py — Data quality checks for price data.

Two surfaces:

- ``validate_parquet`` / ``validate_batch`` / ``validate_refreshed`` — full-history
  inspection used by ``collectors/slim_cache.py`` + ``collectors/prices.py``;
  non-blocking, returns anomaly summary for manifest + email rollups.
- ``validate_today_row`` — write-time per-symbol gate used by
  ``builders/daily_append.py``; returns structured anomalies with per-type
  severity so the caller can hard-fail on definitely-bad rows (negative
  prices, High<Low) while only warning on legitimately-rare-but-possible
  signals (>50% moves on event days, single-day volume spikes).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Thresholds
MAX_DAILY_RETURN = 0.50       # Flag >50% single-day price move
MAX_VOLUME_SPIKE = 10.0       # Flag volume >10x 20-day rolling median
MAX_GAP_TRADING_DAYS = 3      # Flag gaps >3 trading days in a parquet

# ── Write-time anomaly type catalog ────────────────────────────────────────
# Severity: "block" definitely-bad, refuse the write; "warn" allow the write
# but emit a metric so chronic drift is visible. Caller can upgrade types via
# the DAILY_APPEND_BLOCK_ANOMALY_TYPES env var.
ANOMALY_BAD_OHLC = "bad_ohlc"                       # default block
ANOMALY_NEGATIVE_OR_ZERO_CLOSE = "negative_or_zero_close"  # default block
ANOMALY_EXTREME_DAILY_MOVE = "extreme_daily_move"   # default warn
ANOMALY_ZERO_VOLUME = "zero_volume"                 # default warn
ANOMALY_VOLUME_SPIKE = "volume_spike"               # default warn

DEFAULT_BLOCK_ANOMALY_TYPES: frozenset[str] = frozenset({
    ANOMALY_BAD_OHLC,
    ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
})

ALL_ANOMALY_TYPES: frozenset[str] = frozenset({
    ANOMALY_BAD_OHLC,
    ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
    ANOMALY_EXTREME_DAILY_MOVE,
    ANOMALY_ZERO_VOLUME,
    ANOMALY_VOLUME_SPIKE,
})


def validate_today_row(
    today_row: pd.DataFrame,
    hist: pd.DataFrame,
    ticker: str,
) -> dict:
    """
    Inspect a single-row write against its prior history for write-time gating.

    ``today_row`` is the 1-row DataFrame about to be written via
    ``universe_lib.update_batch`` / ``write_batch``; ``hist`` is the existing
    series read via ``read_batch`` in the same pass.

    Returns ``{"ticker": ..., "anomalies": [{"type", "severity", "detail"}, ...]}``.
    Severities are *defaults* — the caller decides final block/warn behavior
    by consulting its configured block set (allowing operators to upgrade
    e.g. ``extreme_daily_move`` to block during a known-quiet observation
    window).
    """
    anomalies: list[dict] = []

    if today_row is None or today_row.empty:
        return {"ticker": ticker, "anomalies": anomalies}

    row = today_row.iloc[0]

    # ── 1. OHLC relationship (bad_ohlc — default block) ────────────────────
    if all(c in today_row.columns for c in ("High", "Low")):
        high = row.get("High")
        low = row.get("Low")
        if pd.notna(high) and pd.notna(low) and high < low:
            anomalies.append({
                "type": ANOMALY_BAD_OHLC,
                "severity": "block",
                "detail": f"High={high:.4f} < Low={low:.4f}",
            })

    # ── 2. Zero or negative close (negative_or_zero_close — default block) ──
    if "Close" in today_row.columns:
        close = row.get("Close")
        if pd.notna(close) and close <= 0:
            anomalies.append({
                "type": ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
                "severity": "block",
                "detail": f"Close={close:.4f}",
            })

    # ── 3. Extreme daily return vs prior close (extreme_daily_move — warn) ──
    # Compared against hist.iloc[-1]["Close"] rather than within today_row
    # because today_row is single-row. Skipped when hist is empty (first
    # write for this symbol — no prior close to compare against).
    if (
        "Close" in today_row.columns
        and "Close" in getattr(hist, "columns", [])
        and not hist.empty
    ):
        today_close = row.get("Close")
        prior_close = hist["Close"].iloc[-1]
        if pd.notna(today_close) and pd.notna(prior_close) and prior_close > 0:
            pct_change = abs(today_close - prior_close) / prior_close
            if pct_change > MAX_DAILY_RETURN:
                anomalies.append({
                    "type": ANOMALY_EXTREME_DAILY_MOVE,
                    "severity": "warn",
                    "detail": (
                        f"|{today_close:.4f}-{prior_close:.4f}|/{prior_close:.4f} "
                        f"= {pct_change:.1%} > {MAX_DAILY_RETURN:.0%}"
                    ),
                })

    # ── 4. Zero volume on trading day (zero_volume — warn) ─────────────────
    if "Volume" in today_row.columns:
        vol = row.get("Volume")
        if pd.notna(vol) and vol == 0:
            anomalies.append({
                "type": ANOMALY_ZERO_VOLUME,
                "severity": "warn",
                "detail": "Volume=0",
            })

    # ── 5. Volume spike vs hist 20-day median (volume_spike — warn) ────────
    if (
        "Volume" in today_row.columns
        and "Volume" in getattr(hist, "columns", [])
        and len(hist) >= 20
    ):
        today_vol = row.get("Volume")
        recent_vols = hist["Volume"].tail(20)
        baseline = recent_vols[recent_vols > 0].median()
        if pd.notna(today_vol) and pd.notna(baseline) and baseline > 0:
            ratio = today_vol / baseline
            if ratio > MAX_VOLUME_SPIKE:
                anomalies.append({
                    "type": ANOMALY_VOLUME_SPIKE,
                    "severity": "warn",
                    "detail": (
                        f"Volume={today_vol:.0f} vs 20d-median={baseline:.0f} "
                        f"(ratio={ratio:.1f}x > {MAX_VOLUME_SPIKE:.0f}x)"
                    ),
                })

    return {"ticker": ticker, "anomalies": anomalies}


def validate_parquet(df: pd.DataFrame, ticker: str) -> dict:
    """
    Validate a single ticker's OHLCV DataFrame.

    Returns dict with anomaly counts and details. Empty anomalies = clean.
    """
    anomalies: list[str] = []

    if df.empty:
        return {"ticker": ticker, "status": "empty", "anomalies": ["empty dataframe"]}

    # ── 1. OHLC relationship ────────────────────────────────────────────────
    if all(c in df.columns for c in ("Open", "High", "Low", "Close")):
        bad_hl = (df["High"] < df["Low"]).sum()
        if bad_hl > 0:
            anomalies.append(f"High<Low on {bad_hl} days")

    # ── 2. Zero or negative prices ──────────────────────────────────────────
    if "Close" in df.columns:
        bad_prices = (df["Close"] <= 0).sum()
        if bad_prices > 0:
            anomalies.append(f"Close<=0 on {bad_prices} days")

    # ── 3. Extreme daily returns ────────────────────────────────────────────
    if "Close" in df.columns and len(df) >= 2:
        returns = df["Close"].pct_change().dropna()
        extreme = returns.abs() > MAX_DAILY_RETURN
        n_extreme = extreme.sum()
        if n_extreme > 0:
            dates = returns[extreme].index.strftime("%Y-%m-%d").tolist()
            anomalies.append(f">{MAX_DAILY_RETURN:.0%} daily move on {n_extreme} days: {dates[:5]}")

    # ── 4. Zero volume on trading days ──────────────────────────────────────
    if "Volume" in df.columns:
        zero_vol = (df["Volume"] == 0).sum()
        if zero_vol > 0:
            anomalies.append(f"zero volume on {zero_vol} days")

    # ── 5. Volume spikes ────────────────────────────────────────────────────
    if "Volume" in df.columns and len(df) >= 25:
        rolling_med = df["Volume"].rolling(20, min_periods=10).median()
        with_baseline = df["Volume"][rolling_med > 0]
        baseline = rolling_med[rolling_med > 0]
        if not baseline.empty:
            ratio = with_baseline / baseline
            spikes = (ratio > MAX_VOLUME_SPIKE).sum()
            if spikes > 0:
                anomalies.append(f"volume >{MAX_VOLUME_SPIKE:.0f}x median on {spikes} days")

    # ── 6. Trading day gaps ─────────────────────────────────────────────────
    if len(df) >= 2:
        idx = pd.to_datetime(df.index).sort_values()
        # Business day diff
        gaps = pd.Series(idx).diff().dt.days.dropna()
        # Exclude weekends (2-day gaps) — flag >5 calendar days (~3 trading days)
        big_gaps = gaps[gaps > 5]
        if not big_gaps.empty:
            gap_details = [
                f"{idx[i-1].strftime('%Y-%m-%d')}→{idx[i].strftime('%Y-%m-%d')} ({int(g)}d)"
                for i, g in big_gaps.items()
            ]
            anomalies.append(f"{len(big_gaps)} gaps >{MAX_GAP_TRADING_DAYS} trading days: {gap_details[:3]}")

    return {
        "ticker": ticker,
        "status": "anomaly" if anomalies else "clean",
        "anomalies": anomalies,
    }


def validate_batch(parquet_dir: Path, tickers: list[str] | None = None) -> dict:
    """
    Validate all parquets in a directory (or a specific subset).

    Returns summary dict suitable for inclusion in manifest.json.
    """
    results = []
    files = sorted(parquet_dir.glob("*.parquet"))

    for f in files:
        ticker = f.stem
        if tickers and ticker not in tickers:
            continue
        try:
            df = pd.read_parquet(f)
            df.index = pd.to_datetime(df.index)
            result = validate_parquet(df, ticker)
            results.append(result)
        except Exception as e:
            results.append({"ticker": ticker, "status": "error", "anomalies": [str(e)]})

    anomaly_tickers = [r for r in results if r["status"] != "clean"]
    total = len(results)

    summary = {
        "total_validated": total,
        "clean": total - len(anomaly_tickers),
        "anomalies": len(anomaly_tickers),
        "anomaly_details": anomaly_tickers[:20],  # Cap for manifest size
    }

    if anomaly_tickers:
        logger.warning(
            "Price validation: %d/%d tickers have anomalies",
            len(anomaly_tickers), total,
        )
        for r in anomaly_tickers[:10]:
            logger.warning("  %s: %s", r["ticker"], "; ".join(r["anomalies"]))
    else:
        logger.info("Price validation: all %d tickers clean", total)

    return summary


def validate_refreshed(
    s3_client,
    bucket: str,
    s3_prefix: str,
    tickers: list[str],
) -> dict:
    """
    Validate freshly refreshed tickers by downloading from S3.

    Only validates the tickers that were just refreshed (not the full cache).
    """
    import tempfile

    results = []
    for ticker in tickers[:100]:  # Cap at 100 to limit S3 calls
        key = f"{s3_prefix}{ticker}.parquet"
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                s3_client.download_file(bucket, key, tmp.name)
                df = pd.read_parquet(tmp.name)
                df.index = pd.to_datetime(df.index)
                result = validate_parquet(df, ticker)
                results.append(result)
        except Exception as e:
            results.append({"ticker": ticker, "status": "error", "anomalies": [str(e)]})

    anomaly_tickers = [r for r in results if r["status"] != "clean"]

    summary = {
        "total_validated": len(results),
        "clean": len(results) - len(anomaly_tickers),
        "anomalies": len(anomaly_tickers),
        "anomaly_details": anomaly_tickers[:20],
    }

    if anomaly_tickers:
        logger.warning(
            "Post-refresh validation: %d/%d tickers have anomalies",
            len(anomaly_tickers), len(results),
        )
    else:
        logger.info("Post-refresh validation: all %d tickers clean", len(results))

    return summary
