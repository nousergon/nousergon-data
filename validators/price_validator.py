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
- ``validate_feature_record`` — write-time per-record gate for the
  *non-OHLCV* feature collectors (``collectors/fundamentals.py`` +
  ``collectors/alternative.py``) that bypass ``builders/daily_append.py``
  entirely. Same structured-anomaly + per-type-severity contract as
  ``validate_today_row``, but the field semantics are caller-supplied
  (a fundamentals row has no High/Low, an analyst-target field has its
  own non-negative invariant) so the caller passes a small spec rather
  than the validator hard-coding OHLCV column names. Covers the generic
  data-corruption surface that is field-agnostic: NaN / inf (always bad —
  these poison ArcticDB + every downstream mean/zscore), negative values
  where the field is semantically non-negative, and gross outliers
  outside a caller-declared sane band.
"""

from __future__ import annotations

import logging
import math
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
# Added 2026-05-18 (ROADMAP L1243 residual): the two intra-bar checks
# validate_today_row did not previously cover.
ANOMALY_INTRABAR_INCONSISTENT = "intrabar_inconsistent"  # default block
ANOMALY_NEGATIVE_VOLUME = "negative_volume"         # default block

# ── Feature-collector anomaly catalog (non-OHLCV) ──────────────────────────
# Used by validate_feature_record for fundamentals.py + alternative.py.
ANOMALY_NAN_OR_INF = "nan_or_inf"                   # default block
ANOMALY_NEGATIVE_WHERE_NONNEG = "negative_where_nonneg"  # default block
ANOMALY_GROSS_OUTLIER = "gross_outlier"             # default warn

DEFAULT_BLOCK_ANOMALY_TYPES: frozenset[str] = frozenset({
    ANOMALY_BAD_OHLC,
    ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
    ANOMALY_INTRABAR_INCONSISTENT,
    ANOMALY_NEGATIVE_VOLUME,
})

# Default block set for the feature collectors. NaN/inf and
# negative-where-impossible are unambiguous corruption — they poison the
# ArcticDB feature store + every downstream mean/zscore — so they block by
# default; a gross outlier may be a legitimate extreme (e.g. a real -300%
# ROE for a wiped-out firm) so it only warns, mirroring #215's
# definitely-bad-blocks / rare-but-possible-warns split.
DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES: frozenset[str] = frozenset({
    ANOMALY_NAN_OR_INF,
    ANOMALY_NEGATIVE_WHERE_NONNEG,
})

ALL_ANOMALY_TYPES: frozenset[str] = frozenset({
    ANOMALY_BAD_OHLC,
    ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
    ANOMALY_EXTREME_DAILY_MOVE,
    ANOMALY_ZERO_VOLUME,
    ANOMALY_VOLUME_SPIKE,
    ANOMALY_INTRABAR_INCONSISTENT,
    ANOMALY_NEGATIVE_VOLUME,
})

ALL_FEATURE_ANOMALY_TYPES: frozenset[str] = frozenset({
    ANOMALY_NAN_OR_INF,
    ANOMALY_NEGATIVE_WHERE_NONNEG,
    ANOMALY_GROSS_OUTLIER,
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

    # ── 1b. Intra-bar consistency: Low <= Close <= High ───────────────────
    # (intrabar_inconsistent — default block). bad_ohlc above only checks
    # High vs Low; a split-day reporting lag or corp-action artifact can
    # land a Close *outside* the [Low, High] band even when High >= Low,
    # which poisons every return/vol feature derived from the bar. This is
    # the first of the two intra-bar checks the ROADMAP L1243 residual
    # called out as not-yet-covered by validate_today_row.
    if all(c in today_row.columns for c in ("High", "Low", "Close")):
        high = row.get("High")
        low = row.get("Low")
        close = row.get("Close")
        if (
            pd.notna(high)
            and pd.notna(low)
            and pd.notna(close)
            and high >= low  # only meaningful when the H/L band is itself sane
            and not (low <= close <= high)
        ):
            anomalies.append({
                "type": ANOMALY_INTRABAR_INCONSISTENT,
                "severity": "block",
                "detail": (
                    f"Close={close:.4f} outside [Low={low:.4f}, "
                    f"High={high:.4f}]"
                ),
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

    # ── 2b. Negative volume (negative_volume — default block) ──────────────
    # The second ROADMAP L1243 residual intra-bar check. The existing
    # zero_volume check (#4 below) only catches Volume==0; a negative
    # volume is unambiguously corrupt (no such thing as negative shares
    # traded) and must hard-fail rather than merely warn.
    if "Volume" in today_row.columns:
        vol = row.get("Volume")
        if pd.notna(vol) and vol < 0:
            anomalies.append({
                "type": ANOMALY_NEGATIVE_VOLUME,
                "severity": "block",
                "detail": f"Volume={vol:.0f} < 0",
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


# Field spec for validate_feature_record. ``nonneg`` = the field is
# semantically non-negative (a negative value is corruption, not a
# legitimate extreme — e.g. a price target or current ratio). ``lo``/``hi``
# = the sane band; a value outside it is flagged as a gross outlier (warn
# by default). ``nonneg``/``lo``/``hi`` are all optional — omit a bound to
# skip that check for the field. NaN/inf is always checked regardless.
class FeatureFieldSpec(dict):
    """Thin dict subclass for self-documenting field specs.

    A spec is just ``{"nonneg": bool, "lo": float, "hi": float}`` with
    every key optional. Subclassing dict keeps it JSON/`**`-friendly while
    giving the spec a name at call sites.
    """


def validate_feature_record(
    record: dict,
    field_specs: dict[str, dict],
    ticker: str,
) -> dict:
    """Inspect a single non-OHLCV feature record against per-field specs.

    Used by the feature collectors (``fundamentals.py`` +
    ``alternative.py``) that write feature-source rows bypassing
    ``builders/daily_append.py``'s ``validate_today_row`` gate. ``record``
    is the about-to-be-written per-ticker dict (a flat fundamentals dict,
    or one sub-section of an alternative-data payload). ``field_specs``
    maps field name → ``{"nonneg": bool, "lo": float, "hi": float}`` (all
    keys optional). Only keys present in *both* ``record`` and
    ``field_specs`` are checked; ``None`` values are skipped (the
    collectors use ``None``/NEUTRAL as a legitimate "no data" sentinel —
    that's the ok_ratio gate's job, not this validator's).

    Returns the same ``{"ticker", "anomalies": [{"type", "severity",
    "detail"}, ...]}`` contract as ``validate_today_row`` so the callers
    reuse the identical block-set / metric wiring.

    Severities are *defaults*; the caller decides final block/warn by
    consulting its configured block set (see
    ``DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES``).
    """
    anomalies: list[dict] = []

    if not record or not field_specs:
        return {"ticker": ticker, "anomalies": anomalies}

    for field, spec in field_specs.items():
        if field not in record:
            continue
        val = record[field]
        if val is None:
            # Legitimate "no data" sentinel — coverage is the ok_ratio
            # gate's responsibility, not value-range validation's.
            continue
        # Coerce to float for numeric checks; non-numeric (e.g. a string
        # "rating") is out of scope for value-range validation — skip it.
        try:
            num = float(val)
        except (TypeError, ValueError):
            continue

        # ── 1. NaN / inf (nan_or_inf — default block) ──────────────────
        # Unambiguous corruption: a single NaN/inf in ArcticDB poisons
        # every cross-sectional mean / zscore that touches the column.
        if math.isnan(num) or math.isinf(num):
            anomalies.append({
                "type": ANOMALY_NAN_OR_INF,
                "severity": "block",
                "detail": f"{field}={val!r} is NaN/inf",
            })
            continue  # further numeric checks on NaN/inf are meaningless

        # ── 2. Negative where non-negative required ────────────────────
        # (negative_where_nonneg — default block).
        if spec.get("nonneg") and num < 0:
            anomalies.append({
                "type": ANOMALY_NEGATIVE_WHERE_NONNEG,
                "severity": "block",
                "detail": f"{field}={num:.6g} < 0 (field declared non-negative)",
            })

        # ── 3. Gross outlier outside the sane band ─────────────────────
        # (gross_outlier — default warn; may be a real extreme).
        lo = spec.get("lo")
        hi = spec.get("hi")
        if lo is not None and num < lo:
            anomalies.append({
                "type": ANOMALY_GROSS_OUTLIER,
                "severity": "warn",
                "detail": f"{field}={num:.6g} < lo={lo:.6g}",
            })
        elif hi is not None and num > hi:
            anomalies.append({
                "type": ANOMALY_GROSS_OUTLIER,
                "severity": "warn",
                "detail": f"{field}={num:.6g} > hi={hi:.6g}",
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
