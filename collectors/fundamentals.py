"""
collectors/fundamentals.py — Finnhub TTM fundamentals collection.

Fetches P/E, P/B, D/E, revenue growth, FCF yield, gross margin, ROE,
current ratio for all universe tickers from Finnhub's
``/stock/metric?symbol=X&metric=all`` endpoint.

Runs weekly in DataPhase1. Cached to S3 at archive/fundamentals/{date}.json.
Daily pipeline reads the cached file (fundamentals are quarterly — don't
change within a week).

Migration history
-----------------
- v1: FMP v3 (sunset 2025-08-31).
- v2: FMP /stable (multi-endpoint: key-metrics-ttm + ratios-ttm + income-statement).
- v3 (this file, 2026-04-24): Finnhub /stock/metric?metric=all — single
  endpoint replaces the three FMP calls; FMP /stable moved to paid tier
  on key-metrics-ttm (HTTP 402 observed 2026-04-24 Sat SF run).

Endpoint contract
-----------------
Single Finnhub call per ticker::

    /stock/metric?symbol=AAPL&metric=all

Response shape::

    {
      "metric": {
        "peTTM": ..., "pbAnnual": ..., "totalDebt/totalEquityAnnual": ...,
        "revenueGrowthTTMYoy": ..., "freeCashFlowTTM": ...,
        "marketCapitalization": ..., "grossMarginTTM": ...,
        "roeTTM": ..., "currentRatioAnnual": ...,
        ...
      },
      "metricType": "all",
      "symbol": "AAPL"
    }

FCF yield isn't directly exposed; computed as ``freeCashFlowTTM /
marketCapitalization``. Other fields map 1-to-1 to Finnhub names with
TTM-preferred / annual-fallback semantics for fields where TTM may be
missing for newer listings.

Rate limiting
-------------
Finnhub free tier is 60 req/min. The shared client in
``collectors.finnhub_client`` enforces a 1.1s minimum interval between
calls (~54/min). 903 universe tickers × 1 call each = ~17 min total —
well within DataPhase1's 30-min budget.

Failure semantics
-----------------
Per-ticker errors are logged at WARNING and fall through to NEUTRAL
values, but the collector hard-fails (``status="error"``) if fewer than
``_MIN_OK_RATIO`` of tickers produced real (non-NEUTRAL) data — catches
silent zero outputs (matches the short_interest collector's guard,
matches the original FMP version's no-silent-fails behavior).
"""

from __future__ import annotations

import json
import logging
import time

from alpha_engine_lib.secrets import get_secret

from .finnhub_client import finnhub_get

logger = logging.getLogger(__name__)

# Minimum fraction of requested tickers that must produce real fundamentals
# (at least one non-zero field) for the run to be considered OK. Below
# this threshold the endpoint is probably broken (auth, quota, schema
# change) — don't let a silently-zeroed output flow into the predictor
# feature store and research scoring.
_MIN_OK_RATIO = 0.90


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _clip(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _pick(metrics: dict, *keys: str, default: float = 0.0) -> float:
    """Return the first key with a non-None value, as a float.

    Finnhub exposes most fields with both TTM and Annual variants;
    callers list TTM first and fall through to Annual / Quarterly when
    a field isn't populated for the given ticker (newer listings, ADRs,
    etc.). The same pattern in the legacy FMP collector accepted both
    ``returnOnEquityTTM`` and ``roeTTM`` for forward compatibility —
    Finnhub's schema is similar but with different naming.
    """
    for key in keys:
        if key in metrics and metrics[key] is not None:
            return _safe_float(metrics[key], default=default)
    return default


# Neutral values for tickers where Finnhub returns nothing usable
NEUTRAL = {
    "pe_ratio": 0.0,
    "pb_ratio": 0.0,
    "debt_to_equity": 0.0,
    "revenue_growth_yoy": 0.0,
    "fcf_yield": 0.0,
    "gross_margin": 0.0,
    "roe": 0.0,
    "current_ratio": 0.0,
}


def _fetch_single_ticker(ticker: str) -> dict:
    """Fetch and normalize fundamental data for a single ticker via Finnhub.

    One round-trip replaces the three-endpoint FMP version. Unrecognized
    or missing tickers (delisted, ADRs without coverage, etc.) return
    NEUTRAL — same shape as before so downstream consumers don't change.
    """
    payload = finnhub_get("stock/metric", {"symbol": ticker, "metric": "all"})
    if not isinstance(payload, dict):
        return NEUTRAL.copy()

    metrics = payload.get("metric") or {}
    if not isinstance(metrics, dict) or not metrics:
        return NEUTRAL.copy()

    # P/E: TTM preferred; peExclExtraTTM smooths special-item noise; annual fallback
    pe_raw = _pick(metrics, "peTTM", "peExclExtraTTM", "peNormalizedAnnual")

    # P/B: annual is the canonical book-value reference; quarterly fallback for newly-listed
    pb_raw = _pick(metrics, "pbAnnual", "pbQuarterly")

    # D/E: Finnhub uses literal slash in field name. Quarterly fallback when annual missing.
    de_raw = _pick(
        metrics,
        "totalDebt/totalEquityAnnual",
        "totalDebt/totalEquityQuarterly",
    )

    # Revenue growth: TTM YoY preferred; quarterly YoY fallback; 5Y last (smooths cycles).
    revenue_growth_raw = _pick(
        metrics,
        "revenueGrowthTTMYoy",
        "revenueGrowthQuarterlyYoy",
        "revenueGrowth5Y",
    )

    # Gross margin: TTM preferred; annual fallback; 5Y last.
    gross_margin_raw = _pick(metrics, "grossMarginTTM", "grossMarginAnnual", "grossMargin5Y")

    # ROE: TTM preferred; Rfy (rolling fiscal year) fallback.
    roe_raw = _pick(metrics, "roeTTM", "roeRfy")

    # Current ratio: annual; quarterly fallback.
    current_ratio_raw = _pick(metrics, "currentRatioAnnual", "currentRatioQuarterly")

    # FCF yield: Finnhub doesn't expose this directly. Compute from raw FCF
    # and market cap. Fall back to NEUTRAL (0.0) when either input is
    # missing or non-positive — clipping below would silently emit
    # potentially-meaningful values for negative-FCF firms.
    fcf_ttm = _pick(metrics, "freeCashFlowTTM", "freeCashFlowAnnual")
    market_cap = _pick(metrics, "marketCapitalization")
    if fcf_ttm and market_cap and market_cap > 0:
        fcf_yield_raw = fcf_ttm / market_cap
    else:
        fcf_yield_raw = 0.0

    # Finnhub returns gross margin and ROE as fractions (e.g. 0.42 for 42%);
    # FMP returned them the same way. Clipping ranges unchanged from the
    # FMP version so downstream feature-engineering / scoring sees the
    # same numeric ranges.
    return {
        "pe_ratio": _clip(pe_raw / 30.0, -3.0, 3.0),
        "pb_ratio": _clip(pb_raw / 5.0, -3.0, 3.0),
        "debt_to_equity": _clip(de_raw / 2.0, -3.0, 3.0),
        "revenue_growth_yoy": _clip(revenue_growth_raw, -1.0, 2.0),
        "fcf_yield": _clip(fcf_yield_raw, -0.5, 0.5),
        "gross_margin": _clip(gross_margin_raw, 0.0, 1.0),
        "roe": _clip(roe_raw, -1.0, 1.0),
        "current_ratio": _clip(current_ratio_raw / 3.0, 0.0, 3.0),
    }


def collect(
    bucket: str,
    tickers: list[str],
    run_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Fetch fundamentals for all tickers and cache to S3.

    Returns summary dict with counts. ``status="error"`` if the ok_ratio
    gate is breached — downstream orchestrator treats the phase as failed.
    """
    import boto3

    api_key = get_secret("FINNHUB_API_KEY", required=False, default="")
    if not api_key:
        # Preflight is expected to catch this earlier; hard-fail here too
        # so a missing key can never land as "0 OK / N errors / all-zeros".
        return {
            "status": "error",
            "error": "FINNHUB_API_KEY not set — refusing to write all-NEUTRAL fundamentals",
        }

    logger.info(
        "Fetching fundamentals for %d tickers from Finnhub (/stock/metric)...",
        len(tickers),
    )
    t0 = time.time()

    results: dict[str, dict] = {}
    n_ok = 0
    n_err = 0

    for ticker in tickers:
        try:
            data = _fetch_single_ticker(ticker)
            results[ticker] = data
            if data != NEUTRAL:
                n_ok += 1
        except Exception as e:
            logger.warning("Fundamental fetch failed for %s: %s", ticker, e)
            results[ticker] = NEUTRAL.copy()
            n_err += 1

    elapsed = time.time() - t0
    ok_ratio = n_ok / max(len(tickers), 1)
    logger.info(
        "Fundamentals fetched in %.1fs: %d populated, %d errors, %d total (ok_ratio=%.1f%%)",
        elapsed, n_ok, n_err, len(results), ok_ratio * 100,
    )

    if ok_ratio < _MIN_OK_RATIO:
        msg = (
            f"only {n_ok}/{len(tickers)} tickers ({ok_ratio:.1%}) had populated "
            f"fundamentals — below {_MIN_OK_RATIO:.0%} threshold. Finnhub endpoint "
            f"likely auth-failed, quota-exhausted, or schema-changed. Refusing "
            f"to write a mostly-zero fundamentals snapshot that would silently "
            f"degrade the predictor + research scoring layers."
        )
        logger.error(msg)
        return {
            "status": "error",
            "error": msg,
            "n_tickers": len(results),
            "n_ok": n_ok,
            "n_errors": n_err,
            "elapsed_seconds": round(elapsed, 1),
        }

    if dry_run:
        logger.info("[dry-run] Would write fundamentals for %d tickers", len(results))
        return {
            "status": "ok",
            "n_tickers": len(results),
            "n_ok": n_ok,
            "n_errors": n_err,
            "elapsed_seconds": round(elapsed, 1),
            "dry_run": True,
        }

    # Write to S3
    s3 = boto3.client("s3")
    key = f"archive/fundamentals/{run_date}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(results, default=str),
        ContentType="application/json",
    )
    logger.info("Fundamentals cached to s3://%s/%s", bucket, key)

    return {
        "status": "ok",
        "n_tickers": len(results),
        "n_ok": n_ok,
        "n_errors": n_err,
        "elapsed_seconds": round(elapsed, 1),
        "s3_key": key,
    }
