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
import os
import time

from alpha_engine_lib.secrets import get_secret

from validators.price_validator import (
    ALL_FEATURE_ANOMALY_TYPES,
    DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES,
    validate_feature_record,
)

from .finnhub_client import finnhub_get

logger = logging.getLogger(__name__)

# ── Write-time value-range gate (ROADMAP L1243, extends #215) ──────────────
# fundamentals.py writes a feature-source snapshot to S3 that bypasses
# builders/daily_append.py's validate_today_row gate entirely. A single
# corrupt field (NaN from a divide-by-near-zero FCF computation, or a
# negative gross_margin from a malformed Finnhub payload) silently poisons
# the predictor feature store + research scoring with no pipeline failure —
# the exact FMP-zero'd-fundamentals class that already burned ~2 weeks of
# alpha. Field specs declare the value-range invariant per output field.
# Clipping (_clip) already bounds the *range*, so the load-bearing residual
# this gate catches is NaN/inf (clip of NaN propagates NaN) + the
# structural non-negativity of margin/ratio fields. lo/hi mirror the clip
# bands so a gross outlier surfaces if a future refactor drops a _clip.
_FUNDAMENTALS_FIELD_SPECS: dict[str, dict] = {
    "pe_ratio":           {"lo": -3.0, "hi": 3.0},
    "pb_ratio":           {"lo": -3.0, "hi": 3.0},
    "debt_to_equity":     {"lo": -3.0, "hi": 3.0},
    "revenue_growth_yoy": {"lo": -1.0, "hi": 2.0},
    "fcf_yield":          {"lo": -0.5, "hi": 0.5},
    "gross_margin":       {"nonneg": True, "lo": 0.0, "hi": 1.0},
    "roe":                {"lo": -1.0, "hi": 1.0},
    "current_ratio":      {"nonneg": True, "lo": 0.0, "hi": 3.0},
}


def _load_fundamentals_block_anomaly_types() -> frozenset[str]:
    """Read ``FUNDAMENTALS_BLOCK_ANOMALY_TYPES`` env var or fall back.

    Format + validation mirror ``daily_append._load_block_anomaly_types``:
    a JSON list of feature-anomaly type strings; unknown types raise (a
    silent typo would let corrupt rows through — NoSilentFails). Empty /
    unset uses the conservative default (NaN/inf + negative-where-nonneg
    block; gross_outlier warns).
    """
    raw = os.environ.get("FUNDAMENTALS_BLOCK_ANOMALY_TYPES", "").strip()
    if not raw:
        return DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"FUNDAMENTALS_BLOCK_ANOMALY_TYPES is not valid JSON: {exc}. "
            f"Expected a JSON list of feature-anomaly type strings."
        ) from exc
    if not isinstance(parsed, list) or not all(isinstance(x, str) for x in parsed):
        raise RuntimeError(
            f"FUNDAMENTALS_BLOCK_ANOMALY_TYPES must be a JSON list of strings, "
            f"got {parsed!r}"
        )
    unknown = set(parsed) - ALL_FEATURE_ANOMALY_TYPES
    if unknown:
        raise RuntimeError(
            f"FUNDAMENTALS_BLOCK_ANOMALY_TYPES contains unknown anomaly "
            f"types: {sorted(unknown)}. Known types: "
            f"{sorted(ALL_FEATURE_ANOMALY_TYPES)}"
        )
    return frozenset(parsed)


def _emit_quality_gate_metrics(
    counts_by_type: dict[str, int], n_blocked: int, n_warned: int
) -> None:
    """Emit ``AlphaEngine/Data/fundamentals_quality_*`` gauges.

    Best-effort: CloudWatch errors WARN but don't fail the collector — the
    aggregated run-level quality-gate logger.error is the load-bearing
    Flow Doctor surface; the metric catches slow drift. Mirrors
    ``builders.daily_append._emit_quality_gate_metrics``.
    """
    if not counts_by_type and n_blocked == 0 and n_warned == 0:
        return
    try:
        import boto3

        cw = boto3.client("cloudwatch")
        metric_data: list[dict] = [
            {
                "MetricName": "fundamentals_quality_blocked_count",
                "Value": float(n_blocked),
                "Unit": "Count",
            },
            {
                "MetricName": "fundamentals_quality_warned_count",
                "Value": float(n_warned),
                "Unit": "Count",
            },
        ]
        for atype, count in counts_by_type.items():
            metric_data.append({
                "MetricName": "fundamentals_quality_anomaly_count",
                "Dimensions": [{"Name": "anomaly_type", "Value": atype}],
                "Value": float(count),
                "Unit": "Count",
            })
        cw.put_metric_data(Namespace="AlphaEngine/Data", MetricData=metric_data)
    except Exception as exc:
        logger.warning(
            "CloudWatch fundamentals_quality_* metric failed: %s. Not "
            "blocking — the aggregated run-level quality-gate logger.error "
            "is the load-bearing Flow Doctor surface.",
            exc,
        )

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
#
# Phase 3a of attractiveness-pillars-260520 (2026-05-20): added 5 new
# fundamental fields backing the Growth + Stewardship pillar quant
# subscores. All Finnhub ``/stock/metric?metric=all`` derived — no new
# API integrations. The composites that consume these fields are added
# in alpha-engine-research/scoring/factor_scoring.py Phase 3b.
NEUTRAL = {
    "pe_ratio": 0.0,
    "pb_ratio": 0.0,
    "debt_to_equity": 0.0,
    "revenue_growth_yoy": 0.0,
    "fcf_yield": 0.0,
    "gross_margin": 0.0,
    "roe": 0.0,
    "current_ratio": 0.0,
    # Growth pillar substrate (Phase 3a) — 3y CAGR signals (smoother than
    # TTM YoY; less noise from base-effect / single-quarter anomalies)
    "revenue_growth_3y": 0.0,
    "eps_growth_3y": 0.0,
    # Stewardship pillar substrate (Phase 3a) — payout discipline +
    # reinvestment intensity. Insider-ownership not surfaced here
    # (Finnhub doesn't expose it via metric=all; deferred to a separate
    # PR if/when it becomes load-bearing).
    "payout_ratio": 0.0,
    "dividend_yield": 0.0,
    "capex_growth_5y": 0.0,
    # SIZE pillar substrate (config#1142) — raw market cap (absolute units).
    # Surfaced from the already-fetched ``marketCapitalization`` metric;
    # the feature engineer takes log() and the cross-sectional pass emits
    # the Barra SIZE loading (size_zscore). 0.0 here -> size NaN downstream
    # (log guard), so a missing-cap ticker is excluded rather than mis-sized.
    "market_cap_raw": 0.0,
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

    # SIZE pillar substrate (config#1142): persist the already-fetched raw
    # market cap, UN-clipped / UN-normalized (it is the ``_raw`` column).
    # ``marketCapitalization`` is surfaced as Finnhub reports it (the same
    # source the fcf_yield ratio above already consumes). Non-positive /
    # missing -> 0.0; the feature engineer's log() guard maps 0.0 -> NaN so
    # a capless ticker is EXCLUDED from the SIZE cross-section, never mis-
    # sized. The SIZE loading is scale-invariant (a constant log shift is
    # removed by cross-sectional z-scoring), so the native unit is fine.
    market_cap_raw = market_cap if (market_cap and market_cap > 0) else 0.0

    # ── Growth pillar substrate (Phase 3a of attractiveness-pillars-260520) ──
    # 3-year CAGR signals from Finnhub. Smoother than TTM YoY for
    # composite ranking (base-effect noise + single-quarter anomalies
    # average out). Annual fallbacks for newer listings without a full
    # 3y history.
    revenue_growth_3y_raw = _pick(metrics, "revenueGrowth3Y", "revenueGrowth5Y")
    eps_growth_3y_raw = _pick(
        metrics, "epsGrowth3Y", "epsBasicExclExtraItemsAnnual5Y", "epsGrowth5Y",
    )

    # ── Stewardship pillar substrate (Phase 3a) ──
    # Payout ratio + dividend yield + capex growth proxy. Insider ownership
    # is NOT here — Finnhub's metric=all does not surface it; would require
    # a separate /stock/insider-transactions integration. Deferred to a
    # follow-up if/when stewardship gains discriminative weight in the
    # composite. The three signals here cover the "capital allocation
    # discipline" axis: payout (return-of-capital intensity), dividend
    # yield (vs. payout, identifies low-yield + low-payout = buyback-
    # heavy retainers), and capex growth (reinvestment intensity).
    payout_ratio_raw = _pick(metrics, "payoutRatioTTM", "payoutRatioAnnual")
    dividend_yield_raw = _pick(
        metrics, "dividendYieldIndicatedAnnual", "currentDividendYieldTTM",
    )
    capex_growth_5y_raw = _pick(metrics, "capitalSpendingGrowth5Y")

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
        # Growth pillar quant signals
        "revenue_growth_3y": _clip(revenue_growth_3y_raw, -0.5, 1.5),
        "eps_growth_3y": _clip(eps_growth_3y_raw, -1.0, 2.0),
        # Stewardship pillar quant signals
        "payout_ratio": _clip(payout_ratio_raw, 0.0, 2.0),
        "dividend_yield": _clip(dividend_yield_raw, 0.0, 0.20),
        "capex_growth_5y": _clip(capex_growth_5y_raw, -1.0, 2.0),
        # SIZE pillar substrate (config#1142): raw market cap in USD,
        # deliberately UN-clipped/UN-normalized (it's a _raw column). The
        # SIZE loading's log + cross-sectional z-score downstream tames the
        # scale; clipping here would corrupt the absolute units.
        "market_cap_raw": market_cap_raw,
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

    # Read FUNDAMENTALS_BLOCK_ANOMALY_TYPES once per run (raises on
    # malformed env — fail fast before fetching).
    block_anomaly_types = _load_fundamentals_block_anomaly_types()

    results: dict[str, dict] = {}
    n_ok = 0
    n_err = 0
    # Write-time value-range gate accounting (parallels daily_append).
    n_quality_blocked = 0  # records replaced with NEUTRAL (block severity)
    n_quality_warned = 0   # records kept but flagged (warn severity)
    quality_counts_by_type: dict[str, int] = {}
    quality_blocked_details: list[str] = []  # "TICKER.type" per block

    for ticker in tickers:
        try:
            data = _fetch_single_ticker(ticker)
            # ── Write-time value-range gate ─────────────────────────────
            # Runs on the fully-shaped (clipped) per-ticker dict before it
            # is queued for the S3 snapshot. block → drop the corrupt row
            # to NEUTRAL + count (a NaN/inf or negative margin would
            # otherwise poison the predictor feature store); the aggregated
            # run-level logger.error below surfaces blocks to Flow Doctor.
            # warn → keep + log + count.
            qg = validate_feature_record(
                data, _FUNDAMENTALS_FIELD_SPECS, ticker
            )
            blocking = [
                a for a in qg["anomalies"]
                if a["type"] in block_anomaly_types
            ]
            if blocking:
                for a in blocking:
                    # WARNING per ticker; the single aggregated run-level
                    # logger.error below is the Flow Doctor surface (one
                    # systemic event → one alert, not one per ticker).
                    logger.warning(
                        "Fundamentals quality gate BLOCK %s.%s: %s",
                        ticker, a["type"], a["detail"],
                    )
                    quality_counts_by_type[a["type"]] = (
                        quality_counts_by_type.get(a["type"], 0) + 1
                    )
                    quality_blocked_details.append(f"{ticker}.{a['type']}")
                n_quality_blocked += 1
                # Refuse the corrupt row; NEUTRAL is the existing
                # no-data sentinel the ok_ratio gate already accounts for.
                results[ticker] = NEUTRAL.copy()
                n_err += 1
                continue
            if qg["anomalies"]:
                for a in qg["anomalies"]:
                    logger.warning(
                        "Fundamentals quality gate WARN %s.%s: %s",
                        ticker, a["type"], a["detail"],
                    )
                    quality_counts_by_type[a["type"]] = (
                        quality_counts_by_type.get(a["type"], 0) + 1
                    )
                n_quality_warned += 1
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
    if n_quality_blocked:
        # Single aggregated ERROR per run — the Flow Doctor surface for the
        # block path (per-ticker lines above are WARNING-only; one systemic
        # event must produce one alert, not one per ticker — see the
        # 2026-06-11 daily_append EOD storm note).
        detail_list = ", ".join(quality_blocked_details[:20])
        if len(quality_blocked_details) > 20:
            detail_list += f", … +{len(quality_blocked_details) - 20} more"
        logger.error(
            "Fundamentals quality gate blocked %d ticker(s) this run "
            "(counts=%s): %s",
            n_quality_blocked, quality_counts_by_type, detail_list,
        )
    elif n_quality_warned:
        logger.info(
            "Fundamentals quality gate: %d blocked, %d warned, counts=%s",
            n_quality_blocked, n_quality_warned, quality_counts_by_type,
        )
    _emit_quality_gate_metrics(
        quality_counts_by_type, n_quality_blocked, n_quality_warned
    )
    _quality_fields = {
        "tickers_quality_blocked": n_quality_blocked,
        "tickers_quality_warned": n_quality_warned,
        "quality_anomaly_counts": quality_counts_by_type,
        "quality_block_anomaly_types": sorted(block_anomaly_types),
    }

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
            **_quality_fields,
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
            **_quality_fields,
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
        **_quality_fields,
    }
