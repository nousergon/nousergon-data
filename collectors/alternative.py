"""
collectors/alternative.py — Alternative data collector for promoted tickers.

Phase 2 collector: runs AFTER research produces signals.json to fetch
alternative data for the ~900+ promoted tickers (buy candidates + tracked).
Processing is concurrent via ThreadPoolExecutor (I/O-bound API calls overlap)
so wall clock stays sub-10-min regardless of universe size.

Data sources:
  - Analyst rating (Finnhub ``/stock/recommendation`` — free tier)
  - Analyst price target (yfinance ``Ticker.info.targetMeanPrice`` — free)
  - Earnings surprises (Finnhub ``/stock/earnings`` — free tier)
  - EPS estimates (FMP ``/stable/analyst-estimates?period=annual`` — free tier)
  - Options flow (yfinance)
  - Insider trading (SEC EDGAR Form 4)
  - Institutional 13F (edgartools)
  - News headlines (Yahoo RSS + EDGAR 8-K)

Provider notes (updated 2026-04-22)
-----------------------------------
FMP v3 sunsetted on 2025-08-31; all FMP calls go to /stable with
query-string tickers. On /stable, these endpoints are paid-tier (HTTP
402 / 403 on free), so the corresponding features are sourced elsewhere:

  - ``grades-consensus`` → Finnhub ``/stock/recommendation``
  - ``price-target-consensus`` → yfinance ``Ticker.info`` exposes
    ``targetMeanPrice`` + ``numberOfAnalystOpinions`` on free. Finnhub's
    ``/stock/price-target`` is paid-only. yfinance ``.info`` is the same
    per-ticker pattern ``short_interest.py`` uses.
  - ``earnings-surprises-bulk`` → Finnhub ``/stock/earnings`` (richer
    historical data anyway)
  - ``analyst-estimates?period=quarter`` → FMP ``period=annual`` still
    works on free tier.

Output: one JSON file per ticker at market_data/weekly/{date}/alternative/{TICKER}.json
plus a manifest at market_data/weekly/{date}/alternative/manifest.json.

Additionally (write-both, additive) writes a flat single-file options
projection at archive/options/{date}.json — the legacy-shaped key that
alpha-engine-predictor's data/options_fetcher.py::load_historical_options
reads. Producer/consumer key + shape differ (verified read-only against
predictor origin/main 2026-05-16); this mirror starts the ≥1-week soak that
gates the separate, later predictor-side consumer swap (yfinance
centralization plan PR 4b). The canonical per-ticker files are untouched.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import time
import threading
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import boto3

from nousergon_lib.secrets import get_secret
import requests

from validators.price_validator import (
    ALL_FEATURE_ANOMALY_TYPES,
    DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES,
    validate_feature_record,
)
from nousergon_lib.yfinance_quiet import quiet_yfinance

logger = logging.getLogger(__name__)


# ── Write-time value-range gate (ROADMAP L1243, extends #215) ──────────────
# alternative.collect writes one feature-source JSON per ticker to S3 that
# bypasses builders/daily_append.py's validate_today_row gate entirely. A
# corrupt numeric sub-field (a NaN put/call ratio from a 0/0 open-interest
# divide, a negative analyst price target from a malformed yfinance .info,
# a negative fund-increasing count) silently degrades the research qual
# sub-score with no pipeline failure. The per-source ok_ratio gate above
# only checks *presence* of data, not whether the present values are
# sane — this gate closes the value-range half.
#
# Specs are declared per (source, field). Only numeric, semantically
# value-constrained fields are listed; free-form / categorical fields
# (rating string, news article lists) are out of scope for value-range
# validation. lo/hi bands are deliberately generous — the goal is to
# catch gross corruption, not to second-guess a legitimately extreme but
# real metric (gross_outlier warns, never blocks by default).
_ALT_FIELD_SPECS: dict[str, dict[str, dict]] = {
    "analyst_consensus": {
        "target_price": {"nonneg": True, "lo": 0.0, "hi": 1_000_000.0},
        "num_analysts": {"nonneg": True, "lo": 0.0, "hi": 200.0},
    },
    "eps_revision": {
        # EPS can legitimately be negative (loss-making firms); only flag
        # absurd magnitudes as a gross outlier.
        "current_estimate": {"lo": -10_000.0, "hi": 10_000.0},
        "revision_4w": {"lo": -100_000.0, "hi": 100_000.0},
    },
    "options_flow": {
        "put_call_ratio": {"nonneg": True, "lo": 0.0, "hi": 1_000.0},
        "iv_rank": {"nonneg": True, "lo": 0.0, "hi": 100.0},
        "expected_move_pct": {"nonneg": True, "lo": 0.0, "hi": 1_000.0},
    },
    "insider_activity": {
        "net_shares_30d": {"lo": -1e12, "hi": 1e12},
    },
    "institutional": {
        "funds_increasing": {"nonneg": True, "lo": 0.0, "hi": 100_000.0},
        "funds_decreasing": {"nonneg": True, "lo": 0.0, "hi": 100_000.0},
    },
}


def _load_alt_block_anomaly_types() -> frozenset[str]:
    """Read ``ALT_BLOCK_ANOMALY_TYPES`` env var or fall back to defaults.

    Format + validation mirror ``fundamentals._load_fundamentals_block_anomaly_types``
    and ``daily_append._load_block_anomaly_types``: a JSON list of
    feature-anomaly type strings; unknown types raise (NoSilentFails).
    """
    raw = os.environ.get("ALT_BLOCK_ANOMALY_TYPES", "").strip()
    if not raw:
        return DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"ALT_BLOCK_ANOMALY_TYPES is not valid JSON: {exc}. "
            f"Expected a JSON list of feature-anomaly type strings."
        ) from exc
    if not isinstance(parsed, list) or not all(isinstance(x, str) for x in parsed):
        raise RuntimeError(
            f"ALT_BLOCK_ANOMALY_TYPES must be a JSON list of strings, "
            f"got {parsed!r}"
        )
    unknown = set(parsed) - ALL_FEATURE_ANOMALY_TYPES
    if unknown:
        raise RuntimeError(
            f"ALT_BLOCK_ANOMALY_TYPES contains unknown anomaly types: "
            f"{sorted(unknown)}. Known types: "
            f"{sorted(ALL_FEATURE_ANOMALY_TYPES)}"
        )
    return frozenset(parsed)


def _validate_alt_payload(
    payload: dict, ticker: str, block_anomaly_types: frozenset[str]
) -> tuple[list[dict], list[dict]]:
    """Run validate_feature_record over each spec'd sub-section of an
    alternative-data payload.

    Returns ``(blocking, warning)`` anomaly lists (each anomaly dict gains
    a ``source`` key so the caller can log which sub-section failed).
    """
    blocking: list[dict] = []
    warning: list[dict] = []
    for source, specs in _ALT_FIELD_SPECS.items():
        section = payload.get(source)
        if not isinstance(section, dict):
            continue
        qg = validate_feature_record(section, specs, ticker)
        for a in qg["anomalies"]:
            a = {**a, "source": source}
            if a["type"] in block_anomaly_types:
                blocking.append(a)
            else:
                warning.append(a)
    return blocking, warning


def _emit_quality_gate_metrics(
    counts_by_type: dict[str, int], n_blocked: int, n_warned: int
) -> None:
    """Emit ``AlphaEngine/Data/alternative_quality_*`` gauges.

    Best-effort: CloudWatch errors WARN but don't fail the collector.
    Mirrors ``builders.daily_append._emit_quality_gate_metrics`` and
    ``fundamentals._emit_quality_gate_metrics``.
    """
    if not counts_by_type and n_blocked == 0 and n_warned == 0:
        return
    try:
        cw = boto3.client("cloudwatch")
        metric_data: list[dict] = [
            {
                "MetricName": "alternative_quality_blocked_count",
                "Value": float(n_blocked),
                "Unit": "Count",
            },
            {
                "MetricName": "alternative_quality_warned_count",
                "Value": float(n_warned),
                "Unit": "Count",
            },
        ]
        for atype, count in counts_by_type.items():
            metric_data.append({
                "MetricName": "alternative_quality_anomaly_count",
                "Dimensions": [{"Name": "anomaly_type", "Value": atype}],
                "Value": float(count),
                "Unit": "Count",
            })
        cw.put_metric_data(Namespace="AlphaEngine/Data", MetricData=metric_data)
    except Exception as exc:
        logger.warning(
            "CloudWatch alternative_quality_* metric failed: %s. Not "
            "blocking — the aggregated run-level quality-gate logger.error "
            "is the load-bearing Flow Doctor surface.",
            exc,
        )


# ── Generic URL-credential scrubber ────────────────────────────────────────
#
# requests.exceptions.HTTPError embeds the full request URL — including any
# ``apikey=``/``api_key=``/``token=`` querystring credential — in its
# ``str()`` representation. The FMP-backed warnings (e.g. "EPS estimate
# failed for AFL: 402 ... ?apikey=<KEY>&...") and Finnhub-backed warnings
# (``token=<KEY>``) would leak the live credential to CloudWatch. Every
# exception-logging site in this file that can carry an HTTP fetch URL
# routes the exception through this scrubber before logging. It is a no-op
# on strings without a matching querystring fragment (idempotent).
#
# Mirrors collectors/daily_closes._scrub_api_key (FRED-specific, masks only
# ``api_key=``); kept local here because that helper is too narrow for the
# FMP ``apikey=`` / Finnhub ``token=`` shapes and importing it would couple
# two unrelated collectors.
_URL_CRED_RE = re.compile(r"(?i)(api_?key|apikey|token)=[^&\s'\"]+")


def _scrub_url_creds(msg: object) -> str:
    """Mask ``apikey=``/``api_key=``/``token=`` querystring secrets.

    Accepts an exception object or any value; stringifies then masks.
    No-op (returns the input string unchanged) when no credential
    fragment is present, and idempotent on already-scrubbed strings.
    """
    return _URL_CRED_RE.sub(lambda m: f"{m.group(1)}=***", str(msg))


# ── Per-source populated-ratio gate ──────────────────────────────────────────
#
# `_fetch_all_alternative` aggregates 6 heterogeneous sources for each
# ticker. A single source going dark (Finnhub auth fail, FMP 402, SEC
# rate-limit ban, etc.) silently nulls out only its sub-section of the
# output dict — overall `_fetch_all_alternative` still returns successfully,
# the manifest writes "ok", and downstream Research scoring picks up a
# mostly-empty alt-data snapshot that degrades the qual sub-score.
#
# The gate below tracks per-source "did this ticker get real data?"
# coverage and hard-fails the run if ANY source's populated ratio falls
# below its source-specific floor. Thresholds are deliberately
# heterogeneous because the 6 sources have very different baseline
# coverage in the S&P 500/400 universe:
#
#   - analyst_consensus: most large-caps have analyst coverage → 0.80
#   - eps_revision:      coverage drops for newer / smaller listings → 0.50
#   - options_flow:      large-caps mostly options-listed but low-vol
#                        days produce flat-line metrics → 0.30
#   - insider_activity:  Form 4 filings are episodic (insiders don't
#                        trade every quarter) → 0.10
#   - institutional:     13F filings are quarterly + ~45-day lag, so
#                        many tickers have no fresh accumulation
#                        signal in any given week → 0.20
#   - news:              most names produce some news weekly via
#                        Yahoo RSS or 8-K → 0.50
#
# A FLAT ok_ratio across all sources would false-positive on tickers
# that are legitimately sparse on a source (a small-cap with no 13F
# filings is not a provider failure). Source-specific floors avoid that.
#
# Override at runtime via the `ALT_MIN_OK_RATIOS` env var (JSON dict),
# e.g. `{"institutional": 0.05}` to relax the 13F floor during a known
# edgartools outage without code changes.

_DEFAULT_MIN_OK_RATIOS: dict[str, float] = {
    "analyst_consensus": 0.80,
    "eps_revision":      0.50,
    "options_flow":      0.30,
    "insider_activity":  0.10,
    "institutional":     0.0,   # permanently dead; replaced by inst_ownership derived table (2026-07-25)
    "news":              0.50,
}


def _load_min_ok_ratios() -> dict[str, float]:
    """Resolve per-source thresholds. Env override merges over defaults."""
    overrides_raw = os.environ.get("ALT_MIN_OK_RATIOS", "")
    if not overrides_raw:
        return dict(_DEFAULT_MIN_OK_RATIOS)
    try:
        overrides = json.loads(overrides_raw)
    except json.JSONDecodeError as exc:
        # Malformed override hard-fails — silent fallback to defaults
        # would leave operators thinking their tuning landed when it
        # didn't. NoSilentFails.
        raise RuntimeError(
            f"ALT_MIN_OK_RATIOS env var is not valid JSON: {exc}. "
            f"Expected a dict like '{{\"institutional\": 0.05}}'."
        ) from exc
    if not isinstance(overrides, dict):
        raise RuntimeError(
            f"ALT_MIN_OK_RATIOS must be a JSON object, got {type(overrides).__name__}"
        )
    unknown = set(overrides) - set(_DEFAULT_MIN_OK_RATIOS)
    if unknown:
        raise RuntimeError(
            f"ALT_MIN_OK_RATIOS contains unknown sources: {sorted(unknown)}. "
            f"Valid sources: {sorted(_DEFAULT_MIN_OK_RATIOS)}"
        )
    merged = dict(_DEFAULT_MIN_OK_RATIOS)
    for k, v in overrides.items():
        if not isinstance(v, (int, float)) or not (0.0 <= v <= 1.0):
            raise RuntimeError(
                f"ALT_MIN_OK_RATIOS[{k!r}] must be a number in [0.0, 1.0], "
                f"got {v!r}"
            )
        merged[k] = float(v)
    return merged


# Predicates: did this source produce real (non-default) data for the
# ticker? "Real" is deliberately loose — any non-empty / non-zero field
# in the source-specific output schema counts as a populated ticker.
# The goal is to detect "provider went dark" (every ticker gets
# defaults) vs "this ticker's sources are sparse" (a few defaults
# scattered across a mostly-populated batch).

def _has_analyst_data(d: dict) -> bool:
    return (
        d.get("rating") is not None
        or d.get("target_price") is not None
        or d.get("num_analysts") is not None
        or bool(d.get("earnings_surprises"))
    )


def _has_revision_data(d: dict) -> bool:
    return d.get("current_estimate") is not None


def _has_options_data(d: dict) -> bool:
    return any(
        d.get(k) is not None
        for k in ("put_call_ratio", "iv_rank", "expected_move_pct")
    )


def _has_insider_data(d: dict) -> bool:
    return bool(d.get("transactions")) or d.get("net_shares_30d", 0) != 0


def _has_institutional_data(d: dict) -> bool:
    return d.get("funds_increasing", 0) > 0 or d.get("funds_decreasing", 0) > 0


def _has_news_data(d: dict) -> bool:
    return bool(d.get("articles")) or bool(d.get("sec_filings_8k"))


_HAS_DATA_PREDICATES = {
    "analyst_consensus": _has_analyst_data,
    "eps_revision":      _has_revision_data,
    "options_flow":      _has_options_data,
    "insider_activity":  _has_insider_data,
    "institutional":     _has_institutional_data,
    "news":              _has_news_data,
}


# ── Predictor-options mirror (write-both, additive) ──────────────────────────
#
# The canonical alternative-data layout is one JSON per ticker at
# ``market_data/weekly/{date}/alternative/{TICKER}.json`` (options nested
# under the ``options_flow`` sub-dict). That is the format research consumes
# and is left untouched.
#
# alpha-engine-predictor's ``data/options_fetcher.py::load_historical_options``
# reads a DIFFERENT, legacy-shaped key: a single flat file at
# ``archive/options/{date}.json`` mapping ``{ticker: {put_call_ratio, iv_rank,
# atm_iv}}``. Verified read-only against predictor origin/main 2026-05-16:
# the consumer takes ``put_call_ratio`` raw then ``np.log()``-transforms it,
# divides ``iv_rank`` by 100, and defaults ``atm_iv`` to 0.0 on miss. The
# producer's ``options_flow`` already emits ``put_call_ratio`` as a raw ratio
# and ``iv_rank`` on a 0-100 scale — exactly the units the consumer expects
# to log/÷100. ``atm_iv`` is not surfaced by ``_fetch_options`` (it is
# computed locally as a variable but never stored); the consumer's 0.0
# default handles its absence gracefully.
#
# Per the S3-contract rule (additive only, never rename/remove, write-both
# for ≥1 week before any consumer relies on it), the collector ADDITIVELY
# also writes the predictor-expected key/shape alongside the canonical
# per-ticker files. This starts the soak that gates the SEPARATE, LATER
# predictor-side consumer swap (yfinance-centralization plan PR 4b — NOT in
# this PR).

# Predictor-expected single-file mirror key (no s3_prefix — predictor reads
# the bucket root, matching its hardcoded ``archive/options/{date}.json``).
_PREDICTOR_OPTIONS_MIRROR_KEY_FMT = "archive/options/{run_date}.json"

# Per-ticker hard timeout for HTTP calls that lack configurable timeouts
# (yfinance, feedparser). SIGALRM interrupts blocking syscalls, bounding
# a hung network fetch to _PER_TICKER_TIMEOUT_S seconds per ticker. This
# prevents a single unresponsive external source (Yahoo, SEC) from consuming
# the entire 600s Lambda budget — the specific failure pattern of issue #4495
# where two tickers each hit the full 600s wall timeout while memory was
# comfortable (293 MB / 341 MB).
_PER_TICKER_TIMEOUT_S = 20


class _TickerHardTimeout(Exception):
    """Raised when a single ticker's alternative data fetch exceeds its budget."""


@contextmanager
def _ticker_hard_timeout(seconds: int, ticker: str):
    """SIGALRM-based per-ticker hard wall-clock timeout.

    Raises ``_TickerHardTimeout`` on expiry. No-op (yields without arming)
    if SIGALRM is unavailable — not the main thread, or a non-POSIX platform.
    Matches the established pattern in weekly_collector._hard_timeout.
    """
    def _handler(signum, frame):
        raise _TickerHardTimeout(
            f"ticker {ticker} exceeded {seconds}s hard timeout"
        )

    try:
        previous = signal.signal(signal.SIGALRM, _handler)
    except (ValueError, AttributeError):
        # Not in the main thread, or SIGALRM unavailable — run unbounded.
        yield
        return
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _build_predictor_options_mirror(
    per_ticker_alt: dict[str, dict],
) -> dict[str, dict]:
    """Project the canonical per-ticker alt-data payloads down to the flat
    ``{ticker: {put_call_ratio, iv_rank, atm_iv}}`` shape that
    alpha-engine-predictor's ``load_historical_options`` expects.

    Only tickers whose ``options_flow`` carried real data are included
    (predictor neutral-fills missing tickers on its side, matching the
    canonical per-ticker file's own _has_options_data gate). ``atm_iv`` is
    emitted as 0.0 since ``_fetch_options`` does not store it — the consumer
    defaults the same value on a missing key, so this is semantically inert
    and forward-compatible if a future PR surfaces a real ATM IV.
    """
    mirror: dict[str, dict] = {}
    for ticker, payload in per_ticker_alt.items():
        opts = (payload or {}).get("options_flow", {}) or {}
        if not _has_options_data(opts):
            continue
        mirror[ticker] = {
            # Raw ratio — predictor log-transforms on read.
            "put_call_ratio": opts.get("put_call_ratio"),
            # 0-100 scale — predictor divides by 100 on read.
            "iv_rank": opts.get("iv_rank"),
            # Not produced by _fetch_options; predictor defaults 0.0 on miss.
            "atm_iv": opts.get("atm_iv", 0.0),
        }
    return mirror


def _process_one_ticker(
    ticker: str,
    run_date: str,
    bucket: str,
    block_anomaly_types: list[dict],
    s3,
    s3_prefix: str,
) -> dict:
    """Fetch, validate, and write alternative data for a single ticker.

    Returns a result dict with ``status`` (``ok``, ``blocked``, ``error``)
    plus the per-ticker data needed by the caller to aggregate counters
    and post-loop quality gates.  All S3 writes happen inside the worker
    so the main thread only aggregates under a lock — it never touches
    per-ticker payloads.

    Designed to be called from a :class:`concurrent.futures.ThreadPoolExecutor`
    so the I/O-bound external API calls overlap.

    Per-ticker hard timeout (_ticker_hard_timeout via SIGALRM) is applied here.
    When called from a ThreadPoolExecutor worker (the normal concurrent path),
    the timeout is a no-op — SIGALRM can only be delivered to the main thread
    (see _ticker_hard_timeout's except ValueError path). When called from the
    main thread (test harness, or a future single-threaded rework), the timeout
    bounds hung yfinance/feedparser HTTP calls to _PER_TICKER_TIMEOUT_S per
    ticker (config#4495).
    """
    try:
        with _ticker_hard_timeout(_PER_TICKER_TIMEOUT_S, ticker):
            data = _fetch_all_alternative(ticker, run_date, bucket)
        blocking, warning = _validate_alt_payload(
            data, ticker, block_anomaly_types
        )
        if blocking:
            blocked_details: list[dict] = []
            for a in blocking:
                logger.warning(
                    "Alternative quality gate BLOCK %s.%s.%s: %s",
                    ticker, a["source"], a["type"], a["detail"],
                )
                blocked_details.append(a)
            return {
                "status": "blocked",
                "ticker": ticker,
                "blocking": blocking,
                "warning": warning,
            }

        key = f"{s3_prefix}weekly/{run_date}/alternative/{ticker}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2, default=str),
            ContentType="application/json",
        )
        source_oks = {}
        for source_name, predicate in _HAS_DATA_PREDICATES.items():
            if predicate(data.get(source_name, {}) or {}):
                source_oks[source_name] = True
        logger.info("Alternative data: %s -> s3://%s/%s", ticker, bucket, key)
        if warning:
            for a in warning:
                logger.warning(
                    "Alternative quality gate WARN %s.%s.%s: %s",
                    ticker, a["source"], a["type"], a["detail"],
                )
        return {
            "status": "ok",
            "ticker": ticker,
            "data": data,
            "source_oks": source_oks,
            "warning": warning,
        }
    except _TickerHardTimeout:
        logger.warning(
            "Alternative data hard timeout for %s (exceeded %ds)",
            ticker, _PER_TICKER_TIMEOUT_S,
        )
        return {
            "status": "error",
            "ticker": ticker,
            "error": f"per-ticker hard timeout ({_PER_TICKER_TIMEOUT_S}s)",
        }
    except Exception as e:
        scrubbed = _scrub_url_creds(e)
        logger.warning("Alternative data failed for %s: %s", ticker, scrubbed)
        return {"status": "error", "ticker": ticker, "error": scrubbed}


def collect(
    bucket: str,
    s3_prefix: str,
    run_date: str | None = None,
    signals_key: str | None = None,
    tickers: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Fetch alternative data for promoted tickers and write to S3.

    Either pass `tickers` directly or provide `signals_key` to read
    promoted tickers from the latest signals.json.

    Args:
        bucket: S3 bucket
        s3_prefix: market_data/ prefix
        run_date: YYYY-MM-DD (defaults to today)
        signals_key: S3 key for signals.json (auto-detected if None)
        tickers: explicit ticker list (overrides signals_key)
        dry_run: validate without writing

    Returns:
        dict with status, tickers_processed, tickers_failed, errors
    """
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()
    s3 = boto3.client("s3")

    # Resolve ticker list. `None` (caller did not specify) and `[]` (caller
    # explicitly resolved no scope) are DIFFERENT claims, and only the first
    # may fall through to the deprecated signals.json resolver
    # (alpha-engine-config#6508): the production entry point
    # (`weekly_collector._run_phase2`) refuses to collect against an implicit
    # ticker list and raises when constituents.json is unreadable, so an
    # empty list reaching here is an explicit "no scope" — skipping is the
    # honest answer, never a silent re-narrowing of scope.
    if tickers is None:
        tickers = _load_promoted_tickers(s3, bucket, signals_key, run_date)
    if not tickers:
        logger.warning("No promoted tickers found — skipping alternative data")
        return {"status": "skipped", "reason": "no tickers"}

    logger.info("Collecting alternative data for %d tickers", len(tickers))
    _assert_scope_stable(s3, bucket, s3_prefix, run_date, len(tickers))

    if dry_run:
        return {
            "status": "ok_dry_run",
            "tickers": len(tickers),
            "ticker_list": tickers[:10],
        }

    # Read ALT_BLOCK_ANOMALY_TYPES once per run (raises on malformed env —
    # fail fast before fetching).
    block_anomaly_types = _load_alt_block_anomaly_types()

    succeeded = 0
    failed = 0
    errors = []
    # Write-time value-range gate accounting (parallels daily_append +
    # fundamentals).
    n_quality_blocked = 0
    n_quality_warned = 0
    quality_counts_by_type: dict[str, int] = {}
    quality_blocked_details: list[str] = []  # "TICKER.source.type" per block
    # Per-source populated counts. Increment only when the source for
    # this ticker carried real (non-default) data — see _HAS_DATA_PREDICATES
    # commentary above for the silent-fail surface this protects against.
    source_ok_counts: dict[str, int] = {k: 0 for k in _HAS_DATA_PREDICATES}
    # Accumulate successful per-ticker payloads so we can additively write
    # the predictor-expected flat single-file mirror (write-both; see
    # _build_predictor_options_mirror commentary). Canonical per-ticker
    # files above are the source of truth and are left untouched.
    per_ticker_alt: dict[str, dict] = {}

    # ── Concurrent ticker processing ─────────────────────────────────────
    # Process all promoted tickers concurrently via ThreadPoolExecutor.
    # The I/O-bound fetch (FMP + yfinance + SEC EDGAR + news) dominates the
    # per-ticker wall clock; threading overlaps the HTTP round-trips so the
    # total runtime scales sub-linearly with universe size (~3 min for 900
    # tickers at 20 workers instead of ~45 min sequentially).
    #
    # Per-ticker hard timeout (_ticker_hard_timeout via SIGALRM) is applied
    # inside _process_one_ticker. In the concurrent ThreadPoolExecutor path,
    # the timeout no-ops (SIGALRM can only be delivered to the main thread —
    # see _ticker_hard_timeout's except ValueError path). The infrastructure
    # and intent are preserved: when _max_workers=1 or when called from a
    # single-threaded test harness, the timeout arms and bounds hung network
    # fetches at _PER_TICKER_TIMEOUT_S per ticker (config#4495).
    #
    # The FMP rate limiter (_fmp_lock + _FMP_MIN_INTERVAL=1s) is the sole
    # serialisation point — workers naturally queue behind it while their
    # other fetches run in parallel.
    #
    # Shared state (counters, per_ticker_alt, quality details, error list)
    # is protected by _agg_lock.  Per-ticker logging + S3 writes happen
    # inside the worker (logging is thread-safe; boto3 S3 client is
    # thread-safe).
    _max_workers = int(os.environ.get("ALT_DATA_MAX_WORKERS", "20"))
    _agg_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=_max_workers) as executor:
        futures = {
            executor.submit(
                _process_one_ticker,
                ticker,
                run_date,
                bucket,
                block_anomaly_types,
                s3,
                s3_prefix,
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            result = future.result()
            with _agg_lock:
                if result["status"] == "ok":
                    succeeded += 1
                    per_ticker_alt[result["ticker"]] = result["data"]
                    for source_name in result.get("source_oks", {}):
                        source_ok_counts[source_name] += 1
                    if result.get("warning"):
                        for a in result["warning"]:
                            quality_counts_by_type[a["type"]] = (
                                quality_counts_by_type.get(a["type"], 0) + 1
                            )
                        n_quality_warned += 1
                elif result["status"] == "blocked":
                    n_quality_blocked += 1
                    failed += 1
                    errors.append({
                        "ticker": result["ticker"],
                        "error": (
                            "value-range gate blocked: "
                            + "; ".join(
                                f"{a['source']}.{a['type']}"
                                for a in result.get("blocking", [])
                            )
                        ),
                    })
                    for a in result.get("blocking", []):
                        quality_counts_by_type[a["type"]] = (
                            quality_counts_by_type.get(a["type"], 0) + 1
                        )
                        quality_blocked_details.append(
                            f"{result['ticker']}.{a['source']}.{a['type']}"
                        )
                else:  # error
                    failed += 1
                    errors.append({
                        "ticker": result["ticker"],
                        "error": result["error"],
                    })

    if n_quality_blocked:
        # Single aggregated ERROR per run — the Flow Doctor surface for the
        # block path (per-ticker lines above are WARNING-only; one systemic
        # event must produce one alert, not one per ticker — see the
        # 2026-06-11 daily_append EOD storm note).
        detail_list = ", ".join(quality_blocked_details[:20])
        if len(quality_blocked_details) > 20:
            detail_list += f", … +{len(quality_blocked_details) - 20} more"
        logger.error(
            "Alternative quality gate blocked %d ticker(s) this run "
            "(counts=%s): %s",
            n_quality_blocked, quality_counts_by_type, detail_list,
        )
    elif n_quality_warned:
        logger.info(
            "Alternative quality gate: %d blocked, %d warned, counts=%s",
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

    # ── Per-source ok_ratio gate ────────────────────────────────────────────
    # Mirrors `fundamentals.py::_MIN_OK_RATIO` and
    # `short_interest.py::_MIN_OK_RATIO` patterns — every alt-data source
    # that has its own potential failure mode (provider auth, quota, schema
    # change) gets its own threshold. A breach is a hard-fail on the whole
    # collector return so the orchestrator treats Phase 2 as failed instead
    # of the manifest landing as "ok" with a silent half-empty payload.
    min_ok_ratios = _load_min_ok_ratios()
    n_total = len(tickers)
    source_ratios: dict[str, float] = {
        source: source_ok_counts[source] / max(n_total, 1)
        for source in _HAS_DATA_PREDICATES
    }
    breached = [
        (source, source_ratios[source], min_ok_ratios[source])
        for source in _HAS_DATA_PREDICATES
        if source_ratios[source] < min_ok_ratios[source]
    ]

    # ── Predictor-options mirror (additive, write-both) ─────────────────────
    # Project the canonical per-ticker options data down to the flat
    # single-file shape alpha-engine-predictor's load_historical_options
    # reads (archive/options/{date}.json). Written BEFORE the gate raises
    # for the same reason as the manifest: the soak that gates predictor
    # PR 4b should proceed on every run that produced options data, not
    # only clean ones. This is a pure producer-side ADD — the canonical
    # per-ticker files (research's consumers) are unaffected, nothing is
    # renamed or removed.
    predictor_options_mirror = _build_predictor_options_mirror(per_ticker_alt)
    predictor_mirror_key = _PREDICTOR_OPTIONS_MIRROR_KEY_FMT.format(
        run_date=run_date
    )
    s3.put_object(
        Bucket=bucket,
        Key=predictor_mirror_key,
        Body=json.dumps(predictor_options_mirror, indent=2, default=str),
        ContentType="application/json",
    )
    logger.info(
        "Predictor options mirror: %d tickers -> s3://%s/%s (write-both)",
        len(predictor_options_mirror), bucket, predictor_mirror_key,
    )

    # Write manifest BEFORE the gate raises — operators triaging a
    # gate breach need the manifest's per-source counts to identify
    # which provider failed. (This mirrors fundamentals.py: status is
    # the gate decision, but the diagnostic payload always lands.)
    manifest = {
        "run_date": run_date,
        "tickers_requested": n_total,
        "tickers_succeeded": succeeded,
        "tickers_failed": failed,
        "source_ok_counts": source_ok_counts,
        "source_ok_ratios": {k: round(v, 4) for k, v in source_ratios.items()},
        "source_min_ok_ratios": min_ok_ratios,
        "errors": errors[:20],
        **_quality_fields,
    }
    manifest_key = f"{s3_prefix}weekly/{run_date}/alternative/manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2, default=str),
        ContentType="application/json",
    )

    if breached:
        breach_lines = [
            f"{src}: {ratio:.1%} < {floor:.0%} threshold "
            f"({source_ok_counts[src]}/{n_total} populated)"
            for src, ratio, floor in breached
        ]
        msg = (
            "alternative.collect: per-source populated-ratio gate breached "
            "for " + str(len(breached)) + " of " + str(len(_HAS_DATA_PREDICATES))
            + " sources — " + "; ".join(breach_lines)
            + ". Likely a provider outage (Finnhub auth/quota, FMP 402, SEC "
            "rate-limit) silently nulled out a sub-section of the alt-data "
            "snapshot. Refusing to mark Phase 2 as ok with a half-empty "
            "payload that would degrade the research scoring layer."
        )
        logger.error(msg)
        return {
            "status": "error",
            "error": msg,
            "tickers_processed": succeeded,
            "tickers_failed": failed,
            "source_ok_counts": source_ok_counts,
            "source_ok_ratios": {k: round(v, 4) for k, v in source_ratios.items()},
            "source_min_ok_ratios": min_ok_ratios,
            "breached_sources": [src for src, _, _ in breached],
            "errors": errors[:20],
            **_quality_fields,
        }

    status = "ok" if failed == 0 else "partial"
    logger.info(
        "alternative.collect: %d tickers, per-source coverage %s",
        n_total,
        ", ".join(f"{k}={source_ratios[k]:.0%}" for k in _HAS_DATA_PREDICATES),
    )
    return {
        "status": status,
        "tickers_processed": succeeded,
        "tickers_failed": failed,
        "source_ok_counts": source_ok_counts,
        "source_ok_ratios": {k: round(v, 4) for k, v in source_ratios.items()},
        "source_min_ok_ratios": min_ok_ratios,
        "errors": errors[:20],
        **_quality_fields,
    }


def load_from_s3(
    bucket: str,
    s3_prefix: str,
    ticker: str,
    run_date: str | None = None,
) -> dict | None:
    """Load alternative data for a single ticker from S3."""
    s3 = boto3.client("s3")
    if not run_date:
        run_date = _get_latest_date(s3, bucket, s3_prefix)
    if not run_date:
        return None
    try:
        key = f"{s3_prefix}weekly/{run_date}/alternative/{ticker}.json"
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


# -- Ticker resolution -------------------------------------------------------

class ScopeChangedUnexpectedly(RuntimeError):
    """The resolved ticker count moved by more than the allowed factor.

    alpha-engine-config-I5814. On 2026-07-21 this collector's input went from
    27 to 902 tickers overnight because an unrelated producer swap changed the
    file it resolved its scope from. Nothing failed, nothing warned; the only
    symptom was a 33x wall-clock increase that broke through the Lambda ceiling
    on the next weekly run and was diagnosed nine days later.

    A scope change is legitimate — the universe does grow — but it is a
    DECISION, and a decision that arrives as a silent 33x is not one. Raising
    here converts it into a one-line, same-run answer.
    """


# Widest run-over-run change treated as ordinary universe churn. S&P 500+400
# reconstitution moves a handful of names quarterly; anything past +-30% is a
# scope change, not churn.
_SCOPE_CHANGE_TOLERANCE = 0.30

# Key for the operator-declared approved-scope baseline.  When a deliberate
# step-change (universe growth, I5814-style scope swap) is approved by a human
# ruling, the ruling is recorded here so the scope check has a declared
# baseline to compare against.  This decouples the guard from the prior-run
# success requirement — without it, a step change followed by N failed runs is
# a deadlock: the only run that can advance the baseline is a run the guard
# permits, and the guard permits none (the prior manifest is pre-change).
# The approved-scope declaration is a REPO artifact, not an S3 object.
#
# It records a human ruling about what the collector's scope may be, and is
# compared against a scope the run DERIVES from constituents.json — the
# overseer-policy invariant 18 split: the bound stays declared, the measurement
# stays derived.
#
# It lives in the checkout because an S3 object would have to be written by
# hand after merge, which is the post-merge operator step
# pull-request-policy.md §4.2 forbids: nothing about it fails when it never
# runs, so the guard would silently keep taking its fail-open path while
# reading green. In the repo the merge button alone deploys it (the collector
# runs from a `git pull --ff-only` checkout on the spot box), and a change to
# the approved scope appears in a diff with its ruling attached.
_APPROVED_SCOPE_PATH = Path(__file__).with_name("alternative_approved_scope.json")


def _read_approved_scope() -> dict | None:
    """Read the operator-declared approved scope baseline from the checkout.

    Returns None when the file is absent or unreadable — no step-change has
    been declared — so the caller falls through to the prior-run comparison.
    """
    try:
        return json.loads(_APPROVED_SCOPE_PATH.read_text())
    except Exception:  # noqa: BLE001 — absence is the fail-open case
        return None


def _assert_scope_stable(
    s3, bucket: str, s3_prefix: str, run_date: str, n_tickers: int
) -> None:
    """Compare this run's ticker count against a declared baseline.

    Resolution order (first match wins):
      1. Operator-approved scope baseline — ``collectors/alternative_approved_scope.json``,
         a REPO artifact so the merge button alone deploys it.  A human ruling
         — e.g. alpha-engine-config-I5814 — that explicitly blessed a scope
         change.  If ``n_tickers`` matches the approved bound (± tolerance),
         the check passes and no prior-run lookup is needed.
      2. Resolution-time scope marker (``scope.json``) in the most recent prior
         run.  Written BEFORE collection, so even a partial/failed run leaves
         a truthful baseline.
      3. Collection-completion manifest (``manifest.json``).  Legacy path;
         only written on success, which is the deadlock this two-tier scheme
         eliminates.

    Fail-open on a MISSING baseline (no approved scope, no scope.json, no
    manifest within 14 days) — there is nothing to compare against and
    refusing to collect would be worse than collecting.  Fail-CLOSED on a
    baseline that exists and disagrees, which is the case this guard is for.
    """
    # ── Tier 0: operator-approved scope baseline ──────────────────────────
    approved = _read_approved_scope()
    if approved:
        approved_n = approved.get("approved_n")
        if isinstance(approved_n, (int, float)) and approved_n > 0:
            approved_n = int(approved_n)
            ratio = n_tickers / approved_n
            if round(abs(ratio - 1.0), 6) <= _SCOPE_CHANGE_TOLERANCE:
                logger.info(
                    "Alternative scope: %d tickers matches approved baseline "
                    "%d (±%.0f%%) — ruling %s (%s).",
                    n_tickers, approved_n,
                    _SCOPE_CHANGE_TOLERANCE * 100,
                    approved.get("ruling", "unknown"),
                    approved.get("approved_on", "unknown"),
                )
                return
            # Falls through — approved baseline exists but this run's scope
            # disagrees.  Still check the prior-run comparison below, but
            # raise with the approved-baseline context if both disagree.
            logger.warning(
                "Alternative scope: %d tickers differs from approved "
                "baseline %d — falling through to prior-run comparison.",
                n_tickers, approved_n,
            )

    # ── Tier 1 & 2: prior-run scope marker, then manifest ─────────────────
    prior_key = None
    prior_n = None
    for days_back in range(1, 15):
        dt = date.fromisoformat(run_date) - timedelta(days=days_back)
        # scope.json (resolution-time) first, then manifest.json (success-only)
        for suffix in ("alternative/scope.json", "alternative/manifest.json"):
            key = f"{s3_prefix}weekly/{dt}/{suffix}"
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(obj["Body"].read())
            except Exception:  # noqa: BLE001 — absence is the fail-open case
                continue
            value = data.get("tickers_requested")
            if isinstance(value, (int, float)) and value > 0:
                prior_key, prior_n = key, int(value)
                break
        if prior_n is not None:
            break

    if prior_n is None:
        logger.warning(
            "Alternative scope: no prior scope/manifest within 14 days of %s "
            "and no approved baseline — collecting %d tickers with no "
            "baseline to compare against, so a scope change cannot be "
            "detected this run.",
            run_date, n_tickers,
        )
        return

    ratio = n_tickers / prior_n
    # round() before comparing: 1300/1000 gives 0.30000000000000004, so a
    # bare `>` would reject a ratio that is exactly at the declared bound.
    if round(abs(ratio - 1.0), 6) > _SCOPE_CHANGE_TOLERANCE:
        raise ScopeChangedUnexpectedly(
            f"alternative: resolved {n_tickers} tickers, prior run "
            f"({prior_key}) requested {prior_n} — a {ratio:.2f}x change, past "
            f"the {_SCOPE_CHANGE_TOLERANCE:.0%} churn tolerance. Either the "
            "constituent universe genuinely moved this much (record why and "
            "widen the tolerance deliberately) or something upstream changed "
            "this collector's scope without meaning to — which is exactly "
            "what happened on 2026-07-21 when 27 became 902 "
            "(alpha-engine-config-I5814)."
        )
    logger.info(
        "Alternative scope: %d tickers, %.2fx the prior run's %d — within the "
        "%.0f%% churn tolerance.",
        n_tickers, ratio, prior_n, _SCOPE_CHANGE_TOLERANCE * 100,
    )


def _load_promoted_tickers(
    s3, bucket: str, signals_key: str | None, run_date: str
) -> list[str]:
    """Extract promoted tickers from the latest signals.json.

    DEPRECATED as the production scope resolver (alpha-engine-config-I5814).
    ``weekly_collector._run_phase2`` now passes the constituent universe
    explicitly, from the same ``constituents.json`` phase 1 hands to
    ``fundamentals.collect``. This remains only for direct/ad-hoc calls that
    pass a ``signals_key``.

    Do NOT restore this as the default. ``signals.json``'s ``universe`` array
    is a sizing envelope for the executor (alpha-engine-config-I5809), so
    resolving scope from it makes this collector's cost and coverage a
    function of whichever signals producer is champion — measured on
    2026-07-21 as a silent 27 -> 902 step.
    """
    if not signals_key:
        signals_key = f"signals/{run_date}/signals.json"

    try:
        obj = s3.get_object(Bucket=bucket, Key=signals_key)
        signals = json.loads(obj["Body"].read())
    except Exception:
        # Try previous trading days
        for days_back in range(1, 8):
            dt = date.fromisoformat(run_date) - timedelta(days=days_back)
            try_key = f"signals/{dt}/signals.json"
            try:
                obj = s3.get_object(Bucket=bucket, Key=try_key)
                signals = json.loads(obj["Body"].read())
                logger.info("Using signals from %s (fallback)", dt)
                break
            except Exception:
                continue
        else:
            return []

    tickers = set()

    # Buy candidates
    for candidate in signals.get("buy_candidates", []):
        t = candidate.get("ticker") or candidate.get("symbol")
        if t:
            tickers.add(t)

    # Full board-width universe (~903 names), NOT "currently held + watchlist"
    # as this comment previously (incorrectly) claimed — measured 2026-07-29,
    # alpha-engine-config#5809. This function is DEPRECATED and reachable
    # only via a direct/ad-hoc call that supplies no explicit `tickers` (see
    # the module docstring above); the production path
    # (`weekly_collector._run_phase2`) never reaches this loop.
    #
    # alpha-engine-config#6450 audited this site and its initial verdict was
    # REPOINT to `nousergon_lib.decision_set` (a narrow ~60-name cut). That
    # verdict does not hold for `alternative.collect`'s actual consumer: per
    # `infrastructure/step_function.json`'s DataPhase2 state comment and
    # PR #1186 (merged 2026-07-31), this collector's per-ticker output feeds
    # 7 registered `alternative`-family feature-store columns
    # (`features/registry.py::CATALOG`, consumers='predictor') for the WHOLE
    # ArcticDB universe — a ticker with no alt data reads as 0.0, which is
    # indistinguishable from a real zero. Narrowing this loop's source to a
    # decision_set cut would silently zero those 7 columns for the ~843
    # names outside the cut. That is why the production resolver
    # (`weekly_collector._run_phase2`) passes the FULL constituent universe
    # explicitly via `tickers=`, sourced from `constituents.json`, not a
    # narrower decision_set cut — see #6450 for the corrected verdict.
    for entry in signals.get("universe", []):
        t = entry.get("ticker") or entry.get("symbol")
        if t:
            tickers.add(t)

    return sorted(tickers)


def _get_latest_date(s3, bucket: str, s3_prefix: str) -> str | None:
    """Get the most recent weekly date from latest_weekly.json."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=f"{s3_prefix}latest_weekly.json")
        return json.loads(obj["Body"].read()).get("date")
    except Exception:
        return None


# -- Per-ticker alternative data aggregation ----------------------------------

def _fetch_all_alternative(ticker: str, run_date: str, bucket: str) -> dict:
    """Fetch all alternative data sources for a single ticker."""
    result = {
        "ticker": ticker,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    # 1. Analyst consensus (FMP)
    result["analyst_consensus"] = _fetch_analyst(ticker)

    # 2. EPS revisions (FMP)
    result["eps_revision"] = _fetch_revisions(ticker, bucket, run_date)

    # 3. Options flow (yfinance)
    result["options_flow"] = _fetch_options(ticker, run_date)

    # 4. Insider activity (SEC EDGAR)
    result["insider_activity"] = _fetch_insider(ticker, run_date)

    # 5. Institutional 13F — DEPRECATED 2026-07-13, function removed 2026-07-25.
    #    _fetch_institutional was killed by OOM (Runtime.OutOfMemory) on every
    #    weekly-SF run since at least May 2026 — CIK-mismatch bug meant it
    #    always returned empty, but the edgar library's Company(ticker) +
    #    get_filings("13F-HR") per-ticker call accumulated memory across 903
    #    universe tickers until the Lambda hit its 512 MB ceiling.
    #    Replaced by data/derived/inst_ownership.py (SEC quarterly bulk).
    result["institutional"] = {
        "accumulation": False,
        "funds_increasing": 0,
        "funds_decreasing": 0,
    }

    # 6. News (Yahoo RSS + EDGAR 8-K)
    result["news"] = _fetch_news(ticker)

    return result


# -- Individual fetchers (self-contained, no cross-repo imports) -------------

# ---- FMP + Finnhub rate limiters ----

_FMP_STABLE = "https://financialmodelingprep.com/stable"
_fmp_lock = threading.Lock()
_fmp_last_call = 0.0
_fmp_daily_count = 0
_FMP_DAILY_LIMIT = 250
_FMP_MIN_INTERVAL = 1.0

# Finnhub state moved to collectors/finnhub_client.py (2026-04-24).


def _fmp_get(endpoint: str, params: dict | None = None) -> dict | list:
    """Rate-limited FMP /stable API call.

    Returns ``[]`` if ``FMP_API_KEY`` is missing, the daily budget is
    exhausted, or a 429 trips the per-minute limit. All other errors
    propagate — the caller's try/except must log at WARNING so silent
    endpoint sunsets (the 2026-04 incident) can't hide.
    """
    global _fmp_last_call, _fmp_daily_count
    api_key = get_secret("FMP_API_KEY", required=False, default="")
    if not api_key:
        return []

    url = f"{_FMP_STABLE}/{endpoint}"
    p = {"apikey": api_key}
    if params:
        p.update(params)

    with _fmp_lock:
        if _fmp_daily_count >= _FMP_DAILY_LIMIT:
            logger.warning("FMP daily budget exhausted (%d calls)", _FMP_DAILY_LIMIT)
            return []
        now = time.monotonic()
        wait = _FMP_MIN_INTERVAL - (now - _fmp_last_call)
        if wait > 0:
            time.sleep(wait)
        _fmp_last_call = time.monotonic()
        _fmp_daily_count += 1

    resp = requests.get(url, params=p, timeout=10)
    if resp.status_code == 429:
        with _fmp_lock:
            _fmp_daily_count = _FMP_DAILY_LIMIT
        return []
    resp.raise_for_status()
    return resp.json()


# Finnhub HTTP client extracted to collectors.finnhub_client (2026-04-24)
# so fundamentals.py can share the same rate-limited state. Local alias
# preserves call-site readability without duplicating throttle logic.
from .finnhub_client import finnhub_get as _finnhub_get  # noqa: E402

# yfinance ``Ticker.info`` does its own HTTP internally (curl_cffi / requests
# under the hood), so the L4499 ``request_with_retry`` chokepoint — which owns
# the GET + status interpretation — cannot wrap it. We instead reuse only the
# shared ``backoff_delay`` math (full-jitter exponential backoff) around a
# bespoke per-ticker attempt loop, exactly as the module docstring's
# ``http_retry`` design note sanctions for consumers with their own control
# flow. This mirrors the resilience intent of the Finnhub analyst-gap fix
# (collectors/finnhub_client.py, #397 / #399) — a one-off Yahoo throttle or
# 5xx must not silently null the ``target_price`` half of analyst_consensus —
# without inventing a second retry mechanism.
from nousergon_lib.http_retry import backoff_delay  # noqa: E402

# Tight attempt cap (issue L4611): yfinance ``.info`` is slow and heavily
# Yahoo-throttled, and target_price is NOT the gating field for the
# analyst_consensus populated-ratio gate (``_has_analyst_data`` is satisfied by
# Finnhub rating / num_analysts / earnings). So we keep this to 2 attempts with
# a low backoff cap to bound the alt-data collection runtime within its SSM
# budget. (The Finnhub sibling uses 3 attempts because it IS the gating source.)
_YF_INFO_MAX_ATTEMPTS = 2
_YF_INFO_BACKOFF_CAP = 4.0  # seconds — low cap; runtime-bounded, not gating


# ---- 1. Analyst consensus ----

def _fetch_analyst(ticker: str) -> dict:
    """Fetch analyst rating + target price + earnings surprises.

    Sources: Finnhub ``/stock/recommendation`` for rating + bull/bear
    analyst counts; yfinance ``Ticker.info`` for the consensus price
    target (Finnhub's ``/stock/price-target`` and FMP's
    ``/stable/price-target-consensus`` are both paid-tier); Finnhub
    ``/stock/earnings`` for historical surprises.
    """
    result = {
        "rating": None,
        "target_price": None,
        "num_analysts": None,
        "earnings_surprises": [],
    }

    # Finnhub analyst recommendation: list of {buy, hold, sell, strongBuy,
    # strongSell, period, symbol}. Most recent is first.
    try:
        data = _finnhub_get("stock/recommendation", {"symbol": ticker})
        if isinstance(data, list) and data:
            latest = data[0]
            totals = {k: latest.get(k, 0) or 0 for k in ("strongBuy", "buy", "hold", "sell", "strongSell")}
            total = sum(totals.values())
            bullish = totals["strongBuy"] + totals["buy"]
            bearish = totals["sell"] + totals["strongSell"]
            if total > 0:
                if bullish > bearish and bullish >= totals["hold"]:
                    result["rating"] = "Buy"
                elif bearish > bullish:
                    result["rating"] = "Sell"
                else:
                    result["rating"] = "Hold"
                result["num_analysts"] = total
    except Exception as e:
        logger.warning("Finnhub recommendation failed for %s: %s", ticker, _scrub_url_creds(e))

    # Finnhub historical earnings surprises: list of {actual, estimate,
    # surprise, surprisePercent, period, quarter, symbol, year}, most
    # recent first. Richer than FMP /stable/earnings (which is the
    # forward-looking calendar).
    try:
        data = _finnhub_get("stock/earnings", {"symbol": ticker})
        if isinstance(data, list) and data:
            surprises = []
            for entry in data[:4]:
                actual = entry.get("actual")
                estimated = entry.get("estimate")
                surprise_pct = entry.get("surprisePercent")
                if surprise_pct is None and actual is not None and estimated not in (None, 0):
                    surprise_pct = round((actual - estimated) / abs(estimated) * 100, 2)
                surprises.append({
                    "date": entry.get("period", ""),
                    "actual": actual,
                    "estimated": estimated,
                    "surprise_pct": surprise_pct,
                })
            result["earnings_surprises"] = surprises
    except Exception as e:
        logger.warning("Finnhub earnings failed for %s: %s", ticker, _scrub_url_creds(e))

    # yfinance Ticker.info: ``targetMeanPrice`` is the consensus price target;
    # ``numberOfAnalystOpinions`` is the count of analysts covering the name
    # (a different denominator than the Finnhub rating counts above — Finnhub's
    # ``num_analysts`` is the sum of current-period rating buckets, used for
    # the Buy/Hold/Sell classification; yfinance's count is the analyst
    # coverage universe for the price-target aggregation). Populate
    # ``num_analysts`` from yfinance only if Finnhub did not supply one.
    # Bounded retry on the transient class. yfinance ``.info`` is a single
    # opaque property access (it owns its own HTTP), so — unlike the Finnhub
    # sibling — there is no Response/status to inspect: any raise is treated as
    # transient and retried up to ``_YF_INFO_MAX_ATTEMPTS`` with full-jitter
    # backoff (shared ``backoff_delay`` math), then degrades loudly (WARN, never
    # raises — a per-ticker yfinance outage must not poison the Phase 2 batch).
    try:
        import yfinance as yf

        info = None
        last_exc: Exception | None = None
        for attempt in range(_YF_INFO_MAX_ATTEMPTS):
            try:
                with quiet_yfinance():
                    info = yf.Ticker(ticker).info
                break
            except Exception as e:  # noqa: BLE001 — yfinance raises opaque types
                last_exc = e
                if attempt == _YF_INFO_MAX_ATTEMPTS - 1:
                    raise
                delay = backoff_delay(attempt, cap=_YF_INFO_BACKOFF_CAP)
                logger.warning(
                    "yfinance .info transient for %s — backing off %.1fs "
                    "(attempt %d/%d): %s",
                    ticker, delay, attempt + 1, _YF_INFO_MAX_ATTEMPTS,
                    _scrub_url_creds(e),
                )
                time.sleep(delay)

        info = info or {}
        target = info.get("targetMeanPrice")
        if target is not None:
            result["target_price"] = round(float(target), 2)
        if result["num_analysts"] is None:
            n = info.get("numberOfAnalystOpinions")
            if n is not None:
                result["num_analysts"] = int(n)
    except Exception as e:
        logger.warning("yfinance target_price failed for %s: %s", ticker, _scrub_url_creds(e))

    return result


# ---- 2. EPS revisions ----

def _fetch_revisions(ticker: str, bucket: str, run_date: str) -> dict:
    """Fetch current EPS estimate and compute the ~4-week estimate revision.

    Source: yfinance ``Ticker.eps_trend`` (migrated 2026-05-18 off the FMP
    ``analyst-estimates`` endpoint, which began returning 402 Payment Required
    on the free tier ~2026-05-17 — paid-tier only — collapsing this source to
    ~15%% populated and breaching DataPhase2's per-source populated-ratio gate
    (``eps_revision`` floor 0.50). yfinance is already the integrated provider
    for ``target_price``/``options_flow`` in this module, so the
    auth/availability/idiom precedent is established).

    ``eps_trend`` is itself a revision series: a DataFrame indexed by period
    (``0q``/``+1q``/``0y``/``+1y``) with columns ``current``, ``7daysAgo``,
    ``30daysAgo``, ``60daysAgo``, ``90daysAgo`` (consensus-mean EPS estimate
    snapshots taken at those lookbacks).

    Return contract (UNCHANGED — ``_has_revision_data`` and all downstream
    consumers key off ``current_estimate``):

    * ``current_estimate`` — the ``0y`` (current fiscal year) row's
      ``current`` column. The annual row mirrors the prior FMP
      ``period="annual"`` choice so the field's semantics are unchanged.
    * ``revision_4w`` — percent change of the consensus annual EPS estimate
      over the trailing ~4 weeks, i.e.
      ``(current - 30daysAgo) / abs(30daysAgo) * 100`` on the ``0y`` row
      (same semantic as the prior FMP path's "estimate now vs ~30d ago",
      but sourced from yfinance's own 30-day-ago snapshot instead of a
      week-old S3 archive — which removes the prior dependency on a
      ``archive/revisions/{date}.json`` snapshot that is never written
      anywhere in this codebase, so the old path was effectively dead).
    * ``streak`` — count of consecutive non-negative steps in the ``0y``
      annual estimate walking newest→oldest across
      ``current → 7daysAgo → 30daysAgo → 60daysAgo → 90daysAgo`` (i.e. a
      "consecutive weeks the estimate did not get cut" run length, max 4).
      The prior FMP implementation hardcoded ``streak`` to 0 (it had no
      multi-snapshot series to derive a streak from); this is a strictly
      more faithful derivation of the field's intended meaning, computed
      from the now-available snapshot series. The field/key is preserved.

    ``bucket``/``run_date`` are retained in the signature (callers and the
    legacy S3-snapshot fallback below are unaffected); they are no longer
    required for the primary path since yfinance carries the 30-day-ago
    snapshot inline.
    """
    result = {
        "current_estimate": None,
        "revision_4w": None,
        "streak": 0,
    }

    # ── Primary: yfinance eps_trend (free, already integrated) ──────────────
    try:
        import yfinance as yf

        et = yf.Ticker(ticker).eps_trend
        if et is not None and not et.empty and "0y" in et.index:
            row = et.loc["0y"]

            def _f(v):
                try:
                    if v is None:
                        return None
                    fv = float(v)
                    return fv if fv == fv else None  # drop NaN
                except (TypeError, ValueError):
                    return None

            cur = _f(row.get("current"))
            d30 = _f(row.get("30daysAgo"))
            if cur is not None:
                result["current_estimate"] = round(cur, 4)
            if cur is not None and d30 is not None and d30 != 0:
                result["revision_4w"] = round((cur - d30) / abs(d30) * 100, 2)

            # streak: consecutive non-negative steps newest→oldest
            seq = [
                _f(row.get(c))
                for c in ("current", "7daysAgo", "30daysAgo", "60daysAgo", "90daysAgo")
            ]
            streak = 0
            for newer, older in zip(seq, seq[1:]):
                if newer is None or older is None:
                    break
                if newer - older >= 0:
                    streak += 1
                else:
                    break
            result["streak"] = streak
    except Exception as e:
        # yfinance exceptions do not embed API credentials (no keyed
        # querystring) — mirror the module's existing yfinance warning idiom
        # (_fetch_analyst's target_price block). The _scrub_url_creds helper
        # added by PR #255 consolidates the FMP/keyed-fetch scrub surface; it
        # is not needed on this unkeyed path. (TODO: once #255 merges, no
        # change required here — kept for reviewer context.)
        logger.warning("yfinance eps_trend failed for %s: %s", ticker, e)

    # ── Legacy fallback: S3 prior snapshot for revision_4w only ────────────
    # Retained for behavioural fidelity with the pre-migration function (and
    # to keep the bucket/run_date params load-bearing). Note: no writer of
    # ``archive/revisions/{date}.json`` exists in this codebase, so this
    # loop is a no-op in practice — kept only as a non-regressing safety net
    # in case yfinance supplied a current estimate but no 30-day-ago snapshot.
    if result["current_estimate"] is not None and result["revision_4w"] is None:
        try:
            s3 = boto3.client("s3")
            today = datetime.strptime(run_date, "%Y-%m-%d")
            for days_ago in range(7, 15):
                check_date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
                try:
                    key = f"archive/revisions/{check_date}.json"
                    obj = s3.get_object(Bucket=bucket, Key=key)
                    prior = json.loads(obj["Body"].read())
                    prior_eps = prior.get(ticker, {}).get("eps_current", 0.0)
                    if prior_eps and result["current_estimate"]:
                        result["revision_4w"] = round(
                            (result["current_estimate"] - prior_eps)
                            / abs(prior_eps) * 100,
                            2,
                        )
                    break
                except Exception:
                    continue
        except Exception:
            pass

    return result


# ---- 3. Options flow ----

def _fetch_options(ticker: str, run_date: str) -> dict:
    """Fetch options-derived signals from yfinance."""
    result = {
        "put_call_ratio": None,
        "iv_rank": None,
        "expected_move_pct": None,
    }

    try:
        import yfinance
        import numpy as np

        t = yfinance.Ticker(ticker)
        expiries = t.options
        if not expiries:
            return result

        # Select nearest expiry with 15-60 DTE, prefer ~30 DTE
        today = datetime.strptime(run_date, "%Y-%m-%d")
        best_exp = None
        best_dte = float("inf")
        for exp_str in expiries:
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
                dte = (exp_date - today).days
                if 15 <= dte <= 60 and abs(dte - 30) < abs(best_dte - 30):
                    best_exp = exp_str
                    best_dte = dte
            except ValueError:
                continue

        if not best_exp:
            # Fallback: nearest expiry > 7 DTE
            for exp_str in expiries:
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
                    if (exp_date - today).days > 7:
                        best_exp = exp_str
                        best_dte = (exp_date - today).days
                        break
                except ValueError:
                    continue

        if not best_exp:
            return result

        chain = t.option_chain(best_exp)
        calls, puts = chain.calls, chain.puts

        # Put/call ratio
        put_oi = puts["openInterest"].sum() if "openInterest" in puts.columns else 0
        call_oi = calls["openInterest"].sum() if "openInterest" in calls.columns else 0
        result["put_call_ratio"] = round(put_oi / max(call_oi, 1), 3)

        # ATM IV
        info = t.info if hasattr(t, "info") else {}
        price = info.get("regularMarketPrice") or info.get("previousClose", 0)
        if not price:
            hist = t.history(period="1d")
            price = float(hist["Close"].iloc[-1]) if not hist.empty else 0

        if price > 0 and "strike" in calls.columns and "impliedVolatility" in calls.columns:
            strikes = calls["strike"].values
            if len(strikes) > 0:
                atm_idx = np.abs(strikes - price).argmin()
                atm_iv = float(calls.iloc[atm_idx]["impliedVolatility"])

                # Average with put ATM IV
                if "strike" in puts.columns and "impliedVolatility" in puts.columns:
                    put_strikes = puts["strike"].values
                    if len(put_strikes) > 0:
                        put_atm_idx = np.abs(put_strikes - price).argmin()
                        atm_iv = (atm_iv + float(puts.iloc[put_atm_idx]["impliedVolatility"])) / 2

                # IV rank approximation via realized vol
                try:
                    hist = t.history(period="1y")
                    if not hist.empty and len(hist) >= 30:
                        returns = hist["Close"].pct_change().dropna()
                        rolling_vol = returns.rolling(20).std() * np.sqrt(252)
                        rolling_vol = rolling_vol.dropna()
                        if len(rolling_vol) >= 10:
                            result["iv_rank"] = round(
                                float((rolling_vol < atm_iv).sum() / len(rolling_vol) * 100), 1
                            )
                except Exception:
                    pass

                # Expected move
                if atm_iv > 0 and best_dte > 0:
                    result["expected_move_pct"] = round(
                        atm_iv * np.sqrt(best_dte / 365) * 100, 2
                    )

    except ImportError:
        logger.debug("yfinance/numpy not available for options data")
    except Exception as e:
        logger.warning("Options fetch failed for %s: %s", ticker, _scrub_url_creds(e))

    return result


# ---- 4. Insider activity ----

_EDGAR_BASE = "https://data.sec.gov"
_SEC_RATE_DELAY = 0.25


def _fetch_insider(ticker: str, run_date: str) -> dict:
    """Fetch insider trading data from SEC EDGAR Form 4."""
    result = {
        "cluster_buying": False,
        "net_shares_30d": 0,
        "transactions": [],
    }

    identity = get_secret("EDGAR_IDENTITY", required=False, default="")
    if not identity:
        return result

    headers = {"User-Agent": identity, "Accept": "application/json"}
    today = datetime.strptime(run_date, "%Y-%m-%d")

    # Look up CIK
    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=headers, timeout=15,
        )
        resp.raise_for_status()
        cik = None
        for entry in resp.json().values():
            if entry.get("ticker", "").upper() == ticker.upper():
                cik = str(entry["cik_str"]).zfill(10)
                break
        if not cik:
            return result
        time.sleep(_SEC_RATE_DELAY)
    except Exception:
        return result

    # Get Form 4 filings
    try:
        resp = requests.get(
            f"{_EDGAR_BASE}/submissions/CIK{cik}.json",
            headers=headers, timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        time.sleep(_SEC_RATE_DELAY)
    except Exception:
        return result

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])

    # Count insider buys/sells in last 30 days from filing metadata
    buyers_30d = set()
    net_shares = 0
    transactions = []

    start_date = today - timedelta(days=90)
    for i, form in enumerate(forms):
        if form != "4" or i >= len(dates):
            continue
        filing_date = dates[i]
        try:
            fd = datetime.strptime(filing_date, "%Y-%m-%d")
        except ValueError:
            continue
        if fd < start_date:
            break

        transactions.append({
            "date": filing_date,
            "days_ago": (today - fd).days,
            "form": "4",
        })

    result["transactions"] = transactions[:10]

    return result


# ---- 5. Institutional 13F (REMOVED 2026-07-25) ----
# _fetch_institutional was killed — CIK-mismatch bug meant it always returned
# empty for operating companies, but the edgar library's Company(ticker) +
# get_filings("13F-HR") per-ticker call accumulated memory across 903 universe
# tickers until the Lambda OOM'd at 512 MB. Replaced by the derived inst_ownership
# table (data/derived/inst_ownership.py, SEC quarterly bulk data).
# The static default result is returned inline in _fetch_all_alternative.

# ---- 6. News ----

def _fetch_news(ticker: str) -> dict:
    """Fetch news from Yahoo RSS and EDGAR 8-K."""
    result = {"articles": [], "sec_filings_8k": []}

    # Yahoo RSS
    try:
        import feedparser
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
        feed = feedparser.parse(url)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
        for entry in feed.entries[:10]:
            try:
                pub = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub:
                    pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
                else:
                    pub_dt = datetime.now(timezone.utc)
                if pub_dt < cutoff:
                    continue
                result["articles"].append({
                    "headline": entry.get("title", "").strip(),
                    "source": entry.get("source", {}).get("title", "Yahoo Finance"),
                    "url": entry.get("link", ""),
                    "published_utc": pub_dt.isoformat(),
                })
            except Exception:
                continue
    except ImportError:
        logger.debug("feedparser not available for news")
    except Exception as e:
        logger.warning("Yahoo RSS failed for %s: %s", ticker, _scrub_url_creds(e))

    # EDGAR 8-K
    try:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start_date = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
        url = (
            f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22"
            f"&dateRange=custom&startdt={start_date}&enddt={end_date}&forms=8-K"
        )
        headers = {"User-Agent": "alpha-engine-data/1.0", "Accept-Encoding": "gzip"}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for hit in data.get("hits", {}).get("hits", [])[:5]:
            src = hit.get("_source", {})
            result["sec_filings_8k"].append({
                "title": src.get("display_names", [ticker])[0],
                "date": src.get("file_date", ""),
                "form_type": src.get("form_type", "8-K"),
            })
    except Exception as e:
        logger.warning("EDGAR 8-K failed for %s: %s", ticker, _scrub_url_creds(e))

    return result
