"""
sf_preflight.py — Predict whether the Saturday SF would succeed BEFORE
launching a spot.

Today's Saturday SF (ne-weekly-freshness-pipeline) is a 50-min spot run
that costs 1 polygon API call (free-tier 5/min budget) per attempt and a
spot bootstrap (~3 min wall-clock + IAM/SSM dance). Repeated launch-fail
cycles burn polygon quota and operator hours. This module simulates the
critical pre-Phase-1 path against real S3 + ArcticDB state and reports
predicted pass/fail per step BEFORE any compute fires.

Usage:
    python sf_preflight.py                         # human-readable summary
    python sf_preflight.py --json                  # structured output
    python sf_preflight.py --bucket <override>     # alternate bucket

Exit codes:
    0  all checks pass — SF is predicted to succeed
    1  ≥1 check fails — fix before redrive

Polygon API budget: 1 call total (one grouped-daily lookup for the prior
trading day). Same call the actual SF makes; reusable in spirit since the
SF re-fetches anyway in MorningEnrich.

What this catches (mapped to today's incidents):
    PR #130 (backfill regression)         — check_backfill_source_freshness
    PR #131 (polygon coverage flake)      — check_polygon_grouped_coverage
    PR #132 (missing-from-closes scoping) — check_predicted_missing_from_closes
    PR #133 (freshness scan scoping)      — check_universe_sample_freshness
    PR #134 (workflow ordering)           — check_universe_drift
    PR #135 (return shape)                — check_constituents_fetch
    Postflight contracts                  — check_postflight_contracts
    IAM misconfiguration (2026-07-27)     — check_sf_iam_reachability (Leg 1)
    Tool-contract flag/version mismatch   — check_tool_contracts (Leg 2)
    Unresolvable JSONPath in SF def       — check_definition_input_coherence (Leg 3)
    Lambda alias memory headroom breach   — check_lambda_memory_headroom (Leg 4)

What this CANNOT catch:
    - Polygon coverage flipping AFTER preflight succeeds (transient
      between preflight + actual SF kickoff). PR #131 is defense for this.
    - ArcticDB write failures (we don't write here).
    - Spot reclaim / SSM timeouts (infrastructure-level).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"

# ── SF IAM-reachability constants (check_sf_iam_reachability) ─────────────────
_REGION = "us-east-1"
_ACCOUNT = "711398986525"
_WEEKLY_SF_ARN = (
    f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:ne-weekly-freshness-pipeline"
)
_SF_ROLE_ARN = f"arn:aws:iam::{_ACCOUNT}:role/alpha-engine-step-functions-role"

# Name tag the weekly SF's per-execution dispatch box carries. Every
# ssm:SendCommand grant against that box is tag-scoped, not instance-scoped,
# because the box has no stable id (nousergon-data#975).
_WEEKLY_SPOT_TAG = "alpha-engine-weekly-freshness-spot"

# Identities OTHER than the SF execution role that send SSM commands to the
# per-execution spot box. Kept explicit because it cannot be derived from the
# SF definition: these are Lambdas the SF invokes, which then send their own
# SSM commands under their OWN execution role.
#
# This list exists because of a real miss: nousergon-data#975 tag-scoped the SF
# role's grant but left substrate-health-gate's enumerating two static instance
# ARNs, so the first post-merge rehearsal died on AccessDeniedException. Adding
# a Lambda that sends SSM to this box without adding it here reopens that gap.
_SSM_SENDING_ROLES = [
    f"arn:aws:iam::{_ACCOUNT}:role/alpha-engine-substrate-health-gate-role",
]

# Same threshold daily_append uses (DAILY_APPEND_MISSING_THRESHOLD).
# Pre-MorningEnrich prune (PR #134) drops stragglers, so the residual
# count should be the chronic polygon-coverage gaps only (BF-B, BRK-B,
# MOG-A, PSTG = 4 today).
_MISSING_FROM_CLOSES_THRESHOLD = 5

# Universe-freshness scan threshold from builders/daily_append.py.
_UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS = 3  # ~5 calendar days under previous threshold, now trading-day-aware

# Postflight SPY freshness threshold (validators/postflight.py).
_POSTFLIGHT_SPY_MAX_STALE_DAYS = 1

# Sample size for the universe-freshness check; matches the post-write
# scan's _UNIVERSE_SCAN_WORKERS budget.
_UNIVERSE_SAMPLE_SIZE = 20


# ── Result types ──────────────────────────────────────────────────────────────


@dataclass
class CheckResult:
    name: str
    status: str  # "ok" | "warn" | "fail"
    message: str
    details: dict = field(default_factory=dict)
    elapsed_seconds: float = 0.0


@dataclass
class PreflightContext:
    bucket: str
    today: str  # YYYY-MM-DD
    prior_trading_day: str  # YYYY-MM-DD
    fresh_constituents: "set[str] | None" = None  # populated by check_constituents_fetch
    arctic_universe_symbols: "set[str] | None" = None  # populated by check_arctic_connectivity
    polygon_returned_tickers: "set[str] | None" = None  # populated by check_polygon_grouped_coverage
    # ArcticDB handles — initialized once in check_arctic_connectivity and
    # reused across downstream checks. ArcticDB on macOS crashes in
    # ``Aws::S3::S3Client::S3Client`` when ``adb.Arctic(uri)`` runs more
    # than once per process (AWS SDK init race), so every check that needs
    # arctic must read these from ctx instead of re-initializing.
    universe_lib: "Any | None" = None
    macro_lib: "Any | None" = None


# ── Individual checks ─────────────────────────────────────────────────────────


def check_constituents_fetch(ctx: PreflightContext) -> CheckResult:
    """Catches PR #135 class: ``constituents.collect()`` return-shape regressions.

    Calls the real ``_fetch_constituents()`` (Wikipedia, no rate limit) and
    asserts the contract: non-empty tickers, complete sector map. The S&P
    500/400 split must each contribute their expected ~500/~400 counts.
    """
    import time
    t0 = time.time()
    try:
        from collectors.constituents import _fetch_constituents
        tickers, sector_map, sector_etf_map, sub_industry_map, sp500, sp400 = _fetch_constituents()
    except Exception as exc:
        return CheckResult(
            name="constituents_fetch",
            status="fail",
            message=f"Wikipedia fetch raised: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    if not tickers:
        return CheckResult(
            name="constituents_fetch",
            status="fail",
            message="Wikipedia returned 0 tickers",
            elapsed_seconds=time.time() - t0,
        )
    if sp500 < 480 or sp500 > 520:
        return CheckResult(
            name="constituents_fetch",
            status="fail",
            message=f"S&P 500 count out of band: {sp500} (expected 480-520)",
            elapsed_seconds=time.time() - t0,
        )
    if sp400 < 380 or sp400 > 420:
        return CheckResult(
            name="constituents_fetch",
            status="fail",
            message=f"S&P 400 count out of band: {sp400} (expected 380-420)",
            elapsed_seconds=time.time() - t0,
        )
    unmapped = [t for t in tickers if t not in sector_map]
    if unmapped:
        return CheckResult(
            name="constituents_fetch",
            status="fail",
            message=f"sector_map missing for {len(unmapped)} tickers (collect would raise)",
            details={"unmapped_sample": unmapped[:10]},
            elapsed_seconds=time.time() - t0,
        )

    ctx.fresh_constituents = set(tickers)
    return CheckResult(
        name="constituents_fetch",
        status="ok",
        message=f"Wikipedia OK: {len(tickers)} tickers ({sp500} S&P 500 + {sp400} S&P 400)",
        details={"total": len(tickers), "sp500": sp500, "sp400": sp400},
        elapsed_seconds=time.time() - t0,
    )


def check_arctic_connectivity(ctx: PreflightContext) -> CheckResult:
    """ArcticDB cluster reachable + macro/universe libraries present.

    Mirrors the existing preflight.py ArcticDB probe but populates the
    universe symbol set into the context for downstream checks.
    """
    import time
    t0 = time.time()
    try:
        import arcticdb as adb
        from nousergon_lib.arcticdb import open_arctic
        arctic = open_arctic(ctx.bucket, region="us-east-1")
        libs = set(arctic.list_libraries())
        if "universe" not in libs or "macro" not in libs:
            return CheckResult(
                name="arctic_connectivity",
                status="fail",
                message=f"ArcticDB missing required libraries: have {sorted(libs)}",
                elapsed_seconds=time.time() - t0,
            )
        ctx.universe_lib = arctic.get_library("universe")
        ctx.macro_lib = arctic.get_library("macro")
        symbols = set(ctx.universe_lib.list_symbols())
        ctx.arctic_universe_symbols = symbols
    except Exception as exc:
        return CheckResult(
            name="arctic_connectivity",
            status="fail",
            message=f"ArcticDB probe raised: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="arctic_connectivity",
        status="ok",
        message=f"ArcticDB reachable; universe library has {len(symbols)} symbols",
        details={"universe_size": len(symbols)},
        elapsed_seconds=time.time() - t0,
    )


def check_universe_drift(ctx: PreflightContext) -> CheckResult:
    """Catches PR #134 class: stragglers in arctic that aren't in
    current constituents, predicting the pre-MorningEnrich prune outcome.

    Computes ``arctic - constituents``, identifies which would actually
    be pruned (last_date >= 5d stale, matching PR #134's absent_days=5).
    """
    import time
    t0 = time.time()
    if ctx.fresh_constituents is None or ctx.arctic_universe_symbols is None:
        return CheckResult(
            name="universe_drift",
            status="fail",
            message="Skipped: prior checks failed to populate context",
            elapsed_seconds=time.time() - t0,
        )

    from features.compute import _SKIP_TICKERS, _is_sector_etf

    candidates = sorted(
        s for s in ctx.arctic_universe_symbols
        if s not in ctx.fresh_constituents
        and s not in _SKIP_TICKERS
        and not _is_sector_etf(s)
    )

    if not candidates:
        return CheckResult(
            name="universe_drift",
            status="ok",
            message="No straggler candidates (arctic ⊆ constituents)",
            elapsed_seconds=time.time() - t0,
        )

    # Reuse the universe lib from check_arctic_connectivity to avoid the
    # macOS arcticdb re-init crash (see PreflightContext docstring).
    if ctx.universe_lib is None:
        return CheckResult(
            name="universe_drift",
            status="fail",
            message="Skipped: arctic_connectivity did not populate universe_lib",
            elapsed_seconds=time.time() - t0,
        )
    universe_lib = ctx.universe_lib
    import pandas as pd
    today_ts = pd.Timestamp(ctx.today)

    will_prune: list[dict] = []
    will_skip: list[dict] = []
    for ticker in candidates:
        try:
            df = universe_lib.tail(ticker, n=1).data
            last_ts = pd.Timestamp(df.index[-1]).normalize() if not df.empty else None
        except Exception:
            last_ts = None
        if last_ts is None:
            will_skip.append({"ticker": ticker, "reason": "unreadable"})
            continue
        # Trading-day staleness via nousergon_lib.dates — the prune
        # decision is "has this ticker missed N+ NYSE sessions since its
        # last write?", which is independent of calendar weekends/holidays.
        from nousergon_lib.dates import trading_days_stale
        days_stale = trading_days_stale(last_ts.date(), today_ts.date().isoformat())
        entry = {"ticker": ticker, "last_date": last_ts.date().isoformat(), "days_stale": days_stale}
        # PR #134 uses absent_days=5 calendar; under trading-day arithmetic
        # ~3 sessions is the equivalent (a week of weekdays minus the
        # weekend buffer that calendar arithmetic absorbed).
        if days_stale > 3:
            will_prune.append(entry)
        else:
            will_skip.append({**entry, "reason": "below_3_trading_day_threshold"})

    # Escalate to FAIL if any straggler is "old enough to prune" (>5d stale)
    # AND we're about to launch a recovery SF that skips MorningEnrich (the
    # only place prune currently runs). The 2026-05-02 SF redrive #6 caught
    # this: skip_data_phase1=true bypassed prune, Backtester preflight
    # halted on the same stragglers. The post-PR-loop fix in backfill.py
    # closes the regenerative loop on the DataPhase1 path; this check
    # gates the manual-recovery path so operators don't burn a 120-min
    # spot to re-discover stragglers we can see right here.
    status = "fail" if will_prune else "ok"
    message_prefix = (
        f"{len(will_prune)} ticker(s) need pruning before any SF launch — "
        if will_prune else ""
    )
    return CheckResult(
        name="universe_drift",
        status=status,
        message=(
            f"{message_prefix}"
            f"{len(candidates)} arctic stragglers; {len(will_prune)} would be pruned, "
            f"{len(will_skip)} too fresh to drop"
        ),
        details={
            "candidates_count": len(candidates),
            "would_prune_count": len(will_prune),
            "would_prune": will_prune[:20],
            "would_skip_count": len(will_skip),
            "remediation": (
                "Run MorningEnrich (full SF) OR manually invoke "
                "prune_delisted_tickers --apply --absent-days 5 against the "
                "would_prune list before launching Backtester / recovery SFs."
            ) if will_prune else None,
        },
        elapsed_seconds=time.time() - t0,
    )


def check_universe_sample_freshness(ctx: PreflightContext) -> CheckResult:
    """Catches PR #133 class: post-write freshness scan tripping on
    expected tickers.

    Sample 20 from ``arctic ∩ constituents`` (the same population the
    actual scan would audit after PR #134's prune drains stragglers).
    Predict any stale.
    """
    import time
    t0 = time.time()
    if ctx.fresh_constituents is None or ctx.arctic_universe_symbols is None:
        return CheckResult(
            name="universe_sample_freshness",
            status="fail",
            message="Skipped: prior checks failed to populate context",
            elapsed_seconds=time.time() - t0,
        )

    import arcticdb as adb
    import pandas as pd
    import random

    relevant = sorted(ctx.arctic_universe_symbols & ctx.fresh_constituents)
    if not relevant:
        return CheckResult(
            name="universe_sample_freshness",
            status="fail",
            message="Empty (arctic ∩ constituents) — universe pruned to nothing or constituents misconfigured",
            elapsed_seconds=time.time() - t0,
        )

    rng = random.Random(ctx.today)
    sample = rng.sample(relevant, min(_UNIVERSE_SAMPLE_SIZE, len(relevant)))

    if ctx.universe_lib is None:
        return CheckResult(
            name="universe_sample_freshness",
            status="fail",
            message="Skipped: arctic_connectivity did not populate universe_lib",
            elapsed_seconds=time.time() - t0,
        )
    universe_lib = ctx.universe_lib
    today = pd.Timestamp(ctx.today).normalize()

    stale: list[dict] = []
    for ticker in sample:
        try:
            df = universe_lib.tail(ticker, n=1).data
            last_ts = pd.Timestamp(df.index[-1]).normalize() if not df.empty else None
        except Exception:
            last_ts = None
        if last_ts is None:
            stale.append({"ticker": ticker, "reason": "unreadable"})
            continue
        from nousergon_lib.dates import trading_days_stale
        days_stale = trading_days_stale(last_ts.date(), today.date().isoformat())
        if days_stale > _UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS:
            stale.append({
                "ticker": ticker,
                "last_date": last_ts.date().isoformat(),
                "trading_days_stale": days_stale,
            })

    if stale:
        return CheckResult(
            name="universe_sample_freshness",
            status="warn",
            message=(
                f"{len(stale)}/{len(sample)} sampled symbols >{_UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS} trading-day(s) "
                f"stale TODAY (post-MorningEnrich would refresh, so not a hard-fail; "
                f"flagging for visibility)"
            ),
            details={"stale": stale[:10]},
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="universe_sample_freshness",
        status="ok",
        message=f"Sampled {len(sample)} symbols, all within {_UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS} trading-day(s) of today",
        elapsed_seconds=time.time() - t0,
    )


def check_polygon_grouped_coverage(ctx: PreflightContext) -> CheckResult:
    """ONE polygon grouped-daily call to predict missing-from-closes.

    Same call the actual SF makes — re-using the rate-limit slot that
    would otherwise be spent during the SF run. Populates the returned
    ticker set into the context for downstream checks.
    """
    import time
    t0 = time.time()
    if ctx.fresh_constituents is None:
        return CheckResult(
            name="polygon_grouped_coverage",
            status="fail",
            message="Skipped: constituents fetch failed",
            elapsed_seconds=time.time() - t0,
        )

    from nousergon_lib.secrets import get_secret
    if not get_secret("POLYGON_API_KEY", required=False):
        # Local-laptop preflight — polygon key lives in .env on the spot
        # and on EC2. Skip without failing so the rest of the report is
        # actionable; on the spot the key is present and this fires.
        return CheckResult(
            name="polygon_grouped_coverage",
            status="warn",
            message="POLYGON_API_KEY not set — skipped (will run on spot/EC2)",
            elapsed_seconds=time.time() - t0,
        )

    try:
        from polygon_client import polygon_client, PolygonForbiddenError
        grouped = polygon_client().get_grouped_daily(ctx.prior_trading_day)
    except PolygonForbiddenError as exc:
        return CheckResult(
            name="polygon_grouped_coverage",
            status="fail",
            message=f"Polygon 403 — same-day fetch on free tier? ({exc})",
            elapsed_seconds=time.time() - t0,
        )
    except Exception as exc:
        return CheckResult(
            name="polygon_grouped_coverage",
            status="fail",
            message=f"Polygon raised: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    if not grouped:
        return CheckResult(
            name="polygon_grouped_coverage",
            status="fail",
            message=f"Polygon returned 0 tickers for {ctx.prior_trading_day}",
            elapsed_seconds=time.time() - t0,
        )

    polygon_symbols = set(grouped.keys())
    ctx.polygon_returned_tickers = polygon_symbols

    requested = ctx.fresh_constituents
    covered = polygon_symbols & requested
    coverage_ratio = len(covered) / len(requested) if requested else 0
    missing = sorted(requested - polygon_symbols)

    if coverage_ratio < 0.95:
        return CheckResult(
            name="polygon_grouped_coverage",
            status="fail",
            message=(
                f"Polygon coverage {coverage_ratio:.1%} below 95% — "
                f"{len(missing)} of {len(requested)} requested constituents missing"
            ),
            details={"missing_sample": missing[:20]},
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="polygon_grouped_coverage",
        status="ok",
        message=(
            f"Polygon returned {len(polygon_symbols)} tickers; covers "
            f"{len(covered)}/{len(requested)} constituents ({coverage_ratio:.1%})"
        ),
        details={
            "polygon_total": len(polygon_symbols),
            "constituents_covered": len(covered),
            "constituents_missing": len(missing),
            "missing_sample": missing[:10],
        },
        elapsed_seconds=time.time() - t0,
    )


def check_predicted_missing_from_closes(ctx: PreflightContext) -> CheckResult:
    """Catches PR #132/#134 class: predict the missing-from-closes count
    daily_append would compute AFTER the pre-MorningEnrich prune drains
    stragglers. Should be the chronic polygon gaps only (≤4 today).
    """
    import time
    t0 = time.time()
    if ctx.fresh_constituents is None or ctx.arctic_universe_symbols is None:
        return CheckResult(
            name="predicted_missing_from_closes",
            status="fail",
            message="Skipped: prior checks failed to populate context",
            elapsed_seconds=time.time() - t0,
        )
    if ctx.polygon_returned_tickers is None:
        return CheckResult(
            name="predicted_missing_from_closes",
            status="warn",
            message="Skipped: polygon check skipped (no API key locally)",
            elapsed_seconds=time.time() - t0,
        )

    # Simulate post-prune state: arctic ∩ constituents (stragglers gone).
    post_prune_arctic = ctx.arctic_universe_symbols & ctx.fresh_constituents

    # Closes will contain whatever polygon returned + per-ticker fallback
    # (PR #131). Per-ticker fallback recovers ~0 of the chronic 4 today
    # (BF-B, BRK-B, MOG-A, PSTG); model worst-case = no recovery.
    expected_closes = ctx.polygon_returned_tickers
    missing = sorted(post_prune_arctic - expected_closes)
    n_missing = len(missing)

    if n_missing > _MISSING_FROM_CLOSES_THRESHOLD:
        return CheckResult(
            name="predicted_missing_from_closes",
            status="fail",
            message=(
                f"Predicted {n_missing} > threshold {_MISSING_FROM_CLOSES_THRESHOLD} "
                f"missing-from-closes after prune. SF would halt MorningEnrich."
            ),
            details={"missing": missing[:20], "threshold": _MISSING_FROM_CLOSES_THRESHOLD},
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="predicted_missing_from_closes",
        status="ok",
        message=(
            f"Predicted {n_missing} missing (under {_MISSING_FROM_CLOSES_THRESHOLD} threshold) "
            f"— WARN-only path"
        ),
        details={"missing": missing, "threshold": _MISSING_FROM_CLOSES_THRESHOLD},
        elapsed_seconds=time.time() - t0,
    )


def check_backfill_source_freshness(ctx: PreflightContext) -> CheckResult:
    """Catches PR #130 class: backfill regression preflight failure.

    Reads SPY's last_date from ArcticDB macro and the staging/daily_closes
    parquet date. If staging exists for the prior trading day, backfill's
    delta-merge will land at that date. Predict whether ArcticDB SPY
    last_date <= effective backfill source last_date (no regression).
    """
    import time
    import io
    import boto3
    import pandas as pd

    t0 = time.time()
    s3 = boto3.client("s3")

    # ArcticDB SPY last_date — reuse macro_lib from check_arctic_connectivity.
    if ctx.macro_lib is None:
        return CheckResult(
            name="backfill_source_freshness",
            status="fail",
            message="Skipped: arctic_connectivity did not populate macro_lib",
            elapsed_seconds=time.time() - t0,
        )
    try:
        spy_df = ctx.macro_lib.tail("SPY", n=1).data
        arctic_spy_last = pd.Timestamp(spy_df.index[-1]).normalize() if not spy_df.empty else None
    except Exception as exc:
        return CheckResult(
            name="backfill_source_freshness",
            status="fail",
            message=f"ArcticDB SPY read raised: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    if arctic_spy_last is None:
        return CheckResult(
            name="backfill_source_freshness",
            status="fail",
            message="ArcticDB SPY is empty",
            elapsed_seconds=time.time() - t0,
        )

    # Backfill source = price_cache + daily_closes delta. Effective last
    # is max(price_cache_last, daily_closes_last). Read SPY parquet.
    #
    # Wave-3 reader migration (ROADMAP L1401): try the new
    # ``reference/price_cache/`` prefix first, fall back to legacy
    # ``predictor/price_cache/`` during the producer write-both soak
    # (PR1 #270 shipped 2026-05-19; soak ≥1 week to ~2026-05-26).
    from builders._price_cache_writeboth import price_cache_read_prefixes

    df = None
    last_exc: Exception | None = None
    for prefix in price_cache_read_prefixes():
        try:
            obj = s3.get_object(Bucket=ctx.bucket, Key=f"{prefix}SPY.parquet")
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            break
        except Exception as exc:
            last_exc = exc
    if df is None:
        return CheckResult(
            name="backfill_source_freshness",
            status="fail",
            message=f"price_cache SPY read raised (both prefixes): {last_exc}",
            elapsed_seconds=time.time() - t0,
        )
    cache_last = pd.Timestamp(df.index[-1]).normalize()

    # Daily delta — staging/daily_closes/{prior_trading_day}.parquet.
    try:
        obj = s3.get_object(
            Bucket=ctx.bucket,
            Key=f"staging/daily_closes/{ctx.prior_trading_day}.parquet",
        )
        delta_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        delta_last = pd.Timestamp(ctx.prior_trading_day).normalize() if "SPY" in delta_df.index else None
    except Exception:
        delta_last = None

    effective_last = cache_last
    if delta_last is not None and delta_last > effective_last:
        effective_last = delta_last

    details = {
        "arctic_spy_last": arctic_spy_last.date().isoformat(),
        "cache_spy_last": cache_last.date().isoformat(),
        "delta_spy_last": delta_last.date().isoformat() if delta_last else None,
        "effective_backfill_source_last": effective_last.date().isoformat(),
    }

    if effective_last < arctic_spy_last:
        return CheckResult(
            name="backfill_source_freshness",
            status="fail",
            message=(
                f"Backfill regression preflight (PR #130) would fail: "
                f"source last={effective_last.date()} < arctic last={arctic_spy_last.date()}"
            ),
            details=details,
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="backfill_source_freshness",
        status="ok",
        message=f"Backfill source ({effective_last.date()}) ≥ arctic ({arctic_spy_last.date()})",
        details=details,
        elapsed_seconds=time.time() - t0,
    )


def check_postflight_contracts(ctx: PreflightContext) -> CheckResult:
    """Verify the S3 contract files postflight (validators/postflight.py)
    will read are present + parseable. Catches latest_weekly.json /
    constituents.json / macro.json / short_interest.json drift before SF
    fires the actual postflight.
    """
    import time
    import boto3
    t0 = time.time()
    s3 = boto3.client("s3")
    issues: list[str] = []

    def _read(key: str) -> "dict | None":
        try:
            obj = s3.get_object(Bucket=ctx.bucket, Key=key)
            return json.loads(obj["Body"].read())
        except Exception as exc:
            issues.append(f"{key}: {exc}")
            return None

    pointer = _read("market_data/latest_weekly.json")
    if pointer:
        ptr_date = pointer.get("date")
        if not ptr_date:
            issues.append("latest_weekly.json missing 'date'")
        else:
            # Each weekly artifact is checked at the pointer's date prefix.
            prefix = pointer.get("s3_prefix", f"market_data/weekly/{ptr_date}/").rstrip("/")
            cons = _read(f"{prefix}/constituents.json")
            if cons:
                if len(cons.get("tickers") or []) < 800:
                    issues.append(f"constituents.json tickers {len(cons.get('tickers') or [])} < 800")
                if not isinstance(cons.get("sector_map"), dict):
                    issues.append("constituents.json missing sector_map dict")
            macro = _read(f"{prefix}/macro.json")
            if macro and macro.get("fed_funds_rate") is None:
                issues.append("macro.json missing fed_funds_rate")

    if issues:
        return CheckResult(
            name="postflight_contracts",
            status="warn",
            message=(
                f"{len(issues)} contract issues; postflight may still pass if Phase 1 "
                f"rewrites these mid-run, but flagging for visibility"
            ),
            details={"issues": issues[:10]},
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="postflight_contracts",
        status="ok",
        message="All postflight contract files present + parseable",
        elapsed_seconds=time.time() - t0,
    )


# ── Tool-contract check (check_tool_contracts) ─────────────────────────────────


# Declared flag→minimum-version table. A stale entry fails safe (under-constrains,
# never falsely blocks). Add rows as new flags ship.
_FLAG_MINIMUM_VERSIONS: dict[str, dict[str, str]] = {
    "krepis": {
        "--correlation-id": "0.18.8",
    },
}


def _parse_checkout_repo(parts: list[str]) -> str | None:
    """Given the split argv parts of a ``commands.$`` value, extract the
    checkout-name that identifies the governing repo.

    The expected shape is::

        /home/ec2-user/<checkout>/.venv/bin/python -m <module> <subcommand> <flags...>

    Returns the checkout name (e.g. ``alpha-engine-dashboard``) or None if
    the pattern doesn't match.
    """
    if len(parts) < 2:
        return None
    python_path = parts[0]
    if not python_path.endswith("/.venv/bin/python"):
        return None
    # /home/ec2-user/alpha-engine-dashboard/.venv/bin/python → alpha-engine-dashboard
    segments = python_path.split("/")
    if len(segments) < 5:
        return None
    return segments[-4]


def _resolve_governing_repo(checkout: str) -> str | None:
    """Map a checkout directory name to the sibling repo name used by
    ``_sibling_repo()``.

    The checkout name is typically ``alpha-engine-<name>`` while the sibling
    repo recognized by _sibling_repo is ``alpha-engine-<name>`` (same
    convention).
    """
    # Direct 1:1 mapping — checkout name == repo name.
    # If a newer convention diverges, expand this table.
    known = {
        "alpha-engine-dashboard": "alpha-engine-dashboard",
        "alpha-engine-data": "alpha-engine-data",
        "alpha-engine-predictor": "alpha-engine-predictor",
        "alpha-engine-research": "alpha-engine-research",
        "alpha-engine-backtester": "alpha-engine-backtester",
        "alpha-engine-evaluator": "alpha-engine-evaluator",
    }
    return known.get(checkout)


def _read_pinned_version(requirements_path) -> str | None:
    """Read a package version from a requirements file.

    Supports ``pinned==X.Y.Z`` lines. Returns the version string or None.
    """
    import re
    try:
        text = requirements_path.read_text()
    except (OSError, IOError):
        return None
    pat = re.compile(r"^krepis==(\S+)", re.MULTILINE)
    m = pat.search(text)
    if m:
        return m.group(1)
    # Also support ``krepis>=X.Y.Z`` or unpinned lines
    pat2 = re.compile(r"^krepis\s*(>=|~=|==)\s*(\S+)", re.MULTILINE)
    m2 = pat2.search(text)
    return m2.group(2) if m2 else None


def _parse_version(version_str: str) -> tuple[int, ...]:
    """Parse a dotted version string into a comparable tuple.

    Handles ``X.Y.Z`` and ``X.Y.Z.devN`` (dev suffix sorts before release).
    Strips pre-release / build-metadata suffixes (e.g. ``1.0.0rc1`` → ``1.0.0``).
    """
    import re
    # Take only up to .dev so dev == the release version for comparison
    base = version_str.split(".dev")[0]
    # Extract just the X.Y.Z numeric parts, dropping rc/alpha/beta suffixes
    m = re.match(r"(\d+(?:\.\d+)*)", base)
    if m:
        try:
            return tuple(int(p) for p in m.group(1).split("."))
        except (ValueError, TypeError):
            return (0,)
    return (0,)


def _check_version_meets(installed: str, required: str) -> bool:
    """True if ``installed >= required`` (semver comparison)."""
    return _parse_version(installed) >= _parse_version(required)


def check_tool_contracts(ctx: PreflightContext) -> CheckResult:
    """Every CLI a stage shells out to accepts the arguments the definition passes.

    Parses ``commands.$`` values in the SF definition for the shape
    ``<checkout>/.venv/bin/python -m <module> <subcommand> <flags...>``,
    resolves the checkout to its governing repo's requirements file, and
    asserts the pinned version satisfies every flag the SF actually emits
    (via the ``_FLAG_MINIMUM_VERSIONS`` table).

    Static analysis — zero AWS API calls, zero network calls. Runs in <1s.
    """
    import time
    import json as _json
    from pathlib import Path

    import boto3 as _boto3

    t0 = time.time()

    sfn = _boto3.client("stepfunctions")
    try:
        live = _json.loads(
            sfn.describe_state_machine(stateMachineArn=_WEEKLY_SF_ARN)["definition"]
        )
    except Exception as exc:  # noqa: BLE001 — surfaced as fail, not swallowed
        return CheckResult(
            name="tool_contracts",
            status="fail",
            message=f"could not read the weekly SF definition: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    failures: list[str] = []
    checked = 0
    seen_repos: set[str] = set()

    def _walk_commands(node) -> None:
        nonlocal checked
        if isinstance(node, dict):
            for key, val in node.items():
                if key == "commands.$" and isinstance(val, str):
                    parts = val.split()
                    checkout = _parse_checkout_repo(parts)
                    if checkout is None:
                        continue
                    checked += 1
                    repo_name = _resolve_governing_repo(checkout)
                    if repo_name is None:
                        failures.append(
                            f"{checkout}: unknown checkout — add to _resolve_governing_repo "
                            f"table or the governing repo is not tracked"
                        )
                        continue
                    seen_repos.add(repo_name)
                    repo_path = _sibling_repo(repo_name)
                    if repo_path is None:
                        failures.append(
                            f"{checkout} → {repo_name}: not checked out as sibling — "
                            f"cannot verify tool contracts"
                        )
                        continue
                    # Check each flag from the command against the version table
                    for i, p in enumerate(parts):
                        if p in _FLAG_MINIMUM_VERSIONS.get("krepis", {}):
                            req_ver = _FLAG_MINIMUM_VERSIONS["krepis"][p]
                            # Find requirements.txt / pyproject.toml
                            req_txt = repo_path / "requirements.txt"
                            req_in = repo_path / "requirements" / "common.in"
                            pinned = None
                            if req_txt.is_file():
                                pinned = _read_pinned_version(req_txt)
                            if pinned is None and req_in.is_file():
                                pinned = _read_pinned_version(req_in)
                            if pinned is None:
                                # Try pyproject.toml
                                pyproj = repo_path / "pyproject.toml"
                                if pyproj.is_file():
                                    import re as _re
                                    # Simple regex for krepis version in pyproject
                                    src = pyproj.read_text()
                                    m = _re.search(
                                        r'krepis[\\s\"]*[>=~=]+\\s*([\\w.]+)',
                                        src,
                                    )
                                    if m:
                                        pinned = m.group(1)
                            if pinned is None:
                                failures.append(
                                    f"{checkout}: flag {p} requires krepis ≥{req_ver} "
                                    f"but pin not found in {repo_name}"
                                )
                                continue
                            if not _check_version_meets(pinned, req_ver):
                                failures.append(
                                    f"{checkout} ({repo_name}): krepis=={pinned} too old "
                                    f"for flag {p} (needs ≥{req_ver}) — the SF will pass "
                                    f"a flag {parts[i]} this box's venv cannot parse"
                                )
                else:
                    _walk_commands(val)
        elif isinstance(node, list):
            for item in node:
                _walk_commands(item)

    # Walk the entire definition for commands.$ — covers all states,
    # including those inside Parallel/Map branches.
    _walk_commands(live)

    # Also check the known repos have their files to avoid silent skip.
    # This makes the check fail loudly when a requirements file is moved
    # or renamed without updating this check.
    for repo_name in seen_repos:
        repo_path = _sibling_repo(repo_name)
        if repo_path is None:
            continue
        req_txt = repo_path / "requirements.txt"
        req_in = repo_path / "requirements" / "common.in"
        pyproj = repo_path / "pyproject.toml"
        if not req_txt.is_file() and not req_in.is_file() and not pyproj.is_file():
            failures.append(
                f"{repo_name}: no requirements.txt, requirements/common.in, "
                f"or pyproject.toml found — cannot verify pins"
            )

    elapsed = time.time() - t0
    if failures:
        return CheckResult(
            name="tool_contracts",
            status="fail",
            message=f"{len(failures)} tool-contract violation(s)",
            details={"failures": failures, "checked": checked},
            elapsed_seconds=elapsed,
        )
    return CheckResult(
        name="tool_contracts",
        status="ok",
        message=f"{checked} command(s) checked; all flags match pinned versions",
        details={"checked": checked, "seen_repos": list(seen_repos)},
        elapsed_seconds=elapsed,
    )


# ── Definition-input coherence check (check_definition_input_coherence) ──────


# The set of built-in JSONPath variables the SF context provides.
_BUILTIN_CONTEXT_VARS = {
    "$$.Execution.Id",
    "$$.Execution.Input",
    "$$.Execution.Name",
    "$$.Execution.RoleArn",
    "$$.Execution.StartTime",
    "$$.State.Name",
    "$$.State.EnteredTime",
    "$$.State.RetryCount",
    "$$.Execution.PreviousEventId",
}

# States that don't need JSONPath checking (they reference paths set by
# the execution input itself, not by prior states).
_SELF_DESCRIBING_KEYS = {
    "Comment", "Type", "Resource", "End",
    "TimeoutSeconds", "HeartbeatSeconds",
    "ResultPath", "ResultSelector", "Parameters",
    "Retry", "Catch",
}


def _collect_jsonpath_refs(node, path: str = "") -> list[tuple[str, str]]:
    """Collect every JSONPath reference (``.$``-suffixed key or JSONPath in
    a string value) from the SF definition.

    Returns ``[(jsonpath_string, context_path)]`` pairs.
    """
    refs: list[tuple[str, str]] = []
    if isinstance(node, dict):
        for key, val in node.items():
            child_path = f"{path}.{key}" if path else key
            if key.endswith(".$") and isinstance(val, str) and val.startswith("$"):
                refs.append((val, child_path))
            elif key.endswith(".$") and isinstance(val, str):
                # Non-JSONPath .$ reference (e.g. literal template)
                pass
            elif isinstance(val, (dict, list)):
                refs.extend(_collect_jsonpath_refs(val, child_path))
    elif isinstance(node, list):
        for i, item in enumerate(node):
            child_path = f"{path}[{i}]"
            if isinstance(item, (dict, list)):
                refs.extend(_collect_jsonpath_refs(item, child_path))
    return refs


def _build_proposed_input(skip_flags: dict[str, bool] | None = None) -> dict:
    """Build a proposed execution input that exercises all code paths.

    Includes realistic values for every known path the SF definition
    references, plus each skip_* flag set to False by default (so the
    JSONPath behind the skip gate is reachable). Takes an optional override
    dict for testing specific skip_* combinations.
    """
    base = {
        "pipeline_role": "weekly",
        "run_date": "2026-07-28",
        "sns_topic_arn": "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
        "ec2_instance_id": "i-0123456789abcdef0",
        "mode": "",
        "research_dry": False,
        "data_phase2_dry": False,
        "regime_action": "produce",
        "shell_run": False,
        "preflight_args": "",
        "skip_weekly_run_day_gate": False,
        "skip_lib_pin_drift_check": False,
        "skip_morning_enrich": False,
        "skip_data_phase1": False,
        "skip_scanner": False,
        "skip_signals_envelope": False,
        "skip_challenger_shadow": False,
        "skip_rag_ingestion": False,
        "skip_thinktank_coverage": False,
        "skip_regime_substrate": False,
        "skip_regime_retrospective_eval": False,
        "skip_research": False,
        "skip_data_phase2": False,
        "skip_eval_judge": False,
        "skip_eval_rolling_mean": False,
        "skip_rationale_clustering": False,
        "skip_replay_concordance": False,
        "skip_counterfactual": False,
        "skip_aggregate_costs": False,
        "skip_predictor_training": False,
        "skip_predictor_backtest": False,
        "skip_portfolio_optimizer_backtest": False,
        "skip_parity": False,
        "skip_post_eval": False,
    }
    if skip_flags:
        base.update(skip_flags)
    return base


def check_definition_input_coherence(ctx: PreflightContext) -> CheckResult:
    """Every JSONPath referenced by a SF state resolves against a proposed
    execution input, including under each skip_* combination.

    Walks the live SF definition, collects every ``.$``-suffixed JSONPath
    reference, and validates each against the proposed input using jmespath.
    Also tests each individual skip_* flag set to True (the input shape
    changes under skip gates — a path that resolves with skip=false may not
    resolve with skip=true if a prior state was bypassed).

    Static analysis — zero network calls, runs in <1s.
    """
    import time
    import json as _json
    import jmespath

    import boto3 as _boto3

    t0 = time.time()

    sfn = _boto3.client("stepfunctions")
    try:
        live = _json.loads(
            sfn.describe_state_machine(stateMachineArn=_WEEKLY_SF_ARN)["definition"]
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="definition_input_coherence",
            status="fail",
            message=f"could not read the weekly SF definition: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    refs = _collect_jsonpath_refs(live)
    builtin_only = {r for r, _ in refs if r.startswith("$$")}
    input_refs = [r for r, _ in refs if r.startswith("$.") and r not in builtin_only]

    if not input_refs:
        return CheckResult(
            name="definition_input_coherence",
            status="ok",
            message="No input JSONPath refs found to validate",
            elapsed_seconds=time.time() - t0,
        )

    # Build the base proposed input (all skip flags false).
    base_input = _build_proposed_input()

    failures: list[str] = []
    checked = 0

    # Resolve each JSONPath against the base input.
    for ref in input_refs:
        checked += 1
        # Strip the leading "$" for jmespath (it uses bare paths)
        jmes_expr = ref.lstrip("$").lstrip(".")
        try:
            result = jmespath.search(jmes_expr, base_input)
        except Exception as exc:
            failures.append(
                f"{ref}: jmespath resolve raised: {exc}"
            )
            continue
        if result is None:
            failures.append(
                f"{ref}: resolved to None against the proposed base input — "
                f"would throw States.Runtime on execution"
            )

    # Also test each skip_* flag individually set to True, since a
    # bypassed state may omit a result path that a downstream state needs.
    skip_flags = [k for k in base_input if k.startswith("skip_")]
    for skip_flag in skip_flags:
        skip_input = _build_proposed_input({skip_flag: True})
        for ref in input_refs:
            jmes_expr = ref.lstrip("$").lstrip(".")
            try:
                result = jmespath.search(jmes_expr, skip_input)
            except Exception:
                # jmespath exception is itself a finding — the execution
                # would fail with a different error at runtime.
                failures.append(
                    f"{ref}: raised under {skip_flag}=true — "
                    f"this skip combination leaves a JSONPath unresolvable"
                )

    elapsed = time.time() - t0
    if failures:
        return CheckResult(
            name="definition_input_coherence",
            status="fail",
            message=f"{len(failures)} JSONPath reference(s) unresolvable",
            details={
                "failures": failures,
                "checked": checked,
                "skip_combinations_tested": len(skip_flags),
            },
            elapsed_seconds=elapsed,
        )
    return CheckResult(
        name="definition_input_coherence",
        status="ok",
        message=(
            f"All {checked} input JSONPath reference(s) resolve against base input "
            f"and {len(skip_flags)} individual skip-flag combinations"
        ),
        details={"checked": checked, "skip_flags_tested": len(skip_flags)},
        elapsed_seconds=elapsed,
    )


# ── Lambda memory headroom check (check_lambda_memory_headroom) ──────────────


_MEMORY_HEADROOM_MARGIN = 128  # MB above observed max — the stated safety buffer
_CLOUDWATCH_METRIC_DAYS = 30  # lookback window
_CLOUDWATCH_PERIOD = 86400  # 1-day period (granular enough for a weekly pipeline)


def check_lambda_memory_headroom(ctx: PreflightContext) -> CheckResult:
    """Each SF-invoked Lambda's alias-resolved memory exceeds its observed
    30d ``maxMemoryUsed`` high-water mark by a stated margin.

    Resolves the **alias** (not ``$LATEST``) using GetFunctionConfiguration,
    then queries CloudWatch ``maxMemoryUsed`` for the last 30 days per Lambda.
    A Lambda whose alias memory <= observed max + margin is at risk of OOM.

    The 2026-07-25 ``DataPhase2 Runtime.OutOfMemory`` was caused precisely
    because ``$LATEST`` was 1024 MB while ``live`` served a 512 MB version.
    A check reading ``$LATEST`` would have reported healthy.

    Read-only API calls — costs <$0.01 per run.
    """
    import time
    import json as _json

    import boto3 as _boto3

    t0 = time.time()

    sfn = _boto3.client("stepfunctions")
    lam = _boto3.client("lambda")
    cw = _boto3.client("cloudwatch")

    try:
        live = _json.loads(
            sfn.describe_state_machine(stateMachineArn=_WEEKLY_SF_ARN)["definition"]
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="lambda_memory_headroom",
            status="fail",
            message=f"could not read the weekly SF definition: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    function_names = sorted(_walk_invoked_lambdas(live))
    failures: list[str] = []
    warnings: list[str] = []
    passed: list[dict] = []
    checked = 0

    from datetime import datetime as _dt, timedelta as _td, timezone as _tz

    now = _dt.now(_tz.utc)
    start_time = now - _td(days=_CLOUDWATCH_METRIC_DAYS)

    for fn in function_names:
        checked += 1
        # 1. Resolve the alias-qualified configuration.
        alias_memory: int | None = None
        alias_name: str | None = None

        # Check if the function name has an alias qualifier (e.g., "fn:live")
        if ":" in fn:
            base_fn, qualifier = fn.rsplit(":", 1)
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=base_fn, Qualifier=qualifier
                )
                alias_memory = cfg.get("MemorySize")
                alias_name = qualifier
            except Exception as exc:  # noqa: BLE001
                warnings.append(
                    f"{fn}: could not resolve alias config ({exc}) — "
                    f"reading $LATEST as fallback (may miss alias/OOM mismatch)"
                )
                try:
                    cfg = lam.get_function_configuration(FunctionName=fn)
                    alias_memory = cfg.get("MemorySize")
                except Exception as exc2:  # noqa: BLE001
                    failures.append(f"{fn}: could not read config at all: {exc2}")
                    continue
        else:
            # Bare function name — no alias, use as-is.
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
                alias_memory = cfg.get("MemorySize")
            except Exception as exc:
                failures.append(f"{fn}: could not read config: {exc}")
                continue

        if alias_memory is None:
            warnings.append(f"{fn}: no MemorySize in config")
            continue

        # 2. Query CloudWatch maxMemoryUsed over the lookback window.
        try:
            metric = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="maxMemoryUsed",
                Dimensions=[
                    {"Name": "FunctionName", "Value": fn.split(":")[0]},
                ],
                StartTime=start_time,
                EndTime=now,
                Period=_CLOUDWATCH_PERIOD,
                Statistics=["Maximum"],
            )
        except Exception as exc:
            warnings.append(
                f"{fn}: could not query maxMemoryUsed ({exc}) — "
                f"cannot verify headroom without metrics"
            )
            continue

        observed_max = 0
        if metric.get("Datapoints"):
            observed_max = int(max(
                dp["Maximum"] for dp in metric["Datapoints"]
            ))

        headroom = alias_memory - observed_max
        entry = {
            "function": fn,
            "alias": alias_name or "$LATEST",
            "alias_memory_mb": alias_memory,
            "observed_max_memory_mb": observed_max,
            "headroom_mb": headroom,
            "margin_mb": _MEMORY_HEADROOM_MARGIN,
        }

        if headroom < _MEMORY_HEADROOM_MARGIN:
            failures.append(
                f"{fn} (alias={alias_name or '$LATEST'}): "
                f"{alias_memory} MB configured, {observed_max} MB observed max — "
                f"headroom {headroom} MB < {_MEMORY_HEADROOM_MARGIN} MB margin — "
                f"OOM risk"
            )
        else:
            passed.append(entry)

    elapsed = time.time() - t0
    if failures:
        return CheckResult(
            name="lambda_memory_headroom",
            status="fail",
            message=(
                f"{len(failures)} Lambda(s) below headroom margin "
                f"({_MEMORY_HEADROOM_MARGIN} MB)"
            ),
            details={
                "failures": failures,
                "warnings": warnings,
                "passed": len(passed),
                "checked": checked,
            },
            elapsed_seconds=elapsed,
        )

    return CheckResult(
        name="lambda_memory_headroom",
        status="ok",
        message=(
            f"All {checked} Lambda(s) above {_MEMORY_HEADROOM_MARGIN} MB headroom margin "
            f"({len(warnings)} warning(s))"
        ),
        details={
            "passed": passed,
            "warnings": warnings,
            "checked": checked,
        },
        elapsed_seconds=elapsed,
    )


# ── Orchestrator ──────────────────────────────────────────────────────────────


# ── Research-side static checks (added 2026-05-02 after the cost-telemetry
# + recursion-budget incidents) ───────────────────────────────────────────────


def _sibling_repo(name: str) -> "Path | None":
    """Resolve a sibling clone of an alpha-engine-* repo from this file's
    location. Returns None if the sibling isn't checked out — checks that
    depend on it then SKIP rather than fail (operator may be running the
    preflight in an environment without sibling clones)."""
    from pathlib import Path
    here = Path(__file__).resolve().parent  # alpha-engine-data
    candidate = here.parent / name
    return candidate if candidate.is_dir() else None


_ANTHROPIC_SNAPSHOT_RE = __import__("re").compile(r"-\d{8}$")


def _normalize_model_for_pricing(model_name: str) -> str:
    """Strip Anthropic ``-YYYYMMDD`` snapshot suffix. Mirrors the function
    in ``alpha-engine-research/graph/llm_cost_tracker.py`` (PR #77). Kept
    here as a static copy so the preflight doesn't need to import the
    research module (which transitively pulls in heavy deps)."""
    return _ANTHROPIC_SNAPSHOT_RE.sub("", model_name)


def check_price_cards_cover_all_models(ctx: PreflightContext) -> CheckResult:
    """Catches the 2026-05-02 PR #77 class: cost-telemetry hard-fail when
    a runtime model name (often a snapshot ID like ``claude-haiku-4-5-
    20251001``) doesn't normalize to any price card.

    Walks every model name referenced by alpha-engine-research's runtime
    config + hardcoded fallbacks, normalizes via the same logic the
    Lambda uses (snapshot-suffix strip), and asserts each maps to a
    card in alpha-engine-config/cost/model_pricing.yaml.

    Pure file I/O, zero LLM cost. Skips if sibling repos aren't checked
    out (CI / restricted environments)."""
    import time
    import yaml as _yaml
    from pathlib import Path
    t0 = time.time()

    config_repo = _sibling_repo("alpha-engine-config")
    research_repo = _sibling_repo("alpha-engine-research")
    if config_repo is None or research_repo is None:
        return CheckResult(
            name="price_cards_cover_all_models",
            status="warn",
            message=(
                f"Sibling repos not checked out (config={config_repo is not None}, "
                f"research={research_repo is not None}) — skipped."
            ),
            elapsed_seconds=time.time() - t0,
        )

    pricing_path = config_repo / "cost" / "model_pricing.yaml"
    if not pricing_path.is_file():
        return CheckResult(
            name="price_cards_cover_all_models",
            status="fail",
            message=f"Missing {pricing_path}",
            elapsed_seconds=time.time() - t0,
        )
    pricing = _yaml.safe_load(pricing_path.read_text())
    card_names = {c["model_name"] for c in pricing.get("cards", [])}

    universe_path = config_repo / "research" / "universe.yaml"
    runtime_models: dict[str, str] = {}
    if universe_path.is_file():
        universe = _yaml.safe_load(universe_path.read_text()) or {}
        sector_cfg = universe.get("sector_teams") or {}
        for k in ("per_stock_model", "strategic_model"):
            v = sector_cfg.get(k) or universe.get(k)
            if v:
                runtime_models[f"sector_teams.{k}"] = v

    # Also scan research_graph.py's hardcoded fallback dict — these names
    # are used when track_llm_cost wiring is incomplete.
    rg_path = research_repo / "graph" / "research_graph.py"
    if rg_path.is_file():
        src = rg_path.read_text()
        # Parse _FALLBACK_AGENT_MODEL_NAMES dict literal — small enough that
        # a regex is fine (vs full AST). Tolerates whitespace + quote style.
        import re as _re
        block = _re.search(
            r"_FALLBACK_AGENT_MODEL_NAMES[^=]*=\s*\{(.*?)\}",
            src, _re.DOTALL,
        )
        if block:
            for m in _re.finditer(r'"([^"]+)"\s*:\s*"([^"]+)"', block.group(1)):
                runtime_models[f"_FALLBACK_AGENT_MODEL_NAMES[{m.group(1)}]"] = m.group(2)

    if not runtime_models:
        return CheckResult(
            name="price_cards_cover_all_models",
            status="warn",
            message="No runtime model names discovered — schema drift in research config?",
            elapsed_seconds=time.time() - t0,
        )

    misses: list[str] = []
    for source, model_name in runtime_models.items():
        normalized = _normalize_model_for_pricing(model_name)
        if normalized not in card_names:
            misses.append(f"{source}={model_name!r} (normalized={normalized!r})")

    if misses:
        return CheckResult(
            name="price_cards_cover_all_models",
            status="fail",
            message=(
                f"{len(misses)} runtime model(s) have no matching price card — "
                f"recompute_cost would raise PriceCardLookupError on the SF run"
            ),
            details={
                "missing": misses,
                "available_cards": sorted(card_names),
            },
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="price_cards_cover_all_models",
        status="ok",
        message=(
            f"All {len(runtime_models)} runtime model(s) map to price cards "
            f"(after snapshot-suffix normalization)"
        ),
        details={"runtime_models": runtime_models},
        elapsed_seconds=time.time() - t0,
    )


def check_recursion_budget_for_response_format(ctx: PreflightContext) -> CheckResult:
    """Catches the 2026-05-02 PR #78 class: ReAct agents using
    ``response_format=...`` need ``recursion_limit > MAX_ITERATIONS * 2``
    because the post-loop structured-extraction call counts against the
    same budget.

    Static scan of the analyst modules; no imports, no LLM. Asserts that
    every file using ``response_format=`` in ``create_react_agent`` also
    sets ``recursion_limit`` with a ``+ 2`` buffer (or higher). The bare
    ``MAX_ITERATIONS * 2`` formula crashes the SF on the structured-output
    extraction call."""
    import time
    import re as _re
    from pathlib import Path
    t0 = time.time()

    research_repo = _sibling_repo("alpha-engine-research")
    if research_repo is None:
        return CheckResult(
            name="recursion_budget_for_response_format",
            status="warn",
            message="alpha-engine-research sibling not checked out — skipped.",
            elapsed_seconds=time.time() - t0,
        )

    targets = [
        research_repo / "agents" / "sector_teams" / "quant_analyst.py",
        research_repo / "agents" / "sector_teams" / "qual_analyst.py",
    ]
    issues: list[str] = []
    checked: list[str] = []

    for path in targets:
        if not path.is_file():
            issues.append(f"{path.name} missing")
            continue
        src = path.read_text()
        uses_response_format = "response_format=" in src
        if not uses_response_format:
            checked.append(f"{path.name}: no response_format — skipped")
            continue
        # Look for any recursion_limit assignment that's NOT bare ``× 2``.
        # Acceptable shapes: ``MAX_ITERATIONS * 2 + 2``, ``MAX_ITERATIONS * 2 + N``,
        # explicit numeric ≥ 18, or a named constant we can resolve.
        bare_x2 = _re.search(
            r"recursion_limit[\"']?\s*:\s*\w+_MAX_ITERATIONS\s*\*\s*2(?!\s*\+)",
            src,
        )
        if bare_x2:
            issues.append(
                f"{path.name}: uses response_format= but recursion_limit is "
                f"bare MAX_ITERATIONS * 2 (no +N buffer) — SF will halt on "
                f"the structured-extraction call"
            )
            continue
        checked.append(f"{path.name}: response_format + buffered recursion_limit ✓")

    if issues:
        return CheckResult(
            name="recursion_budget_for_response_format",
            status="fail",
            message=f"{len(issues)} ReAct site(s) at risk of GraphRecursionError",
            details={"issues": issues, "checked": checked},
            elapsed_seconds=time.time() - t0,
        )

    return CheckResult(
        name="recursion_budget_for_response_format",
        status="ok",
        message=f"All {len(targets)} ReAct site(s) have buffered recursion_limit",
        details={"checked": checked},
        elapsed_seconds=time.time() - t0,
    )


def _walk_invoked_lambdas(node: Any, found: "set[str] | None" = None) -> "set[str]":
    """Collect every FunctionName the SF definition invokes, at any nesting.

    Walks Parallel branches and Map iterators too, so a Lambda added inside one
    is covered without touching this function. Alias/version qualifiers are
    kept as-is; the caller strips them when building the ARN to simulate.
    """
    if found is None:
        found = set()
    if isinstance(node, dict):
        for key, val in node.items():
            if key == "FunctionName" and isinstance(val, str):
                found.add(val)
            else:
                _walk_invoked_lambdas(val, found)
    elif isinstance(node, list):
        for item in node:
            _walk_invoked_lambdas(item, found)
    return found


def _lambda_base_name(function_name: str) -> str:
    """Unqualified function name from either a bare name or a full ARN.

    SF states reference Lambdas both ways — some `Parameters.FunctionName` are
    bare (`alpha-engine-scanner:live`), others are full ARNs. Both may carry an
    alias/version qualifier, which must be stripped before building the ARN to
    simulate, since grants are written against `function:<name>*`.

    Naively splitting on ":" yields "arn" for the ARN form, which simulates
    against a nonexistent function and reports a false denial. That is exactly
    what this helper exists to prevent (caught 2026-07-27 while validating this
    check against live AWS — the check's first run "failed" a role that had the
    grant all along).
    """
    if function_name.startswith("arn:aws:lambda:"):
        # arn:aws:lambda:<region>:<account>:function:<name>[:<qualifier>]
        parts = function_name.split(":")
        return parts[6] if len(parts) > 6 else function_name
    return function_name.split(":")[0]


def _simulate(
    iam: Any,
    role_arn: str,
    action: str,
    resource_arn: str,
    context_entries: "list[dict] | None" = None,
) -> bool:
    """True iff `role_arn` is allowed `action` on `resource_arn`.

    Any error simulating is treated as NOT allowed. A preflight that cannot
    evaluate a gate must not report the gate as passing — "could not check" and
    "no problem" are different answers, and conflating them is the silent
    degradation the weekly-SF policy forbids.
    """
    try:
        resp = iam.simulate_principal_policy(
            PolicySourceArn=role_arn,
            ActionNames=[action],
            ResourceArns=[resource_arn],
            ContextEntries=context_entries or [],
        )
    except Exception:  # noqa: BLE001 — see docstring: cannot-check != allowed
        return False
    return all(r["EvalDecision"] == "allowed" for r in resp["EvaluationResults"])


def check_sf_iam_reachability(ctx: PreflightContext) -> CheckResult:
    """Every identity the weekly SF uses can actually reach what it targets.

    Bug class this catches (three live instances on 2026-07-27 alone):

    * ``alpha-engine-evaluator-role`` lacked ``s3:GetObject`` on
      ``reference/*``. The codified policy granted it; nobody had applied it.
      Every weekly ReportCard degraded, and it surfaced only as a stack trace
      inside a "successful" run.
    * ``alpha-engine-step-functions-role`` could not invoke the new
      weekly-freshness spot dispatcher until its grant was applied — the SF
      would have 404'd at ``DispatchWeeklyFreshnessSpot``.
    * ``alpha-engine-substrate-health-gate-role`` could not ``ssm:SendCommand``
      to the per-execution spot box, because its grant still enumerated two
      static instance ARNs. Caught by a rehearsal; would otherwise have failed
      at 02:00 Saturday.

    All three were knowable in seconds via ``iam:SimulatePrincipalPolicy``, and
    none were knowable from the repo alone — the codified policy and the live
    role disagreed, or the live resource did not exist yet.

    What this asserts, derived from the SF definition itself (so a newly-added
    state is covered without editing this check):

    1. Every ``lambda:invoke`` target EXISTS live and the SF execution role is
       allowed ``lambda:InvokeFunction`` on it.
    2. The SF execution role is allowed ``ssm:SendCommand`` against the
       per-execution spot box's tag.
    3. Every additional role in ``_SSM_SENDING_ROLES`` — identities that are
       NOT the SF role but still send SSM to that same box — is likewise
       allowed. This is the sibling-role gap: a fix applied to the role
       someone was looking at, while its twin went untouched.

    Read-only: `SimulatePrincipalPolicy` and `GetFunctionConfiguration` make no
    changes and cost no spend, which is the whole point of asserting before the
    pipeline launches anything.
    """
    import time

    import boto3

    t0 = time.time()
    sfn = boto3.client("stepfunctions")
    iam = boto3.client("iam")
    lam = boto3.client("lambda")

    failures: list[str] = []
    checked = 0

    try:
        live = json.loads(
            sfn.describe_state_machine(stateMachineArn=_WEEKLY_SF_ARN)["definition"]
        )
    except Exception as exc:  # noqa: BLE001 — surfaced as a fail, not swallowed
        return CheckResult(
            name="sf_iam_reachability",
            status="fail",
            message=f"could not read the weekly SF definition: {exc}",
            elapsed_seconds=time.time() - t0,
        )

    lambda_names = sorted(_walk_invoked_lambdas(live))

    # 1. Lambda existence + SF-role invoke permission.
    for fn in lambda_names:
        checked += 1
        try:
            lam.get_function_configuration(FunctionName=fn)
        except lam.exceptions.ResourceNotFoundException:
            failures.append(
                f"{fn}: invoked by the SF but does not exist live — the state "
                f"will 404 the moment it executes"
            )
            continue
        arn = f"arn:aws:lambda:{_REGION}:{_ACCOUNT}:function:{_lambda_base_name(fn)}"
        if not _simulate(iam, _SF_ROLE_ARN, "lambda:InvokeFunction", arn):
            failures.append(
                f"{fn}: {_SF_ROLE_ARN.rsplit('/', 1)[-1]} is not allowed "
                f"lambda:InvokeFunction on it"
            )

    # 2 + 3. ssm:SendCommand against the per-execution spot box, for every
    # identity that sends to it — not just the SF role.
    spot_ctx = [
        {
            "ContextKeyName": "ssm:resourceTag/Name",
            "ContextKeyValues": [_WEEKLY_SPOT_TAG],
            "ContextKeyType": "string",
        }
    ]
    any_instance = f"arn:aws:ec2:{_REGION}:{_ACCOUNT}:instance/*"
    for role_arn in [_SF_ROLE_ARN, *_SSM_SENDING_ROLES]:
        checked += 1
        if not _simulate(iam, role_arn, "ssm:SendCommand", any_instance, spot_ctx):
            failures.append(
                f"{role_arn.rsplit('/', 1)[-1]}: not allowed ssm:SendCommand "
                f"against instances tagged Name={_WEEKLY_SPOT_TAG} — this role "
                f"sends SSM to the per-execution spot box"
            )

    elapsed = time.time() - t0
    if failures:
        return CheckResult(
            name="sf_iam_reachability",
            status="fail",
            message=f"{len(failures)} identity/target pair(s) unreachable",
            details={"failures": failures, "checked": checked},
            elapsed_seconds=elapsed,
        )
    return CheckResult(
        name="sf_iam_reachability",
        status="ok",
        message=(
            f"{checked} identity/target pair(s) reachable "
            f"({len(lambda_names)} SF-invoked Lambda(s) exist and are invokable; "
            f"{1 + len(_SSM_SENDING_ROLES)} role(s) can SendCommand to the spot box)"
        ),
        details={"lambdas": lambda_names},
        elapsed_seconds=elapsed,
    )


# ArcticDB on macOS crashes in ``Aws::S3::S3Client::S3Client`` if boto3 has
# already initialized the AWS SDK in the process — the arcticdb-bundled
# AWS SDK conflicts with the system one. Initializing arctic FIRST avoids
# this on macOS and is harmless on Linux. (Linux EC2 doesn't hit the race
# at all; this matters only for local-laptop preflight runs.)
CHECKS = [
    check_sf_iam_reachability,
    check_arctic_connectivity,
    check_constituents_fetch,
    check_universe_drift,
    check_universe_sample_freshness,
    check_polygon_grouped_coverage,
    check_predicted_missing_from_closes,
    check_backfill_source_freshness,
    check_postflight_contracts,
    check_price_cards_cover_all_models,
    check_recursion_budget_for_response_format,
    # Legs 2-4 from I4494 (WeeklyPreflight pre-spend gate):
    check_tool_contracts,
    check_definition_input_coherence,
    check_lambda_memory_headroom,
]


def _previous_trading_day_str() -> str:
    """Resolve the prior trading day. Avoids importing weekly_collector
    (which transitively imports boto3 + every collector module) so
    ArcticDB's bundled AWS SDK doesn't conflict with system boto3 — the
    conflict crashes on macOS, see CHECKS docstring.
    """
    from datetime import timedelta
    from nousergon_lib.trading_calendar import is_trading_day
    today = datetime.now(timezone.utc).date()
    candidate = today - timedelta(days=1)
    for _ in range(10):
        if is_trading_day(candidate):
            return candidate.strftime("%Y-%m-%d")
        candidate -= timedelta(days=1)
    raise RuntimeError("Could not find a trading day within the last 10 days")


def run_preflight(bucket: str = DEFAULT_BUCKET) -> tuple[int, list[CheckResult]]:
    """Execute all checks against real state. Returns (n_failures, results).

    Each check runs in its own try/except — a single check raising must
    not abort the others (we want the full picture, not first-fail-bail).
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prior = _previous_trading_day_str()
    ctx = PreflightContext(bucket=bucket, today=today, prior_trading_day=prior)

    results: list[CheckResult] = []
    for check_fn in CHECKS:
        try:
            results.append(check_fn(ctx))
        except Exception as exc:
            results.append(CheckResult(
                name=check_fn.__name__.replace("check_", ""),
                status="fail",
                message=f"Check raised: {type(exc).__name__}: {exc}",
            ))
    n_fail = sum(1 for r in results if r.status == "fail")
    return n_fail, results


# ── CLI ───────────────────────────────────────────────────────────────────────


def _format_human(results: list[CheckResult]) -> str:
    lines = ["", "=" * 70, " Saturday SF Preflight ", "=" * 70, ""]
    icons = {"ok": "[OK]  ", "warn": "[WARN]", "fail": "[FAIL]"}
    for r in results:
        lines.append(f"{icons.get(r.status, '[?]   ')} {r.name:<32} {r.message}")
        if r.status == "fail" and r.details:
            for k, v in r.details.items():
                lines.append(f"        {k}: {v}")
    n_fail = sum(1 for r in results if r.status == "fail")
    n_warn = sum(1 for r in results if r.status == "warn")
    lines.append("")
    lines.append("-" * 70)
    if n_fail == 0 and n_warn == 0:
        lines.append(" Predicted SF outcome: PASS")
    elif n_fail == 0:
        lines.append(f" Predicted SF outcome: PASS with {n_warn} warning(s)")
    else:
        lines.append(f" Predicted SF outcome: FAIL ({n_fail} failure(s), {n_warn} warning(s))")
    lines.append("=" * 70)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--json", action="store_true", help="Emit structured JSON instead of human summary")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    n_fail, results = run_preflight(bucket=args.bucket)

    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2, default=str))
    else:
        print(_format_human(results))

    return 1 if n_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
