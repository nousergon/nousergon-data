"""
sf_preflight.py — Predict whether the Saturday SF would succeed BEFORE
launching a spot.

Today's Saturday SF (alpha-engine-weekly-pipeline) is a 50-min spot run
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
        tickers, sector_map, sector_etf_map, sp500, sp400 = _fetch_constituents()
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
        from alpha_engine_lib.arcticdb import open_arctic
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
        # Trading-day staleness via alpha_engine_lib.dates — the prune
        # decision is "has this ticker missed N+ NYSE sessions since its
        # last write?", which is independent of calendar weekends/holidays.
        from alpha_engine_lib.dates import trading_days_stale
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
        from alpha_engine_lib.dates import trading_days_stale
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

    from alpha_engine_lib.secrets import get_secret
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


# ArcticDB on macOS crashes in ``Aws::S3::S3Client::S3Client`` if boto3 has
# already initialized the AWS SDK in the process — the arcticdb-bundled
# AWS SDK conflicts with the system one. Initializing arctic FIRST avoids
# this on macOS and is harmless on Linux. (Linux EC2 doesn't hit the race
# at all; this matters only for local-laptop preflight runs.)
CHECKS = [
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
]


def _previous_trading_day_str() -> str:
    """Resolve the prior trading day. Avoids importing weekly_collector
    (which transitively imports boto3 + every collector module) so
    ArcticDB's bundled AWS SDK doesn't conflict with system boto3 — the
    conflict crashes on macOS, see CHECKS docstring.
    """
    from datetime import timedelta
    from alpha_engine_lib.trading_calendar import is_trading_day
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
