"""
weekly_collector.py — Centralized weekly data collection for Alpha Engine.

Phase 1 (before research): constituents, prices, macro, universe returns.
Phase 2 (after research): alternative data for promoted tickers.

Phase 1 runs on EC2 via SSM RunCommand (price refresh takes 15-25 min).
Phase 2 runs as Lambda (< 10 min for ~30 tickers).

Usage:
    python weekly_collector.py --phase 1              # Phase 1 only
    python weekly_collector.py --phase 2              # Phase 2 only
    python weekly_collector.py                        # Phase 1 (default)
    python weekly_collector.py --phase 1 --dry-run    # validate Phase 1
    python weekly_collector.py --phase 1 --only prices # single collector
    python weekly_collector.py --phase 2 --only alternative  # explicit
    python weekly_collector.py --daily                # weekday EOD pass (yfinance OHLCV, no VWAP)
    python weekly_collector.py --daily --dry-run      # validate daily
    python weekly_collector.py --morning-enrich       # morning polygon overwrite (prior trading day)
    python weekly_collector.py --morning-enrich --date 2026-04-23  # backfill specific date
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from pathlib import Path

import boto3
import yaml

def _load_dotenv() -> None:
    """Load .env file into os.environ (lightweight, no dependency).

    Defined at module-top so it can run before setup_logging() — local-dev
    workflows put FLOW_DOCTOR_ENABLED + FLOW_DOCTOR_GITHUB_TOKEN in .env,
    and the flow-doctor handler attach reads those at import time.
    Production (Lambda/EC2) gets env from SSM/systemd before Python starts;
    .env is the local-dev fallback.
    """
    env_path = Path(".env")
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            if key and val and key not in os.environ:
                os.environ[key] = val


_load_dotenv()

# Structured logging + flow-doctor singleton via alpha-engine-lib (shared
# pattern across all 5 entrypoints; see executor/main.py for reference).
# Module-top so import-time errors in the collectors block below are also
# captured by flow-doctor's ERROR handler.
from alpha_engine_lib.logging import setup_logging, guard_entrypoint
from alpha_engine_lib.phase_registry import PhaseRegistry
_FLOW_DOCTOR_EXCLUDE_PATTERNS: list[str] = []
_FLOW_DOCTOR_YAML = str(Path(__file__).parent / "flow-doctor.yaml")
setup_logging(
    "data-collector",
    flow_doctor_yaml=_FLOW_DOCTOR_YAML,
    exclude_patterns=_FLOW_DOCTOR_EXCLUDE_PATTERNS,
)

from collectors import constituents, prices, macro, universe_returns, signal_returns, alternative, daily_closes, fundamentals, short_interest, metron_market_data
from builders._price_cache_writeboth import (
    price_cache_write_prefixes as _price_cache_write_prefixes,
)

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    """Load config.yaml, experiment-package first (config#1042).

    Search order mirrors features/feature_engineer.py::_load_feature_cfg_overrides:
    experiments/$ALPHA_ENGINE_EXPERIMENT_ID/data/config.yaml (default experiment
    ``reference``) first, then the legacy top-level alpha-engine-config/data/config.yaml,
    then the repo-local fallback (``path``). The experiment-package layer was already
    live in feature_engineer; this closes the gap that file's docstring references.
    """
    exp = os.environ.get("ALPHA_ENGINE_EXPERIMENT_ID", "reference")
    roots = [
        Path.home() / "alpha-engine-config",
        Path(__file__).parent.parent / "alpha-engine-config",
    ]
    search_paths = [r / "experiments" / exp / "data" / "config.yaml" for r in roots]
    search_paths += [r / "data" / "config.yaml" for r in roots]
    search_paths.append(Path(path))
    for p in search_paths:
        if p.exists():
            with open(p) as f:
                return yaml.safe_load(f)
    raise FileNotFoundError(
        f"Config not found. Searched: {[str(p) for p in search_paths]}"
    )


def _load_chronic_polygon_gaps(config: dict) -> list[str]:
    """Return the sorted list of chronic-polygon-gap tickers from config.

    Empty list when the config section is missing or malformed: the
    chronic-gap self-heal step then becomes a no-op, preserving the
    pre-PR strict ``polygon_only`` behavior. Adding/removing a ticker
    requires a deliberate edit to ``data/config.yaml`` in the
    alpha-engine-config repo (private), surfaced by drift detection if
    polygon coverage recovers for an entry.
    """
    section = config.get("chronic_polygon_gaps") or {}
    tickers = section.get("tickers") or {}
    if not isinstance(tickers, dict):
        return []
    return sorted(tickers.keys())


class _CollectorError(RuntimeError):
    """Raised inside a phase block when a collector returns ``status=error`` so the
    phase writes an ``error`` marker (→ a recovery RE-RUNS it) instead of a lying
    ``ok`` marker. Caught at the call site to preserve best-effort-continue."""

    def __init__(self, name: str, detail) -> None:
        super().__init__(f"{name}: {detail}")
        self.detail = detail


def _build_registry(config: dict, args: argparse.Namespace, date: str) -> "PhaseRegistry | None":
    """Construct a per-date :class:`PhaseRegistry` for marker-based skip/resume +
    watchdog (L4528 — data is the 2nd consumer of the lib phase framework, after
    the backtester), or ``None`` in dry-run.

    Dry-run returns None so a validation pass never writes markers — a dry-run
    marker would claim ``ok`` while writing no artifact, poisoning a later real
    run's auto-skip decision. ``--only <collector>`` and ``--force`` force every
    phase to RUN (the operator explicitly asked for that work) while still writing
    markers. Per-phase hard caps come from the optional ``full_run_hard_caps_seconds``
    config block (absent → watchdog off, no behavior change).
    """
    if args.dry_run:
        return None
    _csv = lambda s: [p.strip() for p in (s or "").split(",") if p.strip()]
    return PhaseRegistry(
        date=date,
        bucket=config["bucket"],
        marker_prefix="data",
        skip_phases=_csv(getattr(args, "skip_phases", "")),
        force=bool(getattr(args, "force", False)) or (getattr(args, "only", None) is not None),
        force_phases=_csv(getattr(args, "force_phases", "")),
        hard_caps=config.get("full_run_hard_caps_seconds") or {},
    )


def _phase_collect(
    reg: "PhaseRegistry | None",
    name: str,
    run_fn,
    *,
    artifact_key: str | None = None,
    supports_auto_skip: bool = True,
) -> dict:
    """Run a collector under the phase registry (markers + L4524 artifact-validated
    auto-skip + watchdog), preserving the module's best-effort-continue posture.

    - ``reg is None`` (dry-run): run ``run_fn`` directly, no markers.
    - auto-skip (prior ``ok`` marker AND its recorded artifact still on S3): return an
      ``ok`` cache-hit dict WITHOUT recomputing. Recorded as ``ok`` — not ``skipped`` —
      because the module's status aggregator fails the run on any non-``ok`` collector,
      and a resumed phase is a success, not a failure.
    - success: ``record_artifact(artifact_key)`` so the next run's L4524 checkpoint can
      verify the output still exists (a marker whose artifact vanished re-runs).
    - collector error / raise: write an ``error`` marker (via ``_CollectorError``) and
      return an error dict so the loop continues AND main()'s aggregation still exits 1.

    ``supports_auto_skip=False`` (multi-file / shared-DB / ArcticDB producers with no
    single stable S3 key) → markers + watchdog only, the phase always runs.
    """
    if reg is None:
        try:
            return run_fn()
        except Exception as e:  # best-effort: record + continue (mirrors prior try/except)
            logger.error("%s failed: %s", name, e)
            return {"status": "error", "error": str(e)}
    try:
        with reg.phase(name, supports_auto_skip=supports_auto_skip) as ctx:
            if ctx.skipped:
                logger.info(
                    "%s: auto-skip (%s) — output already on S3 this date", name, ctx.skip_reason
                )
                return {"status": "ok", "auto_skipped": True, "skip_reason": ctx.skip_reason}
            result = run_fn() or {}
            if result.get("status") == "error":
                raise _CollectorError(name, result.get("error"))
            if artifact_key and result.get("status") in ("ok", "ok_dry_run"):
                ctx.record_artifact(artifact_key)
            return result
    except _CollectorError as ce:
        return {"status": "error", "error": ce.detail}
    except Exception as e:
        logger.error("%s phase failed: %s", name, e)
        return {"status": "error", "error": str(e)}


def _maybe_phase(reg: "PhaseRegistry | None", name: str, **log_ctx):
    """Marker-only phase wrapper (watchdog + START/END marker, no auto-skip) for
    steps whose body manages its own hard-fail/best-effort posture inline —
    MorningEnrich's preflight/append/self-heal. Returns the registry's phase
    context manager, or :func:`contextlib.nullcontext` in dry-run (reg is None).
    The yielded value is unused by these call sites (no ``ctx.skipped`` / no
    ``record_artifact``)."""
    if reg is None:
        return nullcontext()
    return reg.phase(name, supports_auto_skip=False, **log_ctx)


def run_weekly(config: dict, args: argparse.Namespace) -> dict:
    """Run collectors based on mode selection."""
    if getattr(args, "morning_enrich", False):
        return _run_morning_enrich(config, args)

    if getattr(args, "morning_arctic_append", False):
        return _run_morning_arctic_append(config, args)

    if getattr(args, "daily_arctic_append", False):
        return _run_daily_arctic_append(config, args)

    if getattr(args, "chronic_gap_heal", False):
        return _run_chronic_gap_heal(config, args)

    if args.daily:
        return _run_daily(config, args)

    phase = args.phase
    if phase is None:
        phase = 1

    if phase == 1:
        return _run_phase1(config, args)
    elif phase == 2:
        return _run_phase2(config, args)
    else:
        raise ValueError(f"Unknown phase: {phase}")


def _run_phase1(config: dict, args: argparse.Namespace) -> dict:
    """Phase 1: constituents, prices, macro, universe returns."""
    bucket = config["bucket"]
    price_cfg = config.get("price_cache", {})
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    ur_cfg = config.get("universe_returns", {})
    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dry_run = args.dry_run
    only = args.only
    reg = _build_registry(config, args, run_date)

    results: dict = {
        "phase": 1,
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    # ── Preflight ────────────────────────────────────────────────────────────
    # Preflight runs once at the entrypoint via ``main()`` (see preflight.py).
    # The previous _run_phase1-local invocation against ``validators/preflight.py``
    # was retired 2026-04-30 alongside the lib consolidation — both files were
    # running back-to-back with overlapping scope. Single source of truth now.

    # ── 1. Constituents ──────────────────────────────────────────────────────
    tickers: list[str] = []
    if only in (None, "constituents"):
        logger.info("=" * 60)
        logger.info("COLLECTING: constituents")
        logger.info("=" * 60)
        const_result = _phase_collect(
            reg, "constituents",
            lambda: constituents.collect(
                bucket=bucket, s3_prefix=market_prefix, run_date=run_date, dry_run=dry_run,
            ),
            artifact_key=f"{market_prefix}weekly/{run_date}/constituents.json",
        )
        results["collectors"]["constituents"] = const_result
        # Use the tickers returned by collect() directly (empty on auto-skip →
        # the load_from_s3 fallback below repopulates from the cached artifact).
        tickers = const_result.get("tickers", [])

    # If we didn't collect constituents, load from S3
    if not tickers and only not in ("constituents",):
        try:
            existing = constituents.load_from_s3(bucket, market_prefix)
            if existing:
                tickers = existing.get("tickers", [])
                logger.info("Loaded %d tickers from existing constituents.json", len(tickers))
        except Exception as exc:
            logger.warning("S3 constituents load failed — will fall back to Wikipedia: %s", exc)

    # ── 2. Price cache refresh ───────────────────────────────────────────────
    if only in (None, "prices"):
        logger.info("=" * 60)
        logger.info("COLLECTING: price cache")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping price cache refresh")
            results["collectors"]["prices"] = {"status": "skipped", "reason": "no tickers"}
        else:
            # supports_auto_skip=False: prices writes per-ticker parquet (no single
            # stable S3 key to validate), so markers + watchdog only — the phase
            # always runs.
            results["collectors"]["prices"] = _phase_collect(
                reg, "prices",
                lambda: prices.collect(
                    bucket=bucket,
                    tickers=tickers,
                    s3_prefix=price_cfg.get("s3_prefix", "predictor/price_cache/"),
                    fetch_period=price_cfg.get("fetch_period", "10y"),
                    staleness_threshold_days=price_cfg.get("staleness_threshold_days", 3),
                    batch_size=price_cfg.get("refresh_batch_size", 50),
                    dry_run=dry_run,
                ),
                supports_auto_skip=False,
            )

    # ── 3. Slim cache — REMOVED (Wave-4) ─────────────────────────────────────
    # predictor/price_cache_slim/ deleted: every consumer (data macro-breadth
    # + feature compute, backtester exit_timing) reads the ArcticDB universe/
    # macro libs directly. No slim writer; the prefix is gone.

    # ── 4. Macro data ────────────────────────────────────────────────────────
    if only in (None, "macro"):
        logger.info("=" * 60)
        logger.info("COLLECTING: macro data")
        logger.info("=" * 60)
        results["collectors"]["macro"] = _phase_collect(
            reg, "macro",
            lambda: macro.collect(
                bucket=bucket, s3_prefix=market_prefix, run_date=run_date, dry_run=dry_run,
            ),
            artifact_key=f"{market_prefix}weekly/{run_date}/macro.json",
        )

    # ── 4b. Short interest ───────────────────────────────────────────────────
    # Per-ticker yfinance Ticker.info scrape for the full S&P 500+400 universe.
    # FINRA data is bi-monthly (15th + EoM) so weekly Saturday cadence captures
    # every refresh with a buffer. ~10 min on the spot; the constituents list
    # was already fetched earlier in this phase.
    #
    # Gated by config["short_interest"]["enabled"] (default True). Disabling
    # lets the operator soft-launch a new collector without blocking the
    # whole pipeline if yfinance has trouble on the first Saturday — set
    # enabled=false in config, run once manually with --only short_interest,
    # then flip back to true once stable.
    si_cfg = config.get("short_interest", {})
    si_enabled = si_cfg.get("enabled", True)
    if only in (None, "short_interest") and si_enabled:
        logger.info("=" * 60)
        logger.info("COLLECTING: short interest")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping short interest")
            results["collectors"]["short_interest"] = {
                "status": "skipped", "reason": "no tickers",
            }
        else:
            results["collectors"]["short_interest"] = _phase_collect(
                reg, "short_interest",
                lambda: short_interest.collect(
                    bucket=bucket,
                    tickers=tickers,
                    s3_prefix=market_prefix,
                    run_date=run_date,
                    inter_request_delay=si_cfg.get("inter_request_delay", 0.4),
                    dry_run=dry_run,
                ),
                artifact_key=f"{market_prefix}weekly/{run_date}/short_interest.json",
            )
    elif only in (None, "short_interest") and not si_enabled:
        logger.info("short_interest collector disabled via config — skipping")
        results["collectors"]["short_interest"] = {"status": "ok", "skipped": "disabled_in_config"}

    # ── 5. Universe returns ──────────────────────────────────────────────────
    if only in (None, "universe_returns"):
        logger.info("=" * 60)
        logger.info("COLLECTING: universe returns")
        logger.info("=" * 60)
        db_path = ur_cfg.get("db_path")
        if not db_path:
            # Download research.db from S3 to temp dir
            import tempfile
            tmp_dir = tempfile.mkdtemp(prefix="ae-data-")
            db_path = os.path.join(tmp_dir, "research.db")
            try:
                s3 = boto3.client("s3")
                s3.download_file(bucket, "research.db", db_path)
                logger.info("Downloaded research.db to %s", db_path)
            except Exception as e:
                logger.warning("Could not download research.db: %s", e)
                results["collectors"]["universe_returns"] = {"status": "error", "error": str(e)}
                db_path = None

        if db_path:
            # supports_auto_skip=False: writes the shared mutable research.db (no
            # dated artifact to validate) → markers + watchdog only.
            results["collectors"]["universe_returns"] = _phase_collect(
                reg, "universe_returns",
                lambda: universe_returns.collect(
                    bucket=bucket,
                    db_path=db_path,
                    signals_prefix=ur_cfg.get("signals_prefix", "signals"),
                    sector_map_key=ur_cfg.get(
                        "sector_map_key", "predictor/price_cache/sector_map.json"
                    ),
                    dry_run=dry_run,
                ),
                supports_auto_skip=False,
            )

    # ── 5b. Signal returns (score_performance + predictor_outcomes) ────────────
    if only in (None, "signal_returns"):
        logger.info("=" * 60)
        logger.info("COLLECTING: signal returns (score_performance + predictor_outcomes)")
        logger.info("=" * 60)
        # Reuse the same db_path from universe_returns (already pulled from S3)
        sr_db_path = db_path
        if sr_db_path:
            sr_cfg = config.get("signal_returns") or {}
            # supports_auto_skip=False: also writes the shared research.db.
            results["collectors"]["signal_returns"] = _phase_collect(
                reg, "signal_returns",
                lambda: signal_returns.collect(
                    bucket=bucket,
                    db_path=sr_db_path,
                    signals_prefix=ur_cfg.get("signals_prefix", "signals"),
                    dry_run=dry_run,
                    forward_days=int(sr_cfg.get("forward_days", 21)),
                ),
                supports_auto_skip=False,
            )
        else:
            results["collectors"]["signal_returns"] = {"status": "skipped", "reason": "no research.db"}

    # ── 6. Fundamentals ───────────────────────────────────────────────────────
    if only in (None, "fundamentals"):
        logger.info("=" * 60)
        logger.info("COLLECTING: fundamentals (FMP)")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping fundamentals")
            results["collectors"]["fundamentals"] = {"status": "skipped", "reason": "no tickers"}
        else:
            results["collectors"]["fundamentals"] = _phase_collect(
                reg, "fundamentals",
                lambda: fundamentals.collect(
                    bucket=bucket, tickers=tickers, run_date=run_date, dry_run=dry_run,
                ),
                artifact_key=f"archive/fundamentals/{run_date}.json",
            )

    # ── 7. Feature store compute ───────────────────────────────────────────
    if only in (None, "features"):
        logger.info("=" * 60)
        logger.info("COMPUTING: feature store snapshot")
        logger.info("=" * 60)
        from features.compute import compute_and_write
        results["collectors"]["features"] = _phase_collect(
            reg, "features",
            lambda: compute_and_write(date_str=run_date, bucket=bucket, dry_run=dry_run),
            artifact_key=f"features/{run_date}/schema_version.json",
        )

    # ── 8. ArcticDB universe rebuild ─────────────────────────────────────────
    if only in (None, "arcticdb"):
        logger.info("=" * 60)
        logger.info("REBUILDING: ArcticDB universe (full backfill)")
        logger.info("=" * 60)
        from builders.backfill import backfill
        # supports_auto_skip=False: writes ArcticDB (no S3 key); backfill is
        # idempotent and cheap to repeat → markers + watchdog only.
        results["collectors"]["arcticdb"] = _phase_collect(
            reg, "arcticdb",
            lambda: backfill(bucket=bucket, dry_run=dry_run, run_date=run_date),
            supports_auto_skip=False,
        )

    # ── Finalize ─────────────────────────────────────────────────────────────
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    _finalize(results, bucket, market_prefix, run_date, dry_run, only)
    return results


def _run_phase2(config: dict, args: argparse.Namespace) -> dict:
    """Phase 2: alternative data for promoted tickers (after research)."""
    bucket = config["bucket"]
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dry_run = args.dry_run
    reg = _build_registry(config, args, run_date)

    results: dict = {
        "phase": 2,
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    logger.info("=" * 60)
    logger.info("COLLECTING: alternative data (Phase 2)")
    logger.info("=" * 60)
    results["collectors"]["alternative"] = _phase_collect(
        reg, "alternative",
        lambda: alternative.collect(
            bucket=bucket, s3_prefix=market_prefix, run_date=run_date, dry_run=dry_run,
        ),
        artifact_key=f"{market_prefix}weekly/{run_date}/alternative/manifest.json",
    )

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    _finalize(results, bucket, market_prefix, run_date, dry_run, None)
    return results


def _previous_trading_day(reference: datetime | None = None) -> str:
    """Find the most recent trading day strictly before ``reference`` (UTC).

    Used by --morning-enrich to determine which date polygon's grouped-daily
    should be fetched for. Free tier won't serve today's data, so we always
    enrich the prior session. Walks back at most 10 calendar days as a
    runaway guard against a broken trading-calendar implementation.
    """
    from alpha_engine_lib.trading_calendar import is_trading_day
    from datetime import timedelta

    ref = reference or datetime.now(timezone.utc)
    d = ref.date() - timedelta(days=1)
    for _ in range(10):
        if is_trading_day(d):
            return d.strftime("%Y-%m-%d")
        d -= timedelta(days=1)
    raise RuntimeError(
        f"Could not find a trading day in the 10 calendar days before {ref.date()} — "
        f"trading_calendar.is_trading_day appears broken or NYSE has been closed for >1 week."
    )


def _arctic_spy_last_date(bucket: str) -> "date | None":
    """Return SPY's last-indexed date in ArcticDB macro lib, or None on read failure.

    Best-effort: any exception (lib unavailable, empty symbol, transient S3) is
    logged and resolved as None so the skip decision falls through to "run
    polygon". Polygon will surface its own failures loudly downstream.
    """
    try:
        from store.arctic_store import get_macro_lib
        import pandas as pd
        macro_lib = get_macro_lib(bucket)
        df = macro_lib.tail("SPY", n=1).data
        if df.empty:
            return None
        return pd.Timestamp(df.index[-1]).date()
    except Exception as exc:
        logger.warning(
            "ArcticDB SPY last_date read failed (%s) — skip-guard will proceed without staleness check",
            exc,
        )
        return None


def _should_skip_morning_enrich(
    target_date: str,
    arctic_last_date: "date | None",
) -> tuple[bool, str | None]:
    """Decide whether to skip MorningEnrich based on data staleness.

    Returns ``(skip, reason)``. ``skip=True`` means the caller should bail
    out before invoking polygon. Rationale:

    Polygon's free tier 403's same-day grouped-daily, and the next session's
    T+1 settlement isn't visible until the following morning (the Saturday SF
    cron — 09:00 UTC = 02:00 AM PT — was chosen on this basis). On a manual
    midweek "Saturday SF" rerun after the EOD post-market pass has already
    landed today's yfinance row, polygon's prior-trading-day overwrite would
    write *older* data over *newer* data already in ArcticDB.

    The check: if polygon's ``target_date`` is strictly before what's already
    in ArcticDB, skip. The yfinance row written by this afternoon's EOD
    PostMarketData stays authoritative for this run; the next regular
    Saturday SF re-fetches the affected sessions from polygon T+1, restoring
    authoritative VWAP/OHLCV.

    The scheduled Saturday 02:00 PT cron and the weekday-SF 06:15 PT
    MorningEnrich Lambda both target the prior trading day, which equals
    ArcticDB's last_date at that hour, so the check returns ``skip=False``
    and polygon runs as normal.

    Explicit ``--date`` is handled by the caller and bypasses the check.
    """
    if arctic_last_date is None:
        return False, None
    if target_date < arctic_last_date.isoformat():
        return True, (
            f"stale_overwrite (polygon target={target_date}, "
            f"ArcticDB SPY last={arctic_last_date.isoformat()}) — polygon's "
            f"T+1 settled day is older than the yfinance EOD row already in "
            f"ArcticDB; skipping to avoid overwriting newer with older. "
            f"Next Saturday SF re-fetches via polygon T+1."
        )
    return False, None


def _detect_chronic_gap_polygon_recovery(
    bucket: str,
    target_date: str,
    chronic_tickers: list[str],
    daily_closes_prefix: str = "staging/daily_closes/",
) -> dict:
    """Drift alarm: detect when polygon STARTS covering a chronic-gap ticker.

    Pairs with ``_self_heal_chronic_polygon_gaps``. The chronic_polygon_gaps
    allowlist (alpha-engine-config #88) was added because polygon doesn't
    reliably serve these tickers (BF-B/BRK-B/MOG-A/PSTG today). If polygon
    coverage recovers — e.g. polygon adds a Berkshire B share class CIK
    or fixes a flaky data feed — the allowlist entry is no longer needed
    and should be pruned. Without active drift detection the entry would
    persist indefinitely as a silent piece of operational debt.

    Reads ``staging/daily_closes/{target_date}.parquet`` written by
    ``daily_closes.collect(source="polygon_only")`` and counts how many
    chronic_polygon_gaps tickers polygon DID cover today. Emits a
    CloudWatch gauge ``AlphaEngine/Data/chronic_gap_polygon_recovery_count``
    so an alarm can fire if the count > 0 for N consecutive Saturdays
    (operator action: prune the allowlist entry).

    Best-effort: read errors / metric-emit errors log a warning but never
    raise — this is observability, not a load-bearing path.

    Returns a summary dict; caller logs at INFO + persists in collector
    results so the manifest carries a record.
    """
    summary: dict = {
        "status": "ok",
        "chronic_tickers_checked": len(chronic_tickers),
        "polygon_recovered": [],
        "absent_as_expected": [],
        "errors": [],
    }
    if not chronic_tickers:
        return summary

    import io as _io

    try:
        s3 = boto3.client("s3")
        key = f"{daily_closes_prefix.rstrip('/')}/{target_date}.parquet"
        obj = s3.get_object(Bucket=bucket, Key=key)
        import pandas as _pd
        df = _pd.read_parquet(_io.BytesIO(obj["Body"].read()))
    except Exception as exc:
        logger.warning(
            "chronic-gap drift check: could not read staging/daily_closes "
            "parquet for %s — drift alarm skipped this cycle. %s",
            target_date, exc,
        )
        summary["status"] = "skipped"
        summary["errors"].append({"reason": str(exc)})
        return summary

    # Index is ticker (collectors/daily_closes.py:251 sets index=ticker).
    daily_closes_tickers = (
        set(df.index.astype(str)) if df.index.size else set()
    )

    for ticker in chronic_tickers:
        if ticker in daily_closes_tickers:
            summary["polygon_recovered"].append(ticker)
        else:
            summary["absent_as_expected"].append(ticker)

    n_recovered = len(summary["polygon_recovered"])
    if n_recovered > 0:
        logger.warning(
            "chronic-gap drift detected: polygon now covers %d chronic_polygon_gaps "
            "ticker(s) it did not previously serve: %s. Consider pruning these from "
            "alpha-engine-config/predictor.yaml chronic_polygon_gaps.tickers if the "
            "coverage persists across multiple cycles.",
            n_recovered, summary["polygon_recovered"],
        )
    else:
        logger.info(
            "chronic-gap drift: 0 of %d chronic tickers showed up in today's "
            "polygon_only daily_closes — allowlist still load-bearing.",
            len(chronic_tickers),
        )

    # Emit CloudWatch metric — best-effort. Always emits (including 0) so
    # alarm baselines are continuous; CloudWatch missing-data is harder to
    # alarm against than a steady 0 stream.
    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "chronic_gap_polygon_recovery_count",
                "Value": float(n_recovered),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.warning(
            "chronic_gap_polygon_recovery_count metric emit failed: %s — "
            "drift alarm cadence may degrade until next cycle.", exc,
        )

    return summary


def _detect_chronic_gap_constituents_drift(
    bucket: str,
    chronic_tickers: list[str],
) -> dict:
    """Drift alarm: detect when a chronic_polygon_gaps allowlist ticker has
    dropped out of the current S&P 500/400 constituents set.

    Pairs with ``_self_heal_chronic_polygon_gaps`` and serves as the GATE
    on its inputs — non-constituent tickers are filtered out before any
    yfinance fetch or ``backfill(ticker_filter=...)`` call so the heal
    path never hands a non-constituent ticker to the constituents-filtered
    backfill writer (which hard-errs per ``builders/backfill.py``).

    Mirrors :func:`_detect_chronic_gap_polygon_recovery` on the inverse
    axis. The polygon-recovery detector catches the "polygon now serves
    this — remove from allowlist" direction; this detector catches the
    "ticker no longer a constituent — remove from allowlist" direction.

    Origin: 2026-05-27 flow-doctor ERROR "Ticker PSTG not found in
    universe" — PSTG dropped from S&P 500/400 constituents between the
    5/16 and 5/23 weekly partitions (REMOVED cohort = {BK, FLO, PSTG};
    see config private-docs/ROADMAP.md L1772) but stayed in the
    chronic_polygon_gaps allowlist. MorningEnrich yfinance-backfilled
    PSTG.parquet then called ``backfill(ticker_filter='PSTG')``, which
    hard-erred against the constituents filter. The polygon-recovery
    drift detector was the existing axis; this is the missing inverse
    axis.

    Reads the current constituents via the shared chokepoint
    :func:`builders._constituents_loader.load_constituents_for_run_date`
    (no ``run_date`` argument → pointer-following ad-hoc read, which is
    the correct read for a MorningEnrich-time check between Saturday SFs).

    Emits a CloudWatch gauge
    ``AlphaEngine/Data/chronic_gap_non_constituent_count`` for alarming.

    Best-effort: a constituents read failure logs a WARN and returns
    ``status='skipped'`` with the full chronic list as ``still_constituents``
    so the caller falls through to the existing behavior (the original
    hard-err at backfill is then the load-bearing surface). Never raises.

    Returns
    -------
    dict
        ``{"status": "ok"|"skipped", "chronic_tickers_checked": int,
           "still_constituents": list[str], "dropped_non_constituent": list[str],
           "weekly_date": str|None, "errors": list[dict]}``
    """
    summary: dict = {
        "status": "ok",
        "chronic_tickers_checked": len(chronic_tickers),
        "still_constituents": list(chronic_tickers),
        "dropped_non_constituent": [],
        "weekly_date": None,
        "errors": [],
    }
    if not chronic_tickers:
        return summary

    try:
        from builders._constituents_loader import load_constituents_for_run_date
        s3 = boto3.client("s3")
        constituents_set, weekly_date = load_constituents_for_run_date(s3, bucket)
        summary["weekly_date"] = weekly_date
    except Exception as exc:
        logger.warning(
            "chronic-gap constituents-drift check: could not load current "
            "constituents — drift gate skipped this cycle, all %d chronic "
            "ticker(s) will proceed to self-heal. %s",
            len(chronic_tickers), exc,
        )
        summary["status"] = "skipped"
        summary["errors"].append({"reason": str(exc)})
        return summary

    still: list[str] = []
    dropped: list[str] = []
    for ticker in chronic_tickers:
        if ticker in constituents_set:
            still.append(ticker)
        else:
            dropped.append(ticker)

    summary["still_constituents"] = still
    summary["dropped_non_constituent"] = dropped

    if dropped:
        logger.warning(
            "chronic-gap constituents drift detected: %d chronic_polygon_gaps "
            "ticker(s) no longer in current constituents (%s, weekly=%s): %s. "
            "These will be SKIPPED by self-heal — prune from "
            "alpha-engine-config/data/config.yaml chronic_polygon_gaps.tickers "
            "to silence this WARN.",
            len(dropped), bucket, weekly_date, dropped,
        )
    else:
        logger.info(
            "chronic-gap constituents drift: %d of %d chronic tickers still "
            "in current constituents (weekly=%s) — allowlist coherent.",
            len(still), len(chronic_tickers), weekly_date,
        )

    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "chronic_gap_non_constituent_count",
                "Value": float(len(dropped)),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.warning(
            "chronic_gap_non_constituent_count metric emit failed: %s — "
            "drift alarm cadence may degrade until next cycle.", exc,
        )

    return summary


# Hard wall-clock bound for the chronic-gap self-heal (L4605). Generous enough
# never to false-positive a legitimately-slow all-4-stale heal (~4 tickers ×
# (yf.download ≤30s + backfill) ≈ 6 min worst case), but finite so an INFINITE
# network hang in yf.download / backfill is converted to a bounded best-effort
# skip rather than running forever. On the WEEKDAY pipeline the heal is its own
# fail-soft SF state with a 300s SSM timeout (which fires first); on the SATURDAY
# pipeline the heal runs INLINE inside MorningEnrich (5400s SSM budget), so this
# in-process bound is the one that actually prevents an infinite heal hang from
# SIGKILLing the load-bearing Saturday MorningEnrich. Chosen over a separate
# Saturday SF state because the Saturday SF launches a fresh spot instance per
# state — a spot-per-4-ticker-heal would be wasteful (2026-06-11 decision).
_CHRONIC_HEAL_HARD_TIMEOUT_S = 600


class _HardTimeout(BaseException):
    """Raised by :func:`_hard_timeout` on SIGALRM expiry. Subclasses
    BaseException (not Exception) so the per-ticker ``except Exception`` inside
    the self-heal loop does NOT swallow it — the alarm aborts the whole heal
    immediately and propagates to the caller, which logs + continues
    (best-effort). Mirrors how KeyboardInterrupt escapes broad excepts."""


@contextmanager
def _hard_timeout(seconds: int, label: str):
    """SIGALRM-based hard wall-clock bound for a best-effort block.

    Main-thread only (weekly_collector runs as the main thread under both
    ``--morning-enrich`` and ``--chronic-gap-heal``). Raises :class:`_HardTimeout`
    on expiry. No-op (yields without arming) if SIGALRM is unavailable — not the
    main thread, or a non-POSIX platform — so callers behave identically minus
    the bound. SIGALRM interrupts blocking syscalls (socket recv), so it bounds
    a hung network fetch, not just CPU-bound loops.
    """
    def _handler(signum, frame):
        raise _HardTimeout(f"{label} exceeded {seconds}s hard timeout")

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


def _self_heal_chronic_polygon_gaps(
    bucket: str,
    target_date: str,
    chronic_tickers: list[str],
    dry_run: bool = False,
) -> dict:
    """Yfinance-backfill any ArcticDB row gap for chronic-polygon-gap tickers.

    For each ticker in ``chronic_tickers``:
      1. Read ArcticDB universe ``last_date``.
      2. If ``last_date >= target_date``, skip (already fresh — common case
         after the first heal lands).
      3. Else yfinance-fetch ``[last_date+1, target_date]`` OHLCV.
      4. Append the new rows to ``predictor/price_cache/{ticker}.parquet``
         (dedupe by date keep="last" so repeated heals are idempotent). Wave 3
         PR1 mirrors the put to ``reference/price_cache/{ticker}.parquet`` via
         the ``_price_cache_write_prefixes`` helper.
      5. Invoke ``builders.backfill(ticker_filter=ticker)`` — reuses the
         per-ticker compute_features + ArcticDB write path so the new
         rows get the same feature schema as every other ticker.

    Closes the multi-day rot caused by polygon never serving the chronic
    gaps + the EOD yfinance pass occasionally dropping a day. Origin:
    2026-05-09 weekly SF DataPhase1 postflight failure — PSTG ended at
    5/5 (ArcticDB) vs SPY at 5/8 (3d stale, > 2d threshold), every other
    chronic ticker at 5/6 (2d, just under threshold). Without this step
    the only recovery was hand-running a yfinance backfill script.

    Idempotent: tickers already at target_date are skipped, so re-running
    after a partial completion costs only the freshness reads. Best-effort
    per-ticker — one ticker's yfinance failure does not block the others.

    Returns a summary dict with per-ticker outcomes; the caller should
    log it (not raise) so a yfinance hiccup on a chronic gap doesn't
    halt the whole pipeline. Postflight will catch any remaining staleness
    via its uniform check.
    """
    import io as _io

    import yfinance as _yf

    from store.arctic_store import get_universe_lib

    s3 = boto3.client("s3")
    universe_lib = get_universe_lib(bucket)
    target_ts = __import__("pandas").Timestamp(target_date).normalize()

    summary: dict = {
        "checked": len(chronic_tickers),
        "healed": [],
        "skipped_already_fresh": [],
        "errors": [],
    }

    if not chronic_tickers:
        return summary

    import pandas as _pd

    for ticker in chronic_tickers:
        try:
            try:
                df_tail = universe_lib.tail(ticker, n=1).data
                existing_last = (
                    _pd.Timestamp(df_tail.index[-1]).normalize()
                    if df_tail is not None and not df_tail.empty
                    else None
                )
            except Exception:
                existing_last = None

            if existing_last is not None and existing_last >= target_ts:
                summary["skipped_already_fresh"].append(
                    {"ticker": ticker, "last_date": str(existing_last.date())}
                )
                continue

            start_ts = (
                (existing_last + _pd.Timedelta(days=1))
                if existing_last is not None
                else (target_ts - _pd.Timedelta(days=30))
            )
            end_excl = target_ts + _pd.Timedelta(days=1)

            yf_df = _yf.download(
                ticker,
                start=start_ts.strftime("%Y-%m-%d"),
                end=end_excl.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=True,
                # Bound the network call so a hung yfinance fetch can't stall
                # the heal indefinitely. The 2026-06-11 incident was an
                # unbounded yf.download here; the SF state isolation is the
                # primary fix, this is defence-in-depth so a single ticker's
                # stall is capped rather than eating the whole state timeout.
                timeout=30,
            )
            if isinstance(yf_df.columns, _pd.MultiIndex):
                yf_df.columns = yf_df.columns.get_level_values(0)

            yf_df = yf_df[(yf_df.index >= start_ts) & (yf_df.index <= target_ts)]
            if yf_df.empty:
                summary["errors"].append(
                    {"ticker": ticker, "reason": "yfinance returned no rows in target range"}
                )
                continue

            ohlcv_cols = ["Open", "High", "Low", "Close", "Volume"]
            new_rows = yf_df[[c for c in ohlcv_cols if c in yf_df.columns]].copy()

            # Wave 3 PR1: read from legacy (single source of truth during the
            # write-both soak — readers haven't migrated yet); write to both
            # legacy + new prefix on the put back (see
            # builders/_price_cache_writeboth.py for soak contract)
            pcache_key = f"predictor/price_cache/{ticker}.parquet"
            try:
                obj = s3.get_object(Bucket=bucket, Key=pcache_key)
                existing_pcache = _pd.read_parquet(_io.BytesIO(obj["Body"].read()))
            except s3.exceptions.NoSuchKey:
                existing_pcache = _pd.DataFrame(columns=ohlcv_cols)

            combined_pcache = _pd.concat([existing_pcache, new_rows])
            combined_pcache = combined_pcache[
                ~combined_pcache.index.duplicated(keep="last")
            ].sort_index()

            if not dry_run:
                buf = _io.BytesIO()
                combined_pcache.to_parquet(buf, engine="pyarrow", compression="snappy")
                body = buf.getvalue()
                for _prefix in _price_cache_write_prefixes():
                    s3.put_object(
                        Bucket=bucket, Key=f"{_prefix}{ticker}.parquet", Body=body
                    )

                from builders.backfill import backfill as _backfill
                _backfill(bucket=bucket, ticker_filter=ticker, dry_run=False)

            summary["healed"].append(
                {
                    "ticker": ticker,
                    "previous_last_date": (
                        str(existing_last.date()) if existing_last is not None else None
                    ),
                    "rows_added": int(len(new_rows)),
                    "new_last_date": str(new_rows.index[-1].date()),
                }
            )
            logger.info(
                "chronic-gap self-heal: %s healed (prev=%s → new=%s, +%d rows)",
                ticker,
                existing_last.date() if existing_last is not None else "none",
                new_rows.index[-1].date(),
                len(new_rows),
            )
        except Exception as exc:
            logger.exception("chronic-gap self-heal failed for %s", ticker)
            summary["errors"].append({"ticker": ticker, "reason": str(exc)})

    return summary


def _run_morning_enrich(config: dict, args: argparse.Namespace) -> dict:
    """Morning polygon enrichment: overwrite the prior trading day's parquet
    + ArcticDB row with polygon's authoritative OHLCV+VWAP.

    Called by the new MorningEnrich Lambda step in the weekday SF (and
    available via --morning-enrich for backfills). Hard-fails on any polygon
    failure — predictor inference reads ArcticDB right after this runs and
    must see polygon-corrected data, not silently-stale yfinance values.

    Skips the feature_store snapshot step (that already ran with yfinance EOD;
    re-running it is expensive and the polygon delta on OHLCV is typically <1%).
    daily_append's per-ticker compute_features call recomputes per-ticker
    features inside ArcticDB based on the polygon-overwritten row, which is
    what downstream consumers actually read.
    """
    bucket = config["bucket"]
    started_at = datetime.now(timezone.utc).isoformat()

    # Compute target_date PT-aware so a Wed-evening manual rerun (UTC rolled
    # past midnight) doesn't resolve "previous trading day" to today PT — that
    # was the original 403 trap the wall-clock guard worked around.
    if args.date is None:
        from zoneinfo import ZoneInfo
        target_date = _previous_trading_day(
            reference=datetime.now(ZoneInfo("America/Los_Angeles"))
        )
    else:
        target_date = args.date

    # Skip guard: data-staleness check. If polygon's target_date is older than
    # what's already in ArcticDB (yfinance EOD already landed today's row),
    # skip so we don't overwrite newer data with older. See
    # _should_skip_morning_enrich() for full rationale. Explicit --date
    # bypasses the guard so operator-driven backfills still work.
    if args.date is None:
        arctic_last_date = _arctic_spy_last_date(bucket)
        skip, reason = _should_skip_morning_enrich(target_date, arctic_last_date)
        if skip:
            logger.info(
                "Skipping MorningEnrich: %s. Would have targeted %s.",
                reason, target_date,
            )
            return {
                "mode": "morning_enrich",
                "status": "skipped",
                "skip_reason": reason,
                "would_have_targeted": target_date,
                "started_at": started_at,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "collectors": {},
            }
    dry_run = args.dry_run
    daily_cfg = config.get("daily_closes", {})
    # Registry date = target_date (the prior trading day MorningEnrich enriches),
    # so markers land under data/{target_date}/.phases/. None in dry-run.
    reg = _build_registry(config, args, target_date)

    results: dict = {
        "mode": "morning_enrich",
        "date": target_date,
        "started_at": started_at,
        "collectors": {},
    }

    # ── Pre-flight: refresh constituents + prune ArcticDB stragglers ─────────
    # Order matters. Without these, MorningEnrich loads last week's
    # constituents.json (Phase 1's writer hasn't run yet this Saturday), so
    # any S&P churn-out from the past week is invisible — the ticker stays
    # in the request list, polygon doesn't have it (now-delisted), and the
    # downstream missing-from-closes + freshness checks have to defend
    # against the drift via ``expected_tickers`` scoping (PR #132 + PR #133
    # are exactly that defense). Refreshing constituents + pruning here
    # makes the universe coherent BEFORE any check fires, so the bandages
    # become a quiet no-op rather than the load-bearing path.
    #
    # 2026-05-02 redrive #4 (after PR #132/#133 shipped) is the validation
    # window: with the reorder, prune drops the 8 churn-outs (ASGN, GTM,
    # HOLX, KMPR, LW, MOH, MTCH, PAYC) before MorningEnrich's writes;
    # downstream checks see a coherent universe without needing the
    # ``expected_tickers`` scoping at all.
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    if not dry_run:
        logger.info("=" * 60)
        logger.info("REFRESHING: constituents (pre-MorningEnrich)")
        logger.info("=" * 60)
        try:
            with _maybe_phase(reg, "morning_constituents"):
                cons_result = constituents.collect(
                    bucket=bucket,
                    s3_prefix=market_prefix,
                    run_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    dry_run=False,
                )
            results["collectors"]["constituents_preflight"] = cons_result
            tickers = cons_result.get("tickers", [])
            logger.info(
                "Pre-MorningEnrich constituents refresh: %d tickers", len(tickers),
            )
        except Exception as exc:
            logger.exception("Pre-MorningEnrich constituents refresh failed")
            results["collectors"]["constituents_preflight"] = {
                "status": "error", "error": str(exc),
            }
            results["status"] = "failed"
            results["completed_at"] = datetime.now(timezone.utc).isoformat()
            return results

        logger.info("=" * 60)
        logger.info("PRUNING: ArcticDB universe stragglers (pre-MorningEnrich)")
        logger.info("=" * 60)
        try:
            from builders.prune_delisted_tickers import prune_delisted_tickers
            # Use absent_days=5 to match the post-write freshness scan
            # threshold — consistent with what daily_append considers
            # "stale enough to drop". The post-Phase-1 prune still runs
            # later with the conservative 14d default for any newcomers
            # the SF picked up between MorningEnrich and Phase 1.
            with _maybe_phase(reg, "morning_prune"):
                prune_result = prune_delisted_tickers(
                    bucket=bucket,
                    absent_days=5,
                    apply=True,
                    constituents_override=set(tickers),
                )
            results["collectors"]["prune_preflight"] = prune_result
            logger.info(
                "Pre-MorningEnrich prune: pruned %d stragglers (skipped_recent=%d)",
                prune_result.get("pruned_count", 0),
                prune_result.get("skipped_recent_count", 0),
            )
        except Exception as exc:
            # Prune failure is non-fatal here — the bandage scoping in
            # daily_append (PR #132/#133) still tolerates stragglers, and
            # the post-Phase-1 prune gets another shot. Surface it
            # loudly per feedback_no_silent_fails — operator should
            # investigate, but the SF can complete tonight on the
            # bandages alone.
            #
            # Recorded under top-level ``prune_preflight_warning`` rather
            # than ``results["collectors"]`` because the per-collector
            # status aggregator treats any ``error`` entry there as a
            # whole-pipeline failure. The prune side-effect is best-effort
            # observability, not a blocking step.
            logger.error(
                "Pre-MorningEnrich prune failed: %s. Falling through to "
                "MorningEnrich; daily_append's expected_tickers scoping "
                "will still tolerate stragglers, but please investigate "
                "the prune failure.", exc,
            )
            results["prune_preflight_warning"] = {
                "status": "error", "error": str(exc),
            }
    else:
        # Dry-run: skip side effects but still load tickers for the
        # downstream daily_closes call.
        try:
            existing = constituents.load_from_s3(bucket, market_prefix)
            tickers = existing.get("tickers", []) if existing else []
        except Exception:
            tickers = []
        if not tickers:
            try:
                tickers, _, _, _, _ = constituents._fetch_constituents()
            except Exception as exc:
                logger.error("Wikipedia constituents fallback failed: %s", exc)

    if not tickers:
        logger.error("No tickers available for morning enrichment")
        results["status"] = "failed"
        return results

    MACRO_DAILY_TICKERS = [
        "SPY", "GLD", "USO",
        "XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
        "XLP", "XLRE", "XLU", "XLV", "XLY",
        "^VIX", "^VIX3M", "^TNX", "^IRX",
    ]
    tickers = list(dict.fromkeys(tickers + MACRO_DAILY_TICKERS))

    logger.info("=" * 60)
    logger.info("MORNING ENRICH: polygon overwrite for %s (prior trading day)", target_date)
    logger.info("=" * 60)
    # Windowed-reconciliation knobs from config — default window_days=1
    # preserves legacy single-date overwrite. When set to N > 1, polygon
    # makes one grouped-daily call per BDay in the window (N total —
    # bounded by free-tier rate limit). Polygon ignores skip_if_canonical
    # per option (a): re-overwrites within the window to absorb
    # corporate-action backfills.
    window_days = int(daily_cfg.get("window_days", 1))
    skip_if_canonical = bool(daily_cfg.get("skip_if_canonical", False))
    try:
        with _maybe_phase(reg, "morning_daily_closes"):
            dc_result = daily_closes.collect(
                bucket=bucket,
                tickers=tickers,
                run_date=target_date,
                s3_prefix=daily_cfg.get("s3_prefix", "staging/daily_closes/"),
                dry_run=dry_run,
                source="polygon_only",
                window_days=window_days,
                skip_if_canonical=skip_if_canonical,
            )
        results["collectors"]["daily_closes"] = dc_result
    except Exception as e:
        logger.exception("Morning polygon enrichment failed for %s", target_date)
        results["collectors"]["daily_closes"] = {"status": "error", "error": str(e)}
        results["status"] = "failed"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        return results

    # ── Re-append to ArcticDB so the polygon-overwritten row replaces the
    # yfinance row written at EOD. universe_lib.update() is idempotent for
    # same-date overwrites (see daily_append.py:232-242 for the design intent).
    #
    # daily_append is the SLOW part of MorningEnrich (~20-38 min on the
    # t3.small). 2026-06-11: a same-morning rerun's append exceeded the 1800s
    # SSM executionTimeout and SIGKILLed MorningEnrich (L4608). ``--skip-arctic-append``
    # decouples it on the WEEKDAY pipeline, where it runs as its OWN skip-gated,
    # load-bearing SF state (``MorningArcticAppend``, via ``_run_morning_arctic_append``)
    # with a longer timeout AFTER the fast fetch — so (a) the append's duration
    # can no longer time out the fetch, and (b) a recovery rerun can skip the
    # completed fetch and re-run only the append (or vice-versa). The SATURDAY
    # pipeline still runs it INLINE here (no skip flag): the append must precede
    # DataPhase1's postflight, same rationale as the inline chronic-gap heal.
    if not getattr(args, "skip_arctic_append", False):
        logger.info("=" * 60)
        logger.info("APPENDING: ArcticDB universe (morning enrich, %s)", target_date)
        logger.info("=" * 60)
        try:
            from builders.daily_append import daily_append
            with _maybe_phase(reg, "morning_arcticdb"):
                arctic_result = daily_append(
                    date_str=target_date,
                    bucket=bucket,
                    dry_run=dry_run,
                    expected_tickers=tickers,
                )
            results["collectors"]["arcticdb"] = arctic_result
        except Exception as e:
            logger.exception("ArcticDB daily_append (morning enrich) failed for %s", target_date)
            results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}

    # ── Chronic-polygon-gap self-heal ────────────────────────────────────────
    # The chronic-gap drift-detection + yfinance self-heal logic lives in
    # ``_run_chronic_gap_heal`` (shared). It heals BF-B/BRK-B/MOG-A/PSTG row
    # gaps (polygon doesn't reliably serve them) before downstream consumers
    # read ArcticDB.
    #
    # ``--skip-chronic-heal`` decouples it from MorningEnrich on the WEEKDAY
    # pipeline, where it runs as its own fail-soft SF state (``ChronicGapSelfHeal``)
    # AFTER MorningEnrich. Origin: 2026-06-11 — an unbounded ``yf.download`` hang
    # in the inline heal ran out MorningEnrich's SSM ``executionTimeout`` and
    # SIGKILLed the whole command, discarding ~20 min of completed daily_append
    # and failing the weekday pipeline. Splitting it (per the standing rule: a
    # best-effort downstream step must never force re-running a completed
    # upstream task) means a heal hang can no longer touch the load-bearing
    # data write.
    #
    # The SATURDAY pipeline still runs the heal INLINE here (no skip flag): it
    # must precede DataPhase1's postflight, whose freshness gate is the heal's
    # origin (2026-05-09 stale-PSTG failure). The ``yf.download(timeout=30)``
    # bound now caps the hang that the inline path can't otherwise isolate;
    # splitting Saturday's heal into its own state too is a tracked follow-up.
    if not getattr(args, "skip_chronic_heal", False):
        heal = _run_chronic_gap_heal(config, args)
        results["collectors"].update(heal.get("collectors", {}))

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    results["status"] = "ok" if all(s in ("ok", "ok_dry_run") for s in statuses) else "failed"

    # Refresh the `daily_data` health stamp on daily_closes success.
    #
    # Health gate is decoupled from arcticdb daily_append status — the field
    # is named `daily_data` and represents the canonical OHLCV write state,
    # which is what daily_closes produces. ArcticDB append is a downstream
    # consumer of the same data; its failure mode (slow lib.write rewrites,
    # universe drift) is separate and surfaces via the arcticdb collector's
    # own status entry in `results["collectors"]["arcticdb"]`.
    #
    # Pre-2026-05-05 the gate required all collectors ok. Symptom: morning
    # polygon overwrite landed (parquet timestamp 6:21 AM PT, VWAP populated)
    # but health stayed stale on yesterday's EOD yfinance write because
    # arcticdb append failed silently in this Lambda invocation. The
    # `health/daily_data.json` consumer (executor staleness gate, dashboard
    # ingestion-attribution panel) needs the polygon row counts to reflect
    # ingestion truth — the arcticdb failure isn't theirs to gate on.
    #
    # Without this the executor's 26h staleness gate trips on Monday mornings
    # (post-close stamp is from Friday ~13h before market close → ~65h on
    # Monday open). Only write on daily_closes success — a failed
    # daily_closes must leave the prior stamp in place so the gate fires
    # correctly. Post-close DailyData run still writes the canonical stamp;
    # this is a refresh after the polygon overwrite.
    if not dry_run:
        _dc = results["collectors"].get("daily_closes", {})
        _dc_status = _dc.get("status", "unknown")
        if _dc_status in ("ok", "ok_dry_run"):
            try:
                _morning_duration = (
                    datetime.fromisoformat(results["completed_at"])
                    - datetime.fromisoformat(results["started_at"])
                ).total_seconds()
            except Exception:
                _morning_duration = 0.0
            _write_module_health(
                bucket,
                module_name="daily_data",
                run_date=target_date,
                status="ok",
                summary={
                    "tickers_captured": _dc.get("tickers_captured", 0),
                    "polygon": _dc.get("polygon", 0),
                    "fred": _dc.get("fred", 0),
                    "yfinance": _dc.get("yfinance", 0),
                    "morning_enrich": True,
                },
                duration_seconds=_morning_duration,
            )

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        pass
    logger.info(
        "Morning enrichment %s for %s: %s%s",
        results["status"].upper(), target_date,
        ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
        duration,
    )
    return results


def _run_morning_arctic_append(config: dict, args: argparse.Namespace) -> dict:
    """Standalone ArcticDB universe append for the prior trading day (L4608).

    Split out of :func:`_run_morning_enrich` (2026-06-11) into its own weekday-SF
    state (``MorningArcticAppend``). MorningEnrich now does only the fast fetch
    (constituents refresh + prune + polygon daily_closes overwrite, via
    ``--skip-arctic-append``); this runs the SLOW ``daily_append`` that writes
    the polygon-corrected row + recomputed features into the ArcticDB universe
    library.

    Why split: ``daily_append`` ran ~20-38 min on the t3.small and on 2026-06-11
    a same-morning rerun exceeded MorningEnrich's 1800s SSM ``executionTimeout``
    and SIGKILLed it. As its own state the append gets a longer timeout decoupled
    from the fetch, and a recovery rerun can skip whichever half already
    completed (``skip_morning_enrich`` / ``skip_morning_arctic_append``) instead
    of re-paying both.

    LOAD-BEARING: predictor inference reads the ArcticDB universe right after
    this, so an append failure returns ``status="failed"`` → ``main()`` exits 1
    → the SF's ``CheckMorningArcticAppendStatus`` routes to HandleFailure. Reads
    the constituents MorningEnrich just refreshed to S3 (so the expected-ticker
    scope matches the post-prune universe).
    """
    bucket = config["bucket"]
    started_at = datetime.now(timezone.utc).isoformat()

    # Same PT-aware target-date resolution as MorningEnrich, so the append
    # targets the prior trading day MorningEnrich just fetched.
    if args.date is None:
        from zoneinfo import ZoneInfo
        target_date = _previous_trading_day(
            reference=datetime.now(ZoneInfo("America/Los_Angeles"))
        )
    else:
        target_date = args.date

    dry_run = args.dry_run
    results: dict = {
        "mode": "morning_arctic_append",
        "date": target_date,
        "started_at": started_at,
        "collectors": {},
    }

    # Load the constituents MorningEnrich just refreshed to S3 (post-prune
    # universe scope for daily_append's expected-ticker check). S3 → Wikipedia
    # fallback, mirroring _run_daily.
    tickers: list[str] = []
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    try:
        existing = constituents.load_from_s3(bucket, market_prefix)
        if existing:
            tickers = existing.get("tickers", [])
            logger.info("Loaded %d tickers from S3 constituents", len(tickers))
    except Exception as exc:
        logger.warning("S3 constituents load failed — will try Wikipedia fallback: %s", exc)
    if not tickers:
        try:
            tickers, _, _, _, _ = constituents._fetch_constituents()
            logger.info("Loaded %d tickers from Wikipedia (S3 fallback)", len(tickers))
        except Exception as exc:
            logger.error("Wikipedia constituents fallback failed: %s", exc)

    if not tickers:
        logger.error("No tickers available for ArcticDB append")
        results["status"] = "failed"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        return results

    logger.info("=" * 60)
    logger.info("APPENDING: ArcticDB universe (arctic-append state, %s)", target_date)
    logger.info("=" * 60)
    reg = _build_registry(config, args, target_date)
    try:
        from builders.daily_append import daily_append
        with _maybe_phase(reg, "morning_arcticdb"):
            arctic_result = daily_append(
                date_str=target_date,
                bucket=bucket,
                dry_run=dry_run,
                expected_tickers=tickers,
            )
        results["collectors"]["arcticdb"] = arctic_result
        # daily_append returns its own status; surface it as the load-bearing
        # verdict so a write failure halts the pipeline (predictor reads next).
        _status = arctic_result.get("status", "unknown")
        results["status"] = "ok" if _status in ("ok", "ok_dry_run") else "failed"
    except Exception as e:
        logger.exception("ArcticDB daily_append (arctic-append state) failed for %s", target_date)
        results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}
        results["status"] = "failed"

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("ArcticDB append %s for %s", results["status"].upper(), target_date)
    return results


def _run_chronic_gap_heal(config: dict, args: argparse.Namespace) -> dict:
    """Best-effort chronic-polygon-gap drift detection + yfinance self-heal.

    Split out of :func:`_run_morning_enrich` (2026-06-11) into its own
    weekday-SF state (``ChronicGapSelfHeal``). Yfinance-backfills any ArcticDB
    universe row gap for the chronic-gap tickers (BF-B/BRK-B/MOG-A/PSTG by
    default — see config; polygon does not reliably serve them) and emits the
    polygon-recovery + constituents-drift alarms.

    Runs AFTER MorningEnrich's load-bearing daily_append, as a fail-soft SF
    state, so an unbounded ``yf.download`` hang here (the 2026-06-11 SIGKILL
    incident) can never run out MorningEnrich's SSM ``executionTimeout`` and
    throw away completed daily_append work. The standing rule: a best-effort
    downstream step must never force re-running a completed upstream task.

    NEVER raises — the whole body is wrapped so that any unexpected failure
    returns ``status="error"`` rather than propagating a non-zero exit that
    the SF would (correctly, but pointlessly here) treat as a state failure.
    The SF Catch makes a failed state non-fatal regardless; this is
    defence-in-depth so the SSM command itself exits 0. Postflight remains the
    load-bearing freshness gate — a still-stale chronic ticker surfaces there.
    """
    bucket = config["bucket"]
    started_at = datetime.now(timezone.utc).isoformat()

    # Mirror _run_morning_enrich's PT-aware target-date resolution so the heal
    # targets the same trading day the enrich just wrote.
    if args.date is None:
        from zoneinfo import ZoneInfo
        target_date = _previous_trading_day(
            reference=datetime.now(ZoneInfo("America/Los_Angeles"))
        )
    else:
        target_date = args.date

    dry_run = args.dry_run
    daily_cfg = config.get("daily_closes", {})
    results: dict = {
        "mode": "chronic_gap_heal",
        "date": target_date,
        "started_at": started_at,
        "collectors": {},
    }

    try:
        chronic_tickers = _load_chronic_polygon_gaps(config)
        if not chronic_tickers:
            logger.info("chronic-gap heal: no chronic_polygon_gaps configured — nothing to do.")
            results["status"] = "ok"
            results["completed_at"] = datetime.now(timezone.utc).isoformat()
            return results

        # Drift alarm: detect polygon recovery for chronic tickers (BEFORE
        # self-heal so the signal is a clean read of what polygon shipped
        # today, not contaminated by our yfinance backfill). Best-effort,
        # observability only — never raises.
        try:
            drift_result = _detect_chronic_gap_polygon_recovery(
                bucket=bucket,
                target_date=target_date,
                chronic_tickers=chronic_tickers,
                daily_closes_prefix=daily_cfg.get("s3_prefix", "staging/daily_closes/"),
            )
            results["collectors"]["chronic_gap_drift_detection"] = drift_result
        except Exception as e:
            logger.warning("Chronic-gap drift detection failed (non-blocking): %s", e)
            results["collectors"]["chronic_gap_drift_detection"] = {
                "status": "skipped",
                "error": str(e),
            }

        # Drift GATE: filter out chronic tickers that have dropped out of
        # the current constituents set. The heal path ends in
        # ``backfill(ticker_filter=...)``, which hard-errs against the
        # constituents filter for non-constituents (2026-05-27 PSTG
        # flow-doctor alert origin). Filtering here closes the loop so a
        # config that lags a constituents change becomes a WARN + skip
        # instead of a hard ERROR. Best-effort — a read failure falls
        # through with the original list and the existing backfill-side
        # error remains the load-bearing surface.
        try:
            cdrift_result = _detect_chronic_gap_constituents_drift(
                bucket=bucket,
                chronic_tickers=chronic_tickers,
            )
            results["collectors"]["chronic_gap_constituents_drift"] = cdrift_result
            chronic_tickers = cdrift_result["still_constituents"]
        except Exception as e:
            logger.warning("Chronic-gap constituents drift check failed (non-blocking): %s", e)
            results["collectors"]["chronic_gap_constituents_drift"] = {
                "status": "skipped",
                "error": str(e),
            }

        logger.info("=" * 60)
        logger.info(
            "SELF-HEAL: chronic polygon coverage gaps (%d ticker(s): %s)",
            len(chronic_tickers), ", ".join(chronic_tickers),
        )
        logger.info("=" * 60)
        try:
            # Hard wall-clock bound (L4605): yf.download carries timeout=30 but
            # the heal's builders.backfill() call is an unbounded second network
            # path. This watchdog caps the WHOLE per-ticker heal loop so an
            # infinite hang becomes a bounded best-effort skip — the
            # load-bearing surface stays MorningEnrich (weekday: a separate
            # fail-soft SF state; Saturday: DataPhase1's postflight), never a
            # SIGKILL of the inline Saturday MorningEnrich.
            with _hard_timeout(_CHRONIC_HEAL_HARD_TIMEOUT_S, "chronic-gap self-heal"):
                heal_result = _self_heal_chronic_polygon_gaps(
                    bucket=bucket,
                    target_date=target_date,
                    chronic_tickers=chronic_tickers,
                    dry_run=dry_run,
                )
            # Always "ok" by design — chronic-gap self-heal is best-effort.
            results["collectors"]["chronic_gap_self_heal"] = {
                "status": "ok",
                **heal_result,
            }
            logger.info(
                "chronic-gap self-heal: %d healed, %d already-fresh, %d errors",
                len(heal_result["healed"]),
                len(heal_result["skipped_already_fresh"]),
                len(heal_result["errors"]),
            )
        except _HardTimeout as e:
            # Best-effort: a hung heal must not fail the pipeline. Postflight
            # (Saturday) catches any still-stale chronic ticker as the loud gate.
            logger.warning(
                "Chronic-gap self-heal hit the %ds hard timeout for %s — "
                "skipping (best-effort); postflight remains the freshness gate. %s",
                _CHRONIC_HEAL_HARD_TIMEOUT_S, target_date, e,
            )
            results["collectors"]["chronic_gap_self_heal"] = {
                "status": "skipped",
                "error": str(e),
            }
        except Exception as e:
            logger.exception("Chronic-gap self-heal step failed for %s", target_date)
            results["collectors"]["chronic_gap_self_heal"] = {
                "status": "error",
                "error": str(e),
            }
    except Exception as e:  # defence-in-depth — this state must exit 0
        logger.exception("Chronic-gap heal wrapper failed for %s", target_date)
        results["collectors"]["chronic_gap_heal_wrapper"] = {
            "status": "error",
            "error": str(e),
        }

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    # Best-effort step: report ok unless the whole thing fell over. Per-ticker /
    # per-substep failures are recorded in collectors but do not flip the state
    # to failed (the SF Catch makes it non-fatal either way).
    results["status"] = "ok"
    statuses = {
        k: v.get("status", "?") for k, v in results["collectors"].items()
    }
    logger.info("Chronic-gap heal complete for %s: %s", target_date, statuses)
    return results


# Macro symbols are not S&P constituents but are core daily predictor inputs
# (vix_level, vix_term_slope, yield_10y, yield_curve_slope, sector-relative
# features). Appending them lets builders/daily_append.py update the ArcticDB
# macro library every weekday — pre-ArcticDB, the predictor Lambda fetched these
# from yfinance on each run; post-migration, the write path moved here. ETFs
# come from polygon; indices (^-prefix) fall through to FRED then yfinance in
# daily_closes.collect. Shared by the EOD --daily collector and the split-out
# --daily-arctic-append state so both pass daily_append the SAME expected_tickers.
_MACRO_DAILY_TICKERS = [
    "SPY", "GLD", "USO",
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
    "XLP", "XLRE", "XLU", "XLV", "XLY",
    "^VIX", "^VIX3M", "^TNX", "^IRX",
]


def _load_daily_universe_tickers(config: dict) -> list[str]:
    """Load the daily universe (S3 constituents → Wikipedia fallback) plus the
    macro daily tickers. Shared by :func:`_run_daily` and
    :func:`_run_daily_arctic_append` so a split EOD run (PostMarketData computes,
    PostMarketArcticAppend appends) feeds daily_append the identical
    expected-ticker scope. Returns ``[]`` when no constituents are resolvable —
    callers treat that as a hard failure."""
    tickers: list[str] = []
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    try:
        existing = constituents.load_from_s3(config["bucket"], market_prefix)
        if existing:
            tickers = existing.get("tickers", [])
            logger.info("Loaded %d tickers from S3 constituents", len(tickers))
    except Exception as exc:
        logger.warning("S3 constituents load failed — will try Wikipedia fallback: %s", exc)
    if not tickers:
        try:
            tickers, _, _, _, _ = constituents._fetch_constituents()
            logger.info("Loaded %d tickers from Wikipedia (S3 fallback)", len(tickers))
        except Exception as exc:
            logger.error("Wikipedia constituents fallback failed: %s", exc)
    if not tickers:
        return []
    return list(dict.fromkeys(tickers + _MACRO_DAILY_TICKERS))


def _run_daily(config: dict, args: argparse.Namespace) -> dict:
    """Daily mode: capture today's OHLCV closes for all tracked tickers."""
    bucket = config["bucket"]
    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dry_run = args.dry_run
    daily_cfg = config.get("daily_closes", {})
    reg = _build_registry(config, args, run_date)

    results: dict = {
        "mode": "daily",
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    tickers = _load_daily_universe_tickers(config)
    if not tickers:
        logger.error("No tickers available for daily closes")
        results["status"] = "failed"
        return results

    logger.info("=" * 60)
    logger.info("COLLECTING: daily closes")
    logger.info("=" * 60)
    dc_started_at = datetime.now(timezone.utc)
    # Windowed-reconciliation knobs from config — default window_days=1
    # preserves single-date legacy behavior. When N > 1, the EOD yfinance
    # pass scans the last N BDays, filling NaN cells per the source-
    # precedence ladder. With skip_if_canonical=true, tickers that
    # already have an authoritative source skip the yfinance fetch
    # entirely so the batch cost stays near zero in steady state.
    window_days = int(daily_cfg.get("window_days", 1))
    skip_if_canonical = bool(daily_cfg.get("skip_if_canonical", False))
    _dc_prefix = daily_cfg.get("s3_prefix", "staging/daily_closes/")
    # EOD pass uses yfinance_only — polygon free-tier 403's same-day, and
    # silently substituting yfinance was the 2026-04-17 → 2026-04-23 VWAP
    # outage. Morning polygon_only enrichment (separate SF step) fills VWAP
    # the next morning by overwriting these rows with polygon's authoritative
    # OHLCV+VWAP. See module docstring on collectors/daily_closes.py.
    results["collectors"]["daily_closes"] = _phase_collect(
        reg, "daily_closes",
        lambda: daily_closes.collect(
            bucket=bucket,
            tickers=tickers,
            run_date=run_date,
            s3_prefix=_dc_prefix,
            dry_run=dry_run,
            source="yfinance_only",
            window_days=window_days,
            skip_if_canonical=skip_if_canonical,
        ),
        artifact_key=f"{_dc_prefix}{run_date}.parquet",
    )

    # Metron market-data producer — EOD closes + FX for Metron's held-ticker universe.
    # `alpha-engine-data` is the single market-data ground truth for the NE system;
    # Metron reads these artifacts (it makes no direct market-data API calls). Reads its
    # own universe from s3://<bucket>/metron/holdings_universe.json (fail-soft → skipped
    # when absent), independent of the constituent `tickers` above.
    results["collectors"]["metron_market_data"] = _phase_collect(
        reg, "metron_market_data",
        lambda: metron_market_data.collect(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.CLOSES_PREFIX}{run_date}.json",
    )
    # Per-symbol close-history + per-currency FX-history for Metron's NAV reconstruction +
    # as-of-date realized/dividend FX. Per-symbol keys (no single stable artifact) →
    # markers + watchdog only, no auto-skip.
    results["collectors"]["metron_market_data_history"] = _phase_collect(
        reg, "metron_market_data_history",
        lambda: metron_market_data.collect_history(bucket=bucket, dry_run=dry_run),
        supports_auto_skip=False,
    )
    # GICS sectors + SPY weights + earnings dates — Metron's last external fetches, now
    # on the spine so Metron reads ALL market/reference data from `data`.
    results["collectors"]["metron_reference_data"] = _phase_collect(
        reg, "metron_reference_data",
        lambda: metron_market_data.collect_reference(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.SECTORS_PREFIX}latest.json",
    )
    # Macro indicators (FRED observation series) for Metron's Macro page — Metron's last
    # direct external fetch, now on the spine.
    results["collectors"]["metron_macro_data"] = _phase_collect(
        reg, "metron_macro_data",
        lambda: metron_market_data.collect_macro(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.MACRO_PREFIX}latest.json",
    )
    # Tearsheet fundamentals (multiples + balance-sheet ratios) for Metron's held
    # universe — config#1022. Daily cadence; yfinance pass-through values; the
    # 15-min intraday family (config#1023) runs OUTSIDE this pipeline via a systemd
    # timer on the trading box (infrastructure/systemd/metron-intraday.timer).
    results["collectors"]["metron_fundamentals_data"] = _phase_collect(
        reg, "metron_fundamentals_data",
        lambda: metron_market_data.collect_fundamentals(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.FUNDAMENTALS_PREFIX}latest.json",
    )

    # Module health stamp for daily_data — scoped to daily_closes only. The
    # executor gate at alpha-engine/executor/main.py reads this key to decide
    # whether upstream data is fresh. Emitted on both ok and failure paths
    # so downstream can distinguish "ran and failed" from "hasn't run".
    if not dry_run:
        _dc = results["collectors"]["daily_closes"]
        _dc_status = _dc.get("status", "unknown")
        _dc_ok = _dc_status in ("ok", "ok_dry_run")
        _dc_duration = (datetime.now(timezone.utc) - dc_started_at).total_seconds()
        _write_module_health(
            bucket,
            module_name="daily_data",
            run_date=run_date,
            status="ok" if _dc_ok else "failed",
            summary={
                "tickers_captured": _dc.get("tickers_captured", 0),
                "polygon": _dc.get("polygon", 0),
                "fred": _dc.get("fred", 0),
                "yfinance": _dc.get("yfinance", 0),
            },
            error=None if _dc_ok else _dc.get("error", f"daily_closes status={_dc_status}"),
            duration_seconds=_dc_duration,
        )

    # ── Feature store compute ───────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("COMPUTING: feature store snapshot")
    logger.info("=" * 60)
    from features.compute import compute_and_write
    results["collectors"]["features"] = _phase_collect(
        reg, "features",
        lambda: compute_and_write(date_str=run_date, bucket=bucket, dry_run=dry_run),
        artifact_key=f"features/{run_date}/schema_version.json",
    )

    # ── ArcticDB daily append ────────────────────────────────────────────────
    # EOD post-market path: yfinance closes are immutable once written, so
    # re-runs short-circuit on tickers whose target-date row already lives
    # in ArcticDB. skip_if_exists=True keeps re-runs cheap (microsecond
    # in-memory check vs. 904 × ~1.5s slow lib.write rewrites — the path
    # that timed out the 2026-05-01 EOD SF rerun at the SSM 1200s ceiling).
    # MorningEnrich runs (_run_morning_enrich, polygon source) leave the
    # default False so polygon's true VWAP overwrites yfinance's NaN.
    #
    # --skip-arctic-append: the EOD SF runs the slow daily_append as its own
    # load-bearing PostMarketArcticAppend state (longer timeout decoupled from
    # the feature compute), exactly mirroring the weekday MorningEnrich +
    # MorningArcticAppend split (L4608). Without the flag (the Saturday DataPhase
    # path) the append still runs inline here. 2026-06-16: the monolithic
    # --daily run exceeded PostMarketData's 1200s SSM ceiling mid-append → SIGKILL.
    if getattr(args, "skip_arctic_append", False):
        logger.info("Skipping inline ArcticDB append (--skip-arctic-append; "
                    "runs as the EOD SF PostMarketArcticAppend state)")
    else:
        logger.info("=" * 60)
        logger.info("APPENDING: ArcticDB universe (daily)")
        logger.info("=" * 60)
        from builders.daily_append import daily_append
        # supports_auto_skip=False: ArcticDB write (no S3 key) + already cheap on
        # re-run via skip_if_exists=True → markers + watchdog only.
        results["collectors"]["arcticdb"] = _phase_collect(
            reg, "arcticdb",
            lambda: daily_append(
                date_str=run_date,
                bucket=bucket,
                dry_run=dry_run,
                skip_if_exists=True,
                expected_tickers=tickers,
            ),
            supports_auto_skip=False,
        )

    results["completed_at"] = datetime.now(timezone.utc).isoformat()

    # Status
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    if all(s in ("ok", "ok_dry_run") for s in statuses):
        results["status"] = "ok"
    else:
        results["status"] = "failed"

    # Health marker
    if not dry_run and results["status"] == "ok":
        _write_health_marker(bucket, 0, run_date, "ok")

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        pass
    logger.info("Daily collection %s: %s%s", results["status"].upper(),
                ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
                duration)

    return results


def _run_daily_arctic_append(config: dict, args: argparse.Namespace) -> dict:
    """Standalone ArcticDB universe append for the EOD post-market path.

    The daily/EOD twin of :func:`_run_morning_arctic_append`. Split out of
    :func:`_run_daily` (2026-06-16) into its own EOD-SF state
    (``PostMarketArcticAppend``): the monolithic ``--daily`` run does
    daily_closes + metron collectors + feature-store compute + the SLOW
    ``daily_append`` in one shot, and on 2026-06-16 it exceeded PostMarketData's
    1200s SSM ``executionTimeout`` mid-append → SIGKILL → the whole EOD pipeline
    failed (no reconcile, no EOD email). As its own state the append gets a
    longer timeout decoupled from the feature compute, exactly mirroring the
    weekday MorningEnrich + MorningArcticAppend split (L4608).

    LOAD-BEARING: EOD reconcile + predictor inference read the ArcticDB universe
    after this, so an append failure returns ``status="failed"`` → ``main()``
    exits 1 → the SF's ``CheckPostMarketArcticAppendStatus`` routes to
    HandleFailure.

    Targets the same date as :func:`_run_daily` (today's UTC date, or ``--date``)
    and passes ``skip_if_exists=True`` so an operator rerun short-circuits
    tickers whose row already landed — identical semantics to the inline block
    it replaces (the Saturday DataPhase path still appends inline via
    ``--daily`` without ``--skip-arctic-append``). PostMarketData writes today's
    daily_closes parquet that this append reads, so this state runs after it.
    """
    bucket = config["bucket"]
    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dry_run = args.dry_run
    results: dict = {
        "mode": "daily_arctic_append",
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    tickers = _load_daily_universe_tickers(config)
    if not tickers:
        logger.error("No tickers available for ArcticDB append")
        results["status"] = "failed"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        return results

    logger.info("=" * 60)
    logger.info("APPENDING: ArcticDB universe (daily-arctic-append state, %s)", run_date)
    logger.info("=" * 60)
    reg = _build_registry(config, args, run_date)
    try:
        from builders.daily_append import daily_append
        with _maybe_phase(reg, "arcticdb"):
            arctic_result = daily_append(
                date_str=run_date,
                bucket=bucket,
                dry_run=dry_run,
                skip_if_exists=True,
                expected_tickers=tickers,
            )
        results["collectors"]["arcticdb"] = arctic_result
        # daily_append returns its own status; surface it as the load-bearing
        # verdict so a write failure halts the pipeline (reconcile reads next).
        _status = arctic_result.get("status", "unknown")
        results["status"] = "ok" if _status in ("ok", "ok_dry_run") else "failed"
    except Exception as e:
        logger.exception("ArcticDB daily_append (daily-arctic-append state) failed for %s", run_date)
        results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}
        results["status"] = "failed"

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("ArcticDB append %s for %s", results["status"].upper(), run_date)
    return results


def _finalize(
    results: dict,
    bucket: str,
    market_prefix: str,
    run_date: str,
    dry_run: bool,
    only: str | None,
) -> None:
    """Compute status, write manifest, log summary."""
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    if all(s in ("ok", "ok_dry_run") for s in statuses):
        results["status"] = "ok"
    elif any(s == "error" for s in statuses):
        results["status"] = "partial" if any(s == "ok" for s in statuses) else "failed"
    else:
        results["status"] = "partial"

    # Surface per-collector errors to Flow Doctor's ERROR-level handler.
    # Without this, the only logger.error() that fires on a partial run is
    # main()'s generic "non-ok status" summary line — single dedup signature
    # across every partial run, no diagnose-context error text. The helper
    # emits one logger.error per error-status entry with the collector name
    # + original message, restoring per-failure alert granularity.
    from alpha_engine_lib.collector_results import report_collector_errors
    report_collector_errors(results["collectors"])

    if not dry_run and only is None:
        _write_manifest(bucket, market_prefix, run_date, results)
        _write_validation_json(bucket, market_prefix, run_date, results)

    # Postflight: producer-side hard-fail if the outputs we just wrote
    # don't satisfy the consumer contracts downstream modules will enforce
    # at their own preflight. Fails before any downstream Lambda cold-start
    # or spot-EC2 bootstrap. See validators/postflight.py for the full
    # contract spec and the ROADMAP item that motivates it.
    phase = results.get("phase")
    if (
        not dry_run
        and phase == 1  # Only DataPhase1 is gated today; Phase 2 gets its own postflight.
        and only is None
        and results["status"] == "ok"
    ):
        from validators.postflight import DataPostflight, PostflightError
        try:
            DataPostflight(
                bucket=bucket,
                run_date=run_date,
                market_prefix=market_prefix,
                phase=phase,
            ).run()
        except PostflightError as exc:
            logger.error(
                "DataPhase%d POSTFLIGHT FAILED: %s — consumer contracts not met. "
                "Refusing to signal Step Function success.",
                phase, exc,
            )
            results["status"] = "postflight_failed"
            results["postflight_error"] = str(exc)

    # Write health marker for Step Functions
    if not dry_run and phase and only is None:
        _write_health_marker(bucket, phase, run_date, results["status"])

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        pass

    phase_label = f"Phase {phase} " if phase else ""
    logger.info(
        "%scollection %s: %s%s",
        phase_label,
        results["status"].upper(),
        ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
        duration,
    )

    # Send completion email.
    # send_step_email never raises (see emailer.py docstring) — it returns
    # True/False. The old try/except was dead code, AND the False return
    # was being silently dropped. If Gmail SMTP AND SES both fail, the
    # caller needs to know so monitoring isn't blind to a successful run
    # that silently had no notification.
    if not dry_run and only is None:
        from emailer import send_step_email
        step_name = f"Data Phase {phase}" if phase else "Data Collection"
        sent = send_step_email(step_name, results, run_date)
        if not sent:
            # Log at ERROR so CloudWatch alarms (if wired to ERROR-level)
            # surface the missed email. Not raising because the data
            # collection itself succeeded — only monitoring is affected.
            # Downstream Step Function steps can still consume the S3 output.
            logger.error(
                "Step email '%s' failed to send — both Gmail SMTP and SES "
                "fallback returned failure. Monitoring will be blind to "
                "this run's result summary. Check EMAIL_SENDER, "
                "EMAIL_RECIPIENTS, GMAIL_APP_PASSWORD env vars and SES "
                "identity verification.",
                step_name,
            )


def _write_manifest(bucket: str, s3_prefix: str, run_date: str, results: dict) -> None:
    """Write manifest.json and update latest_weekly.json pointer."""
    s3 = boto3.client("s3")

    # Manifest
    manifest_key = f"{s3_prefix}weekly/{run_date}/manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(results, indent=2, default=str),
        ContentType="application/json",
    )

    # Latest pointer
    pointer = {"date": run_date, "s3_prefix": f"{s3_prefix}weekly/{run_date}/"}
    s3.put_object(
        Bucket=bucket,
        Key=f"{s3_prefix}latest_weekly.json",
        Body=json.dumps(pointer, indent=2),
        ContentType="application/json",
    )
    logger.info("Wrote manifest + latest pointer for %s", run_date)


def _write_validation_json(
    bucket: str, s3_prefix: str, run_date: str, results: dict,
) -> None:
    """Aggregate validation results from all collectors and write to S3."""
    collectors = results.get("collectors", {})
    validations: dict[str, dict] = {}

    for name, info in collectors.items():
        val = info.get("validation")
        if val:
            validations[name] = val

    if not validations:
        return

    total_validated = sum(v.get("total_validated", 0) for v in validations.values())
    total_anomalies = sum(v.get("anomalies", 0) for v in validations.values())
    total_clean = sum(v.get("clean", 0) for v in validations.values())

    payload = {
        "date": run_date,
        "total_validated": total_validated,
        "total_clean": total_clean,
        "total_anomalies": total_anomalies,
        "collectors": validations,
    }

    s3 = boto3.client("s3")
    key = f"{s3_prefix}weekly/{run_date}/validation.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, default=str),
        ContentType="application/json",
    )
    logger.info(
        "Wrote validation.json: %d validated, %d anomalies → s3://%s/%s",
        total_validated, total_anomalies, bucket, key,
    )


def _write_health_marker(bucket: str, phase: int, run_date: str, status: str) -> None:
    """Write phase-based health marker (legacy) for Step Functions dependency checking."""
    s3 = boto3.client("s3")
    key = f"health/data_phase{phase}.json"
    marker = {
        "phase": phase,
        "date": run_date,
        "status": status,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(marker, indent=2),
        ContentType="application/json",
    )
    logger.info("Wrote health marker: s3://%s/%s", bucket, key)


def _write_module_health(
    bucket: str,
    module_name: str,
    run_date: str,
    status: str,
    *,
    summary: dict | None = None,
    warnings: list | None = None,
    error: str | None = None,
    duration_seconds: float = 0.0,
) -> None:
    """Write module-scoped health stamp consumed by the executor's
    check_upstream_health() (alpha-engine/executor/health_status.py:91).

    Schema matches executor's write_health() — key pattern
    `health/{module_name}.json` with `last_success` nulled on failure so
    downstream staleness checks can distinguish "ran and failed today" from
    "hasn't run in N hours". Called on both success AND failure paths so the
    stamp reflects the actual last run outcome.
    """
    s3 = boto3.client("s3")
    key = f"health/{module_name}.json"
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "module": module_name,
        "status": status,
        "last_success": now_iso if status != "failed" else None,
        "run_date": run_date,
        "duration_seconds": round(duration_seconds, 1),
        "summary": summary or {},
        "warnings": warnings or [],
        "error": error,
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Wrote module health: s3://%s/%s (%s)", bucket, key, status)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alpha Engine Weekly Data Collector")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing to S3")
    parser.add_argument(
        "--preflight-only", dest="preflight_only", action="store_true",
        help="Run ONLY the entry preflight (DataPreflight: env/secret resolution, "
             "S3 HEAD, polygon/FRED auth-reachability probes, ArcticDB connect + "
             "libraries-present read) then exit 0 BEFORE run_weekly(). No collector "
             "fetch, no S3/ArcticDB/parquet/config write. Friday shell-run dry path "
             "(ROADMAP 'Friday shell-run — per-module dry-path activation' #1) — "
             "catches bootstrap-class breakage ~12h before the real Saturday run.",
    )
    parser.add_argument("--date", default=None, help="Override run date (YYYY-MM-DD)")
    parser.add_argument(
        "--daily", action="store_true",
        help="Daily mode: capture today's OHLCV closes for all tickers (yfinance-only EOD pass).",
    )
    parser.add_argument(
        "--morning-enrich", dest="morning_enrich", action="store_true",
        help="Morning polygon enrichment: overwrite the prior trading day's parquet + ArcticDB row "
             "with polygon's authoritative OHLCV+VWAP. Hard-fails on polygon failure (no yfinance "
             "fallback). --date overrides which trading day to enrich (default: previous trading day).",
    )
    parser.add_argument(
        "--chronic-gap-heal", dest="chronic_gap_heal", action="store_true",
        help="Best-effort: yfinance-backfill ArcticDB row gaps for the chronic-polygon-gap "
             "tickers (polygon doesn't reliably serve them) + emit the polygon-recovery / "
             "constituents-drift alarms. Split out of --morning-enrich (2026-06-11) so a "
             "yfinance hang in this best-effort step can never SIGKILL the load-bearing "
             "MorningEnrich. Never raises — returns a status dict. --date overrides target day.",
    )
    parser.add_argument(
        "--skip-chronic-heal", dest="skip_chronic_heal", action="store_true",
        help="With --morning-enrich: skip the inline chronic-gap self-heal "
             "(the weekday SF runs it as a separate fail-soft ChronicGapSelfHeal "
             "state instead). The Saturday SF omits this flag so the heal still "
             "runs inline before DataPhase1's postflight.",
    )
    parser.add_argument(
        "--skip-arctic-append", dest="skip_arctic_append", action="store_true",
        help="With --morning-enrich: skip the inline ArcticDB daily_append "
             "(the weekday SF runs it as a separate load-bearing MorningArcticAppend "
             "state with a longer timeout). The Saturday SF omits this flag so the "
             "append still runs inline before DataPhase1's postflight.",
    )
    parser.add_argument(
        "--morning-arctic-append", dest="morning_arctic_append", action="store_true",
        help="Standalone ArcticDB universe append for the prior trading day "
             "(the slow daily_append split out of --morning-enrich, L4608). "
             "Load-bearing: exits 1 on append failure. --date overrides target day.",
    )
    parser.add_argument(
        "--daily-arctic-append", dest="daily_arctic_append", action="store_true",
        help="Standalone ArcticDB universe append for the EOD post-market path "
             "(the slow daily_append split out of --daily, 2026-06-16). The EOD SF "
             "runs --daily --skip-arctic-append (compute) then this state with a "
             "longer timeout. Load-bearing: exits 1 on append failure. Targets "
             "today's UTC date (or --date); skip_if_exists short-circuits reruns.",
    )
    parser.add_argument(
        "--phase", type=int, choices=[1, 2], default=None,
        help="Phase 1: pre-research data. Phase 2: post-research alternative data.",
    )
    parser.add_argument(
        "--only",
        choices=["constituents", "prices", "macro", "short_interest", "universe_returns", "alternative", "daily_closes", "features", "arcticdb"],
        help="Run a single collector instead of all",
    )
    # Phase-registry recovery controls (L4528 — markers under data/{date}/.phases/).
    # A recovery re-run of the same date auto-skips collectors whose marker is ok
    # AND whose declared S3 artifact still exists (L4524). These flags override that:
    parser.add_argument(
        "--skip-phases", dest="skip_phases", default="",
        help="CSV of phase names to force-SKIP this run (e.g. 'prices,features').",
    )
    parser.add_argument(
        "--force-phases", dest="force_phases", default="",
        help="CSV of phase names to force-RERUN even if a valid marker exists.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force-rerun ALL phases (ignore every completion marker).",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    # _load_dotenv() + setup_logging() already ran at module-top so import-time
    # errors in the collectors block are captured. Apply user-requested level.
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    config = load_config(args.config)

    # Pre-flight: fail fast on env / connectivity drift before starting
    # the real collection work. See alpha-engine-lib/README.md.
    from preflight import DataPreflight
    if getattr(args, "morning_enrich", False):
        # Dedicated morning_enrich mode (preflight-task-split 2026-05-16):
        # morning-enrich is its own Saturday SF task and needs a proper
        # UNION entry preflight (polygon + FRED secrets + reachability +
        # S3 writeable + ArcticDB libraries present). The previous
        # "daily" mapping only probed ArcticDB freshness and did NOT
        # validate polygon/FRED reachability, even though
        # _run_morning_enrich hits polygon — so a drifted key failed
        # 28min into the spot run instead of in <1s at the entry.
        mode = "morning_enrich"
    elif args.daily or getattr(args, "daily_arctic_append", False):
        # --daily-arctic-append reads the daily_closes PostMarketData wrote +
        # the ArcticDB universe libraries — same preflight surface as --daily.
        mode = "daily"
    else:
        mode = f"phase{args.phase or 1}"
    DataPreflight(config["bucket"], mode).run()

    # Friday shell-run dry path (ROADMAP "Friday shell-run — per-module
    # dry-path activation" owed-item #1). --preflight-only exits HERE,
    # immediately after the existing DataPreflight has passed and strictly
    # BEFORE run_weekly(). run_weekly() is the sole function in this module
    # that performs ANY collector fetch (polygon/FMP/FRED/yfinance) or ANY
    # S3 / ArcticDB / parquet / config / module-health write — gating in
    # front of it makes every fetch/write code path statically unreachable
    # under this flag. The preflight itself only does read-only / auth
    # probes (S3 HEAD, polygon/FRED reference-data auth calls that fetch no
    # collector data, ArcticDB list_libraries) plus an S3 PUT+DELETE
    # sentinel under preflight/ — that sentinel is the preflight's own
    # liveness probe, not a data write, and it self-cleans. No external
    # API data is fetched and no production artifact is mutated.
    if getattr(args, "preflight_only", False):
        logger.info(
            "Pre-flight passed; --preflight-only set — exiting 0 before "
            "run_weekly() (NO collector fetch, NO S3/ArcticDB/config write). "
            "Friday shell-run dry path: bootstrap-class breakage would have "
            "surfaced above."
        )
        raise SystemExit(0)

    results = run_weekly(config, args)

    # Hard-fail on any non-ok status — strict form of the no-silent-fails
    # rule applied while the system is unstable. `partial` previously exited
    # 0 which let SSM report Success and the Step Function march forward on
    # missing/corrupt data. See feedback_hard_fail_until_stable memory for
    # rationale. Lift this back to == "failed" only after the system is
    # demonstrably stable (multiple clean Saturday runs in a row).
    #
    # ``skipped`` is the deliberate-no-op status emitted by _run_morning_enrich
    # when invoked after 1:30pm PT on a trading day (polygon free-tier 403's
    # today's grouped-daily). Treated as success so spot_data_weekly.sh's
    # ``if ! ... exit 1`` check does not trip.
    if results["status"] not in ("ok", "skipped"):
        logger.error(
            "Weekly collection finished with non-ok status=%s — exiting 1 "
            "to halt the pipeline. Per-collector statuses: %s",
            results["status"],
            {k: v.get("status", "?") for k, v in results.get("collectors", {}).items()},
        )
        raise SystemExit(1)


if __name__ == "__main__":
    # Capture an uncaught crash via flow-doctor before re-raising
    # (no-ops when flow-doctor is inactive).
    with guard_entrypoint():
        main()
