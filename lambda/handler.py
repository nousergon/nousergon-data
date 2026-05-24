"""
Lambda entry point — Phase 2 alternative data collector.

Triggered by Step Functions after research produces signals.json.
Fetches alternative data (analyst, revisions, options, insider, institutional,
news) for promoted tickers (~25-30) and writes per-ticker JSON to S3.

Pass {"force": true} to bypass date checks (manual testing).
Pass {"dry_run": true} to validate without writing to S3.
"""

from __future__ import annotations

import logging
import os
import time
import traceback

# Structured logging + flow-doctor singleton via alpha-engine-lib (shared
# pattern across all 5 entrypoints; see executor/main.py for reference).
# When FLOW_DOCTOR_ENABLED=1, attaches a FlowDoctorHandler at ERROR so every
# log.error() call routes through flow-doctor's dispatch (email + GitHub
# issue with dedup + rate limits per flow-doctor.yaml).
#
# Path resolution: LAMBDA_TASK_ROOT (=/var/task in the Lambda image,
# where Dockerfile COPYs flow-doctor.yaml) takes precedence; falls back
# to two-dirs-up from this file for local dev (lambda/handler.py →
# repo root). Mirrors alpha-engine-research/lambda/handler.py.
from alpha_engine_lib.logging import setup_logging
_FLOW_DOCTOR_EXCLUDE_PATTERNS: list[str] = []
_FLOW_DOCTOR_YAML = os.path.join(
    os.environ.get(
        "LAMBDA_TASK_ROOT",
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ),
    "flow-doctor.yaml",
)
setup_logging(
    "data-phase2",
    flow_doctor_yaml=_FLOW_DOCTOR_YAML,
    exclude_patterns=_FLOW_DOCTOR_EXCLUDE_PATTERNS,
)

logger = logging.getLogger(__name__)


def handler(event, context):
    """
    AWS Lambda handler for Phase 2 alternative data collection.

    Event payload:
        phase: int (must be 2)
        date: str (optional, YYYY-MM-DD override)
        force: bool (bypass checks)
        dry_run: bool (validate without writing)

    Returns:
        dict with status: "OK" | "SKIPPED" | "ERROR"
    """
    os.environ.setdefault("XDG_CACHE_HOME", "/tmp")

    force = event.get("force", False)
    dry_run = event.get("dry_run", False)
    run_date = event.get("date")

    _start = time.time()

    try:
        # FINNHUB_API_KEY required since 2026-04-20 — analyst rating + price
        # target now come from Finnhub because FMP /stable moved those behind
        # a paid tier. EDGAR_IDENTITY and POLYGON_API_KEY are optional
        # (graceful degradation in their consumers).
        #
        # Read via get_secret() not os.environ.get() — the bulk-load shim
        # (ssm_secrets.load_secrets) was retired in PR 9f of the .env→SSM
        # arc (2026-05-14). Secrets now load from SSM at consumer sites.
        from alpha_engine_lib.secrets import get_secret
        missing = [
            name for name in ("FMP_API_KEY", "FINNHUB_API_KEY")
            if not get_secret(name, required=False, default="")
        ]
        if missing:
            msg = f"Missing required env vars: {', '.join(missing)}"
            logger.error(msg)
            return {"status": "ERROR", "error": msg}

        # Import collector (deferred to reduce cold-start time)
        import yaml
        from collectors import alternative

        # Load config
        config_path = os.environ.get("CONFIG_PATH", "config.yaml")
        if os.path.exists(config_path):
            with open(config_path) as f:
                config = yaml.safe_load(f)
        else:
            config = {"bucket": "alpha-engine-research", "market_data": {"s3_prefix": "market_data/"}}

        bucket = config.get("bucket", "alpha-engine-research")
        market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")

        # Run Phase 2
        result = alternative.collect(
            bucket=bucket,
            s3_prefix=market_prefix,
            run_date=run_date,
            dry_run=dry_run,
        )

        status = result.get("status", "error")
        duration = time.time() - _start

        # Write health marker
        if not dry_run and status in ("ok", "partial"):
            try:
                import json
                from datetime import datetime, timezone
                import boto3
                s3 = boto3.client("s3")
                s3.put_object(
                    Bucket=bucket,
                    Key="health/data_phase2.json",
                    Body=json.dumps({
                        "phase": 2,
                        "date": run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "status": status,
                        "duration_seconds": round(duration, 1),
                        "tickers_processed": result.get("tickers_processed", 0),
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    }, indent=2),
                    ContentType="application/json",
                )
            except Exception as he:
                logger.warning("Health marker write failed: %s", he)

        if status == "skipped" and dry_run:
            # Canary contract: the deploy.sh canary calls with dry_run=true to
            # verify Lambda boot + S3 read after a new version is published. If
            # there are no tickers for run_date (e.g., Sunday deploy before next
            # signals.json is written), the underlying collector correctly
            # returns status='skipped' — Lambda code is fine, the upstream
            # signals just don't exist yet. Map to canary-status 'SKIPPED'
            # (which deploy.sh:122 already accepts as canary-OK) instead of
            # collapsing into ERROR. Production invocations (dry_run=false)
            # still fall through to the ERROR branch so the Saturday SF's
            # DataPhase2 state surfaces a real "Research output empty" failure.
            logger.info("Phase 2 dry_run skipped — no tickers (canary-safe)")
            return {
                "status": "SKIPPED",
                "skip_reason": result.get("reason", "no tickers"),
                "duration_seconds": round(duration, 1),
                "dry_run": dry_run,
            }
        elif status in ("ok", "partial", "ok_dry_run"):
            logger.info(
                "Phase 2 complete in %.0fs: %s", duration,
                f"{result.get('tickers_processed', 0)} tickers processed"
            )
            return {
                "status": "OK",
                "tickers_processed": result.get("tickers_processed", 0),
                "tickers_failed": result.get("tickers_failed", 0),
                "duration_seconds": round(duration, 1),
                "dry_run": dry_run,
            }
        else:
            return {
                "status": "ERROR",
                "error": result.get("reason") or "collection failed",
                "duration_seconds": round(duration, 1),
            }

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Phase 2 failed: %s\n%s", e, tb)
        return {
            "status": "ERROR",
            "error": str(e),
            "duration_seconds": round(time.time() - _start, 1),
        }
