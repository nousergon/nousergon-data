"""
Lambda handler for the WeeklyPreflight pre-spend gate —
invoked by `ne-weekly-freshness-pipeline` before AcquireMutex.

Runs the sf_preflight checks against live AWS state and returns a pass/fail
verdict. Failures halt the pipeline; a pass proceeds to the mutex + spot
launch.

The Lambda runs read-only API calls (IAM SimulatePrincipalPolicy, Lambda
GetFunctionConfiguration, CloudWatch GetMetricStatistics, Step Functions
DescribeStateMachine) and zero-cost static analysis. ArcticDB/Polygon
checks gracefully skip (no VPC/database access from this Lambda).

Usage:
    Invoked by Step Functions. Returns:
    {"status": "OK", "has_violation": false}              → proceed
    {"status": "FAIL", "has_violation": true, ...}        → halt via ExtractWeeklyPreflightError
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from dataclasses import asdict

logger = logging.getLogger(__name__)

# Lambda-specific bucket override (not the same as the data-plane bucket,
# since this Lambda reads the SF definition, not the research data).
_SF_DEFINITION_BUCKET = os.environ.get("SF_DEFINITION_BUCKET", "alpha-engine-research")
_WEEKLY_SF_NAME = os.environ.get("WEEKLY_SF_NAME", "ne-weekly-freshness-pipeline")


def handler(event: dict, context) -> dict:
    """
    AWS Lambda handler for the weekly preflight.

    Event payload (from Step Functions):
        bucket: str (optional, S3 bucket override)
        sf_name: str (optional, state machine name override)

    Returns:
        dict with status, has_violation, and detailed results.
    """
    bucket = event.get("bucket", _SF_DEFINITION_BUCKET)
    sf_name = event.get("sf_name", _WEEKLY_SF_NAME)

    try:
        # Import sf_preflight — all heavy imports are deferred inside
        # individual check functions, so this never fails at module level.
        import sf_preflight as sfp  # type: ignore[import-untyped]
    except ImportError as exc:
        return {
            "status": "ERROR",
            "has_violation": True,
            "error": f"sf_preflight import failed: {exc}",
            "traceback": traceback.format_exc(),
        }

    try:
        n_fail, results = sfp.run_preflight(bucket=bucket)
    except Exception as exc:
        return {
            "status": "ERROR",
            "has_violation": True,
            "error": f"run_preflight raised: {exc}",
            "traceback": traceback.format_exc(),
        }

    result_dicts = [asdict(r) for r in results]
    fail_results = [r for r in result_dicts if r.get("status") == "fail"]
    warn_results = [r for r in result_dicts if r.get("status") == "warn"]

    if n_fail > 0:
        return {
            "status": "FAIL",
            "has_violation": True,
            "fail_count": n_fail,
            "warn_count": len(warn_results),
            "failures": [r["name"] for r in fail_results],
            "results": result_dicts,
        }

    return {
        "status": "OK",
        "has_violation": False,
        "warn_count": len(warn_results),
        "results": result_dicts,
    }
