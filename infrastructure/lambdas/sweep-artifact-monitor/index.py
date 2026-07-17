"""alpha-engine-sweep-artifact-monitor — post-SF sweep-artifact validation
(alpha-engine-config#2392).

The `alpha-engine-groom-dispatch` SF's `DispatchEndOfSfSweep` state (config#2201
/ #2311) unconditionally reaches a Lambda invoke that fires ONE Haiku
`run_mode=sweep` spot box per trigger cycle, on all three of its paths (Map
success, zero-launches AllSkipped, and a caught Map-launch failure — see
`tests/test_sf_groom_end_of_sf_sweep_wiring.py` in this repo, which pins that
wiring). The sweep box is fire-and-forget: the Lambda returns as soon as the
async SSM command is delivered, and the SF SUCCEEDS long before the sweep box
itself finishes and writes its own S3 run artifact
(`groom/{date}/sweep-{HHMMSS}.json`, `run_kind="sweep"`,
`scripts/write_sweep_artifact.py` in `alpha-engine-config`). Nothing outside
the box itself previously cross-checked that the artifact actually landed — a
silent sweep-skip (IAM, a Lambda error, an SF Catch misroute, or the sweep box
dying before its own artifact-verify) went undetected until stale open PRs
were noticed by hand.

This Lambda closes that gap. It subscribes to EventBridge `Step Functions
Execution Status Change` = SUCCEEDED for `alpha-engine-groom-dispatch` (the
EventBridge rule's own event pattern already excludes FAILED/TIMED_OUT/
ABORTED — acceptance criterion 3: a failed groom-dispatch execution must never
trigger this check, since a failed Map iteration means the box outcome is
already loudly surfaced by Fleet-SF Watch through the execution's own FAILED
status, and the sweep dispatch's own Catch already SNS-notifies a dispatch-
launch failure separately).

On each SUCCEEDED execution:

1. Reads the execution's own OUTPUT (`describe_execution` — `$.sweep`, the
   `DispatchEndOfSfSweep` state's `ResultPath`) to determine what THIS cycle
   actually did, rather than guessing from S3 alone:

   - `sweep.reason == "concurrent_tier_skip"` (config#1979's concurrent
     guard — the dispatcher's `_launch_groom_spot` returns
     `{"launched": False, "reason": "concurrent_tier_skip", ...}` when a
     prior cycle's sweep box is still live under the `sweep` tier) means NO
     box was launched this cycle BY DESIGN — the issue's own documented
     gotcha. Only the NEXT cycle's sweep is expected to produce an artifact;
     alerting here would be a false positive. Skipped, not alerted.
   - `sweep.dispatched is False` (the SF's OWN `RecordSweepDispatchFailure`
     Pass state, reached via `DispatchEndOfSfSweep`'s Catch on a genuine
     launch error) means the SF's `NotifySweepDispatchFailure` state has
     ALREADY published an SNS alert for this exact failure — this Lambda
     must not double-alert the same incident under a different message.
     Skipped, not alerted (recorded as already-covered).
   - Anything else (a launched box — `sweep.launched is True`, or the field
     shape from a version of the payload this Lambda doesn't recognize) is
     the expected-artifact case: check S3.

2. For the expected-artifact case, lists
   `s3://alpha-engine-research/groom/{date}/` for `sweep-*.json` objects
   (the `write_sweep_artifact.py` key convention) and accepts the first one
   whose `run_kind == "sweep"` and `run_start` falls within
   `SWEEP_GRACE_MINUTES` (default 15 — the issue's own acceptance-criterion
   window) of the execution's `stopDate`. The search covers both the
   execution's own UTC completion date AND the prior UTC date, since a
   `stopDate` shortly after midnight UTC can have a `run_start` still dated
   the previous day (the sweep box's own `run_start` is stamped when Claude
   Code fires, a little before the SF's `stopDate`).

3. Missing artifact → publishes to the `alpha-engine-alerts` SNS topic
   (acceptance criterion 2) with the execution ARN + date so the operator can
   go straight to `aws stepfunctions describe-execution` / the S3 prefix.

Sunday has two `alpha-engine-groom-dispatch` triggers (0700 daily-mid and
0900 weekly-gated-reverify) — each is an INDEPENDENT SF execution, so each
gets its own EventBridge SUCCEEDED event and its own independent sweep-
artifact check here; no special-casing needed (the executionArn is always
unique per trigger).

Fail-loud (`feedback_no_silent_fails`): `describe_execution` / `list_objects_v2`
/ `get_object` failures RAISE — the EventBridge retry policy + a CloudWatch
alarm on Lambda errors page the operator rather than this check silently
skipping. `sns.publish` is the delivery surface for an already-detected
finding; its own failure still raises (an alert that silently failed to send
is exactly the failure class this Lambda exists to prevent going unnoticed).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

GROOM_SF_NAME = "alpha-engine-groom-dispatch"
S3_BUCKET = os.environ.get("S3_BUCKET", "alpha-engine-research")
SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN", f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"
)

# Issue-#2392 acceptance window: a sweep artifact's run_start must fall
# within this many minutes of the SF execution's completion (stopDate).
SWEEP_GRACE_MINUTES = int(os.environ.get("SWEEP_GRACE_MINUTES", "15"))


def _parse_sweep_result(output_raw: str) -> dict:
    """Parse the execution output's `sweep` key (DispatchEndOfSfSweep's
    ResultPath). Returns {} if absent/unparseable — treated the same as "an
    unrecognized launched box" (expect-artifact case) rather than silently
    skipping, since a malformed/missing $.sweep on a SUCCEEDED execution is
    itself suspicious and should fall through to the S3 check rather than be
    swallowed."""
    try:
        output = json.loads(output_raw or "{}")
    except (json.JSONDecodeError, ValueError):
        return {}
    sweep = output.get("sweep")
    return sweep if isinstance(sweep, dict) else {}


def _sweep_artifact_expected(sweep: dict) -> tuple[bool, str]:
    """Decide whether THIS cycle's SUCCEEDED execution should have produced a
    sweep artifact. Returns (expected, reason)."""
    if sweep.get("reason") == "concurrent_tier_skip":
        return False, (
            "sweep dispatch skipped by config#1979 concurrent guard — a prior "
            "cycle's sweep box was still live; only the NEXT cycle's sweep is "
            "expected"
        )
    if sweep.get("dispatched") is False:
        return False, (
            "sweep dispatch itself failed and was already recorded + "
            "SNS-notified by the SF's own NotifySweepDispatchFailure state "
            "(config#2201) — not double-alerting the same incident"
        )
    return True, "sweep box launch was dispatched (or outcome unrecognized) — artifact expected"


def _candidate_dates(stop_dt: timezone) -> list[str]:
    """The execution's own UTC completion date, plus the prior UTC date (a
    stopDate shortly after UTC midnight can have a run_start still dated the
    previous day — the sweep box's run_start is stamped when the agent
    fires, a little before the SF's own stopDate)."""
    today = stop_dt.date()
    return [today.isoformat(), (today - timedelta(days=1)).isoformat()]


def _find_sweep_artifact(
    stop_dt: datetime,
    *,
    s3_client: Optional[object] = None,
) -> Optional[dict]:
    """Search groom/{date}/sweep-*.json (today + yesterday, UTC) for a
    run_kind=sweep artifact whose run_start is within SWEEP_GRACE_MINUTES of
    stop_dt. Returns the matching artifact dict, or None if none found.
    Raises on an S3 API failure (fail-loud — see module docstring)."""
    if s3_client is None:  # pragma: no cover — production path
        s3_client = boto3.client("s3", region_name=REGION)

    window_start = stop_dt - timedelta(minutes=SWEEP_GRACE_MINUTES)
    window_end = stop_dt + timedelta(minutes=SWEEP_GRACE_MINUTES)

    for date in _candidate_dates(stop_dt):
        prefix = f"groom/{date}/sweep-"
        resp = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []) or []:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            artifact = json.loads(body)
            if artifact.get("run_kind") != "sweep":
                continue
            run_start_raw = artifact.get("run_start", "")
            if not run_start_raw:
                continue
            run_start = datetime.fromisoformat(run_start_raw.replace("Z", "+00:00"))
            if window_start <= run_start <= window_end:
                return {**artifact, "_s3_key": key}
    return None


def _publish_missing_sweep_alert(
    *, execution_arn: str, execution_name: str, stop_dt: datetime,
    sns_client: Optional[object] = None,
) -> None:
    if sns_client is None:  # pragma: no cover — production path
        sns_client = boto3.client("sns", region_name=REGION)
    message = (
        f"[ERROR] alpha-engine-sweep-artifact-monitor: no run_kind=sweep artifact "
        f"found for {GROOM_SF_NAME} execution {execution_name} "
        f"(completed {stop_dt.isoformat()}) within {SWEEP_GRACE_MINUTES} minutes. "
        f"Expected s3://{S3_BUCKET}/groom/{stop_dt.date().isoformat()}/sweep-*.json "
        f"(or the prior UTC date). The end-of-SF Haiku sweep may have silently "
        f"failed (IAM, Lambda error, sweep box crash before its own artifact-"
        f"verify) — investigate: `aws stepfunctions describe-execution "
        f"--execution-arn {execution_arn}` and "
        f"`aws s3 ls s3://{S3_BUCKET}/groom/{stop_dt.date().isoformat()}/`. "
        f"(alpha-engine-config#2392)"
    )
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Groom-dispatch sweep artifact missing"[:100],
        Message=message,
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    detail = event.get("detail") or {}
    sm_name = str(detail.get("stateMachineArn", "")).rsplit(":", 1)[-1]
    status = detail.get("status", "")

    # Acceptance criterion 3: never alert on FAILED (or any non-SUCCEEDED)
    # execution. The EventBridge rule itself is scoped to SUCCEEDED only, but
    # the handler re-checks so a manually-invoked/synthetic event with the
    # wrong status can never slip through.
    if sm_name != GROOM_SF_NAME or status != "SUCCEEDED":
        logger.info("ignored event: sm=%s status=%s", sm_name, status)
        return {"checked": False, "reason": "wrong_event"}

    execution_arn = detail.get("executionArn", "")
    execution_name = detail.get("name", "")
    sfn = boto3.client("stepfunctions", region_name=REGION)
    described = sfn.describe_execution(executionArn=execution_arn)
    output_raw = described.get("output") or "{}"
    sweep = _parse_sweep_result(output_raw)

    expected, reason = _sweep_artifact_expected(sweep)
    if not expected:
        logger.info(
            "sweep artifact NOT expected for %s: %s (sweep=%s)",
            execution_name, reason, sweep,
        )
        return {"checked": True, "expected": False, "reason": reason,
                "execution_arn": execution_arn}

    stop_date_ms = detail.get("stopDate")
    if stop_date_ms is not None:
        stop_dt = datetime.fromtimestamp(int(stop_date_ms) / 1000, tz=timezone.utc)
    else:
        stop_dt = datetime.now(timezone.utc)

    artifact = _find_sweep_artifact(stop_dt)
    if artifact is not None:
        logger.info(
            "sweep artifact found for %s: %s (run_start=%s)",
            execution_name, artifact["_s3_key"], artifact.get("run_start"),
        )
        return {"checked": True, "expected": True, "found": True,
                "artifact_key": artifact["_s3_key"], "execution_arn": execution_arn}

    logger.warning(
        "MISSING sweep artifact for %s (execution_arn=%s, stop=%s) — alerting",
        execution_name, execution_arn, stop_dt.isoformat(),
    )
    _publish_missing_sweep_alert(
        execution_arn=execution_arn, execution_name=execution_name, stop_dt=stop_dt,
    )
    return {"checked": True, "expected": True, "found": False,
            "alerted": True, "execution_arn": execution_arn}
