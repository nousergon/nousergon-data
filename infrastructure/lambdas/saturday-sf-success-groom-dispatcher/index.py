"""alpha-engine-saturday-sf-success-groom-dispatcher — run the backlog groom
after a SUCCESSFUL Saturday SF.

Brian's ask (2026-06-26): once the Saturday pipeline completes — after the
Director has filed its weekly proposals — run the daily backlog groom again so
the fresh backlog state (including the Director's just-filed items) gets groomed
immediately, rather than waiting for the next scheduled 10pm-PT run. Added while
still building confidence that the daily groom is as thorough as intended.

Wiring (config#2175 — GHA groom execution retired fleet-wide): a dedicated
EventBridge rule on the Saturday SF's SUCCEEDED status targets this Lambda,
which fires an ASYNC (InvocationType=Event) boto3 invoke of
`alpha-engine-scheduled-groom-dispatcher` carrying a demand-all trigger event —
the SAME EC2-spot path every scheduled groom already uses. Demand-all evaluates
the fresh post-SF backlog per tier and launches 0..3 spot boxes, strictly
better than the old unconditional single FULL GHA run this Lambda used to
`repository_dispatch` to `backlog-groom.yml` (workflow deleted; the GitHub
PAT read + urllib dispatch machinery went with it — this Lambda needs no
GitHub credential at all now).

Best-effort with a recording surface (CLAUDE.md no-silent-fails secondary
carve-out): a Lambda-invoke outage logs WARN and is returned in the result but
does NOT raise — triggering the groom is a convenience, NOT the Saturday
pipeline's deliverable, and the scheduled groom cadence is the backstop.
EventBridge's delivery retries + the Lambda error metric still record a hard
failure.

Managed OUTSIDE CloudFormation (same rationale as the watch sibling): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect until
bootstrapped (and the reroute needs the iam-policy.json lambda:InvokeFunction
grant re-applied — see README).
"""

from __future__ import annotations

import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
# Kill-switch: set GROOM_DISPATCH_ENABLED=false on the Lambda to disable the
# trigger without removing the EventBridge rule. Default ON (Brian wants it live).
DISPATCH_ENABLED = os.environ.get("GROOM_DISPATCH_ENABLED", "true").lower() == "true"
# The scheduled-groom-dispatcher Lambda (this repo, sibling dir) — the single
# fleet chokepoint for launching groom EC2-spot boxes (config#1432/#2175).
GROOM_DISPATCHER_FUNCTION = os.environ.get(
    "GROOM_DISPATCHER_FUNCTION", "alpha-engine-scheduled-groom-dispatcher"
)
# The demand-all trigger event (config#2175): the dispatcher enumerates the
# fresh post-SF backlog per tier and launches 0..3 spot boxes — its own pace/
# demand gates apply, exactly as on the cron-scheduled triggers.
DISPATCH_EVENT = {
    "run_mode": "full",
    "trigger": "demand-all",
    "schedule": "saturday-sf-success",
}


def _dispatch_groom(execution_name: str, start_date) -> dict:  # noqa: ARG001 — kept for handler-contract stability
    """Async-invoke the scheduled-groom-dispatcher with the demand-all event.
    Best-effort: records the outcome, never raises (secondary path — the
    scheduled groom cadence is the backstop; recording surfaces are the WARN
    log + this returned result)."""
    if not DISPATCH_ENABLED:
        return {"dispatched": False, "reason": "disabled"}
    try:
        lam = boto3.client("lambda", region_name=REGION)
        resp = lam.invoke(
            FunctionName=GROOM_DISPATCHER_FUNCTION,
            InvocationType="Event",  # async — never babysit the multi-hour groom
            Payload=json.dumps(DISPATCH_EVENT).encode("utf-8"),
        )
        status_code = resp["StatusCode"]
        logger.info(
            "groom dispatcher invoked async: function=%s status=%s event=%s",
            GROOM_DISPATCHER_FUNCTION,
            status_code,
            DISPATCH_EVENT,
        )
        return {"dispatched": True, "status_code": status_code}
    except Exception as exc:  # noqa: BLE001 — secondary path, recorded not raised (see docstring)
        logger.warning("groom dispatcher invoke failed (non-fatal): %s", exc)
        return {"dispatched": False, "error": f"{type(exc).__name__}: {exc}"}


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — fires only on Saturday SF SUCCEEDED, per the
    dedicated rule `alpha-engine-saturday-succeeded-groom`. A defensive status
    re-check guarantees a mis-scoped rule can never fire the groom on a
    non-success terminal state.
    """
    detail = event.get("detail") or {}
    status = detail.get("status", "UNKNOWN")
    sm_name = (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1]
    execution_name = detail.get("name", "")
    logger.info(
        "Saturday-SF success groom trigger: sf=%s status=%s exec=%s",
        sm_name,
        status,
        execution_name,
    )
    if status != "SUCCEEDED":
        logger.info("status %s != SUCCEEDED — no groom dispatch", status)
        return {"dispatched": False, "reason": f"status={status}"}
    result = _dispatch_groom(execution_name, detail.get("startDate"))
    return {"status": status, "groom": result}
