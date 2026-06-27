"""alpha-engine-scheduled-groom-dispatcher — fire the backlog groom on an
EventBridge-Scheduler-driven cadence (the reliable replacement for the GHA
`schedule:` crons).

Why this exists (config#1322): GitHub Actions `schedule:` is best-effort and
routinely fires late or is silently dropped, which defeats the DRAIN-PHASE
3×/day cadence (config#1312). Today's `0 23 * * *` run was never created, and
yesterday's fired ~2h late. EventBridge Scheduler gives guaranteed, on-time
firing with a CloudWatch trail.

Wiring mirrors the sibling `saturday-sf-success-groom-dispatcher`: three
EventBridge Scheduler rules (07:00 / 15:00 / 23:00 UTC, with the same Sun–Fri /
daily day-masks the GHA crons use) each target THIS Lambda, which sends a
`repository_dispatch` (type `scheduled-groom`) to
`nousergon/alpha-engine-config`, where `backlog-groom.yml` consumes it. The
schedule's own input carries `run_mode`/`phase`, which the Lambda forwards in
`client_payload` so the workflow's run-mode step routes the right drain pass.

Reuses the SAME SSM-stored fine-grained PAT
(`/alpha-engine/saturday_sf_watch/github_pat`) the sibling dispatchers use for
repository_dispatch — no new credential.

Fail-loud (UNLIKE the convenience-side success dispatcher): a scheduled groom
IS the deliverable here — it replaces the cron the workflow depends on — so a
GitHub/SSM failure RAISES, letting EventBridge's retries + the Lambda error
metric + a CloudWatch alarm surface the miss, rather than silently swallowing a
dropped pass (the exact failure mode #1322 is fixing).

Managed OUTSIDE CloudFormation (same rationale as the sibling dispatchers):
operator-deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live
effect until bootstrapped, and the GHA `schedule:` crons stay in place as a
belt-and-suspenders backstop until a multi-day on-time-firing soak confirms the
EventBridge path before they can be removed.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
# Kill-switch: set GROOM_DISPATCH_ENABLED=false on the Lambda to disable the
# trigger without deleting the EventBridge Scheduler rules. Default ON.
DISPATCH_ENABLED = os.environ.get("GROOM_DISPATCH_ENABLED", "true").lower() == "true"
DISPATCH_REPO = os.environ.get("DISPATCH_REPO", "nousergon/alpha-engine-config")
DISPATCH_EVENT_TYPE = os.environ.get("DISPATCH_EVENT_TYPE", "scheduled-groom")
GITHUB_PAT_SSM_PARAM = os.environ.get(
    "GITHUB_PAT_SSM_PARAM", "/alpha-engine/saturday_sf_watch/github_pat"
)
# Valid run-modes the workflow's run-mode step honors. Anything else falls back
# to "full" so a malformed schedule input can never wedge the dispatch.
_VALID_RUN_MODES = {"full", "sweep"}
_DEFAULT_RUN_MODE = "full"
_DISPATCH_TIMEOUT_SEC = 15


def _get_github_pat() -> str:
    """Read the fine-grained PAT (SecureString) from SSM. Never logged."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=GITHUB_PAT_SSM_PARAM, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _resolve_run_mode(event: dict) -> str:
    """Pull the desired run-mode from the EventBridge Scheduler input.

    Each Scheduler rule passes a JSON input like {"run_mode": "full",
    "schedule": "0 7 * * 0-5"}. An unknown/missing run_mode degrades to FULL
    (the drain-phase default — all three current crons are full grooms).
    """
    rm = str(event.get("run_mode") or _DEFAULT_RUN_MODE).strip().lower()
    if rm not in _VALID_RUN_MODES:
        logger.warning("unknown run_mode %r — defaulting to %s", rm, _DEFAULT_RUN_MODE)
        rm = _DEFAULT_RUN_MODE
    return rm


def _dispatch_groom(run_mode: str, schedule_label: str) -> dict:
    """Fire the repository_dispatch that triggers backlog-groom.yml.

    Fail-loud: a scheduled groom is the deliverable (it replaces the cron the
    workflow depends on), so any failure RAISES so EventBridge retries + the
    Lambda error metric surface the miss. Returns the success result.
    """
    if not DISPATCH_ENABLED:
        logger.warning("GROOM_DISPATCH_ENABLED=false — scheduled groom NOT dispatched")
        return {"dispatched": False, "reason": "disabled"}
    pat = _get_github_pat()
    payload = {
        "event_type": DISPATCH_EVENT_TYPE,
        "client_payload": {
            "trigger": "eventbridge-scheduled",
            "run_mode": run_mode,
            # `phase` is an alias of run_mode kept for forward-compat with any
            # future multi-phase drain routing; the workflow reads run_mode.
            "phase": run_mode,
            "schedule": schedule_label,
        },
    }
    req = urllib.request.Request(
        f"https://api.github.com/repos/{DISPATCH_REPO}/dispatches",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {pat}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "scheduled-groom-dispatcher",
        },
    )
    with urllib.request.urlopen(req, timeout=_DISPATCH_TIMEOUT_SEC) as resp:
        status_code = resp.status
    logger.info(
        "scheduled-groom repository_dispatch sent to %s (type=%s, run_mode=%s, http=%s)",
        DISPATCH_REPO,
        DISPATCH_EVENT_TYPE,
        run_mode,
        status_code,
    )
    return {"dispatched": True, "status_code": status_code, "run_mode": run_mode}


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge Scheduler handler — fires the backlog groom on cadence.

    `event` is the schedule's configured JSON input, e.g.
    {"run_mode": "full", "schedule": "0 23 * * *"}.
    """
    event = event or {}
    run_mode = _resolve_run_mode(event)
    schedule_label = str(event.get("schedule") or "unknown")
    logger.info(
        "scheduled groom trigger: run_mode=%s schedule=%s", run_mode, schedule_label
    )
    result = _dispatch_groom(run_mode, schedule_label)
    return {"groom": result}
