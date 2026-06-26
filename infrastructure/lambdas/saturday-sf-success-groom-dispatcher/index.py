"""alpha-engine-saturday-sf-success-groom-dispatcher — run the backlog groom
after a SUCCESSFUL Saturday SF.

Brian's ask (2026-06-26): once the Saturday pipeline completes — after the
Director has filed its weekly proposals — run the daily backlog groom again so
the fresh backlog state (including the Director's just-filed items) gets groomed
immediately, rather than waiting for the next scheduled 10pm-PT run. Added while
still building confidence that the daily groom is as thorough as intended.

Wiring mirrors the failure-side sibling `saturday-sf-watch-dispatcher`: a
dedicated EventBridge rule on the Saturday SF's SUCCEEDED status targets this
Lambda, which sends a `repository_dispatch` (type `saturday-sf-success-groom`)
to `nousergon/alpha-engine-config`, where `backlog-groom.yml` listens for it and
runs a FULL groom. Reuses the same SSM-stored fine-grained PAT
(`/alpha-engine/saturday_sf_watch/github_pat`) the watch sibling uses for
repository_dispatch — no new credential.

Best-effort with a recording surface (CLAUDE.md no-silent-fails secondary
carve-out): a GitHub/SSM outage logs WARN and is returned in the result but does
NOT raise — triggering the groom is a convenience, NOT the Saturday pipeline's
deliverable, and the scheduled nightly groom is the backstop. EventBridge's
delivery retries + the Lambda error metric still record a hard failure.

Managed OUTSIDE CloudFormation (same rationale as the watch sibling): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect until
bootstrapped.
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
# trigger without removing the EventBridge rule. Default ON (Brian wants it live).
DISPATCH_ENABLED = os.environ.get("GROOM_DISPATCH_ENABLED", "true").lower() == "true"
DISPATCH_REPO = os.environ.get("DISPATCH_REPO", "nousergon/alpha-engine-config")
DISPATCH_EVENT_TYPE = os.environ.get("DISPATCH_EVENT_TYPE", "saturday-sf-success-groom")
GITHUB_PAT_SSM_PARAM = os.environ.get(
    "GITHUB_PAT_SSM_PARAM", "/alpha-engine/saturday_sf_watch/github_pat"
)
_DISPATCH_TIMEOUT_SEC = 15


def _get_github_pat() -> str:
    """Read the fine-grained PAT (SecureString) from SSM. Never logged."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=GITHUB_PAT_SSM_PARAM, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _dispatch_groom(execution_name: str, start_date) -> dict:
    """Fire the repository_dispatch that triggers backlog-groom.yml. Best-effort:
    records the outcome, never raises (secondary path)."""
    if not DISPATCH_ENABLED:
        return {"dispatched": False, "reason": "disabled"}
    try:
        pat = _get_github_pat()
        payload = {
            "event_type": DISPATCH_EVENT_TYPE,
            "client_payload": {
                "trigger": "saturday-sf-success",
                "execution_name": execution_name,
                "start_date": start_date,
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
                "User-Agent": "saturday-sf-success-groom-dispatcher",
            },
        )
        with urllib.request.urlopen(req, timeout=_DISPATCH_TIMEOUT_SEC) as resp:
            status_code = resp.status
        logger.info(
            "groom repository_dispatch sent to %s (type=%s, http=%s)",
            DISPATCH_REPO,
            DISPATCH_EVENT_TYPE,
            status_code,
        )
        return {"dispatched": True, "status_code": status_code}
    except Exception as exc:  # noqa: BLE001 — secondary path, recorded not raised
        logger.warning("groom repository_dispatch failed (non-fatal): %s", exc)
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
