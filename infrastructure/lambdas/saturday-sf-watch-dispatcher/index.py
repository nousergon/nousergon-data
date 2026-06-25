"""alpha-engine-saturday-sf-watch-dispatcher — Saturday-SF Watch, M1 (OBSERVE).

First slice of the Saturday-SF Watch arc (spec: nousergon/alpha-engine-config#1227).
The arc's end state is an autonomous, SOTA-steered resilience agent that, on a
Saturday SF (`alpha-engine-saturday-pipeline`) failure, diagnoses + enacts an
institutional-grade reliability fix and reruns from the failed step
(ask-forgiveness posture). This Lambda is **M1**: the dedicated trigger path +
the watch-log artifact contract, in **OBSERVE mode only**. It does NOT invoke
any agent and does NOT touch any repo, IAM, or SF definition.

**Why this is NOT a second notifier.** The fleet already has
`alpha-engine-sf-telegram-notifier` (subscribes to all three SFs / all statuses,
pings loud on FAILED with the cause). Re-implementing notification here would be
the redundant-path anti-pattern. This Lambda's distinct responsibilities are:

  1. A **Saturday-only, terminal-failure-only** trigger (its own EventBridge rule
     `alpha-engine-saturday-sf-watch-failed`) — the seam the M2 agent dispatch
     will hang off.
  2. The **watch-log artifact** at
     ``s3://{WATCH_BUCKET}/consolidated/saturday_sf_watch/{run_date}.json`` — the
     contract the M3 dashboard page reads. Establishing it now means the
     dashboard + agent halves build against a stable shape.
  3. A **distinct, SILENT** Telegram record (the notifier already buzzed loud) that
     names the failed state + the artifact location and states OBSERVE mode.

**Fail-loud (CLAUDE.md no-silent-fails).** The watch-log artifact write is the
primary deliverable in OBSERVE mode → it RAISES on failure so a broken producer
surfaces via the Lambda error metric + CW alarm. Enrichment (DescribeExecution /
GetExecutionHistory) and the Telegram record are secondary observability hung off
the primary path: their failure is logged at WARNING and recorded in the artifact
(``cause``/``failed_state`` become null with a reason) — the artifact still
records that a failure was detected.

**M2 seam.** ``AGENT_DISPATCH_ENABLED`` (default ``"false"``) is read and echoed
in the return value + artifact so the cutover to agent dispatch is a single env
flip. M1 contains NO dispatch code — the ``repository_dispatch`` call is added in
M2. Until then this Lambda is inert beyond observing.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone

import boto3

from alpha_engine_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
WATCH_PREFIX = os.environ.get("WATCH_PREFIX", "consolidated/saturday_sf_watch")
# M2 seam — default OFF. M1 never dispatches; this is read + echoed only.
AGENT_DISPATCH_ENABLED = (
    os.environ.get("AGENT_DISPATCH_ENABLED", "false").lower() == "true"
)

SATURDAY_SF_NAME = "alpha-engine-saturday-pipeline"
SCHEMA_VERSION = 1
_CAUSE_MAX_CHARS = 600
# Bound the history scan: fetch the newest N events (reverseOrder), reconstruct
# chronological order locally to find the entered-but-not-exited state. The
# failed state's enclosing StateEntered is always in the tail of the history.
_HISTORY_MAX_EVENTS = 1000


def _sf_client():
    return boto3.client("stepfunctions", region_name=REGION)


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _describe_execution(execution_arn: str) -> dict | None:
    """Best-effort DescribeExecution → top-level error/cause + input. None on error."""
    if not execution_arn:
        return None
    try:
        return _sf_client().describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001 — enrichment, recorded in artifact
        logger.warning("describe_execution failed for %s: %s", execution_arn, exc)
        return None


def _failure_cause(describe_resp: dict | None) -> str:
    if not describe_resp:
        return ""
    error = (describe_resp.get("error") or "").strip()
    cause = (describe_resp.get("cause") or "").strip()
    snippet = f"{error}: {cause}" if (error and cause) else (error or cause)
    if len(snippet) > _CAUSE_MAX_CHARS:
        snippet = snippet[: _CAUSE_MAX_CHARS - 1] + "…"
    return snippet


def _is_preflight(describe_resp: dict | None) -> bool:
    """True iff execution input has ``shell_run=true`` (the Friday-PM dry-pass)."""
    if not describe_resp:
        return False
    try:
        payload = json.loads(describe_resp.get("input") or "{}")
    except (ValueError, TypeError):
        return False
    return bool(payload.get("shell_run"))


def _failed_state_from_history(execution_arn: str) -> str | None:
    """Return the name of the state that was active (entered, not yet exited)
    when the execution failed — i.e. the culprit state.

    Fetches the newest ``_HISTORY_MAX_EVENTS`` events (reverseOrder), reverses
    them to chronological order, and tracks the entered-but-not-exited state via
    a forward scan. A state that fails enters but never cleanly exits, so it is
    the one left dangling at the terminal failure event. Best-effort: returns
    ``None`` on any API error (recorded in the artifact).
    """
    if not execution_arn:
        return None
    try:
        resp = _sf_client().get_execution_history(
            executionArn=execution_arn,
            maxResults=_HISTORY_MAX_EVENTS,
            reverseOrder=True,
            includeExecutionData=False,
        )
    except Exception as exc:  # noqa: BLE001 — enrichment, recorded in artifact
        logger.warning("get_execution_history failed for %s: %s", execution_arn, exc)
        return None

    events = list(reversed(resp.get("events", [])))  # → chronological
    current: str | None = None
    for ev in events:
        etype = ev.get("type", "")
        if etype.endswith("StateEntered"):
            det = ev.get("stateEnteredEventDetails") or {}
            current = det.get("name") or current
        elif etype.endswith("StateExited"):
            det = ev.get("stateExitedEventDetails") or {}
            if det.get("name") == current:
                current = None
    return current


def _run_date(describe_resp: dict | None, detail: dict) -> str:
    """Resolve the Saturday firing date (YYYY-MM-DD) for the artifact key.

    Prefers the execution input's ``run_date`` (the canonical key the pipeline
    stamps its artifacts with), then the execution ``startDate`` epoch-ms, then
    ``now`` UTC. Keeps the watch-log aligned with the artifacts it will later
    report integrity on.
    """
    if describe_resp:
        try:
            payload = json.loads(describe_resp.get("input") or "{}")
            rd = payload.get("run_date")
            if isinstance(rd, str) and rd:
                return rd
        except (ValueError, TypeError):
            pass
    start_ms = detail.get("startDate")
    if isinstance(start_ms, (int, float)) and start_ms > 0:
        return datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date().isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def _artifact_key(run_date: str) -> str:
    return f"{WATCH_PREFIX}/{run_date}.json"


def _load_existing(s3, key: str) -> dict:
    """Read the existing watch-log for this date (so repeated failures in one
    Saturday accumulate), or a fresh skeleton. A missing object (404/403) is the
    common first-failure-of-the-day case, NOT an error."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        if isinstance(data, dict) and isinstance(data.get("events"), list):
            return data
    except Exception as exc:  # noqa: BLE001 — absence is expected; bad blob is recoverable
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", "403"}:
            logger.warning("could not read existing watch-log %s: %s", key, exc)
    return {"schema_version": SCHEMA_VERSION, "events": []}


def _build_event_record(detail: dict, describe_resp: dict | None, run_date: str) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    cause = _failure_cause(describe_resp)
    failed_state = _failed_state_from_history(detail.get("executionArn", ""))
    return {
        "detected_at": now_iso,
        "status": detail.get("status", "UNKNOWN"),
        "state_machine": (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1],
        "execution_name": detail.get("name", ""),
        "execution_arn": detail.get("executionArn", ""),
        "failed_state": failed_state,
        "cause": cause or None,
        "is_preflight": _is_preflight(describe_resp),
        # Filled by the M2+ agent; null in OBSERVE mode.
        "lane": None,
        "action": "observe",
        "agent_dispatch_enabled": AGENT_DISPATCH_ENABLED,
    }


def _write_watch_log(s3, run_date: str, record: dict) -> str:
    """Append the event to the date's watch-log and write it back. PRIMARY
    deliverable — RAISES on failure (fail-loud: a broken producer must surface
    via the Lambda error metric + CW alarm, never silently)."""
    key = _artifact_key(run_date)
    doc = _load_existing(s3, key)
    doc["schema_version"] = SCHEMA_VERSION
    doc["run_date"] = run_date
    doc["updated_at"] = record["detected_at"]
    doc["events"].append(record)
    s3.put_object(
        Bucket=WATCH_BUCKET,
        Key=key,
        Body=json.dumps(doc, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def _notify(record: dict, key: str) -> bool:
    """Distinct, SILENT Telegram record. The sf-telegram-notifier already pinged
    loud on this FAILED event; this is the additive watch receipt (which state,
    where the artifact is, OBSERVE mode). Best-effort — never raises."""
    label = "Saturday Preflight SF" if record["is_preflight"] else "Saturday SF"
    lines = [
        f"\U0001f6f0️ *Saturday-SF Watch — OBSERVE*",
        f"{label}: {record['status']}",
    ]
    if record.get("failed_state"):
        lines.append(f"Failed state: {record['failed_state']}")
    if record.get("cause"):
        lines.append(f"Cause: {record['cause']}")
    lines.append(f"Watch log: s3://{WATCH_BUCKET}/{key}")
    lines.append("_autonomous fix DISABLED (M1 observe-only)_")
    try:
        return bool(send_message("\n".join(lines), disable_notification=True))
    except Exception as exc:  # noqa: BLE001 — secondary observability
        logger.warning("watch Telegram record failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — fires only on Saturday SF terminal failure
    (FAILED / TIMED_OUT / ABORTED), per the dedicated rule
    ``alpha-engine-saturday-sf-watch-failed``.
    """
    detail = event.get("detail") or {}
    sm_name = (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1]
    status = detail.get("status", "UNKNOWN")
    logger.info("Saturday-SF Watch: sf=%s status=%s (OBSERVE)", sm_name, status)

    # Defensive: the rule scopes to the Saturday SF, but never act on anything else.
    if sm_name != SATURDAY_SF_NAME:
        logger.warning("ignoring non-Saturday SF event: %s", sm_name)
        return {"ignored": True, "state_machine": sm_name, "status": status}

    describe_resp = _describe_execution(detail.get("executionArn", ""))
    run_date = _run_date(describe_resp, detail)
    record = _build_event_record(detail, describe_resp, run_date)

    s3 = _s3_client()
    key = _write_watch_log(s3, run_date, record)  # PRIMARY — fail-loud
    telegram_sent = _notify(record, key)          # secondary — best-effort

    logger.info(
        "Saturday-SF Watch recorded: run_date=%s failed_state=%s key=%s telegram=%s",
        run_date, record.get("failed_state"), key, telegram_sent,
    )
    return {
        "status": status,
        "state_machine": sm_name,
        "run_date": run_date,
        "failed_state": record.get("failed_state"),
        "watch_log_key": key,
        "telegram_sent": telegram_sent,
        "agent_dispatch_enabled": AGENT_DISPATCH_ENABLED,
        "action": "observe",
    }
