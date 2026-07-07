"""alpha-engine-sf-telegram-notifier — fan SF status changes into Telegram.

Subscribes to EventBridge `Step Functions Execution Status Change` events for
the three Alpha Engine Step Functions (saturday / weekday / eod) and forwards
a human-readable summary to the fleet alerts forum via flow-doctor
(``notify_event`` / forum topic ``#pipeline``).

Migration arc: config#1742 (fleet Telegram consolidation T2). Falls back to
``nousergon_lib.telegram.send_message`` when flow-doctor is unavailable
(local tests / init failure).

The existing SNS → email path is unaffected.
"""

from __future__ import annotations

import json
import logging
import os

import boto3

from execution_digest import build_execution_digest, parse_run_date_from_input
from flow_doctor_telegram import build_flow_doctor_config, notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import (
    FleetTelegramTopic,
    PIPELINE_OBSERVER_TELEGRAM_TOPICS,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
_FLOW_NAME = "sf-telegram-notifier"
_DB_BASENAME = "flow_doctor_sf_telegram_notifier"

_SF_LABELS: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness SF",
    "ne-preopen-trading-pipeline": "Pre-open Trading SF",
    "ne-postclose-trading-pipeline": "Post-close Trading SF",
}

_PREFLIGHT_LABEL_OVERRIDE: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness Preflight SF",
}

_STATUS_EMOJI: dict[str, str] = {
    "RUNNING": "\U0001f680",
    "SUCCEEDED": "✅",
    "FAILED": "\U0001f534",
    "TIMED_OUT": "⏰",
    "ABORTED": "⛔",
}

_CAUSE_MAX_CHARS = 280
_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})


def build_flow_doctor_config_for_tests() -> dict:
    """Expose config builder for fleet wiring contract tests."""
    return build_flow_doctor_config(
        _FLOW_NAME,
        PIPELINE_OBSERVER_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
    )


def _severity_for_status(status: str) -> str:
    if status in ("FAILED", "TIMED_OUT", "ABORTED"):
        return "warning"
    return "info"


def _dedup_key(detail: dict) -> str:
    return ":".join(
        [
            _FLOW_NAME,
            detail.get("executionArn") or detail.get("name") or "unknown",
            detail.get("status") or "UNKNOWN",
        ]
    )


def _label_for_arn(sm_arn: str, *, is_preflight: bool = False) -> str:
    name = sm_arn.rsplit(":", 1)[-1] if sm_arn else ""
    if is_preflight and name in _PREFLIGHT_LABEL_OVERRIDE:
        return _PREFLIGHT_LABEL_OVERRIDE[name]
    return _SF_LABELS.get(name, name or "Unknown SF")


def _format_duration(started_ms: int | None, stopped_ms: int | None) -> str:
    if started_ms is None or stopped_ms is None:
        return ""
    secs = max(0, (int(stopped_ms) - int(started_ms)) // 1000)
    h, rem = divmod(secs, 3600)
    m, _ = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def _describe_execution(execution_arn: str) -> dict | None:
    if not execution_arn:
        return None
    try:
        sf = boto3.client("stepfunctions", region_name=REGION)
        return sf.describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001
        logger.warning("describe_execution failed for %s: %s", execution_arn, exc)
        return None


def _is_preflight_execution(describe_resp: dict | None) -> bool:
    if not describe_resp:
        return False
    try:
        payload = json.loads(describe_resp.get("input") or "{}")
    except (ValueError, TypeError) as exc:
        logger.warning("could not parse execution input as JSON: %s", exc)
        return False
    return bool(payload.get("shell_run"))


def _failure_cause_from(describe_resp: dict | None) -> str:
    if not describe_resp:
        return ""
    error = (describe_resp.get("error") or "").strip()
    cause = (describe_resp.get("cause") or "").strip()
    if error and cause:
        snippet = f"{error}: {cause}"
    else:
        snippet = error or cause
    if len(snippet) > _CAUSE_MAX_CHARS:
        snippet = snippet[: _CAUSE_MAX_CHARS - 1] + "…"
    return snippet


def _build_message(
    detail: dict,
    describe_resp: dict | None = None,
    *,
    sf_client=None,
    s3_client=None,
) -> tuple[str, bool, bool]:
    """Return ``(text, silent, hollow_suspect)``."""
    status = detail.get("status", "UNKNOWN")
    execution_arn = detail.get("executionArn", "")
    if describe_resp is None:
        describe_resp = _describe_execution(execution_arn)
    is_preflight = _is_preflight_execution(describe_resp)
    label = _label_for_arn(
        detail.get("stateMachineArn", ""), is_preflight=is_preflight
    )
    emoji = _STATUS_EMOJI.get(status, "\U0001f4e8")
    exec_name = detail.get("name", "") or "(unknown execution)"
    hollow_suspect = False

    lines = [f"{emoji} *{label} — {status}*"]

    if status == "RUNNING":
        lines.append(f"Execution: {exec_name}")
        return "\n".join(lines), True, False

    duration = _format_duration(detail.get("startDate"), detail.get("stopDate"))
    if duration:
        lines.append(f"Duration: {duration}")

    if status in _TERMINAL_STATUSES and execution_arn:
        if sf_client is None:
            sf_client = boto3.client("stepfunctions", region_name=REGION)
        if s3_client is None and not is_preflight:
            s3_client = boto3.client("s3", region_name=REGION)
        run_date = parse_run_date_from_input((describe_resp or {}).get("input"))
        digest_lines, hollow_suspect = build_execution_digest(
            execution_arn=execution_arn,
            is_preflight=is_preflight,
            execution_start_ms=detail.get("startDate"),
            run_date=run_date,
            sf_client=sf_client,
            s3_client=None if is_preflight else s3_client,
        )
        if hollow_suspect and status == "SUCCEEDED":
            lines.append("⚠️ *HOLLOW-SUSPECT* — workload state(s) completed implausibly fast")
        lines.append("*States:*")
        lines.extend(digest_lines)

    if status == "FAILED":
        cause = _failure_cause_from(describe_resp)
        if cause:
            lines.append(f"Cause: {cause}")

    lines.append(f"Execution: {exec_name}")
    silent = False
    if status == "SUCCEEDED" and hollow_suspect:
        silent = False
    return "\n".join(lines), silent, hollow_suspect


def handler(event: dict, context) -> dict:  # noqa: ARG001
    detail = event.get("detail") or {}
    status = detail.get("status", "UNKNOWN")
    sm_name = (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1]
    logger.info("SF status change: sf=%s status=%s", sm_name, status)

    text, silent, hollow_suspect = _build_message(detail)
    ok = notify_via_flow_doctor(
        text,
        silent=silent,
        severity="warning" if hollow_suspect and status == "SUCCEEDED" else _severity_for_status(status),
        dedup_key=_dedup_key(detail),
        flow_name=_FLOW_NAME,
        topics=PIPELINE_OBSERVER_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
        context={
            "state_machine": sm_name,
            "execution": detail.get("name"),
            "status": status,
        },
        silent_topic=FleetTelegramTopic.PIPELINE,
    )

    return {
        "status": status,
        "state_machine": sm_name,
        "execution": detail.get("name", ""),
        "telegram_sent": ok,
        "silent": silent,
        "hollow_suspect": hollow_suspect,
    }
