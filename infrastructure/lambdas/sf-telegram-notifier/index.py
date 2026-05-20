"""alpha-engine-sf-telegram-notifier — fan SF status changes into Telegram.

Subscribes to EventBridge `Step Functions Execution Status Change` events for
the three Alpha Engine Step Functions (saturday / weekday / eod) and forwards
a human-readable summary to the alpha-engine Telegram bot via the canonical
``alpha_engine_lib.telegram.send_message`` primitive.

The existing SNS → email path is unaffected: this Lambda subscribes to a new
EventBridge rule (``alpha-engine-sf-status-change``) and does not touch any
SF definition. Adding/removing Telegram coverage is a single-resource flip
with zero blast radius on the trade-decision pipeline.

Event source: ``aws.states`` / ``Step Functions Execution Status Change``
covers all five terminal-or-transition statuses in one rule:
RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED. RUNNING fires once at
execution start; the four terminal statuses fire once each at end. RUNNING
notifications go out silent (in-channel awareness without a phone buzz) so
the weekday SF's daily 5:45 AM PT start does not buzz on every trading day;
all terminal statuses push.

On FAILED, the handler best-effort calls ``states:DescribeExecution`` to
surface the failure cause string. The Telegram primitive never raises, so a
misconfigured bot or transient network error returns ``False`` from
``send_message`` and is logged at WARNING — the EventBridge invocation still
returns success and is not retried.
"""

from __future__ import annotations

import logging
import os

import boto3

from alpha_engine_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

_SF_LABELS: dict[str, str] = {
    "alpha-engine-saturday-pipeline": "Saturday SF",
    "alpha-engine-weekday-pipeline": "Weekday SF",
    "alpha-engine-eod-pipeline": "EOD SF",
}

_STATUS_EMOJI: dict[str, str] = {
    "RUNNING": "\U0001f680",    # 🚀
    "SUCCEEDED": "✅",       # ✅
    "FAILED": "\U0001f534",      # 🔴
    "TIMED_OUT": "⏰",       # ⏰
    "ABORTED": "⛔",         # ⛔
}

_CAUSE_MAX_CHARS = 280


def _label_for_arn(sm_arn: str) -> str:
    name = sm_arn.rsplit(":", 1)[-1] if sm_arn else ""
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


def _fetch_failure_cause(execution_arn: str) -> str:
    """Best-effort fetch of error+cause via DescribeExecution. Never raises."""
    if not execution_arn:
        return ""
    try:
        sf = boto3.client("stepfunctions", region_name=REGION)
        resp = sf.describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001 — fire-and-forget enrichment
        logger.warning("describe_execution failed for %s: %s", execution_arn, exc)
        return ""
    error = (resp.get("error") or "").strip()
    cause = (resp.get("cause") or "").strip()
    if error and cause:
        snippet = f"{error}: {cause}"
    else:
        snippet = error or cause
    if len(snippet) > _CAUSE_MAX_CHARS:
        snippet = snippet[: _CAUSE_MAX_CHARS - 1] + "…"
    return snippet


def _build_message(detail: dict) -> tuple[str, bool]:
    """Return (text, disable_notification) for the given event detail."""
    status = detail.get("status", "UNKNOWN")
    label = _label_for_arn(detail.get("stateMachineArn", ""))
    emoji = _STATUS_EMOJI.get(status, "\U0001f4e8")  # 📨 fallback
    exec_name = detail.get("name", "") or "(unknown execution)"

    lines = [f"{emoji} *{label} — {status}*"]

    if status == "RUNNING":
        lines.append(f"Execution: {exec_name}")
        return "\n".join(lines), True

    duration = _format_duration(detail.get("startDate"), detail.get("stopDate"))
    if duration:
        lines.append(f"Duration: {duration}")

    if status == "FAILED":
        cause = _fetch_failure_cause(detail.get("executionArn", ""))
        if cause:
            lines.append(f"Cause: {cause}")

    lines.append(f"Execution: {exec_name}")
    return "\n".join(lines), False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler for SF execution status changes.

    Expected event shape (CloudWatch Events / EventBridge from aws.states):
      detail.status:           RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED
      detail.stateMachineArn:  SF arn
      detail.executionArn:     execution arn
      detail.name:             execution id
      detail.startDate:        epoch ms
      detail.stopDate:         epoch ms (null while RUNNING)
    """
    detail = event.get("detail") or {}
    status = detail.get("status", "UNKNOWN")
    sm_name = (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1]
    logger.info("SF status change: sf=%s status=%s", sm_name, status)

    text, silent = _build_message(detail)
    ok = send_message(text, disable_notification=silent)

    return {
        "status": status,
        "state_machine": sm_name,
        "execution": detail.get("name", ""),
        "telegram_sent": ok,
        "silent": silent,
    }
