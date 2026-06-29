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

import json
import logging
import os

import boto3

from alpha_engine_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

_SF_LABELS: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness SF",
    "ne-preopen-trading-pipeline": "Pre-open Trading SF",
    "ne-postclose-trading-pipeline": "Post-close Trading SF",
}

# 2026-05-23 Preflight Pipeline rename: when the weekly-freshness SF runs as
# the Friday-PM preflight dry-pass (input ``shell_run=true``), surface a
# distinct ``Weekly Freshness Preflight SF`` label in the Telegram message so
# the operator can tell the two flavors apart in the channel at a
# glance. The state machine name is the same (the dry-pass IS the
# weekly-freshness SF with dry inputs per CLAUDE.md "don't add redundant paths
# around load-bearing scheduled infra"); we differentiate via the
# execution input flag, not via a separate SF.
_PREFLIGHT_LABEL_OVERRIDE: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness Preflight SF",
}

_STATUS_EMOJI: dict[str, str] = {
    "RUNNING": "\U0001f680",    # 🚀
    "SUCCEEDED": "✅",       # ✅
    "FAILED": "\U0001f534",      # 🔴
    "TIMED_OUT": "⏰",       # ⏰
    "ABORTED": "⛔",         # ⛔
}

_CAUSE_MAX_CHARS = 280


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
    """Best-effort DescribeExecution. Returns the response dict on success,
    ``None`` on any error. Single source for both the failure-cause
    extraction and the preflight-input classification — combining the
    two callers into one API call cuts the per-event boto3 cost in half
    and keeps the test mock surface simple (one ``describe_execution``
    call per handler invocation).
    """
    if not execution_arn:
        return None
    try:
        sf = boto3.client("stepfunctions", region_name=REGION)
        return sf.describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001 — fire-and-forget enrichment
        logger.warning("describe_execution failed for %s: %s", execution_arn, exc)
        return None


def _is_preflight_execution(describe_resp: dict | None) -> bool:
    """True iff the execution's input has ``shell_run=true``.

    The Saturday SF runs as either the real Saturday firing (no
    ``shell_run`` input, $.pipeline_label="") or the Friday-PM Preflight
    Pipeline dry-pass (``shell_run=true``, $.pipeline_label=" Preflight").
    """
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


def _build_message(detail: dict) -> tuple[str, bool]:
    """Return (text, disable_notification) for the given event detail."""
    status = detail.get("status", "UNKNOWN")
    # Single DescribeExecution call serving both the preflight-input
    # classification (label override) and the FAILED-path cause
    # enrichment. RUNNING events also benefit from the label override
    # — knowing 'Saturday Preflight SF RUNNING' vs 'Saturday SF RUNNING'
    # at-a-glance is the operator's main use case for the rename.
    describe_resp = _describe_execution(detail.get("executionArn", ""))
    is_preflight = _is_preflight_execution(describe_resp)
    label = _label_for_arn(
        detail.get("stateMachineArn", ""), is_preflight=is_preflight
    )
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
        cause = _failure_cause_from(describe_resp)
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
