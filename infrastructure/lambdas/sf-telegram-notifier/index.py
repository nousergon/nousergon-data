"""alpha-engine-sf-telegram-notifier — fan SF status changes into Telegram.

Subscribes to EventBridge `Step Functions Execution Status Change` events for
the three Alpha Engine Step Functions (saturday / weekday / eod) and forwards
a human-readable summary to the fleet alerts forum via flow-doctor
(``notify_event`` / forum topic ``#pipeline``).

Migration arc: config#1742 (fleet Telegram consolidation T2). Falls back to
``alpha_engine_lib.telegram.send_message`` when flow-doctor is unavailable
(local tests / init failure).

The existing SNS → email path is unaffected.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import boto3

from alpha_engine_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

_flow_doctor = None
_flow_doctor_init_attempted = False

_SF_LABELS: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness SF",
    "ne-preopen-trading-pipeline": "Pre-open Trading SF",
    "ne-postclose-trading-pipeline": "Post-close Trading SF",
}

_PREFLIGHT_LABEL_OVERRIDE: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness Preflight SF",
}

_STATUS_EMOJI: dict[str, str] = {
    "RUNNING": "\U0001f680",    # 🚀
    "SUCCEEDED": "✅",
    "FAILED": "\U0001f534",
    "TIMED_OUT": "⏰",
    "ABORTED": "⛔",
}

_CAUSE_MAX_CHARS = 280


def build_flow_doctor_config() -> dict:
    """Build lambda flow-doctor config from fleet canonical topic layout."""
    from nousergon_lib.flow_doctor_fleet import (
        PIPELINE_OBSERVER_TELEGRAM_TOPICS,
        fleet_telegram_notifier_dicts,
    )

    return {
        "flow_name": "sf-telegram-notifier",
        "repo": "nousergon/nousergon-data",
        "owner": "@brianmcmahon",
        "notify": fleet_telegram_notifier_dicts(
            PIPELINE_OBSERVER_TELEGRAM_TOPICS
        ),
        "store": {
            "type": "sqlite",
            "path": "/tmp/flow_doctor_sf_telegram_notifier.db",
        },
        "dedup_cooldown_minutes": 1,
        "rate_limits": {"max_alerts_per_day": 100},
    }


def _materialize_flow_doctor_yaml() -> str:
    import yaml

    path = Path("/tmp/flow_doctor_sf_telegram_notifier.yaml")
    path.write_text(
        yaml.safe_dump(build_flow_doctor_config(), sort_keys=False),
        encoding="utf-8",
    )
    return str(path)


def _get_flow_doctor():
    """Lazy-init shared FlowDoctor (forum routing). Returns None on fallback path."""
    global _flow_doctor, _flow_doctor_init_attempted
    if _flow_doctor is not None:
        return _flow_doctor
    if _flow_doctor_init_attempted:
        return None
    _flow_doctor_init_attempted = True
    if os.environ.get("FLOW_DOCTOR_ENABLED", "1") != "1":
        return None
    try:
        from nousergon_lib.logging import get_flow_doctor, setup_logging

        setup_logging(
            "sf-telegram-notifier",
            flow_doctor_yaml=_materialize_flow_doctor_yaml(),
        )
        _flow_doctor = get_flow_doctor()
    except Exception as exc:  # noqa: BLE001 — fall back to send_message
        logger.warning(
            "flow-doctor init failed — falling back to send_message: %s", exc
        )
    return _flow_doctor


def _pipeline_telegram_notifier(fd):
    """Return the ``#pipeline`` TelegramNotifier, if configured."""
    from flow_doctor.notify.telegram import TelegramNotifier
    from nousergon_lib.flow_doctor_fleet import (
        FleetTelegramTopic,
        fleet_telegram_thread_id_env,
    )

    want = os.environ.get(
        fleet_telegram_thread_id_env(FleetTelegramTopic.PIPELINE)
    )
    for notifier in fd._notifiers:
        if not isinstance(notifier, TelegramNotifier):
            continue
        thread_id = getattr(notifier, "message_thread_id", None)
        if thread_id is not None and str(thread_id) == str(want):
            return notifier
    return None


def _severity_for_status(status: str) -> str:
    if status in ("FAILED", "TIMED_OUT", "ABORTED"):
        return "warning"
    return "info"


def _dedup_key(detail: dict) -> str:
    return ":".join(
        [
            "sf-telegram-notifier",
            detail.get("executionArn") or detail.get("name") or "unknown",
            detail.get("status") or "UNKNOWN",
        ]
    )


def _notify_via_flow_doctor(
    text: str,
    *,
    detail: dict,
    silent: bool,
) -> bool:
    fd = _get_flow_doctor()
    if fd is None:
        return send_message(text, disable_notification=silent)

    status = detail.get("status", "UNKNOWN")
    subject = text.split("\n", 1)[0].replace("*", "")

    if silent:
        pipeline = _pipeline_telegram_notifier(fd)
        if pipeline is not None:
            return pipeline.send_raw(text, disable_notification=True) is not None
        logger.warning(
            "pipeline Telegram notifier missing — RUNNING ping via notify_event"
        )

    report_id = fd.notify_event(
        subject,
        body=text,
        severity=_severity_for_status(status),
        context={
            "state_machine": (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1],
            "execution": detail.get("name"),
            "status": status,
        },
        dedup_key=_dedup_key(detail),
    )
    return report_id is not None


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
    except Exception as exc:  # noqa: BLE001 — fire-and-forget enrichment
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


def _build_message(detail: dict) -> tuple[str, bool]:
    """Return (text, silent) for the given event detail."""
    status = detail.get("status", "UNKNOWN")
    describe_resp = _describe_execution(detail.get("executionArn", ""))
    is_preflight = _is_preflight_execution(describe_resp)
    label = _label_for_arn(
        detail.get("stateMachineArn", ""), is_preflight=is_preflight
    )
    emoji = _STATUS_EMOJI.get(status, "\U0001f4e8")
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
    detail = event.get("detail") or {}
    status = detail.get("status", "UNKNOWN")
    sm_name = (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1]
    logger.info("SF status change: sf=%s status=%s", sm_name, status)

    text, silent = _build_message(detail)
    ok = _notify_via_flow_doctor(text, detail=detail, silent=silent)

    return {
        "status": status,
        "state_machine": sm_name,
        "execution": detail.get("name", ""),
        "telegram_sent": ok,
        "silent": silent,
    }
