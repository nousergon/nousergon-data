"""Shared flow-doctor Telegram routing for alpha-engine-data Lambdas (config#1742)."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from nousergon_lib.telegram import send_message

logger = logging.getLogger(__name__)

_FLOW_DOCTOR_BY_NAME: dict[str, Any | None] = {}
_INIT_ATTEMPTED: set[str] = set()


def reset_flow_doctor_cache() -> None:
    """Test hook — clear lazy-init state between handler invocations."""
    _FLOW_DOCTOR_BY_NAME.clear()
    _INIT_ATTEMPTED.clear()


def build_flow_doctor_config(
    flow_name: str,
    topics: Sequence[Any],
    *,
    db_basename: str,
    repo: str = "nousergon/nousergon-data",
) -> dict:
    from nousergon_lib.flow_doctor_fleet import fleet_telegram_notifier_dicts

    return {
        "flow_name": flow_name,
        "repo": repo,
        "owner": "@brianmcmahon",
        "notify": fleet_telegram_notifier_dicts(topics),
        "store": {"type": "sqlite", "path": f"/tmp/{db_basename}.db"},
        "dedup_cooldown_minutes": 1,
        "rate_limits": {"max_alerts_per_day": 100},
    }


def get_flow_doctor(
    flow_name: str,
    topics: Sequence[Any],
    *,
    db_basename: str,
) -> Any | None:
    if flow_name in _FLOW_DOCTOR_BY_NAME:
        return _FLOW_DOCTOR_BY_NAME[flow_name]
    if flow_name in _INIT_ATTEMPTED:
        return None
    _INIT_ATTEMPTED.add(flow_name)
    if os.environ.get("FLOW_DOCTOR_ENABLED", "1") != "1":
        _FLOW_DOCTOR_BY_NAME[flow_name] = None
        return None
    try:
        import yaml
        from nousergon_lib.logging import get_flow_doctor, setup_logging

        cfg = build_flow_doctor_config(flow_name, topics, db_basename=db_basename)
        path = Path(f"/tmp/flow_doctor_{db_basename}.yaml")
        path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
        setup_logging(flow_name, flow_doctor_yaml=str(path))
        fd = get_flow_doctor()
        _FLOW_DOCTOR_BY_NAME[flow_name] = fd
        return fd
    except Exception as exc:  # noqa: BLE001 — fall back to send_message
        logger.warning("flow-doctor init failed for %s: %s", flow_name, exc)
        _FLOW_DOCTOR_BY_NAME[flow_name] = None
        return None


def topic_telegram_notifier(fd: Any, topic: Any) -> Any | None:
    from flow_doctor.notify.telegram import TelegramNotifier
    from nousergon_lib.flow_doctor_fleet import fleet_telegram_thread_id_env

    want = os.environ.get(fleet_telegram_thread_id_env(topic))
    for notifier in fd._notifiers:
        if not isinstance(notifier, TelegramNotifier):
            continue
        thread_id = getattr(notifier, "message_thread_id", None)
        if thread_id is not None and str(thread_id) == str(want):
            return notifier
    return None


def notify_via_flow_doctor(
    text: str,
    *,
    silent: bool,
    severity: str,
    dedup_key: str,
    flow_name: str,
    topics: Sequence[Any],
    db_basename: str,
    context: Optional[Dict[str, Any]] = None,
    silent_topic: Any | None = None,
) -> bool:
    """Route ``text`` through flow-doctor forum topics; fallback to ``send_message``."""
    fd = get_flow_doctor(flow_name, topics, db_basename=db_basename)
    if fd is None:
        return send_message(text, disable_notification=silent)

    subject = text.replace("*", "").strip()

    if silent and silent_topic is not None:
        notifier = topic_telegram_notifier(fd, silent_topic)
        if notifier is not None:
            return notifier.send_raw(text, disable_notification=True) is not None

    report_id = fd.notify_event(
        subject,
        body=None,
        severity=severity,
        context=context or {},
        dedup_key=dedup_key,
    )
    return report_id is not None
