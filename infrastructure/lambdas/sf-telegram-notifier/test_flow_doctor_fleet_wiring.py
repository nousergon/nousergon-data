"""sf-telegram-notifier flow-doctor config matches fleet canonical layout."""

from __future__ import annotations

from nousergon_lib.flow_doctor_fleet import (
    PIPELINE_OBSERVER_TELEGRAM_TOPICS,
    fleet_telegram_notifier_dicts,
)

from index import build_flow_doctor_config


def test_build_flow_doctor_config_matches_pipeline_observer_topics():
    cfg = build_flow_doctor_config()
    telegram_blocks = [n for n in cfg["notify"] if n.get("type") == "telegram"]
    expected = fleet_telegram_notifier_dicts(PIPELINE_OBSERVER_TELEGRAM_TOPICS)
    assert telegram_blocks == expected
