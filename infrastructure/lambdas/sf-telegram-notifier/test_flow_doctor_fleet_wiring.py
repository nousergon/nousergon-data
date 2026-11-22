"""sf-telegram-notifier flow-doctor config: the daily pipelines message has
one destination, the same for success and failure (Brian 2026-11-14)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nousergon_lib.flow_doctor_fleet import (
    FleetTelegramTopic,
    fleet_telegram_notifier_dicts,
    fleet_telegram_topic_spec,
)

from index import build_flow_doctor_config_for_tests


def test_day_message_goes_to_the_pipeline_topic_only():
    cfg = build_flow_doctor_config_for_tests()
    telegram_blocks = [n for n in cfg["notify"] if n.get("type") == "telegram"]
    assert telegram_blocks == fleet_telegram_notifier_dicts((FleetTelegramTopic.PIPELINE,))


def test_success_and_failure_reach_the_same_channels():
    # info = all succeeded, warning = something failed. Every topic the
    # message can reach must accept both, or the outcomes land in different places.
    assert {"info", "warning"} <= set(fleet_telegram_topic_spec(FleetTelegramTopic.PIPELINE).notify_on)
