"""Pins the trading box's stop between the two post-close halves.

Brian, 2026-10-01: *"can we shut it down after postclose part 1 and boot it
back up when part 2 is ready to begin?"*

After the 2026-09-30 split (#1996) ``ne-postclose-trading-pipeline`` ends
~16:02 ET with the box up, and ``ne-postclose-reconcile-pipeline`` starts only
when ``ne-data-collection-eod`` (18:15 ET) finishes. The
``alpha-engine-idle-stop-trading`` schedule stops the box in between. Its time
is bounded on both sides:

* after every crucible-executor post-close timer that needs the box, in both
  DST seasons (trader-reconcile 16:45 America/New_York; eod-reconcile-standalone
  21:05 UTC and reference-rate-publish 21:15 UTC, i.e. 17:15 EDT at the latest);
* before the collection starts, so it can never land on a running reconcile.

Restart costs nothing new because the reconcile pipeline opens with
StartTradingInstance + the SSM-readiness poll.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

REPO = Path(__file__).resolve().parent.parent
INFRA = REPO / "infrastructure"
CFN = INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"
RECONCILE = INFRA / "step_function_eod_reconcile.json"
MANIFEST = INFRA / "automation_pause.json"

NAME = "alpha-engine-idle-stop-trading"
ET = ZoneInfo("America/New_York")

#: Co-tenant timers on the trading box that must have fired before the stop.
_ET_TIMERS = [time(16, 45)]
_UTC_TIMERS = [time(21, 5), time(21, 15)]
#: Run-time allowance after the latest timer fires.
_MARGIN_MIN = 20
#: ne-data-collection-eod's cron (data-collection-eod schedule, America/New_York).
_COLLECTION_START_ET = time(18, 15)


@pytest.fixture(scope="module")
def block() -> str:
    src = CFN.read_text(encoding="utf-8")
    m = re.search(rf"^\s*Name: {re.escape(NAME)}\s*$", src, re.MULTILINE)
    assert m, f"{NAME} is not declared in {CFN.name}"
    return src[m.end(): m.end() + 800]


def _stop_time_et(block: str) -> time:
    m = re.search(r"ScheduleExpression: 'cron\((\d+) (\d+) \? \* MON-FRI \*\)'", block)
    assert m, block
    assert "ScheduleExpressionTimezone: 'America/New_York'" in block
    return time(int(m.group(2)), int(m.group(1)))


def test_it_stops_the_trading_instance(block):
    assert "State: ENABLED" in block
    assert "Arn: 'arn:aws:scheduler:::aws-sdk:ec2:stopInstances'" in block
    assert '{"InstanceIds": ["${TradingInstanceId}"]}' in block


@pytest.mark.parametrize("day", ["2026-07-15", "2026-12-15"], ids=["EDT", "EST"])
def test_it_fires_after_every_co_tenant_timer_and_before_the_collection(block, day):
    d = datetime.fromisoformat(day).date()
    stop = datetime.combine(d, _stop_time_et(block), ET)
    fires = [datetime.combine(d, t, ET) for t in _ET_TIMERS]
    fires += [datetime.combine(d, t, timezone.utc) for t in _UTC_TIMERS]
    latest = max(fires)
    assert (stop - latest).total_seconds() >= _MARGIN_MIN * 60, (stop, latest)
    assert stop < datetime.combine(d, _COLLECTION_START_ET, ET)


def test_the_reconcile_pipeline_starts_the_box_before_any_ssm_call():
    doc = json.loads(RECONCILE.read_text(encoding="utf-8"))
    states = doc["States"]
    # Walk the first-Next chain from StartAt until the first sendCommand.
    seen, name = [], doc["StartAt"]
    while name and name not in seen:
        seen.append(name)
        st = states[name]
        if st.get("Resource", "").endswith(":ssm:sendCommand"):
            break
        nxt = st.get("Next") or st.get("Default")
        if st.get("Type") == "Choice":
            # Normal path: the gate's PROCEED / mutex-acquire branch.
            nxt = next((c["Next"] for c in st["Choices"]
                        if c["Next"] in {"CheckMutexRole", "AcquireMutex",
                                         "StartTradingInstance"}), st.get("Default"))
        name = nxt
    assert "StartTradingInstance" in seen, seen
    assert seen.index("StartTradingInstance") < seen.index("DescribeInstanceInfo")


def test_it_is_classified_as_kept_in_the_pause_manifest():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert NAME in manifest["not_paused"]
