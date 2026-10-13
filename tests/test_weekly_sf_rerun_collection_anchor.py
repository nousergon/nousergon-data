"""weekly_sf_rerun.py hands the rerun its cycle's collection anchor.

alpha-engine-config-I12228. The weekly SF grades collection manifests against
`$.collection_not_before`, floored to the execution's own start. A recovery
rerun must carry the failed cycle's ORIGINAL start instead, or it cannot see
the collection that already ran for its cycle. The anchor is the earliest of
the chain: each execution's InitializeInput output (which carries an anchor an
earlier rerun inherited), else its own start.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "weekly_sf_rerun.py"


@pytest.fixture(scope="module")
def mod():
    if "weekly_sf_rerun" in sys.modules:
        return sys.modules["weekly_sf_rerun"]
    spec = importlib.util.spec_from_file_location("weekly_sf_rerun", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    sys.modules["weekly_sf_rerun"] = m
    spec.loader.exec_module(m)
    return m


def _history(started, *, anchor=None, entered=("InitializeInput",)):
    inp = {"pipeline_role": "weekly", "run_date": "2026-10-09"}
    events = [{
        "type": "ExecutionStarted",
        "timestamp": started,
        "executionStartedEventDetails": {"input": json.dumps(inp)},
    }]
    if anchor is not None:
        events.append({
            "type": "PassStateExited",
            "stateExitedEventDetails": {
                "name": "InitializeInput",
                "output": json.dumps({**inp, "collection_not_before": anchor}),
            },
        })
    events += [
        {"type": "PassStateEntered", "stateEnteredEventDetails": {"name": n}} for n in entered
    ]
    return events


SCHEDULED = datetime(2026, 10, 10, 9, 0, 49, 274000, tzinfo=timezone.utc)
MANUAL = datetime(2026, 10, 10, 13, 44, 45, 167000, tzinfo=timezone.utc)


def test_a_pre_change_chain_anchors_on_its_earliest_start(mod):
    """Histories from before InitializeInput stamped the anchor: the scheduled
    run's own start wins over the later manual rerun's."""
    got = mod.cycle_collection_anchor([_history(SCHEDULED), _history(MANUAL)])
    assert got == "2026-10-10T09:00:49.274Z"


def test_an_inherited_anchor_wins_over_the_executions_own_start(mod):
    """A rerun that already carried the cycle's anchor passes it on, even when
    the chain lookup cannot see the original execution."""
    got = mod.cycle_collection_anchor(
        [_history(MANUAL, anchor="2026-10-10T09:00:49.274Z")]
    )
    assert got == "2026-10-10T09:00:49.274Z"


def test_recorded_string_timestamps_and_the_source_start_fallback(mod):
    assert mod.cycle_collection_anchor([_history("2026-10-10T09:00:49.274Z")]) == \
        "2026-10-10T09:00:49.274Z"
    bare = [{"type": "ExecutionStarted", "executionStartedEventDetails": {"input": "{}"}}]
    assert mod.cycle_collection_anchor([bare], SCHEDULED) == "2026-10-10T09:00:49.274Z"
    assert mod.cycle_collection_anchor([bare]) == ""


def test_the_anchor_reaches_the_emitted_input(mod):
    plan = mod.derive_plan(_history(MANUAL), start_time=MANUAL,
                           prior_histories=[("scheduled", _history(SCHEDULED))])
    assert plan.collection_not_before == "2026-10-10T09:00:49.274Z"
    assert plan.rerun_input()["collection_not_before"] == "2026-10-10T09:00:49.274Z"


def test_no_anchor_means_no_key(mod):
    """Nothing derivable: leave the key out and let the SF floor apply."""
    bare = [{"type": "ExecutionStarted",
             "executionStartedEventDetails": {"input": json.dumps({"run_date": "2026-10-09"})}}]
    plan = mod.derive_plan(bare)
    assert "collection_not_before" not in plan.rerun_input()
