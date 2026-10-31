"""The weekday SF's failure alert and terminal Cause name the failing state.

alpha-engine-config-I10974. ``ne-preopen-trading-pipeline`` failed on
2026-09-17 at ``CheckCodeFreshnessStatus`` and every operator surface said
"One or more weekday pipeline steps failed." The real error lived only inside
an SSM command invocation on the trading box. Every failure routed through
``HandleFailure`` ended with that same static Cause, whichever of the ~40
states failed.

The fix carries the failing state and its cause in two workflow variables,
``failed_state`` and ``failed_cause``. Variables rather than a ``$.failure``
state field because a Choice cannot write state data but CAN assign a
variable, and twelve of the edges into ``HandleFailure`` are Choice rules or
Defaults. ``InitializeInput`` (StartAt) assigns a floor for both, so a
reference in ``HandleFailure`` or ``FailExecution`` can never throw
States.Runtime and replace the real failure with a reporting error.

These tests pin:
  * every edge into HandleFailure assigns failed_state (or inherits it from
    a predecessor that does, for the force-stop hop), and the value is the
    name of a real state, so the SNS Subject bound holds;
  * InitializeInput assigns the floor;
  * FailExecution carries no static Cause and derives it from the variables;
  * the code-freshness Default carries the poll result, whose stderr is the
    poller's bounded tail, never an unbounded field.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
_DAILY = _REPO / "infrastructure" / "step_function_daily.json"
_POLLER = _REPO / "infrastructure" / "lambdas" / "ssm-liveness-poller" / "index.py"
HF = "HandleFailure"
# Reaches HandleFailure on both edges and deliberately assigns nothing: the
# Stamp*Unresponsive state that routed to it already did, and the force-stop
# outcome must not overwrite the real cause.
INHERITS = "ForceStopUnresponsiveInstance"


@pytest.fixture(scope="module")
def definition() -> dict:
    return json.loads(_DAILY.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def states(definition) -> dict:
    return definition["States"]


def _edges_into(states: dict, target: str):
    """Yield (owner_state, assign_block_or_None, kind) for every edge."""
    for name, s in states.items():
        for c in s.get("Catch", []):
            if c["Next"] == target:
                yield name, c.get("Assign"), "catch"
        if s["Type"] == "Choice":
            for r in s.get("Choices", []):
                if r["Next"] == target:
                    yield name, r.get("Assign"), "rule"
            if s.get("Default") == target:
                yield name, s.get("Assign"), "default"
        elif s.get("Next") == target:
            yield name, s.get("Assign"), "next"


def test_every_edge_into_handle_failure_names_its_state(states):
    edges = list(_edges_into(states, HF))
    assert len(edges) >= 30, "the funnel shrank; re-check this test's premise"
    missing = []
    for owner, assign, kind in edges:
        if owner == INHERITS:
            continue
        if not assign or assign.get("failed_state") != owner:
            missing.append(f"{owner} ({kind})")
        elif not ("failed_cause" in assign or "failed_cause.$" in assign):
            missing.append(f"{owner} ({kind}) sets no failed_cause")
    assert not missing, (
        "edges into HandleFailure that do not name their failing state: "
        + ", ".join(missing)
    )


def test_the_force_stop_hop_inherits_from_assigning_predecessors(states):
    preds = [n for n, a, k in _edges_into(states, INHERITS)]
    assert preds, "ForceStopUnresponsiveInstance has no predecessors"
    for p in preds:
        assert states[p]["Assign"]["failed_state"] == p
    # Its own edges must not overwrite the predecessor's cause.
    fs = states[INHERITS]
    assert "Assign" not in fs
    assert all("Assign" not in c for c in fs.get("Catch", []))


def test_every_assigned_failed_state_is_a_state_name(states):
    # The SNS Subject bound in test_sf_notifier_totality_wiring rests on this.
    def walk(node):
        if isinstance(node, dict):
            a = node.get("Assign")
            if isinstance(a, dict) and "failed_state" in a:
                yield a["failed_state"]
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, list):
            for v in node:
                yield from walk(v)

    values = set(walk(states))
    assert values - {"unrecorded"} <= set(states)
    assert all("failed_state.$" not in json.dumps(s) for s in states.values())


def test_initialize_input_assigns_the_floor(definition, states):
    assert definition["StartAt"] == "InitializeInput"
    a = states["InitializeInput"]["Assign"]
    assert a["failed_state"] == "unrecorded"
    assert a["failed_cause"]


def test_fail_execution_cause_is_derived_not_constant(states):
    fe = states["FailExecution"]
    assert "Cause" not in fe, "Cause and CausePath are mutually exclusive"
    assert "$failed_state" in fe["CausePath"]
    assert "$failed_cause" in fe["CausePath"]
    # Consumers key on the Error label; the per-run detail belongs in Cause.
    assert fe["Error"] == "DailyPipelineFailure"
    assert "ErrorPath" not in fe


def test_handle_failure_names_state_and_cause_before_the_link(states):
    params = states[HF]["Parameters"]
    assert "$failed_state" in params["Subject.$"]
    msg = params["Message.$"]
    link = msg.index("https://dashboard.nousergon.ai/pipeline-status")
    assert msg.index("failed at state {}") < link
    assert msg.index("Cause: {}") < link
    args = msg.rsplit("',", 1)[1]
    assert args.strip().startswith("$failed_state, $failed_cause")
    # The whole state still rides along (alpha-engine-config-I8685).
    assert "States.JsonToString($)" in msg


def test_code_freshness_default_carries_the_bounded_poll_result(states):
    a = states["CheckCodeFreshnessStatus"]["Assign"]
    assert a["failed_state"] == "CheckCodeFreshnessStatus"
    assert a["failed_cause.$"] == "States.JsonToString($.code_freshness_poll)"
    # That object's stderr is the poller's TAIL, never the raw content.
    sel = states["WaitForCodeFreshness"]["ResultSelector"]
    assert sel["StandardErrorContent.$"] == "$.Payload.stderr_tail"
    src = _POLLER.read_text(encoding="utf-8")
    cap = int(re.search(r"^_STDERR_TAIL_CHARS = (\d+)", src, re.M).group(1))
    assert cap <= 2000, "stderr tail must stay bounded well under SNS/Fail limits"
    assert re.search(r'"stderr_tail": \(inv\.get\("StandardErrorContent"\) or ""\)\[\s*-_STDERR_TAIL_CHARS:', src)
