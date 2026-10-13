"""The weekly run-slot mutex is released by a failed run and taken over from one.

alpha-engine-config-I12228. Measured 2026-10-10: the scheduled weekly
(`37870cca-..._a83d0c88-...`, started 09:00Z) FAILED at 10:55Z on the
collection readiness gate and never released its DynamoDB run-slot item
`ne-weekly-freshness-pipeline#weekly#2026-10-09` (24h TTL). The operator rerun
`weekly-rerun-2026-10-10-1` (pipeline_role "weekly") lost AcquireMutex to it.
At that hour the Catch still named `DynamoDB.ConditionalCheckFailedException`
(upper-case B, the optimized-integration namespace) while the aws-sdk task
raises `DynamoDb.ConditionalCheckFailedException`, so the conflict fell into
the States.ALL fail-open and the rerun ran every stage only to end DegradedRun
with reason weekly_mutex_acquire_fail_open. nousergon-data#2137 fixed the
namespace the same evening; with nothing else changed, the next such rerun
would have hit MutexConflict instead. Two changes close that:

(a) every Fail terminal releases the slot THIS execution holds, best-effort;
(b) a held slot whose holder is terminal-FAILED is taken over in-machine,
    under the same rule scripts/weekly_sf_rerun.py already applied by hand.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from tests.sf_run_slot_release_helpers import (
    RELEASING_TERMINALS,
    assert_release_hop,
    release_gate,
    release_task,
)

ROOT = Path(__file__).resolve().parents[1]
SF = ROOT / "infrastructure" / "step_function.json"
SCRIPT = ROOT / "scripts" / "weekly_sf_rerun.py"
TABLE = "alpha-engine-sf-execution-mutex"
HOLDER_ARN = "$.mutex_holder.Item.execution_arn.S"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(SF.read_text())["States"]


@pytest.fixture(scope="module")
def rerun_mod():
    if "weekly_sf_rerun" in sys.modules:
        return sys.modules["weekly_sf_rerun"]
    spec = importlib.util.spec_from_file_location("weekly_sf_rerun", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    sys.modules["weekly_sf_rerun"] = m
    spec.loader.exec_module(m)
    return m


def _key(state: dict) -> str:
    params = state["Parameters"]
    item = params.get("Item") or params.get("Key")
    return item["mutex_key"]["S.$"]


# ── (b) takeover ──────────────────────────────────────────────────────────────


def test_a_held_slot_is_inspected_not_failed_on_sight(states):
    (ccf,) = [
        c for c in states["AcquireMutex"]["Catch"]
        if c["ErrorEquals"] == ["DynamoDb.ConditionalCheckFailedException"]
    ]
    assert ccf["Next"] == "ReadRunSlotHolder"
    read = states["ReadRunSlotHolder"]
    assert read["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:getItem"
    assert read["Parameters"]["TableName"] == TABLE
    assert read["Parameters"]["ConsistentRead"] is True
    assert _key(read) == _key(states["AcquireMutex"]), (
        "the read must name exactly the item AcquireMutex lost on"
    )


@pytest.mark.parametrize("name", ["ReadRunSlotHolder", "DescribeRunSlotHolder"])
def test_an_unreadable_holder_is_a_conflict_never_a_takeover(states, name):
    catches = states[name]["Catch"]
    assert [c["ErrorEquals"] for c in catches] == [["States.ALL"]]
    assert all(c["Next"] == "MutexConflict" for c in catches)


def test_a_vanished_or_anonymous_holder_is_a_conflict(states):
    gate = states["CheckRunSlotHolderPresent"]
    assert gate["Default"] == "MutexConflict"
    (rule,) = gate["Choices"]
    assert rule == {"Variable": HOLDER_ARN, "IsPresent": True, "Next": "DescribeRunSlotHolder"}
    describe = states["DescribeRunSlotHolder"]
    assert describe["Resource"] == "arn:aws:states:::aws-sdk:sfn:describeExecution"
    assert describe["Parameters"] == {"ExecutionArn.$": HOLDER_ARN}
    assert "ResultSelector" not in describe, (
        "an absent ResultSelector path throws States.Runtime, which no Catch "
        "takes; the status is read behind an IsPresent guard instead"
    )
    assert describe["ResultPath"] == "$.mutex_holder_execution"
    assert describe["Next"] == "CheckRunSlotHolderTerminal"


_STATUS = "$.mutex_holder_execution.Status"


def _matches(rule: dict, status: str | None) -> bool:
    """A minimal evaluator for the Choice operators this state uses."""
    if "And" in rule:
        return all(_matches(r, status) for r in rule["And"])
    if "Or" in rule:
        return any(_matches(r, status) for r in rule["Or"])
    assert rule["Variable"] == _STATUS
    if "IsPresent" in rule:
        return (status is not None) == rule["IsPresent"]
    assert status is not None, "an unguarded read of an absent path throws States.Runtime"
    return status == rule["StringEquals"]


def _route_for(states: dict, status: str | None) -> str:
    choice = states["CheckRunSlotHolderTerminal"]
    for rule in choice["Choices"]:
        if _matches(rule, status):
            return rule["Next"]
    return choice["Default"]


def test_an_unexpected_describe_shape_is_a_conflict(states):
    assert _route_for(states, None) == "MutexConflict"


_STATUSES = ("RUNNING", "PENDING_REDRIVE", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")


@pytest.mark.parametrize("status", _STATUSES)
def test_the_takeover_rule_is_the_rerun_tools_steal_rule(states, rerun_mod, status):
    """One rule, two enforcers: the machine takes over exactly when
    scripts/weekly_sf_rerun.py::decide_mutex_action would steal."""
    item = {"execution_arn": {"S": "arn:aws:states:us-east-1:1:execution:sm:holder"}}
    decision = rerun_mod.decide_mutex_action(item, status, "k", "src")
    expected = "TakeOverRunSlot" if decision.action == "steal" else "MutexConflict"
    assert _route_for(states, status) == expected


def test_the_takeover_replaces_only_the_holder_it_proved_terminal(states):
    take = states["TakeOverRunSlot"]
    acquire = states["AcquireMutex"]
    assert take["Resource"] == acquire["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:putItem"
    params = take["Parameters"]
    assert params["TableName"] == TABLE
    assert params["ConditionExpression"] == "attribute_not_exists(mutex_key) OR execution_arn = :holder"
    assert params["ExpressionAttributeValues"] == {":holder": {"S.$": HOLDER_ARN}}
    item = dict(params["Item"])
    assert item.pop("taken_over_from") == {"S.$": HOLDER_ARN}
    assert item == acquire["Parameters"]["Item"], (
        "a taken-over item must be the item a clean AcquireMutex writes, so the "
        "release and the next conflict read it the same way"
    )
    assert take["ResultPath"] == acquire["ResultPath"] == "$.mutex_result"
    assert take["Next"] == acquire["Next"], "a takeover proceeds exactly like a clean acquire"
    routes = {tuple(c["ErrorEquals"]): c["Next"] for c in take["Catch"]}
    assert routes[("DynamoDb.ConditionalCheckFailedException",)] == "MutexConflict"
    assert routes[("States.ALL",)] == "SetMutexAcquireDegradedFlag"


def test_a_takeover_is_not_degraded(states):
    """The slot was proven free; nothing on the takeover path sets a flag."""
    for name in ("ReadRunSlotHolder", "CheckRunSlotHolderPresent", "DescribeRunSlotHolder",
                 "CheckRunSlotHolderTerminal", "TakeOverRunSlot"):
        assert "degraded" not in str(states[name].get("ResultPath", ""))


# ── (a) release ───────────────────────────────────────────────────────────────


def _inbound(states: dict, target: str) -> set[str]:
    found = set()
    for name, st in states.items():
        edges = [st.get("Next"), st.get("Default")]
        edges += [c.get("Next") for c in st.get("Choices", [])]
        edges += [c.get("Next") for c in st.get("Catch", [])]
        if target in edges:
            found.add(name)
    return found


def test_every_fail_terminal_but_the_conflict_releases(states):
    """A new Fail terminal must decide: release, or be MutexConflict (which
    never held the slot)."""
    fails = {n for n, s in states.items() if s["Type"] == "Fail"}
    assert fails == set(RELEASING_TERMINALS) | {"MutexConflict"}


@pytest.mark.parametrize("terminal", RELEASING_TERMINALS)
def test_the_release_hop_is_the_only_way_into_the_terminal(states, terminal):
    assert_release_hop(states, terminal)
    assert _inbound(states, terminal) == {release_gate(terminal), release_task(terminal)}
    assert _inbound(states, release_task(terminal)) == {release_gate(terminal)}


@pytest.mark.parametrize("terminal", RELEASING_TERMINALS)
def test_the_release_deletes_only_this_executions_own_item(states, terminal):
    task = states[release_task(terminal)]
    params = task["Parameters"]
    assert params["TableName"] == TABLE
    assert _key(task) == _key(states["AcquireMutex"])
    assert params["ConditionExpression"] == "execution_arn = :me"
    assert params["ExpressionAttributeValues"] == {":me": {"S.$": "$$.Execution.Id"}}


def test_only_an_acquisition_writes_the_release_gate_flag(states):
    """$.mutex_result is the release gate's whole guard: it must mean "this
    execution holds the slot" and nothing else."""
    writers = {n for n, s in states.items() if s.get("ResultPath") == "$.mutex_result"}
    assert writers == {"AcquireMutex", "TakeOverRunSlot"}
    for st in states.values():
        for c in st.get("Catch", []):
            assert c.get("ResultPath") != "$.mutex_result"
    assert "mutex_result" not in states["InitializeInput"]["Parameters"]["merged.$"]


def test_a_succeeded_slot_keeps_its_item(states):
    """Deliberate: a completed slot must still refuse a later duplicate trigger
    (the run-slot key's whole purpose, config#2280). Only Fail terminals release."""
    succeeds = {n for n, s in states.items() if s["Type"] == "Succeed"}
    assert succeeds
    for name in succeeds:
        assert not any(i.startswith("ReleaseRunSlot") for i in _inbound(states, name))
