"""Generic cross-SF guard: any Step Function that issues ``ssm:sendCommand``
against ``$.trading_instance_id`` MUST first pass through an
``ec2:startInstances`` + ``describeInstanceInformation`` (``PingStatus ==
Online``) readiness gate — for EVERY ``infrastructure/step_function*.json``,
not just the ones that happen to have been patched after an incident.

Origin — 2026-06-30 EOD (``ne-postclose-trading-pipeline``) recovery rerun
``watch-rerun-2026-06-30-1`` died at ``EODReconcile``'s ``ssm:sendCommand``
with ``Ssm.InvalidInstanceIdException: Instances not in a valid state``: the
trading instance was stopped by the prior run's terminal
``ForceStopInstance``, and the EOD SF had no "start instance" step, so every
recovery rerun deterministically hit a down box. Fixed for the EOD SF in
nousergon-data#576 (``StartTradingInstance`` -> SSM-readiness poll at the
post-mutex chokepoint) — the same ``InvalidInstanceIdException``-on-not-ready
class the daily/preopen SF already solved in config#1430
(``StartExecutorEC2`` -> ``SSMReadyChoice``).

The invariant is now implemented **by hand, per SF** (config#1552). ASL has no
include mechanism, so nothing stops a future or third SF — or a refactor that
drops the block from an existing one — from re-opening the class at site N+1.
This test lifts the hand-maintained per-SF invariant to a single generic,
statically-derived guard: for every ``step_function*.json`` that contains an
``ssm:sendCommand`` targeting ``InstanceIds.$ == "$.trading_instance_id"``,
assert (a) a readiness gate (start + describe-with-Online-choice) exists, and
(b) by static forward reachability from ``StartAt``, no such ``sendCommand``
is reachable without first crossing the gate's Online branch.

Scope note: ``step_function.json`` (the Saturday/weekly research SF) issues
``ssm:sendCommand`` against a DIFFERENT instance variable
(``$.ec2_instance_id``) and is out of scope for this specific invariant —
this guard only asserts on the ``$.trading_instance_id`` shape the 2026-06-30
incident and config#1430 both concern. ``step_function_groom.json`` issues no
``ssm:sendCommand`` at all and trivially passes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_TRADING_INSTANCE_VAR = "$.trading_instance_id"

_SF_FILES = sorted(p.name for p in _INFRA.glob("step_function*.json"))


def _load(sf_file: str) -> dict:
    return json.loads((_INFRA / sf_file).read_text())


def _send_command_states(states: dict) -> dict[str, dict]:
    """Task states that issue ssm:sendCommand against the trading instance."""
    out = {}
    for name, st in states.items():
        if st.get("Type") != "Task":
            continue
        if "ssm:sendCommand" not in str(st.get("Resource", "")):
            continue
        instance_ids = st.get("Parameters", {}).get("InstanceIds.$")
        if instance_ids == _TRADING_INSTANCE_VAR:
            out[name] = st
    return out


def _start_instance_states(states: dict) -> dict[str, dict]:
    out = {}
    for name, st in states.items():
        if st.get("Type") != "Task":
            continue
        if "ec2:startInstances" not in str(st.get("Resource", "")):
            continue
        if st.get("Parameters", {}).get("InstanceIds.$") == _TRADING_INSTANCE_VAR:
            out[name] = st
    return out


def _online_choice_edges(states: dict) -> list[tuple[str, str]]:
    """(choice_state_name, next_state) for every Choice branch that routes on
    PingStatus == "Online" for the trading instance's describeInstanceInformation
    poll — the readiness gate's "box is up" edge."""
    # First, find describeInstanceInformation states filtered on the trading
    # instance, to anchor which ResultPath / downstream Choice is the gate.
    describe_names = set()
    for name, st in states.items():
        if st.get("Type") != "Task":
            continue
        if "ssm:describeInstanceInformation" not in str(st.get("Resource", "")):
            continue
        filters = st.get("Parameters", {}).get("Filters", [])
        if any(f.get("Key") == "InstanceIds" and f.get("Values.$") == _TRADING_INSTANCE_VAR
               for f in filters):
            describe_names.add(name)
    if not describe_names:
        return []

    edges = []
    for name, st in states.items():
        if st.get("Type") != "Choice":
            continue
        for choice in st.get("Choices", []):
            conditions = choice.get("And", [choice])
            is_online = any(
                c.get("StringEquals") == "Online" and "PingStatus" in str(c.get("Variable", ""))
                for c in conditions
            )
            if is_online and "Next" in choice:
                edges.append((name, choice["Next"]))
    return edges


def _forward_graph(states: dict) -> dict[str, set[str]]:
    """Happy-path forward edges only (Next / Default / Choices[].Next) — Catch
    edges to HandleFailure are deliberately excluded, matching the precedent
    in test_sf_eod_instance_start_wiring.py: an error path bypassing the gate
    doesn't reach real work, only the failure handler."""
    graph: dict[str, set[str]] = {name: set() for name in states}
    for name, st in states.items():
        if "Next" in st:
            graph[name].add(st["Next"])
        if st.get("Type") == "Choice":
            for choice in st.get("Choices", []):
                if "Next" in choice:
                    graph[name].add(choice["Next"])
            if "Default" in st:
                graph[name].add(st["Default"])
    return graph


def _reachable(graph: dict[str, set[str]], start: str, blocked_edges: set[tuple[str, str]]) -> set[str]:
    seen = {start}
    stack = [start]
    while stack:
        cur = stack.pop()
        for nxt in graph.get(cur, ()):
            if (cur, nxt) in blocked_edges:
                continue
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


@pytest.fixture(scope="module")
def sf_docs() -> dict[str, dict]:
    return {f: _load(f) for f in _SF_FILES}


def test_sf_files_discovered(sf_docs):
    assert sf_docs, "no step_function*.json definitions found in infrastructure/"


@pytest.mark.parametrize("sf_file", _SF_FILES)
class TestTradingInstanceReadinessGate:
    def test_readiness_gate_present_when_gated_sendcommand_exists(self, sf_file, sf_docs):
        states = sf_docs[sf_file]["States"]
        sends = _send_command_states(states)
        if not sends:
            pytest.skip(f"{sf_file}: no ssm:sendCommand targets {_TRADING_INSTANCE_VAR}")

        starts = _start_instance_states(states)
        assert starts, (
            f"{sf_file} issues ssm:sendCommand against {_TRADING_INSTANCE_VAR} "
            f"({sorted(sends)}) but has no ec2:startInstances state for it — "
            f"a stopped/never-booted box will hard-fail every sendCommand with "
            f"Ssm.InvalidInstanceIdException (2026-06-30 EOD rerun incident, "
            f"config#1552)."
        )

        online_edges = _online_choice_edges(states)
        assert online_edges, (
            f"{sf_file} has a describeInstanceInformation poll on "
            f"{_TRADING_INSTANCE_VAR} but no Choice state branches on "
            f"PingStatus == \"Online\" — the readiness poll exists but nothing "
            f"gates work on it."
        )

    def test_sendcommand_unreachable_without_crossing_readiness_gate(self, sf_file, sf_docs):
        """Static reachability: remove the readiness gate's Online edge(s) from
        the graph; no trading-instance sendCommand may still be reachable from
        StartAt. This is what would catch a future SF (or a refactor of an
        existing one) that re-opens the class by routing around the gate."""
        doc = sf_docs[sf_file]
        states = doc["States"]
        sends = _send_command_states(states)
        if not sends:
            pytest.skip(f"{sf_file}: no ssm:sendCommand targets {_TRADING_INSTANCE_VAR}")

        online_edges = set(_online_choice_edges(states))
        graph = _forward_graph(states)
        reachable_without_gate = _reachable(graph, doc["StartAt"], blocked_edges=online_edges)

        leaked = set(sends) & reachable_without_gate
        assert not leaked, (
            f"{sf_file}: {sorted(leaked)} issue ssm:sendCommand against "
            f"{_TRADING_INSTANCE_VAR} but are reachable from StartAt without "
            f"first crossing the readiness gate's PingStatus==Online branch — "
            f"a box that's stopped/never-registered can reach a sendCommand "
            f"here (the exact 2026-06-30 EOD rerun failure, generalized "
            f"cross-SF per config#1552)."
        )


def test_guard_would_have_caught_the_pre_576_topology(sf_docs):
    """Meta-test: prove the guard is load-bearing, not vacuously green. Rewire
    a deep copy of the EOD SF back to its PRE-#576 topology — the entry paths
    (``CheckMutexRole`` default, ``AcquireMutex`` success/fail-open) routed
    straight to ``CheckSkipPostMarketData``, skipping ``StartTradingInstance``
    and the readiness poll entirely, which is the actual 2026-06-30 incident
    shape — and confirm the SAME reachability check the parametrized test
    above uses flags ``EODReconcile`` as reachable without crossing the gate.
    If this meta-test doesn't fail on the old topology, the guard is vacuous."""
    doc = sf_docs["step_function_eod.json"]
    states = json.loads(json.dumps(doc["States"]))  # deep copy — do not mutate the fixture
    states["CheckMutexRole"]["Default"] = "CheckSkipPostMarketData"
    states["AcquireMutex"]["Next"] = "CheckSkipPostMarketData"
    for c in states["AcquireMutex"]["Catch"]:
        if "States.ALL" in c["ErrorEquals"]:
            c["Next"] = "CheckSkipPostMarketData"

    sends = _send_command_states(states)
    assert "EODReconcile" in sends

    online_edges = set(_online_choice_edges(states))
    graph = _forward_graph(states)
    reachable_without_gate = _reachable(graph, doc["StartAt"], blocked_edges=online_edges)

    assert "EODReconcile" in reachable_without_gate, (
        "meta-test failed to reproduce the pre-#576 incident shape: "
        "EODReconcile should be reachable without crossing the readiness "
        "gate once the mutex entry paths bypass StartTradingInstance — if "
        "it isn't, this meta-test isn't actually proving the guard above is "
        "load-bearing."
    )
