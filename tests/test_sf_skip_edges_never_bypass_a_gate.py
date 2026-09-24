"""No ``skip_*`` flag may route around a gate — alpha-engine-config-I11268.

The defect this closes: in ``infrastructure/step_function.json`` the
``CheckSkipMorningEnrich`` skip branch (``skip_morning_enrich: true``) went
straight to ``CheckSkipDataPhase1``, while its Default went through
``SubstrateHealthGate``. So setting a flag that means "skip morning-enrich"
also skipped the substrate health gate for the ENTIRE weekly run — every
stage after it ran with the gate never evaluated, and nothing downstream
recorded the bypass. The gate existed, was correct, and was unreachable on
that path.

Two layers:

1. The instance: ``SubstrateHealthGate`` now sits between acquiring the
   weekly box and ``CheckShellRun``, so every run that has a box to dispatch
   onto is gated before any stage (and any ``CheckSkip*``) is reached. The
   only gate-free route is ``CheckSpotDispatchNeeded``'s derived no-box
   bypass, which acquires no box and is proven to reach no box stage.
2. The class, over all three definitions: for every ``CheckSkip*`` Choice
   branch whose condition reads a ``$.skip_*`` flag, every gate that is a
   MANDATORY waypoint on the Default path to the point where the two paths
   rejoin must also be a mandatory waypoint on the skip path — minus a gate
   that belongs to the skipped work itself, which is listed below with its
   reason.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
DEFINITIONS = ("step_function.json", "step_function_daily.json", "step_function_eod.json")

#: Gates a skip branch MAY bypass because the gate IS the skipped work (or
#: exists only to protect it). Keyed ``(definition, CheckSkip state)``.
#: Exhaustive and add-by-PR-only: an entry here is an argument that the gate
#: protects nothing but the stage the flag skips. Empty today — the class
#: test found no such case in any of the three definitions.
SKIPPED_WORK_GATES: dict[tuple[str, str], frozenset[str]] = {}


def _load(name: str) -> dict:
    return json.loads((INFRA / name).read_text())["States"]


def _successors(state: dict, *, recovery: bool = True) -> list[str]:
    out = [state[k] for k in ("Next", "Default") if k in state]
    out += [c["Next"] for c in state.get("Choices", [])]
    if recovery:
        out += [c["Next"] for c in state.get("Catch", [])]
    return out


def _reachable(states: dict, start: str, *, banned: frozenset = frozenset(), recovery: bool = True) -> set[str]:
    seen: set[str] = set()
    stack = [start]
    while stack:
        n = stack.pop()
        if n in seen or n in banned or n not in states:
            continue
        seen.add(n)
        stack.extend(_successors(states[n], recovery=recovery))
    return seen


def _is_gate(name: str) -> bool:
    return name.endswith("Gate")


def _reads_skip_flag(choice: dict) -> bool:
    return '"$.skip_' in json.dumps(choice)


def _rejoin(states: dict, target: str) -> str:
    """Where a skip branch rejoins the pipeline: its target, or — when the
    target is a ``Pass`` marker that only records the skip (e.g.
    ``MarkParityVerdictUnknownByCadence``) — the first non-Pass state after it."""
    seen = set()
    while states.get(target, {}).get("Type") == "Pass" and target not in seen:
        seen.add(target)
        target = states[target]["Next"]
    return target


def _mandatory_gates(states: dict, start: str, goal: str) -> set[str]:
    """Gates every path from ``start`` to ``goal`` passes through."""
    return {
        g for g in _reachable(states, start)
        if _is_gate(g) and goal not in _reachable(states, start, banned=frozenset({g}))
    }


def _skip_branches(states: dict):
    for name, state in states.items():
        if not name.startswith("CheckSkip") or state.get("Type") != "Choice":
            continue
        for choice in state.get("Choices", []):
            if _reads_skip_flag(choice):
                yield name, state, choice


def _bypasses(definition: str, states: dict) -> list[str]:
    found = []
    for name, state, choice in _skip_branches(states):
        default = state["Default"]
        rejoin = _rejoin(states, choice["Next"])
        assert rejoin in _reachable(states, default), (
            f"{definition}:{name}: the skip branch rejoins at {rejoin!r}, which the Default "
            f"branch ({default!r}) never reaches — the invariant cannot be evaluated. Extend "
            "_rejoin() for the new shape rather than skipping this state."
        )
        on_default = _mandatory_gates(states, default, rejoin)
        on_skip = _mandatory_gates(states, choice["Next"], rejoin) if choice["Next"] != rejoin else set()
        allowed = SKIPPED_WORK_GATES.get((definition, name), frozenset())
        for gate in sorted(on_default - on_skip - allowed):
            found.append(
                f"{definition}:{name} skip branch -> {choice['Next']!r} bypasses {gate!r}, "
                f"which every Default path to {rejoin!r} passes through"
            )
    return found


@pytest.mark.parametrize("definition", DEFINITIONS)
def test_no_skip_flag_routes_around_a_gate(definition):
    states = _load(definition)
    assert list(_skip_branches(states)), f"{definition} has no CheckSkip* skip branch to check"
    assert _bypasses(definition, states) == []


def _rerun_module():
    script = INFRA.parent / "scripts" / "weekly_sf_rerun.py"
    spec = importlib.util.spec_from_file_location("weekly_sf_rerun", script)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["weekly_sf_rerun"] = mod
    spec.loader.exec_module(mod)
    return mod


def _edges_into(states: dict, target: str) -> set[tuple[str, str]]:
    """``(source, how)`` for every forward edge into ``target``."""
    out = set()
    for name, state in states.items():
        for key in ("Next", "Default"):
            if state.get(key) == target:
                out.add((name, key))
        for i, rule in enumerate(state.get("Choices", [])):
            if rule.get("Next") == target:
                out.add((name, f"Choices[{i}]"))
    return out


def test_every_box_acquisition_exits_through_the_substrate_gate():
    """The instance (I11268 deliverable 1). The gate sits between acquiring
    the weekly box and CheckShellRun — the convergence every box stage is
    downstream of — so it is entered whatever skip_* flags the run carries.
    The ONLY other forward edge into CheckShellRun is CheckSpotDispatchNeeded's
    no-box bypass, which acquires no box (nothing to gate) and is proven below
    to reach no box-addressing state. Relaunch re-entry after a lost
    bootstrap box goes through the gate too."""
    definition = json.loads((INFRA / "step_function.json").read_text())
    states = definition["States"]
    healthy = [
        rule["Next"] for rule in states["CheckSubstrateHealthGate"]["Choices"]
        if "CheckShellRun" in _reachable(states, rule["Next"], recovery=False)
    ]
    assert len(healthy) == 1, "CheckSubstrateHealthGate's HEALTHY edge no longer leads to CheckShellRun"
    # alpha-engine-config-I11312: the HEALTHY edge now enters the observe-mode
    # on-spot preflight pass before CheckShellRun. Everything between the two
    # is "behind the gate": entered from nowhere but the HEALTHY edge.
    behind = _reachable(states, healthy[0], banned=frozenset({"CheckShellRun"}))
    for name, state in states.items():
        if name in behind or name == "CheckSubstrateHealthGate":
            continue
        leaked = set(_successors(state)) & behind
        assert not leaked, f"{name} enters {sorted(leaked)} without passing SubstrateHealthGate"
    gated_exits = {
        (src, how) for src, how in _edges_into(states, "CheckShellRun") if src in behind
    }
    assert gated_exits, "nothing behind the gate reaches CheckShellRun"

    wsr = _rerun_module()
    bypass = wsr.spot_dispatch_bypass_rule(definition)
    dispatch = states[wsr.SPOT_DISPATCH_GATE]
    bypass_edges = {
        (wsr.SPOT_DISPATCH_GATE, f"Choices[{i}]")
        for i, rule in enumerate(dispatch["Choices"])
        if rule == bypass
    }
    assert bypass_edges, "CheckSpotDispatchNeeded no longer carries the derived no-box bypass"
    assert _edges_into(states, "CheckShellRun") == gated_exits | bypass_edges

    # Every acquisition route reaches CheckShellRun only THROUGH the gate.
    for start in ("DispatchWeeklyFreshnessSpot", "NormalizeEc2InstanceId", "ResumeAfterSubstrateRelaunch"):
        assert "CheckShellRun" in _reachable(states, start, recovery=False)
        assert "CheckShellRun" not in _reachable(
            states, start, banned=frozenset({"SubstrateHealthGate"}), recovery=False,
        ), f"{start} reaches CheckShellRun without entering SubstrateHealthGate"

    # The gate-free bypass is sound: with its flags set, no box stage can run.
    flags = {f: True for f in wsr.box_dispatch_flags(definition)}
    assert wsr.box_states_needing_dispatch(definition, flags) == set()


def test_skip_morning_enrich_still_skips_only_morning_enrich():
    """The flag keeps its meaning: CheckSkipMorningEnrich routes to MorningEnrich,
    or past it to CheckSkipDataPhase1 — and the gate is already behind it."""
    states = _load("step_function.json")
    gate = states["CheckSkipMorningEnrich"]
    assert gate["Default"] == "MorningEnrich"
    assert [c["Next"] for c in gate["Choices"]] == ["CheckSkipDataPhase1"]
    assert states["CheckShellRun"]["Default"] == "CheckSkipMorningEnrich"
    assert states["ApplyShellRunDefaults"]["Next"] == "CheckSkipMorningEnrich"


def test_a_reintroduced_bypass_is_caught():
    """The class test must fail on the pre-fix shape (I11268's closes-when)."""
    states = _load("step_function.json")
    for name in ("NormalizeEc2InstanceId", "WrapEc2InstanceIdInArray", "RouteAfterBootstrapSuccess"):
        state = states[name]
        for key in ("Next", "Default"):
            if state.get(key) == "SubstrateHealthGate":
                state[key] = "CheckShellRun"
    states["CheckSubstrateHealthGate"]["Choices"][0]["Next"] = "MorningEnrich"
    states["CheckSkipMorningEnrich"]["Default"] = "SubstrateHealthGate"
    found = _bypasses("step_function.json", states)
    assert any("'SubstrateHealthGate'" in f and "CheckSkipMorningEnrich" in f for f in found), found
