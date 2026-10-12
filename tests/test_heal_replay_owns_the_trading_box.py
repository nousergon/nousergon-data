"""After a heal replay is dispatched, the replay owns the trading box.

2026-10-08 heal-replay stop race. The reconcile backstop
``eod-reconcile-backstop-2026-10-08-1791512138`` converged its heal loop and
``HealDispatchReplay`` started ``eod-heal-replay-2026-10-08-eod-reconcile-
backstop-2026-10-08-1791512138`` at 03:19:13Z (fire-and-forget). The parent
then drained and ran its own ``StopTradingInstance`` at 03:19:30Z. The
replay's ``StartTradingInstance`` had been a no-op on the running box at
03:19:17Z (running -> running), so the stop landed under it: its
``RefreshExecutorDeploy`` went to a stopping box at 03:19:32Z, ended
``DeliveryTimedOut``, and the replay FAILED at 03:39:42Z. No eod_pnl row for
2026-10-08.

The fix is structural: ``HealConvergedNotify`` (reached only on a successful
dispatch) routes around ``DrainTraderReconcile`` and ``StopTradingInstance``.
These tests pin the three halves of that:

(a) dispatch -> the dispatching execution never stops the box;
(b) no dispatch -> every route to a terminal after the box starts still stops it;
(c) the replay itself can never take the hand-off route, so it always stops
    the box at its own end.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SF = json.loads(
    (REPO / "infrastructure" / "step_function_eod_reconcile.json").read_text(encoding="utf-8")
)
STATES = SF["States"]

STOPS = {"StopTradingInstance", "ForceStopInstance"}
HAND_OFF = "HealConvergedNotify"
TERMINAL_TYPES = {"Succeed", "Fail"}


def _targets(state: dict) -> list[str]:
    out = [state[k] for k in ("Next", "Default") if k in state]
    out += [c["Next"] for c in state.get("Choices", [])]
    out += [c["Next"] for c in state.get("Catch", [])]
    return out


#: 2026-10-09 box ownership: these Choices skip the stop when the box was
#: already running at this execution's start. Their owned edge is the only
#: one ``_owned_targets`` follows, i.e. the execution that started the box.
OWNERSHIP_GUARDS = {"CheckBoxOwnedBeforeStop", "CheckBoxOwnedBeforeForceStop"}


def _owned_targets(name: str) -> list[str]:
    st = STATES[name]
    if name in OWNERSHIP_GUARDS:
        return [c["Next"] for c in st["Choices"]]
    return _targets(st)


def _reachable(start: str, blocked: set[str] = frozenset(), owned: bool = False) -> set[str]:
    seen: set[str] = set()
    todo = [start]
    while todo:
        name = todo.pop()
        if name in seen or name in blocked:
            continue
        seen.add(name)
        todo.extend(_owned_targets(name) if owned else _targets(STATES[name]))
    return seen


def _feeders(name: str) -> set[str]:
    return {n for n, st in STATES.items() if name in _targets(st)}


def _terminals(names: set[str]) -> set[str]:
    return {n for n in names if STATES[n]["Type"] in TERMINAL_TYPES}


# ── (a) dispatch -> no stop by the dispatcher ───────────────────────────────


def test_the_hand_off_state_is_reached_only_by_a_successful_dispatch():
    """The skip must not be reachable without a replay that owns the box."""
    assert _feeders(HAND_OFF) == {"HealDispatchReplay"}
    dispatch = STATES["HealDispatchReplay"]
    assert dispatch["Next"] == HAND_OFF
    assert all(c["Next"] != HAND_OFF for c in dispatch["Catch"])
    assert dispatch["Resource"] == "arn:aws:states:::states:startExecution"
    assert dispatch["Parameters"]["StateMachineArn.$"] == "$$.StateMachine.Id"


def test_after_a_dispatch_the_dispatcher_never_drains_or_stops_the_box():
    st = STATES[HAND_OFF]
    assert st["Next"] == "ReadExerciseCadence"
    assert [c["Next"] for c in st["Catch"]] == ["ReadExerciseCadence"]
    after = _reachable(HAND_OFF)
    assert not after & (STOPS | {"DrainTraderReconcile", "HandleFailure"}), sorted(after)
    # And it still reaches a terminal: the hand-off is not a dead end.
    assert _terminals(after), sorted(after)


# ── (b) no dispatch -> the box is still stopped ─────────────────────────────


def test_no_dispatch_routes_still_drain_and_stop():
    for name in ("HealReplayDispatchFailed", "HealNonConvergent"):
        st = STATES[name]
        assert st["Next"] == "CheckBoxOwnedBeforeStop", name
        assert [c["Next"] for c in st["Catch"]] == ["CheckBoxOwnedBeforeStop"], name
    assert "StopTradingInstance" in _reachable("CheckBoxOwnedBeforeStop", owned=True)


def test_every_route_from_the_box_start_to_a_terminal_stops_it_unless_handed_off():
    """For an execution that started the box (owned edges only), with the stops
    and the hand-off removed, nothing after StartTradingInstance can reach a
    terminal. A new route that ends a run with the box up fails here."""
    leaked = _terminals(_reachable("StartTradingInstance", blocked=STOPS | {HAND_OFF}, owned=True))
    assert leaked == set(), sorted(leaked)


# ── (c) the replay stops the box at its own end ─────────────────────────────


def test_a_replay_can_never_take_the_hand_off_route():
    """The replay runs this same machine with pipeline_role=operator-replay.
    CheckHealLoopEligible sends that role to HealNonConvergent (drain + stop)
    BEFORE InitHealLoop, and InitHealLoop is the only way into the heal loop
    that contains HealDispatchReplay. So the replay never dispatches, never
    reaches the hand-off, and by the test above always ends through a stop."""
    replay_input = STATES["HealDispatchReplay"]["Parameters"]["Input"]
    assert replay_input["pipeline_role"] == "operator-replay"

    eligible = STATES["CheckHealLoopEligible"]
    first = eligible["Choices"][0]
    assert first["Next"] == "HealNonConvergent"
    role_rules = [r for r in first["And"] if r.get("StringEquals")]
    assert role_rules == [{"Variable": "$.pipeline_role", "StringEquals": "operator-replay"}]
    assert eligible["Default"] == "InitHealLoop"

    assert _feeders("InitHealLoop") == {"CheckHealLoopEligible"}
    assert "HealDispatchReplay" in _reachable("InitHealLoop")
    # Without InitHealLoop, nothing in the machine reaches the dispatch.
    without_loop = _reachable(SF["StartAt"], blocked={"InitHealLoop"})
    assert not {"HealDispatchReplay", HAND_OFF} & without_loop
