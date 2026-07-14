"""config#2279 — every weekly-SF SSM state's Retry ladder must match its
DECLARED class. Completes the normalization config#2250 (data-PR752) started
on the Lambda states.

Pre-fix, SF-level retry ladders were inversely proportional to stage cost:
the data spots carried the gold 4+2 jittered ladder while the most expensive
stages (the 7200s backtester family, predictor training, the model-zoo
states) carried 1-2 thin tries — an SSM API transient on a 2h stage was
terminal on the second occurrence.

Idempotency, verified per the issue before widening: a spot-stage
``sendCommand`` state's Retry fires ONLY when the SendCommand API call
itself failed (throttle / 5xx / dead dispatch box) — BEFORE the launcher
command ever ran, so no spot was launched and no partial S3 writes exist.
Post-delivery outcomes are owned by the ``WaitFor*`` poll loop and the
``Check*Status`` gates, which a send-state Retry never re-enters. Re-issue
is therefore idempotent for EVERY stage, including training/backtest — no
stage needs the thin-ladder carve-out the issue contemplated.

Classes (a NEW SSM state must be added to exactly one list below, or this
test fails — retry-naked states can't ship):
  * spot-stage sendCommand  → the gold 4+2 jittered ladder;
  * health-observe sendCommand → NO Retry (best-effort observability with a
    fail-soft Catch; a ladder would only delay completion, never protect
    spend — the rationale is inline in each state's Comment);
  * getCommandInvocation poll → an Ssm.InvocationDoesNotExist* ladder
    (SendCommand→GetCommandInvocation eventual consistency).
"""
from __future__ import annotations

import json
import pathlib

import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

SPOT_STAGE_SEND_STATES = {
    "MorningEnrich",
    "DataPhase1",
    "RAGIngestion",
    "PredictorTraining",
    "ResolveZooSpecs",
    "TrainSpecDispatch",
    "ModelZooSelect",
    "Backtester",
    "PredictorBacktest",
    "PortfolioOptimizerBacktest",
    "Parity",
    "Evaluator",
}
HEALTH_OBSERVE_SEND_STATES = {
    "SaturdayHealthCheck",
    "WeeklySubstrateHealthCheck",
}

GOLD_LADDER = [
    {
        "ErrorEquals": ["States.TaskFailed", "States.Timeout"],
        "MaxAttempts": 4,
        "IntervalSeconds": 30,
        "BackoffRate": 2.0,
        "MaxDelaySeconds": 300,
        "JitterStrategy": "FULL",
    },
    {
        "ErrorEquals": ["States.ALL"],
        "MaxAttempts": 2,
        "IntervalSeconds": 30,
        "BackoffRate": 2.0,
        "MaxDelaySeconds": 300,
        "JitterStrategy": "FULL",
    },
]


def _iter_states():
    definition = json.loads(_WEEKLY.read_text())

    def _walk(states):
        for name, state in states.items():
            yield name, state
            if state.get("Type") == "Parallel":
                for branch in state.get("Branches", []):
                    yield from _walk(branch["States"])
            if state.get("Type") == "Map":
                iterator = state.get("Iterator") or state.get("ItemProcessor")
                if iterator:
                    yield from _walk(iterator["States"])

    yield from _walk(definition["States"])


def _ssm_states(suffix: str) -> dict[str, dict]:
    return {
        name: state
        for name, state in _iter_states()
        if state.get("Resource", "").endswith(f"aws-sdk:ssm:{suffix}")
    }


def _normalized_ladder(state: dict) -> list[dict]:
    """Retry rules with Comments stripped (rationale prose is free to evolve;
    the retry SEMANTICS are what's pinned)."""
    return [
        {k: v for k, v in rule.items() if k != "Comment"}
        for rule in state.get("Retry", [])
    ]


def test_every_send_command_state_is_classified():
    send_states = set(_ssm_states("sendCommand"))
    declared = SPOT_STAGE_SEND_STATES | HEALTH_OBSERVE_SEND_STATES
    unclassified = send_states - declared
    assert not unclassified, (
        f"config#2279: new sendCommand state(s) {sorted(unclassified)} must be "
        "added to a declared retry-ladder class in this test (spot-stage → "
        "gold 4+2 ladder; health-observe → no Retry + fail-soft Catch)"
    )
    missing = declared - send_states
    assert not missing, f"declared states no longer exist: {sorted(missing)}"


@pytest.mark.parametrize("name", sorted(SPOT_STAGE_SEND_STATES))
def test_spot_stage_carries_gold_ladder(name):
    states = _ssm_states("sendCommand")
    assert _normalized_ladder(states[name]) == GOLD_LADDER, (
        f"config#2279: {name} drifted from the declared spot-stage 4+2 "
        "jittered ladder"
    )


@pytest.mark.parametrize("name", sorted(HEALTH_OBSERVE_SEND_STATES))
def test_health_observe_has_no_retry_but_fail_soft_catch(name):
    states = _ssm_states("sendCommand")
    state = states[name]
    assert "Retry" not in state, (
        f"{name} is declared health-observe (no Retry — see its inline "
        "config#2279 rationale); reclassify it if that changed"
    )
    catches = state.get("Catch", [])
    assert catches and catches[0]["ErrorEquals"] == ["States.ALL"], (
        f"{name} must fail soft (States.ALL Catch) — without a ladder, the "
        "Catch is what keeps a health-check failure from failing a green run"
    )
    # The no-Retry deviation must carry its written rationale in-definition.
    assert "config#2279" in state.get("Comment", "")


def test_every_poll_state_has_invocation_does_not_exist_ladder():
    """SendCommand→GetCommandInvocation eventual consistency: every poll
    state must retry the not-yet-visible-invocation error class."""
    offenders = []
    for name, state in _ssm_states("getCommandInvocation").items():
        rules = state.get("Retry", [])
        covered = any(
            any(e.startswith("Ssm.InvocationDoesNotExist") for e in rule["ErrorEquals"])
            for rule in rules
        )
        if not covered:
            offenders.append(name)
    assert not offenders, (
        f"poll state(s) without the InvocationDoesNotExist ladder: {offenders}"
    )


def test_convention_is_declared_in_definition_comment():
    definition = json.loads(_WEEKLY.read_text())
    assert "config#2279" in definition["Comment"], (
        "the retry convention declaration was dropped from the definition's "
        "top-level Comment"
    )
