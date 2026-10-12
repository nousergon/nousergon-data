"""The run-slot release hop in front of the weekly SF's Fail terminals.

alpha-engine-config-I12228 put a two-state hop in front of each Fail terminal
of ``step_function.json`` (``FailExecution``, ``DegradedRun``, ``VacuousRun``):
``CheckRunSlotHeldBefore<T>`` (Choice: did THIS execution acquire the run-slot?)
-> ``ReleaseRunSlotBefore<T>`` (best-effort conditional DeleteItem) -> ``<T>``.
Every edge that used to name the terminal now names the gate.

Many tests pin "this edge reaches that terminal". They assert the same property
through ``through_release``, which resolves a gate to its terminal AND asserts
the hop cannot change the outcome: both of the gate's arms, and both of the
release task's exits (Next and every Catch), land on the same terminal. Same
reasoning as ``sf_degraded_summary_helpers``: one place for the shape, so the
next topology change has one place to update.
"""
from __future__ import annotations

RELEASING_TERMINALS = ("FailExecution", "DegradedRun", "VacuousRun")
GATE_PREFIX = "CheckRunSlotHeldBefore"
TASK_PREFIX = "ReleaseRunSlotBefore"


def release_gate(terminal: str) -> str:
    return f"{GATE_PREFIX}{terminal}"


def release_task(terminal: str) -> str:
    return f"{TASK_PREFIX}{terminal}"


def assert_release_hop(states: dict, terminal: str) -> None:
    """The gate + release task in front of ``terminal`` can only ever reach it."""
    gate = states[release_gate(terminal)]
    task_name = release_task(terminal)
    assert gate["Type"] == "Choice"
    assert gate["Default"] == terminal, (
        f"{release_gate(terminal)}: an execution that never acquired the run-slot "
        f"must go straight to {terminal}"
    )
    (rule,) = gate["Choices"]
    assert rule == {"Variable": "$.mutex_result", "IsPresent": True, "Next": task_name}, (
        "the release must be gated on $.mutex_result, which only a successful "
        "AcquireMutex / TakeOverRunSlot writes"
    )

    task = states[task_name]
    assert task["Type"] == "Task"
    assert task["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:deleteItem"
    assert task["Next"] == terminal
    catches = task.get("Catch") or []
    assert any(c["ErrorEquals"] == ["States.ALL"] for c in catches), (
        f"{task_name} is best-effort: a release failure must never mask {terminal}"
    )
    assert all(c["Next"] == terminal for c in catches)
    # The terminal reads $ (CausePath); the hop must hand it on untouched.
    assert task["ResultPath"] not in ("$", None)
    assert all(c.get("ResultPath") not in ("$", None) for c in catches)
    assert "OutputPath" not in task and "InputPath" not in task


def through_release(states: dict, target: str) -> str:
    """``target``, or the terminal it guards when it is a release gate."""
    if not target.startswith(GATE_PREFIX):
        return target
    terminal = target[len(GATE_PREFIX):]
    assert terminal in RELEASING_TERMINALS, target
    assert_release_hop(states, terminal)
    return terminal
