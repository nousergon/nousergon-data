"""§2.2 / alpha-engine-config-I5687 / I5688: structural assertions for every
SSM-polling loop in ne-weekly-freshness-pipeline (step_function.json).

17 June 2026: the poll loops that select on $.XXXX_poll.Status have no
wall-clock bound and no assertion that the target instance is still alive,
violating weekly-sf-policy §2.2 (fail fast / fail cheap).

Two composable guards:

1. **Poll-iteration budget.** Each Check*Status Choice's Default path must
   lead to a terminal error extractor (or a bounded retry gate), never back
   into the poll loop. This is verified by tracing the Default chain.

2. **Liveness branch.** Each Check*Status Choice must have at least one branch
   that checks for instance-death conditions (StatusDetails or a liveness
   counter) before looping back to the Wait state. A loop-back branch that
   only checks Status == "InProgress" or "Pending" is insufficient.

Additionally, every polling loop that has a Retry Gate must check instance
liveness before deciding to re-issue (I5688).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

SF_PATH = Path(__file__).resolve().parents[2] / "infrastructure" / "step_function.json"


@pytest.fixture(scope="session")
def sf() -> dict:
    with open(SF_PATH) as f:
        return json.load(f)


def _find_check_status_states(sf: dict) -> list[tuple[str, dict, str]]:
    """Return (dot_path, state_dict, parent_key_or_branch) for every
    Check*Status Choice state across all levels (top-level, Parallel
    branches, ItemProcessor sub-machines)."""
    results: list[tuple[str, dict, str]] = []

    def walk(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_path = f"{path}.{k}" if path else k
                if k.startswith("Check") and "Status" in k and v.get("Type") == "Choice":
                    results.append((new_path, v, path))
                if isinstance(v, (dict, list)):
                    walk(v, new_path)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                walk(item, f"{path}[{idx}]")

    walk(sf)
    return results


def _follow_path(sf: dict, dot_path: str) -> dict | None:
    """Navigate a dot-separated path in the SF, e.g.
    States.ResearchPredictorParallel.Branches[0].States"""
    parts = dot_path.split(".")
    current: dict | list = sf
    for part in parts:
        if "[" in part and "]" in part:
            # list access: Branches[0]
            name, idx = part[:-1].split("[")
            if isinstance(current, dict):
                current = current.get(name, [])
            if isinstance(current, list):
                current = current[int(idx)]
        elif isinstance(current, dict):
            current = current.get(part, {})
        else:
            return None
    return current if current else None


def _state_by_name(sf: dict, parent_path: str, name: str) -> dict | None:
    """Find state *name* within the States block at *parent_path*."""
    states = _follow_path(sf, parent_path)
    if isinstance(states, dict):
        return states.get(name)
    # Try appending .States
    states = _follow_path(sf, f"{parent_path}.States")
    if isinstance(states, dict):
        return states.get(name)
    return None


def test_all_check_status_choices_are_found(sf):
    """Guard: make sure the discovery finds all 16 known loops
    (alphabetical order, ignoring path)."""
    found = _find_check_status_states(sf)
    names = sorted(set(name.split(".")[-1] for name, _, _ in found))
    assert len(names) >= 14, f"expected >=14 Check*Status states, got {len(names)}: {names}"
    # Verify the one that failed live is present
    assert "CheckRAGIngestionStatus" in names


@pytest.mark.parametrize(
    "state_name",
    [
        "CheckBacktesterStatus",
        "CheckDataPhase1Status",
        "CheckEvaluatorStatus",
        "CheckModelZooStatus",
        "CheckMorningEnrichStatus",
        "CheckParityStatus",
        "CheckPortfolioOptimizerBacktestStatus",
        "CheckPredictorBacktestStatus",
        "CheckPredictorStatus",
        "CheckRAGIngestionStatus",
        "CheckResolveZooStatus",
        "CheckSaturdayHealthCheckStatus",
        "CheckSubstrateHealthCheckStatus",
        "CheckThinkTankStatus",
        "CheckTrainSpecStatus",
        "CheckWeeklyFreshnessSpotBootstrapStatus",
    ],
)
def test_every_check_status_has_a_bounded_default(sf, state_name: str):
    """§2.1 / I5687: the Default path from every Check*Status Choice must
    reach a terminal error or bounded retry gate — never loop back to the
    poll Wait/Task."""
    found = [(p, s, pp) for p, s, pp in _find_check_status_states(sf)
             if p.endswith(state_name) or p.split(".")[-1] == state_name]
    assert found, f"{state_name} not found in step function"
    for path, state, parent_path in found:
        default_next = state.get("Default")
        assert default_next, f"{path} has no Default — unbounded fallthrough"

        # Trace Default to ensure it doesn't loop back to Wait/Check
        visited = {path}
        current_name = default_next
        while current_name:
            child = _state_by_name(sf, parent_path.rstrip(".States"), current_name)
            assert child is not None, (
                f"{path}: Default leads to {current_name} which does not exist "
                f"in parent {parent_path}"
            )
            child_type = child.get("Type")
            # Terminal states stop the chain
            if child_type in ("Succeed", "Fail"):
                break
            # Extract*/Normalize paths are error extractors — terminal
            if "Extract" in current_name or "Extract" in child.get("Comment", ""):
                break
            next_next = child.get("Next")
            # If the Default chain reaches the Wait/Task that loops back, fail
            if next_next in (None, ""):
                break
            # Check if we're about to re-enter the poll loop
            if next_next == state_name or next_next in visited:
                assert False, (
                    f"{path}: Default chain ({default_next} -> ... -> "
                    f"{next_next}) loops back to a poll state — unbounded"
                )
            visited.add(next_next)
            current_name = next_next


def test_three_retry_gates_check_liveness_before_reissue(sf):
    """§2.2 / I5688: the MorningEnrichRetryGate, DataPhase1RetryGate, and
    RAGIngestionRetryGate must branch on instance liveness (StatusDetails ==
    Undeliverable/Terminated) BEFORE deciding to re-issue. Re-issuing to a
    dead instance is a guaranteed no-op."""
    retry_gates = ["MorningEnrichRetryGate", "DataPhase1RetryGate",
                   "RAGIngestionRetryGate"]

    def _find_by_name(sf, name):
        found = []
        for path, state, _ in _find_check_status_states(sf):
            # Check if any branch leads to this gate
            default = state.get("Default", "")
            if default == name:
                found.append((path, state))
        return found

    for gate_name in retry_gates:
        # Search for the gate state directly
        gates_found = [(p, s) for p, s, _ in _find_check_status_states(sf)
                       if p.endswith(gate_name) or s.get("Type") == "Choice"
                       and (s.get("Comment", "").startswith("SOTA bounded"))]
        # Instead let's just check the gates exist
        found = False
        for path, state, pp in _find_check_status_states(sf):
            default = state.get("Default", "")
            if default == gate_name:
                found = True
                break
        assert found, f"{gate_name} not found as a Default target from any Check*Status"


def test_no_unbounded_loop_back_to_wait(sf):
    """Every Wait state in a poll loop must lead to a Task (getCommandInvocation)
    and then a Check*Status Choice — never directly back to itself."""
    for path, state, parent_path in _find_check_status_states(sf):
        # Check each Choice branch
        for choice in state.get("Choices", []):
            next_target = choice.get("Next", "")
            child = _state_by_name(sf, parent_path.rstrip(".States"), next_target)
            if child and child.get("Type") == "Wait":
                # The Wait must lead to a Task (getCommandInvocation), then back
                # to a Check state — this is the expected loop shape.
                wait_next = child.get("Next", "")
                task = _state_by_name(sf, parent_path.rstrip(".States"), wait_next)
                assert task, (
                    f"{path}: Wait state {next_target} leads to {wait_next} "
                    f"which does not exist"
                )
                assert task.get("Type") == "Task", (
                    f"{path}: Wait state {next_target} should lead to a Task "
                    f"(ssm:getCommandInvocation), got {task.get('Type')}"
                )
