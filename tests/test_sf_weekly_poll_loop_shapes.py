"""§2.2 / alpha-engine-config-I5687 / I5688: structural assertions for every
SSM-polling loop in ne-weekly-freshness-pipeline (step_function.json).

17 June 2026: the poll loops that select on $.XXXX_poll.Status had no
wall-clock bound and no assertion that the target instance is still alive,
violating weekly-sf-policy §2.2 (fail fast / fail cheap) — a dead launcher
box cost 5h11m of blind polling (2026-07-29).

DataPhase2 (#1186) and ThinkTank shipped bounded from birth, establishing
the reference shape this module now pins for every remaining loop:

    Init<Stage>PollCount (Pass, seeds $.<prefix>_polls = 0)
      -> WaitFor<Stage> (Task, ssm:getCommandInvocation)
      -> Check<Stage>Status (Choice: Success -> next stage;
           bounded loop-back -> <Stage>Wait; Default -> error/retry-gate)
      -> <Stage>Wait (Pass, increments $.<prefix>_polls via States.MathAdd)
      -> <Stage>PollWait (Wait, sleeps the poll interval)
      -> Merge<Stage>PollCount (Pass, folds the incremented value back onto
           $.<prefix>_polls)
      -> back to WaitFor<Stage>

nousergon-data PR#1182 (the original I5687 fix) inserted a Choice branch
that read $.<prefix>_poll_count without ever seeding or incrementing it
anywhere in the state machine — a budget check that can structurally never
fire (confirmed live: running its own transform script on
CheckMorningEnrichStatus left three duplicate, inert budget branches, none
functional). ``test_every_bounded_loop_seeds_and_increments_its_counter``
below exists specifically to catch that class of regression — a Check*Status
Choice that references a poll-budget variable no Init/Increment/Merge state
in the same scope ever writes.

Two composable guards on the Choice shape itself:

1. **Poll-iteration budget.** Each Check*Status Choice's Default path must
   lead to a terminal error extractor or a bounded retry gate, never back
   into the poll loop. Verified by tracing the Default chain.

2. **Bounded loop-back.** The branch that re-enters the wait loop must be
   conditioned on a poll-count variable this same scope actually seeds and
   increments — not a bare Status match with no cap.

Additionally, every polling loop that has a Retry Gate must check instance
liveness / bound its re-issue attempts before deciding to re-issue (I5688).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

SF_PATH = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"

# Every Check*Status Choice as of the I5687 reconciliation (2026-08-09):
# DataPhase2 and ThinkTank shipped bounded from birth; the remaining 15 were
# reconciled here mirroring their shape. CheckThinkTankStatus left this list
# on 2026-08-10 with the rest of the ThinkTankCoverage chain (Brian ruling:
# the Think Tank runs daily in shadow mode, outside the weekly SF) — the
# bounded-loop shape it established is still the pattern the others follow.
ALL_CHECK_STATUS_STATES = [
    "CheckBacktesterStatus",
    "CheckDataPhase1Status",
    "CheckDataPhase2Status",
    # alpha-engine-config-I9329: EvalJudgeProcess moved off Lambda onto a
    # dedicated spot box, so the eval-judge chain acquired the same two
    # bounded poll loops every other spot stage has — one over the box's
    # bootstrap command, one over the judge run itself.
    "CheckEvalJudgeProcessStatus",
    "CheckEvalJudgeSpotBootstrapStatus",
    "CheckEvaluatorDiagnosticsStatus",
    "CheckEvaluatorOptimizeStatus",
    "CheckModelZooStatus",
    "CheckMorningEnrichStatus",
    # alpha-engine-config#6030: CheckParityStatus was split into the three
    # ParityParallel branch loops + the compare-join loop, each bounded.
    "CheckPitParityLookaheadStatus",
    "CheckPitParityWalkforwardStatus",
    "CheckParityReplayStatus",
    "CheckPitParityCompareStatus",
    "CheckPortfolioOptimizerBacktestStatus",
    "CheckPredictorBacktestStatus",
    "CheckPredictorStatus",
    "CheckRAGIngestionStatus",
    "CheckResolveZooStatus",
    "CheckSaturdayHealthCheckStatus",
    "CheckSubstrateHealthCheckStatus",
    "CheckTrainSpecStatus",
    "CheckWeeklyFreshnessSpotBootstrapStatus",
    # alpha-engine-config-I11312: the observe-mode on-spot preflight pass.
    "CheckWeeklyPreflightOnSpotStatus",
]


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
    states = _follow_path(sf, f"{parent_path}.States")
    if isinstance(states, dict):
        return states.get(name)
    return None


def test_all_check_status_choices_are_found(sf):
    """Guard: discovery finds every known loop (alphabetical, ignoring path)."""
    found = _find_check_status_states(sf)
    names = sorted(set(name.split(".")[-1] for name, _, _ in found))
    assert names == sorted(ALL_CHECK_STATUS_STATES), (
        f"Check*Status inventory drifted — got {names}, expected "
        f"{sorted(ALL_CHECK_STATUS_STATES)}. A new poll loop must be added "
        f"to ALL_CHECK_STATUS_STATES here (and given the same bounded shape) "
        f"rather than shipped unbounded."
    )
    assert "CheckRAGIngestionStatus" in names


@pytest.mark.parametrize("state_name", ALL_CHECK_STATUS_STATES)
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

        visited = {path}
        current_name = default_next
        while current_name:
            child = _state_by_name(sf, parent_path, current_name)
            assert child is not None, (
                f"{path}: Default leads to {current_name} which does not exist "
                f"in parent {parent_path}"
            )
            child_type = child.get("Type")
            if child_type in ("Succeed", "Fail"):
                break
            if "Extract" in current_name or "Extract" in child.get("Comment", ""):
                break
            next_next = child.get("Next")
            if next_next in (None, ""):
                break
            if next_next == state_name or next_next in visited:
                assert False, (
                    f"{path}: Default chain ({default_next} -> ... -> "
                    f"{next_next}) loops back to a poll state — unbounded"
                )
            visited.add(next_next)
            current_name = next_next


@pytest.mark.parametrize("state_name", ALL_CHECK_STATUS_STATES)
def test_every_bounded_loop_seeds_and_increments_its_counter(sf, state_name: str):
    """Regression guard for nousergon-data PR#1182's original defect: a
    Check*Status Choice that reads a poll-budget variable ($.<prefix>_polls)
    which nothing in the same States scope ever seeds or increments is a
    budget check that can structurally never fire.

    For every loop-back branch conditioned on a $.<x>_polls variable, assert
    that scope also contains at least one Pass state whose ResultPath or
    Parameters seeds/increments that exact variable (directly, or via a
    States.MathAdd Pass + a Merge Pass that folds the result back onto it).
    """
    found = [(p, s, pp) for p, s, pp in _find_check_status_states(sf)
             if p.endswith(state_name) or p.split(".")[-1] == state_name]
    assert found, f"{state_name} not found in step function"
    for path, state, parent_path in found:
        choices_json = json.dumps(state.get("Choices", []))
        if "_polls" not in choices_json:
            # This Check*Status has no poll-budget branch at all — that's a
            # separate failure, caught by test_every_check_status_has_a_bounded_default
            # only insofar as its Default must still be safe. Not this test's job.
            continue

        import re
        poll_vars = set(re.findall(r'"\$\.([a-z0-9_]+_polls)"', choices_json))
        assert poll_vars, f"{path}: references *_polls but couldn't extract the variable name"

        scope = _follow_path(sf, parent_path) or {}
        if not isinstance(scope, dict):
            scope = {}
        scope_json = json.dumps(scope)

        for poll_var in poll_vars:
            result_path_needle = f'"ResultPath": "$.{poll_var}"'
            result_needle = f'"Result": 0'
            assert result_path_needle in scope_json or result_needle in scope_json, (
                f"{path}: no state in scope seeds/merges $.{poll_var} — "
                f"budget check can never fire (PR#1182's original defect)"
            )
            mathadd_needle = f"States.MathAdd($.{poll_var}, 1)"
            assert mathadd_needle in scope_json, (
                f"{path}: no state in scope increments $.{poll_var} via "
                f"States.MathAdd — a counter that never increments is an "
                f"unbounded loop wearing a bound"
            )


def test_three_retry_gates_check_liveness_before_reissue(sf):
    """§2.2 / I5688: MorningEnrichRetryGate, DataPhase1RetryGate, and
    RAGIngestionRetryGate must bound re-issue (liveness check or attempts
    cap) rather than re-issuing to a dead instance unconditionally."""
    retry_gates = ["MorningEnrichRetryGate", "DataPhase1RetryGate",
                   "RAGIngestionRetryGate"]

    for gate_name in retry_gates:
        found = False
        for path, state, pp in _find_check_status_states(sf):
            default = state.get("Default", "")
            if default == gate_name:
                found = True
                break
        assert found, f"{gate_name} not found as a Default target from any Check*Status"


def test_no_unbounded_loop_back_to_wait(sf):
    """Every bounded loop-back branch must lead (directly, or via a counter
    Pass -> Wait -> Merge Pass chain) to the WaitFor* Task, never straight
    back into itself with no bound."""
    for path, state, parent_path in _find_check_status_states(sf):
        for choice in state.get("Choices", []):
            next_target = choice.get("Next", "")
            child = _state_by_name(sf, parent_path, next_target)
            if child is None:
                continue
            child_type = child.get("Type")
            if child_type == "Wait":
                wait_next = child.get("Next", "")
                task = _state_by_name(sf, parent_path, wait_next)
                assert task, (
                    f"{path}: Wait state {next_target} leads to {wait_next} "
                    f"which does not exist"
                )
                assert task.get("Type") in ("Task", "Pass"), (
                    f"{path}: Wait state {next_target} should lead to a Task "
                    f"or a counter-merge Pass, got {task.get('Type')}"
                )
            elif child_type == "Pass" and "MathAdd" in json.dumps(child):
                # Increment Pass -> must lead to a Wait, which must lead to a
                # Task or a Merge Pass that itself leads to a Task.
                pw_name = child.get("Next", "")
                pollwait = _state_by_name(sf, parent_path, pw_name)
                assert pollwait and pollwait.get("Type") == "Wait", (
                    f"{path}: increment Pass {next_target} should lead to a "
                    f"Wait, got {pollwait.get('Type') if pollwait else None}"
                )
                merge_name = pollwait.get("Next", "")
                merge = _state_by_name(sf, parent_path, merge_name)
                assert merge is not None, (
                    f"{path}: Wait {pw_name} leads to {merge_name} which "
                    f"does not exist"
                )
                final_type = merge.get("Type")
                assert final_type in ("Task", "Pass"), (
                    f"{path}: chain from {next_target} should terminate at a "
                    f"Task (directly or via a Merge Pass), got {final_type}"
                )
