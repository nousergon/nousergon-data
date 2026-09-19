"""alpha-engine-config-I11106 — ModelZooSelect's declared-unservable path.

The 2026-09-19 weekly run terminated DEGRADED because ModelZooSelect reported
failure, but the stage did its job: it evaluated the M-slot arms and correctly
concluded nothing qualifies for promotion. That is a VERDICT
(sf-pipeline-policy.md §2.3a), not a stage failure, and the SF previously could
not tell the two apart — both rendered as a non-Success poll.

The MODEL_ZOO_SELECT_OUTCOME v1 contract (crucible-predictor's
spot-model-zoo-select workload) now prints exactly one stdout line and exits 0
on both a genuine decided outcome and a correctly-refused (unservable) one.
CheckModelZooStatus splits a Success poll on that marker: the declared-unservable
edge sets the branch-local $.model_zoo_unservable and continues to BranchBComplete
without ever touching MarkModelZooDegraded / $.research_degraded_local /
$.degraded_summary — a genuine failure/timeout is completely unchanged.

These are structural walks over the definition, not live executions.
KNOWN-FRAGILE, ACCEPTED: the underlying Choice string-matches
StandardOutputContent (the same idiom as ParityReplayResourceKill /
PitParityCompareResourceKill) rather than reading the durable
arena/model/{date}.json::decision.status artifact — alpha-engine-config-I11101
deliverable 4 tracks moving it onto that structured read.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"


@pytest.fixture
def doc():
    return json.loads((_INFRA / "step_function.json").read_text())


@pytest.fixture
def branch_b(doc):
    return doc["States"]["ResearchPredictorParallel"]["Branches"][1]["States"]


def _edges(state: dict) -> list[str]:
    out = []
    if "Next" in state:
        out.append(state["Next"])
    if "Default" in state:
        out.append(state["Default"])
    for rule in state.get("Choices", []) or []:
        if "Next" in rule:
            out.append(rule["Next"])
    for catch in state.get("Catch", []) or []:
        if "Next" in catch:
            out.append(catch["Next"])
    return out


def _first_path_to(states: dict, start: str, stop: str, max_steps: int = 200) -> list[str]:
    """DFS from `start` to the first reachable `stop`, returning the path taken.

    Fails loudly if `stop` is unreachable, or if the walk references a state
    name that does not exist in `states` — a silent typo in a `Next`/`Default`
    is exactly the class this guard exists to catch before AWS does.
    """
    seen: set[str] = set()
    stack = [[start]]
    while stack:
        path = stack.pop()
        node = path[-1]
        if node == stop:
            return path
        if node in seen or len(path) > max_steps:
            continue
        seen.add(node)
        state = states.get(node)
        assert state is not None, f"path {path} references undefined state {node!r}"
        for nxt in _edges(state):
            stack.append(path + [nxt])
    raise AssertionError(f"{stop!r} not reachable from {start!r} within {max_steps} steps")


def test_unservable_marker_reaches_branch_b_complete_without_degrading(branch_b):
    """An unservable marker on a Success poll must reach BranchBComplete
    without entering any Mark*Degraded state and without writing
    $.degraded_summary anywhere along the way."""
    path = _first_path_to(branch_b, "ModelZooUnservableDeclared", "BranchBComplete")

    degraded_states = [n for n in path if "Degraded" in n]
    assert not degraded_states, (
        "a declared-unservable run must never enter a Mark*Degraded state: "
        f"path was {path}"
    )
    for name in path:
        state = branch_b[name]
        assert state.get("ResultPath") != "$.degraded_summary", (
            f"{name} writes $.degraded_summary on the declared-unservable path: {path}"
        )


def test_unservable_declared_never_touches_research_degraded_local(branch_b):
    """The arena's own correct-refusal verdict must never fold into
    $.research_degraded_local — that would render a correct refusal as though
    ResearchPredictorParallel itself had broken (sf-pipeline-policy.md §2.3a)."""
    for name in ("ModelZooUnservableDeclared", "PublishModelZooUnservableNotice"):
        state = branch_b[name]
        assert state.get("ResultPath") != "$.research_degraded_local"
        params = state.get("Parameters") or {}
        assert "research_degraded_local" not in json.dumps(params)


def test_check_model_zoo_status_routes_unservable_before_plain_success(branch_b):
    """The unservable Choice rule must be checked BEFORE the bare
    Status==Success rule (Choice evaluates rules in order; first match wins),
    or the unservable marker would never be reached."""
    rules = branch_b["CheckModelZooStatus"]["Choices"]
    unservable_idx = next(
        i for i, r in enumerate(rules) if r.get("Next") == "ModelZooUnservableDeclared"
    )
    plain_success_idx = next(
        i
        for i, r in enumerate(rules)
        if r.get("Next") == "BranchBComplete"
        and r.get("Variable") == "$.model_zoo_poll.Status"
    )
    assert unservable_idx < plain_success_idx


def test_non_zero_exit_still_reaches_mark_model_zoo_degraded(branch_b):
    """A genuine ModelZooSelect failure/timeout — unchanged by this issue —
    must still reach MarkModelZooDegraded via the existing fail-open chain."""
    path = _first_path_to(branch_b, "ModelZooSelectLivenessGate", "MarkModelZooDegraded")
    assert path[-1] == "MarkModelZooDegraded"
    # And the existing convergence point is on that path.
    assert "PublishModelZooFailureImmediate" in path


def test_success_without_marker_takes_the_unchanged_normal_path(branch_b):
    """A Success poll whose StandardOutputContent does NOT carry the
    declared-unservable marker must take the same bare Status==Success edge
    that existed before alpha-engine-config-I11106 — unabsorbed by the new
    unservable rule."""
    rules = branch_b["CheckModelZooStatus"]["Choices"]
    plain_success = next(
        r
        for r in rules
        if r.get("Variable") == "$.model_zoo_poll.Status"
        and r.get("StringEquals") == "Success"
    )
    assert plain_success["Next"] == "BranchBComplete"
    assert "And" not in plain_success, (
        "the plain Success rule must remain a bare single-condition rule; "
        "any additional condition belongs on the unservable rule ahead of it, "
        "never grafted onto this one"
    )


def test_unservable_marker_match_is_anchored_on_the_full_literal(branch_b):
    """§ known-fragile-pattern guard: the StringMatches literal must be the
    FULL contract marker, never a bare '*unservable*' or similar loose match
    (alpha-engine-config-I11101 records a false match on this idiom escalating
    a recoverable failure into a hard weekly failure elsewhere in this SF)."""
    rules = branch_b["CheckModelZooStatus"]["Choices"]
    unservable_rule = next(r for r in rules if r.get("Next") == "ModelZooUnservableDeclared")
    conds = unservable_rule["And"]
    (string_match,) = [c for c in conds if "StringMatches" in c]
    assert string_match["StringMatches"] == "*MODEL_ZOO_SELECT_OUTCOME: unservable*"
    (is_present,) = [
        c for c in conds if c.get("Variable") == "$.model_zoo_poll.StandardOutputContent"
        and "IsPresent" in c
    ]
    assert is_present["IsPresent"] is True


def test_wait_for_model_zoo_captures_standard_output_content(branch_b):
    """The Choice cannot read StandardOutputContent unless WaitForModelZoo's
    ResultSelector actually carries it forward from the raw SSM response."""
    selector = branch_b["WaitForModelZoo"]["ResultSelector"]
    assert selector["StandardOutputContent.$"] == "$.StandardOutputContent"


def test_model_zoo_unservable_is_floored_and_threaded_to_completion_markers(doc):
    """model_zoo_unservable must be floored false in InitializeInput (both
    polarities, sf-pipeline-policy.md §2.3a rule 3) and embedded in BOTH
    completion markers so a reconciler can see it on every terminal, not only
    the declared-unservable one."""
    init_params = doc["States"]["InitializeInput"]["Parameters"]["merged.$"]
    assert '"model_zoo_unservable":false' in init_params

    for marker in ("WriteCompletionMarker", "WriteCompletionMarkerDegraded"):
        body = doc["States"][marker]["Parameters"]["Body.$"]
        assert '"model_zoo_unservable":{}' in body
        assert "States.JsonToString($.model_zoo_unservable)" in body


def test_set_model_zoo_unservable_is_the_sole_writer_of_the_top_level_flag(doc):
    """SetModelZooUnservable must be the only state writing $.model_zoo_unservable
    at top scope — every OTHER writer of a field with this name is branch-local
    (ResultPath == "$.model_zoo_unservable" inside ResearchPredictorParallel's
    branch B, which is a disjoint JSONPath scope from the top-level document)."""
    top_states = doc["States"]
    writers = [
        name
        for name, st in top_states.items()
        if isinstance(st, dict) and st.get("ResultPath") == "$.model_zoo_unservable"
    ]
    assert writers == ["SetModelZooUnservable"]
