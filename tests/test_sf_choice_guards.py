"""config#2275 — weekly-SF Choice states must never States.Runtime on an
absent payload field.

ASL Choice states cannot carry Catch. Any Choice rule that dereferences a
Variable path which is ABSENT at evaluation time throws `States.Runtime`,
ending the execution FAILED **without routing through NormalizeFailureContext
/ HandleFailure** — no structured failure email, no `$.error` context (the
2026-07-03 incident class). The fix is structural, and this test makes it
durable: EVERY leaf comparison in EVERY Choice state of the weekly definition
must be either

  1. an `IsPresent` check itself (including inside a `Not`) — a guard is
     always safe to evaluate;
  2. short-circuit-guarded: inside an `And` whose EARLIER operand is exactly
     `{Variable: <same path>, IsPresent: true}` (ASL evaluates And operands
     in order and stops at the first false — the canonical guard idiom), at
     any ancestor And level;
  3. provably floored upstream:
     a. a single-segment path (`$.key`) whose key `InitializeInput` floors
        for every execution (its JsonMerge defaults literal + the injected
        `run_date`) — valid at the top scope and inside Parallel branches
        (branch input is the parallel's effective input; ResultPath writes
        merge, never drop keys), NOT inside Map iterators (per-item input);
     b. a two-plus-segment path (`$.x.y...`) where EVERY in-scope
        predecessor of the Choice writes `ResultPath == $.x` with `y` pinned
        as a key of its ResultSelector / Parameters / Result — an absent
        source field then fails inside the TASK (whose Catch routes to the
        normalizer chain), never inside the Choice.

Comparison-path operators (`*Path`) dereference their VALUE too — held to the
same rule. Every Choice must also carry a Default (a rule-miss with no
Default is States.Runtime as well).

The drill half executes the actual Choice logic (a faithful mini evaluator
with ASL short-circuit semantics) against partial payloads — e.g. the
research Lambda returning `{}` — asserting the malformed input routes to the
explicit degraded/error route (ultimately HandleFailure via the Extract*Error
normalizers), never a States.Runtime throw.
"""
from __future__ import annotations

import fnmatch
import json
import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).parent.parent
_WEEKLY = _REPO_ROOT / "infrastructure" / "step_function.json"
# alpha-engine-config-I2544/I2545: the two child SFs split out of the
# weekly SF carry their own Choice states (the eval-judge chain's
# CheckSkipEvalJudge/EvalJudgePollChoice/etc. and the modelzoo fan-out's
# CheckResolveZooStatus/CheckModelZooStatus/etc.) and are in scope for the
# SAME config#2275 guard-or-floor discipline.
_ADVISORY = _REPO_ROOT / "infrastructure" / "step_function_advisory.json"
_MODELZOO = _REPO_ROOT / "infrastructure" / "step_function_modelzoo.json"


def _load(path: pathlib.Path = _WEEKLY) -> dict:
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# scope walking
# ---------------------------------------------------------------------------

def _iter_scopes(definition: dict):
    """Yield (scope_path, states_dict, in_map) for the top scope, every
    Parallel branch, and every Map iterator (in_map=True: per-item input, the
    InitializeInput floors do NOT reach it)."""
    def _walk(states: dict, path: str, in_map: bool):
        yield path, states, in_map
        for name, state in states.items():
            if state.get("Type") == "Parallel":
                for i, branch in enumerate(state.get("Branches", [])):
                    yield from _walk(branch["States"], f"{path}/{name}[{i}]", in_map)
            if state.get("Type") == "Map":
                iterator = state.get("Iterator") or state.get("ItemProcessor")
                if iterator:
                    yield from _walk(iterator["States"], f"{path}/{name}[map]", True)
    yield from _walk(definition["States"], "", False)


def _initialize_input_floors(definition: dict) -> set[str]:
    """Keys the SF's initializer Pass state (its StartAt — InitializeInput
    on the weekly SF, InitializeAdvisoryInput / InitializeModelZooInput on
    the two I2544/I2545 child SFs) guarantees on $ for every execution: the
    FIRST embedded JsonMerge defaults literal, plus any States.Format-
    injected run_date. Parsed MECHANICALLY from the state so the floor set
    can never drift from the definition."""
    init_state_name = definition["StartAt"]
    params = definition["States"][init_state_name]["Parameters"]["merged.$"]
    start = params.index("States.StringToJson('") + len("States.StringToJson('")
    end = params.index("')", start)
    literal = params[start:end].replace('\\"', '"')
    floors = set(json.loads(literal))
    if "run_date" in params:
        floors.add("run_date")
    assert "sns_topic_arn" in floors, f"{init_state_name} defaults parse failed"
    return floors


def _predecessors(states: dict, target: str) -> list[dict]:
    """Every state in this scope with an edge into `target` (Next, Default,
    Choice-rule Next, Catch Next)."""
    preds = []
    for state in states.values():
        edges = {state.get("Next"), state.get("Default")}
        for rule in state.get("Choices", []) or []:
            edges.add(rule.get("Next"))
        for catch in state.get("Catch", []) or []:
            edges.add(catch.get("Next"))
        if target in edges:
            preds.append(state)
    return preds


def _pinned_keys(state: dict) -> set[str]:
    """Keys a state's output object is guaranteed to carry at ResultPath."""
    source = state.get("ResultSelector") or (
        state.get("Parameters") if state.get("Type") == "Pass" else None
    ) or (state.get("Result") if state.get("Type") == "Pass" else None)
    if not isinstance(source, dict):
        return set()
    return {k[:-2] if k.endswith(".$") else k for k in source}


def _is_floored(var: str, choice_name: str, states: dict, in_map: bool,
                top_floors: set[str]) -> bool:
    segments = var.lstrip("$.").split(".")
    if len(segments) == 1:
        return (not in_map) and segments[0] in top_floors
    root, first_child = segments[0], segments[1]
    preds = _predecessors(states, choice_name)
    if not preds:
        return False
    for pred in preds:
        if pred.get("ResultPath") != f"$.{root}":
            return False
        if first_child not in _pinned_keys(pred):
            return False
    return True


# ---------------------------------------------------------------------------
# rule AST walking
# ---------------------------------------------------------------------------

def _leaf_violations(rule: dict, guards: frozenset[str], context: str,
                     floored) -> list[str]:
    """Return violation strings for every unguarded/unfloored dereference in
    this rule subtree. `guards` = variable paths already IsPresent-guarded by
    an earlier operand of an ancestor And."""
    violations: list[str] = []
    if "And" in rule:
        acquired = set(guards)
        for operand in rule["And"]:
            violations.extend(
                _leaf_violations(operand, frozenset(acquired), context, floored)
            )
            if operand.get("IsPresent") is True and "Variable" in operand:
                acquired.add(operand["Variable"])
        return violations
    if "Or" in rule:
        for operand in rule["Or"]:
            violations.extend(_leaf_violations(operand, guards, context, floored))
        return violations
    if "Not" in rule:
        return _leaf_violations(rule["Not"], guards, context, floored)

    var = rule.get("Variable")
    if var is None:
        return [f"{context}: rule with no Variable/And/Or/Not: {rule}"]
    operators = {
        k: v for k, v in rule.items() if k not in ("Variable", "Next", "Comment")
    }
    if "IsPresent" in operators:
        return []  # a guard is always safe to evaluate
    if var not in guards and not floored(var):
        violations.append(
            f"{context}: Variable {var!r} dereferenced by {sorted(operators)} "
            "without an earlier IsPresent guard in its And, and not provably "
            "floored upstream"
        )
    for op, value in operators.items():
        if op.endswith("Path"):
            if value not in guards and not floored(value):
                violations.append(
                    f"{context}: comparison path {value!r} ({op}) is not "
                    "IsPresent-guarded or provably floored"
                )
    return violations


# alpha-engine-config-I2544/I2545 (2026-07-14): the weekly SF's Choice count
# dropped from 51 -> 40 when the eval-judge chain (8 Choices:
# CheckSkipEvalJudge/CheckMonthlyCadence/EvalJudgePollChoice/
# EvalJudgePollDecision/CheckSkipRationaleClustering/
# CheckSkipReplayConcordance/CheckSkipCounterfactual/CheckSkipAggregateCosts)
# and the modelzoo fan-out (3 Choices: CheckResolveZooStatus/
# CheckModelZooStatus/CheckTrainSpecStatus — the last inside
# ModelZooTrainMap's Map iterator) moved to the two new child SFs, which
# carry the SAME config#2275 guard-or-floor discipline (see
# _MIN_CHOICES_SEEN below and the two new per-file tests).
_MIN_CHOICES_SEEN = {_WEEKLY: 35, _ADVISORY: 8, _MODELZOO: 3}


@pytest.mark.parametrize("sf_path", [_WEEKLY, _ADVISORY, _MODELZOO], ids=lambda p: p.stem)
def test_every_choice_variable_is_guarded_or_floored(sf_path):
    definition = _load(sf_path)
    top_floors = _initialize_input_floors(definition)
    violations: list[str] = []
    choices_seen = 0
    for scope_path, states, in_map in _iter_scopes(definition):
        for name, state in states.items():
            if state.get("Type") != "Choice":
                continue
            choices_seen += 1
            context = f"{scope_path}/{name}"
            if "Default" not in state:
                violations.append(
                    f"{context}: Choice has NO Default — a rule-miss throws "
                    "States.Runtime"
                )

            def floored(var, _name=name, _states=states, _in_map=in_map):
                return _is_floored(var, _name, _states, _in_map, top_floors)

            for rule in state.get("Choices", []):
                violations.extend(
                    _leaf_violations(rule, frozenset(), context, floored)
                )
    min_expected = _MIN_CHOICES_SEEN[sf_path]
    assert choices_seen >= min_expected, (
        f"walker regressed on {sf_path.name}: only {choices_seen} Choice "
        f"states found (expected >= {min_expected})"
    )
    assert not violations, (
        f"config#2275 regression on {sf_path.name} — Choice dereferences "
        "that can States.Runtime on an absent field (guard with "
        "And:[{IsPresent}, ...] or floor upstream):\n" + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# drill: partial payloads route to the explicit degraded/error path
# ---------------------------------------------------------------------------

class StatesRuntime(Exception):
    """Stand-in for ASL States.Runtime (absent path dereferenced)."""


_ABSENT = object()


def _resolve(path: str, data):
    node = data
    for segment in path.lstrip("$.").split("."):
        if not isinstance(node, dict) or segment not in node:
            return _ABSENT
        node = node[segment]
    return node


def _eval_rule(rule: dict, data) -> bool:
    """Faithful mini evaluator with ASL short-circuit semantics (And stops at
    the first false operand — what makes the guard idiom work)."""
    if "And" in rule:
        return all(_eval_rule(op, data) for op in rule["And"])
    if "Or" in rule:
        return any(_eval_rule(op, data) for op in rule["Or"])
    if "Not" in rule:
        return not _eval_rule(rule["Not"], data)
    var = _resolve(rule["Variable"], data)
    ops = {k: v for k, v in rule.items() if k not in ("Variable", "Next", "Comment")}
    assert len(ops) == 1, f"unexpected multi-operator rule: {rule}"
    op, expected = next(iter(ops.items()))
    if op == "IsPresent":
        return (var is not _ABSENT) == expected
    if var is _ABSENT:
        raise StatesRuntime(rule["Variable"])
    if op.endswith("Path"):
        resolved = _resolve(expected, data)
        if resolved is _ABSENT:
            raise StatesRuntime(expected)
        expected = resolved
        op = op[: -len("Path")]
    if op in ("StringEquals", "BooleanEquals", "NumericEquals"):
        return var == expected
    if op == "StringLessThan":
        return var < expected
    if op == "StringGreaterThanEquals":
        return var >= expected
    if op == "NumericGreaterThanEquals":
        return var >= expected
    if op == "StringMatches":
        return fnmatch.fnmatchcase(str(var), expected)
    raise AssertionError(f"evaluator does not implement {op} — extend the drill")


def _find_state(definition: dict, name: str) -> dict:
    for _, states, _ in _iter_scopes(definition):
        if name in states:
            return states[name]
    raise AssertionError(f"state {name!r} not found")


def _choice_target(definition: dict, choice_name: str, data) -> str:
    state = _find_state(definition, choice_name)
    assert state["Type"] == "Choice"
    for rule in state["Choices"]:
        if _eval_rule(rule, data):
            return rule["Next"]
    return state["Default"]


@pytest.mark.parametrize(
    ("choice", "partial_input", "expected_route"),
    [
        # NOTE: the pre-existing "research Lambda returns {}" drill against
        # CheckResearchStatus was retired here — alpha-engine-config-I2515
        # Phase B removed the multi-agent Research state (and
        # CheckResearchStatus) entirely. SignalsEnvelope, its load-bearing
        # replacement, detects failure via a plain Task Catch (which always
        # populates $.error deterministically) rather than a payload-status
        # Choice, so this specific {} ambiguity class no longer applies to
        # Branch A's producer.
        ("WeeklyRunDayGateChoice", {"weekly_run_day_gate": {"Payload": {}}},
         "WeeklyRunDayGateMalformed"),
        ("LibPinDriftGate", {"libpin_drift_result": {"Payload": {}}},
         "LibPinGateDegraded"),  # fail-open, VISIBLY (config#2278)
        ("PipelineContractGate", {"pipeline_contract_result": {"Payload": {}}},
         "PipelineContractGateDegraded"),  # fail-open, VISIBLY (config#2278)
        # NOTE: the EvalJudgePollChoice/EvalJudgePollDecision drill cases
        # moved to test_partial_payload_routes_to_explicit_path_advisory
        # below — alpha-engine-config-I2544 lifted the eval-judge chain into
        # step_function_advisory.json.
        # Healthy-path sanity: the guards must not change live semantics.
        ("WeeklyRunDayGateChoice",
         {"weekly_run_day_gate": {"Payload": {"is_weekly_run_day": False}}},
         "WeeklyRunDaySkip"),
        ("LibPinDriftGate",
         {"libpin_drift_result": {"Payload": {"has_drift": True}}},
         "ExtractLibPinDriftError"),
    ],
)
def test_partial_payload_routes_to_explicit_path(choice, partial_input, expected_route):
    definition = _load()
    assert _choice_target(definition, choice, partial_input) == expected_route


@pytest.mark.parametrize(
    ("choice", "partial_input", "expected_route"),
    [
        ("EvalJudgePollChoice", {"eval_judge_submit": {"Payload": {}}},
         "EvalRollingMean"),  # eval is observability — fail-soft
        ("EvalJudgePollDecision", {"eval_judge_poll": {"Payload": {}}},
         "EvalRollingMean"),  # malformed poll payload — fail-soft, no Wait loop
        # Healthy-path sanity: the guards must not change live semantics.
        ("EvalJudgePollDecision",
         {"eval_judge_poll": {"Payload": {"processing_status": "polling"}}},
         "EvalJudgePollWait"),
    ],
)
def test_partial_payload_routes_to_explicit_path_advisory(choice, partial_input, expected_route):
    """alpha-engine-config-I2544: same drill, re-pointed at the advisory
    child SF the eval-judge chain now lives in."""
    definition = _load(_ADVISORY)
    assert _choice_target(definition, choice, partial_input) == expected_route


def test_branch_a_failed_route_reaches_handle_failure():
    """A FAILED Branch A (e.g. via ExtractSignalsEnvelopeError, config-
    I2515 Phase B's renamed successor to the retired ExtractResearchError)
    marks branch A failed; CheckBranchOutcomes routes a failed branch into
    ExtractParallelBranchError, whose Next-chain lands on HandleFailure via
    the NormalizeFailureContext chokepoint (config#1819)."""
    definition = _load()
    assert _choice_target(
        definition, "CheckBranchOutcomes",
        {"branch_outcomes": {"branch_a_status": "FAILED", "branch_b_status": "OK"}},
    ) == "ExtractParallelBranchError"
    # Follow the top-scope Next chain (Pass/Task states) to HandleFailure.
    current = "ExtractParallelBranchError"
    seen = []
    for _ in range(10):
        seen.append(current)
        if current == "HandleFailure":
            break
        state = definition["States"][current]
        nxt = state.get("Next") or state.get("Default")
        assert nxt, f"chain from ExtractParallelBranchError dead-ends at {seen}"
        current = nxt
    assert current == "HandleFailure", f"chain never reached HandleFailure: {seen}"
