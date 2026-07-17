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
  3. provably floored upstream — DEFINITE ASSIGNMENT (config-I2767 upgraded
     this from a direct-predecessor check to a transitive backward walk over
     the in-scope state graph, after the 2026-07-16 postclose incident showed
     the eod/daily/groom definitions were entirely outside this test's scope
     and their structurally-safe counters, e.g. `$.ssm_poll.attempts`, sit
     several key-preserving states upstream of their Choice):
     a. a single-segment path (`$.key`) floored at scope entry — the
        initializer Pass's JsonMerge defaults literal (+ injected run_date)
        for a top scope that HAS one (weekly/advisory/modelzoo/daily; the
        eod and groom top scopes have NO initializer and floor nothing), or
        the Map state's ItemSelector/Parameters pinned keys for a Map
        iterator scope;
     b. a one- or two-segment path (`$.x` / `$.x.y`) that on EVERY backward
        path from the Choice to scope entry hits a state that definitely
        assigns it (`ResultPath == $.x` with `y` pinned in ResultSelector /
        Parameters / Result, or a deeper `$.x.y...` ResultPath that creates
        the intermediate) before hitting scope entry or a key-dropping state
        (`ResultPath` absent i.e. `$`, an OutputPath reshape, or a Catch
        edge whose default ResultPath replaces the input with the error
        object). Cycles resolve optimistically (every real execution enters
        a loop from outside it). Three-plus-segment paths (`$.x.Payload.z`)
        are NEVER floorable — a Lambda payload's inner shape cannot be
        pinned — and must be guarded.

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
# config-I2767 (2026-07-16 postclose States.Runtime incident): the eod,
# daily, and groom definitions were NEVER in this test's scope — the I2702
# CheckDegradedOutcome Choice landed exactly in that coverage hole and
# crashed the first fully-green postclose run. ALL SIX definitions are now
# held to the same discipline.
_EOD = _REPO_ROOT / "infrastructure" / "step_function_eod.json"
_DAILY = _REPO_ROOT / "infrastructure" / "step_function_daily.json"
_GROOM = _REPO_ROOT / "infrastructure" / "step_function_groom.json"


def _load(path: pathlib.Path = _WEEKLY) -> dict:
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# scope walking
# ---------------------------------------------------------------------------

def _pinned_keys_of(source) -> set[str]:
    if not isinstance(source, dict):
        return set()
    return {k[:-2] if k.endswith(".$") else k for k in source}


def _pinned_keys(state: dict) -> set[str]:
    """Keys a state's result object is guaranteed to carry at ResultPath."""
    source = state.get("ResultSelector") or (
        state.get("Parameters") if state.get("Type") == "Pass" else None
    ) or (state.get("Result") if state.get("Type") == "Pass" else None)
    return _pinned_keys_of(source)


def _iter_scopes(definition: dict, top_floors: set[str]):
    """Yield (scope_path, states_dict, scope_floors, initializer_name) for
    the top scope, every Parallel branch, and every Map iterator.

    scope_floors = single-segment keys guaranteed present on the scope's
    input for every execution: the initializer floors at the top scope
    (propagated into Parallel branches — branch input is the parallel's
    effective input, and ResultPath writes merge, never drop keys), or the
    Map state's ItemSelector/Parameters pinned keys for an iterator scope
    (per-item input; the top floors do NOT reach it)."""
    def _walk(states: dict, path: str, floors: set[str], init_name, start):
        yield path, states, floors, init_name, start
        for name, state in states.items():
            if state.get("Type") == "Parallel":
                branch_floors = (
                    _pinned_keys_of(state.get("Parameters"))
                    if state.get("Parameters") is not None else floors
                )
                for i, branch in enumerate(state.get("Branches", [])):
                    yield from _walk(branch["States"], f"{path}/{name}[{i}]",
                                     branch_floors, None, branch["StartAt"])
            if state.get("Type") == "Map":
                iterator = state.get("Iterator") or state.get("ItemProcessor")
                if iterator:
                    item_floors = _pinned_keys_of(
                        state.get("ItemSelector") or state.get("Parameters")
                    )
                    yield from _walk(iterator["States"], f"{path}/{name}[map]",
                                     item_floors, None, iterator["StartAt"])
    yield from _walk(definition["States"], "", top_floors,
                     definition["StartAt"] if top_floors else None,
                     definition["StartAt"])


def _initialize_input_floors(definition: dict) -> set[str]:
    """Keys the SF's initializer Pass state (its StartAt — InitializeInput
    on the weekly/daily SFs, InitializeAdvisoryInput / InitializeModelZooInput
    on the two I2544/I2545 child SFs) guarantees on $ for every execution:
    the FIRST embedded JsonMerge defaults literal, plus any States.Format-
    injected run_date. Parsed MECHANICALLY from the state so the floor set
    can never drift from the definition. A definition WITHOUT the initializer
    idiom (eod StartAt is a Choice, groom's InitRunState merges no defaults
    literal onto $) floors NOTHING — config-I2767."""
    init_state = definition["States"][definition["StartAt"]]
    params = (init_state.get("Parameters") or {}).get("merged.$", "")
    marker = "States.StringToJson('"
    if init_state.get("Type") != "Pass" or marker not in params:
        return set()
    start = params.index(marker) + len(marker)
    end = params.index("')", start)
    literal = params[start:end].replace('\\"', '"')
    floors = set(json.loads(literal))
    if "run_date" in params:
        floors.add("run_date")
    assert "sns_topic_arn" in floors, f"{definition['StartAt']} defaults parse failed"
    return floors


def _in_edges(states: dict) -> dict:
    """target -> list of (pred_name, kind, catch_result_path). kind is
    'next' (Next / Default / Choice-rule Next) or 'catch'."""
    edges: dict = {}
    for name, state in states.items():
        targets = {state.get("Next"), state.get("Default")}
        for rule in state.get("Choices", []) or []:
            targets.add(rule.get("Next"))
        for target in targets:
            if target:
                edges.setdefault(target, []).append((name, "next", None))
        for catch in state.get("Catch", []) or []:
            if catch.get("Next"):
                edges.setdefault(catch["Next"], []).append(
                    (name, "catch", catch.get("ResultPath", "$"))
                )
    return edges


class _FloorAnalysis:
    """Definite-assignment analysis for one scope (config-I2767): is a one-
    or two-segment path present on EVERY execution path reaching a state's
    entry? Backward walk with optimistic cycle resolution (an in-progress
    node counts as satisfied — every real execution enters a loop from
    outside it, so the acyclic prefixes decide)."""

    def __init__(self, states: dict, scope_floors: set[str],
                 initializer_name, scope_start: str):
        self.states = states
        self.floors = scope_floors
        self.initializer = initializer_name
        self.scope_start = scope_start
        self.edges = _in_edges(states)

    def floored(self, var: str, choice_name: str) -> bool:
        segments = var.lstrip("$.").split(".")
        if len(segments) > 2 or any("[" in s for s in segments):
            return False  # inner payload shape / array index: guard required
        root = segments[0]
        child = segments[1] if len(segments) > 1 else None
        return self._present_at_entry(choice_name, root, child, set())

    # -- core recursion ----------------------------------------------------
    def _present_at_entry(self, name: str, root, child, visiting) -> bool:
        if name in visiting:
            return True  # optimistic on cycles
        visiting = visiting | {name}
        inbound = self.edges.get(name, [])
        if name == self.scope_start:
            # the raw-entry path is always possible, EVEN IF loops also
            # re-enter the start state — both must carry the path
            if not (child is None and root in self.floors):
                return False
        elif not inbound:
            return False  # unreachable in-scope: no proof
        for pred_name, kind, catch_rp in inbound:
            pred = self.states[pred_name]
            if kind == "catch":
                if not self._catch_out(pred_name, pred, catch_rp, root, child,
                                       visiting):
                    return False
            elif not self._out_present(pred_name, pred, root, child, visiting):
                return False
        return True

    def _out_present(self, name, state, root, child, visiting) -> bool:
        """Does `state`'s OUTPUT definitely carry the path?"""
        if name == self.initializer:
            return child is None and root in self.floors
        if state.get("OutputPath") not in (None, "$"):
            return False  # output reshaped: conservative
        if state.get("Type") in ("Choice", "Wait", "Succeed", "Fail"):
            return self._present_at_entry(name, root, child, visiting)
        rp = state.get("ResultPath", "$")
        if rp is None:
            return self._present_at_entry(name, root, child, visiting)
        if rp == "$":
            pins = _pinned_keys(state)
            if root not in pins:
                return False
            if child is None:
                return True
            return child in self._literal_child_pins(state, root)
        rp_segments = rp[2:].split(".")
        if rp_segments[0] != root:
            # merge elsewhere: the path survives iff present on entry
            return self._present_at_entry(name, root, child, visiting)
        if len(rp_segments) == 1:
            # ResultPath == $.root: the node is REPLACED by the result
            return child is None or child in _pinned_keys(state)
        # ResultPath == $.root.x...: root is created/kept; child present if
        # it IS x, else only if it survived from the state's own input
        if child is None or rp_segments[1] == child:
            return True
        return self._present_at_entry(name, root, child, visiting)

    def _catch_out(self, name, state, catch_rp, root, child, visiting) -> bool:
        """A Catch edge's output = the state's RAW INPUT with the error
        object merged at the catch ResultPath (default '$' — which REPLACES
        the input with the bare error object)."""
        if catch_rp == "$" or catch_rp is None:
            return False
        rp_segments = catch_rp[2:].split(".")
        if rp_segments[0] == root:
            # error object lands at/under root — its shape is {Error, Cause}
            if child is None:
                return True
            if len(rp_segments) > 1 and rp_segments[1] == child:
                return True
            return child in ("Error", "Cause") and len(rp_segments) == 1
        return self._present_at_entry(name, root, child, visiting)

    @staticmethod
    def _literal_child_pins(state, root) -> set[str]:
        source = state.get("ResultSelector") or (
            state.get("Parameters") if state.get("Type") == "Pass" else None
        ) or (state.get("Result") if state.get("Type") == "Pass" else None)
        if isinstance(source, dict) and isinstance(source.get(root), dict):
            return _pinned_keys_of(source[root])
        return set()


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
# carry the SAME config#2275 guard-or-floor discipline.
# config-I2767 (2026-07-16): eod/daily/groom added — the whole point of the
# incident fix. Counts are walker-regression floors, not exact.
_MIN_CHOICES_SEEN = {
    _WEEKLY: 35, _ADVISORY: 8, _MODELZOO: 3,
    _EOD: 15, _DAILY: 12, _GROOM: 4,
}


@pytest.mark.parametrize(
    "sf_path", [_WEEKLY, _ADVISORY, _MODELZOO, _EOD, _DAILY, _GROOM],
    ids=lambda p: p.stem,
)
def test_every_choice_variable_is_guarded_or_floored(sf_path):
    definition = _load(sf_path)
    top_floors = _initialize_input_floors(definition)
    violations: list[str] = []
    choices_seen = 0
    for scope_path, states, floors, init_name, start in _iter_scopes(
            definition, top_floors):
        analysis = _FloorAnalysis(states, floors, init_name, start)
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

            def floored(var, _name=name, _analysis=analysis):
                return _analysis.floored(var, _name)

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
    for _, states, _, _, _ in _iter_scopes(definition, set()):
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


@pytest.mark.parametrize(
    ("choice", "partial_input", "expected_route"),
    [
        # THE 2026-07-16 incident: a fully-green day never sets
        # $.degraded_summary — must end NormalSucceeded, not States.Runtime.
        ("CheckDegradedOutcome", {}, "NormalSucceeded"),
        ("CheckDegradedOutcome", {"degraded_summary": {"degraded": True}},
         "DegradedSucceeded"),
        # Malformed dispatcher payload before the cost-guard tail: route to
        # the visible data-spot failure notifier, never a crash that skips
        # StopTradingInstance.
        ("CheckPostMarketDataSpotLaunched",
         {"postmarket_launch": {"Payload": {}}}, "ExtractDataSpotError"),
        ("CheckPostMarketArcticAppendSpotLaunched",
         {"postmarket_arctic_launch": {"Payload": {}}}, "ExtractDataSpotError"),
        # Heal-loop probes: absent fields keep the loop bounded, no crash.
        ("HealCheckConverged", {"precondition_probe": {"Payload": {}}},
         "HealLoopIncrement"),
        ("HealLoopGate",
         {"heal_loop": {"attempts": 0}, "precondition_probe": {"Payload": {}}},
         "HealLaunchPostMarketDataSpot"),
        ("HealLoopGate",
         {"heal_loop": {"attempts": 2}, "precondition_probe": {"Payload": {}}},
         "HealNonConvergent"),
        ("CheckHealLoopEligible", {}, "InitHealLoop"),
        # Healthy-path sanity: the guards must not change live semantics.
        ("CheckHealLoopEligible", {"pipeline_role": "operator-replay"},
         "HealNonConvergent"),
        ("CheckHealLoopEligible", {"pipeline_role": "eod"}, "InitHealLoop"),
        ("CheckPostMarketDataSpotLaunched",
         {"postmarket_launch": {"Payload": {"data_spot": {"launched": True}}}},
         "PollPostMarketDataSpot"),
        ("CheckPostMarketDataSpotLaunched",
         {"postmarket_launch": {"Payload": {"data_spot": {"launched": False}}}},
         "CheckSkipCaptureSnapshot"),
        ("HealCheckConverged",
         {"precondition_probe": {"Payload": {"precondition_met": True}}},
         "HealDispatchReplay"),
    ],
)
def test_partial_payload_routes_to_explicit_path_eod(choice, partial_input, expected_route):
    """config-I2767: same drill, pointed at the postclose SF the 2026-07-16
    States.Runtime incident occurred in."""
    definition = _load(_EOD)
    assert _choice_target(definition, choice, partial_input) == expected_route


@pytest.mark.parametrize(
    ("choice", "partial_input", "expected_route"),
    [
        # Malformed gate/dispatcher payloads pre-trading: fail CLOSED and
        # loud — never trade on an unverifiable verdict, never crash.
        ("DeployDriftGate", {"drift_result": {"Payload": {}}}, "HandleFailure"),
        ("TradingDayGateChoice", {"trading_day_gate": {"Payload": {}}},
         "HandleFailure"),
        ("CoverageGapChoice", {"coverage_result": {"Payload": {}}},
         "HandleFailure"),
        ("FinalCoverageGate", {"coverage_recheck_result": {"Payload": {}}},
         "HandleFailure"),
        # Data-spot launched-gates are the exception to fail-closed: a
        # data-spot failure must NEVER block daemon start (config#1767 #4),
        # so malformed routes to the fail-open-but-LOUD notifier chain.
        ("CheckMorningEnrichSpotLaunched",
         {"morning_enrich_launch": {"Payload": {}}}, "ExtractDataSpotError"),
        ("CheckMorningArcticAppendSpotLaunched",
         {"arctic_append_launch": {"Payload": {}}}, "ExtractDataSpotError"),
        # Healthy-path sanity: the guards must not change live semantics.
        ("TradingDayGateChoice",
         {"trading_day_gate": {"Payload": {"is_trading_day": False}}},
         "NotifyHolidaySkip"),
        ("TradingDayGateChoice",
         {"trading_day_gate": {"Payload": {"is_trading_day": True}}},
         "StartExecutorEC2"),
        ("DeployDriftGate", {"drift_result": {"Payload": {"has_drift": False}}},
         "TradingDayGate"),
        ("CheckMorningEnrichSpotLaunched",
         {"morning_enrich_launch": {"Payload": {"data_spot": {"launched": False}}}},
         "CheckSkipPredictorInference"),
    ],
)
def test_partial_payload_routes_to_explicit_path_daily(choice, partial_input, expected_route):
    """config-I2767: same drill, pointed at the preopen SF."""
    definition = _load(_DAILY)
    assert _choice_target(definition, choice, partial_input) == expected_route


@pytest.mark.parametrize(
    ("choice", "partial_input", "expected_route"),
    [
        # Unverifiable launch escalates via the exhausted-notifier — never
        # mislabeled as the INTENTIONAL kill-switch skip, never a crash.
        ("CheckLaunched", {"groomLaunch": {"Payload": {}}},
         "GroomRetriesExhausted"),
        # Healthy-path sanity: the guards must not change live semantics.
        ("CheckLaunched",
         {"groomLaunch": {"Payload": {"groom": {"launched": True}}}},
         "PollGroomCommand"),
        ("CheckLaunched",
         {"groomLaunch": {"Payload": {"groom": {"launched": False}}}},
         "GroomSkipped"),
    ],
)
def test_partial_payload_routes_to_explicit_path_groom(choice, partial_input, expected_route):
    """config-I2767: same drill, pointed at the groom-dispatch SF."""
    definition = _load(_GROOM)
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
