"""alpha-engine-config-I10540 — ``$.degraded_summary`` must ACCUMULATE, not
overwrite, across a weekly execution's degraded stages.

THE DEFECT THIS PINS
--------------------
Every ``Set*DegradedSummary`` Pass state wrote the WHOLE ``$.degraded_summary``
object via a literal ``Parameters`` block carrying a scalar ``"reason"``, with
``ResultPath: "$.degraded_summary"``. The last one to fire wins and every
earlier degradation is dropped from the durable artifact (the terminal
``DegradedRun`` cause and the ``_sf_completion/`` marker).

Measured on the 2026-09-19 execution
(``3c2fe2f8-7ea5-850e-8449-4f3dd5dd69e2_d0e3eae8-866c-2bf2-6988-96c79fff8cef``,
run_date 2026-09-18): ``SetResearchPredictorDegradedSummary`` fired (history
event 6288, ``weekly_research_predictor_branch_fail_open``), then
``SetAggregateCostsDegradedSummary`` (7722, ``weekly_aggregate_costs_fail_open``)
overwrote it — the completion marker at
``s3://alpha-engine-research/_sf_completion/ne-weekly-freshness-pipeline/2026-09-18.json``
names only AggregateCosts. Recurrence of the identical shape measured on the
2026-09-12 run (AggregateCosts dropped ChallengerShadow).

THE FIX
-------
Each ``Set*DegradedSummary`` now merges a family-keyed entry into
``$.degraded_summary.reasons`` via ``States.JsonMerge($.degraded_summary.reasons,
<this family's fragment>, false)`` instead of replacing the whole object.
``degraded_summary`` is floored in ``InitializeInput`` at
``{"degraded": false, "reasons": {}, "run_date": null}`` so the first setter on
any execution never merges onto a missing path. ``SetScannerResourceKillDegraded
Summary`` and ``SetResearchPredictorDegradedSummary`` deliberately share the
family key ``research_predictor`` — ``CheckScannerResourceKillReason``'s
overwrite-with-a-more-specific-reason intent is preserved, scoped to that one
family entry.
"""
from __future__ import annotations

import json
import pathlib
import re

import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

_MERGE_PREFIX = "States.JsonMerge($.degraded_summary.reasons, States.StringToJson('"

#: State name -> family key. Two states deliberately share one key: the
#: resource-kill overwrite is scoped to the research_predictor family entry,
#: not the whole reasons object (alpha-engine-config-I7812's intent, preserved
#: under the new shape).
_FAMILY_BY_STATE = {
    "SetResearchPredictorDegradedSummary": "research_predictor",
    "SetScannerResourceKillDegradedSummary": "research_predictor",
    "SetAggregateCostsDegradedSummary": "aggregate_costs",
    "SetScannerLeaderboardDegradedSummary": "scanner_leaderboard",
    "SetLibPinGateDegradedSummary": "lib_pin_gate",
    "SetPipelineContractGateDegradedSummary": "pipeline_contract_gate",
    "SetEvaluatorGateDegradedSummary": "evaluator_gate",
    "SetEvaluatorDirectorGateDegradedSummary": "evaluator_director_gate",
    "SetParityDegradedSummary": "parity",
    "SetParityCompareDegradedSummary": "parity_compare",
    "SetSaturdayHealthCheckDegradedSummary": "saturday_health_check",
    "SetSubstrateHealthCheckDegradedSummary": "substrate_health_check",
    "SetReportCardDegradedSummary": "report_card",
    "SetScannerLeaderboardResourceKillSummary": "scanner_leaderboard",
    "SetMutexAcquireDegradedFlagSummary": "mutex_acquire",
    "NotifyCompleteDegraded": "completion_notify",
    "NotifyShellRunCompleteDegraded": "preflight_notify",
}


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text(encoding="utf-8"))["States"]


@pytest.fixture(scope="module")
def floor(states) -> dict:
    merged = states["InitializeInput"]["Parameters"]["merged.$"]
    m = re.search(r"StringToJson\('(\{.*?\})'\)", merged)
    assert m, "InitializeInput's innermost defaults blob was not found"
    return json.loads(m.group(1))


def _degraded_summary_setters(states: dict) -> dict[str, dict]:
    return {
        name: body
        for name, body in states.items()
        if body.get("ResultPath") == "$.degraded_summary"
        and isinstance(body.get("Parameters"), dict)
        and body["Parameters"].get("degraded") is True
    }


def test_every_known_setter_is_covered_by_this_test(states):
    """The state set this module pins must match the live definition exactly —
    an uncovered new setter would silently keep the last-write-wins defect."""
    setters = _degraded_summary_setters(states)
    assert set(setters) == set(_FAMILY_BY_STATE), (
        f"the Set*DegradedSummary state set changed: found {sorted(setters)}, "
        f"this test knows {sorted(_FAMILY_BY_STATE)}. A new fail-open path must "
        "be added to _FAMILY_BY_STATE with its own family key, or it silently "
        "inherits the last-write-wins defect this module exists to prevent."
    )


@pytest.mark.parametrize("name,family", sorted(_FAMILY_BY_STATE.items()))
def test_setter_merges_into_reasons_rather_than_replacing(states, name, family):
    """THE regression this module exists for.

    A setter reverted to a bare literal ``Parameters`` block (no ``reasons.$``,
    or a ``reasons.$`` that does not incorporate the PRIOR
    ``$.degraded_summary.reasons``) reproduces the last-write-wins defect: two
    degradations in one execution collapse to one entry.
    """
    params = states[name]["Parameters"]
    reasons_expr = params.get("reasons.$")
    assert reasons_expr, (
        f"{name} sets degraded: true with no reasons.$ — it replaces the whole "
        "$.degraded_summary object instead of accumulating into .reasons, which "
        "is the exact alpha-engine-config-I10540 defect (measured on the "
        "2026-09-12 and 2026-09-19 weekly runs)."
    )
    assert reasons_expr.startswith(_MERGE_PREFIX), (
        f"{name}.reasons.$ = {reasons_expr!r} does not merge onto the prior "
        "$.degraded_summary.reasons — a setter that replaces .reasons wholesale "
        "reintroduces last-write-wins one level down."
    )
    fragment_text = reasons_expr[len(_MERGE_PREFIX):].split("'), false)")[0]
    fragment = json.loads(fragment_text)
    assert fragment == {family: params["reason"]}, (
        f"{name} must merge exactly {{{family!r}: {params['reason']!r}}} into "
        f"reasons, got {fragment}"
    )


def test_scanner_resource_kill_and_research_predictor_share_one_family_key(states):
    """CheckScannerResourceKillReason's overwrite is preserved, scoped to the
    research_predictor family entry only — it must not clobber any other
    family's reason when both fire in the same execution."""
    rp = states["SetResearchPredictorDegradedSummary"]["Parameters"]["reasons.$"]
    srk = states["SetScannerResourceKillDegradedSummary"]["Parameters"]["reasons.$"]
    rp_fragment = json.loads(rp[len(_MERGE_PREFIX):].split("'), false)")[0])
    srk_fragment = json.loads(srk[len(_MERGE_PREFIX):].split("'), false)")[0])
    assert set(rp_fragment) == set(srk_fragment) == {"research_predictor"}
    assert rp_fragment["research_predictor"] != srk_fragment["research_predictor"], (
        "the resource-kill reason must be MORE SPECIFIC than the generic "
        "research-predictor reason it overwrites, or the split is pointless"
    )


def test_degraded_summary_is_floored_with_an_empty_reasons_object(floor):
    """The first setter on any execution must never call JsonMerge on a missing
    path — CheckDegradedOutcome's ``IsPresent && BooleanEquals(true)`` read
    must also never fire on a clean run, so ``degraded`` is floored ``False``,
    never absent."""
    assert floor.get("degraded_summary") == {
        "degraded": False,
        "reasons": {},
        "run_date": None,
    }


def test_check_degraded_outcome_never_routes_a_floored_clean_run_as_degraded(states):
    """Verifies the claim the floor's safety rests on: CheckDegradedOutcome
    reads $.degraded_summary.degraded with BooleanEquals(true), so the floored
    ``False`` routes to the clean WriteCompletionMarker, never to
    WriteCompletionMarkerDegraded."""
    choice = states["CheckDegradedOutcome"]["Choices"][0]
    leaves = choice["And"]
    assert any(
        leaf.get("Variable") == "$.degraded_summary.degraded" and leaf.get("IsPresent") is True
        for leaf in leaves
    )
    assert any(
        leaf.get("Variable") == "$.degraded_summary.degraded" and leaf.get("BooleanEquals") is True
        for leaf in leaves
    ), (
        "CheckDegradedOutcome must compare BooleanEquals(true), not test "
        "IsPresent alone — a floored (present, False) value would otherwise "
        "route every clean run to WriteCompletionMarkerDegraded"
    )
    assert states["CheckDegradedOutcome"]["Default"] == "WriteCompletionMarker"


def test_degraded_run_cause_path_renders_every_accumulated_reason(states):
    """The terminal must render the WHOLE reasons map, not just the historical
    singular ``.reason`` field, so a run with two or more degradations names
    all of them — the literal Closes-when of alpha-engine-config-I10540."""
    cause_path = states["DegradedRun"]["CausePath"]
    assert "States.JsonToString($.degraded_summary.reasons)" in cause_path, (
        "DegradedRun's CausePath does not render $.degraded_summary.reasons — "
        "a run with two degraded families would still only expose the historical "
        "singular .reason in its human-readable cause"
    )
    assert "States.JsonToString($.degraded_summary)" in cause_path, (
        "DegradedRun's CausePath must still carry the full object (stage_error "
        "and friends live there, outside .reasons)"
    )


def test_a_two_family_execution_accumulates_both_reasons_end_to_end():
    """Fixture replay of the 2026-09-19 shape: ResearchPredictor fires, then
    AggregateCosts fires. Simulates the two Pass states' merge semantics
    directly against the JSON in the definition, without a live SFN account."""
    doc = json.loads(_WEEKLY.read_text(encoding="utf-8"))
    states = doc["States"]

    degraded_summary = {"degraded": False, "reasons": {}, "run_date": None}

    def apply(name: str) -> None:
        params = states[name]["Parameters"]
        family, reason = next(iter(
            json.loads(
                params["reasons.$"][len(_MERGE_PREFIX):].split("'), false)")[0]
            ).items()
        ))
        degraded_summary["reasons"] = {**degraded_summary["reasons"], family: reason}
        degraded_summary["degraded"] = True
        degraded_summary["run_date"] = "2026-09-18"

    apply("SetResearchPredictorDegradedSummary")
    apply("SetAggregateCostsDegradedSummary")

    assert degraded_summary["reasons"] == {
        "research_predictor": "weekly_research_predictor_branch_fail_open",
        "aggregate_costs": "weekly_aggregate_costs_fail_open",
    }, (
        "both families must survive in $.degraded_summary.reasons — this is "
        "the exact 2026-09-19 execution shape alpha-engine-config-I10540 measured "
        "collapsing to one entry"
    )
