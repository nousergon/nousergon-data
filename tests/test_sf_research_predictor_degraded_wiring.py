"""alpha-engine-config#6722 — ResearchPredictorParallel's own internal
fail-open Catch routes must propagate to the terminal notifier.

Context: alpha-engine-config#6715 built
tests/test_sf_structural_contract.py::test_every_fail_open_catch_route_sets_the_degraded_flag_or_is_exempt,
a structural walker asserting every fail-open Catch route passes through a
state that writes the degraded-flag JSONPath the terminal notifier actually
reads. Running it against step_function.json surfaced 21 pre-existing gaps
inside ResearchPredictorParallel (config#6722's full list) — the walker
itself cannot trace them directly because ASL Parallel branches are isolated
JSONPath scopes: a Pass state inside a branch cannot write an outer-scope
path like $.gate_degraded, so no fix inside a branch is ever visible to a
generic forward walk starting at top-level Choice states.

The fix (mirrors PR1277's ParityParallel branch-fold pattern,
alpha-engine-config#6030's CheckParityBranchOutcomes):
  1. Each branch seeds a branch-LOCAL $.research_degraded_local=false at its
     own StartAt (InitResearchDegradedFlag for Branch A, InitPredictorDegradedFlag
     for Branch B).
  2. Every fail-open Catch inside a branch routes through a small
     Mark*Degraded Pass state that sets $.research_degraded_local=true
     without changing the original continuation.
  3. Every branch terminal (BranchAComplete/BranchAFailed,
     BranchBComplete/BranchBFailed/PredictorTrainingSkipped) hoists that
     local marker into its own branch_a_degraded/branch_b_degraded field,
     UNCONDITIONALLY (including the false/skip/failed paths) so the
     post-join Parameters.$ extraction can never throw States.Runtime.
  4. AggregateBranchOutcomes (the Parallel join) hoists both fields onto
     $.branch_outcomes.
  5. CheckResearchPredictorDegraded (new Choice, spliced onto
     CheckBranchOutcomes' non-FAILED Default) ORs the two branch flags and,
     if either is true, routes through SetResearchPredictorDegraded — the
     SOLE writer of the new top-level $.research_predictor_degraded flag —
     before continuing to CheckSkipBacktester exactly as the clean path
     always did.
  6. CheckGateDegradedNotify (the terminal completion-email selector) gets a
     new top rule: research_predictor_degraded=true ALWAYS routes to the
     generic NotifyCompleteMultipleDegraded, even when it is the ONLY family
     degraded — deliberately given no single-flag notifier of its own,
     since it already covers 25+ distinct internal routes.

Because the generic structural walker cannot verify a cross-branch fold,
the 21 corresponding tests/test_sf_structural_contract.py
_DEGRADED_FLAG_EXEMPT entries stay in place (not deleted) with their
"VIOLATION — tracked" reason replaced by a real, verified justification
citing this module — the same disposition PR1277 used for ParityParallel's
six intra-branch entries.
"""
from __future__ import annotations

import itertools
import json
import pathlib

import pytest
from tests.sf_degraded_summary_helpers import assert_degraded_continuation

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_WEEKLY.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


@pytest.fixture(scope="module")
def parallel(states) -> dict:
    return states["ResearchPredictorParallel"]


@pytest.fixture(scope="module")
def branch_a(parallel) -> dict:
    return parallel["Branches"][0]["States"]


@pytest.fixture(scope="module")
def branch_b(parallel) -> dict:
    return parallel["Branches"][1]["States"]


# ---------------------------------------------------------------------------
# Branch-local seeding
# ---------------------------------------------------------------------------


def test_branch_a_starts_at_init_research_degraded(parallel):
    assert parallel["Branches"][0]["StartAt"] == "InitResearchDegradedFlag"


def test_branch_b_starts_at_init_predictor_degraded(parallel):
    assert parallel["Branches"][1]["StartAt"] == "InitPredictorDegradedFlag"


def test_init_research_degraded_seeds_false(branch_a):
    st = branch_a["InitResearchDegradedFlag"]
    assert st["Type"] == "Pass"
    # alpha-engine-config-I11073: an object, not a bare boolean. `routes` is the
    # accumulator every Mark*Degraded state appends its own name to, seeded here
    # so the first route's Parameters.$ read of it cannot throw States.Runtime.
    assert st["Result"] == {"degraded": False, "routes": ""}
    assert st["ResultPath"] == "$.research_degraded_local"
    assert st["Next"] == "CheckSkipScanner"


def test_init_predictor_degraded_seeds_false(branch_b):
    st = branch_b["InitPredictorDegradedFlag"]
    assert st["Type"] == "Pass"
    assert st["Result"] == {"degraded": False, "routes": ""}
    assert st["ResultPath"] == "$.research_degraded_local"
    assert st["Next"] == "CheckSkipPredictorTraining"


# ---------------------------------------------------------------------------
# Branch A Mark*Degraded states — each threads the local flag without
# changing the pre-existing fail-open continuation.
# ---------------------------------------------------------------------------

_BRANCH_A_MARK_STATES = {
    "MarkScannerDegraded": "CheckSkipRegimeSubstrate",
    "MarkRegimeSubstrateDegraded": "CheckSkipSignalsEnvelope",
    "MarkChallengerShadowDegraded": "CheckSkipRAGIngestion",
    "MarkRegimeRetrospectiveEvalDegraded": "CheckSkipDataPhase2",
    "MarkEvalJudgeDegraded": "EvalRollingMean",
    "MarkEvalRollingMeanDegraded": "CheckSkipRationaleClustering",
    # alpha-engine-config-I10539 (Brian ruling 2026-09-16, option b): the
    # ReplayConcordance stage is retired, so MarkRationaleClusteringDegraded
    # folds straight onto CheckSkipCounterfactual and
    # MarkReplayConcordanceDegraded no longer exists.
    "MarkRationaleClusteringDegraded": "CheckSkipCounterfactual",
    # alpha-engine-config-I7194: Counterfactual is Branch A's last work state
    # now that the aggregator runs at the top level, so this fold lands on the
    # branch terminal directly. MarkAggregateCostsDegraded left this table with
    # the aggregator — it no longer writes $.research_degraded_local, because
    # the branch-local fold does not exist outside the Parallel; its top-level
    # replacement writes $.aggregate_costs_degraded and is pinned by
    # tests/test_sf_aggregate_costs_wiring.py.
    "MarkCounterfactualDegraded": "BranchAComplete",
}


@pytest.mark.parametrize("name,next_target", sorted(_BRANCH_A_MARK_STATES.items()))
def test_branch_a_mark_state_shape(branch_a, name, next_target):
    st = branch_a[name]
    assert st["Type"] == "Pass"
    # alpha-engine-config-I11073: each route sets the flag AND appends its own
    # name. A bare `Result: True` discards the route identity at the one state
    # that knows it, and ten routes then share one indistinguishable boolean.
    assert "Result" not in st
    assert st["Parameters"]["degraded"] is True
    assert st["Parameters"]["routes.$"] == (
        f"States.Format('{{}},{{}}',$.research_degraded_local.routes,'{name}')"
    )
    assert st["ResultPath"] == "$.research_degraded_local"
    assert st["Next"] == next_target


@pytest.mark.parametrize(
    "owner",
    [
        "Scanner",
        "RegimeSubstrate",
        "ChallengerShadow",
        "RegimeRetrospectiveEval",
        "EvalJudgeSubmitFirstSaturday",
        "EvalJudgeSubmitWeekly",
        # alpha-engine-config-I9329: EvalJudgePoll is gone; the spot
        # dispatcher took its place as a fail-open owner in this chain, and
        # EvalJudgeProcess is now an ssm:sendCommand rather than a
        # lambda:invoke — its Catch obligation is unchanged.
        "DispatchEvalJudgeSpot",
        "EvalJudgeProcess",
        "EvalRollingMean",
        "RationaleClustering",
        # alpha-engine-config-I10539: ReplayConcordance is retired.
        "Counterfactual",
    ],
)
def test_branch_a_owner_catch_routes_through_a_mark_state(branch_a, owner):
    """Every one of the 12 fail-open owner states routes its Catch through
    SOME Mark*Degraded state (shared where multiple owners converge, e.g.
    all four eval-judge submit/poll/process states share
    MarkEvalJudgeDegraded — the pre-existing convergence on EvalRollingMean
    is unchanged, only detoured through the flag-setter)."""
    catches = branch_a[owner]["Catch"]
    # alpha-engine-config-I7812: Scanner carries a SECOND, EARLIER Catch for
    # States.Timeout / Lambda.Unknown — a resource kill is not a domain failure
    # and must not fold into this generic fail-open (sf-pipeline-policy.md §3);
    # it is asserted in tests/test_sf_scanner_resource_kill_i7812.py. Every
    # owner's LAST catch is still the States.ALL fail-open this test owns, and
    # no other owner may grow a second one without a declared reason.
    assert len(catches) == (2 if owner == "Scanner" else 1), (
        f"{owner} has {len(catches)} Catch entries; only Scanner has a declared "
        "resource-kill fork (alpha-engine-config-I7812)"
    )
    catch = catches[-1]
    assert catch["ErrorEquals"] == ["States.ALL"]
    # alpha-engine-config-I9636: an owner may route through ONE Extract* Pass
    # that stamps the phase before the shared Mark*Degraded convergence. Nine
    # arrivals share MarkEvalJudgeDegraded and it records a bare boolean, so
    # without the hop "the submit was refused for billing" and "the spot box
    # never booted" are the same bytes on the execution record — which is
    # exactly why the 2026-08-29 run's degradation could not be diagnosed from
    # the record and had to be reconstructed from 7,858 history events. The
    # hop is bounded at one and must name a phase, so it can never become a
    # place to hide a continuation change.
    target = catch["Next"]
    if target.startswith("Extract"):
        hop = branch_a[target]
        assert hop["Type"] == "Pass", target
        assert hop["ResultPath"].endswith("_error"), target
        assert hop["Parameters"].get("phase"), (
            f"{target} is an Extract hop that names no phase — the whole point"
        )
        target = hop["Next"]
    assert target.startswith("Mark") and target.endswith("Degraded")
    assert target in branch_a
    assert branch_a[target]["ResultPath"] == "$.research_degraded_local"
    assert branch_a[target]["Parameters"]["degraded"] is True


def test_no_state_writes_the_old_dead_thinktank_path(branch_a, branch_b, states):
    """Regression guard: $.thinktank_degraded must have zero writers anywhere
    in the file post-repoint — a stray second writer would resurrect the
    dead-flag trap config#6715 was built to catch."""
    all_states = {**states, **branch_a, **branch_b}
    writers = [
        name for name, st in all_states.items()
        if isinstance(st, dict) and st.get("ResultPath") == "$.thinktank_degraded"
    ]
    assert writers == []


# ---------------------------------------------------------------------------
# Branch B: the model-zoo rotation group shares ONE convergence point
# (PublishModelZooFailureImmediate) for all 5 fail-open Catches.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "owner",
    ["ResolveZooSpecs", "WaitResolveZoo", "ModelZooTrainMap", "ModelZooSelect", "WaitForModelZoo"],
)
def test_branch_b_model_zoo_owner_catch_converges_on_publish(branch_b, owner):
    (catch,) = branch_b[owner]["Catch"]
    assert catch["Next"] == "PublishModelZooFailureImmediate"


def test_publish_model_zoo_failure_routes_through_mark_model_zoo_degraded(branch_b):
    st = branch_b["PublishModelZooFailureImmediate"]
    assert st["Next"] == "MarkModelZooDegraded"
    (catch,) = st["Catch"]
    assert catch["Next"] == "MarkModelZooDegraded"


def test_mark_model_zoo_degraded_shape(branch_b):
    st = branch_b["MarkModelZooDegraded"]
    assert st["Type"] == "Pass"
    assert "Result" not in st
    assert st["Parameters"]["degraded"] is True
    assert st["Parameters"]["routes.$"] == (
        "States.Format('{},{}',$.research_degraded_local.routes,"
        "'MarkModelZooDegraded')"
    )
    assert st["ResultPath"] == "$.research_degraded_local"
    assert st["Next"] == "BranchBComplete"


# ---------------------------------------------------------------------------
# Every branch terminal sets its *_degraded field UNCONDITIONALLY — the
# Parameters.$ extraction downstream must never throw States.Runtime.
# ---------------------------------------------------------------------------


def test_branch_a_complete_hoists_local_flag(branch_a):
    st = branch_a["BranchAComplete"]
    assert st["Parameters"]["branch_a"]["branch_a_status"] == "OK"
    assert (
        st["Parameters"]["branch_a"]["branch_a_degraded.$"]
        == "$.research_degraded_local.degraded"
    )
    # alpha-engine-config-I11073: the names leave the branch here or nowhere —
    # a Parallel branch is its own JSONPath scope.
    assert (
        st["Parameters"]["branch_a"]["branch_a_routes.$"]
        == "$.research_degraded_local.routes"
    )


def test_branch_a_failed_sets_degraded_false(branch_a):
    st = branch_a["BranchAFailed"]
    assert st["Parameters"]["branch_a"]["branch_a_status"] == "FAILED"
    assert st["Parameters"]["branch_a"]["branch_a_degraded"] is False
    # Every terminal must set every field the join extracts, or the post-join
    # Parameters.$ throws States.Runtime on whichever path ran.
    assert st["Parameters"]["branch_a"]["branch_a_routes"] == ""


def test_branch_b_complete_hoists_local_flag(branch_b):
    st = branch_b["BranchBComplete"]
    assert st["Parameters"]["branch_b"]["branch_b_status"] == "OK"
    assert (
        st["Parameters"]["branch_b"]["branch_b_degraded.$"]
        == "$.research_degraded_local.degraded"
    )
    # alpha-engine-config-I11073: the names leave the branch here or nowhere —
    # a Parallel branch is its own JSONPath scope.
    assert (
        st["Parameters"]["branch_b"]["branch_b_routes.$"]
        == "$.research_degraded_local.routes"
    )


def test_branch_b_failed_sets_degraded_false(branch_b):
    st = branch_b["BranchBFailed"]
    assert st["Parameters"]["branch_b"]["branch_b_status"] == "FAILED"
    assert st["Parameters"]["branch_b"]["branch_b_degraded"] is False
    # Every terminal must set every field the join extracts, or the post-join
    # Parameters.$ throws States.Runtime on whichever path ran.
    assert st["Parameters"]["branch_b"]["branch_b_routes"] == ""


def test_predictor_training_skipped_sets_degraded_false(branch_b):
    st = branch_b["PredictorTrainingSkipped"]
    assert st["Result"]["branch_b"]["branch_b_status"] == "OK"
    assert st["Result"]["branch_b"]["branch_b_degraded"] is False
    assert st["Result"]["branch_b"]["branch_b_routes"] == ""


# ---------------------------------------------------------------------------
# The post-join fold
# ---------------------------------------------------------------------------


def test_aggregate_branch_outcomes_hoists_both_degraded_fields(states):
    p = states["AggregateBranchOutcomes"]["Parameters"]
    assert p["branch_a_degraded.$"] == "$.parallel_result[0].branch_a.branch_a_degraded"
    assert p["branch_b_degraded.$"] == "$.parallel_result[1].branch_b.branch_b_degraded"


def test_check_branch_outcomes_default_is_the_degraded_fold(states):
    assert states["CheckBranchOutcomes"]["Default"] == "CheckResearchPredictorDegraded"


def test_check_research_predictor_degraded_ors_both_branches(states):
    """config#2275: each Or leaf is itself an IsPresent-guarded
    And:[{IsPresent},{BooleanEquals}] pair — a bare Or of unguarded
    BooleanEquals leaves would States.Runtime on any payload where a branch
    field happens to be absent (tests/test_sf_choice_guards.py enforces this
    convention repo-wide)."""
    c = states["CheckResearchPredictorDegraded"]
    assert c["Type"] == "Choice"
    (choice,) = c["Choices"]
    leaf_vars = set()
    for and_leaf in choice["Or"]:
        conds = and_leaf["And"]
        var_names = {cond["Variable"] for cond in conds}
        assert len(var_names) == 1, "each Or leaf must guard exactly one Variable"
        (var,) = var_names
        leaf_vars.add(var)
        present = next(c for c in conds if "IsPresent" in c)
        boolean = next(c for c in conds if "BooleanEquals" in c)
        assert present["IsPresent"] is True
        assert boolean["BooleanEquals"] is True
    assert leaf_vars == {
        "$.branch_outcomes.branch_a_degraded",
        "$.branch_outcomes.branch_b_degraded",
    }
    assert choice["Next"] == "SetResearchPredictorDegraded"
    assert c["Default"] == "CheckSkipBacktester"


def test_set_research_predictor_degraded_shape(states):
    st = states["SetResearchPredictorDegraded"]
    assert st["Type"] == "Pass"
    assert st["Result"] is True
    assert st["ResultPath"] == "$.research_predictor_degraded"
    # alpha-engine-config-I11073: the family boolean now hands off to the state
    # that names WHICH routes fired, which hands back to the summary. A Pass has
    # exactly one ResultPath, so this is three states, not one.
    assert st["Next"] == "SetResearchPredictorDegradedRoutes"
    routes = states["SetResearchPredictorDegradedRoutes"]
    assert routes["Type"] == "Pass"
    assert routes["ResultPath"] == "$.research_predictor_degraded_routes"
    assert routes["Next"] == "SetResearchPredictorDegradedSummary"
    # alpha-engine-config-I7812: the family flag still routes through its own
    # summary; that summary now continues into the resource-kill reason fork,
    # whose Default is the unchanged CheckSkipBacktester.
    assert_degraded_continuation(
        states, "SetResearchPredictorDegraded", "CheckScannerResourceKillReason",
        via="SetResearchPredictorDegradedRoutes",
    )
    fork = states["CheckScannerResourceKillReason"]
    assert fork["Type"] == "Choice"
    assert fork["Default"] == "CheckSkipBacktester"
    assert states["SetScannerResourceKillDegradedSummary"]["Next"] == "CheckSkipBacktester"


def test_only_set_research_predictor_degraded_writes_the_flag(states):
    """SF-controlled: exactly one Pass state may write
    $.research_predictor_degraded (mirrors test_only_parity_degraded_pass_sets_the_flag)."""
    writers = [
        name for name, st in states.items()
        if st.get("ResultPath") == "$.research_predictor_degraded"
    ]
    assert writers == ["SetResearchPredictorDegraded"]


# ---------------------------------------------------------------------------
# The hard invariant: research_predictor_degraded=true never reaches plain
# NotifyComplete, for every combination of the five degraded flags, and
# ALWAYS resolves to the generic combined notifier (no dedicated single-flag
# notifier exists for this family by design).
# ---------------------------------------------------------------------------


def _notify_target(states, data: dict) -> str:
    """Evaluate the CheckShellRunNotify -> CheckGateDegradedNotify selection
    with ASL short-circuit semantics against a partial payload. Mirrors
    tests/test_sf_parity_gate_notify_wiring.py::_notify_target exactly."""
    def eval_rule(rule):
        if "And" in rule:
            return all(eval_rule(op) for op in rule["And"])
        var = rule["Variable"].lstrip("$.")
        present = var in data
        if "IsPresent" in rule:
            return present == rule["IsPresent"]
        assert present, f"unguarded dereference of {var} in drill payload {data}"
        return data[var] == rule["BooleanEquals"]

    cur = "CheckShellRunNotify"
    while states[cur]["Type"] in ("Choice", "Pass"):
        if states[cur]["Type"] == "Pass":
            # alpha-engine-config#5950: a normalizer Pass may sit between the
            # gate and its notifier, flooring the optional diagnostic fields the
            # notifier dereferences. It has no Choices, so it cannot change WHICH
            # notifier is reached — walk through it rather than widening the
            # allowed-target list, which would let a future Pass hide a wrong
            # destination from this test.
            cur = states[cur]["Next"]
            continue
        for rule in states[cur]["Choices"]:
            if eval_rule(rule):
                cur = rule["Next"]
                break
        else:
            cur = states[cur]["Default"]
    return cur


_FLAGS = (
    "gate_degraded",
    "health_check_degraded",
    "report_card_degraded",
    "parity_degraded",
    "research_predictor_degraded",
)
_FLAG_COMBOS = [dict(zip(_FLAGS, bits)) for bits in itertools.product([True, False], repeat=len(_FLAGS))]


@pytest.mark.parametrize(
    "combo",
    _FLAG_COMBOS,
    ids=lambda c: (
        ("g" if c["gate_degraded"] else "-")
        + ("h" if c["health_check_degraded"] else "-")
        + ("r" if c["report_card_degraded"] else "-")
        + ("p" if c["parity_degraded"] else "-")
        + ("R" if c["research_predictor_degraded"] else "-")
    ),
)
def test_research_predictor_degraded_never_reaches_plain_notify_complete(states, combo):
    payload = {k: True for k, v in combo.items() if v}
    target = _notify_target(states, payload)
    if combo["research_predictor_degraded"]:
        assert target != "NotifyComplete", (
            f"payload {payload} reached plain NotifyComplete — a "
            "ResearchPredictorParallel internal degradation must always "
            "surface in the terminal notification (weekly-sf-policy.md "
            "§2.3/§2.3a)"
        )
        assert target == "NotifyCompleteMultipleDegraded", (
            f"payload {payload} resolved to {target!r} — "
            "research_predictor_degraded is deliberately folded into the "
            "generic combined notifier ONLY, with no single-flag notifier "
            "of its own."
        )
        subject = states[target]["Parameters"]["Subject"]
        message = states[target]["Parameters"]["Message"]
        assert "research" in message.lower() or "research" in subject.lower()
    else:
        if not any(combo.values()):
            assert target == "NotifyComplete"


def test_multiple_degraded_names_research_predictor(states):
    st = states["NotifyCompleteMultipleDegraded"]
    subject = st["Parameters"]["Subject"]
    message = st["Parameters"]["Message"]
    assert "research_predictor_degraded" in message
    # alpha-engine-config-I7418: this used to assert "SUCCESS" in subject.
    # Since config-I6891 a degraded run routes through CheckDegradedOutcome ->
    # WriteCompletionMarkerDegraded -> DegradedRun, a **Fail** state — so a
    # notifier whose subject leads with SUCCESS states the opposite of the
    # run's own terminal, and the guard was pinning the false claim.
    assert "DEGRADED" in subject
    assert "SUCCESS" not in subject, (
        "a degraded run terminates FAILED (config-I6891); a subject leading "
        "with SUCCESS contradicts the execution's own status"
    )
    assert 0 < len(subject) <= 100


def test_multiple_degraded_still_names_all_prior_families(states):
    """Regression guard: adding the fifth family must not have dropped any
    of the four pre-existing ones from the generic notifier's text."""
    message = states["NotifyCompleteMultipleDegraded"]["Parameters"]["Message"].lower()
    for needle in ("gate", "health check", "report card", "parity"):
        assert needle in message
