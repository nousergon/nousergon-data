"""alpha-engine-config-I11299 — a Director that reports `degraded` must not
terminate the weekly SF at plain ``NotifyComplete``.

``sf-pipeline-policy.md`` §2.3b, clause
``SFP-2.3b-stage-status-is-the-worst-substatus``: *a stage that returns named
sub-results returns a status no better than the worst of them; a sub-result
reporting an error is a stage degradation, and the run does not terminate as a
clean success.*

**Pre-fix, MEASURED** on three of the four 2026-09-19 executions that entered
``Director`` (read from their execution histories 2026-09-21) —
``3c2fe2f8-…_d0e3eae8-…`` (the scheduled run), ``watch-rerun-2026-09-18-1`` and
``watch-rerun-2026-09-18-3``::

    director_result.Payload.status = "ok"
    director_result.Payload.retro  = "error"   # a self-grading REFUSAL

The last two terminated ``ExecutionSucceeded`` with
``degraded_summary.degraded: false``. ``Director``'s success edge went straight
to ``DirectorComplete``, so no state in this definition ever read below the
enclosing status, and the weekly cycle published no RetroGrade for at least two
consecutive cycles with nothing anywhere going red. The refusal itself is
CORRECT and its root cause is tracked separately
(``alpha-engine-config-I8202``); this module is only about the state machine
being able to see it.

**The shape pinned here**, mirroring the ``aggregate_costs_degraded`` family
(``alpha-engine-config-I7194``) exactly:

1. ``Director``'s success edge routes to ``CheckDirectorSubResults``, a Choice
   that fires on exactly one string.
2. That Choice sets ``$.director_degraded`` via ``DirectorSubStatusDegraded``,
   then the sibling summary Pass writes ``$.degraded_summary``, then
   ``DirectorComplete`` — the same edge the clean path takes.
3. ``$.director_degraded`` threads into ``CheckGateDegradedNotify`` as the
   seventh family, registered LAST, so a run whose ONLY degradation is the
   Director cannot reach ``NotifyComplete``'s "All steps completed
   successfully" while terminating in ``DegradedRun``.
4. Every path still converges on ``DirectorComplete``, the success-only
   witness ``scripts/weekly_sf_rerun.py`` derives the completed set from — a
   Director that degraded still COMPLETED and must not be re-run.
"""
from __future__ import annotations

import itertools
import json
import pathlib

import pytest

from tests.sf_degraded_summary_helpers import (
    assert_completion_notifier_chain,
    assert_degraded_continuation,
    notify_target,
)

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

_CHOICE = "CheckDirectorSubResults"
_SETTER = "DirectorSubStatusDegraded"
_FLAG = "$.director_degraded"

#: The status string the Choice fires on, and the ONLY one. It is the literal
#: `director.substatus.STAGE_DEGRADED` emits in crucible-evaluator; the two are
#: a cross-repo contract and this test is the nousergon-data half of it.
_DEGRADED = "degraded"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


# ---------------------------------------------------------------------------
# 1. Director's success edge now reads its own sub-results
# ---------------------------------------------------------------------------


def test_director_success_edge_goes_through_the_substatus_choice(states):
    assert states["Director"]["Next"] == _CHOICE


def test_the_director_catch_is_untouched(states):
    """config#6408 (Brian's 2026-08-04 operator ruling) is a SEPARATE rule: a
    Director that FAILED is terminal. This clause is about a Director that
    SUCCEEDED while one of its legs did not, and must not move the other."""
    catch = states["Director"]["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "NormalizeFailureContext"


def test_the_choice_is_ispresent_guarded_and_fires_on_one_string(states):
    """config#2275: a Choice dereferencing a path that can be absent guards it
    with IsPresent first. ``$.director_result`` is written by the IMMEDIATE
    predecessor's ResultPath, which is the one-hop floor
    ``tests/test_sf_choice_guards.py`` traces."""
    choice = states[_CHOICE]
    assert choice["Type"] == "Choice"
    assert len(choice["Choices"]) == 1
    rule = choice["Choices"][0]["And"]
    assert rule[0] == {"Variable": "$.director_result.Payload.status", "IsPresent": True}
    assert rule[1] == {
        "Variable": "$.director_result.Payload.status",
        "StringEquals": _DEGRADED,
    }
    assert choice["Choices"][0]["Next"] == _SETTER
    assert choice["Default"] == "DirectorComplete"


@pytest.mark.parametrize(
    "status", ["ok", "disabled", "dry_run", "DEGRADED", "degraded_somewhat", ""]
)
def test_only_the_exact_string_degrades_the_run(states, status):
    """A Director on an OLDER image returns ``ok`` and takes the Default, so
    the edge is inert rather than broken and the two repos may land in either
    order. Every other status likewise."""
    rule = states[_CHOICE]["Choices"][0]["And"][1]
    assert (status == rule["StringEquals"]) is (status == _DEGRADED)


# ---------------------------------------------------------------------------
# 2. flag -> summary -> named alert -> the same edge the clean path takes
# ---------------------------------------------------------------------------


def test_the_flag_is_set_before_the_summary(states):
    setter = states[_SETTER]
    assert setter["Type"] == "Pass"
    assert setter["Result"] is True
    assert setter["ResultPath"] == _FLAG


def test_the_summary_chain_reaches_the_terminal_the_same_way_its_siblings_do(states):
    """I6891's Option-A chokepoint: a fail-open route added later inherits the
    honest terminal by writing ``$.degraded_summary`` and touching
    ``CheckDegradedOutcome`` not at all."""
    summary = assert_degraded_continuation(states, _SETTER, "DirectorComplete")
    params = states[summary]["Parameters"]
    assert params["degraded"] is True
    assert params["reason"] == "weekly_director_substatus_degraded"
    # I10540: ACCUMULATE a family-keyed entry, never replace the whole object.
    assert params["reasons.$"].startswith("States.JsonMerge($.degraded_summary.reasons")
    assert '"director"' in params["reasons.$"]
    # The terminal dereferences `reason` unguarded and every sibling setter is
    # reachable from a Choice, which writes no $.<stage>_error — so carrying a
    # stage_error.$ here would throw States.Runtime on exactly this path.
    assert "stage_error.$" not in params


def test_the_degraded_path_converges_on_director_complete(states):
    """Both routes descend from Director's ONE success edge, which is the
    property the rerun deriver's witness asserts. A Director that degraded
    still wrote its plan and must not be re-run."""
    assert states["Set" + _SETTER + "Summary"]["Next"] == "DirectorComplete"
    assert states[_CHOICE]["Default"] == "DirectorComplete"
    assert states["DirectorComplete"]["Next"] == "CheckSkipScannerLeaderboard"


def test_there_is_no_named_sns_alert_for_this_family_yet(states):
    """A deliberate, tracked staging — not an oversight.

    ``PublishAggregateCostsDegraded``'s shape (alpha-engine-config-I8336) is
    the right one for this family too: registered LAST in
    ``CheckGateDegradedNotify``, ``$.director_degraded`` folds into
    ``NotifyCompleteMultipleDegraded``, which deliberately names no specific
    family, so the operator reads "two or more families degraded, check the
    execution record". The named alert is a new SUBSTANTIVE Task state, and
    ``tests/test_pipeline_status_registry_source_check.py`` requires every one
    of those to carry a ``nousergon_lib.pipeline_status.registry`` entry
    merged and PINNED in the same PR — a three-repo lockstep against a
    merge-time-autobumped lib version, which cannot be sequenced inside the
    2026-09-26 window this change is for. The honest terminal, the family
    flag and the named ``degraded_summary.reasons.director`` entry all land
    here and are what I11299's Closes-when asks for; the alert is the
    refinement. This test exists so adding it later is a deliberate act with
    a red line to delete, not a silent widening. Tracked:
    ``alpha-engine-config-I11322``.
    """
    assert "PublishDirectorSubStatusDegraded" not in states


# ---------------------------------------------------------------------------
# 3. the seventh family reaches the completion notifier
# ---------------------------------------------------------------------------


def test_director_degraded_is_registered_last_in_the_notify_choice(states):
    """LAST on purpose: a run that also degraded something more consequential
    gets that notification, and ``PublishDirectorSubStatusDegraded`` has
    already named this one."""
    rules = states["CheckGateDegradedNotify"]["Choices"]
    last = rules[-1]
    assert last["And"][0]["Variable"] == _FLAG
    assert last["And"][1] == {"Variable": _FLAG, "BooleanEquals": True}
    assert last["Next"] == "NormalizeMultipleDegradedContext"
    # And nowhere else, so there is exactly one rule to reason about.
    assert sum(1 for r in rules if r["And"][0]["Variable"] == _FLAG) == 1


_OTHER_FAMILIES = (
    "$.gate_degraded",
    "$.health_check_degraded",
    "$.report_card_degraded",
    "$.parity_degraded",
    "$.research_predictor_degraded",
    "$.scanner_leaderboard_degraded",
    "$.aggregate_costs_degraded",
)


@pytest.mark.parametrize(
    "others", list(itertools.product([False, True], repeat=len(_OTHER_FAMILIES)))
)
def test_a_director_degraded_run_never_reaches_plain_notify_complete(states, others):
    """The hard invariant, over every combination of the other seven families.

    This is the assertion whose absence let two ``ExecutionSucceeded`` runs
    report ``degraded: false`` over a refused RetroGrade.
    """
    payload = {"director_degraded": True}
    # A Choice payload carries only the flags actually SET — an absent flag is
    # IsPresent-guarded, never present-as-false.
    payload.update(
        {
            var.removeprefix("$."): True
            for var, value in zip(_OTHER_FAMILIES, others)
            if value
        }
    )
    notifier = notify_target(states, payload)
    assert notifier != "NotifyComplete", (
        f"payload {payload} reached plain NotifyComplete while terminating in "
        "DegradedRun — the exact shape of the two 2026-09-19 reruns that "
        "reported degraded: false over a refused RetroGrade"
    )
    # And the notifier it DOES reach converges on the honest terminal.
    assert_completion_notifier_chain(states, notifier)


def test_a_clean_run_still_reaches_notify_complete(states):
    """The rule must not widen: absent flag = never degraded."""
    assert notify_target(states, {}) == "NotifyComplete"
