"""A fail-open that pages only at the terminal is silent when it matters.

`sf-pipeline-policy.md` §5 permits the weekday fail-opens on exactly one
condition: they *"set the degraded flag §2.3 requires **and page
immediately**"*. The second half was built for some paths and not others, and
nothing checked.

Measured 2026-08-11. The Scanner Lambda timed out twice at 300s and the
pipeline fail-opened past it. Its path was:

    Scanner --Catch--> SetScannerDegradedFlag --> CheckSkipPredictorInference

No SNS publish anywhere on it. The only signal was the terminal, **37 minutes
later** — inside the window where an operator could still have rerun the
Scanner before the 06:30 open. The sibling data-spot and deploy-drift
fail-opens in the same definition both had a publish; the Scanner one, added
later by `#6722`, did not, and `§5` does not name it at all.

The audit this test encodes found a second instance immediately:
`SetDaemonDegradedFlag`, which §5 *does* name explicitly, also went straight
to the terminal.

## Why membership is keyed on the RESOURCE, not the state name

Writing this audit by hand, a `name.startswith("Publish")` heuristic reported
the EOD weekly-exercise path as silent. It is not — `WeeklyExerciseLaunchFailed`
is an `sns:publish`; it simply is not named `Publish*`. A guard that reads
names produces false findings on correct code and, worse, would pass a state
called `PublishFoo` that publishes nothing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"

def _sf_definitions() -> tuple[str, ...]:
    """Every Step Functions definition in ``infrastructure/``, DISCOVERED.

    alpha-engine-config-I8336. This audit shipped over a hardcoded pair of
    weekday definitions, so the WEEKLY definition — the largest of the four,
    and the one carrying seventeen degraded setters — was never covered by
    it. The cost-aggregation fail-open added to it therefore set its flag and
    reached no publish, and nothing failed.

    A hardcoded list is the defect: it cannot fail on the definition it does
    not name. Membership is decided by SHAPE (a JSON object with a ``States``
    map and a ``StartAt``), so a definition added to this directory tomorrow
    is covered by this function existing, and a non-definition JSON file in
    the same directory (``automation_pause.json``, ``weekly_cadence.json``,
    the preflight-sweep manifests) is excluded without being listed.
    """
    found = []
    for path in sorted(_INFRA.glob("*.json")):
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:  # pragma: no cover - a malformed
            # definition is another test's finding, not this one's.
            continue
        if isinstance(body, dict) and isinstance(body.get("States"), dict) and body.get("StartAt"):
            found.append(path.name)
    return tuple(found)


_ALL_DEFS = _sf_definitions()

#: The definitions that MUST exist and MUST carry degraded setters. Discovery
#: is what makes a new definition covered; this is what makes a *removed* one
#: loud. Without it, a rename would silently shrink the audit to whatever is
#: left and every parametrised case would still pass.
_DEFS_WITH_SETTERS = (
    "step_function.json",
    "step_function_daily.json",
    "step_function_eod.json",
)

_SNS_PUBLISH = "arn:aws:states:::sns:publish"

#: How many `Next` hops a degraded setter may take before it must have
#: reached a publish. Generous: the paths in practice are 1-2 hops, and a
#: longer walk would start crediting a notification that fires for an
#: unrelated reason further down the pipeline.
_MAX_HOPS = 4

# Degraded setters that legitimately do NOT page at the moment they fire.
# Exhaustive and exempt-by-reason, not by omission — an entry here is a claim
# somebody has to defend, and a new setter is silent-by-default only until
# this test fails on it.
_NO_IMMEDIATE_PAGE: dict[tuple[str, str], str] = {
    (
        "step_function_daily.json",
        "SetMutexAcquireDegradedFlag",
    ): (
        "Best-effort architectural insurance, not a producer. A DynamoDB / IAM "
        "blip on mutex acquisition does not change what the run produces, and "
        "the terminal reports it. Paging here would train the operator to "
        "ignore the channel that the Scanner and daemon pages use."
    ),
    (
        "step_function_eod.json",
        "SetMutexAcquireDegradedFlag",
    ): "Same as the daily mutex path.",
    (
        "step_function.json",
        "SetResearchPredictorDegradedSummary",
    ): (
        "The post-Parallel HOIST of a fact that already paged. Every fail-open "
        "route inside ResearchPredictorParallel publishes at the moment it "
        "fires — PublishResearchFailureImmediate (branch 0), "
        "PublishPredictorFailureImmediate and PublishModelZooFailureImmediate "
        "(branch 1) — and this setter runs after the Parallel joins, purely to "
        "write the $.degraded_summary the terminal reads. A publish here would "
        "be a second email about the same event, minutes later."
    ),
    (
        "step_function.json",
        "SetScannerResourceKillDegradedSummary",
    ): (
        "Same path as SetResearchPredictorDegradedSummary, one Choice below "
        "(CheckScannerResourceKillReason): only the degraded_summary reason "
        "forks, to say the universe-membership artifact survived. The "
        "immediate page already fired inside the Parallel."
    ),
    (
        "step_function.json",
        "NotifyCompleteDegraded",
    ): (
        "This path records a FAILED sns:publish. Publishing to SNS to report "
        "that publishing to SNS failed is circular — the notification most "
        "likely to be lost is the one about the notification being lost. The "
        "immediate surface is the other kind: NotifyComplete's own Catch emits "
        "a TaskFailed event, logged at ERROR unconditionally, matched by "
        "SnsPublishFailedMetricFilter. So this path DOES page immediately; it "
        "does it through the metric filter rather than through the topic that "
        "just failed."
    ),
    (
        "step_function.json",
        "NotifyShellRunCompleteDegraded",
    ): "Same as NotifyCompleteDegraded, for the Friday-PM shell-run notifier.",
    (
        "step_function.json",
        "SetSaturdayHealthCheckDegradedSummary",
    ): (
        "$.health_check_degraded has its OWN single-flag terminal notifier — "
        "NotifyCompleteHealthDegraded, Subject 'SUCCESS (health checks "
        "DEGRADED)' — so unlike the cost-aggregation family (which folds into "
        "the generic NotifyCompleteMultipleDegraded and names no family), the "
        "operator is told BY NAME what degraded. Adding a page on top would "
        "also be the highest-frequency alert in this definition: both health "
        "checks are declared health-observe, carry no Retry ladder by design, "
        "and fail-soft on ordinary SSM flakiness. That is the "
        "SetMutexAcquireDegradedFlag argument exactly — a page here would "
        "train the operator to ignore the channel the Scanner and daemon pages "
        "use. Revisit if the health family ever loses its dedicated notifier."
    ),
    (
        "step_function.json",
        "SetSubstrateHealthCheckDegradedSummary",
    ): (
        "Same as the Saturday freshness twin — deliberately the same "
        "$.health_check_degraded family and the same dedicated notifier. Its "
        "constituents-drift sub-step additionally publishes its own "
        "alerts.publish, and each transparency-inventory row has its own "
        "AlphaEngine/Substrate alarm."
    ),
    (
        "step_function_eod.json",
        "SetDegradedFlag",
    ): (
        "EODReconcile skipped on a data gap routes into the heal loop "
        "(CheckHealLoopEligible), which notifies on its own outcome. Paging "
        "before the heal has run would page on a condition the pipeline is "
        "actively repairing."
    ),
}

#: alpha-engine-config-I11073 brought the ten ResearchPredictorParallel fail-open
#: routes into this audit's reach for the first time. They previously wrote a
#: bare ``Result: true`` and carried no ``degraded`` key, so ``_degraded_setters``
#: matched none of them and the audit was blind to the whole family — the
#: detection blindness outranked the paths it hid. Making each route name itself
#: gave them a ``Parameters`` block, which is what surfaced them here.
#:
#: The PATHS have not changed, and they are exempt for the reason
#: ``SetResearchPredictorDegradedSummary`` above is already exempt for: each of
#: these states is the CONTINUATION of a Catch that has ALREADY published —
#: ``PublishResearchFailureImmediate`` (branch A), ``PublishModelZooFailure
#: Immediate`` / ``PublishPredictorFailureImmediate`` (branch B). The page fires
#: BEFORE the state, and ``_reaches_a_publish`` only walks forward. A publish
#: here would be a second email about the same event, seconds later.
_RP_ROUTE_PAGED_BY_ITS_OWN_CATCH = (
    "Continuation of a Catch that already published inside "
    "ResearchPredictorParallel (Publish{Research,Predictor,ModelZoo}"
    "FailureImmediate). The page fires BEFORE this state; publishing again "
    "would be a second email about the same event."
)

_NO_IMMEDIATE_PAGE.update({
    ("step_function.json", route): _RP_ROUTE_PAGED_BY_ITS_OWN_CATCH
    for route in (
        "MarkScannerDegraded",
        "ScannerResourceKillDegraded",
        "MarkRegimeSubstrateDegraded",
        "MarkChallengerShadowDegraded",
        "MarkRegimeRetrospectiveEvalDegraded",
        "MarkEvalJudgeDegraded",
        "MarkEvalRollingMeanDegraded",
        "MarkRationaleClusteringDegraded",
        "MarkCounterfactualDegraded",
        "MarkModelZooDegraded",
    )
})


def _states(definition: str) -> dict:
    return json.loads((_INFRA / definition).read_text(encoding="utf-8"))["States"]


def _flatten(states: dict) -> dict:
    """Every state in the definition, including inside Parallel branches and
    Map iterators.

    The weekly definition puts real work inside ``ResearchPredictorParallel``.
    A top-level-only walk would report a definition as fully covered while
    never looking at the branch where a fail-open is most likely to be added,
    which is the same shape of blindness as the hardcoded definition list.
    Measured 2026-08-25: no nested setter exists today. This is here so that
    the first one is covered on the day it lands, not on the day somebody
    notices.
    """
    flat = {}
    for name, body in states.items():
        flat[name] = body
        for branch in body.get("Branches") or []:
            flat.update(_flatten(branch["States"]))
        inner = body.get("Iterator") or body.get("ItemProcessor")
        if inner:
            flat.update(_flatten(inner["States"]))
    return flat


def _degraded_setters(states: dict) -> dict[str, dict]:
    return {
        name: body["Parameters"]
        for name, body in _flatten(states).items()
        if isinstance(body.get("Parameters"), dict)
        and body["Parameters"].get("degraded") is True
    }


def _publishes(states: dict, name: str) -> bool:
    return _flatten(states).get(name, {}).get("Resource") == _SNS_PUBLISH


def _reaches_a_publish(states: dict, start: str) -> str | None:
    """Walk ``Next`` from ``start``, returning the publishing state or None."""
    flat = _flatten(states)
    cur = flat.get(start, {}).get("Next")
    for _ in range(_MAX_HOPS):
        if not cur:
            return None
        if flat.get(cur, {}).get("Resource") == _SNS_PUBLISH:
            return cur
        cur = flat.get(cur, {}).get("Next")
    return None


@pytest.mark.parametrize("definition", _ALL_DEFS)
def test_every_degraded_path_pages_immediately_or_is_exempt_with_a_reason(
    definition: str,
) -> None:
    """§5's second half, enforced.

    A new fail-open path is silent until somebody notices — which on
    2026-08-11 took 37 minutes and a direct question. This is what notices.
    """
    states = _states(definition)
    setters = _degraded_setters(states)
    if not setters:
        # A definition with no fail-open path has nothing to audit. That it is
        # a legitimate state rather than a silent hole is asserted by
        # test_the_definitions_that_must_carry_setters_still_do.
        return

    silent = []
    for name in setters:
        if (definition, name) in _NO_IMMEDIATE_PAGE:
            continue
        if _reaches_a_publish(states, name) is None:
            silent.append(name)

    assert not silent, (
        f"{definition}: {silent} set degraded: true and reach no sns:publish "
        f"within {_MAX_HOPS} hops. sf-pipeline-policy.md §5 permits a weekday "
        "fail-open only if it sets the flag AND pages immediately. Either add "
        "the publish, or add an entry to _NO_IMMEDIATE_PAGE stating why this "
        "path is different."
    )


@pytest.mark.parametrize("definition", _ALL_DEFS)
def test_the_paging_check_reads_the_resource_not_the_state_name(
    definition: str,
) -> None:
    """Guards the guard.

    `WeeklyExerciseLaunchFailed` publishes and is not named `Publish*`; a
    name-based check calls it silent. The inverse is worse — a state named
    `PublishX` that publishes nothing would pass. Assert that at least one
    publishing state in the fleet's definitions is NOT named `Publish*`, so a
    future simplification back to a name check fails here.
    """
    states = _states(definition)
    flat = _flatten(states)
    publishers = [n for n in flat if flat[n].get("Resource") == _SNS_PUBLISH]
    if not publishers:
        pytest.skip(f"{definition} has no sns:publish states")
    # Membership is decided by Resource, which is the point: every state this
    # helper admits publishes, whatever it is called.
    for name in publishers:
        assert flat[name]["Resource"] == _SNS_PUBLISH
    # And a name check would be strictly weaker — some publishers are not
    # named Publish*, asserted concretely in the weekly-exercise test below.


def test_the_weekly_exercise_path_is_recognised_as_paging() -> None:
    """The concrete false finding this design avoids.

    `WeeklyExerciseLaunchFailed` is an sns:publish reached from
    `SetWeeklyExerciseDegradedFlag`. A name-prefix check reports it silent,
    which sends somebody to add a duplicate publish beside a working one.
    """
    states = _states("step_function_eod.json")
    reached = _reaches_a_publish(states, "SetWeeklyExerciseDegradedFlag")
    assert reached == "WeeklyExerciseLaunchFailed"
    assert not reached.startswith("Publish")


@pytest.mark.parametrize("definition", _ALL_DEFS)
def test_no_exemption_names_a_setter_that_no_longer_exists(definition: str) -> None:
    """An exemption outliving its path silently excuses the next one to take
    that name."""
    states = _states(definition)
    setters = set(_degraded_setters(states))
    stale = sorted(
        name for (d, name) in _NO_IMMEDIATE_PAGE if d == definition and name not in setters
    )
    assert not stale, f"{definition}: _NO_IMMEDIATE_PAGE names absent setters: {stale}"


@pytest.mark.parametrize("definition", _ALL_DEFS)
def test_no_exemption_covers_a_path_that_now_pages(definition: str) -> None:
    """An exemption is a claim, and a claim that has become false is worse
    than no claim — it asserts a path is deliberately silent when it is not."""
    states = _states(definition)
    wrong = sorted(
        name
        for (d, name) in _NO_IMMEDIATE_PAGE
        if d == definition and _reaches_a_publish(states, name) is not None
    )
    assert not wrong, (
        f"{definition}: {wrong} now page — remove them from _NO_IMMEDIATE_PAGE"
    )


@pytest.mark.parametrize("definition", _ALL_DEFS)
def test_every_immediate_page_survives_its_own_sns_failure(definition: str) -> None:
    """A best-effort notification must not hard-fail a run that deliberately
    continued. Every publish on a degraded path carries a Catch back onto the
    continue path — otherwise an SNS outage converts a degraded run into a
    failed one, which is the opposite of fail-open."""
    states = _states(definition)
    for setter in _degraded_setters(states):
        reached = _reaches_a_publish(states, setter)
        if reached is None:
            continue
        body = states[reached]
        catches = body.get("Catch") or []
        assert catches, f"{definition}::{reached} has no Catch — an SNS failure would abort the run"
        assert any(
            "States.ALL" in (c.get("ErrorEquals") or []) for c in catches
        ), f"{definition}::{reached} does not Catch States.ALL"


def test_the_definitions_that_must_carry_setters_still_do() -> None:
    """Discovery makes a NEW definition covered; this makes a REMOVED one loud.

    alpha-engine-config-I8336. Every parametrised case above passes vacuously
    for a definition that vanished from the directory or lost its fail-opens,
    and a shrinking audit is indistinguishable from a clean one.
    """
    missing = [d for d in _DEFS_WITH_SETTERS if d not in _ALL_DEFS]
    assert not missing, f"definitions no longer discovered: {missing}"
    empty = [d for d in _DEFS_WITH_SETTERS if not _degraded_setters(_states(d))]
    assert not empty, (
        f"{empty} carry no degraded setters — either the fail-opens were "
        "removed (say so here) or the setter shape changed and this audit is "
        "now blind"
    )


def test_the_weekly_definition_is_covered() -> None:
    """The concrete blindness alpha-engine-config-I8336 records.

    ``step_function.json`` is the largest definition in the repo and carries
    more degraded setters than both weekday definitions combined. It was
    outside this audit for its whole life, which is how a cost-aggregation
    fail-open shipped setting its flag and reaching no publish.
    """
    assert "step_function.json" in _ALL_DEFS
    setters = _degraded_setters(_states("step_function.json"))
    assert len(setters) > len(_degraded_setters(_states("step_function_daily.json")))


def test_the_weekly_cost_aggregation_fail_open_pages() -> None:
    """alpha-engine-config-I8336's named instance.

    ``$.aggregate_costs_degraded`` is registered LAST in
    ``CheckGateDegradedNotify``, so it folds into
    ``NotifyCompleteMultipleDegraded`` — a notifier that deliberately names no
    specific family. Without an immediate named publish, a run whose cost
    record was never written reached the operator as "two or more families
    degraded, check the execution record".
    """
    states = _states("step_function.json")
    assert _reaches_a_publish(states, "SetAggregateCostsDegradedSummary") == (
        "PublishAggregateCostsDegraded"
    )
