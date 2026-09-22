"""A cost fan-in coverage gap ends the weekly run SUCCEEDED, named.

**Brian ruling 2026-09-21**, recorded on ``alpha-engine-config-I11298``, his
words: *"proceed with rec b"*.

(b) as it was put to him: when ``AggregateCosts`` finds a cost-coverage gap,
the weekly run ends SUCCEEDED with a named ``cost_coverage_gap`` sub-status and
its own lower-severity alert. Reason: cost attribution is accounting; it does
not feed the report card, the champion promotion or the signal set, which is
what ``sf-pipeline-policy.md`` §3's fail-hard rule covers. What it trades, as
stated to him: a cost gap no longer forces attention by failing the run, so the
separate alert has to do that job — which is why ``PublishCostCoverageGap`` is
part of the deliverable rather than a follow-up.

**The measured motivation.** The 2026-09-19 SCHEDULED execution
(``3c2fe2f8-…_d0e3eae8-…``) raised exactly this error at 19,450 s of a
19,451 s run — ``TaskFailed`` event 7718, ``error: "CostCoverageError"``,
``missing: ['director-plan']`` — and 5 h 24 m of completed work was recorded as
a failed weekly cycle.

**What this module pins, and the line it will not let move:** a coverage gap
takes the clean terminal; a BROKEN AGGREGATOR does not. The ruling changes what
a gap DOES, never what counts as one.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

_GAP_FLAG = "$.cost_coverage_gap"
_MARK = "MarkCostCoverageGap"
_DETAIL = "SetCostCoverageGapDetail"
_PUBLISH = "PublishCostCoverageGap"

#: The error name the LIVE execution history reports for this condition. Not
#: guessed: read from the 2026-09-19 scheduled run's ``TaskFailed`` event 7718.
_COVERAGE_ERROR = "CostCoverageError"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


# ---------------------------------------------------------------------------
# the split: a gap is not the same event as a broken aggregator
# ---------------------------------------------------------------------------


def test_the_coverage_gap_catch_is_first_and_names_exactly_one_error(states):
    catches = states["AggregateCosts"]["Catch"]
    assert catches[0]["ErrorEquals"] == [_COVERAGE_ERROR]
    assert catches[0]["Next"] == _MARK
    assert catches[0]["ResultPath"] == "$.aggregate_costs_error"


def test_every_other_failure_still_terminates_degraded(states):
    """Do not let "coverage gap" become a catch-all that swallows a broken
    aggregator.

    ``CostCoverageUnmeasured`` is the one the aggregator keeps deliberately
    distinct — a fault in the CHECK, not a finding about the pipeline — and
    ``sf-pipeline-policy.md`` §2.3a rule 2 is why it may not take the clean
    terminal: a missing verdict propagates as ``UNKNOWN``, never as a pass, so
    a check that could not run must not inherit the outcome granted to a check
    that ran and found something.
    """
    catches = states["AggregateCosts"]["Catch"]
    assert catches[-1]["ErrorEquals"] == ["States.ALL"]
    assert catches[-1]["Next"] == "MarkAggregateCostsDegraded"
    assert len(catches) == 2, "a third Catch needs its own reasoning here"
    # And the degraded route is untouched: still flag -> summary -> alert.
    assert states["MarkAggregateCostsDegraded"]["ResultPath"] == "$.aggregate_costs_degraded"
    assert states["SetAggregateCostsDegradedSummary"]["Parameters"]["degraded"] is True


@pytest.mark.parametrize(
    "error_name",
    ["CostCoverageUnmeasured", "CostCaptureStaleError", "Lambda.Unknown", "States.Timeout"],
)
def test_no_other_error_name_reaches_the_clean_route(states, error_name):
    assert error_name not in states["AggregateCosts"]["Catch"][0]["ErrorEquals"]


# ---------------------------------------------------------------------------
# the gap route sets a sub-status and NOT a degraded flag
# ---------------------------------------------------------------------------


def test_the_gap_sets_its_own_flag_and_nothing_else(states):
    mark = states[_MARK]
    assert mark["Type"] == "Pass"
    assert mark["Result"] is True
    assert mark["ResultPath"] == _GAP_FLAG
    assert mark["Next"] == _DETAIL


def test_the_gap_route_never_writes_the_degraded_summary(states):
    """The content of the ruling. ``CheckDegradedOutcome`` reads
    ``$.degraded_summary.degraded``; nothing on this route may set it."""
    # Comments are prose about WHY and legitimately name what the route must
    # not do; the assertion is about the route's BEHAVIOUR, so it reads the
    # states with their Comments stripped.
    blob = json.dumps(
        {n: {k: v for k, v in states[n].items() if k != "Comment"} for n in (_MARK, _DETAIL, _PUBLISH)}
    )
    assert "degraded_summary" not in blob
    assert "aggregate_costs_degraded" not in blob
    for name in (_MARK, _DETAIL, _PUBLISH):
        assert states[name].get("ResultPath") != "$.degraded_summary"


def test_the_detail_carries_the_verdict_without_parsing_it(states):
    """``States.StringToJson`` over PRODUCER BYTES raises ``States.Runtime``,
    which is NOT catchable — not by a Task's ``Catch``, not by an enclosing
    ``Parallel``'s (measured twice live, 2026-09-19). Destructuring the
    aggregator's message here would kill the very run this ruling exists to
    let finish, so the Error and the Cause are copied verbatim and the Cause
    carries ``missing`` / ``observed`` / ``stages_entered`` as the aggregator
    wrote them."""
    params = states[_DETAIL]["Parameters"]
    assert params["gap"] is True
    assert params["error.$"] == "$.aggregate_costs_error.Error"
    assert params["verdict.$"] == "$.aggregate_costs_error.Cause"
    assert states[_DETAIL]["ResultPath"] == "$.cost_coverage_gap_detail"
    assert "StringToJson" not in json.dumps(
        {k: v for k, v in states[_DETAIL].items() if k != "Comment"}
    ), "no intrinsic may parse the aggregator's Cause — States.Runtime from an intrinsic is uncatchable"


def test_the_gap_reaches_the_ordinary_completion_path(states):
    """SUCCEEDED, via the same edge the clean and skip routes take."""
    assert states[_PUBLISH]["Next"] == "CheckShellRunNotify"
    assert states[_PUBLISH]["Catch"][0]["Next"] == "CheckShellRunNotify"


# ---------------------------------------------------------------------------
# the alert the ruling makes load-bearing
# ---------------------------------------------------------------------------


def test_the_notice_exists_and_is_constants_only(states):
    """config#1819: the aggregator's Cause is unbounded producer text and
    ``States.Format``-ing it into an SNS body is forbidden — so the notice
    names the CONDITION and points at ``$.cost_coverage_gap_detail``."""
    publish = states[_PUBLISH]
    assert publish["Resource"] == "arn:aws:states:::sns:publish"
    params = publish["Parameters"]
    assert params["TopicArn.$"] == "$.sns_topic_arn"
    for field in ("Subject", "Message"):
        assert isinstance(params[field], str)
        assert "States.Format" not in params[field]
        assert f"{field}.$" not in params
    assert "cost_coverage_gap_detail" in params["Message"]
    assert publish["Catch"][0]["ErrorEquals"] == ["States.ALL"]


def _playbooks() -> dict:
    import yaml

    return yaml.safe_load(
        (pathlib.Path(__file__).parent.parent / "infrastructure" / "overseer" / "playbooks.yaml").read_text()
    )


def _tracked_only_bus_classes() -> dict:
    """Every DECLARED class routed to the drain by I11332's filter policy.

    DERIVED from ``playbooks.yaml``, never hand-kept — deliverable 3 of
    ``alpha-engine-config-I11332``, and the reason it is written that way: an
    allow list kept in step with a declared set by hand is a defect class this
    fleet hit three times this month.
    """
    return {
        c["class"]: c
        for c in _playbooks()["alert_classes"]
        if c.get("tier") == "tracked-only" and c.get("intake") == "bus"
    }


def _publish_states(states: dict) -> dict:
    return {
        n: st
        for n, st in states.items()
        if st.get("Resource") == "arn:aws:states:::sns:publish"
    }


def test_the_routing_attribute_matches_the_contract_exactly(states):
    """``alpha-engine-config-I11332``, Brian ruling 2026-09-21 ("proceed with
    rec b").

    The question put to him: this notice publishes to ``$.sns_topic_arn``,
    which on a scheduled run is ``alpha-engine-alerts-muted`` — ZERO
    subscriptions — so it reaches nobody, which is the unmet CONDITION of his
    I11298 ruling (a cost gap no longer fails the run, so the alert has to do
    that job). He ruled (b): the drain subscribes to that topic with a FILTER
    POLICY admitting only declared tracked-only classes, rather than
    wholesale — because the same topic carries the pipeline's 28 per-stage
    notices, and a wholesale subscription is the flood he ruled against on
    2026-08-21.

    The name, DataType and value are FIXED by that issue so this half and the
    ``nous-ergon-ops`` subscription half cannot drift apart. This test is the
    nousergon-data side of that contract.
    """
    attrs = states[_PUBLISH]["Parameters"]["MessageAttributes"]
    assert attrs == {
        "alert_class": {
            "DataType": "String",
            "StringValue": "weekly_cost_coverage_gap",
        }
    }


def test_every_tracked_only_bus_class_carries_its_routing_attribute(states):
    """The property, over the DERIVED set.

    A class declared ``tier: tracked-only`` + ``intake: bus`` is one the
    filter policy admits; if its publish state carries no ``alert_class``
    attribute the message is filtered OUT and the class is a control that
    emits to nobody — the exact condition I11332 exists to end.
    """
    declared = _tracked_only_bus_classes()
    assert declared, "no tracked-only/bus class is declared — the derivation broke"
    publishers = _publish_states(states)
    for class_id, row in declared.items():
        state_name = row["source"].rsplit("::", 1)[-1]
        if state_name not in publishers:
            # The class is emitted from somewhere other than this definition
            # (a Lambda, a script). Not this test's business.
            continue
        attrs = publishers[state_name].get("Parameters", {}).get("MessageAttributes") or {}
        assert attrs.get("alert_class", {}).get("StringValue") == class_id, (
            f"{state_name} declares alert class {class_id!r} as tracked-only/bus "
            f"but carries no matching alert_class attribute — the drain's filter "
            f"policy would drop it (alpha-engine-config-I11332)"
        )
        assert attrs["alert_class"]["DataType"] == "String"


def test_no_per_stage_notice_carries_a_routing_attribute(states):
    """The other half, and the one the build-window mute depends on.

    ``alpha-engine-config-I9751`` item 3a muted the v1 pipeline's per-stage
    notices deliberately. An ``alert_class`` attribute on one of those would
    walk it straight through the filter policy and into the drain — Brian's
    2026-08-21 flood, re-created by an attribute nobody thought of as a
    routing decision.
    """
    declared = _tracked_only_bus_classes()
    routed_states = {row["source"].rsplit("::", 1)[-1] for row in declared.values()}
    offenders = []
    for name, st in _publish_states(states).items():
        if name in routed_states:
            continue
        attrs = st.get("Parameters", {}).get("MessageAttributes") or {}
        if attrs:
            offenders.append(name)
    assert not offenders, (
        f"{offenders} carry MessageAttributes but declare no tracked-only/bus "
        f"alert class — they would pass the drain's filter policy and re-create "
        f"the per-stage flood the build-window mute exists to prevent"
    )


def test_the_notice_declares_an_alert_class(states):
    """observability-policy.md §7.4: a notify-only class declares its
    remediation path. The row lives in this repo, which is what the
    ``alert-class-pr-guard`` workflow grades."""
    import yaml

    playbooks = yaml.safe_load(
        (pathlib.Path(__file__).parent.parent / "infrastructure" / "overseer" / "playbooks.yaml").read_text()
    )
    rows = {c["class"]: c for c in playbooks["alert_classes"]}
    row = rows["weekly_cost_coverage_gap"]
    assert _PUBLISH in row["source"]
    # §7.2: a page is for what is actionable NOW; an accounting gap is not.
    assert row["severities"] == ["warning"]
    assert row["tier"] == "tracked-only"
    assert row["intake"] == "bus"


# ---------------------------------------------------------------------------
# the sub-status is on every surface that reports the run
# ---------------------------------------------------------------------------


def test_the_gap_is_named_on_every_completion_marker(states):
    for marker in (
        "WriteCompletionMarker",
        "WriteCompletionMarkerDegraded",
        "WriteCompletionMarkerCalendar",
        "WriteCompletionMarkerDegradedCalendar",
    ):
        body = states[marker]["Parameters"]["Body.$"]
        assert '"cost_coverage_gap":{}' in body, marker
        assert "States.JsonToString($.cost_coverage_gap)" in body, marker


def test_the_gap_flag_is_floored_so_a_clean_run_never_throws(states):
    floor = states["InitializeInput"]["Parameters"]["merged.$"]
    assert '"cost_coverage_gap":false' in floor.replace("\\", "")


def test_director_plan_is_still_required(states):
    """The ruling changes what a gap DOES, not what counts as a gap."""
    coverage = states["AggregateCosts"]["Parameters"]["Payload"]["coverage"]
    assert coverage["required_producers"]["Director"] == ["director-plan"]
    assert "director-plan" not in json.dumps(coverage["conditional_producers"])
    assert "director-plan" not in json.dumps(coverage["allowed_producers"])
