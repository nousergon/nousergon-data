"""alpha-engine-config-I7812 — a Scanner resource kill must be NAMED as one, and
its fail-open must be CONDITIONED on the universe-membership artifact.

sf-pipeline-policy.md §3 (SFP-3-resource-kill-halts-and-is-named, Brian's
2026-08-13 ruling) sets three obligations for an OOM or a timeout. Pre-fix, the
weekly SF's ``Scanner`` state met none of them cleanly:

1. **Halt.** ``Scanner``'s only ``Catch`` was ``States.ALL`` → ``MarkScannerDegraded``,
   so a 440s kill folded into the generic Branch-A fail-open and the run
   continued as though the scan had merely errored.
2. **Never auto-retry an unchanged kill.** ``Scanner``'s retrier listed
   ``States.TaskFailed``, which matches a ``Lambda.Unknown`` runtime exit — i.e.
   an OOM or a ``Sandbox.Timedout`` — so a kill was replayed once against the
   same workload on the same substrate.
3. **Name it.** Nothing on either the degraded or the failed surface said
   "timeout": the degraded reason read ``weekly_research_predictor_branch_fail_open``
   whether the scan crashed on a domain precondition or was killed at its budget.

The worked example is the 2026-08-20 kill this issue was opened for:
``universe_membership`` wrote at 08:48:05 and the invocation died at the 450s
ceiling afterwards, so the day's LOAD-BEARING artifact was intact while
``research/cuts_leaderboard/2026-08-20.json`` was never written. A run killed
*before* the membership write is a materially different run — the predictor
would resolve tomorrow's scoring universe from a frozen population
(alpha-engine-config-I4818) — and the two must not share an outcome.

So the fix is a kill-specific ``Catch`` that probes
``s3://alpha-engine-research/universe_membership/latest.json`` (the pointer,
written LAST of the three keys, and immune to the trading-day-vs-calendar-date
gap ``$.run_date`` cannot close) and then:

* pointer fresh for this ``run_date``  → fail-open, DEGRADED, reason
  ``weekly_scanner_resource_kill_membership_intact``;
* pointer stale / absent               → HALT via Branch A's own chokepoint;
* probe unmeasurable                   → HALT, with a distinct Error name
  (§2.3a rule 2: UNKNOWN is never a pass).

``ScannerLeaderboard`` — the same Lambda under the same 440s budget — gets the
naming half of the same fix (it has no partial-success condition: the board is
its whole deliverable).

No live weekly execution was triggered for this coverage. As in
``test_sf_parity_resource_kill_halt_i7267.py``, the fixture-based substitute
below walks the ACTUAL ``Next`` / ``Default`` / ``Choices`` edges in
``infrastructure/step_function.json`` — the same state-name transitions a real
``get-execution-history`` call would show.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

#: The two error names a Lambda-backed stage is killed under: the SF budget
#: binding (States.Timeout) and the function's own runtime exit — an OOM or a
#: Sandbox.Timedout — which surfaces as Lambda.Unknown.
KILL_ERRORS = ["States.Timeout", "Lambda.Unknown"]


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_WEEKLY.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


@pytest.fixture(scope="module")
def branch_a(states) -> dict:
    return states["ResearchPredictorParallel"]["Branches"][0]["States"]


def _catch_for(state: dict, errors: list[str]) -> dict:
    matches = [c for c in state.get("Catch", []) if c["ErrorEquals"] == errors]
    assert len(matches) == 1, f"expected exactly one Catch on {errors}, found {len(matches)}"
    return matches[0]


# ---------------------------------------------------------------------------
# Obligation 2 — never auto-retry an unchanged kill.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stage", ["Scanner", "ScannerLeaderboard"])
def test_a_resource_kill_is_never_retried(states, branch_a, stage):
    """A zero-attempt retrier for the kill errors must sit AHEAD of the broad
    States.TaskFailed retrier. ASL evaluates retriers in order and
    States.TaskFailed matches Lambda.Unknown, so without the exclusion a kill is
    replayed once — the same workload on the same substrate, which is how one
    loud failure becomes a quiet loop."""
    state = (branch_a if stage == "Scanner" else states)[stage]
    retry = state["Retry"]
    assert retry[0]["ErrorEquals"] == KILL_ERRORS
    assert retry[0]["MaxAttempts"] == 0
    broad = [i for i, r in enumerate(retry) if "States.TaskFailed" in r["ErrorEquals"]]
    assert broad and min(broad) > 0, (
        "the States.TaskFailed retrier must come AFTER the zero-attempt kill "
        "exclusion, or the exclusion never fires"
    )


# ---------------------------------------------------------------------------
# Scanner — the conditional fail-open.
# ---------------------------------------------------------------------------


def test_a_kill_does_not_enter_the_generic_fail_open(branch_a):
    """The kill Catch must be evaluated BEFORE the States.ALL fail-open, and
    must not land on MarkScannerDegraded."""
    catches = branch_a["Scanner"]["Catch"]
    assert [c["ErrorEquals"] for c in catches] == [KILL_ERRORS, ["States.ALL"]]
    assert catches[0]["Next"] == "ScannerResourceKill"
    assert catches[1]["Next"] == "MarkScannerDegraded"
    # Both write the same path, so $.scanner_error is present on either route
    # and the halt states can carry it without a guarded read.
    assert all(c["ResultPath"] == "$.scanner_error" for c in catches)


def test_the_probe_reads_the_membership_pointer_not_a_date_derived_key(branch_a):
    """The dated key is keyed on the TRADING DAY (scanner_handler normalizes
    through nousergon_lib.dates.resolve_trading_day) while this SF's $.run_date
    is the CALENDAR date of Execution.StartTime — on the Saturday weekly run
    they differ, and ASL has no calendar to close the gap. The pointer is
    written LAST of the three membership keys, so a fresh pointer proves all
    three landed, and a freshness compare on a FIXED key needs no date
    arithmetic at all."""
    probe = branch_a["ProbeUniverseMembershipPointer"]
    assert probe["Resource"] == "arn:aws:states:::aws-sdk:s3:headObject"
    assert probe["Parameters"] == {
        "Bucket": "alpha-engine-research",
        "Key": "universe_membership/latest.json",
    }
    assert probe["ResultSelector"] == {
        "pointer_last_modified_date.$": (
            "States.ArrayGetItem(States.StringSplit($.LastModified, 'T'), 0)"
        )
    }
    assert probe["ResultPath"] == "$.universe_membership_probe"


def test_membership_intact_fail_opens_onto_scanners_own_convergence_point(branch_a):
    """The 2026-08-20 shape: the kill landed after the membership write, so the
    day's primary data is intact and the run may continue — DEGRADED, never
    clean, and never anywhere Scanner's success edge would not have gone."""
    gate = branch_a["CheckUniverseMembershipFresh"]
    assert gate["Type"] == "Choice"
    (rule,) = gate["Choices"]
    var = "$.universe_membership_probe.pointer_last_modified_date"
    assert [c["Variable"] for c in rule["And"]] == [var, var]
    assert rule["And"][0]["StringMatches"] == "20*-*-*", (
        "the shape guard is load-bearing: a non-ISO LastModified serialization "
        "would let a bare lexicographic >= silently WRONG-PASS, which is the "
        "exact defect this gate exists to prevent"
    )
    # alpha-engine-config-I8809: LastModified is a wall-clock write time, so
    # the reference is $.calendar_date. Against the post-NormalizeRunDates
    # trading day this fail-open would widen every Saturday.
    assert rule["And"][1]["StringGreaterThanEqualsPath"] == "$.calendar_date"
    assert rule["Next"] == "ScannerResourceKillDegraded"

    degraded = branch_a["ScannerResourceKillDegraded"]
    # alpha-engine-config-I11073: names itself like every other route.
    assert "Result" not in degraded
    assert degraded["Parameters"]["degraded"] is True
    assert degraded["Parameters"]["routes.$"] == (
        "States.Format('{},{}',$.research_degraded_local.routes,"
        "'ScannerResourceKillDegraded')"
    )
    assert degraded["ResultPath"] == "$.research_degraded_local"
    assert degraded["Next"] == branch_a["Scanner"]["Next"] == "CheckSkipRegimeSubstrate"


@pytest.mark.parametrize(
    "halt_state,reached_from",
    [
        ("ScannerResourceKillHalt", "membership pointer stale or absent"),
        ("ScannerMembershipProbeUnknownHalt", "the probe could not establish it"),
    ],
)
def test_every_unproven_membership_halts_through_branch_as_chokepoint(
    branch_a, halt_state, reached_from
):
    """Obligation 1. Both halts route into Branch A's own hard-fail chokepoint,
    so the sibling PredictorTraining branch is never cancelled and the SF fails
    after the join via CheckBranchOutcomes — and the named Error/Cause reach the
    operator through PublishResearchFailureImmediate's SNS message and
    FailExecution's CausePath, both of which render
    States.JsonToString($.error)."""
    st = branch_a[halt_state]
    assert st["Type"] == "Pass"
    assert st["ResultPath"] == "$.error"
    assert st["Next"] == "NormalizeBranchAFailureContext"
    assert branch_a["NormalizeBranchAFailureContext"]["Next"] == "PublishResearchFailureImmediate"
    assert branch_a["PublishResearchFailureImmediate"]["Next"] == "BranchAFailed"


def test_the_unknown_halt_is_distinct_from_the_absent_halt(branch_a):
    """sf-pipeline-policy.md §2.3a rule 2 — 'the artifact is missing' and 'we
    could not look' lead to different operator actions, and collapsing them is
    how an unmeasured state gets reported as a measured one."""
    absent = branch_a["ScannerResourceKillHalt"]["Parameters"]["Error"]
    unknown = branch_a["ScannerMembershipProbeUnknownHalt"]["Parameters"]["Error"]
    assert absent != unknown
    assert branch_a["CheckUniverseMembershipFresh"]["Default"] == "ScannerResourceKillHalt"
    assert (
        _catch_for(branch_a["ProbeUniverseMembershipPointer"], ["States.ALL"])["Next"]
        == "ScannerMembershipProbeUnknownHalt"
    )


@pytest.mark.parametrize(
    "halt_state", ["ScannerResourceKillHalt", "ScannerMembershipProbeUnknownHalt"]
)
def test_the_halt_cause_names_the_kill_and_carries_the_detail(branch_a, halt_state):
    """Obligation 3, at the surface an operator actually reads. The Cause is a
    CONSTANT and the variable detail rides as sibling keys: every path into
    these states guarantees those paths, but a States.Format over them would be
    one more error handler able to destroy its own error — the failure mode
    NormalizeBranchAFailureContext exists for."""
    params = branch_a[halt_state]["Parameters"]
    cause = params["Cause"]
    assert cause.startswith("RESOURCE KILL (TIMEOUT/OOM):")
    assert "440s" in cause, "the limit must be named, not just the fact of a kill"
    assert "universe_membership/latest.json" in cause
    assert "Do NOT re-run unchanged" in cause or "do NOT re-run the scan unchanged" in cause
    assert "Cause.$" not in params
    assert params["scanner_error.$"] == "$.scanner_error"


# ---------------------------------------------------------------------------
# Obligation 3 on the fail-open path — the terminal cause must say "resource
# kill", or a killed run and a merely-broken run read identically.
# ---------------------------------------------------------------------------


def test_the_kill_marker_is_seeded_both_polarities_and_hoisted_out_of_the_branch(
    states, branch_a
):
    """A Parallel branch cannot write an outer-scope JSONPath, so the marker is
    hoisted at the branch terminals. Both terminals must set it unconditionally
    or AggregateBranchOutcomes' Parameters.$ extraction throws States.Runtime —
    and it is seeded false at the InitializeInput floor so it exists on every
    run, which §2.3a rule 3 requires independently."""
    floor = states["InitializeInput"]["Parameters"]["merged.$"]
    assert '"scanner_resource_kill":false' in floor
    assert branch_a["ScannerResourceKill"]["ResultPath"] == "$.scanner_resource_kill"
    assert branch_a["ScannerResourceKill"]["Result"] is True
    # alpha-engine-config-I8194: the hoisted fields moved one level down,
    # inside the branch_a envelope that Parameters now IS (no ResultPath).
    assert branch_a["BranchAComplete"]["Parameters"]["branch_a"][
        "scanner_resource_kill.$"
    ] == "$.scanner_resource_kill"
    assert (
        branch_a["BranchAFailed"]["Parameters"]["branch_a"][
            "scanner_resource_kill"
        ]
        is False
    )
    assert states["AggregateBranchOutcomes"]["Parameters"]["scanner_resource_kill.$"] == (
        "$.parallel_result[0].branch_a.scanner_resource_kill"
    )


def test_a_killed_run_and_a_broken_run_do_not_share_a_terminal_cause(states):
    """DegradedRun's CausePath renders $.degraded_summary.reason, and
    WriteCompletionMarkerDegraded embeds the whole summary in
    _sf_completion/. This is where §3's test is answered: read the last failure
    of the pipeline — can you tell it was killed for resources?"""
    fork = states["CheckScannerResourceKillReason"]
    assert fork["Type"] == "Choice"
    assert states["SetResearchPredictorDegradedSummary"]["Next"] == "CheckScannerResourceKillReason"
    assert fork["Default"] == "CheckSkipBacktester"
    (rule,) = fork["Choices"]
    assert [c["Variable"] for c in rule["And"]] == [
        "$.branch_outcomes.scanner_resource_kill"
    ] * 2, "must be IsPresent-guarded (config#2275) so a pre-deploy in-flight run cannot throw"
    assert rule["And"][0]["IsPresent"] is True
    assert rule["And"][1]["BooleanEquals"] is True
    assert rule["Next"] == "SetScannerResourceKillDegradedSummary"

    kill_summary = states["SetScannerResourceKillDegradedSummary"]
    generic = states["SetResearchPredictorDegradedSummary"]
    assert kill_summary["ResultPath"] == generic["ResultPath"] == "$.degraded_summary"
    assert kill_summary["Parameters"]["reason"] != generic["Parameters"]["reason"]
    assert kill_summary["Parameters"]["reason"] == (
        "weekly_scanner_resource_kill_membership_intact"
    )
    assert kill_summary["Next"] == "CheckSkipBacktester"


def test_the_leaderboard_kill_is_named_too(states):
    """Class sweep: ScannerLeaderboard invokes the SAME Lambda under the SAME
    440s budget. It keeps I7813's fail-open — the board is observe-only and the
    run already terminates in DegradedRun, so §3's 'must not proceed as if
    measured' is satisfied by the terminal — and gains the naming it lacked.
    There is no artifact condition here: the board IS the whole deliverable, so
    there is no partial success to distinguish."""
    catches = states["ScannerLeaderboard"]["Catch"]
    assert [c["ErrorEquals"] for c in catches] == [KILL_ERRORS, ["States.ALL"]]
    assert catches[0]["Next"] == "ScannerLeaderboardResourceKill"
    kill = states["ScannerLeaderboardResourceKill"]
    assert kill["ResultPath"] == "$.scanner_leaderboard_degraded"
    summary = states[kill["Next"]]
    assert summary["Parameters"]["reason"] == "weekly_scanner_leaderboard_resource_kill"
    assert summary["Parameters"]["reason"] != (
        states["SetScannerLeaderboardDegradedSummary"]["Parameters"]["reason"]
    )
    # Converges on the EXISTING alert — no new alert source is introduced.
    assert summary["Next"] == "PublishScannerLeaderboardDegraded"


def test_no_timeout_or_memory_value_moved(states, branch_a):
    """alpha-engine-config-I7851 owns the p95 re-derivation of both budgets and
    is gated on 3 clean weekly invocations post-deploy. 450.0s was a kill, not a
    measurement; this PR must not have quietly changed either number."""
    assert branch_a["Scanner"]["TimeoutSeconds"] == 440
    assert states["ScannerLeaderboard"]["TimeoutSeconds"] == 440
