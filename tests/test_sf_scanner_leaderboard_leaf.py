"""The observe-only scanner leaderboard runs as a weekly-SF LEAF state
(alpha-engine-config-I7813).

Why this file exists rather than one more assertion bolted onto the report-card
wiring suite: the leaf's whole value is a set of *negative* properties — it must
not gate anything, must not be reachable before the Report Card, must not be
skipped when the Report Card fails, and must not end the run green when it
fails. Each of those is a different way the "moved to a leaf" claim can be true
on paper and false in the definition.

The discriminator the ruling gives (issue body): **does any stage branch on the
number?** If a Choice or a gate reads it, it is a CONTROL and stays where it is;
if it is only written and rendered, it is a REPORT and moves. Applied per board:

- ``scanner/leaderboard/{date}.json`` — REPORT. Read by crucible-dashboard's
  Experiments view and by gate predicates; no stage branches on it. MOVED here.
- ``research/cuts_leaderboard/{date}.json`` — CONTROL. ``crucible-research``
  ``scoring/cut_promotion.py`` branches on it and writes the live champion
  pointer, so it stays inside the Scanner state with its consumer.
- ``research/producer_leaderboard/{date}.json`` — CONTROL. ``crucible-backtester``
  ``optimizer/champion_promotion.py`` reads it at the Backtester stage, which
  runs EARLIER than this leaf; moving it here would starve its consumer. It also
  already has its own TimeoutSeconds + Catch on EvalRollingMean.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_DEF = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_DEF.read_text())["States"]


def _top_level_order(defn_states: dict) -> list[str]:
    return list(defn_states)


def test_the_leaf_runs_after_report_card_and_director(states):
    """Placement, asserted by REACHABILITY rather than by key order — key order
    in a JSON object is not execution order, and asserting on it would pass for
    a state wired anywhere."""
    assert states["ReportCard"]["Next"] == "CheckSkipDirector"
    assert states["CheckSkipDirector"]["Default"] == "Director"
    # alpha-engine-config-I11299: Director's success edge now passes through
    # CheckDirectorSubResults (§2.3b); both of its routes converge on
    # DirectorComplete, so the ordering this test pins is unchanged.
    assert states["Director"]["Next"] == "CheckDirectorRetroRefused"
    assert states["CheckDirectorRetroRefused"]["Default"] == "CheckDirectorSubResults"
    assert states["CheckDirectorSubResults"]["Default"] == "DirectorComplete"
    # Every edge that previously ended the advisory tail now enters the gate.
    assert states["DirectorComplete"]["Next"] == "CheckSkipScannerLeaderboard"
    assert states["PublishReportCardDegraded"]["Next"] == "CheckSkipScannerLeaderboard"
    skip_targets = {c["Next"] for c in states["CheckSkipDirector"]["Choices"]}
    assert skip_targets == {"CheckSkipScannerLeaderboard"}


def test_nothing_downstream_reads_the_leafs_output(states):
    """The blast-radius test (sf-pipeline-policy.md §2.1): can this stage fail
    without preventing any stage that does not consume its output? It can only
    be answered YES if no Choice anywhere in the definition branches on the
    leaf's result — the moment one does, the board is a control again and this
    placement is wrong."""
    blob = json.dumps(states)
    # The Task's own ResultPath is the only place the result may appear, plus
    # the error path's own error capture.
    assert states["ScannerLeaderboard"]["ResultPath"] == "$.scanner_leaderboard_result"
    assert blob.count('"$.scanner_leaderboard_result"') == 1, (
        "something other than the leaf's own ResultPath references "
        "$.scanner_leaderboard_result — if a Choice now branches on the board, "
        "it is a CONTROL and must move back beside its consumer"
    )


def test_a_failed_report_card_does_not_skip_the_leaf(states):
    """ReportCard's Catch bypasses the Director (no card to weigh) — but the
    board does not consume the report card, so it must still be built. This is
    the exact inversion §2.1 forbids: one advisory's failure silencing an
    unrelated one."""
    assert all(c["Next"] == "ReportCardDegraded" for c in states["ReportCard"]["Catch"])
    assert states["ReportCardDegraded"]["Next"] == "SetReportCardDegradedSummary"
    assert states["SetReportCardDegradedSummary"]["Next"] == "PublishReportCardDegraded"
    assert states["PublishReportCardDegraded"]["Next"] == "CheckSkipScannerLeaderboard"
    for catch in states["PublishReportCardDegraded"]["Catch"]:
        assert catch["Next"] == "CheckSkipScannerLeaderboard"


def test_the_leaf_has_its_own_timeout_below_the_function_timeout(states):
    """alpha-engine-config-I6855's invariant: an SF budget at or above the
    Lambda's own timeout can never bind, and the overrun then surfaces as an
    opaque Lambda error instead of a States.Timeout naming the state. The
    function is 450s; 440 keeps the SF the effective budget. Re-derivation from
    a measured p95 is alpha-engine-config-I7851."""
    leaf = states["ScannerLeaderboard"]
    assert leaf["TimeoutSeconds"] == 440
    assert leaf["TimeoutSeconds"] < 450


def test_the_leaf_invokes_the_scanner_lambda_in_board_only_mode(states):
    """The leaf reuses the scanner Lambda — so no new IAM grant, and no
    post-merge operator step — and the ONLY thing separating it from the
    Scanner state is the explicit mode. A leaf that dropped the mode would
    silently re-run the whole universe scan after the Report Card and overwrite
    the day's candidates.json."""
    payload = states["ScannerLeaderboard"]["Parameters"]["Payload"]
    assert (
        states["ScannerLeaderboard"]["Parameters"]["FunctionName"]
        == states["ResearchPredictorParallel"]["Branches"][0]["States"]["Scanner"][
            "Parameters"
        ]["FunctionName"]
    ), "the leaf must reuse the Scanner state's Lambda, or it needs its own IAM grant"
    assert payload["mode"] == "scanner_leaderboard"
    assert payload["run_date.$"] == "$.run_date"


def test_a_leaf_failure_degrades_the_run_loudly_and_never_silently(states):
    """sf-pipeline-policy.md §2.3 + the 2026-07-28 Option-A ruling, accepted
    explicitly in I7813's 'Known consequence': the board is observe-only, so its
    failure must not stop the notify or the marker — but the run must not read
    as a clean success either. Three obligations, each asserted:
    the flag is set, a NAMED alert fires, and the terminal marker says DEGRADED.
    """
    leaf = states["ScannerLeaderboard"]
    # alpha-engine-config-I7812: the leaf's LAST catch is the generic fail-open
    # this test owns; the earlier one forks a resource kill (States.Timeout /
    # Lambda.Unknown) so the terminal cause and the DEGRADED marker can name it
    # (sf-pipeline-policy.md §3 obligation 3). Both land on a flag-setter that
    # sets the SAME $.scanner_leaderboard_degraded and both converge on the same
    # named alert, so every obligation below is asserted on both routes.
    assert [c["Next"] for c in leaf["Catch"]] == [
        "ScannerLeaderboardResourceKill",
        "ScannerLeaderboardDegraded",
    ]
    assert all(c["ResultPath"] == "$.scanner_leaderboard_error" for c in leaf["Catch"])

    kill = states["ScannerLeaderboardResourceKill"]
    assert kill["Result"] is True
    assert kill["ResultPath"] == "$.scanner_leaderboard_degraded"
    assert kill["Next"] == "SetScannerLeaderboardResourceKillSummary"
    kill_summary = states["SetScannerLeaderboardResourceKillSummary"]
    assert kill_summary["Parameters"]["degraded"] is True
    assert "resource_kill" in kill_summary["Parameters"]["reason"]
    assert kill_summary["ResultPath"] == "$.degraded_summary"
    assert kill_summary["Next"] == "PublishScannerLeaderboardDegraded"

    flag = states["ScannerLeaderboardDegraded"]
    assert flag["Result"] is True
    assert flag["ResultPath"] == "$.scanner_leaderboard_degraded"
    assert flag["Next"] == "SetScannerLeaderboardDegradedSummary"

    summary = states["SetScannerLeaderboardDegradedSummary"]
    assert summary["Parameters"]["degraded"] is True
    assert summary["ResultPath"] == "$.degraded_summary"
    assert summary["Next"] == "PublishScannerLeaderboardDegraded"

    alert = states["PublishScannerLeaderboardDegraded"]
    assert alert["Resource"] == "arn:aws:states:::sns:publish"
    assert "ScannerLeaderboard" in alert["Parameters"]["Subject"]
    # Best-effort: the alert can fail without changing the outcome it reports.
    # alpha-engine-config-I7194: the tail's convergence point is now the
    # cost-aggregation gate, which every real-completion path enters before
    # CheckShellRunNotify — the aggregator runs AFTER Director so it can see
    # the director-plan cost rows. The leaf's own routing is unchanged in
    # meaning: one hop, best-effort, no fork.
    assert alert["Next"] == "CheckSkipAggregateCosts"
    for catch in alert["Catch"]:
        assert catch["Next"] == "CheckSkipAggregateCosts"

    # $.degraded_summary is what CheckDegradedOutcome routes on.
    outcome = states["CheckDegradedOutcome"]
    assert any(
        cond.get("Variable") == "$.degraded_summary.degraded"
        for choice in outcome["Choices"]
        for cond in (choice.get("And") or [choice])
    )
    # alpha-engine-config-I8809: the legacy-partition copy of the degraded
    # marker sits between it and DegradedRun for the migration window; it is
    # fail-soft. Deleted at the 2026-09-05 cutover.
    assert states["WriteCompletionMarkerDegraded"]["Next"] == "WriteCompletionMarkerDegradedCalendar"
    assert states["WriteCompletionMarkerDegradedCalendar"]["Next"] == "DegradedRun"
    assert states["DegradedRun"]["Type"] == "Fail"


def test_the_terminal_notify_can_never_call_a_degraded_leaf_a_success(states):
    """Without its own rule in CheckGateDegradedNotify, a run whose only
    degradation is the leaf falls through to NotifyComplete — 'All steps
    completed successfully' — while terminating in DegradedRun. That exact
    combination is what the config#6685 fix removed for the report card; this
    pins that the leaf did not reintroduce it."""
    gate = states["CheckGateDegradedNotify"]
    matching = [
        c
        for c in gate["Choices"]
        if any(
            cond.get("Variable") == "$.scanner_leaderboard_degraded"
            for cond in (c.get("And") or [c])
        )
    ]
    assert len(matching) == 1
    assert matching[0]["Next"] == "NotifyCompleteScannerLeaderboardDegraded"
    # Second-to-last before the clean default: a run that ALSO degraded
    # something consequential must report that instead. alpha-engine-config-I7194
    # appended one further rule after this one — $.aggregate_costs_degraded,
    # the tail's sixth and least consequential family — so the property this
    # pins is "after every more consequential family", not "physically last".
    # alpha-engine-config-I11299 registered a SEVENTH family after the cost
    # aggregation one, so the leaf rule is now third from last.
    assert gate["Choices"].index(matching[0]) == len(gate["Choices"]) - 3
    tail_vars = {
        cond.get("Variable")
        for rule in gate["Choices"][-2:]
        for cond in (rule.get("And") or [rule])
    }
    assert tail_vars == {"$.aggregate_costs_degraded", "$.director_degraded"}
    assert gate["Default"] == "NotifyComplete"
    assert states["NotifyCompleteScannerLeaderboardDegraded"]["Next"] == "CheckDegradedOutcome"


def test_the_leaf_is_skippable_and_witnessed_for_rerun(states):
    """sf-pipeline-policy.md §2.5: recovery is one mechanical command, which
    needs (a) a skip flag the rerun deriver can emit and (b) a success-ONLY
    witness. Witnessing on the shared tail convergence point would mark a
    bypassed leaf complete and skip it on the rerun — the I6055 trap.
    alpha-engine-config-I7194 moved that convergence point one state earlier,
    to CheckSkipAggregateCosts; it is still shared by the skip and success
    routes, so the witness still has to be the leaf's own Pass."""
    gate = states["CheckSkipScannerLeaderboard"]
    assert gate["Default"] == "ScannerLeaderboard"
    assert {c["Next"] for c in gate["Choices"]} == {"CheckSkipAggregateCosts"}
    assert states["ScannerLeaderboard"]["Next"] == "ScannerLeaderboardComplete"
    assert states["ScannerLeaderboardComplete"]["Type"] == "Pass"
    assert states["ScannerLeaderboardComplete"]["Next"] == "CheckSkipAggregateCosts"
    # Only the leaf's success edge enters the witness.
    enterers = [
        name for name, body in states.items()
        if body.get("Next") == "ScannerLeaderboardComplete"
    ]
    assert enterers == ["ScannerLeaderboard"]
