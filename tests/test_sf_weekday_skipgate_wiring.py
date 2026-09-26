"""Pins the per-task `CheckSkip<State>` rerun gates on the WEEKDAY SF (L4606).

Directive (Brian, 2026-06-11): *"the saturday weekly sf can be rerun task by
task, i expect morning and eod to adopt the same structure."* The Saturday SF
has 10 `CheckSkip<State>` gates; the weekday SF had zero, so a recovery rerun
re-ran every state from the top — the 2026-06-11 chronic-gap rerun had to
re-run the already-completed MorningEnrich `daily_append` (~20 min) because no
skip-gate existed.

This adds a skip-gate before each weekday work task so an operator rerun
passing `{"skip_<task>": true}` (or a future marker-based auto-skip) resumes at
the first incomplete task. Mirrors `test_sf_morning_enrich_split_wiring.py`'s
skip-gate assertions on the Saturday SF.

config#1767 (Phase 2): MorningEnrich + MorningArcticAppend were relocated OFF
the always-on ae-trading box onto TWO independent ephemeral spot boxes (each
dispatched via the alpha-engine-data-spot-dispatcher Lambda, each self-
terminating). CheckSkipMorningEnrich now gates the whole spot data phase
(LaunchMorningEnrichSpot) and its skip edge jumps straight to
CheckSkipChronicGapHeal — the old separate CheckSkipMorningArcticAppend gate
was removed along with the on-trading append state. This also supersedes the
short-lived Phase-1 single-shared-spot pattern (CheckSkipDataSpot /
LaunchDailyDataSpot / ReadDataSpotId / CheckDataSpotLaunched /
CheckDataSpotToTerminate / TerminateDailyDataSpot) — none of those states
exist post-merge.

config#1811 (2026-07-06, unrelated to config#1767 but merged the same week):
CodeFreshnessGate now runs on the trading box right after the SSM-ready poll,
BEFORE any of the above — it self-heals + verifies all 3 repo checkouts are on
current main, closing the 2026-07-06 wedged/stale-box incident where the
pipeline burned ~40 min before the old planner-time deploy-drift preflight
finally refused. Its SUCCESS verdict is the actual entry point into the data
phase (CheckSkipMorningEnrich).

alpha-engine-config-I2717/I2722 (2026-07-16): the CheckSkipChronicGapHeal gate
+ ChronicGapSelfHeal (and its liveness-poll quintet) were REMOVED entirely —
the heal moved to a standalone EventBridge-triggered daily job, off this SF's
critical path. alpha-engine-config-I6494: CheckSkipMorningEnrich's skip edge
and the data-phase spot success edges now route to CheckSkipPredictorInference (the weekday
Scanner before PredictorInference). Likewise PredictorHealthCheck +
PredictorDriftCheck were REMOVED and re-homed onto their own direct
EventBridge triggers — CoverageGapChoice and FinalCoverageGate (the
coverage-gap Choice states that used to Default into PredictorHealthCheck)
now Default straight to CheckSkipMorningPlanner.

Catches regressions like:
- A skip-gate dropped, so an entry edge points straight at the task again.
- A gate's skip edge pointing at the wrong next gate (breaks the resume chain).
- A gate's Default not running its task (would skip the task unconditionally).
- The happy path (no skip flags) no longer running every task in order.
- CodeFreshnessGate's SUCCESS verdict pointing at a state that no longer
  exists (e.g. the retired single-shared-spot `CheckDataSpotLaunched`).
- The chronic-gap-heal gate/state quintet or the predictor health/drift
  states reappearing in this SF instead of staying on their standalone
  EventBridge triggers.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

# (gate, task, skip_flag, next_gate) in pipeline order.
# alpha-engine-config-I2717: CheckSkipChronicGapHeal + ChronicGapSelfHeal
# removed entirely. alpha-engine-config-I6494: CheckSkipMorningEnrich's skip
# edge now routes to CheckSkipPredictorInference (the weekday Scanner was removed by I7811 when the
# spot data phase is skipped); Scanner then joins CheckSkipPredictorInference.
_CHAIN = [
    # I7811 (Brian ruling 2026-08-20): the scanner forms its cuts WEEKLY, so
    # CheckSkipScanner/Scanner are gone from this pipeline and the morning-enrich
    # skip lands on the predictor gate directly.
    # alpha-engine-config-I11269: the gate now fronts the bounded readiness
    # wait on ne-data-collection-morning's manifests, not the spot data phase.
    ("CheckSkipMorningEnrich", "WaitForCollectionManifests", "skip_morning_enrich", "CheckSkipPredictorInference"),
    ("CheckSkipPredictorInference", "PredictorInference", "skip_predictor_inference", "CheckSkipMorningPlanner"),
    ("CheckSkipMorningPlanner", "RunMorningPlanner", "skip_morning_planner", "CheckSkipRunDaemon"),
    # config#2857 + alpha-engine-config#6692: the skip edge now routes through
    # CheckDegradedOutcome (Option-A degraded-terminal parity with the EOD SF)
    # on its way to the SF-envelope completion marker (still a genuine
    # pipeline SUCCESS on the Default/non-degraded branch) before PipelineComplete.
    ("CheckSkipRunDaemon", "RunDaemon", "skip_run_daemon", "CheckDegradedOutcome"),
]


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


class TestGatePresence:
    @pytest.mark.parametrize("gate", [c[0] for c in _CHAIN])
    def test_gate_exists(self, states, gate):
        assert gate in states, f"{gate} missing from weekday SF"
        assert states[gate]["Type"] == "Choice"


class TestGateShape:
    @pytest.mark.parametrize("gate,task,flag,nxt", _CHAIN)
    def test_skip_flag_routes_to_next_gate(self, states, gate, task, flag, nxt):
        choices = states[gate]["Choices"]
        assert len(choices) == 1, f"{gate} should have exactly one skip choice"
        c = choices[0]
        variables = {cond["Variable"] for cond in c["And"]}
        assert variables == {f"$.{flag}"}, (
            f"{gate} skip choice must gate on $.{flag}"
        )
        # And[ IsPresent, BooleanEquals true ] — same shape as the Saturday gates.
        assert any(cond.get("IsPresent") is True for cond in c["And"])
        assert any(cond.get("BooleanEquals") is True for cond in c["And"])
        assert c["Next"] == nxt, f"{gate} skip must route to {nxt}"

    @pytest.mark.parametrize("gate,task,flag,nxt", _CHAIN)
    def test_default_runs_the_task(self, states, gate, task, flag, nxt):
        default = states[gate]["Default"]
        # alpha-engine-config-I11269: CheckSkipMorningEnrich's Default threads
        # through two Pass seeds (InitCollectionReadinessPoll,
        # SeedCollectionReadiness) before WaitForCollectionManifests — follow
        # Pass-state hops so the gate/skip invariant this test pins still holds.
        hops = 0
        while default != task and states[default]["Type"] == "Pass" and hops < 2:
            default, hops = states[default]["Next"], hops + 1
        assert default == task, (
            f"{gate} Default must (eventually) run {task} (missing flag = run as normal)"
        )


class TestEntryEdgesRouteThroughGates:
    """Every edge that used to enter a task now enters its skip-gate."""

    def test_trading_day_gate_runs_before_box_then_enters_codefreshness(self, states):
        # config#1430: the NYSE holiday check moved OFF the box into the predictor
        # Lambda and now gates BEFORE StartExecutorEC2. config#1767 (Phase 2)
        # retired the Phase-1 pre-launch-before-boot spot step (CheckSkipDataSpot /
        # LaunchDailyDataSpot) — each Phase-2 spot now launches lazily, later,
        # from its own CheckSkipMorningEnrich gate — so the trading-day success
        # path goes straight to StartExecutorEC2.
        assert states["DeployDriftGate"]["Default"] == "TradingDayGate"
        assert states["TradingDayGateChoice"]["Default"] == "StartExecutorEC2"
        false_branch = [
            c["Next"]
            for c in states["TradingDayGateChoice"]["Choices"]
            # config-I2767: unwrap the And[IsPresent, BooleanEquals] guard.
            if any(op.get("BooleanEquals") is False for op in c.get("And", [c]))
        ]
        assert false_branch == ["NotifyHolidaySkip"]
        assert states["TradingDayGateFailed"]["Next"] == "StartExecutorEC2"

    def test_ssm_ready_enters_codefreshness_gate_then_morning_gate(self, states):
        # config#1811: once the box is up, the SSM-ready success branch enters
        # CodeFreshnessGate (verify all 3 repo checkouts are on current main
        # BEFORE any pipeline work — the 2026-07-06 incident burned ~40 min
        # before the planner-time deploy-drift preflight refused), whose
        # SUCCESS verdict enters the sf-pipeline-policy 2.3a correctness gate
        # (alpha-engine-config-I9466), whose own SUCCESS enters the first
        # morning work gate (CheckSkipMorningEnrich) — the Phase-2 equivalent
        # of what used to be the (now-retired) single-shared-spot
        # CheckDataSpotLaunched hop.
        online = [
            c["Next"]
            for c in states["SSMReadyChoice"]["Choices"]
            if "And" in c
        ]
        assert online == ["CodeFreshnessGate"]
        fresh = [
            c["Next"] for c in states["CheckCodeFreshnessStatus"]["Choices"]
            if c.get("StringEquals") == "SUCCESS"
        ]
        assert fresh == ["CorrectnessVerdictGate"]
        verdict = [
            c["Next"] for c in states["CheckCorrectnessVerdictStatus"]["Choices"]
            if c.get("StringEquals") == "SUCCESS"
        ]
        assert verdict == ["CheckSkipMorningEnrich"], (
            "the correctness gate must sit BETWEEN code freshness and the first "
            "morning work gate — before any spot launch, so a non-PASS verdict "
            "costs ~2 minutes rather than a full data phase"
        )

    def test_collection_ready_enters_predictor_gate(self, states):
        # alpha-engine-config-I11269: the enrich and Arctic-append spot legs
        # (config#1767) whose Success edges rejoined the trading path at
        # CheckSkipPredictorInference are gone; the readiness wait's "ready"
        # edge is the one entry now, and the fail-open normalizer the other.
        ready = states["CheckCollectionReadiness"]["Choices"][0]
        assert ready["Next"] == "CheckSkipPredictorInference"
        assert states["PublishDataSpotFailureImmediate"]["Next"] == "CheckSkipPredictorInference"

    def test_data_phase_no_longer_on_trading_box(self, states):
        # config#1767 deliverable #2: the trading path retains NO data-phase SSM
        # states — the relocated on-trading states (and the retired single-
        # shared-spot Phase-1 lifecycle states it superseded) are gone.
        for gone in (
            "MorningEnrich", "MorningArcticAppend", "CheckMorningEnrichStatus",
            "CheckMorningArcticAppendStatus", "CheckSkipMorningArcticAppend",
            "CheckSkipDataSpot", "LaunchDailyDataSpot", "ReadDataSpotId",
            "ParseDataSpotId", "CheckDataSpotLaunched", "CheckDataSpotToTerminate",
            "TerminateDailyDataSpot", "ForceTerminateUnresponsiveDataSpot",
        ):
            assert gone not in states, f"{gone} should have moved to the spot dispatcher"

    def test_chronic_gap_heal_quintet_and_gate_removed(self, states):
        # alpha-engine-config-I2717/I2722 (2026-07-16): the heal moved
        # entirely off this SF into the standalone --daily-heal job — see
        # test_sf_chronic_gap_heal_wiring.py for the dedicated removal pin.
        for gone in (
            "CheckSkipChronicGapHeal", "ChronicGapSelfHeal", "InitChronicGapPoll",
            "WaitForChronicGap", "CheckChronicGapStatus", "ChronicGapWait",
            "StampChronicGapUnresponsive",
        ):
            assert gone not in states, f"{gone} should have moved to the standalone daily-heal job"

    def test_coverage_gates_enter_planner_gate_directly(self, states):
        # alpha-engine-config-I2722 (2026-07-16): PredictorHealthCheck +
        # PredictorDriftCheck removed and re-homed onto their own direct
        # EventBridge triggers — see test_sf_predictor_drift_check_wiring.py
        # for the dedicated removal pin. CoverageGapChoice/FinalCoverageGate
        # now Default straight to the morning-planner skip-gate.
        assert states["CoverageGapChoice"]["Default"] == "CheckSkipMorningPlanner"
        assert states["FinalCoverageGate"]["Default"] == "CheckSkipMorningPlanner"

    def test_planner_success_enters_daemon_gate(self, states):
        # config#1811: RunMorningPlanner's poll uses the liveness-poller verdict
        # ("SUCCESS", all-caps), not the plain SSM Status field.
        success = [c["Next"] for c in states["CheckMorningPlannerStatus"]["Choices"]
                   if c.get("StringEquals") == "SUCCESS"]
        assert success == ["CheckSkipRunDaemon"]

    def test_daemon_is_last_step(self, states):
        # RunDailyNews removed (alpha-engine-config#1089): the standalone 04:00
        # daily-news chain now produces the artifact, so the weekday SF ends at
        # the daemon restart instead of routing into a news chain.
        # config#2857: now routes through the SF-envelope completion marker
        # before PipelineComplete. alpha-engine-config#6692: an extra
        # CheckDegradedOutcome hop now sits in between (Option-A parity) so a
        # data-spot fail-open degraded flag, or the daemon's own restart-
        # failure Catch, still routes the terminal to WriteCompletionMarkerDegraded
        # -> DegradedRun instead of a plain SUCCEEDED execution.
        assert states["RunDaemon"]["Next"] == "CheckDegradedOutcome"
        assert states["CheckDegradedOutcome"]["Default"] == "WriteCompletionMarker"
        assert states["WriteCompletionMarker"]["Next"] == "PipelineComplete"


class TestPaths:
    def _walk(self, states, start, skip_flags):
        """Walk from `start`, taking gate skip-edges for flags in `skip_flags`,
        else running the task; for status-check Choices follow the terminal
        success edge (either the plain SSM "Success" the dual-spot poll loops
        use, or the config#1811 liveness-poller verdict "SUCCESS" the
        trading-box loops use), or the spot-launched:true edge for the two
        dual-spot *SpotLaunched gates."""
        order, seen, cur = [], set(), start
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            st = states[cur]
            if cur in {c[0] for c in _CHAIN}:
                flag = next(c[2] for c in _CHAIN if c[0] == cur)
                cur = st["Choices"][0]["Next"] if flag in skip_flags else st["Default"]
                continue
            order.append(cur)
            if st["Type"] == "Succeed":
                break
            if st["Type"] == "Choice":
                succ = [
                    c["Next"] for c in st.get("Choices", [])
                    if c.get("StringEquals") in ("Success", "SUCCESS")
                ]
                launched = (
                    # config-I2767: unwrap the And[IsPresent, BooleanEquals] guard.
                    [c["Next"] for c in st.get("Choices", [])
                     if any(op.get("BooleanEquals") is True for op in c.get("And", [c]))]
                    if cur.endswith("SpotLaunched") or cur == "CheckCollectionReadiness" else []
                )
                cur = (succ or launched or [st.get("Default")])[0]
            else:
                cur = st.get("Next")
        return order

    def test_happy_path_runs_every_task_in_order(self, states):
        order = self._walk(states, "CheckSkipMorningEnrich", skip_flags=set())
        tasks_in_order = [c[1] for c in _CHAIN]
        idxs = [order.index(t) for t in tasks_in_order]
        assert all(t in order for t in tasks_in_order), order
        assert idxs == sorted(idxs), f"tasks out of order: {order}"
        assert order[-1] == "PipelineComplete"

    def test_full_skip_reaches_complete_without_running_tasks(self, states):
        all_flags = {c[2] for c in _CHAIN}
        order = self._walk(states, "CheckSkipMorningEnrich", skip_flags=all_flags)
        for task in (c[1] for c in _CHAIN):
            assert task not in order, f"{task} ran despite its skip flag"
        # config#2857: the skip edge still passes through the SF-envelope
        # completion marker on its way to PipelineComplete.
        # alpha-engine-config#6692: now via CheckDegradedOutcome first (Option-A
        # parity) — the walker's generic Choice handling falls through to its
        # Default (WriteCompletionMarker) since no degraded flag is set on
        # this all-flags-skipped path.
        assert order == ["CheckDegradedOutcome", "WriteCompletionMarker", "PipelineComplete"]

    def test_skip_data_phase_resumes_at_scanner_then_predictor(self, states):
        """config#1767: skip_morning_enrich skips the ENTIRE spot data phase
        (enrich + append both on independent spots) — the old separate
        skip_morning_arctic_append gate is gone. alpha-engine-config-I6494:
        the skip resumes at Scanner (still needed for weekday membership /
        factor-profile freshness on ArcticDB), then PredictorInference."""
        order = self._walk(states, "CheckSkipMorningEnrich", skip_flags={"skip_morning_enrich"})
        assert "WaitForCollectionManifests" not in order
        assert order[0] == "PredictorInference"
        assert order[-1] == "PipelineComplete"

    def test_happy_path_waits_for_the_collection_before_the_predictor(self, states):
        """alpha-engine-config-I11269: no spot launch runs from this SF; the
        predictor reads what ne-data-collection-morning wrote, so the wait
        precedes it."""
        order = self._walk(states, "CheckSkipMorningEnrich", skip_flags=set())
        assert order.index("WaitForCollectionManifests") < order.index("PredictorInference")
        assert not [n for n in order if n.startswith("Launch") and n.endswith("Spot")], order
