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
phase (CheckSkipMorningEnrich). PredictorHealthCheck also now hands off to the
PredictorDriftCheck producer (config#1853) before the planner gate.

Catches regressions like:
- A skip-gate dropped, so an entry edge points straight at the task again.
- A gate's skip edge pointing at the wrong next gate (breaks the resume chain).
- A gate's Default not running its task (would skip the task unconditionally).
- The happy path (no skip flags) no longer running every task in order.
- CodeFreshnessGate's SUCCESS verdict pointing at a state that no longer
  exists (e.g. the retired single-shared-spot `CheckDataSpotLaunched`).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

# (gate, task, skip_flag, next_gate) in pipeline order.
_CHAIN = [
    ("CheckSkipMorningEnrich", "LaunchMorningEnrichSpot", "skip_morning_enrich", "CheckSkipChronicGapHeal"),
    ("CheckSkipChronicGapHeal", "ChronicGapSelfHeal", "skip_chronic_gap_heal", "CheckSkipPredictorInference"),
    ("CheckSkipPredictorInference", "PredictorInference", "skip_predictor_inference", "CheckSkipMorningPlanner"),
    ("CheckSkipMorningPlanner", "RunMorningPlanner", "skip_morning_planner", "CheckSkipRunDaemon"),
    ("CheckSkipRunDaemon", "RunDaemon", "skip_run_daemon", "PipelineComplete"),
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
        assert states[gate]["Default"] == task, (
            f"{gate} Default must run {task} (missing flag = run as normal)"
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
            if c.get("BooleanEquals") is False
        ]
        assert false_branch == ["NotifyHolidaySkip"]
        assert states["TradingDayGateFailed"]["Next"] == "StartExecutorEC2"

    def test_ssm_ready_enters_codefreshness_gate_then_morning_gate(self, states):
        # config#1811: once the box is up, the SSM-ready success branch enters
        # CodeFreshnessGate (verify all 3 repo checkouts are on current main
        # BEFORE any pipeline work — the 2026-07-06 incident burned ~40 min
        # before the planner-time deploy-drift preflight refused), whose
        # SUCCESS verdict enters the first morning work gate
        # (CheckSkipMorningEnrich) — the Phase-2 equivalent of what used to be
        # the (now-retired) single-shared-spot CheckDataSpotLaunched hop.
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
        assert fresh == ["CheckSkipMorningEnrich"]

    def test_morning_enrich_spot_success_enters_append_spot(self, states):
        # config#1767: the enrich fetch now runs on its own ephemeral spot. Its
        # poll-status Success enters the Arctic-append spot launch (both run on
        # independent spots).
        success = [c["Next"] for c in states["CheckMorningEnrichSpotStatus"]["Choices"]
                   if c.get("StringEquals") == "Success"]
        assert success == ["LaunchMorningArcticAppendSpot"]

    def test_arctic_append_spot_success_enters_heal_gate(self, states):
        # config#1767: the Arctic append also runs on its own spot; its Success
        # rejoins the trading path at CheckSkipChronicGapHeal.
        success = [c["Next"] for c in states["CheckMorningArcticAppendSpotStatus"]["Choices"]
                   if c.get("StringEquals") == "Success"]
        assert success == ["CheckSkipChronicGapHeal"]

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

    def test_chronic_gap_terminal_enters_predictor_gate(self, states):
        # CheckChronicGapStatus Default + both heal Catches. ChronicGapSelfHeal
        # stays on the trading box (config#1811 upgraded it to the liveness-poll
        # loop, but there is no shared spot left to terminate, so it never
        # routes through a spot-terminate hook).
        assert states["CheckChronicGapStatus"]["Default"] == "CheckSkipPredictorInference"
        assert states["ChronicGapSelfHeal"]["Catch"][0]["Next"] == "CheckSkipPredictorInference"
        assert states["WaitForChronicGap"]["Catch"][0]["Next"] == "CheckSkipPredictorInference"

    def test_predictor_health_enters_drift_check(self, states):
        # config#1853: PredictorHealthCheck now routes into the drift-check
        # producer (fail-soft, both success and Catch) before the planner gate.
        assert states["PredictorHealthCheck"]["Next"] == "PredictorDriftCheck"
        assert states["PredictorHealthCheck"]["Catch"][0]["Next"] == "PredictorDriftCheck"

    def test_drift_check_enters_planner_gate(self, states):
        assert states["PredictorDriftCheck"]["Next"] == "CheckSkipMorningPlanner"
        assert states["PredictorDriftCheck"]["Catch"][0]["Next"] == "CheckSkipMorningPlanner"

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
        assert states["RunDaemon"]["Next"] == "PipelineComplete"


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
                    [c["Next"] for c in st.get("Choices", []) if c.get("BooleanEquals") is True]
                    if cur.endswith("SpotLaunched") else []
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
        assert order == ["PipelineComplete"]

    def test_skip_data_phase_resumes_at_heal(self, states):
        """config#1767: skip_morning_enrich now skips the ENTIRE spot data phase
        (enrich + append both on independent spots) and resumes at the
        chronic-gap heal — the old separate skip_morning_arctic_append gate is
        gone."""
        order = self._walk(states, "CheckSkipMorningEnrich", skip_flags={"skip_morning_enrich"})
        assert "LaunchMorningEnrichSpot" not in order
        assert "LaunchMorningArcticAppendSpot" not in order
        assert order[0] == "ChronicGapSelfHeal"
        assert "PredictorInference" in order and order[-1] == "PipelineComplete"

    def test_happy_path_runs_data_phase_on_spot(self, states):
        """The data phase runs as spot-launch states, not on-trading SSM."""
        order = self._walk(states, "CheckSkipMorningEnrich", skip_flags=set())
        assert "LaunchMorningEnrichSpot" in order
        assert "LaunchMorningArcticAppendSpot" in order
        # Enrich spot precedes append spot precedes the heal.
        assert order.index("LaunchMorningEnrichSpot") < order.index("LaunchMorningArcticAppendSpot")
        assert order.index("LaunchMorningArcticAppendSpot") < order.index("ChronicGapSelfHeal")
