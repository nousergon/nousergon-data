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

Catches regressions like:
- A skip-gate dropped, so an entry edge points straight at the task again.
- A gate's skip edge pointing at the wrong next gate (breaks the resume chain).
- A gate's Default not running its task (would skip the task unconditionally).
- The happy path (no skip flags) no longer running every task in order.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

# (gate, task, skip_flag, next_gate) in pipeline order.
_CHAIN = [
    ("CheckSkipMorningEnrich", "MorningEnrich", "skip_morning_enrich", "CheckSkipMorningArcticAppend"),
    ("CheckSkipMorningArcticAppend", "MorningArcticAppend", "skip_morning_arctic_append", "CheckSkipChronicGapHeal"),
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

    def test_trading_day_gate_runs_before_box_then_enters_morning_gate(self, states):
        # config#1430: the NYSE holiday check moved OFF the box into the predictor
        # Lambda and now gates BEFORE StartExecutorEC2.
        assert states["DeployDriftGate"]["Default"] == "TradingDayGate"
        assert states["TradingDayGateChoice"]["Default"] == "StartExecutorEC2"
        false_branch = [
            c["Next"]
            for c in states["TradingDayGateChoice"]["Choices"]
            if c.get("BooleanEquals") is False
        ]
        assert false_branch == ["NotifyHolidaySkip"]
        # Once the box is up, the SSM-ready success branch enters the first morning
        # work gate (CheckSkipMorningEnrich) — it no longer runs an on-box check.
        online = [
            c["Next"]
            for c in states["SSMReadyChoice"]["Choices"]
            if "And" in c
        ]
        assert online == ["CheckSkipMorningEnrich"]

    def test_trading_day_gate_failed_proceeds_as_trading_day(self, states):
        assert states["TradingDayGateFailed"]["Next"] == "StartExecutorEC2"

    def test_morning_enrich_success_enters_append_gate(self, states):
        # L4608: the slow daily_append is now its own load-bearing state behind
        # CheckSkipMorningArcticAppend, after the fast MorningEnrich fetch.
        success = [c["Next"] for c in states["CheckMorningEnrichStatus"]["Choices"]
                   if c.get("StringEquals") == "Success"]
        assert success == ["CheckSkipMorningArcticAppend"]

    def test_arctic_append_success_enters_heal_gate(self, states):
        success = [c["Next"] for c in states["CheckMorningArcticAppendStatus"]["Choices"]
                   if c.get("StringEquals") == "Success"]
        assert success == ["CheckSkipChronicGapHeal"]

    def test_arctic_append_is_load_bearing(self, states):
        # Unlike the fail-soft heal, daily_append must halt the pipeline on
        # failure — predictor reads the ArcticDB universe right after.
        assert states["CheckMorningArcticAppendStatus"]["Default"] == "HandleFailure"
        assert states["MorningArcticAppend"]["Catch"][0]["Next"] == "HandleFailure"
        assert states["WaitForMorningArcticAppend"]["Catch"][0]["Next"] == "HandleFailure"
        # Its SSM command runs the standalone append entrypoint, with a longer
        # timeout than MorningEnrich's 1800s.
        from tests.sf_command_utils import extract_commands
        cmds = "\n".join(extract_commands(states["MorningArcticAppend"]))
        assert "weekly_collector.py --morning-arctic-append" in cmds
        et = states["MorningArcticAppend"]["Parameters"]["Parameters"]["executionTimeout"]
        assert int(et[0] if isinstance(et, list) else et) > 1800

    def test_chronic_gap_terminal_enters_predictor_gate(self, states):
        # CheckChronicGapStatus Default + both heal Catches.
        assert states["CheckChronicGapStatus"]["Default"] == "CheckSkipPredictorInference"
        assert states["ChronicGapSelfHeal"]["Catch"][0]["Next"] == "CheckSkipPredictorInference"
        assert states["WaitForChronicGap"]["Catch"][0]["Next"] == "CheckSkipPredictorInference"

    def test_predictor_health_enters_planner_gate(self, states):
        assert states["PredictorHealthCheck"]["Next"] == "CheckSkipMorningPlanner"
        assert states["PredictorHealthCheck"]["Catch"][0]["Next"] == "CheckSkipMorningPlanner"

    def test_planner_success_enters_daemon_gate(self, states):
        success = [c["Next"] for c in states["CheckMorningPlannerStatus"]["Choices"]
                   if c.get("StringEquals") == "Success"]
        assert success == ["CheckSkipRunDaemon"]

    def test_daemon_is_last_step(self, states):
        # RunDailyNews removed (alpha-engine-config#1089): the standalone 04:00
        # daily-news chain now produces the artifact, so the weekday SF ends at
        # the daemon restart instead of routing into a news chain.
        assert states["RunDaemon"]["Next"] == "PipelineComplete"


class TestPaths:
    def _walk(self, states, start, skip_flags):
        """Walk from `start`, taking gate skip-edges for flags in `skip_flags`,
        else running the task; for status-check Choices follow Success/terminal."""
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
                succ = [c["Next"] for c in st.get("Choices", []) if c.get("StringEquals") == "Success"]
                cur = succ[0] if succ else st.get("Default")
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

    def test_skip_fetch_only_resumes_at_append(self, states):
        """MorningEnrich fetch already done → skip it, run the append onward."""
        order = self._walk(states, "CheckSkipMorningEnrich", skip_flags={"skip_morning_enrich"})
        assert "MorningEnrich" not in order
        assert order[0] == "MorningArcticAppend"
        assert "PredictorInference" in order and order[-1] == "PipelineComplete"

    def test_skip_fetch_and_append_resumes_at_heal(self, states):
        """The exact 6/11 recovery case: fetch + append both done → skip both,
        resume at the heal → predictions."""
        order = self._walk(
            states, "CheckSkipMorningEnrich",
            skip_flags={"skip_morning_enrich", "skip_morning_arctic_append"},
        )
        assert "MorningEnrich" not in order and "MorningArcticAppend" not in order
        assert order[0] == "ChronicGapSelfHeal"
        assert "PredictorInference" in order and order[-1] == "PipelineComplete"
