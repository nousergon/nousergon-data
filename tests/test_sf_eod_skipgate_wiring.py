"""Pins the per-task `CheckSkip<State>` rerun gates on the EOD SF (L4607).

Directive (Brian, 2026-06-11): *"the saturday weekly sf can be rerun task by
task, i expect morning and eod to adopt the same structure."* The EOD SF had
zero skip-gates, so an EODReconcile-failure rerun re-ran PostMarketData +
CaptureSnapshot from scratch. This adds a skip-gate before each EOD work task
so an operator rerun passing `{"skip_<task>": true}` resumes at the first
incomplete task. Mirrors the weekday SF gates (L4606) and the Saturday SF.

config#1614 (2026-07-02): skip flags are honored ONLY when the execution
input carries `pipeline_role == "operator-replay"`. On any other role (the
daemon's `eod`, `daily`, absent, etc.) the flags are structurally INERT and
every task runs — closing the 2026-06-30 forced-green vector where a
recovery rerun passed `skip_capture_snapshot=true` on a live-role input and
silently bypassed the CaptureSnapshot axis guard (config#1610). Skips are
ignored, not fatal: the downstream fail-loud guards stay the enforcement.

The final cost-guard `StopTradingInstance` is intentionally NOT skip-gated — it
must always run to release the trading EC2.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"

# (gate, task, skip_flag, next_after_skip) in pipeline order.
# PostMarketArcticAppend was split out of PostMarketData 2026-06-16 (the slow
# daily_append, mirroring the weekday MorningArcticAppend split L4608) — it gets
# its own skip-gate so a recovery rerun can skip whichever half already completed.
_CHAIN = [
    # config#1549 — the hoisted top-of-pipeline executor-deploy refresh gate runs
    # FIRST (right after the SSM-readiness gate), so the entire EOD run executes
    # latest origin/main by construction. Skipping it resumes at the first work gate.
    ("CheckSkipRefreshExecutorDeploy", "RefreshExecutorDeploy", "skip_refresh_executor_deploy", "CheckSkipPostMarketData"),
    ("CheckSkipPostMarketData", "PostMarketData", "skip_post_market_data", "CheckSkipPostMarketArcticAppend"),
    ("CheckSkipPostMarketArcticAppend", "PostMarketArcticAppend", "skip_post_market_arctic_append", "CheckSkipCaptureSnapshot"),
    ("CheckSkipCaptureSnapshot", "CaptureSnapshot", "skip_capture_snapshot", "CheckSkipEODReconcile"),
    ("CheckSkipEODReconcile", "EODReconcile", "skip_eod_reconcile", "CheckSkipDailySubstrateHealthCheck"),
    ("CheckSkipDailySubstrateHealthCheck", "DailySubstrateHealthCheck", "skip_daily_substrate_health_check", "StopTradingInstance"),
]


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


class TestGatePresenceAndShape:
    @pytest.mark.parametrize("gate,task,flag,nxt", _CHAIN)
    def test_gate_exists_and_shaped(self, states, gate, task, flag, nxt):
        assert gate in states and states[gate]["Type"] == "Choice"
        c = states[gate]["Choices"][0]
        # config#1614: every skip gate requires BOTH the flag AND the
        # operator-replay pipeline_role — a skip flag on a live-role input
        # is inert.
        assert {cond["Variable"] for cond in c["And"]} == {f"$.{flag}", "$.pipeline_role"}
        flag_conds = [x for x in c["And"] if x["Variable"] == f"$.{flag}"]
        role_conds = [x for x in c["And"] if x["Variable"] == "$.pipeline_role"]
        assert any(x.get("IsPresent") is True for x in flag_conds)
        assert any(x.get("BooleanEquals") is True for x in flag_conds)
        assert any(x.get("IsPresent") is True for x in role_conds)
        assert any(x.get("StringEquals") == "operator-replay" for x in role_conds)
        assert c["Next"] == nxt
        assert states[gate]["Default"] == task


class TestEntryEdgesRouteThroughGates:
    def test_mutex_paths_enter_instance_start_then_post_market_gate(self, states):
        # 2026-06-30: all three post-mutex entries now go through the
        # StartTradingInstance re-runnability guard first, which — once the box
        # is SSM-Online — converges on the CheckSkipPostMarketData rerun-gate
        # chain. (The ensure-running block is pinned in detail by
        # test_sf_eod_instance_start_wiring.py.)
        assert states["CheckMutexRole"]["Default"] == "StartTradingInstance"
        assert states["AcquireMutex"]["Next"] == "StartTradingInstance"
        failopen = [c["Next"] for c in states["AcquireMutex"]["Catch"]
                    if "States.ALL" in c["ErrorEquals"]]
        assert failopen == ["StartTradingInstance"]
        # The ensure-running block's SSM-ready path must land on the first gate,
        # which is now the hoisted deploy-refresh gate (config#1549).
        online = [c["Next"] for c in states["SSMReadyChoice"]["Choices"]
                  if any(x.get("StringEquals") == "Online" for x in c.get("And", []))]
        assert online == ["CheckSkipRefreshExecutorDeploy"]

    def test_refresh_success_enters_post_market_gate(self, states):
        # config#1549: after the deploy refresh succeeds, control enters the
        # first work gate (CheckSkipPostMarketData) — the whole run now executes
        # on latest origin/main.
        succ = [c["Next"] for c in states["CheckRefreshExecutorDeployStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["CheckSkipPostMarketData"]

    def test_post_market_success_enters_arctic_append_gate(self, states):
        succ = [c["Next"] for c in states["CheckPostMarketStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["CheckSkipPostMarketArcticAppend"]

    def test_arctic_append_success_enters_snapshot_gate(self, states):
        succ = [c["Next"] for c in states["CheckPostMarketArcticAppendStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["CheckSkipCaptureSnapshot"]

    def test_snapshot_success_enters_reconcile_gate(self, states):
        succ = [c["Next"] for c in states["CheckSnapshotStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["CheckSkipEODReconcile"]

    def test_eod_success_enters_substrate_gate(self, states):
        succ = [c["Next"] for c in states["CheckEODStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["CheckSkipDailySubstrateHealthCheck"]


class TestPaths:
    def _walk(self, states, skip_flags, pipeline_role="operator-replay"):
        """Simulate the gate chain. Mirrors ASL Choice semantics: the skip
        branch is taken only when the flag is set AND pipeline_role ==
        "operator-replay" (config#1614)."""
        gates = {c[0] for c in _CHAIN}
        order, seen, cur = [], set(), "CheckSkipRefreshExecutorDeploy"
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            st = states[cur]
            if cur in gates:
                flag = next(c[2] for c in _CHAIN if c[0] == cur)
                skip_taken = flag in skip_flags and pipeline_role == "operator-replay"
                cur = st["Choices"][0]["Next"] if skip_taken else st["Default"]
                continue
            order.append(cur)
            if st.get("End") or st["Type"] in ("Succeed", "Fail"):
                break
            if st["Type"] == "Choice":
                succ = [c["Next"] for c in st.get("Choices", []) if c.get("StringEquals") == "Success"]
                cur = succ[0] if succ else st.get("Default")
            else:
                cur = st.get("Next")
        return order

    def test_happy_path_runs_every_task_then_stops_instance(self, states):
        order = self._walk(states, skip_flags=set())
        tasks = [c[1] for c in _CHAIN]
        idxs = [order.index(t) for t in tasks]
        assert idxs == sorted(idxs), order
        assert order[-1] == "StopTradingInstance"

    def test_full_skip_still_stops_the_instance(self, states):
        order = self._walk(states, skip_flags={c[2] for c in _CHAIN})
        for task in (c[1] for c in _CHAIN):
            assert task not in order, f"{task} ran despite skip flag"
        # Cost-guard cleanup must ALWAYS run.
        assert order == ["StopTradingInstance"]

    def test_skip_refresh_resumes_at_post_market_data(self, states):
        # config#1549: skipping only the deploy refresh (e.g. an operator rerun
        # on a box already known fresh) resumes at the first work task.
        order = self._walk(states, skip_flags={"skip_refresh_executor_deploy"})
        assert "RefreshExecutorDeploy" not in order
        assert order[0] == "PostMarketData"
        assert order[-1] == "StopTradingInstance"

    def test_skip_post_market_resumes_at_arctic_append(self, states):
        # Also skip the leading refresh gate so the walk starts cleanly at the
        # PostMarket portion under test (config#1549 added the refresh gate first).
        order = self._walk(states, skip_flags={"skip_refresh_executor_deploy", "skip_post_market_data"})
        assert "PostMarketData" not in order
        assert order[0] == "PostMarketArcticAppend"
        assert order[-1] == "StopTradingInstance"

    def test_skip_post_market_and_arctic_append_resumes_at_snapshot(self, states):
        order = self._walk(states, skip_flags={"skip_refresh_executor_deploy", "skip_post_market_data", "skip_post_market_arctic_append"})
        assert "PostMarketData" not in order
        assert "PostMarketArcticAppend" not in order
        assert order[0] == "CaptureSnapshot"
        assert order[-1] == "StopTradingInstance"


class TestSkipFlagsInertOutsideOperatorReplay:
    """config#1614 closes-when: a skip flag on a non-operator-replay input is
    structurally ignored — the 2026-06-30 skip_capture_snapshot forced-green
    vector no longer exists for live/daemon/watch-initiated runs."""

    def test_all_skips_inert_on_live_eod_role(self, states):
        order = self._walk(states, skip_flags={c[2] for c in _CHAIN}, pipeline_role="eod")
        for task in (c[1] for c in _CHAIN):
            assert task in order, f"{task} was skipped despite non-replay role"
        assert order[-1] == "StopTradingInstance"

    def test_all_skips_inert_when_role_absent(self, states):
        order = self._walk(states, skip_flags={c[2] for c in _CHAIN}, pipeline_role=None)
        for task in (c[1] for c in _CHAIN):
            assert task in order, f"{task} was skipped despite absent role"

    def test_operator_replay_still_honors_skips(self, states):
        order = self._walk(
            states,
            skip_flags={c[2] for c in _CHAIN},
            pipeline_role="operator-replay",
        )
        assert order == ["StopTradingInstance"]

    _walk = TestPaths._walk
