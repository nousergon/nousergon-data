"""Pins the per-task `CheckSkip<State>` rerun gates on the EOD SF (L4607).

Directive (Brian, 2026-06-11): *"the saturday weekly sf can be rerun task by
task, i expect morning and eod to adopt the same structure."* The EOD SF had
zero skip-gates, so an EODReconcile-failure rerun re-ran PostMarketData +
CaptureSnapshot from scratch. This adds a skip-gate before each EOD work task
so an operator rerun passing `{"skip_<task>": true}` resumes at the first
incomplete task. Mirrors the weekday SF gates (L4606) and the Saturday SF.

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
        assert {cond["Variable"] for cond in c["And"]} == {f"$.{flag}"}
        assert any(cond.get("IsPresent") is True for cond in c["And"])
        assert any(cond.get("BooleanEquals") is True for cond in c["And"])
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
        # The ensure-running block's SSM-ready path must land on the first gate.
        online = [c["Next"] for c in states["SSMReadyChoice"]["Choices"]
                  if any(x.get("StringEquals") == "Online" for x in c.get("And", []))]
        assert online == ["CheckSkipPostMarketData"]

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
    def _walk(self, states, skip_flags):
        gates = {c[0] for c in _CHAIN}
        order, seen, cur = [], set(), "CheckSkipPostMarketData"
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            st = states[cur]
            if cur in gates:
                flag = next(c[2] for c in _CHAIN if c[0] == cur)
                cur = st["Choices"][0]["Next"] if flag in skip_flags else st["Default"]
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

    def test_skip_post_market_resumes_at_arctic_append(self, states):
        order = self._walk(states, skip_flags={"skip_post_market_data"})
        assert "PostMarketData" not in order
        assert order[0] == "PostMarketArcticAppend"
        assert order[-1] == "StopTradingInstance"

    def test_skip_post_market_and_arctic_append_resumes_at_snapshot(self, states):
        order = self._walk(states, skip_flags={"skip_post_market_data", "skip_post_market_arctic_append"})
        assert "PostMarketData" not in order
        assert "PostMarketArcticAppend" not in order
        assert order[0] == "CaptureSnapshot"
        assert order[-1] == "StopTradingInstance"
