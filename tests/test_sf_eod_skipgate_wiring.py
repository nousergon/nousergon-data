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

alpha-engine-config-I2722 (2026-07-16): the `CheckSkipDailySubstrateHealthCheck`
gate + `DailySubstrateHealthCheck` task (and the rest of that 7-state chain)
were REMOVED — the substrate check is genuinely consumer-free within this SF
(verified) and moves to a standalone dashboard-box systemd timer
(crucible-dashboard `infrastructure/systemd/substrate-health-daily.service`
+ `.timer`). `CheckSkipEODReconcile`'s skip edge and `CheckEODStatus`'s
Success edge — the two `_CHAIN` predecessors that used to feed the deleted
gate — now route directly to `StopTradingInstance`, same as the 3
self-heal-outcome notifiers (`HealReplayDispatchFailed`/`HealConvergedNotify`/
`HealNonConvergent`, pinned in test_sf_eod_precondition_probe_wiring.py).
`TestSubstrateHealthCheckChainRemoved` below pins the chain's absence and the
rewiring.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"

# (gate, task, skip_flag, next_after_skip) in pipeline order.
#
# config#1767 (Phase 2): the EOD data phase (PostMarketData + PostMarketArcticAppend)
# was relocated OFF the always-on ae-trading box onto an ephemeral spot box.
# CheckSkipPostMarketData now gates the whole spot data phase (LaunchPostMarketDataSpot)
# and its skip edge jumps to CheckSkipCaptureSnapshot (the old
# CheckSkipPostMarketArcticAppend gate was removed with the on-trading append state).
# The spot data-phase wiring is pinned separately in
# test_sf_data_spot_relocation_wiring.py.
_CHAIN = [
    # config#1549 — the hoisted top-of-pipeline executor-deploy refresh gate runs
    # FIRST (right after the SSM-readiness gate), so the entire EOD run executes
    # latest origin/main by construction. Skipping it resumes at the first work gate.
    ("CheckSkipRefreshExecutorDeploy", "RefreshExecutorDeploy", "skip_refresh_executor_deploy", "CheckSkipPostMarketData"),
    # 2026-07-14: CheckSkipPostMarketData's Default now runs through
    # InitDataSpotRetryCounter (the new spot-interruption retry-budget init)
    # before LaunchPostMarketDataSpot — pinned separately in
    # TestEODDataSpotRetryBudget (test_sf_data_spot_relocation_wiring.py).
    ("CheckSkipPostMarketData", "InitDataSpotRetryCounter", "skip_post_market_data", "CheckSkipCaptureSnapshot"),
    # config-I2702 (2026-07-15): the skip edge now lands on
    # ProbeEODReconcilePrecondition (the new verify-by-artifact precondition
    # probe), not directly on CheckSkipEODReconcile — every path into the
    # reconcile gate, skip or not, must probe fresh.
    ("CheckSkipCaptureSnapshot", "CaptureSnapshot", "skip_capture_snapshot", "ProbeEODReconcilePrecondition"),
    # alpha-engine-config-I2722 (2026-07-16): the skip edge used to land on
    # CheckSkipDailySubstrateHealthCheck; that gate + the whole
    # DailySubstrateHealthCheck chain were removed (spun out to a dashboard-box
    # systemd timer), so this now routes straight to the cost-guard tail.
    ("CheckSkipEODReconcile", "EODReconcile", "skip_eod_reconcile", "StopTradingInstance"),
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

    def test_post_market_spot_success_enters_arctic_append_spot(self, states):
        # config#1767: the post-market fetch now runs on spot; its poll Success
        # enters the Arctic-append retry-counter init (2026-07-14), which then
        # launches the Arctic-append spot workload (both run on spot).
        succ = [c["Next"] for c in states["CheckPostMarketDataSpotStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["InitDataSpotArcticRetryCounter"]
        assert states["InitDataSpotArcticRetryCounter"]["Next"] == "LaunchPostMarketArcticAppendSpot"

    def test_arctic_append_spot_success_enters_snapshot_gate(self, states):
        # config#1767: the EOD Arctic append also runs on spot; its Success
        # rejoins the reconcile/snapshot path at CheckSkipCaptureSnapshot.
        succ = [c["Next"] for c in states["CheckPostMarketArcticAppendSpotStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["CheckSkipCaptureSnapshot"]

    def test_data_phase_no_longer_on_trading_box(self, states):
        # config#1767 deliverable #2: the EOD path retains NO data-phase SSM
        # states — reconcile/snapshot/stop stays, the data fetch/append moved.
        for gone in ("PostMarketData", "PostMarketArcticAppend", "CheckPostMarketStatus",
                     "CheckPostMarketArcticAppendStatus", "CheckSkipPostMarketArcticAppend"):
            assert gone not in states, f"{gone} should have moved to the spot dispatcher"

    def test_snapshot_success_enters_reconcile_gate(self, states):
        # config-I2702: CaptureSnapshot's success now enters the new
        # verify-by-artifact precondition probe, which itself feeds
        # CheckSkipEODReconcile (pinned separately below).
        succ = [c["Next"] for c in states["CheckSnapshotStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["ProbeEODReconcilePrecondition"]
        assert states["ProbeEODReconcilePrecondition"]["Next"] == "CheckSkipEODReconcile"

    def test_eod_success_enters_stop_trading_instance(self, states):
        # alpha-engine-config-I2722 (2026-07-16): CheckEODStatus's Success edge
        # used to feed CheckSkipDailySubstrateHealthCheck; that gate + chain
        # are removed, so it now routes directly to the cost-guard tail.
        succ = [c["Next"] for c in states["CheckEODStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["StopTradingInstance"]


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
                # config#1767: the spot launched-gate states branch on
                # launched:true (BooleanEquals) — follow that on the happy path.
                # config-I2767: the rule is now And[IsPresent, BooleanEquals]-
                # guarded, so unwrap the And when matching.
                def _ops(rule):
                    merged = {}
                    for op in rule.get("And", [rule]):
                        merged.update(op)
                    return merged
                launched = (
                    [c["Next"] for c in st.get("Choices", []) if _ops(c).get("BooleanEquals") is True]
                    if cur.endswith("SpotLaunched") else []
                )
                cur = (succ or launched or [st.get("Default")])[0]
            else:
                cur = st.get("Next")
        return order

    def test_happy_path_runs_every_task_then_stops_instance(self, states):
        order = self._walk(states, skip_flags=set())
        tasks = [c[1] for c in _CHAIN]
        idxs = [order.index(t) for t in tasks]
        assert idxs == sorted(idxs), order
        # config-I2702 deliverable #4: StopTradingInstance no longer
        # terminates the execution directly — a fully-green run (no gap ever
        # detected, $.degraded_summary never set) routes through
        # CheckDegradedOutcome to the ordinary NormalSucceeded terminal.
        assert order[-3:] == ["StopTradingInstance", "CheckDegradedOutcome", "NormalSucceeded"]

    def test_full_skip_still_stops_the_instance(self, states):
        order = self._walk(states, skip_flags={c[2] for c in _CHAIN})
        for task in (c[1] for c in _CHAIN):
            assert task not in order, f"{task} ran despite skip flag"
        # Cost-guard cleanup must ALWAYS run, then reach the normal terminal.
        # ProbeEODReconcilePrecondition still runs even on a fully-skipped
        # path (default pipeline_role="operator-replay" here) — same
        # unconditional-probe behavior pinned in
        # test_operator_replay_still_honors_skips below.
        assert order == [
            "ProbeEODReconcilePrecondition", "StopTradingInstance",
            "CheckDegradedOutcome", "NormalSucceeded",
        ]

    def test_skip_refresh_resumes_at_data_spot(self, states):
        # config#1549: skipping only the deploy refresh (e.g. an operator rerun
        # on a box already known fresh) resumes at the first work task, which is
        # now the spot data-phase retry-counter init (config#1767; counter init
        # added 2026-07-14) then the spot launch itself.
        order = self._walk(states, skip_flags={"skip_refresh_executor_deploy"})
        assert "RefreshExecutorDeploy" not in order
        assert order[0] == "InitDataSpotRetryCounter"
        assert order[1] == "LaunchPostMarketDataSpot"
        assert order[-3:] == ["StopTradingInstance", "CheckDegradedOutcome", "NormalSucceeded"]

    def test_skip_data_phase_resumes_at_snapshot(self, states):
        # config#1767: skip_post_market_data now skips the ENTIRE spot data phase
        # (fetch + append both on spot) and resumes at the snapshot gate — the
        # old separate skip_post_market_arctic_append gate is gone.
        order = self._walk(states, skip_flags={"skip_refresh_executor_deploy", "skip_post_market_data"})
        assert "LaunchPostMarketDataSpot" not in order
        assert "LaunchPostMarketArcticAppendSpot" not in order
        assert order[0] == "CaptureSnapshot"
        assert order[-3:] == ["StopTradingInstance", "CheckDegradedOutcome", "NormalSucceeded"]

    def test_happy_path_runs_data_phase_on_spot(self, states):
        # config#1767: the EOD data phase runs as spot-launch states, in order,
        # before the snapshot.
        order = self._walk(states, skip_flags={"skip_refresh_executor_deploy"})
        assert "LaunchPostMarketDataSpot" in order
        assert "LaunchPostMarketArcticAppendSpot" in order
        assert order.index("LaunchPostMarketDataSpot") < order.index("LaunchPostMarketArcticAppendSpot")
        assert order.index("LaunchPostMarketArcticAppendSpot") < order.index("CaptureSnapshot")


class TestSkipFlagsInertOutsideOperatorReplay:
    """config#1614 closes-when: a skip flag on a non-operator-replay input is
    structurally ignored — the 2026-06-30 skip_capture_snapshot forced-green
    vector no longer exists for live/daemon/watch-initiated runs."""

    def test_all_skips_inert_on_live_eod_role(self, states):
        order = self._walk(states, skip_flags={c[2] for c in _CHAIN}, pipeline_role="eod")
        for task in (c[1] for c in _CHAIN):
            assert task in order, f"{task} was skipped despite non-replay role"
        assert order[-3:] == ["StopTradingInstance", "CheckDegradedOutcome", "NormalSucceeded"]

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
        # config-I2702: even a fully-skipped operator-replay run still probes
        # the precondition fresh (ProbeEODReconcilePrecondition sits between
        # the CheckSkipCaptureSnapshot skip edge and CheckSkipEODReconcile
        # unconditionally) before its own skip_eod_reconcile flag takes over.
        assert order == [
            "ProbeEODReconcilePrecondition", "StopTradingInstance",
            "CheckDegradedOutcome", "NormalSucceeded",
        ]

    _walk = TestPaths._walk


class TestSubstrateHealthCheckChainRemoved:
    """alpha-engine-config-I2722 (2026-07-16): the 7-state
    DailySubstrateHealthCheck chain is removed from the EOD SF — genuinely
    consumer-free (verified), re-homed as a standalone systemd timer on the
    dashboard box (crucible-dashboard
    infrastructure/systemd/substrate-health-daily.{service,timer}). Per-row
    CloudWatch metrics (AlphaEngine/Substrate) + existing alarms carry the
    alerting independently of the SF, so this is a pure re-homing, not a loss
    of observability.

    Pins both halves of the change so a future edit can't silently
    reintroduce the chain or leave a predecessor's edge dangling:
    (1) all 7 removed state names are absent, (2) every predecessor that used
    to feed CheckSkipDailySubstrateHealthCheck now routes straight to
    StopTradingInstance.
    """

    _REMOVED_STATES = (
        "CheckSkipDailySubstrateHealthCheck",
        "DailySubstrateHealthCheck",
        "WaitForDailySubstrateHealthCheck",
        "CheckDailySubstrateHealthCheckStatus",
        "DailySubstrateHealthCheckPollWait",
        "SubstrateHealthCheckDegraded",
        "PublishSubstrateHealthCheckDegradedAlert",
    )

    @pytest.mark.parametrize("removed", _REMOVED_STATES)
    def test_state_absent(self, states, removed):
        assert removed not in states, (
            f"{removed} should have been removed (alpha-engine-config-I2722 "
            "substrate-check spin-out) — found it still in step_function_eod.json."
        )

    def test_check_eod_status_success_rewired_to_stop_trading_instance(self, states):
        succ = [c["Next"] for c in states["CheckEODStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["StopTradingInstance"]

    def test_check_skip_eod_reconcile_skip_edge_rewired(self, states):
        skip_choice = states["CheckSkipEODReconcile"]["Choices"][0]
        assert skip_choice["Next"] == "StopTradingInstance"

    @pytest.mark.parametrize(
        "heal_state",
        ["HealReplayDispatchFailed", "HealConvergedNotify", "HealNonConvergent"],
    )
    def test_heal_outcome_notifiers_rewired(self, states, heal_state):
        st = states[heal_state]
        assert st["Next"] == "StopTradingInstance"
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == "StopTradingInstance"

    def test_no_dangling_reference_to_removed_states_anywhere(self, states):
        """No Next/Default/Choices[].Next/Catch[].Next in the WHOLE SF may
        target any of the 7 removed states — a stricter, file-wide version of
        the per-predecessor checks above."""
        removed = set(self._REMOVED_STATES)

        def _refs(state):
            out = []
            if "Next" in state:
                out.append(state["Next"])
            if "Default" in state:
                out.append(state["Default"])
            for c in state.get("Choices", []) or []:
                if "Next" in c:
                    out.append(c["Next"])
            for c in state.get("Catch", []) or []:
                if "Next" in c:
                    out.append(c["Next"])
            return out

        dangling = {
            name: [t for t in _refs(st) if t in removed]
            for name, st in states.items()
            if any(t in removed for t in _refs(st))
        }
        assert not dangling, f"dangling reference(s) to removed substrate states: {dangling}"
