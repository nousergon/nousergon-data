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

alpha-engine-config-I11269 (post-close split): the EOD run is now TWO machines.
``ne-postclose-trading-pipeline`` (step_function_eod.json) runs at ~16:00 ET
and carries the gates that do not need the collector (deploy refresh +
CaptureSnapshot); ``ne-postclose-reconcile-pipeline``
(step_function_eod_reconcile.json) starts when ne-data-collection-eod finishes
and carries the collector-dependent gates (collection wait + EODReconcile).
Every gate is pinned against the machine it now lives in, and the
operator-replay-only rule (config#1614) is asserted for BOTH machines.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_POSTCLOSE_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"
_RECONCILE_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod_reconcile.json"

# (gate, task, skip_flag, next_after_skip) in pipeline order, per machine.
#
# config#1767 (Phase 2): the EOD data phase (PostMarketData + PostMarketArcticAppend)
# was relocated OFF the always-on ae-trading box onto an ephemeral spot box.
# The old CheckSkipPostMarketArcticAppend gate was removed with the
# on-trading append state. The spot data-phase wiring is pinned separately in
# test_sf_data_spot_relocation_wiring.py.
#
# alpha-engine-config-I11269: the spot data phase is REMOVED — the standalone
# ne-data-collection-eod schedule runs it — and CheckSkipPostMarketData now
# gates the bounded readiness wait on that collection's run manifests
# (WaitForCollectionManifests, I11264) instead.
_POSTCLOSE_CHAIN = [
    # config#1549 — the hoisted top-of-pipeline executor-deploy refresh gate runs
    # FIRST (right after the SSM-readiness gate), so the entire run executes
    # latest origin/main by construction. Skipping it resumes at the first work gate.
    ("CheckSkipRefreshExecutorDeploy", "RefreshExecutorDeploy", "skip_refresh_executor_deploy", "CheckSkipCaptureSnapshot"),
    # alpha-engine-config#5569 (2026-08-09): the Default runs through
    # InitCaptureSnapshotRetryCounter (the same-day bounded-retry budget init
    # for CaptureSnapshot's irreversible per-day deadline) before CaptureSnapshot
    # itself — pinned separately in test_sf_capture_snapshot_retry_wiring.py.
    # alpha-engine-config-I11269: CaptureSnapshot is the LAST work task of the
    # post-close machine, so its skip edge lands on the degraded-outcome tail.
    ("CheckSkipCaptureSnapshot", "InitCaptureSnapshotRetryCounter", "skip_capture_snapshot", "StopTradingInstance"),
]

#: alpha-engine-config-I12220: the missing-snapshot self-heal runs right after
#: the deploy refresh, skipped or not. It has no skip gate on purpose: when the
#: snapshot is already in S3 its command exits before opening an IB session, so
#: there is nothing to skip, and when it is missing nothing after it can succeed.
_SNAPSHOT_HEAL = ["CaptureSnapshot", "WaitForCaptureSnapshot", "CheckSnapshotStatus"]

_RECONCILE_CHAIN = [
    ("CheckSkipRefreshExecutorDeploy", "RefreshExecutorDeploy", "skip_refresh_executor_deploy", "CaptureSnapshot"),
    # alpha-engine-config-I11269: CheckSkipPostMarketData's Default enters the
    # readiness wait (InitCollectionReadinessPoll -> ... ->
    # WaitForCollectionManifests) — pinned in test_sf_data_spot_relocation_wiring.py
    # and tests/test_v1_collection_readiness_wait.py. Its skip edge lands on
    # ProbeEODReconcilePrecondition (config-I2702: every path into the
    # reconcile gate, skip or not, must probe fresh). Before the split it
    # landed on CheckSkipCaptureSnapshot, which is now in the other machine.
    ("CheckSkipPostMarketData", "InitCollectionReadinessPoll", "skip_post_market_data", "ProbeEODReconcilePrecondition"),
    # alpha-engine-config-I2722 (2026-07-16): the skip edge used to land on
    # CheckSkipDailySubstrateHealthCheck; that gate + the whole
    # DailySubstrateHealthCheck chain were removed (spun out to a dashboard-box
    # systemd timer), so this now routes straight to the cost-guard tail --
    # entered, since alpha-engine-config-I12020, through the bounded
    # trader-reconcile drain (DrainTraderReconcile) that keeps the box up
    # until the v2 trader's boot-time reconcile has settled. Since 2026-10-09
    # the drain is itself entered through CheckBoxOwnedBeforeStop: a box this
    # execution did not start is neither drained nor stopped.
    ("CheckSkipEODReconcile", "EODReconcile", "skip_eod_reconcile", "CheckBoxOwnedBeforeStop"),
]

_MACHINES = {
    "postclose": (_POSTCLOSE_PATH, _POSTCLOSE_CHAIN),
    "reconcile": (_RECONCILE_PATH, _RECONCILE_CHAIN),
}

_POSTCLOSE_TAIL = ["StopTradingInstance", "CheckDegradedOutcome", "WriteCompletionMarkerNormal", "NormalSucceeded"]
#: alpha-engine-config-I12020: the reconcile machine enters its stop through
#: the trader-reconcile drain; the drain's happy edge is Success.
#: 2026-10-09: and the drain through the box-ownership check, whose happy
#: edge (this execution started the box) is owned == true.
_RECONCILE_STOP_ENTRY = "CheckBoxOwnedBeforeStop"
_RECONCILE_TAIL = [
    "CheckBoxOwnedBeforeStop", "DrainTraderReconcile", "WaitForTraderReconcileDrain", "CheckTraderReconcileDrainStatus",
    "StopTradingInstance", "ReadExerciseCadence", "CheckExerciseCadence",
    "LaunchWeeklyExerciseRun", "CheckDegradedOutcome", "WriteCompletionMarkerNormal", "NormalSucceeded",
]


def _load(path: Path) -> dict:
    return json.loads(path.read_text())["States"]


@pytest.fixture(scope="module")
def postclose() -> dict:
    return _load(_POSTCLOSE_PATH)


@pytest.fixture(scope="module")
def reconcile() -> dict:
    return _load(_RECONCILE_PATH)


# Backwards-compatible name for the collector-gated machine, where the
# reconcile/heal/substrate assertions below live.
@pytest.fixture(scope="module")
def states(reconcile) -> dict:
    return reconcile


_ALL_GATES = [(m, *g) for m, (_p, chain) in _MACHINES.items() for g in chain]


class TestGatePresenceAndShape:
    @pytest.mark.parametrize("machine,gate,task,flag,nxt", _ALL_GATES)
    def test_gate_exists_and_shaped(self, machine, gate, task, flag, nxt):
        states = _load(_MACHINES[machine][0])
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

    @pytest.mark.parametrize("machine", sorted(_MACHINES))
    def test_no_gate_for_a_task_the_machine_does_not_run(self, machine):
        """alpha-engine-config-I11269: each skip gate lives in exactly the
        machine that runs its task — a gate left behind in the other machine
        would be a skip flag no state honours."""
        states = _load(_MACHINES[machine][0])
        declared = {g[0] for g in _MACHINES[machine][1]}
        present = {n for n in states if n.startswith("CheckSkip")}
        assert present == declared, (machine, present ^ declared)


class TestEntryEdgesRouteThroughGates:
    def test_mutex_paths_enter_instance_start_then_post_market_gate(self, postclose):
        states = postclose
        # 2026-06-30: all three post-mutex entries now go through the
        # StartTradingInstance re-runnability guard first, which — once the box
        # is SSM-Online — converges on the rerun-gate chain. (The
        # ensure-running block is pinned in detail by
        # test_sf_eod_instance_start_wiring.py.)
        # alpha-engine-config-I8102: the three mutex edges now enter
        # DeployDriftCheck, and EVERY route out of the drift block converges
        # on StartTradingInstance — the gate is inserted in front of the
        # ensure-running guard, not in place of it, so the invariant this test
        # protects (no ssm:sendCommand before the box is confirmed
        # SSM-Online) is unchanged. Asserted as convergence rather than as
        # three literal edges so a fourth route added later cannot slip past
        # either state.
        assert states["CheckMutexRole"]["Default"] == "DeployDriftCheck"
        assert states["AcquireMutex"]["Next"] == "DeployDriftCheck"
        failopen = [c["Next"] for c in states["AcquireMutex"]["Catch"]
                    if "States.ALL" in c["ErrorEquals"]]
        # alpha-engine-config#6722: the fail-open threads $.degraded_summary
        # through SetMutexAcquireDegradedFlag before continuing.
        assert failopen == ["SetMutexAcquireDegradedFlag"]
        assert states["SetMutexAcquireDegradedFlag"]["Next"] == "DeployDriftCheck"
        assert states["DeployDriftCheck"]["Next"] == "DeployDriftGate"
        drift_exits = {states["DeployDriftGate"]["Default"]}
        for choice in states["DeployDriftGate"]["Choices"]:
            drift_exits.add(states[choice["Next"]]["Next"])
        for catch in states["DeployDriftCheck"]["Catch"]:
            drift_exits.add(states[catch["Next"]]["Next"])
        # Every non-Default route lands on the single notify state, which then
        # continues to StartTradingInstance (and its own Catch does too).
        resolved = set()
        for exit_state in drift_exits:
            if exit_state == "PublishDeployDriftDegraded":
                notify = states[exit_state]
                resolved.add(notify["Next"])
                resolved.update(c["Next"] for c in notify["Catch"])
            else:
                resolved.add(exit_state)
        assert resolved == {"StartTradingInstance"}, resolved
        # The ensure-running block's SSM-ready path must land on the first gate,
        # which is now the hoisted deploy-refresh gate (config#1549).
        online = [c["Next"] for c in states["SSMReadyChoice"]["Choices"]
                  if any(x.get("StringEquals") == "Online" for x in c.get("And", []))]
        assert online == ["CheckSkipRefreshExecutorDeploy"]

    def test_reconcile_mutex_paths_enter_instance_start(self, reconcile):
        # alpha-engine-config-I11269: the reconcile machine carries no
        # DeployDriftCheck (the crucible-predictor probe does not declare its
        # sf_name yet — tracked follow-up), so all three post-mutex entries go
        # straight to the ensure-running guard. The invariant is the same: no
        # ssm:sendCommand before the box is confirmed SSM-Online.
        states = reconcile
        assert "DeployDriftCheck" not in states
        assert states["CheckMutexRole"]["Default"] == "StartTradingInstance"
        assert states["AcquireMutex"]["Next"] == "StartTradingInstance"
        failopen = [c["Next"] for c in states["AcquireMutex"]["Catch"]
                    if "States.ALL" in c["ErrorEquals"]]
        assert failopen == ["SetMutexAcquireDegradedFlag"]
        assert states["SetMutexAcquireDegradedFlag"]["Next"] == "StartTradingInstance"
        online = [c["Next"] for c in states["SSMReadyChoice"]["Choices"]
                  if any(x.get("StringEquals") == "Online" for x in c.get("And", []))]
        assert online == ["CheckSkipRefreshExecutorDeploy"]

    @pytest.mark.parametrize("machine,first_work_gate", [
        ("postclose", "CheckSkipCaptureSnapshot"),
        # alpha-engine-config-I12220: the missing-snapshot self-heal, then
        # CheckSkipPostMarketData.
        ("reconcile", "CaptureSnapshot"),
    ])
    def test_refresh_success_enters_first_work_gate(self, machine, first_work_gate):
        # config#1549: after the deploy refresh succeeds, control enters the
        # first work gate — the whole run executes on latest origin/main.
        states = _load(_MACHINES[machine][0])
        succ = [c["Next"] for c in states["CheckRefreshExecutorDeployStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == [first_work_gate]

    def test_collection_ready_enters_precondition_probe(self, reconcile):
        # alpha-engine-config-I11269: the three spot legs (post-market-data,
        # post-market-arctic-append, edgar-pit-fundamentals-daily) are gone;
        # the readiness wait's "ready" edge is the one entry now. The not-ready
        # edges reach it too, through the fail-open normalizer. Since the
        # split, the next step is the precondition probe (CaptureSnapshot ran
        # at 16:00 in the other machine).
        ready = reconcile["CheckCollectionReadiness"]["Choices"][0]
        assert ready["Next"] == "ProbeEODReconcilePrecondition"
        assert reconcile["PublishDataSpotFailureImmediate"]["Next"] == "ProbeEODReconcilePrecondition"

    @pytest.mark.parametrize("machine", sorted(_MACHINES))
    def test_data_phase_no_longer_on_trading_box(self, machine):
        # config#1767 deliverable #2: the EOD path retains NO data-phase SSM
        # states — reconcile/snapshot/stop stays, the data fetch/append moved.
        states = _load(_MACHINES[machine][0])
        for gone in ("PostMarketData", "PostMarketArcticAppend", "CheckPostMarketStatus",
                     "CheckPostMarketArcticAppendStatus", "CheckSkipPostMarketArcticAppend"):
            assert gone not in states, f"{gone} should have moved to the spot dispatcher"

    def test_snapshot_success_enters_degraded_outcome(self, postclose):
        # alpha-engine-config-I11269: CaptureSnapshot is the post-close
        # machine's last work task; its success stops the box (2026-10-01,
        # Brian: "stop it when postclose part 1 completes") and then enters
        # the Option-A terminal (the precondition probe it used to feed moved
        # to the reconcile machine, pinned below).
        succ = [c["Next"] for c in postclose["CheckSnapshotStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["StopTradingInstance"]
        assert postclose["StopTradingInstance"]["Next"] == "CheckDegradedOutcome"

    def test_precondition_probe_feeds_reconcile_gate(self, reconcile):
        # config-I2702: the verify-by-artifact precondition probe feeds
        # CheckSkipEODReconcile on every path.
        assert reconcile["ProbeEODReconcilePrecondition"]["Next"] == "CheckSkipEODReconcile"

    def test_eod_success_enters_stop_trading_instance(self, reconcile):
        # alpha-engine-config-I2722 (2026-07-16): CheckEODStatus's Success edge
        # used to feed CheckSkipDailySubstrateHealthCheck; that gate + chain
        # are removed, so it now routes directly to the cost-guard tail
        # (through the trader-reconcile drain, alpha-engine-config-I12020).
        succ = [c["Next"] for c in reconcile["CheckEODStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == [_RECONCILE_STOP_ENTRY]


def _walk(states, chain, skip_flags, pipeline_role="operator-replay"):
    """Simulate the gate chain. Mirrors ASL Choice semantics: the skip
    branch is taken only when the flag is set AND pipeline_role ==
    "operator-replay" (config#1614)."""
    gates = {c[0] for c in chain}
    order, seen, cur = [], set(), "CheckSkipRefreshExecutorDeploy"
    while cur and cur in states and cur not in seen:
        seen.add(cur)
        st = states[cur]
        if cur in gates:
            flag = next(c[2] for c in chain if c[0] == cur)
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
            # alpha-engine-config-I11269: CheckCollectionReadiness's
            # happy edge is ready == true (its first rule), the same shape.
            launched = (
                [c["Next"] for c in st.get("Choices", []) if _ops(c).get("BooleanEquals") is True]
                if cur.endswith("SpotLaunched")
                or cur in ("CheckCollectionReadiness", "CheckBoxOwnedBeforeStop") else []
            )
            # alpha-engine-config-I6689: CheckExerciseCadence branches on
            # StringEquals "daily"/"weekly-only"/"off", not "Success" or a
            # *SpotLaunched BooleanEquals — the happy-path simulation here
            # always takes the "daily" branch (LaunchWeeklyExerciseRun),
            # matching this SF's actual pre-config#6689 default behavior.
            cadence_daily = (
                [c["Next"] for c in st.get("Choices", []) if c.get("StringEquals") == "daily"]
                if cur == "CheckExerciseCadence" else []
            )
            cur = (succ or launched or cadence_daily or [st.get("Default")])[0]
        else:
            cur = st.get("Next")
    return order


class TestPaths:
    @pytest.mark.parametrize("machine,tail", [
        ("postclose", _POSTCLOSE_TAIL),
        ("reconcile", _RECONCILE_TAIL),
    ])
    def test_happy_path_runs_every_task_in_order(self, machine, tail):
        path, chain = _MACHINES[machine]
        states = _load(path)
        order = _walk(states, chain, skip_flags=set())
        tasks = [c[1] for c in chain]
        idxs = [order.index(t) for t in tasks]
        assert idxs == sorted(idxs), order
        # config-I2702 deliverable #4: a fully-green run (no gap ever
        # detected, $.degraded_summary never set) routes through
        # CheckDegradedOutcome to the ordinary NormalSucceeded terminal.
        assert order[-len(tail):] == tail, order

    def test_postclose_success_path_stops_the_box(self, postclose):
        # Brian, 2026-10-01: "lets just stop it when postclose part 1
        # completes" — skipped or not, the 16:00 machine stops the trading
        # box before its terminal; the reconcile machine starts it again.
        for flags in (set(), {c[2] for c in _POSTCLOSE_CHAIN}):
            order = _walk(postclose, _POSTCLOSE_CHAIN, skip_flags=flags)
            assert "StopTradingInstance" in order, order
            assert order.index("StopTradingInstance") < order.index("CheckDegradedOutcome")

    def test_full_skip_still_stops_the_instance(self, reconcile):
        order = _walk(reconcile, _RECONCILE_CHAIN, skip_flags={c[2] for c in _RECONCILE_CHAIN})
        for task in (c[1] for c in _RECONCILE_CHAIN):
            assert task not in order, f"{task} ran despite skip flag"
        # Cost-guard cleanup must ALWAYS run, then reach the normal terminal.
        # ProbeEODReconcilePrecondition still runs even on a fully-skipped
        # path (default pipeline_role="operator-replay" here) — same
        # unconditional-probe behavior pinned in
        # test_operator_replay_still_honors_skips below, and so does the
        # missing-snapshot self-heal (alpha-engine-config-I12220).
        assert order == [*_SNAPSHOT_HEAL, "ProbeEODReconcilePrecondition", *_RECONCILE_TAIL]

    def test_postclose_full_skip_reaches_normal_terminal(self, postclose):
        order = _walk(postclose, _POSTCLOSE_CHAIN, skip_flags={c[2] for c in _POSTCLOSE_CHAIN})
        for task in (c[1] for c in _POSTCLOSE_CHAIN):
            assert task not in order, f"{task} ran despite skip flag"
        assert order == _POSTCLOSE_TAIL

    def test_skip_refresh_resumes_at_the_collection_wait(self, reconcile):
        # config#1549: skipping only the deploy refresh (e.g. an operator rerun
        # on a box already known fresh) resumes at the first work task, which
        # is the collection readiness wait (alpha-engine-config-I11269; it was
        # the spot data phase from config#1767 until then).
        order = _walk(reconcile, _RECONCILE_CHAIN, skip_flags={"skip_refresh_executor_deploy"})
        assert "RefreshExecutorDeploy" not in order
        assert order[:3] == _SNAPSHOT_HEAL
        assert order[3:6] == [
            "InitCollectionReadinessPoll", "SeedCollectionReadiness", "WaitForCollectionManifests",
        ]
        assert order[-len(_RECONCILE_TAIL):] == _RECONCILE_TAIL

    def test_skip_refresh_resumes_at_snapshot_in_postclose(self, postclose):
        # alpha-engine-config#5569: resumes at the retry-counter init, then
        # CaptureSnapshot itself.
        order = _walk(postclose, _POSTCLOSE_CHAIN, skip_flags={"skip_refresh_executor_deploy"})
        assert "RefreshExecutorDeploy" not in order
        assert order[:2] == ["InitCaptureSnapshotRetryCounter", "CaptureSnapshot"]
        assert order[-len(_POSTCLOSE_TAIL):] == _POSTCLOSE_TAIL

    def test_skip_data_phase_resumes_at_precondition_probe(self, reconcile):
        # config#1767: skip_post_market_data skips the ENTIRE data phase (now
        # the collection wait) — the old separate skip_post_market_arctic_append
        # gate is gone. alpha-engine-config-I11269: with CaptureSnapshot in the
        # 16:00 machine, the run resumes at the fresh precondition probe.
        order = _walk(reconcile, _RECONCILE_CHAIN,
                      skip_flags={"skip_refresh_executor_deploy", "skip_post_market_data"})
        assert "WaitForCollectionManifests" not in order
        assert order[:3] == _SNAPSHOT_HEAL
        assert order[3] == "ProbeEODReconcilePrecondition"
        assert order[4] == "EODReconcile"
        assert order[-len(_RECONCILE_TAIL):] == _RECONCILE_TAIL

    def test_happy_path_waits_for_the_collection_before_the_reconcile(self, reconcile):
        # alpha-engine-config-I11269: the reconcile reads what the EOD
        # collection wrote, so the wait precedes the probe and EODReconcile;
        # no spot launch runs.
        order = _walk(reconcile, _RECONCILE_CHAIN, skip_flags={"skip_refresh_executor_deploy"})
        assert order.index("WaitForCollectionManifests") < order.index("ProbeEODReconcilePrecondition")
        assert order.index("WaitForCollectionManifests") < order.index("EODReconcile")
        assert not [n for n in order if n.startswith("Launch") and n.endswith("Spot")], order

    def test_postclose_never_waits_for_the_collection(self, postclose):
        # alpha-engine-config-I11269: the whole point of the split — nothing
        # in the 16:00 machine depends on the collector.
        for name in ("WaitForCollectionManifests", "InitCollectionReadinessPoll",
                     "ProbeEODReconcilePrecondition", "EODReconcile", "HealStartCollection"):
            assert name not in postclose, name


class TestSkipFlagsInertOutsideOperatorReplay:
    """config#1614 closes-when: a skip flag on a non-operator-replay input is
    structurally ignored — the 2026-06-30 skip_capture_snapshot forced-green
    vector no longer exists for live/daemon/watch-initiated runs. Asserted on
    both machines since the I11269 split."""

    @pytest.mark.parametrize("machine,tail", [
        ("postclose", _POSTCLOSE_TAIL),
        ("reconcile", _RECONCILE_TAIL),
    ])
    def test_all_skips_inert_on_live_eod_role(self, machine, tail):
        path, chain = _MACHINES[machine]
        order = _walk(_load(path), chain, skip_flags={c[2] for c in chain}, pipeline_role="eod")
        for task in (c[1] for c in chain):
            assert task in order, f"{task} was skipped despite non-replay role"
        assert order[-len(tail):] == tail

    @pytest.mark.parametrize("machine", sorted(_MACHINES))
    def test_all_skips_inert_when_role_absent(self, machine):
        path, chain = _MACHINES[machine]
        order = _walk(_load(path), chain, skip_flags={c[2] for c in chain}, pipeline_role=None)
        for task in (c[1] for c in chain):
            assert task in order, f"{task} was skipped despite absent role"

    def test_operator_replay_still_honors_skips(self, reconcile):
        order = _walk(
            reconcile, _RECONCILE_CHAIN,
            skip_flags={c[2] for c in _RECONCILE_CHAIN},
            pipeline_role="operator-replay",
        )
        # config-I2702: even a fully-skipped operator-replay run still probes
        # the precondition fresh (ProbeEODReconcilePrecondition sits between
        # the CheckSkipPostMarketData skip edge and CheckSkipEODReconcile
        # unconditionally) before its own skip_eod_reconcile flag takes over,
        # and the missing-snapshot self-heal still runs (alpha-engine-config-I12220).
        assert order == [*_SNAPSHOT_HEAL, "ProbeEODReconcilePrecondition", *_RECONCILE_TAIL]

    def test_operator_replay_still_honors_skips_in_postclose(self, postclose):
        order = _walk(
            postclose, _POSTCLOSE_CHAIN,
            skip_flags={c[2] for c in _POSTCLOSE_CHAIN},
            pipeline_role="operator-replay",
        )
        assert order == _POSTCLOSE_TAIL


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

    @pytest.mark.parametrize("machine", sorted(_MACHINES))
    @pytest.mark.parametrize("removed", _REMOVED_STATES)
    def test_state_absent(self, machine, removed):
        path = _MACHINES[machine][0]
        assert removed not in _load(path), (
            f"{removed} should have been removed (alpha-engine-config-I2722 "
            f"substrate-check spin-out) — found it still in {path.name}."
        )

    def test_check_eod_status_success_rewired_to_stop_trading_instance(self, states):
        succ = [c["Next"] for c in states["CheckEODStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == [_RECONCILE_STOP_ENTRY]

    def test_check_skip_eod_reconcile_skip_edge_rewired(self, states):
        skip_choice = states["CheckSkipEODReconcile"]["Choices"][0]
        assert skip_choice["Next"] == _RECONCILE_STOP_ENTRY

    # HealConvergedNotify routes around the stop entry since the 2026-10-08
    # heal-replay stop race: the replay it follows owns the box
    # (tests/test_heal_replay_owns_the_trading_box.py).
    @pytest.mark.parametrize(
        "heal_state",
        ["HealReplayDispatchFailed", "HealNonConvergent"],
    )
    def test_heal_outcome_notifiers_rewired(self, states, heal_state):
        st = states[heal_state]
        assert st["Next"] == _RECONCILE_STOP_ENTRY
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == _RECONCILE_STOP_ENTRY

    @pytest.mark.parametrize("machine", sorted(_MACHINES))
    def test_no_dangling_reference_to_removed_states_anywhere(self, machine):
        """No Next/Default/Choices[].Next/Catch[].Next in the WHOLE SF may
        target any of the 7 removed states — a stricter, file-wide version of
        the per-predecessor checks above."""
        states = _load(_MACHINES[machine][0])
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
