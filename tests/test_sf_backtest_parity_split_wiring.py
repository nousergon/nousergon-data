"""Pins the Backtester → Backtest + Parity split in the Saturday SF.

Origin: the preflight-task-split (2026-05-16, plan
alpha-engine-docs/private/preflight-task-split-260516.md). The standing
rule — every preflight-bearing action is its own SF task; a downstream
failure must never re-run a completed upstream task — was violated by
the old combined `Backtester` state, which ran
`spot_backtest.sh --skip-stages=evaluator` = backtest (~121 min,
10y simulate + param sweep) THEN parity on one spot. Every parity
recovery re-paid the 121-min backtest.

Naming decision (lower-churn option, per task spec): the existing
`Backtester` state name is KEPT for the backtest-stage state (its
SSM command flips --skip-stages=evaluator → --skip-stages=parity,evaluator
so it runs ONLY the backtest stage), and a NEW `Parity` quartet is added
after it. Keeping `Backtester` avoids rewiring DriftDetection's two
Next/Catch edges and all inbound references to CheckSkipBacktester.

This is SF-wiring-only: spot_backtest.sh's --skip-stages already supports
backtest/parity/evaluator independently (validated stage vocabulary
_KNOWN_STAGES="backtest parity evaluator") — no backtester-repo change.

L4472 phase-split (2026-05-31, ROADMAP L4472): the single `Backtester`
state ran simulate+param_sweep+predictor-backtest+Phase4+optimizer/cov/
gamma in ONE SSM command whose SUMMED runtime exceeded the SSM execution
timeout on a fresh date (L4470). The backtest stage is now decomposed by
--mode into THREE sequential SF states, each with its own SSM timeout +
independent redrive:
  Backtester               --mode=param-sweep --no-pit-parity   (simulate+sweep)
  PredictorBacktest        --mode=predictor-backtest --no-pit-parity  (predictor+Phase4)
  PortfolioOptimizerBacktest --mode=portfolio-optimizer-backtest --no-pit-parity
  Parity                   --skip-stages=backtest,evaluator     (parity + pit_parity HERE, L4486)
The happy path is now:
  CheckSkipBacktester → Backtester → PredictorBacktest →
  PortfolioOptimizerBacktest → CheckSkipParity → Parity →
  CheckSkipEvaluator → Evaluator.
skip_backtester still skips the whole backtest-family (routes past
CheckSkipParity to CheckSkipEvaluator). L4486 (2026-06-05): pit_parity fires
exactly once, RELOCATED to the standalone Parity state (fresh process, ≥8 GB
floor) — the other three states pass --no-pit-parity. It used to run stacked in
PredictorBacktest, OOM-guard-failing on the 8 GB box (2nd predictor_pipeline
after the main one held ~3.5 GB).

alpha-engine-config-I4442/I4497 SF cutover (2026-08-09, crucible-backtester
#631): the --mode/--skip-stages/--pit-parity-enabled flag vocabulary
described above is HISTORICAL. Each of the five backtest-family states now
invokes its own dedicated script (spot_backtester.sh / spot_predictor_
backtest.sh / spot_portfolio_optimizer_backtest.sh / spot_parity.sh /
spot_evaluator.sh) with no stage-multiplexing flag at all — the flag-based
mode selection this docstring documents was the DEFECT I4442/I4497 exist to
remove (one shared launcher script per SF state, not five modes of one
script). spot_backtest.sh is retained on disk, byte-identical, only as the
rollback path; no SF state invokes it any longer. The ordering/Catch/
timeout/ResultPath invariants below are unaffected by the cutover and still
apply to the per-stage scripts.

alpha-engine-config#6030 (2026-08-09): the standalone Parity state was
itself found to bundle THREE logical stages (pit_parity lookahead pass +
walkforward pass + parity replay) behind spot_parity.sh — the same §2.1
violation one level down. It is now a ParityParallel of three fail-open
branch quartets (PitParityLookahead / PitParityWalkforward / ParityReplay,
each its own spot + script + timeout + skip flag) joined by a
PitParityCompare quartet that reads the per-pass artifacts
(parity/{run_date}/pit_stats_*.json, crucible-backtester
contracts/pit_stats_pass.schema.json) and writes
backtest/{run_date}/pit_parity.json — verdict UNKNOWN when a pass artifact
is missing (§2.3a). The parity assertions below pin that topology.

This test catches regressions like:
- Someone reverts a backtest-family state's SSM command back to invoking
  the shared spot_backtest.sh monolith (with any --mode/--skip-stages flag)
  instead of its own dedicated script.
- Someone wires Parity BEFORE Backtester, or drops the Parity state.
- Someone reroutes the backtest-family chain so a phase is skipped or
  re-ordered (e.g. predictor before sim).
- Someone drops the HandleFailure Catch on the new states.
- The old single combined-Backtester semantics (--skip-stages=evaluator)
  reappears anywhere.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.sf_degraded_summary_helpers import assert_degraded_continuation


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


@pytest.fixture(scope="module")
def parity_branches(states) -> dict:
    """{branch_base_name: branch_states} for the ParityParallel branches
    (alpha-engine-config#6030), keyed by each branch's StartAt gate's
    Default target (the work-quartet base name)."""
    out = {}
    for branch in states["ParityParallel"]["Branches"]:
        gate = branch["States"][branch["StartAt"]]
        out[gate["Default"]] = branch["States"]
    return out


_PARITY_BRANCH_SPECS = {
    # base: (skip flag, var, script, branch status key, poll bound, exec_to)
    "PitParityLookahead": (
        "skip_pit_parity_lookahead", "pit_parity_lookahead",
        "spot_pit_lookahead.sh", "branch_pit_lookahead", 216, "5400"),
    "PitParityWalkforward": (
        "skip_pit_parity_walkforward", "pit_parity_walkforward",
        "spot_pit_walkforward.sh", "branch_pit_walkforward", 216, "5400"),
    "ParityReplay": (
        "skip_parity_replay", "parity_replay",
        "spot_parity_replay.sh", "branch_parity_replay", 108, "2700"),
}


class TestQuartetPresence:
    """The backtest-stage quartet (kept name `Backtester` + helpers) and
    the new `Parity` quartet (+ Wait/Extract helpers) must both exist,
    mirroring the RAGIngestion / Backtester quartet shape."""

    @pytest.mark.parametrize(
        "name",
        [
            # backtest-stage quartet — name KEPT (lower-churn option)
            "CheckSkipBacktester",
            "Backtester",
            "WaitForBacktester",
            "CheckBacktesterStatus",
            "BacktesterWait",
            "ExtractBacktesterError",
            # parity family (alpha-engine-config#6030 split)
            "CheckSkipParity",
            "ParityParallel",
            "AggregateParityBranchOutcomes",
            "CheckParityBranchOutcomes",
            # degrade-not-fail route (alpha-engine-config-I6025, now the
            # post-join branch-degraded fold)
            "ParityDegraded",
            "PublishParityDegraded",
            # compare/join quartet (alpha-engine-config#6030)
            "CheckSkipPitParityCompare",
            "PitParityCompare",
            "InitPitParityComparePollCount",
            "WaitForPitParityCompare",
            "CheckPitParityCompareStatus",
            "PitParityCompareWait",
            "PitParityComparePollWait",
            "MergePitParityComparePollCount",
            "PitParityCompareComplete",
            "ParityCompareDegraded",
            "PublishParityCompareDegraded",
        ],
    )
    def test_state_exists(self, states, name):
        assert name in states, f"{name} missing from Saturday SF States"

    def test_extract_parity_error_retired(self, states):
        """alpha-engine-config-I6025: Parity no longer fails the SF, so its
        error normalizer is gone — the degrade route owns the failure path."""
        assert "ExtractParityError" not in states

    def test_old_bundled_parity_quartet_retired(self, states):
        """alpha-engine-config#6030: the single bundled Parity quartet is
        gone — ParityParallel + PitParityCompare own the topology now."""
        for name in ("Parity", "WaitForParity", "CheckParityStatus", "ParityWait"):
            assert name not in states, f"{name} must not reappear post-#6030"

    @pytest.mark.parametrize("base", sorted(_PARITY_BRANCH_SPECS))
    def test_branch_quartet_states_exist(self, parity_branches, base):
        assert base in parity_branches, f"{base} branch missing from ParityParallel"
        b = parity_branches[base]
        for name in (
            f"CheckSkip{base}", base, f"Init{base}PollCount", f"WaitFor{base}",
            f"Check{base}Status", f"{base}Wait", f"{base}PollWait",
            f"Merge{base}PollCount", f"{base}Complete", f"{base}Skipped",
            f"{base}Degraded",
        ):
            assert name in b, f"{name} missing from the {base} branch"

    def test_no_standalone_backtest_state(self, states):
        """Lower-churn option chosen: there is intentionally NO separate
        `Backtest` state — the backtest stage stays in the kept-name
        `Backtester` state. This pins that decision so a future rename
        doesn't half-migrate."""
        assert "Backtest" not in states, (
            "Lower-churn naming option was chosen: the backtest stage lives "
            "in the kept `Backtester` state, not a new `Backtest` state."
        )


def marker_name_in(state: dict) -> bool:
    """True when `state` has any edge to MarkParityVerdictUnknownByCadence."""
    target = "MarkParityVerdictUnknownByCadence"
    edges = [state.get("Next"), state.get("Default")]
    edges += [c.get("Next") for c in state.get("Choices", []) or []]
    edges += [c.get("Next") for c in state.get("Catch", []) or []]
    return target in edges


class TestChainOrdering:
    """... → CheckSkipBacktester → Backtester (backtest stage) →
    WaitForBacktester → CheckBacktesterStatus(success) → CheckSkipParity →
    Parity → WaitForParity → CheckParityStatus(success) →
    CheckSkipEvaluator (existing downstream unchanged)."""

    def test_skip_backtester_default_runs_backtester(self, states):
        # config#2362 Option A: CheckSkipBacktester's Default now falls
        # through the additive CheckSkipBacktesterStageOnly gate before
        # Backtester (still runs Backtester on a scheduled/normal run).
        assert states["CheckSkipBacktester"]["Default"] == "CheckSkipBacktesterStageOnly"
        assert states["CheckSkipBacktesterStageOnly"]["Default"] == "Backtester"

    def test_skip_backtester_whole_pair_routes_to_evaluator_skipgate(self, states):
        """{"skip_backtester": true} keeps its original whole-pair semantics:
        skip BOTH backtest and parity, converging on CheckSkipEvaluator.

        alpha-engine-config-I11103: it now converges there VIA
        MarkParityVerdictUnknownByCadence instead of jumping straight to it.
        The semantics are untouched — parity still never runs on this edge —
        but the skip is now recorded rather than silent. See
        test_the_pair_skip_records_the_parity_verdict_as_unknown below."""
        choices = states["CheckSkipBacktester"]["Choices"]
        assert len(choices) == 1
        c = choices[0]
        variables = {cond["Variable"] for cond in c["And"]}
        assert variables == {"$.skip_backtester"}
        assert c["Next"] == "MarkParityVerdictUnknownByCadence"
        assert states["MarkParityVerdictUnknownByCadence"]["Next"] == "CheckSkipEvaluator"

    def test_the_pair_skip_records_the_parity_verdict_as_unknown(self, states):
        """alpha-engine-config-I11103, the defect this closes.

        CheckSkipParity is reached normally ONLY from the backtester family's
        own tail, so CheckSkipBacktester's skip arm — landing downstream of it
        — routed around MarkParityVerdictUnknownByCadence, the one state whose
        job is recording that parity was skipped by declaration.
        parity_verdict_unknown therefore stayed at its InitializeInput floor of
        false, which reads to a consumer as 'parity was evaluated and was
        fine'.

        MEASURED on watch-rerun-2026-09-18-3: 23 CheckSkip* gates entered,
        CheckSkipParity not among them, and the run terminated carrying
        parity_verdict_unknown=false on a cycle where every parity poll was
        not_set. weekly_sf_rerun.py sets skip_backtester whenever the source
        execution completed the backtester and re-applies skip_parity from the
        scheduled trigger's Input, so both flags are set on EVERY recovery —
        this was the normal case, not an edge one."""
        marker = states["MarkParityVerdictUnknownByCadence"]
        assert marker["ResultPath"] == "$.parity_verdict_unknown"
        assert marker["Result"] is True

        # Both declared ways of skipping parity must reach it. Rerouting this
        # arm through CheckSkipParity was the other candidate and is REJECTED:
        # with skip_parity unset it would RUN parity on a skip_backtester run,
        # inverting the config#2362 whole-pair contract asserted above.
        reaches = {
            n for n, st in states.items()
            if marker_name_in(st) 
        }
        assert {"CheckSkipBacktester", "CheckSkipParity"} <= reaches, (
            f"only {sorted(reaches)} route to the marker; both declared skips must"
        )

    def test_backtester_routes_to_wait_state(self, states):
        # alpha-engine-config-I5687: Backtester dispatches through the
        # poll-budget seed (InitBacktesterPollCount) before the first poll,
        # mirroring the DataPhase2/ThinkTank precedent.
        assert states["Backtester"]["Next"] == "InitBacktesterPollCount"
        assert states["InitBacktesterPollCount"]["Next"] == "WaitForBacktester"
        assert states["InitBacktesterPollCount"]["ResultPath"] == "$.backtester_polls"

    def test_backtester_wait_routes_to_status_check(self, states):
        assert states["WaitForBacktester"]["Next"] == "CheckBacktesterStatus"

    def test_backtester_success_routes_to_predictor_backtest(self, states):
        """L4472: Backtester (simulate-only) success hands off to the
        PredictorBacktest state, not directly to CheckSkipParity — the
        predictor+Phase4 block now runs in its own SF state so its runtime
        no longer sums into the simulate SSM command. config#830 inserted the
        CheckSkipPredictorBacktest skip-gate in front of PredictorBacktest
        (so mode=backtest-eval can bypass it); the gate's Default still runs
        PredictorBacktest, so the handoff is unchanged on scheduled runs."""
        success = [
            c["Next"]
            for c in states["CheckBacktesterStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["CheckSkipPredictorBacktest"], (
            "Backtester (simulate-only) success must hand off to the "
            "PredictorBacktest skip-gate — the L4472 phase-split runs "
            "predictor+Phase4 in its own state so a fresh simulate never "
            "carries the post-sweep stack into one SSM execution timeout."
        )
        assert states["CheckSkipPredictorBacktest"]["Default"] == "PredictorBacktest", (
            "CheckSkipPredictorBacktest must default to running PredictorBacktest "
            "so a normal Saturday run is unaffected by the config#830 skip-gate."
        )

    def test_backtester_status_loops_and_default(self, states):
        # alpha-engine-config-I5687: the loop-back branch is now a single
        # bounded And[] (IsPresent + Or[InProgress,Pending] + NumericLessThan
        # cap) rather than two bare StringEquals branches — the poll budget
        # bounds the loop, mirroring DataPhase2/ThinkTank.
        bounded = [
            c for c in states["CheckBacktesterStatus"]["Choices"]
            if "And" in c
        ]
        assert len(bounded) == 1
        variables = {cond.get("Variable") for cond in bounded[0]["And"]}
        assert "$.backtester_polls" in variables
        assert bounded[0]["Next"] == "BacktesterWait"
        assert states["BacktesterWait"]["Next"] == "BacktesterPollWait"
        assert states["BacktesterPollWait"]["Next"] == "MergeBacktesterPollCount"
        assert states["MergeBacktesterPollCount"]["Next"] == "WaitForBacktester"
        # config#6938: routed through the liveness gate, which separates a
        # reclaimed launcher from a Backtester failure.
        assert (
            states["CheckBacktesterStatus"]["Default"]
            == "BacktesterLivenessGate"
        )
        assert (
            states["BacktesterLivenessGate"]["Default"]
            == "ExtractBacktesterError"
        )

    def test_skip_parity_default_runs_parity(self, states):
        assert states["CheckSkipParity"]["Default"] == "ParityParallel"

    def test_skip_parity_honors_skip_flag(self, states):
        """{"skip_parity": true} must route to MarkParityVerdictUnknownByCadence
        and on to CheckSkipEvaluator (alpha-engine-config-I11103 inserted the
        marker between them without changing the skip topology)."""
        choices = states["CheckSkipParity"]["Choices"]
        assert len(choices) == 1
        c = choices[0]
        variables = {cond["Variable"] for cond in c["And"]}
        assert variables == {"$.skip_parity"}
        assert c["Next"] == "MarkParityVerdictUnknownByCadence"
        assert states["MarkParityVerdictUnknownByCadence"]["Next"] == "CheckSkipEvaluator"

    def test_parallel_join_chain(self, states):
        """ParityParallel → AggregateParityBranchOutcomes →
        CheckParityBranchOutcomes → (degraded fold | compare gate) →
        PitParityCompare → ... → CheckSkipEvaluator."""
        assert states["ParityParallel"]["Next"] == "AggregateParityBranchOutcomes"
        assert states["AggregateParityBranchOutcomes"]["Next"] == "CheckParityBranchOutcomes"
        cbo = states["CheckParityBranchOutcomes"]
        assert cbo["Default"] == "CheckSkipPitParityCompare"
        # alpha-engine-config-I7267: RESOURCE_KILL is checked FIRST (routes
        # to the shared hard-fail path, never reaching the compare) — the
        # pre-existing DEGRADED fold (still fail-open through the compare)
        # is now Choices[1].
        assert cbo["Choices"][0]["Next"] == "PitParityResourceKillDetected"
        resource_kill_vars = {c["Variable"] for c in cbo["Choices"][0]["Or"]}
        assert resource_kill_vars == {
            "$.parity_branch_outcomes.pit_lookahead_status",
            "$.parity_branch_outcomes.pit_walkforward_status",
            "$.parity_branch_outcomes.parity_replay_status",
        }
        for cond in cbo["Choices"][0]["Or"]:
            assert cond["StringEquals"] == "RESOURCE_KILL"
        assert states["PitParityResourceKillDetected"]["Next"] == "NormalizeFailureContext"

        assert cbo["Choices"][1]["Next"] == "ParityDegraded"
        degraded_vars = {c["Variable"] for c in cbo["Choices"][1]["Or"]}
        assert degraded_vars == {
            "$.parity_branch_outcomes.pit_lookahead_status",
            "$.parity_branch_outcomes.pit_walkforward_status",
            "$.parity_branch_outcomes.parity_replay_status",
        }
        for cond in cbo["Choices"][1]["Or"]:
            assert cond["StringEquals"] == "DEGRADED"

    def test_aggregate_hoists_all_three_branches(self, states):
        params = states["AggregateParityBranchOutcomes"]["Parameters"]
        assert params == {
            "pit_lookahead_status.$":
                "$.parity_parallel_result[0].branch_pit_lookahead.status",
            "pit_walkforward_status.$":
                "$.parity_parallel_result[1].branch_pit_walkforward.status",
            "parity_replay_status.$":
                "$.parity_parallel_result[2].branch_parity_replay.status",
        }
        # branch order in the Parallel matches the aggregate's indexing
        starts = [b["StartAt"] for b in states["ParityParallel"]["Branches"]]
        assert starts == [
            "CheckSkipPitParityLookahead",
            "CheckSkipPitParityWalkforward",
            "CheckSkipParityReplay",
        ]

    def test_compare_success_routes_to_existing_evaluator_skipgate(self, states):
        success = [
            c["Next"]
            for c in states["CheckPitParityCompareStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["PitParityCompareComplete"]
        assert states["PitParityCompareComplete"]["Next"] == "CheckSkipEvaluator"

    def test_compare_status_loops_bounded_and_degrades(self, states):
        check = states["CheckPitParityCompareStatus"]
        assert check["Default"] == "ParityCompareDegraded"
        # bounded poll loop (I5687 — the DataPhase2 shape, not the old
        # unbounded ParityWait)
        loop = [c for c in check["Choices"] if "And" in c]
        assert len(loop) == 1
        bound = [x for x in loop[0]["And"] if "NumericLessThan" in x]
        assert bound and bound[0]["NumericLessThan"] == 108
        assert loop[0]["Next"] == "PitParityCompareWait"
        assert states["PitParityCompareWait"]["Next"] == "PitParityComparePollWait"
        assert states["PitParityComparePollWait"]["Next"] == "MergePitParityComparePollCount"
        assert states["MergePitParityComparePollCount"]["Next"] == "WaitForPitParityCompare"

    @pytest.mark.parametrize("base", sorted(_PARITY_BRANCH_SPECS))
    def test_branch_quartet_wiring(self, parity_branches, base):
        """Per-branch: skip-gate → sendCommand → bounded poll loop →
        Complete/Skipped/Degraded terminals, all End:true (branch-level
        fail-open — a branch NEVER throws into the Parallel)."""
        flag, var, script, branch_key, poll_bound, _ = _PARITY_BRANCH_SPECS[base]
        b = parity_branches[base]
        gate = b[f"CheckSkip{base}"]
        assert gate["Default"] == base
        assert gate["Choices"][0]["Next"] == f"{base}Skipped"
        assert {c["Variable"] for c in gate["Choices"][0]["And"]} == {f"$.{flag}"}
        assert b[base]["Next"] == f"Init{base}PollCount"
        assert b[f"Init{base}PollCount"]["Next"] == f"WaitFor{base}"
        assert b[f"WaitFor{base}"]["Next"] == f"Check{base}Status"
        check = b[f"Check{base}Status"]
        if base in ("PitParityLookahead", "PitParityWalkforward"):
            # alpha-engine-config-I7267: a terminal non-Success now routes
            # through a marker-check for the RESOURCE_KILL classification
            # before falling to the existing *Degraded fail-open — see
            # test_sf_parity_resource_kill_halt_i7267.py for the full chain.
            assert check["Default"] == f"{base}ResourceKillCheck"
        else:
            assert check["Default"] == f"{base}Degraded"
        success = [c["Next"] for c in check["Choices"]
                   if c.get("StringEquals") == "Success"]
        assert success == [f"{base}Complete"]
        loop = [c for c in check["Choices"] if "And" in c]
        assert len(loop) == 1
        bound = [x for x in loop[0]["And"] if "NumericLessThan" in x]
        assert bound and bound[0]["NumericLessThan"] == poll_bound
        # every terminal ends the branch SUCCESS with the status recorded
        for terminal, status in (
            (f"{base}Complete", "OK"),
            (f"{base}Skipped", "SKIPPED"),
            (f"{base}Degraded", "DEGRADED"),
        ):
            st = b[terminal]
            assert st["Type"] == "Pass"
            assert st.get("End") is True, f"{terminal} must End the branch"
            # alpha-engine-config-I8194: the status envelope is nested
            # under branch_key INSIDE Parameters and ResultPath is gone,
            # so the branch returns ~40 bytes instead of its whole
            # effective input. $.parity_parallel_result[i].{branch_key}
            # .status still resolves exactly as before.
            assert st["Parameters"] == {branch_key: {"status": status}}
            assert "ResultPath" not in st

    def test_backtest_reachable_strictly_before_parity(self, sf, states):
        """Walk the HAPPY path from StartAt and assert Backtester (backtest
        stage) is visited strictly before Parity, and Parity strictly
        before the existing post-backtester state (CheckSkipEvaluator →
        Evaluator).

        Happy-path heuristic at a Choice: take the first forward edge —
        i.e. the first choice/Default target that is NOT an error/wait
        sink (Extract*Error, *Wait, HandleFailure, FailExecution). This
        generalizes over both the SSM-status gates (StringEquals
        "Success") and the Lambda-status gates (StringEquals "OK"/
        "SKIPPED", e.g. CheckResearchStatus), so the walk does not divert
        into the failure branch before reaching the Backtester chain."""

        def _is_sink(name: str) -> bool:
            return (
                name is None
                or name.startswith("Extract")
                or name.endswith("LivenessGate")  # config#6938: error-side branch
                or name.startswith("NormalizeFailureContext")
                or name.endswith("Wait")
                or name.endswith("RetryGate")
                or name.endswith("Reissue")
                or name.endswith("Degraded")
                or name in ("HandleFailure", "FailExecution")
                # alpha-engine-config-I11269: the readiness wait's not-yet
                # loop (the analogue of an InProgress/Pending poll edge).
                or name == "CheckCollectionReadinessBudget"
            )

        order: list[str] = []
        seen: set[str] = set()
        cur = sf["StartAt"]
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            order.append(cur)
            st = states[cur]
            if st.get("Type") == "Choice":
                default = st.get("Default")
                if not _is_sink(default):
                    # Skip-gate: Default = run the action (the no-skip
                    # happy path); the choices route AROUND the action.
                    cur = default
                else:
                    # Status-gate: Default is an error sink; the forward
                    # path is the first non-sink choice (Success / OK /
                    # SKIPPED), not the InProgress/Pending *Wait loops.
                    forward = [
                        c["Next"]
                        for c in st.get("Choices", [])
                        if not _is_sink(c.get("Next"))
                    ]
                    cur = forward[0] if forward else default
            else:
                cur = st.get("Next")
            if cur == "EvaluatorDiagnostics":
                order.append(cur)
                break
        assert "Backtester" in order, order
        assert "ParityParallel" in order, order
        assert "PitParityCompare" in order, order
        assert "EvaluatorDiagnostics" in order, order
        assert order.index("Backtester") < order.index("ParityParallel"), (
            "Backtester (backtest stage) must precede the parity family — "
            "the whole point of the split is a parity failure never re-runs "
            "the 121-min backtest."
        )
        assert order.index("ParityParallel") < order.index("PitParityCompare"), (
            "The compare/join must run AFTER the Parallel — each pass alone "
            "produces no usable artifact (the product is the delta)."
        )
        assert order.index("PitParityCompare") < order.index("EvaluatorDiagnostics"), (
            "The parity family must precede the existing post-backtester "
            "Evaluator — downstream chain ordering is preserved."
        )


class TestSsmCommandShape:
    """Backtester invokes --skip-stages=parity,evaluator (backtest stage
    only); Parity invokes --skip-stages=backtest,evaluator (parity stage
    only). The old combined --skip-stages=evaluator must NOT appear."""

    def _commands(self, states, name):
        from tests.sf_command_utils import extract_commands
        return extract_commands(states[name])

    def test_backtester_invokes_simulation_stage_only(self, states):
        """L4472: Backtester runs ONLY the simulation pipeline. Post
        alpha-engine-config-I4442/I4497 SF cutover (2026-08-09,
        crucible-backtester#631), it invokes its own dedicated
        spot_backtester.sh — the monolith + --mode flag it used to run is
        retained unchanged only as the rollback path, and the new script
        carries no stage-multiplexing flag at all (spot_backtester.sh does
        only the simulation stage; there is nothing left to skip)."""
        joined = " ".join(self._commands(states, "Backtester"))
        assert "spot_backtester.sh" in joined, (
            "Backtester must run its dedicated per-stage script post-cutover."
        )
        assert "spot_backtest.sh" not in joined, (
            "Backtester must not fall back to the shared monolith launcher."
        )
        assert "--skip-stages" not in joined
        assert "--mode=" not in joined

    @pytest.mark.parametrize("base", sorted(_PARITY_BRANCH_SPECS))
    def test_parity_branch_invokes_its_own_script(self, parity_branches, base):
        flag, var, script, branch_key, poll_bound, _ = _PARITY_BRANCH_SPECS[base]
        from tests.sf_command_utils import extract_commands
        joined = " ".join(extract_commands(parity_branches[base][base]))
        assert script in joined, f"{base} must run {script}"
        assert "spot_backtest.sh" not in joined
        assert "spot_parity.sh " not in joined
        assert "--skip-stages" not in joined
        assert "--pit-parity-enabled" not in joined

    def test_compare_invokes_compare_script(self, states):
        joined = " ".join(self._commands(states, "PitParityCompare"))
        assert "spot_parity_compare.sh" in joined
        assert "spot_backtest.sh" not in joined
        assert "--skip-stages" not in joined

    def test_bundled_spot_parity_script_unwired(self, sf):
        """alpha-engine-config#6030: the bundled spot_parity.sh launcher
        (pit_parity passes + replay in one) must no longer be invoked by ANY
        SF state — it survives on disk in crucible-backtester only as the
        rollback path (retirement: alpha-engine-config-I6725)."""
        blob = json.dumps(sf)
        # Command form, not bare name — state Comments legitimately narrate
        # the retired launcher's history.
        assert "bash infrastructure/spot_parity.sh" not in blob

    def test_no_combined_backtester_skip_stages_anywhere(self, sf):
        """The old single combined-Backtester invocation
        (--skip-stages=evaluator, runs backtest+parity together) must be
        gone everywhere in the SF — and post-cutover, the monolith
        spot_backtest.sh is not invoked by any state at all (each backtest-
        family state runs its own dedicated script; the monolith survives
        on disk only as the rollback path)."""
        blob = json.dumps(sf)
        assert "spot_backtest.sh --skip-stages=evaluator" not in blob, (
            "The old combined backtest+parity invocation reappeared — a "
            "parity failure would again re-run the 121-min backtest."
        )
        assert '"spot_backtest.sh' not in blob, (
            "No SF state should invoke the shared monolith launcher post "
            "alpha-engine-config-I4442/I4497 cutover — every backtest-family "
            "state has its own dedicated script now."
        )

    def test_backtester_command_starts_with_pipefail(self, states):
        cmds = self._commands(states, "Backtester")
        assert cmds[0].startswith("set ") and "pipefail" in cmds[0]

    def test_parity_family_commands_start_with_pipefail(self, states, parity_branches):
        from tests.sf_command_utils import extract_commands
        for base in _PARITY_BRANCH_SPECS:
            cmds = extract_commands(parity_branches[base][base])
            assert cmds[0].startswith("set ") and "pipefail" in cmds[0], base
        cmds = self._commands(states, "PitParityCompare")
        assert cmds[0].startswith("set ") and "pipefail" in cmds[0]

    @pytest.mark.parametrize(
        "state_kind,slug,script",
        [
            ("PitParityLookahead", "pit-lookahead", "spot_pit_lookahead.sh"),
            ("PitParityWalkforward", "pit-walkforward", "spot_pit_walkforward.sh"),
            ("ParityReplay", "parity-replay", "spot_parity_replay.sh"),
            ("PitParityCompare", "parity-compare", "spot_parity_compare.sh"),
        ],
    )
    def test_parity_family_log_capture_via_lib_cli(
            self, states, parity_branches, state_kind, slug, script):
        """Log-capture via the lib CLI form (lib v0.25.0), never an inline
        trap — same invariant the old Parity state carried, per stage."""
        from tests.sf_command_utils import extract_commands
        if state_kind in parity_branches:
            st = parity_branches[state_kind][state_kind]
        else:
            st = states[state_kind]
        cmds = extract_commands(st)
        work = next(c for c in cmds if "krepis.ssm_log_capture run" in c)
        assert f"--slug {slug}" in work
        assert f"--log /var/log/{slug}.log" in work
        assert f"-- bash infrastructure/{script}" in work
        assert not any(c.startswith("trap ") for c in cmds), (
            "Inline trap must not coexist with the lib CLI — the CLI "
            "internalizes the trap."
        )


class TestBudgetParity:
    """alpha-engine-config#6030: each split stage carries its OWN declared
    budget (sf-pipeline-policy §4 — sized per stage from the sf_budgets.py
    calibration block, no longer the old bundle's copied 7200), with the
    SF-state TimeoutSeconds strictly exceeding the inner SSM
    executionTimeout (the I6018 ordering rule, not replicated four times)."""

    @pytest.mark.parametrize("base", sorted(_PARITY_BRANCH_SPECS))
    def test_branch_budget_chain_ordering(self, parity_branches, base):
        _, _, _, _, _, exec_to = _PARITY_BRANCH_SPECS[base]
        st = parity_branches[base][base]
        inner = int(st["Parameters"]["Parameters"]["executionTimeout"][0])
        assert inner == int(exec_to)
        assert st["Parameters"]["TimeoutSeconds"] == inner
        assert st["TimeoutSeconds"] > inner, (
            f"{base}: SF state TimeoutSeconds must strictly exceed the SSM "
            "executionTimeout (I6018 ordering)"
        )
        assert st["TimeoutSeconds"] == inner + 60

    def test_compare_budget_chain_ordering(self, states):
        st = states["PitParityCompare"]
        inner = int(st["Parameters"]["Parameters"]["executionTimeout"][0])
        assert inner == 2700
        assert st["TimeoutSeconds"] == 2760

    def test_parity_family_retry_matches_backtester_convention(
            self, states, parity_branches):
        """The config#2279 4+2 jittered sendCommand ladder — unchanged."""
        expect = states["Backtester"]["Retry"]

        def _strip(r):
            return [{k: v for k, v in tier.items() if k != "Comment"} for tier in r]

        for base in _PARITY_BRANCH_SPECS:
            assert _strip(parity_branches[base][base]["Retry"]) == _strip(expect), base
        assert _strip(states["PitParityCompare"]["Retry"]) == _strip(expect)


class TestCatchSemantics:
    """alpha-engine-config-I6025 degrade-not-fail split:

    * Parity + WaitForParity Catch States.ALL → ParityDegraded (send/poll
      infra failure degrades the run — parity is observability and must not
      kill Evaluator/ReportCard/Director);
    * the kept Backtester state keeps its NormalizeFailureContext Catch
      (the backtest family still halts the SF on infra failure — the
      anti-auto-promote-garbage rule).
    """

    @pytest.mark.parametrize("base", sorted(_PARITY_BRANCH_SPECS))
    def test_branch_catches_route_to_branch_degraded(self, parity_branches, base):
        """Branch-level fail-open (alpha-engine-config#6030): both Task
        states of each branch Catch States.ALL → the branch's OWN degraded
        terminal — never NormalizeFailureContext, never a raw branch throw
        (an uncaught branch failure would make the SF Parallel kill the
        sibling branches — the §4 blast-radius violation this Catch
        prevents)."""
        _, var, _, _, _, _ = _PARITY_BRANCH_SPECS[base]
        b = parity_branches[base]
        for name in (base, f"WaitFor{base}"):
            catches = b[name]["Catch"]
            assert len(catches) >= 1
            for c in catches:
                assert c["ErrorEquals"] == ["States.ALL"]
                assert c["Next"] == f"{base}Degraded", (
                    f"{name}'s Catch must route to {base}Degraded "
                    "(branch-level fail-open, alpha-engine-config#6030)"
                )
                assert c["ResultPath"] == f"$.{var}_error"

    @pytest.mark.parametrize("name", ["PitParityCompare", "WaitForPitParityCompare"])
    def test_compare_catches_route_to_compare_degraded(self, states, name):
        catches = states[name]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == "ParityCompareDegraded"
            assert c["ResultPath"] == "$.pit_parity_compare_error"

    def test_parallel_backstop_catch_degrades(self, states):
        catches = states["ParityParallel"]["Catch"]
        assert len(catches) == 1
        assert catches[0]["ErrorEquals"] == ["States.ALL"]
        assert catches[0]["Next"] == "ParityDegraded"
        assert catches[0]["ResultPath"] == "$.parity_error"

    def test_backtester_still_catches_handle_failure(self, states):
        """Regression guard — the kept Backtester state must keep its
        NormalizeFailureContext Catch through this split."""
        catches = states["Backtester"]["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "NormalizeFailureContext"
            and c["ResultPath"] == "$.error"
            for c in catches
        )

    def test_parity_degraded_routes_to_publish_then_compare(self, states):
        """The branch-degraded fold: ParityDegraded → PublishParityDegraded
        → CheckSkipPitParityCompare — the SF CONTINUES *to the compare*
        (§2.3a: the compare must still run and emit verdict UNKNOWN for the
        missing pass artifacts; never HandleFailure, never straight to
        Evaluator around the join)."""
        assert states["ParityDegraded"]["Type"] == "Pass"
        assert states["ParityDegraded"]["Result"] is True
        assert states["ParityDegraded"]["ResultPath"] == "$.parity_degraded"
        assert_degraded_continuation(states, "ParityDegraded", "PublishParityDegraded")
        assert states["PublishParityDegraded"]["Next"] == "CheckSkipPitParityCompare"
        assert states["PublishParityDegraded"]["Catch"][0]["Next"] == "CheckSkipPitParityCompare"

    def test_compare_degraded_routes_to_publish_then_evaluator(self, states):
        assert states["ParityCompareDegraded"]["Type"] == "Pass"
        assert states["ParityCompareDegraded"]["Result"] is True
        assert states["ParityCompareDegraded"]["ResultPath"] == "$.parity_degraded"
        assert_degraded_continuation(
            states, "ParityCompareDegraded", "PublishParityCompareDegraded",
        )
        assert states["PublishParityCompareDegraded"]["Next"] == "CheckSkipEvaluator"
        assert states["PublishParityCompareDegraded"]["Catch"][0]["Next"] == "CheckSkipEvaluator"


class TestResultPathIsolation:
    """The parity-family states must not stomp on each other's (or
    Backtester's) SSM result paths — the three branches share the SAME
    execution-input copy semantics inside the Parallel, but their result
    paths must still be distinct for the aggregate/rerun diagnostics."""

    def test_distinct_result_paths(self, states, parity_branches):
        paths = {states["Backtester"]["ResultPath"],
                 states["PitParityCompare"]["ResultPath"]}
        for base in _PARITY_BRANCH_SPECS:
            paths.add(parity_branches[base][base]["ResultPath"])
        assert len(paths) == 5, f"result paths collide: {paths}"
        assert states["PitParityCompare"]["ResultPath"] == "$.pit_parity_compare_result"

    def test_waits_read_their_own_command_id(self, states, parity_branches):
        for base in _PARITY_BRANCH_SPECS:
            _, var, _, _, _, _ = _PARITY_BRANCH_SPECS[base]
            cmd_id = parity_branches[base][f"WaitFor{base}"]["Parameters"]["CommandId.$"]
            assert cmd_id == f"$.{var}_result.Command.CommandId", base
        cmd_id = states["WaitForPitParityCompare"]["Parameters"]["CommandId.$"]
        assert cmd_id == "$.pit_parity_compare_result.Command.CommandId"


class TestL4472PhaseSplit:
    """Pins the L4472 three-way split of the backtest stage: Backtester
    (simulate) → PredictorBacktest (predictor+Phase4) →
    PortfolioOptimizerBacktest (optimizer/cov/gamma) → CheckSkipParity.
    Each heavy block is its own SF state so no single SSM command carries
    the SUMMED 60-100 min runtime that blew the timeout on a fresh date."""

    def _commands(self, states, name):
        from tests.sf_command_utils import extract_commands
        return extract_commands(states[name])

    @pytest.mark.parametrize(
        "name",
        [
            "PredictorBacktest",
            "WaitForPredictorBacktest",
            "CheckPredictorBacktestStatus",
            "PredictorBacktestWait",
            "ExtractPredictorBacktestError",
            "PortfolioOptimizerBacktest",
            "WaitForPortfolioOptimizerBacktest",
            "CheckPortfolioOptimizerBacktestStatus",
            "PortfolioOptimizerBacktestWait",
            "ExtractPortfolioOptimizerBacktestError",
        ],
    )
    def test_new_state_exists(self, states, name):
        assert name in states, f"{name} missing — L4472 split incomplete"

    def test_chain_backtester_predictor_optimizer_parity(self, states):
        """Backtester → PredictorBacktest → PortfolioOptimizerBacktest →
        CheckSkipParity, each via its status gate's Success edge. config#830
        inserted skip-gates (CheckSkipPredictorBacktest /
        CheckSkipPortfolioOptimizerBacktest) in front of the predictor and
        optimizer stages; each defaults to running its stage, so the L4472
        chain ordering is preserved on a normal run."""
        def success(check):
            return [
                c["Next"] for c in states[check]["Choices"]
                if c.get("StringEquals") == "Success"
            ]
        assert success("CheckBacktesterStatus") == ["CheckSkipPredictorBacktest"]
        assert states["CheckSkipPredictorBacktest"]["Default"] == "PredictorBacktest"
        assert success("CheckPredictorBacktestStatus") == ["CheckSkipPortfolioOptimizerBacktest"]
        assert states["CheckSkipPortfolioOptimizerBacktest"]["Default"] == "PortfolioOptimizerBacktest"
        assert success("CheckPortfolioOptimizerBacktestStatus") == ["CheckSkipParity"]

    def test_predictor_backtest_invokes_predictor_mode(self, states):
        # Post I4442/I4497 cutover (crucible-backtester#631): dedicated
        # script, no --mode/--skip-stages/--no-pit-parity flags — the new
        # script's executable code carries no stage-multiplexing flag at all.
        joined = " ".join(self._commands(states, "PredictorBacktest"))
        assert "spot_predictor_backtest.sh" in joined
        assert "spot_backtest.sh" not in joined
        assert "--skip-stages" not in joined
        assert "--mode=" not in joined

    def test_optimizer_invokes_optimizer_mode_no_pit(self, states):
        joined = " ".join(self._commands(states, "PortfolioOptimizerBacktest"))
        assert "spot_portfolio_optimizer_backtest.sh" in joined
        assert "spot_backtest.sh" not in joined
        assert "--skip-stages" not in joined
        assert "--mode=" not in joined

    def test_pit_parity_passes_fire_exactly_once_each(self, sf, parity_branches):
        """alpha-engine-config#6030 successor of the L4486 exactly-once
        invariant: each pit_parity pass fires exactly once, in its OWN
        branch, and nowhere else in the SF; the compare fires exactly once
        after the join. Structural: WHICH script each state invokes."""
        blob = json.dumps(sf)
        for script in ("spot_pit_lookahead.sh", "spot_pit_walkforward.sh",
                       "spot_parity_replay.sh", "spot_parity_compare.sh"):
            assert blob.count(f"bash infrastructure/{script}") == 1, (
                f"{script} must be invoked by exactly one SF state"
            )

    def test_backtest_family_states_invoke_their_own_scripts(self, sf):
        """Kept half of the retired L4486 exactly-once test: each remaining
        backtest-family state invokes its OWN dedicated script (post
        I4442/I4497 cutover), never the monolith."""
        from tests.sf_command_utils import extract_commands
        states = sf["States"]
        family_scripts = {
            "Backtester": "spot_backtester.sh",
            "PredictorBacktest": "spot_predictor_backtest.sh",
            "PortfolioOptimizerBacktest": "spot_portfolio_optimizer_backtest.sh",
        }
        for name, script in family_scripts.items():
            joined = " ".join(extract_commands(states[name]))
            assert script in joined, f"{name} must invoke {script}"

    @pytest.mark.parametrize(
        "name",
        ["PredictorBacktest", "PortfolioOptimizerBacktest"],
    )
    def test_new_task_catches_handle_failure(self, states, name):
        # config#1819: routes through NormalizeFailureContext, not
        # HandleFailure directly (was HandleFailure pre-fix).
        catches = states[name]["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "NormalizeFailureContext"
            and c["ResultPath"] == "$.error"
            for c in catches
        )

    def test_new_states_distinct_result_paths(self, states):
        paths = {
            states["Backtester"]["ResultPath"],
            states["PredictorBacktest"]["ResultPath"],
            states["PortfolioOptimizerBacktest"]["ResultPath"],
        }
        assert len(paths) == 3, f"result paths collide: {paths}"
        assert states["PredictorBacktest"]["ResultPath"] == "$.predictor_backtest_result"
        assert states["PortfolioOptimizerBacktest"]["ResultPath"] == "$.portfolio_optimizer_result"

    @pytest.mark.parametrize(
        "check,wait",
        [
            ("CheckPredictorBacktestStatus", "PredictorBacktestWait"),
            ("CheckPortfolioOptimizerBacktestStatus", "PortfolioOptimizerBacktestWait"),
        ],
    )
    def test_new_status_gates_loop_and_error_default(self, states, check, wait):
        # alpha-engine-config-I5687: bounded And[] loop-back, mirrors
        # DataPhase2/ThinkTank/Backtester.
        prefix = {
            "CheckPredictorBacktestStatus": "predictor_backtest",
            "CheckPortfolioOptimizerBacktestStatus": "portfolio_optimizer",
        }[check]
        label = {
            "CheckPredictorBacktestStatus": "PredictorBacktest",
            "CheckPortfolioOptimizerBacktestStatus": "PortfolioOptimizerBacktest",
        }[check]
        bounded = [c for c in states[check]["Choices"] if "And" in c]
        assert len(bounded) == 1
        variables = {cond.get("Variable") for cond in bounded[0]["And"]}
        assert f"$.{prefix}_polls" in variables
        assert bounded[0]["Next"] == wait
        assert states[wait]["Next"] == f"{label}PollWait"
        assert states[f"{label}PollWait"]["Next"] == f"Merge{label}PollCount"
        assert states[f"Merge{label}PollCount"]["Next"] == f"WaitFor{label}"
        # config#6938: the non-Success arm now reaches the normalizer through
        # a liveness gate that separates a reclaimed launcher from a workload
        # failure. Assert the route reaches an Extract*, not that the first hop
        # is one.
        default = states[check]["Default"]
        if default.endswith("LivenessGate"):
            default = states[default]["Default"]
        assert default.startswith("Extract")

    def test_new_states_timeout_matches_backtester(self, states):
        """Each split state gets its own full SSM execution timeout — the
        point of the split is that none carries the summed runtime."""
        bt_to = states["Backtester"]["TimeoutSeconds"]
        bt_exec = states["Backtester"]["Parameters"]["Parameters"]["executionTimeout"]
        for name in ("PredictorBacktest", "PortfolioOptimizerBacktest"):
            assert states[name]["TimeoutSeconds"] == bt_to
            assert states[name]["Parameters"]["Parameters"]["executionTimeout"] == bt_exec
