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

This test catches regressions like:
- Someone reverts Backtester's SSM command back to --skip-stages=evaluator
  (re-bundles parity into the backtest task) or drops --mode=param-sweep
  (re-bundles the heavy post-sweep phases back into one SSM command).
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


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


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
            # new parity quartet
            "CheckSkipParity",
            "Parity",
            "WaitForParity",
            "CheckParityStatus",
            "ParityWait",
            "ExtractParityError",
        ],
    )
    def test_state_exists(self, states, name):
        assert name in states, f"{name} missing from Saturday SF States"

    def test_no_standalone_backtest_state(self, states):
        """Lower-churn option chosen: there is intentionally NO separate
        `Backtest` state — the backtest stage stays in the kept-name
        `Backtester` state. This pins that decision so a future rename
        doesn't half-migrate."""
        assert "Backtest" not in states, (
            "Lower-churn naming option was chosen: the backtest stage lives "
            "in the kept `Backtester` state, not a new `Backtest` state."
        )


class TestChainOrdering:
    """... → CheckSkipBacktester → Backtester (backtest stage) →
    WaitForBacktester → CheckBacktesterStatus(success) → CheckSkipParity →
    Parity → WaitForParity → CheckParityStatus(success) →
    CheckSkipEvaluator (existing downstream unchanged)."""

    def test_skip_backtester_default_runs_backtester(self, states):
        assert states["CheckSkipBacktester"]["Default"] == "Backtester"

    def test_skip_backtester_whole_pair_routes_to_evaluator_skipgate(self, states):
        """{"skip_backtester": true} keeps its original whole-pair
        semantics: skip BOTH backtest and parity → CheckSkipEvaluator."""
        choices = states["CheckSkipBacktester"]["Choices"]
        assert len(choices) == 1
        c = choices[0]
        variables = {cond["Variable"] for cond in c["And"]}
        assert variables == {"$.skip_backtester"}
        assert c["Next"] == "CheckSkipEvaluator"

    def test_backtester_routes_to_wait_state(self, states):
        assert states["Backtester"]["Next"] == "WaitForBacktester"

    def test_backtester_wait_routes_to_status_check(self, states):
        assert states["WaitForBacktester"]["Next"] == "CheckBacktesterStatus"

    def test_backtester_success_routes_to_predictor_backtest(self, states):
        """L4472: Backtester (simulate-only) success hands off to the new
        PredictorBacktest state, not directly to CheckSkipParity — the
        predictor+Phase4 block now runs in its own SF state so its runtime
        no longer sums into the simulate SSM command."""
        success = [
            c["Next"]
            for c in states["CheckBacktesterStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["PredictorBacktest"], (
            "Backtester (simulate-only) success must hand off to "
            "PredictorBacktest — the L4472 phase-split runs predictor+Phase4 "
            "in its own state so a fresh simulate never carries the post-sweep "
            "stack into one SSM execution timeout."
        )

    def test_backtester_status_loops_and_default(self, states):
        nexts = {
            c["StringEquals"]: c["Next"]
            for c in states["CheckBacktesterStatus"]["Choices"]
        }
        assert nexts["InProgress"] == "BacktesterWait"
        assert nexts["Pending"] == "BacktesterWait"
        assert states["BacktesterWait"]["Next"] == "WaitForBacktester"
        assert (
            states["CheckBacktesterStatus"]["Default"]
            == "ExtractBacktesterError"
        )

    def test_skip_parity_default_runs_parity(self, states):
        assert states["CheckSkipParity"]["Default"] == "Parity"

    def test_skip_parity_honors_skip_flag(self, states):
        """{"skip_parity": true} must route to CheckSkipEvaluator
        (mirrors the skip_backtester / skip_evaluator shape)."""
        choices = states["CheckSkipParity"]["Choices"]
        assert len(choices) == 1
        c = choices[0]
        variables = {cond["Variable"] for cond in c["And"]}
        assert variables == {"$.skip_parity"}
        assert c["Next"] == "CheckSkipEvaluator"

    def test_parity_routes_to_wait_state(self, states):
        assert states["Parity"]["Next"] == "WaitForParity"

    def test_parity_wait_routes_to_status_check(self, states):
        assert states["WaitForParity"]["Next"] == "CheckParityStatus"

    def test_parity_success_routes_to_existing_evaluator_skipgate(self, states):
        """Parity success → CheckSkipEvaluator — the existing
        post-backtester state, UNCHANGED. This pins the original
        Backtester→CheckSkipEvaluator edge now lives off Parity."""
        success = [
            c["Next"]
            for c in states["CheckParityStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["CheckSkipEvaluator"], (
            "Parity success must hand off to the existing post-backtester "
            "state (CheckSkipEvaluator) — downstream chain is unchanged."
        )

    def test_parity_status_loops_and_default(self, states):
        nexts = {
            c["StringEquals"]: c["Next"]
            for c in states["CheckParityStatus"]["Choices"]
        }
        assert nexts["InProgress"] == "ParityWait"
        assert nexts["Pending"] == "ParityWait"
        assert states["ParityWait"]["Next"] == "WaitForParity"
        assert states["CheckParityStatus"]["Default"] == "ExtractParityError"

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
                or name.endswith("Wait")
                or name.endswith("RetryGate")
                or name.endswith("Reissue")
                or name in ("HandleFailure", "FailExecution")
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
            if cur == "Evaluator":
                order.append(cur)
                break
        assert "Backtester" in order, order
        assert "Parity" in order, order
        assert "Evaluator" in order, order
        assert order.index("Backtester") < order.index("Parity"), (
            "Backtester (backtest stage) must precede Parity — the whole "
            "point of the split is a parity failure never re-runs the "
            "121-min backtest."
        )
        assert order.index("Parity") < order.index("Evaluator"), (
            "Parity must precede the existing post-backtester Evaluator — "
            "downstream chain ordering is preserved."
        )


class TestSsmCommandShape:
    """Backtester invokes --skip-stages=parity,evaluator (backtest stage
    only); Parity invokes --skip-stages=backtest,evaluator (parity stage
    only). The old combined --skip-stages=evaluator must NOT appear."""

    def _commands(self, states, name):
        from tests.sf_command_utils import extract_commands
        return extract_commands(states[name])

    def test_backtester_invokes_simulation_stage_only(self, states):
        """L4472: Backtester runs ONLY the simulation pipeline via
        --mode=param-sweep, with --no-pit-parity (pit_parity belongs to
        PredictorBacktest). It must still skip the parity+evaluator stages."""
        joined = " ".join(self._commands(states, "Backtester"))
        assert "spot_backtest.sh --mode=param-sweep --no-pit-parity --skip-stages=parity,evaluator" in joined, (
            "Backtester must run --mode=param-sweep (simulation only) with "
            "--no-pit-parity post-L4472-split — dropping --mode re-bundles the "
            "heavy predictor/optimizer phases back into one SSM command."
        )
        assert "--skip-stages=evaluator" not in joined
        assert "--skip-stages=backtest,evaluator" not in joined

    def test_parity_invokes_parity_stage_only(self, states):
        joined = " ".join(self._commands(states, "Parity"))
        assert "spot_backtest.sh --pit-parity-enabled=1 --skip-stages=backtest,evaluator" in joined, (
            "Parity must run ONLY the parity stage."
        )
        assert "--skip-stages=evaluator" not in joined
        assert "--skip-stages=parity,evaluator" not in joined

    def test_no_combined_backtester_skip_stages_anywhere(self, sf):
        """The old single combined-Backtester invocation
        (--skip-stages=evaluator, runs backtest+parity together) must be
        gone everywhere in the SF."""
        blob = json.dumps(sf)
        assert "spot_backtest.sh --skip-stages=evaluator" not in blob, (
            "The old combined backtest+parity invocation reappeared — a "
            "parity failure would again re-run the 121-min backtest."
        )

    def test_backtester_command_starts_with_pipefail(self, states):
        cmds = self._commands(states, "Backtester")
        assert cmds[0].startswith("set ") and "pipefail" in cmds[0]

    def test_parity_command_starts_with_pipefail(self, states):
        cmds = self._commands(states, "Parity")
        assert cmds[0].startswith("set ") and "pipefail" in cmds[0]

    def test_parity_log_capture_via_lib_cli(self, states):
        """Parity's log-capture is satisfied by the lib CLI form
        (lib v0.25.0), not by an inline `trap 'aws s3 cp ...' EXIT`
        line. 2026-05-22 lift: the inline-trap form broke under
        `commands.$ States.Array(...)` ASL escape semantics (caught by
        Friday-PM dry-pass). See alpha-engine-lib PR #57 + sibling
        states in test_sf_morning_enrich_split_wiring.py.
        """
        cmds = self._commands(states, "Parity")
        work = next(
            c for c in cmds if "nousergon_lib.ssm_log_capture run" in c
        )
        assert "--slug parity" in work
        assert "--log /var/log/parity.log" in work
        assert "-- bash infrastructure/spot_backtest.sh --pit-parity-enabled=1 --skip-stages=backtest,evaluator" in work
        assert not any(c.startswith("trap ") for c in cmds), (
            "Inline trap must not coexist with the lib CLI — the CLI "
            "internalizes the trap."
        )


class TestBudgetParity:
    """Parity must keep the heavy budget pattern of the old combined
    Backtester (copied, not under-sized — per task spec)."""

    def test_parity_timeout_matches_backtester(self, states):
        assert (
            states["Parity"]["TimeoutSeconds"]
            == states["Backtester"]["TimeoutSeconds"]
        )

    def test_parity_ssm_execution_timeout_matches_backtester(self, states):
        bt = states["Backtester"]["Parameters"]["Parameters"]["executionTimeout"]
        pa = states["Parity"]["Parameters"]["Parameters"]["executionTimeout"]
        assert pa == bt
        assert states["Parity"]["Parameters"]["TimeoutSeconds"] == (
            states["Backtester"]["Parameters"]["TimeoutSeconds"]
        )

    def test_parity_retry_matches_backtester(self, states):
        assert states["Parity"]["Retry"] == states["Backtester"]["Retry"]


class TestCatchSemantics:
    """Both new Task states must Catch States.ALL → HandleFailure with
    ResultPath $.error, exactly like the Backtester quartet (the SF halts
    on infra failure of these states)."""

    @pytest.mark.parametrize("name", ["Parity", "WaitForParity"])
    def test_catch_routes_to_handle_failure(self, states, name):
        catches = states[name]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == "HandleFailure"
            assert c["ResultPath"] == "$.error"

    def test_backtester_still_catches_handle_failure(self, states):
        """Regression guard — the kept Backtester state must keep its
        HandleFailure Catch through this split."""
        catches = states["Backtester"]["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "HandleFailure"
            and c["ResultPath"] == "$.error"
            for c in catches
        )

    def test_parity_extract_error_routes_to_handle_failure(self, states):
        st = states["ExtractParityError"]
        assert st["Type"] == "Pass"
        assert st["ResultPath"] == "$.error"
        assert st["Next"] == "HandleFailure"
        assert st["Parameters"]["phase"] == "Parity"


class TestResultPathIsolation:
    """Parity must not stomp on Backtester's SSM result path."""

    def test_distinct_result_paths(self, states):
        assert (
            states["Parity"]["ResultPath"]
            != states["Backtester"]["ResultPath"]
        )
        assert states["Parity"]["ResultPath"] == "$.parity_result"

    def test_wait_reads_parity_command_id(self, states):
        cmd_id = states["WaitForParity"]["Parameters"]["CommandId.$"]
        assert "parity_result" in cmd_id


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
        CheckSkipParity, each via its status gate's Success edge."""
        def success(check):
            return [
                c["Next"] for c in states[check]["Choices"]
                if c.get("StringEquals") == "Success"
            ]
        assert success("CheckBacktesterStatus") == ["PredictorBacktest"]
        assert success("CheckPredictorBacktestStatus") == ["PortfolioOptimizerBacktest"]
        assert success("CheckPortfolioOptimizerBacktestStatus") == ["CheckSkipParity"]

    def test_predictor_backtest_invokes_predictor_mode(self, states):
        joined = " ".join(self._commands(states, "PredictorBacktest"))
        assert "spot_backtest.sh --mode=predictor-backtest --no-pit-parity --skip-stages=parity,evaluator" in joined
        # L4486: pit_parity NO LONGER runs here (it was stacked after the main
        # predictor_pipeline → 8 GB OOM-guard fail). Relocated to the Parity state.
        assert "--no-pit-parity" in joined

    def test_optimizer_invokes_optimizer_mode_no_pit(self, states):
        joined = " ".join(self._commands(states, "PortfolioOptimizerBacktest"))
        assert "spot_backtest.sh --mode=portfolio-optimizer-backtest --no-pit-parity --skip-stages=parity,evaluator" in joined

    def test_pit_parity_runs_exactly_once_in_parity_state(self, sf):
        """L4486: pit_parity fires EXACTLY ONCE, in the standalone Parity state
        (fresh process, ≥8 GB floor via --mode=all). A state runs pit_parity iff
        it neither passes --no-pit-parity NOR skips the `pit_parity` stage token.
        Backtester / PredictorBacktest / PortfolioOptimizerBacktest all pass
        --no-pit-parity; Parity does not, and its --skip-stages=backtest,evaluator
        does not contain pit_parity."""
        import re
        from tests.sf_command_utils import extract_commands
        states = sf["States"]

        def runs_pit_parity(name):
            cmd = " ".join(extract_commands(states[name]))
            # isolate the spot_backtest.sh invocation flags
            m = re.search(r"spot_backtest\.sh ([^']*)", cmd)
            flags = m.group(1) if m else ""
            if "--no-pit-parity" in flags:
                return False
            skip = re.search(r"--skip-stages=(\S+)", flags)
            skipped = skip.group(1).split(",") if skip else []
            return "pit_parity" not in skipped

        family = ["Backtester", "PredictorBacktest", "PortfolioOptimizerBacktest", "Parity"]
        runners = [n for n in family if runs_pit_parity(n)]
        assert runners == ["Parity"], (
            f"pit_parity must fire exactly once, in Parity; got runners={runners}"
        )

    @pytest.mark.parametrize(
        "name",
        ["PredictorBacktest", "PortfolioOptimizerBacktest"],
    )
    def test_new_task_catches_handle_failure(self, states, name):
        catches = states[name]["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "HandleFailure"
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
        nexts = {c["StringEquals"]: c["Next"] for c in states[check]["Choices"]}
        assert nexts["InProgress"] == wait
        assert nexts["Pending"] == wait
        assert states[check]["Default"].startswith("Extract")

    def test_new_states_timeout_matches_backtester(self, states):
        """Each split state gets its own full SSM execution timeout — the
        point of the split is that none carries the summed runtime."""
        bt_to = states["Backtester"]["TimeoutSeconds"]
        bt_exec = states["Backtester"]["Parameters"]["Parameters"]["executionTimeout"]
        for name in ("PredictorBacktest", "PortfolioOptimizerBacktest"):
            assert states[name]["TimeoutSeconds"] == bt_to
            assert states[name]["Parameters"]["Parameters"]["executionTimeout"] == bt_exec
