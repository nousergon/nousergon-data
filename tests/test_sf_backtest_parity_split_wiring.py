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

This test catches regressions like:
- Someone reverts Backtester's SSM command back to --skip-stages=evaluator
  (re-bundles parity into the 121-min backtest task).
- Someone wires Parity BEFORE Backtester, or drops the Parity state.
- Someone reroutes CheckBacktesterStatus(success) past CheckSkipParity
  straight to CheckSkipEvaluator (silently drops parity).
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

    def test_backtester_success_routes_to_parity_skipgate(self, states):
        success = [
            c["Next"]
            for c in states["CheckBacktesterStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["CheckSkipParity"], (
            "Backtester (backtest stage) success must hand off to "
            "CheckSkipParity — Parity runs AFTER a completed backtest, and "
            "must never re-run the 121-min backtest on its own failure."
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

    def test_backtester_invokes_backtest_stage_only(self, states):
        joined = " ".join(self._commands(states, "Backtester"))
        assert "spot_backtest.sh --skip-stages=parity,evaluator" in joined, (
            "Backtester must run ONLY the backtest stage post-split — "
            "--skip-stages=evaluator re-bundles parity into the 121-min "
            "backtest task."
        )
        assert "--skip-stages=evaluator" not in joined
        assert "--skip-stages=backtest,evaluator" not in joined

    def test_parity_invokes_parity_stage_only(self, states):
        joined = " ".join(self._commands(states, "Parity"))
        assert "spot_backtest.sh --skip-stages=backtest,evaluator" in joined, (
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

    def test_parity_has_s3_log_trap_before_work(self, states):
        cmds = self._commands(states, "Parity")
        trap_idx = next(
            i
            for i, c in enumerate(cmds)
            if c.startswith("trap ")
            and "_ssm_logs" in c
            and "parity.log" in c
        )
        work_idx = next(
            i for i, c in enumerate(cmds) if "| tee /var/log/parity.log" in c
        )
        assert trap_idx < work_idx
        assert "|| true" in cmds[trap_idx]


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
