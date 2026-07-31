"""DataPhase2's budget chain must stay ordered across three files.

alpha-engine-config-I5759. DataPhase2 moved off a `lambda:invoke` (900s hard
maximum, and it needs ~33 min) onto the spot dispatch->poll quartet. Its
runtime bound is now expressed in three places that can drift independently:

    infrastructure/spot_data_weekly.sh   workload timeout + box watchdog
    infrastructure/step_function.json    SSM executionTimeout + SF TimeoutSeconds
    infrastructure/sf_budgets.py         the declared stage budget

The ordering is load-bearing, not cosmetic. If the workload budget ever meets
the box watchdog, systemd shuts the instance down mid-loop and the manifest is
never written — the alpha-engine-config-I5208 failure mode, reintroduced by
config drift. This is the same guard shape as
tests/test_rag_ingestion_news_budget_wiring.py.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from infrastructure.sf_budgets import STAGE_BUDGETS

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_JSON = _REPO_ROOT / "infrastructure" / "step_function.json"
_LAUNCHER = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"

# Measured 2026-07-30 on weekly execution 1e856026: 402 of 903 tickers in 900s
# of steady state. Two _finnhub_get calls per ticker x _FINNHUB_MIN_INTERVAL
# (1.1s) slept while holding _finnhub_lock = a serial floor no amount of
# ThreadPoolExecutor concurrency can move.
_MEASURED_SECONDS_PER_TICKER = 2.2
_UNIVERSE_AT_MEASUREMENT = 903


def _shell_const(name: str) -> int:
    text = _LAUNCHER.read_text()
    match = re.search(rf"^{re.escape(name)}=(\d+)", text, re.MULTILINE)
    assert match, f"{name} not found in {_LAUNCHER.name}"
    return int(match.group(1))


def _phase2_watchdog_seconds() -> int:
    """MAX_RUNTIME_SECONDS as set by the phase2-only default branch."""
    text = _LAUNCHER.read_text()
    match = re.search(
        r'if \[ "\$RUN_MODE" = "phase2-only" \].*?MAX_RUNTIME_SECONDS=(\d+)',
        text,
        re.DOTALL,
    )
    assert match, "the phase2-only MAX_RUNTIME_SECONDS default branch is missing"
    return int(match.group(1))


@pytest.fixture(scope="module")
def data_phase2_state() -> dict:
    sf = json.loads(_SF_JSON.read_text())
    branch_a = sf["States"]["ResearchPredictorParallel"]["Branches"][0]["States"]
    assert "DataPhase2" in branch_a, "DataPhase2 left Branch A"
    return branch_a["DataPhase2"]


def test_data_phase2_dispatches_to_spot_not_lambda(data_phase2_state):
    """The regression that started this. A lambda:invoke here CANNOT hold the
    workload: the floor is ~2000s and Lambda's maximum is 900s."""
    assert data_phase2_state["Resource"] == "arn:aws:states:::aws-sdk:ssm:sendCommand"
    commands = data_phase2_state["Parameters"]["Parameters"]["commands.$"]
    assert "spot_data_weekly.sh --phase2-only" in commands


def test_launcher_accepts_the_phase2_only_flag():
    text = _LAUNCHER.read_text()
    assert "--phase2-only) RUN_MODE=\"phase2-only\"" in text
    assert 'if [ "$RUN_MODE" = "phase2-only" ]; then' in text


def test_budget_chain_is_strictly_ordered(data_phase2_state):
    """workload < box watchdog < SSM executionTimeout <= SF TimeoutSeconds."""
    workload = _shell_const("PHASE2_ONLY_WORKLOAD_TIMEOUT_SECONDS")
    watchdog = _phase2_watchdog_seconds()
    execution_timeout = int(
        data_phase2_state["Parameters"]["Parameters"]["executionTimeout"][0]
    )
    sf_timeout = data_phase2_state["TimeoutSeconds"]

    assert workload < watchdog, (
        f"workload budget {workload}s >= box watchdog {watchdog}s — systemd "
        "would shut the box down mid-collection and the manifest would never "
        "be written (the alpha-engine-config-I5208 failure mode)."
    )
    assert watchdog < execution_timeout, (
        f"box watchdog {watchdog}s >= SSM executionTimeout {execution_timeout}s "
        "— SSM would give up before the box's own backstop fires, so a hung "
        "run would be reported as a command failure with no box-side log."
    )
    assert execution_timeout <= sf_timeout, (
        f"SSM executionTimeout {execution_timeout}s > SF TimeoutSeconds "
        f"{sf_timeout}s — the SF would abandon a command still legitimately "
        "running."
    )


def test_declared_execution_timeout_matches_the_shell_constant(data_phase2_state):
    """The shell's documentation constant and the SF must agree; a constant
    that drifts from the thing it documents is worse than no constant."""
    assert _shell_const("PHASE2_ONLY_EXECUTION_TIMEOUT_SECONDS") == int(
        data_phase2_state["Parameters"]["Parameters"]["executionTimeout"][0]
    )


def test_stage_budget_matches_the_sf_execution_timeout(data_phase2_state):
    budget = STAGE_BUDGETS["DataPhase2"]
    assert budget.current_timeout_seconds == int(
        data_phase2_state["Parameters"]["Parameters"]["executionTimeout"][0]
    )
    assert budget.pipeline_segment == "branch_a"


def test_workload_budget_covers_the_measured_serial_floor():
    """The budget must clear the floor with real headroom, and the headroom
    must be stated in tickers rather than left as a feeling."""
    workload = _shell_const("PHASE2_ONLY_WORKLOAD_TIMEOUT_SECONDS")
    floor = _UNIVERSE_AT_MEASUREMENT * _MEASURED_SECONDS_PER_TICKER
    assert workload > floor * 1.5, (
        f"workload budget {workload}s leaves under 1.5x headroom over the "
        f"measured {floor:.0f}s floor for {_UNIVERSE_AT_MEASUREMENT} tickers."
    )
    # How far the universe can grow before this budget is the binding
    # constraint. Stated so a future universe expansion trips a review here
    # rather than a Saturday timeout.
    headroom_tickers = int(workload / _MEASURED_SECONDS_PER_TICKER)
    assert headroom_tickers >= 1_600, (
        f"the workload budget covers only {headroom_tickers} tickers at the "
        f"measured {_MEASURED_SECONDS_PER_TICKER}s/ticker floor."
    )


def test_poll_bound_covers_the_execution_timeout(data_phase2_state):
    """The poll loop must be able to outlast the work it waits for, or the
    bound fails runs that were about to succeed."""
    sf = json.loads(_SF_JSON.read_text())
    branch_a = sf["States"]["ResearchPredictorParallel"]["Branches"][0]["States"]
    wait_seconds = branch_a["DataPhase2PollWait"]["Seconds"]
    bound = next(
        cond["NumericLessThan"]
        for choice in branch_a["CheckDataPhase2Status"]["Choices"]
        if "And" in choice
        for cond in choice["And"]
        if "NumericLessThan" in cond
    )
    execution_timeout = int(
        data_phase2_state["Parameters"]["Parameters"]["executionTimeout"][0]
    )
    assert bound * wait_seconds >= execution_timeout, (
        f"poll bound {bound} x {wait_seconds}s = {bound * wait_seconds}s is "
        f"below the SSM executionTimeout {execution_timeout}s — the loop would "
        "give up on a command SSM is still willing to run."
    )


def test_preflight_only_reaches_the_collector():
    """Friday shell-run dry path. --preflight-only must be forwarded to
    weekly_collector.py, which exits 0 before run_weekly() — the sole function
    performing any collector fetch or S3 write."""
    text = _LAUNCHER.read_text()
    assert 'PHASE2_COLLECTOR_ARGS="--preflight-only"' in text
    assert "weekly_collector.py --phase 2 ${PHASE2_COLLECTOR_ARGS}" in text


def test_preflight_only_does_not_emit_a_heartbeat():
    """A preflight is not a completed collection. Emitting the heartbeat on
    the dry path would make the Friday shell run indistinguishable from a real
    Saturday collection to every freshness monitor watching that dimension."""
    text = _LAUNCHER.read_text()
    phase2_block = text.split('if [ "$RUN_MODE" = "phase2-only" ]; then', 1)[1]
    phase2_block = phase2_block.split("\n# ── Full / data-only", 1)[0]
    guard_index = phase2_block.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then\n        echo "Preflight-only run')
    heartbeat_index = phase2_block.index('--dimensions "Process=data-phase2"')
    assert guard_index < heartbeat_index, (
        "the preflight guard must precede the heartbeat emission"
    )
