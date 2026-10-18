"""The reconcile machine keeps the box up for the v2 trader's own reconcile
(alpha-engine-config-I12020).

ne-postclose-reconcile-pipeline boots the trading box, and that boot is when
crucible-executor's Persistent=true ``alpha-engine-trader-reconcile`` timer
catches up. On 2026-10-07 the machine stopped the box ~2 minutes after starting
it; the trader's reconcile had connected ~58s after the cold boot, before the
dockerised IB Gateway finished its login, and was filed as an expired session.
crucible-trader now waits, bounded, for the gateway port before connecting --
which only helps if the box is still up. ``DrainTraderReconcile`` is that
guarantee, and these tests pin its four properties:

1. Every non-failure route to ``StopTradingInstance`` passes the drain.
2. The drain FAILS OPEN toward the stop: no edge out of it reaches a halt
   state or leaves the box running, and an unsettled drain pages and degrades.
3. Its budget covers the trader's own readiness wait, and the SSM/state
   timeouts are ordered so the state never abandons a live command.
4. Settling is not success (alpha-engine-config-I12292). On 2026-10-12, -13
   and -14 the drain printed ``settled: ActiveState=failed Result=exit-code``,
   exited 0, and the pipeline SUCCEEDED with no trader reconcile behind it. A
   unit that settled failed now exits 3, which the machine routes to its own
   degraded reason, an immediate page and the ``DegradedRun`` terminal.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
RECONCILE = REPO / "infrastructure" / "step_function_eod_reconcile.json"
STATES = json.loads(RECONCILE.read_text(encoding="utf-8"))["States"]

UNIT = "alpha-engine-trader-reconcile.service"
DRAIN = (
    "DrainTraderReconcile", "WaitForTraderReconcileDrain", "CheckTraderReconcileDrainStatus",
    "TraderReconcileDrainWait", "ExtractTraderReconcileDrainStatusError",
    "SetTraderReconcileDrainDegraded", "PublishTraderReconcileDrainUnsettled",
    # alpha-engine-config-I12292: the unit settled, but failed.
    "SetTraderReconcileUnitFailedDegraded",
)
HALTS = {"HandleFailure", "ForceStopInstance", "FailExecution"}

#: crucible-trader `broker_session.READY_DEADLINE_S`: how long the trader waits
#: for the gateway port before it gives up. The drain must outlast it.
TRADER_READY_DEADLINE_S = 300


def _targets(state: dict) -> list[str]:
    out = [state[k] for k in ("Next", "Default") if k in state]
    out += [c["Next"] for c in state.get("Choices", [])]
    out += [c["Next"] for c in state.get("Catch", [])]
    return out


def _commands() -> list[str]:
    return STATES["DrainTraderReconcile"]["Parameters"]["Parameters"]["commands"]


def _budget_s() -> int:
    line = next(c for c in _commands() if c.startswith("deadline=$((SECONDS+"))
    return int(line.removeprefix("deadline=$((SECONDS+").removesuffix("))"))


def test_every_route_into_the_stop_comes_through_the_drain():
    feeders = {n for n, st in STATES.items() if "StopTradingInstance" in _targets(st)}
    assert feeders <= set(DRAIN), sorted(feeders - set(DRAIN))
    # And the drain is actually entered from the work paths, not orphaned.
    # Since 2026-10-09 the work paths enter it through the box-ownership
    # check (tests/test_sf_pre_session_window_and_box_ownership.py): an owned
    # box is drained and stopped, a box another actor started is neither.
    into_drain = {n for n, st in STATES.items() if "DrainTraderReconcile" in _targets(st)}
    assert into_drain == {"CheckBoxOwnedBeforeStop"}
    into_guard = {n for n, st in STATES.items() if "CheckBoxOwnedBeforeStop" in _targets(st)}
    assert {"CheckEODStatus", "CheckSkipEODReconcile",
            "HealReplayDispatchFailed", "HealNonConvergent"} <= into_guard
    # HealConvergedNotify is deliberately NOT a feeder: after a successful
    # dispatch the replay owns the box and runs its OWN drain before its own
    # stop (tests/test_heal_replay_owns_the_trading_box.py).
    assert "HealConvergedNotify" not in into_drain | into_guard


def test_the_drain_fails_open_toward_the_stop():
    for name in DRAIN:
        targets = set(_targets(STATES[name]))
        assert not targets & HALTS, (name, targets)
        assert targets <= set(DRAIN) | {"StopTradingInstance"}, (name, targets)
    for name in ("DrainTraderReconcile", "WaitForTraderReconcileDrain",
                 "PublishTraderReconcileDrainUnsettled"):
        catches = [c for c in STATES[name]["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1, name


def test_an_unsettled_drain_degrades_and_pages():
    check = STATES["CheckTraderReconcileDrainStatus"]
    assert check["Choices"][0] == {
        "Variable": "$.trader_reconcile_drain_poll.Status",
        "StringEquals": "Success",
        "Next": "StopTradingInstance",
    }
    assert check["Default"] == "ExtractTraderReconcileDrainStatusError"
    # Only exit 1 (the budget) and the SSM terminals reach the unsettled
    # reason; exit 3 is the settled-but-failed unit (see below).
    assert [c["Next"] for c in check["Choices"]].count("ExtractTraderReconcileDrainStatusError") == 0
    flag = STATES["SetTraderReconcileDrainDegraded"]
    assert flag["ResultPath"] == "$.degraded_summary"
    assert flag["Parameters"]["degraded"] is True
    assert flag["Next"] == "PublishTraderReconcileDrainUnsettled"
    assert STATES["PublishTraderReconcileDrainUnsettled"]["Resource"] == "arn:aws:states:::sns:publish"


def test_the_budget_outlasts_the_trader_wait_and_the_timeouts_are_ordered():
    budget = _budget_s()
    execution = int(STATES["DrainTraderReconcile"]["Parameters"]["Parameters"]["executionTimeout"][0])
    state_timeout = STATES["DrainTraderReconcile"]["TimeoutSeconds"]
    # The trader's readiness wait plus provisioning, its venv build and the
    # reconcile itself: at least twice the wait, never merely equal to it.
    assert budget >= 2 * TRADER_READY_DEADLINE_S
    assert budget < execution < state_timeout
    doc = json.loads(RECONCILE.read_text(encoding="utf-8"))
    assert doc["TimeoutSeconds"] > state_timeout


def test_the_unit_it_drains_is_the_trader_reconcile_unit():
    assert any(c == f"unit={UNIT}" for c in _commands())
    systemd = REPO.parent / "crucible-executor" / "infrastructure" / "systemd"
    if not systemd.is_dir():
        pytest.skip("crucible-executor checkout not present")
    assert (systemd / UNIT).is_file()
    assert "Persistent=true" in (systemd / UNIT.replace(".service", ".timer")).read_text()


# ── the script itself, against a fake systemctl ──────────────────────────────


def _run(
    tmp_path: Path, states: list[str], jobs: list[int], result: str = "success"
) -> subprocess.CompletedProcess:
    """Run the drain's commands with `systemctl` answering ActiveState from
    ``states`` and the queued-job count from ``jobs`` (one entry per poll; the
    last repeats), Result as ``result``, `journalctl` printing a fixed tail,
    and `sleep` a no-op."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (tmp_path / "states").write_text("\n".join(states) + "\n")
    (tmp_path / "jobs").write_text("\n".join(map(str, jobs)) + "\n")
    (tmp_path / "result").write_text(result + "\n")
    fake = f"""#!/usr/bin/env bash
state_dir={tmp_path}
next() {{
  f="$state_dir/$1"; n=$(wc -l < "$f")
  if [ "$n" -gt 1 ]; then head -n1 "$f"; tail -n +2 "$f" > "$f.t" && mv "$f.t" "$f"; else head -n1 "$f"; fi
}}
case "$1 $2" in
  "show -p")
    if [ "$3" = ActiveState ]; then next states; else cat "$state_dir/result"; fi ;;
  "list-jobs --no-legend")
    count=$(next jobs)
    for ((i=0; i<count; i++)); do echo "1 {UNIT} start waiting"; done ;;
esac
"""
    journal = "#!/usr/bin/env bash\necho \"journal: $*\"\necho 'trader-provision.sh: boom'\n"
    for name, body in (("systemctl", fake), ("sleep", "#!/usr/bin/env bash\nexit 0\n"),
                       ("journalctl", journal)):
        path = bindir / name
        path.write_text(body)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}
    return subprocess.run(["bash", "-c", "\n".join(_commands())], env=env,
                          capture_output=True, text=True, timeout=30)


def test_a_settled_unit_returns_at_once(tmp_path):
    result = _run(tmp_path, ["inactive"], [0])
    assert result.returncode == 0, result.stderr
    assert "settled: ActiveState=inactive" in result.stdout


def test_it_waits_for_a_running_then_a_queued_unit(tmp_path):
    result = _run(tmp_path, ["activating", "activating", "inactive", "inactive"], [0, 0, 1, 0])
    assert result.returncode == 0, result.stderr
    assert "settled" in result.stdout
    # Three polls went by before it settled: two activating, one queued.
    assert (tmp_path / "states").read_text().strip() == "inactive"


def test_the_unsettled_diagnostic_goes_to_stderr_and_fails():
    line = next(c for c in _commands() if "stopping the box anyway" in c)
    assert line.rstrip().endswith(">&2")
    assert "exit 1" in _commands()[_commands().index(line) + 1]


# ── alpha-engine-config-I12292: settled is not succeeded ────────────────────
#
# The three settled shapes the drain can see, run through the real script and
# then through the machine's Choice, so the two halves are graded together.

UNIT_FAILED_EXIT = 3


def _route(status: str, response_code: int) -> str:
    """Where CheckTraderReconcileDrainStatus sends a poll, evaluated by hand
    over the Choice rules this definition uses (StringEquals, IsNumeric,
    NumericEquals, And)."""
    poll = {"Status": status, "ResponseCode": response_code}

    def holds(rule: dict) -> bool:
        if "And" in rule:
            return all(holds(r) for r in rule["And"])
        field = rule["Variable"].removeprefix("$.trader_reconcile_drain_poll.")
        value = poll[field]
        if "StringEquals" in rule:
            return value == rule["StringEquals"]
        if "IsNumeric" in rule:
            return isinstance(value, (int, float)) is rule["IsNumeric"]
        if "NumericEquals" in rule:
            return value == rule["NumericEquals"]
        raise AssertionError(f"rule shape not modelled here: {rule}")

    check = STATES["CheckTraderReconcileDrainStatus"]
    return next((c["Next"] for c in check["Choices"] if holds(c)), check["Default"])


def _ssm_status(returncode: int) -> str:
    """AWS-RunShellScript: exit 0 is Success, any other exit is Failed."""
    return "Success" if returncode == 0 else "Failed"


def _degraded_reason_after(entry: str) -> str | None:
    """Follow Next edges from ``entry`` to StopTradingInstance and return the
    degraded reason the walk stamps on $.degraded_summary, if any."""
    reason, name = None, entry
    while name != "StopTradingInstance":
        state = STATES[name]
        params = state.get("Parameters")
        if state.get("ResultPath") == "$.degraded_summary" and isinstance(params, dict):
            reason = params["reason"]
        name = state["Next"]
    return reason


def test_shape_success_a_unit_that_settled_successfully_stops_cleanly(tmp_path):
    result = _run(tmp_path, ["inactive"], [0], result="success")
    assert result.returncode == 0, result.stderr
    assert "settled: ActiveState=inactive Result=success" in result.stdout
    assert _route(_ssm_status(result.returncode), result.returncode) == "StopTradingInstance"


def test_shape_failed_exit_code_is_a_degraded_run_with_its_own_reason(tmp_path):
    """The 2026-10-12/13/14 shape: ActiveState=failed Result=exit-code."""
    result = _run(tmp_path, ["failed"], [0], result="exit-code")
    assert result.returncode == UNIT_FAILED_EXIT, (result.stdout, result.stderr)
    # The diagnostic is on stderr, the only stream WaitForTraderReconcileDrain maps.
    assert "settled FAILED: ActiveState=failed Result=exit-code" in result.stderr
    assert "trader-provision.sh: boom" in result.stderr
    assert "-u alpha-engine-trader-reconcile.service" in result.stderr
    assert "settled:" not in result.stdout
    entry = _route(_ssm_status(result.returncode), result.returncode)
    assert entry == "SetTraderReconcileUnitFailedDegraded"
    assert _degraded_reason_after(entry) == "eod_trader_reconcile_unit_failed"


@pytest.mark.parametrize("state,unit_result", [
    ("failed", "timeout"),
    ("failed", "signal"),
    # A unit that went back to inactive but whose last run did not succeed.
    ("inactive", "exit-code"),
])
def test_shape_timed_out_or_otherwise_unsuccessful_is_the_same_degraded_run(
    tmp_path, state, unit_result
):
    result = _run(tmp_path, [state], [0], result=unit_result)
    assert result.returncode == UNIT_FAILED_EXIT, (result.stdout, result.stderr)
    assert f"ActiveState={state} Result={unit_result}" in result.stderr
    entry = _route(_ssm_status(result.returncode), result.returncode)
    assert _degraded_reason_after(entry) == "eod_trader_reconcile_unit_failed"


def test_shape_not_settled_keeps_its_own_reason(tmp_path):
    """Never settled inside the budget: exit 1, the pre-existing unsettled
    reason, distinct from a unit that settled and failed."""
    commands = _commands()
    # Shrink the budget so a unit stuck in `activating` exhausts it at once.
    script = "\n".join(c.replace("deadline=$((SECONDS+1080))", "deadline=$((SECONDS+0))")
                        for c in commands)
    bindir = tmp_path / "bin"
    _run(tmp_path, ["inactive"], [0])  # builds the fakes under tmp_path/bin
    (tmp_path / "states").write_text("activating\n")
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}
    result = subprocess.run(["bash", "-c", script], env=env, capture_output=True,
                            text=True, timeout=30)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert "after 1080s; stopping the box anyway" in result.stderr
    entry = _route(_ssm_status(result.returncode), result.returncode)
    assert entry == "ExtractTraderReconcileDrainStatusError"
    assert _degraded_reason_after(entry) == "eod_trader_reconcile_drain_unsettled"


def test_ssm_terminals_other_than_failed_still_read_as_unsettled():
    for status in ("TimedOut", "Cancelled", "Cancelling"):
        assert _route(status, -1) == "ExtractTraderReconcileDrainStatusError", status


def test_the_unit_failed_route_degrades_pages_and_still_stops_the_box():
    flag = STATES["SetTraderReconcileUnitFailedDegraded"]
    assert flag["Type"] == "Pass"
    assert flag["ResultPath"] == "$.degraded_summary"
    assert flag["Parameters"]["degraded"] is True
    assert flag["Parameters"]["reason"] == "eod_trader_reconcile_unit_failed"
    # The poll carries the settled state and the journal tail into the
    # DegradedRun cause, which derives from $.degraded_summary.
    assert flag["Parameters"]["stage_error.$"] == "$.trader_reconcile_drain_poll"
    # Pages at once (sf-pipeline-policy.md §5) through the drain's own
    # publish, whose own Catch also lands on the stop.
    assert flag["Next"] == "PublishTraderReconcileDrainUnsettled"
    assert "FAILED" in flag["Parameters"]["page_subject"]
    # And the stop's tail reaches the degraded terminal for this flag.
    degraded = STATES["CheckDegradedOutcome"]["Choices"][0]
    assert degraded["Next"] == "WriteCompletionMarkerDegraded"
    assert STATES["WriteCompletionMarkerDegraded"]["Next"] == "DegradedRun"


def test_the_exit_code_the_script_uses_is_the_one_the_choice_reads():
    assert f"exit {UNIT_FAILED_EXIT}" in [c.strip() for c in _commands()]
    rule = next(c for c in STATES["CheckTraderReconcileDrainStatus"]["Choices"]
                if c["Next"] == "SetTraderReconcileUnitFailedDegraded")
    assert {"Variable": "$.trader_reconcile_drain_poll.ResponseCode",
            "NumericEquals": UNIT_FAILED_EXIT} in rule["And"]


def test_the_drain_page_names_whichever_outcome_reached_it():
    """One publish state serves both degraded outcomes of the drain, so its
    Subject and Message must come from the setter that routed there -- a
    static "NOT DRAINED" subject would mislabel a unit that settled FAILED."""
    page = STATES["PublishTraderReconcileDrainUnsettled"]["Parameters"]
    assert "Subject" not in page
    assert page["Subject.$"] == "$.degraded_summary.page_subject"
    assert "$.degraded_summary.page_detail" in page["Message.$"]
    assert "$.degraded_summary.stage_error" in page["Message.$"]
    feeders = {n for n, st in STATES.items()
               if "PublishTraderReconcileDrainUnsettled" in _targets(st)}
    assert feeders == {"SetTraderReconcileDrainDegraded", "SetTraderReconcileUnitFailedDegraded"}
    subjects = set()
    for name in feeders:
        params = STATES[name]["Parameters"]
        assert params["page_detail"].strip(), name
        # SNS: a subject is at most 100 ASCII characters, one line.
        subject = params["page_subject"]
        assert 0 < len(subject) <= 100 and subject.isascii() and "\n" not in subject, name
        subjects.add(subject)
    assert len(subjects) == 2
