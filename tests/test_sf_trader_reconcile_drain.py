"""The reconcile machine keeps the box up for the v2 trader's own reconcile
(alpha-engine-config-I12020).

ne-postclose-reconcile-pipeline boots the trading box, and that boot is when
crucible-executor's Persistent=true ``alpha-engine-trader-reconcile`` timer
catches up. On 2026-10-07 the machine stopped the box ~2 minutes after starting
it; the trader's reconcile had connected ~58s after the cold boot, before the
dockerised IB Gateway finished its login, and was filed as an expired session.
crucible-trader now waits, bounded, for the gateway port before connecting --
which only helps if the box is still up. ``DrainTraderReconcile`` is that
guarantee, and these tests pin its three properties:

1. Every non-failure route to ``StopTradingInstance`` passes the drain.
2. The drain FAILS OPEN toward the stop: no edge out of it reaches a halt
   state or leaves the box running, and an unsettled drain pages and degrades.
3. Its budget covers the trader's own readiness wait, and the SSM/state
   timeouts are ordered so the state never abandons a live command.
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
    into_drain = {n for n, st in STATES.items() if "DrainTraderReconcile" in _targets(st)}
    assert {"CheckEODStatus", "CheckSkipEODReconcile",
            "HealReplayDispatchFailed", "HealNonConvergent"} <= into_drain
    # HealConvergedNotify is deliberately NOT a feeder: after a successful
    # dispatch the replay owns the box and runs its OWN drain before its own
    # stop (tests/test_heal_replay_owns_the_trading_box.py).
    assert "HealConvergedNotify" not in into_drain


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


def _run(tmp_path: Path, states: list[str], jobs: list[int]) -> subprocess.CompletedProcess:
    """Run the drain's commands with `systemctl` answering ActiveState from
    ``states`` and the queued-job count from ``jobs`` (one entry per poll; the
    last repeats), and `sleep` a no-op."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (tmp_path / "states").write_text("\n".join(states) + "\n")
    (tmp_path / "jobs").write_text("\n".join(map(str, jobs)) + "\n")
    fake = f"""#!/usr/bin/env bash
state_dir={tmp_path}
next() {{
  f="$state_dir/$1"; n=$(wc -l < "$f")
  if [ "$n" -gt 1 ]; then head -n1 "$f"; tail -n +2 "$f" > "$f.t" && mv "$f.t" "$f"; else head -n1 "$f"; fi
}}
case "$1 $2" in
  "show -p")
    if [ "$3" = ActiveState ]; then next states; else echo success; fi ;;
  "list-jobs --no-legend")
    for _ in $(seq 1 "$(next jobs)"); do echo "1 {UNIT} start waiting"; done ;;
esac
"""
    for name, body in (("systemctl", fake), ("sleep", "#!/usr/bin/env bash\nexit 0\n")):
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
