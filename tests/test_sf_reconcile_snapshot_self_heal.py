"""The post-close reconcile captures a MISSING snapshot instead of failing on it.

alpha-engine-config-I12220 (Brian, 2026-10-10: "we should make this failure
heal itself"). On 2026-10-09 the 22:30Z ``ne-postclose-trading-pipeline``
backstop cold-started the trading box and ran CaptureSnapshot ~25 s later. Its
~30 s IB connect retry met a closed API port and then a gateway still at the
paper-trading disclaimer, so ``trades/snapshots/2026-10-09.json`` was never
written. At 00:26Z ``ne-postclose-reconcile-pipeline`` reached EODReconcile,
which has no live-IB fallback, and failed. Brian captured it by hand at 01:32Z.

Two halves of the fix, both pinned here:

* the reconcile machine runs its own presence-gated ``CaptureSnapshot`` right
  after the deploy refresh and before the collection wait, the heal loop and
  EODReconcile (graph tests below);
* the command it sends does nothing when the snapshot exists, refuses after
  ET midnight, and otherwise runs ``snapshot_capturer.py``, whose readiness
  wait (crucible-executor, ``GATEWAY_READY_TIMEOUT_S`` = 420 s) the SSM
  ``executionTimeout`` must cover (behavioural shell tests below: the real
  command is rendered from the definition and run against stub ``aws``,
  ``date`` and ``python`` binaries).
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_RECONCILE = _REPO_ROOT / "infrastructure" / "step_function_eod_reconcile.json"
_POSTCLOSE = _REPO_ROOT / "infrastructure" / "step_function_eod.json"

#: crucible-executor executor/snapshot_capturer.py::GATEWAY_READY_TIMEOUT_S. The
#: SSM command must outlive the readiness wait plus the capture itself, or SSM
#: kills a capture that was about to succeed on a cold box.
_EXECUTOR_GATEWAY_READY_TIMEOUT_S = 420
_CAPTURE_HEADROOM_S = 120

_HEAL = ("CaptureSnapshot", "WaitForCaptureSnapshot", "CheckSnapshotStatus")


def _doc(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def reconcile() -> dict:
    return _doc(_RECONCILE)


@pytest.fixture(scope="module")
def states(reconcile) -> dict:
    return reconcile["States"]


def _targets(st: dict) -> list[str]:
    out = [st[k] for k in ("Next", "Default") if k in st]
    out += [c["Next"] for c in st.get("Choices", [])]
    out += [c["Next"] for c in st.get("Catch", [])]
    return out


def _reachable(states: dict, start: str, blocked: set[str] = frozenset()) -> set[str]:
    seen, stack = set(), [start]
    while stack:
        cur = stack.pop()
        if cur in seen or cur in blocked or cur not in states:
            continue
        seen.add(cur)
        stack.extend(_targets(states[cur]))
    return seen


# ── where the self-heal sits ─────────────────────────────────────────────────


def test_every_route_to_eodreconcile_passes_the_self_heal(reconcile, states):
    """Snapshot missing -> the capture runs BEFORE EODReconcile: with the
    capture removed, nothing reaches EODReconcile (or the heal loop that
    dispatches the replay that runs it)."""
    for downstream in ("EODReconcile", "CheckSkipPostMarketData", "HealDispatchReplay"):
        assert downstream in _reachable(states, reconcile["StartAt"])
        assert downstream not in _reachable(states, reconcile["StartAt"], blocked={"CaptureSnapshot"})


def test_only_a_successful_capture_poll_continues_the_pipeline(states):
    ok = [c["Next"] for c in states["CheckSnapshotStatus"]["Choices"]
          if c.get("StringEquals") == "Success"]
    assert ok == ["CheckSkipPostMarketData"]
    preds = {name for name, st in states.items() if "CheckSkipPostMarketData" in _targets(st)}
    assert preds == {"CheckSnapshotStatus"}


def test_the_capture_runs_on_refreshed_code(states):
    """The readiness wait lives in executor code, so the capture comes after
    the deploy refresh (done or operator-skipped), never before it."""
    ok = [c["Next"] for c in states["CheckRefreshExecutorDeployStatus"]["Choices"]
          if c.get("StringEquals") == "Success"]
    assert ok == ["CaptureSnapshot"]
    assert states["CheckSkipRefreshExecutorDeploy"]["Choices"][0]["Next"] == "CaptureSnapshot"
    preds = {name for name, st in states.items() if "CaptureSnapshot" in _targets(st)}
    assert preds == {"CheckRefreshExecutorDeployStatus", "CheckSkipRefreshExecutorDeploy"}


def test_the_capture_is_behind_the_market_hours_and_pre_session_gates(reconcile, states):
    """nousergon-data#2129: no box start (and so no capture) in session or in
    the [08:00, 09:30) ET pre-session window. Every route to the capture
    passes the market-hours gate, then either the pre-session window check or
    the gate's validated operator override (RecordMarketHoursOverride)."""
    start = reconcile["StartAt"]
    assert start == "MarketHoursGate"
    assert "CaptureSnapshot" not in _reachable(states, start, blocked={"MarketHoursGate"})
    assert "CaptureSnapshot" not in _reachable(
        states, start, blocked={"PreSessionWindowChoice", "RecordMarketHoursOverride"})


def test_the_capture_never_stops_a_box_it_does_not_own(states):
    """nousergon-data#2127/#2129: every stop reachable from the capture,
    success or failure, goes through an ownership check first."""
    reach = _reachable(states, "CaptureSnapshot",
                       blocked={"CheckBoxOwnedBeforeStop", "CheckBoxOwnedBeforeForceStop"})
    assert not {"StopTradingInstance", "ForceStopInstance", "DrainTraderReconcile"} & reach
    for name in _HEAL:
        assert "ec2:" not in states[name].get("Resource", "")


def test_a_failed_self_heal_pages_then_hard_fails(states):
    page = "PageCaptureSnapshotIrreversibleFailure"
    assert [c["Next"] for c in states["CaptureSnapshot"]["Catch"]] == [page]
    assert [c["Next"] for c in states["WaitForCaptureSnapshot"]["Catch"]] == [page]
    assert states["CheckSnapshotStatus"]["Default"] == "SnapshotStatusError"
    assert states["SnapshotStatusError"]["Next"] == page
    assert states[page]["Next"] == "HandleFailure"
    assert [c["Next"] for c in states[page]["Catch"]] == ["HandleFailure"]
    # the capture's stderr (gateway_not_ready / past-deadline reason) reaches
    # the page and HandleFailure; stdout never enters state (DataLimitExceeded).
    selector = states["WaitForCaptureSnapshot"]["ResultSelector"]
    assert selector["StandardErrorContent.$"] == "$.StandardErrorContent"
    assert "StandardOutputContent.$" not in selector
    assert states["SnapshotStatusError"]["Parameters"]["stderr.$"] == "$.snapshot_poll.StandardErrorContent"


@pytest.mark.parametrize("path", [_RECONCILE, _POSTCLOSE], ids=lambda p: p.name)
def test_ssm_outlives_the_gateway_readiness_wait(path):
    """Both CaptureSnapshot states: the SSM command must outlive the
    executor's 420 s readiness wait plus the capture (it was 120 s, which
    would have killed a capture still waiting for a cold gateway)."""
    st = _doc(path)["States"]["CaptureSnapshot"]
    execution_timeout = int(st["Parameters"]["Parameters"]["executionTimeout"][0])
    assert execution_timeout >= _EXECUTOR_GATEWAY_READY_TIMEOUT_S + _CAPTURE_HEADROOM_S
    assert st["TimeoutSeconds"] > execution_timeout


# ── what the command does (rendered from the definition and run) ─────────────

def _parse_intrinsic(text: str, values: dict[str, str]) -> list[str]:
    """Render the ``commands.$`` ``States.Array(...)`` of this definition.

    Supports exactly what the state uses: single-quoted literals (with ``\\'``
    / ``\\{`` / ``\\}`` / ``\\\\`` escapes), ``States.Format('...', args)`` and
    ``$.x`` / ``$$.Execution.Name`` references.
    """
    pos = 0

    def skip_ws():
        nonlocal pos
        while pos < len(text) and text[pos] in " \n\t":
            pos += 1

    def literal() -> str:
        nonlocal pos
        assert text[pos] == "'"
        pos += 1
        out = []
        while text[pos] != "'":
            if text[pos] == "\\":
                out.append(text[pos + 1])
                pos += 2
                continue
            out.append(text[pos])
            pos += 1
        pos += 1
        return "".join(out)

    def path_ref() -> str:
        nonlocal pos
        m = re.compile(r"\$\$?[A-Za-z0-9_.]*").match(text, pos)
        pos = m.end()
        return values[m.group(0)]

    def call() -> list[str] | str:
        nonlocal pos
        m = re.compile(r"States\.(Array|Format)\(").match(text, pos)
        assert m, text[pos:pos + 40]
        pos = m.end()
        args = []
        while True:
            skip_ws()
            args.append(expr())
            skip_ws()
            if text[pos] == ",":
                pos += 1
                continue
            assert text[pos] == ")"
            pos += 1
            break
        if m.group(1) == "Array":
            return args
        fmt, rest = args[0], iter(args[1:])
        return re.sub(r"\{\}", lambda _m: next(rest), fmt)

    def expr():
        skip_ws()
        if text[pos] == "'":
            return literal()
        if text.startswith("States.", pos):
            return call()
        return path_ref()

    rendered = expr()
    assert isinstance(rendered, list)
    return rendered


def _stub(directory: Path, name: str, body: str) -> None:
    path = directory / name
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def _run_capture_command(tmp_path: Path, *, snapshot_present: bool, et_date: str,
                         capturer_rc: int = 0, run_date: str = "2026-10-09"):
    commands_ref = _doc(_RECONCILE)["States"]["CaptureSnapshot"]["Parameters"]["Parameters"]["commands.$"]
    lines = _parse_intrinsic(commands_ref, {
        "$.run_date": run_date, "$$.Execution.Name": "eod-reconcile-2026-10-09-test",
    })
    home = tmp_path / "alpha-engine"
    (home / ".venv" / "bin").mkdir(parents=True)
    (home / ".venv" / "bin" / "activate").write_text("")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls.log"
    _stub(bin_dir, "aws", f'echo "aws $*" >> {calls}\nexit {0 if snapshot_present else 255}\n')
    _stub(bin_dir, "date", f'echo "date TZ=$TZ $*" >> {calls}\necho {et_date}\n')
    _stub(bin_dir, "python", f'echo "python $*" >> {calls}\nexit {capturer_rc}\n')
    script = "\n".join(lines).replace("/home/ec2-user/alpha-engine", str(home))
    env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "HOME": str(tmp_path)}
    proc = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env, check=False)  # noqa: S603, S607 - fixed argv, test-only
    return proc, (calls.read_text() if calls.exists() else "")


def test_snapshot_present_means_no_capture(tmp_path):
    proc, calls = _run_capture_command(tmp_path, snapshot_present=True, et_date="2026-10-09")
    assert proc.returncode == 0, proc.stderr
    assert "aws s3api head-object --bucket alpha-engine-research --key trades/snapshots/2026-10-09.json" in calls
    assert "python" not in calls
    assert "present, no capture" in proc.stdout


def test_snapshot_missing_on_the_same_et_day_runs_the_capture(tmp_path):
    proc, calls = _run_capture_command(tmp_path, snapshot_present=False, et_date="2026-10-09")
    assert proc.returncode == 0, proc.stderr
    assert "date TZ=America/New_York +%F" in calls
    capture = [ln for ln in calls.splitlines() if ln.startswith("python ")]
    assert capture == [
        "python -m krepis.ssm_log_capture run --correlation-id eod-reconcile-2026-10-09-test "
        "--slug snapshot --log /var/log/snapshot.log -- python executor/snapshot_capturer.py "
        "--date 2026-10-09"
    ]
    assert calls.index("aws s3api") < calls.index("python ")


def test_a_capture_that_fails_fails_the_state(tmp_path):
    """gateway_not_ready (or any capturer failure) is a non-zero exit, so the
    poll sees Failed and the page + HandleFailure route runs."""
    proc, _calls = _run_capture_command(tmp_path, snapshot_present=False, et_date="2026-10-09",
                                        capturer_rc=1)
    assert proc.returncode == 1


def test_snapshot_missing_after_et_midnight_fails_with_the_reason_and_no_capture(tmp_path):
    proc, calls = _run_capture_command(tmp_path, snapshot_present=False, et_date="2026-10-10")
    assert proc.returncode == 3
    assert "python" not in calls
    assert "MISSING" in proc.stderr and "same-ET-day capture deadline" in proc.stderr
    assert "2026-10-10" in proc.stderr
