"""The post-close machine RUNS the v2 trader's reconcile before it stops the
box (alpha-engine-config-I12376, Phase 4 PR-H).

``ne-postclose-trading-pipeline`` stops the trading box at ~20:01Z, so the
16:45 America/New_York ``alpha-engine-trader-reconcile`` timer never fired on
time: it caught up only at ne-postclose-reconcile-pipeline's evening boot, and
when that boot landed after 00:00Z IB returned none of the session's
executions. On 10-19 and 10-20 every share and cash delta in the DegradedRun
traced exactly to a v1 fill the reconcile could not see.

``DrainTraderReconcile`` in this machine now starts the unit and waits for it,
between CaptureSnapshot and ``StopTradingInstance``. These tests pin:

1. Order: snapshot (or its operator-replay skip) -> drain -> stop -> terminal,
   and every non-failure route into the stop passes the drain.
2. It never runs beside the trader's other gateway clients: the start waits
   for ``alpha-engine-trader-session`` and ``alpha-engine-trader-pin-smoke``.
3. A failed reconcile stays failed (exit 3 -> eod_trader_reconcile_unit_failed
   -> DegradedRun), and the box is stopped on every outcome.
4. It starts the unit only while run_date is still the current day in UTC and
   New York (after 13:00): a late replay drains only and never re-files a
   session with its fills missing.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
POSTCLOSE = REPO / "infrastructure" / "step_function_eod.json"
RECONCILE = REPO / "infrastructure" / "step_function_eod_reconcile.json"
DOC = json.loads(POSTCLOSE.read_text(encoding="utf-8"))
STATES = DOC["States"]
REC_STATES = json.loads(RECONCILE.read_text(encoding="utf-8"))["States"]

UNIT = "alpha-engine-trader-reconcile.service"
PEERS = ("alpha-engine-trader-session.service", "alpha-engine-trader-pin-smoke.service")
DRAIN = (
    "DrainTraderReconcile", "WaitForTraderReconcileDrain", "CheckTraderReconcileDrainStatus",
    "TraderReconcileDrainWait", "ExtractTraderReconcileDrainStatusError",
    "SetTraderReconcileDrainDegraded", "PublishTraderReconcileDrainUnsettled",
    "SetTraderReconcileUnitFailedDegraded",
)
HALTS = {"HandleFailure", "ForceStopInstance", "FailExecution"}
RUN_DATE = "2026-10-21"


def _targets(state: dict) -> list[str]:
    out = [state[k] for k in ("Next", "Default") if k in state]
    out += [c["Next"] for c in state.get("Choices", [])]
    out += [c["Next"] for c in state.get("Catch", [])]
    return out


# ── the intrinsic, evaluated ─────────────────────────────────────────────────


def _split_args(body: str) -> list[str]:
    """Top-level comma split of an intrinsic's argument list. The literals in
    this state carry no escaped quote, brace or backslash (pinned below), so a
    quote toggle and a paren depth are the whole grammar needed."""
    args, cur, depth, quoted = [], "", 0, False
    for ch in body:
        if ch == "'":
            quoted = not quoted
        elif not quoted and ch == "(":
            depth += 1
        elif not quoted and ch == ")":
            depth -= 1
        if ch == "," and depth == 0 and not quoted:
            args.append(cur.strip())
            cur = ""
            continue
        cur += ch
    args.append(cur.strip())
    return args


def _eval_arg(arg: str, run_date: str) -> str:
    if arg.startswith("'"):
        assert arg.endswith("'"), arg
        return arg[1:-1]
    assert arg.startswith("States.Format(") and arg.endswith(")"), arg
    template, *values = _split_args(arg[len("States.Format("):-1])
    assert values == ["$.run_date"], values
    return template[1:-1].replace("{}", run_date)


def _commands(run_date: str = RUN_DATE) -> list[str]:
    expr = STATES["DrainTraderReconcile"]["Parameters"]["Parameters"]["commands.$"]
    assert expr.startswith("States.Array(") and expr.endswith(")")
    return [_eval_arg(a, run_date) for a in _split_args(expr[len("States.Array("):-1])]


def test_the_literals_need_no_intrinsic_escaping():
    expr = STATES["DrainTraderReconcile"]["Parameters"]["Parameters"]["commands.$"]
    for arg in _split_args(expr[len("States.Array("):-1]):
        if arg.startswith("'"):
            inner = arg[1:-1]
            for reserved in ("'", "{", "}", "\\"):
                assert reserved not in inner, (reserved, inner)


def test_the_settle_loop_is_the_reconcile_machines_own():
    """Shape reuse, not a fork: after its start preamble the loop is the
    reconcile machine's drain, line for line (exit 3 on a failed unit)."""
    theirs = REC_STATES["DrainTraderReconcile"]["Parameters"]["Parameters"]["commands"]
    loop = theirs[theirs.index("while :; do"):]
    ours = _commands()
    assert ours[-len(loop):] == loop
    # pipefail first, as every post-close SSM block (test_sf_ssm_pipefail_wiring.py).
    assert ours[0] == "set -u -o pipefail"
    assert f"unit={UNIT}" in ours and "deadline=$((SECONDS+1080))" in ours
    assert f"run_date={RUN_DATE}" in ours


# ── 1. order ─────────────────────────────────────────────────────────────────


def test_snapshot_success_and_the_replay_skip_both_enter_the_drain():
    succ = [c["Next"] for c in STATES["CheckSnapshotStatus"]["Choices"]
            if c.get("StringEquals") == "Success"]
    assert succ == ["DrainTraderReconcile"]
    assert STATES["CheckSkipCaptureSnapshot"]["Choices"][0]["Next"] == "DrainTraderReconcile"
    into_drain = {n for n, st in STATES.items() if "DrainTraderReconcile" in _targets(st)}
    assert into_drain == {"CheckSnapshotStatus", "CheckSkipCaptureSnapshot"}


def test_every_route_into_the_stop_comes_through_the_drain():
    feeders = {n for n, st in STATES.items() if "StopTradingInstance" in _targets(st)}
    assert feeders <= set(DRAIN), sorted(feeders - set(DRAIN))
    assert STATES["StopTradingInstance"]["Next"] == "CheckDegradedOutcome"


def test_the_drain_finishes_before_the_stop_on_the_happy_path():
    order, name = [], "CheckSnapshotStatus"
    while name not in ("NormalSucceeded", "DegradedRun"):
        order.append(name)
        st = STATES[name]
        if name == "CheckSnapshotStatus":
            name = "DrainTraderReconcile"
        elif name == "CheckTraderReconcileDrainStatus":
            name = st["Choices"][0]["Next"]  # Success
        elif name == "CheckDegradedOutcome":
            name = st["Default"]
        else:
            name = st["Next"]
    assert order == [
        "CheckSnapshotStatus", "DrainTraderReconcile", "WaitForTraderReconcileDrain",
        "CheckTraderReconcileDrainStatus", "StopTradingInstance", "CheckDegradedOutcome",
        "WriteCompletionMarkerNormal",
    ]


# ── 3. fail-open toward the stop; a failure stays a failure ──────────────────


def test_the_drain_fails_open_toward_the_stop():
    for name in DRAIN:
        targets = set(_targets(STATES[name]))
        assert not targets & HALTS, (name, targets)
        assert targets <= set(DRAIN) | {"StopTradingInstance"}, (name, targets)
    for name in ("DrainTraderReconcile", "WaitForTraderReconcileDrain",
                 "PublishTraderReconcileDrainUnsettled"):
        catches = [c for c in STATES[name]["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1 and catches[0]["Next"] in set(DRAIN) | {"StopTradingInstance"}, name


def test_the_choice_and_the_degraded_setters_match_the_reconcile_machine():
    for name in ("CheckTraderReconcileDrainStatus", "WaitForTraderReconcileDrain",
                 "TraderReconcileDrainWait", "ExtractTraderReconcileDrainStatusError",
                 "PublishTraderReconcileDrainUnsettled"):
        ours = {k: v for k, v in STATES[name].items() if k != "Comment"}
        theirs = {k: v for k, v in REC_STATES[name].items() if k != "Comment"}
        assert ours == theirs, name
    for name, reason in (("SetTraderReconcileDrainDegraded", "eod_trader_reconcile_drain_unsettled"),
                         ("SetTraderReconcileUnitFailedDegraded", "eod_trader_reconcile_unit_failed")):
        params = STATES[name]["Parameters"]
        assert params["degraded"] is True and params["reason"] == reason
        assert STATES[name]["ResultPath"] == "$.degraded_summary"
        assert "ne-postclose-trading-pipeline" in params["page_detail"]
    # The degraded flag reaches the DegradedRun terminal after the stop.
    degraded = STATES["CheckDegradedOutcome"]["Choices"][0]
    assert {"Variable": "$.degraded_summary.degraded", "BooleanEquals": True} in degraded["And"]
    assert degraded["Next"] == "WriteCompletionMarkerDegraded"
    assert STATES["WriteCompletionMarkerDegraded"]["Next"] == "DegradedRun"


def test_the_timeouts_are_ordered_and_fit_the_machine():
    drain = STATES["DrainTraderReconcile"]
    execution = int(drain["Parameters"]["Parameters"]["executionTimeout"][0])
    assert 1080 < execution < drain["TimeoutSeconds"]
    assert DOC["TimeoutSeconds"] > drain["TimeoutSeconds"]


# ── the script, against fakes ────────────────────────────────────────────────


def _route(status: str, response_code: int) -> str:
    poll = {"Status": status, "ResponseCode": response_code}

    def holds(rule: dict) -> bool:
        if "And" in rule:
            return all(holds(r) for r in rule["And"])
        value = poll[rule["Variable"].removeprefix("$.trader_reconcile_drain_poll.")]
        if "StringEquals" in rule:
            return value == rule["StringEquals"]
        if "IsNumeric" in rule:
            return isinstance(value, (int, float)) is rule["IsNumeric"]
        if "NumericEquals" in rule:
            return value == rule["NumericEquals"]
        raise AssertionError(rule)

    check = STATES["CheckTraderReconcileDrainStatus"]
    return next((c["Next"] for c in check["Choices"] if holds(c)), check["Default"])


def _reason_after(entry: str) -> str | None:
    reason, name = None, entry
    while name != "StopTradingInstance":
        state = STATES[name]
        if state.get("ResultPath") == "$.degraded_summary":
            reason = state["Parameters"]["reason"]
        name = state["Next"]
    return reason


def _run(
    tmp_path: Path,
    *,
    unit_states: list[str],
    unit_jobs: list[int] | None = None,
    result: str = "success",
    peers: dict[str, list[str]] | None = None,
    utc_day: str = RUN_DATE,
    et_day: str = RUN_DATE,
    et_hhmm: str = "1602",
    start_rc: int = 0,
    budget: int | None = None,
) -> subprocess.CompletedProcess:
    """Run the state's commands with fake `systemctl`, `date`, `journalctl`
    and `sleep`. Each unit answers ActiveState from its own list (one entry per
    poll; the last repeats). Every call is appended to ``calls``."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    sd = tmp_path / "units"
    sd.mkdir()
    for unit, states in {UNIT: unit_states, **(peers or {})}.items():
        (sd / f"{unit}.states").write_text("\n".join(states) + "\n")
    (sd / f"{UNIT}.jobs").write_text("\n".join(map(str, unit_jobs or [0])) + "\n")
    (sd / "result").write_text(result + "\n")
    fake = f"""#!/usr/bin/env bash
sd={sd}
echo "$*" >> {tmp_path}/calls
pop() {{
  f="$sd/$1"; [ -f "$f" ] || {{ echo "$2"; return; }}
  n=$(wc -l < "$f")
  if [ "$n" -gt 1 ]; then head -n1 "$f"; tail -n +2 "$f" > "$f.t" && mv "$f.t" "$f"; else head -n1 "$f"; fi
}}
case "$1" in
  show)
    if [ "$3" = ActiveState ]; then pop "$5.states" inactive; else cat "$sd/result"; fi ;;
  list-jobs)
    count=$(pop "$3.jobs" 0)
    for ((i=0; i<count; i++)); do echo "1 $3 start waiting"; done ;;
  start)
    exit {start_rc} ;;
esac
"""
    date = f"""#!/usr/bin/env bash
if [ "$1" = -u ]; then echo {utc_day}
elif [ "$TZ" != America/New_York ]; then echo "fake date: TZ=$TZ $*" >&2; exit 9
elif [ "$1" = +%F ]; then echo {et_day}
else echo {et_hhmm}; fi
"""
    journal = "#!/usr/bin/env bash\necho 'trader_reconcile.sh: boom'\n"
    for name, body in (("systemctl", fake), ("date", date), ("journalctl", journal),
                       ("sleep", "#!/usr/bin/env bash\nexit 0\n")):
        path = bindir / name
        path.write_text(body)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
    script = "\n".join(_commands())
    if budget is not None:
        script = script.replace("deadline=$((SECONDS+1080))", f"deadline=$((SECONDS+{budget}))")
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}
    return subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True,
                          timeout=30)


def _calls(tmp_path: Path) -> list[str]:
    path = tmp_path / "calls"
    return path.read_text().splitlines() if path.exists() else []


def _started(tmp_path: Path) -> bool:
    return f"start --no-block {UNIT}" in _calls(tmp_path)


def test_at_the_close_it_starts_the_unit_and_waits_for_success(tmp_path):
    result = _run(tmp_path, unit_states=["activating", "activating", "inactive"])
    assert result.returncode == 0, result.stderr
    assert _started(tmp_path)
    assert f"{UNIT} started for run_date {RUN_DATE}" in result.stdout
    assert "settled: ActiveState=inactive Result=success" in result.stdout
    assert _route("Success", 0) == "StopTradingInstance"


def test_it_never_starts_beside_the_trader_session_or_the_pin_smoke(tmp_path):
    result = _run(tmp_path, unit_states=["inactive"],
                  peers={PEERS[0]: ["active", "active", "inactive"],
                         PEERS[1]: ["activating", "inactive"]})
    assert result.returncode == 0, result.stderr
    calls = _calls(tmp_path)
    start = calls.index(f"start --no-block {UNIT}")
    for peer in PEERS:
        polls = [i for i, c in enumerate(calls) if c.endswith(f"ActiveState --value {peer}")]
        assert polls and max(polls) < start, (peer, calls)
    # Both peers were polled until they read inactive: three and two polls.
    assert sum(c.endswith(f"--value {PEERS[0]}") for c in calls) == 3
    assert sum(c.endswith(f"--value {PEERS[1]}") for c in calls) == 2


def test_a_peer_still_running_at_the_budget_is_unsettled_and_never_started(tmp_path):
    result = _run(tmp_path, unit_states=["inactive"], peers={PEERS[0]: ["active"]}, budget=0)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert not _started(tmp_path)
    assert f"{PEERS[0]} still ActiveState=active" in result.stderr
    assert "NOT started; stopping the box anyway" in result.stderr
    entry = _route("Failed", 1)
    assert entry == "ExtractTraderReconcileDrainStatusError"
    assert _reason_after(entry) == "eod_trader_reconcile_drain_unsettled"


def test_a_refused_start_is_unsettled_and_still_stops(tmp_path):
    result = _run(tmp_path, unit_states=["inactive"], start_rc=5)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert "systemctl start --no-block" in result.stderr
    assert _reason_after(_route("Failed", 1)) == "eod_trader_reconcile_drain_unsettled"


@pytest.mark.parametrize("state,unit_result", [
    ("failed", "exit-code"), ("failed", "timeout"), ("inactive", "exit-code"),
])
def test_a_failed_reconcile_stays_failed(tmp_path, state, unit_result):
    result = _run(tmp_path, unit_states=["activating", state], result=unit_result)
    assert result.returncode == 3, (result.stdout, result.stderr)
    assert _started(tmp_path)
    assert f"settled FAILED: ActiveState={state} Result={unit_result}" in result.stderr
    assert "trader_reconcile.sh: boom" in result.stderr
    entry = _route("Failed", 3)
    assert entry == "SetTraderReconcileUnitFailedDegraded"
    assert _reason_after(entry) == "eod_trader_reconcile_unit_failed"


def test_a_unit_still_running_at_the_budget_is_unsettled(tmp_path):
    result = _run(tmp_path, unit_states=["activating"], budget=0)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert "after 1080s; stopping the box anyway" in result.stderr


@pytest.mark.parametrize("utc_day,et_day,et_hhmm,why", [
    ("2026-10-22", RUN_DATE, "2030", "after UTC midnight: IB no longer returns the session"),
    ("2026-10-22", "2026-10-22", "0130", "the next morning"),
    (RUN_DATE, RUN_DATE, "0815", "before the close: the trader binds to the previous session"),
    ("2026-10-23", "2026-10-23", "1700", "another day's close"),
])
def test_outside_the_sessions_own_day_it_drains_only(tmp_path, utc_day, et_day, et_hhmm, why):
    result = _run(tmp_path, unit_states=["inactive"], utc_day=utc_day, et_day=et_day,
                  et_hhmm=et_hhmm)
    assert result.returncode == 0, (why, result.stderr)
    assert not _started(tmp_path), why
    assert f"{UNIT} NOT started" in result.stdout
    assert "settled: ActiveState=inactive" in result.stdout


def test_a_half_day_close_still_starts(tmp_path):
    result = _run(tmp_path, unit_states=["inactive"], et_hhmm="1305")
    assert result.returncode == 0, result.stderr
    assert _started(tmp_path)
