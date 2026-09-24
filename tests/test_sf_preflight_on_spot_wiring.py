"""WeeklyPreflightOnSpot wiring in infrastructure/step_function.json
(alpha-engine-config-I11312).

Three properties, each the reason the state exists or the reason it is safe:

1. PLACEMENT — entered only from CheckSubstrateHealthGate's HEALTHY edge,
   exits only to CheckShellRun: after the box is bootstrapped and proven
   SSM-responsive, before any stage is dispatched.
2. OBSERVE MODE FAILS OPEN — no path out of the observe chain reaches a
   failure state, sets $.error, or writes a degraded flag.
3. The observed-FAIL exit code in the module and the ResponseCode the
   Choice matches are the same number.
"""

from __future__ import annotations

import json
from pathlib import Path

import sf_preflight_on_spot as spot

_DEF = Path(__file__).resolve().parent.parent / "infrastructure" / "step_function.json"

CHAIN = {
    "WeeklyPreflightOnSpot",
    "InitWeeklyPreflightOnSpotPollCount",
    "WaitForWeeklyPreflightOnSpot",
    "CheckWeeklyPreflightOnSpotStatus",
    "WeeklyPreflightOnSpotWait",
    "WeeklyPreflightOnSpotPollWait",
    "MergeWeeklyPreflightOnSpotPollCount",
    "RecordWeeklyPreflightOnSpot",
    "RecordWeeklyPreflightOnSpotFail",
    "WeeklyPreflightOnSpotUnobserved",
    "PublishWeeklyPreflightOnSpotNotice",
}


def _states() -> dict:
    return json.loads(_DEF.read_text())["States"]


def _succ(state: dict) -> list[str]:
    out = [state[k] for k in ("Next", "Default") if k in state]
    out += [c["Next"] for c in state.get("Choices", [])]
    out += [c["Next"] for c in state.get("Catch", [])]
    return out


def test_chain_is_present():
    assert CHAIN <= set(_states())


def test_only_entry_is_the_substrate_gate_healthy_edge():
    states = _states()
    entries = {
        (name, tgt) for name, st in states.items() if name not in CHAIN
        for tgt in _succ(st) if tgt in CHAIN
    }
    assert entries == {("CheckSubstrateHealthGate", "WeeklyPreflightOnSpot")}
    healthy = states["CheckSubstrateHealthGate"]["Choices"]
    assert [c["Next"] for c in healthy] == ["WeeklyPreflightOnSpot"]
    assert json.dumps(healthy[0]).count('"HEALTHY"') == 1


def test_only_exit_is_check_shell_run():
    states = _states()
    exits = {tgt for name in CHAIN for tgt in _succ(states[name]) if tgt not in CHAIN}
    assert exits == {"CheckShellRun"}


def test_every_non_success_outcome_proceeds():
    """Fail OPEN: both Catches land on the unobserved arm, the Choice Default
    too, and the notice's own Catch goes on to CheckShellRun."""
    states = _states()
    for name in ("WeeklyPreflightOnSpot", "WaitForWeeklyPreflightOnSpot"):
        catches = states[name]["Catch"]
        assert [c["ErrorEquals"] for c in catches] == [["States.ALL"]]
        assert catches[0]["Next"] == "WeeklyPreflightOnSpotUnobserved"
        assert catches[0]["ResultPath"] == "$.weekly_preflight_on_spot_error"
    assert states["CheckWeeklyPreflightOnSpotStatus"]["Default"] == "WeeklyPreflightOnSpotUnobserved"
    notice = states["PublishWeeklyPreflightOnSpotNotice"]
    assert notice["Next"] == "CheckShellRun"
    assert [c["Next"] for c in notice["Catch"]] == ["CheckShellRun"]
    assert "Retry" not in states["WeeklyPreflightOnSpot"], (
        "observe-mode probe: a retry ladder only delays the run it cannot protect"
    )


def test_observe_chain_writes_no_error_or_degraded_flag():
    """Observe mode may not change how the run terminates: no $.error (the
    HandleFailure input), no $.gate_degraded / $.degraded_summary family."""
    states = _states()
    for name in CHAIN:
        rp = states[name].get("ResultPath", "")
        assert rp == "" or rp.startswith("$.weekly_preflight_on_spot"), (name, rp)


def test_observed_fail_code_matches_the_choice():
    rules = _states()["CheckWeeklyPreflightOnSpotStatus"]["Choices"]
    fail = [r for r in rules if r["Next"] == "RecordWeeklyPreflightOnSpotFail"]
    assert len(fail) == 1
    codes = [leaf["NumericEquals"] for leaf in fail[0]["And"] if "NumericEquals" in leaf]
    assert codes == [spot.OBSERVED_FAIL_EXIT_CODE]


def test_command_runs_the_module_from_the_data_venv():
    cmd = _states()["WeeklyPreflightOnSpot"]["Parameters"]["Parameters"]["commands.$"]
    assert "/home/ec2-user/alpha-engine-data/.venv/bin/python sf_preflight_on_spot.py" in cmd
    assert "cd /home/ec2-user/alpha-engine-data" in cmd
    # The module is the LAST command, so its exit code is the script's —
    # which is what CheckWeeklyPreflightOnSpotStatus matches on.
    assert cmd.rstrip(")").rstrip().endswith("$.run_date,$$.Execution.Name")
    assert cmd.index("sf_preflight_on_spot.py") > cmd.index("cd /home/ec2-user/alpha-engine-data")
    # Nothing but the module's verdict line may reach stdout: the git pull is
    # redirected so RecordWeeklyPreflightOnSpot's stored line stays clean.
    assert "pull --ff-only origin main >&2" in cmd


def test_internal_budget_is_inside_the_ssm_timeout_and_poll_cap():
    states = _states()
    ssm_timeout = int(states["WeeklyPreflightOnSpot"]["Parameters"]["Parameters"]["executionTimeout"][0])
    assert spot.BUDGET_SECONDS < ssm_timeout
    wait = states["WeeklyPreflightOnSpotPollWait"]["Seconds"]
    cap = next(
        leaf["NumericLessThan"]
        for rule in states["CheckWeeklyPreflightOnSpotStatus"]["Choices"]
        for leaf in rule.get("And", [])
        if "NumericLessThan" in leaf
    )
    assert cap * wait >= ssm_timeout, "the poll budget must outlast the command it polls"


def test_recorded_on_both_polarities():
    """sf-pipeline-policy §2.3a rule 3: `observed` is written on every arm."""
    states = _states()
    seen = {
        states[n]["Parameters"]["observed"]
        for n in ("RecordWeeklyPreflightOnSpot", "RecordWeeklyPreflightOnSpotFail",
                  "WeeklyPreflightOnSpotUnobserved")
    }
    assert seen == {True, False}
    for n in ("RecordWeeklyPreflightOnSpotFail", "WeeklyPreflightOnSpotUnobserved"):
        params = states[n]["Parameters"]
        assert "headline" in params and ("detail" in params or "detail.$" in params), n
