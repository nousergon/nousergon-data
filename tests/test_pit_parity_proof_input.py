"""The pit_parity proof input runs the parity family and nothing else
(alpha-engine-config-I7309).

Checked against the checked-in definition, not a synthetic one. These are the
properties an operator relies on when pasting the printed command:

1. Every ``skip_*`` gate the definition reads is decided by the input, except
   the two left at their defaults on purpose. A new gate added to the
   definition fails here until this input decides it.
2. Under ``weekly_sf_rerun``'s own simulator, exactly LibPinDriftCheck and the
   parity family run: no training, no backtest, no Evaluator, no tail.
3. ``skip_backtester`` is false. Its skip route jumps past the parity family,
   so flipping it would silently turn this into a run that proves nothing.
4. The run_date can never be a weekly cycle's (Monday to Wednesday only), so
   the proof's pit_parity.json cannot become a cycle's contamination verdict.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "pit_parity_proof_input.py"
SF_PATH = REPO_ROOT / "infrastructure" / "step_function.json"


def _load():
    spec = importlib.util.spec_from_file_location("pit_parity_proof_input", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pit_parity_proof_input"] = mod
    spec.loader.exec_module(mod)
    return mod


mod = _load()
rerun = mod._rerun

PARITY_WORK = {
    "parity", "pit_parity_lookahead", "pit_parity_walkforward",
    "parity_replay", "pit_parity_compare",
}


def _definition_skip_flags() -> set:
    text = SF_PATH.read_text()
    return set(re.findall(r"\$\.(skip_[a-z0-9_]+)", text))


def test_every_skip_gate_in_the_definition_is_decided():
    payload = mod.build_proof_input("2026-10-20")
    decided = {k for k in payload if k.startswith("skip_")}
    undecided = _definition_skip_flags() - decided - mod.LEFT_AT_DEFAULT
    assert not undecided, (
        f"the weekly definition reads {sorted(undecided)} but the proof input "
        "leaves them at their defaults; decide each one in "
        "scripts/pit_parity_proof_input.py"
    )


def test_no_flag_is_inert():
    payload = mod.build_proof_input("2026-10-20")
    inert = {k for k in payload if k.startswith("skip_")} - _definition_skip_flags()
    assert not inert, f"flags the definition never reads: {sorted(inert)}"


def test_only_lib_pin_check_and_the_parity_family_run():
    payload = mod.build_proof_input("2026-10-20")
    ran = rerun._simulate_reachable_works(payload, {})
    # post_eval's row is the tail ENTRY; skip_post_eval=true bypasses every
    # stage behind it, which the next test checks on the graph itself.
    assert ran - {"post_eval"} == {"lib_pin_drift_check"} | PARITY_WORK


def test_the_tail_is_bypassed_on_the_graph():
    """From the post-Evaluator gate on, no stage's work state is reachable,
    even under the walk's over-approximation (unknown Choices take both arms,
    Catch routes are followed)."""
    sm = json.loads(SF_PATH.read_text())
    payload = mod.build_proof_input("2026-10-20")
    assert payload["skip_post_eval"] is True
    reach = rerun._reachable_from(sm, "CheckSkipPostEval", payload)
    works = {s.work for s in rerun.STAGES}
    assert not works & reach, sorted(works & reach)


def test_skip_backtester_stays_false_because_its_skip_route_jumps_past_parity():
    states = rerun._flat_states(json.loads(SF_PATH.read_text()))
    gate = states["CheckSkipBacktester"]
    assert [c["Next"] for c in gate["Choices"]] == ["MarkParityVerdictUnknownByCadence"]
    payload = mod.build_proof_input("2026-10-20")
    assert payload["skip_backtester"] is False
    assert payload["skip_backtester_stage_only"] is True
    flipped = dict(payload, skip_backtester=True)
    assert not rerun._simulate_reachable_works(flipped, {}) & PARITY_WORK


def test_role_is_not_a_cadence_role():
    payload = mod.build_proof_input("2026-10-20")
    assert payload["pipeline_role"] not in rerun.CADENCE_ROLES
    assert payload["pipeline_role"] != "weekly"


def test_alerts_go_to_the_muted_topic():
    assert mod.build_proof_input("2026-10-20")["sns_topic_arn"].endswith(
        ":alpha-engine-alerts-muted"
    )


def test_run_date_and_calendar_date_are_pinned():
    payload = mod.build_proof_input("2026-10-21")
    assert payload["run_date"] == payload["calendar_date"] == "2026-10-21"


@pytest.mark.parametrize("day", ["2026-10-19", "2026-10-20", "2026-10-21"])
def test_monday_to_wednesday_is_accepted(day):
    mod.build_proof_input(day)


@pytest.mark.parametrize("day", ["2026-10-22", "2026-10-23", "2026-10-24", "2026-10-25"])
def test_a_day_that_can_be_a_cycle_run_date_is_refused(day):
    with pytest.raises(mod.ProofInputError):
        mod.build_proof_input(day)


def test_malformed_run_date_is_refused():
    with pytest.raises(mod.ProofInputError):
        mod.build_proof_input("10/20/2026")


def test_name_is_never_a_rerun_or_rehearsal_name():
    name = mod.execution_name("2026-10-20", "1")
    assert name == "pit-parity-proof-2026-10-20-1"
    assert not name.startswith(("watch-rerun-", "rehearsal-"))
    with pytest.raises(mod.ProofInputError):
        mod.execution_name("2026-10-20", "A B")


def test_main_prints_the_command_and_starts_nothing(capsys, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)  # any AWS import would raise
    assert mod.main(["--run-date", "2026-10-20"]) == 0
    out = capsys.readouterr().out
    assert "aws stepfunctions start-execution" in out
    assert "--name pit-parity-proof-2026-10-20-1" in out


def test_main_refuses_a_friday(capsys):
    assert mod.main(["--run-date", "2026-10-23"]) == 1
    assert "REFUSED" in capsys.readouterr().err
