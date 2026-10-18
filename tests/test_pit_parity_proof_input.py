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
4. The run_date can never be a weekly cycle's (Monday to Wednesday only, and
   an NYSE session so NormalizeRunDates cannot re-date it), so the proof's
   pit_parity.json cannot become a cycle's contamination verdict.
5. The input reaches ValidatePredictorSkipWeightsFresh, so the command is
   printed only for a run_date whose arena/model verdict the gate accepts
   (alpha-engine-config-I7309 comment, 2026-10-17).
"""

from __future__ import annotations

import importlib.util
import io
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


class _NoSuchKey(Exception):
    pass


class _FakeS3:
    """GetObject only: anything else the script called would AttributeError."""

    def __init__(self, objects: dict):
        self.objects = objects
        self.calls: list = []

    def get_object(self, Bucket, Key):
        self.calls.append((Bucket, Key))
        if (Bucket, Key) not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)].encode())}


def _s3_with(run_date: str, body: str) -> _FakeS3:
    return _FakeS3({("alpha-engine-research", f"arena/model/{run_date}.verdict"): body})


def _flat(sm: dict) -> dict:
    return rerun._flat_states(sm)


def test_the_proof_input_reaches_the_verdict_gate():
    """skip_predictor_training is validated, not honoured blind, on this input:
    the production route through CheckSkipPredictorTraining is the verdict
    gate, so the verdict pre-check cannot be dropped silently."""
    from run_scope import evaluate_rule

    sm = json.loads(SF_PATH.read_text())
    payload = mod.build_proof_input("2026-10-20")
    assert payload["skip_predictor_training"] is True
    assert rerun.predictor_skip_needs_freshness(payload)
    assert "ValidatePredictorSkipWeightsFresh" in rerun._reachable_from(
        sm, sm["StartAt"], payload
    )
    gate = _flat(sm)["CheckSkipPredictorTraining"]
    production = dict(payload, rehearsal_marker={"is_rehearsal": False})
    for rule in gate["Choices"]:
        verdict = evaluate_rule(rule, production, set())
        assert verdict in (True, False), rule
        if verdict:
            assert rule["Next"] == "ValidatePredictorSkipWeightsFresh"
            break
    else:
        raise AssertionError("no rule matched; Default is not the verdict gate")


def test_the_gate_reads_the_key_the_script_checks():
    states = _flat(json.loads(SF_PATH.read_text()))
    params = states["ValidatePredictorSkipWeightsFresh"]["Parameters"]
    assert params["Bucket"] == rerun.PREDICTOR_SKIP_VERDICT_BUCKET
    assert params["Key.$"] == "States.Format('arena/model/{}.verdict', $.run_date)"
    assert mod.verdict_key("2026-10-14") == "arena/model/2026-10-14.verdict"


def test_a_monday_holiday_is_refused():
    # Labor Day 2026: NormalizeRunDates would re-date it to Friday 2026-09-04.
    with pytest.raises(mod.ProofInputError, match="2026-09-04"):
        mod.build_proof_input("2026-09-07")


@pytest.mark.parametrize("word", ["decided", "held", "unmeasurable", "unservable", "bootstrap"])
def test_every_status_word_is_accepted(word):
    payload = mod.build_proof_input("2026-10-14")
    assert mod.check_proof_verdict(_s3_with("2026-10-14", word), "2026-10-14", payload) == word


def test_main_prints_the_precondition_and_the_command_and_starts_nothing(capsys, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)  # any AWS import would raise
    s3 = _s3_with("2026-10-14", "unservable")
    assert mod.main(["--run-date", "2026-10-14"], s3=s3) == 0
    assert s3.calls == [("alpha-engine-research", "arena/model/2026-10-14.verdict")]
    out = capsys.readouterr().out
    pre = out.index(
        "aws s3api get-object --bucket alpha-engine-research "
        "--key arena/model/2026-10-14.verdict /dev/stdout"
    )
    start = out.index("aws stepfunctions start-execution")
    assert pre < start
    assert "reads 'unservable'" in out
    assert "--name pit-parity-proof-2026-10-14-1" in out


def test_main_refuses_a_run_date_with_no_verdict(capsys):
    """The 2026-10-20 command the first version printed: no ModelZooSelect
    ran that day, so the run would end at PredictorSkipWeightsStale."""
    assert mod.main(["--run-date", "2026-10-20"], s3=_FakeS3({})) == 1
    captured = capsys.readouterr()
    assert "REFUSED" in captured.err
    assert "ValidatePredictorSkipWeightsFresh" in captured.err
    assert "start-execution" not in captured.out


def test_main_refuses_an_unknown_verdict_body(capsys):
    assert mod.main(["--run-date", "2026-10-14"], s3=_s3_with("2026-10-14", "decided\n")) == 1
    captured = capsys.readouterr()
    assert "REFUSED" in captured.err
    assert "start-execution" not in captured.out


def test_main_fails_closed_without_boto3(capsys, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)
    assert mod.main(["--run-date", "2026-10-14"]) == 1
    assert "REFUSED" in capsys.readouterr().err


def test_main_refuses_a_friday_before_reading_anything(capsys):
    s3 = _FakeS3({})
    assert mod.main(["--run-date", "2026-10-23"], s3=s3) == 1
    assert "REFUSED" in capsys.readouterr().err
    assert s3.calls == []
