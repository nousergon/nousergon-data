"""The weekday skip-to-green refusal: `weekday_sf_rerun.py --start` must not dispatch a
rerun that can enter none of the pipeline's declared substantive stages
(alpha-engine-config-I11081, Brian's ruling 2026-10-24 "(a) Declared stages").

WHY A DECLARED SUBSET, AND WHY THE COMMISSIONING TEST
-----------------------------------------------------
Measured 2026-10-23: importing the weekly guard's analysis as-is, over the full
``PIPELINE_STAGE_ORDER`` spine, gives a guard that CANNOT fire for the preopen
pipeline. With every declared skip flag true, ``StartExecutorEC2`` and
``CodeFreshnessGate`` are still enterable for any role, and the reconcile spine
keeps ``StopTradingInstance``, because no flag gates those stages. A guard over
that spine looks correct on a synthetic input and is inert on the real shape,
the same trap ``test_weekly_sf_rerun_vacuity.py`` documents for the weekly path.

So the guard reads ``nousergon_lib``'s ``VACUITY_SPINE``, and
``test_commissioning_*`` below proves, against each checked-in definition, that an
all-flags-true input enters NONE of those stages. That proof is what makes the
guard able to fire. If a definition change ever routes a substantive stage past
its gate (the weekly ``ResumeAfterSubstrateRelaunch`` shape), this test fails
here rather than leaving the guard inert in production.

THE FIXTURES ARE REAL CAPTURED StartExecution INPUTS
----------------------------------------------------
Each one was read 2026-10-24 with ``describe-execution`` (read-only). This repo is
public, so ``sns_topic_arn`` and ``market_hours_override.{authorized_by,reason}``
are redacted. Neither plays a part in the analysis.

* ``input_postclose_replay_i9627_2026_08_31``: ``eod-2026-08-31-replay-i9627``,
  ``skip_capture_snapshot`` + ``skip_post_market_data``. A real all-skip postclose
  input under today's definition, so it is REFUSED.
* ``input_postclose_replay_pr558_r2_2026_09_14``: ``skip_post_market_data`` only.
  CaptureSnapshot is still enterable, so it is ALLOWED.
* ``input_preopen_operator_rerun_2026_09_17_150920``: an unflagged preopen
  recovery, ALLOWED.
* ``input_preopen_rerun_i11368_override_2026_09_22``: two of the four work stages
  skipped, still ALLOWED.

No all-skip PREOPEN input exists in the 200 most recent executions, because the
recoveries that ran were all substantive. The refused preopen case is therefore the
real captured 2026-09-17 input carrying the four flags the helper derives when every
work stage completed. That is the input ``--start`` would dispatch for a re-rerun of
a run that already finished.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "weekday_sf_rerun.py"
FIXTURES = Path(__file__).parent / "fixtures" / "weekday_sf_rerun"
DEFS = {
    "daily": REPO_ROOT / "infrastructure" / "step_function_daily.json",
    "eod": REPO_ROOT / "infrastructure" / "step_function_eod.json",
    "eod-reconcile": REPO_ROOT / "infrastructure" / "step_function_eod_reconcile.json",
}


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location("weekday_sf_rerun_vacuity_mod", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    sys.modules["weekday_sf_rerun_vacuity_mod"] = m
    spec.loader.exec_module(m)
    return m


def _def(key: str) -> dict:
    return json.loads(DEFS[key].read_text())


def _input(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


def _pipeline(mod, key):
    return {p.key: p for p in mod.PIPELINES}[key]


def _all_flags(mod, key) -> dict:
    return {s.flag: True for s in _pipeline(mod, key).stages}


# ── commissioning: the guard can fire on every weekday pipeline ───────────────


@pytest.mark.parametrize("key", ["daily", "eod", "eod-reconcile"])
def test_commissioning_all_flags_true_enters_no_substantive_stage(mod, key):
    pipeline = _pipeline(mod, key)
    role = pipeline.emitted_role or "daily"
    inp = {"pipeline_role": role, "run_date": "2026-10-23", **_all_flags(mod, key)}
    spine, enterable, attribution = mod.vacuity_verdict(pipeline, _def(key), inp)
    assert spine, f"{key}: no declared substantive stages"
    assert enterable == (), (
        f"{key}: with every skip flag true, {enterable} is still enterable. The guard "
        "is INERT for this pipeline. A route now enters a substantive stage past its "
        "gate (the weekly ResumeAfterSubstrateRelaunch shape), or VACUITY_SPINE names "
        "an ungated stage.")
    assert all(attribution.get(s) for s in spine), (
        f"{key}: the refusal could not name the flag that disables {spine}")


@pytest.mark.parametrize("key", ["daily", "eod", "eod-reconcile"])
def test_commissioning_unflagged_input_enters_every_substantive_stage(mod, key):
    pipeline = _pipeline(mod, key)
    inp = {"pipeline_role": pipeline.emitted_role or "daily", "run_date": "2026-10-23"}
    spine, enterable, _ = mod.vacuity_verdict(pipeline, _def(key), inp)
    assert set(enterable) == set(spine), f"{key}: unflagged input cannot enter {set(spine) - set(enterable)}"


def test_the_full_spine_would_have_been_inert_for_preopen(mod):
    """Pins the measurement that made the declared subset necessary. If this stops
    holding, the subset is no longer load-bearing and the issue should be re-read."""
    sys.path.insert(0, str(REPO_ROOT / "infrastructure" / "lambdas" / "weekly-run-scope"))
    from nousergon_lib.pipeline_status.registry import stage_order_for
    from run_scope import enabled_spine_stages
    inp = {"pipeline_role": "daily", **_all_flags(mod, "daily")}
    still = enabled_spine_stages(_def("daily"), inp, stage_order_for("ne-preopen-trading-pipeline"))
    assert {"StartExecutorEC2", "CodeFreshnessGate"} <= set(still)


# ── the closes-when cases, on real captured inputs ────────────────────────────


def test_real_all_skip_postclose_input_is_refused_naming_the_flag(mod):
    pipeline = _pipeline(mod, "eod")
    inp = _input("input_postclose_replay_i9627_2026_08_31")
    with pytest.raises(SystemExit) as exc:
        mod.refuse_vacuous_rerun(pipeline, _def("eod"), inp, "")
    msg = str(exc.value)
    assert "vacuous rerun" in msg and "CaptureSnapshot: skip_capture_snapshot" in msg
    assert "--accept-vacuous-rerun" in msg


def test_all_skip_preopen_input_is_refused_naming_each_flag(mod):
    pipeline = _pipeline(mod, "daily")
    inp = {**_input("input_preopen_operator_rerun_2026_09_17_150920"), **_all_flags(mod, "daily")}
    with pytest.raises(SystemExit) as exc:
        mod.refuse_vacuous_rerun(pipeline, _def("daily"), inp, "")
    msg = str(exc.value)
    for line in ("PredictorInference: skip_predictor_inference",
                 "RunMorningPlanner: skip_morning_planner",
                 "RunDaemon: skip_run_daemon"):
        assert line in msg


@pytest.mark.parametrize("key,name", [
    ("daily", "input_preopen_operator_rerun_2026_09_17_150920"),
    ("daily", "input_preopen_rerun_i11368_override_2026_09_22"),
    ("eod", "input_postclose_replay_pr558_r2_2026_09_14"),
])
def test_real_substantive_inputs_are_allowed(mod, key, name):
    enterable = mod.refuse_vacuous_rerun(_pipeline(mod, key), _def(key), _input(name), "")
    assert enterable


def test_i11368_partial_rerun_reports_exactly_what_it_will_still_run(mod):
    _, enterable, _ = mod.vacuity_verdict(
        _pipeline(mod, "daily"), _def("daily"), _input("input_preopen_rerun_i11368_override_2026_09_22"))
    assert enterable == ("RunMorningPlanner", "RunDaemon")


# ── the escape hatch ──────────────────────────────────────────────────────────


def test_accepting_a_vacuous_rerun_lets_it_through(mod):
    inp = _input("input_postclose_replay_i9627_2026_08_31")
    assert mod.refuse_vacuous_rerun(_pipeline(mod, "eod"), _def("eod"), inp, "drill") == ()


def test_the_accepted_reason_rides_into_the_execution_input(mod):
    plan = mod.RerunPlan(pipeline_key="eod", run_date="2026-08-31", run_date_provenance="x",
                         original_input={"pipeline_role": "operator-replay"},
                         emitted_role="operator-replay")
    assert "vacuous_rerun_accepted_reason" not in plan.rerun_input()
    plan.accept_vacuous_reason = "re-proving the snapshot write path"
    assert plan.rerun_input()["vacuous_rerun_accepted_reason"] == "re-proving the snapshot write path"


def test_an_undeclared_pipeline_cannot_be_judged(mod):
    ghost = mod.Pipeline(key="ghost", label="ghost", sm_name="ne-no-such-pipeline",
                         stages=(), role_conjunct=None, emitted_role=None)
    with pytest.raises(SystemExit, match="no declared substantive stages"):
        mod.vacuity_verdict(ghost, _def("daily"), {})


# ── wiring: --dry-run reports, --start enforces before any dispatch ───────────


class _FakeSF:
    def __init__(self, sm_def, source_input):
        self.sm_def, self.source_input, self.started = sm_def, source_input, []

    def describe_execution(self, executionArn):
        return {"status": "FAILED", "input": json.dumps(self.source_input)}

    def describe_state_machine(self, stateMachineArn):
        return {"definition": json.dumps(self.sm_def)}

    def start_execution(self, **kw):
        self.started.append(kw)
        return {"executionArn": "arn:started"}


def _wire(mod, monkeypatch, key, rerun_input):
    fake = _FakeSF(_def(key), rerun_input)
    import types
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda *a, **k: fake))
    plan = mod.RerunPlan(pipeline_key=key, run_date=rerun_input.get("run_date", "2026-08-31"),
                         run_date_provenance="fixture", original_input=rerun_input,
                         emitted_role=_pipeline(mod, key).emitted_role)
    monkeypatch.setattr(mod, "fetch_history", lambda sf, arn: [])
    monkeypatch.setattr(mod, "derive_plan", lambda p, events, start_time=None: plan)
    monkeypatch.setattr(mod, "verify_skip_flags_live", lambda *a, **k: None)
    monkeypatch.setattr(mod, "list_all_executions", lambda *a, **k: [])
    monkeypatch.setattr(mod, "market_is_open", lambda region: (False, {"now_et": "x", "reason": "closed"}))
    return fake


ARN = "arn:aws:states:us-east-1:711398986525:execution:ne-postclose-trading-pipeline:x"


def test_dry_run_reports_the_vacuous_verdict(mod, monkeypatch, capsys):
    _wire(mod, monkeypatch, "eod", _input("input_postclose_replay_i9627_2026_08_31"))
    assert mod.main(["--execution-arn", ARN]) == 0
    assert "VACUOUS" in capsys.readouterr().err


def test_start_refuses_before_dispatch(mod, monkeypatch):
    fake = _wire(mod, monkeypatch, "eod", _input("input_postclose_replay_i9627_2026_08_31"))
    with pytest.raises(SystemExit, match="vacuous rerun"):
        mod.main(["--execution-arn", ARN, "--start"])
    assert fake.started == []


def test_start_with_acceptance_dispatches_and_carries_the_reason(mod, monkeypatch):
    fake = _wire(mod, monkeypatch, "eod", _input("input_postclose_replay_i9627_2026_08_31"))
    assert mod.main(["--execution-arn", ARN, "--start", "--accept-vacuous-rerun", "drill"]) == 0
    assert json.loads(fake.started[0]["input"])["vacuous_rerun_accepted_reason"] == "drill"


def test_start_of_a_substantive_input_dispatches_without_a_reason(mod, monkeypatch):
    fake = _wire(mod, monkeypatch, "eod", _input("input_postclose_replay_pr558_r2_2026_09_14"))
    assert mod.main(["--execution-arn", ARN, "--start"]) == 0
    assert "vacuous_rerun_accepted_reason" not in json.loads(fake.started[0]["input"])
