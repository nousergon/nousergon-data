"""The skip-to-green refusal: `weekly_sf_rerun.py --start` must not dispatch a
rerun that can enter no substantive stage (alpha-engine-config-I11075, the
pre-flight half of I9693 deliverable 3).

WHY THESE FIXTURES AND NOT A SYNTHETIC ONE
------------------------------------------
Two earlier instruments passed a synthetic all-flags-true test and were INERT on
the real inputs, which is how both looked correct:

* graph reachability with the flags applied still left five spine stages
  standing under an all-flags-true set, because `ResumeAfterSubstrateRelaunch`
  jumps into them past their gates;
* "every declared skip_* flag is true" never fired, because the real
  `watch-rerun-2026-09-11-1` set 25 of the 31 declared flags and was vacuous
  anyway.

So the load-bearing tests here run the captured StartExecution inputs of
`watch-rerun-2026-09-11-1` and `watch-rerun-2026-09-04-4` — two of the thirteen
skip-to-green recoveries I9693 measured — against the checked-in definition. A
change that makes the refusal inert again fails HERE, not on a Saturday.

(The fixtures are the real inputs with `sns_topic_arn` redacted: this repo is
public, and the topic plays no part in the analysis.)
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "weekly_sf_rerun.py"
FIXTURES = Path(__file__).parent / "fixtures" / "weekly_sf_rerun"
SF_PATH = REPO_ROOT / "infrastructure" / "step_function.json"
WEEKLY_ARN = (
    "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
)

#: The two captured recovery inputs. Both terminated SUCCEEDED having produced
#: nothing — the outcome the SF's own VacuousRun terminal now calls a failure
#: (nousergon-data-PR1808) and this guard now refuses before the dispatch.
REAL_VACUOUS_INPUTS = (
    "input_watch_rerun_2026_09_11_1",
    "input_watch_rerun_2026_09_04_4",
)


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location("weekly_sf_rerun", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    sys.modules["weekly_sf_rerun"] = m
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def sf_def() -> dict:
    return json.loads(SF_PATH.read_text())


@pytest.fixture(scope="module")
def spine(mod) -> tuple:
    """The LANDED spine — excludes stages the pinned nousergon-lib has
    declared ahead of (PENDING_DEFINITION_STAGES) or behind (RETIRING_
    DEFINITION_STAGES) this repo's own step_function.json
    (alpha-engine-config-I11267). The raw `stage_order_for()` is what
    production code must never feed directly into a vacuity/coverage
    judgment — see `weekly_sf_rerun._landed_spine`'s docstring for why."""
    return mod._landed_spine(WEEKLY_ARN)


def _input(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


# ---------------------------------------------------------------------------
# The refusal, on the real inputs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fixture", REAL_VACUOUS_INPUTS)
def test_refuses_the_real_skip_to_green_rerun_inputs(mod, sf_def, fixture):
    with pytest.raises(SystemExit) as exc:
        mod.refuse_vacuous_rerun(sf_def, WEEKLY_ARN, _input(fixture), "")
    message = str(exc.value)
    assert "vacuous rerun" in message
    assert "alpha-engine-config-I11075" in message
    assert "--accept-vacuous-rerun" in message


@pytest.mark.parametrize("fixture", REAL_VACUOUS_INPUTS)
def test_the_real_inputs_can_enter_no_spine_stage(mod, sf_def, spine, fixture):
    assert mod.enabled_spine_stages(sf_def, _input(fixture), spine) == ()


@pytest.mark.parametrize("fixture", REAL_VACUOUS_INPUTS)
def test_the_refusal_names_a_flag_for_every_individually_gated_stage(
    mod, sf_def, spine, fixture
):
    """The message must be actionable: every stage it lists as disabled either
    names the flag that did it, or is disabled by the conjunction — and the
    stages with their own gate must name it rather than hiding behind the set.
    """
    attribution = mod.disabling_flags_for_input(sf_def, _input(fixture), spine)
    assert set(attribution) == set(spine)
    assert attribution["Scanner"] == ("skip_scanner",)
    assert attribution["RAGIngestion"] == ("skip_rag_ingestion",)
    assert "skip_predictor_training" in attribution["ModelZooSelect"]


def test_the_escape_hatch_needs_a_reason_and_then_allows_the_dispatch(mod, sf_def):
    payload = _input(REAL_VACUOUS_INPUTS[0])
    # An empty reason is not acceptance.
    with pytest.raises(SystemExit):
        mod.refuse_vacuous_rerun(sf_def, WEEKLY_ARN, payload, "")
    # A named one is.
    assert mod.refuse_vacuous_rerun(
        sf_def, WEEKLY_ARN, payload, "re-stamping the completion marker only"
    ) == ()


def test_the_accepted_reason_rides_into_the_execution_input(mod):
    """`describe-execution` must answer 'why was this empty run dispatched'."""
    plan = mod.RerunPlan(
        run_date="2026-09-18",
        run_date_provenance="test",
        original_input={"sns_topic_arn": "arn:aws:sns:us-east-1:0:t"},
    )
    assert "vacuous_rerun_accepted_reason" not in plan.rerun_input()
    plan.accept_vacuous_reason = "completion marker re-stamp, nothing else to run"
    emitted = plan.rerun_input()
    assert emitted["vacuous_rerun_accepted_reason"] == (
        "completion marker re-stamp, nothing else to run"
    )


# ---------------------------------------------------------------------------
# Non-inertness in the other direction: the guard must not refuse real work
# ---------------------------------------------------------------------------

def test_the_scheduled_cadence_input_can_enter_every_spine_stage(mod, sf_def, spine):
    """A false refusal is worse than the bug — it would block every recovery.

    The scheduled trigger sets no skip flags of its own beyond the cadence
    declaration, so an unflagged input must reach all 17 declared spine stages.
    """
    assert mod.enabled_spine_stages(sf_def, {}, spine) == tuple(spine)
    assert mod.refuse_vacuous_rerun(sf_def, WEEKLY_ARN, {}, "") == tuple(spine)


@pytest.mark.parametrize(
    "flag,stage",
    [
        ("skip_scanner", "Scanner"),
        ("skip_signals_envelope", "SignalsEnvelope"),
        ("skip_eval_judge", "EvalRollingMean"),
        ("skip_report_card", "ReportCard"),
        ("skip_director", "Director"),
    ],
)
def test_clearing_one_flag_puts_exactly_that_stage_back(
    mod, sf_def, spine, flag, stage
):
    """Sensitivity, the property the two inert instruments lacked: the verdict
    must move when the input moves, one flag at a time."""
    payload = _input(REAL_VACUOUS_INPUTS[0])
    assert payload.get(flag) is True, f"{flag} is not set in the captured input"
    relaxed = {k: v for k, v in payload.items() if k != flag}
    assert mod.enabled_spine_stages(sf_def, relaxed, spine) == (stage,)


def test_re_enabling_a_box_stage_also_reopens_the_substrate_resume_targets(
    mod, sf_def, spine
):
    """The over-approximation is deliberate and must stay visible.

    `ResumeAfterSubstrateRelaunch` re-enters five work states directly, past
    their gates, whenever `$.error.phase` can be set — which becomes possible
    the moment ANY stage on the box can run and lose its substrate. So clearing
    `skip_rag_ingestion` legitimately puts six stages back, not one. A future
    change that narrows this to `RAGIngestion` alone has started asserting a
    path cannot be taken when it can, and the refusal becomes a false
    accusation on the next substrate-loss recovery.
    """
    payload = _input(REAL_VACUOUS_INPUTS[0])
    relaxed = {k: v for k, v in payload.items() if k != "skip_rag_ingestion"}
    assert mod.enabled_spine_stages(sf_def, relaxed, spine) == (
        "MorningEnrich",
        "DataPhase1",
        "RAGIngestion",
        "Backtester",
        "EvaluatorDiagnostics",
        "EvaluatorOptimize",
    )


# ---------------------------------------------------------------------------
# Coverage: the mapping is derived, and a spine stage with no gate fails here
# ---------------------------------------------------------------------------

def test_every_declared_spine_stage_is_switchable_off(mod, sf_def, spine):
    """Deliverable 1's coverage guarantee.

    A spine stage no combination of declared flags can disable is a stage the
    vacuity verdict cannot reason about, and it silently weakens the refusal for
    every OTHER stage — the run is called substantive because of a stage nobody
    can turn off. Adding an entry to `PIPELINE_STAGE_ORDER` (or a stage whose
    gate is missing from the definition) fails here.
    """
    import run_scope

    assert run_scope.ungated_spine_stages(sf_def, spine) == ()
    assert len(spine) == 17, (
        "the weekly spine changed — re-measure the guard against the real "
        "recovery inputs before adjusting this number"
    )


def test_the_gating_is_read_from_the_definition_not_listed(mod, sf_def, spine):
    """A renamed gate must change the answer, not be silently tolerated.

    Stripping every CheckSkip Choice's skip branch (so no flag can disable
    anything) must make an all-flags-true input substantive again — proof the
    verdict is computed from the definition in hand rather than from a table.
    """
    import copy

    import run_scope

    stripped = copy.deepcopy(sf_def)
    states = run_scope.flatten_states(stripped.get("States", {}))
    for name, body in states.items():
        if name.startswith("CheckSkip") and body.get("Type") == "Choice":
            body["Choices"] = []
    all_true = {f: True for f in run_scope.declared_skip_flags(sf_def)}
    assert run_scope.enabled_spine_stages(stripped, all_true, spine) == tuple(spine)
