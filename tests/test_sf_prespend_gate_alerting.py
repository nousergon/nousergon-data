"""config#2278 — the pre-spend gates must fail OPEN but never SILENTLY.

LibPinDriftCheck and PipelineContractCheck exist to catch a co-install /
contract break BEFORE the SF spends on a spot. Both deliberately fail open
(availability over gating for a weekly pipeline) — but pre-fix, a gate-infra
flake (GitHub/S3 fetch, Lambda cold-start) silently converted "checked and
clean" into "never checked": Catch(States.ALL) proceeded with no SNS, no
flag, and only a single 1-attempt transient Retry rule. Worse, the lib-pin
gate's Catch jumped straight to CheckMutexRole — silently skipping the
SIBLING contract gate as well.

Shape pinned here (mirrors WeeklyRunDayGateFailed's fail-open+alert model):
  1. one more Retry tier per gate (transient States.TaskFailed/Timeout, 2
     attempts, backoff) so most flakes never degrade at all;
  2. Catch → <Gate>Degraded Pass (sets ``gate_degraded: true``) →
     Publish<Gate>Degraded SNS (constants-only Subject per config#1819;
     best-effort Catch) → proceed — lib-pin's degraded chain re-enters
     PipelineContractCheck (sibling gate no longer skipped), contract's
     proceeds to CheckMutexRole;
  3. a malformed gate payload (no has_drift / has_violation — the
     config#2275 IsPresent absence route) lands on the SAME degraded chain;
  4. ``gate_degraded`` threads into the completion email:
     CheckShellRunNotify → CheckGateDegradedNotify →
     NotifyCompleteGatesDegraded (constants-only "DEGRADED: pre-spend gates
     DEGRADED)" Subject) | NotifyComplete.
"""
from __future__ import annotations

import json
import pathlib

import pytest
from tests.sf_degraded_summary_helpers import (
    assert_completion_notifier_chain,
    assert_degraded_continuation,
)

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


GATES = [
    # (check state, result field, degraded pass, publish state, proceed-to,
    #  cause path)
    ("LibPinDriftCheck", "$.libpin_drift_result.Payload.has_drift",
     "LibPinGateDegraded", "PublishLibPinGateDegraded", "PipelineContractCheck",
     "$.libpin_gate_degraded_cause"),
    ("PipelineContractCheck", "$.pipeline_contract_result.Payload.has_violation",
     "PipelineContractGateDegraded", "PublishPipelineContractGateDegraded",
     "EvaluatorDeployDriftCheck", "$.pipeline_contract_gate_degraded_cause"),
    # config#2348: third pre-spend sibling gate — evaluator's 2 Lambdas,
    # checked in sequence (grading, then director). Each has its own
    # Check/Gate/Degraded/Publish quartet; both mirror the shape above.
    # The grading gate's degraded chain proceeds to the director gate's
    # Check (NOT straight to CheckMutexRole) — same "don't skip the sibling
    # gate" fix config#2278 applied to LibPinGateDegraded.
    ("EvaluatorDeployDriftCheck", "$.evaluator_deploy_drift_result.Payload.has_drift",
     "EvaluatorGateDegraded", "PublishEvaluatorGateDegraded",
     "EvaluatorDirectorDeployDriftCheck", "$.evaluator_gate_degraded_cause"),
    ("EvaluatorDirectorDeployDriftCheck", "$.evaluator_director_deploy_drift_result.Payload.has_drift",
     "EvaluatorDirectorGateDegraded", "PublishEvaluatorDirectorGateDegraded",
     "WeeklyPreflight", "$.evaluator_director_gate_degraded_cause"),
]

_PARAMS = ("check", "_field", "degraded", "publish", "proceed", "cause_path")
_IDS = [g[0] for g in GATES]


@pytest.mark.parametrize(_PARAMS, GATES, ids=_IDS)
def test_gate_has_transient_retry_tier(
    states, check, _field, degraded, publish, proceed, cause_path
):
    retries = states[check]["Retry"]
    by_errors = {tuple(sorted(r["ErrorEquals"])): r for r in retries}
    transient = by_errors[("States.TaskFailed", "States.Timeout")]
    assert transient["MaxAttempts"] == 2
    assert transient["BackoffRate"] > 1.0
    lambda_tier = by_errors[("Lambda.ServiceException", "Lambda.TooManyRequestsException")]
    assert lambda_tier["MaxAttempts"] == 2


@pytest.mark.parametrize(_PARAMS, GATES, ids=_IDS)
def test_gate_catch_routes_through_degraded_alert_chain(
    states, check, _field, degraded, publish, proceed, cause_path
):
    (catch,) = states[check]["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    # alpha-engine-config-I7302: the Catch enters the chain one state earlier,
    # at the normalizer that records WHICH cause degraded the gate, and that
    # normalizer hands off to the unchanged degraded Pass.
    assert catch["Next"] == f"{degraded}FromError"
    assert states[f"{degraded}FromError"]["Next"] == degraded

    degraded_state = states[degraded]
    assert degraded_state["Type"] == "Pass"
    assert degraded_state["Result"] is True
    assert degraded_state["ResultPath"] == "$.gate_degraded"
    assert_degraded_continuation(states, degraded, publish)

    publish_state = states[publish]
    assert publish_state["Resource"] == "arn:aws:states:::sns:publish"
    assert publish_state["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
    # config#1819: the Subject stays a constant. alpha-engine-config-I7302
    # deliberately parameterized the MESSAGE — the constant asserted "gate
    # Lambda failed after retries, or returned a malformed payload", a cause
    # set that excludes the arm which actually fires in production (measured
    # on watch-rerun-2026-08-13-5: the Lambda returned HTTP 200 in 142 ms).
    # config#1819's States.Runtime hazard is closed structurally instead: the
    # only paths dereferenced are two scalars written by the normalizer Pass
    # immediately upstream on BOTH inbound arms — see
    # test_degraded_message_names_the_cause_from_a_guaranteed_present_path.
    assert "Subject" in publish_state["Parameters"]
    assert "Subject.$" not in publish_state["Parameters"]
    assert "Message" not in publish_state["Parameters"]
    assert "DEGRADED" in publish_state["Parameters"]["Subject"]
    assert len(publish_state["Parameters"]["Subject"]) <= 100
    # Fail-open: alert then PROCEED — and a publish failure proceeds too.
    assert publish_state["Next"] == proceed
    (publish_catch,) = publish_state["Catch"]
    assert publish_catch["ErrorEquals"] == ["States.ALL"]
    assert publish_catch["Next"] == proceed


def test_libpin_degraded_chain_no_longer_skips_sibling_gate(states):
    """Pre-fix, LibPinDriftCheck's Catch jumped straight to CheckMutexRole,
    silently skipping PipelineContractCheck as well."""
    assert states["PublishLibPinGateDegraded"]["Next"] == "PipelineContractCheck"


def test_evaluator_degraded_chain_no_longer_skips_sibling_gate(states):
    """config#2348: same fix applied to the evaluator gate pair — the grading
    Lambda's degraded chain must not silently skip the director Lambda's
    check."""
    assert states["PublishEvaluatorGateDegraded"]["Next"] == "EvaluatorDirectorDeployDriftCheck"


@pytest.mark.parametrize(("gate", "field", "degraded"), [
    ("LibPinDriftGate", "$.libpin_drift_result.Payload.has_drift",
     "LibPinGateDegraded"),
    ("PipelineContractGate", "$.pipeline_contract_result.Payload.has_violation",
     "PipelineContractGateDegraded"),
    ("EvaluatorDeployDriftGate", "$.evaluator_deploy_drift_result.Payload.has_drift",
     "EvaluatorGateDegraded"),
    ("EvaluatorDirectorDeployDriftGate", "$.evaluator_director_deploy_drift_result.Payload.has_drift",
     "EvaluatorDirectorGateDegraded"),
])
def test_malformed_gate_payload_routes_to_degraded_chain(states, gate, field, degraded):
    """The config#2275 absence route: a payload WITHOUT the verdict field is
    'could not check' — same degraded chain as a Lambda failure."""
    absence_rule = next(
        r for r in states[gate]["Choices"]
        if r.get("Not", {}).get("Variable") == field
        and r["Not"].get("IsPresent") is True
    )
    assert absence_rule["Next"] == f"{degraded}FromProbe"
    assert states[f"{degraded}FromProbe"]["Next"] == degraded


def test_gate_degraded_threads_into_completion_email(states):
    assert states["CheckShellRunNotify"]["Default"] == "CheckGateDegradedNotify"

    choice = states["CheckGateDegradedNotify"]
    # config#2276 extended this Choice with health_check_degraded rules
    # (most-specific-first ordering, pinned in
    # tests/test_sf_health_check_honesty_wiring.py). The gates-ONLY rule —
    # exactly the two gate_degraded operands — must still exist and still
    # route to the gates-degraded notifier.
    rule = next(
        r for r in choice["Choices"]
        if [c["Variable"] for c in r.get("And", [])]
        == ["$.gate_degraded", "$.gate_degraded"]
    )
    guard, comparison = rule["And"]
    assert guard == {"Variable": "$.gate_degraded", "IsPresent": True}
    assert comparison == {"Variable": "$.gate_degraded", "BooleanEquals": True}
    assert rule["Next"] == "NotifyCompleteGatesDegraded"
    assert choice["Default"] == "NotifyComplete"

    notify = states["NotifyCompleteGatesDegraded"]
    assert notify["Resource"] == "arn:aws:states:::sns:publish"
    assert "DEGRADED" in notify["Parameters"]["Subject"]
    # alpha-engine-config-I7418: was `assert "SUCCESS" in ...`. Since
    # config-I6891 a degraded run terminates in the DegradedRun **Fail**
    # state, so a subject leading with SUCCESS contradicts the execution's
    # own status — the guard was pinning the false claim.
    assert "SUCCESS" not in notify["Parameters"]["Subject"]
    assert len(notify["Parameters"]["Subject"]) <= 100
    assert "Subject.$" not in notify["Parameters"]
    # config#2857: converges into the SF-envelope completion marker before
    # ending, rather than Ending here directly.
    assert "End" not in notify
    assert_completion_notifier_chain(states, "NotifyCompleteGatesDegraded")
    (catch,) = notify["Catch"]
    assert catch["Next"] == "NotifyCompleteDegraded"  # config#1819 idiom


def test_only_degraded_passes_set_gate_degraded(states):
    """The completion-email marker must be SF-controlled: exactly the six
    gate-degraded Pass states (config#2348 added the evaluator pair;
    alpha-engine-config#6722 added SetMutexAcquireDegradedFlag for the
    mutex-acquire infra-error fail-open, the same pre-spend-precondition
    family as the other four; alpha-engine-config-I11112 added
    WeeklyPreflightGateDegraded for the SAME family — WeeklyPreflight is
    another pre-spend gate probe whose own coverage gap fails open) may
    write $.gate_degraded."""
    writers = [
        name for name, st in states.items()
        if st.get("ResultPath") == "$.gate_degraded"
    ]
    assert sorted(writers) == [
        "EvaluatorDirectorGateDegraded",
        "EvaluatorGateDegraded",
        "LibPinGateDegraded",
        "PipelineContractGateDegraded",
        "SetMutexAcquireDegradedFlag",
        "WeeklyPreflightGateDegraded",
    ]


# --------------------------------------------------------------------------
# alpha-engine-config-I7302 — the degraded alert must name the cause the
# execution history shows, not a cause set chosen at authoring time.
#
# Measured 2026-08-13, ne-weekly-freshness-pipeline/watch-rerun-2026-08-13-5:
# PipelineContractCheck reached TaskSucceeded (HTTP 200, 142 ms, zero
# retries) and returned {"violations": [], "boundary_count": null, "reason":
# "fetch_failed"}. The gate degraded via the Choice absence arm — and the
# constant SNS Message asserted "gate Lambda failed after retries, or
# returned a malformed payload". Both halves were false, and the one field
# that WAS true (reason) never reached the reader.
# --------------------------------------------------------------------------

_LAMBDA_FAILURE_CLAIM = "gate Lambda failed after retries"


@pytest.mark.parametrize(_PARAMS, GATES, ids=_IDS)
def test_degraded_message_names_the_cause_from_a_guaranteed_present_path(
    states, check, _field, degraded, publish, proceed, cause_path
):
    """The Message interpolates the normalizer's two scalars and nothing else.

    This is what keeps config#1819's States.Runtime hazard closed while the
    Message is parameterized: every JsonPath the notifier dereferences is
    written by the Pass immediately upstream, on BOTH inbound arms.
    """
    message = states[publish]["Parameters"]["Message.$"]
    assert message.startswith("States.Format(")

    args = message[message.rindex("'") + 1:].rstrip(")")
    referenced = [a.strip() for a in args.split(",") if a.strip()]
    assert referenced == [f"{cause_path}.cause", f"{cause_path}.detail"], (
        f"{publish} dereferences {referenced} — only the normalizer's own "
        f"scalars under {cause_path} are guaranteed present on both the "
        "Catch arm and the Choice absence arm"
    )

    template = message[message.index("'") + 1:message.rindex("'")]
    assert template.count("{}") == len(referenced)


@pytest.mark.parametrize(_PARAMS, GATES, ids=_IDS)
def test_degraded_message_template_is_intrinsic_safe(
    states, check, _field, degraded, publish, proceed, cause_path
):
    """A States.Format argument is a SINGLE-QUOTED ASL literal.

    An apostrophe in the prose, or a stray curly brace (a `{run_date}`-style
    placeholder copied from a sibling alert), terminates or mis-parses the
    intrinsic and the whole definition fails to deploy — a red CI is the
    good outcome; the bad one is a notifier that raises States.Runtime on
    the exact path it exists to report.
    """
    message = states[publish]["Parameters"]["Message.$"]
    template = message[message.index("'") + 1:message.rindex("'")]
    assert "'" not in template, f"{publish}: apostrophe in a States.Format literal"
    assert template.count("{") == template.count("}") == template.count("{}")


@pytest.mark.parametrize(_PARAMS, GATES, ids=_IDS)
def test_degraded_message_does_not_assert_an_unverified_cause(
    states, check, _field, degraded, publish, proceed, cause_path
):
    """The regression this issue exists for.

    The alert may DEFINE what the cause codes mean; it may not ASSERT that a
    particular one happened. Only the interpolated `cause` scalar, written by
    whichever normalizer actually ran, may do that.
    """
    message = states[publish]["Parameters"]["Message.$"]
    template = message[message.index("'") + 1:message.rindex("'")]
    claim_line = next(
        (ln for ln in template.splitlines()
         if _LAMBDA_FAILURE_CLAIM in ln and "Cause codes:" not in ln),
        None,
    )
    assert claim_line is None, (
        f"{publish} asserts {_LAMBDA_FAILURE_CLAIM!r} outside the cause-code "
        f"legend: {claim_line!r}. The gate degrades far more often via the "
        "Choice absence arm, where the Lambda succeeded."
    )


@pytest.mark.parametrize(_PARAMS, GATES, ids=_IDS)
def test_both_degraded_causes_converge_on_one_result_path(
    states, check, _field, degraded, publish, proceed, cause_path
):
    from_probe = states[f"{degraded}FromProbe"]
    from_error = states[f"{degraded}FromError"]

    for name, st in ((f"{degraded}FromProbe", from_probe),
                     (f"{degraded}FromError", from_error)):
        assert st["Type"] == "Pass"
        assert st["ResultPath"] == cause_path, name
        assert st["Next"] == degraded, name
        assert set(st["Parameters"]) == {"cause", "detail.$"}, name
        assert st["Parameters"]["detail.$"].startswith("States.JsonToString("), name

    assert from_probe["Parameters"]["cause"] == "probe_returned_no_verdict"
    assert from_error["Parameters"]["cause"] == "lambda_failed_after_retries"

    # Each normalizer may only read the path its OWN inbound arm guarantees.
    result_path = states[check]["ResultPath"]
    (catch,) = states[check]["Catch"]
    assert from_probe["Parameters"]["detail.$"] == (
        f"States.JsonToString({result_path}.Payload)"
    )
    assert from_error["Parameters"]["detail.$"] == (
        f"States.JsonToString({catch['ResultPath']})"
    )


@pytest.mark.parametrize(_PARAMS, GATES, ids=_IDS)
def test_normalizers_do_not_touch_the_gate_degraded_marker(
    states, check, _field, degraded, publish, proceed, cause_path
):
    """The completion-email marker stays SF-controlled and single-writer.

    `test_only_degraded_passes_set_gate_degraded` pins the writer list; these
    eight new states must not join it, or the family-boolean accumulation
    `CheckGateDegradedNotify` depends on changes shape.
    """
    for name in (f"{degraded}FromProbe", f"{degraded}FromError"):
        assert states[name]["ResultPath"] != "$.gate_degraded"
        assert states[name]["ResultPath"] != "$.degraded_summary"
