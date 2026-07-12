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
     NotifyCompleteGatesDegraded (constants-only "SUCCESS (pre-spend gates
     DEGRADED)" Subject) | NotifyComplete.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


GATES = [
    # (check state, result field, degraded pass, publish state, proceed-to)
    ("LibPinDriftCheck", "$.libpin_drift_result.Payload.has_drift",
     "LibPinGateDegraded", "PublishLibPinGateDegraded", "PipelineContractCheck"),
    ("PipelineContractCheck", "$.pipeline_contract_result.Payload.has_violation",
     "PipelineContractGateDegraded", "PublishPipelineContractGateDegraded",
     "CheckMutexRole"),
]


@pytest.mark.parametrize(("check", "_field", "degraded", "publish", "proceed"),
                         GATES, ids=[g[0] for g in GATES])
def test_gate_has_transient_retry_tier(states, check, _field, degraded, publish, proceed):
    retries = states[check]["Retry"]
    by_errors = {tuple(sorted(r["ErrorEquals"])): r for r in retries}
    transient = by_errors[("States.TaskFailed", "States.Timeout")]
    assert transient["MaxAttempts"] == 2
    assert transient["BackoffRate"] > 1.0
    lambda_tier = by_errors[("Lambda.ServiceException", "Lambda.TooManyRequestsException")]
    assert lambda_tier["MaxAttempts"] == 2


@pytest.mark.parametrize(("check", "_field", "degraded", "publish", "proceed"),
                         GATES, ids=[g[0] for g in GATES])
def test_gate_catch_routes_through_degraded_alert_chain(
    states, check, _field, degraded, publish, proceed
):
    (catch,) = states[check]["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == degraded

    degraded_state = states[degraded]
    assert degraded_state["Type"] == "Pass"
    assert degraded_state["Result"] is True
    assert degraded_state["ResultPath"] == "$.gate_degraded"
    assert degraded_state["Next"] == publish

    publish_state = states[publish]
    assert publish_state["Resource"] == "arn:aws:states:::sns:publish"
    assert publish_state["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
    # config#1819: constants only — a parameterized Subject/Message here
    # would reintroduce the SNS-contract States.Runtime class.
    assert "Subject" in publish_state["Parameters"]
    assert "Subject.$" not in publish_state["Parameters"]
    assert "Message.$" not in publish_state["Parameters"]
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


@pytest.mark.parametrize(("gate", "field", "degraded"), [
    ("LibPinDriftGate", "$.libpin_drift_result.Payload.has_drift",
     "LibPinGateDegraded"),
    ("PipelineContractGate", "$.pipeline_contract_result.Payload.has_violation",
     "PipelineContractGateDegraded"),
])
def test_malformed_gate_payload_routes_to_degraded_chain(states, gate, field, degraded):
    """The config#2275 absence route: a payload WITHOUT the verdict field is
    'could not check' — same degraded chain as a Lambda failure."""
    absence_rule = next(
        r for r in states[gate]["Choices"]
        if r.get("Not", {}).get("Variable") == field
        and r["Not"].get("IsPresent") is True
    )
    assert absence_rule["Next"] == degraded


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
    assert "SUCCESS" in notify["Parameters"]["Subject"]
    assert len(notify["Parameters"]["Subject"]) <= 100
    assert "Subject.$" not in notify["Parameters"]
    assert notify["End"] is True
    (catch,) = notify["Catch"]
    assert catch["Next"] == "NotifyCompleteDegraded"  # config#1819 idiom


def test_only_degraded_passes_set_gate_degraded(states):
    """The completion-email marker must be SF-controlled: exactly the two
    gate-degraded Pass states may write $.gate_degraded."""
    writers = [
        name for name, st in states.items()
        if st.get("ResultPath") == "$.gate_degraded"
    ]
    assert sorted(writers) == ["LibPinGateDegraded", "PipelineContractGateDegraded"]
