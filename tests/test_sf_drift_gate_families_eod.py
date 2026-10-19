"""alpha-engine-config-I8102 — pins the POSTCLOSE DeployDriftGate.

Sibling of ``test_sf_drift_gate_families.py``, which pins the same gate in the
preopen definition. Both files exist because the two pipelines were given the
same branch FAMILIES for different reasons, and the reasons are what a future
edit is likely to get wrong:

* **Preopen** has a market-open deadline. A false halt costs an unmanaged
  real-money session, which is why ``alpha-engine-config-I7799`` moved its
  ``sf_drift`` off the SHA stamp and onto a comparison of the deployed
  definition body.
* **Postclose** has no deadline — a lost run costs a mechanical rerun
  (``weekday_sf_rerun.py``, sf-pipeline-policy §2.5). What it has instead is
  the day's book: the position snapshot, ``EODReconcile``, the ``eod_pnl`` row
  and the NAV every downstream consumer reads. An orchestration nobody can
  vouch for producing those numbers is §2.3a rule 3's prohibited case — a run
  presenting results without saying whether the correctness check ran.

So ``sf_drift`` is a HALT family here too, and this file asserts it is routed
as one.

**It halts, as of ``alpha-engine-config-I8141``.** The gate shipped in
sf-pipeline-policy §7a observe mode (``alpha-engine-config-I8102``) and was
promoted once its written criterion was met: the three HALT-family branches and
``DeployDriftCheck``'s Catch route to ``HandleFailure`` (the Choice branches via
``SetDeployDriftHaltError``, which writes the shared ``$.error`` that
``HandleFailure`` formats — a Choice cannot write state). ``cf_drift`` and
``deploy_stamp_stale`` stay permanently non-halting.

The tests below pin:

  1-3. The three HALT-family branches (``sf_drift`` true, ``sf_drift`` absent,
       ``cf_drift`` absent) exist, in that order, ahead of the degrade family,
       and route to ``HandleFailure`` via ``SetDeployDriftHaltError``.
  4-5. ``cf_drift=true`` and ``deploy_stamp_stale=true`` route to the
       permanently non-halting degrade path.
  6.   A clean payload reaches ``StartTradingInstance`` via ``Default`` — the
       gate costs a clean day nothing.
  7.   The probe's own failure (Catch) is an unmeasured verdict and halts via
       ``HandleFailure`` with ``ResultPath`` ``$.error``.
  8.   The single notify state cannot raise ``States.Format`` on any path that
       reaches it, and fails open to ``StartTradingInstance``.
  9.   The gate is sited before the pipeline spends anything, and every route
       into ``StartTradingInstance`` from the mutex passes through it —
       the leak this test exists to catch is a fourth caller added later that
       jumps the gate.
 10.   ``sf_name`` is explicit. Without it the probe answers about the PREOPEN
       state machine, and this gate would read a confident verdict about a
       pipeline that is not itself.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"

_HALT_TARGET = "SetDeployDriftHaltError"
_HANDLE_FAILURE = "HandleFailure"
_DEGRADE_TARGET = "SetDeployDriftDegradedFlag"
_CLEAN_NEXT = "StartTradingInstance"


@pytest.fixture(scope="module")
def states():
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def gate(states):
    return states["DeployDriftGate"]


def _and_pair(choice):
    guard, comparison = choice["And"]
    return guard, comparison


def test_gate_has_five_branches_and_a_default(gate):
    assert gate["Type"] == "Choice"
    assert len(gate["Choices"]) == 5
    assert gate["Default"] == _CLEAN_NEXT


def test_sf_drift_true_is_the_first_branch_and_is_halt_family(gate):
    guard, comparison = _and_pair(gate["Choices"][0])
    assert guard == {
        "Variable": "$.drift_result.Payload.sf_drift", "IsPresent": True,
    }
    assert comparison["Variable"] == "$.drift_result.Payload.sf_drift"
    assert comparison["BooleanEquals"] is True
    assert gate["Choices"][0]["Next"] == _HALT_TARGET


def test_sf_drift_absent_is_halt_family(gate):
    """An unmeasured verdict is never a pass (§2.3a rule 2)."""
    c = gate["Choices"][1]
    assert c["Not"] == {
        "Variable": "$.drift_result.Payload.sf_drift", "IsPresent": True,
    }
    assert c["Next"] == _HALT_TARGET


def test_cf_drift_absent_is_halt_family(gate):
    c = gate["Choices"][2]
    assert c["Not"] == {
        "Variable": "$.drift_result.Payload.cf_drift", "IsPresent": True,
    }
    assert c["Next"] == _HALT_TARGET


def test_cf_drift_true_degrades_permanently(gate):
    guard, comparison = _and_pair(gate["Choices"][3])
    assert guard["Variable"] == "$.drift_result.Payload.cf_drift"
    assert comparison["BooleanEquals"] is True
    assert gate["Choices"][3]["Next"] == _DEGRADE_TARGET


def test_deploy_stamp_stale_degrades_after_cf_drift_and_before_default(gate):
    guard, comparison = _and_pair(gate["Choices"][4])
    assert guard["Variable"] == "$.drift_result.Payload.deploy_stamp_stale"
    assert comparison["BooleanEquals"] is True
    assert gate["Choices"][4]["Next"] == _DEGRADE_TARGET


def test_halt_family_and_degrade_family_are_disjoint(gate):
    """The three halt-family branches must all precede the degrade family.

    Ordering is the whole semantics of an ASL Choice: a degrade branch placed
    above ``sf_drift`` would silently relabel a definition mismatch as
    orchestration metadata, which is the 2026-08-05/07 miscalibration run in
    reverse.
    """
    targets = [c["Next"] for c in gate["Choices"]]
    assert targets == [_HALT_TARGET] * 3 + [_DEGRADE_TARGET] * 2




def test_probe_failure_halts_via_handle_failure_with_the_shared_error_path(states):
    """The probe could not answer: an UNMEASURED verdict is never a pass
    (§2.3a rule 2). Routed exactly as every other Catch that reaches
    ``HandleFailure``, which reads ``$.error``."""
    catch = states["DeployDriftCheck"]["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["ResultPath"] == "$.error"
    assert catch["Next"] == _HANDLE_FAILURE


def test_halt_family_writes_the_shared_error_before_handle_failure(states):
    """A Choice cannot write state and ``HandleFailure`` formats ``$.error``;
    routing a halt branch straight there would raise ``States.Runtime`` inside
    the failure alert (``tests/test_sf_field_reachability.py`` catches this)."""
    flag = states[_HALT_TARGET]
    assert flag["Type"] == "Pass"
    assert flag["ResultPath"] == "$.error"
    assert flag["Parameters"]["Error"] == "DeployDriftHalt"
    # Whole $.drift_result, never .Payload: cannot itself raise.
    assert flag["Parameters"]["Cause.$"] == "States.JsonToString($.drift_result)"
    assert flag["Next"] == _HANDLE_FAILURE


def test_observe_mode_states_are_gone(states):
    for name in (
        "SetDeployDriftObserveWouldHaltFlag",
        "SetDeployDriftProbeUnreadableFlag",
    ):
        assert name not in states, name


def test_notify_interpolates_only_fields_every_inbound_path_writes(states):
    """Every ``States.Format`` argument must resolve on the inbound path.

    A ``States.Format`` on an absent path raises ``States.Runtime``, which this
    state's own Catch would swallow into ``StartTradingInstance`` — losing the
    page on exactly the runs that most need it. Restricting the arguments to
    ``$.degraded_summary`` (written by whichever Pass routed here) makes that
    structurally impossible rather than merely unlikely.
    """
    notify = states["PublishDeployDriftDegraded"]
    message = notify["Parameters"]["Message.$"]
    assert "$.degraded_summary.reason" in message
    assert "States.JsonToString($.degraded_summary)" in message
    assert "$.drift_result" not in message
    assert "$.drift_error" not in message

    # Fails open: an SNS outage may not convert a degrade into a halt.
    assert notify["Catch"][0]["Next"] == _CLEAN_NEXT
    assert notify["Next"] == _CLEAN_NEXT
    # Literal ARN, as every other publish in this definition — a malformed
    # $.sns_topic_arn must not be able to silence the alert.
    assert notify["Parameters"]["TopicArn"].startswith("arn:aws:sns:")
    assert "TopicArn.$" not in notify["Parameters"]

    params = states[_DEGRADE_TARGET]["Parameters"]
    assert params["reason"]
    assert states[_DEGRADE_TARGET]["ResultPath"] == "$.degraded_summary"



def test_gate_sits_between_the_mutex_and_the_first_spend(states):
    """Every mutex outcome must pass through the gate.

    The failure this catches is additive: a later edit adding a fourth route
    into ``StartTradingInstance`` from the mutex block, bypassing the check
    without touching it.
    """
    assert states["CheckMutexRole"]["Default"] == "DeployDriftCheck"
    assert states["AcquireMutex"]["Next"] == "DeployDriftCheck"
    assert states["SetMutexAcquireDegradedFlag"]["Next"] == "DeployDriftCheck"
    assert states["DeployDriftCheck"]["Next"] == "DeployDriftGate"

    # ...and nothing else reaches StartTradingInstance except the gate's own
    # clean/degraded routes.
    def _nexts(state):
        out = []
        for key in ("Next", "Default"):
            if state.get(key):
                out.append(state[key])
        for branch in (state.get("Choices") or []) + (state.get("Catch") or []):
            if branch.get("Next"):
                out.append(branch["Next"])
        return out

    callers = {
        name for name, state in states.items()
        if _CLEAN_NEXT in _nexts(state)
    }
    assert callers == {"DeployDriftGate", "PublishDeployDriftDegraded"}, callers


def test_probe_is_asked_about_the_postclose_state_machine(states):
    """Without ``sf_name`` the probe defaults to the PREOPEN pipeline, and this
    gate would halt-or-pass on a verdict about a state machine that is not the
    one it is guarding. ``crucible-predictor-PR545`` made the key load-bearing
    and made an undeclared name raise; this pins the caller's half."""
    payload = states["DeployDriftCheck"]["Parameters"]["Payload"]
    assert payload["action"] == "check_deploy_drift"
    assert payload["sf_name"] == "ne-postclose-trading-pipeline"
    assert states["DeployDriftCheck"]["Parameters"]["FunctionName"].endswith(":live")
    assert states["DeployDriftCheck"]["ResultPath"] == "$.drift_result"
