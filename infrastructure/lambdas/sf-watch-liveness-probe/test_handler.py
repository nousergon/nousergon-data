"""Unit tests for sf-watch-liveness-probe index.handler.

Stubs alpha_engine_lib.telegram.send_message (no live Telegram) and mocks
boto3 events/stepfunctions/lambda/s3 clients. Asserts: a clean environment
alerts nothing, each individual wiring problem is detected, problems are
deduplicated by content (not re-alerted every run), and the dedup state
clears once the environment goes clean again.
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_lib_pkg = types.ModuleType("alpha_engine_lib")
_telegram_mod = types.ModuleType("alpha_engine_lib.telegram")
_telegram_mod.send_message = MagicMock(return_value=True)
_lib_pkg.telegram = _telegram_mod
sys.modules.setdefault("alpha_engine_lib", _lib_pkg)
sys.modules.setdefault("alpha_engine_lib.telegram", _telegram_mod)

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402

REGION = "us-east-1"
ACCOUNT_ID = "711398986525"
RULE_ARN_PATTERN = "arn:aws:events:us-east-1:711398986525:rule/alpha-engine-saturday-sf-watch-failed"


class FakeClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _healthy_event_pattern() -> str:
    return json.dumps({
        "source": ["aws.states"],
        "detail-type": ["Step Functions Execution Status Change"],
        "detail": {
            "stateMachineArn": [
                f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{name}"
                for name in index.EXPECTED_PIPELINE_NAMES
            ],
            "status": ["FAILED", "TIMED_OUT", "ABORTED"],
        },
    })


def _make_events_client(*, rule_missing=False, state="ENABLED", pattern=None, no_target=False):
    events = MagicMock()
    if rule_missing:
        events.describe_rule.side_effect = FakeClientError("ResourceNotFoundException")
        return events
    events.describe_rule.return_value = {
        "State": state,
        "EventPattern": pattern if pattern is not None else _healthy_event_pattern(),
    }
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{index.EXPECTED_TARGET_FUNCTION}"
    events.list_targets_by_rule.return_value = {
        "Targets": [] if no_target else [{"Arn": fn_arn}]
    }
    return events


def _make_sfn_client(*, missing_names=()):
    sfn = MagicMock()

    def describe(stateMachineArn):  # noqa: N803 — boto3 kwarg name
        name = stateMachineArn.rsplit(":", 1)[-1]
        if name in missing_names:
            raise FakeClientError("StateMachineDoesNotExist")
        return {"name": name, "status": "ACTIVE"}

    sfn.describe_state_machine.side_effect = describe
    return sfn


def _make_lambda_client(*, missing=False, state="Active", last_update="Successful"):
    lam = MagicMock()
    if missing:
        lam.get_function_configuration.side_effect = FakeClientError("ResourceNotFoundException")
    else:
        lam.get_function_configuration.return_value = {"State": state, "LastUpdateStatus": last_update}
    return lam


def _make_s3_client(*, existing_fingerprint=None):
    s3 = MagicMock()
    if existing_fingerprint is None:
        s3.get_object.side_effect = FakeClientError("NoSuchKey")
    else:
        body = MagicMock()
        body.read.return_value = json.dumps({"fingerprint": existing_fingerprint}).encode()
        s3.get_object.return_value = {"Body": body}
    return s3


def _clients_factory(events, sfn, lam, s3):
    def factory(name, region_name=None):
        return {"events": events, "stepfunctions": sfn, "lambda": lam, "s3": s3}[name]
    return factory


@pytest.fixture(autouse=True)
def reset_telegram():
    _telegram_mod.send_message.reset_mock()
    _telegram_mod.send_message.return_value = True
    yield


def test_all_clean_alerts_nothing():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert result == {"problems": [], "alerted": False, "clean": True}
    _telegram_mod.send_message.assert_not_called()


def test_dead_rule_alerts():
    events = _make_events_client(rule_missing=True)
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("does NOT EXIST" in p for p in result["problems"])
    assert result["alerted"] is True
    _telegram_mod.send_message.assert_called_once()


def test_disabled_rule_alerts():
    events = _make_events_client(state="DISABLED")
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("not ENABLED" in p for p in result["problems"])


def test_wrong_target_alerts():
    events = _make_events_client(no_target=True)
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("does not target" in p for p in result["problems"])


def test_missing_pipeline_arn_in_rule_alerts():
    """The exact 2026-06-29 bug class: a registered pipeline dropped from the
    rule's own EventPattern (e.g. after a rename)."""
    incomplete = json.dumps({
        "detail": {
            "stateMachineArn": [
                f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{name}"
                for name in index.EXPECTED_PIPELINE_NAMES
                if name != "alpha-engine-groom-dispatch"
            ]
        }
    })
    events = _make_events_client(pattern=incomplete)
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("MISSING expected pipeline" in p and "alpha-engine-groom-dispatch" in p for p in result["problems"])


def test_dead_state_machine_arn_alerts():
    """The exact 2026-06-29 bug class: registered in the rule, but the SF
    itself no longer exists (deleted/renamed)."""
    events = _make_events_client()
    sfn = _make_sfn_client(missing_names={"alpha-engine-eod-pipeline"})
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("NO live Step Function" in p and "alpha-engine-eod-pipeline" in p for p in result["problems"])


def test_unhealthy_lambda_alerts():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client(state="Failed")
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("not Active" in p for p in result["problems"])


def test_repeat_problem_is_suppressed_not_realerted():
    events = _make_events_client(rule_missing=True)
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    fingerprint = index._problem_fingerprint([f"EventBridge rule '{index.RULE_NAME}' does NOT EXIST"])
    s3 = _make_s3_client(existing_fingerprint=fingerprint)
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert result["alerted"] is False  # same problem as last time — suppressed
    _telegram_mod.send_message.assert_not_called()


def test_dedup_state_cleared_once_healthy_again():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client(existing_fingerprint="stale-fingerprint-from-a-past-problem")
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        index.handler({}, None)
    s3.put_object.assert_called_once()
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert written["fingerprint"] is None


def _sibling_dispatcher_pipeline_names() -> set[str]:
    """Parse the SF names registered in the sibling saturday-sf-watch-dispatcher
    Lambda's own PIPELINES dict — the source of truth this probe must mirror."""
    import re

    path = Path(__file__).parent.parent / "saturday-sf-watch-dispatcher" / "index.py"
    text = path.read_text()
    start = text.index("PIPELINES: dict")
    end = text.index("\n}\n", start)
    block = text[start:end]
    return set(re.findall(r'^\s*"([\w.-]+)":\s*\{', block, re.M))


def test_expected_pipeline_names_in_lockstep_with_dispatcher_registry():
    """REGRESSION GUARD: this probe's EXPECTED_PIPELINE_NAMES must exactly match
    the sibling saturday-sf-watch-dispatcher's own PIPELINES registry — drift
    here would mean the liveness probe silently checks a stale set of pipelines
    (missing a newly-added one, or false-alarming on a removed one), which is
    the exact class of silent drift this probe exists to catch elsewhere."""
    sibling = _sibling_dispatcher_pipeline_names()
    mine = set(index.EXPECTED_PIPELINE_NAMES)
    assert mine == sibling, (
        f"EXPECTED_PIPELINE_NAMES drifted from saturday-sf-watch-dispatcher's "
        f"PIPELINES registry — only-here: {sorted(mine - sibling)}, "
        f"only-there: {sorted(sibling - mine)}"
    )


def test_unexpected_error_is_not_swallowed():
    """An error code OTHER than the specific 'does not exist' ones must
    propagate — a broken probe should surface via the Lambda error metric,
    not silently report 'all clean'."""
    events = MagicMock()
    events.describe_rule.side_effect = FakeClientError("ThrottlingException")
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        with pytest.raises(FakeClientError):
            index.handler({}, None)
