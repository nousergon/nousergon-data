"""Unit tests for sf-watch-liveness-probe index.handler.

Mocks flow-doctor notify (no live Telegram) and boto3 events/stepfunctions/lambda/s3/ec2
clients. Asserts: a clean environment alerts nothing, each individual wiring problem is
detected, problems are deduplicated by content (not re-alerted every run), and the dedup
state clears once the environment goes clean again.

EC2-spot dispatch leg (config#2265): both spot-leg dispatcher Lambdas must exist and be
Active; kill-switch env values are REPORTED in the probe record but never alerted on
(a deliberate operator disable is state, not an incident); the launch config
(AMI/SG/subnets) read from the spot dispatcher's live env must still exist in EC2, and a
MISSING launch-config env key is itself a loud finding, never a skip.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
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


# Healthy live env for the spot dispatcher — ids here are test fixtures; the
# REAL values live in sf-watch-spot-dispatcher/deploy.sh (pins) and index.py
# (defaults), held equal by that Lambda's own lockstep test.
HEALTHY_SPOT_ENV = {
    "LOG_LEVEL": "INFO",
    "SF_WATCH_DISPATCH_ENABLED": "true",
    "SF_WATCH_AMI_ID": "ami-test0000000000001",
    "SF_WATCH_SECURITY_GROUP": "sg-test000000000001",
    "SF_WATCH_SUBNETS": "subnet-testaaa,subnet-testbbb",
}
HEALTHY_CI_ENV = {"LOG_LEVEL": "INFO", "CI_WATCH_DISPATCH_ENABLED": "true"}


def _make_lambda_client(
    *,
    missing=False,
    state="Active",
    last_update="Successful",
    spot_missing=False,
    spot_state="Active",
    spot_env=HEALTHY_SPOT_ENV,
    ci_missing=False,
    ci_env=HEALTHY_CI_ENV,
):
    """One fake serves all three probed functions: the legacy EventBridge
    dispatcher (missing/state/last_update) plus the two spot-leg dispatchers."""
    lam = MagicMock()

    def get_config(FunctionName):  # noqa: N803 — boto3 kwarg name
        if FunctionName == index.EXPECTED_TARGET_FUNCTION:
            if missing:
                raise FakeClientError("ResourceNotFoundException")
            return {"State": state, "LastUpdateStatus": last_update}
        if FunctionName == index.SPOT_DISPATCHER_FUNCTION:
            if spot_missing:
                raise FakeClientError("ResourceNotFoundException")
            return {
                "State": spot_state,
                "LastUpdateStatus": "Successful",
                "Environment": {"Variables": dict(spot_env)},
            }
        if FunctionName == index.CI_WATCH_DISPATCHER_FUNCTION:
            if ci_missing:
                raise FakeClientError("ResourceNotFoundException")
            return {
                "State": "Active",
                "LastUpdateStatus": "Successful",
                "Environment": {"Variables": dict(ci_env)},
            }
        raise AssertionError(f"probe queried an unexpected Lambda: {FunctionName}")

    lam.get_function_configuration.side_effect = get_config
    return lam


def _make_ec2_client(
    *,
    ami_ids=(HEALTHY_SPOT_ENV["SF_WATCH_AMI_ID"],),
    ami_state="available",
    sg_ids=(HEALTHY_SPOT_ENV["SF_WATCH_SECURITY_GROUP"],),
    subnet_ids=tuple(HEALTHY_SPOT_ENV["SF_WATCH_SUBNETS"].split(",")),
):
    """Mirrors the Filters-based lookups: a missing resource comes back as an
    EMPTY result set, exactly like the real DescribeImages/SecurityGroups/
    Subnets calls with an id filter."""
    ec2 = MagicMock()

    def describe_images(Filters, IncludeDeprecated=False):  # noqa: N803
        requested = set(Filters[0]["Values"])
        return {"Images": [{"ImageId": i, "State": ami_state} for i in sorted(requested & set(ami_ids))]}

    def describe_security_groups(Filters):  # noqa: N803
        requested = set(Filters[0]["Values"])
        return {"SecurityGroups": [{"GroupId": g} for g in sorted(requested & set(sg_ids))]}

    def describe_subnets(Filters):  # noqa: N803
        requested = set(Filters[0]["Values"])
        return {"Subnets": [{"SubnetId": s} for s in sorted(requested & set(subnet_ids))]}

    ec2.describe_images.side_effect = describe_images
    ec2.describe_security_groups.side_effect = describe_security_groups
    ec2.describe_subnets.side_effect = describe_subnets
    return ec2


def _make_s3_client(*, existing_fingerprint=None):
    s3 = MagicMock()
    if existing_fingerprint is None:
        s3.get_object.side_effect = FakeClientError("NoSuchKey")
    else:
        body = MagicMock()
        body.read.return_value = json.dumps({"fingerprint": existing_fingerprint}).encode()
        s3.get_object.return_value = {"Body": body}
    return s3


def _clients_factory(events, sfn, lam, s3, ec2=None):
    ec2 = ec2 if ec2 is not None else _make_ec2_client()

    def factory(name, region_name=None):
        return {"events": events, "stepfunctions": sfn, "lambda": lam, "s3": s3, "ec2": ec2}[name]
    return factory


HEALTHY_KILL_SWITCHES = {
    "SF_WATCH_DISPATCH_ENABLED": "true",
    "CI_WATCH_DISPATCH_ENABLED": "true",
}


@pytest.fixture(autouse=True)
def reset_notify(monkeypatch):
    mock = MagicMock(return_value=True)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock)
    yield mock


def test_all_clean_alerts_nothing():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert result == {
        "problems": [],
        "alerted": False,
        "clean": True,
        "kill_switches": HEALTHY_KILL_SWITCHES,
    }
    index.notify_via_flow_doctor.assert_not_called()


def test_dead_rule_alerts():
    events = _make_events_client(rule_missing=True)
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("does NOT EXIST" in p for p in result["problems"])
    assert result["alerted"] is True
    index.notify_via_flow_doctor.assert_called_once()


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
                if name != "ne-weekly-freshness-pipeline"
            ]
        }
    })
    events = _make_events_client(pattern=incomplete)
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any("MISSING expected pipeline" in p and "ne-weekly-freshness-pipeline" in p for p in result["problems"])


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


# ── EC2-spot dispatch leg (config#2265) ──────────────────────────────────────


def test_spot_dispatcher_missing_alerts_and_skips_launch_config_probe():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client(spot_missing=True)
    s3 = _make_s3_client()
    ec2 = _make_ec2_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3, ec2)):
        result = index.handler({}, None)
    assert any(
        "spot-leg dispatcher Lambda" in p and index.SPOT_DISPATCHER_FUNCTION in p and "does NOT EXIST" in p
        for p in result["problems"]
    )
    assert result["alerted"] is True
    assert result["kill_switches"]["SF_WATCH_DISPATCH_ENABLED"] == "UNREADABLE(function missing)"
    # No env to read → no launch-config EC2 probing (the does-NOT-EXIST alert
    # is the recording surface).
    ec2.describe_images.assert_not_called()


def test_ci_watch_dispatcher_missing_alerts():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client(ci_missing=True)
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any(
        index.CI_WATCH_DISPATCHER_FUNCTION in p and "does NOT EXIST" in p for p in result["problems"]
    )
    assert result["alerted"] is True
    assert result["kill_switches"]["CI_WATCH_DISPATCH_ENABLED"] == "UNREADABLE(function missing)"


def test_spot_dispatcher_not_active_alerts():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client(spot_state="Pending")
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert any(
        index.SPOT_DISPATCHER_FUNCTION in p and "not Active" in p for p in result["problems"]
    )


def test_kill_switch_false_is_reported_not_alerted():
    """A deliberate operator disable is STATE, not an incident: the value must
    land in the probe record while the run stays clean and silent."""
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client(
        spot_env={**HEALTHY_SPOT_ENV, "SF_WATCH_DISPATCH_ENABLED": "false"},
        ci_env={**HEALTHY_CI_ENV, "CI_WATCH_DISPATCH_ENABLED": "false"},
    )
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert result["clean"] is True
    assert result["alerted"] is False
    assert result["kill_switches"] == {
        "SF_WATCH_DISPATCH_ENABLED": "false",
        "CI_WATCH_DISPATCH_ENABLED": "false",
    }
    index.notify_via_flow_doctor.assert_not_called()


def test_kill_switch_unset_reported_as_default_true():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client(ci_env={"LOG_LEVEL": "INFO"})
    s3 = _make_s3_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert result["clean"] is True
    assert result["kill_switches"]["CI_WATCH_DISPATCH_ENABLED"] == "unset(default:true)"


def test_missing_ami_alerts():
    """The deregistered-AMI silent-break guard — the headline check of the
    spot leg (config#2265 closes-when drill (a))."""
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    ec2 = _make_ec2_client(ami_ids=())
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3, ec2)):
        result = index.handler({}, None)
    assert any(
        HEALTHY_SPOT_ENV["SF_WATCH_AMI_ID"] in p and "NOT FOUND" in p for p in result["problems"]
    )
    assert result["alerted"] is True
    index.notify_via_flow_doctor.assert_called_once()


def test_ami_not_available_state_alerts():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    ec2 = _make_ec2_client(ami_state="failed")
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3, ec2)):
        result = index.handler({}, None)
    assert any("state=failed, not available" in p for p in result["problems"])


def test_missing_security_group_alerts():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    ec2 = _make_ec2_client(sg_ids=())
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3, ec2)):
        result = index.handler({}, None)
    assert any(
        HEALTHY_SPOT_ENV["SF_WATCH_SECURITY_GROUP"] in p and "NOT FOUND" in p
        for p in result["problems"]
    )


def test_missing_subnet_alerts_naming_only_the_missing_ones():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    ec2 = _make_ec2_client(subnet_ids=("subnet-testaaa",))  # subnet-testbbb gone
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3, ec2)):
        result = index.handler({}, None)
    assert any(
        "subnet(s) NOT FOUND" in p and "subnet-testbbb" in p and "subnet-testaaa" not in p
        for p in result["problems"]
    )


def test_missing_launch_config_env_key_alerts_not_skips():
    """Fail-loud on env absence: an unreadable launch config is itself the
    finding — the probe must never silently skip the AMI/SG/subnet checks."""
    stripped = {k: v for k, v in HEALTHY_SPOT_ENV.items() if k != "SF_WATCH_AMI_ID"}
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client(spot_env=stripped)
    s3 = _make_s3_client()
    ec2 = _make_ec2_client()
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3, ec2)):
        result = index.handler({}, None)
    assert any(
        "MISSING launch-config" in p and "SF_WATCH_AMI_ID" in p for p in result["problems"]
    )
    assert result["alerted"] is True
    # Unknown ids are not probed — the MISSING-key alert is the recording surface.
    ec2.describe_images.assert_not_called()


def test_unexpected_ec2_error_is_not_swallowed():
    events = _make_events_client()
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    s3 = _make_s3_client()
    ec2 = _make_ec2_client()
    ec2.describe_images.side_effect = FakeClientError("UnauthorizedOperation")
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3, ec2)):
        with pytest.raises(FakeClientError):
            index.handler({}, None)


def test_repeat_problem_is_suppressed_not_realerted():
    events = _make_events_client(rule_missing=True)
    sfn = _make_sfn_client()
    lam = _make_lambda_client()
    fingerprint = index._problem_fingerprint([f"EventBridge rule '{index.RULE_NAME}' does NOT EXIST"])
    s3 = _make_s3_client(existing_fingerprint=fingerprint)
    with patch("index.boto3.client", side_effect=_clients_factory(events, sfn, lam, s3)):
        result = index.handler({}, None)
    assert result["alerted"] is False  # same problem as last time — suppressed
    index.notify_via_flow_doctor.assert_not_called()


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
    # Match only keys at the dict's own 4-space indent — deeper-nested keys
    # (e.g. a per-pipeline "fast_path" sub-config) aren't pipeline names.
    return set(re.findall(r'^ {4}"([\w.-]+)":\s*\{', block, re.M))


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
