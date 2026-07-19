"""Unit tests for the overseer-liveness-probe handler (alpha-engine-config-I2831).

Covers the registry-driven check dispatch, each check type's ported logic, the
content-fingerprint dedup, and the fail-loud contract — with no AWS I/O.

Hermetic: `nousergon_lib` + `flow_doctor_telegram` are git-only / bundled deps
the deploy test gate does not install as importable module-scope names, so they
are stubbed in sys.modules BEFORE `import index` (mirrors the sibling probes'
tests). `yaml` (pyyaml) IS installed. The notify path is a no-op stub;
individual tests that assert alerting monkeypatch `index.notify_via_flow_doctor`.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Stub nousergon_lib + flow_doctor_telegram before importing index ──────────
_ng = types.ModuleType("nousergon_lib")
_ng_telegram = types.ModuleType("nousergon_lib.telegram")
_ng_telegram.send_message = lambda *a, **k: None
_ng_fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")


class _FleetTelegramTopic:
    CRITICAL = "CRITICAL"
    OPS_HEALTH = "OPS_HEALTH"


_ng_fleet.FleetTelegramTopic = _FleetTelegramTopic
_ng.telegram = _ng_telegram
_ng.flow_doctor_fleet = _ng_fleet
sys.modules.setdefault("nousergon_lib", _ng)
sys.modules.setdefault("nousergon_lib.telegram", _ng_telegram)
sys.modules.setdefault("nousergon_lib.flow_doctor_fleet", _ng_fleet)

_fdt = types.ModuleType("flow_doctor_telegram")
_fdt.notify_via_flow_doctor = lambda *a, **k: True  # type: ignore[attr-defined]
sys.modules["flow_doctor_telegram"] = _fdt

from _shared.hermetic_import_guard import (  # noqa: E402
    assert_hermetic_imports_satisfied,
)

assert_hermetic_imports_satisfied(__file__)

import index  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 7, 17, 20, 0, tzinfo=UTC)
ACCT = index.ACCOUNT_ID
REG = index.REGION


class FakeClientError(Exception):
    """Mimics botocore ClientError enough for index._error_code."""

    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _client_factory(**clients):
    """Return a boto3.client side_effect dispatching by service name; any
    unexpected service is a test bug (fails loudly)."""
    def make(service, **_kw):
        assert service in clients, f"unexpected boto3.client({service!r})"
        return clients[service]
    return make


# ══════════════════════════════════════════════════════════════════════════
# eventbridge_rule
# ══════════════════════════════════════════════════════════════════════════

_SFW_RULE_SPEC = {
    "type": "eventbridge_rule",
    "rule_name": "alpha-engine-saturday-sf-watch-failed",
    "expect_enabled": True,
    "expect_target_function": "alpha-engine-saturday-sf-watch-dispatcher",
    "expect_state_machines": ["ne-weekly-freshness-pipeline", "ne-preopen-trading-pipeline"],
}


def _events_client_for(state="ENABLED", target_fn="alpha-engine-saturday-sf-watch-dispatcher",
                       registered=("ne-weekly-freshness-pipeline", "ne-preopen-trading-pipeline"),
                       raise_code=None):
    ev = MagicMock()
    if raise_code:
        ev.describe_rule.side_effect = FakeClientError(raise_code)
        return ev
    pattern = {"detail": {"stateMachineArn": [
        f"arn:aws:states:{REG}:{ACCT}:stateMachine:{n}" for n in registered
    ]}}
    ev.describe_rule.return_value = {"State": state, "EventPattern": json.dumps(pattern)}
    ev.list_targets_by_rule.return_value = {"Targets": [
        {"Arn": f"arn:aws:lambda:{REG}:{ACCT}:function:{target_fn}"}
    ]}
    return ev


def test_eventbridge_rule_all_clean():
    ev = _events_client_for()
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, ks = index._check_eventbridge_rule(_SFW_RULE_SPEC, NOW)
    assert problems == []
    assert ks == {}


def test_eventbridge_rule_missing_raises_finding_not_error():
    ev = _events_client_for(raise_code="ResourceNotFoundException")
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, _ = index._check_eventbridge_rule(_SFW_RULE_SPEC, NOW)
    assert len(problems) == 1 and "does NOT EXIST" in problems[0]


def test_eventbridge_rule_disabled_is_a_problem():
    ev = _events_client_for(state="DISABLED")
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, _ = index._check_eventbridge_rule(_SFW_RULE_SPEC, NOW)
    assert any("not ENABLED" in p for p in problems)


def test_eventbridge_rule_wrong_target():
    ev = _events_client_for(target_fn="some-other-lambda")
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, _ = index._check_eventbridge_rule(_SFW_RULE_SPEC, NOW)
    assert any("does not target" in p for p in problems)


def test_eventbridge_rule_missing_registered_state_machine():
    ev = _events_client_for(registered=("ne-weekly-freshness-pipeline",))  # missing preopen
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, _ = index._check_eventbridge_rule(_SFW_RULE_SPEC, NOW)
    assert any("MISSING expected pipeline" in p and "ne-preopen-trading-pipeline" in p for p in problems)


def test_eventbridge_rule_extra_registered_state_machine():
    ev = _events_client_for(registered=(
        "ne-weekly-freshness-pipeline", "ne-preopen-trading-pipeline", "ne-rogue-pipeline"))
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, _ = index._check_eventbridge_rule(_SFW_RULE_SPEC, NOW)
    assert any("UNEXPECTED extra pipeline" in p and "ne-rogue-pipeline" in p for p in problems)


def test_eventbridge_rule_unexpected_error_raises():
    ev = _events_client_for(raise_code="ThrottlingException")
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        with pytest.raises(FakeClientError):
            index._check_eventbridge_rule(_SFW_RULE_SPEC, NOW)


def test_eventbridge_rule_queue_target_on_custom_bus_clean():
    """Intake-rule variant: target is a queue ARN, rule lives on a custom bus."""
    spec = {
        "type": "eventbridge_rule",
        "rule_name": "overseer-intake-alert-events",
        "event_bus_name": "nousergon-alerts",
        "expect_enabled": True,
        "expect_target_queue": "nousergon-overseer-intake",
    }
    ev = MagicMock()
    ev.describe_rule.return_value = {"State": "ENABLED", "EventPattern": "{}"}
    ev.list_targets_by_rule.return_value = {"Targets": [
        {"Arn": f"arn:aws:sqs:{REG}:{ACCT}:nousergon-overseer-intake"}
    ]}
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, _ = index._check_eventbridge_rule(spec, NOW)
    assert problems == []
    # custom bus must be threaded to both describe + list-targets
    assert ev.describe_rule.call_args.kwargs.get("EventBusName") == "nousergon-alerts"
    assert ev.list_targets_by_rule.call_args.kwargs.get("EventBusName") == "nousergon-alerts"


def test_eventbridge_rule_queue_target_missing():
    spec = {
        "type": "eventbridge_rule",
        "rule_name": "overseer-intake-cw-alarm-state",
        "expect_target_queue": "nousergon-overseer-intake",
    }
    ev = MagicMock()
    ev.describe_rule.return_value = {"State": "ENABLED", "EventPattern": "{}"}
    ev.list_targets_by_rule.return_value = {"Targets": []}
    with patch("index.boto3.client", side_effect=_client_factory(events=ev)):
        problems, _ = index._check_eventbridge_rule(spec, NOW)
    assert any("does not target" in p and "sqs" in p for p in problems)


# ══════════════════════════════════════════════════════════════════════════
# state_machines_exist
# ══════════════════════════════════════════════════════════════════════════

_SM_SPEC = {"type": "state_machines_exist",
            "state_machines": ["ne-weekly-freshness-pipeline", "ne-preopen-trading-pipeline"]}


def test_state_machines_all_exist():
    sfn = MagicMock()
    sfn.describe_state_machine.return_value = {"stateMachineArn": "x"}
    with patch("index.boto3.client", side_effect=_client_factory(stepfunctions=sfn)):
        problems, _ = index._check_state_machines_exist(_SM_SPEC, NOW)
    assert problems == []


def test_state_machine_dead_arn_is_finding():
    sfn = MagicMock()
    sfn.describe_state_machine.side_effect = [
        {"stateMachineArn": "x"},
        FakeClientError("StateMachineDoesNotExist"),
    ]
    with patch("index.boto3.client", side_effect=_client_factory(stepfunctions=sfn)):
        problems, _ = index._check_state_machines_exist(_SM_SPEC, NOW)
    assert len(problems) == 1 and "dead ARN" in problems[0]


def test_state_machine_unexpected_error_raises():
    sfn = MagicMock()
    sfn.describe_state_machine.side_effect = FakeClientError("AccessDenied")
    with patch("index.boto3.client", side_effect=_client_factory(stepfunctions=sfn)):
        with pytest.raises(FakeClientError):
            index._check_state_machines_exist(_SM_SPEC, NOW)


# ══════════════════════════════════════════════════════════════════════════
# lambda_active (+ kill switch + launch config)
# ══════════════════════════════════════════════════════════════════════════

def _lambda_cfg(state="Active", last="Successful", env=None):
    return {"State": state, "LastUpdateStatus": last,
            "Environment": {"Variables": env or {}}}


def test_lambda_active_clean_reports_kill_switch():
    spec = {"type": "lambda_active", "function": "alpha-engine-sf-watch-spot-dispatcher",
            "report_kill_switch": "SF_WATCH_DISPATCH_ENABLED"}
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(
        env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam})):
        problems, ks = index._check_lambda_active(spec, NOW)
    assert problems == []
    assert ks == {"SF_WATCH_DISPATCH_ENABLED": "true"}


def test_lambda_active_missing_function():
    spec = {"type": "lambda_active", "function": "alpha-engine-ci-watch-dispatcher",
            "report_kill_switch": "CI_WATCH_DISPATCH_ENABLED"}
    lam = MagicMock()
    lam.get_function_configuration.side_effect = FakeClientError("ResourceNotFoundException")
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam})):
        problems, ks = index._check_lambda_active(spec, NOW)
    assert any("does NOT EXIST" in p for p in problems)
    assert ks == {"CI_WATCH_DISPATCH_ENABLED": "UNREADABLE(function missing)"}


def test_lambda_active_not_active():
    spec = {"type": "lambda_active", "function": "alpha-engine-overseer-dispatcher"}
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(state="Pending")
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam})):
        problems, ks = index._check_lambda_active(spec, NOW)
    assert any("not Active" in p for p in problems)
    assert ks == {}


def test_lambda_kill_switch_false_reported_not_alerted():
    spec = {"type": "lambda_active", "function": "alpha-engine-sf-watch-spot-dispatcher",
            "report_kill_switch": "SF_WATCH_DISPATCH_ENABLED"}
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(
        env={"SF_WATCH_DISPATCH_ENABLED": "false"})
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam})):
        problems, ks = index._check_lambda_active(spec, NOW)
    assert problems == []  # a disabled switch is state, never a finding
    assert ks == {"SF_WATCH_DISPATCH_ENABLED": "false"}


def test_lambda_kill_switch_unset_defaults_true():
    spec = {"type": "lambda_active", "function": "alpha-engine-sf-watch-spot-dispatcher",
            "report_kill_switch": "SF_WATCH_DISPATCH_ENABLED"}
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(env={})
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam})):
        _, ks = index._check_lambda_active(spec, NOW)
    assert ks == {"SF_WATCH_DISPATCH_ENABLED": "unset(default:true)"}


_LC_SPEC = {
    "type": "lambda_active",
    "function": "alpha-engine-sf-watch-spot-dispatcher",
    "launch_config": {"ami_env": "SF_WATCH_AMI_ID", "security_group_env": "SF_WATCH_SECURITY_GROUP",
                      "subnets_env": "SF_WATCH_SUBNETS"},
}
_GOOD_LC_ENV = {"SF_WATCH_AMI_ID": "ami-1", "SF_WATCH_SECURITY_GROUP": "sg-1",
                "SF_WATCH_SUBNETS": "subnet-1,subnet-2"}


def _ec2_ok():
    ec2 = MagicMock()
    ec2.describe_images.return_value = {"Images": [{"State": "available"}]}
    ec2.describe_security_groups.return_value = {"SecurityGroups": [{"GroupId": "sg-1"}]}
    ec2.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-1"}, {"SubnetId": "subnet-2"}]}
    return ec2


def test_launch_config_all_present_clean():
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(env=_GOOD_LC_ENV)
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam, "ec2": _ec2_ok()})):
        problems, _ = index._check_lambda_active(_LC_SPEC, NOW)
    assert problems == []


def test_launch_config_missing_ami():
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(env=_GOOD_LC_ENV)
    ec2 = _ec2_ok()
    ec2.describe_images.return_value = {"Images": []}
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam, "ec2": ec2})):
        problems, _ = index._check_lambda_active(_LC_SPEC, NOW)
    assert any("AMI" in p and "NOT FOUND" in p for p in problems)


def test_launch_config_missing_subnet_names_only_missing():
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(env=_GOOD_LC_ENV)
    ec2 = _ec2_ok()
    ec2.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-1"}]}  # subnet-2 gone
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam, "ec2": ec2})):
        problems, _ = index._check_lambda_active(_LC_SPEC, NOW)
    assert any("subnet" in p and "subnet-2" in p and "subnet-1" not in p for p in problems)


def test_launch_config_missing_env_key_is_finding_not_skip():
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(
        env={"SF_WATCH_AMI_ID": "ami-1", "SF_WATCH_SECURITY_GROUP": "sg-1"})  # no SUBNETS
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam})):
        problems, _ = index._check_lambda_active(_LC_SPEC, NOW)
    assert any("MISSING launch-config key" in p and "SF_WATCH_SUBNETS" in p for p in problems)


def test_launch_config_unexpected_ec2_error_raises():
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(env=_GOOD_LC_ENV)
    ec2 = _ec2_ok()
    ec2.describe_images.side_effect = FakeClientError("UnauthorizedOperation")
    with patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam, "ec2": ec2})):
        with pytest.raises(FakeClientError):
            index._check_lambda_active(_LC_SPEC, NOW)


# ══════════════════════════════════════════════════════════════════════════
# sqs_queue_exists
# ══════════════════════════════════════════════════════════════════════════

_Q_SPEC = {"type": "sqs_queue_exists", "queue_name": "nousergon-overseer-intake",
           "expect_dlq": "nousergon-overseer-intake-dlq"}


def test_sqs_queue_and_dlq_present():
    sqs = MagicMock()
    sqs.get_queue_url.return_value = {"QueueUrl": "https://x"}
    with patch("index.boto3.client", side_effect=_client_factory(sqs=sqs)):
        problems, _ = index._check_sqs_queue_exists(_Q_SPEC, NOW)
    assert problems == []
    assert sqs.get_queue_url.call_count == 2


def test_sqs_queue_missing():
    sqs = MagicMock()
    sqs.get_queue_url.side_effect = FakeClientError("AWS.SimpleQueueService.NonExistentQueue")
    with patch("index.boto3.client", side_effect=_client_factory(sqs=sqs)):
        problems, _ = index._check_sqs_queue_exists(_Q_SPEC, NOW)
    assert any("intake queue" in p and "does NOT EXIST" in p for p in problems)


def test_sqs_dlq_missing_only():
    sqs = MagicMock()
    sqs.get_queue_url.side_effect = [{"QueueUrl": "https://x"},
                                     FakeClientError("QueueDoesNotExist")]
    with patch("index.boto3.client", side_effect=_client_factory(sqs=sqs)):
        problems, _ = index._check_sqs_queue_exists(_Q_SPEC, NOW)
    assert len(problems) == 1 and "intake DLQ" in problems[0]


def test_sqs_unexpected_error_raises():
    sqs = MagicMock()
    sqs.get_queue_url.side_effect = FakeClientError("AccessDenied")
    with patch("index.boto3.client", side_effect=_client_factory(sqs=sqs)):
        with pytest.raises(FakeClientError):
            index._check_sqs_queue_exists(_Q_SPEC, NOW)


# ══════════════════════════════════════════════════════════════════════════
# run_window (ported groom accounting)
# ══════════════════════════════════════════════════════════════════════════

_RW_SPEC = {
    "type": "run_window", "label": "groom", "artifact_prefix": "groom/",
    "ceiling_min": 360, "margin_min": 45, "lookback_hours": 30,
    "schedule": [{"hour": 1, "minute": 0, "dows": [0, 1, 2, 3, 4, 5, 6], "label": "01:00"}],
}


class _FakeRunWindowS3:
    """Serves run artifacts from a {date: [artifact_dicts]} map; no decision log."""

    def __init__(self, artifacts_by_date):
        self._by_date = artifacts_by_date

    def list_objects_v2(self, Bucket, Prefix, **_):
        # Prefix is "groom/{date}/" or "groom/decisions/{date}/"
        if "decisions/" in Prefix:
            return {"Contents": []}
        date = Prefix.split("/")[1]
        arts = self._by_date.get(date, [])
        return {"Contents": [{"Key": f"{Prefix}run-{i}.json"} for i in range(len(arts))]}

    def get_object(self, Bucket, Key, **_):
        parts = Key.split("/")
        date, idx = parts[1], int(parts[-1].split("-")[1].split(".")[0])
        art = self._by_date[date][idx]
        return {"Body": _Body(json.dumps(art).encode())}


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


def test_run_window_trigger_with_artifact_in_window_clean():
    # With NOW=07-17 20:00 + lookback 30h, the one mature in-window trigger is
    # 07-17 01:00; give it a covering artifact (run_start inside [T, T+405min]).
    s3 = _FakeRunWindowS3({"2026-07-17": [{"run_start": "2026-07-17T01:05:00+00:00"}]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC, NOW)
    assert problems == []


def test_run_window_trigger_without_artifact_is_missed():
    s3 = _FakeRunWindowS3({})  # no artifacts at all
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC, NOW)
    assert problems and all("filed NO" in p for p in problems)


def test_run_window_fail_loud_on_primary_s3_error():
    s3 = MagicMock()
    # expected triggers come from schedule (no decision log); artifact fetch raises
    s3.list_objects_v2.side_effect = FakeClientError("InternalError")
    with patch("index._s3_client", return_value=s3):
        with pytest.raises(FakeClientError):
            index._check_run_window(_RW_SPEC, NOW)


def test_run_window_single_silent_death_not_masked_by_later_success():
    # Two triggers (two days). Day A has no artifact, Day B has one. Day A must still miss.
    s3 = _FakeRunWindowS3({"2026-07-17": [{"run_start": "2026-07-17T01:10:00+00:00"}]})
    spec = dict(_RW_SPEC, lookback_hours=48)
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(spec, NOW)
    # 2026-07-16 01:00 missed (no artifact); 2026-07-17 01:00 covered.
    assert any("2026-07-16 01:00" in p for p in problems)
    assert not any("2026-07-17 01:00" in p for p in problems)


# ══════════════════════════════════════════════════════════════════════════
# sf_watch_invocation_success (config#2901)
# ══════════════════════════════════════════════════════════════════════════

_SFWS_SPEC = {
    "type": "sf_watch_invocation_success",
    "label": "sf-watch-invocation",
    "pipelines": {"ne-preopen-trading-pipeline": "consolidated/weekday_sf_watch"},
    "ceiling_min": 10,
    "margin_min": 5,
    "lookback_hours": 30,
}
_SFWS_ARN = f"arn:aws:states:{index.REGION}:{index.ACCOUNT_ID}:stateMachine:ne-preopen-trading-pipeline"


def _sfws_execution(name="exec-1", status="FAILED", stop_offset_min=60):
    return {
        "executionArn": f"{_SFWS_ARN}:{name}",
        "name": name,
        "status": status,
        "startDate": NOW - timedelta(minutes=stop_offset_min + 1),
        "stopDate": NOW - timedelta(minutes=stop_offset_min),
    }


def _sfws_sfn(executions, describe_input=None):
    sfn = MagicMock()
    sfn.list_executions.return_value = {"executions": executions}
    sfn.describe_execution.return_value = (
        {"input": json.dumps({"run_date": describe_input})} if describe_input else {"input": "{}"}
    )
    return sfn


def _sfws_s3(watch_log_events=None, missing=False):
    s3 = MagicMock()
    if missing:
        s3.get_object.side_effect = FakeClientError("NoSuchKey")
    else:
        doc = {"events": watch_log_events or []}
        s3.get_object.return_value = {"Body": _Body(json.dumps(doc).encode())}
    return s3


def test_sf_watch_invocation_success_logged_execution_clean():
    ex = _sfws_execution()
    sfn = _sfws_sfn([ex])
    s3 = _sfws_s3(watch_log_events=[{"execution_arn": ex["executionArn"]}])
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert problems == []


def test_sf_watch_invocation_success_missing_log_object_is_finding():
    """The crash-on-invocation class (2026-07-17 incident): a terminal
    execution happened but the dispatcher never even created the date's
    watch-log object."""
    ex = _sfws_execution()
    sfn = _sfws_sfn([ex])
    s3 = _sfws_s3(missing=True)
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert len(problems) == 1
    assert "NO matching watch-log entry" in problems[0]
    assert "ne-preopen-trading-pipeline" in problems[0]


def test_sf_watch_invocation_success_log_exists_but_execution_not_recorded():
    """Watch-log for the date exists (some other execution logged fine) but
    THIS execution's arn is absent — also a miss."""
    ex = _sfws_execution(name="exec-2")
    sfn = _sfws_sfn([ex])
    s3 = _sfws_s3(watch_log_events=[{"execution_arn": f"{_SFWS_ARN}:some-other-exec"}])
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert len(problems) == 1


def test_sf_watch_invocation_success_ignores_non_terminal_status():
    ex = _sfws_execution(status="RUNNING")
    sfn = _sfws_sfn([ex])
    s3 = _sfws_s3(missing=True)
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert problems == []


def test_sf_watch_invocation_success_ignores_immature_execution():
    """An execution that stopped inside ceiling+margin is not yet mature —
    the dispatcher may simply not have finished writing yet."""
    ex = _sfws_execution(stop_offset_min=2)
    sfn = _sfws_sfn([ex])
    s3 = _sfws_s3(missing=True)
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert problems == []


def test_sf_watch_invocation_success_ignores_execution_before_lookback():
    ex = _sfws_execution(stop_offset_min=60 * 40)  # 40h ago, lookback is 30h
    sfn = _sfws_sfn([ex])
    s3 = _sfws_s3(missing=True)
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert problems == []


def test_sf_watch_invocation_success_uses_run_date_from_execution_input():
    """run_date comes from describe_execution's input.run_date (mirrors the
    dispatcher's own _run_date), not the execution's calendar start date."""
    ex = _sfws_execution()
    sfn = _sfws_sfn([ex], describe_input="2026-07-01")
    s3 = _sfws_s3(watch_log_events=[{"execution_arn": ex["executionArn"]}])
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert problems == []
    s3.get_object.assert_called_once_with(
        Bucket=index.WATCH_BUCKET, Key="consolidated/weekday_sf_watch/2026-07-01.json"
    )


def test_sf_watch_invocation_success_fail_loud_on_unexpected_s3_error():
    ex = _sfws_execution()
    sfn = _sfws_sfn([ex])
    s3 = MagicMock()
    s3.get_object.side_effect = FakeClientError("AccessDenied")
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        with pytest.raises(FakeClientError):
            index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)


def test_sf_watch_invocation_success_fail_loud_on_unexpected_sfn_error():
    sfn = MagicMock()
    sfn.list_executions.side_effect = FakeClientError("AccessDenied")
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=MagicMock()):
        with pytest.raises(FakeClientError):
            index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)


def test_sf_watch_invocation_success_paginates_list_executions():
    ex_old = _sfws_execution(name="old", stop_offset_min=60 * 40)  # past horizon
    ex_new = _sfws_execution(name="new", stop_offset_min=60)
    sfn = MagicMock()
    sfn.list_executions.side_effect = [
        {"executions": [ex_new], "nextToken": "tok"},
        {"executions": [ex_old]},
    ]
    sfn.describe_execution.return_value = {"input": "{}"}
    s3 = _sfws_s3(missing=True)
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFWS_SPEC, NOW)
    assert len(problems) == 1 and "'new'" in problems[0]
    assert sfn.list_executions.call_count == 2


# ══════════════════════════════════════════════════════════════════════════
# aggregation, dedup, handler, registry
# ══════════════════════════════════════════════════════════════════════════

def test_iter_check_specs_orders_playbooks_then_watch_plane():
    reg = {
        "playbooks": {
            "b-pb": {"liveness": {"checks": [{"type": "lambda_active", "function": "alpha-engine-b"}]}},
            "a-pb": {"liveness": {"checks": [{"type": "lambda_active", "function": "alpha-engine-a"}]}},
            "no-liveness": {},
        },
        "watch_plane_liveness": {"checks": [{"type": "sqs_queue_exists", "queue_name": "q"}]},
    }
    specs = index._iter_check_specs(reg)
    labels = [lbl for lbl, _ in specs]
    assert labels == ["playbook:a-pb", "playbook:b-pb", "watch_plane"]


def test_run_checks_unknown_type_raises_registry_error():
    reg = {"playbooks": {"x": {"liveness": {"checks": [{"type": "bogus_check"}]}}}}
    with patch("index._registry", return_value=reg):
        with pytest.raises(index._RegistryError):
            index._run_checks(NOW)


def test_run_checks_aggregates_problems_and_kill_switches():
    reg = {
        "playbooks": {
            "sf-watch": {"liveness": {"checks": [
                {"type": "lambda_active", "function": "alpha-engine-spot",
                 "report_kill_switch": "SF_WATCH_DISPATCH_ENABLED"},
            ]}},
        },
        "watch_plane_liveness": {"checks": [
            {"type": "sqs_queue_exists", "queue_name": "nousergon-overseer-intake"},
        ]},
    }
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(state="Pending",
                                                             env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    sqs = MagicMock()
    sqs.get_queue_url.side_effect = FakeClientError("QueueDoesNotExist")
    with patch("index._registry", return_value=reg), \
         patch("index.boto3.client", side_effect=_client_factory(**{"lambda": lam, "sqs": sqs})):
        problems, ks = index._run_checks(NOW)
    assert any("not Active" in p for p in problems)
    assert any("does NOT EXIST" in p for p in problems)
    assert ks == {"SF_WATCH_DISPATCH_ENABLED": "true"}


def test_registry_malformed_raises():
    with patch("index.REGISTRY_PATH", Path("/nonexistent/playbooks.yaml")):
        index._REGISTRY_CACHE = None
        with pytest.raises(index._RegistryError):
            index._registry()
    index._REGISTRY_CACHE = None


def _handler_with(problems, kill_switches=None, state_fingerprint=None):
    """Drive handler with _run_checks + S3 dedup state stubbed."""
    s3 = MagicMock()
    if state_fingerprint is None:
        s3.get_object.side_effect = FakeClientError("NoSuchKey")
    else:
        s3.get_object.return_value = {"Body": _Body(json.dumps({"fingerprint": state_fingerprint}).encode())}
    notify = MagicMock(return_value=True)
    with patch("index._run_checks", return_value=(problems, kill_switches or {})), \
         patch("index._s3_client", return_value=s3), \
         patch("index.notify_via_flow_doctor", notify):
        result = index.handler({}, None)
    return result, s3, notify


def test_handler_clean_no_alert():
    result, s3, notify = _handler_with([])
    assert result["clean"] is True and result["alerted"] is False
    notify.assert_not_called()


def test_handler_new_problem_alerts_and_persists_fingerprint():
    result, s3, notify = _handler_with(["EventBridge rule 'r' does NOT EXIST"])
    assert result["alerted"] is True
    notify.assert_called_once()
    s3.put_object.assert_called_once()  # fingerprint persisted


def test_handler_unchanged_problem_suppressed():
    fp = index._problem_fingerprint(["EventBridge rule 'r' does NOT EXIST"])
    result, s3, notify = _handler_with(["EventBridge rule 'r' does NOT EXIST"], state_fingerprint=fp)
    assert result["alerted"] is False
    notify.assert_not_called()


def test_handler_clears_dedup_state_when_healthy_again():
    result, s3, notify = _handler_with([], state_fingerprint="stale-fp")
    assert result["clean"] is True
    # cleared: put_object called with fingerprint None
    s3.put_object.assert_called_once()
    body = json.loads(s3.put_object.call_args.kwargs["Body"].decode())
    assert body["fingerprint"] is None


# ── Real-registry integration (the whole point: the shipped registry drives it)

_REAL_REGISTRY = index.yaml.safe_load(
    (Path(__file__).resolve().parents[3] / "infrastructure" / "overseer" / "playbooks.yaml").read_text()
)


def test_real_registry_only_uses_known_check_types():
    """A registry declaring a check type the probe can't run would fail-loud at
    runtime — catch it here instead. Guards the registry↔probe contract."""
    used = {spec["type"] for _, spec in index._iter_check_specs(_REAL_REGISTRY)}
    assert used, "real registry declares no liveness checks"
    unknown = used - set(index.CHECKERS)
    assert not unknown, f"registry uses check types with no checker: {sorted(unknown)}"


def test_real_registry_sf_watch_pipelines_anchor_shared():
    """The eventbridge_rule expect list and the state_machines_exist list are one
    YAML anchor — they must be identical (the whole reason for the anchor)."""
    checks = _REAL_REGISTRY["playbooks"]["sf-watch"]["liveness"]["checks"]
    ebr = next(c for c in checks if c["type"] == "eventbridge_rule")
    sme = next(c for c in checks if c["type"] == "state_machines_exist")
    assert ebr["expect_state_machines"] == sme["state_machines"]


def test_real_registry_watch_plane_covers_dispatcher_and_intake():
    checks = _REAL_REGISTRY["watch_plane_liveness"]["checks"]
    functions = {c.get("function") for c in checks if c["type"] == "lambda_active"}
    queues = {c.get("queue_name") for c in checks if c["type"] == "sqs_queue_exists"}
    assert "alpha-engine-overseer-dispatcher" in functions
    assert "nousergon-overseer-intake" in queues
