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


def _details(findings):
    """Findings carry a scannable headline AND the full prose; assertions about
    what was FOUND read `detail`, so a wording change to the page can never
    quietly turn a coverage test green."""
    return [f["detail"] for f in findings]


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
    assert problems and all("filed NO" in p["detail"] for p in problems)


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
    assert any("2026-07-16 01:00" in p["detail"] for p in problems)
    assert not any("2026-07-17 01:00" in p["detail"] for p in problems)


# ══════════════════════════════════════════════════════════════════════════
# scheduler_schedule_exists
# ══════════════════════════════════════════════════════════════════════════

_SCHED_SPEC = {"type": "scheduler_schedule_exists", "schedule_name": "alpha-engine-alert-drain-1000utc"}


def test_scheduler_schedule_exists_clean():
    sched = MagicMock()
    sched.get_schedule.return_value = {"Name": "alpha-engine-alert-drain-1000utc", "State": "ENABLED"}
    with patch("index.boto3.client", side_effect=_client_factory(scheduler=sched)):
        problems, _ = index._check_scheduler_schedule_exists(_SCHED_SPEC, NOW)
    assert problems == []


def test_scheduler_schedule_missing():
    sched = MagicMock()
    sched.get_schedule.side_effect = FakeClientError("ResourceNotFoundException")
    with patch("index.boto3.client", side_effect=_client_factory(scheduler=sched)):
        problems, _ = index._check_scheduler_schedule_exists(_SCHED_SPEC, NOW)
    assert len(problems) == 1 and "does NOT EXIST" in problems[0]


def test_scheduler_schedule_disabled_is_a_problem():
    sched = MagicMock()
    sched.get_schedule.return_value = {"Name": "alpha-engine-alert-drain-1000utc", "State": "DISABLED"}
    with patch("index.boto3.client", side_effect=_client_factory(scheduler=sched)):
        problems, _ = index._check_scheduler_schedule_exists(_SCHED_SPEC, NOW)
    assert any("not ENABLED" in p for p in problems)


def test_scheduler_schedule_unexpected_error_raises():
    sched = MagicMock()
    sched.get_schedule.side_effect = FakeClientError("ThrottlingException")
    with patch("index.boto3.client", side_effect=_client_factory(scheduler=sched)):
        with pytest.raises(FakeClientError):
            index._check_scheduler_schedule_exists(_SCHED_SPEC, NOW)


# ══════════════════════════════════════════════════════════════════════════
# sf_watch_invocation_success
# ══════════════════════════════════════════════════════════════════════════

_SFI_SPEC = {
    "type": "sf_watch_invocation_success",
    "pipelines": [
        {"state_machine": "ne-weekly-freshness-pipeline", "watch_prefix": "consolidated/saturday_sf_watch"},
    ],
    "response_window_min": 10,
    "lookback_hours": 24,
}

_SFI_EXEC_ARN = f"arn:aws:states:{REG}:{ACCT}:execution:ne-weekly-freshness-pipeline:run1"


def _sfn_for_invocation_success(executions, describe_input=None, describe_raises=None):
    """Serves ``executions`` only for the FAILED statusFilter call (matching
    real-world single-status fixtures below) and an empty page for
    TIMED_OUT/ABORTED — the real check queries all 3 status filters
    unconditionally, so a naive return_value-for-everything mock would
    triple-count each fixture execution."""
    sfn = MagicMock()

    def _list_executions(**kwargs):
        if kwargs.get("statusFilter") == "FAILED":
            return {"executions": executions}
        return {"executions": []}

    sfn.list_executions.side_effect = _list_executions
    if describe_raises:
        sfn.describe_execution.side_effect = FakeClientError(describe_raises)
    else:
        sfn.describe_execution.return_value = {"input": json.dumps(describe_input or {})}
    return sfn


def _mature_execution(stop_offset_min=30, name="run1", status="FAILED", arn=_SFI_EXEC_ARN):
    return {
        "executionArn": arn,
        "name": name,
        "status": status,
        "startDate": NOW - timedelta(hours=1),
        "stopDate": NOW - timedelta(minutes=stop_offset_min),
    }


def test_sf_watch_invocation_success_event_recorded_is_clean():
    execu = _mature_execution()
    sfn = _sfn_for_invocation_success([execu], describe_input={"run_date": "2026-07-17"})
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": _Body(json.dumps(
        {"events": [{"execution_arn": _SFI_EXEC_ARN}]}).encode())}
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)
    assert problems == []
    s3.get_object.assert_called_once_with(
        Bucket=index.WATCH_BUCKET, Key="consolidated/saturday_sf_watch/2026-07-17.json")


def test_sf_watch_invocation_success_missing_event_is_a_finding():
    execu = _mature_execution()
    sfn = _sfn_for_invocation_success([execu], describe_input={"run_date": "2026-07-17"})
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": _Body(json.dumps({"events": []}).encode())}
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)
    assert len(problems) == 1
    assert "NO" in problems[0]["detail"] and "matching watch-log event" in problems[0]["detail"]


def test_sf_watch_invocation_success_no_watch_log_at_all_is_a_finding():
    """The exact 2026-07-17 incident class: the dispatcher never even wrote a
    fresh watch-log for the day (crashed before its PRIMARY write)."""
    execu = _mature_execution()
    sfn = _sfn_for_invocation_success([execu], describe_input={"run_date": "2026-07-17"})
    s3 = MagicMock()
    s3.get_object.side_effect = FakeClientError("NoSuchKey")
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)
    assert len(problems) == 1


def test_sf_watch_invocation_success_immature_failure_skipped():
    """A failure younger than response_window_min is NOT yet a finding — the
    dispatcher may still be mid-flight."""
    execu = _mature_execution(stop_offset_min=2)  # 2 min ago, window is 10 min
    sfn = _sfn_for_invocation_success([execu])
    s3 = MagicMock()
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)
    assert problems == []
    s3.get_object.assert_not_called()


def test_sf_watch_invocation_success_run_date_falls_back_to_start_date():
    """A malformed/absent run_date in the execution input degrades to the
    startDate's calendar date (mirrors the producer's own tolerant fallback)."""
    execu = _mature_execution()
    execu["startDate"] = datetime(2026, 7, 16, 23, 0, tzinfo=timezone.utc)
    sfn = _sfn_for_invocation_success([execu], describe_input={})  # no run_date key
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": _Body(json.dumps(
        {"events": [{"execution_arn": _SFI_EXEC_ARN}]}).encode())}
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)
    assert problems == []
    s3.get_object.assert_called_once_with(
        Bucket=index.WATCH_BUCKET, Key="consolidated/saturday_sf_watch/2026-07-16.json")


def test_sf_watch_invocation_success_list_executions_unexpected_error_raises():
    execu = _mature_execution()
    sfn = MagicMock()
    sfn.list_executions.side_effect = FakeClientError("AccessDenied")
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=MagicMock()):
        with pytest.raises(FakeClientError):
            index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)


def test_sf_watch_invocation_success_watch_log_read_unexpected_error_raises():
    """The exact 2026-07-17 bug class from the DISPATCHER side, verified from
    the PROBE side too: a 403/AccessDenied reading the watch-log must RAISE,
    never be treated as 'no events yet' (which would hide the crash)."""
    execu = _mature_execution()
    sfn = _sfn_for_invocation_success([execu], describe_input={"run_date": "2026-07-17"})
    s3 = MagicMock()
    s3.get_object.side_effect = FakeClientError("AccessDenied")
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        with pytest.raises(FakeClientError):
            index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)


def test_sf_watch_invocation_success_pagination_stops_past_horizon():
    fresh = _mature_execution(stop_offset_min=30, name="fresh")
    stale = dict(_mature_execution(stop_offset_min=30, name="stale"))
    stale["stopDate"] = NOW - timedelta(hours=48)  # older than 24h lookback
    sfn = MagicMock()

    def _list_executions(**kwargs):
        if kwargs.get("statusFilter") == "FAILED":
            assert "nextToken" not in kwargs, "pagination must stop once past horizon"
            return {"executions": [fresh, stale], "nextToken": "should-not-be-used"}
        return {"executions": []}

    sfn.list_executions.side_effect = _list_executions
    sfn.describe_execution.return_value = {"input": json.dumps({"run_date": "2026-07-17"})}
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": _Body(json.dumps(
        {"events": [{"execution_arn": _SFI_EXEC_ARN}]}).encode())}
    with patch("index._sfn_client", return_value=sfn), patch("index._s3_client", return_value=s3):
        problems, _ = index._check_sf_watch_invocation_success(_SFI_SPEC, NOW)
    assert problems == []
    # Only the in-horizon execution triggers a describe/get_object call — the
    # stale one is excluded and pagination stops (nextToken never consulted).
    assert sfn.describe_execution.call_count == 1


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
        problems, ks, run, failed = index._run_checks(NOW)
    details = _details(problems)
    assert any("not Active" in p for p in details)
    assert any("does NOT EXIST" in p for p in details)
    assert ks == {"SF_WATCH_DISPATCH_ENABLED": "true"}
    assert (run, failed) == (2, 0)


# ── I4473: per-check isolation ───────────────────────────────────────────────


def _isolation_registry():
    """One check that will blow up (scheduler) + two that will report normally."""
    return {
        "playbooks": {
            "sf-watch": {"liveness": {"checks": [
                {"type": "lambda_active", "function": "alpha-engine-spot",
                 "report_kill_switch": "SF_WATCH_DISPATCH_ENABLED"},
            ]}},
        },
        "watch_plane_liveness": {"checks": [
            {"type": "scheduler_schedule_exists",
             "schedule_name": "alpha-engine-alert-drain-0400utc"},
            {"type": "sqs_queue_exists", "queue_name": "nousergon-overseer-intake"},
        ]},
    }


def test_run_checks_isolates_a_raising_check_and_still_runs_the_others():
    """The 2026-07-23 regression: an AccessDenied on ONE check aborted the whole
    handler, erasing ~25 checks' coverage for four days."""
    lam = MagicMock()
    lam.get_function_configuration.return_value = _lambda_cfg(
        state="Pending", env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    sched = MagicMock()
    sched.get_schedule.side_effect = FakeClientError("AccessDeniedException")
    sqs = MagicMock()
    sqs.get_queue_url.side_effect = FakeClientError("QueueDoesNotExist")
    with patch("index._registry", return_value=_isolation_registry()), \
         patch("index.boto3.client", side_effect=_client_factory(
             **{"lambda": lam, "scheduler": sched, "sqs": sqs})):
        problems, ks, run, failed = index._run_checks(NOW)

    details = _details(problems)
    # the raising check became its own problem line...
    assert any("FAILED TO RUN" in p and "scheduler_schedule_exists" in p for p in details)
    # ...and BOTH other checks still reported their real findings
    assert any("not Active" in p for p in details)
    assert any("does NOT EXIST" in p for p in details)
    assert ks == {"SF_WATCH_DISPATCH_ENABLED": "true"}
    assert (run, failed) == (3, 1)
    assert not any("PROBE BLIND" in p for p in details)


def test_run_checks_unknown_type_still_raises_not_isolated():
    """An unknown check type is a packaging bug that invalidates the whole
    registry — it must stay fail-loud, NOT be isolated like a runtime error."""
    reg = {"playbooks": {"x": {"liveness": {"checks": [{"type": "bogus_check"}]}}}}
    with patch("index._registry", return_value=reg):
        with pytest.raises(index._RegistryError):
            index._run_checks(NOW)


def test_run_checks_all_failing_reports_probe_blind_first():
    reg = {"watch_plane_liveness": {"checks": [
        {"type": "scheduler_schedule_exists", "schedule_name": "s-one"},
        {"type": "scheduler_schedule_exists", "schedule_name": "s-two"},
    ]}}
    sched = MagicMock()
    sched.get_schedule.side_effect = FakeClientError("AccessDeniedException")
    with patch("index._registry", return_value=reg), \
         patch("index.boto3.client", side_effect=_client_factory(scheduler=sched)):
        problems, _ks, run, failed = index._run_checks(NOW)
    assert run == failed == 2
    assert problems[0]["detail"].startswith("PROBE BLIND")
    assert "all 2 liveness checks failed to run" in problems[0]["detail"]


def test_run_checks_partial_failure_is_not_probe_blind():
    reg = {"watch_plane_liveness": {"checks": [
        {"type": "scheduler_schedule_exists", "schedule_name": "s-one"},
        {"type": "sqs_queue_exists", "queue_name": "nousergon-overseer-intake"},
    ]}}
    sched = MagicMock()
    sched.get_schedule.side_effect = FakeClientError("AccessDeniedException")
    sqs = MagicMock()
    sqs.get_queue_url.return_value = {"QueueUrl": "https://q"}
    sqs.get_queue_attributes.return_value = {"Attributes": {"RedrivePolicy": json.dumps(
        {"deadLetterTargetArn": "arn:aws:sqs:us-east-1:711398986525:nousergon-overseer-intake-dlq"})}}
    with patch("index._registry", return_value=reg), \
         patch("index.boto3.client", side_effect=_client_factory(scheduler=sched, sqs=sqs)):
        problems, _ks, run, failed = index._run_checks(NOW)
    assert (run, failed) == (2, 1)
    assert not any("PROBE BLIND" in p for p in _details(problems))


def test_registry_malformed_raises():
    with patch("index.REGISTRY_PATH", Path("/nonexistent/playbooks.yaml")):
        index._REGISTRY_CACHE = None
        with pytest.raises(index._RegistryError):
            index._registry()
    index._REGISTRY_CACHE = None


def _handler_with(problems, kill_switches=None, state_per_problem=None,
                  checks_run=None, checks_failed=0):
    """Drive handler with _run_checks + S3 dedup state stubbed.

    ``state_per_problem`` is the prior ``per_problem`` dict (maps stable-key
    to problem-text). ``None`` means the state object never existed (first
    run); ``{}`` means the state was cleared (previously healthy).

    Plain strings are wrapped as findings (headline == detail) so these tests
    stay about handler behaviour rather than message shape."""
    problems = [p if isinstance(p, dict) else index._finding("test", p) for p in problems]
    s3 = MagicMock()
    if state_per_problem is None:
        s3.get_object.side_effect = FakeClientError("NoSuchKey")
    else:
        s3.get_object.return_value = {"Body": _Body(json.dumps(
            {"per_problem": state_per_problem}).encode())}
    notify = MagicMock(return_value=True)
    if checks_run is None:
        checks_run = max(len(problems), 1)
    with patch("index._run_checks",
               return_value=(problems, kill_switches or {}, checks_run, checks_failed)), \
         patch("index._s3_client", return_value=s3), \
         patch("index.notify_via_flow_doctor", notify):
        result = index.handler({}, None)
    return result, s3, notify


def test_handler_reports_check_coverage_counts():
    result, _s3, _notify = _handler_with([], checks_run=25, checks_failed=0)
    assert result["checks_run"] == 25 and result["checks_failed"] == 0


def test_handler_alert_names_incomplete_coverage():
    result, _s3, notify = _handler_with(
        ["watch_plane: liveness check 'scheduler_schedule_exists' FAILED TO RUN (X: y)"],
        checks_run=25, checks_failed=1,
    )
    assert result["checks_failed"] == 1
    text = notify.call_args[0][0]
    assert "Coverage incomplete" in text and "1 of 25" in text
    assert "UNVERIFIED, not confirmed healthy" in text


def test_handler_clean_run_alert_omits_coverage_caveat():
    _result, _s3, notify = _handler_with(["some unrelated finding"],
                                         checks_run=25, checks_failed=0)
    assert "Coverage incomplete" not in notify.call_args[0][0]


def test_handler_clean_no_alert():
    result, s3, notify = _handler_with([])
    assert result["clean"] is True and result["alerted"] is False
    notify.assert_not_called()


def test_handler_clears_dedup_state_when_healthy_again():
    """After a problem clears, the dedup state is written as {} (not None)
    so the next clean-fast-path does not re-trigger."""
    result, s3, notify = _handler_with([], state_per_problem={"fp-old": "previous problem"})
    assert result["clean"] is True
    s3.put_object.assert_called_once()
    body = json.loads(s3.put_object.call_args.kwargs["Body"].decode())
    assert body["per_problem"] == {}


def test_handler_first_ever_run_alerts_everything():
    """First run (state_per_problem=None) — every problem is 'new'."""
    problems = ["Lambda 'x' does NOT EXIST", "rule 'y' is DISABLED"]
    result, s3, notify = _handler_with(problems, state_per_problem=None)
    assert result["alerted"] is True
    assert result["new"] == 2
    assert result["continuing"] == 0
    notify.assert_called_once()
    # two writes: the detail report the page points at, then the dedup state
    written = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
    assert any(k.startswith(index.REPORT_PREFIX) for k in written)
    assert index.STATE_KEY in written
    text = notify.call_args[0][0]
    assert "2 new" in text
    assert "\u2022 *test*: Lambda 'x' does NOT EXIST" in text
    assert "\u2022 *test*: rule 'y' is DISABLED" in text


def test_handler_new_problem_added_pages_only_new():
    """Two problems persist; ONE new one appears — only the new one pages."""
    p1 = "Lambda 'x' does NOT EXIST"
    p2 = "rule 'y' is DISABLED"
    p3 = "sqs queue 'q' does NOT EXIST"
    state = {index._stable_problem_key(p): p for p in [p1, p2]}
    problems = [p1, p2, p3]
    result, s3, notify = _handler_with(problems, state_per_problem=state)
    assert result["alerted"] is True
    assert result["new"] == 1
    assert result["continuing"] == 2
    text = notify.call_args[0][0]
    assert "\u2022 *test*: sqs queue 'q' does NOT EXIST" in text
    assert "Lambda 'x' does NOT EXIST" not in text  # continuing, not re-listed
    assert "rule 'y' is DISABLED" not in text
    assert "continuing" in text


def test_handler_problem_set_unchanged_suppressed():
    """Exactly the same problem set as last time -> no alert."""
    p1 = "Lambda 'x' does NOT EXIST"
    state = {index._stable_problem_key(p): p for p in [p1]}
    result, s3, notify = _handler_with([p1], state_per_problem=state)
    assert result["alerted"] is False
    notify.assert_not_called()


def test_handler_problem_resolved_without_new_is_suppressed():
    """One problem was alerted; now it's gone — no alert (the count just
    decreased to zero silently)."""
    p1 = "Lambda 'x' does NOT EXIST"
    state = {index._stable_problem_key(p): p for p in [p1]}
    result, s3, notify = _handler_with([], state_per_problem=state)
    assert result["alerted"] is False
    assert result["clean"] is True
    notify.assert_not_called()
    # state cleared
    body = json.loads(s3.put_object.call_args.kwargs["Body"].decode())
    assert body["per_problem"] == {}


def test_handler_problem_changed_replaces_old():
    """Problem text changed → the old fingerprint resolves, new one alerts."""
    old_p = "run '01:00' @ 2026-07-28 01:00Z filed NO report"
    new_p = "run '01:00' @ 2026-07-29 01:00Z filed NO report"
    state = {index._stable_problem_key(old_p): old_p}
    result, s3, notify = _handler_with([new_p], state_per_problem=state)
    assert result["alerted"] is True
    assert result["new"] == 1
    assert result["continuing"] == 0
    assert result["resolved"] == 1
    text = notify.call_args[0][0]
    assert "\u2022 *test*:" in text and "2026-07-29" in text


def test_handler_problem_reappears_after_resolution_pages_again():
    """A problem that was seen, aged out, then returns should page again."""
    p = "Lambda 'x' does NOT EXIST"
    # First run: pages
    result1, s3, _notify = _handler_with([p], state_per_problem=None)
    assert result1["alerted"] is True

    # Problem resolves
    result2, _s3, _notify2 = _handler_with([], state_per_problem={index._stable_problem_key(p): p})
    assert result2["clean"] is True

    # Problem returns → should page again (state was cleared)
    result3, _s3, notify3 = _handler_with([p], state_per_problem=None)
    assert result3["alerted"] is True
    assert result3["new"] == 1


def test_handler_new_problem_count_capped_at_five():
    """More than 5 new problems lists 5 then says '...and N more'."""
    problems = [f"problem #{i}" for i in range(10)]
    result, s3, notify = _handler_with(problems, state_per_problem=None)
    assert result["alerted"] is True
    assert result["new"] == 10
    text = notify.call_args[0][0]
    # First 5 with the 🆕 prefix
    assert text.count("\u2022 *test*:") == index._MAX_HEADLINES
    assert f"\u2026and {10 - index._MAX_HEADLINES} more new" in text


def test_handler_return_includes_diff_counts():
    """Handler return dict includes new/continuing/resolved breakdown."""
    p_old = "Lambda 'x' does NOT EXIST"
    p_new = "rule 'y' is DISABLED"
    state = {index._stable_problem_key(p_old): p_old}
    result, _s3, _notify = _handler_with([p_old, p_new], state_per_problem=state)
    assert result["new"] == 1
    assert result["continuing"] == 1
    assert result["resolved"] == 0


def test_handler_old_format_state_treated_as_first_run():
    """Old single-fingerprint format → treated as None (first run)."""
    s3 = MagicMock()
    # Old state format: {"fingerprint": "...", "updated_at": "..."}
    s3.get_object.return_value = {"Body": _Body(json.dumps(
        {"fingerprint": "abc123", "updated_at": "2026-07-01T00:00:00+00:00"}).encode())}
    notify = MagicMock(return_value=True)
    # _run_checks is the sole producer of findings and always normalises to
    # dicts, so the stub must too — a raw string here would test a shape
    # production cannot produce.
    with patch("index._run_checks",
               return_value=([index._finding("test", "rule 'r' does NOT EXIST")], {}, 1, 0)), \
         patch("index._s3_client", return_value=s3), \
         patch("index.notify_via_flow_doctor", notify):
        result = index.handler({}, None)
    assert result["alerted"] is True
    assert result["new"] == 1


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


# ══════════════════════════════════════════════════════════════════════════
# run_window: productive_when — an artifact proves a BOOT, not a RUN
# ══════════════════════════════════════════════════════════════════════════
#
# 2026-07-28: all three groom lanes crash-cascaded two minutes after boot (a
# comment truncated the bootstrap's `runuser`, so groom_run.sh ran as root and
# every chunk agent refused to start). Each lane still wrote a well-formed run
# artifact, so the existence-only check reported full coverage while 378 issues
# went un-dispositioned.

_RW_SPEC_PRODUCTIVE = dict(
    _RW_SPEC,
    productive_when=[{"field": "engaged", "gt": 0}, {"field": "total_issues", "eq": 0}],
)

# Verbatim fields from s3://alpha-engine-research/groom/2026-07-28/
# 9d2004971a8e4b39ad554a47cd80ae39.json (the high lane).
_REAL_CRASH_CASCADE_ARTIFACT = {
    "schema_version": 10,
    "run_start": "2026-07-17T01:05:00+00:00",  # re-dated into this suite's window
    "run_kind": "coverage",
    "issue_filter": "high-only",
    "stop_reason": "3 consecutive chunk-agent crashes — aborting the run loudly "
                   "(18 issues un-dispositioned) instead of burning the queue's "
                   "re-queue budget",
    "floor_fail": True,
    "spot_interrupted": False,
    "engaged": 0,
    "floor": 8,
    "total_issues": 21,
    "processed": 9,
    "undispositioned": 21,
    "elapsed_min": 3,
}


def test_crash_cascaded_run_is_flagged_despite_writing_an_artifact():
    """The regression this check exists for, driven by the real artifact."""
    s3 = _FakeRunWindowS3({"2026-07-17": [_REAL_CRASH_CASCADE_ARTIFACT]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC_PRODUCTIVE, NOW)
    assert len(problems) == 1, problems
    assert "RAN BUT DID NO WORK" in problems[0]["detail"]
    assert "engaged=0" in problems[0]["detail"] and "total_issues=21" in problems[0]["detail"]
    assert "chunk-agent crashes" in problems[0]["detail"]
    # ...and it must NOT also be reported as a missing artifact: the trigger IS
    # covered. Two different findings; only the productivity one applies.
    assert "filed NO" not in problems[0]


def test_same_artifact_without_the_predicate_is_still_silent():
    """Existence-only semantics are preserved for specs that don't opt in —
    so adding the key to one registry entry cannot change another's."""
    s3 = _FakeRunWindowS3({"2026-07-17": [_REAL_CRASH_CASCADE_ARTIFACT]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC, NOW)
    assert problems == []


def test_productive_run_is_silent():
    art = dict(_REAL_CRASH_CASCADE_ARTIFACT, engaged=7, floor_fail=False, stop_reason="")
    s3 = _FakeRunWindowS3({"2026-07-17": [art]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC_PRODUCTIVE, NOW)
    assert problems == []


def test_empty_queue_is_a_legitimate_zero():
    """engaged=0 with nothing to engage is a healthy run, not a dead one."""
    art = dict(_REAL_CRASH_CASCADE_ARTIFACT, engaged=0, total_issues=0, floor_fail=False)
    s3 = _FakeRunWindowS3({"2026-07-17": [art]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC_PRODUCTIVE, NOW)
    assert problems == []


def test_one_dead_lane_is_visible_even_when_a_sibling_lane_covered_the_trigger():
    """Reported per-artifact, not per-trigger: the mid lane succeeding must not
    mask the high lane doing nothing."""
    good = dict(_REAL_CRASH_CASCADE_ARTIFACT, engaged=12, floor_fail=False)
    s3 = _FakeRunWindowS3({"2026-07-17": [good, _REAL_CRASH_CASCADE_ARTIFACT]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC_PRODUCTIVE, NOW)
    assert len(problems) == 1 and "RAN BUT DID NO WORK" in problems[0]["detail"]


def test_unknown_operator_raises_rather_than_silently_passing():
    spec = dict(_RW_SPEC, productive_when=[{"field": "engaged", "roughly": 3}])
    s3 = _FakeRunWindowS3({"2026-07-17": [_REAL_CRASH_CASCADE_ARTIFACT]})
    with patch("index._s3_client", return_value=s3):
        with pytest.raises(index._RegistryError):
            index._check_run_window(spec, NOW)


def test_bool_is_not_treated_as_a_number_by_gt():
    """`engaged: True` must not satisfy `{engaged: {gt: 0}}` — Python would say
    True > 0, which would make a malformed artifact look productive."""
    art = dict(_REAL_CRASH_CASCADE_ARTIFACT, engaged=True)
    s3 = _FakeRunWindowS3({"2026-07-17": [art]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC_PRODUCTIVE, NOW)
    assert len(problems) == 1


def test_registry_declares_productive_when_for_groom():
    """The live registry must actually opt groom in — the code path above is
    inert otherwise, which is precisely how this failure stayed invisible."""
    import yaml
    reg = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "overseer" / "playbooks.yaml").read_text()
    )
    checks = [
        c
        for pb in reg["playbooks"].values()
        for c in ((pb.get("liveness") or {}).get("checks") or [])
        if c.get("type") == "run_window" and c.get("label") == "groom"
    ]
    assert checks, "no run_window check labelled 'groom' in the registry"
    for c in checks:
        assert c.get("productive_when"), (
            "the groom run_window check must declare productive_when — without it "
            "a crash-cascaded run that writes an artifact reads as full coverage"
        )


# ══════════════════════════════════════════════════════════════════════════
# run_start_field — the fleet's run artifacts do not share one schema
# (the 2026-07-29 false-page regression)
# ══════════════════════════════════════════════════════════════════════════

# The alert-drain ledger's real shape: `started_at`, never `run_start`.
_DRAIN_SPEC = dict(
    _RW_SPEC,
    label="alert-drain",
    artifact_prefix="groom/",       # _FakeRunWindowS3 keys off the prefix shape only
    run_start_field="started_at",
)


def test_run_window_reads_the_registry_declared_start_field():
    """A drain-ledger-shaped artifact covers its trigger. Before run_start_field
    the reader asked every ledger for `run_start`, got None, skipped it, and
    reported the trigger as a silent death — four consecutive false pages on
    2026-07-29 while the drain was running normally."""
    s3 = _FakeRunWindowS3({"2026-07-17": [{"started_at": "2026-07-17T01:05:00+00:00"}]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_DRAIN_SPEC, NOW)
    assert problems == []


def test_run_window_artifact_missing_declared_start_field_raises():
    """The regression guard proper: a skip here is indistinguishable from a
    genuinely missing run, which is the ONE distinction this check exists to
    make. It must fail loud, not degrade into a false page."""
    s3 = _FakeRunWindowS3({"2026-07-17": [{"run_start": "2026-07-17T01:05:00+00:00"}]})
    with patch("index._s3_client", return_value=s3):
        with pytest.raises(index._RegistryError) as exc:
            index._check_run_window(_DRAIN_SPEC, NOW)
    assert "started_at" in str(exc.value)


def test_run_window_default_start_field_is_unchanged_for_groom():
    """Specs without the key keep reading `run_start` — additive per entry."""
    s3 = _FakeRunWindowS3({"2026-07-17": [{"run_start": "2026-07-17T01:05:00+00:00"}]})
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(_RW_SPEC, NOW)
    assert problems == []


def test_registry_declares_started_at_for_the_alert_drain_ledger():
    """The live registry must name the drain ledger's real field. Without this
    the check above is inert and the drain leg pages on every mature trigger."""
    import yaml
    reg = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "overseer" / "playbooks.yaml").read_text()
    )
    checks = [
        c
        for pb in reg["playbooks"].values()
        for c in ((pb.get("liveness") or {}).get("checks") or [])
        if c.get("type") == "run_window" and c.get("artifact_prefix", "").startswith("overseer/drain_ledger")
    ]
    assert checks, "no run_window check over the alert-drain ledger in the registry"
    for c in checks:
        assert c.get("run_start_field") == "started_at", (
            "the drain ledger keys its start timestamp 'started_at'; reading the "
            "groom artifact's 'run_start' name against it reports 100% of mature "
            "triggers as silent deaths"
        )


# ══════════════════════════════════════════════════════════════════════════
# Page shape — a summons, not a report
# ══════════════════════════════════════════════════════════════════════════


def test_many_missed_runs_render_as_one_headline_not_one_line_each():
    """Five missed triggers used to render as five near-identical paragraphs,
    burying the one finding that differed. They collapse to a single line."""
    s3 = _FakeRunWindowS3({})
    spec = dict(_RW_SPEC, lookback_hours=120)
    with patch("index._s3_client", return_value=s3):
        problems, _ = index._check_run_window(spec, NOW)
    assert len(problems) == 1
    headline = problems[0]["headline"]
    assert "scheduled runs filed no report" in headline
    assert headline.count("\n") == 0
    # every missed trigger is still fully accounted for, in the detail
    assert problems[0]["detail"].count("filed NO") >= 5


def test_page_carries_headlines_and_a_detail_pointer_not_full_prose():
    long_detail = "x" * 400
    finding = index._finding("alert-drain", "1 of 4 scheduled runs filed no report", long_detail)
    _result, s3, notify = _handler_with([finding], checks_run=25, checks_failed=0)
    text = notify.call_args[0][0]
    assert "*alert-drain*: 1 of 4 scheduled runs filed no report" in text
    assert long_detail not in text            # prose stays out of the page
    assert index.REPORT_PREFIX in text        # ...and the page says where it is
    assert any(
        c.kwargs["Key"].startswith(index.REPORT_PREFIX) for c in s3.put_object.call_args_list
    )


def test_page_inlines_detail_when_the_report_write_fails():
    """Honest degradation: a failed report write must never silently drop the
    findings it was carrying."""
    finding = index._finding("groom", "short headline", "the full prose that matters")
    s3 = MagicMock()
    s3.get_object.side_effect = FakeClientError("NoSuchKey")
    s3.put_object.side_effect = [FakeClientError("AccessDenied"), None]
    notify = MagicMock(return_value=True)
    with patch("index._run_checks", return_value=([finding], {}, 25, 0)), \
         patch("index._s3_client", return_value=s3), \
         patch("index.notify_via_flow_doctor", notify):
        index.handler({}, None)
    text = notify.call_args[0][0]
    assert "Detail report unavailable" in text
    assert "the full prose that matters" in text


def test_page_caps_headlines_and_says_how_many_it_dropped():
    many = 12
    findings = [index._finding(f"c{i}", f"headline {i}") for i in range(many)]
    _result, _s3, notify = _handler_with(findings, checks_run=25, checks_failed=0)
    text = notify.call_args[0][0]
    assert "headline 0" in text and f"headline {index._MAX_HEADLINES - 1}" in text
    assert f"headline {index._MAX_HEADLINES}" not in text
    assert f"…and {many - index._MAX_HEADLINES} more" in text


def test_dedup_key_ignores_headline_wording():
    """Dedup keys on DETAIL, so re-wording the page cannot re-page Brian."""
    a = index._finding("groom", "one phrasing", "the finding")
    b = index._finding("groom", "a completely different phrasing", "the finding")
    assert (index._stable_problem_key(a["detail"])
            == index._stable_problem_key(b["detail"]))
