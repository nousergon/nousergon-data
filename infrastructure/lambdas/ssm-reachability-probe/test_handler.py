"""Tests for the SSM reachability probe (alpha-engine-config-I6198).

The probe exists because SSM — the single transport by which every unattended
workload in this fleet receives its work — went unreachable VPC-wide for 2h31m
on 2026-08-03 and emitted nothing. So the load-bearing assertions here are not
"does it count correctly" but:

  - does a box that NEVER registered get counted (the actual failure mode, and
    the one `DescribeInstanceInformation` alone cannot see)
  - is a healthy fleet published as an explicit 0, so that missing data means a
    dead probe rather than a quiet one
  - does a failed scan RAISE instead of publishing a false green
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

_HERE = Path(__file__).resolve().parent


def _load(monkeypatch, *, instances, ssm_info, put_side_effect=None, env=None):
    """Import index.py fresh with boto3 stubbed, return (module, put_calls)."""
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)

    ec2 = mock.MagicMock()
    ec2.get_paginator.return_value.paginate.return_value = [
        {"Reservations": [{"Instances": instances}]}
    ]
    ssm = mock.MagicMock()
    ssm.get_paginator.return_value.paginate.return_value = [
        {"InstanceInformationList": ssm_info}
    ]
    cloudwatch = mock.MagicMock()
    if put_side_effect is not None:
        cloudwatch.put_metric_data.side_effect = put_side_effect

    clients = {"ec2": ec2, "ssm": ssm, "cloudwatch": cloudwatch}

    spec = importlib.util.spec_from_file_location("ssm_probe_index", _HERE / "index.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["ssm_probe_index"] = module
    spec.loader.exec_module(module)
    monkeypatch.setattr(
        module.boto3, "client", lambda name, region_name=None: clients[name]
    )
    return module, cloudwatch


def _instance(iid, name="alpha-engine-groom-spot", *, age_seconds=3600):
    return {
        "InstanceId": iid,
        "LaunchTime": datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
        "Tags": [{"Key": "Name", "Value": name}],
    }


def _datapoints(cloudwatch):
    return cloudwatch.put_metric_data.call_args.kwargs["MetricData"]


def _aggregate(cloudwatch, metric):
    return next(
        d["Value"] for d in _datapoints(cloudwatch)
        if d["MetricName"] == metric and "Dimensions" not in d
    )


def test_counts_an_instance_that_never_registered_with_ssm(monkeypatch):
    """The 2026-08-03 failure mode.

    A box that never registers is ABSENT from DescribeInstanceInformation, so a
    probe built on that call alone reports a healthy fleet while the fleet is
    down. Ground truth has to start from DescribeInstances.
    """
    module, cloudwatch = _load(
        monkeypatch,
        instances=[_instance("i-never-registered")],
        ssm_info=[],  # SSM has never heard of it
    )
    result = module.handler({}, None)

    assert result["unreachable"] == 1
    assert result["unreachable_instances"] == ["i-never-registered"]
    assert _aggregate(cloudwatch, module.UNREACHABLE_METRIC) == 1.0


def test_counts_an_instance_whose_connection_was_lost(monkeypatch):
    """The dashboard box's shape: registered once, now ConnectionLost."""
    module, cloudwatch = _load(
        monkeypatch,
        instances=[_instance("i-dash", "alpha-engine-dashboard")],
        ssm_info=[{"InstanceId": "i-dash", "PingStatus": "ConnectionLost"}],
    )
    result = module.handler({}, None)

    assert result["unreachable"] == 1
    per_box = [d for d in _datapoints(cloudwatch) if "Dimensions" in d]
    assert per_box[0]["Dimensions"] == [{"Name": "name", "Value": "alpha-engine-dashboard"}]


def test_healthy_fleet_publishes_an_explicit_zero(monkeypatch):
    """`no data` must never be indistinguishable from `all reachable`."""
    module, cloudwatch = _load(
        monkeypatch,
        instances=[_instance("i-ok")],
        ssm_info=[{"InstanceId": "i-ok", "PingStatus": "Online"}],
    )
    result = module.handler({}, None)

    assert result["unreachable"] == 0
    assert _aggregate(cloudwatch, module.UNREACHABLE_METRIC) == 0.0, (
        "a healthy scan must publish 0, not publish nothing"
    )


def test_heartbeat_is_published_on_every_invocation(monkeypatch):
    """The detector has to be observable itself, healthy or not."""
    module, cloudwatch = _load(
        monkeypatch,
        instances=[_instance("i-ok")],
        ssm_info=[{"InstanceId": "i-ok", "PingStatus": "Online"}],
    )
    module.handler({}, None)

    assert _aggregate(cloudwatch, module.HEARTBEAT_METRIC) == 1.0


def test_a_freshly_launched_box_is_not_counted(monkeypatch):
    """Registration takes tens of seconds — normal boot is not an outage."""
    module, _ = _load(
        monkeypatch,
        instances=[_instance("i-booting", age_seconds=30)],
        ssm_info=[],
    )
    result = module.handler({}, None)

    assert result["expected"] == 0
    assert result["unreachable"] == 0


def test_a_box_older_than_grace_is_counted(monkeypatch):
    """The boundary the previous test brackets — otherwise grace could be
    infinite and the probe would never report anything."""
    module, _ = _load(
        monkeypatch,
        instances=[_instance("i-stuck", age_seconds=301)],
        ssm_info=[],
    )
    assert module.handler({}, None)["unreachable"] == 1


def test_non_fleet_instances_are_ignored(monkeypatch):
    """An unrelated instance in the account must not page this fleet."""
    module, _ = _load(
        monkeypatch,
        instances=[_instance("i-other", "someone-elses-box")],
        ssm_info=[],
    )
    result = module.handler({}, None)

    assert result["expected"] == 0
    assert result["unreachable"] == 0


def test_the_aggregate_datapoint_survives_truncation(monkeypatch):
    """PutMetricData caps at 1000 datapoints.

    The aggregate is what the alarm evaluates; a large outage must not push it
    out of the call and leave the alarm reading missing-data.
    """
    instances = [_instance(f"i-{n:04d}") for n in range(1200)]
    module, cloudwatch = _load(monkeypatch, instances=instances, ssm_info=[])
    module.handler({}, None)

    sent = _datapoints(cloudwatch)
    assert len(sent) == 1000
    assert sent[0]["MetricName"] == module.UNREACHABLE_METRIC
    assert "Dimensions" not in sent[0]
    assert sent[0]["Value"] == 1200.0


def test_a_failed_scan_raises_rather_than_publishing_a_false_green(monkeypatch):
    """A probe that swallows its own errors is worse than no probe."""
    module, _ = _load(
        monkeypatch,
        instances=[_instance("i-ok")],
        ssm_info=[{"InstanceId": "i-ok", "PingStatus": "Online"}],
        put_side_effect=RuntimeError("cloudwatch throttled"),
    )
    with pytest.raises(RuntimeError, match="cloudwatch throttled"):
        module.handler({}, None)
