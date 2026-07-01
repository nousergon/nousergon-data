"""Unit tests for the alpha-engine-spot-orphan-reaper Lambda handler.

Mocks boto3 EC2 + CloudWatch clients so tests run without AWS calls. Locks the
single-global-cap semantics (config#1492): no per-workload budget table — every
alpha-engine spot is reaped only after the one fleet-wide threshold
(MAX_SPOT_BUDGET_SECONDS + GRACE_SECONDS). Includes the exact regression that
motivated the redesign: a live 6h groom box must NOT be reaped at 2.5–3h.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure the handler module is importable from the test file
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

# Default threshold = MAX_SPOT_BUDGET_SECONDS (21600) + GRACE_SECONDS (1800) = 23400s.
THRESHOLD = 23400


@pytest.fixture
def index_module(monkeypatch):
    """Reload the handler module with the test env so module-level vars resolve."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("MAX_SPOT_BUDGET_SECONDS", "21600")
    monkeypatch.setenv("GRACE_SECONDS", "1800")
    monkeypatch.setenv("DRY_RUN", "false")
    if "index" in sys.modules:
        del sys.modules["index"]
    return importlib.import_module("index")


def _spot(instance_id: str, name: str, age_seconds: int, instance_type: str = "c5.large"):
    """Build a mock describe-instances entry."""
    return {
        "InstanceId": instance_id,
        "InstanceType": instance_type,
        "Tags": [{"Key": "Name", "Value": name}],
        "LaunchTime": datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
    }


def _describe_instances_paginator(spots: list[dict]):
    paginator = MagicMock()
    paginator.paginate.return_value = [{
        "Reservations": [{"Instances": spots}],
    }]
    return paginator


def _run(index_module, spots):
    ec2 = MagicMock()
    ec2.get_paginator.return_value = _describe_instances_paginator(spots)
    cw = MagicMock()
    with patch.object(index_module.boto3, "client",
                      side_effect=lambda svc, **kw: ec2 if svc == "ec2" else cw):
        out = index_module.handler({}, None)
    return out, ec2, cw


class TestThresholdConfig:
    def test_threshold_is_budget_plus_grace(self, index_module):
        assert index_module.REAP_AFTER_SECONDS == THRESHOLD

    def test_threshold_overridable_via_env(self, monkeypatch):
        # A workload that legitimately needs a longer watchdog bumps ONE number.
        monkeypatch.setenv("MAX_SPOT_BUDGET_SECONDS", "28800")  # 8h
        monkeypatch.setenv("GRACE_SECONDS", "1800")
        if "index" in sys.modules:
            del sys.modules["index"]
        mod = importlib.import_module("index")
        assert mod.REAP_AFTER_SECONDS == 30600


class TestHandler:
    def test_live_groom_at_3h_is_not_reaped(self, index_module):
        # REGRESSION (config#1492): the 6h groom box was killed at 2.5h by the old
        # per-workload default. Under the single cap a 3h-old groom is safe.
        spots = [_spot("i-groom", "alpha-engine-groom-spot", age_seconds=10800)]
        out, ec2, _ = _run(index_module, spots)
        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_orphaned_groom_past_threshold_is_reaped(self, index_module):
        # A groom box that outlived its own 6h watchdog + grace is a genuine orphan.
        spots = [_spot("i-groom", "alpha-engine-groom-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, cw = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-groom"]
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-groom"])
        cw.put_metric_data.assert_called_once()

    def test_no_orphans_when_all_young(self, index_module):
        spots = [
            _spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=600),
            _spot("i-0002", "alpha-engine-data-weekly-20260511", age_seconds=7800),
        ]
        out, ec2, cw = _run(index_module, spots)
        assert out["scanned"] == 2
        assert out["orphans_detected"] == 0
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()
        cw.put_metric_data.assert_not_called()

    def test_boundary_just_under_threshold_is_safe(self, index_module):
        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD - 60)]
        out, ec2, _ = _run(index_module, spots)
        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_boundary_just_over_threshold_is_reaped(self, index_module):
        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD + 60)]
        out, ec2, _ = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-0001"]

    def test_dry_run_does_not_terminate(self, monkeypatch):
        monkeypatch.setenv("MAX_SPOT_BUDGET_SECONDS", "21600")
        monkeypatch.setenv("GRACE_SECONDS", "1800")
        monkeypatch.setenv("DRY_RUN", "true")
        if "index" in sys.modules:
            del sys.modules["index"]
        index_module = importlib.import_module("index")

        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD + 600)]
        out, ec2, _ = _run(index_module, spots)
        assert out["dry_run"] is True
        assert out["orphans_detected"] == 1
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()

    def test_terminate_failure_is_logged_but_does_not_crash(self, index_module):
        spots = [
            _spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD + 600),
            _spot("i-0002", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD + 1600),
        ]
        ec2 = MagicMock()
        ec2.get_paginator.return_value = _describe_instances_paginator(spots)
        ec2.terminate_instances.side_effect = [
            Exception("simulated AWS error"),
            {"TerminatingInstances": [{"InstanceId": "i-0002"}]},
        ]
        cw = MagicMock()
        with patch.object(index_module.boto3, "client",
                          side_effect=lambda svc, **kw: ec2 if svc == "ec2" else cw):
            out = index_module.handler({}, None)

        assert out["orphans_detected"] == 2
        assert out["terminated"] == ["i-0002"]
        assert ec2.terminate_instances.call_count == 2
