"""Unit tests for the alpha-engine-spot-orphan-reaper Lambda handler.

Mocks boto3 EC2 + CloudWatch clients so tests run without AWS calls.
Locks the budget table, age-threshold + grace math, dry-run semantics,
and partial-failure resilience.
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


@pytest.fixture
def index_module(monkeypatch):
    """Reload the handler module with the test env so module-level vars resolve."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
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


class TestBudgetTable:
    def test_data_weekly_budget(self, index_module):
        assert index_module._budget_for_name("alpha-engine-data-weekly-20260511") == 5400

    def test_drift_budget(self, index_module):
        assert index_module._budget_for_name("alpha-engine-drift-20260511") == 1800

    def test_train_budget(self, index_module):
        assert index_module._budget_for_name("alpha-engine-gbm-train-20260511") == 5400

    def test_backtest_budget(self, index_module):
        assert index_module._budget_for_name("alpha-engine-backtest-20260511") == 7200

    def test_unknown_falls_to_default(self, index_module):
        assert index_module._budget_for_name("alpha-engine-mystery-20260511") == 7200

    def test_matched_prefix(self, index_module):
        assert index_module._matched_prefix("alpha-engine-backtest-20260511") == "alpha-engine-backtest-"
        assert index_module._matched_prefix("alpha-engine-novel-20260511") == "alpha-engine-other-"


class TestHandler:
    def test_no_orphans_when_all_young(self, index_module):
        # Both spots are well within budget — none should be terminated.
        spots = [
            _spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=600),
            _spot("i-0002", "alpha-engine-data-weekly-20260511", age_seconds=900),
        ]
        ec2 = MagicMock()
        ec2.get_paginator.return_value = _describe_instances_paginator(spots)
        cw = MagicMock()

        with patch.object(index_module.boto3, "client", side_effect=lambda svc, **kw: ec2 if svc == "ec2" else cw):
            out = index_module.handler({}, None)

        assert out["scanned"] == 2
        assert out["orphans_detected"] == 0
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()
        cw.put_metric_data.assert_not_called()

    def test_terminates_orphan_past_budget_plus_grace(self, index_module):
        # backtest budget=7200, grace=1800 → threshold 9000s; spot age 10000s → orphan
        spots = [
            _spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=10000),
            _spot("i-0002", "alpha-engine-drift-20260511", age_seconds=600),  # young
        ]
        ec2 = MagicMock()
        ec2.get_paginator.return_value = _describe_instances_paginator(spots)
        cw = MagicMock()

        with patch.object(index_module.boto3, "client", side_effect=lambda svc, **kw: ec2 if svc == "ec2" else cw):
            out = index_module.handler({}, None)

        assert out["scanned"] == 2
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-0001"]
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-0001"])
        cw.put_metric_data.assert_called_once()

    def test_grace_buffer_protects_just_over_budget(self, index_module):
        # backtest budget=7200; spot 7800s old (over budget, within grace) → NOT orphan
        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=7800)]
        ec2 = MagicMock()
        ec2.get_paginator.return_value = _describe_instances_paginator(spots)
        cw = MagicMock()

        with patch.object(index_module.boto3, "client", side_effect=lambda svc, **kw: ec2 if svc == "ec2" else cw):
            out = index_module.handler({}, None)

        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_dry_run_does_not_terminate(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "true")
        if "index" in sys.modules:
            del sys.modules["index"]
        index_module = importlib.import_module("index")

        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=10000)]
        ec2 = MagicMock()
        ec2.get_paginator.return_value = _describe_instances_paginator(spots)
        cw = MagicMock()

        with patch.object(index_module.boto3, "client", side_effect=lambda svc, **kw: ec2 if svc == "ec2" else cw):
            out = index_module.handler({}, None)

        assert out["dry_run"] is True
        assert out["orphans_detected"] == 1
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()

    def test_terminate_failure_is_logged_but_does_not_crash(self, index_module):
        spots = [
            _spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=10000),
            _spot("i-0002", "alpha-engine-backtest-20260511", age_seconds=11000),
        ]
        ec2 = MagicMock()
        ec2.get_paginator.return_value = _describe_instances_paginator(spots)
        # First terminate raises; second succeeds — verify we continue past the first
        ec2.terminate_instances.side_effect = [
            Exception("simulated AWS error"),
            {"TerminatingInstances": [{"InstanceId": "i-0002"}]},
        ]
        cw = MagicMock()

        with patch.object(index_module.boto3, "client", side_effect=lambda svc, **kw: ec2 if svc == "ec2" else cw):
            out = index_module.handler({}, None)

        assert out["orphans_detected"] == 2
        # Only the second succeeds; first's failure does not abort the loop
        assert out["terminated"] == ["i-0002"]
        assert ec2.terminate_instances.call_count == 2
