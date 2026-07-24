"""Unit tests for the alpha-engine-spot-orphan-reaper Lambda handler.

Mocks boto3 EC2 + CloudWatch + S3 clients so tests run without AWS calls.
Locks the single-global-cap semantics (config#1492): no per-workload budget
table — every alpha-engine spot is reaped only after the one fleet-wide
threshold (MAX_SPOT_BUDGET_SECONDS + GRACE_SECONDS). Includes the exact
regression that motivated the redesign: a live 6h groom box must NOT be
reaped at 2.5-3h.

Also covers the ci-watch-dispatcher migration's additive incomplete-reap
alert: ``nousergon_lib.telegram`` is stubbed in sys.modules before `import
index` (config#1746 hermetic-import-guard pattern — same as scheduled-groom-
dispatcher/ci-watch-dispatcher's test files) so this suite stays network-free.
"""

from __future__ import annotations

import importlib
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure the handler module is importable from the test file
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(SCRIPT_DIR.parent))

# Default threshold = MAX_SPOT_BUDGET_SECONDS (21600) + GRACE_SECONDS (1800) = 23400s.
THRESHOLD = 23400


class _FakeSendMessage:
    """Records every call so tests can assert on the alert text/args."""

    def __init__(self):
        self.calls: list[tuple[tuple, dict]] = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return True


@pytest.fixture
def index_module(monkeypatch):
    """Reload the handler module with the test env so module-level vars resolve."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("MAX_SPOT_BUDGET_SECONDS", "21600")
    monkeypatch.setenv("GRACE_SECONDS", "1800")
    monkeypatch.setenv("DRY_RUN", "false")

    # Stub nousergon_lib.telegram (index.py's one git-only import) — derived
    # from index.py's live import graph and asserted below, so a future new
    # import that this stub doesn't cover fails loud here, not at deploy time.
    fake_send_message = _FakeSendMessage()
    tel_mod = types.ModuleType("nousergon_lib.telegram")
    tel_mod.send_message = fake_send_message
    sys.modules["nousergon_lib.telegram"] = tel_mod

    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)

    if "index" in sys.modules:
        del sys.modules["index"]
    mod = importlib.import_module("index")
    mod._test_send_message = fake_send_message  # expose for assertions
    return mod


def _spot(instance_id: str, name: str, age_seconds: int, instance_type: str = "c5.large",
         ci_watch_repo: str | None = None, ci_watch_sha: str | None = None,
         sf_watch_cadence: str | None = None, sf_watch_pipeline: str | None = None,
         sf_watch_run_date: str | None = None, alert_drain_run_id: str | None = None):
    """Build a mock describe-instances entry."""
    tags = [{"Key": "Name", "Value": name}]
    if ci_watch_repo is not None:
        tags.append({"Key": "ci-watch-repo", "Value": ci_watch_repo})
    if ci_watch_sha is not None:
        tags.append({"Key": "ci-watch-sha", "Value": ci_watch_sha})
    if sf_watch_cadence is not None:
        tags.append({"Key": "sf-watch-cadence", "Value": sf_watch_cadence})
    if sf_watch_pipeline is not None:
        tags.append({"Key": "sf-watch-pipeline", "Value": sf_watch_pipeline})
    if sf_watch_run_date is not None:
        tags.append({"Key": "sf-watch-run-date", "Value": sf_watch_run_date})
    if alert_drain_run_id is not None:
        tags.append({"Key": "alert-drain-run-id", "Value": alert_drain_run_id})
    return {
        "InstanceId": instance_id,
        "InstanceType": instance_type,
        "Tags": tags,
        "LaunchTime": datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
    }


def _describe_instances_paginator(spots: list[dict]):
    paginator = MagicMock()
    paginator.paginate.return_value = [{
        "Reservations": [{"Instances": spots}],
    }]
    return paginator


class _NotFound(Exception):
    pass


def _run(index_module, spots, s3_marker_exists: bool = False):
    ec2 = MagicMock()
    ec2.get_paginator.return_value = _describe_instances_paginator(spots)
    cw = MagicMock()
    s3 = MagicMock()
    if s3_marker_exists:
        s3.head_object.return_value = {}
    else:
        s3.head_object.side_effect = _NotFound("404 Not Found")
    clients = {"ec2": ec2, "cloudwatch": cw, "s3": s3}
    with patch.object(index_module.boto3, "client",
                      side_effect=lambda svc, **kw: clients[svc]):
        out = index_module.handler({}, None)
    return out, ec2, cw, s3


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
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_orphaned_groom_past_threshold_is_reaped(self, index_module):
        # A groom box that outlived its own 6h watchdog + grace is a genuine orphan.
        spots = [_spot("i-groom", "alpha-engine-groom-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-groom"]
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-groom"])
        cw.put_metric_data.assert_called_once()
        # NOT a ci-watch box — the incomplete-reap alert must never fire.
        assert out["ci_watch_incomplete_reaps"] == []
        assert index_module._test_send_message.calls == []

    def test_no_orphans_when_all_young(self, index_module):
        spots = [
            _spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=600),
            _spot("i-0002", "alpha-engine-data-weekly-20260511", age_seconds=7800),
        ]
        out, ec2, cw, _s3 = _run(index_module, spots)
        assert out["scanned"] == 2
        assert out["orphans_detected"] == 0
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()
        cw.put_metric_data.assert_not_called()

    def test_boundary_just_under_threshold_is_safe(self, index_module):
        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD - 60)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_boundary_just_over_threshold_is_reaped(self, index_module):
        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD + 60)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-0001"]

    def test_dry_run_does_not_terminate(self, monkeypatch):
        monkeypatch.setenv("MAX_SPOT_BUDGET_SECONDS", "21600")
        monkeypatch.setenv("GRACE_SECONDS", "1800")
        monkeypatch.setenv("DRY_RUN", "true")
        fake_send_message = _FakeSendMessage()
        tel_mod = types.ModuleType("nousergon_lib.telegram")
        tel_mod.send_message = fake_send_message
        sys.modules["nousergon_lib.telegram"] = tel_mod
        if "index" in sys.modules:
            del sys.modules["index"]
        index_module = importlib.import_module("index")

        spots = [_spot("i-0001", "alpha-engine-backtest-20260511", age_seconds=THRESHOLD + 600)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
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
        s3 = MagicMock()
        clients = {"ec2": ec2, "cloudwatch": cw, "s3": s3}
        with patch.object(index_module.boto3, "client",
                          side_effect=lambda svc, **kw: clients[svc]):
            out = index_module.handler({}, None)

        assert out["orphans_detected"] == 2
        assert out["terminated"] == ["i-0002"]
        assert ec2.terminate_instances.call_count == 2


class TestCiWatchIncompleteReapAlert:
    """ci-watch-dispatcher migration: additive alert scoped to ONLY
    Name=alpha-engine-ci-watch-spot boxes — every other tag's reap path
    (covered above) must stay byte-for-byte unaffected."""

    def test_reaped_without_marker_fires_alert(self, index_module):
        spots = [_spot("i-ciwatch", "alpha-engine-ci-watch-spot", age_seconds=THRESHOLD + 600,
                       ci_watch_repo="nousergon/alpha-engine-config", ci_watch_sha="abc123def456")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert out["terminated"] == ["i-ciwatch"]
        assert out["ci_watch_incomplete_reaps"] == ["i-ciwatch"]
        s3.head_object.assert_called_once_with(
            Bucket="alpha-engine-research",
            # repo's "/" is flattened to "-" (matches ci_watch_run.sh's own
            # escaping when it WRITES the marker) — this key must reflect that,
            # not a literal nested "nousergon/alpha-engine-config-..." path.
            Key="ci_watch/_control/completed/nousergon-alpha-engine-config-abc123def456.json",
        )
        assert len(index_module._test_send_message.calls) == 1
        (text,), kwargs = index_module._test_send_message.calls[0]
        assert "reaped WITHOUT completing" in text
        assert "nousergon/alpha-engine-config" in text
        assert "abc123def456" in text
        assert kwargs["disable_notification"] is False

    def test_reaped_with_marker_present_does_not_alert(self, index_module):
        spots = [_spot("i-ciwatch", "alpha-engine-ci-watch-spot", age_seconds=THRESHOLD + 600,
                       ci_watch_repo="nousergon/alpha-engine-config", ci_watch_sha="abc123def456")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["terminated"] == ["i-ciwatch"]
        assert out["ci_watch_incomplete_reaps"] == []
        assert index_module._test_send_message.calls == []

    def test_s3_error_still_fires_alert_fail_safe_direction(self, index_module):
        # Any inability to CONFIRM completion (a real 404 OR an unrelated S3
        # error) must fire the alert — the safer failure direction (an
        # occasional false-positive beats silently missing a real incomplete
        # run). Covered here via a generic exception (throttle/auth-shaped).
        spots = [_spot("i-ciwatch", "alpha-engine-ci-watch-spot", age_seconds=THRESHOLD + 600,
                       ci_watch_repo="nousergon/alpha-engine-config", ci_watch_sha="abc123def456")]
        ec2 = MagicMock()
        ec2.get_paginator.return_value = _describe_instances_paginator(spots)
        cw = MagicMock()
        s3 = MagicMock()
        s3.head_object.side_effect = RuntimeError("S3 throttled")
        clients = {"ec2": ec2, "cloudwatch": cw, "s3": s3}
        with patch.object(index_module.boto3, "client",
                          side_effect=lambda svc, **kw: clients[svc]):
            out = index_module.handler({}, None)
        assert out["ci_watch_incomplete_reaps"] == ["i-ciwatch"]
        assert len(index_module._test_send_message.calls) == 1

    def test_missing_repo_sha_tags_treated_as_incomplete(self, index_module):
        # A box reaped before its repo/sha tags ever landed (e.g. the
        # dispatcher's tag-write failed) — cannot look up a marker, so the
        # safer direction is to alert rather than silently skip.
        spots = [_spot("i-ciwatch", "alpha-engine-ci-watch-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["ci_watch_incomplete_reaps"] == ["i-ciwatch"]
        s3.head_object.assert_not_called()  # nothing to look up without repo+sha
        assert len(index_module._test_send_message.calls) == 1

    def test_other_tags_never_trigger_s3_lookup_or_alert(self, index_module):
        spots = [_spot("i-groom", "alpha-engine-groom-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert out["ci_watch_incomplete_reaps"] == []
        s3.head_object.assert_not_called()
        assert index_module._test_send_message.calls == []


class TestSfWatchIncompleteReapAlert:
    """Finishing config#2001 (SF-watch's EC2-spot migration): additive alert
    scoped to ONLY Name=alpha-engine-sf-watch-spot boxes, built on the same
    generalized WATCH_KINDS path CI-watch uses — every other tag's reap path
    (covered above) must stay byte-for-byte unaffected, and CI-watch's own
    path must stay byte-for-byte unaffected too (see TestCiWatchIncompleteReapAlert,
    unmodified by this class's existence)."""

    def test_reaped_without_marker_fires_alert(self, index_module):
        spots = [_spot("i-sfwatch", "alpha-engine-sf-watch-spot", age_seconds=THRESHOLD + 600,
                       sf_watch_cadence="saturday", sf_watch_pipeline="ne-weekly-freshness-pipeline",
                       sf_watch_run_date="2026-07-11")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert out["terminated"] == ["i-sfwatch"]
        assert out["sf_watch_incomplete_reaps"] == ["i-sfwatch"]
        s3.head_object.assert_called_once_with(
            Bucket="alpha-engine-research",
            Key="sf_watch/_control/completed/saturday-ne-weekly-freshness-pipeline-2026-07-11.json",
        )
        assert len(index_module._test_send_message.calls) == 1
        (text,), kwargs = index_module._test_send_message.calls[0]
        assert "reaped WITHOUT completing" in text
        assert "saturday" in text
        assert "ne-weekly-freshness-pipeline" in text
        assert kwargs["disable_notification"] is False

    def test_reaped_with_marker_present_does_not_alert(self, index_module):
        spots = [_spot("i-sfwatch", "alpha-engine-sf-watch-spot", age_seconds=THRESHOLD + 600,
                       sf_watch_cadence="saturday", sf_watch_pipeline="ne-weekly-freshness-pipeline",
                       sf_watch_run_date="2026-07-11")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["terminated"] == ["i-sfwatch"]
        assert out["sf_watch_incomplete_reaps"] == []
        assert index_module._test_send_message.calls == []

    def test_missing_discriminator_tags_treated_as_incomplete(self, index_module):
        spots = [_spot("i-sfwatch", "alpha-engine-sf-watch-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["sf_watch_incomplete_reaps"] == ["i-sfwatch"]
        s3.head_object.assert_not_called()
        assert len(index_module._test_send_message.calls) == 1

    def test_ci_watch_and_sf_watch_boxes_are_independently_tracked(self, index_module):
        """Both kinds reaped in the same scan must each land only in their
        own result key — no cross-contamination between WATCH_KINDS entries."""
        spots = [
            _spot("i-ciwatch", "alpha-engine-ci-watch-spot", age_seconds=THRESHOLD + 600,
                 ci_watch_repo="nousergon/alpha-engine-config", ci_watch_sha="abc123"),
            _spot("i-sfwatch", "alpha-engine-sf-watch-spot", age_seconds=THRESHOLD + 600,
                 sf_watch_cadence="saturday", sf_watch_pipeline="ne-weekly-freshness-pipeline",
                 sf_watch_run_date="2026-07-11"),
        ]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert set(out["terminated"]) == {"i-ciwatch", "i-sfwatch"}
        assert out["ci_watch_incomplete_reaps"] == ["i-ciwatch"]
        assert out["sf_watch_incomplete_reaps"] == ["i-sfwatch"]
        assert len(index_module._test_send_message.calls) == 2


class TestAlertDrainIncompleteReapAlert:
    """config#3173: alert-drain had ZERO incomplete-reap coverage before this —
    additive alert scoped to ONLY Name=alpha-engine-alert-drain-spot boxes,
    built on the same generalized WATCH_KINDS path CI-watch/SF-watch use;
    every other tag's reap path (covered above) stays byte-for-byte
    unaffected."""

    def test_reaped_without_marker_fires_alert(self, index_module):
        spots = [_spot("i-drain", "alpha-engine-alert-drain-spot", age_seconds=THRESHOLD + 600,
                       alert_drain_run_id="drain-2026-07-22T1200Z")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert out["terminated"] == ["i-drain"]
        assert out["alert_drain_incomplete_reaps"] == ["i-drain"]
        s3.head_object.assert_called_once_with(
            Bucket="alpha-engine-research",
            Key="overseer/_control/completed/alert-drain-drain-2026-07-22T1200Z.json",
        )
        assert len(index_module._test_send_message.calls) == 1
        (text,), kwargs = index_module._test_send_message.calls[0]
        assert "reaped WITHOUT completing" in text
        assert "drain-2026-07-22T1200Z" in text
        assert kwargs["disable_notification"] is False

    def test_reaped_with_marker_present_does_not_alert(self, index_module):
        spots = [_spot("i-drain", "alpha-engine-alert-drain-spot", age_seconds=THRESHOLD + 600,
                       alert_drain_run_id="drain-2026-07-22T1200Z")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["terminated"] == ["i-drain"]
        assert out["alert_drain_incomplete_reaps"] == []
        assert index_module._test_send_message.calls == []

    def test_missing_run_id_tag_treated_as_incomplete(self, index_module):
        spots = [_spot("i-drain", "alpha-engine-alert-drain-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["alert_drain_incomplete_reaps"] == ["i-drain"]
        s3.head_object.assert_not_called()
        assert len(index_module._test_send_message.calls) == 1

    def test_all_three_watch_kinds_independently_tracked(self, index_module):
        spots = [
            _spot("i-ciwatch", "alpha-engine-ci-watch-spot", age_seconds=THRESHOLD + 600,
                 ci_watch_repo="nousergon/alpha-engine-config", ci_watch_sha="abc123"),
            _spot("i-sfwatch", "alpha-engine-sf-watch-spot", age_seconds=THRESHOLD + 600,
                 sf_watch_cadence="saturday", sf_watch_pipeline="ne-weekly-freshness-pipeline",
                 sf_watch_run_date="2026-07-11"),
            _spot("i-drain", "alpha-engine-alert-drain-spot", age_seconds=THRESHOLD + 600,
                 alert_drain_run_id="drain-2026-07-22T1200Z"),
        ]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert set(out["terminated"]) == {"i-ciwatch", "i-sfwatch", "i-drain"}
        assert out["ci_watch_incomplete_reaps"] == ["i-ciwatch"]
        assert out["sf_watch_incomplete_reaps"] == ["i-sfwatch"]
        assert out["alert_drain_incomplete_reaps"] == ["i-drain"]
        assert len(index_module._test_send_message.calls) == 3
