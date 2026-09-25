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
         sf_watch_run_date: str | None = None, alert_drain_run_id: str | None = None,
         thinktank_trading_day: str | None = None, thinktank_run_token: str | None = None,
         watchdog_deadline: str | None = None):
    """Build a mock describe-instances entry."""
    tags = [{"Key": "Name", "Value": name}]
    if watchdog_deadline is not None:
        tags.append({"Key": "watchdog-deadline", "Value": watchdog_deadline})
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
    if thinktank_trading_day is not None:
        tags.append({"Key": "thinktank-trading-day", "Value": thinktank_trading_day})
    if thinktank_run_token is not None:
        tags.append({"Key": "thinktank-run-token", "Value": thinktank_run_token})
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


def _metric_calls(cw, metric_name: str) -> list:
    """put_metric_data calls carrying ``metric_name``. The handler also emits
    per-market scan metrics on every run (alpha-engine-config-I11108), so a
    test about the per-name reap series must select it rather than count all
    put_metric_data calls."""
    return [
        c for c in cw.put_metric_data.call_args_list
        if any(d["MetricName"] == metric_name for d in c.kwargs["MetricData"])
    ]


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


class TestWatchdogDeadlineTag:
    """config#5695: per-box watchdog-deadline tag takes precedence over the
    global cap when present and parseable. Legacy boxes without the tag are
    still reaped at the global cap (unchanged behavior)."""

    def _deadline_str(self, age_seconds: int) -> str:
        """Build an ISO8601 UTC deadline string that fired ``age_seconds`` ago
        (deadline was set ``age_seconds`` in the past from now)."""
        return (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def _future_deadline_str(self, offset_seconds: int) -> str:
        """Build an ISO8601 UTC deadline string in the future."""
        return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def test_watchdog_deadline_tag_lifts_threshold_above_global_cap(self, index_module):
        """A box with a past deadline but still within the delta from the
        deadline to the global cap would have been reaped by the global cap.
        With a deadline tag that has NOT yet been reached, the box survives."""
        # Deadline 12h from now; the global cap (6.5h) would have reaped the
        # box at age 7h, but the deadline tag protects it until 12h + grace.
        deadline = self._future_deadline_str(12 * 3600)
        spots = [_spot("i-weekly", "alpha-engine-weekly-freshness-spot",
                       age_seconds=7 * 3600, watchdog_deadline=deadline)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_watchdog_deadline_expired_triggers_reap(self, index_module):
        """A box whose watchdog-deadline + grace has passed IS reaped."""
        # Deadline was set to 6h after launch, which is 2h ago (age=8h),
        # and grace (0.5h) has also passed.
        deadline = self._deadline_str(2 * 3600)  # deadline 2h in the past
        spots = [_spot("i-weekly", "alpha-engine-weekly-freshness-spot",
                       age_seconds=8 * 3600, watchdog_deadline=deadline)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-weekly"]

    def test_watchdog_deadline_barely_within_grace_not_reaped(self, index_module):
        """A box within GRACE_SECONDS of its watchdog deadline is NOT reaped."""
        # deadline 25min ago (1500s). Launch 7000s ago → deadline set at
        # 5500s after launch. effective_threshold = 5500 + 1800 (grace) = 7300s.
        # age = 7000s ≤ 7300s → safe.
        spots = [_spot("i-weekly", "alpha-engine-weekly-freshness-spot",
                       age_seconds=7000, watchdog_deadline=self._deadline_str(1500))]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_watchdog_deadline_just_beyond_grace_is_reaped(self, index_module):
        deadline = self._deadline_str(2000)  # deadline 33 min ago (>1800 grace)
        spots = [_spot("i-weekly", "alpha-engine-weekly-freshness-spot",
                       age_seconds=7500, watchdog_deadline=deadline)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-weekly"]

    def test_box_without_tag_still_at_global_cap(self, index_module):
        """A box WITHOUT a watchdog-deadline tag uses the global cap unchanged."""
        spots = [_spot("i-groom", "alpha-engine-groom-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-groom"]

    def test_box_without_tag_below_global_cap_is_safe(self, index_module):
        spots = [_spot("i-groom", "alpha-engine-groom-spot", age_seconds=THRESHOLD - 60)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_malformed_deadline_falls_back_to_global_cap(self, index_module):
        """A malformed (non-ISO8601) watchdog-deadline tag drops to global cap."""
        spots = [_spot("i-weekly", "alpha-engine-weekly-freshness-spot",
                       age_seconds=THRESHOLD + 600,
                       watchdog_deadline="not-a-valid-date")]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == ["i-weekly"]

    def test_orphan_detail_includes_effective_reap_threshold_and_deadline(self, index_module):
        deadline = self._future_deadline_str(12 * 3600)
        spots = [_spot("i-weekly", "alpha-engine-weekly-freshness-spot",
                       age_seconds=THRESHOLD + 600, watchdog_deadline=deadline)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["orphans_detected"] == 0

    def test_return_summary_includes_global_fallback_when_no_tag(self, index_module):
        spots = [_spot("i-groom", "alpha-engine-groom-spot", age_seconds=THRESHOLD + 600)]
        out, ec2, _cw, _s3 = _run(index_module, spots)
        assert out["reap_after_seconds"] == THRESHOLD
        # Each orphan detail shows the global cap as its reap_after
        assert out["orphan_detail"][0]["reap_after_seconds"] == THRESHOLD


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
        assert len(_metric_calls(cw, "spot_orphans_terminated")) == 1
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
        assert _metric_calls(cw, "spot_orphans_terminated") == []

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


class TestThinkTankIncompleteReapAlert:
    """alpha-engine-config-I5752 — Think Tank was the fourth box class and the
    only one still missing a WATCH_KINDS row, so a box that overran its 2.5h
    watchdog and reached the 6.5h age cap was terminated with nobody told.

    This row covers the HANG end. The fast-fail end is covered on the box
    (crucible-research#558: `on_exit` publishes to alpha-engine-alerts on a
    non-zero rc, the window flow-doctor cannot see). Neither substitutes for
    the other, which is why both exist.
    """

    def test_reaped_without_marker_fires_alert(self, index_module):
        spots = [_spot("i-tt", "alpha-engine-thinktank-spot", age_seconds=THRESHOLD + 600,
                       thinktank_trading_day="2026-07-30", thinktank_run_token="tok123")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert out["terminated"] == ["i-tt"]
        assert out["thinktank_incomplete_reaps"] == ["i-tt"]
        s3.head_object.assert_called_once_with(
            Bucket="alpha-engine-research",
            Key="thinktank/_control/completed/2026-07-30-tok123.json",
        )
        assert len(index_module._test_send_message.calls) == 1
        (text,), kwargs = index_module._test_send_message.calls[0]
        assert "reaped WITHOUT completing" in text
        assert "tok123" in text
        assert kwargs["disable_notification"] is False

    def test_reaped_with_marker_present_does_not_alert(self, index_module):
        spots = [_spot("i-tt", "alpha-engine-thinktank-spot", age_seconds=THRESHOLD + 600,
                       thinktank_trading_day="2026-07-30", thinktank_run_token="tok123")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["terminated"] == ["i-tt"]
        assert out["thinktank_incomplete_reaps"] == []
        assert index_module._test_send_message.calls == []

    def test_missing_discriminator_tags_treated_as_incomplete(self, index_module):
        """A box reaped with either tag absent cannot be looked up either way;
        since config#2292 tagging is atomic with RunInstances, so this is a
        genuine anomaly worth the alert rather than the old launch->tag race."""
        spots = [_spot("i-tt", "alpha-engine-thinktank-spot", age_seconds=THRESHOLD + 600,
                       thinktank_trading_day="2026-07-30")]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=True)
        assert out["thinktank_incomplete_reaps"] == ["i-tt"]
        s3.head_object.assert_not_called()
        assert len(index_module._test_send_message.calls) == 1

    def test_the_key_matches_what_the_box_actually_writes(self, index_module):
        """The contract between two repos, asserted rather than assumed.

        `thinktank_spot_bootstrap.sh` writes
        thinktank/_control/completed/${TRADING_DAY}-${RUN_TOKEN}.json. If the
        tuple order in WATCH_KINDS or the prefix ever drifts from that, the
        reaper looks up a key nothing writes and EVERY reap alerts — noisy
        rather than silent, but wrong either way.
        """
        kind = next(
            wk for wk in index_module.WATCH_KINDS
            if wk.tag_name == "alpha-engine-thinktank-spot"
        )
        key = index_module._completion_key(
            kind,
            {"thinktank-trading-day": "2026-07-30", "thinktank-run-token": "deadbeef"},
        )
        assert key == "thinktank/_control/completed/2026-07-30-deadbeef.json"

    def test_all_four_watch_kinds_independently_tracked(self, index_module):
        spots = [
            _spot("i-ciwatch", "alpha-engine-ci-watch-spot", age_seconds=THRESHOLD + 600,
                 ci_watch_repo="nousergon/alpha-engine-config", ci_watch_sha="abc123"),
            _spot("i-sfwatch", "alpha-engine-sf-watch-spot", age_seconds=THRESHOLD + 600,
                 sf_watch_cadence="saturday", sf_watch_pipeline="ne-weekly-freshness-pipeline",
                 sf_watch_run_date="2026-07-11"),
            _spot("i-drain", "alpha-engine-alert-drain-spot", age_seconds=THRESHOLD + 600,
                 alert_drain_run_id="drain-2026-07-22T1200Z"),
            _spot("i-tt", "alpha-engine-thinktank-spot", age_seconds=THRESHOLD + 600,
                 thinktank_trading_day="2026-07-30", thinktank_run_token="tok123"),
        ]
        out, ec2, _cw, s3 = _run(index_module, spots, s3_marker_exists=False)
        assert set(out["terminated"]) == {"i-ciwatch", "i-sfwatch", "i-drain", "i-tt"}
        assert out["ci_watch_incomplete_reaps"] == ["i-ciwatch"]
        assert out["sf_watch_incomplete_reaps"] == ["i-sfwatch"]
        assert out["alert_drain_incomplete_reaps"] == ["i-drain"]
        assert out["thinktank_incomplete_reaps"] == ["i-tt"]
        assert len(index_module._test_send_message.calls) == 4


# ── alpha-engine-config-I11108 / I7185 / I11575: on-demand run boxes ─────────


def _box(instance_id: str, name: str, age_seconds: int, *, lifecycle: str | None,
         launch_market: str | None, watchdog_deadline: str | None = None) -> dict:
    """A describe-instances entry carrying the fields the two scans filter on."""
    inst = _spot(instance_id, name, age_seconds, watchdog_deadline=watchdog_deadline)
    if lifecycle is not None:
        inst["InstanceLifecycle"] = lifecycle
    if launch_market is not None:
        inst["Tags"].append({"Key": "LaunchMarket", "Value": launch_market})
    return inst


def _matches(inst: dict, flt: dict) -> bool:
    tags = {t["Key"]: t["Value"] for t in inst["Tags"]}
    name, values = flt["Name"], flt["Values"]
    if name == "instance-state-name":
        return True  # every fixture box is running
    if name == "instance-lifecycle":
        return inst.get("InstanceLifecycle") in values
    if name.startswith("tag:"):
        value = tags.get(name[len("tag:"):])
        if value is None:
            return False
        return any(value.startswith(v[:-1]) if v.endswith("*") else value == v for v in values)
    raise AssertionError(f"unexpected filter {flt}")


def _run_filtered(index_module, fleet: list[dict], sfn=None):
    """Like _run, but the fake EC2 applies the Filters it is given, the way
    EC2 does (all filters AND together), so the scan's scope is under test."""
    paginator = MagicMock()
    paginator.paginate.side_effect = lambda Filters: [{
        "Reservations": [{"Instances": [
            i for i in fleet if all(_matches(i, f) for f in Filters)
        ]}],
    }]
    ec2 = MagicMock()
    ec2.get_paginator.return_value = paginator
    cw = MagicMock()
    s3 = MagicMock()
    s3.head_object.side_effect = _NotFound("404 Not Found")
    clients = {"ec2": ec2, "cloudwatch": cw, "s3": s3}
    if sfn is not None:
        clients["stepfunctions"] = sfn
    with patch.object(index_module.boto3, "client",
                      side_effect=lambda svc, **kw: clients[svc]):
        out = index_module.handler({}, None)
    return out, ec2, cw


class TestOnDemandRunBoxes:
    def test_on_demand_launcher_box_past_its_deadline_is_reaped(self, index_module):
        # The 2026-09-19 weekly box: on-demand fallback, never seen by the
        # spot-only scan, leaked ~8h. Fails against the pre-fix filter.
        past = (datetime.now(timezone.utc) - timedelta(seconds=7200)).isoformat()
        fleet = [_box("i-weekly", "alpha-engine-weekly-freshness-spot", 50000,
                      lifecycle=None, launch_market="on-demand", watchdog_deadline=past)]
        out, ec2, _cw = _run_filtered(index_module, fleet)
        assert out["terminated"] == ["i-weekly"]
        assert out["orphan_detail"][0]["market"] == "on-demand"
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-weekly"])

    def test_on_demand_launcher_box_within_its_deadline_is_kept(self, index_module):
        future = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
        fleet = [_box("i-weekly", "alpha-engine-weekly-freshness-spot", 30000,
                      lifecycle=None, launch_market="on-demand", watchdog_deadline=future)]
        out, ec2, _cw = _run_filtered(index_module, fleet)
        assert out["scanned"] == 1
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()

    def test_long_lived_boxes_without_the_launcher_tag_are_never_scanned(self, index_module):
        # alpha-engine-dashboard has run on-demand since 2026-07-27 with no
        # watchdog-deadline tag. A Name-only scan would reap it at the 6.5h
        # fallback cap; the launcher's LaunchMarket tag is what keeps it out.
        fleet = [
            _box("i-dash", "alpha-engine-dashboard", 60 * 86400, lifecycle=None, launch_market=None),
            _box("i-exec", "alpha-engine-executor", THRESHOLD * 3, lifecycle=None, launch_market=None),
        ]
        out, ec2, _cw = _run_filtered(index_module, fleet)
        assert out["scanned"] == 0
        ec2.terminate_instances.assert_not_called()

    def test_spot_box_is_still_reaped_and_counted_once(self, index_module):
        # launch_with_fallback also tags a spot win LaunchMarket=spot; a box
        # matching both scans must be considered once.
        fleet = [_box("i-groom", "alpha-engine-groom-spot", THRESHOLD + 600,
                      lifecycle="spot", launch_market="spot")]
        out, ec2, _cw = _run_filtered(index_module, fleet)
        assert out["scanned"] == 1
        assert out["terminated"] == ["i-groom"]
        assert out["terminated_by_market"] == {"spot": 1}
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-groom"])

    def test_other_name_prefixes_stay_out_of_scope(self, index_module):
        fleet = [_box("i-v2", "crucible-v2-experiment.backfill", THRESHOLD * 2,
                      lifecycle=None, launch_market="on-demand")]
        out, ec2, _cw = _run_filtered(index_module, fleet)
        assert out["scanned"] == 0
        ec2.terminate_instances.assert_not_called()


class TestScanMetrics:
    def _points(self, cw) -> dict[tuple[str, str], float]:
        calls = _metric_calls(cw, "orphan_reaper_candidates")
        assert len(calls) == 1
        return {
            (d["MetricName"], d["Dimensions"][0]["Value"]): d["Value"]
            for d in calls[0].kwargs["MetricData"]
        }

    def test_an_empty_scan_still_emits_zero_candidates(self, index_module):
        # I11108 deliverable 2: "found nothing to look at" must be a data
        # point, not an absence that reads the same as a healthy fleet.
        _out, _ec2, cw = _run_filtered(index_module, [])
        assert self._points(cw) == {
            ("orphan_reaper_candidates", "spot"): 0.0,
            ("orphan_reaper_terminated", "spot"): 0.0,
            ("orphan_reaper_candidates", "on-demand"): 0.0,
            ("orphan_reaper_terminated", "on-demand"): 0.0,
        }

    def test_candidates_and_reaps_are_split_by_market(self, index_module):
        past = (datetime.now(timezone.utc) - timedelta(seconds=7200)).isoformat()
        fleet = [
            _box("i-spot", "alpha-engine-backtest-20260925", 600, lifecycle="spot", launch_market="spot"),
            _box("i-od", "alpha-engine-weekly-freshness-spot", 50000,
                 lifecycle=None, launch_market="on-demand", watchdog_deadline=past),
        ]
        out, _ec2, cw = _run_filtered(index_module, fleet)
        assert out["scanned_by_market"] == {"spot": 1, "on-demand": 1}
        points = self._points(cw)
        assert points[("orphan_reaper_candidates", "spot")] == 1.0
        assert points[("orphan_reaper_terminated", "spot")] == 0.0
        assert points[("orphan_reaper_candidates", "on-demand")] == 1.0
        assert points[("orphan_reaper_terminated", "on-demand")] == 1.0


# ── alpha-engine-config-I11569: a finished rehearsal's box ends early ────────

_EXEC_PREFIX = "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:"


def _weekly_box(instance_id: str, execution_name: str, age_seconds: int = 7200) -> dict:
    """An on-demand weekly launcher box, still inside its 13h watchdog-deadline."""
    deadline = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat()
    inst = _box(instance_id, "alpha-engine-weekly-freshness-spot", age_seconds,
                lifecycle=None, launch_market="on-demand", watchdog_deadline=deadline)
    inst["Tags"].append({"Key": "execution-id", "Value": _EXEC_PREFIX + execution_name})
    return inst


def _sfn(status: str, stopped_seconds_ago: int | None = None, raises: Exception | None = None):
    sfn = MagicMock()
    if raises is not None:
        sfn.describe_execution.side_effect = raises
    else:
        desc = {"status": status}
        if stopped_seconds_ago is not None:
            desc["stopDate"] = datetime.now(timezone.utc) - timedelta(seconds=stopped_seconds_ago)
        sfn.describe_execution.return_value = desc
    return sfn


class TestRehearsalBoxes:
    def test_failed_rehearsal_box_is_reaped_after_the_grace(self, index_module):
        # rehearsal-2026-09-24-1: FailExecution at 22:51Z, box kept to 11:00Z.
        sfn = _sfn("FAILED", stopped_seconds_ago=3700)
        out, ec2, _cw = _run_filtered(index_module, [_weekly_box("i-reh", "rehearsal-2026-09-24-1")], sfn)
        assert out["terminated"] == ["i-reh"]
        assert out["orphan_detail"][0]["reap_reason"] == "rehearsal-finished"
        sfn.describe_execution.assert_called_once_with(
            executionArn=_EXEC_PREFIX + "rehearsal-2026-09-24-1")
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-reh"])

    def test_rehearsal_inside_the_grace_is_kept(self, index_module):
        sfn = _sfn("FAILED", stopped_seconds_ago=600)
        out, ec2, _cw = _run_filtered(index_module, [_weekly_box("i-reh", "rehearsal-2026-09-24-2")], sfn)
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()

    def test_running_rehearsal_is_kept(self, index_module):
        sfn = _sfn("RUNNING")
        out, ec2, _cw = _run_filtered(index_module, [_weekly_box("i-reh", "rehearsal-2026-09-25-1")], sfn)
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()

    def test_describe_failure_keeps_the_box(self, index_module):
        # Before the role carries states:DescribeExecution, every call is
        # AccessDenied; the box must fall back to its own deadline.
        sfn = _sfn("", raises=RuntimeError("AccessDeniedException"))
        out, ec2, _cw = _run_filtered(index_module, [_weekly_box("i-reh", "rehearsal-2026-09-24-1")], sfn)
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()

    def test_failed_production_run_keeps_its_box_for_the_watch_rerun(self, index_module):
        # A non-rehearsal execution's box is reused by weekly_sf_rerun via
        # $.ec2_instance_id, so it is never looked up, let alone reaped early.
        sfn = _sfn("FAILED", stopped_seconds_ago=7200)
        out, ec2, _cw = _run_filtered(
            index_module, [_weekly_box("i-prod", "2b6ab316-8060-4011-8140-cccf0f2194bd")], sfn)
        assert out["terminated"] == []
        sfn.describe_execution.assert_not_called()
        ec2.terminate_instances.assert_not_called()

    def test_dry_run_reports_but_does_not_terminate(self, monkeypatch, index_module):
        monkeypatch.setattr(index_module, "DRY_RUN", True)
        sfn = _sfn("FAILED", stopped_seconds_ago=3700)
        out, ec2, _cw = _run_filtered(index_module, [_weekly_box("i-reh", "rehearsal-2026-09-24-1")], sfn)
        assert out["orphans_detected"] == 1
        assert out["terminated"] == []
        ec2.terminate_instances.assert_not_called()

    def test_iam_grants_describe_on_rehearsal_executions_only(self):
        import json
        policy = json.loads((SCRIPT_DIR / "iam-policy.json").read_text())
        grants = [s for s in policy["Statement"] if "states:DescribeExecution" in str(s["Action"])]
        assert len(grants) == 1
        assert grants[0]["Resource"].endswith(":execution:ne-weekly-freshness-pipeline:rehearsal-*")
