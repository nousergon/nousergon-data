"""Unit tests for the canary-replay dispatcher (alpha-engine-config#2246).

Hermetic: ``nousergon_lib.spot_dispatch`` and ``boto3`` are stubbed in
sys.modules BEFORE importing index — this repo's own
``nousergon-lib/tests/test_spot_dispatch.py`` already covers
``spot_dispatch``'s internal retry/fallback mechanics; these tests validate
ONLY this Lambda's orchestration: event validation + deterministic
run_token derivation, the concurrency dedupe guard, spot-launch failure
handling, launch-time extra_tags verification (config#2836 — replaced the
separate discriminator-tag retry-then-terminate path with tag-on-create),
post-launch-failure terminate, and the kill-switch short-circuit.
"""

from __future__ import annotations

import importlib
import os
import sys
import types

import pytest


class _SpotLaunchError(Exception):
    pass


class _SpotProbeError(Exception):
    pass


def _install_stubs(
    launch_with_fallback_impl=None,
    running_instance_ids_impl=None,
    send_async_command_impl=None,
    wait_ssm_online_raises=None,
    delete_object_raises=None,
):
    captured_extra_tags: dict[str, str] = {}

    spot_dispatch_mod = types.ModuleType("nousergon_lib.spot_dispatch")
    spot_dispatch_mod.SpotLaunchError = _SpotLaunchError
    spot_dispatch_mod.SpotProbeError = _SpotProbeError

    spot_dispatch_mod.launch_with_fallback = launch_with_fallback_impl or (
        lambda *a, **kw: captured_extra_tags.update(kw.get("extra_tags") or {}) or ("i-abc123", "spot")
    )
    spot_dispatch_mod.running_instance_ids = running_instance_ids_impl or (lambda *a, **kw: [])

    def _wait_ssm_online(instance_id, **kw):
        if wait_ssm_online_raises:
            raise wait_ssm_online_raises

    spot_dispatch_mod.wait_ssm_online = _wait_ssm_online
    spot_dispatch_mod.send_async_command = send_async_command_impl or (
        lambda *a, **kw: "cmd-xyz"
    )

    terminated: list[str] = []

    def _terminate_on_failure(instance_id, **kw):
        terminated.append(instance_id)

    spot_dispatch_mod.terminate_on_failure = _terminate_on_failure

    nousergon_lib_mod = types.ModuleType("nousergon_lib")
    nousergon_lib_mod.spot_dispatch = spot_dispatch_mod
    sys.modules["nousergon_lib"] = nousergon_lib_mod
    sys.modules["nousergon_lib.spot_dispatch"] = spot_dispatch_mod

    deleted_keys: list[tuple] = []

    class _FakeS3:
        def delete_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
            if delete_object_raises:
                raise delete_object_raises
            deleted_keys.append((Bucket, Key))
            return {}

    boto3_mod = types.ModuleType("boto3")

    def _fake_client(name, **kw):
        return _FakeS3()

    boto3_mod.client = _fake_client
    sys.modules["boto3"] = boto3_mod

    return terminated, deleted_keys, captured_extra_tags


def _reload_index():
    sys.path.insert(0, os.path.dirname(__file__))
    if "index" in sys.modules:
        del sys.modules["index"]
    import index  # noqa: PLC0415

    return importlib.reload(index)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    import time

    monkeypatch.setattr(time, "sleep", lambda *a, **kw: None)


class TestEventValidation:
    def test_scheduled_mode_defaults_refs_to_main(self):
        _install_stubs()
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is True
        assert result["research_ref"] == "main"
        assert result["data_ref"] == "main"
        assert result["run_token"].startswith("sched-")

    def test_scheduled_run_token_is_deterministic_per_iso_week(self):
        from datetime import datetime, timezone

        _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)  # a Thursday
        assert index._scheduled_run_token(now) == "sched-2026w29"

    def test_pr_mode_requires_repo_pr_number_sha(self):
        _install_stubs()
        index = _reload_index()
        result = index.handler({"mode": "pr"}, None)
        assert result["launched"] is False
        assert result["reason"] == "invalid_event"

    def test_pr_mode_derives_deterministic_run_token(self):
        _install_stubs()
        index = _reload_index()
        result = index.handler(
            {
                "mode": "pr",
                "repo": "nousergon/crucible-research",
                "pr_number": "42",
                "sha": "abc1234567890abc1234567890abc1234567890",
                "research_ref": "feat/some-branch",
            },
            None,
        )
        assert result["launched"] is True
        assert result["run_token"] == "pr-nousergon-crucible-research-42-abc123456789"
        assert result["research_ref"] == "feat/some-branch"
        assert result["data_ref"] == "main"

    def test_invalid_mode_returns_clean_launched_false(self):
        _install_stubs()
        index = _reload_index()
        result = index.handler({"mode": "bogus"}, None)
        assert result["launched"] is False
        assert result["reason"] == "invalid_event"

    def test_malformed_ref_rejected(self):
        _install_stubs()
        index = _reload_index()
        result = index.handler({"mode": "scheduled", "research_ref": "; rm -rf /"}, None)
        assert result["launched"] is False
        assert result["reason"] == "invalid_event"


class TestConcurrencyDedupe:
    def test_existing_box_for_same_token_skips_launch(self):
        _install_stubs(running_instance_ids_impl=lambda *a, **kw: ["i-existing"])
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is False
        assert result["reason"] == "concurrent_skip"
        assert result["existing_instance_ids"] == ["i-existing"]

    def test_probe_error_degrades_but_still_launches(self):
        def _raise(*a, **kw):
            raise _SpotProbeError("probe boom")

        _install_stubs(running_instance_ids_impl=_raise)
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is True
        assert result["dedupe_degraded"] is True
        assert "probe boom" in result["dedupe_probe_error"]


class TestLaunchFailures:
    def test_spot_launch_failure_returns_clean_launched_false(self):
        def _raise(*a, **kw):
            raise _SpotLaunchError("no capacity anywhere")

        _install_stubs(launch_with_fallback_impl=_raise)
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is False
        assert result["reason"] == "launch_failed"

    def test_ssm_online_failure_terminates_box_and_returns_clean_failure(self):
        terminated, _, _ = _install_stubs(wait_ssm_online_raises=TimeoutError("ssm never came online"))
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is False
        assert result["reason"] == "post_launch_failed"
        assert terminated == ["i-abc123"]


class TestExtraTags:
    """run_token discriminator tags are now applied at launch time via
    ``extra_tags`` on ``launch_with_fallback``, closing the TOCTOU race
    between a separate ``create_tags`` call and the concurrency dedupe
    probe (the tag is atomically present from the moment the instance
    exists — no untagged window)."""

    def test_extra_tags_contains_the_run_token(self):
        _, _, captured = _install_stubs()
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert captured.get("canary-replay-run-token") == result["run_token"]

    def test_extra_tags_pr_mode(self):
        _, _, captured = _install_stubs()
        index = _reload_index()
        result = index.handler(
            {
                "mode": "pr",
                "repo": "nousergon/crucible-research",
                "pr_number": "42",
                "sha": "abc1234567890abc1234567890abc1234567890",
            },
            None,
        )
        assert result["launched"] is True
        assert captured.get("canary-replay-run-token") == result["run_token"]


class TestKillSwitch:
    def test_dispatch_disabled_env_short_circuits(self, monkeypatch):
        _install_stubs()
        monkeypatch.setenv("CANARY_REPLAY_DISPATCH_ENABLED", "false")
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is False
        assert result["reason"] == "disabled"


class TestMarkerKey:
    def test_launched_verdict_carries_the_marker_key_a_poller_would_read(self):
        _install_stubs()
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["marker_key"] == f"tmp/canary/{result['run_token']}.json"

    def test_launched_verdict_carries_a_dispatched_at_timestamp(self):
        import time

        _install_stubs()
        index = _reload_index()
        before = time.time()
        result = index.handler({"mode": "scheduled"}, None)
        after = time.time()
        assert before <= result["dispatched_at"] <= after


class TestStaleMarkerGuard:
    """config-I2753: run_token is deterministic per (repo, PR, sha) / ISO
    week, so a re-dispatch against an unchanged commit/week can find a
    STALE marker left over from a prior attempt at the exact same S3 key.
    The dispatcher must delete that key before launching, so a poller can
    never observe a leftover verdict from a previous dispatch."""

    def test_launch_clears_the_marker_key_before_launching(self):
        _, deleted_keys, _ = _install_stubs()
        index = _reload_index()
        result = index.handler(
            {
                "mode": "pr",
                "repo": "nousergon/crucible-research",
                "pr_number": "444",
                "sha": "a9af99d41c4863970aa0d05588b5ce20afe05d10",
            },
            None,
        )
        assert result["launched"] is True
        assert deleted_keys == [
            ("alpha-engine-research", "tmp/canary/pr-nousergon-crucible-research-444-a9af99d41c48.json")
        ]

    def test_marker_clear_failure_aborts_dispatch_without_launching(self):
        launched: list[str] = []

        def _tracking_launch(*a, **kw):
            launched.append("called")
            return ("i-abc123", "spot")

        _, _, _ = _install_stubs(
            launch_with_fallback_impl=_tracking_launch,
            delete_object_raises=RuntimeError("S3 DeleteObject throttled"),
        )
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is False
        assert result["reason"] == "marker_clear_failed"
        assert "S3 DeleteObject throttled" in result["error"]
        # The whole point of clearing FIRST is that a clear failure must
        # never let a box get launched whose eventual marker a poller
        # can't trust to be freshly reported — assert we never even tried.
        assert launched == []

    def test_concurrent_skip_never_attempts_to_clear_the_marker(self):
        # A live box is already working this exact token — its own,
        # eventual marker write is the one we want; clearing here would
        # race the live box's own in-flight completion write.
        _, deleted_keys, _ = _install_stubs(
            running_instance_ids_impl=lambda *a, **kw: ["i-existing"]
        )
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["reason"] == "concurrent_skip"
        assert deleted_keys == []
