"""Unit tests for the canary-replay dispatcher (alpha-engine-config#2246).

Hermetic: ``nousergon_lib.spot_dispatch`` and ``boto3`` are stubbed in
sys.modules BEFORE importing index — this repo's own
``nousergon-lib/tests/test_spot_dispatch.py`` already covers
``spot_dispatch``'s internal retry/fallback mechanics; these tests validate
ONLY this Lambda's orchestration: event validation + deterministic
run_token derivation, the concurrency dedupe guard, spot-launch failure
handling, discriminator-tag retry-then-terminate, post-launch-failure
terminate, and the kill-switch short-circuit.
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
    create_tags_failures=0,
    wait_ssm_online_raises=None,
):
    spot_dispatch_mod = types.ModuleType("nousergon_lib.spot_dispatch")
    spot_dispatch_mod.SpotLaunchError = _SpotLaunchError
    spot_dispatch_mod.SpotProbeError = _SpotProbeError

    spot_dispatch_mod.launch_with_fallback = launch_with_fallback_impl or (
        lambda *a, **kw: ("i-abc123", "spot")
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

    tags_created: list[tuple] = []
    create_tags_attempts = {"n": 0}

    class _FakeEc2:
        def create_tags(self, Resources, Tags):  # noqa: N803 — boto3 kwarg names
            create_tags_attempts["n"] += 1
            if create_tags_attempts["n"] <= create_tags_failures:
                raise RuntimeError(f"CreateTags throttled (attempt {create_tags_attempts['n']})")
            tags_created.append((Resources, Tags))
            return {}

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: _FakeEc2()
    sys.modules["boto3"] = boto3_mod

    return terminated, tags_created


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

    def test_tag_write_retries_then_terminates_on_final_failure(self):
        terminated, tags_created = _install_stubs(create_tags_failures=99)
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is False
        assert result["reason"] == "tag_write_failed"
        assert terminated == ["i-abc123"]

    def test_tag_write_succeeds_after_transient_failures(self):
        terminated, tags_created = _install_stubs(create_tags_failures=2)
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is True
        assert terminated == []
        assert len(tags_created) == 1

    def test_ssm_online_failure_terminates_box_and_returns_clean_failure(self):
        terminated, _ = _install_stubs(wait_ssm_online_raises=TimeoutError("ssm never came online"))
        index = _reload_index()
        result = index.handler({"mode": "scheduled"}, None)
        assert result["launched"] is False
        assert result["reason"] == "post_launch_failed"
        assert terminated == ["i-abc123"]


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
