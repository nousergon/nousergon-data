"""Unit tests for alpha-engine-weekly-freshness-spot-dispatcher (config#2248).

Hermetic: ``nousergon_lib.spot_dispatch`` is stubbed in sys.modules BEFORE
``import index`` — same shape as alert-drain-dispatcher/test_handler.py.
Validates: the happy-path launch+bootstrap dispatch, the kill-switch RAISES
(no silent skip — this Lambda has no fail-open branch, unlike data-spot-
dispatcher), a launch failure propagates (fail-loud — the SF's own Catch
converts it, not this Lambda), and a post-launch SSM failure terminates the
box before re-raising. Also pins the bootstrap command string covers all
four repo clones + the dashboard venv build + the long-lived watchdog.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent


class SpotLaunchError(Exception):
    pass


class SpotProbeError(Exception):
    pass


@pytest.fixture
def index_mod(monkeypatch):
    sd = types.ModuleType("nousergon_lib.spot_dispatch")
    sd.SpotLaunchError = SpotLaunchError
    sd.SpotCapacityExhausted = type("SpotCapacityExhausted", (SpotLaunchError,), {})
    sd.SpotQuotaExceededError = type("SpotQuotaExceededError", (SpotLaunchError,), {})
    sd.SpotProbeError = SpotProbeError
    sd.launch_with_fallback = MagicMock(return_value=("i-weeklylauncher", "spot"))
    sd.wait_ssm_online = MagicMock()
    sd.send_async_command = MagicMock(return_value="cmd-weekly-1")
    sd.terminate_on_failure = MagicMock()
    lib = types.ModuleType("nousergon_lib")
    lib.spot_dispatch = sd
    monkeypatch.setitem(sys.modules, "nousergon_lib", lib)
    monkeypatch.setitem(sys.modules, "nousergon_lib.spot_dispatch", sd)
    monkeypatch.syspath_prepend(str(SCRIPT_DIR))
    sys.modules.pop("index", None)
    index = importlib.import_module("index")
    return index, sd


class TestHappyPath:
    def test_dispatches_and_returns_instance_id(self, index_mod):
        index, sd = index_mod
        out = index.handler({}, None)
        assert out["instance_id"] == "i-weeklylauncher"
        assert out["market"] == "spot"
        assert out["command_id"] == "cmd-weekly-1"
        assert "run_token" in out and out["run_token"]
        sd.launch_with_fallback.assert_called_once()
        sd.wait_ssm_online.assert_called_once_with(
            "i-weeklylauncher", region=index.REGION,
            ssm_online_budget_sec=index.SSM_ONLINE_BUDGET_SEC,
        )
        sd.send_async_command.assert_called_once()

    def test_launch_uses_executor_profile_and_weekly_tag(self, index_mod):
        index, sd = index_mod
        index.handler({}, None)
        kwargs = sd.launch_with_fallback.call_args.kwargs
        assert kwargs["iam_instance_profile"] == "alpha-engine-executor-profile"
        assert kwargs["tag_name"] == "alpha-engine-weekly-freshness-spot"

    def test_force_on_demand_passthrough(self, index_mod):
        index, sd = index_mod
        index.handler({"force_on_demand": True}, None)
        kwargs = sd.launch_with_fallback.call_args.kwargs
        assert kwargs["force_on_demand"] is True

    def test_run_token_is_unique_per_invocation(self, index_mod):
        index, sd = index_mod
        out1 = index.handler({}, None)
        out2 = index.handler({}, None)
        assert out1["run_token"] != out2["run_token"]


class TestKillSwitch:
    def test_disabled_raises_instead_of_silent_skip(self, index_mod, monkeypatch):
        """Unlike data-spot-dispatcher's {"launched": false} skip, this
        dispatcher has no fail-open branch downstream — disabling it without
        supplying $.ec2_instance_id explicitly must be loud, not silent."""
        index, sd = index_mod
        monkeypatch.setattr(index, "DISPATCH_ENABLED", False)
        with pytest.raises(RuntimeError, match="WEEKLY_SPOT_DISPATCH_ENABLED=false"):
            index.handler({}, None)
        sd.launch_with_fallback.assert_not_called()


class TestFailLoud:
    def test_launch_failure_propagates(self, index_mod):
        index, sd = index_mod
        sd.launch_with_fallback.side_effect = SpotLaunchError("spot+on-demand exhausted")
        with pytest.raises(SpotLaunchError):
            index.handler({}, None)
        sd.wait_ssm_online.assert_not_called()

    def test_post_launch_ssm_online_failure_terminates_then_raises(self, index_mod):
        index, sd = index_mod
        sd.wait_ssm_online.side_effect = RuntimeError("ssm never online")
        with pytest.raises(RuntimeError, match="ssm never online"):
            index.handler({}, None)
        sd.terminate_on_failure.assert_called_once_with(
            "i-weeklylauncher", region=index.REGION, label="weekly-freshness"
        )

    def test_post_launch_send_command_failure_terminates_then_raises(self, index_mod):
        index, sd = index_mod
        sd.send_async_command.side_effect = RuntimeError("send_command AccessDenied")
        with pytest.raises(RuntimeError, match="AccessDenied"):
            index.handler({}, None)
        sd.terminate_on_failure.assert_called_once()


class TestBootstrapCommand:
    def test_clones_all_four_repos_at_expected_paths(self, index_mod):
        index, sd = index_mod
        index.handler({}, None)
        cmd = sd.send_async_command.call_args.args[1]
        assert "/home/ec2-user/alpha-engine-data" in cmd
        assert "/home/ec2-user/alpha-engine-config" in cmd
        assert "/home/ec2-user/alpha-engine-backtester" in cmd
        assert "/home/ec2-user/alpha-engine-dashboard" in cmd
        assert "nousergon/nousergon-data" in cmd
        assert "nousergon/alpha-engine-config" in cmd
        assert "nousergon/crucible-backtester" in cmd
        assert "nousergon/crucible-dashboard" in cmd

    def test_builds_dashboard_venv(self, index_mod):
        index, sd = index_mod
        index.handler({}, None)
        cmd = sd.send_async_command.call_args.args[1]
        assert "cd /home/ec2-user/alpha-engine-dashboard" in cmd
        assert ".venv" in cmd
        assert "requirements.txt" in cmd

    def test_arms_long_lived_watchdog_exceeding_sf_timeout(self, index_mod):
        index, sd = index_mod
        index.handler({}, None)
        cmd = sd.send_async_command.call_args.args[1]
        assert f"--on-active={index.WATCHDOG_SECONDS}" in cmd
        # SF top-level TimeoutSeconds (step_function.json) is 43200s (12h) —
        # the watchdog must clear it with headroom, never fire on a healthy run.
        assert index.WATCHDOG_SECONDS > 43200

    def test_reads_pat_from_ssm_for_private_repos(self, index_mod):
        index, sd = index_mod
        index.handler({}, None)
        cmd = sd.send_async_command.call_args.args[1]
        assert index.GH_PAT_SSM in cmd
        assert "x-access-token" in cmd

    def test_execution_timeout_passed_to_send_async_command(self, index_mod):
        index, sd = index_mod
        index.handler({}, None)
        kwargs = sd.send_async_command.call_args.kwargs
        assert kwargs["execution_timeout_seconds"] == index.BOOTSTRAP_TIMEOUT_SECONDS
