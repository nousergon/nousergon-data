"""Handler tests for alpha-engine-alert-drain-dispatcher (alpha-engine-config-I2824).

Pins the executor contract: clean-JSON verdicts (never raises), single-lane
concurrency lock, drill run-id isolation (drill- prefix, never collides with
a real run's), atomic extra_tags launch, post-launch terminate-on-failure,
kill switch, and the degraded-probe coverage-beats-dedupe posture.

Hermetic: nousergon_lib.spot_dispatch is stubbed in sys.modules before
``import index`` (same shape as the sibling dispatchers' test files).
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
    sd.SpotProbeError = SpotProbeError
    sd.launch_with_fallback = MagicMock(return_value=("i-abc123", "spot"))
    sd.wait_ssm_online = MagicMock()
    sd.send_async_command = MagicMock(return_value="cmd-1")
    sd.terminate_on_failure = MagicMock()
    sd.running_instance_ids = MagicMock(return_value=[])
    lib = types.ModuleType("nousergon_lib")
    lib.spot_dispatch = sd
    monkeypatch.setitem(sys.modules, "nousergon_lib", lib)
    monkeypatch.setitem(sys.modules, "nousergon_lib.spot_dispatch", sd)
    monkeypatch.syspath_prepend(str(SCRIPT_DIR))
    sys.modules.pop("index", None)
    index = importlib.import_module("index")
    return index, sd


class TestVerdicts:
    def test_launches_with_run_id_and_atomic_tags(self, index_mod):
        index, sd = index_mod
        out = index.handler({"trigger": "scheduled-1000utc"}, None)
        assert out["launched"] is True and out["reason"] == "launched"
        assert out["run_id"].startswith("drain-")
        tags = sd.launch_with_fallback.call_args.kwargs["extra_tags"]
        assert tags[index.DRAIN_RUN_ID_TAG_KEY] == out["run_id"]
        assert index.DRAIN_DRILL_TAG_KEY not in tags
        assert (
            sd.launch_with_fallback.call_args.kwargs["iam_instance_profile"]
            == "alpha-engine-alert-drain-executor-profile"
        )

    def test_drill_run_id_prefix_isolated(self, index_mod):
        index, sd = index_mod
        out = index.handler({"is_drill": "true"}, None)
        assert out["run_id"].startswith("drill-")
        assert not out["run_id"].startswith("drain-")
        tags = sd.launch_with_fallback.call_args.kwargs["extra_tags"]
        assert tags[index.DRAIN_DRILL_TAG_KEY] == "true"

    def test_disabled_kill_switch(self, index_mod, monkeypatch):
        index, sd = index_mod
        monkeypatch.setattr(index, "DISPATCH_ENABLED", False)
        out = index.handler({}, None)
        assert out == {"launched": False, "reason": "disabled"}
        sd.launch_with_fallback.assert_not_called()

    def test_concurrent_skip_single_lane(self, index_mod):
        index, sd = index_mod
        sd.running_instance_ids.return_value = ["i-live"]
        out = index.handler({}, None)
        assert out["reason"] == "concurrent_skip"
        assert out["existing_instance_ids"] == ["i-live"]
        sd.launch_with_fallback.assert_not_called()

    def test_probe_failure_degrades_but_launches(self, index_mod):
        index, sd = index_mod
        sd.running_instance_ids.side_effect = SpotProbeError("api down")
        out = index.handler({}, None)
        assert out["launched"] is True and out["dedupe_degraded"] is True
        assert "api down" in out["dedupe_probe_error"]

    def test_launch_failure_clean_verdict(self, index_mod):
        index, sd = index_mod
        sd.launch_with_fallback.side_effect = SpotLaunchError("exhausted")
        out = index.handler({}, None)
        assert out == {"launched": False, "reason": "launch_failed", "error": "exhausted"}

    def test_post_launch_failure_terminates_box(self, index_mod):
        index, sd = index_mod
        sd.wait_ssm_online.side_effect = RuntimeError("ssm never online")
        out = index.handler({}, None)
        assert out["reason"] == "post_launch_failed"
        sd.terminate_on_failure.assert_called_once()

    def test_invalid_event_clean_verdict(self, index_mod):
        index, _ = index_mod
        out = index.handler({"is_drill": "bogus"}, None)
        assert out["reason"] == "invalid_event"


class TestBootstrapCommand:
    def test_command_targets_alert_drain_bootstrap_with_run_id(self, index_mod):
        index, sd = index_mod
        index.handler({"is_drill": "false"}, None)
        cmd = sd.send_async_command.call_args.args[1]
        assert "infrastructure/alert_drain_spot_bootstrap.sh" in cmd
        assert "--run-id" in cmd and "--is-drill" in cmd
        assert index.DRAIN_GH_PAT_SSM in cmd


def test_model_threaded_into_bootstrap_export():
    """config-I3293: a router-injected model lands as the DRAIN_MODEL export
    in the SSM bootstrap command; absent, the export is empty (run script's
    inline default applies via its :- expansion)."""
    import index
    cmd = index._bootstrap_command("drain-x", "false", "claude-sonnet-5")
    assert 'export DRAIN_MODEL="claude-sonnet-5"' in cmd
    cmd_default = index._bootstrap_command("drain-x", "false")
    assert 'export DRAIN_MODEL=""' in cmd_default


def test_malformed_model_rejected():
    """Shell-injection guard: a model failing _MODEL_RE is a clean
    invalid_event decline, never interpolated into the SSM command."""
    import index
    out = index.handler({"trigger": "x", "model": 'claude-$(rm -rf /)'}, None)
    assert out == {"launched": False, "reason": "invalid_event",
                   "error": out["error"]}
