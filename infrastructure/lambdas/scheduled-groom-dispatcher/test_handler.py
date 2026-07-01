"""Unit tests for the EventBridge-Scheduler → groom-spot dispatcher (config#1432).

Hermetic: `nousergon_lib.ec2_spot` and `boto3` are stubbed in sys.modules BEFORE
importing index, so the tests run without either installed (matching how
deploy.sh runs them with bare python3). Validates: a schedule event launches a
spot box and fires an async SSM command carrying the run_mode; the on-demand
fallback on spot capacity exhaustion; run_mode normalisation; the kill-switch
short-circuit; and fail-loud (a launch failure RAISES so EventBridge retries +
the error metric surface the miss).
"""

from __future__ import annotations

import importlib
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(__file__))


# ── Stub nousergon_lib.ec2_spot + boto3 before importing index ─────────────────
class _SpotLaunchError(Exception):
    pass


class _SpotCapacityExhausted(_SpotLaunchError):
    pass


def _install_stubs(launch_impl, boto_clients):
    ec2_spot_mod = types.ModuleType("nousergon_lib.ec2_spot")
    ec2_spot_mod.SpotLaunchError = _SpotLaunchError
    ec2_spot_mod.SpotCapacityExhausted = _SpotCapacityExhausted
    ec2_spot_mod.launch = launch_impl
    pkg = types.ModuleType("nousergon_lib")
    pkg.ec2_spot = ec2_spot_mod
    sys.modules["nousergon_lib"] = pkg
    sys.modules["nousergon_lib.ec2_spot"] = ec2_spot_mod

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: boto_clients[name]
    sys.modules["boto3"] = boto3_mod


class _FakeWaiter:
    def wait(self, **kw):
        return None


class _FakeEc2:
    def __init__(self):
        self.terminated = []

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803 — boto3 kwarg name
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}


class _FakeSsm:
    def __init__(self):
        self.sent = []

    def describe_instance_information(self, **kw):
        return {"InstanceInformationList": [{"PingStatus": "Online"}]}

    def send_command(self, **kw):
        self.sent.append(kw)
        return {"Command": {"CommandId": "cmd-123"}}


def _load(monkeypatch, *, launch_impl=None, env=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm()
    ec2 = _FakeEc2()
    clients = {"ec2": ec2, "ssm": ssm}
    if launch_impl is None:
        launch_impl = lambda types_, subnets, **kw: "i-stub"  # noqa: E731
    _install_stubs(launch_impl, clients)
    import index

    importlib.reload(index)
    index._test_ssm = ssm  # expose for assertions
    index._test_ec2 = ec2
    return index


def test_schedule_event_launches_spot_and_sends_async_ssm(monkeypatch):
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["spot"] = kw.get("spot")
        calls["profile"] = kw.get("iam_instance_profile")
        return "i-abc"

    idx = _load(monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["instance_id"] == "i-abc"
    assert g["market"] == "spot"
    assert g["command_id"] == "cmd-123"
    assert g["run_mode"] == "full"
    assert calls["spot"] is True
    # The async SSM command carries the bootstrap that runs the FULL groom.
    sent = idx._test_ssm.sent[0]
    cmd = sent["Parameters"]["commands"][0]
    assert "groom_spot_bootstrap.sh --mode full" in cmd
    # AL2023 ships neither git nor python3.12 — the prelude must install them
    # BEFORE the clone (regression guard for the first cutover failure).
    assert "dnf install -y -q git python3.12" in cmd
    assert cmd.index("dnf install") < cmd.index("git clone")
    # SSM RunShellScript has no $HOME — git config/clone need it (cutover bug #2).
    assert "export HOME=/root" in cmd
    # Under `set -u` in a double-quoted context, a `$`-encoded run_url ($252F...)
    # would expand as positional params and abort the prelude (cutover bug #3).
    assert "$252F" not in cmd
    assert sent["Parameters"]["executionTimeout"] == [str(idx.MAX_RUNTIME_SECONDS)]


def test_on_demand_fallback_on_spot_capacity_exhaustion(monkeypatch):
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        if kw.get("spot"):
            raise _SpotCapacityExhausted("no capacity in any pool")
        return "i-ondemand"

    idx = _load(monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "x"}, None)
    assert out["groom"]["market"] == "on-demand"
    assert out["groom"]["instance_id"] == "i-ondemand"
    assert seen == [True, False]  # tried spot, then on-demand


def test_unknown_run_mode_falls_back_to_full(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "bogus"}, None)
    assert out["groom"]["run_mode"] == "full"
    assert "--mode full" in idx._test_ssm.sent[0]["Parameters"]["commands"][0]


def test_sweep_run_mode_is_forwarded(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "sweep"}, None)
    assert out["groom"]["run_mode"] == "sweep"
    assert "--mode sweep" in idx._test_ssm.sent[0]["Parameters"]["commands"][0]


def test_high_only_schedule_forwards_model_and_issue_filter(monkeypatch):
    # The 3rd (Opus, 8am PT) schedule's event carries model + issue_filter —
    # these must reach the box as exported env vars ahead of the bootstrap exec.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {"run_mode": "full", "model": "claude-opus-4-8", "issue_filter": "high-only",
         "schedule": "0 15 * * *"},
        None,
    )
    g = out["groom"]
    assert g["model"] == "claude-opus-4-8"
    assert g["issue_filter"] == "high-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=claude-opus-4-8" in cmd
    assert "export GROOM_ISSUE_FILTER=high-only" in cmd
    assert cmd.index("export GROOM_MODEL") < cmd.index("groom_spot_bootstrap.sh")


def test_missing_model_and_issue_filter_default_to_sonnet_queue(monkeypatch):
    # The 2 pre-existing Sonnet schedules don't set model/issue_filter — must
    # default exactly like before this feature (no behavior change for them).
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    g = out["groom"]
    assert g["model"] == "claude-sonnet-5"
    assert g["issue_filter"] == "default"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=claude-sonnet-5" in cmd
    assert "export GROOM_ISSUE_FILTER=default" in cmd


def test_unknown_issue_filter_falls_back_to_default(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "issue_filter": "bogus"}, None)
    assert out["groom"]["issue_filter"] == "default"


def test_malformed_model_falls_back_to_default(monkeypatch):
    # A model string with shell metacharacters must be rejected outright rather
    # than embedded into the SSM command (defense-in-depth allowlist).
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "model": "claude; rm -rf /"}, None)
    assert out["groom"]["model"] == "claude-sonnet-5"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "claude; rm -rf /" not in cmd
    assert "export GROOM_MODEL=claude-sonnet-5" in cmd


def test_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "false"})
    out = idx.handler({"run_mode": "full"}, None)
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "disabled"
    assert idx._test_ssm.sent == []  # nothing launched


def test_launch_failure_raises(monkeypatch):
    # Fail-loud: a scheduled groom is the deliverable, so a launch failure must
    # RAISE (EventBridge retries + the Lambda error metric record the miss).
    def _boom(types_, subnets, **kw):
        raise _SpotLaunchError("RunInstances denied")

    idx = _load(monkeypatch, launch_impl=_boom, env={"GROOM_DISPATCH_ENABLED": "true"})
    with pytest.raises(_SpotLaunchError, match="RunInstances denied"):
        idx.handler({"run_mode": "full"}, None)


def test_post_launch_failure_terminates_instance_no_orphan(monkeypatch):
    # If the box launches but a later step (SSM-online / SendCommand) fails, the
    # box would orphan (no bootstrap → no watchdog/trap). The dispatcher must
    # terminate it before re-raising. Regression cover for the 2026-06-30 orphan.
    idx = _load(
        monkeypatch,
        launch_impl=lambda types_, subnets, **kw: "i-orphan",  # noqa: E731
        env={"GROOM_DISPATCH_ENABLED": "true"},
    )

    def _boom_send(**kw):
        raise RuntimeError("SSM SendCommand failed")

    idx._test_ssm.send_command = _boom_send
    with pytest.raises(RuntimeError, match="SendCommand failed"):
        idx.handler({"run_mode": "full"}, None)
    # The just-launched box was terminated (not orphaned) before the re-raise.
    assert idx._test_ec2.terminated == ["i-orphan"]
