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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


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
    fleet_mod = types.ModuleType("nousergon_lib.flow_doctor_fleet")

    class _FleetTelegramTopic:
        CRITICAL = "critical"
        OPS_HEALTH = "ops_health"
        GROOM = "groom"
        PIPELINE = "pipeline"

    fleet_mod.FleetTelegramTopic = _FleetTelegramTopic
    pkg.flow_doctor_fleet = fleet_mod
    sys.modules["nousergon_lib"] = pkg
    sys.modules["nousergon_lib.ec2_spot"] = ec2_spot_mod
    sys.modules["nousergon_lib.flow_doctor_fleet"] = fleet_mod

    fdt_mod = types.ModuleType("flow_doctor_telegram")
    fdt_mod.notify_via_flow_doctor = lambda *a, **k: True  # type: ignore[attr-defined]
    sys.modules["flow_doctor_telegram"] = fdt_mod

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


class _FakeS3Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3Paginator:
    def __init__(self, objects: dict):
        self._objects = objects

    def paginate(self, Bucket, Prefix):  # noqa: N803 — boto3 kwarg names
        keys = [k for k in self._objects if k.startswith(Prefix)]
        yield {"Contents": [{"Key": k} for k in keys]}


class _FakeS3:
    """Fake S3 client for the pace gate's boto3-native WET reader.

    ``objects`` maps a full S3 key to its raw JSON bytes content — mirrors the
    real ``claude_code_usage/{source}/{date}[/{run}].json`` layout.
    """

    def __init__(self, objects: dict | None = None):
        self._objects = objects or {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakeS3Paginator(self._objects)

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        return {"Body": _FakeS3Body(self._objects[Key])}


def _load(monkeypatch, *, launch_impl=None, env=None, s3_objects=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm()
    ec2 = _FakeEc2()
    s3 = _FakeS3(s3_objects)
    clients = {"ec2": ec2, "ssm": ssm, "s3": s3}
    if launch_impl is None:
        launch_impl = lambda types_, subnets, **kw: "i-stub"  # noqa: E731
    _install_stubs(launch_impl, clients)
    import index

    importlib.reload(index)
    index._test_ssm = ssm  # expose for assertions
    index._test_ec2 = ec2
    index._test_s3 = s3
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


# ── Pre-boot pace gate (2026-07-04) ─────────────────────────────────────────
def _wet_doc(wet: float) -> bytes:
    import json
    return json.dumps({"by_hour": {"10": {"opus": {"wet": wet}}}}).encode()


def _spy_notify(monkeypatch, idx):
    """Replace notify_via_flow_doctor with a recorder."""
    calls = []
    monkeypatch.setattr(
        idx,
        "notify_via_flow_doctor",
        lambda text, **kw: calls.append((text, kw)) or True,
    )
    return calls


def test_pace_gate_skips_launch_when_usage_ahead_of_pace(monkeypatch):
    # 1 day into the window (elapsed_frac ~= 1/7 ~= 0.143): 50% of the weekly
    # ceiling already consumed is way ahead of pace -> skip BEFORE any launch.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-06-29.json":
                            _wet_doc(0.5 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=1)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    notified = _spy_notify(monkeypatch, idx)

    def _launch(types_, subnets, **kw):
        raise AssertionError("spot launch must NOT be attempted when the pace gate skips")

    monkeypatch.setattr(idx.ec2_spot, "launch", _launch)
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    g = out["groom"]
    assert g["launched"] is False
    assert g["reason"] == "pace_gate_skip"
    assert g["exceeded"] is True
    assert idx._test_ssm.sent == []  # no bootstrap command ever sent
    # The pre-boot skip must notify — it's the ONLY place this outcome is ever
    # visible (a run that never boots has no on-box groom_run.sh to ping).
    assert len(notified) == 1
    assert notified[0][1]["silent"] is True
    assert notified[0][1]["severity"] == "info"
    assert notified[0][1]["silent_topic"] is not None
    assert "SKIPPED" in notified[0][0]
    assert "soft budget threshold passed before boot" in notified[0][0]
    assert "never launched" in notified[0][0]


def test_pace_gate_allows_launch_when_on_pace(monkeypatch):
    # 50% elapsed, only 10% of the ceiling used -> well under pace -> launches.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-06-29.json":
                            _wet_doc(0.1 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=3, hours=12)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    notified = _spy_notify(monkeypatch, idx)
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["launched"] is True
    assert len(idx._test_ssm.sent) == 1
    assert notified == []  # no ping when nothing was skipped


def test_pace_gate_disabled_still_launches_even_if_ahead_of_pace(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                   "GROOM_PACE_GATE_ENABLED": "false"},
                s3_objects={"claude_code_usage/groom/2026-06-29.json":
                            _wet_doc(0.99 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=1)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    notified = _spy_notify(monkeypatch, idx)
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["launched"] is True
    assert notified == []  # kill-switch disables the ping too — the gate never ran


def test_pace_gate_fails_safe_and_still_launches_on_s3_error(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    notified = _spy_notify(monkeypatch, idx)

    def _boom(**kw):
        raise RuntimeError("S3 unreachable")
    monkeypatch.setattr(idx._test_s3, "get_paginator", _boom)
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["launched"] is True
    assert notified == []  # fail-safe path never trips (exceeded=False), no ping


def test_missing_model_and_issue_filter_default_to_mid_queue(monkeypatch):
    # Schedules with no model/issue_filter must default to Sonnet / mid-only.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    g = out["groom"]
    assert g["model"] == "claude-sonnet-5"
    assert g["issue_filter"] == "mid-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=claude-sonnet-5" in cmd
    assert "export GROOM_ISSUE_FILTER=mid-only" in cmd


def test_low_only_schedule_forwards_haiku_model_and_filter(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {"run_mode": "full", "model": "claude-haiku-4-5", "issue_filter": "low-only",
         "schedule": "0 7 * * *"},
        None,
    )
    g = out["groom"]
    assert g["model"] == "claude-haiku-4-5"
    assert g["issue_filter"] == "low-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=claude-haiku-4-5" in cmd
    assert "export GROOM_ISSUE_FILTER=low-only" in cmd


def test_unknown_issue_filter_falls_back_to_mid_only(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "issue_filter": "bogus"}, None)
    assert out["groom"]["issue_filter"] == "mid-only"


def test_malformed_model_falls_back_to_default(monkeypatch):
    # A model string with shell metacharacters must be rejected outright rather
    # than embedded into the SSM command (defense-in-depth allowlist).
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "model": "claude; rm -rf /"}, None)
    assert out["groom"]["model"] == "claude-sonnet-5"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "claude; rm -rf /" not in cmd
    assert "export GROOM_MODEL=claude-sonnet-5" in cmd


def test_soft_limit_min_override_forwarded_for_bounded_test(monkeypatch):
    # A manual invoke can bound a test run (e.g. 60 min) without touching any
    # live schedule — none of the 3 SCHED_INPUTS carry this key.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler({"run_mode": "full", "issue_filter": "high-only", "soft_limit_min": 60}, None)
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "--soft-limit-min 60" in cmd


def test_missing_soft_limit_min_omits_the_flag(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler({"run_mode": "full"}, None)
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "--soft-limit-min" not in cmd


def test_malformed_soft_limit_min_ignored(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler({"run_mode": "full", "soft_limit_min": "not-a-number"}, None)
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "--soft-limit-min" not in cmd


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


def test_run_token_generated_and_exported_and_returned(monkeypatch):
    # config#1645: the dispatch Step Function's completion-marker check needs a
    # per-attempt token that reaches the box (as GROOM_RUN_TOKEN) AND comes back
    # in the Lambda's own response (so the SF can build the S3 key to check).
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    g = out["groom"]
    assert "run_token" in g and g["run_token"], "run_token missing from the Lambda's response"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert f"export GROOM_RUN_TOKEN={g['run_token']}" in cmd
    assert cmd.index("export GROOM_RUN_TOKEN") < cmd.index("groom_spot_bootstrap.sh")


def test_run_token_differs_across_invocations(monkeypatch):
    # Each SF relaunch attempt calls the Lambda again — a stale token reused
    # across attempts would let a dead attempt's (absent) marker be confused
    # with a fresh attempt's, defeating the whole relaunch-detection mechanism.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out1 = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    out2 = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out1["groom"]["run_token"] != out2["groom"]["run_token"]


def test_force_on_demand_skips_spot_entirely(monkeypatch):
    # config#1645: the SF's final bounded relaunch attempt after repeated
    # mid-run spot interruption sets force_on_demand — must go straight to
    # on-demand, not attempt (and possibly lose to) spot a third time.
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        return "i-forced"

    idx = _load(monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "force_on_demand": True}, None)
    assert out["groom"]["market"] == "on-demand"
    assert seen == [False], "force_on_demand must skip the spot attempt outright"


def test_force_on_demand_absent_by_default(monkeypatch):
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        return "i-normal"

    idx = _load(monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler({"run_mode": "full"}, None)
    assert seen == [True], "the 2 pre-existing schedules' behavior must be unchanged"


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
