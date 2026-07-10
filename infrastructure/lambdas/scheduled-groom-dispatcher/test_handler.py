"""Unit tests for the EventBridge-Scheduler → groom-spot dispatcher (config#1432).

Hermetic: ``nousergon_lib.ec2_spot`` and ``boto3`` are stubbed in sys.modules
BEFORE importing index; ``nousergon_lib.flow_doctor_fleet`` is the REAL pinned
enum installed by deploy.sh's preflight gate (config#1772 — no hand-maintained
FleetTelegramTopic fake). Validates: a schedule event launches a spot box and
fires an async SSM command carrying the run_mode; the on-demand fallback on spot
capacity exhaustion; run_mode normalisation; the kill-switch short-circuit; and
fail-loud (a launch failure RAISES so EventBridge retries + the error metric
surface the miss).
"""

from __future__ import annotations

import importlib
import json
import os
import re
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Stub nousergon_lib.ec2_spot + boto3 before importing index ─────────────────
class _SpotLaunchError(Exception):
    pass


class _SpotCapacityExhausted(_SpotLaunchError):
    pass


def _install_stubs(launch_impl, boto_clients):
    # Real nousergon_lib.flow_doctor_fleet (FleetTelegramTopic enum) is installed
    # into TEST_DEPS by deploy.sh — do NOT hand-roll it here (config#1772).
    ec2_spot_mod = types.ModuleType("nousergon_lib.ec2_spot")
    ec2_spot_mod.SpotLaunchError = _SpotLaunchError
    ec2_spot_mod.SpotCapacityExhausted = _SpotCapacityExhausted
    ec2_spot_mod.launch = launch_impl
    sys.modules["nousergon_lib.ec2_spot"] = ec2_spot_mod

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
    def __init__(self, running_tier_instances=None):
        self.terminated = []
        self.tags_created = []
        # config#1979: (issue_filter -> [instance_ids]) already "live" for the
        # concurrent-tier guard's describe_instances check to find.
        self._running_tier_instances = dict(running_tier_instances or {})

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803 — boto3 kwarg name
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}

    def create_tags(self, Resources, Tags):  # noqa: N803 — boto3 kwarg names
        self.tags_created.append((Resources, Tags))
        return {}

    def describe_instances(self, Filters):  # noqa: N803 — boto3 kwarg name
        by_name = {f["Name"]: f["Values"] for f in Filters}
        issue_filter = by_name.get("tag:groom-issue-filter", [None])[0]
        ids = self._running_tier_instances.get(issue_filter, [])
        return {"Reservations": [{"Instances": [{"InstanceId": i} for i in ids]}]} if ids else {"Reservations": []}


class _FakeSsm:
    def __init__(self, parameters=None):
        self.sent = []
        self.parameters = dict(parameters or {})

    def describe_instance_information(self, **kw):
        return {"InstanceInformationList": [{"PingStatus": "Online"}]}

    def send_command(self, **kw):
        self.sent.append(kw)
        return {"Command": {"CommandId": "cmd-123"}}

    def get_parameter(self, Name):  # noqa: N803 — boto3 API
        if Name not in self.parameters:
            raise RuntimeError(f"Parameter {Name} not found")
        return {"Parameter": {"Value": self.parameters[Name]}}


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

    def list_objects_v2(self, Bucket, Prefix):  # noqa: N803 — boto3 kwarg names
        # config#2038: _load_recent_engagements calls this directly (no
        # paginator) — single-page return is sufficient for these tests.
        keys = [k for k in self._objects if k.startswith(Prefix)]
        return {"Contents": [{"Key": k} for k in keys]}

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        return {"Body": _FakeS3Body(self._objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):  # noqa: N803 — boto3 kwarg names
        # config#2152: records queue-manifest / trigger-record writes for
        # assertions; stored alongside the seeded read objects.
        self._objects[Key] = Body
        return {}


def _load(monkeypatch, *, launch_impl=None, env=None, s3_objects=None, ssm_parameters=None,
         running_tier_instances=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm(ssm_parameters)
    ec2 = _FakeEc2(running_tier_instances=running_tier_instances)
    s3 = _FakeS3(s3_objects)
    clients = {"ec2": ec2, "ssm": ssm, "s3": s3}
    if launch_impl is None:
        launch_impl = lambda types_, subnets, **kw: "i-stub"  # noqa: E731
    _install_stubs(launch_impl, clients)
    # Derive the stub requirement from index.py's live import graph and fail
    # loud on drift here, rather than as a ModuleNotFoundError at deploy time
    # (config#1746 — this stub has drifted three times: config#1742/#1748).
    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)
    # nousergon_lib.spot_dispatch (config#2106) sits between index.py and the
    # stubbed nousergon_lib.ec2_spot/boto3 above. Its own `from nousergon_lib
    # import ec2_spot` / `import boto3` bindings are resolved once at ITS
    # import time — if it's already cached in sys.modules from a prior test's
    # stub, `import index` + reload(index) alone would NOT re-resolve those
    # bindings (index just re-fetches the same, stale spot_dispatch module
    # object). Reload spot_dispatch in place first (never a bare del+reimport
    # — see reference_pytest_del_reimport_vs_reload_fixture_corruption_260709)
    # so every test sees the CURRENT stub.
    if "nousergon_lib.spot_dispatch" in sys.modules:
        importlib.reload(sys.modules["nousergon_lib.spot_dispatch"])
    else:
        import nousergon_lib.spot_dispatch  # noqa: F401 — first import picks up the current stub

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
    # The Opus (6pm PT) schedule's event carries model + issue_filter —
    # these must reach the box as exported env vars ahead of the bootstrap exec.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {"run_mode": "full", "model": "claude-opus-4-8", "issue_filter": "high-only",
         "schedule": "0 1 * * *"},
        None,
    )
    g = out["groom"]
    assert g["model"] == "claude-opus-4-8"
    assert g["issue_filter"] == "high-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=claude-opus-4-8" in cmd
    assert "export GROOM_ISSUE_FILTER=high-only" in cmd
    assert cmd.index("export GROOM_MODEL") < cmd.index("groom_spot_bootstrap.sh")


def test_high_only_schedule_forwards_pr_budget(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {
            "run_mode": "full",
            "model": "claude-opus-4-8",
            "issue_filter": "high-only",
            "schedule": "0 1 * * *",
            "pr_budget": 100,
        },
        None,
    )
    assert out["groom"]["pr_budget"] == 100
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_PR_BUDGET=100" in cmd


def test_deploy_schedule_high_only_carries_pr_budget():
    deploy_sh = (
        Path(__file__).resolve().parent / "deploy.sh"
    ).read_text()
    assert '"pr_budget":100' in deploy_sh or '"pr_budget": 100' in deploy_sh
    # Haiku/Sonnet schedules must NOT inherit the Opus override.
    low_line = next(
        line for line in deploy_sh.splitlines()
        if "low-only" in line and "SCHED_INPUTS" not in line
    )
    assert "pr_budget" not in low_line


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
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.5 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=1)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    notified = _spy_notify(monkeypatch, idx)

    def _launch(types_, subnets, **kw):
        raise AssertionError("spot launch must NOT be attempted when the pace gate skips")

    # index.py now delegates through nousergon_lib.spot_dispatch (config#2106)
    # rather than calling nousergon_lib.ec2_spot directly — patch the entry
    # point it actually calls.
    monkeypatch.setattr(idx.spot_dispatch, "launch_with_fallback", _launch)
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
                s3_objects={"claude_code_usage/groom/2026-07-15.json":
                            _wet_doc(0.1 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=3, hours=12)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    notified = _spy_notify(monkeypatch, idx)
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["launched"] is True
    assert len(idx._test_ssm.sent) == 1
    assert notified == []  # no ping when nothing was skipped


def test_pace_gate_suspended_when_operator_override_active(monkeypatch):
    # Way over pace, but SSM override is active -> still launch.
    idx = _load(
        monkeypatch,
        env={"GROOM_DISPATCH_ENABLED": "true"},
        s3_objects={"claude_code_usage/groom/2026-07-13.json":
                    _wet_doc(0.9 * 1_140_000_000)},
        ssm_parameters={"/alpha-engine/groom/dynamic_budget_override_until": "2099-01-01T00:00"},
    )
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=1)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["launched"] is True


def test_pace_gate_disabled_still_launches_even_if_ahead_of_pace(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                   "GROOM_PACE_GATE_ENABLED": "false"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
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


def test_gated_reverify_schedule_forwards_filter(monkeypatch):
    # config#1891 Sunday lane: "gated-reverify" must pass validation — it was
    # missing from _VALID_ISSUE_FILTERS (PR #681 added only the schedule), so
    # the weekly lane would have silently run as mid-only.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {"run_mode": "full", "model": "claude-haiku-4-5",
         "issue_filter": "gated-reverify", "schedule": "0 9 * * 0"},
        None,
    )
    g = out["groom"]
    assert g["issue_filter"] == "gated-reverify"
    assert g["model"] == "claude-haiku-4-5"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_ISSUE_FILTER=gated-reverify" in cmd


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
         "schedule": "0 19 * * *"},
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


# ── config#1979: concurrent-same-tier guard ───────────────────────────────────
def test_concurrent_tier_skip_when_same_tier_already_running(monkeypatch):
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-new"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"mid-only": ["i-already-running"]},
    )
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    g = out["groom"]
    assert g["launched"] is False
    assert g["reason"] == "concurrent_tier_skip"
    assert g["existing_instance_ids"] == ["i-already-running"]
    assert launched == []  # never even attempted a spot launch — zero spend


def test_different_tier_running_does_not_block_launch(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"high-only": ["i-other-tier"]},
    )
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert out["groom"]["launched"] is True
    assert out["groom"]["instance_id"] == "i-new"


def test_launched_instance_gets_tagged_with_its_tier(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"GROOM_DISPATCH_ENABLED": "true"},
    )
    idx.handler({"run_mode": "full", "issue_filter": "high-only", "schedule": "x"}, None)
    assert idx._test_ec2.tags_created == [
        (["i-new"], [{"Key": "groom-issue-filter", "Value": "high-only"}])
    ]


def test_concurrent_tier_check_fails_safe_and_still_launches(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"GROOM_DISPATCH_ENABLED": "true"},
    )

    def _boom(Filters):  # noqa: N803 — boto3 kwarg name
        raise RuntimeError("EC2 API hiccup")

    idx._test_ec2.describe_instances = _boom
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    # A broken check must never block a launch — it's an optimization, not a
    # correctness gate (mirrors the pace gate / demand gate fail-safe posture).
    assert out["groom"]["launched"] is True


# ── config#1933: demand-driven dispatch (enumerate-then-decide) ──────────────
# groom_eligibility is PURE (no boto3), so these tests use the REAL module —
# the decision math itself is covered in nousergon-lib; here we test the
# Lambda wiring: skip path, bundle/model override, bypasses, and fail-safe.


def _stub_stats(monkeypatch, idx, counts, oldest=None, has_p0=False):
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats",
                        lambda token: (counts, oldest or {}, has_p0))
    monkeypatch.setattr(idx, "_write_decision_record",
                        lambda *a, **k: None)
    monkeypatch.setattr(idx, "_notify_demand_skip", lambda *a, **k: None)


def test_demand_gate_skips_light_queue_with_zero_launch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 3, "mid": 40, "high": 2})
    out = idx.handler({"run_mode": "full", "model": "claude-haiku-4-5",
                       "issue_filter": "low-only", "schedule": "0 19 * * *"}, None)
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "demand_gate_skip"
    assert not idx._test_ssm.sent  # no box, no SSM command


def test_demand_gate_bundles_and_downgrades_model(monkeypatch):
    # Opus slot, no high issues, starving low+mid -> ONE Sonnet run.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 5, "mid": 6, "high": 0})
    out = idx.handler({"run_mode": "full", "model": "claude-opus-4-8",
                       "issue_filter": "high-only", "schedule": "0 1 * * *"}, None)
    g = out["groom"]
    assert g["launched"] and g["issue_filter"] == "mid+low"
    assert g["model"] == "claude-sonnet-5"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_ISSUE_FILTER=mid+low" in cmd
    assert "export GROOM_MODEL=claude-sonnet-5" in cmd


def test_demand_gate_full_queues_run_own_tier(monkeypatch):
    # Brian's 8/9/10: the mid slot runs mid-only on Sonnet, nothing bundles.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
                       "issue_filter": "mid-only", "schedule": "0 7 * * *"}, None)
    assert out["groom"]["launched"] and out["groom"]["issue_filter"] == "mid-only"


def test_demand_gate_bypassed_for_reverify_force_and_sweep(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    called = []
    monkeypatch.setattr(idx, "_enumerate_tier_stats",
                        lambda token: called.append(1) or ({}, {}, False))
    # gated-reverify: no tier queue -> gate bypassed, launch proceeds
    out = idx.handler({"run_mode": "full", "model": "claude-haiku-4-5",
                       "issue_filter": "gated-reverify"}, None)
    assert out["groom"]["launched"]
    # force_on_demand (relaunch SF final retry): must never be blocked
    out = idx.handler({"run_mode": "full", "issue_filter": "low-only",
                       "force_on_demand": True}, None)
    assert out["groom"]["launched"] and out["groom"]["issue_filter"] == "low-only"
    # sweep mode untouched
    out = idx.handler({"run_mode": "sweep"}, None)
    assert out["groom"]["launched"]
    assert not called  # enumeration never ran for any bypass


def test_demand_gate_fail_safe_launches_legacy_on_enumeration_error(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    def boom(token):
        raise RuntimeError("github down")
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats", boom)
    out = idx.handler({"run_mode": "full", "model": "claude-haiku-4-5",
                       "issue_filter": "low-only"}, None)
    assert out["groom"]["launched"] and out["groom"]["issue_filter"] == "low-only"


def test_demand_gate_kill_switch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_DEMAND_GATE_ENABLED": "false"})
    called = []
    monkeypatch.setattr(idx, "_enumerate_tier_stats",
                        lambda token: called.append(1) or ({}, {}, False))
    out = idx.handler({"run_mode": "full", "issue_filter": "low-only"}, None)
    assert out["groom"]["launched"] and not called


# ── config#1933 SYMMETRIC triggers (Brian's ratified correction) ─────────────


def _stub_fresh_stats(monkeypatch, idx, counts, oldest=None, p0=(), tier_issues=None):
    # config#2152: default tier_issues fabricates N placeholder issues per tier
    # so the count and the manifest queue stay consistent by construction —
    # mirroring the real single-walk enumeration.
    if tier_issues is None:
        tier_issues = {t: [{"repo": "nousergon/alpha-engine-config", "number": 9000 + i,
                            "title": f"{t} issue {i}", "labels": [f"complexity:{t}"],
                            "updated_at": "2026-07-10T00:00:00Z"}
                           for i in range(n)] for t, n in counts.items()}
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (counts, oldest or {}, list(p0), tier_issues))
    monkeypatch.setattr(idx, "_write_trigger_record", lambda *a, **k: None)
    monkeypatch.setattr(idx, "_notify_demand_skip", lambda *a, **k: None)


def _demand_event(sched="0 1 * * *"):
    return {"run_mode": "full", "trigger": "demand-all", "schedule": sched}


def test_symmetric_trigger_brians_8_9_10_launches_three_boxes(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    g = out["groom"]
    assert g["trigger"] == "demand-all"
    launched = {(l["issue_filter"], l["model"]) for l in g["launches"]}
    assert launched == {("high-only", "claude-opus-4-8"),
                        ("mid-only", "claude-sonnet-5"),
                        ("low-only", "claude-haiku-4-5")}
    cmds = [c["Parameters"]["commands"][0] for c in idx._test_ssm.sent]
    assert len(cmds) == 3
    # config#2129: every co-launched box gets its OWN disjoint sweep
    # partition — no more "only the first box sweeps" starvation. All 3
    # share the SAME partition_count (3) with distinct partition_index
    # values 0/1/2 (order follows decide_trigger's high-first pool order).
    assert "GROOM_NO_SWEEP=1" not in cmds[0]
    for c in cmds:
        assert "export GROOM_SWEEP_PARTITION_COUNT=3" in c
    indices = set()
    for c in cmds:
        m = re.search(r"export GROOM_SWEEP_PARTITION_INDEX=(\d+)", c)
        assert m, f"missing partition index export in: {c}"
        indices.add(int(m.group(1)))
    assert indices == {0, 1, 2}


def test_symmetric_trigger_light_backlog_zero_boxes(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 2, "mid": 3, "high": 1})
    out = idx.handler(_demand_event("0 7 * * *"), None)
    assert out["groom"]["launches"] == []
    assert not idx._test_ssm.sent


def test_symmetric_trigger_thin_pool_downgrades_model(monkeypatch):
    # 5 low + 6 mid + 0 high pooled -> ONE Sonnet box regardless of trigger time.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 5, "mid": 6, "high": 0})
    out = idx.handler(_demand_event(), None)
    ls = out["groom"]["launches"]
    assert len(ls) == 1 and ls[0]["issue_filter"] == "mid+low"
    assert ls[0]["model"] == "claude-sonnet-5"


def test_symmetric_trigger_skips_on_enumeration_error(monkeypatch):
    """demand-all enumeration failure now returns early — no legacy fallthrough.

    config#2142: the skip must also PAGE ops-health (a skipped trigger means
    NO groom boxes launch for the slot — the predecessor CloudWatch-only
    warning hid a dead engagement scan for 8 consecutive triggers)."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    notifications = _spy_notify(monkeypatch, idx)
    def boom(token):
        raise RuntimeError("github down")
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh", boom)
    out = idx.handler(_demand_event(), None)
    assert not out["groom"]["launched"]
    assert out["groom"]["reason"] == "demand_all_failed"
    assert len(notifications) == 1
    text, kw = notifications[0]
    assert "FAILED" in text and "github down" in text
    assert kw["severity"] == "warning" and kw["silent"] is False


def test_load_recent_engagements_raises_on_s3_access_denied(monkeypatch):
    """config#2142: an engagement-scan read failure must RAISE, never degrade
    to an empty map. The old fail-safe ``{}`` ("skip nothing") silently
    disabled fresh-skip on every trigger from ship (2026-07-08) to 2026-07-10
    when the role lacked ListBucket on groom/{date}/ — the dispatcher
    advertised pre-skip counts (e.g. high=26) that deflated on-box (10)."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    def denied(Bucket, Prefix):  # noqa: N803 — boto3 kwarg names
        raise RuntimeError("AccessDenied: s3:ListBucket")
    monkeypatch.setattr(idx._test_s3, "list_objects_v2", denied)
    with pytest.raises(RuntimeError, match="AccessDenied"):
        idx._load_recent_engagements()


def test_non_demand_events_keep_legacy_behavior(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    called = []
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: called.append(1) or ({}, {}, []))
    monkeypatch.setattr(idx, "_demand_decision", lambda f, s: None)
    out = idx.handler({"run_mode": "full", "model": "claude-haiku-4-5",
                       "issue_filter": "gated-reverify"}, None)
    assert out["groom"]["launched"] and not called


# ── config#2129: decide_only / launch_decided (two-phase SF Map-state flow) ──
# The SF no longer invokes this Lambda once per trigger and tries to poll a
# response shape that varies 1-vs-N launches. decide_only computes 0..N
# launch decisions WITHOUT launching; launch_decided launches EXACTLY one
# already-decided box. Both must never actually boot a spot instance.


def test_decide_only_demand_all_returns_launches_without_launching(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})

    def _launch(types_, subnets, **kw):
        raise AssertionError("decide_only must never launch a spot instance")
    monkeypatch.setattr(idx.spot_dispatch, "launch_with_fallback", _launch)

    out = idx.handler({**_demand_event(), "decide_only": True}, None)
    d = out["decide"]
    assert d["trigger"] == "demand-all"
    assert len(d["launches"]) == 3
    assert {e["issue_filter"] for e in d["launches"]} == {"high-only", "mid-only", "low-only"}
    counts = {e["partition_count"] for e in d["launches"]}
    assert counts == {3}
    assert sorted(e["partition_index"] for e in d["launches"]) == [0, 1, 2]
    assert idx._test_ssm.sent == []


def test_decide_only_single_tier_returns_one_launch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
                       "issue_filter": "mid-only", "schedule": "0 7 * * *",
                       "decide_only": True}, None)
    d = out["decide"]
    assert d["launches"] == [{"model": "claude-sonnet-5", "issue_filter": "mid-only",
                              "partition_index": 0, "partition_count": 1}]
    assert idx._test_ssm.sent == []


def test_decide_only_ungated_direct_dispatch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "model": "claude-haiku-4-5",
                       "issue_filter": "gated-reverify", "decide_only": True}, None)
    assert out["decide"]["launches"] == [{"model": "claude-haiku-4-5",
                                          "issue_filter": "gated-reverify",
                                          "partition_index": 0, "partition_count": 1}]
    assert idx._test_ssm.sent == []


def test_decide_only_demand_gate_skip_shape(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 3, "mid": 40, "high": 2})
    out = idx.handler({"run_mode": "full", "model": "claude-haiku-4-5",
                       "issue_filter": "low-only", "schedule": "0 19 * * *",
                       "decide_only": True}, None)
    d = out["decide"]
    assert d["launches"] == []
    assert d["launched"] is False
    assert d["reason"] == "demand_gate_skip"
    assert idx._test_ssm.sent == []


def test_decide_only_demand_all_failure_shape(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(RuntimeError("github down")))
    out = idx.handler({**_demand_event(), "decide_only": True}, None)
    assert out["decide"]["launches"] == []
    assert out["decide"]["reason"] == "demand_all_failed"


def test_decide_only_respects_pace_gate_skip(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.5 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=1)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *",
                       "decide_only": True}, None)
    d = out["decide"]
    assert d["launches"] == []
    assert d["reason"] == "pace_gate_skip"
    assert idx._test_ssm.sent == []


def test_launch_decided_launches_exactly_the_given_decision(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    # A launch_decided event must NEVER touch demand-gate/fresh-stat enumeration
    # (the decision was already made by a prior decide_only call).
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(
                            AssertionError("launch_decided must not re-enumerate")))
    out = idx.handler({
        "run_mode": "full", "schedule": "0 1 * * *", "model": "claude-haiku-4-5",
        "issue_filter": "low-only", "partition_index": 2, "partition_count": 3,
        "launch_decided": True,
    }, None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["model"] == "claude-haiku-4-5" and g["issue_filter"] == "low-only"
    assert g["partition_index"] == 2 and g["partition_count"] == 3
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_SWEEP_PARTITION_INDEX=2" in cmd
    assert "export GROOM_SWEEP_PARTITION_COUNT=3" in cmd


def test_launch_decided_bypasses_pace_gate(monkeypatch):
    # A relaunch of an already-decided box must not be re-blocked by the
    # pre-boot pace gate — that gate is a per-TRIGGER decision, made once by
    # decide_only; re-checking it per-box would let a mid-run pace shift
    # cancel a box the trigger already committed to.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.9 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=1)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    out = idx.handler({
        "run_mode": "full", "schedule": "0 1 * * *", "model": "claude-opus-4-8",
        "issue_filter": "high-only", "launch_decided": True,
    }, None)
    assert out["groom"]["launched"] is True


def test_launch_decided_defaults_partition_when_absent(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "0 1 * * *",
                       "launch_decided": True}, None)
    g = out["groom"]
    assert g["partition_index"] == 0 and g["partition_count"] == 1
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "GROOM_SWEEP_PARTITION" not in cmd  # count<=1 -> no export at all


# ── config#2038: engagement lookback + disposition set must come from the lib ──


def test_load_recent_engagements_uses_lib_lookback_window(monkeypatch):
    """A run artifact just inside ge.ENGAGEMENT_LOOKBACK_DAYS ago must be
    picked up — this would be dropped by the old hardcoded range(3) whenever
    the lib's lookback is > 3 (config#2038's actual drift: 4 vs 3)."""
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    import nousergon_lib.groom_eligibility as ge

    now = datetime.now(ZoneInfo("UTC"))
    farthest = now - timedelta(days=ge.ENGAGEMENT_LOOKBACK_DAYS - 1)
    art = json.dumps({
        "run_start": farthest.isoformat().replace("+00:00", "Z"),
        "elapsed_min": 5,
        "issues": [{"repo": "nousergon/alpha-engine-config", "number": 999,
                    "disposition": "commented"}],
    }).encode()
    key = f"groom/{farthest.strftime('%Y-%m-%d')}/run1.json"
    idx = _load(monkeypatch, s3_objects={key: art})
    engagements = idx._load_recent_engagements()
    assert ("nousergon/alpha-engine-config", 999) in engagements


def test_load_recent_engagements_uses_lib_engaged_dispositions(monkeypatch):
    """A "labeled" disposition (config#1928/#1890 — label-only edits are the
    NORM for blocked dispositions) must count as engaged — this comes from
    ge.ENGAGED_DISPOSITIONS, not a local hardcoded tuple that could drift."""
    from datetime import datetime, timezone

    import nousergon_lib.groom_eligibility as ge

    assert "labeled" in ge.ENGAGED_DISPOSITIONS
    now = datetime.now(timezone.utc)
    art = json.dumps({
        "run_start": now.isoformat().replace("+00:00", "Z"),
        "elapsed_min": 5,
        "issues": [{"repo": "nousergon/alpha-engine-config", "number": 998,
                    "disposition": "labeled"}],
    }).encode()
    key = f"groom/{now.strftime('%Y-%m-%d')}/run1.json"
    idx = _load(monkeypatch, s3_objects={key: art})
    engagements = idx._load_recent_engagements()
    assert ("nousergon/alpha-engine-config", 998) in engagements


# ── config#2152: queue manifests (observer phase) ───────────────────────────────


def test_symmetric_trigger_writes_queue_manifests(monkeypatch):
    """Every launched box gets a manifest at the deterministic key carrying the
    exact issue list behind its launch decision — counts and queue derive from
    the same enumeration walk (config#2152 enumerate-once)."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event("0 1 * * *"), None)
    manifests = out["groom"]["queue_manifests"]
    assert set(manifests) == {"low-only", "mid-only", "high-only"}
    for filt, key in manifests.items():
        assert key.startswith("groom/queues/") and key.endswith(f"-{filt}.json")
        doc = json.loads(idx._test_s3._objects[key])
        assert doc["schema_version"] == 1
        assert doc["issue_filter"] == filt
        assert doc["issue_count"] == len(doc["issues"])
        assert all({"repo", "number", "title", "labels", "updated_at"} <= set(i)
                   for i in doc["issues"])
    # per-tier counts flow through to the per-filter manifests
    assert json.loads(idx._test_s3._objects[manifests["high-only"]])["issue_count"] == 10


def test_queue_manifest_write_failure_does_not_block_launch(monkeypatch):
    """Observer phase: a manifest write failure is logged (driver-side parity
    reports it) but the boxes still launch — grooms are the primary deliverable."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    def boom(**kw):
        raise RuntimeError("AccessDenied: s3:PutObject")
    monkeypatch.setattr(idx._test_s3, "put_object", boom)
    out = idx.handler(_demand_event(), None)
    assert out["groom"]["queue_manifests"] == {}
    assert len(out["groom"]["launches"]) == 3


def test_skipped_tier_gets_no_manifest(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 2, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    # low (2 < floor 8) rides upward or skips — only launched filters get manifests
    launched_filters = {l["issue_filter"] for l in out["groom"]["launches"]}
    assert set(out["groom"]["queue_manifests"]) == launched_filters


# ── config#2152/#2147: queue_manifest_key passthrough (drain / cutover opt-in) ──
# config#2175 gate/market split: a manifest run no longer needs (or wants)
# force_on_demand — the key's presence bypasses the demand-count gates on its
# own, and the box launches SPOT-FIRST like every other run.


def test_manifest_key_reaches_bootstrap_env(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "manual",
                       "model": "claude-haiku-4-5", "issue_filter": "low-only",
                       "queue_manifest_key": "groom/queues/drain/2026-07-10-low.json"}, None)
    assert out["groom"]["launched"]
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_QUEUE_MANIFEST_KEY=groom/queues/drain/2026-07-10-low.json" in cmd


def test_no_manifest_key_no_export(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "manual", "force_on_demand": True,
                       "model": "claude-haiku-4-5", "issue_filter": "low-only"}, None)
    assert out["groom"]["launched"]
    assert "GROOM_QUEUE_MANIFEST_KEY" not in idx._test_ssm.sent[0]["Parameters"]["commands"][0]


def test_malformed_manifest_key_fails_loud(monkeypatch):
    """The key lands on a root-shell command line — strict charset, fail loud."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    with pytest.raises(ValueError, match="invalid queue_manifest_key"):
        idx.handler({"run_mode": "full", "schedule": "manual",
                     "model": "claude-haiku-4-5", "issue_filter": "low-only",
                     "queue_manifest_key": "groom/x; rm -rf /"}, None)


# ── config#2175: manifest runs skip demand gates + launch spot-first ─────────


def test_manifest_key_skips_single_tier_demand_gate_and_launches_spot(monkeypatch):
    """A manifest run with NO force_on_demand must (a) never run the demand-
    count enumeration (the gate counts GitHub, meaningless for an explicit
    operator queue) and (b) launch SPOT-FIRST — the old behavior forced
    drains onto on-demand boxes purely to bypass the gate."""
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        return "i-drain"

    idx = _load(monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"})
    enumerated = []
    monkeypatch.setattr(idx, "_enumerate_tier_stats",
                        lambda token: enumerated.append(1) or ({}, {}, False))
    out = idx.handler({"run_mode": "full", "schedule": "manual",
                       "model": "claude-haiku-4-5", "issue_filter": "low-only",
                       "queue_manifest_key": "groom/queues/drain/2026-07-10-low.json"}, None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["market"] == "spot"
    assert seen == [True], "manifest run must try spot first, not force on-demand"
    assert enumerated == [], "manifest run must never run demand-gate enumeration"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_QUEUE_MANIFEST_KEY=groom/queues/drain/2026-07-10-low.json" in cmd


def test_manifest_key_skips_demand_all_block(monkeypatch):
    """queue_manifest_key + trigger=demand-all: the explicit queue wins — the
    demand-all fan-out (which would enumerate GitHub and launch 0..3 boxes on
    its own manifests) must be bypassed in favor of ONE box on the given key."""
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        return "i-drain"

    idx = _load(monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(
                            AssertionError("manifest run must not enumerate demand-all")))
    out = idx.handler({"run_mode": "full", "trigger": "demand-all", "schedule": "manual",
                       "model": "claude-sonnet-5", "issue_filter": "mid-only",
                       "queue_manifest_key": "groom/queues/drain/2026-07-10-mid.json"}, None)
    g = out["groom"]
    assert g["launched"] is True and g["market"] == "spot"
    assert seen == [True]
    assert len(idx._test_ssm.sent) == 1  # ONE box, not a demand-all fan-out


def test_manifest_key_still_honors_pace_gate(monkeypatch):
    """Deliberate (config#2175): the weekly WET pace gate applies to drains
    too — a manifest bypasses the demand-COUNT gates only, never the budget."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.5 * 1_140_000_000)})
    fixed_now = idx.WEEKLY_RESET_ANCHOR + idx.timedelta(days=1)
    monkeypatch.setattr(idx, "datetime", type("D", (), {
        "now": staticmethod(lambda tz=None: fixed_now)}))
    out = idx.handler({"run_mode": "full", "schedule": "manual",
                       "model": "claude-haiku-4-5", "issue_filter": "low-only",
                       "queue_manifest_key": "groom/queues/drain/2026-07-10-low.json"}, None)
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "pace_gate_skip"
    assert idx._test_ssm.sent == []


def test_scheduled_demand_all_without_manifest_key_still_enumerates(monkeypatch):
    """Scheduled (non-manifest) triggers are UNAFFECTED by the config#2175
    split — demand-all still enumerates and fans out per tier."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    assert len(out["groom"]["launches"]) == 3
