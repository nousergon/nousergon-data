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
import sys
import types
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Stub nousergon_lib.ec2_spot + boto3 before importing index ─────────────────
class _SpotLaunchError(Exception):
    pass


class _SpotCapacityExhausted(_SpotLaunchError):
    pass


class _SpotQuotaExceededError(_SpotLaunchError):
    """config#2698 — account-wide spot quota (e.g. MaxSpotInstanceCountExceeded),
    distinct from ordinary per-pool capacity exhaustion."""


class _GitHubAppTokenError(RuntimeError):
    """Mirrors nousergon_lib.github_app.GitHubAppTokenError (config-I2785)."""


def _install_stubs(launch_impl, boto_clients):
    # Real nousergon_lib.flow_doctor_fleet (FleetTelegramTopic enum) is installed
    # into TEST_DEPS by deploy.sh — do NOT hand-roll it here (config#1772).
    ec2_spot_mod = types.ModuleType("nousergon_lib.ec2_spot")
    ec2_spot_mod.SpotLaunchError = _SpotLaunchError
    ec2_spot_mod.SpotCapacityExhausted = _SpotCapacityExhausted
    ec2_spot_mod.SpotQuotaExceededError = _SpotQuotaExceededError
    ec2_spot_mod.launch = launch_impl
    sys.modules["nousergon_lib.ec2_spot"] = ec2_spot_mod

    fdt_mod = types.ModuleType("flow_doctor_telegram")
    fdt_mod.notify_via_flow_doctor = lambda *a, **k: True  # type: ignore[attr-defined]
    sys.modules["flow_doctor_telegram"] = fdt_mod

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: boto_clients[name]
    sys.modules["boto3"] = boto3_mod

    # config-I2785: deterministic App-token path. Default = mint failure, so
    # every pre-existing test keeps its exact prior _github_token behavior
    # (SSM PAT via _FakeSSM); the App-first ordering tests override
    # installation_token on this stub.
    ga_mod = types.ModuleType("nousergon_lib.github_app")
    ga_mod.GitHubAppTokenError = _GitHubAppTokenError  # type: ignore[attr-defined]

    def _default_mint(**kw):
        raise _GitHubAppTokenError("stubbed: no App creds in hermetic tests")

    ga_mod.installation_token = _default_mint  # type: ignore[attr-defined]
    sys.modules["nousergon_lib.github_app"] = ga_mod


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

    def get_parameter(self, Name, WithDecryption=False):  # noqa: N803 — boto3 API
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
    """Fake S3 client for the dispatcher's boto3-native S3 reads/writes.

    ``objects`` maps a full S3 key to its raw JSON bytes content (usage docs,
    queue manifests, decision records).
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
    # The high-only (01:00 UTC / 6pm PT) schedule's event carries model +
    # issue_filter — these must reach the box as exported env vars ahead of
    # the bootstrap exec.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {"run_mode": "full", "model": "claude-sonnet-5", "issue_filter": "high-only",
         "schedule": "0 1 * * *"},
        None,
    )
    g = out["groom"]
    assert g["model"] == "claude-sonnet-5"
    assert g["issue_filter"] == "high-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=claude-sonnet-5" in cmd
    assert "export GROOM_ISSUE_FILTER=high-only" in cmd
    assert cmd.index("export GROOM_MODEL") < cmd.index("groom_spot_bootstrap.sh")


def test_high_only_schedule_forwards_pr_budget(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {
            "run_mode": "full",
            "model": "claude-sonnet-5",
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
    # Haiku/Sonnet-mid schedules must NOT inherit the high-only override.
    low_line = next(
        line for line in deploy_sh.splitlines()
        if "low-only" in line and "SCHED_INPUTS" not in line
    )
    assert "pr_budget" not in low_line


# ── Usage pacing dismantled (Brian ruling 2026-07-14) ───────────────────────
# The pre-boot pace gate, its SSoT ceiling wiring (config-I2461), the SSM
# operator override, and the Anthropic-only WET reader were all REMOVED with
# the rest of usage pacing. These tests pin the dismantle: no amount of
# recorded usage may block or defer a scheduled launch anymore, and the env
# kill-switch is inert (the gate it disabled no longer exists).
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


def test_heavy_recorded_usage_never_blocks_launch(monkeypatch):
    # Pre-dismantle this fixture (50% of the old ceiling consumed 1 day into
    # the window) skipped the launch with reason=pace_gate_skip. Now: launch.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.5 * 1_140_000_000)})
    notified = _spy_notify(monkeypatch, idx)
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["launched"] is True
    assert len(idx._test_ssm.sent) == 1
    assert notified == []  # nothing skipped -> nothing to ping


def test_legacy_pace_gate_env_flag_is_inert(monkeypatch):
    # A stale GROOM_PACE_GATE_ENABLED=true in the live Lambda env (deploys
    # don't prune unknown env vars) must have zero effect.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                   "GROOM_PACE_GATE_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.99 * 1_140_000_000)})
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["launched"] is True


def test_demand_all_failure_writes_skip_decision_record(monkeypatch):
    # config-I2540: an enumeration failure must leave a decision record with
    # skip_reason=demand_all_failed (empty decisions list), so a MISSING
    # record file unambiguously means "the scheduler never invoked the
    # Lambda" — the 2026-07-14 incident class where the console could not
    # distinguish an outage from a quiet early exit.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(RuntimeError("github down")))
    out = idx.handler(_demand_event(), None)
    assert out["groom"]["reason"] == "demand_all_failed"
    records = {k: v for k, v in idx._test_s3._objects.items()
               if k.startswith("groom/decisions/")}
    assert len(records) == 1, f"exactly one skip record expected, got {list(records)}"
    doc = json.loads(list(records.values())[0])
    assert doc["skip_reason"] == "demand_all_failed"
    assert doc["decisions"] == []
    assert "github down" in doc["error"]
    assert doc["schema_version"] == 2


def test_demand_all_failure_skip_record_write_error_never_masks_the_skip(monkeypatch):
    # The record write is best-effort: an S3 failure there must not turn the
    # (already-notified) skip into a crash — no-silent-swallows carve-out,
    # recorded via the CW warning log.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(RuntimeError("github down")))
    def _boom(**kw):
        raise RuntimeError("S3 down too")
    monkeypatch.setattr(idx._test_s3, "put_object", _boom)
    out = idx.handler(_demand_event(), None)
    assert out["groom"]["reason"] == "demand_all_failed"


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
    # correctness gate (mirrors the demand gate fail-safe posture).
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
    # high slot, no high issues, starving low+mid -> ONE Sonnet run.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 5, "mid": 6, "high": 0})
    out = idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
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
    assert launched == {("high-only", "claude-sonnet-5"),
                        ("mid-only", "claude-sonnet-5"),
                        ("low-only", "claude-haiku-4-5")}
    cmds = [c["Parameters"]["commands"][0] for c in idx._test_ssm.sent]
    assert len(cmds) == 3
    # config#2201: groom boxes are pure issue-coverage workers — the
    # config#2129 per-box sweep-partition exports are retired (the dispatch
    # SF's end-of-SF run_mode=sweep box owns ALL PR sweeping now).
    for c in cmds:
        assert "GROOM_NO_SWEEP" not in c
        assert "GROOM_SWEEP_PARTITION" not in c


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
    # config#2201: decide entries carry no partition fields any more — the
    # end-of-SF sweep box replaced per-box partitioned sweeps.
    for e in d["launches"]:
        assert "partition_index" not in e and "partition_count" not in e
    assert idx._test_ssm.sent == []


def test_decide_only_single_tier_returns_one_launch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
                       "issue_filter": "mid-only", "schedule": "0 7 * * *",
                       "decide_only": True}, None)
    d = out["decide"]
    assert d["launches"] == [{"model": "claude-sonnet-5", "issue_filter": "mid-only"}]
    assert idx._test_ssm.sent == []


def test_decide_only_ungated_direct_dispatch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "model": "claude-haiku-4-5",
                       "issue_filter": "gated-reverify", "decide_only": True}, None)
    assert out["decide"]["launches"] == [{"model": "claude-haiku-4-5",
                                          "issue_filter": "gated-reverify"}]
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


def test_launch_decided_launches_exactly_the_given_decision(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    # A launch_decided event must NEVER touch demand-gate/fresh-stat enumeration
    # (the decision was already made by a prior decide_only call).
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(
                            AssertionError("launch_decided must not re-enumerate")))
    out = idx.handler({
        "run_mode": "full", "schedule": "0 1 * * *", "model": "claude-haiku-4-5",
        "issue_filter": "low-only",
        "launch_decided": True,
    }, None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["model"] == "claude-haiku-4-5" and g["issue_filter"] == "low-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=claude-haiku-4-5" in cmd
    assert "export GROOM_ISSUE_FILTER=low-only" in cmd


def test_launch_decided_launches_regardless_of_recorded_usage(monkeypatch):
    # A relaunch of an already-decided box launches unconditionally — no
    # usage-derived gate applies (the pace gate that once could re-block this
    # path pre-config#2129 was dismantled 2026-07-14).
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.9 * 1_140_000_000)})
    out = idx.handler({
        "run_mode": "full", "schedule": "0 1 * * *", "model": "claude-sonnet-5",
        "issue_filter": "high-only", "launch_decided": True,
    }, None)
    assert out["groom"]["launched"] is True


# ── config#2201: end-of-SF sweep box (run_mode=sweep + launch_decided) ───────
# The dispatch SF's final DispatchEndOfSfSweep state fires this exact event
# after the groom Map winds down (and on the zero-launches path): ONE Haiku
# sweep box per trigger cycle, guarded on its own distinct 'sweep' lane tag.


_SWEEP_SF_EVENT = {
    "run_mode": "sweep", "launch_decided": True, "model": "claude-haiku-4-5",
    "issue_filter": "mid-only", "schedule": "end-of-sf-sweep",
}


def test_sweep_launch_decided_launches_haiku_sweep_box(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    # Unconditional by design: no demand enumeration on the sweep path.
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(
                            AssertionError("sweep launch_decided must not enumerate")))
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["run_mode"] == "sweep"
    assert g["model"] == "claude-haiku-4-5"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "groom_spot_bootstrap.sh --mode sweep" in cmd
    assert "export GROOM_MODEL=claude-haiku-4-5" in cmd


def test_sweep_box_tagged_with_distinct_sweep_lane(monkeypatch):
    # The concurrent guard keys on tag groom-issue-filter — a sweep box tagged
    # with its (inert) issue_filter verbatim would collide with the mid-only
    # GROOM box's tag. Sweep boxes get the distinct 'sweep' tag value instead;
    # the event's issue_filter still passes the lib filter validation.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    assert out["groom"]["tier_tag"] == "sweep"
    assert out["groom"]["issue_filter"] == "mid-only"
    assert idx._test_ec2.tags_created == [
        (["i-stub"], [{"Key": "groom-issue-filter", "Value": "sweep"}])
    ]


def test_sweep_launch_skipped_when_sweep_box_already_live(monkeypatch):
    launched = []
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: launched.append(1) or "i-new",  # noqa: E731
        env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"sweep": ["i-live-sweep"]},
    )
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    g = out["groom"]
    assert g["launched"] is False
    assert g["reason"] == "concurrent_tier_skip"
    assert g["existing_instance_ids"] == ["i-live-sweep"]
    assert launched == []  # zero spend — the live sweep box owns this cycle


def test_sweep_launch_not_blocked_by_live_mid_only_groom_box(monkeypatch):
    # The exact collision the distinct tag exists to prevent: a live mid-only
    # GROOM box (routine — Sonnet runs can go hours) must never starve the
    # end-of-SF sweep.
    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"mid-only": ["i-live-mid-groom"]},
    )
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    assert out["groom"]["launched"] is True
    assert out["groom"]["tier_tag"] == "sweep"


def test_live_sweep_box_does_not_block_mid_only_groom_launch(monkeypatch):
    # Symmetric half: a still-running sweep box must not block the next
    # mid-only groom launch (their queues are disjoint: open PRs vs issues).
    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"sweep": ["i-live-sweep"]},
    )
    out = idx.handler({"run_mode": "full", "schedule": "0 7 * * *",
                       "model": "claude-sonnet-5", "issue_filter": "mid-only",
                       "launch_decided": True}, None)
    assert out["groom"]["launched"] is True
    assert out["groom"]["tier_tag"] == "mid-only"


def test_sweep_launch_failure_raises_for_sf_catch(monkeypatch):
    # The SF's DispatchEndOfSfSweep state converts this raise into a recorded,
    # non-fatal skip (Catch → RecordSweepDispatchFailure) — the Lambda itself
    # stays fail-loud so direct invokes/EventBridge retries also see the miss.
    def _boom(types_, subnets, **kw):
        raise _SpotLaunchError("RunInstances denied")

    idx = _load(monkeypatch, launch_impl=_boom, env={"GROOM_DISPATCH_ENABLED": "true"})
    with pytest.raises(_SpotLaunchError, match="RunInstances denied"):
        idx.handler(dict(_SWEEP_SF_EVENT), None)


# ── config#2667: launch_decided (sweep) dispatches now write a decision ─────
# record too — previously ONLY the demand-all path did, leaving the
# dispatch-decision log (groom/decisions/{date}/*.json, the ground truth
# the overseer-liveness-probe run_window check reads to detect a
# silently-missing run artifact) structurally blind to sweep-mode dispatches.


def test_sweep_launch_writes_decision_record(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler(dict(_SWEEP_SF_EVENT), None)
    records = {k: v for k, v in idx._test_s3._objects.items()
               if k.startswith("groom/decisions/") and "/sweep-" in k}
    assert len(records) == 1, f"exactly one sweep decision record expected, got {list(idx._test_s3._objects)}"
    doc = json.loads(list(records.values())[0])
    assert doc["schema_version"] == 2
    assert doc["run_mode"] == "sweep"
    assert doc["trigger"] == "launch_decided"
    assert doc["decisions"] == [{
        "launch": True, "issue_filter": "mid-only", "model": "claude-haiku-4-5",
        "reason": "launch_decided", "tier_tag": "sweep",
    }]
    assert "decided_at" in doc


def test_sweep_skip_launch_writes_decision_record_with_launch_false(monkeypatch):
    # The concurrent-lane skip path (a prior cycle's sweep box still live) is
    # itself a launch_decided invocation that must ALSO leave a record — with
    # launch=false, so the liveness probe correctly does NOT expect an
    # artifact for it (see overseer-liveness-probe's run_window
    # _rw_decision_launched).
    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"sweep": ["i-live-sweep"]},
    )
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    assert out["groom"]["launched"] is False
    records = {k: v for k, v in idx._test_s3._objects.items()
               if k.startswith("groom/decisions/") and "/sweep-" in k}
    assert len(records) == 1
    doc = json.loads(list(records.values())[0])
    assert doc["decisions"][0]["launch"] is False
    assert doc["decisions"][0]["reason"] == "concurrent_tier_skip"


def test_sweep_decision_record_write_failure_never_blocks_dispatch(monkeypatch):
    # Best-effort, mirrors _write_trigger_record/_write_skip_record: a record
    # -write failure must never turn an already-successful sweep launch into
    # a crash.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})

    def _boom(**kw):
        raise RuntimeError("S3 down")

    monkeypatch.setattr(idx._test_s3, "put_object", _boom)
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    assert out["groom"]["launched"] is True


def test_full_mode_launch_decided_also_writes_sweep_style_decision_record(monkeypatch):
    # The launch_decided fast-path is shared by sweep AND any other
    # pre-decided relaunch (e.g. the SF's bounded-relaunch loop for a
    # full-mode box) — the record write applies uniformly to the whole
    # launch_decided branch, not just run_mode=sweep.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler({
        "run_mode": "full", "schedule": "0 1 * * *", "model": "claude-sonnet-5",
        "issue_filter": "high-only", "launch_decided": True,
    }, None)
    records = {k: v for k, v in idx._test_s3._objects.items()
               if k.startswith("groom/decisions/") and "/sweep-" in k}
    assert len(records) == 1
    doc = json.loads(list(records.values())[0])
    assert doc["run_mode"] == "full"
    assert doc["decisions"][0]["launch"] is True


def test_launch_decided_never_exports_partition_envs(monkeypatch):
    # config#2201: the config#2129 partition machinery is fully retired — a
    # stale caller still sending partition fields must not resurrect the
    # exports (they're simply ignored), and no launch path emits them.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "0 1 * * *",
                       "partition_index": 2, "partition_count": 3,
                       "launch_decided": True}, None)
    assert out["groom"]["launched"] is True
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "GROOM_SWEEP_PARTITION" not in cmd
    assert "GROOM_NO_SWEEP" not in cmd


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


def test_manifest_key_launches_regardless_of_recorded_usage(monkeypatch):
    """Post-dismantle (2026-07-14): a manifest drain launches even with heavy
    recorded usage — the weekly WET pace gate that used to apply to drains
    (config#2175) is gone with the rest of usage pacing."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                s3_objects={"claude_code_usage/groom/2026-07-13.json":
                            _wet_doc(0.5 * 1_140_000_000)})
    out = idx.handler({"run_mode": "full", "schedule": "manual",
                       "model": "claude-haiku-4-5", "issue_filter": "low-only",
                       "queue_manifest_key": "groom/queues/drain/2026-07-10-low.json"}, None)
    assert out["groom"]["launched"] is True
    assert len(idx._test_ssm.sent) == 1


def test_scheduled_demand_all_without_manifest_key_still_enumerates(monkeypatch):
    """Scheduled (non-manifest) triggers are UNAFFECTED by the config#2175
    split — demand-all still enumerates and fans out per tier."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    assert len(out["groom"]["launches"]) == 3


# ── _github_token auth ordering (config-I2785) ──────────────────────────────
# App installation token first, PAT fallback — the 2026-07-16 GitHub outage
# (config-I2784) 503'd user-token REST while App tokens were unaffected; the
# ordering under test is that incident's structural fix.


def test_github_token_prefers_app_installation_token(monkeypatch):
    idx = _load(monkeypatch)
    ga = sys.modules["nousergon_lib.github_app"]
    monkeypatch.setattr(ga, "installation_token", lambda **kw: "ghs_app")
    # No PAT parameter seeded — proves the SSM PAT path is never consulted
    # while the App path serves.
    assert idx._github_token() == "ghs_app"


def test_github_token_falls_back_to_pat_on_mint_failure(monkeypatch):
    # The stub's default installation_token raises GitHubAppTokenError.
    idx = _load(monkeypatch, ssm_parameters={
        "/alpha-engine/saturday_sf_watch/github_pat": "pat_value",
    })
    assert idx._github_token() == "pat_value"


# ── config#3173: mechanical per-day dispatch ceiling ────────────────────────


def _ledger_objects_for(count: int, date: str) -> dict:
    return {
        f"groom/_control/dispatch-ledger/{date}/tok-{i}.json": b"{}"
        for i in range(count)
    }


def test_dispatch_ceiling_exhausted_suppresses_launch_zero_spend(monkeypatch):
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-new"

    idx = _load(
        monkeypatch, launch_impl=_launch,
        env={"GROOM_DISPATCH_ENABLED": "true", "GROOM_MAX_DISPATCHES_DAILY": "5"},
    )
    monkeypatch.setattr(idx, "_prior_launch_count_today", lambda: 5)
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    g = out["groom"]
    assert g["launched"] is False
    assert g["reason"] == "dispatch_ceiling_exhausted"
    assert g["prior_dispatch_count"] == 5
    assert g["dispatch_ceiling"] == 5
    assert launched == []  # never even attempted a spot launch — zero spend


def test_dispatch_under_ceiling_launches_and_records_ledger_entry(monkeypatch):
    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true", "GROOM_MAX_DISPATCHES_DAILY": "5"},
    )
    monkeypatch.setattr(idx, "_prior_launch_count_today", lambda: 4)
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert out["groom"]["launched"] is True
    ledger = {k: v for k, v in idx._test_s3._objects.items()
              if k.startswith("groom/_control/dispatch-ledger/")}
    assert len(ledger) == 1, f"expected exactly one ledger entry, got {list(ledger)}"
    doc = json.loads(list(ledger.values())[0])
    assert doc["run_token"] == out["groom"]["run_token"]
    assert doc["tier_tag"]


def test_prior_launch_count_today_ignores_other_dates(monkeypatch):
    # Seed only a DIFFERENT date's ledger keys — none should count toward
    # "today" (a real UTC date the test doesn't control), so the real
    # implementation must read 0 regardless of what date it runs on.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
               s3_objects=_ledger_objects_for(9, "2000-01-01"))
    assert idx._prior_launch_count_today() == 0


def test_dispatch_ledger_write_failure_never_blocks_the_launch(monkeypatch):
    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true", "GROOM_MAX_DISPATCHES_DAILY": "5"},
    )
    monkeypatch.setattr(idx, "_prior_launch_count_today", lambda: 0)

    def _boom(**kw):
        raise RuntimeError("S3 down")

    monkeypatch.setattr(idx._test_s3, "put_object", _boom)
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert out["groom"]["launched"] is True


def test_dispatch_ceiling_checked_after_concurrent_tier_skip(monkeypatch):
    # A concurrent-lane skip must short-circuit BEFORE the ceiling count read
    # — no reason to spend an S3 list call when the launch was already going
    # to be skipped for an unrelated reason.
    calls = {"count": 0}

    def _count():
        calls["count"] += 1
        return 0

    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"mid-only": ["i-already-running"]},
    )
    monkeypatch.setattr(idx, "_prior_launch_count_today", _count)
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert out["groom"]["reason"] == "concurrent_tier_skip"
    assert calls["count"] == 0


# ── config-I3227: org-wide ruling:pending-exec PR demand ─────────────────────
# Ruled PRs (config-I3199 — a binding operator ruling awaiting execution)
# previously contributed ZERO demand to this Lambda's pre-boot per-tier
# counts: only issues were ever enumerated, so a backlog of ruled PRs alone
# could never clear a tier's floor or trip the anti-starvation escape valve.
# These tests cover: the new org-wide search-primary/per-repo-fallback PR
# enumeration itself, its wiring into both _enumerate_tier_stats and
# _enumerate_tier_stats_fresh (counts/oldest/p0), and the acceptance-
# criteria synthetic case from alpha-engine-config-I3227 — N ruled PRs with
# ZERO issues still produce a launch decision once N >= floor or the escape
# valve fires.


class _FakeHTTPResponse:
    """Minimal stand-in for the ``with urllib.request.urlopen(...) as resp``
    context manager index.py's REST helpers all use."""

    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _search_pr_item(repo, number, labels, updated_at="2026-07-20T00:00:00Z",
                    title="ruled PR"):
    """A GitHub /search/issues result item shape (used by both the search
    endpoint and, identically, the /repos/{repo}/issues fallback endpoint —
    both are issues-API-shaped, PRs included via the ``pull_request`` key)."""
    return {
        "number": number, "title": title,
        "labels": [{"name": lbl} for lbl in labels],
        "updated_at": updated_at,
        "repository_url": f"https://api.github.com/repos/{repo}",
        "pull_request": {"url": "https://api.github.com/dummy"},
    }


def _recent_iso() -> str:
    """A timestamp well inside DEFAULT_MAX_WAIT_HOURS — never trips the
    escape valve on its own."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def test_enumerate_ruling_pending_prs_search_primary(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    items = [_search_pr_item("nousergon/nousergon-data", 950,
                             ["ruling:pending-exec", "complexity:mid"])]
    calls = []

    def _fake_urlopen(req, timeout=30):
        calls.append(req.full_url)
        assert "/search/issues?q=" in req.full_url
        assert "org%3Anousergon" in req.full_url
        assert "is%3Apr" in req.full_url
        assert "ruling%3Apending-exec" in req.full_url
        return _FakeHTTPResponse(json.dumps({"items": items}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)
    out = idx._enumerate_ruling_pending_prs("tok")
    assert len(out) == 1
    assert out[0]["repo"] == "nousergon/nousergon-data"
    assert out[0]["number"] == 950
    assert len(calls) == 1, "no per-repo fallback calls when search succeeds"


def test_enumerate_ruling_pending_prs_falls_back_on_search_failure(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    search_calls = []

    def _fake_urlopen(req, timeout=30):
        url = req.full_url
        if "/search/issues" in url:
            search_calls.append(url)
            raise urllib.error.URLError("search API down")
        if url == "https://api.github.com/orgs/nousergon/repos?per_page=100&page=1":
            return _FakeHTTPResponse(json.dumps([
                {"full_name": "nousergon/alpha-engine-config"},
                {"full_name": "nousergon/nousergon-data"},
            ]).encode())
        if url.startswith("https://api.github.com/repos/nousergon/nousergon-data/issues"):
            return _FakeHTTPResponse(json.dumps([
                _search_pr_item("nousergon/nousergon-data", 42, ["ruling:pending-exec"]),
            ]).encode())
        if url.startswith("https://api.github.com/repos/nousergon/alpha-engine-config/issues"):
            return _FakeHTTPResponse(b"[]")
        raise AssertionError(f"unexpected urlopen call in this test: {url}")

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)
    out = idx._enumerate_ruling_pending_prs("tok")
    assert len(search_calls) == 1, "search must be tried exactly once before falling back"
    assert len(out) == 1
    assert out[0]["repo"] == "nousergon/nousergon-data"
    assert out[0]["number"] == 42


def test_enumerate_ruling_pending_prs_per_repo_failure_does_not_blank_the_rest(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})

    def _fake_urlopen(req, timeout=30):
        url = req.full_url
        if "/search/issues" in url:
            raise urllib.error.URLError("search API down")
        if url == "https://api.github.com/orgs/nousergon/repos?per_page=100&page=1":
            return _FakeHTTPResponse(json.dumps([
                {"full_name": "nousergon/broken-repo"},
                {"full_name": "nousergon/nousergon-data"},
            ]).encode())
        if url.startswith("https://api.github.com/repos/nousergon/broken-repo/issues"):
            raise urllib.error.URLError("this repo 404s")
        if url.startswith("https://api.github.com/repos/nousergon/nousergon-data/issues"):
            return _FakeHTTPResponse(json.dumps([
                _search_pr_item("nousergon/nousergon-data", 7, ["ruling:pending-exec"]),
            ]).encode())
        raise AssertionError(f"unexpected urlopen call in this test: {url}")

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)
    out = idx._enumerate_ruling_pending_prs("tok")
    assert len(out) == 1
    assert out[0]["number"] == 7


def _fake_urlopen_empty_issue_pages(req, timeout=30):
    """Every /repos/{repo}/issues?state=open (no labels= filter) call —
    i.e. the plain BACKLOG_REPOS issue walk — returns an empty page, so a
    test using this can isolate itself to the ruling:pending-exec PR path
    (stubbed separately via idx._enumerate_ruling_pending_prs)."""
    url = req.full_url
    assert "/issues?state=open&per_page=" in url and "labels=" not in url, (
        f"unexpected urlopen call — only the plain issue walk should run: {url}")
    return _FakeHTTPResponse(b"[]")


def test_enumerate_tier_stats_fresh_folds_in_ruling_pending_prs(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen_empty_issue_pages)
    fake_prs = [
        _search_pr_item("nousergon/nousergon-data", 900 + i,
                        ["ruling:pending-exec", "complexity:mid"], _recent_iso())
        for i in range(9)
    ]
    for pr in fake_prs:
        pr["repo"] = "nousergon/nousergon-data"  # normally stamped by _enumerate_ruling_pending_prs
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: fake_prs)
    counts, oldest, p0_tiers, tier_issues = idx._enumerate_tier_stats_fresh("tok")
    assert counts == {"low": 0, "mid": 9, "high": 0}
    assert p0_tiers == []
    assert len(tier_issues["mid"]) == 9
    assert all(it["title"].startswith("[PR] ") for it in tier_issues["mid"])
    assert {it["number"] for it in tier_issues["mid"]} == {900 + i for i in range(9)}


def test_enumerate_tier_stats_fresh_ruling_pending_pr_p0_sets_escape_valve_flag(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen_empty_issue_pages)
    fake_pr = _search_pr_item("nousergon/nousergon-data", 901,
                              ["ruling:pending-exec", "complexity:high", "P0"], _recent_iso())
    fake_pr["repo"] = "nousergon/nousergon-data"
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: [fake_pr])
    counts, oldest, p0_tiers, tier_issues = idx._enumerate_tier_stats_fresh("tok")
    assert counts["high"] == 1
    assert p0_tiers == ["high"]


def test_enumerate_tier_stats_folds_in_ruling_pending_prs(monkeypatch):
    # Legacy single-slot enumeration (_demand_decision's path) gets the same
    # fold-in, for parity with the fresh-stat path used by the live
    # demand-all schedules.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen_empty_issue_pages)
    fake_pr = _search_pr_item("nousergon/nousergon-data", 902,
                              ["ruling:pending-exec", "complexity:low"], _recent_iso())
    fake_pr["repo"] = "nousergon/nousergon-data"
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: [fake_pr])
    counts, oldest, has_p0 = idx._enumerate_tier_stats("tok")
    assert counts == {"low": 1, "mid": 0, "high": 0}
    assert has_p0 is False


# ── acceptance criteria (alpha-engine-config-I3227): N ruling:pending-exec ───
# PRs with ZERO issues still produce a launch decision once N >= floor or the
# anti-starvation escape valve fires — exercised end-to-end through handler().


def test_demand_all_launches_from_ruling_pending_prs_alone_at_floor(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen_empty_issue_pages)
    fake_prs = []
    for i in range(idx.ge.DEFAULT_FLOOR):  # exactly at the floor, zero issues
        pr = _search_pr_item("nousergon/nousergon-data", 910 + i,
                             ["ruling:pending-exec", "complexity:mid"], _recent_iso())
        pr["repo"] = "nousergon/nousergon-data"
        fake_prs.append(pr)
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: fake_prs)
    out = idx.handler(_demand_event(), None)
    launched_filters = {l["issue_filter"] for l in out["groom"]["launches"]}
    assert "mid-only" in launched_filters, out["groom"]
    assert idx._test_ssm.sent, "a real spot box must have been dispatched"


def test_demand_all_ruling_pending_prs_below_floor_fresh_no_launch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen_empty_issue_pages)
    pr = _search_pr_item("nousergon/nousergon-data", 920,
                         ["ruling:pending-exec", "complexity:mid"], _recent_iso())
    pr["repo"] = "nousergon/nousergon-data"
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: [pr])
    out = idx.handler(_demand_event(), None)
    assert out["groom"]["launches"] == []
    assert not idx._test_ssm.sent


def test_demand_all_stale_ruling_pending_pr_fires_escape_valve(monkeypatch):
    # A single ruled PR, zero issues, well below the floor — but its
    # updated_at is far past DEFAULT_MAX_WAIT_HOURS (72h), so it must fire
    # the anti-starvation escape valve exactly like an overdue actionable
    # issue would (config-I3227 acceptance criteria).
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen_empty_issue_pages)
    pr = _search_pr_item("nousergon/nousergon-data", 921,
                         ["ruling:pending-exec", "complexity:mid"],
                         "2020-01-01T00:00:00Z")
    pr["repo"] = "nousergon/nousergon-data"
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: [pr])
    out = idx.handler(_demand_event(), None)
    launched_filters = {l["issue_filter"] for l in out["groom"]["launches"]}
    assert "mid-only" in launched_filters, out["groom"]
    launch = next(l for l in out["groom"]["launches"] if l["issue_filter"] == "mid-only")
    assert idx._test_ssm.sent


# ── Quota-fallback direct invoke (3-repo feature): {"mode": "fallback",
# "tier": "<low|mid|high>"} fired by lambda:InvokeFunction (never
# EventBridge) when an on-box groom run winds down on mid-run Claude-quota
# exhaustion (alpha-engine-config groom_driver.py, config#1803 classifier).
# Purely additive: no live SCHED_INPUTS event ever carries a top-level
# "mode" key, so this must never fire on — or change the behavior of — any
# existing invocation shape.

@pytest.mark.parametrize("tier,expected_filter", [
    ("low", "low-only"), ("mid", "mid-only"), ("high", "high-only"),
])
def test_fallback_dispatch_launches_one_box_per_tier_with_deepseek_backend(
        monkeypatch, tier, expected_filter):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    # The fallback path must SKIP demand-eligibility enumeration entirely —
    # it is a rescue for already-in-flight work, not a fresh demand decision.
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: (_ for _ in ()).throw(
                            AssertionError("fallback dispatch must not re-enumerate demand")))
    monkeypatch.setattr(idx, "_demand_decision",
                        lambda *a, **k: (_ for _ in ()).throw(
                            AssertionError("fallback dispatch must not run the demand gate")))
    out = idx.handler({"mode": "fallback", "tier": tier}, None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["issue_filter"] == expected_filter
    assert g["backend"] == "deepseek"
    assert len(idx._test_ssm.sent) == 1
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_BACKEND=deepseek" in cmd
    assert f"export GROOM_ISSUE_FILTER={expected_filter}" in cmd
    assert cmd.index("export GROOM_BACKEND") < cmd.index("groom_spot_bootstrap.sh")


def test_fallback_dispatch_writes_own_decision_record(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler({"mode": "fallback", "tier": "mid", "schedule": "quota-rescue"}, None)
    records = {k: v for k, v in idx._test_s3._objects.items()
               if k.startswith("groom/decisions/") and "/fallback-" in k}
    assert len(records) == 1, f"exactly one fallback decision record expected, got {list(idx._test_s3._objects)}"
    doc = json.loads(list(records.values())[0])
    assert doc["schema_version"] == 2
    assert doc["trigger"] == "quota_fallback"
    assert doc["tier"] == "mid"
    assert doc["backend"] == "deepseek"
    assert doc["schedule"] == "quota-rescue"
    assert doc["decisions"][0]["launch"] is True
    assert doc["decisions"][0]["issue_filter"] == "mid-only"


def test_fallback_dispatch_reuses_launch_chokepoint_concurrency_guard(monkeypatch):
    # Reuses _launch_groom_spot verbatim — the SAME concurrency guard every
    # other dispatch path shares must also apply here (no duplicated launch
    # logic that could silently diverge on this new path).
    launched = []
    idx = _load(
        monkeypatch,
        launch_impl=lambda types_, subnets, **kw: launched.append(1) or "i-new",  # noqa: E731
        env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"high-only": ["i-live-high"]},
    )
    out = idx.handler({"mode": "fallback", "tier": "high"}, None)
    g = out["groom"]
    assert g["launched"] is False
    assert g["reason"] == "concurrent_tier_skip"
    assert launched == []


@pytest.mark.parametrize("bad_tier", [None, "", "bogus", "sweep", "low-only"])
def test_fallback_dispatch_invalid_tier_raises(monkeypatch, bad_tier):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    event = {"mode": "fallback"}
    if bad_tier is not None:
        event["tier"] = bad_tier
    with pytest.raises(ValueError):
        idx.handler(event, None)
    assert not idx._test_ssm.sent  # fail loud BEFORE any spend, never a partial launch


def test_fallback_dispatch_tier_is_case_insensitive(monkeypatch):
    # Mirrors every other event-key resolver in this handler
    # (_resolve_run_mode/_resolve_issue_filter/_resolve_model all .lower()) —
    # a case-insensitive tier is intentional, not an oversight.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"mode": "fallback", "tier": "HIGH"}, None)
    assert out["groom"]["launched"] is True
    assert out["groom"]["issue_filter"] == "high-only"


def test_fallback_mode_key_never_set_by_any_live_schedule_input():
    # The discriminator (event.get("mode") == "fallback") is safe only if no
    # live EventBridge Scheduler input ever carries a top-level "mode" key —
    # verify that invariant directly against deploy.sh's SCHED_INPUTS (the
    # single source of truth for what the 3 live cron triggers actually send).
    deploy_sh = (Path(__file__).resolve().parent / "deploy.sh").read_text()
    sched_inputs_block = deploy_sh.split("SCHED_INPUTS=(", 1)[1].split(")", 1)[0]
    assert '"mode"' not in sched_inputs_block


def test_normal_cron_event_unaffected_by_fallback_branch(monkeypatch):
    # A real EventBridge-Scheduler-driven event (no "mode" key at all) must
    # behave EXACTLY as before — the new branch is purely additive and must
    # never intercept it, even if some other key happens to collide in future.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 0, "mid": 9, "high": 0})
    out = idx.handler(_demand_event(), None)
    assert "groom" in out
    assert out["groom"]["trigger"] == "demand-all"
    assert idx._test_ssm.sent
    for cmd in (s["Parameters"]["commands"][0] for s in idx._test_ssm.sent):
        assert "GROOM_BACKEND" not in cmd


def test_legacy_direct_invoke_event_unaffected_by_fallback_branch(monkeypatch):
    # Legacy direct invoke (no decide_only/launch_decided/mode key at all) —
    # the pre-existing "decide AND launch in one call" shape — must also be
    # completely untouched.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only",
                       "model": "claude-sonnet-5"}, None)
    assert out["groom"]["launched"] is True
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "GROOM_BACKEND" not in cmd


# ── alpha-engine-config-I3479: PRIMARY-mode DeepSeek backend selection for
# SCHEDULED low/mid tier launches (GROOM_PRIMARY_DEEPSEEK_TIERS, ships
# UNARMED). Distinct from the quota-FALLBACK leg above (_handle_fallback_
# dispatch, which always threads GROOM_BACKEND_DEEPSEEK regardless of this
# env var — a rescue for an already-in-flight Claude run, not a pre-planned
# routing decision) — this governs the demand-all / single-tier-demand-gate /
# decide_only / launch_decided SCHEDULED-dispatch paths only. Sweep-mode and
# mode=fallback are UNCHANGED.


def test_primary_backend_for_armed_selects_deepseek_when_every_tier_qualifies(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    assert idx._primary_backend_for(("low",)) == "deepseek"
    assert idx._primary_backend_for(("mid",)) == "deepseek"
    assert idx._primary_backend_for(("mid", "low")) == "deepseek"


def test_primary_backend_for_armed_any_high_in_bundle_blocks_it(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    assert idx._primary_backend_for(("high",)) is None
    # A high+mid attach-upward bundle: mid alone WOULD qualify, but any-high
    # in the bundle blocks the WHOLE box (one box, one provider).
    assert idx._primary_backend_for(("mid", "high")) is None


def test_primary_backend_for_empty_tiers_is_none(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    assert idx._primary_backend_for(()) is None


def test_primary_backend_for_unarmed_env_always_none(monkeypatch):
    # Default (no env var at all) — the SHIPPED state.
    idx = _load(monkeypatch, env={})
    assert idx._primary_backend_for(("low",)) is None
    assert idx._primary_backend_for(("low", "mid")) is None


def test_primary_backend_env_unset_omits_backend_on_demand_all(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    for e in out["groom"]["launches"]:
        assert "backend" not in e
    for d in out["groom"]["decisions"]:
        assert "backend" not in d
    for cmd in (s["Parameters"]["commands"][0] for s in idx._test_ssm.sent):
        assert "GROOM_BACKEND" not in cmd


def test_primary_backend_env_unset_omits_backend_on_demand_gate(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
                       "issue_filter": "mid-only", "schedule": "0 7 * * *"}, None)
    assert "backend" not in out["groom"]
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "GROOM_BACKEND" not in cmd


def test_primary_backend_armed_pure_low_bundle_routes_deepseek(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    low_entry = next(e for e in out["groom"]["launches"] if e["issue_filter"] == "low-only")
    assert low_entry["backend"] == "deepseek"
    low_decision = next(d for d in out["groom"]["decisions"] if d["issue_filter"] == "low-only")
    assert low_decision["backend"] == "deepseek"
    low_cmd = next(
        s["Parameters"]["commands"][0] for s in idx._test_ssm.sent
        if "export GROOM_ISSUE_FILTER=low-only" in s["Parameters"]["commands"][0]
    )
    assert "export GROOM_BACKEND=deepseek" in low_cmd


def test_primary_backend_armed_pure_mid_bundle_routes_deepseek(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    mid_entry = next(e for e in out["groom"]["launches"] if e["issue_filter"] == "mid-only")
    assert mid_entry["backend"] == "deepseek"


def test_primary_backend_armed_low_plus_mid_bundle_routes_deepseek(monkeypatch):
    # Thin-pool bundling (no standalone high): 5 low + 6 mid + 0 high -> ONE
    # "mid+low" bundle. Every tier in the bundle qualifies -> deepseek.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 5, "mid": 6, "high": 0})
    out = idx.handler(_demand_event(), None)
    ls = out["groom"]["launches"]
    assert len(ls) == 1 and ls[0]["issue_filter"] == "mid+low"
    assert ls[0]["backend"] == "deepseek"
    assert out["groom"]["decisions"][0]["backend"] == "deepseek"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_BACKEND=deepseek" in cmd


def test_primary_backend_armed_high_only_bundle_stays_claude(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler(_demand_event(), None)
    high_entry = next(e for e in out["groom"]["launches"] if e["issue_filter"] == "high-only")
    assert "backend" not in high_entry
    high_decision = next(d for d in out["groom"]["decisions"] if d["issue_filter"] == "high-only")
    assert "backend" not in high_decision
    high_cmd = next(
        s["Parameters"]["commands"][0] for s in idx._test_ssm.sent
        if "export GROOM_ISSUE_FILTER=high-only" in s["Parameters"]["commands"][0]
    )
    assert "GROOM_BACKEND" not in high_cmd


def test_primary_backend_armed_high_plus_mid_bundle_stays_claude(monkeypatch):
    # config#2409 attach-upward: mid (thin) rides UP into high's standalone
    # run when there's no standalone mid run of its own -> tiers=(mid, high).
    # mid alone WOULD qualify for DeepSeek, but any-high blocks the whole
    # bundle — one box, one provider, high stays on the Max plan by ruling.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 3, "high": 10})
    out = idx.handler(_demand_event(), None)
    bundle = next(e for e in out["groom"]["launches"] if e["issue_filter"] == "high+mid")
    assert "backend" not in bundle
    bundle_decision = next(d for d in out["groom"]["decisions"] if d["issue_filter"] == "high+mid")
    assert "backend" not in bundle_decision
    # The separate low-only standalone run in the SAME trigger DOES qualify.
    low_entry = next(e for e in out["groom"]["launches"] if e["issue_filter"] == "low-only")
    assert low_entry["backend"] == "deepseek"


def test_primary_backend_demand_gate_decision_record_includes_backend_when_armed(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(idx, "_enumerate_tier_stats",
                        lambda token: ({"low": 8, "mid": 9, "high": 10}, {}, False))
    monkeypatch.setattr(idx, "_notify_demand_skip", lambda *a, **k: None)
    idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
                "issue_filter": "mid-only", "schedule": "0 7 * * *"}, None)
    records = {k: v for k, v in idx._test_s3._objects.items()
               if k.startswith("groom/decisions/") and k.endswith("/mid.json")}
    assert len(records) == 1
    doc = json.loads(list(records.values())[0])
    assert doc["backend"] == "deepseek"


def test_decide_only_single_tier_includes_backend_when_armed(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
                       "issue_filter": "mid-only", "schedule": "0 7 * * *",
                       "decide_only": True}, None)
    assert out["decide"]["launches"] == [
        {"model": "claude-sonnet-5", "issue_filter": "mid-only", "backend": "deepseek"}
    ]
    assert idx._test_ssm.sent == []


def test_decide_only_demand_all_includes_backend_when_armed(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    out = idx.handler({**_demand_event(), "decide_only": True}, None)
    by_filter = {e["issue_filter"]: e for e in out["decide"]["launches"]}
    assert by_filter["low-only"]["backend"] == "deepseek"
    assert by_filter["mid-only"]["backend"] == "deepseek"
    assert "backend" not in by_filter["high-only"]
    assert idx._test_ssm.sent == []


def test_launch_decided_backend_round_trip_deepseek(monkeypatch):
    # Simulates the SF Map's wholesale per-item merge: a decide_only entry
    # carrying "backend": "deepseek" (see test above) round-trips into the
    # matching launch_decided invocation's event.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({
        "run_mode": "full", "schedule": "0 1 * * *", "model": "claude-sonnet-5",
        "issue_filter": "low-only", "backend": "deepseek",
        "launch_decided": True,
    }, None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["backend"] == "deepseek"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_BACKEND=deepseek" in cmd
    records = {k: v for k, v in idx._test_s3._objects.items()
               if k.startswith("groom/decisions/") and "/sweep-" in k}
    assert len(records) == 1
    doc = json.loads(list(records.values())[0])
    assert doc["decisions"][0]["backend"] == "deepseek"


def test_launch_decided_backend_case_insensitive(monkeypatch):
    # Mirrors every other event-key resolver in this handler.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({
        "run_mode": "full", "schedule": "x", "issue_filter": "low-only",
        "backend": "DeepSeek", "launch_decided": True,
    }, None)
    assert out["groom"]["backend"] == "deepseek"


def test_launch_decided_backend_absent_stays_claude(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({
        "run_mode": "full", "schedule": "x", "issue_filter": "low-only",
        "launch_decided": True,
    }, None)
    assert "backend" not in out["groom"]
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "GROOM_BACKEND" not in cmd


@pytest.mark.parametrize("bad_backend", ["claude", "deepseek-typo", "openrouter", "anthropic"])
def test_launch_decided_invalid_backend_raises(monkeypatch, bad_backend):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    with pytest.raises(ValueError):
        idx.handler({
            "run_mode": "full", "schedule": "x", "issue_filter": "low-only",
            "backend": bad_backend, "launch_decided": True,
        }, None)
    assert not idx._test_ssm.sent  # fail loud BEFORE any spend


def test_sweep_launch_decided_stays_claude_even_when_primary_armed(monkeypatch):
    # Sweep dispatches never carry a "backend" key in their event (the SF's
    # DispatchEndOfSfSweep state never sets one) — arming
    # GROOM_PRIMARY_DEEPSEEK_TIERS must not retroactively affect them, since
    # sweep boxes never go through _primary_backend_for at all.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    assert "backend" not in out["groom"]
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "GROOM_BACKEND" not in cmd


def test_fallback_dispatch_backend_independent_of_primary_tiers_env(monkeypatch):
    # The quota-fallback leg (_handle_fallback_dispatch) always threads
    # GROOM_BACKEND_DEEPSEEK regardless of GROOM_PRIMARY_DEEPSEEK_TIERS —
    # even for "high", which PRIMARY-mode would never route to DeepSeek.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    out = idx.handler({"mode": "fallback", "tier": "high"}, None)
    assert out["groom"]["backend"] == "deepseek"


def test_deploy_sh_arms_primary_deepseek_tiers_low_mid():
    # Structural pin, INVERTED at arming (2026-07-22, config-I3488 step 3 —
    # Brian's DeepSeek-primary ruling, config-I3479): BOTH `--environment
    # 'Variables={...}'` calls must now carry GROOM_PRIMARY_DEEPSEEK_TIERS
    # with the value DOUBLE-QUOTED ("low,mid") — a raw comma inside CLI
    # shorthand splits map entries and fails ParamValidation (verified live).
    # This guards against (a) silent DISARM by a deploy.sh refactor dropping
    # the var from one or both maps, and (b) re-introducing the unquoted form.
    # Disarming is a deliberate reviewed PR that flips this pin back.
    deploy_sh = (
        Path(__file__).resolve().parent / "deploy.sh"
    ).read_text()
    armed_lines = [
        line for line in deploy_sh.splitlines()
        if "--environment 'Variables=" in line
        and not line.lstrip().startswith("#")  # doc comment references the pattern too
    ]
    assert len(armed_lines) == 2
    for line in armed_lines:
        assert 'GROOM_PRIMARY_DEEPSEEK_TIERS="low,mid"' in line
