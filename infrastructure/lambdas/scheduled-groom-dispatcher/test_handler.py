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

# groom-sweep-policy §2.3: `nousergon_lib.groom_eligibility.TIER_MODELS` is the
# SINGLE OWNER of the tier->model assignment. Derive expectations from it rather
# than restating the model names here — a second hardcoded copy is exactly how
# this Lambda came to dispatch claude-sonnet-5 for the high tier for days after
# v0.124.16 moved it to deepseek-v4-pro (alpha-engine-config-I4796).
from nousergon_lib.groom_eligibility import TIER_MODELS

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


class _FakeSfn:
    """Minimal ``stepfunctions`` client.

    Two concerns live in one fake (same boto3 client):
    - Cycle-singleton guard (alpha-engine-config-I5371): ``executions`` is the
      RUNNING set the fake reports; ``error`` (when set) makes
      ``list_executions`` raise, exercising the fail-CLOSED path.
    - Lane-death reconciler (config-I5229): ``send_task_failure`` tracking for
      the dead-lane → SF collapse path."""

    def __init__(self, executions=None, error=None):
        self.executions = list(executions or [])
        self.error = error
        self.calls: list[dict] = []
        self.send_task_failure_calls: list[dict] = []

    def list_executions(self, **kw):  # noqa: D102 — boto3 shape
        self.calls.append(kw)
        if self.error is not None:
            raise self.error
        return {"executions": self.executions}

    def send_task_failure(self, taskToken, error, cause):  # noqa: N803 — boto3 kwarg names
        self.send_task_failure_calls.append({
            "taskToken": taskToken, "error": error, "cause": cause,
        })


def _exec(arn, started, name=None):
    """RUNNING-execution record shaped like boto3's ``list_executions``."""
    return {"executionArn": arn, "name": name or arn.rsplit(":", 1)[-1],
            "status": "RUNNING",
            "startDate": datetime(2026, 7, 29, 0, 0, tzinfo=timezone.utc)
                         .replace(hour=started)}


_SM = "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-groom-dispatch"
_EXEC_PREFIX = "arn:aws:states:us-east-1:711398986525:execution:alpha-engine-groom-dispatch"


class _FakeWaiter:
    def wait(self, **kw):
        return None


class _FakeEc2:
    def __init__(self, running_tier_instances=None, spot_reclaimed=None):
        self.terminated = []
        # `create_tags` appended to this list without it ever being initialised
        # — an AttributeError waiting for the first test to exercise the tag
        # path. I6199 is that first test.
        self.tags_created: list[tuple[list[str], list[dict]]] = []
        # config#1979: (issue_filter -> [instance_ids]) already "live" for the
        # concurrent-tier guard's describe_instances check to find.
        self._running_tier_instances = dict(running_tier_instances or {})
        # config-I5229: instance_id -> state name for the reconciler's
        # InstanceIds-based describe_instances.
        self._instance_states: dict[str, str] = {}
        # 2026-08-02 spot-reclaim race: instance_ids whose spot request is
        # marked-for-termination / closed (Status.Code) while the instance is
        # still `running`. termination_imminent() probes these via
        # describe_spot_instance_requests to see past State.Name during the
        # 2-min interruption-notice window. Each id maps to the fake spot
        # request record the paginator returns.
        self._spot_reclaimed: dict[str, dict] = dict(spot_reclaimed or {})
        # I6199: instance_id -> tags the reconciler reads to recover WHY the
        # dispatcher tore a box down.
        self._instance_tags: dict[str, list[dict]] = {}

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803 — boto3 kwarg name
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}

    def create_tags(self, Resources, Tags):  # noqa: N803 — boto3 kwarg names
        self.tags_created.append((Resources, Tags))
        return {}

    def describe_instances(self, Filters=None, InstanceIds=None):  # noqa: N803 — boto3 kwarg names
        # config-I5229: the lane-death reconciler calls describe_instances with
        # InstanceIds (batch lookup). The concurrent-tier guard uses Filters
        # (tag-based lookup). Support BOTH call shapes.
        if InstanceIds:
            # Reconciler / termination_imminent path — return the stubbed states
            # for these ids, plus a SpotInstanceRequestId when one is registered
            # (so the reclaim probe can look it up). Default state is `terminated`
            # (a dead lane's instance) — tests exercising the reclaim race set
            # `running` explicitly via _instance_states.
            instances = []
            for iid in InstanceIds:
                state = self._instance_states.get(iid, "terminated")
                inst = {"InstanceId": iid, "State": {"Name": state}}
                if iid in self._spot_reclaimed:
                    inst["SpotInstanceRequestId"] = f"sir-{iid}"
                if iid in self._instance_tags:
                    inst["Tags"] = self._instance_tags[iid]
                instances.append(inst)
            return {"Reservations": [{"Instances": instances}]} if instances else {"Reservations": []}
        if Filters:
            by_name = {f["Name"]: f["Values"] for f in Filters}
            issue_filter = by_name.get("tag:groom-issue-filter", [None])[0]
            ids = self._running_tier_instances.get(issue_filter, [])
            return {"Reservations": [{"Instances": [{"InstanceId": i} for i in ids]}]} if ids else {"Reservations": []}
        return {"Reservations": []}

    def get_paginator(self, name):
        # 2026-08-02: termination_imminent paginates describe_spot_instance_requests
        # to read Status.Code for instances still `running` but spot-reclaimed.
        if name == "describe_spot_instance_requests":
            requests = list(self._spot_reclaimed.values())
            return _FakeSpotRequestPaginator(requests)
        raise AssertionError(f"unexpected EC2 paginator in hermetic tests: {name!r}")


class _FakeSpotRequestPaginator:
    """Yields the stubbed spot-instance-request records for termination_imminent."""

    def __init__(self, requests: list[dict]):
        self._requests = requests

    def paginate(self, SpotInstanceRequestIds=None):  # noqa: N803 — boto3 kwarg name
        wanted = set(SpotInstanceRequestIds or [])
        page = [r for r in self._requests if r.get("SpotInstanceRequestId") in wanted]
        yield {"SpotInstanceRequests": page}


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

    def head_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        # config-I5229: the lane-death reconciler uses head_object to check
        # for completion markers without fetching the body.
        if Key in self._objects:
            return {"ContentLength": len(self._objects[Key])}
        import botocore.exceptions
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )


def _load(monkeypatch, *, launch_impl=None, env=None, s3_objects=None, ssm_parameters=None,
         running_tier_instances=None, sfn_executions=None, sfn_error=None,
         spot_reclaimed=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm(ssm_parameters)
    ec2 = _FakeEc2(running_tier_instances=running_tier_instances,
                   spot_reclaimed=spot_reclaimed)
    s3 = _FakeS3(s3_objects)
    sfn = _FakeSfn(executions=sfn_executions, error=sfn_error)
    clients = {"ec2": ec2, "ssm": ssm, "s3": s3, "stepfunctions": sfn}
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
    index._test_sfn = sfn
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
    # groom-primary-deepseek (2026-07-23): every slot now launches all 3 tiers
    # + sweep, so all SCHED_INPUTS carry pr_budget for the high tier box.
    sched_inputs_lines = [
        line for line in deploy_sh.splitlines()
        if "SCHED_INPUTS" in line or line.strip().startswith("'{\"run_mode\"")
    ]
    pr_budget_lines = [
        line for line in sched_inputs_lines
        if "pr_budget" in line
    ]
    # config#1311: 3 daily + 1 Sunday = 4 demand-all slots, all carry pr_budget
    # (every slot launches all 3 tiers including high when the queue clears).
    assert len(pr_budget_lines) == 4, (
        f"expected 4 SCHED_INPUT entries with pr_budget (3 daily + Sunday, "
        f"demand-all cadence), got {len(pr_budget_lines)}: {pr_budget_lines}"
    )


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
        {"run_mode": "full", "model": "deepseek-v4-flash",
         "issue_filter": "gated-reverify", "schedule": "0 9 * * 0"},
        None,
    )
    g = out["groom"]
    assert g["issue_filter"] == "gated-reverify"
    assert g["model"] == "deepseek-v4-flash"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_ISSUE_FILTER=gated-reverify" in cmd


def test_missing_model_and_issue_filter_default_to_mid_queue(monkeypatch):
    # Schedules with no model/issue_filter must default to DeepSeek / mid-only.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    g = out["groom"]
    assert g["model"] == "deepseek-v4-pro"
    assert g["issue_filter"] == "mid-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=deepseek-v4-pro" in cmd
    assert "export GROOM_ISSUE_FILTER=mid-only" in cmd


def test_low_only_schedule_forwards_haiku_model_and_filter(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        {"run_mode": "full", "model": "deepseek-v4-flash", "issue_filter": "low-only",
         "schedule": "0 19 * * *"},
        None,
    )
    g = out["groom"]
    assert g["model"] == "deepseek-v4-flash"
    assert g["issue_filter"] == "low-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=deepseek-v4-flash" in cmd
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
    assert out["groom"]["model"] == "deepseek-v4-pro"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "claude; rm -rf /" not in cmd
    assert "export GROOM_MODEL=deepseek-v4-pro" in cmd


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
    extra_tags_captured = {}

    def _launch(types_, subnets, **kw):
        extra_tags_captured["value"] = kw.get("extra_tags")
        return "i-new"

    idx = _load(
        monkeypatch, launch_impl=_launch,  # noqa: E731
        env={"GROOM_DISPATCH_ENABLED": "true"},
    )
    idx.handler({"run_mode": "full", "issue_filter": "high-only", "schedule": "x"}, None)
    # config#5303: the groom-issue-filter tag now rides the RunInstances call
    # as extra_tags (atomic with launch), not a separate post-launch create_tags.
    # I5727 (nousergon-lib v0.124.23): launch_with_fallback now adds
    # LaunchMarket/LaunchReason to extra_tags on EVERY launch, so this asserts
    # the caller's tag survives rather than exact-dict equality — the contract
    # is "the lane tag rides RunInstances", not "nothing else does".
    assert extra_tags_captured["value"]["groom-issue-filter"] == "high-only"
    assert extra_tags_captured["value"]["LaunchMarket"] == "spot"
    assert extra_tags_captured["value"]["LaunchReason"] == "spot_ok"


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


# ── 2026-08-02: spot-reclaim race — reclaim-aware relaunch (attempt > 0) ──────
# The on-box watcher fires send_task_failure(SpotInterrupted) on the IMDS notice
# ~2 min before AWS terminates the box, so the SF's relaunch runs while the prior
# instance is still `running` and the concurrent guard (config#1979) would
# suppress the replacement as a "duplicate" — the lane dies unworked. A relaunch
# (attempt > 0) re-probes the "live" instances and proceeds past any that are
# termination-imminent (the dying prior attempt, not a competing workload).

def _reclaimed_sir(instance_id, status_code="marked-for-termination", state="active"):
    """A spot-instance-request record the fake paginator returns for a
    spot-reclaimed instance still in `running` state."""
    return {"SpotInstanceRequestId": f"sir-{instance_id}",
            "State": state, "Status": {"Code": status_code}}


def test_relaunch_proceeds_when_prior_instance_is_spot_reclaimed(monkeypatch):
    """The race case (2026-08-02 20:00 UTC low-only): attempt 1 finds the prior
    box still `running` but its spot request is marked-for-termination. The
    replacement MUST launch — suppressing here loses the lane."""
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-replacement"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"low-only": ["i-dying"]},
        spot_reclaimed={"i-dying": _reclaimed_sir("i-dying")},
    )
    # The concurrent guard (Filters) returns i-dying as "live"; the reclaim
    # probe (InstanceIds) must see it as `running` (not the default terminated)
    # so the spot-request lookup is the thing that flags it imminent.
    idx._test_ec2._instance_states["i-dying"] = "running"
    out = idx.handler(
        {"run_mode": "full", "issue_filter": "low-only", "schedule": "0 20 * * *",
         "launch_decided": True, "attempt": 1, "Token": "tok-1"},
        None,
    )
    assert out["groom"]["launched"] is True
    assert out["groom"]["instance_id"] == "i-replacement"
    assert launched == [True]


def test_relaunch_proceeds_when_prior_instance_is_already_terminated(monkeypatch):
    """The reconciler-late case: by the time the relaunch fires, the prior box
    has flipped to `terminated` (describe-instances still returns it for ~1h).
    State alone is enough to see this one — no spot-request lookup needed."""
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-replacement"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"low-only": ["i-dead"]},
    )
    idx._test_ec2._instance_states["i-dead"] = "terminated"
    out = idx.handler(
        {"run_mode": "full", "issue_filter": "low-only", "schedule": "0 20 * * *",
         "launch_decided": True, "attempt": 1, "Token": "tok-1"},
        None,
    )
    assert out["groom"]["launched"] is True
    assert launched == [True]


def test_relaunch_still_skips_when_prior_instance_is_genuinely_live(monkeypatch):
    """A real duplicate (attempt > 0 but the prior box is healthy) still
    suppresses — config#1979's intent is preserved for the genuine case."""
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-should-not-launch"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"},
        # Prior box running, no spot-reclaim entry -> healthy.
        running_tier_instances={"mid-only": ["i-healthy"]},
    )
    idx._test_ec2._instance_states["i-healthy"] = "running"
    out = idx.handler(
        {"run_mode": "full", "issue_filter": "mid-only", "schedule": "0 20 * * *",
         "launch_decided": True, "attempt": 1, "Token": "tok-1"},
        None,
    )
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "concurrent_tier_skip"
    assert out["groom"]["existing_instance_ids"] == ["i-healthy"]
    assert launched == []


def test_first_launch_attempt_zero_still_skips_on_live_box(monkeypatch):
    """attempt 0 (a first launch, not a relaunch) with a live box is a genuine
    duplicate — the reclaim bypass is scoped to attempt > 0 only."""
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-should-not-launch"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"low-only": ["i-existing"]},
        # Even with a reclaim signal, attempt 0 must NOT bypass — a first launch
        # has no prior attempt to be "dying," so this would be a true duplicate.
        spot_reclaimed={"i-existing": _reclaimed_sir("i-existing")},
    )
    idx._test_ec2._instance_states["i-existing"] = "running"
    out = idx.handler(
        {"run_mode": "full", "issue_filter": "low-only", "schedule": "0 20 * * *",
         "launch_decided": True, "attempt": 0, "Token": "tok-0"},
        None,
    )
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "concurrent_tier_skip"
    assert launched == []


def test_reclaim_probe_failure_fails_safe_to_skip(monkeypatch):
    """A broken spot-request probe must never risk a duplicate — degrade to the
    original guard suppression. The lane is lost for this cycle (the reconciler
    pages), but a duplicate-launch is the worse failure."""
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-maybe"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"low-only": ["i-maybe"]},
    )
    # The concurrent probe (running_instance_ids) succeeds and returns the id;
    # the reclaim probe (termination_imminent) calls describe_instances with
    # InstanceIds and we make THAT raise.
    original_describe = idx._test_ec2.describe_instances

    def _describe(Filters=None, InstanceIds=None):  # noqa: N803 — boto3 kwarg names
        if InstanceIds:
            raise RuntimeError("EC2 API hiccup on reclaim probe")
        return original_describe(Filters=Filters)

    idx._test_ec2.describe_instances = _describe
    out = idx.handler(
        {"run_mode": "full", "issue_filter": "low-only", "schedule": "0 20 * * *",
         "launch_decided": True, "attempt": 1, "Token": "tok-1"},
        None,
    )
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "concurrent_tier_skip"
    assert launched == []


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
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
                       "issue_filter": "low-only", "schedule": "0 19 * * *"}, None)
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "demand_gate_skip"
    assert not idx._test_ssm.sent  # no box, no SSM command


def test_demand_gate_bundles_and_downgrades_model(monkeypatch):
    # high slot, no high issues, starving low+mid -> ONE box at the highest
    # present tier's model (mid=deepseek-v4-flash in v0.124.15). decide_slot
    # bundling logic unchanged; only TIER_MODELS changed.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 5, "mid": 6, "high": 0})
    out = idx.handler({"run_mode": "full", "model": "claude-sonnet-5",
                       "issue_filter": "high-only", "schedule": "0 1 * * *"}, None)
    g = out["groom"]
    assert g["launched"] and g["issue_filter"] == "mid+low"
    assert g["model"] == "deepseek-v4-flash"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_ISSUE_FILTER=mid+low" in cmd
    assert "export GROOM_MODEL=deepseek-v4-flash" in cmd


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
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
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
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
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
    # Every tier launches independently at its OWN tier's model, read from the
    # lib that owns the assignment (groom-sweep-policy §5 tier table).
    assert launched == {("high-only", TIER_MODELS["high"]),
                        ("mid-only", TIER_MODELS["mid"]),
                        ("low-only", TIER_MODELS["low"])}
    cmds = [c["Parameters"]["commands"][0] for c in idx._test_ssm.sent]
    assert len(cmds) == 3
    # config#2201: groom boxes are pure issue-coverage workers — the
    # config#2129 per-box sweep-partition exports are retired (the dispatch
    # SF's end-of-SF run_mode=sweep box owns ALL PR sweeping now).
    for c in cmds:
        assert "GROOM_NO_SWEEP" not in c
        assert "GROOM_SWEEP_PARTITION" not in c


def test_symmetric_trigger_all_tiers_launch_when_any_issues(monkeypatch):
    # groom-primary-deepseek (v0.124.15): unconditional per-tier launch —
    # every tier with >0 actionable issues launches its own box regardless
    # of floor=8. low=2, mid=3, high=1 → 3 boxes.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 2, "mid": 3, "high": 1})
    out = idx.handler(_demand_event("0 7 * * *"), None)
    launches = out["groom"]["launches"]
    assert len(launches) == 3
    issue_filters = {l["issue_filter"] for l in launches}
    assert issue_filters == {"low-only", "mid-only", "high-only"}
    models = {l["model"] for l in launches}
    assert models == {TIER_MODELS["low"], TIER_MODELS["mid"], TIER_MODELS["high"]}
    assert len(idx._test_ssm.sent) == 3


def test_symmetric_trigger_separate_boxes_no_bundling(monkeypatch):
    # groom-primary-deepseek (v0.124.15): no thin-tier bundling — low=5
    # launches its own low-only box (deepseek-v4-flash), mid=6 launches its
    # own mid-only box (deepseek-v4-flash). high=0 is skipped.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 5, "mid": 6, "high": 0})
    out = idx.handler(_demand_event(), None)
    ls = out["groom"]["launches"]
    assert len(ls) == 2
    issue_filters = {l["issue_filter"] for l in ls}
    assert issue_filters == {"low-only", "mid-only"}
    for l in ls:
        assert l["model"] == "deepseek-v4-flash"


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


def test_load_recent_engagements_no_longer_exists(monkeypatch):
    """config#2146: the 72h fresh-skip cooldown this engagement-horizon scan
    fed is retired — eligibility is disposition-structural now
    (ge.is_actionable alone). The function had no other caller, so it's
    removed with the cooldown rather than left as dead S3-scanning weight."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    assert not hasattr(idx, "_load_recent_engagements")


def test_non_demand_events_keep_legacy_behavior(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    called = []
    monkeypatch.setattr(idx, "_enumerate_tier_stats_fresh",
                        lambda token: called.append(1) or ({}, {}, []))
    monkeypatch.setattr(idx, "_demand_decision", lambda f, s: None)
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
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
    assert d["launches"] == [{"model": "deepseek-v4-flash", "issue_filter": "mid-only"}]
    assert idx._test_ssm.sent == []


def test_decide_only_ungated_direct_dispatch(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
                       "issue_filter": "gated-reverify", "decide_only": True}, None)
    assert out["decide"]["launches"] == [{"model": "deepseek-v4-flash",
                                          "issue_filter": "gated-reverify"}]
    assert idx._test_ssm.sent == []


def test_decide_only_demand_gate_skip_shape(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_stats(monkeypatch, idx, {"low": 3, "mid": 40, "high": 2})
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
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
        "run_mode": "full", "schedule": "0 1 * * *", "model": "deepseek-v4-flash",
        "issue_filter": "low-only",
        "launch_decided": True,
    }, None)
    g = out["groom"]
    assert g["launched"] is True
    assert g["model"] == "deepseek-v4-flash" and g["issue_filter"] == "low-only"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_MODEL=deepseek-v4-flash" in cmd
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
    "run_mode": "sweep", "launch_decided": True, "model": "deepseek-v4-flash",
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
    assert g["model"] == "deepseek-v4-flash"
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "groom_spot_bootstrap.sh --mode sweep" in cmd
    assert "export GROOM_MODEL=deepseek-v4-flash" in cmd


def test_sweep_box_tagged_with_distinct_sweep_lane(monkeypatch):
    # The concurrent guard keys on tag groom-issue-filter — a sweep box tagged
    # with its (inert) issue_filter verbatim would collide with the mid-only
    # GROOM box's tag. Sweep boxes get the distinct 'sweep' tag value instead;
    # the event's issue_filter still passes the lib filter validation.
    extra_tags_captured = {}

    def _launch(types_, subnets, **kw):
        extra_tags_captured["value"] = kw.get("extra_tags")
        return "i-stub"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"},
    )
    out = idx.handler(dict(_SWEEP_SF_EVENT), None)
    assert out["groom"]["tier_tag"] == "sweep"
    assert out["groom"]["issue_filter"] == "mid-only"
    # config#5303: sweep lane tag rides the RunInstances call as extra_tags.
    # I5727 (nousergon-lib v0.124.23): launch_with_fallback now adds
    # LaunchMarket/LaunchReason to extra_tags on EVERY launch, so this asserts
    # the caller's tag survives rather than exact-dict equality — the contract
    # is "the lane tag rides RunInstances", not "nothing else does".
    assert extra_tags_captured["value"]["groom-issue-filter"] == "sweep"
    assert extra_tags_captured["value"]["LaunchMarket"] == "spot"
    assert extra_tags_captured["value"]["LaunchReason"] == "spot_ok"


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
        "launch": True, "issue_filter": "mid-only", "model": "deepseek-v4-flash",
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
    # config-I5229: the EXPECTATION LEDGER write (_write_dispatch_ledger_entry)
    # is now FAIL-LOUD — a registration-write failure IS a paging condition
    # (§2.7) and the just-launched box is terminated. The DECISION RECORD
    # (_write_sweep_decision_record) remains best-effort: it is written AFTER
    # the launch returns and a failure must never turn an already-successful
    # sweep launch into a crash.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})

    # Only break the decision-record write (key starts with groom/decisions/),
    # not the expectation-ledger write (which must succeed for the launch to
    # proceed).
    orig_put = idx._test_s3.put_object

    def _fail_decision_only(Bucket, Key, Body, **kw):  # noqa: N803
        if isinstance(Key, str) and Key.startswith("groom/decisions/"):
            raise RuntimeError("S3 down for decision records")
        return orig_put(Bucket, Key, Body, **kw)

    monkeypatch.setattr(idx._test_s3, "put_object", _fail_decision_only)
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


# ── config#2146: fresh-skip retirement — ge.is_actionable alone gates entry ──


def test_enumerate_tier_stats_fresh_no_longer_engagement_gated(monkeypatch):
    """config#2146: a recently-touched issue is no longer excluded/deflated —
    the 72h fresh-skip cooldown ge.fresh_skip_active used to apply here is
    retired, and disposition-structural eligibility (ge.is_actionable) is
    the ONLY gate left. A same-day S3 run artifact records a ``commented``
    engagement on an issue that is ALSO still open with a mid-tier label; it
    must still be counted (there is no engagement-horizon lookup left at
    all — ``_load_recent_engagements`` no longer exists)."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    now = datetime.now(timezone.utc)
    art = json.dumps({
        "run_start": now.isoformat().replace("+00:00", "Z"), "elapsed_min": 5,
        "issues": [{"repo": "nousergon/alpha-engine-config", "number": 999,
                    "disposition": "commented"}],
    }).encode()
    key = f"groom/{now.strftime('%Y-%m-%d')}/run1.json"
    idx._test_s3._objects[key] = art
    recently_updated = now.isoformat().replace("+00:00", "Z")

    def fake_urlopen(req, timeout=30):
        url = req.full_url
        if ("/repos/nousergon/alpha-engine-config/issues?state=open&per_page="
                in url and "page=1" in url):
            return _FakeHTTPResponse(json.dumps([
                {"number": 999, "labels": [{"name": "complexity:mid"}],
                 "updated_at": recently_updated},
            ]).encode())
        return _FakeHTTPResponse(b"[]")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: [])
    counts, _oldest, _p0, tier_issues = idx._enumerate_tier_stats_fresh("tok")
    assert counts["mid"] == 1
    assert tier_issues["mid"][0]["number"] == 999


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
    reports it) but the boxes still launch — grooms are the primary deliverable.
    config-I5229: only the manifest writes fail; the expectation-ledger write
    (now fail-loud) must still succeed."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 9, "high": 10})
    orig_put = idx._test_s3.put_object
    def _fail_queues_only(Bucket, Key, Body, **kw):  # noqa: N803
        if isinstance(Key, str) and Key.startswith("groom/queues/"):
            raise RuntimeError("AccessDenied: s3:PutObject on queues")
        return orig_put(Bucket, Key, Body, **kw)
    monkeypatch.setattr(idx._test_s3, "put_object", _fail_queues_only)
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
                       "model": "deepseek-v4-flash", "issue_filter": "low-only",
                       "queue_manifest_key": "groom/queues/drain/2026-07-10-low.json"}, None)
    assert out["groom"]["launched"]
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert "export GROOM_QUEUE_MANIFEST_KEY=groom/queues/drain/2026-07-10-low.json" in cmd


def test_no_manifest_key_no_export(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "schedule": "manual", "force_on_demand": True,
                       "model": "deepseek-v4-flash", "issue_filter": "low-only"}, None)
    assert out["groom"]["launched"]
    assert "GROOM_QUEUE_MANIFEST_KEY" not in idx._test_ssm.sent[0]["Parameters"]["commands"][0]


def test_malformed_manifest_key_fails_loud(monkeypatch):
    """The key lands on a root-shell command line — strict charset, fail loud."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    with pytest.raises(ValueError, match="invalid queue_manifest_key"):
        idx.handler({"run_mode": "full", "schedule": "manual",
                     "model": "deepseek-v4-flash", "issue_filter": "low-only",
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
                       "model": "deepseek-v4-flash", "issue_filter": "low-only",
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
                       "model": "deepseek-v4-flash", "issue_filter": "low-only",
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


def test_dispatch_ledger_write_failure_terminates_box_and_raises(monkeypatch):
    """config-I5229: the ledger write is now FAIL-LOUD — a registration-write
    failure IS a paging condition per groom-sweep-policy §2.7. The box is
    terminated (no orphan) and the error propagates."""
    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true", "GROOM_MAX_DISPATCHES_DAILY": "5"},
    )
    monkeypatch.setattr(idx, "_prior_launch_count_today", lambda: 0)

    def _boom(**kw):
        # I6460: the per-lane dispatch lease also writes via put_object
        # (a different key, locks/groom-lane-*.lock) BEFORE the ledger —
        # only the ledger key must fail here, so the lease acquire still
        # succeeds and the launch proceeds far enough to need terminating.
        if "dispatch-ledger" in kw.get("Key", ""):
            raise RuntimeError("S3 down")
        return idx._test_s3.__class__.put_object(idx._test_s3, **kw)

    monkeypatch.setattr(idx._test_s3, "put_object", _boom)
    with pytest.raises(RuntimeError, match="S3 down"):
        idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    # The just-launched box was terminated — no orphaned instance.
    assert "i-stub" in idx._test_ec2.terminated


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


def test_demand_all_ruling_pending_prs_launch_when_any_present(monkeypatch):
    # groom-primary-deepseek (v0.124.15): unconditional per-tier launch —
    # a single ruling:pending-exec PR with complexity:mid, zero issues,
    # still launches (1 > 0).
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(idx, "_github_token", lambda: "tok")
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen_empty_issue_pages)
    pr = _search_pr_item("nousergon/nousergon-data", 920,
                         ["ruling:pending-exec", "complexity:mid"], _recent_iso())
    pr["repo"] = "nousergon/nousergon-data"
    monkeypatch.setattr(idx, "_enumerate_ruling_pending_prs", lambda token: [pr])
    out = idx.handler(_demand_event(), None)
    launches = out["groom"]["launches"]
    assert len(launches) == 1
    assert launches[0]["issue_filter"] == "mid-only"
    assert idx._test_ssm.sent


def test_demand_all_ruling_pending_pr_launches_unconditionally(monkeypatch):
    # groom-primary-deepseek (v0.124.15): unconditional per-tier launch —
    # escape valves and staleness thresholds are retired; any positive
    # count launches regardless of age or floor.
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


def test_primary_backend_armed_low_and_mid_separate_boxes_deepseek(monkeypatch):
    # groom-primary-deepseek (v0.124.15): no bundling — low=5 → low-only box,
    # mid=6 → mid-only box, both on DeepSeek since both tiers are armed.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 5, "mid": 6, "high": 0})
    out = idx.handler(_demand_event(), None)
    ls = out["groom"]["launches"]
    assert len(ls) == 2
    issue_filters = {l["issue_filter"] for l in ls}
    assert issue_filters == {"low-only", "mid-only"}
    for l in ls:
        assert l["backend"] == "deepseek"
    assert {d["issue_filter"] for d in out["groom"]["decisions"]} == {"low-only", "mid-only"}
    for cmd in (s["Parameters"]["commands"][0] for s in idx._test_ssm.sent):
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


def test_primary_backend_armed_high_stays_claude_mid_goes_deepseek(monkeypatch):
    # groom-primary-deepseek (v0.124.15): every tier launches independently.
    # mid=3 → its own DeepSeek box (mid in GROOM_PRIMARY_DEEPSEEK_TIERS).
    # high=10 → its own Claude box (high NOT in GROOM_PRIMARY_DEEPSEEK_TIERS).
    # No attach-upward bundling — each tier is a separate launch.
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true",
                                  "GROOM_PRIMARY_DEEPSEEK_TIERS": "low,mid"})
    _stub_fresh_stats(monkeypatch, idx, {"low": 8, "mid": 3, "high": 10})
    out = idx.handler(_demand_event(), None)
    high_entry = next(e for e in out["groom"]["launches"] if e["issue_filter"] == "high-only")
    assert "backend" not in high_entry
    mid_entry = next(e for e in out["groom"]["launches"] if e["issue_filter"] == "mid-only")
    assert mid_entry["backend"] == "deepseek"
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
        {"model": "deepseek-v4-flash", "issue_filter": "mid-only", "backend": "deepseek"}
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


def test_deploy_sh_arms_primary_deepseek_tiers_low_mid_high():
    # Structural pin, INVERTED at arming (2026-07-22, config-I3488 step 3 —
    # Brian's DeepSeek-primary ruling, config-I3479): BOTH `--environment
    # 'Variables={...}'` calls must now carry GROOM_PRIMARY_DEEPSEEK_TIERS
    # with the value DOUBLE-QUOTED ("low,mid,high") — a raw comma inside CLI
    # shorthand splits map entries and fails ParamValidation (verified live).
    # 2026-07-24: high tier added to the armed set.
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
        assert 'GROOM_PRIMARY_DEEPSEEK_TIERS="low,mid,high"' in line


def test_task_token_is_read_from_the_event_not_the_context(monkeypatch):
    """config-I4333: the token arrives in the Lambda Payload.

    `getattr(getattr(context, "task", None), "token", "")` returned "" on every
    invocation — a Python Lambda context has no `task` attribute — so the
    callback was never sent and the SF fell through to its timeout path.
    """
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})

    assert idx._task_token({"Token": "AQCEAAAAKgAAAAMAAAAA"}) == "AQCEAAAAKgAAAAMAAAAA"
    assert idx._task_token({"Token": "  padded  "}) == "padded"
    # Absent is legitimate (manual invoke, sweep dispatch) — "" not an error.
    assert idx._task_token({}) == ""
    assert idx._task_token({"Token": None}) == ""
    assert idx._task_token({"Token": ""}) == ""

    # A Lambda context object never carries the token; reading it must not
    # reintroduce the silent-"" regression.
    class _Ctx:
        pass

    assert not hasattr(_Ctx(), "task")


# ── Lane-death reconciler tests (alpha-engine-config-I5229) ────────────────────

# The ledger is date-partitioned and the reconciler scans today + yesterday, so
# a fixture pinned to a literal date passes on the day it is written and is
# dead every day after. That is exactly what happened here: these tests were
# authored 2026-07-28 with a hardcoded `dispatch-ledger/<that date>/` prefix and
# had been silently returning `open_expectations: 0` ever since — five tests
# asserting nothing, while the PR body claimed "135/135 passing".
#
# Derive the partition from the same clock the reconciler uses. `_ledger_key`
# takes an offset so the date-boundary case can be exercised deliberately
# rather than by accident.


def _ledger_key(run_token: str = "tok1", *, days_ago: int = 0) -> str:
    from datetime import datetime, timedelta, timezone
    day = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    return f"groom/_control/dispatch-ledger/{day}/{run_token}.json"


def _future_deadline(hours: int = 6) -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()



def test_reconcile_no_open_expectations_is_quiet(monkeypatch):
    """No dispatch-ledger entries → no deaths, no pages, quiet return."""
    idx = _load(monkeypatch, s3_objects={
        # Only a completed entry (marker exists) — not an open expectation.
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-dead", "deadline_utc": _future_deadline(),
        }).encode(),
        "groom/_control/completed/tok1.json": json.dumps({
            "outcome": "success", "rc": 0,
        }).encode(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["reconciled"] is True
    assert result["open_expectations"] == 0
    assert result["deaths"] == 0
    assert result["overdue"] == 0


def test_reconcile_detects_lane_death_instance_terminated(monkeypatch):
    """Open expectation + instance terminated → lane_died verdict."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-dead", "deadline_utc": _future_deadline(),
        }).encode(),
    })
    # Instance not in _instance_states → defaults to "terminated"
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["deaths"] == 1
    assert result["overdue"] == 0


def test_reconcile_detects_lane_death_instance_stopped(monkeypatch):
    """Open expectation + instance stopped (terminal state) → lane_died verdict."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-stopped", "deadline_utc": _future_deadline(),
        }).encode(),
    })
    idx._test_ec2._instance_states["i-stopped"] = "stopped"
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["deaths"] == 1


def test_reconcile_detects_overdue_running_instance(monkeypatch):
    """Open expectation + instance still running but past deadline → overdue."""
    from datetime import datetime, timedelta, timezone

    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-running", "deadline_utc": past,
        }).encode(),
    })
    idx._test_ec2._instance_states["i-running"] = "running"
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["overdue"] == 1
    assert result["deaths"] == 0


def test_reconcile_skips_running_instance_within_deadline(monkeypatch):
    """Open expectation + instance still running, deadline not yet reached → quiet."""
    from datetime import datetime, timedelta, timezone

    future = (datetime.now(timezone.utc) + timedelta(hours=5)).isoformat()
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-running", "deadline_utc": future,
        }).encode(),
    })
    idx._test_ec2._instance_states["i-running"] = "running"
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["deaths"] == 0
    assert result["overdue"] == 0


def test_reconcile_skips_completed_lane(monkeypatch):
    """Completion marker exists → reconciler skips, regardless of instance state."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-dead", "deadline_utc": _future_deadline(),
        }).encode(),
        "groom/_control/completed/tok1.json": json.dumps({
            "outcome": "success", "rc": 0,
        }).encode(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["deaths"] == 0
    assert result["open_expectations"] == 0


def test_reconcile_describe_instances_error_is_fail_safe(monkeypatch):
    """EC2 describe-instances fails → fail-safe: no deaths reported.
    The reconciler must never page on a broken EC2 API — the next tick retries."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-dead", "deadline_utc": _future_deadline(),
        }).encode(),
    })
    # Make describe_instances with InstanceIds raise.
    orig = idx._test_ec2.describe_instances

    def _raising(*a, **kw):
        if kw.get("InstanceIds"):
            raise RuntimeError("simulated EC2 API outage")
        return orig(*a, **kw)

    idx._test_ec2.describe_instances = _raising
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["deaths"] == 0
    assert "describe_error" in result


def test_reconcile_mode_cannot_collide_with_other_shapes(monkeypatch):
    """mode=reconcile is checked before resolve_run_mode — an event carrying
    both mode=reconcile AND demand-all shapes must still reconcile, not launch."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"}, s3_objects={
        # Add a demand-all event shape that would normally launch — should be
        # ignored because mode=reconcile short-circuits first.
    })
    result = idx.handler({
        "mode": "reconcile",
        "run_mode": "full",
        "model": "some-model",
        "issue_filter": "high-only",
        "trigger": "demand-all",
        "schedule": "should-be-ignored",
    }, None)
    assert result["reconciled"] is True
    # No instance was launched — the demand-all fields were never resolved.


# ── Trigger-health reconciler tests (alpha-engine-config-I4988) ────────────────
# The trigger-health leg screens dispatch-SF executions for their decision-
# record receipts. Same date-proofing discipline as the lane-death tests:
# derive the partition and slot keys from the same clock the reconciler
# uses (now-relative startDates), never literal dates.


def _sfn_exec(hours_ago: float, name: str = "exec1", status: str = "SUCCEEDED"):
    """boto3-shaped execution record with a now-relative startDate."""
    from datetime import datetime, timedelta, timezone
    return {
        "executionArn": f"{_EXEC_PREFIX}:{name}",
        "name": name,
        "status": status,
        "startDate": datetime.now(timezone.utc) - timedelta(hours=hours_ago),
    }


def _decision_key_for_execution(execution: dict, kind: str = "trigger") -> str:
    """The decision-record key the dispatcher would have written for this
    execution's start minute — derive, never hardcode (the date-proofing
    lesson of the lane-death fixtures)."""
    from datetime import datetime, timezone
    started = execution["startDate"].astimezone(timezone.utc)
    return f"groom/decisions/{started:%Y-%m-%d}/{kind}-{started:%H%M}.json"


def test_trigger_health_quiet_when_decision_record_exists(monkeypatch):
    """Mature execution with its trigger decision record → no page."""
    exec_ = _sfn_exec(hours_ago=3)
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={
        _decision_key_for_execution(exec_): b'{"trigger": "demand-all"}',
    })
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 1
    assert th["paged"] == 0
    assert th["missing"] == []


def test_trigger_health_pages_when_execution_left_no_record(monkeypatch):
    """Mature execution with NO decision record → page + actioned marker."""
    exec_ = _sfn_exec(hours_ago=3)
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={})
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 1
    assert th["paged"] == 1
    assert len(th["missing"]) == 1
    assert th["missing"][0]["execution_name"] == "exec1"
    # Actioned marker written so the next 5-min tick does not re-page.
    from datetime import timezone
    started = exec_["startDate"].astimezone(timezone.utc)
    slot_id = f"{started:%Y-%m-%d}-{started:%H%M}"
    assert f"groom/_control/reconciled-trigger/{slot_id}.json" in idx._test_s3._objects


def test_trigger_health_sweep_record_satisfies(monkeypatch):
    """A sweep-mode execution is satisfied by its sweep-{HHMM} record."""
    exec_ = _sfn_exec(hours_ago=3)
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={
        _decision_key_for_execution(exec_, kind="sweep"): b'{"trigger": "sweep"}',
    })
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 1
    assert th["paged"] == 0


def test_trigger_health_skips_execution_within_maturity(monkeypatch):
    """An execution younger than the maturity window is not yet a miss — the
    SF's single retry (60s) plus Lambda latency can still land the record."""
    exec_ = _sfn_exec(hours_ago=0.25)  # 15 min — under the 45-min maturity
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={})
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 0
    assert th["paged"] == 0


def test_trigger_health_actioned_slot_is_not_repaged(monkeypatch):
    """An actioned marker suppresses the re-page on the next tick."""
    exec_ = _sfn_exec(hours_ago=3)
    from datetime import timezone
    started = exec_["startDate"].astimezone(timezone.utc)
    slot_id = f"{started:%Y-%m-%d}-{started:%H%M}"
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={
        f"groom/_control/reconciled-trigger/{slot_id}.json": b'{"outcome": "trigger_death"}',
    })
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 1
    assert th["paged"] == 0
    assert th["missing"] == []


def test_trigger_health_no_executions_is_quiet(monkeypatch):
    idx = _load(monkeypatch, sfn_executions=[], s3_objects={})
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 0
    assert th["paged"] == 0


def test_trigger_health_list_executions_error_is_fail_safe(monkeypatch):
    """A broken SF API must never page — the tick is skipped and retried."""
    idx = _load(monkeypatch, sfn_executions=[], sfn_error=RuntimeError("sfn down"))
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 0
    assert th["paged"] == 0
    assert "error" in th
    # The lane-death leg still ran (its own result unaffected).
    assert result["reconciled"] is True


def test_trigger_health_ignores_stale_execution_outside_lookback(monkeypatch):
    """Executions older than the 30h lookback are outside the check window."""
    exec_ = _sfn_exec(hours_ago=50)
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={})
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 0
    assert th["paged"] == 0


def test_trigger_health_pages_running_execution_without_record(monkeypatch):
    """The 2026-07-28 shape: an execution still RUNNING (SF waiting on a task
    token) but no decision record — the dispatcher died mid-flight. Must page
    even though the execution never reached a terminal state."""
    exec_ = _sfn_exec(hours_ago=3, status="RUNNING")
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={})
    result = idx.handler({"mode": "reconcile"}, None)
    th = result["trigger_health"]
    assert th["checked"] == 1
    assert th["paged"] == 1
    assert th["missing"][0]["execution_status"] == "RUNNING"


# ── F8 scheduled-cycle record (groom-sweep-policy §2.8, alpha-engine-config-
# I6325) ─────────────────────────────────────────────────────────────────────
# Deliverable 1: the scheduler emits a scheduled-cycle record independently
# of the cycle — sourced from the dispatch SF's own execution history, the
# same source the trigger-health leg already reads.


def test_scheduled_cycle_record_written_for_mature_execution(monkeypatch):
    """A mature execution gets a groom/_control/scheduled/{date}/{hhmm}.json
    record, regardless of whether its decision record exists."""
    exec_ = _sfn_exec(hours_ago=3)
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={})
    idx.handler({"mode": "reconcile"}, None)
    from datetime import timezone
    started = exec_["startDate"].astimezone(timezone.utc)
    key = f"groom/_control/scheduled/{started:%Y-%m-%d}/{started:%H%M}.json"
    assert key in idx._test_s3._objects
    body = json.loads(idx._test_s3._objects[key])
    assert body["execution_arn"] == exec_["executionArn"]


def test_scheduled_cycle_record_written_even_when_decision_record_present(monkeypatch):
    """A healthy execution (decision record present, no page) still gets its
    scheduled record — F8's 'scheduled' count must not depend on the cycle's
    own health."""
    exec_ = _sfn_exec(hours_ago=3)
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={
        _decision_key_for_execution(exec_): b'{"trigger": "demand-all"}',
    })
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["trigger_health"]["paged"] == 0
    from datetime import timezone
    started = exec_["startDate"].astimezone(timezone.utc)
    key = f"groom/_control/scheduled/{started:%Y-%m-%d}/{started:%H%M}.json"
    assert key in idx._test_s3._objects


def test_scheduled_cycle_record_idempotent_across_ticks(monkeypatch):
    """A slot already carrying a scheduled record is not overwritten on the
    next tick — the write is a plain existence check, not a refresh."""
    exec_ = _sfn_exec(hours_ago=3)
    from datetime import timezone
    started = exec_["startDate"].astimezone(timezone.utc)
    key = f"groom/_control/scheduled/{started:%Y-%m-%d}/{started:%H%M}.json"
    seeded = json.dumps({"execution_arn": "PRE-EXISTING", "sentinel": True}).encode()
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={key: seeded})
    idx.handler({"mode": "reconcile"}, None)
    assert idx._test_s3._objects[key] == seeded


def test_scheduled_cycle_record_not_written_within_maturity_window(monkeypatch):
    """An execution still within its maturity window is not yet examined at
    all, so no scheduled record is written for it prematurely — mirrors the
    trigger-health leg's own 'checked == 0' behavior for the same input."""
    exec_ = _sfn_exec(hours_ago=0.25)
    idx = _load(monkeypatch, sfn_executions=[exec_], s3_objects={})
    idx.handler({"mode": "reconcile"}, None)
    assert not any(k.startswith("groom/_control/scheduled/") for k in idx._test_s3._objects)


# ── F8 lane-start-health reconciler (alpha-engine-config-I6325) ─────────────
# Deliverable 3 (fast paging leg): a decided lane launch (a dispatch-ledger
# entry that reached EC2) that never wrote its on-box started marker within
# the maturity window pages — the I4987 failure mode, caught in ~15 minutes
# instead of the 6h SF timeout. Distinct population from the trigger-health
# tests above: those cover "the Lambda never decided"; these cover "the
# Lambda decided and launched, but the charter never ran."


def _mature_ledger_entry(run_token: str = "tok1", *, minutes_ago: float = 20,
                         schedule: str = "0 4 * * *") -> bytes:
    from datetime import datetime, timedelta, timezone
    recorded = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
    return json.dumps({
        "run_token": run_token, "tier_tag": "mid-only", "schedule": schedule,
        "instance_id": "i-launched", "deadline_utc": _future_deadline(),
        "recorded_at": recorded,
    }).encode()


def _started_key(run_token: str = "tok1", *, days_ago: int = 0) -> str:
    from datetime import datetime, timedelta, timezone
    day = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    return f"groom/_control/started/{day}/{run_token}.json"


def test_lane_start_health_pages_when_never_started(monkeypatch):
    """A mature, decided, launched lane with no started or completed marker
    pages — the box never reached its charter."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): _mature_ledger_entry(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    lsh = result["lane_start_health"]
    assert lsh["checked"] == 1
    assert lsh["paged"] == 1
    assert "groom/_control/reconciled-lane-start/tok1.json" in idx._test_s3._objects


def test_lane_start_health_quiet_when_started_marker_present(monkeypatch):
    """A started marker (the charter began) satisfies the check — no page."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): _mature_ledger_entry(),
        _started_key(): json.dumps({"mode": "full", "started_at": "2026-08-04T00:00:00Z"}).encode(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    lsh = result["lane_start_health"]
    assert lsh["checked"] == 1
    assert lsh["paged"] == 0


def test_lane_start_health_quiet_when_completed_marker_present(monkeypatch):
    """A completed marker with no started marker still satisfies the check —
    the lane reached SOME terminal outcome; it is not silently stuck."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): _mature_ledger_entry(),
        "groom/_control/completed/tok1.json": json.dumps({"outcome": "failure", "rc": 1}).encode(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    lsh = result["lane_start_health"]
    assert lsh["checked"] == 1
    assert lsh["paged"] == 0


def test_lane_start_health_not_yet_mature_is_not_checked(monkeypatch):
    """A lane decided moments ago (well under the maturity window) is not yet
    a miss — bootstrap (dnf install + git clone) legitimately takes minutes."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): _mature_ledger_entry(minutes_ago=2),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    lsh = result["lane_start_health"]
    assert lsh["checked"] == 0
    assert lsh["paged"] == 0


def test_lane_start_health_actioned_lane_is_not_repaged(monkeypatch):
    """An already-actioned never-started lane is not re-paged on the next tick."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): _mature_ledger_entry(),
        "groom/_control/reconciled-lane-start/tok1.json": json.dumps(
            {"outcome": "lane_never_started"}).encode(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    lsh = result["lane_start_health"]
    assert lsh["checked"] == 1
    assert lsh["paged"] == 0


def test_lane_start_health_ignores_pre_launch_only_ledger_entry(monkeypatch):
    """config#3173's FIRST (pre-launch) ledger write carries no instance_id —
    there is no EC2 request yet to check a started marker against, so it must
    never be counted or paged."""
    from datetime import datetime, timezone
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 4 * * *",
            "instance_id": "", "deadline_utc": "",
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }).encode(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    lsh = result["lane_start_health"]
    assert lsh["checked"] == 0
    assert lsh["paged"] == 0


def test_lane_start_health_listing_error_is_fail_safe(monkeypatch):
    """A broken S3 listing must never page — the tick is skipped and retried.

    Calls the reconciler leg directly (not through handler({"mode":
    "reconcile"})) because `_reconcile_lane_death` reads the SAME dispatch-
    ledger prefix earlier in that dispatch chain and has no fail-safe wrapper
    of its own around its listing call — patching `get_paginator` globally
    would fail that unrelated, earlier leg instead of exercising this one.
    """
    idx = _load(monkeypatch, s3_objects={_ledger_key(): _mature_ledger_entry()})

    class _RaisingPaginator:
        def paginate(self, **kw):
            raise RuntimeError("simulated S3 outage")

    idx._test_s3.get_paginator = lambda name: _RaisingPaginator()
    result = idx._reconcile_lane_start_health()
    assert result["checked"] == 0
    assert result["paged"] == 0


def test_lane_start_health_batches_multiple_lanes_into_one_page(monkeypatch):
    """2026-08-04 incident: this leg's first-ever run found a 2-day backlog of
    27 never-started lanes and paged each individually, flooding the operator
    channel and plausibly tripping Telegram's own rate limit. N discoveries in
    one cycle must produce exactly ONE notify call, not N."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok1"): _mature_ledger_entry("tok1"),
        _ledger_key("tok2"): _mature_ledger_entry("tok2"),
        _ledger_key("tok3"): _mature_ledger_entry("tok3"),
    })
    notified = _spy_notify(monkeypatch, idx)
    result = idx._reconcile_lane_start_health()
    assert result["checked"] == 3
    assert result["paged"] == 3
    assert len(notified) == 1  # one digest, not three individual pages
    text, kw = notified[0]
    assert "3 groom lane(s) NEVER STARTED" in text
    assert "tok1" in text and "tok2" in text and "tok3" in text
    assert kw["context"]["count"] == 3
    # Each lane is still individually actioned, so a later tick doesn't
    # re-discover (and re-page) any of them.
    for tok in ("tok1", "tok2", "tok3"):
        assert f"groom/_control/reconciled-lane-start/{tok}.json" in idx._test_s3._objects


def test_reconcile_sends_task_failure_for_dead_lane_with_token(monkeypatch):
    """Lane death with a task_token → send-task-failure collapses the hung SF
    execution immediately instead of waiting for the 6h timeout."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key(): json.dumps({
            "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
            "instance_id": "i-dead", "deadline_utc": _future_deadline(),
            "task_token": "SF_TOKEN_DEAD_LANE",
        }).encode(),
    })
    idx._test_ec2._instance_states["i-dead"] = "terminated"

    result = idx.handler({"mode": "reconcile"}, None)
    assert result["deaths"] == 1
    assert len(idx._test_sfn.send_task_failure_calls) == 1
    assert idx._test_sfn.send_task_failure_calls[0]["taskToken"] == "SF_TOKEN_DEAD_LANE"
    assert idx._test_sfn.send_task_failure_calls[0]["error"] == "LaneDeath"
    assert "i-dead" in idx._test_sfn.send_task_failure_calls[0]["cause"]


# ── Cycle singleton (alpha-engine-config-I5371) ──────────────────────────────
#
# The dispatch SF ran TimeoutSeconds=72000 (20h) against an 8h trigger cadence
# with no singleton state, so up to three cycles could be alive at once BY
# CONSTRUCTION. Measured live 2026-07-29: two executions RUNNING at the same
# time (both hung on their task-token callback) plus a third that ran 18h
# before failing; each enumerated the backlog independently and dispatched an
# agent at the same issue, producing 19 duplicate PRs across 16 clusters.


def _decide_evt(arn_suffix="c", **extra):
    return {"run_mode": "full", "model": "deepseek-v4-flash",
            "issue_filter": "gated-reverify", "decide_only": True,
            "executionArn": f"{_EXEC_PREFIX}:{arn_suffix}", **extra}


def test_cycle_singleton_proceeds_when_only_self_is_running(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                sfn_executions=[_exec(f"{_EXEC_PREFIX}:c", 5)])
    out = idx.handler(_decide_evt("c"), None)
    assert out["decide"]["launches"] == [{"model": "deepseek-v4-flash",
                                          "issue_filter": "gated-reverify"}]
    # It queried the right state machine, derived from our own execution ARN.
    assert idx._test_sfn.calls[0]["stateMachineArn"] == _SM
    assert idx._test_sfn.calls[0]["statusFilter"] == "RUNNING"


def test_cycle_singleton_skips_when_an_earlier_cycle_is_running(monkeypatch):
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                sfn_executions=[_exec(f"{_EXEC_PREFIX}:older", 1),
                                _exec(f"{_EXEC_PREFIX}:c", 5)])

    def _launch(types_, subnets, **kw):
        raise AssertionError("a skipped cycle must never launch a box")
    monkeypatch.setattr(idx.spot_dispatch, "launch_with_fallback", _launch)

    d = idx.handler(_decide_evt("c"), None)["decide"]
    assert d["launches"] == []
    # §2.4: the exit is NAMED, never a silent empty result.
    assert d["reason"] == "concurrent_cycle_skip"
    assert d["blocking_executions"] == ["older"]


def test_cycle_singleton_oldest_wins_so_the_running_cycle_is_not_preempted(monkeypatch):
    """We are the OLDEST live cycle — a newer sibling must not block us."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                sfn_executions=[_exec(f"{_EXEC_PREFIX}:c", 1),
                                _exec(f"{_EXEC_PREFIX}:newer", 9)])
    d = idx.handler(_decide_evt("c"), None)["decide"]
    assert d["launches"], "the oldest live cycle must proceed"
    assert "reason" not in d or d.get("reason") != "concurrent_cycle_skip"


def test_cycle_singleton_breaks_a_startdate_tie_by_arn_so_exactly_one_survives(monkeypatch):
    """Two cycles at the identical startDate: the (startDate, arn) total order
    must let exactly one through, never both and never neither."""
    same = [_exec(f"{_EXEC_PREFIX}:aaa", 3), _exec(f"{_EXEC_PREFIX}:bbb", 3)]
    survivors = []
    for me in ("aaa", "bbb"):
        idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                    sfn_executions=same)
        d = idx.handler(_decide_evt(me), None)["decide"]
        if d["launches"]:
            survivors.append(me)
    assert survivors == ["aaa"], f"exactly one cycle must survive, got {survivors}"


def test_cycle_singleton_fails_closed_when_the_probe_errors(monkeypatch):
    """A probe failure means we do not KNOW. A wrongly-skipped cycle self-heals
    on the next 8h trigger; a wrongly-launched concurrent cycle leaves duplicate
    PRs a human must disentangle. Asymmetric — so skip."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                sfn_error=RuntimeError("AccessDeniedException"))

    def _launch(types_, subnets, **kw):
        raise AssertionError("an indeterminate probe must never launch")
    monkeypatch.setattr(idx.spot_dispatch, "launch_with_fallback", _launch)

    d = idx.handler(_decide_evt("c"), None)["decide"]
    assert d["launches"] == []
    assert d["reason"] == "concurrent_cycle_probe_failed"
    assert "AccessDeniedException" in d["detail"]


def test_cycle_singleton_fails_closed_when_self_absent_from_running_set(monkeypatch):
    """We are executing, so we MUST be in our own RUNNING set. Absence means the
    listing is not describing reality — refuse to conclude 'no siblings'."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
                sfn_executions=[_exec(f"{_EXEC_PREFIX}:someone-else", 4)])
    d = idx.handler(_decide_evt("c"), None)["decide"]
    assert d["launches"] == []
    assert d["reason"] == "concurrent_cycle_probe_failed"


def test_cycle_singleton_rejects_a_malformed_execution_arn(monkeypatch):
    """A silently-wrong ARN would make list_executions return an empty set,
    which reads exactly like 'no sibling cycles' — the §2.4 absence-as-benign
    failure this guard exists to prevent."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    d = idx.handler(_decide_evt("c", executionArn="not-an-arn"), None)["decide"]
    assert d["launches"] == []
    assert d["reason"] == "concurrent_cycle_probe_failed"


def test_cycle_singleton_is_inert_without_an_execution_arn(monkeypatch):
    """A direct/legacy `aws lambda invoke` has no SF context, so there is no
    cycle to be a sibling OF — a real absence, not an unknown. It must not
    consult Step Functions at all."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
                       "issue_filter": "gated-reverify", "decide_only": True}, None)
    assert out["decide"]["launches"]
    assert idx._test_sfn.calls == []


def test_cycle_singleton_does_not_block_launch_decided(monkeypatch):
    """`launch_decided` is a per-lane launch WITHIN an already-decided cycle
    (including the SF's relaunch retries and the unconditional end-of-SF sweep).
    Blocking it would let the guard cancel the surviving cycle's own lanes."""
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(kw)
        return "i-lane"
    idx = _load(monkeypatch, launch_impl=_launch,
                env={"GROOM_DISPATCH_ENABLED": "true"},
                sfn_executions=[_exec(f"{_EXEC_PREFIX}:older", 1),
                                _exec(f"{_EXEC_PREFIX}:c", 5)])
    out = idx.handler({"run_mode": "full", "model": "deepseek-v4-flash",
                       "issue_filter": "mid-only", "launch_decided": True,
                       "executionArn": f"{_EXEC_PREFIX}:c"}, None)
    assert out["groom"]["launched"] is True
    assert idx._test_sfn.calls == [], "launch_decided must not consult the singleton"


def test_reconcile_sees_yesterdays_partition(monkeypatch):
    """A lane registered before UTC midnight is still reconciled after it.

    The ledger is date-partitioned. A box launched at 23:50 UTC that dies at
    00:10 has its expectation filed under YESTERDAY, and a reconciler scanning
    only today would report `open_expectations: 0` — perfectly healthy-looking,
    every single day, for a window as wide as the lane budget. Fixed alongside
    the date-pinned fixtures (alpha-engine-config-I5229).
    """
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok-yesterday", days_ago=1): json.dumps({
            "run_token": "tok-yesterday", "tier_tag": "mid-only",
            "schedule": "0 1 * * *", "instance_id": "i-dead",
            "deadline_utc": _future_deadline(),
        }).encode(),
    })
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["open_expectations"] == 1, (
        "an expectation filed before UTC midnight became invisible to the reconciler"
    )
    assert result["deaths"] == 1


def test_reconcile_counts_a_token_once_across_partitions(monkeypatch):
    """The same run_token under both days is one expectation, not two."""
    body = json.dumps({
        "run_token": "tok1", "tier_tag": "mid-only", "schedule": "0 1 * * *",
        "instance_id": "i-dead", "deadline_utc": _future_deadline(),
    }).encode()
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok1", days_ago=0): body,
        _ledger_key("tok1", days_ago=1): body,
    })
    result = idx.handler({"mode": "reconcile"}, None)
    assert result["open_expectations"] == 1
    assert result["deaths"] == 1, "one dead lane must not page twice"


def test_reconciler_fixtures_are_not_date_pinned():
    """Meta-test: no reconciler fixture may hardcode a ledger partition date.

    A date-pinned fixture inside a rolling window passes on the day it is
    written and asserts nothing thereafter — silently, because the test still
    reports green. Five of these sat dead in this file from 2026-07-28 until
    2026-07-30. The class is cheap to exclude permanently, so exclude it.
    """
    import re
    from pathlib import Path

    source = Path(__file__).read_text(encoding="utf-8")
    # Strip comments so the explanatory prose above may name the original date.
    code = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    pinned = re.findall(r'dispatch-ledger/\d{4}-\d{2}-\d{2}', code)
    assert not pinned, (
        f"date-pinned ledger partition(s) in test fixtures: {sorted(set(pinned))} — "
        "derive the partition from the clock via _ledger_key() instead"
    )


def test_reconciler_pages_a_death_only_once(monkeypatch):
    """An actioned expectation is a CLOSED expectation.

    Brian, 2026-07-30: "why are the groom lane death error messages repeating?
    they should be sent one time only." The reconciler paged and sent
    send-task-failure but recorded nothing, so every 5-minute tick re-detected
    the same dead lane and paged again. The per-run_token dedup key on the
    notification did not hold across invocations.
    """
    ledger = {
        _ledger_key("tok-dead"): json.dumps({
            "run_token": "tok-dead", "tier_tag": "mid-only",
            "schedule": "0 20 * * *", "instance_id": "i-dead",
            "deadline_utc": _future_deadline(),
        }).encode(),
    }
    idx = _load(monkeypatch, s3_objects=dict(ledger))

    first = idx.handler({"mode": "reconcile"}, None)
    assert first["deaths"] == 1, "the first tick must detect and page the death"

    # The second tick runs against the SAME bucket the first one wrote to.
    second = idx.handler({"mode": "reconcile"}, None)
    assert second["deaths"] == 0, (
        "the reconciler re-detected an already-actioned lane death — this is "
        "the repeating-alert defect: it pages every 5 minutes until the ledger "
        "entry ages out of the scan window"
    )
    assert second["open_expectations"] == 0


def test_actioned_marker_is_not_written_to_the_completed_prefix(monkeypatch):
    """A death must not be indistinguishable from a clean completion.

    `completed/` means "the box finished its work and said so". Writing a
    reclaimed lane into it would corrupt that meaning for every other consumer,
    so the reconciler uses its own prefix.
    """
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok-dead"): json.dumps({
            "run_token": "tok-dead", "tier_tag": "mid-only",
            "schedule": "0 20 * * *", "instance_id": "i-dead",
            "deadline_utc": _future_deadline(),
        }).encode(),
    })
    idx.handler({"mode": "reconcile"}, None)
    written = getattr(idx._test_s3, "_objects", {})
    completed = [k for k in written if "_control/completed/" in k]
    actioned = [k for k in written if "_control/reconciled/" in k]
    assert not completed, (
        f"reconciler wrote into the completed/ prefix: {completed} — a dead "
        "lane would read as a successful one"
    )
    assert actioned, "no reconciled/ marker written; the expectation stays open"


# ── Spot retry ladder: instance-type rotation (alpha-engine-config-I5923) ─────
#
# Brian ruling 2026-07-31: "we should be attempting different instance types
# before using on demand. at least two different types. otherwise we are
# practically defaulting to on demand during prime time."
#
# `krepis.ec2_spot.launch` walks `for instance_type in instance_types: for
# subnet_id in subnets` in FIXED order and rotates only on a launch-time
# capacity ERROR. A mid-run reclamation is not a launch error, so before this
# change every relaunch restarted at the head of the pool — the exact type that
# had just proved exhausted. The state-machine half (max_retries, the attempt
# counter on both relaunch paths) is pinned in
# tests/test_groom_instance_type_rotation.py.

def test_rotation_changes_the_leading_type_each_attempt(monkeypatch):
    """Consecutive attempts for one lane must not lead with the same type."""
    idx = _load(monkeypatch)
    leads = [idx._rotated_instance_types("mid-only", n)[0] for n in range(3)]
    assert len(set(leads)) == 3, (
        f"attempts 0..2 lead with {leads} — a relaunch must not re-enter the "
        "capacity pool that just failed"
    )


def test_rotation_is_a_rotation_not_a_truncation(monkeypatch):
    """Every type stays reachable on every attempt.

    A genuinely scarce window must still be able to walk the whole pool before
    the caller escalates to on-demand; the rotation changes ORDER, not
    membership.
    """
    idx = _load(monkeypatch)
    for attempt in range(len(idx.INSTANCE_TYPES) + 2):
        rotated = idx._rotated_instance_types("mid-only", attempt)
        assert sorted(rotated) == sorted(idx.INSTANCE_TYPES)


def test_co_launched_lanes_lead_with_different_types(monkeypatch):
    """alpha-engine-config-I4989 — the three lanes must not converge.

    They co-launch with MaxConcurrency=3; sharing one ordered pool put all three
    on the same type in the same AZ, so one capacity event took the whole cycle
    (measured 2026-07-30 and again 2026-07-31).
    """
    idx = _load(monkeypatch)
    leads = {
        lane: idx._rotated_instance_types(lane, 0)[0]
        for lane in ("low-only", "mid-only", "high-only")
    }
    assert len(set(leads.values())) == 3, f"lanes converge on one pool: {leads}"


def test_rotation_survives_an_absent_or_malformed_attempt(monkeypatch):
    """Degrade to the pre-rotation order, never raise.

    The value only selects WHICH pool is tried first, so a launch that does not
    happen is strictly worse than one starting at the wrong offset.
    """
    idx = _load(monkeypatch)
    assert idx._resolve_attempt({}) == 0
    assert idx._resolve_attempt({"attempt": None}) == 0
    assert idx._resolve_attempt({"attempt": "not-a-number"}) == 0
    assert idx._resolve_attempt({"attempt": -5}) == 0
    assert idx._resolve_attempt({"attempt": "2"}) == 2
    assert idx._rotated_instance_types("unknown-lane", 0) == idx.INSTANCE_TYPES


def test_launch_passes_the_rotated_pool_to_the_launcher(monkeypatch):
    """End-to-end: the rotation must actually reach ec2_spot.launch.

    A rotation computed and then discarded at the call site is the failure this
    guards — the whole defect being fixed is that the launcher was always handed
    the same fixed-order list.
    """
    seen = {}

    def _launch(types_, subnets, **kw):
        seen["types"] = list(types_)
        return "i-rot"

    idx = _load(monkeypatch, launch_impl=_launch,
                env={"GROOM_DISPATCH_ENABLED": "true"})
    idx.handler({"run_mode": "full", "schedule": "0 20 * * *",
                 "issue_filter": "mid-only", "launch_decided": True,
                 "attempt": 2}, None)
    expected = idx._rotated_instance_types("mid-only", 2)
    assert seen["types"] == expected, (
        f"launcher received {seen.get('types')}, expected the rotated pool "
        f"{expected} — the rotation was computed and discarded"
    )


def test_pool_spans_more_than_one_instance_family(monkeypatch):
    """Two types in one family is diversification in name only (I4989)."""
    idx = _load(monkeypatch)
    families = {t.split(".")[0] for t in idx.INSTANCE_TYPES}
    assert len(families) >= 3, (
        f"pool spans only {families} — separate capacity pools require separate "
        "FAMILIES, not just separate sizes"
    )


# ── Completion-aware lane classification (alpha-engine-config-I5914) ──────────
#
# Instance state is not a proxy for run completion. A lane that finished its
# work and was reclaimed during wind-down — before writing its completion
# marker — is dead by EC2 state and successful by every measure that matters.
#
# Measured 2026-07-31 20:00 UTC: the high-only lane wrote
# groom/2026-07-31/303ace0ac87148e0a21c5ba235bb5f4e.json at 20:35:50Z with
# "FINAL: 4 closed, engaged=10/10, stop='queue drained'" and rc=0, was reclaimed
# at 20:38Z, and was paged as a 🔴 lane DEATH indistinguishable from the sibling
# lane that genuinely lost four chunks of work.

def _artifact_key(run_token: str, *, days_ago: int = 0) -> str:
    from datetime import datetime, timedelta, timezone
    day = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    return f"groom/{day}/{run_token}.json"


def _dead_lane_objects(run_token: str, **extra) -> dict:
    objs = {
        _ledger_key(run_token): json.dumps({
            "run_token": run_token, "tier_tag": "high-only",
            "schedule": "0 20 * * *", "instance_id": "i-dead",
            "deadline_utc": _future_deadline(),
        }).encode(),
    }
    objs.update(extra)
    return objs


def test_completed_lane_reclaimed_post_run_is_not_paged_as_a_death(monkeypatch):
    """The 2026-07-31 high-only case, replayed."""
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects(
        "tok-done", **{_artifact_key("tok-done"): b'{"final": true}'}))
    notifications = _spy_notify(monkeypatch, idx)
    out = idx.handler({"mode": "reconcile"}, None)

    assert out["deaths"] == 0, "a completed lane must not count as a death"
    assert out["reclaimed_post_run"] == 1
    assert len(notifications) == 1
    text, kw = notifications[0]
    assert kw["severity"] == "warning", (
        "a lane that completed its work must not page at error severity"
    )
    assert kw["silent"] is True, (
        "2026-08-03 notification cleanup: reclaimed-post-run is recorded, not "
        "buzzed — no work was lost and it is in the reconcile ledger"
    )
    assert "DEATH" not in text
    assert "completing" in text or "completed" in text


def test_lane_with_no_artifact_still_pages_as_a_death(monkeypatch):
    """The genuine-loss case must keep its existing behaviour exactly."""
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects("tok-lost"))
    notifications = _spy_notify(monkeypatch, idx)
    out = idx.handler({"mode": "reconcile"}, None)

    assert out["deaths"] == 1
    assert out["reclaimed_post_run"] == 0
    text, kw = notifications[0]
    assert kw["severity"] == "error"
    assert "DEATH" in text


def test_actioned_marker_records_the_true_outcome(monkeypatch):
    """Every consumer of this key was being told `lane_died` for a success."""
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects(
        "tok-done", **{_artifact_key("tok-done"): b"{}"}))
    _spy_notify(monkeypatch, idx)
    idx.handler({"mode": "reconcile"}, None)

    written = getattr(idx._test_s3, "_objects", {})
    key = "groom/_control/reconciled/tok-done.json"
    assert key in written, f"expectation not closed; wrote {list(written)}"
    record = json.loads(written[key].decode() if isinstance(written[key], bytes)
                        else written[key])
    assert record["outcome"] == "lane_reclaimed_post_run"
    assert record["completion_evidence"], "the proving key must be recorded"


def test_completion_probe_covers_the_previous_day_partition(monkeypatch):
    """The artifact is partitioned by the RUN's UTC date, not the tick's.

    A lane launched at 23:5x writes to the previous day's prefix; probing only
    today would re-page it as a death every tick until the ledger aged out.
    """
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects(
        "tok-x", **{_artifact_key("tok-x", days_ago=1): b"{}"}))
    _spy_notify(monkeypatch, idx)
    out = idx.handler({"mode": "reconcile"}, None)
    assert out["reclaimed_post_run"] == 1 and out["deaths"] == 0


def test_completion_marker_alone_still_proves_completion(monkeypatch):
    """The pre-existing signal keeps working — the artifact is additive."""
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok-m"): json.dumps({
            "run_token": "tok-m", "tier_tag": "mid-only",
            "schedule": "0 20 * * *", "instance_id": "i-dead",
            "deadline_utc": _future_deadline(),
        }).encode(),
        "groom/_control/completed/tok-m.json": b'{"outcome": "success"}',
    })
    notifications = _spy_notify(monkeypatch, idx)
    out = idx.handler({"mode": "reconcile"}, None)
    # A completion marker closes the expectation BEFORE the death check, so this
    # lane is never even an open expectation.
    assert out["deaths"] == 0 and not notifications


# ── The SF's relaunch guard is real now (alpha-engine-config-I5914) ───────────
#
# CheckCompletionMarkerTaskToken has existed since config#1645 writing
# $.markerResult that NOTHING read: CheckRetryBudget branched on retry_count vs
# max_retries alone. The state's NAME asserted a guard the machine did not have.

def test_retry_marker_reports_completion_for_a_finished_lane(monkeypatch):
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok-fin"): json.dumps({
            "run_token": "tok-fin", "tier_tag": "high-only",
            "schedule": "0 20 * * *", "instance_id": "i-1",
        }).encode(),
        _artifact_key("tok-fin"): b"{}",
    })
    out = idx.handler({"retryMarker": True, "run_mode": "full",
                       "launchDecision": {"issue_filter": "high-only"}}, None)
    assert out["lane_completed"] is True
    assert out["run_token"] == "tok-fin"
    assert out["evidence"]


def test_retry_marker_reports_incomplete_for_a_truncated_lane(monkeypatch):
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok-cut"): json.dumps({
            "run_token": "tok-cut", "tier_tag": "high-only",
            "schedule": "0 20 * * *", "instance_id": "i-1",
        }).encode(),
    })
    out = idx.handler({"retryMarker": True, "run_mode": "full",
                       "launchDecision": {"issue_filter": "high-only"}}, None)
    assert out["lane_completed"] is False, (
        "a lane with no artifact must stay relaunchable"
    )


def test_retry_marker_resolves_the_lane_from_launch_decision(monkeypatch):
    """The lane identity is NESTED under launchDecision, not top-level.

    Reading `event["issue_filter"]` resolves empty for every lane, so all three
    would share one tier_tag and answer for each other.
    """
    idx = _load(monkeypatch, s3_objects={
        _ledger_key("tok-high"): json.dumps({
            "run_token": "tok-high", "tier_tag": "high-only",
            "schedule": "0 20 * * *", "instance_id": "i-1",
        }).encode(),
        _artifact_key("tok-high"): b"{}",
    })
    # Asking about a DIFFERENT lane must not match high-only's artifact.
    out = idx.handler({"retryMarker": True, "run_mode": "full",
                       "launchDecision": {"issue_filter": "low-only"}}, None)
    assert out["tier_tag"] == "low-only"
    assert out["lane_completed"] is False


def test_retry_marker_fails_soft_when_the_ledger_is_unreadable(monkeypatch):
    """An unreadable ledger must not suppress a legitimate relaunch."""
    idx = _load(monkeypatch, s3_objects={})
    out = idx.handler({"retryMarker": True, "run_mode": "full",
                       "launchDecision": {"issue_filter": "mid-only"}}, None)
    assert out["lane_completed"] is False


# ── The page must name the dispatcher's reason, not the EC2 state (I6199) ─────
#
# On 2026-08-03 eleven boxes were terminated by THIS Lambda after
# `RuntimeError: SSM agent not Online after 180s`, and every page read
# `instance is shutting-down — not in ('pending', 'running')`. EC2 state is a
# proxy: `shutting-down` is equally true of a spot reclaim, a completed run
# winding down, and a dispatcher terminate after a failed bootstrap. Two groom
# cycles were spent looking for a groom defect that did not exist.

_SSM_TIMEOUT_REASON = "RuntimeError: SSM agent not Online after 180s for i-dead"


def _tag_dead_lane(idx, reason: str = _SSM_TIMEOUT_REASON) -> None:
    idx._test_ec2._instance_tags["i-dead"] = [
        {"Key": "Name", "Value": "alpha-engine-groom-spot"},
        {"Key": "termination-reason", "Value": reason},
        {"Key": "termination-source", "Value": "dispatcher"},
    ]


def test_lane_death_page_names_the_dispatcher_reason_not_the_instance_state(monkeypatch):
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects("tok-ssm"))
    _tag_dead_lane(idx)
    notifications = _spy_notify(monkeypatch, idx)

    out = idx.handler({"mode": "reconcile"}, None)

    assert out["deaths"] == 1
    text, kw = notifications[0]
    assert kw["severity"] == "error"
    assert "SSM agent not Online after 180s" in text, (
        f"page must carry the reason the dispatcher recorded, got: {text}"
    )
    assert "not in ['pending', 'running']" not in text, (
        "page must not restate the EC2 state as if it were a diagnosis"
    )


def test_lane_death_page_records_the_dispatcher_reason_on_the_actioned_marker(monkeypatch):
    """Every downstream consumer of the marker reads `reason` too."""
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects("tok-ssm2"))
    _tag_dead_lane(idx)
    _spy_notify(monkeypatch, idx)

    idx.handler({"mode": "reconcile"}, None)

    marker = json.loads(
        idx._test_s3._objects["groom/_control/reconciled/tok-ssm2.json"].decode()
    )
    assert marker["outcome"] == "lane_died"
    assert "SSM agent not Online" in marker["reason"]


def test_lane_death_falls_back_to_instance_state_when_untagged(monkeypatch):
    """A spot reclaim carries no dispatcher tag — behaviour must be unchanged."""
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects("tok-reclaim"))
    notifications = _spy_notify(monkeypatch, idx)

    out = idx.handler({"mode": "reconcile"}, None)

    assert out["deaths"] == 1
    text, _ = notifications[0]
    assert "is terminated" in text
    assert "dispatcher terminated" not in text


def test_launch_failure_tags_the_instance_with_the_real_exception(monkeypatch):
    """The producer half: the reason must reach EC2 before the terminate."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    monkeypatch.setattr(
        idx, "_wait_ssm_online",
        lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("SSM agent not Online after 180s for i-test")
        ),
    )

    with pytest.raises(RuntimeError):
        idx.handler({"run_mode": "full", "schedule": "0 4 * * *"}, None)

    assert idx._test_ec2.terminated, "a box whose bootstrap never landed must be torn down"
    tagged = {t["Key"]: t["Value"]
              for _, tags in idx._test_ec2.tags_created for t in tags}
    assert "SSM agent not Online after 180s" in tagged.get("termination-reason", "")
    assert tagged.get("termination-source") == "dispatcher"


# ── Alert bodies must not carry a credential-shaped run token ────────────────
#
# A bare `run_token=<32 lowercase hex>` matches gitleaks' stock `generic-api-key`
# rule. These bodies travel onto the Overseer intake bus, alert-drain reads the
# queue into its LLM request, and the DLP egress proxy scans that outbound body —
# so one lane-death alert returns HTTP 400, the drain exits rc=1, and because
# nothing is deleted from the queue without a ledger record the SAME message
# wedges every subsequent run. Measured 2026-08-03: 12 of 79 sampled intake
# messages, all this emitter, all this field, four consecutive drain runs dead.

_HEX32_TOKEN = "af87ec3d9b1e4c7a2f60d583be914c02"


def _lane_death_body(monkeypatch, token):
    idx = _load(monkeypatch, s3_objects=_dead_lane_objects(token))
    _tag_dead_lane(idx)
    notifications = _spy_notify(monkeypatch, idx)
    idx.handler({"mode": "reconcile"}, None)
    return notifications[0]


def test_lane_death_body_truncates_the_run_token(monkeypatch):
    """The rendered body must not contain the full 32-char token."""
    text, _kw = _lane_death_body(monkeypatch, _HEX32_TOKEN)
    assert _HEX32_TOKEN not in text, (
        "the full run_token reached the notification body; gitleaks' "
        "generic-api-key rule matches `run_token=<32 hex>` and the DLP egress "
        "proxy will 400 every alert-drain run that reads this message"
    )
    assert _HEX32_TOKEN[:12] in text, (
        "the truncated prefix must survive — it is what correlates a body "
        "against the dispatch ledger by eye"
    )


def test_lane_death_dedup_key_keeps_the_full_token(monkeypatch):
    """Truncation is a RENDERING change; machine-read fields are untouched.

    The dedup key is what collapses repeated pages for one dead lane into one
    alert. Truncating it there would widen the key and could collide two lanes
    sharing a 12-char prefix.
    """
    _text, kw = _lane_death_body(monkeypatch, _HEX32_TOKEN)
    assert kw["dedup_key"].endswith(_HEX32_TOKEN), (
        f"dedup_key lost the full token: {kw['dedup_key']}"
    )


def test_short_token_is_rendered_whole(monkeypatch):
    """No ellipsis on a token that was never long enough to look like a key."""
    text, _kw = _lane_death_body(monkeypatch, "tok-short")
    assert "run_token=tok-short " in text or "run_token=tok-short\n" in text, (
        f"a short token must render verbatim, got: {text}"
    )


# ── alpha-engine-config-I6460 (§5.9): per-lane singleton dispatch lease ──────
# groom-sweep-policy.md §5.9: "The loop holds a singleton lease per lane. A
# dispatch that cannot take the lease exits, recording that it yielded; it
# does not queue behind the holder and it does not run beside it." The lease
# PRIMITIVE itself (acquire/TTL-self-recovery/force-override) is unit-tested
# directly in nousergon-lib's own test_dispatch_lease.py — these tests pin
# the DISPATCHER's behavior around that primitive: what it does when the
# lease can/cannot be acquired, and that it never bypasses the mechanism.
#
# `idx.dispatch_lease` is the real `nousergon_lib.dispatch_lease` module
# (not stubbed — only ec2_spot/boto3/github_app are hermetic stubs here), so
# these tests monkeypatch its `acquire_lease`/`release_lease` functions
# directly per groom-sweep-conformance's own "mock the lease backend"
# guidance rather than relying on `_FakeS3`, which does not implement S3's
# `IfNoneMatch` conditional-PUT semantics at all (every `put_object` call
# unconditionally succeeds) and so cannot exercise a real acquire conflict.

def test_lane_lease_yield_when_held_by_another(monkeypatch):
    """The core §5.9 property applied to the actual dispatch path: when the
    per-lane lease cannot be acquired, the dispatcher yields IMMEDIATELY —
    it never queues, never launches, and records why."""
    launched = []
    idx = _load(
        monkeypatch,
        launch_impl=lambda types_, subnets, **kw: launched.append(True) or "i-should-never-launch",
        env={"GROOM_DISPATCH_ENABLED": "true"},
    )
    holder = idx.dispatch_lease.LeaseHolder(
        owner_id="other-cycle", started_at="2026-08-04T00:00:00Z",
        ttl_epoch=9_999_999_999, hostname="box-y", pid=2,
    )

    def _acquire(lock_key, *, owner_id, ttl_seconds, bucket, s3_client=None, force=False):
        return idx.dispatch_lease.LeaseAcquireResult(acquired=False, holder=holder)

    monkeypatch.setattr(idx.dispatch_lease, "acquire_lease", _acquire)

    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    g = out["groom"]
    assert g["launched"] is False
    assert g["reason"] == "lane_lease_yielded"
    assert g["lease_holder_owner_id"] == "other-cycle"
    assert launched == []  # never attempted a spot launch — zero spend


def test_lane_lease_acquired_allows_normal_launch(monkeypatch):
    """A successfully-acquired lease does not block or alter a normal
    launch — the happy path is unchanged."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"})
    calls = []

    def _acquire(lock_key, *, owner_id, ttl_seconds, bucket, s3_client=None, force=False):
        calls.append({"lock_key": lock_key, "force": force})
        holder = idx.dispatch_lease.LeaseHolder(
            owner_id=owner_id, started_at="x", ttl_epoch=1, hostname="h", pid=1)
        return idx.dispatch_lease.LeaseAcquireResult(acquired=True, holder=holder)

    monkeypatch.setattr(idx.dispatch_lease, "acquire_lease", _acquire)
    monkeypatch.setattr(idx.dispatch_lease, "release_lease", lambda *a, **kw: None)

    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert out["groom"]["launched"] is True
    # Both leases were taken: the lane lease, and the shared spot-launch
    # lease wrapping the RunInstances call.
    lock_keys = {c["lock_key"] for c in calls}
    assert "locks/groom-lane-mid-only.lock" in lock_keys
    assert "locks/groom-spot-capacity-pool.lock" in lock_keys
    # attempt=0 (no live-EC2 proof of a dead holder) — never forces.
    assert all(c["force"] is False for c in calls)


def test_lane_lease_relaunch_attempt_forces_override(monkeypatch):
    """A bounded SF relaunch (attempt > 0) whose prior box was confirmed
    termination-imminent must pass force=True for the LANE lease — it has
    independent, live proof the recorded holder is dead."""
    idx = _load(
        monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true"},
        running_tier_instances={"low-only": ["i-dying"]},
        spot_reclaimed={"i-dying": _reclaimed_sir("i-dying")},
    )
    lane_calls = []

    def _acquire(lock_key, *, owner_id, ttl_seconds, bucket, s3_client=None, force=False):
        if lock_key == "locks/groom-lane-low-only.lock":
            lane_calls.append(force)
        holder = idx.dispatch_lease.LeaseHolder(
            owner_id=owner_id, started_at="x", ttl_epoch=1, hostname="h", pid=1)
        return idx.dispatch_lease.LeaseAcquireResult(acquired=True, holder=holder)

    monkeypatch.setattr(idx.dispatch_lease, "acquire_lease", _acquire)
    monkeypatch.setattr(idx.dispatch_lease, "release_lease", lambda *a, **kw: None)

    out = idx.handler(
        {"run_mode": "full", "issue_filter": "low-only", "schedule": "x", "attempt": 1}, None)
    assert out["groom"]["launched"] is True
    assert lane_calls == [True]


def test_lane_lease_released_on_dispatch_ceiling_skip(monkeypatch):
    """A lease acquired but then not used (daily ceiling exhausted) must be
    released immediately rather than blocking the lane for the full TTL."""
    idx = _load(monkeypatch, env={"GROOM_DISPATCH_ENABLED": "true", "GROOM_MAX_DISPATCHES_DAILY": "0"})
    released = []

    def _acquire(lock_key, *, owner_id, ttl_seconds, bucket, s3_client=None, force=False):
        holder = idx.dispatch_lease.LeaseHolder(
            owner_id=owner_id, started_at="x", ttl_epoch=1, hostname="h", pid=1)
        return idx.dispatch_lease.LeaseAcquireResult(acquired=True, holder=holder)

    def _release(lock_key, *, bucket, s3_client=None):
        released.append(lock_key)

    monkeypatch.setattr(idx.dispatch_lease, "acquire_lease", _acquire)
    monkeypatch.setattr(idx.dispatch_lease, "release_lease", _release)
    monkeypatch.setattr(idx, "_prior_launch_count_today", lambda: 0)

    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert out["groom"]["launched"] is False
    assert out["groom"]["reason"] == "dispatch_ceiling_exhausted"
    assert "locks/groom-lane-mid-only.lock" in released


def test_spot_launch_lease_exhausted_retries_raises_without_launching(monkeypatch):
    """§5.9.3 shared-resource lease: if the brief RunInstances critical
    section can never be claimed (bounded retries exhausted), the dispatch
    fails loud rather than launching un-serialized — a silent bypass would
    reopen the exact 2026-07-28 race this lease exists to close."""
    launched = []
    idx = _load(
        monkeypatch,
        launch_impl=lambda types_, subnets, **kw: launched.append(True) or "i-should-never-launch",
        env={"GROOM_DISPATCH_ENABLED": "true"},
    )
    monkeypatch.setattr(idx.time, "sleep", lambda *_a, **_kw: None)  # no real waiting in tests

    def _acquire(lock_key, *, owner_id, ttl_seconds, bucket, s3_client=None, force=False):
        holder = idx.dispatch_lease.LeaseHolder(
            owner_id="rival", started_at="x", ttl_epoch=1, hostname="h", pid=1)
        if lock_key == "locks/groom-spot-capacity-pool.lock":
            return idx.dispatch_lease.LeaseAcquireResult(acquired=False, holder=holder)
        return idx.dispatch_lease.LeaseAcquireResult(
            acquired=True,
            holder=idx.dispatch_lease.LeaseHolder(
                owner_id=owner_id, started_at="x", ttl_epoch=1, hostname="h", pid=1),
        )

    released = []
    monkeypatch.setattr(idx.dispatch_lease, "acquire_lease", _acquire)
    monkeypatch.setattr(idx.dispatch_lease, "release_lease",
                        lambda lock_key, **kw: released.append(lock_key))

    with pytest.raises(RuntimeError, match="spot-launch lease"):
        idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert launched == []  # never reached RunInstances
    # The LANE lease (acquired successfully) was released on the failure path.
    assert "locks/groom-lane-mid-only.lock" in released


def test_spot_launch_lease_wraps_and_releases_around_launch_instance(monkeypatch):
    """The shared spot-launch lease is acquired immediately before, and
    released immediately after, the RunInstances call — never held across
    the lane's subsequent SSM/bootstrap steps."""
    order = []

    def _launch(types_, subnets, **kw):
        order.append("run_instances")
        return "i-stub"

    idx = _load(monkeypatch, launch_impl=_launch, env={"GROOM_DISPATCH_ENABLED": "true"})

    def _acquire(lock_key, *, owner_id, ttl_seconds, bucket, s3_client=None, force=False):
        if lock_key == "locks/groom-spot-capacity-pool.lock":
            order.append("acquire_spot_launch")
        holder = idx.dispatch_lease.LeaseHolder(
            owner_id=owner_id, started_at="x", ttl_epoch=1, hostname="h", pid=1)
        return idx.dispatch_lease.LeaseAcquireResult(acquired=True, holder=holder)

    def _release(lock_key, *, bucket, s3_client=None):
        if lock_key == "locks/groom-spot-capacity-pool.lock":
            order.append("release_spot_launch")

    monkeypatch.setattr(idx.dispatch_lease, "acquire_lease", _acquire)
    monkeypatch.setattr(idx.dispatch_lease, "release_lease", _release)

    out = idx.handler({"run_mode": "full", "issue_filter": "mid-only", "schedule": "x"}, None)
    assert out["groom"]["launched"] is True
    assert order == ["acquire_spot_launch", "run_instances", "release_spot_launch"]


# ── §5.9.4: yielded-dispatch paging when a lane starves for a whole day ──────

class _FixedNow(datetime):
    """Subclasses stdlib datetime so `index.py`'s `datetime.now(tz)` calls
    resolve to a fixed instant — only `.now()` is overridden, every other
    classmethod (`.fromisoformat`, the constructor, etc.) is unchanged."""
    _fixed = datetime(2026, 8, 4, 23, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls._fixed if tz is not None else cls._fixed.replace(tzinfo=None)


def _decision_record(*entries):
    return json.dumps({"schema_version": 2, "decisions": list(entries)}).encode()


def test_lane_yield_starvation_pages_when_all_yielded(monkeypatch):
    idx = _load(monkeypatch, s3_objects={
        "groom/decisions/2026-08-04/trigger-0400.json": _decision_record(
            {"tier_tag": "mid-only", "launch": False, "reason": "lane_lease_yielded"}),
        "groom/decisions/2026-08-04/trigger-1200.json": _decision_record(
            {"tier_tag": "mid-only", "launch": False, "reason": "lane_lease_yielded"}),
    })
    monkeypatch.setattr(idx, "datetime", _FixedNow)
    result = idx.handler({"mode": "reconcile"}, None)
    ys = result["lane_yield_starvation"]
    assert ys["evaluated"] is True
    assert ys["starved_lanes"] == ["mid-only"]
    assert ys["paged"] == 1
    assert "groom/_control/reconciled-lane-yield-starvation/2026-08-04.json" in idx._test_s3._objects


def test_lane_yield_starvation_no_page_when_a_launch_occurred(monkeypatch):
    idx = _load(monkeypatch, s3_objects={
        "groom/decisions/2026-08-04/trigger-0400.json": _decision_record(
            {"tier_tag": "mid-only", "launch": False, "reason": "lane_lease_yielded"}),
        "groom/decisions/2026-08-04/trigger-1200.json": _decision_record(
            {"tier_tag": "mid-only", "launch": True}),
    })
    monkeypatch.setattr(idx, "datetime", _FixedNow)
    result = idx.handler({"mode": "reconcile"}, None)
    ys = result["lane_yield_starvation"]
    assert ys["starved_lanes"] == []
    assert ys["paged"] == 0


def test_lane_yield_starvation_skipped_before_eval_hour(monkeypatch):
    class _EarlyNow(datetime):
        _fixed = datetime(2026, 8, 4, 13, 0, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return cls._fixed if tz is not None else cls._fixed.replace(tzinfo=None)

    idx = _load(monkeypatch, s3_objects={
        "groom/decisions/2026-08-04/trigger-0400.json": _decision_record(
            {"tier_tag": "mid-only", "launch": False, "reason": "lane_lease_yielded"}),
    })
    monkeypatch.setattr(idx, "datetime", _EarlyNow)
    result = idx.handler({"mode": "reconcile"}, None)
    ys = result["lane_yield_starvation"]
    assert ys["evaluated"] is False
    assert ys["paged"] == 0


def test_lane_yield_starvation_ignores_non_yield_skip_reasons(monkeypatch):
    """A lane that was skipped for an unrelated reason (e.g. the ceiling)
    every time is not a lease-starvation condition — only lane_lease_yielded
    counts toward this specific page."""
    idx = _load(monkeypatch, s3_objects={
        "groom/decisions/2026-08-04/trigger-0400.json": _decision_record(
            {"tier_tag": "mid-only", "launch": False, "reason": "dispatch_ceiling_exhausted"}),
    })
    monkeypatch.setattr(idx, "datetime", _FixedNow)
    result = idx.handler({"mode": "reconcile"}, None)
    ys = result["lane_yield_starvation"]
    assert ys["starved_lanes"] == []
    assert ys["paged"] == 0
