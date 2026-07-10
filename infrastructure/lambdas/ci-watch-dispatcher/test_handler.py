"""Unit tests for the synchronous CI-failure -> ci-watch-spot dispatcher.

Hermetic: ``nousergon_lib.ec2_spot`` and ``boto3`` are stubbed in sys.modules
BEFORE importing index (mirrors scheduled-groom-dispatcher/test_handler.py).
Validates: a valid event launches a spot box and fires an async SSM command;
the on-demand fallback on spot capacity exhaustion; a total launch failure
(spot AND on-demand exhausted) returns a clean launched:false rather than
raising; the (repo, sha)-scoped concurrency lock (narrower than groom's
per-tier lock — two different shas on the same repo must NOT block each
other); a post-launch SSM-send failure terminates the box and returns
launched:false; a malformed event returns launched:false rather than raising;
the kill-switch short-circuit.
"""

from __future__ import annotations

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
        self.tags_created = []
        # {(repo, sha) -> [instance_ids]} already "live" for the concurrency
        # guard's describe_instances check to find.
        self._running_instances = dict(running_instances or {})

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
        repo = by_name.get("tag:ci-watch-repo", [None])[0]
        sha = by_name.get("tag:ci-watch-sha", [None])[0]
        ids = self._running_instances.get((repo, sha), [])
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
    # Derive the stub requirement from index.py's live import graph and fail
    # loud on drift here, rather than as a ModuleNotFoundError at deploy time
    # (config#1746 pattern — see scheduled-groom-dispatcher/test_handler.py).
    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)
    import importlib

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
    return index


def _event(**overrides):
    base = {
        "repo": "nousergon/alpha-engine-config",
        "sha": "abc1234def5678900000000000000000000abcd",
        "run_id": "123456789",
        "run_url": "https://github.com/nousergon/alpha-engine-config/actions/runs/123456789",
        "workflow": "Fleet CI",
        "branch": "main",
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

    idx = _load(monkeypatch, launch_impl=_launch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["reason"] == "launched"
    assert out["instance_id"] == "i-abc"
    assert out["market"] == "spot"
    assert out["command_id"] == "cmd-123"
    assert out["repo"] == "nousergon/alpha-engine-config"
    assert calls["spot"] is True
    assert calls["profile"] == "alpha-engine-ci-watch-executor-profile"
    assert calls["tag_name"] == "alpha-engine-ci-watch-spot"
    # The instance is tagged with its repo+sha for the concurrency guard.
    assert idx._test_ec2.tags_created == [
        (["i-abc"], [{"Key": "ci-watch-repo", "Value": "nousergon/alpha-engine-config"},
                     {"Key": "ci-watch-sha", "Value": "abc1234def5678900000000000000000000abcd"}])
    ]
    sent = idx._test_ssm.sent[0]
    cmd = sent["Parameters"]["commands"][0]
    # ci_watch_spot_bootstrap.sh (alpha-engine-config) takes its CI fields as
    # CLI FLAGS, not env vars — assert the actual invocation shape, not an
    # `export CI_WATCH_*` form the bootstrap script never reads.
    assert "exec bash infrastructure/ci_watch_spot_bootstrap.sh" in cmd
    assert '--ci-repo "nousergon/alpha-engine-config"' in cmd
    assert '--ci-sha "abc1234def5678900000000000000000000abcd"' in cmd
    assert '--ci-run-url "https://github.com' in cmd
    assert "export HOME=/root" in cmd
    # run_token is NOT threaded into the box (no in-box consumer — the
    # completion marker keys directly on repo+sha) — only a Lambda-side
    # correlation id, surfaced via the SSM Comment field instead.
    assert "run_token" not in cmd
    assert "CI_WATCH_RUN_TOKEN" not in cmd
    assert "token" in sent["Comment"]

    assert sent["Parameters"]["executionTimeout"] == [str(idx.MAX_RUNTIME_SECONDS)]


def test_on_demand_fallback_on_spot_capacity_exhaustion(monkeypatch):
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        if kw.get("spot"):
            raise _SpotCapacityExhausted("no capacity in any pool")
        return "i-ondemand"

    idx = _load(monkeypatch, launch_impl=_launch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["market"] == "on-demand"
    assert out["instance_id"] == "i-ondemand"
    assert seen == [True, False]  # tried spot, then on-demand


def test_total_launch_exhaustion_returns_clean_false_not_raise(monkeypatch):
    # SYNCHRONOUS contract (index.py docstring): unlike groom's fail-loud
    # posture, spot AND on-demand both exhausted must be a clean return, not
    # an exception — the GHA caller needs a JSON verdict to branch on.
    def _launch(types_, subnets, **kw):
        raise _SpotCapacityExhausted("exhausted everywhere")

    idx = _load(monkeypatch, launch_impl=_launch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"
    assert "exhausted everywhere" in out["error"]
    assert idx._test_ssm.sent == []


def test_non_capacity_launch_error_returns_clean_false(monkeypatch):
    def _launch(types_, subnets, **kw):
        raise _SpotLaunchError("RunInstances denied")

    idx = _load(monkeypatch, launch_impl=_launch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"


def test_concurrency_skip_when_same_repo_and_sha_already_running(monkeypatch):
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-new"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"CI_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("nousergon/alpha-engine-config", "abc1234def5678900000000000000000000abcd"): ["i-already-running"],
        },
    )
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "concurrent_skip"
    assert out["existing_instance_ids"] == ["i-already-running"]
    assert launched == []  # never even attempted a spot launch — zero spend


def test_different_sha_same_repo_is_not_blocked(monkeypatch):
    # This is the whole point of the (repo, sha) granularity — a bare-repo
    # lock (like groom's per-tier lock) would wrongly starve a second commit's
    # independent CI failure on the same repo.
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"CI_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("nousergon/alpha-engine-config", "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"): ["i-other-sha"],
        },
    )
    out = idx.handler(_event(sha="abc1234def5678900000000000000000000abcd"), None)
    assert out["launched"] is True
    assert out["instance_id"] == "i-new"


def test_different_repo_same_sha_is_not_blocked(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"CI_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("nousergon/other-repo", "abc1234def5678900000000000000000000abcd"): ["i-other-repo"],
        },
    )
    out = idx.handler(_event(), None)
    assert out["launched"] is True


def test_concurrency_check_fails_safe_and_still_launches(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"CI_WATCH_DISPATCH_ENABLED": "true"},
    )

    def _boom(Filters):  # noqa: N803 — boto3 kwarg name
        raise RuntimeError("EC2 API hiccup")

    idx._test_ec2.describe_instances = _boom
    out = idx.handler(_event(), None)
    # A broken check must never block a launch — optimization, not a
    # correctness gate (mirrors groom's fail-safe posture on its own guard).
    assert out["launched"] is True


def test_post_launch_ssm_failure_terminates_instance_returns_clean_false(monkeypatch):
    idx = _load(
        monkeypatch,
        launch_impl=lambda types_, subnets, **kw: "i-orphan",  # noqa: E731
        env={"CI_WATCH_DISPATCH_ENABLED": "true"},
    )

    def _boom_send(**kw):
        raise RuntimeError("SSM SendCommand failed")

    idx._test_ssm.send_command = _boom_send
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "post_launch_failed"
    assert out["instance_id"] == "i-orphan"
    # The just-launched box was terminated (not orphaned), and the handler
    # still returned a clean result instead of raising.
    assert idx._test_ec2.terminated == ["i-orphan"]


def test_malformed_event_returns_clean_false_not_raise(monkeypatch):
    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(sha="not-a-sha"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"
    assert idx._test_ssm.sent == []


def test_missing_field_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    event = _event()
    del event["repo"]
    out = idx.handler(event, None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"


def test_run_url_with_dollar_sign_rejected(monkeypatch):
    # Under `set -u` in the double-quoted prelude export, a `$`-bearing
    # run_url could expand as a positional param and abort the prelude
    # (same gotcha groom's own run_url note documents).
    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(run_url="https://example.com/$2Fbad"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"


def test_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "false"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "disabled"
    assert idx._test_ssm.sent == []
