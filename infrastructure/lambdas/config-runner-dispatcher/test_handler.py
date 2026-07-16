"""Unit tests for the three-phase (webhook receiver / worker / reconcile)
config-runner spot dispatcher.

Hermetic: ``nousergon_lib.ec2_spot`` and ``boto3`` are stubbed in sys.modules
before importing index (mirrors ci-watch-dispatcher/test_handler.py). Covers:
webhook HMAC signature verification, event/action/label/repo filtering, the
async self-invoke on a matching queued job, the kill-switch, the worker
phase's launch/dedup/tag/SSM-dispatch behavior (spot-first with on-demand
fallback, job-id-scoped concurrency lock, terminate-on-post-launch-failure,
config#2267-style dedupe_degraded on a probe failure), and the reconcile
backstop (config-I2653 — dispatches a fresh runner for any queued job
matching our label that's sat stale with no in-flight box; ``urllib.request``
is monkeypatched per-test rather than stubbed in sys.modules, since it's
stdlib and always resolvable).
"""

from __future__ import annotations

import hashlib
import hmac as hmac_stdlib
import json
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

WEBHOOK_SECRET = "test-webhook-secret"


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
    def __init__(self, running_job_ids=None, create_tags_failures=0):
        self.terminated = []
        self.tags_created = []
        self.create_tags_attempts = 0
        self._create_tags_failures = create_tags_failures
        self._running_job_ids = dict(running_job_ids or {})  # {job_id: [instance_ids]}

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}

    def create_tags(self, Resources, Tags):  # noqa: N803
        self.create_tags_attempts += 1
        if self.create_tags_attempts <= self._create_tags_failures:
            raise RuntimeError(f"CreateTags throttled (attempt {self.create_tags_attempts})")
        self.tags_created.append((Resources, Tags))
        return {}

    def describe_instances(self, Filters):  # noqa: N803
        by_name = {f["Name"]: f["Values"] for f in Filters}
        # _bootstrap_and_reap enumerates by tag:Name WITHOUT a job-id filter
        # (I2692); tests populate self.boxes with full instance dicts. The
        # dedupe probe filters by job-id (with or without tag:Name) and must
        # keep hitting the legacy branch below.
        if "tag:Name" in by_name and "tag:config-runner-job-id" not in by_name:
            return {"Reservations": [{"Instances": list(getattr(self, "boxes", []))}]}
        job_id = by_name.get("tag:config-runner-job-id", [None])[0]
        ids = self._running_job_ids.get(job_id, [])
        return {"Reservations": [{"Instances": [{"InstanceId": i} for i in ids]}]} if ids else {"Reservations": []}


class _FakeSsm:
    def __init__(self):
        self.sent = []
        self.params = {
            "/alpha-engine/config_runner/webhook_secret": WEBHOOK_SECRET,
            "/alpha-engine/config_runner/github_read_pat": "test-read-only-pat",
        }

    def get_parameter(self, Name, WithDecryption=True):  # noqa: N803
        return {"Parameter": {"Value": self.params[Name]}}

    def describe_instance_information(self, **kw):
        # _bootstrap_and_reap (I2692) matches entries by InstanceId; tests
        # populate self.online_ids. Legacy default (no online_ids) keeps the
        # old anonymous-Online shape for any pre-I2692 call sites.
        online = getattr(self, "online_ids", None)
        if online is None:
            return {"InstanceInformationList": [{"PingStatus": "Online"}]}
        return {"InstanceInformationList": [
            {"InstanceId": i, "PingStatus": "Online"} for i in online]}

    def send_command(self, **kw):
        self.sent.append(kw)
        return {"Command": {"CommandId": "cmd-123"}}


class _FakeLambda:
    def __init__(self):
        self.invocations = []

    def invoke(self, **kw):
        self.invocations.append(kw)
        return {"StatusCode": 202}


def _load(monkeypatch, *, launch_impl=None, env=None, running_job_ids=None,
          create_tags_failures=0):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "alpha-engine-config-runner-dispatcher")
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm()
    ec2 = _FakeEc2(running_job_ids=running_job_ids, create_tags_failures=create_tags_failures)
    lam = _FakeLambda()
    clients = {"ec2": ec2, "ssm": ssm, "lambda": lam}
    if launch_impl is None:
        launch_impl = lambda types_, subnets, **kw: "i-stub"  # noqa: E731
    _install_stubs(launch_impl, clients)

    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)
    import importlib

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
    index._test_lambda = lam
    return index


def _sign(body_bytes: bytes, secret: str = WEBHOOK_SECRET) -> str:
    return "sha256=" + hmac_stdlib.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()


def _webhook_event(payload: dict, *, event_type: str = "workflow_job", secret: str = WEBHOOK_SECRET) -> dict:
    body = json.dumps(payload)
    body_bytes = body.encode("utf-8")
    return {
        "requestContext": {"http": {"method": "POST"}},
        "headers": {
            "x-github-event": event_type,
            "x-hub-signature-256": _sign(body_bytes, secret),
        },
        "body": body,
        "isBase64Encoded": False,
    }


def _job_payload(**overrides):
    base = {
        "action": "queued",
        "repository": {"full_name": "nousergon/alpha-engine-config"},
        "workflow_job": {"id": 987654321, "labels": ["self-hosted", "alpha-engine-config-spot"]},
    }
    base.update(overrides)
    return base


# ── Phase 1: webhook receiver ────────────────────────────────────────────────


def test_webhook_valid_queued_job_self_invokes_and_returns_200(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    resp = idx.handler(_webhook_event(_job_payload()), None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["accepted"] is True
    assert body["job_id"] == 987654321
    assert len(idx._test_lambda.invocations) == 1
    inv = idx._test_lambda.invocations[0]
    assert inv["InvocationType"] == "Event"
    assert inv["FunctionName"] == "alpha-engine-config-runner-dispatcher"
    worker_payload = json.loads(inv["Payload"])
    assert worker_payload == {"config_runner_job_id": "987654321"}


def test_webhook_invalid_signature_returns_401(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    resp = idx.handler(_webhook_event(_job_payload(), secret="wrong-secret"), None)
    assert resp["statusCode"] == 401
    assert idx._test_lambda.invocations == []


def test_webhook_missing_signature_header_returns_401(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    event = _webhook_event(_job_payload())
    del event["headers"]["x-hub-signature-256"]
    resp = idx.handler(event, None)
    assert resp["statusCode"] == 401


def test_webhook_non_workflow_job_event_is_noop(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    resp = idx.handler(_webhook_event({"zen": "hello"}, event_type="ping"), None)
    assert resp["statusCode"] == 200
    assert idx._test_lambda.invocations == []


def test_webhook_non_queued_action_is_noop(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    resp = idx.handler(_webhook_event(_job_payload(action="completed")), None)
    assert resp["statusCode"] == 200
    assert idx._test_lambda.invocations == []


def test_webhook_wrong_repo_is_noop(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    resp = idx.handler(
        _webhook_event(_job_payload(repository={"full_name": "nousergon/some-other-repo"})), None
    )
    assert resp["statusCode"] == 200
    assert idx._test_lambda.invocations == []


def test_webhook_missing_our_label_is_noop(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    resp = idx.handler(
        _webhook_event(_job_payload(workflow_job={"id": 1, "labels": ["ubuntu-latest"]})), None
    )
    assert resp["statusCode"] == 200
    assert idx._test_lambda.invocations == []


def test_webhook_disabled_flag_skips_invoke(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "false"})
    resp = idx.handler(_webhook_event(_job_payload()), None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["launched"] is False
    assert body["reason"] == "disabled"
    assert idx._test_lambda.invocations == []


# ── Phase 2: worker (async self-invoked) ─────────────────────────────────────


def test_worker_valid_job_launches_tags_and_defers_bootstrap(monkeypatch):
    # I2692 two-phase contract: the worker phase launches + tags and returns
    # in seconds — NO in-Lambda SSM wait/send (that wait used to blow the
    # 60s Lambda timeout and manufacture zombie boxes). The bootstrap is
    # delivered by _bootstrap_and_reap on a later reconcile pass.
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["spot"] = kw.get("spot")
        calls["profile"] = kw.get("iam_instance_profile")
        calls["tag_name"] = kw.get("tag_name")
        return "i-abc"

    idx = _load(monkeypatch, launch_impl=_launch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    out = idx.handler({"config_runner_job_id": "987654321"}, None)
    assert out["launched"] is True
    assert out["reason"] == "launched_bootstrap_pending"
    assert out["instance_id"] == "i-abc"
    assert out["market"] == "spot"
    assert calls["profile"] == "alpha-engine-config-runner-executor-profile"
    assert calls["tag_name"] == "alpha-engine-config-runner-spot"
    assert idx._test_ec2.tags_created == [
        (["i-abc"], [{"Key": "config-runner-job-id", "Value": "987654321"}])
    ]
    assert idx._test_ssm.sent == [], "worker phase must never send SSM commands (I2692)"


def test_worker_on_demand_fallback_on_spot_capacity_exhaustion(monkeypatch):
    seen = []

    def _launch(types_, subnets, **kw):
        seen.append(kw.get("spot"))
        if kw.get("spot"):
            raise _SpotCapacityExhausted("no capacity")
        return "i-ondemand"

    idx = _load(monkeypatch, launch_impl=_launch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    out = idx.handler({"config_runner_job_id": "1"}, None)
    assert out["launched"] is True
    assert out["market"] == "on-demand"
    assert seen == [True, False]


def test_worker_total_launch_exhaustion_returns_clean_false(monkeypatch):
    def _launch(types_, subnets, **kw):
        raise _SpotCapacityExhausted("exhausted")

    idx = _load(monkeypatch, launch_impl=_launch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    out = idx.handler({"config_runner_job_id": "1"}, None)
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"
    assert idx._test_ssm.sent == []


def test_worker_concurrency_skip_when_job_already_running(monkeypatch):
    launched = []

    def _launch(types_, subnets, **kw):
        launched.append(True)
        return "i-new"

    idx = _load(
        monkeypatch, launch_impl=_launch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"},
        running_job_ids={"55": ["i-already-running"]},
    )
    out = idx.handler({"config_runner_job_id": "55"}, None)
    assert out["launched"] is False
    assert out["reason"] == "concurrent_skip"
    assert out["existing_instance_ids"] == ["i-already-running"]
    assert launched == []


def test_worker_different_job_id_is_not_blocked(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"},
        running_job_ids={"55": ["i-other-job"]},
    )
    out = idx.handler({"config_runner_job_id": "56"}, None)
    assert out["launched"] is True


def test_worker_probe_failure_launches_with_dedupe_degraded(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-degraded",  # noqa: E731
        env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"},
    )

    def _probe_down(*a, **kw):
        raise idx.SpotProbeError("probe failed: ThrottlingException")

    monkeypatch.setattr(idx.spot_dispatch, "running_instance_ids", _probe_down)
    out = idx.handler({"config_runner_job_id": "1"}, None)
    assert out["launched"] is True
    assert out["dedupe_degraded"] is True


def test_worker_persistent_tag_write_failure_terminates_and_fails(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-untaggable",  # noqa: E731
        env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"},
        create_tags_failures=99,
    )
    out = idx.handler({"config_runner_job_id": "1"}, None)
    assert out["launched"] is False
    assert out["reason"] == "tag_write_failed"
    assert idx._test_ec2.terminated == ["i-untaggable"]
    assert idx._test_ssm.sent == []


def test_worker_quota_exhaustion_pages_loudly(monkeypatch):
    # I2692: MaxSpotInstanceCountExceeded used to be ERROR-log-only while
    # fleet CI silently queued for an hour — it must page now.
    from nousergon_lib.spot_dispatch import SpotLaunchError

    def _launch(types_, subnets, **kw):
        raise SpotLaunchError(
            "RunInstances failed with non-capacity error "
            "MaxSpotInstanceCountExceeded (t3.medium@subnet-x): "
            "Max spot instance count exceeded")

    idx = _load(monkeypatch, launch_impl=_launch,
                env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    pages = []
    monkeypatch.setattr(idx, "_page", lambda msg: pages.append(msg))
    out = idx.handler({"config_runner_job_id": "1"}, None)
    assert out["launched"] is False
    assert out["reason"] == "launch_failed"
    assert len(pages) == 1 and "QUOTA EXHAUSTED" in pages[0]


# ── _bootstrap_and_reap (I2692 two-phase state machine) ──────────────────────

from datetime import datetime, timedelta, timezone  # noqa: E402


def _box(iid, *, age_seconds, job_id="j1", bootstrapped=False):
    tags = [{"Key": "Name", "Value": "alpha-engine-config-runner-spot"}]
    if job_id:
        tags.append({"Key": "config-runner-job-id", "Value": job_id})
    if bootstrapped:
        tags.append({"Key": "config-runner-bootstrap-sent", "Value": "2026-01-01T00:00:00Z"})
    return {"InstanceId": iid, "Tags": tags,
            "LaunchTime": datetime.now(timezone.utc) - timedelta(seconds=age_seconds)}


def test_bootstrap_and_reap_sends_bootstrap_to_online_untagged_box(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    idx._test_ec2.boxes = [_box("i-new", age_seconds=70, job_id="42")]
    idx._test_ssm.online_ids = ["i-new"]
    stats = idx._bootstrap_and_reap()
    assert stats["bootstrapped"] == 1
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert 'exec bash infrastructure/config_runner_spot_bootstrap.sh --job-id "42"' in cmd
    assert any(r == ["i-new"] and tags[0]["Key"] == "config-runner-bootstrap-sent"
               for r, tags in idx._test_ec2.tags_created)


def test_bootstrap_and_reap_waits_on_young_offline_box(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    idx._test_ec2.boxes = [_box("i-booting", age_seconds=60)]
    idx._test_ssm.online_ids = []
    stats = idx._bootstrap_and_reap()
    assert stats["waiting_ssm"] == 1
    assert idx._test_ec2.terminated == []
    assert idx._test_ssm.sent == []


def test_bootstrap_and_reap_reaps_box_ssm_never_came_up(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    idx._test_ec2.boxes = [_box("i-zombie", age_seconds=400)]
    idx._test_ssm.online_ids = []
    stats = idx._bootstrap_and_reap()
    assert stats["reaped_no_ssm"] == 1
    assert idx._test_ec2.terminated == ["i-zombie"]


def test_bootstrap_and_reap_reaps_over_lifetime_box_even_if_bootstrapped(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    idx._test_ec2.boxes = [_box("i-leak", age_seconds=6000, bootstrapped=True)]
    idx._test_ssm.online_ids = []
    stats = idx._bootstrap_and_reap()
    assert stats["reaped_lifetime"] == 1
    assert idx._test_ec2.terminated == ["i-leak"]


def test_bootstrap_and_reap_healthy_bootstrapped_box_untouched(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    idx._test_ec2.boxes = [_box("i-working", age_seconds=600, bootstrapped=True)]
    idx._test_ssm.online_ids = ["i-working"]
    stats = idx._bootstrap_and_reap()
    assert stats["healthy"] == 1
    assert idx._test_ec2.terminated == []
    assert idx._test_ssm.sent == []


def test_bootstrap_and_reap_send_failure_leaves_untagged_for_retry(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    idx._test_ec2.boxes = [_box("i-flaky", age_seconds=100, job_id="7")]
    idx._test_ssm.online_ids = ["i-flaky"]

    def _boom_send(**kw):
        raise RuntimeError("SSM SendCommand failed")

    idx._test_ssm.send_command = _boom_send
    stats = idx._bootstrap_and_reap()
    assert stats["errors"] == 1
    assert stats["bootstrapped"] == 0
    # No bootstrap-sent tag written — the next reconcile pass retries.
    assert all(tags[0]["Key"] != "config-runner-bootstrap-sent"
               for _, tags in idx._test_ec2.tags_created)


def test_worker_missing_job_id_returns_invalid_event(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    out = idx.handler({}, None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"


# ── Phase 3: reconcile backstop (config-I2653) ───────────────────────────────


class _FakeHttpResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _install_gh_api_stub(idx, monkeypatch, runs_by_path):
    """runs_by_path: {url_path_suffix: response_dict}. Any request whose URL
    ends with a registered suffix returns that canned response."""
    def _fake_urlopen(req, timeout=10):  # noqa: ARG001
        url = req.full_url if hasattr(req, "full_url") else req.get_full_url()
        for suffix, payload in runs_by_path.items():
            if url.endswith(suffix):
                return _FakeHttpResponse(payload)
        raise AssertionError(f"unexpected GH API call: {url}")

    monkeypatch.setattr(idx.urllib.request, "urlopen", _fake_urlopen)


def _iso(seconds_ago: float) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_reconcile_dispatches_stale_orphaned_job(monkeypatch):
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-rescued",  # noqa: E731
        env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"},
    )
    _install_gh_api_stub(idx, monkeypatch, {
        "/actions/runs?status=queued&per_page=50": {
            "workflow_runs": [{"id": 555}],
        },
        "/actions/runs/555/jobs": {
            "jobs": [{
                "id": 999, "status": "queued",
                "labels": ["self-hosted", "alpha-engine-config-spot"],
                "created_at": _iso(200),
            }],
        },
    })
    out = idx.handler({"reconcile": True}, None)
    assert out["reconciled"] == 1
    assert out["dispatched"][0]["job_id"] == "999"
    assert out["dispatched"][0]["result"]["launched"] is True


def test_reconcile_skips_job_within_normal_dispatch_window(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    _install_gh_api_stub(idx, monkeypatch, {
        "/actions/runs?status=queued&per_page=50": {"workflow_runs": [{"id": 1}]},
        "/actions/runs/1/jobs": {
            "jobs": [{
                "id": 1, "status": "queued",
                "labels": ["self-hosted", "alpha-engine-config-spot"],
                "created_at": _iso(10),  # well under the 90s default threshold
            }],
        },
    })
    out = idx.handler({"reconcile": True}, None)
    assert out["reconciled"] == 0
    assert out["dispatched"] == []


def test_reconcile_skips_job_already_covered_by_an_in_flight_box(monkeypatch):
    idx = _load(
        monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"},
        running_job_ids={"777": ["i-already-covering-it"]},
    )
    _install_gh_api_stub(idx, monkeypatch, {
        "/actions/runs?status=queued&per_page=50": {"workflow_runs": [{"id": 2}]},
        "/actions/runs/2/jobs": {
            "jobs": [{
                "id": 777, "status": "queued",
                "labels": ["self-hosted", "alpha-engine-config-spot"],
                "created_at": _iso(200),
            }],
        },
    })
    out = idx.handler({"reconcile": True}, None)
    assert out["reconciled"] == 0
    assert out["skipped"] == ["777"]


def test_reconcile_ignores_jobs_without_our_label(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})
    _install_gh_api_stub(idx, monkeypatch, {
        "/actions/runs?status=queued&per_page=50": {"workflow_runs": [{"id": 3}]},
        "/actions/runs/3/jobs": {
            "jobs": [{
                "id": 3, "status": "queued", "labels": ["ubuntu-latest"],
                "created_at": _iso(200),
            }],
        },
    })
    out = idx.handler({"reconcile": True}, None)
    assert out["reconciled"] == 0


def test_reconcile_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "false"})
    out = idx.handler({"reconcile": True}, None)
    assert out == {"reconciled": 0, "reason": "disabled"}


def test_reconcile_list_runs_failure_returns_clean_result_not_raise(monkeypatch):
    idx = _load(monkeypatch, env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})

    def _boom(req, timeout=10):  # noqa: ARG001
        raise idx.urllib.error.URLError("network blip")

    monkeypatch.setattr(idx.urllib.request, "urlopen", _boom)
    out = idx.handler({"reconcile": True}, None)
    assert out["reconciled"] == 0
    assert out["reason"] == "list_runs_failed"


def test_reconcile_dispatches_oldest_job_first(monkeypatch):
    # FIFO fairness: under quota pressure the OLDEST stale job must get the
    # first launch attempt — newest-first listing order let fresh churn
    # starve old jobs indefinitely (PR2690, 2026-07-15).
    launched = []

    def _launch(types_, subnets, **kw):
        return f"i-{len(launched)}"

    idx = _load(monkeypatch, launch_impl=_launch,
                env={"CONFIG_RUNNER_DISPATCH_ENABLED": "true"})

    old = (datetime.now(timezone.utc) - timedelta(seconds=5000)).strftime("%Y-%m-%dT%H:%M:%SZ")
    newer = (datetime.now(timezone.utc) - timedelta(seconds=200)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def fake_get(path):
        if path.endswith("&per_page=50") or "/actions/runs?" in path:
            return {"workflow_runs": [{"id": 1}, {"id": 2}]}
        if "/runs/1/jobs" in path:
            # newest-first listing: run 1 (listed first) has the NEWER job
            return {"jobs": [{"id": 111, "status": "queued", "created_at": newer,
                              "labels": ["self-hosted", "alpha-engine-config-spot"]}]}
        if "/runs/2/jobs" in path:
            return {"jobs": [{"id": 222, "status": "queued", "created_at": old,
                              "labels": ["self-hosted", "alpha-engine-config-spot"]}]}
        raise AssertionError(f"unexpected GH GET {path}")

    monkeypatch.setattr(idx, "_gh_api_get", fake_get)
    out = idx.handler({"reconcile": True}, None)
    order = [d["job_id"] for d in out["dispatched"]]
    assert order == ["222", "111"], f"oldest job must dispatch first, got {order}"
