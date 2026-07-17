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
the kill-switch short-circuit; and the config#2267 launch-path hardening — a
failed concurrency probe launches WITH dedupe_degraded:true recorded
(site 1), and the load-bearing discriminator tags ride the RunInstances
launch call ATOMICALLY via extra_tags (site 2 root fix, config#2292) — the
PR758 post-launch create_tags bounded-retry/terminate path is gone entirely.
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
        # {(repo, sha) -> [instance_ids]} already "live" for the concurrency
        # guard's describe_instances check to find.
        self._running_instances = dict(running_instances or {})

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803 — boto3 kwarg name
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}

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
        calls["extra_tags"] = kw.get("extra_tags")
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
    # config#2292 root fix: the repo+sha discriminator tags ride the SAME
    # RunInstances call as the launch itself (extra_tags), not a separate
    # post-launch create_tags call.
    assert calls["extra_tags"] == {
        "ci-watch-repo": "nousergon/alpha-engine-config",
        "ci-watch-sha": "abc1234def5678900000000000000000000abcd",
    }
    assert idx._test_ec2.terminated == []
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


def test_concurrency_check_failure_still_launches(monkeypatch):
    # config#2267 site 1 POLICY: a broken probe must never block a launch —
    # coverage beats dedupe. (Under nousergon-lib >= 0.106.0 the raw
    # describe_instances error surfaces as SpotProbeError and the launch is
    # flagged dedupe_degraded; under the old fail-open lib it degrades to []
    # silently. Either way the box launches — the explicit degraded-flag
    # contract is pinned separately below.)
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-new",  # noqa: E731
        env={"CI_WATCH_DISPATCH_ENABLED": "true"},
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
        env={"CI_WATCH_DISPATCH_ENABLED": "true"},
    )

    def _probe_down(*args, **kwargs):
        raise idx.SpotProbeError("concurrency probe failed for tag_name='alpha-engine-ci-watch-spot': ThrottlingException: rate exceeded")

    monkeypatch.setattr(idx.spot_dispatch, "running_instance_ids", _probe_down)
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["dedupe_degraded"] is True
    # The verdict names the probe error — the GHA caller archives it.
    assert "ThrottlingException" in out["dedupe_probe_error"]
    # A healthy probe keeps the flag False.
    idx2 = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out2 = idx2.handler(_event(), None)
    assert out2["launched"] is True
    assert out2["dedupe_degraded"] is False
    assert "dedupe_probe_error" not in out2


def test_discriminator_tags_ride_the_launch_call_not_a_separate_create_tags(monkeypatch):
    """config#2292 root fix for config#2267 site 2: the (repo, sha)
    discriminator tags are passed to spot_dispatch.launch_with_fallback as
    extra_tags — merged into krepis.ec2_spot.launch's RunInstances
    TagSpecifications — so there is no separate post-launch create_tags call
    left to retry or fail. A launch that succeeds is a fully-tagged launch,
    unconditionally; no ec2.create_tags call happens at all."""
    idx = _load(
        monkeypatch, launch_impl=lambda types_, subnets, **kw: "i-atomic",  # noqa: E731
        env={"CI_WATCH_DISPATCH_ENABLED": "true"},
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


# ── config#2223: weekly synthetic canary drill ───────────────────────────────


def test_drill_synthesizes_isolated_identity_and_launches(monkeypatch):
    """The happy path the weekly canary exists to verify: `{"is_drill":
    "true"}` (the EventBridge Scheduler rule's entire static Input)
    round-trips the REAL launch pipe with a code-synthesized drill identity —
    DRILL_REPO + per-day sha — plus the sf-watch-drill discriminator tag."""
    from datetime import datetime, timezone

    calls = {}

    def _launch(types_, subnets, **kw):
        calls["extra_tags"] = kw.get("extra_tags")
        return "i-stub"

    idx = _load(monkeypatch, launch_impl=_launch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler({"is_drill": "true"}, None)
    expected_sha = idx._drill_sha(datetime.now(timezone.utc))
    assert out["launched"] is True
    assert out["is_drill"] is True
    assert out["repo"] == idx.DRILL_REPO == "nousergon/ci-watch-drill"
    assert out["sha"] == expected_sha
    assert calls["extra_tags"] == {
        "ci-watch-repo": idx.DRILL_REPO,
        "ci-watch-sha": expected_sha,
        "sf-watch-drill": "true",
    }
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--is-drill "true"' in cmd
    assert f'--ci-repo "{idx.DRILL_REPO}"' in cmd
    assert f'--ci-sha "{expected_sha}"' in cmd


def test_drill_identity_can_never_collide_with_a_real_dispatch(monkeypatch):
    """Pins the structural isolation invariant (index.DRILL_REPO comment):
    the drill sha is allowlist-valid but per-day-deterministic, DRILL_REPO is
    not a real fleet repository, and the slash-flattened completion-marker
    stem contains 'drill-' — so a drill can never dedupe-block a real
    dispatch and its marker key never collides with a real one."""
    from datetime import datetime, timezone

    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    sha = idx._drill_sha(datetime.now(timezone.utc))
    assert idx._SHA_RE.match(sha)
    # ci_watch_run.sh's completion key stem is "{repo//\//-}-{sha}" — for a
    # drill that is "nousergon-ci-watch-drill-<sha>", which contains "drill-".
    marker_stem = f"{idx.DRILL_REPO.replace('/', '-')}-{sha}"
    assert "drill-" in marker_stem
    # Deterministic within a day — a duplicate drill dedupes against itself.
    assert sha == idx._drill_sha(datetime.now(timezone.utc))


def test_drill_ignores_payload_supplied_identity(monkeypatch):
    """ISOLATION INVARIANT: even a payload that smuggles a real (repo, sha)
    into a drill gets the code-synthesized drill identity — no payload can
    point a drill at a real dispatch's lock/marker keys."""
    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler({
        "is_drill": "true",
        "repo": "nousergon/alpha-engine-config",
        "sha": "abc1234def5678900000000000000000000abcd",
        "run_id": "123456789",
        "run_url": "https://github.com/nousergon/alpha-engine-config/actions/runs/123456789",
        "workflow": "Fleet CI",
        "branch": "main",
    }, None)
    assert out["launched"] is True
    assert out["repo"] == idx.DRILL_REPO
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--ci-repo "nousergon/alpha-engine-config"' not in cmd


def test_drill_same_day_duplicate_dedupes_against_itself_only(monkeypatch):
    from datetime import datetime, timezone

    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    sha = idx._drill_sha(datetime.now(timezone.utc))
    idx2 = _load(
        monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"},
        running_instances={(idx.DRILL_REPO, sha): ["i-drill-live"]},
    )
    out = idx2.handler({"is_drill": "true"}, None)
    assert out["launched"] is False
    assert out["reason"] == "concurrent_skip"
    assert idx2._test_ssm.sent == []


def test_malformed_is_drill_returns_clean_false(monkeypatch):
    idx = _load(monkeypatch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler({"is_drill": "yes-please"}, None)
    assert out["launched"] is False
    assert out["reason"] == "invalid_event"
    assert idx._test_ssm.sent == []


def test_non_drill_dispatch_carries_no_drill_tag_and_is_drill_false(monkeypatch):
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["extra_tags"] = kw.get("extra_tags")
        return "i-stub"

    idx = _load(monkeypatch, launch_impl=_launch, env={"CI_WATCH_DISPATCH_ENABLED": "true"})
    out = idx.handler(_event(), None)
    assert out["launched"] is True
    assert out["is_drill"] is False
    assert "sf-watch-drill" not in calls["extra_tags"]
    cmd = idx._test_ssm.sent[0]["Parameters"]["commands"][0]
    assert '--is-drill "false"' in cmd
