"""Tests for alpha-engine-thinktank-spot-dispatcher (config-I5208 §47).

The load-bearing assertions here are about the budget/timeout coupling, the
fail-loud posture, and (alpha-engine-config-I11597) the self-starting launch:
the box's user-data is its whole dispatch, and a retry of the same event
launches nothing new. A dispatcher that launches a box whose deadline the job
unit's cap will preempt reintroduces the exact lost-terminal-writes bug this
migration exists to fix, so that inequality is asserted rather than left to a
comment.
"""

from __future__ import annotations

import copy
import importlib.util
import os
import re
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(monkeypatch_env: dict | None = None):
    """Import index.py fresh so module-level env-derived constants re-evaluate."""
    for k, v in (monkeypatch_env or {}).items():
        os.environ[k] = v
    spec = importlib.util.spec_from_file_location(
        "thinktank_spot_dispatcher_index", os.path.join(_HERE, "index.py")
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["thinktank_spot_dispatcher_index"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def _clean_env():
    keys = [
        "THINKTANK_RUN_BUDGET_SECONDS",
        "THINKTANK_SPOT_RUN_TIMEOUT_SECONDS",
        "THINKTANK_SPOT_WATCHDOG_SECONDS",
        "THINKTANK_SPOT_DISPATCH_ENABLED",
    ]
    saved = {k: os.environ.get(k) for k in keys}
    for k in keys:
        os.environ.pop(k, None)
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


class _Ctx:
    """A stand-in Lambda context. Only the request id matters now: it is the
    launch's idempotency key."""

    def __init__(self, request_id: str = "req-1"):
        self.aws_request_id = request_id


class TestTimingCoupling:
    def test_default_budget_is_below_the_unit_timeout_by_more_than_the_reserve(self):
        """The box's deadline must land before the job unit's cap, with room
        for the terminal writes. thinktank.run._TERMINAL_WRITE_RESERVE_S is
        120s."""
        mod = _load()
        reserve = 120
        assert mod.RUN_BUDGET_SECONDS + reserve < mod.RUN_TIMEOUT_SECONDS

    def test_watchdog_sits_above_the_unit_timeout(self):
        """systemd's own kill at the cap (which the bootstrap trap and the
        unit's ExecStopPost turn into a clean self-terminate) must always win
        before the orphan watchdog fires."""
        mod = _load()
        assert mod.WATCHDOG_SECONDS > mod.RUN_TIMEOUT_SECONDS

    def test_handler_refuses_to_launch_when_the_coupling_is_violated(self):
        """A config drift that inverts the inequality must fail loud at
        dispatch, not produce a box that gets guillotined mid-run."""
        mod = _load(
            {
                "THINKTANK_RUN_BUDGET_SECONDS": "7200",
                "THINKTANK_SPOT_RUN_TIMEOUT_SECONDS": "7200",
            }
        )
        with pytest.raises(ValueError, match="must be strictly below"):
            mod.handler({}, None)

    def test_budget_reaches_the_box_as_an_env_export(self):
        """The job script is the only channel carrying the budget to the box;
        if it is dropped the runner falls back to its own default and the
        dispatcher's timeout coupling becomes a fiction."""
        mod = _load()
        cmd = mod._job_script("tok123")
        assert f"export THINKTANK_RUN_BUDGET_SECONDS={mod.RUN_BUDGET_SECONDS}" in cmd


class TestJobScript:
    def test_execs_the_repo_owned_bootstrap_not_an_inline_copy(self):
        """§47 sub-rule (a): one shared entrypoint. The prelude must hand off
        to the version-controlled script rather than inline the run steps."""
        cmd = _load()._job_script("tok")
        assert "exec bash infrastructure/thinktank_spot_bootstrap.sh" in cmd

    def test_sets_home_because_the_unit_runs_as_root_without_one(self):
        cmd = _load()._job_script("tok")
        assert "export HOME=/home/ec2-user" in cmd

    def test_installs_git_and_python_before_cloning(self):
        """Stock AL2023 ships neither; the clone is the first thing that needs
        git, so the install must precede it in the command text."""
        cmd = _load()._job_script("tok")
        assert cmd.index("dnf install") < cmd.index("git clone")

    def test_arms_the_orphan_watchdog(self):
        cmd = _load()._job_script("tok")
        assert "alpha-engine-thinktank-spot-watchdog" in cmd

    def test_prelude_failure_shuts_the_box_down(self):
        """A botched launch must never idle — spot-orphan-reaper is a 6.5h age
        cap, not a health check."""
        cmd = _load()._job_script("tok")
        assert "shutdown -h now" in cmd


class TestLibCallSignatures:
    """Bind every library call against the REAL library signature.

    The 2026-07-29 smoke run died on
    ``running_instance_ids() missing 1 required positional argument:
    'discriminator_tags'`` — an error every unit test above missed, because
    they monkeypatch the lib functions themselves, so no test ever touched the
    real signature. Mocks make a call site untestable exactly where it talks to
    someone else's contract. ``inspect.signature().bind()`` closes that without
    needing AWS. (The replay tests below go further and run the real krepis
    launch against a fake EC2.)
    """

    def test_running_instance_ids_call_binds(self):
        import inspect

        from nousergon_lib import spot_dispatch as real

        mod = _load()
        inspect.signature(real.running_instance_ids).bind(
            mod.INSTANCE_TAG_NAME, {}, region=mod.REGION
        )

    def test_launch_self_starting_call_binds(self, monkeypatch):
        import inspect

        from krepis import ec2_spot as real

        mod = _load()
        captured = {}

        def _capture(*args, **kwargs):
            captured["args"], captured["kwargs"] = args, kwargs
            return real.SelfStartingLaunch("i-abc", "spot", False, False)

        monkeypatch.setattr(mod.ec2_spot, "launch_self_starting", _capture)
        mod._launch_instance("tok", "req-1")
        inspect.signature(real.launch_self_starting).bind(
            *captured["args"], **captured["kwargs"]
        )

    def test_the_dispatcher_no_longer_talks_to_ssm(self):
        """alpha-engine-config-I11597: no SSM wait, no send, no post-launch
        terminate window. Read from the source so a revert cannot hide in a
        helper."""
        with open(os.path.join(_HERE, "index.py")) as fh:
            src = fh.read()
        for gone in ("wait_ssm_online", "send_async_command", "terminate_on_failure",
                     "send_command", "describe_instance_information"):
            assert not re.search(rf"\b{gone}\s*\(", src), f"{gone}( is back in index.py"


class TestDispatchPosture:
    def test_disabled_dispatcher_raises_rather_than_returning_a_quiet_noop(self):
        """A disabled dispatcher must never be indistinguishable from a healthy
        run that had nothing to do."""
        mod = _load({"THINKTANK_SPOT_DISPATCH_ENABLED": "false"})
        with pytest.raises(RuntimeError, match="will not run today"):
            mod.handler({}, None)

    def test_degraded_dedupe_probe_launches_anyway_and_records_it(self, monkeypatch):
        """config#2267: a degraded EC2 API must not read as 'no duplicate'.
        For a once-daily arm, coverage beats dedupe — but the choice is
        recorded, never silent."""
        mod = _load()
        monkeypatch.setattr(
            mod, "_already_running", lambda: (_ for _ in ()).throw(mod.SpotProbeError("boom"))
        )
        monkeypatch.setattr(
            mod,
            "_launch_instance",
            lambda *a, **k: mod.ec2_spot.SelfStartingLaunch("i-abc", "spot", False, False),
        )
        out = mod.handler({}, _Ctx())
        assert out["launched"] is True
        assert out["dedupe_degraded"] is True

    def test_existing_box_short_circuits_the_launch(self, monkeypatch):
        mod = _load()
        monkeypatch.setattr(mod, "_already_running", lambda: ["i-live"])
        monkeypatch.setattr(
            mod, "_launch_instance", lambda *a, **k: pytest.fail("must not launch")
        )
        out = mod.handler({}, _Ctx())
        assert out == {"launched": False, "reason": "already_running", "instance_ids": ["i-live"]}

    def test_launch_failure_raises_for_the_async_retry(self, monkeypatch):
        mod = _load()
        monkeypatch.setattr(mod, "_already_running", lambda: [])

        def _exhausted(*a, **k):
            raise mod.SpotLaunchError("spot + on-demand exhausted")

        monkeypatch.setattr(mod, "_launch_instance", _exhausted)
        with pytest.raises(mod.SpotLaunchError):
            mod.handler({}, _Ctx())


class TestRunToken:
    def test_derived_from_the_request_id_in_the_old_shape(self):
        """Same request -> same token (a replay must present the SAME tags and
        user-data, or EC2 refuses the ClientToken); 32 hex chars, the shape
        uuid4().hex had, so the completion-marker key format is unchanged."""
        mod = _load()
        a = mod._run_token("req-A")
        assert a == mod._run_token("req-A")
        assert a != mod._run_token("req-B")
        assert re.fullmatch(r"[0-9a-f]{32}", a)

    def test_handler_uses_the_request_id_as_the_idempotency_key(self, monkeypatch):
        mod = _load()
        seen = {}
        monkeypatch.setattr(mod, "_already_running", lambda: [])

        def _capture(run_token, idempotency_key, **_):
            seen.update(run_token=run_token, key=idempotency_key)
            return mod.ec2_spot.SelfStartingLaunch("i-abc", "spot", False, False)

        monkeypatch.setattr(mod, "_launch_instance", _capture)
        out = mod.handler({}, _Ctx("req-Q"))
        assert seen == {"run_token": mod._run_token("req-Q"), "key": "req-Q"}
        assert out["run_token"] == seen["run_token"]


class TestUserData:
    """The user-data IS the dispatch now: nothing follows it."""

    def test_installs_and_starts_the_job_unit_without_blocking(self):
        mod = _load()
        ud = mod._user_data("tok123")
        assert ud.startswith("#!/bin/bash\n")
        assert mod._job_script("tok123") in ud, "the job must ride verbatim"
        assert f"systemctl start --no-block {mod.JOB_UNIT}.service" in ud
        assert "Type=oneshot" in ud
        assert f"TimeoutStartSec={mod.RUN_TIMEOUT_SECONDS}" in ud
        assert "ExecStopPost=/sbin/shutdown -h now" in ud

    def test_fits_the_ec2_limit_with_room(self):
        from krepis.ec2_spot import USER_DATA_MAX_BYTES

        assert len(_load()._user_data("f" * 32).encode()) < USER_DATA_MAX_BYTES // 2

    def test_carries_no_secret(self):
        """User-data is readable via DescribeInstanceAttribute. It may NAME a
        secret (an SSM parameter) but never carry one."""
        ud = _load()._user_data("tok123")
        for pattern in (
            r"x-access-token", r"\bghp_", r"\bgithub_pat_", r"\bsk-[A-Za-z0-9]",
            r"\bAKIA[0-9A-Z]{8}", r"--with-decryption", r"get-parameter",
        ):
            assert not re.search(pattern, ud), f"user-data matches {pattern}"
        assert "KREPIS_ROUTER_CREDENTIAL_SECRET=ROUTER_CONSUMER_THINKTANK" in ud


class TestDiscriminatorTags:
    """alpha-engine-config-I5752 — the tags spot-orphan-reaper rebuilds the
    completion-marker key from. Without them a reaped box is unlookupable and
    every reap alerts."""

    def test_tags_are_passed_atomically_with_launch(self, monkeypatch):
        mod = _load()
        captured = {}

        def _fake_launch(*args, **kwargs):
            captured.update(kwargs)
            return ("i-abc", "spot")

        monkeypatch.setattr(mod.ec2_spot, "launch_self_starting", _fake_launch)
        mod._launch_instance("tok123", "req-1")
        assert captured["extra_tags"] == {
            "thinktank-trading-day": mod._trading_day(),
            "thinktank-run-token": "tok123",
        }

    def test_tag_order_reproduces_the_key_the_box_writes(self):
        mod = _load()
        """The reaper joins discriminator values by its WATCH_KINDS tuple order
        and appends '.json' to the prefix. The box writes
        ${TRADING_DAY}-${RUN_TOKEN}.json, so trading-day must come first."""
        tags = mod._discriminator_tags("tok123")
        joined = "-".join(
            [tags["thinktank-trading-day"], tags["thinktank-run-token"]]
        )
        assert joined == f"{mod._trading_day()}-tok123"

    def test_the_box_cannot_span_utc_midnight(self):
        """Why the dispatcher may compute the trading day independently of the
        box: both use the UTC date, and the box cannot outlive the day it
        started. Dispatch is 14:30 UTC; this asserts the margin rather than
        trusting it, so moving the schedule or raising the timeout fails here
        instead of surfacing as a silently-missing marker."""
        mod = _load()
        dispatch_hour_utc = 14.5  # cron(30 14 * * ? *), alpha-research-thinktank-daily
        hours_to_midnight = 24 - dispatch_hour_utc
        assert mod.RUN_TIMEOUT_SECONDS / 3600 < hours_to_midnight
        assert mod.WATCHDOG_SECONDS / 3600 < hours_to_midnight


# ── Router addressing (alpha-engine-config-I6367 / I6373) ────────────────


class TestRouterEnvReachesTheBox:
    """Brian's ruling 2026-08-03: no agent directly linked to OpenRouter. The
    Think Tank's tiers address model groups through the authenticated router
    edge, and the box cannot derive any of what that needs for itself."""

    def _prelude(self):
        return _load()._job_script("tok123")

    def test_every_router_var_is_exported(self):
        prelude = self._prelude()
        for var in (
            "KREPIS_EXEC_CONTEXT",
            "KREPIS_LITELLM_PROXY_URL",
            "KREPIS_ROUTER_CREDENTIAL_SECRET",
            "KREPIS_APPCONFIG_APPLICATION",
            "KREPIS_APPCONFIG_CONFIG_PROFILE",
            "KREPIS_APPCONFIG_ENVIRONMENT",
        ):
            assert f"export {var}=" in prelude, (
                f"{var} never reaches the box — krepis' AppConfig path is "
                "opt-in on all three APPCONFIG vars and SWALLOWS its errors, "
                "so a missing one surfaces later as "
                "'LLM_MODEL_REGISTRY.yaml not found', naming neither"
            )

    def test_exec_context_is_ec2_not_lambda(self):
        """It names WHERE CODE RUNS (R28), never how it is attached, and never
        which routes are wanted. Declaring `lambda` from an EC2 box to force a
        route would be a lie the registry then acts on."""
        assert "export KREPIS_EXEC_CONTEXT=ec2" in self._prelude()

    def test_credential_secret_is_the_boxs_own_not_the_shared_one(self):
        """The edge identifies a consumer BY its credential VALUE, and
        krepis.secrets resolves SSM BEFORE os.environ — so naming
        LITELLM_MASTER_KEY here would collapse this box into the director's
        identity at the edge no matter what the environment says."""
        prelude = self._prelude()
        assert (
            "export KREPIS_ROUTER_CREDENTIAL_SECRET=ROUTER_CONSUMER_THINKTANK"
            in prelude
        )
        assert "KREPIS_ROUTER_CREDENTIAL_SECRET=LITELLM_MASTER_KEY" not in prelude

    def test_router_url_is_the_edge_not_a_loopback(self):
        """This is a stock-AMI spot box: the dashboard box's local egress
        proxy at 127.0.0.1:8990 does not answer here. A loopback URL would
        make every call fail connect and read as the router being down."""
        prelude = self._prelude()
        assert "export KREPIS_LITELLM_PROXY_URL=https://router.nousergon.ai:8443" in prelude
        assert "KREPIS_LITELLM_PROXY_URL=http://127.0.0.1" not in prelude

    def test_no_openrouter_credential_is_handed_to_the_box(self):
        assert "OPENROUTER" not in self._prelude()


# ── The I11532 incident, replayed on the self-starting path (I11597) ──────


class _LambdaKilled(BaseException):
    """Lambda's timeout is not an exception the handler can catch — the
    process just stops, so no `except Exception` cleanup runs. A BaseException
    reproduces that."""


class _FakeEc2:
    """EC2 as the real launch path sees it, with the two behaviours the
    self-starting design rests on: a ClientToken already seen with the same
    parameters returns the SAME instance (a mismatch raises
    IdempotentParameterMismatch), and DescribeInstances honours the Name,
    state and client-token filters. Blind switches model DescribeInstances'
    eventual consistency."""

    def __init__(self):
        self.instances: dict[str, dict] = {}
        self.by_token: dict[str, tuple[str, dict]] = {}
        self.run_calls = 0
        self.kill_after_next_launch = False
        self.name_filter_blind = False
        self.token_filter_blind = False

    def run_instances(self, **kwargs):
        from botocore.exceptions import ClientError

        self.run_calls += 1
        token = kwargs.get("ClientToken")
        params = {k: copy.deepcopy(v) for k, v in kwargs.items() if k != "ClientToken"}
        if token in self.by_token:
            iid, seen = self.by_token[token]
            if seen != params:
                raise ClientError(
                    {"Error": {"Code": "IdempotentParameterMismatch", "Message": "x"}},
                    "RunInstances",
                )
            return {"Instances": [{"InstanceId": iid}]}
        iid = f"i-{len(self.instances) + 1:04d}"
        tags = next(
            spec["Tags"] for spec in kwargs["TagSpecifications"]
            if spec["ResourceType"] == "instance"
        )
        self.instances[iid] = {
            "InstanceId": iid,
            "ClientToken": token or "",
            "State": {"Name": "running"},
            "Tags": list(tags),
            "UserData": kwargs.get("UserData"),
        }
        if token:
            self.by_token[token] = (iid, params)
        if self.kill_after_next_launch:
            self.kill_after_next_launch = False
            raise _LambdaKilled()
        return {"Instances": [{"InstanceId": iid}]}

    def describe_instances(self, Filters=(), **_):  # noqa: N803 - boto3 casing
        hits = list(self.instances.values())
        for f in Filters:
            name, values = f["Name"], set(f["Values"])
            if name == "client-token":
                if self.token_filter_blind:
                    return {"Reservations": []}
                hits = [i for i in hits if i["ClientToken"] in values]
            elif name == "tag:Name":
                if self.name_filter_blind:
                    return {"Reservations": []}
                hits = [
                    i for i in hits
                    if any(t["Key"] == "Name" and t["Value"] in values for t in i["Tags"])
                ]
            elif name == "instance-state-name":
                hits = [i for i in hits if i["State"]["Name"] in values]
            else:
                raise AssertionError(f"unexpected filter {name}")
        view = [{k: v for k, v in i.items() if k != "UserData"} for i in hits]
        return {"Reservations": [{"Instances": view}]} if view else {"Reservations": []}


@pytest.fixture
def ec2(monkeypatch):
    """Route EVERY boto3 client the handler, nousergon_lib and krepis create
    to one fake EC2. Asking for any other service fails the test — which is
    how 'no SSM wait, no send' is asserted rather than assumed."""
    import boto3

    fake = _FakeEc2()

    def _client(service, **_):
        assert service == "ec2", f"the dispatcher asked for a {service!r} client"
        return fake

    monkeypatch.setattr(boto3, "client", _client)
    return fake


class TestLaunchRecord:
    """alpha-engine-config-I5752: with no command_id, a reconciler judges a
    self-started run from a launch record + the completion marker + instance
    state. The record sits beside the marker, keyed the same way."""

    def test_record_and_marker_share_one_key(self):
        mod = _load()
        token = "f" * 32
        day = mod._trading_day()
        ud = mod._user_data(token)
        launched = f"s3://alpha-engine-research/thinktank/_control/launched/{day}-{token}.json"
        assert mod._launch_record_uri(token) == launched
        assert f"aws s3 cp - {launched} --region {mod.REGION}" in ud
        completed = f"s3://alpha-engine-research/thinktank/_control/completed/{day}-{token}.json"
        assert f'"completion_marker": "{completed}"' in ud

    def test_marker_key_is_the_one_the_reaper_derives(self):
        """Same key shape spot-orphan-reaper rebuilds from the discriminator
        tags: {completion_prefix}{trading-day}-{run-token}.json."""
        mod = _load()
        tags = mod._discriminator_tags("tok")
        assert mod._record_key(mod.COMPLETION_PREFIX, "tok") == (
            f"thinktank/_control/completed/{tags['thinktank-trading-day']}-"
            f"{tags['thinktank-run-token']}.json"
        )

    def test_record_carries_what_the_reconciler_needs(self):
        ud = _load()._user_data("tok123")
        for field in ('"run_token": "tok123"', '"budget_seconds": 5400', '"workload": "thinktank"',
                      '"timeout_seconds": 7200', '"instance_id": "%s"', '"deadline_at": "%s"'):
            assert field in ud, field

    def test_the_user_data_is_replay_stable(self):
        """EC2 only returns a ClientToken's instance for IDENTICAL parameters."""
        mod = _load()
        assert mod._user_data("tok123") == mod._user_data("tok123")


class TestIncidentReplay:
    """2026-09-23: the Lambda was killed right after the launch. Then the box
    sat idle, because its job was a second step nobody took. Now the job rides
    the launch, and the retry — same event, same request id — must not launch
    a second box whichever EC2 view it gets."""

    def _killed_first_attempt(self, mod, ec2):
        ec2.kill_after_next_launch = True
        with pytest.raises(_LambdaKilled):
            mod.handler({}, _Ctx("req-A"))
        assert list(ec2.instances) == ["i-0001"]
        return ec2.instances["i-0001"]

    def test_the_box_carries_its_own_job(self, ec2):
        """Nothing after RunInstances ran, and nothing needed to: the box's
        user-data installs and starts the job, under the run token its tags
        carry, so its completion marker is the one the reaper will look for."""
        mod = _load()
        box = self._killed_first_attempt(mod, ec2)
        token = mod._run_token("req-A")
        tags = {t["Key"]: t["Value"] for t in box["Tags"]}
        assert tags["thinktank-run-token"] == token
        assert tags["Name"] == mod.INSTANCE_TAG_NAME
        assert box["UserData"] == mod._user_data(token)
        assert f"export THINKTANK_SPOT_RUN_TOKEN={token}" in box["UserData"]
        assert "exec bash infrastructure/thinktank_spot_bootstrap.sh" in box["UserData"]
        assert f"systemctl start --no-block {mod.JOB_UNIT}.service" in box["UserData"]

    def test_retry_that_sees_the_box_skips_it(self, ec2):
        """The pre-I11597 retry did exactly this and lost the day, because the
        box it skipped had no job. Skipping is now CORRECT: the box is running
        its job."""
        mod = _load()
        self._killed_first_attempt(mod, ec2)
        launches = ec2.run_calls
        out = mod.handler({}, _Ctx("req-A"))
        assert out == {"launched": False, "reason": "already_running", "instance_ids": ["i-0001"]}
        assert ec2.run_calls == launches and len(ec2.instances) == 1

    def test_retry_whose_name_probe_is_blind_gets_the_same_box_back(self, ec2):
        """DescribeInstances is eventually consistent: the Name probe can miss
        a box launched seconds ago. The launch's own client-token probe finds
        it and launches nothing."""
        mod = _load()
        self._killed_first_attempt(mod, ec2)
        ec2.name_filter_blind = True
        launches = ec2.run_calls
        out = mod.handler({}, _Ctx("req-A"))
        assert out["launched"] is True and out["replayed"] is True
        assert out["instance_id"] == "i-0001"
        assert out["run_token"] == mod._run_token("req-A")
        assert ec2.run_calls == launches and len(ec2.instances) == 1

    def test_retry_blind_to_everything_still_gets_the_same_box_back(self, ec2):
        """Both probes blind: RunInstances itself is the last line. The replay
        presents the same ClientToken with the same parameters (derived run
        token, same tags, same user-data), and EC2 returns the original box."""
        mod = _load()
        self._killed_first_attempt(mod, ec2)
        ec2.name_filter_blind = ec2.token_filter_blind = True
        out = mod.handler({}, _Ctx("req-A"))
        assert out["launched"] is True and out["instance_id"] == "i-0001"
        assert len(ec2.instances) == 1

    def test_a_different_event_after_the_box_is_gone_launches_normally(self, ec2):
        mod = _load()
        self._killed_first_attempt(mod, ec2)
        ec2.instances["i-0001"]["State"]["Name"] = "terminated"
        out = mod.handler({}, _Ctx("req-B"))
        assert out["launched"] is True and out["replayed"] is False
        assert out["instance_id"] == "i-0002"

    def test_a_healthy_dispatch_is_one_call_and_no_ssm(self, ec2):
        mod = _load()
        out = mod.handler({}, _Ctx("req-A"))
        assert out == {
            "launched": True,
            "replayed": False,
            "instance_id": "i-0001",
            "market": "spot",
            "run_token": mod._run_token("req-A"),
            "launch_record": mod._launch_record_uri(mod._run_token("req-A")),
            "dedupe_degraded": False,
            "idempotency_probe_degraded": False,
        }
        assert ec2.run_calls == 1
