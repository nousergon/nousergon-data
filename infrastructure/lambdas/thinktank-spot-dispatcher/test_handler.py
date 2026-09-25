"""Tests for alpha-engine-thinktank-spot-dispatcher (config-I5208 §47).

The load-bearing assertions here are about the budget/timeout coupling and the
fail-loud posture. A dispatcher that launches a box whose deadline SSM will
preempt reintroduces the exact lost-terminal-writes bug this migration exists
to fix, so that inequality is asserted rather than left to a comment.
"""

from __future__ import annotations

import datetime
import importlib.util
import os
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
        "THINKTANK_SPOT_SSM_ONLINE_BUDGET_SEC",
        "THINKTANK_SPOT_ADOPT_WINDOW_SEC",
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


class TestTimingCoupling:
    def test_default_budget_is_below_the_ssm_timeout_by_more_than_the_reserve(self):
        """The box's deadline must land before SSM's kill, with room for the
        terminal writes. thinktank.run._TERMINAL_WRITE_RESERVE_S is 120s."""
        mod = _load()
        reserve = 120
        assert mod.RUN_BUDGET_SECONDS + reserve < mod.RUN_TIMEOUT_SECONDS

    def test_watchdog_sits_above_the_ssm_timeout(self):
        """SSM's own kill (which the bootstrap trap turns into a clean
        self-terminate) must always win before the orphan watchdog fires."""
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
        """The bootstrap command is the only channel carrying the budget to
        the box; if it is dropped the runner falls back to its own default and
        the dispatcher's timeout coupling becomes a fiction."""
        mod = _load()
        cmd = mod._bootstrap_command("tok123")
        assert f"export THINKTANK_RUN_BUDGET_SECONDS={mod.RUN_BUDGET_SECONDS}" in cmd


class TestBootstrapCommand:
    def test_execs_the_repo_owned_bootstrap_not_an_inline_copy(self):
        """§47 sub-rule (a): one shared entrypoint. The prelude must hand off
        to the version-controlled script rather than inline the run steps."""
        cmd = _load()._bootstrap_command("tok")
        assert "exec bash infrastructure/thinktank_spot_bootstrap.sh" in cmd

    def test_sets_home_because_ssm_runs_as_root_without_one(self):
        cmd = _load()._bootstrap_command("tok")
        assert "export HOME=/home/ec2-user" in cmd

    def test_installs_git_and_python_before_cloning(self):
        """Stock AL2023 ships neither; the clone is the first thing that needs
        git, so the install must precede it in the command text."""
        cmd = _load()._bootstrap_command("tok")
        assert cmd.index("dnf install") < cmd.index("git clone")

    def test_arms_the_orphan_watchdog(self):
        cmd = _load()._bootstrap_command("tok")
        assert "alpha-engine-thinktank-spot-watchdog" in cmd

    def test_prelude_failure_shuts_the_box_down(self):
        """A botched launch must never idle — spot-orphan-reaper is a 6.5h age
        cap, not a health check."""
        cmd = _load()._bootstrap_command("tok")
        assert "shutdown -h now" in cmd


class TestLibCallSignatures:
    """Bind every spot_dispatch call against the REAL library signature.

    The 2026-07-29 smoke run died on
    ``running_instance_ids() missing 1 required positional argument:
    'discriminator_tags'`` — an error every unit test above missed, because
    they monkeypatch ``_running_boxes`` and the ``spot_dispatch`` functions
    themselves, so no test ever touched the real signature. Mocks make a
    call site untestable exactly where it talks to someone else's contract.
    ``inspect.signature().bind()`` closes that without needing AWS.
    """

    def test_running_instance_ids_call_binds(self):
        import inspect

        from nousergon_lib import spot_dispatch as real

        mod = _load()
        inspect.signature(real.running_instance_ids).bind(
            mod.INSTANCE_TAG_NAME, {}, region=mod.REGION
        )

    def test_launch_with_fallback_call_binds(self):
        import inspect

        from nousergon_lib import spot_dispatch as real

        mod = _load()
        inspect.signature(real.launch_with_fallback).bind(
            mod.INSTANCE_TYPES,
            mod.SUBNETS,
            image_id=mod.AMI_ID,
            key_name=mod.KEY_NAME,
            security_group_ids=[mod.SECURITY_GROUP],
            iam_instance_profile=mod.IAM_PROFILE,
            volume_size_gb=mod.VOLUME_SIZE_GB,
            tag_name=mod.INSTANCE_TAG_NAME,
            region=mod.REGION,
            force_on_demand=False,
        )

    def test_send_async_command_call_binds(self):
        import inspect

        from nousergon_lib import spot_dispatch as real

        mod = _load()
        inspect.signature(real.send_async_command).bind(
            "i-abc",
            mod._bootstrap_command("tok"),
            comment="x",
            region=mod.REGION,
            cw_log_group=mod.CW_LOG_GROUP,
            execution_timeout_seconds=mod.RUN_TIMEOUT_SECONDS,
        )

    def test_wait_ssm_online_and_terminate_calls_bind(self):
        import inspect

        from nousergon_lib import spot_dispatch as real

        mod = _load()
        inspect.signature(real.wait_ssm_online).bind(
            "i-abc", region=mod.REGION, ssm_online_budget_sec=mod.SSM_ONLINE_BUDGET_SEC
        )
        inspect.signature(real.terminate_on_failure).bind(
            "i-abc", region=mod.REGION, label="thinktank-spot"
        )


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
            mod, "_running_boxes", lambda: (_ for _ in ()).throw(mod.SpotProbeError("boom"))
        )
        monkeypatch.setattr(mod, "_launch_instance", lambda _run_token, force_on_demand=False, extra_tags=None: ("i-abc", "spot"))
        monkeypatch.setattr(mod.spot_dispatch, "wait_ssm_online", lambda *a, **k: None)
        monkeypatch.setattr(mod.spot_dispatch, "send_async_command", lambda *a, **k: "cmd-1")
        monkeypatch.setattr(mod, "_record_dispatch", lambda *a: True)
        out = mod.handler({}, None)
        assert out["launched"] is True
        assert out["dedupe_degraded"] is True

    def test_existing_box_short_circuits_the_launch(self, monkeypatch):
        mod = _load()
        monkeypatch.setattr(
            mod,
            "_running_boxes",
            lambda: [{"instance_id": "i-live", "launch_time": None, "tags": {}}],
        )
        out = mod.handler({}, None)
        assert out["launched"] is False
        assert out["reason"] == "already_running"

    def test_ssm_failure_terminates_the_box_before_raising(self, monkeypatch):
        """Between launch and the bootstrap landing there is no watchdog on the
        box yet, so this window must tear the box down itself."""
        mod = _load()
        terminated: list[str] = []
        monkeypatch.setattr(mod, "_running_boxes", lambda: [])
        monkeypatch.setattr(mod, "_launch_instance", lambda _run_token, force_on_demand=False, extra_tags=None: ("i-xyz", "spot"))
        monkeypatch.setattr(
            mod.spot_dispatch,
            "wait_ssm_online",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("ssm never came online")),
        )
        monkeypatch.setattr(
            mod.spot_dispatch,
            "terminate_on_failure",
            lambda iid, **k: terminated.append(iid),
        )
        with pytest.raises(RuntimeError, match="ssm never came online"):
            mod.handler({}, None)
        assert terminated == ["i-xyz"]


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

        monkeypatch.setattr(mod.spot_dispatch, "launch_with_fallback", _fake_launch)
        mod._launch_instance("tok123")
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
        return _load()._bootstrap_command("tok123")

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


# ── Lambda-timeout headroom (alpha-engine-config-I11532) ──────────────────


_DEPLOY_SH = os.path.join(_HERE, "deploy.sh")


def _deploy_fn_timeout() -> int:
    """FN_TIMEOUT as deploy.sh declares it — the ONE place the live timeout is
    set. Parsed, not assumed: the I11532 equality survived review precisely
    because this number and SSM_ONLINE_BUDGET_SEC lived in different files."""
    import re

    with open(_DEPLOY_SH) as fh:
        found = re.findall(r"^FN_TIMEOUT=(\d+)\s*$", fh.read(), flags=re.M)
    assert len(found) == 1, f"deploy.sh must declare FN_TIMEOUT exactly once, found {found}"
    return int(found[0])


class _Ctx:
    """A stand-in Lambda context: a request id and a remaining-time clock."""

    def __init__(self, request_id: str = "req-1", remaining_sec: float = 900.0):
        self.aws_request_id = request_id
        self._remaining_ms = int(remaining_sec * 1000)

    def get_remaining_time_in_millis(self) -> int:
        return self._remaining_ms


class TestLambdaTimeoutHeadroom:
    def test_index_mirrors_the_timeout_deploy_sh_declares(self):
        assert _load().LAMBDA_TIMEOUT_SECONDS == _deploy_fn_timeout()

    @staticmethod
    def _fits(mod, timeout: int) -> bool:
        """Everything between launch and return, strictly inside ``timeout``,
        with the lib's instance_running waiter counted — it runs BEFORE the SSM
        budget starts."""
        return (
            mod.INSTANCE_RUNNING_WAIT_MAX_SEC
            + mod.SSM_ONLINE_BUDGET_SEC
            + mod.DISPATCH_RESERVE_SEC
            < timeout
        )

    def test_the_whole_dispatch_fits_strictly_inside_the_function_timeout(self):
        """THE I11532 guard. 2026-09-23: timeout 300s == SSM_ONLINE_BUDGET_SEC
        300s, so the wait was allowed to eat the invocation and the send never
        happened."""
        mod = _load()
        timeout = _deploy_fn_timeout()
        assert self._fits(mod, timeout), (
            f"instance_running {mod.INSTANCE_RUNNING_WAIT_MAX_SEC}s + SSM Online "
            f"{mod.SSM_ONLINE_BUDGET_SEC}s + reserve {mod.DISPATCH_RESERVE_SEC}s "
            f"does not fit strictly inside deploy.sh FN_TIMEOUT={timeout}s"
        )

    def test_the_guard_rejects_the_2026_09_23_configuration(self):
        """A guard that cannot fail proves nothing: the timeout that lost the
        day, and a budget raised to eat the new headroom, must both be red."""
        mod = _load()
        assert not self._fits(mod, 300)
        greedy = _load({"THINKTANK_SPOT_SSM_ONLINE_BUDGET_SEC": str(_deploy_fn_timeout())})
        assert not self._fits(greedy, _deploy_fn_timeout())

    def test_the_lib_waiter_ceiling_is_what_index_budgets_for(self):
        """INSTANCE_RUNNING_WAIT_MAX_SEC is a mirror of a number inside the
        REAL lib; read the lib, so a lib bump that lengthens the waiter fails
        here instead of silently eating the headroom."""
        import inspect
        import re

        from nousergon_lib import spot_dispatch as real

        src = inspect.getsource(real.wait_ssm_online)
        delay = re.search(r'"Delay":\s*(\d+)', src)
        attempts = re.search(r'"MaxAttempts":\s*(\d+)', src)
        assert delay and attempts, "wait_ssm_online's instance_running waiter changed shape"
        assert int(delay.group(1)) * int(attempts.group(1)) <= _load().INSTANCE_RUNNING_WAIT_MAX_SEC

    def test_the_wait_is_clamped_to_what_the_invocation_has_left(self):
        """Runtime half: whatever the live timeout, the wait gives up with
        enough left to terminate + raise, never gets killed mid-wait."""
        mod = _load()
        assert mod._ssm_online_budget(_Ctx(remaining_sec=900)) == mod.SSM_ONLINE_BUDGET_SEC
        assert mod._ssm_online_budget(_Ctx(remaining_sec=300)) == 300 - 200 - 60
        assert mod._ssm_online_budget(_Ctx(remaining_sec=100)) == 0
        assert mod._ssm_online_budget(None) == mod.SSM_ONLINE_BUDGET_SEC

    def test_adopt_window_covers_the_whole_async_retry_schedule(self):
        """Initial invoke + ~1 min + retry + ~2 min + retry, each up to the
        timeout: an orphan from the first attempt must still be adoptable by
        the last."""
        mod = _load()
        timeout = _deploy_fn_timeout()
        assert mod.ADOPT_WINDOW_SEC >= 2 * timeout + 60 + 120 + mod.DISPATCH_RESERVE_SEC
        assert mod.ADOPT_WINDOW_SEC > timeout


# ── Interrupted-dispatch recovery (alpha-engine-config-I11532) ────────────


class _LambdaKilled(BaseException):
    """Lambda's timeout is not an exception the handler can catch — the
    process just stops, so no `except Exception` cleanup runs. A BaseException
    reproduces that: it sails past the terminate-on-failure handler exactly as
    the 2026-09-23 kill did."""


class _FakeEc2:
    """Just enough EC2 for the handler's reads and writes, over a shared
    instance table so a retry sees what the killed attempt left behind."""

    def __init__(self, world: "_World"):
        self.world = world

    def describe_instances(self, InstanceIds):  # noqa: N803 - boto3 casing
        insts = [
            {
                "InstanceId": iid,
                "LaunchTime": box["launch_time"],
                "Tags": [{"Key": k, "Value": v} for k, v in box["tags"].items()],
            }
            for iid, box in self.world.boxes.items()
            if iid in InstanceIds
        ]
        return {"Reservations": [{"Instances": insts}]}

    def create_tags(self, Resources, Tags):  # noqa: N803 - boto3 casing
        for iid in Resources:
            self.world.boxes[iid]["tags"].update({t["Key"]: t["Value"] for t in Tags})


class _World:
    def __init__(self, mod, monkeypatch):
        self.mod = mod
        self.boxes: dict[str, dict] = {}
        self.sent: list[tuple[str, str]] = []
        self.launched: list[str] = []
        self.terminated: list[str] = []
        self.ssm_waits: list[tuple[str, int]] = []
        self.kill_during_ssm_wait = False
        ec2 = _FakeEc2(self)
        monkeypatch.setattr(mod.boto3, "client", lambda svc, **k: ec2)
        monkeypatch.setattr(mod.spot_dispatch, "launch_with_fallback", self._launch)
        monkeypatch.setattr(
            mod.spot_dispatch, "running_instance_ids", lambda name, disc, region: list(self.boxes)
        )
        monkeypatch.setattr(mod.spot_dispatch, "wait_ssm_online", self._wait)
        monkeypatch.setattr(mod.spot_dispatch, "send_async_command", self._send)
        monkeypatch.setattr(
            mod.spot_dispatch, "terminate_on_failure", lambda iid, **k: self.terminated.append(iid)
        )

    def _launch(self, types, subnets, *, tag_name, extra_tags, **_):
        iid = f"i-{len(self.boxes) + 1:04d}"
        self.boxes[iid] = {
            "launch_time": datetime.datetime.now(datetime.timezone.utc),
            "tags": {"Name": tag_name, **extra_tags},
        }
        self.launched.append(iid)
        return iid, "spot"

    def _wait(self, iid, *, region, ssm_online_budget_sec):
        self.ssm_waits.append((iid, ssm_online_budget_sec))
        if self.kill_during_ssm_wait:
            raise _LambdaKilled()

    def _send(self, iid, command, **_):
        cid = f"cmd-{len(self.sent) + 1}"
        self.sent.append((iid, command))
        return cid

    def age(self, iid, seconds):
        self.boxes[iid]["launch_time"] = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(seconds=seconds)


class TestInterruptedDispatchRecovery:
    def _interrupted_first_attempt(self, mod, world, request_id="req-A"):
        """Replay 2026-09-23: launch succeeds, Lambda dies in the SSM wait."""
        world.kill_during_ssm_wait = True
        with pytest.raises(_LambdaKilled):
            mod.handler({}, _Ctx(request_id=request_id))
        world.kill_during_ssm_wait = False
        assert world.launched == ["i-0001"] and world.sent == []
        assert world.terminated == [], "a Lambda kill runs no cleanup"
        box = world.boxes["i-0001"]
        assert mod.COMMAND_ID_TAG not in box["tags"]
        return box

    def test_the_async_retry_sends_the_command_to_the_orphan_instead_of_skipping(
        self, monkeypatch
    ):
        """THE closes-when case: launch happened, send never did. EventBridge's
        retry (same request id) must finish the dispatch on that box — not
        skip it as a healthy concurrent run, and not launch a second box."""
        mod = _load()
        world = _World(mod, monkeypatch)
        box = self._interrupted_first_attempt(mod, world)
        world.age("i-0001", 60)  # the retry lands ~1 min later

        out = mod.handler({}, _Ctx(request_id="req-A"))

        assert out["launched"] is True and out["adopted"] is True
        assert out["instance_id"] == "i-0001"
        assert world.launched == ["i-0001"], "the retry must not launch a second box"
        assert [iid for iid, _ in world.sent] == ["i-0001"]
        # Same run token as the launch tagged, so the completion marker the box
        # writes is the one spot-orphan-reaper reconstructs from its tags.
        token = box["tags"]["thinktank-run-token"]
        assert out["run_token"] == token
        assert f"export THINKTANK_SPOT_RUN_TOKEN={token}" in world.sent[0][1]
        assert box["tags"][mod.COMMAND_ID_TAG] == out["command_id"]
        assert out["command_tagged"] is True

    def test_an_orphan_older_than_any_invocation_is_adopted_by_any_request(
        self, monkeypatch
    ):
        """If the retry's request id ever differs, age is the proof: no
        invocation outlives the timeout, so nothing can still be dispatching."""
        mod = _load()
        world = _World(mod, monkeypatch)
        self._interrupted_first_attempt(mod, world, request_id="req-A")
        world.age("i-0001", mod.LAMBDA_TIMEOUT_SECONDS + 60)

        out = mod.handler({}, _Ctx(request_id="req-B"))

        assert out["adopted"] is True and [iid for iid, _ in world.sent] == ["i-0001"]

    def test_a_young_undispatched_box_of_another_request_is_left_alone(self, monkeypatch):
        """A duplicate EventBridge delivery or a manual invoke can arrive while
        the first invocation is still legitimately waiting for SSM. Sending
        then would double-run the box, so it skips, as before."""
        mod = _load()
        world = _World(mod, monkeypatch)
        self._interrupted_first_attempt(mod, world, request_id="req-A")
        world.age("i-0001", 120)

        out = mod.handler({}, _Ctx(request_id="req-B"))

        assert out == {"launched": False, "reason": "already_running", "instance_ids": ["i-0001"]}
        assert world.sent == []

    def test_a_healthy_dispatched_box_still_skips_the_retry(self, monkeypatch):
        """The guard's original job survives: a box that GOT its command is a
        live run, and a retry must neither resend nor launch."""
        mod = _load()
        world = _World(mod, monkeypatch)
        first = mod.handler({}, _Ctx(request_id="req-A"))
        assert first["launched"] is True and first["adopted"] is False
        assert world.boxes["i-0001"]["tags"][mod.COMMAND_ID_TAG] == first["command_id"]
        world.age("i-0001", mod.LAMBDA_TIMEOUT_SECONDS + 60)

        for rid in ("req-A", "req-B"):
            out = mod.handler({}, _Ctx(request_id=rid))
            assert out["launched"] is False and out["reason"] == "already_running"
        assert len(world.sent) == 1 and world.launched == ["i-0001"]

    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(lambda b: b["tags"].update({"thinktank-trading-day": "2000-01-01"}), id="other-trading-day"),
            pytest.param(lambda b: b["tags"].pop("thinktank-dispatch-request-id"), id="pre-I11532-box"),
            pytest.param(lambda b: b["tags"].update({"termination-reason": "x"}), id="being-terminated"),
        ],
    )
    def test_boxes_that_are_not_provably_this_days_orphan_are_skipped(
        self, monkeypatch, mutate
    ):
        mod = _load()
        world = _World(mod, monkeypatch)
        self._interrupted_first_attempt(mod, world)
        mutate(world.boxes["i-0001"])

        out = mod.handler({}, _Ctx(request_id="req-A"))

        assert out["launched"] is False and world.sent == []

    def test_an_orphan_past_the_adopt_window_is_skipped(self, monkeypatch):
        mod = _load()
        world = _World(mod, monkeypatch)
        self._interrupted_first_attempt(mod, world)
        world.age("i-0001", mod.ADOPT_WINDOW_SEC + 60)

        out = mod.handler({}, _Ctx(request_id="req-A"))

        assert out["launched"] is False and world.sent == []

    def test_an_adopted_box_whose_ssm_still_fails_is_terminated_and_raises(
        self, monkeypatch
    ):
        """Adoption keeps the fail-loud posture: if the orphan still cannot be
        reached it is torn down and the error raised, so the next retry
        launches a fresh box rather than finding the same orphan again."""
        mod = _load()
        world = _World(mod, monkeypatch)
        self._interrupted_first_attempt(mod, world)

        def _fail(*a, **k):
            raise RuntimeError("SSM agent not Online")

        monkeypatch.setattr(mod.spot_dispatch, "wait_ssm_online", _fail)
        with pytest.raises(RuntimeError, match="not Online"):
            mod.handler({}, _Ctx(request_id="req-A"))
        assert world.terminated == ["i-0001"]

    def test_the_launch_stamps_the_request_id_atomically(self, monkeypatch):
        mod = _load()
        world = _World(mod, monkeypatch)
        mod.handler({}, _Ctx(request_id="req-Z"))
        assert world.boxes["i-0001"]["tags"][mod.DISPATCH_REQUEST_ID_TAG] == "req-Z"

    def test_a_tagging_failure_after_the_send_never_raises(self, monkeypatch):
        """The command is already running; raising would trigger an async
        retry that resends it. Report it, do not throw."""
        mod = _load()
        world = _World(mod, monkeypatch)

        def _deny(**_):
            raise RuntimeError("AccessDenied")

        monkeypatch.setattr(_FakeEc2, "create_tags", lambda self, **k: _deny(**k))
        out = mod.handler({}, _Ctx(request_id="req-A"))
        assert out["launched"] is True and out["command_tagged"] is False
        assert world.terminated == []

    def test_a_failed_tag_read_degrades_to_launch_rather_than_skipping(self, monkeypatch):
        """config#2267: an unreadable guard must never read as 'skip'."""
        mod = _load()
        world = _World(mod, monkeypatch)
        world.boxes["i-9999"] = {
            "launch_time": datetime.datetime.now(datetime.timezone.utc),
            "tags": {"Name": mod.INSTANCE_TAG_NAME},
        }

        def _boom(self, InstanceIds):  # noqa: N803
            raise RuntimeError("throttled")

        monkeypatch.setattr(_FakeEc2, "describe_instances", _boom)
        out = mod.handler({}, _Ctx(request_id="req-A"))
        assert out["launched"] is True and out["dedupe_degraded"] is True
