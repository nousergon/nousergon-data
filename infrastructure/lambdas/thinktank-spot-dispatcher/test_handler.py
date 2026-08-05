"""Tests for alpha-engine-thinktank-spot-dispatcher (config-I5208 §47).

The load-bearing assertions here are about the budget/timeout coupling and the
fail-loud posture. A dispatcher that launches a box whose deadline SSM will
preempt reintroduces the exact lost-terminal-writes bug this migration exists
to fix, so that inequality is asserted rather than left to a comment.
"""

from __future__ import annotations

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
    they monkeypatch ``_already_running`` and the ``spot_dispatch`` functions
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
            mod, "_already_running", lambda: (_ for _ in ()).throw(mod.SpotProbeError("boom"))
        )
        monkeypatch.setattr(mod, "_launch_instance", lambda _run_token, force_on_demand=False: ("i-abc", "spot"))
        monkeypatch.setattr(mod.spot_dispatch, "wait_ssm_online", lambda *a, **k: None)
        monkeypatch.setattr(mod.spot_dispatch, "send_async_command", lambda *a, **k: "cmd-1")
        out = mod.handler({}, None)
        assert out["launched"] is True
        assert out["dedupe_degraded"] is True

    def test_existing_box_short_circuits_the_launch(self, monkeypatch):
        mod = _load()
        monkeypatch.setattr(mod, "_already_running", lambda: ["i-live"])
        out = mod.handler({}, None)
        assert out["launched"] is False
        assert out["reason"] == "already_running"

    def test_ssm_failure_terminates_the_box_before_raising(self, monkeypatch):
        """Between launch and the bootstrap landing there is no watchdog on the
        box yet, so this window must tear the box down itself."""
        mod = _load()
        terminated: list[str] = []
        monkeypatch.setattr(mod, "_already_running", lambda: [])
        monkeypatch.setattr(mod, "_launch_instance", lambda _run_token, force_on_demand=False: ("i-xyz", "spot"))
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
