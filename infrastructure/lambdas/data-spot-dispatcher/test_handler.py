"""Unit tests for the data-spot dispatcher's launch fallback (config#2698).

Hermetic: ``nousergon_lib.ec2_spot``, ``krepis``/``krepis.alerts``, and
``boto3`` are stubbed in sys.modules BEFORE importing index (mirrors
ci-watch-dispatcher/test_handler.py). This Lambda calls
``nousergon_lib.ec2_spot.launch()`` DIRECTLY (not through
``nousergon_lib.spot_dispatch.launch_with_fallback``, the chokepoint every
other spot-dispatcher Lambda uses), so it needed its own
``SpotQuotaExceededError`` branch rather than picking one up for free from a
nousergon-lib pin bump alone.

Validates the issue's acceptance criterion: a stubbed launch raising
``SpotQuotaExceededError`` (e.g. MaxSpotInstanceCountExceeded) lands an
on-demand instance, attempts spot exactly once (no type x subnet rotation —
pointless against an account-wide quota ceiling), and fires an
``alerts.publish(severity="warning", ...)`` operator page. Also pins the
pre-existing ``SpotCapacityExhausted`` on-demand fallback (regression guard)
and the end-to-end handler happy path.
"""

from __future__ import annotations

import os
import sys
import types

import pytest

# The REAL renderer, captured before any stub replaces the `krepis` package
# below. krepis.spot_bootstrap is a pure function of its spec — no AWS, no
# clock, no environment reads — which is why the bootstrap was moved into it
# (alpha-engine-config-I7372); stubbing it would leave the rendered script,
# the only thing this Lambda actually sends, untested.
import krepis.spot_bootstrap as _REAL_SPOT_BOOTSTRAP  # noqa: E402

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Stub nousergon_lib.ec2_spot + krepis.alerts + boto3 before importing index ──
class _SpotLaunchError(Exception):
    pass


class _SpotCapacityExhausted(_SpotLaunchError):
    pass


class _SpotQuotaExceededError(_SpotLaunchError):
    pass


def _install_stubs(launch_impl, boto_clients, publish_impl=None):
    ec2_spot_mod = types.ModuleType("nousergon_lib.ec2_spot")
    ec2_spot_mod.SpotLaunchError = _SpotLaunchError
    ec2_spot_mod.SpotCapacityExhausted = _SpotCapacityExhausted
    ec2_spot_mod.SpotQuotaExceededError = _SpotQuotaExceededError
    ec2_spot_mod.launch = launch_impl
    sys.modules["nousergon_lib.ec2_spot"] = ec2_spot_mod

    # index.py's module-level `from nousergon_lib import ec2_spot` resolves the
    # TOP-LEVEL `nousergon_lib` name first — the hermetic_import_guard (and the
    # real import machinery) needs that stubbed too, not just the submodule.
    nousergon_lib_mod = types.ModuleType("nousergon_lib")
    nousergon_lib_mod.ec2_spot = ec2_spot_mod
    sys.modules["nousergon_lib"] = nousergon_lib_mod

    krepis_mod = types.ModuleType("krepis")
    krepis_alerts_mod = types.ModuleType("krepis.alerts")
    krepis_alerts_mod.publish = publish_impl or (lambda *a, **kw: None)
    krepis_mod.alerts = krepis_alerts_mod
    sys.modules["krepis"] = krepis_mod
    sys.modules["krepis.alerts"] = krepis_alerts_mod
    # NOT stubbed — see the module-level import.
    krepis_mod.spot_bootstrap = _REAL_SPOT_BOOTSTRAP
    sys.modules["krepis.spot_bootstrap"] = _REAL_SPOT_BOOTSTRAP

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: boto_clients[name]
    sys.modules["boto3"] = boto3_mod


class _FakeWaiter:
    def wait(self, **kw):
        return None


class _FakeEc2:
    def __init__(self):
        self.terminated = []

    def get_waiter(self, name):
        return _FakeWaiter()

    def terminate_instances(self, InstanceIds):  # noqa: N803 — boto3 kwarg name
        self.terminated.extend(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": i} for i in InstanceIds]}


class _FakeSsm:
    def __init__(self):
        self.sent = []

    def describe_instance_information(self, **kw):
        return {"InstanceInformationList": [{"PingStatus": "Online"}]}

    def send_command(self, **kw):
        self.sent.append(kw)
        return {"Command": {"CommandId": "cmd-123"}}


def _load(monkeypatch, *, launch_impl, publish_impl=None, env=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    ssm = _FakeSsm()
    ec2 = _FakeEc2()
    clients = {"ec2": ec2, "ssm": ssm}
    _install_stubs(launch_impl, clients, publish_impl=publish_impl)

    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)

    import importlib

    if "index" in sys.modules:
        importlib.reload(sys.modules["index"])
    else:
        import index  # noqa: F401
    return sys.modules["index"], ssm, ec2


def test_spot_quota_exceeded_falls_back_to_on_demand_no_rotation_and_pages(monkeypatch):
    """config#2698 acceptance criterion: stubbed RunInstances quota error ⇒
    on-demand instance, spot attempted exactly once, warning page emitted."""
    calls = []
    published = []

    def launch_impl(types_, subnets, *, spot, **kw):
        calls.append(spot)
        if spot:
            raise _SpotQuotaExceededError("MaxSpotInstanceCountExceeded")
        return "i-ondemand"

    def publish_impl(message, *, severity=None, **kw):
        published.append((message, severity, kw))

    index, ssm, ec2 = _load(monkeypatch, launch_impl=launch_impl, publish_impl=publish_impl)

    instance_id, market = index._launch_instance()

    assert instance_id == "i-ondemand"
    assert market == "on-demand"
    # Exactly one spot attempt, then one on-demand attempt — no type x subnet
    # rotation against an account-wide quota ceiling.
    assert calls == [True, False]
    assert len(published) == 1
    message, severity, kw = published[0]
    assert severity == "warning"
    assert "quota" in message.lower()
    assert kw.get("dedup_key", "").startswith("spot-quota-exceeded-")


def test_spot_capacity_exhausted_still_falls_back_to_on_demand(monkeypatch):
    """Regression guard: the pre-existing capacity-exhaustion fallback (which
    predates config#2698) must keep working unchanged."""
    calls = []

    def launch_impl(types_, subnets, *, spot, **kw):
        calls.append(spot)
        if spot:
            raise _SpotCapacityExhausted("all pools exhausted")
        return "i-ondemand"

    index, ssm, ec2 = _load(monkeypatch, launch_impl=launch_impl)

    instance_id, market = index._launch_instance()

    assert instance_id == "i-ondemand"
    assert market == "on-demand"
    assert calls == [True, False]


def test_handler_happy_path_dispatches_bootstrap(monkeypatch):
    def launch_impl(types_, subnets, *, spot, **kw):
        return "i-spotbox"

    index, ssm, ec2 = _load(monkeypatch, launch_impl=launch_impl)

    result = index.handler({"workload": "morning-enrich"}, None)

    assert result["data_spot"]["launched"] is True
    assert result["data_spot"]["instance_id"] == "i-spotbox"
    assert result["data_spot"]["market"] == "spot"
    assert len(ssm.sent) == 1


def test_kill_switch_short_circuits(monkeypatch):
    def launch_impl(types_, subnets, *, spot, **kw):
        raise AssertionError("launch must not be called under the kill-switch")

    index, ssm, ec2 = _load(
        monkeypatch, launch_impl=launch_impl, env={"DATA_SPOT_DISPATCH_ENABLED": "false"}
    )

    result = index.handler({"workload": "morning-enrich"}, None)

    assert result == {"data_spot": {"launched": False, "reason": "disabled", "workload": "morning-enrich"}}


def test_bootstrap_is_the_shared_renderers_output(monkeypatch):
    """alpha-engine-config-I7372 — asserted by containment, never restated."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    cmd = index._bootstrap_command(
        "morning-enrich", "python weekly_collector.py --morning-enrich", "tok"
    )
    assert _REAL_SPOT_BOOTSTRAP.render_bootstrap(index._bootstrap_spec()) in cmd
    # The silent interpreter fallback this handler carried, and the
    # SSM-liveness watchdog it never had.
    assert "PYTHON_BIN" not in cmd
    assert "ec2-spot-watchdog" in cmd


def test_the_pat_reaches_only_the_private_repo(monkeypatch):
    """nousergon-data is public and was already cloned without a credential;
    alpha-engine-config is the one that needs the PAT, read on the BOX."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    cmd = index._bootstrap_command(
        "morning-enrich", "python weekly_collector.py --morning-enrich", "tok"
    )
    authed = [ln for ln in cmd.splitlines() if "x-access-token" in ln]
    assert len(authed) == 1, authed
    assert index.CONFIG_REPO in authed[0]


# ── alpha-engine-config-I10739: the standalone data-collection stack ─────────


def _install_calendar(monkeypatch, answer):
    """krepis.trading_calendar is imported lazily by the handler; the krepis
    stub above has no such submodule, so provide one that records its input."""
    seen = []
    mod = types.ModuleType("krepis.trading_calendar")

    def is_trading_day(d):
        seen.append(d)
        if isinstance(answer, Exception):
            raise answer
        return answer

    mod.is_trading_day = is_trading_day
    monkeypatch.setitem(sys.modules, "krepis.trading_calendar", mod)
    return seen


def test_weekly_phase1_workload_runs_phase1_then_prune_as_one_pipeline_element(monkeypatch):
    """Same two commands, same order as spot_data_phase1.sh; the subshell makes
    PIPESTATUS[0] the pair's exit code, so a failed phase 1 cannot pass."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    workload, cmd = index._resolve_workload({"workload": "weekly-phase-one"})
    assert workload == "weekly-phase-one"
    assert cmd.startswith("( ") and cmd.endswith(" )")
    phase1 = cmd.index("weekly_collector.py --phase 1")
    prune = cmd.index("builders.prune_delisted_tickers --apply")
    assert phase1 < prune
    assert "&&" in cmd[phase1:prune]
    rendered = index._bootstrap_command("weekly-phase-one", cmd, "tok")
    assert f"{cmd} 2>&1 | tee -a" in rendered
    assert "rc=${PIPESTATUS[0]}" in rendered


def test_trading_day_check_launches_nothing_and_uses_the_new_york_date(monkeypatch):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    def launch_impl(*a, **kw):
        raise AssertionError("trading-day-check must never launch a box")

    index, ssm, _ec2 = _load(monkeypatch, launch_impl=launch_impl)
    seen = _install_calendar(monkeypatch, False)
    # 2026-09-08 00:30 UTC is still Monday 2026-09-07 (Labor Day) in New York.
    now = datetime(2026, 9, 8, 0, 30, tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo("America/New_York")
    )
    result = index._trading_day_check(now=now)
    assert result == {"trading_day": {"date": "2026-09-07", "is_trading_day": False}}
    assert [d.isoformat() for d in seen] == ["2026-09-07"]
    assert index.handler({"action": "trading-day-check"}, None)["trading_day"]["is_trading_day"] is False
    assert ssm.sent == []


def test_trading_day_check_propagates_calendar_errors(monkeypatch):
    """An out-of-coverage calendar raises; the SF's Catch pages. Never a guess."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    _install_calendar(monkeypatch, RuntimeError("calendar expired"))
    with pytest.raises(RuntimeError, match="calendar expired"):
        index.handler({"action": "trading-day-check"}, None)
