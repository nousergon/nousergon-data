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


def _install_stubs(monkeypatch, launch_impl, boto_clients, publish_impl=None):
    # Every sys.modules write goes through monkeypatch.setitem so it is UNDONE
    # at teardown. A bare `sys.modules[...] = stub` outlives this file: any test
    # sharing the process afterwards (e.g. `pytest <this file> tests/`) imports
    # the stub `nousergon_lib`/`krepis`/`boto3` instead of the real package and
    # fails by import order (alpha-engine-config-I11229).
    ec2_spot_mod = types.ModuleType("nousergon_lib.ec2_spot")
    ec2_spot_mod.SpotLaunchError = _SpotLaunchError
    ec2_spot_mod.SpotCapacityExhausted = _SpotCapacityExhausted
    ec2_spot_mod.SpotQuotaExceededError = _SpotQuotaExceededError
    ec2_spot_mod.launch = launch_impl
    monkeypatch.setitem(sys.modules, "nousergon_lib.ec2_spot", ec2_spot_mod)

    # index.py's module-level `from nousergon_lib import ec2_spot` resolves the
    # TOP-LEVEL `nousergon_lib` name first — the hermetic_import_guard (and the
    # real import machinery) needs that stubbed too, not just the submodule.
    nousergon_lib_mod = types.ModuleType("nousergon_lib")
    nousergon_lib_mod.ec2_spot = ec2_spot_mod
    monkeypatch.setitem(sys.modules, "nousergon_lib", nousergon_lib_mod)

    krepis_mod = types.ModuleType("krepis")
    krepis_alerts_mod = types.ModuleType("krepis.alerts")
    krepis_alerts_mod.publish = publish_impl or (lambda *a, **kw: None)
    krepis_mod.alerts = krepis_alerts_mod
    monkeypatch.setitem(sys.modules, "krepis", krepis_mod)
    monkeypatch.setitem(sys.modules, "krepis.alerts", krepis_alerts_mod)
    # NOT stubbed — see the module-level import.
    krepis_mod.spot_bootstrap = _REAL_SPOT_BOOTSTRAP
    monkeypatch.setitem(sys.modules, "krepis.spot_bootstrap", _REAL_SPOT_BOOTSTRAP)

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: boto_clients[name]
    monkeypatch.setitem(sys.modules, "boto3", boto3_mod)


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
    _install_stubs(monkeypatch, launch_impl, clients, publish_impl=publish_impl)

    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied

    assert_hermetic_imports_satisfied(__file__)

    # Load THIS Lambda's index.py by path, fresh against this test's stubs, and
    # drop it again at teardown (alpha-engine-config-I11229). Two reasons:
    #   * an `index` left in sys.modules keeps references to the stubs above
    #     after they are restored;
    #   * `import index` resolves through sys.path, and every Lambda has an
    #     index.py. In a shared process, another test that puts its own
    #     Lambda's directory at the front of sys.path makes the name resolve to
    #     THAT handler (measured: `pytest <this file> tests/` loaded the
    #     run-scope Lambda's index.py here).
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "index", os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.py")
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "index", module)
    spec.loader.exec_module(module)
    return module, ssm, ec2


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


def test_every_launch_carries_the_cost_allocation_tag(monkeypatch):
    """alpha-engine-config-I10788: every launched instance — spot, the
    on-demand fallback, AND force_on_demand — carries `component=data-
    collection` in the SAME RunInstances TagSpecifications entry as the
    Name tag, unconditionally, so the resource class the issue's scope
    measurement found carrying no cost tag at all now does."""
    seen_extra_tags = []

    def launch_impl(types_, subnets, *, spot, extra_tags=None, **kw):
        seen_extra_tags.append(extra_tags)
        return "i-tagged"

    index, ssm, ec2 = _load(monkeypatch, launch_impl=launch_impl)

    instance_id, market = index._launch_instance()
    assert instance_id == "i-tagged"
    assert seen_extra_tags[-1] == {"component": "data-collection"}

    # force_on_demand path (spot-interruption retry) also carries it.
    instance_id, market = index._launch_instance(force_on_demand=True)
    assert seen_extra_tags[-1] == {"component": "data-collection"}


def test_cost_tag_survives_alongside_per_run_identity_tags(monkeypatch):
    """The handler's per-run identity tags (execution_id, run_date,
    pipeline_role -> config#5504) and the cost tag are DISTINCT keys, so
    neither one drops the other when both are present on the same launch."""

    def launch_impl(types_, subnets, *, spot, **kw):
        return "i-both"

    index, ssm, ec2 = _load(monkeypatch, launch_impl=launch_impl)

    seen_extra_tags = []
    real_launch = sys.modules["nousergon_lib.ec2_spot"].launch

    def wrapped(types_, subnets, *, extra_tags=None, **kw):
        seen_extra_tags.append(extra_tags)
        return real_launch(types_, subnets, extra_tags=extra_tags, **kw)

    sys.modules["nousergon_lib.ec2_spot"].launch = wrapped

    result = index.handler(
        {"workload": "morning-enrich", "execution_id": "exec-1", "run_date": "2026-09-21"}, None
    )

    assert result["data_spot"]["launched"] is True
    assert seen_extra_tags[-1] == {
        "execution-id": "exec-1",
        "run-date": "2026-09-21",
        "component": "data-collection",
    }


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
        "morning-enrich",
        "python weekly_collector.py --morning-enrich",
        "tok",
        instance_id="i-x",
        trading_day="2026-09-21",
    )
    spec = index._bootstrap_spec(
        "morning-enrich",
        run_log=_REAL_SPOT_BOOTSTRAP.RunLog(
            local_path="/var/log/data-spot-morning-enrich.log",
            s3_uri=index._run_log_uri("morning-enrich", "2026-09-21", "i-x"),
            # alpha-engine-config-I11359: `_bootstrap_command` turns this on;
            # mirrored here or `render_bootstrap(spec)` diverges from `cmd`.
            gzip=True,
        ),
    )
    assert _REAL_SPOT_BOOTSTRAP.render_bootstrap(spec) in cmd
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
    assert f"\n{cmd}\nrc=$?" in rendered
    # No pipe any more: the renderer's run-log block `exec`s this shell's
    # stdout through tee, so the collector runs unpiped and `$?` IS its own
    # exit code (alpha-engine-config-I11353).
    assert "| tee -a" not in rendered.split("_run_log_shipper_loop")[-1]
    assert "rc=${PIPESTATUS[0]}" not in rendered


# ── alpha-engine-config-I11002: D34 (chronic-gap-heal) had no successor ──────


def test_chronic_gap_heal_is_in_the_allowlist_and_needs_no_trading_day(monkeypatch):
    """D34's heal operates on the current chronic-gap state, not a historical
    trading day — same 'today' semantics weekly-phase-one and
    rag-weekly-ingestion already use, so it is deliberately absent from
    `_WORKLOADS_REQUIRING_TRADING_DAY`."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    assert "chronic-gap-heal" in index._WORKLOADS
    assert "chronic-gap-heal" not in index._WORKLOADS_REQUIRING_TRADING_DAY
    workload, cmd = index._resolve_workload({"workload": "chronic-gap-heal"})
    assert workload == "chronic-gap-heal"
    assert cmd == "python weekly_collector.py --chronic-gap-heal"
    rendered = index._bootstrap_command("chronic-gap-heal", cmd, "tok")
    assert f"\n{cmd}\nrc=$?" in rendered
    # No pipe any more: the renderer's run-log block `exec`s this shell's
    # stdout through tee, so the collector runs unpiped and `$?` IS its own
    # exit code (alpha-engine-config-I11353).
    assert "| tee -a" not in rendered.split("_run_log_shipper_loop")[-1]
    assert "rc=${PIPESTATUS[0]}" not in rendered


# ── alpha-engine-config-I10778, plan P-11: the pre-cutover shadow run ────────


def test_shadow_weekday_is_in_the_allowlist(monkeypatch):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    assert "shadow-weekday" in index._WORKLOADS
    assert "shadow-weekday" in index._WORKLOADS_REQUIRING_TRADING_DAY


def test_shadow_weekday_renders_the_four_shadow_runs_then_parity_in_order(monkeypatch):
    """One subshell, five legs in the declared order, one exit code -- and each
    of the four boundary invocations gets the SAME weekly_collector.py flags the
    scheduled workloads above it use, plus `--date` pinning it to the requested
    historical trading day.

    The legs are NO LONGER `&&`-chained (alpha-engine-config-I11200): each runs
    independently and records its exit code, so the comparator always runs. The
    order assertion below is about SEQUENCE, not about any leg gating the next.
    `test_shadow_legs_independent.py` owns the independence property itself.
    """
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    workload, cmd = index._resolve_workload(
        {"workload": "shadow-weekday", "trading_day": "2026-09-14"}
    )
    assert workload == "shadow-weekday"
    assert cmd.startswith("( ") and cmd.endswith(" )")
    assert "{trading_day}" not in cmd  # substituted, never left as a template

    legs = [
        "--morning-enrich --skip-chronic-heal --skip-arctic-append --date 2026-09-14",
        "--morning-arctic-append --date 2026-09-14",
        "--daily --skip-arctic-append --date 2026-09-14",
        "--daily-arctic-append --date 2026-09-14",
        "python -m shadow parity --trading-day 2026-09-14 --legs-file $LEGS",
    ]
    positions = [cmd.index(leg) for leg in legs]
    assert positions == sorted(positions), "legs must run in the declared order"
    # Every shadow-run leg AND the parity comparison target the requested
    # trading day, not "today" (4 `shadow run` legs + 1 `shadow parity`).
    assert cmd.count("--trading-day 2026-09-14") == 5
    assert "--store s3://alpha-engine-research/data_collection" in cmd
    # Each producer leg records its outcome, and the workload still fails when
    # any of them did: the four recorded legs plus the comparator's own code.
    assert cmd.count(">> $LEGS") == 4
    assert cmd.count("RC_ALL=$RC") == 4
    assert "[ $RC_ALL -ne 0 ] && exit $RC_ALL" in cmd

    rendered = index._bootstrap_command("shadow-weekday", cmd, "tok")
    assert f"\n{cmd}\nrc=$?" in rendered
    # No pipe any more: the renderer's run-log block `exec`s this shell's
    # stdout through tee, so the collector runs unpiped and `$?` IS its own
    # exit code (alpha-engine-config-I11353).
    assert "| tee -a" not in rendered.split("_run_log_shipper_loop")[-1]
    assert "rc=${PIPESTATUS[0]}" not in rendered


def _every_resolved_workload(index):
    for workload in sorted(index._WORKLOADS):
        event = {"workload": workload}
        if workload in index._WORKLOADS_REQUIRING_TRADING_DAY:
            event["trading_day"] = "2026-09-14"
        yield index._resolve_workload(event)


def test_every_workload_boot_installs_gitleaks_and_gates_on_dlp_preflight(monkeypatch):
    """alpha-engine-config-I10866 deliverable 4 (class: I10370). The 2026-09-15
    shadow-weekday box's flow-doctor diagnosis failed closed with 'gitleaks
    binary not found on PATH'. Every workload this dispatcher runs makes LLM
    calls through flow-doctor, so every rendered bootstrap must install the
    pinned binary and pass the DLP preflight BEFORE the collector starts."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    for workload, cmd in _every_resolved_workload(index):
        rendered = index._bootstrap_command(workload, cmd, "tok")
        asset = f"gitleaks_{index.GITLEAKS_VERSION}_linux_x64.tar.gz"
        assert asset in rendered, workload
        assert f'echo "{index.GITLEAKS_SHA256}  /tmp/gitleaks.tar.gz" | sha256sum -c -' in rendered, workload
        preflight = rendered.index("python -m krepis.session_dlp preflight || fail")
        installed = rendered.index('command -v gitleaks >/dev/null 2>&1 || fail')
        venv = rendered.index("source .venv/bin/activate")
        run = rendered.index(f"\n{cmd}\nrc=$?")
        assert venv < installed < preflight < run, workload
        assert "KREPIS_DLP_DISABLED" not in rendered


def test_gitleaks_pin_moves_in_lockstep_with_spot_common():
    import re

    here = os.path.dirname(os.path.abspath(__file__))
    common = open(os.path.join(here, "..", "..", "_spot_common.sh"), encoding="utf-8").read()
    version = re.search(r"^GITLEAKS_VERSION=(\S+)$", common, re.M).group(1)
    sha = re.search(r"^GITLEAKS_SHA256=(\S+)$", common, re.M).group(1)
    source = open(os.path.join(here, "index.py"), encoding="utf-8").read()
    assert f'GITLEAKS_VERSION = "{version}"' in source
    assert f'GITLEAKS_SHA256 = "{sha}"' in source


def test_shadow_weekday_runtime_cap_covers_the_chained_legs(monkeypatch):
    """The four chained legs plus the ArcticDB seed cannot fit the shared
    7200 s default. SSM executionTimeout and the box's hard-stop timer must
    both carry the larger cap, and every other workload keeps the default.

    `shadow-sameday` runs the SAME four legs plus parity and carries the SAME
    cap: it differs from `shadow-weekday` only in resolving its trading day on
    the box rather than from the event, which changes nothing about how long
    the chain takes (alpha-engine-config-I11203)."""
    index, ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    for workload, cmd in _every_resolved_workload(index):
        if workload in ("shadow-weekday", "shadow-sameday"):
            expected = 18000
        elif workload == "shadow-morning":
            # Two legs plus both comparators and a bounded wait for v1's own
            # append: measured 42-45 min before the ArcticDB leg, ~96 min worst
            # case with it (alpha-engine-config-I11546) — still well under the
            # chained workloads' budget, not a copy of it.
            expected = 7200
        elif workload == "post-market-data":
            # alpha-engine-config-I11363: a DECLARED EOD cap, measured 30.4-45.7
            # min over 2026-09-15..23, not the inherited default.
            expected = 5400
        elif workload == "post-market-arctic-append":
            # Same arc: measured 24.7-28.1 min.
            expected = 3600
        else:
            expected = index.MAX_RUNTIME_SECONDS
        assert index._max_runtime_seconds(workload) == expected
        assert index._bootstrap_spec(workload).max_runtime_seconds == expected
        index._send_bootstrap("i-x", workload, cmd, "tok")
        assert ssm.sent[-1]["Parameters"]["executionTimeout"] == [str(expected)]
    assert index._max_runtime_seconds(None) == index.MAX_RUNTIME_SECONDS


def test_shadow_weekday_parity_window_is_a_declared_non_requirement(monkeypatch):
    """alpha-engine-config-I10892 deliverable 3. Parity grades against the
    VersionId v1's manifest recorded for the trading day, so a dispatch that
    overlaps v1's D+1 postclose is NOT refused. The declaration exists, is
    None, and resolving the workload stays silent and succeeds."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    assert index._WORKLOAD_PARITY_WINDOW == {
        "shadow-weekday": None,
        "shadow-sameday": None,
        "shadow-morning": None,
        "shadow-parity": None,
        "arctic-parity": None,
    }
    assert set(index._WORKLOAD_PARITY_WINDOW) <= set(index._WORKLOADS)
    workload, _cmd = index._resolve_workload({"workload": "shadow-weekday", "trading_day": "2026-09-14"})
    assert workload == "shadow-weekday"


def test_shadow_weekday_requires_trading_day(monkeypatch):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    with pytest.raises(ValueError, match="requires event\\['trading_day'\\]"):
        index._resolve_workload({"workload": "shadow-weekday"})


@pytest.mark.parametrize(
    "bad_day",
    [
        "",
        "not-a-date",
        "2026/09/14",
        "26-09-14",
        "2026-13-40",  # regex-shaped, not a real calendar date
        "2026-09-14; rm -rf /",  # injection attempt
        "2026-09-14T00:00:00",  # trailing content the regex must not tolerate
    ],
)
def test_shadow_weekday_rejects_malformed_trading_day(monkeypatch, bad_day):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    with pytest.raises(ValueError):
        index._resolve_workload({"workload": "shadow-weekday", "trading_day": bad_day})


def test_shadow_weekday_other_workloads_ignore_trading_day(monkeypatch):
    """A non-templated workload never requires (or substitutes) trading_day —
    only the workloads that opt in via _WORKLOADS_REQUIRING_TRADING_DAY do."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    workload, cmd = index._resolve_workload({"workload": "morning-enrich"})
    assert workload == "morning-enrich"
    assert cmd == index._WORKLOADS["morning-enrich"]


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


# ── alpha-engine-config-I10787 (P-20): the run-manifest completion check ──────
#
# The four failure modes each get their own test, because "the check failed" is
# not the claim — the claim is that a MISSING manifest, a non-ok STATUS, an
# undelivered KEY and a row count under its FLOOR are four distinct, separately
# named findings, and the ASL routes each to its own Fail state.

import datetime as _dt
import json as _json
import pathlib as _pathlib

_REPO_ROOT = _pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))  # the packaged data_gate/ + registry.d/, as in the zip

_STARTED = "2026-09-14T20:45:00.000Z"
_PREFIX = "data_collection/runs/D20"


def _manifest(**over):
    doc = {
        "schema_version": "data_run_manifest.v1",
        "run_id": "01K5AAAAAAAAAAAAAAAAAAAAAA",
        "unit_id": "D20",
        "trading_day": "2026-09-14",
        "status": "ok",
        "reason": "",
        "started": "2026-09-14T20:46:00Z",
        "finished": "2026-09-14T20:59:00Z",
        "outputs": [
            {"key": "market_data/eod_closes/2026-09-14.json", "rows_out": 88},
            {"key": "market_data/eod_closes/latest.json", "rows_out": 88},
            {"key": "market_data/fx/2026-09-14.json", "rows_out": 12},
            {"key": "market_data/fx/latest.json", "rows_out": 12},
        ],
        "guards": [],
    }
    doc.update(over)
    return doc


class _FakeS3:
    """Only the two calls the check makes, so a third would fail loud here too."""

    def __init__(self, objects):
        self.objects = dict(objects)  # key -> manifest dict
        self.list_calls = []

    def list_objects_v2(self, **kw):
        self.list_calls.append(kw)
        prefix, after = kw["Prefix"], kw.get("StartAfter", "")
        keys = sorted(k for k in self.objects if k.startswith(prefix) and k > after)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        class _Body:
            def __init__(self, payload):
                self._payload = payload

            def read(self):
                return _json.dumps(self._payload).encode()

        return {"Body": _Body(self.objects[Key])}


def _check(index, objects, units=("D20",), started_at=_STARTED):
    s3 = _FakeS3(objects)
    return index._completion_check(
        {"action": "completion-check", "collection": "eod", "units": list(units),
         "started_at": started_at},
        s3_client=s3,
    ), s3


def _index(monkeypatch):
    return _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")[0]


def test_completion_check_passes_when_every_declared_key_is_published(monkeypatch):
    index = _index(monkeypatch)
    result, s3 = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": _manifest()})
    completion = result["completion"]
    assert completion["ok"] is True
    assert completion["failure_mode"] == ""
    assert completion["findings"] == []
    row = completion["units"][0]
    assert row["unit"] == "D20" and row["status"] == "ok"
    # D20 declares four key templates; all four are S3 key templates, all graded.
    assert row["keys_checked"] == 4 and row["unverifiable"] == []
    # The listing is bounded by a StartAfter sentinel rather than walking the
    # unit's whole history, and it never guesses a trading day.
    assert s3.list_calls[0]["StartAfter"].startswith(f"{_PREFIX}/2026-09-1")


def test_completion_check_launches_nothing(monkeypatch):
    """Same contract as trading-day-check: a pure computation on the dispatcher."""
    def launch_impl(*a, **kw):
        raise AssertionError("completion-check must never launch a box")

    index, ssm, _ec2 = _load(monkeypatch, launch_impl=launch_impl)
    index._completion_check(
        {"units": ["D20"], "started_at": _STARTED, "collection": "eod"},
        s3_client=_FakeS3({f"{_PREFIX}/2026-09-14/01K5AA.json": _manifest()}),
    )
    assert ssm.sent == []


# ── failure mode 1 of 4: the manifest is missing for THIS execution ───────────


def test_manifest_missing_when_the_unit_left_no_record(monkeypatch):
    index = _index(monkeypatch)
    result, _ = _check(index, {})
    completion = result["completion"]
    assert completion["ok"] is False
    assert completion["failure_mode"] == "manifest_missing"
    assert [f["mode"] for f in completion["findings"]] == ["manifest_missing"]
    assert completion["findings"][0]["unit"] == "D20"
    assert _PREFIX in completion["findings"][0]["detail"]


def test_a_manifest_that_finished_before_this_execution_is_missing_not_stale_pass(monkeypatch):
    """The freshness half of the old HEAD check, kept: yesterday's run is not
    this run, and the Cause names the stale run so the operator sees which."""
    index = _index(monkeypatch)
    old = _manifest(finished="2026-09-13T21:00:00Z", trading_day="2026-09-13")
    result, _ = _check(index, {f"{_PREFIX}/2026-09-13/01K4ZZ.json": old})
    assert result["completion"]["failure_mode"] == "manifest_missing"
    assert "01K4ZZ" in result["completion"]["findings"][0]["detail"]


# ── failure mode 2 of 4: status is not ok ────────────────────────────────────


def test_a_failed_run_is_a_named_failure_not_a_missing_output(monkeypatch):
    index = _index(monkeypatch)
    doc = _manifest(status="failed", reason="RuntimeError: yfinance 429", outputs=[])
    result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": doc})
    completion = result["completion"]
    assert completion["failure_mode"] == "run_not_ok"
    assert [f["mode"] for f in completion["findings"]] == ["run_not_ok"]
    assert "yfinance 429" in completion["findings"][0]["detail"]


def test_not_applicable_is_not_a_pass_for_a_unit_the_machine_names(monkeypatch):
    """The rule: naming a unit in verify_units IS the machine's declaration that
    this run must publish it. A unit that may legitimately no-op is not named."""
    index = _index(monkeypatch)
    doc = _manifest(status="not_applicable", reason="no_new_data_declared", outputs=[])
    result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": doc})
    assert result["completion"]["failure_mode"] == "run_not_ok"
    assert "not_applicable" in result["completion"]["findings"][0]["detail"]


# ── failure mode 3 of 4: a declared key is absent from outputs[] ──────────────


def test_a_declared_key_absent_from_outputs_fails_by_name(monkeypatch):
    """The whole point of P-20: the old check covered two keys. D20 publishes
    four, and dropping fx is now a finding naming the unit AND the key."""
    index = _index(monkeypatch)
    doc = _manifest(outputs=[
        {"key": "market_data/eod_closes/2026-09-14.json", "rows_out": 88},
        {"key": "market_data/eod_closes/latest.json", "rows_out": 88},
    ])
    result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": doc})
    completion = result["completion"]
    assert completion["failure_mode"] == "output_missing"
    missing = sorted(f["key"] for f in completion["findings"])
    assert missing == ["market_data/fx/latest.json", "market_data/fx/{date}.json"]
    assert all(f["unit"] == "D20" for f in completion["findings"])


def test_the_date_placeholder_resolves_to_the_manifests_own_trading_day(monkeypatch):
    """Never a day this check computes: a producer and a grader that disagree
    about what "today" was is the defect, not the evidence."""
    index = _index(monkeypatch)
    doc = _manifest(trading_day="2026-09-11", outputs=[
        {"key": "market_data/eod_closes/2026-09-11.json", "rows_out": 88},
        {"key": "market_data/eod_closes/latest.json", "rows_out": 88},
        {"key": "market_data/fx/2026-09-11.json", "rows_out": 12},
        {"key": "market_data/fx/latest.json", "rows_out": 12},
    ])
    result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": doc})
    assert result["completion"]["ok"] is True


def test_an_auto_skipped_unit_is_exempt_from_key_coverage_only(monkeypatch):
    """A same-date auto-skip DECLARES itself with an empty_fresh guard reading of
    not_applicable, and the auto-skip predicate re-verified the artifact on S3
    first — so the object is there, the unit simply did not rewrite it. The
    exemption reaches key coverage and nothing else."""
    index = _index(monkeypatch)
    guard = [{"guard": "empty_fresh", "verdict": "not_applicable",
              "detail": "D20 published nothing on this run (same-date auto-skip)"}]
    result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json":
                               _manifest(outputs=[], guards=guard)})
    assert result["completion"]["ok"] is True
    assert result["completion"]["units"][0]["auto_skipped"] is True

    # ...and it does NOT exempt the status.
    result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json":
                               _manifest(status="failed", outputs=[], guards=guard)})
    assert result["completion"]["failure_mode"] == "run_not_ok"


# ── failure mode 4 of 4: rows_out below the declared floor ────────────────────


def test_an_empty_but_fresh_key_fails_against_the_default_floor(monkeypatch):
    index = _index(monkeypatch)
    doc = _manifest(outputs=[
        {"key": "market_data/eod_closes/2026-09-14.json", "rows_out": 88},
        {"key": "market_data/eod_closes/latest.json", "rows_out": 0},
        {"key": "market_data/fx/2026-09-14.json", "rows_out": 12},
        {"key": "market_data/fx/latest.json", "rows_out": 12},
    ])
    result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": doc})
    completion = result["completion"]
    assert completion["failure_mode"] == "rows_below_floor"
    finding = completion["findings"][0]
    assert finding["unit"] == "D20"
    assert finding["key"] == "market_data/eod_closes/latest.json"
    # The finding names the floor AND the measured count.
    assert "rows_out=0" in finding["detail"] and "floor of 1" in finding["detail"]


def test_a_declared_floor_beats_the_default(monkeypatch):
    index = _index(monkeypatch)
    units = index._unit_descriptors()
    units["D20"] = dict(units["D20"], completeness={"rows_out_floor": 80})
    try:
        doc = _manifest(outputs=[
            {"key": "market_data/eod_closes/2026-09-14.json", "rows_out": 88},
            {"key": "market_data/eod_closes/latest.json", "rows_out": 79},
            {"key": "market_data/fx/2026-09-14.json", "rows_out": 88},
            {"key": "market_data/fx/latest.json", "rows_out": 88},
        ])
        result, _ = _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": doc})
        assert result["completion"]["failure_mode"] == "rows_below_floor"
        assert "floor of 80" in result["completion"]["findings"][0]["detail"]
    finally:
        index._predicate().reset_unit_cache()


def test_a_unit_whose_collector_reports_no_count_is_graded_on_presence(monkeypatch):
    """D31's descriptor declares rows_out_floor N/A-NOT-IMPL because its
    collector reports no row count at all (run_units.PhaseUnit.rows_key is None),
    which makes its manifest rows_out a constant 0. Grading a constant against a
    floor tests the constant, so the key is graded on presence — and the response
    says so rather than reporting a floor nobody measured."""
    index = _index(monkeypatch)
    doc = _manifest(unit_id="D31", outputs=[
        {"key": "features/2026-09-14/technical.parquet", "rows_out": 0},
        {"key": "features/metron_supplemental/2026-09-14.parquet", "rows_out": 0},
    ])
    result, _ = _check(index, {"data_collection/runs/D31/2026-09-14/01K5AA.json": doc},
                       units=("D31",))
    completion = result["completion"]
    assert completion["ok"] is True
    assert completion["units"][0]["floor"] == "not_applicable (N/A-NOT-IMPL)"
    # `features/metron_supplemental/` is a declared PREFIX and matches by prefix.
    assert completion["units"][0]["keys_checked"] == 2


# ── precedence, unverifiable writes, and the loud refusals ───────────────────


def test_the_named_mode_is_the_most_upstream_cause(monkeypatch):
    """A missing manifest explains a missing output, never the reverse — so with
    both present the execution's named error is manifest_missing."""
    index = _index(monkeypatch)
    short = _manifest(unit_id="D19", outputs=[])
    result, _ = _check(
        index,
        {"data_collection/runs/D19/2026-09-14/01K5AA.json": short},
        units=("D19", "D20"),
    )
    completion = result["completion"]
    assert {f["mode"] for f in completion["findings"]} == {"output_missing", "manifest_missing"}
    assert completion["failure_mode"] == "manifest_missing"
    assert completion["failure_mode"] == index._predicate().COMPLETION_FAILURE_MODES[0]


def test_a_prose_writes_entry_is_counted_not_silently_dropped(monkeypatch):
    """`arcticdb/universe (library)` is not an S3 key and no manifest can carry
    it. It is returned under `unverifiable` and logged, so "this unit's publish
    claim grades nothing" is visible rather than swallowed."""
    index = _index(monkeypatch)
    doc = _manifest(unit_id="D18", outputs=[], guards=[])
    result, _ = _check(index, {"data_collection/runs/D18/2026-09-14/01K5AA.json": doc},
                       units=("D18",))
    row = result["completion"]["units"][0]
    assert row["unverifiable"] == ["arcticdb/universe (library)"]
    assert row["keys_checked"] == 0
    assert result["completion"]["ok"] is True  # nothing gradable, nothing claimed


def test_a_unit_with_no_descriptor_raises_rather_than_passing(monkeypatch):
    index = _index(monkeypatch)
    with pytest.raises(ValueError, match="no descriptor"):
        _check(index, {}, units=("D99",))


def test_an_empty_unit_list_raises(monkeypatch):
    index = _index(monkeypatch)
    with pytest.raises(ValueError, match="mis-wired input"):
        index._completion_check({"units": [], "started_at": _STARTED}, s3_client=_FakeS3({}))


def test_an_unparseable_timestamp_raises_rather_than_guessing(monkeypatch):
    index = _index(monkeypatch)
    with pytest.raises(ValueError, match="started_at"):
        index._completion_check({"units": ["D20"], "started_at": "yesterday"},
                                s3_client=_FakeS3({}))
    bad = _manifest(finished="not-a-time")
    with pytest.raises(ValueError, match="finished"):
        _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": bad})


def test_an_output_without_rows_out_raises(monkeypatch):
    """`rows_out` is required by data_run_manifest.v1: "we did not count" and
    "we counted zero" are different facts with opposite consequences."""
    index = _index(monkeypatch)
    doc = _manifest(outputs=[{"key": "market_data/eod_closes/latest.json"}])
    with pytest.raises(ValueError, match="no rows_out"):
        _check(index, {f"{_PREFIX}/2026-09-14/01K5AA.json": doc})


def test_the_handler_routes_the_completion_check_action(monkeypatch):
    """The SF reaches this through handler(), not the private function."""
    index = _index(monkeypatch)
    monkeypatch.setattr(
        index, "_completion_check", lambda event, **kw: {"completion": {"seen": event["units"]}}
    )
    out = index.handler({"action": "completion-check", "units": ["D20"]}, None)
    assert out == {"completion": {"seen": ["D20"]}}


def test_the_execution_start_time_parses_as_the_freshness_bound(monkeypatch):
    index = _index(monkeypatch)
    now = _dt.datetime(2026, 9, 14, 20, 45, tzinfo=_dt.timezone.utc)
    assert index._parse_ts(_STARTED, where="x") == now


# ── alpha-engine-config-I10855: multi-key units record EVERY published key ────
#
# Withholding shape, against the REAL completion-check logic (never a stub):
# for every unit whose descriptor declares more than one published-key
# template, a manifest carrying every key a full run actually writes PASSES,
# and a manifest missing any ONE of them FAILS, naming the missing key. D20
# (above) and D31 (`test_a_unit_whose_collector_reports_no_count_is_graded_on_
# presence`) already cover two such units; this table covers the rest fixed by
# nousergon-data-PR<TBD> (weekly_collector.py `_phase_collect`/
# `_record_phase_lineage` `extra_outputs`).
#
# Some declared templates are WILDCARDS ("{ticker}.json", "*", trailing "/")
# that a single-path-segment companion file (manifest.json, consolidated.json,
# a manifest key) already satisfies — D15/D21/D26's ``outputs`` below are
# deliberately fewer than their unit's key-shaped template count for exactly
# that reason (see weekly_collector.py's `extra_outputs` comments for each).
_MULTI_KEY_UNIT_OUTPUTS: dict[str, list[dict]] = {
    # alpha-engine-config-I10898: D01 publishes EIGHT keys, not two — three
    # dual-path maps alongside the constituents pair. The completion check
    # grades every declared key, so a full run's manifest records all eight.
    "D01": [
        {"key": "market_data/weekly/2026-09-14/constituents.json", "rows_out": 903},
        {"key": "market_data/latest_weekly.json", "rows_out": 903},
        {"key": "data/sector_map.json", "rows_out": 11},
        {"key": "reference/price_cache/sector_map.json", "rows_out": 11},
        {"key": "data/sub_industry_map.json", "rows_out": 903},
        {"key": "reference/price_cache/sub_industry_map.json", "rows_out": 903},
        {"key": "data/sub_sector_etf_map.json", "rows_out": 11},
        {"key": "reference/price_cache/sub_sector_etf_map.json", "rows_out": 11},
    ],
    "D05": [
        {"key": "market_data/weekly/2026-09-14/macro.json", "rows_out": 40},
        {"key": "market_data/macro_history.parquet", "rows_out": 520},
        {"key": "market_data/macro_release_calendar.parquet", "rows_out": 12},
    ],
    "D07": [
        {"key": "market_data/universe_classification/2026-09-14.json", "rows_out": 900},
        {"key": "market_data/universe_classification/latest.json", "rows_out": 900},
    ],
    "D08": [
        {"key": "research.db", "rows_out": 1200},
        {"key": "backups/research_2026-09-14.db", "rows_out": 1200},
    ],
    "D12": [
        {"key": "features/2026-09-14/technical.parquet", "rows_out": 903},
        {"key": "features/2026-09-14/fundamental.parquet", "rows_out": 903},
        {"key": "features/2026-09-14/interaction.parquet", "rows_out": 903},
        {"key": "features/2026-09-14/macro.parquet", "rows_out": 1},
        {"key": "features/2026-09-14/alternative.parquet", "rows_out": 903},
    ],
    "D15": [
        {"key": "market_data/weekly/2026-09-14/alternative/manifest.json", "rows_out": 903},
        {"key": "market_data/weekly/2026-09-14/alternative/scope.json", "rows_out": 903},
    ],
    "D21": [
        {"key": "market_data/close_history/consolidated.json", "rows_out": 903},
        {"key": "market_data/fx_history/USD.json", "rows_out": 1},
    ],
    "D22": [
        {"key": "market_data/sectors/latest.json", "rows_out": 903},
        {"key": "market_data/earnings/latest.json", "rows_out": 200},
    ],
    "D26": [
        {"key": "market_data/technicals/rating_history/_manifest.json", "rows_out": 252},
    ],
}


@pytest.mark.parametrize("unit_id", sorted(_MULTI_KEY_UNIT_OUTPUTS))
def test_multi_key_unit_full_run_passes(monkeypatch, unit_id):
    index = _index(monkeypatch)
    outputs = _MULTI_KEY_UNIT_OUTPUTS[unit_id]
    doc = _manifest(unit_id=unit_id, outputs=outputs)
    prefix = f"data_collection/runs/{unit_id}"
    result, _ = _check(index, {f"{prefix}/2026-09-14/01K5AA.json": doc}, units=(unit_id,))
    completion = result["completion"]
    assert completion["ok"] is True, completion["findings"]
    assert completion["failure_mode"] == ""
    assert completion["units"][0]["unverifiable"] == []


@pytest.mark.parametrize("unit_id", sorted(_MULTI_KEY_UNIT_OUTPUTS))
def test_multi_key_unit_skipping_one_key_fails_naming_it(monkeypatch, unit_id):
    """Withholding shape: drop exactly one recorded key from an otherwise-full
    run and the check must fail — never silently pass on the remaining keys —
    and every finding must name a key this run genuinely did not publish."""
    outputs = _MULTI_KEY_UNIT_OUTPUTS[unit_id]
    prefix = f"data_collection/runs/{unit_id}"
    for i in range(len(outputs)):
        index = _index(monkeypatch)
        withheld = outputs[:i] + outputs[i + 1:]
        doc = _manifest(unit_id=unit_id, outputs=withheld)
        result, _ = _check(index, {f"{prefix}/2026-09-14/01K5AA.json": doc}, units=(unit_id,))
        completion = result["completion"]
        assert completion["ok"] is False, (
            f"{unit_id}: withholding {outputs[i]['key']!r} did not fail the check "
            f"(remaining keys satisfy every declared template — the withheld key "
            f"was never actually required)"
        )
        assert completion["failure_mode"] == "output_missing"
        assert all(f["mode"] == "output_missing" for f in completion["findings"])
        assert all(f["unit"] == unit_id for f in completion["findings"])


# ── alpha-engine-config-I10861: whole-mode units record REAL S3 keys, never a
# synthesized `arcticdb://{unit_id}` that no descriptor template could ever
# match. D17/D33/D34 (below) each publish at least one graded S3 key
# template; D18/D32 declare ONLY the (unverifiable) ArcticDB library write and
# are already covered by test_a_prose_writes_entry_is_counted_not_silently_
# dropped (D18) — the same shape applies to D32 and is not duplicated here.
#
# D33/D34 also declare `arcticdb/universe (library)`, which lands in
# `unverifiable` rather than `keys_checked` (same as D18/D32) — so these use
# their own pass/withhold pair instead of the `_MULTI_KEY_UNIT_OUTPUTS` table
# above, whose `test_multi_key_unit_full_run_passes` asserts `unverifiable ==
# []` for every entry (true for D01/D05/.../D26, none of which declare an
# ArcticDB write; false for D33/D34).
_WHOLE_MODE_UNIT_OUTPUTS: dict[str, list[dict]] = {
    # D17 morning-enrich: the polygon overwrite this run staged.
    "D17": [
        {"key": "staging/daily_closes/2026-09-14.parquet", "rows_out": 903},
    ],
    # D33 daily-heal: the heal-summary artifact (always written) plus one
    # `staging/daily_closes/*`-matching key for a day the universe-gap heal
    # actually staged this run.
    "D33": [
        {"key": "data/heal/daily/2026-09-14.json", "rows_out": 2},
        {"key": "staging/daily_closes/2026-09-11.parquet", "rows_out": 2},
    ],
    # D34 chronic-gap-heal: `reference/price_cache/*` — corrected from the
    # descriptor's stale `staging/daily_closes/*` (this mode never writes
    # that key; see registry.d/units/D34-chronic-gap-heal.yaml).
    "D34": [
        {"key": "reference/price_cache/PSTG.parquet", "rows_out": 12},
    ],
}


@pytest.mark.parametrize("unit_id", sorted(_WHOLE_MODE_UNIT_OUTPUTS))
def test_whole_mode_unit_full_run_passes(monkeypatch, unit_id):
    index = _index(monkeypatch)
    outputs = _WHOLE_MODE_UNIT_OUTPUTS[unit_id]
    doc = _manifest(unit_id=unit_id, outputs=outputs)
    prefix = f"data_collection/runs/{unit_id}"
    result, _ = _check(index, {f"{prefix}/2026-09-14/01K5AA.json": doc}, units=(unit_id,))
    completion = result["completion"]
    assert completion["ok"] is True, completion["findings"]
    assert completion["failure_mode"] == ""


@pytest.mark.parametrize("unit_id", sorted(_WHOLE_MODE_UNIT_OUTPUTS))
def test_whole_mode_unit_skipping_one_key_fails_naming_it(monkeypatch, unit_id):
    outputs = _WHOLE_MODE_UNIT_OUTPUTS[unit_id]
    prefix = f"data_collection/runs/{unit_id}"
    for i in range(len(outputs)):
        index = _index(monkeypatch)
        withheld = outputs[:i] + outputs[i + 1:]
        doc = _manifest(unit_id=unit_id, outputs=withheld)
        result, _ = _check(index, {f"{prefix}/2026-09-14/01K5AA.json": doc}, units=(unit_id,))
        completion = result["completion"]
        assert completion["ok"] is False, (
            f"{unit_id}: withholding {outputs[i]['key']!r} did not fail the check "
            f"(remaining keys satisfy every declared template — the withheld key "
            f"was never actually required)"
        )
        assert completion["failure_mode"] == "output_missing"
        assert all(f["mode"] == "output_missing" for f in completion["findings"])
        assert all(f["unit"] == unit_id for f in completion["findings"])


# ── alpha-engine-config-I10920: the standalone parity comparators ────────────


def _shadow_dispatch_declaration():
    """Load `shadow/dispatch.py` BY PATH, outside the `shadow` package.

    Importing `shadow` here would pull in `shadow.root` -> boto3 and
    `shadow.parity` -> arcticdb/pandas into a Lambda test process whose boto3
    is a stub. The declaration is deliberately import-free so it can be read
    like this; see its module docstring.
    """
    import importlib.util

    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "..", "..", "..", "shadow", "dispatch.py")
    spec = importlib.util.spec_from_file_location("_shadow_dispatch_decl", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _shadow_module_stems():
    here = os.path.dirname(os.path.abspath(__file__))
    shadow_dir = os.path.join(here, "..", "..", "..", "shadow")
    return sorted(
        name[: -len(".py")]
        for name in os.listdir(shadow_dir)
        if name.endswith(".py")
    )


def test_every_shadow_module_declares_whether_it_is_dispatchable():
    """THE CLASS FIX (alpha-engine-config-I10920).

    `shadow/arctic_parity.py` merged with no `_WORKLOADS` entry and therefore
    could not run at all — ArcticDB is unreachable from the laptop
    (I9771) and only this dispatcher puts code in-region. Nothing was red.
    A module under `shadow/` must now answer the question one way or the
    other: it carries a workload, or it states why it needs none.
    """
    decl = _shadow_dispatch_declaration()
    declared = set(decl.DISPATCHABLE_MODULES) | set(decl.NOT_DISPATCHABLE_MODULES)
    overlap = set(decl.DISPATCHABLE_MODULES) & set(decl.NOT_DISPATCHABLE_MODULES)
    assert not overlap, f"declared both dispatchable and not: {sorted(overlap)}"

    on_disk = set(_shadow_module_stems())
    assert not (on_disk - declared), (
        "undeclared shadow module(s) — add a workload in the dispatcher and an "
        "entry in shadow/dispatch.py::DISPATCHABLE_MODULES, or say in "
        "NOT_DISPATCHABLE_MODULES why it needs none: "
        f"{sorted(on_disk - declared)}"
    )
    assert not (declared - on_disk), (
        f"shadow/dispatch.py declares module(s) that do not exist: {sorted(declared - on_disk)}"
    )
    # A reason, never a blank — an empty string is how this map would quietly
    # become a skip-list.
    for name, reason in decl.NOT_DISPATCHABLE_MODULES.items():
        assert reason.strip(), name


def test_every_dispatchable_shadow_module_resolves_to_a_real_workload(monkeypatch):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    decl = _shadow_dispatch_declaration()
    for module, (workload, subcommand) in decl.DISPATCHABLE_MODULES.items():
        assert workload in index._WORKLOADS, f"{module}: no _WORKLOADS[{workload!r}]"
        if workload in decl.DATE_FREE_WORKLOADS:
            assert workload not in index._WORKLOADS_REQUIRING_TRADING_DAY, (
                f"{module}: {workload!r} is declared date-free but still requires a trading day"
            )
            _resolved, cmd = index._resolve_workload({"workload": workload})
        else:
            assert workload in index._WORKLOADS_REQUIRING_TRADING_DAY, (
                f"{module}: {workload!r} must refuse a dispatch with no trading day"
            )
            _resolved, cmd = index._resolve_workload(
                {"workload": workload, "trading_day": "2026-09-14"}
            )
        assert subcommand in cmd, (
            f"{module}: {workload!r} does not invoke `python -m {subcommand}`"
        )


def test_date_free_workloads_are_dispatchable_and_carry_a_reason():
    """A date-free key is an exception to the trading-day rule above, so it
    must be one of the declared dispatchable workloads and say why."""
    decl = _shadow_dispatch_declaration()
    dispatchable = {workload for workload, _sub in decl.DISPATCHABLE_MODULES.values()}
    for workload, reason in decl.DATE_FREE_WORKLOADS.items():
        assert workload in dispatchable, workload
        assert reason.strip(), workload


def test_shadow_prune_is_report_only(monkeypatch):
    """alpha-engine-config-I11447. Deleting a shadow ArcticDB library cannot be
    undone, so the dispatcher's prune workload only reports: no `--apply`, no
    event field that could add one, and nothing else in the allowlist reaches
    `shadow prune --apply` either."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    resolved, cmd = index._resolve_workload({"workload": "shadow-prune"})
    assert resolved == "shadow-prune"
    assert cmd == "python -m shadow prune"
    # Extra event fields are ignored, never rendered into the command.
    _resolved, cmd2 = index._resolve_workload(
        {"workload": "shadow-prune", "apply": True, "trading_day": "2026-09-14"}
    )
    assert cmd2 == cmd
    # The delete is reachable through exactly two keys: the operator one, named
    # for it, and the daily post-grade prune Brian approved on 2026-09-24.
    deleting = {
        workload for workload, command in index._WORKLOADS.items()
        if "shadow prune" in command and "--apply" in command
    }
    assert deleting == {"shadow-prune-apply", "shadow-sameday"}
    _resolved, apply_cmd = index._resolve_workload({"workload": "shadow-prune-apply"})
    assert apply_cmd == "python -m shadow prune --apply"


def test_shadow_sameday_prunes_only_after_the_day_is_graded(monkeypatch):
    """alpha-engine-config-I11447. The daily prune runs after `shadow parity`
    and only when it graded the day: exit 0 (MET) or 1 (NOT MET). Exit 2 (the
    comparison failed) and a parity step that never ran delete nothing, and a
    prune failure is not hidden behind the daily NOT-MET exit."""
    import subprocess

    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    cmd = index._WORKLOADS["shadow-sameday"]
    assert cmd.index("python -m shadow prune --apply") > cmd.index("python -m shadow parity")
    start = cmd.index("PRUNE_RC=0;")
    tail = cmd[start:]
    script = tail.replace("python -m shadow prune --apply", 'echo PRUNED; (exit "$FAKE_PRUNE_RC")')

    def run(rc_all, parity_rc, prune_rc=0):
        body = f"RC_ALL={rc_all}; PARITY_RC={parity_rc}; FAKE_PRUNE_RC={prune_rc}; ( {script}"
        out = subprocess.run(["bash", "-c", body], capture_output=True, text=True)
        return out.returncode, "PRUNED" in out.stdout

    assert run(0, 0) == (0, True)
    assert run(0, 1) == (1, True)       # NOT MET is still a graded day
    assert run(0, 2) == (2, False)      # comparison failed: delete nothing
    assert run(0, 1, prune_rc=3) == (3, True)   # prune failure outranks NOT MET
    assert run(4, 0) == (4, True)       # a failed leg still exits as the leg did


@pytest.mark.parametrize("workload", ["shadow-parity", "arctic-parity"])
def test_standalone_comparator_is_in_the_allowlist(monkeypatch, workload):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    assert workload in index._WORKLOADS
    assert workload in index._WORKLOADS_REQUIRING_TRADING_DAY


@pytest.mark.parametrize(
    "workload,expected",
    [
        (
            "shadow-parity",
            "python -m shadow parity --trading-day 2026-09-14 "
            "--store s3://alpha-engine-research/data_collection",
        ),
        (
            "arctic-parity",
            "python -m shadow arctic-parity --trading-day 2026-09-14 "
            "--store s3://alpha-engine-research/data_collection",
        ),
    ],
)
def test_standalone_comparator_renders_the_requested_trading_day(monkeypatch, workload, expected):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    resolved, cmd = index._resolve_workload(
        {"workload": workload, "trading_day": "2026-09-14"}
    )
    assert resolved == workload
    assert cmd == expected
    assert "{trading_day}" not in cmd
    rendered = index._bootstrap_command(workload, cmd, "tok")
    assert f"\n{cmd}\nrc=$?" in rendered
    # No pipe any more: the renderer's run-log block `exec`s this shell's
    # stdout through tee, so the collector runs unpiped and `$?` IS its own
    # exit code (alpha-engine-config-I11353).
    assert "| tee -a" not in rendered.split("_run_log_shipper_loop")[-1]
    assert "rc=${PIPESTATUS[0]}" not in rendered


@pytest.mark.parametrize("workload", ["shadow-parity", "arctic-parity"])
def test_standalone_comparator_requires_trading_day(monkeypatch, workload):
    """`Closes-when` of alpha-engine-config-I10920: removing a comparator from
    `_WORKLOADS_REQUIRING_TRADING_DAY` fails here. Defaulting to "today" would
    silently compare the wrong day's shadow prefix."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    with pytest.raises(ValueError, match="requires event\\['trading_day'\\]"):
        index._resolve_workload({"workload": workload})


@pytest.mark.parametrize("workload", ["shadow-parity", "arctic-parity"])
@pytest.mark.parametrize(
    "bad_day",
    ["", "not-a-date", "2026/09/14", "2026-13-40", "2026-09-14; rm -rf /", "2026-09-14T00:00:00"],
)
def test_standalone_comparator_rejects_malformed_trading_day(monkeypatch, workload, bad_day):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    with pytest.raises(ValueError):
        index._resolve_workload({"workload": workload, "trading_day": bad_day})


@pytest.mark.parametrize("workload", ["shadow-parity", "arctic-parity"])
def test_standalone_comparator_writes_no_live_key(monkeypatch, workload):
    """Neither comparator may address a live `market_data/*` prefix: every byte
    either lands under `data_collection/` (the report and its run manifest) or
    is a read. The store argument is the ONLY write target in the command."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    _resolved, cmd = index._resolve_workload(
        {"workload": workload, "trading_day": "2026-09-14"}
    )
    assert "market_data" not in cmd
    assert cmd.count("s3://") == 1
    assert "--store s3://alpha-engine-research/data_collection" in cmd


# ── alpha-engine-config-I11353: the run log outlives the box ─────────────────
#
# The 2026-09-21 shadow-sameday box wrote 1,025,054 bytes to the SSM CloudWatch
# stream and the stream STOPPED there — three minutes into a 73-minute run,
# before all three of the leg failures it was being read for. The box's log then
# died with the instance, and every run manifest recorded
# `log_location: local:ip-172-31-33-124.ec2.internal:<pid>`, a host that no
# longer exists. These tests pin the four properties that end that class.


def _rendered(index, workload="shadow-sameday", instance_id="i-09e64cb257b556b82",
              trading_day="2026-09-21"):
    _w, cmd = index._resolve_workload({"workload": workload})
    return cmd, index._bootstrap_command(
        workload, cmd, "tok", instance_id=instance_id, trading_day=trading_day
    )


def test_run_log_key_is_workload_trading_day_and_instance(monkeypatch):
    """One key per box per trading day, addressable from the manifest alone."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    expected = (
        "s3://alpha-engine-research/data_collection/logs/"
        "shadow-sameday/2026-09-21/i-09e64cb257b556b82.log"
    )
    assert index._run_log_uri("shadow-sameday", "2026-09-21", "i-09e64cb257b556b82") == expected
    _cmd, rendered = _rendered(index)
    assert expected in rendered


def test_the_manifest_writer_is_told_the_same_key_the_shipper_writes(monkeypatch):
    """ONE literal, exported and shipped to. A manifest that names a key the
    shipper did not write is worse than `local:` — it reads as resolvable."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    uri = index._run_log_uri("shadow-sameday", "2026-09-21", "i-09e64cb257b556b82")
    _cmd, rendered = _rendered(index)
    assert f"{index.RUN_LOG_ENV}={uri}" in rendered
    # The writer's half of the same contract is asserted by
    # tests/test_run_log_shipping_i11353.py, which can import run_units —
    # this module stubs nousergon_lib, so run_units is unimportable here.
    assert index.RUN_LOG_ENV == "ALPHA_ENGINE_RUN_LOG_S3"


def test_every_workload_ships_its_log_on_exit_failure_and_sigterm(monkeypatch):
    """The trap covers all three paths, on EVERY workload — a clean finish, a
    non-zero exit, and the SIGTERM the spot hard-timeout unit sends."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    for workload, cmd in _every_resolved_workload(index):
        rendered = index._bootstrap_command(
            workload, cmd, "tok", instance_id="i-x", trading_day="2026-09-21"
        )
        # Fires on success AND failure: the ship is unconditional inside the
        # EXIT trap, and only the `fail` branch is conditional on rc.
        assert "trap 'rc=$?; _ship_if_available; [ \"$rc\" -eq 0 ] || fail" in rendered, workload
        # The renderer's own signal trap — a spot reclaim or the hard-timeout
        # unit's SIGTERM ships before the shell dies.
        assert "trap '_stop_run_log_shipper; _ship_run_log' TERM INT" in rendered, workload
        # And periodically, so a SIGKILL still leaves at most one interval.
        assert "_run_log_shipper_loop &" in rendered, workload
        # `fail` itself ships too when it runs before the renderer's `finish`
        # trap exists (after that, `finish` ships). alpha-engine-config-I11200:
        # it no longer powers the box off inline — see the outcome tests below.
        assert 'fail() { _DATA_SPOT_REASON="$1"; echo "[data-spot-prelude] FATAL: $1";' in rendered, workload
        assert "trap - EXIT; _ship_if_available;" in rendered, workload
        assert "set -uo pipefail" in rendered, workload


def test_the_whole_script_is_captured_not_only_the_collector(monkeypatch):
    """`exec > >(tee …)` before the workload, so provisioning, the DLP gate and
    the collector all land in one object. The old form tee'd the collector
    alone, so a failure in the venv build left nothing durable at all."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    _cmd, rendered = _rendered(index)
    exec_at = rendered.index("exec > >(tee -a /var/log/data-spot-shadow-sameday.log)")
    venv_at = rendered.index("python3.12 -m venv .venv")
    assert exec_at < venv_at


def test_a_spec_with_no_run_log_exports_nothing_and_keeps_local(monkeypatch):
    """`local:` stays the honest answer when no log is being shipped — the
    daily report renders that as a detection gap, never as fine."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    spec = index._bootstrap_spec("morning-enrich")
    assert spec.run_log is None
    assert index.RUN_LOG_ENV not in spec.exports


def test_the_dispatch_result_names_the_log(monkeypatch):
    """A failed execution's history must NAME the log, not leave a reader to
    reconstruct the key from an instance id and a guess at the partition."""
    index, ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-0abc")
    monkeypatch.setattr(index, "_wait_ssm_online", lambda iid: None)
    monkeypatch.setattr(index, "_run_log_trading_day", lambda declared=None: "2026-09-21")
    out = index.handler({"workload": "morning-enrich"}, None)["data_spot"]
    assert out["log_location"] == (
        "s3://alpha-engine-research/data_collection/logs/"
        "morning-enrich/2026-09-21/i-0abc.log"
    )
    assert out["log_location"] in ssm.sent[-1]["Parameters"]["commands"][0]


def test_the_trading_day_partition_prefers_the_declared_day(monkeypatch):
    """A templated workload files its log under the day it is replaying, not
    under the day the box happened to boot."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    assert index._run_log_trading_day("2026-09-14") == "2026-09-14"


# ── alpha-engine-config-I11200 deliverable 4: the outcome is the box's record ──
#
# Every data-spot command SSM still retained on 2026-09-25 whose workload
# FAILED read `Failed / Undeliverable`, ResponseCode -1, no output (e114f53e,
# 9c9cb8c1, 3a812b85, c76d243b) — because `fail()` ran `shutdown -h now`
# inline and the box powered off before the SSM agent reported. These tests
# EXECUTE the exit paths of the script the Lambda actually sends (its prelude
# and the renderer's `finish` trap, cut from `_bootstrap_command`'s output)
# against PATH shims, rather than asserting on its text alone.

_DEFAULT_RECORDER = "/usr/local/sbin/data-spot-record-outcome"


def _exit_path(index, tmp_path, scenario, *, with_finish=True):
    """Run the prelude (+ the `finish` trap) followed by ``scenario``.

    Returns (exit code, calls made to shutdown/systemd-run/aws, the outcome
    record the box uploaded or None)."""
    import json
    import re
    import subprocess

    rendered = index._bootstrap_command(
        "shadow-weekday", "true", "tok",
        instance_id="i-022fabadd97ccc71d", trading_day="2026-09-14",
    )
    recorder = tmp_path / "data-spot-record-outcome"
    rendered = rendered.replace(
        getattr(index, "OUTCOME_RECORDER", _DEFAULT_RECORDER), str(recorder)
    )
    prelude = rendered[: rendered.index("\nset -eo pipefail\n")]
    finish = re.search(r"^finish\(\) \{.*?^trap finish EXIT$", rendered, re.M | re.S)

    shims = tmp_path / "bin"
    shims.mkdir()
    calls, record = tmp_path / "calls", tmp_path / "record.json"
    for name, body in (
        ("shutdown", 'echo "shutdown $*" >> "$CALLS"'),
        ("systemd-run", 'echo "systemd-run $*" >> "$CALLS"'),
        ("aws", 'echo "aws $*" >> "$CALLS"; case "$*" in *"s3 cp - "*) cat > "$RECORD";; esac'),
    ):
        shim = shims / name
        shim.write_text(f"#!/bin/sh\n{body}\nexit 0\n")
        shim.chmod(0o755)

    # The renderer's run-log block defines these; the exit paths call them.
    ship = '_stop_run_log_shipper() { :; }\n_ship_run_log() { echo "ship" >> "$CALLS"; }\n'
    script = "\n".join(
        [prelude, ship, finish.group(0) if (with_finish and finish) else "", scenario]
    )
    env = {**os.environ, "PATH": f"{shims}:{os.environ['PATH']}",
           "CALLS": str(calls), "RECORD": str(record)}
    proc = subprocess.run(["bash", "-c", script], env=env, capture_output=True,
                          text=True, timeout=60)
    made = calls.read_text().splitlines() if calls.exists() else []
    doc = json.loads(record.read_text()) if record.exists() else None
    return proc.returncode, made, doc


def _powered_off_inline(calls):
    return [c for c in calls if c.startswith("shutdown ")]


def test_a_failed_workload_exits_with_its_code_before_the_box_powers_off(monkeypatch, tmp_path):
    """The c76d243b shape: the workload exits 1 and `fail` is called. The
    shell must exit 1 with the power-off DEFERRED, so the SSM agent reports
    `Failed` with the real exit code instead of `Undeliverable` / -1."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    rc, calls, doc = _exit_path(
        index, tmp_path, 'false || fail "workload shadow-weekday exited 1"'
    )
    assert rc == 1
    assert _powered_off_inline(calls) == [], calls
    assert any(c.startswith("systemd-run --on-active=60 ") for c in calls), calls
    assert doc is not None, "no outcome record was published"
    assert doc["status"] == "failed" and doc["rc"] == 1
    assert doc["reason"] == "workload shadow-weekday exited 1"
    assert doc["schema"] == "data_spot_outcome.v1"
    assert doc["trading_day"] == "2026-09-14"
    assert doc["instance_id"] == "i-022fabadd97ccc71d"
    assert doc["log_location"].endswith("/shadow-weekday/2026-09-14/i-022fabadd97ccc71d.log")


def test_a_successful_workload_records_ok_and_still_powers_off(monkeypatch, tmp_path):
    """Before I11200 the success path never shut down at all: a 5-minute
    morning-enrich box (i-0cc74e7235d4ff72d, 2026-09-24) was still refreshing
    its instance-role credentials 1h41m later, idling to the hard cap."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    rc, calls, doc = _exit_path(index, tmp_path, 'echo "[data-spot] workload shadow-weekday complete"')
    assert rc == 0
    assert _powered_off_inline(calls) == [], calls
    assert any(c.startswith("systemd-run --on-active=60 ") for c in calls), calls
    assert doc is not None and doc["status"] == "ok" and doc["rc"] == 0
    assert doc["reason"] is None


def test_a_failure_before_the_finish_trap_exists_is_recorded_and_deferred_too(monkeypatch, tmp_path):
    """A failure inside the renderer's own timer/run-log blocks reaches the
    prelude's `fail` with no `finish` defined yet. Same outcome, no race."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    rc, calls, doc = _exit_path(
        index, tmp_path, 'false || fail "hard-timeout arm failed"', with_finish=False
    )
    assert rc == 1
    assert _powered_off_inline(calls) == [], calls
    assert any(c.startswith("systemd-run --on-active=60 ") for c in calls), calls
    assert doc is not None and doc["status"] == "failed"
    assert doc["reason"] == "hard-timeout arm failed"


def test_a_reason_with_quotes_cannot_corrupt_the_record(monkeypatch, tmp_path):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    reason = """it said "no" and 'maybe' \\ then {"x": 1}"""
    _rc, _calls, doc = _exit_path(index, tmp_path, f"false || fail {__import__('shlex').quote(reason)}")
    assert doc is not None and doc["reason"] == reason


def test_the_hard_timeout_kill_records_killed(monkeypatch, tmp_path):
    """A timer kill never reaches `finish`; the renderer's kill recorder runs
    the same writer, so the record says `killed`, not nothing."""
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    rendered = index._bootstrap_command(
        "shadow-weekday", "true", "tok", instance_id="i-x", trading_day="2026-09-14"
    )
    kill_block = rendered[rendered.index("<<'RECORDKILL'"): rendered.index("\nRECORDKILL\n")]
    assert f'{index.OUTCOME_RECORDER} killed -1 "timer: ${{KILL_REASON:-unknown}}"' in kill_block
    _rc, _calls, doc = _exit_path(
        index, tmp_path,
        f'KILL_REASON=budget_exhausted sh {tmp_path}/data-spot-record-outcome killed -1 '
        '"timer: budget_exhausted"',
    )
    assert doc is not None and doc["status"] == "killed" and doc["rc"] == -1


def test_the_dispatch_result_names_the_outcome_record_beside_the_log(monkeypatch):
    """Where to read the verdict, handed to every caller at launch."""
    index, ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-0abc")
    monkeypatch.setattr(index, "_wait_ssm_online", lambda iid: None)
    monkeypatch.setattr(index, "_run_log_trading_day", lambda declared=None: "2026-09-21")
    out = index.handler({"workload": "morning-enrich"}, None)["data_spot"]
    assert out["outcome_location"] == (
        "s3://alpha-engine-research/data_collection/logs/"
        "morning-enrich/2026-09-21/i-0abc.outcome.json"
    )
    assert out["outcome_location"] in ssm.sent[-1]["Parameters"]["commands"][0]


def test_every_workload_renders_the_deferred_shutdown_and_the_recorder(monkeypatch):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    for workload, cmd in _every_resolved_workload(index):
        rendered = index._bootstrap_command(
            workload, cmd, "tok", instance_id="i-x", trading_day="2026-09-21"
        )
        assert "trap finish EXIT" in rendered, workload
        assert f"--on-active={index.SHUTDOWN_DELAY_SECONDS} " in rendered, workload
        assert f"cat > {index.OUTCOME_RECORDER} <<'OUTCOME'" in rendered, workload
        # The recorder is installed before anything that can call it.
        assert rendered.index(f"cat > {index.OUTCOME_RECORDER}") < rendered.index("fail() {"), workload
        # No inline power-off anywhere this Lambda writes (the renderer keeps
        # one only as the fallback when the delayed timer cannot be armed).
        prelude = rendered[: rendered.index("\nset -eo pipefail\n")]
        assert "; shutdown -h now; exit 1" not in prelude, workload


# ── Isolation: the stubs above must not outlive the test that installed them ──
# (alpha-engine-config-I11229). This file used to assign stubs straight into
# sys.modules, so `pytest <this file> tests/` in one process failed every
# collector test that imported after it: `cannot import name 'run_manifest'
# from 'nousergon_lib' (unknown location)`.

_STUBBED_NAMES = (
    "nousergon_lib",
    "nousergon_lib.ec2_spot",
    "krepis",
    "krepis.alerts",
    "krepis.spot_bootstrap",
    "boto3",
    "index",
)
_MISSING = object()


def test_the_stubs_are_restored_when_the_test_ends():
    before = {name: sys.modules.get(name, _MISSING) for name in _STUBBED_NAMES}
    with pytest.MonkeyPatch.context() as mp:
        index, _ssm, _ec2 = _load(mp, launch_impl=lambda t, s, **kw: "i-x")
        # Inside the test the handler really does see the stubs...
        assert sys.modules["nousergon_lib"].__spec__ is None
        assert index.ec2_spot is sys.modules["nousergon_lib.ec2_spot"]
        # ...and it is THIS Lambda's handler, whatever else is on sys.path.
        assert os.path.samefile(
            index.__file__, os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.py")
        )
    # ...and once it ends, every entry is exactly what it was before.
    after = {name: sys.modules.get(name, _MISSING) for name in _STUBBED_NAMES}
    leaked = [name for name in _STUBBED_NAMES if after[name] is not before[name]]
    assert leaked == [], f"sys.modules entries leaked past teardown: {leaked}"


def test_the_real_nousergon_lib_still_resolves_after_this_file():
    """Runs LAST in this file, i.e. after every stubbing test above: the name a
    collector imports must resolve to the installed package, not a stub."""
    import importlib
    import importlib.util

    lib = sys.modules.get("nousergon_lib")
    assert lib is None or lib.__spec__ is not None, (
        "a stub `nousergon_lib` (no __spec__) is still in sys.modules"
    )
    if importlib.util.find_spec("nousergon_lib") is None:
        # deploy.sh's minimal preflight install omits the lib; the restoration
        # itself is asserted by the test above regardless.
        return
    run_manifest = importlib.import_module("nousergon_lib.run_manifest")
    assert run_manifest.__file__, "nousergon_lib.run_manifest has no file: not the real module"
