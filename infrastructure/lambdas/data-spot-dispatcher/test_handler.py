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


# ── alpha-engine-config-I10778, plan P-11: the pre-cutover shadow run ────────


def test_shadow_weekday_is_in_the_allowlist(monkeypatch):
    index, _ssm, _ec2 = _load(monkeypatch, launch_impl=lambda t, s, **kw: "i-x")
    assert "shadow-weekday" in index._WORKLOADS
    assert "shadow-weekday" in index._WORKLOADS_REQUIRING_TRADING_DAY


def test_shadow_weekday_renders_the_four_shadow_runs_then_parity_in_order(monkeypatch):
    """Same subshell + && pipeline-element shape as weekly-phase-one: one exit
    code for all five legs, and each of the four boundary invocations gets the
    SAME weekly_collector.py flags the scheduled workloads above it use, plus
    `--date` pinning it to the requested historical trading day."""
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
        "python -m shadow parity --trading-day 2026-09-14 "
        "--store s3://alpha-engine-research/data_collection",
    ]
    positions = [cmd.index(leg) for leg in legs]
    assert positions == sorted(positions), "legs must run in the declared order"
    # Every shadow-run leg AND the parity comparison target the requested
    # trading day, not "today" (4 `shadow run` legs + 1 `shadow parity`).
    assert cmd.count("--trading-day 2026-09-14") == 5
    assert cmd.count("&&") == 4  # five legs, four joins — any leg's failure halts the chain

    rendered = index._bootstrap_command("shadow-weekday", cmd, "tok")
    assert f"{cmd} 2>&1 | tee -a" in rendered
    assert "rc=${PIPESTATUS[0]}" in rendered


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
        index._UNITS_CACHE = None


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
    assert completion["failure_mode"] == index.COMPLETION_FAILURE_MODES[0]


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
    "D01": [
        {"key": "market_data/weekly/2026-09-14/constituents.json", "rows_out": 903},
        {"key": "market_data/latest_weekly.json", "rows_out": 903},
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
