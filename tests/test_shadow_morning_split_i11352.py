"""The morning legs run on v1's morning cadence, not twelve hours early.

`alpha-engine-config-I11352`. The first scheduled `shadow-sameday` (trading day
2026-09-21, launched 22:30:34Z) ran v1's two MORNING legs at 18:30 ET on day D.
Both failed:

    morning-enrich         exit 1  (D17, morning_daily_closes status='error')
    morning-arctic-append  exit 1  (D18, NoSuchKey, 0.05 s)

Neither is a collector defect. `collectors/daily_closes.py::collect` in window
mode is TARGET-driven — the aggregate is `error` when `per_date[target_date]`
is not ok — and the phase manifest shows 2026-09-08…09-18 all ok at 928
tickers with the run failing on target 2026-09-21 itself. Polygon's
grouped-daily bar for session D is final the NEXT morning, which is why v1
schedules these two legs at `cron(30 7 ? * MON-FRI *)` for the PREVIOUS
session. `morning-arctic-append` then died a twentieth of a second later
reading the parquet the failed leg never wrote.

This module asserts the split at the level the dispatcher owns: which legs each
workload runs, which legs-group each names to the comparator, and that the
morning legs appear in exactly one of them.

The DATE LOGIC of the two guards is pinned by `test_shadow_sameday_guard.py`;
the CloudFormation schedules by `test_data_collection_stack.py`.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
INDEX = REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"


@pytest.fixture(scope="module")
def workloads() -> dict[str, str]:
    """`index._WORKLOADS`, loaded without importing the Lambda's boto3 world.

    The dispatcher module is not importable from `tests/` (it shadows the
    repo-root `index` name and pulls boto3/krepis at import), so the module is
    loaded under a private name with the two heavy imports stubbed — the same
    shape `test_handler.py` uses, minus the SSM/EC2 fakes it does not need.
    """
    stubs = {}
    for name in ("boto3", "krepis", "krepis.alerts", "krepis.spot_bootstrap",
                 "nousergon_lib", "nousergon_lib.ec2_spot"):
        if name not in sys.modules:
            stubs[name] = sys.modules[name] = types.ModuleType(name)
    boto3 = sys.modules["boto3"]
    if not hasattr(boto3, "client"):
        boto3.client = lambda *a, **k: None  # type: ignore[attr-defined]
    alerts = sys.modules["krepis.alerts"]
    if not hasattr(alerts, "publish"):
        alerts.publish = lambda *a, **k: None  # type: ignore[attr-defined]
    sys.modules["krepis"].alerts = alerts  # type: ignore[attr-defined]
    boot = sys.modules["krepis.spot_bootstrap"]
    for attr, value in (
        ("SpotBootstrapSpec", object),
        ("RunLog", object),
        ("render_bootstrap", lambda spec: ""),
    ):
        if not hasattr(boot, attr):
            setattr(boot, attr, value)
    spot = sys.modules["nousergon_lib.ec2_spot"]
    for attr in ("SpotCapacityExhausted", "SpotQuotaExceededError"):
        if not hasattr(spot, attr):
            setattr(spot, attr, type(attr, (Exception,), {}))
    if not hasattr(spot, "launch"):
        spot.launch = lambda *a, **k: ""  # type: ignore[attr-defined]
    sys.modules["nousergon_lib"].ec2_spot = spot  # type: ignore[attr-defined]

    spec = importlib.util.spec_from_file_location("_i11352_dispatcher", INDEX)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    try:
        yield module._WORKLOADS
    finally:
        for name in stubs:
            sys.modules.pop(name, None)


MORNING_LEGS = (
    "--morning-enrich --skip-chronic-heal --skip-arctic-append",
    "--morning-arctic-append",
)
POST_MARKET_LEGS = (
    "--daily --skip-arctic-append",
    "--daily-arctic-append",
)


def test_shadow_morning_exists_and_runs_the_two_morning_legs(workloads):
    cmd = workloads["shadow-morning"]
    for leg in MORNING_LEGS:
        assert f"{leg} --date $TD" in cmd, leg
    positions = [cmd.index(f"{leg} --date $TD") for leg in MORNING_LEGS]
    assert positions == sorted(positions), "enrich writes the parquet the append reads"


def test_shadow_morning_runs_NO_post_market_leg(workloads):
    """The split is a split: a leg in both workloads would double-write the
    shadow prefix for the same trading day from two boxes."""
    cmd = workloads["shadow-morning"]
    for leg in POST_MARKET_LEGS:
        assert leg not in cmd, leg


def test_shadow_sameday_no_longer_runs_the_morning_legs(workloads):
    """The regression this issue exists for. Both failed on the first
    scheduled run because Polygon's bar for session D is a D+1 fact."""
    cmd = workloads["shadow-sameday"]
    assert "--morning-enrich" not in cmd
    assert "--morning-arctic-append" not in cmd
    for leg in POST_MARKET_LEGS:
        assert f"{leg} --date $TD" in cmd, leg


def test_the_morning_legs_live_in_exactly_one_same_day_axis_workload(workloads):
    """`shadow-weekday` keeps all four — it REPLAYS a completed day, where the
    D+1 bar is already final, so the vendor-state argument does not apply to
    it. The two box-resolved-day workloads partition the legs between them."""
    box_resolved = ("shadow-sameday", "shadow-morning")
    carrying = [w for w in box_resolved if "--morning-enrich" in workloads[w]]
    assert carrying == ["shadow-morning"]
    assert "--morning-enrich" in workloads["shadow-weekday"]


@pytest.mark.parametrize(
    "workload,group",
    [("shadow-sameday", "sameday"), ("shadow-morning", "morning")],
)
def test_each_workload_declares_its_legs_group_to_the_comparator(workloads, group, workload):
    """Without `--legs-group` the second dispatch's report would REPLACE the
    first's legs rather than merge with them, and the missing group would read
    as silence instead of as `legs_known: false`."""
    cmd = workloads[workload]
    assert f"--legs-group {group}" in cmd
    assert cmd.count("--legs-group ") == 1


def test_the_comparator_still_always_runs_on_both(workloads):
    """alpha-engine-config-I11200's property survives the split: `set +e`, each
    leg records its own exit code, and the comparator runs regardless — a
    partial run NAMES its gaps instead of producing no report at all."""
    for workload, legs in (("shadow-sameday", 2), ("shadow-morning", 2)):
        cmd = workloads[workload]
        assert "set +e" in cmd
        assert cmd.count(">> $LEGS") == legs
        assert cmd.count("RC_ALL=$RC") == legs
        assert "[ $RC_ALL -ne 0 ] && exit $RC_ALL; " in cmd
    # `shadow-sameday` ends on the parity code unless its post-grade prune
    # failed (alpha-engine-config-I11447); pinned by the dispatcher's
    # `test_shadow_sameday_prunes_only_after_the_day_is_graded`.
    assert workloads["shadow-sameday"].endswith(
        "[ $RC_ALL -ne 0 ] && exit $RC_ALL; [ $PRUNE_RC -ne 0 ] && exit $PRUNE_RC; exit $PARITY_RC )"
    )
    # `shadow-morning` ends on the ArcticDB comparator's code, which grades the
    # whole rewritten report (alpha-engine-config-I11546); the executed shell
    # is pinned by `test_shadow_morning_arctic_parity_i11546.py`.
    assert workloads["shadow-morning"].endswith(
        "[ $RC_ALL -ne 0 ] && exit $RC_ALL; [ $PARITY_RC -eq 2 ] && exit $PARITY_RC; exit $ARCTIC_RC )"
    )


def test_the_shadow_morning_guard_asks_the_calendar_not_the_weekday(workloads):
    """`nousergon_lib.trading_calendar`, not `date +%u`. A weekday check runs
    on Thanksgiving, when v1's own morning schedule
    (`require_trading_day: true`) does not — so there would be no v1 copy of
    the keys to grade against."""
    cmd = workloads["shadow-morning"]
    assert "from nousergon_lib.trading_calendar import is_trading_day" in cmd
    assert 'if [ "$IS_SESSION" != "1" ]' in cmd
    # And the second question, which the first does not answer.
    assert 'if [ "$TD" = "$TODAY" ]' in cmd


def test_the_shadow_morning_chronic_heal_stays_skipped(workloads):
    """A shadow must never heal live data."""
    assert "--skip-chronic-heal" in workloads["shadow-morning"]


def test_shadow_morning_needs_no_trading_day_from_the_event(workloads):
    """Its day is resolved on the box, like `shadow-sameday`'s: an EventBridge
    Scheduler rule carries a STATIC input and cannot compute a date."""
    assert "{trading_day}" not in workloads["shadow-morning"]
