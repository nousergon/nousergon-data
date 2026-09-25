"""sf_preflight_on_spot.py — the observe-mode on-spot preflight pass
(alpha-engine-config-I11312).

Pins the three properties the state machine relies on: it runs exactly the
checks the Lambda profile cannot reach, it escalates a REQUIRED check's skip
(tool_contracts included, alpha-engine-config-I11568) as the gap it exists to
close while listing optional skips apart, and it fails OPEN — no
crash, timeout or AWS write failure can turn into a non-zero exit other than
the one deliberate observed-FAIL code.
"""

from __future__ import annotations

import json

import pytest

import sf_preflight as sp
import sf_preflight_on_spot as spot

# The seven checks alpha-engine-config-I11312 names, by capability group.
_GROUP = {
    "arctic_connectivity", "constituents_fetch", "universe_drift",
    "universe_sample_freshness", "polygon_grouped_coverage",
    "predicted_missing_from_closes", "backfill_source_freshness",
}
_CHECKOUT = {"price_cards_cover_all_models", "recursion_budget_for_response_format", "tool_contracts"}


def _names(fns):
    return {fn.__name__.replace("check_", "") for fn in fns}


def test_spot_checks_are_exactly_the_ones_the_lambda_cannot_reach():
    assert _names(spot._spot_checks()) == _GROUP | _CHECKOUT
    lambda_reachable = {
        fn for fn in sp.CHECKS
        if sp.CHECK_CAPABILITIES[fn.__name__] <= sp.LAMBDA_CAPABILITIES
    }
    assert not lambda_reachable & set(spot._spot_checks()), (
        "an AWS-only check would run under the box's instance role, which lacks "
        "the IAM/Lambda read grants those checks need — it belongs to the Lambda"
    )


def test_spot_checks_keep_checks_order():
    """Order matters: arctic_connectivity populates ctx for the checks after it."""
    order = [fn for fn in sp.CHECKS if fn in spot._spot_checks()]
    assert spot._spot_checks() == order


def test_in_scope_group_is_derived_from_the_capability_table():
    derived = {
        fn.__name__.replace("check_", "") for fn in sp.CHECKS
        if sp.CHECK_CAPABILITIES[fn.__name__] & spot.IN_SCOPE_CAPABILITIES
    }
    assert derived == _GROUP | _CHECKOUT


def test_checkout_is_in_scope_so_required_tool_contracts_runs_somewhere():
    """alpha-engine-config-I11568: tool_contracts is REQUIRED and needs
    CAP_CHECKOUT; with checkout out of scope here as well as absent from the
    Lambda, a required check ran in NO environment."""
    assert sp.CAP_CHECKOUT in spot.IN_SCOPE_CAPABILITIES
    for fn in sp.CHECKS:
        if sp.CHECK_REQUIRED.get(fn.__name__, True) and not (
            sp.CHECK_CAPABILITIES[fn.__name__] <= sp.LAMBDA_CAPABILITIES
        ):
            assert sp.CHECK_CAPABILITIES[fn.__name__] - {sp.CAP_AWS} <= (
                spot.IN_SCOPE_CAPABILITIES
            ), f"{fn.__name__} is required and reachable in neither profile"


def _run_with(monkeypatch, caps, outcome="ok"):
    """Run the real run_preflight over _spot_checks with every check stubbed."""
    stubs = []
    for fn in spot._spot_checks():
        name = fn.__name__

        def _stub(ctx, _n=name):
            return sp.CheckResult(name=_n.replace("check_", ""), status=outcome, message="stub")

        _stub.__name__ = name
        stubs.append(_stub)
    monkeypatch.setattr(spot, "_spot_checks", lambda: stubs)
    monkeypatch.setattr(spot, "detect_capabilities", lambda: frozenset(caps))
    monkeypatch.setattr(sp, "_previous_trading_day_str", lambda: "2026-09-24")
    return spot.observe("test-bucket", "2026-09-25")


_BOX = {sp.CAP_AWS, sp.CAP_ARCTIC, sp.CAP_REPO_MODULES, sp.CAP_POLYGON, sp.CAP_CHECKOUT}


def test_full_box_runs_every_spot_check_including_the_checkout_group(monkeypatch):
    rec = _run_with(monkeypatch, _BOX)
    assert rec["verdict"] == "OK"
    assert rec["group_required_skip_count"] == 0
    assert rec["required_skip_names"] == []
    assert rec["expected_skip_names"] == []
    assert rec["ran_count"] == len(_GROUP | _CHECKOUT)


def test_a_box_without_its_checkouts_is_a_blind_spot_on_tool_contracts(monkeypatch):
    """alpha-engine-config-I11568: a lost sibling checkout must not read as
    an expected skip — tool_contracts is REQUIRED, so it escalates. The two
    OPTIONAL checkout checks are listed apart and never escalate."""
    rec = _run_with(monkeypatch, _BOX - {sp.CAP_CHECKOUT})
    assert rec["verdict"] == "BLIND_SPOT"
    assert rec["group_required_skip_names"] == ["tool_contracts"]
    assert set(rec["expected_skip_names"]) == _CHECKOUT - {"tool_contracts"}
    assert rec["fail_count"] == 0


def test_a_lost_capability_is_a_named_blind_spot_not_a_fail(monkeypatch):
    rec = _run_with(monkeypatch, _BOX - {sp.CAP_POLYGON})
    assert rec["verdict"] == "BLIND_SPOT"
    assert set(rec["group_required_skip_names"]) == {
        "polygon_grouped_coverage", "predicted_missing_from_closes",
    }
    assert rec["fail_count"] == 0


def test_a_failing_check_is_a_fail_verdict(monkeypatch):
    rec = _run_with(monkeypatch, _BOX, outcome="fail")
    assert rec["verdict"] == "FAIL"
    assert set(rec["fail_names"]) == _GROUP | _CHECKOUT


def test_a_crash_is_recorded_not_raised(monkeypatch):
    def _boom():
        raise RuntimeError("probe exploded")

    monkeypatch.setattr(spot, "detect_capabilities", _boom)
    rec = spot.observe("test-bucket", "2026-09-25")
    assert rec["verdict"] == "ERROR"
    assert "probe exploded" in rec["error"]
    assert rec["timed_out"] is False


def test_the_budget_escapes_the_per_check_except(monkeypatch):
    """run_preflight catches Exception per check; the budget must not be
    swallowed into one check's fail and let the run carry on past it."""
    assert not issubclass(spot._BudgetExceeded, Exception)

    def _hang(ctx):
        raise spot._BudgetExceeded("simulated budget")

    _hang.__name__ = "check_arctic_connectivity"
    monkeypatch.setattr(spot, "_spot_checks", lambda: [_hang])
    monkeypatch.setattr(spot, "detect_capabilities", lambda: frozenset(_BOX))
    monkeypatch.setattr(sp, "_previous_trading_day_str", lambda: "2026-09-24")
    rec = spot.observe("test-bucket", "2026-09-25")
    assert rec["verdict"] == "ERROR"
    assert rec["timed_out"] is True


@pytest.mark.parametrize(
    "verdict,expected_rc",
    [("OK", 0), ("BLIND_SPOT", 0), ("ERROR", 0), ("FAIL", spot.OBSERVED_FAIL_EXIT_CODE)],
)
def test_exit_code_is_zero_unless_an_observed_fail(monkeypatch, capsys, verdict, expected_rc):
    monkeypatch.setattr(spot, "observe", lambda b, d: {"mode": "observe", "verdict": verdict, "fail_names": []})
    monkeypatch.setattr(spot, "_write_artifact", lambda b, k, r: None)
    monkeypatch.setattr(spot, "_emit_metrics", lambda r: None)
    rc = spot.main(["--run-date", "2026-09-25", "--execution-name", "exec-1"])
    assert rc == expected_rc
    line = json.loads(capsys.readouterr().out.strip())
    assert line["verdict"] == verdict
    assert line["artifact"] == (
        "s3://alpha-engine-research/health/weekly_preflight_on_spot/2026-09-25/exec-1.json"
    )


def test_aws_write_failures_are_recorded_not_raised(monkeypatch, capsys):
    monkeypatch.setattr(spot, "observe", lambda b, d: {"mode": "observe", "verdict": "OK"})

    class _Broken:
        def __getattr__(self, name):
            raise RuntimeError("no credentials")

    import boto3

    monkeypatch.setattr(boto3, "client", lambda *a, **k: _Broken())
    assert spot.main(["--run-date", "2026-09-25", "--execution-name", "exec-1"]) == 0
    line = json.loads(capsys.readouterr().out.strip())
    assert "no credentials" in line["artifact_error"]
    assert "no credentials" in line["metric_error"]


def test_stdout_is_exactly_one_json_line(monkeypatch, capsys):
    monkeypatch.setattr(spot, "observe", lambda b, d: {
        "mode": "observe", "verdict": "OK", "results": [{"message": "x" * 50_000}],
    })
    monkeypatch.setattr(spot, "_write_artifact", lambda b, k, r: None)
    monkeypatch.setattr(spot, "_emit_metrics", lambda r: None)
    spot.main(["--run-date", "2026-09-25", "--execution-name", "e"])
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert len(out) < 24_000, "SSM truncates StandardOutputContent at 24,000 characters"
    json.loads(out)


@pytest.mark.parametrize("key", ["pk-test", None])
def test_detect_capabilities_measures_polygon(monkeypatch, key):
    import nousergon_lib.secrets as secrets

    monkeypatch.setattr(secrets, "get_secret", lambda name, required=False: key)
    caps = spot.detect_capabilities()
    assert (sp.CAP_POLYGON in caps) is bool(key)
    # This repo's own modules are importable from its checkout.
    assert sp.CAP_REPO_MODULES in caps


def _siblings_present(monkeypatch, present):
    from pathlib import Path

    monkeypatch.setattr(
        sp, "_sibling_repo",
        lambda name: Path("/home/ec2-user") / name if name in present else None,
    )


def test_detect_claims_checkout_when_every_sibling_resolves(monkeypatch):
    _siblings_present(monkeypatch, set(sp.CHECKOUT_SIBLINGS))
    assert sp.CAP_CHECKOUT in spot.detect_capabilities()


@pytest.mark.parametrize("lost", sp.CHECKOUT_SIBLINGS)
def test_detect_withholds_checkout_when_any_sibling_is_absent(monkeypatch, lost):
    """All or nothing: a partial set is what turns tool_contracts' "not
    checked out as sibling" into a false fail on the box's own layout."""
    _siblings_present(monkeypatch, set(sp.CHECKOUT_SIBLINGS) - {lost})
    assert sp.CAP_CHECKOUT not in spot.detect_capabilities()


def test_a_raising_checkout_probe_withholds_checkout(monkeypatch):
    def _raise(name):
        raise OSError("stat failed")

    monkeypatch.setattr(sp, "_sibling_repo", _raise)
    caps = spot.detect_capabilities()
    assert sp.CAP_CHECKOUT not in caps
    assert sp.CAP_AWS in caps


def test_a_raising_probe_withholds_its_capability(monkeypatch):
    import nousergon_lib.secrets as secrets

    def _raise(name, required=False):
        raise RuntimeError("ssm unreachable")

    monkeypatch.setattr(secrets, "get_secret", _raise)
    caps = spot.detect_capabilities()
    assert sp.CAP_POLYGON not in caps
    assert sp.CAP_AWS in caps


# ── alpha-engine-config-I11566: one cause is one failure ─────────────────────


def _classify(statuses):
    return spot.classify([sp.CheckResult(name=n, status=s, message="") for n, s in statuses])


def test_a_blocked_dependent_is_not_a_second_fail():
    """rehearsal-2026-09-24-1 shape: constituents_fetch failed and its four
    dependents are BLOCKED — fail_count 1, not 5."""
    rec = _classify([
        ("arctic_connectivity", "ok"),
        ("constituents_fetch", "fail"),
        ("universe_drift", "blocked"),
        ("universe_sample_freshness", "blocked"),
        ("polygon_grouped_coverage", "blocked"),
        ("predicted_missing_from_closes", "blocked"),
        ("backfill_source_freshness", "ok"),
    ])
    assert rec["verdict"] == "FAIL"
    assert rec["fail_count"] == 1
    assert rec["fail_names"] == ["constituents_fetch"]
    assert rec["blocked_count"] == 4
    assert rec["ran_count"] == 3


def test_blocked_without_a_fail_is_a_blind_spot_never_ok():
    rec = _classify([("arctic_connectivity", "ok"), ("universe_drift", "blocked")])
    assert rec["verdict"] == "BLIND_SPOT"
    assert rec["blocked_names"] == ["universe_drift"]


# ── alpha-engine-config-I11567: explicit region ──────────────────────────────


def test_region_matches_sf_preflight():
    assert spot.REGION == sp._REGION


def test_aws_clients_work_with_no_region_in_the_environment(monkeypatch, tmp_path, capsys):
    """The SSM shell exports no AWS_DEFAULT_REGION. Real boto3 client
    construction (which raises NoRegionError for cloudwatch without a region)
    must succeed; only the network call is stubbed."""
    for var in ("AWS_DEFAULT_REGION", "AWS_REGION", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    empty = tmp_path / "aws-config"
    empty.write_text("")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))

    import boto3

    real_client = boto3.client
    built = []

    class _Sink:
        def put_metric_data(self, **_k):
            return {}

        def put_object(self, **_k):
            return {}

    def _client(service, *a, **k):
        real = real_client(service, *a, **k)  # raises NoRegionError if unset
        built.append((service, real.meta.region_name))
        return _Sink()

    # A fresh session so the default one cannot carry a cached region.
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
    monkeypatch.setattr(boto3, "client", _client)
    monkeypatch.setattr(spot, "observe", lambda b, d: {"mode": "observe", "verdict": "OK"})
    assert spot.main(["--run-date", "2026-09-25", "--execution-name", "exec-1"]) == 0
    line = json.loads(capsys.readouterr().out.strip())
    assert "metric_error" not in line, line
    assert "artifact_error" not in line, line
    assert sorted(built) == [("cloudwatch", "us-east-1"), ("s3", "us-east-1")]
