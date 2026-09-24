"""sf_preflight_on_spot.py — the observe-mode on-spot preflight pass
(alpha-engine-config-I11312).

Pins the three properties the state machine relies on: it runs exactly the
checks the Lambda profile cannot reach, it classifies CAP_CHECKOUT skips as
expected rather than as the gap it exists to close, and it fails OPEN — no
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
    assert derived == _GROUP


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


_BOX = {sp.CAP_AWS, sp.CAP_ARCTIC, sp.CAP_REPO_MODULES, sp.CAP_POLYGON}


def test_full_box_closes_the_group_and_only_checkout_skips(monkeypatch):
    rec = _run_with(monkeypatch, _BOX)
    assert rec["verdict"] == "OK"
    assert rec["group_required_skip_count"] == 0
    assert set(rec["expected_skip_names"]) == _CHECKOUT
    # tool_contracts is REQUIRED in CHECK_REQUIRED, so the Lambda-comparable
    # count still names it — expected here, never escalated to the verdict.
    assert rec["required_skip_names"] == ["tool_contracts"]
    assert rec["ran_count"] == len(_GROUP)


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
    assert set(rec["fail_names"]) == _GROUP


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
def test_detect_capabilities_measures_polygon_and_never_claims_checkout(monkeypatch, key):
    import nousergon_lib.secrets as secrets

    monkeypatch.setattr(secrets, "get_secret", lambda name, required=False: key)
    caps = spot.detect_capabilities()
    assert sp.CAP_CHECKOUT not in caps
    assert (sp.CAP_POLYGON in caps) is bool(key)
    # This repo's own modules are importable from its checkout.
    assert sp.CAP_REPO_MODULES in caps


def test_a_raising_probe_withholds_its_capability(monkeypatch):
    import nousergon_lib.secrets as secrets

    def _raise(name, required=False):
        raise RuntimeError("ssm unreachable")

    monkeypatch.setattr(secrets, "get_secret", _raise)
    caps = spot.detect_capabilities()
    assert sp.CAP_POLYGON not in caps
    assert sp.CAP_AWS in caps
