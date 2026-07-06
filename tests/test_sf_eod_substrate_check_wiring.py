"""Pins the Phase 2 → 3 substrate-health-check wiring in the EOD SF.

Mirrors ``test_sf_substrate_check_wiring.py`` (the Saturday SF version).
The new states ``DailySubstrateHealthCheck`` and
``WaitForDailySubstrateHealthCheck`` chain off the success path of
``CheckEODStatus`` and run the row-driven
``nousergon_lib.transparency`` checker on the dashboard EC2 with
``--cadence daily``.

The Saturday SF runs ``--cadence weekly`` which sweeps weekly + daily
rows. The weekday EOD SF runs ``--cadence daily`` so daily-emitting
rows (lineage, risk_events, residual_pct) get checked on the day they
land — without this, a bad emission Mon-Thu wouldn't surface until
Saturday's run.

Catches regressions like:
- Someone reroutes ``CheckEODStatus`` Success back to ``StopTradingInstance``
  and silently drops the substrate check.
- Someone removes the substrate state thinking the weekly check is
  enough (it isn't — that's the gap this state closes).
- Someone flips the substrate Catch into a hard-fail and starts halting
  EOD shutdown on row-level failure (per-row alarms own paging — the
  Catch is for SSM/infra failures only). Worse: a hard-fail Catch could
  prevent ``StopTradingInstance`` from running, leaving the trading EC2
  up overnight (cost overrun).
- Someone targets the trading EC2 instead of the dashboard EC2 (the
  trading EC2 doesn't have the dashboard repo or lib pin installed).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


class TestStatePresence:
    """Both new states must exist and chain after the existing EOD reconcile."""

    def test_daily_substrate_check_state_exists(self, states):
        assert "DailySubstrateHealthCheck" in states

    def test_wait_for_daily_substrate_check_exists(self, states):
        assert "WaitForDailySubstrateHealthCheck" in states


class TestChainOrdering:
    """Wiring goes: CheckEODStatus → Substrate → WaitForSubstrate → StopTradingInstance."""

    def test_check_eod_status_success_routes_to_substrate(self, states):
        choices = states["CheckEODStatus"]["Choices"]
        success_choice = next(
            (c for c in choices if c.get("StringEquals") == "Success"), None
        )
        assert success_choice is not None, "CheckEODStatus must have a Success branch"
        # Since L4607 the substrate check sits behind the
        # CheckSkipDailySubstrateHealthCheck rerun gate, whose Default runs it.
        # CheckEODStatus Success → that gate → (no skip flag) the substrate
        # check — it still runs after EODReconcile, not skipped to stop.
        assert success_choice["Next"] == "CheckSkipDailySubstrateHealthCheck", (
            "CheckEODStatus Success must hand off to the substrate skip-gate."
        )
        assert states["CheckSkipDailySubstrateHealthCheck"]["Default"] == (
            "DailySubstrateHealthCheck"
        ), "the skip-gate's Default must run the substrate check"

    def test_substrate_check_routes_to_wait_state(self, states):
        assert states["DailySubstrateHealthCheck"]["Next"] == (
            "WaitForDailySubstrateHealthCheck"
        )

    def test_wait_for_substrate_routes_to_stop_trading_instance(self, states):
        assert states["WaitForDailySubstrateHealthCheck"]["Next"] == "StopTradingInstance"


class TestCatchSemantics:
    """Substrate failures must NOT halt EOD shutdown.

    Cost-guard requirement: trading EC2 must always stop, regardless of
    substrate-check outcome. Per-row CloudWatch alarms own paging on
    row-level failures; the SF Catch only fires on infra-level failures
    (SSM unreachable, EC2 down). Either way, the failure path must
    terminate at StopTradingInstance, not HandleFailure.
    """

    def test_substrate_check_catch_continues_to_stop(self, states):
        catches = states["DailySubstrateHealthCheck"]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["Next"] == "StopTradingInstance", (
                f"Substrate Catch must continue to StopTradingInstance, not "
                f"{c['Next']!r} — letting the trading EC2 run overnight on a "
                f"substrate-check infra failure is a cost regression."
            )

    def test_substrate_wait_catch_continues_to_stop(self, states):
        catches = states["WaitForDailySubstrateHealthCheck"]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["Next"] == "StopTradingInstance"


class TestCommandShape:
    """The SSM command must invoke the lib CLI with --cadence daily --alert.

    Drops here would silently neuter the check (e.g. dropping --alert
    suppresses SNS without changing exit code; dropping --cadence flips
    to argparse error; flipping to --cadence weekly would re-check the
    same rows the Sat SF already covers and miss nothing new).
    """

    @pytest.fixture
    def commands(self, states) -> list[str]:
        return states["DailySubstrateHealthCheck"]["Parameters"]["Parameters"]["commands"]

    def test_invokes_transparency_module(self, commands):
        assert any(
            "python -m nousergon_lib.transparency" in cmd for cmd in commands
        )

    def test_passes_cadence_daily(self, commands):
        joined = " ".join(commands)
        assert "--cadence daily" in joined, (
            "Daily SF must run --cadence daily; --cadence weekly would "
            "duplicate the Sat SF coverage and miss the daily-only rows."
        )

    def test_passes_alert_flag(self, commands):
        joined = " ".join(commands)
        assert "--alert" in joined, (
            "Without --alert, row-level failures emit metrics but no SNS. "
            "Removing this flag silently degrades the gate."
        )

    def test_runs_on_dashboard_ec2(self, commands):
        # The dispatcher EC2 has the lib installed; confirm we cd there.
        joined = " ".join(commands)
        assert "alpha-engine-dashboard" in joined

    def test_pulls_latest_dashboard_main_before_running(self, commands):
        # Stale repo on the dispatcher would run an outdated lib pin.
        joined = " ".join(commands)
        assert "git" in joined and "pull" in joined


class TestInstanceTargeting:
    """The substrate state targets the dashboard EC2 (ec2_instance_id),
    not the trading EC2 (trading_instance_id).

    The trading EC2 doesn't have alpha-engine-dashboard or the lib pin
    installed; targeting it would fail at the cd step. The dashboard
    EC2 is the SF dispatcher with the lib installed (same place the
    Sat SF runs WeeklySubstrateHealthCheck).
    """

    def test_substrate_check_targets_dashboard_ec2(self, states):
        params = states["DailySubstrateHealthCheck"]["Parameters"]
        assert params["InstanceIds.$"] == "$.ec2_instance_id", (
            "Substrate check must target $.ec2_instance_id (dashboard EC2), "
            "not $.trading_instance_id (trading EC2 lacks the dashboard repo)."
        )

    def test_wait_for_substrate_polls_dashboard_ec2(self, states):
        params = states["WaitForDailySubstrateHealthCheck"]["Parameters"]
        assert params["InstanceId.$"] == "$.ec2_instance_id[0]"


class TestResultPathIsolation:
    """The substrate state must not stomp on the EOD reconcile result."""

    def test_distinct_result_paths(self, states):
        eod_path = states["EODReconcile"]["ResultPath"]
        sub_path = states["DailySubstrateHealthCheck"]["ResultPath"]
        assert eod_path != sub_path, (
            "Both states use ssm:sendCommand and need separate ResultPath "
            "fields so the wait states can resolve the right CommandId."
        )

    def test_wait_state_reads_substrate_command_id(self, states):
        params = states["WaitForDailySubstrateHealthCheck"]["Parameters"]
        # SF Parameters use ``CommandId.$`` (the dot-dollar suffix marks
        # the value as a JSONPath reference rather than a literal).
        cmd_id = params["CommandId.$"]
        assert "substrate_check_result" in cmd_id, (
            "WaitForDailySubstrateHealthCheck must poll the substrate "
            "command, not the EOD reconcile command."
        )


class TestStopTradingInstanceUnchanged:
    """StopTradingInstance must remain a terminal state — no rewiring
    that defers it past the substrate check or makes it conditional."""

    def test_stop_trading_instance_is_terminal(self, states):
        # End=True means this state ends the execution (success path).
        assert states["StopTradingInstance"].get("End") is True, (
            "StopTradingInstance must remain a terminal End=true state on "
            "the success path — anything else is a cost-overrun risk."
        )


class TestHandleFailureCostGuardHardening:
    """Pin the 2026-05-14 cost-guard hardening on ``HandleFailure``.

    Background: 2026-05-14 EOD recovery v2 SF execution failed at
    ``HandleFailure`` with `Invalid parameter: TopicArn Reason: An
    ARN must have at least 6 elements, not 5`. Root cause: the
    recovery input payload had a malformed ``sns_topic_arn`` (colon
    replaced with a space between ``us-east-1`` and the account ID).
    Because ``HandleFailure`` had no ``Catch``, the SNS publish
    failure aborted the whole SF before reaching ``ForceStopInstance``
    — leaving the trading EC2 running until manual stop. The state's
    own comment (`"Failure alert via SNS — instance still stops to
    avoid cost"`) was unenforced.

    Two-part fix:
    1. Hardcode the SNS topic ARN (no ``$.sns_topic_arn`` indirection)
       so a malformed input field can never block the cost-guard.
    2. Catch ``States.ALL`` on ``HandleFailure`` and route to
       ``ForceStopInstance`` so the cost-guard fires regardless of
       SNS-side failure (throttling, IAM drift, transient outage,
       future failure modes).
    """

    def test_topic_arn_is_literal_not_jsonpath(self, states):
        """Hardcoded ARN — no ``TopicArn.$`` indirection.

        A future PR that re-introduces the JSONPath form
        (``TopicArn.$``) would re-open the malformed-input attack
        surface that broke 2026-05-14 EOD recovery.
        """
        params = states["HandleFailure"]["Parameters"]
        assert "TopicArn" in params, (
            "HandleFailure.Parameters must include a literal 'TopicArn' field."
        )
        assert "TopicArn.$" not in params, (
            "HandleFailure must NOT use 'TopicArn.$' (JSONPath indirection) — "
            "the ARN is fixed and per-execution variability creates a "
            "corruption surface (2026-05-14 incident: malformed sns_topic_arn "
            "in recovery input → 'ARN must have at least 6 elements' → "
            "ForceStopInstance never fired → trading EC2 left running)."
        )
        # Spot-check the ARN shape — exactly 6 colon-separated parts,
        # SNS service, alpha-engine-alerts topic.
        arn = params["TopicArn"]
        parts = arn.split(":")
        assert len(parts) == 6, f"SNS ARN must have 6 parts; got {len(parts)}: {arn!r}"
        assert parts[:3] == ["arn", "aws", "sns"], f"Unexpected ARN prefix: {arn!r}"
        assert parts[5] == "alpha-engine-alerts", (
            f"ARN must point to alpha-engine-alerts topic; got {parts[5]!r}"
        )

    def test_handle_failure_has_catch_to_force_stop_instance(self, states):
        """HandleFailure must Catch States.ALL → ForceStopInstance.

        Defense-in-depth: even with the hardcoded ARN, any SNS-side
        failure (throttling, IAM drift, outage) must NOT block the
        cost-guard. The trading EC2 must always stop.
        """
        catches = states["HandleFailure"].get("Catch")
        assert catches, (
            "HandleFailure must define a 'Catch' block so SNS-side failures "
            "(throttling, IAM drift, outage) do NOT block ForceStopInstance. "
            "Without this, any publish failure leaves the trading EC2 running "
            "(2026-05-14 incident)."
        )
        all_catch = next(
            (c for c in catches if "States.ALL" in c.get("ErrorEquals", [])),
            None,
        )
        assert all_catch is not None, (
            "HandleFailure.Catch must include a 'States.ALL' branch — partial "
            "catches leave failure surfaces uncovered."
        )
        assert all_catch["Next"] == "ForceStopInstance", (
            f"HandleFailure Catch must route to ForceStopInstance, not "
            f"{all_catch['Next']!r}. The cost-guard is the load-bearing "
            "step; alert delivery is best-effort."
        )

    def test_input_schema_no_longer_requires_sns_topic_arn(self, states):
        """Once the ARN is hardcoded, no state's Parameters or input/output
        path should reference ``$.sns_topic_arn`` — confirms the SF input
        schema can drop the field on the next manual recovery payload.

        Walks the JSON tree (rather than grepping the serialized text) so
        Comment fields that explain *why* the indirection was removed
        don't trip the test.
        """

        def _walk_for_jsonpath_use(node, path="$"):
            hits: list[str] = []
            if isinstance(node, dict):
                for k, v in node.items():
                    sub = f"{path}.{k}"
                    if k == "Comment":
                        # Comments document intent — they're allowed to
                        # mention ``$.sns_topic_arn`` historically.
                        continue
                    if isinstance(v, str) and v == "$.sns_topic_arn":
                        hits.append(sub)
                    elif k.endswith(".$") and isinstance(v, str) and "sns_topic_arn" in v:
                        hits.append(sub)
                    else:
                        hits.extend(_walk_for_jsonpath_use(v, sub))
            elif isinstance(node, list):
                for i, v in enumerate(node):
                    hits.extend(_walk_for_jsonpath_use(v, f"{path}[{i}]"))
            return hits

        live_uses = _walk_for_jsonpath_use(states)
        assert not live_uses, (
            "No state should bind to '$.sns_topic_arn' after the 2026-05-14 "
            "hardening — the ARN is hardcoded in HandleFailure and the input "
            "field is no longer needed. Found live JSONPath references at: "
            f"{live_uses}. A reintroduction means someone re-added the "
            "indirection and re-opened the corruption surface."
        )
