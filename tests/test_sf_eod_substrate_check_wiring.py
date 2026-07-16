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

config#2326 (ports config#2276's weekly-SF health-check-honesty fix onto
the EOD SF) changed the WAIT/CATCH shape pinned below:
- ``WaitForDailySubstrateHealthCheck`` no longer check-once's straight to
  ``StopTradingInstance``; it now feeds ``CheckDailySubstrateHealthCheckStatus``,
  a poll-to-terminal-status Choice (Success -> StopTradingInstance,
  in-flight -> DailySubstrateHealthCheckPollWait -> re-poll, terminal
  non-Success -> SubstrateHealthCheckDegraded).
- Both states' Catches now route through ``SubstrateHealthCheckDegraded``
  (sets ``$.health_check_degraded``) instead of directly to
  ``StopTradingInstance`` — but the COST-GUARD INVARIANT this file has
  always pinned is UNCHANGED: every path still terminates at
  ``StopTradingInstance``, never ``HandleFailure``. Degraded now ALSO
  publishes a dedicated best-effort SNS alert
  (``PublishSubstrateHealthCheckDegradedAlert``) before continuing, since
  (per test_sf_notifier_totality_wiring.py's audit) the EOD SF has no
  success-path notifier to thread the flag into.

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
- Someone reintroduces a check-once poll (the config#2276/config#2326
  masking-Catch defect class) or a runtime ``pip install``.
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

    def test_wait_for_substrate_routes_to_status_check(self, states):
        """config#2326: check-once fix — WaitForDailySubstrateHealthCheck no
        longer chains directly to StopTradingInstance; it feeds the
        terminal-status Choice so an in-flight/hung command is polled to
        completion instead of being treated as done on the first read."""
        assert states["WaitForDailySubstrateHealthCheck"]["Next"] == (
            "CheckDailySubstrateHealthCheckStatus"
        )


class TestCatchSemantics:
    """Substrate failures must NOT halt EOD shutdown.

    Cost-guard requirement: trading EC2 must always stop, regardless of
    substrate-check outcome. Per-row CloudWatch alarms own paging on
    row-level failures; the SF Catch only fires on infra-level failures
    (SSM unreachable, EC2 down). Either way, the failure path must
    terminate at StopTradingInstance, not HandleFailure.

    config#2326: the Catches no longer jump DIRECTLY to StopTradingInstance
    (that was the masking-Catch defect config#2276 closed on the weekly SF)
    — they now route through SubstrateHealthCheckDegraded, which sets
    $.health_check_degraded and publishes a best-effort alert before still
    continuing to StopTradingInstance. The cost-guard invariant (must reach
    StopTradingInstance, must NOT reach HandleFailure) is preserved; only
    the masking (silent, unsignaled continue) is removed.
    """

    def test_substrate_check_catch_routes_through_degraded(self, states):
        catches = states["DailySubstrateHealthCheck"]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["Next"] == "SubstrateHealthCheckDegraded", (
                f"Substrate Catch must route through SubstrateHealthCheckDegraded "
                f"(sets health_check_degraded + alerts), not {c['Next']!r} — a "
                "direct jump to StopTradingInstance is the silent-skip masking "
                "config#2326 (mirroring config#2276) closed."
            )

    def test_substrate_wait_catch_routes_through_degraded(self, states):
        catches = states["WaitForDailySubstrateHealthCheck"]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["Next"] == "SubstrateHealthCheckDegraded"

    def test_degraded_pass_still_terminates_at_stop_trading_instance(self, states):
        """The degraded path must still reach StopTradingInstance (via the
        alert publish) — visibility must never come at the cost of delaying
        or skipping the cost-guard shutdown."""
        degraded = states["SubstrateHealthCheckDegraded"]
        assert degraded["Type"] == "Pass"
        assert degraded["Result"] is True
        assert degraded["ResultPath"] == "$.health_check_degraded"
        alert = states[degraded["Next"]]
        assert alert["Type"] == "Task"
        assert alert["Resource"] == "arn:aws:states:::sns:publish"
        assert alert["Next"] == "StopTradingInstance"
        # The alert's own Catch must ALSO continue to StopTradingInstance —
        # a best-effort notifier must never be able to block the cost-guard.
        (alert_catch,) = alert["Catch"]
        assert alert_catch["ErrorEquals"] == ["States.ALL"]
        assert alert_catch["Next"] == "StopTradingInstance"

    def test_degraded_never_reaches_handle_failure(self, states):
        """The exact failure mode a careless port could introduce: routing
        the new degraded/alert states into HandleFailure instead of
        StopTradingInstance, which would delay/block the cost-guard."""
        for name in (
            "SubstrateHealthCheckDegraded",
            "PublishSubstrateHealthCheckDegradedAlert",
        ):
            st = states[name]
            assert st.get("Next") != "HandleFailure"
            for c in st.get("Catch", []) or []:
                assert c.get("Next") != "HandleFailure"


class TestPollToTerminalStatus:
    """config#2326: the check-once poll fix — mirrors the weekly SF's
    CheckSaturdayHealthCheckStatus / CheckSubstrateHealthCheckStatus shape
    (config#2276) and this repo's own CheckMorningEnrichStatus idiom."""

    def test_status_choice_success_goes_to_stop_trading_instance(self, states):
        choice = states["CheckDailySubstrateHealthCheckStatus"]
        assert choice["Type"] == "Choice"
        success = next(
            c for c in choice["Choices"] if c.get("StringEquals") == "Success"
        )
        assert success["Variable"] == "$.substrate_check_poll.Status"
        assert success["Next"] == "StopTradingInstance"

    def test_status_choice_loops_on_exactly_in_flight_statuses(self, states):
        choice = states["CheckDailySubstrateHealthCheckStatus"]
        in_flight = next(c for c in choice["Choices"] if "Or" in c)
        looped = {op["StringEquals"] for op in in_flight["Or"]}
        assert looped == {"InProgress", "Pending", "Delayed"}
        assert in_flight["Next"] == "DailySubstrateHealthCheckPollWait"

    def test_status_choice_default_is_degraded(self, states):
        """The drill edge: a terminal non-Success (Failed / TimedOut /
        Cancelled) must land on the degraded Pass, not fall through to
        StopTradingInstance un-signaled."""
        assert states["CheckDailySubstrateHealthCheckStatus"]["Default"] == (
            "SubstrateHealthCheckDegraded"
        )

    def test_poll_wait_loops_back_to_wait_state(self, states):
        wait = states["DailySubstrateHealthCheckPollWait"]
        assert wait["Type"] == "Wait"
        assert wait["Next"] == "WaitForDailySubstrateHealthCheck"


class TestNoRuntimePipInstall:
    """config#2326: deps come from the dashboard box's deploy-time venv sync
    (crucible-dashboard infrastructure/deploy-on-merge.sh pip-installs on
    requirements.txt diff; nousergon-lib is tag-pinned so a lib bump always
    diffs requirements.txt) — same rationale test_sf_health_check_honesty_wiring.py
    pins for the weekly SF's WeeklySubstrateHealthCheck."""

    def test_no_pip_install_in_substrate_check_commands(self, states):
        cmds = states["DailySubstrateHealthCheck"]["Parameters"]["Parameters"]["commands"]
        assert not any("pip install" in cmd for cmd in cmds), (
            "runtime pip install must not reappear in DailySubstrateHealthCheck"
        )

    def test_no_pip_install_anywhere_in_eod_definition(self, sf):
        def _commands(states):
            for name, st in states.items():
                cmds = (st.get("Parameters", {}) or {}).get("Parameters", {}).get(
                    "commands"
                )
                if cmds:
                    yield name, cmds

        offenders = [
            name for name, cmds in _commands(sf["States"])
            if "pip install" in " ".join(cmds)
        ]
        assert not offenders, f"runtime pip install in: {offenders}"


class TestTimeoutConvention:
    """config#2326 convention (mirrors config#2276): inner executionTimeout =
    script budget; SSM Parameters.TimeoutSeconds = 60 uniform (delivery
    timeout); outer Task TimeoutSeconds = inner + 30."""

    def test_timeout_triple(self, states):
        st = states["DailySubstrateHealthCheck"]
        ssm_params = st["Parameters"]["Parameters"]
        inner = int(ssm_params["executionTimeout"][0])
        assert st["Parameters"]["TimeoutSeconds"] == 60
        assert st["TimeoutSeconds"] == inner + 30


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
    """StopTradingInstance must remain UNCONDITIONALLY reachable and must
    never be deferred past the substrate check — the cost-guard invariant.

    UPDATED config-I2702 (2026-07-15) deliverable #4: StopTradingInstance is
    no longer a bare End=true terminal — it now routes to CheckDegradedOutcome
    so a run that skipped EODReconcile (a data-gap self-heal) ends in a
    DISTINCT terminal (DegradedSucceeded) rather than the same plain
    ExecutionSucceeded a fully-green day gets. The cost-guard invariant this
    class actually protects — "the trading EC2 always gets stopped, no
    matter what happened upstream" — is UNCHANGED: CheckDegradedOutcome only
    ever routes to one of two Succeed states, both AFTER StopTradingInstance
    has already run. Neither NormalSucceeded nor DegradedSucceeded can be
    reached without StopTradingInstance first — see the ASL wiring pinned in
    test_sf_eod_precondition_probe_wiring.py.
    """

    def test_stop_trading_instance_routes_to_degraded_outcome_check(self, states):
        sti = states["StopTradingInstance"]
        assert "End" not in sti, (
            "StopTradingInstance is no longer a bare End=true terminal "
            "(config-I2702 deliverable #4) — it must route onward via Next."
        )
        assert sti["Next"] == "CheckDegradedOutcome"

    def test_degraded_outcome_check_only_reaches_succeed_states(self, states):
        cdo = states["CheckDegradedOutcome"]
        assert cdo["Type"] == "Choice"
        targets = {c["Next"] for c in cdo["Choices"]} | {cdo["Default"]}
        for t in targets:
            assert states[t]["Type"] == "Succeed", (
                f"CheckDegradedOutcome must only ever route to a Succeed "
                f"state (the cost-guard has already run by this point) — "
                f"{t} is Type={states[t]['Type']}"
            )

    def test_stop_trading_instance_catch_unchanged(self, states):
        # The failure path (Catch -> HandleFailure -> ForceStopInstance) is
        # untouched by this change — only the SUCCESS Next moved.
        catches = states["StopTradingInstance"].get("Catch", [])
        assert any(
            c["ErrorEquals"] == ["States.ALL"] and c["Next"] == "HandleFailure"
            for c in catches
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
