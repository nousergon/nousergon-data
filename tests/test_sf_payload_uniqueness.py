"""Step Functions payload + policy + state-set UNIQUENESS chokepoints.

Origin: ROADMAP L302 P0-retrospective wider audit (2026-05-27). The
2026-05-26 dup-EB-target incident (PR #322) closed the specific
content-vs-uniqueness gap at the EventBridge target layer. The same
meta-pattern — tests pin WHAT was put, not HOW MANY were put or
whether anything ELSE was put — applies to six other surfaces in
this repo's CI:

  1. Lambda invoke Payload field-sets (eval-judge chain + aggregate-
     costs + every Saturday/weekday SF Lambda call site).
  2. SF role IAM ``lambda:InvokeFunction`` Statement count (multiple
     stale statements with overlapping ARNs could silently grant
     extra privileges).
  3. Weekday-SF SSM ``FLOW_DOCTOR_ENABLED=1`` ORDERING (existing test
     pins presence; this pins it appears BEFORE setup-logging runs).
  4. EOD-SF input-schema field closure (existing test asserts
     ``$.sns_topic_arn`` is absent; this pins the schema as a closed
     set so future field bloat surfaces at PR time).
  5. Friday-shell-run spot-state count (existing test parametrizes
     over 8 named states; this pins the count so an orphaned legacy
     ResearchML_old state with stale dry-flag wiring would fail loud).

The shape is the same per surface: pin a closed registry of expected
keys/states, fail loud when the actual set diverges. Mirrors PR #322's
TestCFNTargetUniqueness pattern.

Composes with [[reference-eventbridge-target-uniqueness-invariant]] +
[[feedback-audit-findings-become-roadmap-followups]].
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_INFRA = _REPO_ROOT / "infrastructure"

_SF_SATURDAY = _INFRA / "step_function.json"
_SF_WEEKDAY = _INFRA / "step_function_daily.json"
_SF_EOD = _INFRA / "step_function_eod.json"

_SF_ROLE_POLICY = _INFRA / "iam" / "alpha-engine-step-functions-role.json"


def _flatten_states(sf_doc: dict) -> dict:
    """Flatten top-level + every Parallel branch's states into one dict.

    Mirrors the helper in test_sf_aggregate_costs_wiring.py /
    test_sf_eval_judge_wiring.py so this file can be read in isolation.
    """
    flat: dict = dict(sf_doc["States"])
    for st in sf_doc["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat


# ── Finding 2 + 4: Lambda Payload field-sets are closed ─────────────────


# Pinned key sets, one per Lambda invoke state across all 3 SFs. Updating
# a Payload (adding/removing a field) is a deliberate act — extend this
# registry in the SAME PR that makes the wiring change. The registry is
# the single source of truth for "what fields each Lambda's SF Payload
# carries"; PRs that drift the JSON without updating it fail loud here.
#
# Saturday SF — alpha-engine-research + alpha-engine-data Lambdas
_SATURDAY_PAYLOAD_KEYS: dict[str, frozenset[str]] = {
    # L4517: preventive cross-repo lib-pin drift gate (predictor-inference Lambda).
    "LibPinDriftCheck": frozenset({"action"}),
    # config#693 (L4595): pre-spend pipeline-contract preflight gate, wired
    # directly after LibPinDriftGate's pass-through (predictor-inference Lambda).
    "PipelineContractCheck": frozenset({"action"}),
    # config#2348: pre-spend evaluator Lambda-SHA drift gate pair, wired
    # directly after PipelineContractGate's pass-through. Two separate Lambda
    # invokes (grading, then director) — each checks its OWN :live alias's
    # baked GIT_SHA against origin/main independently.
    "EvaluatorDeployDriftCheck": frozenset({"action"}),
    "EvaluatorDirectorDeployDriftCheck": frozenset({"action"}),
    # config#1824 weekly run-day gate (pure calendar; mirrors LibPinDriftCheck shape).
    "WeeklyRunDayGate": frozenset({"action"}),
    "Scanner": frozenset({"dry_run_llm.$", "run_date.$"}),
    "ThinkTankCoverage": frozenset({"mode", "sf_cover_target", "sf_cover_ceiling", "run_date.$"}),
    "RegimeSubstrate": frozenset({"action.$"}),
    "RegimeRetrospectiveEval": frozenset({"action.$"}),
    "Research": frozenset({"dry_run_llm.$", "force", "weekly_run", "skip_dry_run_gate"}),
    "DataPhase2": frozenset({"dry_run.$", "phase"}),
    "EvalJudgeSubmitFirstSaturday": frozenset(
        {"date.$", "dry_run_llm.$", "force_sonnet_pass", "capture_lookback_days"}
    ),
    "EvalJudgeSubmitWeekly": frozenset(
        {"date.$", "dry_run_llm.$", "force_sonnet_pass", "capture_lookback_days"}
    ),
    "EvalJudgePoll": frozenset(
        {"batch_id.$", "dry_run_llm.$", "max_wait_seconds", "submit_iso.$"}
    ),
    "EvalJudgeProcess": frozenset(
        {"batch_id.$", "dry_run_llm.$", "plan_s3_key.$"}
    ),
    "EvalRollingMean": frozenset({"end_time_iso.$"}),
    "RationaleClustering": frozenset({"dry_run_llm.$", "end_time_iso.$"}),
    "ReplayConcordance": frozenset(
        {
            "dry_run_llm.$",
            "end_time_iso.$",
            "max_artifacts",
            "target_models",
            "window_days",
        }
    ),
    "Counterfactual": frozenset(
        {"dry_run_llm.$", "end_time_iso.$", "max_depth", "window_days"}
    ),
    "AggregateCosts": frozenset({"date.$", "dry_run_llm.$"}),
    # Evaluator Report Card v2 (Layer B) — alpha-engine-evaluator:live. Builds
    # evaluator/{date}/report_card.json; non-fatal (own Catch → notify gate).
    # dry_run.$=$.research_dry → no-write on the Friday preflight (ROADMAP L4504).
    "ReportCard": frozenset({"date.$", "dry_run.$"}),
    # Director (Layer C, Part II) — alpha-engine-evaluator-director:live. Final
    # advisory task; reads the fresh report card, writes director/{date}/
    # action_plan.json; flag-gated (DIRECTOR_ENABLED) + non-fatal (own Catch).
    # dry_run.$=$.research_dry → no-Opus / no-write probe on the preflight (L4504).
    "Director": frozenset({"date.$", "dry_run.$"}),
}

# config#1811: the liveness-aware SSM poll iteration — one shared payload
# contract across all five weekday poll loops (the point of the
# consolidation; a divergent key-set here means a loop drifted from the
# shared ssm-liveness-poller contract).
_LIVENESS_POLLER_KEYS = frozenset({
    "instance_id.$",
    "command_id.$",
    "attempts.$",
    "ping_misses.$",
    "max_attempts",
    "max_ping_misses",
    "step",
})

# Weekday SF — alpha-engine-predictor Lambdas + the ssm-liveness-poller
_WEEKDAY_PAYLOAD_KEYS: dict[str, frozenset[str]] = {
    "DeployDriftCheck": frozenset({"action"}),
    # config#1430: NYSE trading-day gate, moved OFF the box into the
    # predictor-inference Lambda and run BEFORE StartExecutorEC2 (replaces the
    # cold-box SSM trading_calendar check whose stdout was unreliably captured).
    "TradingDayGate": frozenset({"action"}),
    "PredictorInference": frozenset({"action"}),
    "CheckPredictorCoverage": frozenset({"action"}),
    "ReinvokePredictor": frozenset({"action", "tickers.$"}),
    "RecheckCoverage": frozenset({"action"}),
    "PredictorHealthCheck": frozenset({"action"}),
    # config#1853: daily prediction-health producer — writes
    # predictor/metrics/drift_{trading_day}.json every weekday.
    "PredictorDriftCheck": frozenset({"action", "date.$"}),
    # config#1811: liveness-aware poll loops that stayed on the trading box
    # (CodeFreshnessGate, ChronicGapSelfHeal, RunMorningPlanner) share the
    # ssm-liveness-poller payload contract. WaitForMorningEnrich/
    # WaitForMorningArcticAppend do NOT appear here — config#1767 (Phase 2)
    # relocated those two onto independent ephemeral spot boxes whose own
    # PollMorningEnrichSpot/PollMorningArcticAppendSpot poll directly via
    # ssm:getCommandInvocation (a Task, not a lambda:invoke Payload), so they
    # are out of scope for this Lambda-Payload registry.
    "WaitForCodeFreshness": _LIVENESS_POLLER_KEYS,
    "WaitForChronicGap": _LIVENESS_POLLER_KEYS,
    "WaitForMorningPlanner": _LIVENESS_POLLER_KEYS,
    # config#1767 (Phase 2): the data phase (enrich + Arctic append) was relocated
    # onto two independent ephemeral spot boxes via the alpha-engine-data-spot-
    # dispatcher Lambda. Each launch state passes a single {"workload": <key>}
    # selecting the collector invocation; the dispatcher returns
    # {data_spot:{launched,instance_id,...}}.
    "LaunchMorningEnrichSpot": frozenset({"workload"}),
    "LaunchMorningArcticAppendSpot": frozenset({"workload"}),
}


def _enumerate_lambda_payloads(sf_doc: dict) -> dict[str, frozenset[str]]:
    """Return {state_name: frozenset(payload_keys)} for every Lambda invoke
    state with a static dict Payload."""
    out: dict[str, frozenset[str]] = {}
    for name, st in _flatten_states(sf_doc).items():
        if st.get("Type") != "Task":
            continue
        if "lambda:invoke" not in st.get("Resource", "").lower():
            continue
        payload = st.get("Parameters", {}).get("Payload")
        if isinstance(payload, dict):
            out[name] = frozenset(payload.keys())
    return out


class TestSaturdaySFPayloadFieldSetsClosed:
    """Every Saturday-SF Lambda Payload's key-set is pinned. Drift =
    explicit registry update.

    Closes L302 wider-audit findings (eval_judge_wiring + aggregate_costs +
    every other Lambda Payload that wasn't covered by an existing field-
    count test).
    """

    @pytest.fixture(scope="class")
    def actual_payloads(self) -> dict[str, frozenset[str]]:
        return _enumerate_lambda_payloads(
            json.loads(_SF_SATURDAY.read_text())
        )

    def test_every_lambda_payload_is_in_registry(self, actual_payloads):
        """No unregistered Lambda Payload states. A new Lambda call site
        added to the Saturday SF without updating ``_SATURDAY_PAYLOAD_KEYS``
        fails loud here — extending the registry IS the contract."""
        extra = set(actual_payloads) - set(_SATURDAY_PAYLOAD_KEYS)
        assert not extra, (
            f"Saturday SF has Lambda invoke states with Payloads NOT in the "
            f"_SATURDAY_PAYLOAD_KEYS registry: {sorted(extra)}. Either add "
            "them to the registry with their expected key-set, or remove the "
            "Lambda call. The registry is the chokepoint that catches "
            "untested Payload drift at PR time."
        )

    def test_no_registry_entry_missing_from_sf(self, actual_payloads):
        """A registry entry for a state that no longer exists in the SF
        means either the state was renamed or removed without updating
        the registry — drift in the opposite direction."""
        missing = set(_SATURDAY_PAYLOAD_KEYS) - set(actual_payloads)
        assert not missing, (
            f"_SATURDAY_PAYLOAD_KEYS registry has entries for states no "
            f"longer in the Saturday SF: {sorted(missing)}. Either remove "
            "them or re-add the SF state."
        )

    @pytest.mark.parametrize("state_name", sorted(_SATURDAY_PAYLOAD_KEYS))
    def test_payload_keys_match_registry(self, actual_payloads, state_name):
        """For each registered state, the live Payload key set MUST match
        the registry exactly — no extras (silent field bloat), no missing
        (silent field drops)."""
        if state_name not in actual_payloads:
            pytest.skip(
                f"{state_name} not present in SF — covered by "
                "test_no_registry_entry_missing_from_sf"
            )
        expected = _SATURDAY_PAYLOAD_KEYS[state_name]
        actual = actual_payloads[state_name]
        assert actual == expected, (
            f"Saturday SF state {state_name!r} Payload keys drifted from "
            f"registry. Extras: {sorted(actual - expected)} | "
            f"Missing: {sorted(expected - actual)}. If the change is "
            "deliberate, update _SATURDAY_PAYLOAD_KEYS in this test file "
            "in the SAME PR."
        )


class TestWeekdaySFPayloadFieldSetsClosed:
    """Same chokepoint as Saturday but for the weekday SF Lambda Payloads."""

    @pytest.fixture(scope="class")
    def actual_payloads(self) -> dict[str, frozenset[str]]:
        return _enumerate_lambda_payloads(
            json.loads(_SF_WEEKDAY.read_text())
        )

    def test_every_lambda_payload_is_in_registry(self, actual_payloads):
        extra = set(actual_payloads) - set(_WEEKDAY_PAYLOAD_KEYS)
        assert not extra, (
            f"Weekday SF has Lambda invoke states with Payloads NOT in the "
            f"_WEEKDAY_PAYLOAD_KEYS registry: {sorted(extra)}."
        )

    def test_no_registry_entry_missing_from_sf(self, actual_payloads):
        missing = set(_WEEKDAY_PAYLOAD_KEYS) - set(actual_payloads)
        assert not missing, (
            f"_WEEKDAY_PAYLOAD_KEYS registry has entries for states no "
            f"longer in the weekday SF: {sorted(missing)}."
        )

    @pytest.mark.parametrize("state_name", sorted(_WEEKDAY_PAYLOAD_KEYS))
    def test_payload_keys_match_registry(self, actual_payloads, state_name):
        if state_name not in actual_payloads:
            pytest.skip(
                f"{state_name} not present in SF — covered by "
                "test_no_registry_entry_missing_from_sf"
            )
        expected = _WEEKDAY_PAYLOAD_KEYS[state_name]
        actual = actual_payloads[state_name]
        assert actual == expected, (
            f"Weekday SF state {state_name!r} Payload keys drifted from "
            f"registry. Extras: {sorted(actual - expected)} | "
            f"Missing: {sorted(expected - actual)}."
        )


# ── Finding 3: SF role has exactly one lambda:InvokeFunction Statement ──


class TestSFRoleInvokeFunctionStatementCount:
    """``alpha-engine-step-functions-role`` declares
    ``lambda:InvokeFunction`` in EXACTLY ONE Statement.

    Multiple statements with overlapping ARN patterns would silently
    grant additional privileges beyond the canonical list — e.g. a
    stale Statement from a pre-2026 refactor with a hardcoded
    deprecated ARN would let the SF invoke a Lambda it shouldn't.

    The existing ``test_every_invoked_lambda_has_iam_grant`` walks
    every Statement's resources; that catches missing grants but NOT
    stale ones. This test closes the other half.
    """

    def test_exactly_one_invoke_function_statement(self):
        doc = json.loads(_SF_ROLE_POLICY.read_text())
        invoke_stmts = []
        for i, stmt in enumerate(doc.get("Statement", [])):
            actions = stmt.get("Action")
            actions_list = (
                [actions] if isinstance(actions, str) else (actions or [])
            )
            if "lambda:InvokeFunction" in actions_list:
                invoke_stmts.append(
                    (i, stmt.get("Sid"), len(actions_list))
                )
        assert len(invoke_stmts) == 1, (
            f"Expected EXACTLY 1 Statement with lambda:InvokeFunction in "
            f"alpha-engine-step-functions-role.json; found "
            f"{len(invoke_stmts)}: {invoke_stmts}. Multiple statements "
            "with overlapping ARN patterns silently grant extra "
            "privileges — consolidate into one or document a non-overlap "
            "guarantee at PR time."
        )


# ── Finding 5: FLOW_DOCTOR_ENABLED appears EARLY in SSM command blocks ──


_EARLY_COMMAND_WINDOW = 3
"""``FLOW_DOCTOR_ENABLED=1`` must appear within the first 3 commands of
every weekday-SF SSM block. The handler ``setup_logging`` is invoked
after `source .venv/bin/activate` (typically command 4+); the env var
MUST be set before then. Pinning index < 3 absorbs minor reformatting
(adding a leading comment line) without breaking the contract."""


def _iter_weekday_ssm_command_blocks() -> list[tuple[str, list[str]]]:
    sf = json.loads(_SF_WEEKDAY.read_text())
    out: list[tuple[str, list[str]]] = []
    for name, st in _flatten_states(sf).items():
        if st.get("Type") != "Task":
            continue
        if "ssm" not in st.get("Resource", "").lower():
            continue
        params = st.get("Parameters", {}).get("Parameters", {})
        cmds = params.get("commands")
        if isinstance(cmds, list):
            out.append((name, cmds))
    return out


class TestWeekdaySSMFlowDoctorOrdering:
    """The existing ``test_weekday_sf_ssm_blocks_export_flow_doctor_enabled``
    pins that ``FLOW_DOCTOR_ENABLED=1`` appears SOMEWHERE in each weekday
    SSM block. This closes the ordering gap: the flag must appear
    BEFORE ``source .venv/bin/activate`` (which triggers
    setup_logging's env-var read).

    2026-05-11 incident exact pattern: a future PR could keep the
    flag in the block but move it after the venv activation, leaving
    setup_logging gated and flow-doctor silently disabled.
    """

    def test_flow_doctor_enabled_appears_in_first_three_commands(self):
        offenders: list[str] = []
        for name, cmds in _iter_weekday_ssm_command_blocks():
            idx = next(
                (
                    i
                    for i, c in enumerate(cmds)
                    if "FLOW_DOCTOR_ENABLED=1" in c
                ),
                -1,
            )
            if idx < 0:
                # Already covered by the existing presence test —
                # don't double-report here.
                continue
            if idx >= _EARLY_COMMAND_WINDOW:
                offenders.append(f"{name} (FLOW_DOCTOR_ENABLED at index {idx})")
        assert not offenders, (
            f"Weekday SF SSM blocks have FLOW_DOCTOR_ENABLED=1 appearing "
            f"AFTER the first {_EARLY_COMMAND_WINDOW} commands:\n  - "
            + "\n  - ".join(offenders)
            + "\n\nMove the export to the top of the commands array — "
            "setup_logging reads the env var when `.venv/bin/activate` "
            "sources it, which is typically command 4+. See 2026-05-11 "
            "silent-MorningEnrich incident."
        )


# ── Finding 6: EOD-SF input schema is a closed set ──


# Fields the EOD SF accepts via its top-level input. Updating this
# registry MUST happen in the same PR that wires a new input field
# through the SF. The existing
# `test_input_schema_no_longer_requires_sns_topic_arn` confirms
# `$.sns_topic_arn` is absent post-removal; this enumerates the
# remaining accepted fields so future bloat surfaces at PR time.
#
# Computed by walking the EOD SF for every `$.X` reference in
# Parameters / Choices / ResultPath / InputPath, then filtering to
# top-level fields (single segment after `$`).
def _eod_referenced_input_fields() -> frozenset[str]:
    # Walk the parsed SF and capture the FIRST segment of every string value
    # that STARTS with ``$.`` (equivalent to the old ``"$.`` text regex, since
    # ``"$.`` only ever opens a JSON string value) — EXCEPT inside a
    # ``ResultSelector`` block. Within a ResultSelector ``$`` rebinds to the raw
    # task result, so its ``"$.Status"`` / ``"$.CommandId"`` RHS values are NOT
    # top-level SF-state fields (config#1163: the weekday/EOD poll-trim
    # ResultSelectors introduced exactly these and must not pollute the
    # top-level namespace registry, which would otherwise mask a real future
    # ``$.Status`` ResultPath collision).
    import re

    refs: set[str] = set()

    def _walk(obj) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "ResultSelector":
                    continue
                _walk(v)
        elif isinstance(obj, list):
            for v in obj:
                _walk(v)
        elif isinstance(obj, str):
            m = re.match(r"\$\.([A-Za-z_][A-Za-z0-9_]*)", obj)
            if m:
                refs.add(m.group(1))

    _walk(json.loads(_SF_EOD.read_text()))
    return frozenset(refs)


class TestEODSFTopLevelFieldsClosed:
    """Pin the EOD SF's top-level field set as a closed registry.

    Two field categories share the top-level ``$.<X>`` namespace:
      * **Input fields** — set by the SF's invoker (EventBridge or
        operator manual start). Drive Choice routing + Lambda Payloads.
      * **Intermediate fields** — populated by ResultPath on Task
        outputs (``$.eod_result``, ``$.postmarket_poll``, etc.).
        Read by downstream Choice / Catch / Lambda invocations.

    Both classes occupy the same namespace, so a new ResultPath that
    accidentally shadows an input field (or vice versa) silently
    corrupts the state machine. Pinning the union catches both shapes
    at PR time.

    Existing ``test_input_schema_no_longer_requires_sns_topic_arn``
    pins the absence of a single retired field; this pins the closed
    set so any addition (or rename drifting both sides) fails loud.
    """

    # Registry of every top-level ``$.<X>`` field the EOD SF
    # references — UNION of input fields + intermediate ResultPath
    # fields. Snapshot from step_function_eod.json on 2026-05-27.
    _EXPECTED_EOD_TOP_LEVEL_FIELDS: frozenset[str] = frozenset(
        {
            # Intermediate ResultPath outputs
            "ec2_instance_id",
            "eod_poll",
            "eod_result",
            "error",
            "failure_notify",
            "failure_notify_error",
            "force_stop_result",
            "postmarket_poll",
            # PostMarketArcticAppend (2026-06-16) — slow daily_append split out
            # of PostMarketData into its own state (mirrors MorningArcticAppend
            # L4608); emits its own poll ResultPath.
            "postmarket_arctic_poll",
            # config#1767 (Phase 2): the EOD data phase (PostMarketData +
            # PostMarketArcticAppend) was relocated OFF the on-trading SSM path
            # onto an ephemeral spot box. The old on-trading send ResultPaths
            # ($.postmarket_result, $.postmarket_arctic_result) are gone; each
            # spot launch emits its dispatcher-Lambda ResultPath and a fail-open
            # error path. The poll ResultPaths above are reused by the spot poll.
            "postmarket_launch",
            "postmarket_arctic_launch",
            "data_spot_error",
            "data_spot_failure_notify",
            "snapshot_poll",
            "snapshot_result",
            "stop_result",
            "substrate_check_error",
            "substrate_check_poll",
            "substrate_check_result",
            "trading_instance_id",
            # L274 SF MutualExclusionGuard (2026-05-27) — CheckMutexRole
            # reads $.pipeline_role; AcquireMutex emits $.mutex_result on
            # success, $.mutex_conflict on ConditionalCheckFailed Catch,
            # and $.mutex_error on the fail-open States.ALL Catch.
            "mutex_conflict",
            "mutex_error",
            "mutex_result",
            "pipeline_role",
            # L4607 per-task rerun gates — each CheckSkip<State> reads an
            # optional boolean skip flag from the execution input so an
            # operator recovery rerun can resume at the first incomplete task.
            "skip_post_market_data",
            # config#1767: skip_post_market_arctic_append removed — its gate
            # (CheckSkipPostMarketArcticAppend) moved with the on-trading append
            # state; skip_post_market_data now skips the whole spot data phase.
            "skip_capture_snapshot",
            "skip_eod_reconcile",
            "skip_daily_substrate_health_check",
            # StartTradingInstance re-runnability guard (2026-06-30) —
            # ec2:startInstances emits $.ec2_start_result; the SSM-readiness
            # poll emits $.ssm_describe_result (describeInstanceInformation) and
            # $.ssm_poll (bounded attempts counter). Ensures the box is up +
            # SSM-Online before the first sendCommand, so an operator recovery
            # rerun after the prior run's ForceStopInstance no longer dies with
            # Ssm.InvalidInstanceIdException.
            "ec2_start_result",
            "ssm_describe_result",
            "ssm_poll",
            # config#1549 — top-of-pipeline executor-deploy refresh chokepoint.
            # CheckSkipRefreshExecutorDeploy reads $.skip_refresh_executor_deploy
            # (optional rerun flag); RefreshExecutorDeploy emits
            # $.refresh_executor_deploy_result (sendCommand) and its poll emits
            # $.refresh_executor_deploy_poll (getCommandInvocation, trimmed by
            # ResultSelector). Hoists nousergon-data#574's per-step boot-pull to
            # a single chokepoint so the whole EOD run executes latest main.
            "skip_refresh_executor_deploy",
            "refresh_executor_deploy_result",
            "refresh_executor_deploy_poll",
            # substrate health check (EOD SF) — fail-notify paths
            "health_check_degraded",
            "substrate_health_check_degraded_notify",
            "substrate_health_check_degraded_notify_error",
        }
    )

    def test_eod_top_level_field_set_is_closed(self):
        actual = _eod_referenced_input_fields()
        unregistered = actual - self._EXPECTED_EOD_TOP_LEVEL_FIELDS
        assert not unregistered, (
            f"EOD SF references top-level ``$.<X>`` field(s) not in the "
            f"closed registry: {sorted(unregistered)}. If the addition "
            "is deliberate, add them to _EXPECTED_EOD_TOP_LEVEL_FIELDS in "
            "this test file in the SAME PR. The registry IS the namespace "
            "contract — preventing silent ResultPath/input-field collisions."
        )

    def test_no_registry_entry_missing_from_sf(self):
        """A registry entry for a field the SF no longer references
        means the field was renamed or removed without updating the
        registry — drift in the opposite direction."""
        actual = _eod_referenced_input_fields()
        missing = self._EXPECTED_EOD_TOP_LEVEL_FIELDS - actual
        assert not missing, (
            f"_EXPECTED_EOD_TOP_LEVEL_FIELDS has registry entries no "
            f"longer in the EOD SF: {sorted(missing)}. Either re-add the "
            "field reference or remove it from the registry."
        )


# ── Finding 7: Friday-shell-run spot-state count is closed ──


# Pin the count of SPOT states (states that boot a spot via
# `bash infrastructure/spot_*.sh ...`) in the Saturday SF. Matches the
# `_SPOT_STATES` registry in test_sf_friday_shell_run_wiring.py:115. An
# orphaned legacy state with a similar shape (e.g. ResearchML_old) would
# fail this count.
# 8 → 10 on 2026-05-31 (ROADMAP L4472): the single Backtester spot state
# was split into Backtester (simulate) + PredictorBacktest +
# PortfolioOptimizerBacktest so no single SSM command carries the summed
# 60-100 min post-sweep runtime that blew the timeout (L4470).
# 10 → 11 on 2026-06-08 (ROADMAP L4544): ModelZooRotation — the best-effort
# model-zoo weekly rotation + CPCV selection, sequential after PredictorTraining
# success in Branch B (same spot instance, off the live-trading path).
# Still 11 on config#1083 (2026-06-15): ModelZooRotation was REPLACED by the
# parallel fan-out — ResolveZooSpecs (NOT a spot; runs list-rotation-specs on the
# box) → ModelZooTrainMap (per-spec spots, but TrainSpecDispatch lives in the Map
# ItemProcessor, which _flatten_states does NOT descend into) → ModelZooSelect
# (the one flat-level spot launcher that takes ModelZooRotation's slot). Net
# flat-level spot count is unchanged at 11.
# 11 → 10 on config#902 (2026-07-02): the standalone DriftDetection spot state
# was COLLAPSED — drift is now bundled onto the PredictorTraining spot
# (crucible-predictor spot_train.sh runs monitoring.drift_detector non-blocking
# after training succeeds, on the same instance), so it no longer launches its
# own spot. DriftDetection dropped out of the flat-level spot set.
_EXPECTED_SATURDAY_SPOT_STATE_COUNT = 10


def _saturday_spot_states() -> list[str]:
    """Find every Task state whose `commands` contains
    `bash infrastructure/spot_*.sh` — these are the spot-instance
    launchers and must equal the documented 8."""
    sf = json.loads(_SF_SATURDAY.read_text())
    out: list[str] = []
    for name, st in _flatten_states(sf).items():
        if st.get("Type") != "Task":
            continue
        if "ssm" not in st.get("Resource", "").lower():
            continue
        params = st.get("Parameters", {}).get("Parameters", {})
        # commands may be a literal list (DriftDetection pre-data#261)
        # or a States.Format reference under `commands.$` (post-rewire).
        for key in ("commands", "commands.$"):
            v = params.get(key)
            if isinstance(v, list):
                joined = " ".join(v)
            elif isinstance(v, str):
                joined = v
            else:
                continue
            if "infrastructure/spot_" in joined and ".sh" in joined:
                out.append(name)
                break
    return out


class TestSaturdaySFSpotStateCount:
    """Closes the spot-state set as exactly 8. Pre-rewire
    test_sf_friday_shell_run_wiring.py parametrizes over the 8 expected
    names but doesn't assert there are EXACTLY 8 — an orphaned legacy
    spot state from an incomplete refactor would slip through.
    """

    def test_exactly_eight_spot_states_in_saturday_sf(self):
        spots = _saturday_spot_states()
        assert len(spots) == _EXPECTED_SATURDAY_SPOT_STATE_COUNT, (
            f"Saturday SF should have EXACTLY "
            f"{_EXPECTED_SATURDAY_SPOT_STATE_COUNT} spot-launching states; "
            f"found {len(spots)}: {sorted(spots)}. Either an orphaned "
            "legacy state slipped through an incomplete refactor or a "
            "deliberate spot-state addition needs the test bump."
        )
