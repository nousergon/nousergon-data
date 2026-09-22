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
# alpha-engine-config-I2544/I2545: the two child SFs split out of the


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
    # config#2249: fast pre-dispatch substrate health gate, immediately
    # before MorningEnrich (alpha-engine-substrate-health-gate Lambda).
    # alpha-engine-config-I10172: run_date.$ added — this stage had never
    # once written a _stage_coverage verdict (the I8228 "never wired" class);
    # its Payload carried no execution identity at all to key one by.
    "SubstrateHealthGate": frozenset({"instance_id.$", "run_date.$"}),
    # alpha-engine-config-I8214: the stage-coverage sweep at the tail of the
    # run. Deliberately NOT threading execution_arn: the sweep reads the whole
    # CYCLE (every contributing execution for this run_date), not this one
    # execution — passing an execution arn would invite a handler that reads
    # only its own caller, which is the 1-of-16 reading I8186 is about.
    # alpha-engine-config-I8809: calendar_date is the LEGACY partition. The
    # sweep unions it with the trading-day family until the 2026-09-05 cutover,
    # so one cycle split across both reads as one cycle instead of ~28 false
    # absences. Removed with the fallback at the cutover.
    # alpha-engine-config-I10170: +observer_execution_arn.$ ($$.Execution.Id).
    # This state runs INSIDE the execution it grades, so that execution is
    # RUNNING whenever it grades — measured on the live 2026-09-04 cycle, the
    # sweep declared the cycle in_flight and paged 13 absences in the same
    # artifact one second before that execution succeeded. Naming the observer
    # lets its entered states count without its status making its own cycle
    # non-terminal. Any OTHER running execution still yields in_flight.
    "WeeklyCoverageSweep": frozenset(
        {
            "run_date.$",
            "calendar_date.$",
            "dry_run.$",
            "state_machine_arn.$",
            "observer_execution_arn.$",
        }
    ),
    # alpha-engine-config-I8809: the weekly graph's ONE date normalization —
    # calendar date in, cycle trading day out. Reuses alpha-engine-weekly-preflight
    # rather than adding a function, because a new Lambda needs an IAM role
    # bootstrap and that is an operator step a PR cannot perform.
    "NormalizeRunDates": frozenset({"action", "calendar_date.$"}),
    # L4517: preventive cross-repo lib-pin drift gate (predictor-inference Lambda).
    # alpha-engine-config-I8155: +run_date.$ — the stage-coverage verdict this
    # Lambda writes is keyed on the execution's own run_date, and without it
    # in the Payload the handler substituted datetime.now(), which matches
    # $.run_date only while the stage runs on the same UTC day the execution
    # started.
    "LibPinDriftCheck": frozenset({"action", "run_date.$", "execution_arn.$"}),  # +execution_arn.$ (I9247)
    # config#693 (L4595): pre-spend pipeline-contract preflight gate, wired
    # directly after LibPinDriftGate's pass-through (predictor-inference Lambda).
    "PipelineContractCheck": frozenset({"action", "run_date.$", "execution_arn.$"}),  # +run_date.$ (I8155), +execution_arn.$ (I9247)
    # config#2348: pre-spend evaluator Lambda-SHA drift gate pair, wired
    # directly after PipelineContractGate's pass-through. Two separate Lambda
    # invokes (grading, then director) — each checks its OWN :live alias's
    # baked GIT_SHA against origin/main independently.
    "EvaluatorDeployDriftCheck": frozenset({"action"}),
    "EvaluatorDirectorDeployDriftCheck": frozenset({"action"}),
    # config#1824 weekly run-day gate (pure calendar; mirrors LibPinDriftCheck shape).
    "WeeklyRunDayGate": frozenset({"action", "run_date.$", "execution_arn.$"}),  # +run_date.$ (I8155), +execution_arn.$ (I9247)
    "Scanner": frozenset({"dry_run_llm.$", "run_date.$"}),
    # alpha-engine-config-I7813: the same scanner Lambda, invoked as a
    # post-Director leaf with an explicit `mode` so it builds ONLY the
    # observe-only scanner/leaderboard board. The literal `mode` is what
    # keeps this payload distinct from Scanner's above.
    "ScannerLeaderboard": frozenset({"dry_run_llm.$", "run_date.$", "mode"}),
    "RegimeSubstrate": frozenset({"action.$", "run_date.$", "execution_arn.$"}),  # +run_date.$ (I8155), +execution_arn.$ (I9247)
    "RegimeRetrospectiveEval": frozenset({"action.$", "run_date.$", "execution_arn.$"}),  # +run_date.$ (I8155), +execution_arn.$ (I9247)
    # alpha-engine-config-I2515 Phase B: replaces the removed multi-agent
    # Research state as the signals.json producer.
    # config-I2916: preflight.$=$.research_dry threads the Friday-PM shell-run
    # signal so the signals-envelope Lambda downgrades its I2880 universe-board
    # fallback-staleness guard to a WARN (the dry Scanner leaves the dated board
    # absent every Friday). DISTINCT from dry_run_llm — the read/build/write
    # path still runs; only the expected-stale-fallback bound is relaxed.
    "SignalsEnvelope": frozenset({"run_date.$", "target", "preflight.$"}),
    # alpha-engine-config-I2515 Phase B: keeps the no_agent champion-baseline
    # shadow alive for the producer leaderboard post graph-runner removal.
    "ChallengerShadow": frozenset({"mode", "date.$"}),
    # alpha-engine-config-I7726 — same research-runner Lambda, different mode.
    "ResearchSelfTest": frozenset({"mode", "date.$"}),
    "EvalJudgeSubmitFirstSaturday": frozenset(
        {"date.$", "dry_run_llm.$", "force_sonnet_pass", "capture_lookback_days",
         "run_date.$"}
    ),
    "EvalJudgeSubmitWeekly": frozenset(
        {"date.$", "dry_run_llm.$", "force_sonnet_pass", "capture_lookback_days"}
    ),
    # alpha-engine-config-I9329: EvalJudgePoll and EvalJudgeProcess both left
    # this registry, for different reasons. Poll was DELETED with the provider
    # batch API it existed to drive (-I9263). Process still exists and keeps
    # its name, but it is no longer a lambda:invoke at all — it is an
    # ssm:sendCommand, so it has no Payload for this registry to close over;
    # its command string is pinned by tests/test_sf_eval_judge_wiring.py
    # instead. The dispatcher that launches its box is the new Lambda here.
    "DispatchEvalJudgeSpot": frozenset(
        {"execution_id.$", "run_date.$", "pipeline_role", "force_on_demand.$"}
    ),
    # alpha-engine-config-I10194 §1: `run_date.$` was ADDED to these five
    # states. All five are crucible-research Lambda handlers that derive their
    # own run_date for the krepis.stage_coverage verdict from
    # `end_time_iso` — the RAW calendar `$$.Execution.StartTime` — because no
    # Payload carried `$.run_date` at all. The two differ on any weekend
    # cycle, so the verdict lands under a plausible-looking but wrong prefix
    # (the I8155 class, at the Lambda half). `end_time_iso` STAYS: it is the
    # trailing analysis window's right edge, a different question.
    #
    # This half is INERT until crucible-research's handlers prefer
    # `event.get("run_date")` (I10194 §1 step (2), a separate PR) — the
    # handlers ignore the extra key today, which is what makes it safe to
    # merge first.
    "EvalRollingMean": frozenset({"end_time_iso.$", "run_date.$"}),
    "RationaleClustering": frozenset(
        {"dry_run_llm.$", "end_time_iso.$", "run_date.$"}
    ),
    # ReplayConcordance LEFT this table under alpha-engine-config-I10539
    # (Brian ruling 2026-09-16, option b) with the state itself — an entry
    # naming a state this walk cannot reach exempts nothing while reading
    # as coverage.
    "Counterfactual": frozenset(
        {"dry_run_llm.$", "end_time_iso.$", "max_depth", "window_days", "run_date.$"}
    ),
    # `coverage` (config-I7179) is the fan-in declaration: which stages must
    # have produced a cost record by the time the aggregator reads
    # _cost_raw/{date}/. It is a nested object rather than a flat key because
    # the denominator is a SET per stage — the defect it detects (every record
    # in the prefix coming from one producer that is no longer in this
    # pipeline) is invisible to any count.
    "AggregateCosts": frozenset({"date.$", "dry_run_llm.$", "coverage"}),
    # Evaluator Report Card v2 (Layer B) — alpha-engine-evaluator:live. Builds
    # evaluator/{date}/report_card.json; non-fatal (own Catch → notify gate).
    # dry_run.$=$.research_dry → no-write on the Friday preflight (ROADMAP L4504).
    # alpha-engine-config-I7282: `gate_state` — the run's correctness-gate
    # verdicts (sf-pipeline-policy.md 2.3a rule 3). A nested object rather than
    # flat keys because it is a VERSIONED CROSS-REPO CONTRACT with
    # crucible-evaluator (grading/pipeline_gates.py); its internal shape is
    # pinned by infrastructure/contracts/sf_gate_state.v1.schema.json and by
    # tests/test_sf_gate_state_wiring.py, not by this namespace registry.
    # RunScope (alpha-engine-config-I7620) — alpha-engine-weekly-run-scope.
    # Derives THIS execution's scope and writes backtest/{date}/run_scope.json.
    # The three context values are the whole point: it cannot fetch its own
    # history or definition without Execution.Id / StateMachine.Id, and
    # Execution.Input carries the run's skip_* flags, read ONLY to explain a
    # NOT_REACHED row (a disposition is always decided by the execution record,
    # never by what the input asked for). dry_run.$=$.research_dry — the
    # Friday-PM shell run derives everything and writes nothing.
    "RunScope": frozenset({
        "run_date.$", "dry_run.$", "execution_arn.$", "state_machine_arn.$",
        "execution_input.$",
    }),
    # alpha-engine-config-I7392: `run_scope` is the run's OWN scope, threaded
    # in-band from $.run_scope_result.Payload (the RunScope Task immediately
    # upstream) and seeded at the InitializeInput floor. It was previously
    # delivered ONLY as backtest/{date}/run_scope.json — the one delivery path a
    # rehearsal is forbidden to write, since the RunScope Lambda skips its
    # put_object on dry_run. Consumer: crucible-evaluator
    # grading/artifacts.py::_read_run_scope (in-band first, S3 as the fallback).
    "ReportCard": frozenset({
        "date.$", "dry_run.$", "snapshot", "gate_state", "run_scope.$",
    }),
    # Director (Layer C, Part II) — alpha-engine-evaluator-director:live. Final
    # advisory task; reads the fresh report card, writes director/{date}/
    # action_plan.json; flag-gated (DIRECTOR_ENABLED) + non-fatal (own Catch).
    # dry_run.$=$.research_dry → no-Opus / no-write probe on the preflight (L4504).
    # alpha-engine-config-I7282: same `gate_state` block as ReportCard, byte-
    # identical (one contract, one consumer implementation).
    "Director": frozenset({"date.$", "dry_run.$", "gate_state"}),
    # config#2248: launches the launcher spot that replaces the always-on
    # dashboard box as the $.ec2_instance_id source. It reads the rest of its
    # config from Lambda env vars.
    # config#5504: per-run identity threading for cost attribution.
    # config-I7120: force_on_demand on the FIRST launch too. This one box is the
    # shared substrate all 13 stage-liveness gates address via $.ec2_instance_id;
    # config-I7119's SubstrateRelaunchGate recovers only the 8 top-level sites,
    # and the 5 inside ResearchPredictorParallel are unreachable from it by ASL
    # scoping. Removing the reclaim is the only measure covering all 13.
    # alpha-engine-config-I8155: run_date.$ added — this Lambda's stage-
    # coverage verdict (DispatchWeeklyFreshnessSpot / RelaunchWeeklyFreshnessSpot,
    # both INFRASTRUCTURE/GATE stages) had been writing under an empty
    # run_date since I7214 shipped, because neither Payload carried one.
    # alpha-engine-config-I10172: sf_stage added — a Payload literal naming
    # each Task's OWN identity. Both callers pass force_on_demand: true (see
    # comments above and on RelaunchWeeklyFreshnessSpot below), so the
    # boolean no longer distinguishes them: the handler's old
    # force_on_demand-derived stage name always resolved to
    # RelaunchWeeklyFreshnessSpot, and this state's own invocations wrote
    # their coverage verdict under the OTHER state's name —
    # DispatchWeeklyFreshnessSpot had zero verdicts in any partition, ever
    # (measured 2026-09-08).
    "DispatchWeeklyFreshnessSpot": frozenset(
        {"execution_id.$", "run_date.$", "force_on_demand", "sf_stage"}
    ),
    # config-I7119: the SAME dispatcher, invoked to replace a launcher box that
    # was reclaimed mid-run. force_on_demand was added to the dispatcher in
    # config#2248 and documented there as "reserved for a future bounded
    # retry-on-relaunch ... no current caller sets it" — this state was the
    # first caller and config-I7120 made the initial dispatch the second, so the
    # two payloads are now identical. A literal `true`, not a `.$` path: the
    # decision is structural, never execution input.
    "RelaunchWeeklyFreshnessSpot": frozenset(
        {"execution_id.$", "run_date.$", "force_on_demand", "sf_stage"}
    ),
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
# alpha-engine-config-I2717/I2722 (2026-07-16): PredictorHealthCheck,
# PredictorDriftCheck, and the WaitForChronicGap liveness-poll entry were
# REMOVED from this SF entirely (heal -> standalone daily job; health/drift
# checks -> their own direct EventBridge triggers, see
# infrastructure/cloudformation/alpha-engine-orchestration.yaml). Removing
# their registry entries here is the deliberate drift-direction check this
# registry pattern enforces (test_no_registry_entry_missing_from_sf below).
_WEEKDAY_PAYLOAD_KEYS: dict[str, frozenset[str]] = {
    "DeployDriftCheck": frozenset({"action"}),
    # config#1430: NYSE trading-day gate, moved OFF the box into the
    # predictor-inference Lambda and run BEFORE StartExecutorEC2 (replaces the
    # cold-box SSM trading_calendar check whose stdout was unreliably captured).
    "TradingDayGate": frozenset({"action"}),
    # alpha-engine-config-I7111: the NYSE market-hours boundary, the first
    # state of both trading pipelines. Unlike every other gate here it passes
    # two context fields as well as the action. `now.$` is
    # $$.Execution.StartTime, so the verdict is a property of the execution
    # rather than of the Lambda's clock — deterministic, replayable, and
    # immune to a slow cold start pushing a refused run past the close.
    # `execution_input.$` is $$.Execution.Input passed WHOLE, because an
    # "override.$": "$.market_hours_override" parameter would throw
    # States.Runtime on every run that carries no override, i.e. every
    # normal day.
    # `pipeline` is a LITERAL (no `.$`), and that is the whole point:
    # alpha-engine-config-I11384 lets the preopen chain — and ONLY the preopen
    # chain — reach PROCEED_REMEDIATION, so if this key could be read from the
    # execution input any caller could claim to be it. The literal-vs-path
    # distinction is asserted directly in
    # tests/test_sf_market_hours_gate_wiring.py::TestPipelineLiteralIsUnspoofable.
    "MarketHoursGate": frozenset(
        {"action", "now.$", "execution_input.$", "pipeline"}
    ),
    "PredictorInference": frozenset({"action"}),
    "CheckPredictorCoverage": frozenset({"action"}),
    "ReinvokePredictor": frozenset({"action", "tickers.$"}),
    "RecheckCoverage": frozenset({"action"}),
    # config#1811: liveness-aware poll loops that stayed on the trading box
    # (CodeFreshnessGate, RunMorningPlanner) share the ssm-liveness-poller
    # payload contract. WaitForMorningEnrich/WaitForMorningArcticAppend do NOT
    # appear here — config#1767 (Phase 2) relocated those two onto independent
    # ephemeral spot boxes whose own PollMorningEnrichSpot/
    # PollMorningArcticAppendSpot poll directly via ssm:getCommandInvocation (a
    # Task, not a lambda:invoke Payload), so they are out of scope for this
    # Lambda-Payload registry.
    "WaitForCodeFreshness": _LIVENESS_POLLER_KEYS,
    "WaitForMorningPlanner": _LIVENESS_POLLER_KEYS,
    # alpha-engine-config-I9466: the sf-pipeline-policy 2.3a correctness-verdict
    # gate runs on the trading box (the SF role's s3:GetObject is scoped to three
    # keys and grants nothing on backtest/ or config/, so a native
    # aws-sdk:s3:getObject read would need an IAM change in another repo), and so
    # it polls through the same ssm-liveness-poller contract as the two above.
    "WaitForCorrectnessVerdict": _LIVENESS_POLLER_KEYS,
    # config#1767 (Phase 2): the data phase (enrich + Arctic append) was relocated
    # onto two independent ephemeral spot boxes via the alpha-engine-data-spot-
    # dispatcher Lambda. Each launch state passes {"workload": <key>,
    # "force_on_demand.$"} selecting the collector invocation + threading the
    # config#2542 retry-budget's on-demand override; the dispatcher returns
    # {data_spot:{launched,instance_id,...}}.
    # config#5504: execution_id.$ threads $$.Execution.Id into the dispatcher
    # payload so the spot box carries per-run identity tags for cost attribution.
    "LaunchMorningEnrichSpot": frozenset({"workload", "force_on_demand.$", "execution_id.$"}),
    "LaunchMorningArcticAppendSpot": frozenset({"workload", "force_on_demand.$", "execution_id.$"}),
    # alpha-engine-config-I7811 (Brian ruling 2026-08-20): the weekday Scanner
    # entry was REMOVED with the state. The scanner forms its two cuts WEEKLY,
    # on the Saturday pipeline, whose own "Scanner" payload registry entry above
    # is unaffected.
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


# TestSFRoleInvokeFunctionStatementCount (exactly one lambda:InvokeFunction
# Statement) was ported to nous-ergon-ops — the SF role policy
# (infrastructure/iam/alpha-engine-step-functions-role.json) now lives there.
# The invariant (single statement preventing stale-grant drift) is enforced
# in nous-ergon-ops/tests/ per the infra/drop-iam-moved-to-ops cleanup.

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
            # alpha-engine-config#5950 — NormalizeEODFailureContext's scratch
            # key. HandleFailure formats States.JsonToString($.error), and three
            # inbound edges never set it (MarketHoursGateChoice's Default, and
            # PageCaptureSnapshotIrreversibleFailure's Next and Catch), so the
            # EOD pipeline's own failure REPORTER raised States.Runtime on those
            # paths and the run died carrying the reporting error instead of the
            # real one. The floor uses the same JsonMerge-into-$.merged +
            # OutputPath idiom as the weekly SF's NormalizeFailureContext, so
            # $.merged is transient: it exists only INSIDE that one Pass and is
            # re-rooted away before any successor sees it. Registered here
            # because the closed-namespace scan reads the definition text, not
            # the runtime payload.
            "merged",
            # Intermediate ResultPath outputs
            # alpha-engine-config-I6891: WriteCompletionMarkerDegraded's
            # putObject result. It needs a ResultPath at all because the
            # default is $, which replaces the state input — so the
            # DegradedRun terminal's $.degraded_summary.reason dereference
            # could not resolve and the run failed States.Runtime instead of
            # DegradedRun. Named to be read by nothing.
            "degraded_marker_result",
            # alpha-engine-config-I7111 — the MarketHoursGate namespace. The
            # gate's own ResultPath ($.market_hours_gate) and its Catch's
            # ($.market_hours_gate_error) are the two load-bearing ones: the
            # first is what MarketHoursGateChoice keys on and what the notify
            # states render; the second is what
            # SetMarketHoursUnverifiedDegraded threads into
            # $.degraded_summary.stage_error. The four *_notify paths are
            # sns:publish results, named to be read by nothing (same
            # convention as degraded_marker_result above) — they exist only so
            # a publish result cannot replace the state input the way the
            # I6891 incident did.
            "market_hours_gate",
            "market_hours_gate_error",
            "market_hours_blocked_notify",
            "market_hours_notify_error",
            "market_hours_override_malformed_notify",
            "market_hours_override_notify",
            "market_hours_unverified_notify",
            # alpha-engine-config-I8102 — the DeployDriftCheck namespace.
            # $.drift_result is the probe payload DeployDriftGate keys on;
            # $.drift_error is the Catch path's, kept SEPARATE because on that
            # path there is no Payload and a Pass reading $.drift_result.Payload
            # would raise States.Runtime. $.deploy_drift_degraded_notify is the
            # sns:publish result, named to be read by nothing (same convention
            # as degraded_marker_result above).
            "drift_result",
            "drift_error",
            "deploy_drift_degraded_notify",
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
            # 2026-07-14 incident fix: bounded (1x) relaunch-on-a-fresh-box
            # retry for a spot-reclaimed data-spot workload, plus a distinct
            # loud skip of EODReconcile when the retry is exhausted (today's
            # SPY close genuinely never landed in ArcticDB, so eod_reconcile.py's
            # _spy_close hard-fail would otherwise be guaranteed).
            "data_spot_retry",
            "data_spot_arctic_retry",
            # alpha-engine-config-I10750: the edgar-pit-fundamentals-daily leg
            # added after post-market-arctic-append — same launch/poll/retry
            # ResultPath shape as the two legs above.
            "edgar_pit_launch",
            "edgar_pit_poll",
            "data_spot_edgar_retry",
            "eod_skip_notify",
            "snapshot_poll",
            "snapshot_result",
            "stop_result",
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
            # alpha-engine-config-I2722 (2026-07-16): skip_daily_substrate_health_check
            # + the whole DailySubstrateHealthCheck chain (and its dedicated
            # fail-notify fields, health_check_degraded /
            # substrate_health_check_degraded_notify[_error] / substrate_check_*)
            # were REMOVED — the check re-homed to a standalone dashboard-box
            # systemd timer (crucible-dashboard), genuinely consumer-free
            # within this SF. Per-row CloudWatch alarms carry the alerting
            # independently of the SF.
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
            # config-I2702 (2026-07-15): closed-loop self-heal for post-close
            # data gaps. "run_date" is a PRE-EXISTING top-level input field
            # (used since day one, embedded inside States.Format() command
            # strings like EODReconcile's) that only now gets a BARE `"$.
            # run_date"` reference — the regex above only matches strings that
            # START with `$.`, and Lambda Payload fields
            # (ProbeEODReconcilePrecondition, HealReProbe, HealDispatchReplay's
            # Input) are the first place it's referenced that way.
            "run_date",
            # ProbeEODReconcilePrecondition (deliverable #1): fresh verify-by-
            # artifact read of the macro-freshness sentinel, replacing the old
            # $.data_spot_error flag test at CheckSkipEODReconcile. Re-emitted
            # (overwritten) by HealReProbe inside the heal loop below.
            "precondition_probe",
            # SetDegradedFlag (deliverable #4): persistent flag read by
            # CheckDegradedOutcome (after the StopTradingInstance cost-guard
            # tail) to route to the distinct DegradedSucceeded terminal instead
            # of NormalSucceeded.
            "degraded_summary",
            # The closed self-heal loop (deliverable #3): InitHealLoop /
            # HealLoopIncrement carry the attempts counter; each heal
            # iteration's data-spot dispatch + poll emits its own launch/poll/
            # error ResultPath (mirrors the pre-existing postmarket_launch /
            # postmarket_poll / data_spot_error naming, prefixed heal_ to keep
            # the original phase's fields untouched); HealDispatchReplay emits
            # the auto-replay StartExecution result (or its Catch error); the
            # three outcome notifications (converged / replay-dispatch-failed /
            # non-convergent) each emit their own SNS ResultPath.
            "heal_loop",
            "heal_postmarket_launch",
            "heal_postmarket_poll",
            "heal_arctic_launch",
            "heal_arctic_poll",
            "heal_error",
            "heal_replay_dispatch",
            "heal_replay_dispatch_error",
            "heal_replay_dispatch_failed_notify",
            "heal_converged_notify",
            "heal_nonconvergent_notify",
            # config-I5489: postclose chains the weekly pipeline as an
            # "exercise" run on every trading day. LaunchWeeklyExerciseRun
            # emits the fire-and-forget StartExecution result (or its Catch
            # error); WeeklyExerciseLaunchFailed emits the alert's SNS
            # ResultPath (and its own Catch error, so a failure to alert
            # about a failure to launch still cannot strand the execution).
            "weekly_exercise_run",
            "weekly_exercise_launch_error",
            "weekly_exercise_launch_notify",
            "weekly_exercise_launch_notify_error",
            # alpha-engine-config-I6689: ReadExerciseCadence reads the
            # declared cadence from SSM (source: infrastructure/
            # weekly_cadence.json) and gates LaunchWeeklyExerciseRun via
            # CheckExerciseCadence, so daily<->weekly-only<->off is a
            # one-line manifest diff instead of an SF-topology edit.
            # $.exercise_cadence_param is both the Task's ResultPath AND
            # the value SetCadenceReadDegraded / SetCadenceUnknownValueDegraded
            # float, so all three fail-open paths (read failure, unrecognized
            # value) agree on one field name for CheckExerciseCadence to read.
            "exercise_cadence_param",
            "exercise_cadence_read_error",
            "exercise_cadence_degraded_notify",
            "exercise_cadence_degraded_notify_error",
            "exercise_cadence_unknown_notify",
            "exercise_cadence_unknown_notify_error",
            # alpha-engine-config#5569 (2026-08-09): bounded 1-retry same-day
            # budget for CaptureSnapshot, the EOD pipeline's only stage with an
            # irreversible per-day deadline. InitCaptureSnapshotRetryCounter /
            # IncrementCaptureSnapshotRetry carry the attempts counter;
            # PageCaptureSnapshotFailureImmediate emits its SNS ResultPath on
            # the FIRST failure (before the retry runs); PageCaptureSnapshot-
            # IrreversibleFailure emits its own distinct SNS ResultPath once the
            # retry budget is exhausted. $.error is reused (already registered
            # above) by CaptureSnapshotRetryExhausted's normalizer.
            "capture_snapshot_retry",
            "capture_snapshot_page_notify",
            "capture_snapshot_irreversible_notify",
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
# 10 → 9 on alpha-engine-config-I2545 (2026-07-14): ModelZooSelect (the last
# remaining flat-level model-zoo spot launcher) moved to the new Sunday-
# triggered ne-modelzoo-sunday-pipeline child SF (step_function_modelzoo.json).
# 9 → 10 on alpha-engine-config-I2890 (2026-07-17): the I2544/I2545 splits were
# REVERSED — ModelZooSelect is back inline in Branch B (the Sunday child SF and
# the advisory child SF are retired; the weekly SF runs the full pre-split
# pattern again, all-Saturday).
# 10 → 11 on alpha-engine-config-I5759 (2026-07-31): DataPhase2 was
# repointed from a lambda:invoke to the spot dispatch->poll quartet. Its
# wall clock is a provider-imposed serial floor (2 Finnhub calls/ticker x
# a 1.1s sleep held inside a module-global lock = 2.2 s/ticker, measured
# flat at 903 tickers), so ~33 min against Lambda's 900s HARD maximum —
# a ceiling no further bump can raise.
# 11 → 14 on alpha-engine-config#6030 (2026-08-09): the bundled Parity spot
# state was split into a ParityParallel of three fail-open branch spots
# (PitParityLookahead / PitParityWalkforward / ParityReplay) plus the
# PitParityCompare join spot — net +3 flat-level spot launchers (the three
# branches ARE descended into by _flatten_states, same as DataPhase2's
# Branch A siblings).
# 14 → 15 on alpha-engine-config-I3112 deliverable 3 (2026-08-11): the single
# Evaluator spot state was split into EvaluatorDiagnostics -> EvaluatorOptimize,
# each dispatching its own spot via spot_evaluator.sh --eval-half=. The extra
# launcher is a KNOWN, measured cost and not an oversight: _spot_common.sh
# terminates its instance in a trap with no keep-alive or attach path, so two
# SF states means two boots. Measured at ~200s per boot against a ~4h20m
# pipeline (the 2026-08-08 succeeded run put the whole merged Evaluator stage
# at 482s, of which evaluate.py was 282s). Reusing one box would mean weakening
# a termination trap every stage depends on, to save ~200s.
_EXPECTED_SATURDAY_SPOT_STATE_COUNT = 15


def _spot_states(sf_path: Path) -> list[str]:
    """Find every Task state whose `commands` contains
    `bash infrastructure/spot_*.sh` — these are the spot-instance
    launchers."""
    sf = json.loads(sf_path.read_text())
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
    """Closes the spot-state set at the declared count (see the count-history
    changelog comment above `_EXPECTED_SATURDAY_SPOT_STATE_COUNT`).
    Pre-rewire test_sf_friday_shell_run_wiring.py parametrizes over the
    expected names but doesn't assert an EXACT count — an orphaned legacy
    spot state from an incomplete refactor would slip through.
    """

    def test_spot_state_count_is_exactly_the_declared_count(self):
        spots = _spot_states(_SF_SATURDAY)
        assert len(spots) == _EXPECTED_SATURDAY_SPOT_STATE_COUNT, (
            f"Saturday SF should have EXACTLY "
            f"{_EXPECTED_SATURDAY_SPOT_STATE_COUNT} spot-launching states; "
            f"found {len(spots)}: {sorted(spots)}. Either an orphaned "
            "legacy state slipped through an incomplete refactor or a "
            "deliberate spot-state addition needs the test bump."
        )


# alpha-engine-config-I2890 (2026-07-17): the ModelZoo Sunday child SF was
# retired (I2544/I2545 splits reversed) — ModelZooSelect is counted in the
# Saturday census above; no separate child-SF census remains.
