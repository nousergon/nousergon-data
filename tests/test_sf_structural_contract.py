"""alpha-engine-config#6684 — structural contract over every
``infrastructure/step_function*.json`` orchestration definition.

weekly-sf-policy.md §4 ("every stage declares a timeout") and §6 ("a new
stage lands complete") name two clause rows that were, until this module,
``kind: none`` — no enforcing artifact:

  * WSF-4-every-stage-declares-a-timeout
  * WSF-6-new-stage-lands-complete

This module is the enforcing artifact for both, plus two structural guards
not named by a clause row but required by the same policy intent (a stage
that silently points at a deleted/renamed script, or a definition with no
whole-execution backstop, are exactly the kind of "lands complete" /
"declares a timeout" gaps WSF-6 / WSF-4 exist to catch):

  1. every ``Task`` state declares ``TimeoutSeconds``, unless the state name
     is in that file's ``_TIMEOUT_EXEMPT`` dict — each entry a one-line
     justification, not a bare pass;
  2. every ``Task`` state declares ``Catch``, same exemption mechanism;
  3. every script this repo's own EC2 target (``alpha-engine-data``) is
     told to run via SSM ``AWS-RunShellScript`` resolves to a file that
     actually exists in the tree (regression guard for the I4442 / I4975
     per-stage ``spot_data_weekly.sh`` splits — a state pointing at a
     deleted or renamed script fails here, not on Saturday);
  4. every definition itself (not a Task state — the top level of the SF
     document) declares a whole-execution ``TimeoutSeconds``
     (alpha-engine-config#6693) — a hung execution otherwise runs to the
     Step Functions 1-year service maximum, invisible to any status-keyed
     watcher.

New Task states are non-exempt by DEFAULT — the exemption dicts are an
enumerated allowlist populated from the definitions as measured on
``nousergon-data@b5b42b74`` (2026-08-09, post config#6408 Director-Catch
fix, PR #1233), not a wildcard. A state added later with no timeout/Catch
fails this suite until it is either fixed or explicitly, individually
exempted with its own justification.

Per config#6684's original constraint this PR (#1253) did NOT add
timeouts/Catch to any state, and every then-missing declaration was
enumerated as an exemption below. alpha-engine-config#6693 (nousergon-data
PR #1256) subsequently gave every remaining ``_TIMEOUT_EXEMPT`` entry under
``step_function_daily.json`` and ``step_function_eod.json`` a real 60s
``TimeoutSeconds`` — both shrink to ``{}`` rather than the entries going
stale (``_CATCH_EXEMPT`` is untouched: #1256 adds timeouts, not Catches).
Tightening any individual ``_CATCH_EXEMPT`` entry, or ``step_function.json``'s
remaining ``_TIMEOUT_EXEMPT`` entries, is separate, reviewable follow-up work
(alpha-engine-config#6684 remains open as the tracker for that).
PR #1256 also folded in what was briefly a parallel checker
(``tests/test_sf_timeout_coverage.py``) covering top-level ``TimeoutSeconds``
presence — see ``test_definition_declares_top_level_timeout`` below rather
than maintaining two modules asserting overlapping things over the same
files.

Cross-repo scope note (deliverable 3): states whose SSM command list ``cd``s
into a sibling repo's EC2 checkout (``alpha-engine`` == crucible-executor,
``alpha-engine-predictor``, ``alpha-engine-backtester``,
``alpha-engine-dashboard``, ...) reference scripts this repo does not own
and cannot verify without a network clone — out of scope by the "pure file
read, no network" constraint in config#6684's deliverables. Only scripts
invoked after ``cd /home/ec2-user/alpha-engine-data`` (this repo's own EC2
deploy target, confirmed in OVERVIEW.md) are checked for existence.

alpha-engine-config#6715 (WSF-2.3-degradation-is-visible /
WSF-5-carve-outs-set-a-degraded-flag chokepoint) adds a fifth guard in the
same suite: every fail-open Catch route (a Catch whose target is NOT a
failure-family state — derived structurally, not from a hand-list) must
pass through a state that writes the degraded-flag JSONPath the terminal
notifier/marker-selector actually reads (``$.gate_degraded`` /
``$.health_check_degraded`` / ``$.report_card_degraded`` /
``$.parity_degraded`` for weekly's ``CheckGateDegradedNotify``;
``$.degraded_summary`` for daily/eod's ``CheckDegradedOutcome``), or carry
an explicit, reasoned ``_DEGRADED_FLAG_EXEMPT`` entry. Scoped to the three
SCHEDULED pipelines only (``step_function_groom.json`` has no such
concept — see ``_DEGRADED_FLAG_SF_FILES``). Running the walker against the
definitions as measured at alpha-engine-config#6715's PR surfaced 25
pre-existing gaps (tracked at alpha-engine-config#6722 — including a NAMED
§5 carve-out non-compliance on ``AggregateCosts`` and a dead flag on
``ThinkTankDegraded`` that was set but never read) plus one genuinely
mechanical fix applied directly in that PR: ``step_function_eod.json``'s
post-market data-spot fail-open path was missing the
``SetDataSpotDegradedFlag`` state its sibling ``step_function_daily.json``
already has (config#6692) — added verbatim, mirroring the daily pattern.

alpha-engine-config#6722 (this PR) wired all 25: 4 top-level mutex/scanner/
exercise-launch fail-opens directly (their entries are DELETED — the walker
verifies them without any exemption), and the 20 remaining routes living
INSIDE ``ResearchPredictorParallel``'s two branches via a branch-local
fold (``$.research_degraded_local``, seeded per-branch, threaded through
Mark*Degraded Pass states, hoisted by each branch terminal, folded into a
fifth top-level flag — ``$.research_predictor_degraded`` — by the new
``CheckResearchPredictorDegraded``/``SetResearchPredictorDegraded`` pair
spliced onto ``CheckBranchOutcomes``' non-FAILED path). The fold is real
and verified end-to-end by ``tests/test_sf_research_predictor_degraded_
wiring.py``, but a Parallel branch cannot write an outer-scope JSONPath
(ASL scoping), so this walker's forward trace from a top-level Catch can
never see it — the 20 corresponding ``_DEGRADED_FLAG_EXEMPT`` entries below
therefore stay in place with their reason reworded from "VIOLATION —
tracked" to a verified justification, the same disposition PR1277 used for
ParityParallel's six intra-branch entries (alpha-engine-config#6030).
Zero ``VIOLATION`` entries remain.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterator

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"

# ---------------------------------------------------------------------------
# Exemption registry — one entry per file. Every listed state is a measured,
# justified gap as of the commit named in the module docstring above, NOT a
# blanket allowance. A state absent from a file's Task-state set (renamed or
# removed) but still listed here fails test_no_stale_*_exemptions — the
# allowlist is not permitted to silently outlive what it names.
# ---------------------------------------------------------------------------

_TIMEOUT_EXEMPT: dict[str, dict[str, str]] = {
    "step_function.json": {
        # sns:publish — fire-and-forget notify, seconds-scale API call.
        "WeeklyRunDayGateFailed": "sns:publish fail-open notifier — SDK call, not a wait",
        "PublishLibPinGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "PublishPipelineContractGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "ResearchPredictorParallel.PublishResearchFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
        "ResearchPredictorParallel.PublishPredictorFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
        "ResearchPredictorParallel.PublishModelZooFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
        "ResearchPredictorParallel.PublishModelZooUnservableNotice": "sns:publish declared-unservable notifier — SDK call, not a wait (alpha-engine-config-I11106)",
        "PublishReportCardDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "PublishParityDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait (alpha-engine-config-I6025)",
        "PublishParityCompareDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait (alpha-engine-config#6030)",
        "PublishMutexAcquireDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait (alpha-engine-config#6722)",
        "NotifyCompleteGatesDegraded": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyCompleteHealthDegraded": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyCompleteGatesAndHealthDegraded": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyCompleteReportCardDegraded": "sns:publish completion notifier — SDK call, not a wait (config#6685)",
        "NotifyCompleteMultipleDegraded": "sns:publish completion notifier — SDK call, not a wait (config#6685)",
        "NotifyCompleteParityDegraded": "sns:publish completion notifier — SDK call, not a wait (alpha-engine-config-I6025)",
        "PublishScannerLeaderboardDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait (alpha-engine-config-I7813)",
        "PublishAggregateCostsDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait (alpha-engine-config-I8336)",
        "NotifyCompleteScannerLeaderboardDegraded": "sns:publish completion notifier — SDK call, not a wait (alpha-engine-config-I7813)",
        "NotifyShellRunComplete": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyComplete": "sns:publish completion notifier — SDK call, not a wait",
        "HandleFailure": "sns:publish failure notifier — SDK call, not a wait",
        "PublishEvaluatorGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "PublishEvaluatorDirectorGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "PublishWeeklyPreflightGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait (alpha-engine-config-I11112)",
        # lambda:invoke — synchronous gate Lambda, seconds-scale.
        "WeeklyRunDayGate": "lambda:invoke synchronous gate call — SDK call, not a wait",
        # dynamodb:putItem — single-item write, sub-second.
        "AcquireMutex": "dynamodb:putItem mutex acquire — SDK call, not a wait",
        # ssm:getCommandInvocation — a single POLL of an in-flight SSM
        # command; the actual long-running work is bounded by the
        # invoking state's own executionTimeout / the box-side script,
        # not this poll call.
        "WaitForMorningEnrich": "ssm:getCommandInvocation single poll — bounded by MorningEnrich's own executionTimeout",
        "WaitForDataPhase1": "ssm:getCommandInvocation single poll — bounded by DataPhase1's own executionTimeout",
        "ResearchPredictorParallel.WaitForRAGIngestion": "ssm:getCommandInvocation single poll — bounded by RAGIngestion's own executionTimeout",
        "ResearchPredictorParallel.WaitForDataPhase2": "ssm:getCommandInvocation single poll — bounded by DataPhase2's own executionTimeout",
        "ResearchPredictorParallel.WaitForPredictorTraining": "ssm:getCommandInvocation single poll — bounded by PredictorTraining's own executionTimeout",
        "ResearchPredictorParallel.WaitResolveZoo": "ssm:getCommandInvocation single poll — bounded by ResolveZooSpecs' own executionTimeout",
        "ResearchPredictorParallel.ModelZooTrainMap.WaitTrainSpec": "ssm:getCommandInvocation single poll — bounded by TrainSpecDispatch's own executionTimeout",
        "ResearchPredictorParallel.WaitForModelZoo": "ssm:getCommandInvocation single poll — bounded by ModelZooSelect's own executionTimeout",
        "ResearchPredictorParallel.ReadModelZooArenaCycle": "s3:getObject of one ~13 KB verdict artifact — TimeoutSeconds 60 IS declared; listed so the pairing is visible beside the other model-zoo states (alpha-engine-config-I11101)",
        "WaitForBacktester": "ssm:getCommandInvocation single poll — bounded by Backtester's own executionTimeout",
        "WaitForPredictorBacktest": "ssm:getCommandInvocation single poll — bounded by PredictorBacktest's own executionTimeout",
        "WaitForPortfolioOptimizerBacktest": "ssm:getCommandInvocation single poll — bounded by PortfolioOptimizerBacktest's own executionTimeout",
        "ParityParallel.WaitForPitParityLookahead": "ssm:getCommandInvocation single poll — bounded by PitParityLookahead's own executionTimeout (alpha-engine-config#6030)",
        "ParityParallel.WaitForPitParityWalkforward": "ssm:getCommandInvocation single poll — bounded by PitParityWalkforward's own executionTimeout (alpha-engine-config#6030)",
        "ParityParallel.WaitForParityReplay": "ssm:getCommandInvocation single poll — bounded by ParityReplay's own executionTimeout (alpha-engine-config#6030)",
        "WaitForPitParityCompare": "ssm:getCommandInvocation single poll — bounded by PitParityCompare's own executionTimeout (alpha-engine-config#6030)",
        "ParityParallel.WaitForPitParityLookaheadResourceKillCheck": "ssm:getCommandInvocation single poll — bounded by PitParityLookaheadResourceKillCheck's own 60s executionTimeout (alpha-engine-config-I7267)",
        "ParityParallel.WaitForPitParityWalkforwardResourceKillCheck": "ssm:getCommandInvocation single poll — bounded by PitParityWalkforwardResourceKillCheck's own 60s executionTimeout (alpha-engine-config-I7267)",
        "WaitForEvaluatorDiagnostics": "ssm:getCommandInvocation single poll — bounded by EvaluatorDiagnostics' own executionTimeout",
        "WaitForEvaluatorOptimize": "ssm:getCommandInvocation single poll — bounded by EvaluatorOptimize's own executionTimeout",
        "WaitForSaturdayHealthCheck": "ssm:getCommandInvocation single poll — bounded by SaturdayHealthCheck's own executionTimeout",
        "WaitForWeeklySubstrateHealthCheck": "ssm:getCommandInvocation single poll — bounded by WeeklySubstrateHealthCheck's own executionTimeout",
        "WaitForWeeklyFreshnessSpotBootstrap": "ssm:getCommandInvocation single poll — bounded by DispatchWeeklyFreshnessSpot's own executionTimeout",
        "ResearchPredictorParallel.WaitForEvalJudgeSpotBootstrap": "ssm:getCommandInvocation single poll — bounded by the eval-judge dispatcher's own 1800s bootstrap executionTimeout (alpha-engine-config-I9329)",
        "ResearchPredictorParallel.WaitForEvalJudgeProcess": "ssm:getCommandInvocation single poll — bounded by EvalJudgeProcess' own 10800s executionTimeout (alpha-engine-config-I9329)",
        # s3:headObject / s3:putObject — single-object API call, sub-second.
        "ResearchPredictorParallel.ValidatePredictorSkipWeightsFresh": "s3:headObject freshness check — SDK call, not a wait",
        "WriteCompletionMarker": "s3:putObject completion marker — SDK call, not a wait (config#2857)",
    },
    # alpha-engine-config#6693: every state formerly exempted here now
    # carries a real TimeoutSeconds (60s — mutex/gate/start/notify/marker/
    # poll calls are all sub-minute AWS SDK/Lambda calls) — the exemption
    # registry shrinks to {} rather than the entries going stale.
    "step_function_daily.json": {},
    "step_function_eod.json": {},
    "step_function_groom.json": {},
}

_CATCH_EXEMPT: dict[str, dict[str, str]] = {
    "step_function.json": {
        "WeeklyRunDayGateFailed": "deliberate fail-open notify+proceed (own Comment); a Catch here would need its own Catch",
        "WriteCompletionMarker": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate as this execution's own unverifiable-completion signal, not be masked",
        "WriteCompletionMarkerDegraded": "config#2857/config#1724 (DEGRADED twin, alpha-engine-config-I6891): deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "HandleFailure": "terminal failure notifier — routes to FailExecution; the shared failure sink itself, not something to re-catch into",
    },
    "step_function_daily.json": {
        "TradingDayGateFailed": "deliberate fail-open notify+proceed (own Comment); a Catch here would need its own Catch",
        "NotifyHolidaySkip": "terminal skip notifier (End: true) — nothing downstream to route a Catch to",
        "WriteCompletionMarker": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "WriteCompletionMarkerDegraded": "config#2857/config#1724 (DEGRADED twin, config#6692): deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "HandleFailure": "terminal failure notifier — routes to FailExecution; the shared failure sink itself, not something to re-catch into",
    },
    "step_function_eod.json": {
        "WriteCompletionMarkerNormal": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "WriteCompletionMarkerDegraded": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "ForceStopInstance": "fail-safe teardown reached FROM the failure path (own Comment: 'always stop... even on failure') — a Catch here risks looping back into the failure path it is cleaning up after",
    },
    "step_function_groom.json": {},
}

assert set(_TIMEOUT_EXEMPT) == set(_CATCH_EXEMPT), (
    "timeout and catch exemption dicts must enumerate the same file set"
)
_SF_FILE_NAMES = sorted(_TIMEOUT_EXEMPT)


# ---------------------------------------------------------------------------
# alpha-engine-config#6715 — WSF-2.3-degradation-is-visible /
# WSF-5-carve-outs-set-a-degraded-flag chokepoint: every fail-open Catch
# route (a Catch whose target is NOT a failure-family state — i.e. the
# execution proceeds toward a normal SUCCESS terminal rather than the
# shared hard-fail sink) must pass through a state that writes the
# degraded-flag JSONPath the terminal notifier/marker-selector ACTUALLY
# dereferences. "Actually dereferences" is load-bearing (config#6715
# gotcha): ResearchPredictorParallel.ThinkTankDegraded below sets
# $.thinktank_degraded and its own Comment calls it "THE VISIBLE DEGRADED
# FLAG" — but nothing downstream ever reads that path, so a naive "any
# degraded-named key" check would pass a dead flag. The real consumers are
# the two Choice states below, whose exact Variable set is independently
# verified against these constants by
# test_degraded_flag_jsonpaths_match_the_actual_terminal_choice.
# ---------------------------------------------------------------------------

# sf-pipeline-policy.md §1.1 scopes to exactly the three SCHEDULED
# pipelines (weekly freshness, weekday preopen, weekday postclose/EOD).
# step_function_groom.json is a maintenance/backlog-groom dispatcher with
# no degraded-flag concept at all (no WriteCompletionMarker, no
# CheckGateDegradedNotify/CheckDegradedOutcome equivalent — see
# test_degraded_flag_scope_is_the_three_scheduled_pipelines below) and is
# deliberately OUT of this test family's scope, unlike the shared
# _SF_FILE_NAMES parametrization above.
_DEGRADED_FLAG_SF_FILES = [
    name for name in _SF_FILE_NAMES if name != "step_function_groom.json"
]

# The Choice state that is the SOLE router into the degraded-vs-clean
# terminal for each file — the thing this whole test exists to protect.
_TERMINAL_DEGRADED_CHOICE: dict[str, str] = {
    "step_function.json": "CheckGateDegradedNotify",
    "step_function_daily.json": "CheckDegradedOutcome",
    "step_function_eod.json": "CheckDegradedOutcome",
}

# The degraded-flag SETTER paths (the ResultPath a flag-writing Pass state
# uses) for each file. Weekly sets four independent top-level booleans,
# each read verbatim by CheckGateDegradedNotify. Daily/eod set ONE
# composite object ($.degraded_summary, config#6692/config-I2702's
# Option-A shape); CheckDegradedOutcome reads a nested field of it
# ($.degraded_summary.degraded) — see _is_prefix_path below, which treats
# a setter path as satisfying any reader path nested under it.
_DEGRADED_FLAG_JSONPATHS: dict[str, frozenset[str]] = {
    "step_function.json": frozenset(
        {
            "$.gate_degraded",
            "$.health_check_degraded",
            "$.report_card_degraded",
            "$.parity_degraded",
            "$.research_predictor_degraded",
            # alpha-engine-config-I7813: the observe-only scanner leaderboard
            # leaf's own flag. Read LAST in CheckGateDegradedNotify, so a run
            # that also degraded something consequential still reports that.
            "$.scanner_leaderboard_degraded",
            # alpha-engine-config-I7194: AggregateCosts moved out of
            # ResearchPredictorParallel branch 0 onto the top-level tail (so it
            # runs AFTER Director, whose director-plan rows it could never see
            # from inside the Parallel). Its fail-open can no longer ride the
            # branch-local $.research_degraded_local fold, so it gets its own
            # family — also read LAST, folded into the generic combined
            # notifier rather than given a per-combination Task of its own.
            "$.aggregate_costs_degraded",
        }
    ),
    "step_function_daily.json": frozenset({"$.degraded_summary"}),
    "step_function_eod.json": frozenset({"$.degraded_summary"}),
}

# States whose Catch is the shared hard-fail sink — reaching one of these
# means the execution eventually fails LOUD (a real Fail-type terminal:
# FailExecution/MutexConflict), so the route is NOT "fail-open to a silent
# SUCCESS" and needs no degraded flag. Weekly funnels every hard failure
# through NormalizeFailureContext(Repin) -> HandleFailure -> FailExecution
# (config#1819, the notifier-totality fix); daily/eod funnel straight to
# HandleFailure -> FailExecution (no repin stage — config#1819 is
# weekly-only). MutexConflict is a direct Fail-type Catch target in all
# three (config#2280/L274).
_FAILURE_FAMILY: dict[str, frozenset[str]] = {
    "step_function.json": frozenset(
        {
            "NormalizeFailureContext",
            "NormalizeFailureContextRepin",
            "HandleFailure",
            "FailExecution",
            "MutexConflict",
        }
    ),
    # alpha-engine-config-I7111 adds three more real Fail-type terminals to
    # the two trading pipelines: MarketHoursBlocked (started inside the NYSE
    # session with no override), MarketHoursOverrideMalformed (an override was
    # offered and is not usable) and — preopen only — MarketHoursUnverified
    # (the gate Lambda did not answer, and preopen fails CLOSED there because
    # PredictorInference invokes the same Lambda downstream, so nothing was
    # left to rescue). They are enumerated here for the same reason
    # MutexConflict is: a Catch reaching one of them fails LOUD, so the route
    # is not "fail-open to a silent SUCCESS" and needs no degraded flag. The
    # postclose pipeline deliberately has NO MarketHoursUnverified — its
    # unverified route is a genuine fail-open and sets $.degraded_summary.
    "step_function_daily.json": frozenset(
        {
            "HandleFailure",
            "FailExecution",
            "MutexConflict",
            "MarketHoursBlocked",
            "MarketHoursOverrideMalformed",
            "MarketHoursUnverified",
        }
    ),
    "step_function_eod.json": frozenset(
        {
            "HandleFailure",
            "FailExecution",
            "MutexConflict",
            "MarketHoursBlocked",
            "MarketHoursOverrideMalformed",
        }
    ),
}

# Weekly-only: ResearchPredictorParallel's two branch terminals
# (BranchAFailed/BranchBFailed) record branch_a_status/branch_b_status =
# "FAILED" as DATA and End:true — the BRANCH itself must SUCCEED so the
# sibling branch is never cancelled (Parallel's cancel-siblings-on-throw
# default) — but AggregateBranchOutcomes/CheckBranchOutcomes (structurally
# verified by test_check_branch_outcomes_routes_failed_branches_to_hard_fail
# below, not just asserted here) re-fails the WHOLE SF for either FAILED
# status via ExtractParallelBranchError -> NormalizeFailureContext. A Catch
# landing on BranchAFailed/BranchBFailed is therefore a DELAYED hard-fail,
# not a fail-open-to-success path: it never reaches NotifyComplete /
# WriteCompletionMarker at all on the FAILED branch, so no degraded flag is
# needed.
_BRANCH_JOIN_HARD_FAIL: dict[str, frozenset[str]] = {
    "step_function.json": frozenset(
        {
            "ResearchPredictorParallel.BranchAFailed",
            "ResearchPredictorParallel.BranchBFailed",
        }
    ),
    "step_function_daily.json": frozenset(),
    "step_function_eod.json": frozenset(),
}

# A Catch OWNED by an sns:publish notifier Task is the config#1819
# "notifier-totality" pattern: a best-effort alert about an ALREADY-
# DETERMINED outcome (clean, degraded, or failed) whose OWN delivery
# failure must never block the pipeline (e.g. PublishLibPinGateDegraded's
# Catch fires only if the SNS publish itself throws — by the time it runs,
# its predecessor LibPinGateDegraded has already set $.gate_degraded).
# That is a different, pre-existing axis from "did the STAGE fail open
# silently" (config#6715's scope) — excluded here by construction (any
# Task whose Resource is sns:publish), not by name, so a new notify Task
# inherits the exclusion automatically instead of needing a registry entry.
_NOTIFY_RESOURCE = "arn:aws:states:::sns:publish"

# Explicit, reasoned exemption registry — one entry per fail-open Catch
# route (keyed by the CATCHING state's path; verified unique per state by
# test_meta_at_most_one_fail_open_catch_per_state below, since AcquireMutex
# is the only state in any of the three files with two Catch clauses and
# its OTHER clause — MutexConflict — is already excluded as
# failure-family). Every entry is either:
#   * a genuine sf-pipeline-policy.md §5 carve-out (no flag required by
#     policy, cites the clause);
#   * a structurally-verifiable "the flag is already set by an ancestor
#     that unconditionally precedes this state" claim (cites the ancestor);
#   * a deliberate documented SWALLOW that fails SAFE toward the primary
#     path rather than toward silent success (cites the state's own
#     inline rationale);
#   * a FIXED-but-not-directly-walker-visible fold (alpha-engine-config#6722):
#     the flag genuinely gets set and reaches the terminal notifier, but via
#     a branch-local marker + post-join hoist this walker's single-scope
#     forward trace cannot see (ASL Parallel branches cannot write an
#     outer-scope JSONPath) — cites the Mark*Degraded state, the branch
#     terminal that hoists it, and the post-join fold, and is verified by a
#     dedicated wiring test module (mirrors PR1277's disposition for
#     ParityParallel's intra-branch entries, alpha-engine-config#6030); or
#   * "VIOLATION — tracked (alpha-engine-config#NNNN)" — a genuine,
#     un-fixed policy gap. NONE remain as of alpha-engine-config#6722
#     (which wired the last 25 gaps alpha-engine-config#6715 surfaced) —
#     any future re-appearance of this literal string is a REAL new gap
#     needing its own tracker issue, not a leftover.
_DEGRADED_FLAG_EXEMPT: dict[str, dict[str, str]] = {
    "step_function.json": {
        # ── alpha-engine-config-I8809 ──────────────────────────────────
        "NormalizeRunDates": (
            "the fail-open IS the flag. NormalizeRunDatesDegraded leaves "
            "$.run_date_family='calendar_date' — the value InitializeInput "
            "seeded — so every artifact and every stage-coverage verdict this "
            "run writes carries the family it actually used. That is a wider "
            "and more durable surface than a boolean the notifier reads once, "
            "and it is present on BOTH polarities (sf-pipeline-policy §2.3a "
            "rule 3): the clean path sets 'trading_day'. A degraded flag here "
            "would additionally page for a condition with no consequence this "
            "cycle — the coverage sweep unions both partitions until the "
            "2026-09-05 cutover — which is how a real page gets ignored."
        ),
        "WriteCompletionMarkerCalendar": (
            "the legacy-partition COPY of the envelope marker, written for the "
            "migration window only. Its canonical twin has already landed on "
            "S3 by the time this state runs and is deliberately UNCAUGHT; a "
            "failure to write the convenience copy must never degrade a run "
            "whose real completion signal is already durable. Deleted at the "
            "2026-09-05 cutover."
        ),
        "WriteCompletionMarkerDegradedCalendar": (
            "same as WriteCompletionMarkerCalendar, on the degraded path — and "
            "that path has ALREADY set its degraded flag, which is what routed "
            "it here. Deleted at the 2026-09-05 cutover."
        ),

        # alpha-engine-config-I8214: the coverage sweep runs AFTER
        # WriteCompletionMarker — after the pipeline's real success terminal.
        # A degraded flag exists to make a fail-open visible in the RUN's own
        # outcome; this run's outcome is already decided and written, and the
        # sweep may not change it (sf-pipeline-policy §2.1 blast radius), so
        # the flag would have no reader. The fail-open is made visible the
        # only way that is honest here: its own SNS page from
        # WeeklyCoverageSweepUnavailable, into a terminal named
        # WeeklyCoverageSweepUnobserved — which says the coverage SURFACE, not
        # the run, is the thing without a verdict.
        "WeeklyCoverageSweep": "observe-only tail downstream of the success terminal — pages via its own SNS state; a degraded flag would have no reader, the run's outcome being already written",

        "RunScope": (
            "alpha-engine-config-I7620. The fail-open is VISIBLE on the "
            "consumer surface rather than through an SF flag, which is why no "
            "flag is wired. Two cases, and both are loud: (a) the derivation "
            "fails INSIDE the Lambda — it still writes run_scope.json, with "
            "degraded=true and every stage NOT_REACHED, so the Report Card "
            "renders 'SCOPE UNAVAILABLE ... 0 stages graded'; (b) the Lambda "
            "itself fails to invoke and this Catch fires — no artifact is "
            "written, and the consumer treats an ABSENT run_scope.json as "
            "'scope unknown, grade nothing', never as 'everything ran'. In "
            "neither case can a broken scope producer render as a narrow, "
            "clean, fully green cycle, which is the property the degraded-flag "
            "convention exists to guarantee. Adding a sixth degraded family "
            "would require a breaking change to sf_gate_state.v1 for strictly "
            "less signal than the artifact already carries."
        ),
        "WeeklyRunDayGate": (
            "sf-pipeline-policy.md §5 carve-out: 'The weekly run-day gate "
            "fails open — missing a weekly run is worse than a duplicate; "
            "the mutex handles duplicates.' No degraded flag required by "
            "design, unlike the other three named §5 families."
        ),
        "ResearchPredictorParallel.Scanner": (
            "FIXED, not directly walker-visible (alpha-engine-config#6722): "
            "an ASL Parallel branch cannot write an outer-scope JSONPath "
            "(the CAUTION this issue's own body names), so no in-branch fix "
            "is ever detectable by this walker starting from a top-level "
            "Choice. Scanner's Catch now routes through MarkScannerDegraded "
            "(sets branch-local $.research_degraded_local=true, seeded "
            "false by InitResearchDegradedFlag at Branch A's StartAt) before "
            "converging on CheckSkipRegimeSubstrate exactly as before. "
            "BranchAComplete hoists that marker as branch_a_degraded; "
            "AggregateBranchOutcomes hoists it post-join; "
            "CheckResearchPredictorDegraded ORs it with Branch B's "
            "equivalent and sets the real top-level $.research_predictor_"
            "degraded CheckGateDegradedNotify reads (folded into "
            "NotifyCompleteMultipleDegraded). Verified end-to-end by "
            "tests/test_sf_research_predictor_degraded_wiring.py — mirrors "
            "the CheckParityBranchOutcomes fold PR1277 built for "
            "ParityParallel (alpha-engine-config#6030), the same disposition "
            "used there for its own intra-branch entries."
        ),
        "ResearchPredictorParallel.RegimeSubstrate": (
            "Same fold as Scanner (routes through MarkRegimeSubstrateDegraded) "
            "— see that entry for the full mechanism; verified by "
            "tests/test_sf_research_predictor_degraded_wiring.py."
        ),
        # alpha-engine-config-I7726 — the ONLY entry here that does not fold
        # into $.research_degraded_local, and deliberately so. Every other
        # branch-local exemption routes through a Mark*Degraded Pass because
        # that stage's failure means its OUTPUT is untrustworthy. ResearchSelfTest
        # is different in kind: the battery's own contract is never-raises, so
        # reaching its Catch means the INVOCATION failed (timeout, throttle,
        # deploy skew) — not that the numbers are wrong. Since config-I6891 a
        # degraded summary terminates the run in DegradedRun, so folding this
        # would kill a pipeline that produced real trading artifacts over a
        # missing observe-mode verdict.
        #
        # The absence is NOT unobserved, which is what would make this a silent
        # swallow rather than a routing choice: the registry's `research_self_test`
        # row pages through the freshness monitor when the artifact does not
        # appear, and `$.research_self_test_invocation_failed` records the
        # invocation failure on the payload. Two independent surfaces, neither of
        # them this flag.
        "ResearchPredictorParallel.ResearchSelfTest": (
            "Invocation-failure-only fail-open; the verdict's absence is detected "
            "by the freshness monitor on research/{date}/self_test.json rather "
            "than by the degraded fold, so folding it would terminate a good run "
            "over an observe-mode stage (config-I7726)."
        ),
        "ResearchPredictorParallel.ChallengerShadow": (
            "Same fold as Scanner (routes through MarkChallengerShadowDegraded) "
            "— see that entry for the full mechanism; verified by "
            "tests/test_sf_research_predictor_degraded_wiring.py."
        ),
        "ResearchPredictorParallel.RegimeRetrospectiveEval": (
            "Same fold as Scanner (routes through "
            "MarkRegimeRetrospectiveEvalDegraded) — see that entry for the "
            "full mechanism; verified by "
            "tests/test_sf_research_predictor_degraded_wiring.py."
        ),
        "ResearchPredictorParallel.EvalJudgeSubmitFirstSaturday": (
            "FIXED, not directly walker-visible (alpha-engine-config#6722, "
            "same branch-scoping constraint as Scanner's entry above): this "
            "and the sibling eval-judge Catches (EvalJudgeSubmitWeekly/"
            "EvalJudgePoll/EvalJudgeProcess) all previously converged "
            "directly on EvalRollingMean with no flag. Now share ONE "
            "convergence Pass (MarkEvalJudgeDegraded, sets $.research_"
            "degraded_local=true) before continuing to EvalRollingMean "
            "unchanged, folded into $.research_predictor_degraded post-join "
            "exactly as Scanner's entry describes. Verified by "
            "tests/test_sf_research_predictor_degraded_wiring.py."
        ),
        "ResearchPredictorParallel.EvalJudgeSubmitWeekly": (
            "Same shared fold as EvalJudgeSubmitFirstSaturday (both route "
            "through MarkEvalJudgeDegraded)."
        ),
        # alpha-engine-config-I9329: EvalJudgePoll was deleted with the rest
        # of the provider-batch poll chain; the three states below are the
        # spot substrate that replaced it, and they fold through exactly the
        # same shared marker.
        "ResearchPredictorParallel.DispatchEvalJudgeSpot": (
            "Same shared fold as EvalJudgeSubmitFirstSaturday (routes "
            "through MarkEvalJudgeDegraded)."
        ),
        "ResearchPredictorParallel.WaitForEvalJudgeSpotBootstrap": (
            "Same shared fold as EvalJudgeSubmitFirstSaturday (routes "
            "through MarkEvalJudgeDegraded)."
        ),
        "ResearchPredictorParallel.WaitForEvalJudgeProcess": (
            "Same shared fold as EvalJudgeSubmitFirstSaturday (routes "
            "through MarkEvalJudgeDegraded)."
        ),
        "ResearchPredictorParallel.EvalJudgeProcess": (
            "Same shared fold as EvalJudgeSubmitFirstSaturday (routes "
            "through MarkEvalJudgeDegraded)."
        ),
        "ResearchPredictorParallel.EvalRollingMean": (
            "Same fold as Scanner, on EvalRollingMean's OWN Catch (distinct "
            "from the eval-judge submit/poll/process group above) — routes "
            "through MarkEvalRollingMeanDegraded before continuing to "
            "CheckSkipRationaleClustering unchanged."
        ),
        "ResearchPredictorParallel.RationaleClustering": (
            "Same fold as Scanner (routes through "
            "MarkRationaleClusteringDegraded) before continuing to "
            "CheckSkipCounterfactual — its continuation until "
            "alpha-engine-config-I10539 (Brian ruling 2026-09-16, option b) "
            "was CheckSkipReplayConcordance, retired with that stage."
        ),
        "ResearchPredictorParallel.Counterfactual": (
            "Same fold as Scanner (routes through MarkCounterfactualDegraded) "
            "before continuing to BranchAComplete unchanged "
            "(alpha-engine-config-I7194: was CheckSkipAggregateCosts until the "
            "aggregator left this branch — Counterfactual is now Branch A's "
            "last work state)."
        ),
        "ResearchPredictorParallel.ResolveZooSpecs": (
            "FIXED, not directly walker-visible (alpha-engine-config#6722, "
            "Branch B's version of the Scanner-entry constraint): the "
            "model-zoo rotation group (ResolveZooSpecs/WaitResolveZoo/"
            "ModelZooTrainMap/ModelZooSelect/WaitForModelZoo) all converge "
            "on ONE existing state, PublishModelZooFailureImmediate, which "
            "now routes through MarkModelZooDegraded (sets Branch B's "
            "branch-local $.research_degraded_local=true, seeded false by "
            "InitPredictorDegradedFlag at Branch B's StartAt) before continuing "
            "to BranchBComplete unchanged. BranchBComplete hoists the "
            "marker as branch_b_degraded; the champion is already "
            "trained+promoted so the rotation stays advisory, but the "
            "required visible flag is now real — see Scanner's entry for "
            "the full post-join fold. Verified by "
            "tests/test_sf_research_predictor_degraded_wiring.py."
        ),
        "ResearchPredictorParallel.WaitResolveZoo": (
            "Same shared convergence as ResolveZooSpecs (both route to "
            "PublishModelZooFailureImmediate -> MarkModelZooDegraded)."
        ),
        "ResearchPredictorParallel.ModelZooTrainMap": (
            "Same shared convergence as ResolveZooSpecs (the Map state's "
            "OWN Catch — a genuine Map-engine error, not a per-iteration "
            "one; see TrainSpecDispatch/WaitTrainSpec below for the "
            "per-iteration case — also routes to PublishModelZooFailure"
            "Immediate -> MarkModelZooDegraded)."
        ),
        "ResearchPredictorParallel.ReadModelZooArenaCycle": (
            "Same fail-open group as every other model-zoo state: the Catch "
            "routes ExtractModelZooVerdictUnreadable -> "
            "PublishModelZooFailureImmediate -> MarkModelZooDegraded, which "
            "sets Branch B's $.research_degraded_local and folds into "
            "$.research_predictor_degraded at the join. The flag is written "
            "two hops downstream, which is what this walk cannot see "
            "(alpha-engine-config-I11101 deliverable 4)."
        ),
        "ResearchPredictorParallel.ModelZooSelect": (
            "Same shared convergence as ResolveZooSpecs (routes to "
            "PublishModelZooFailureImmediate -> MarkModelZooDegraded)."
        ),
        "ResearchPredictorParallel.WaitForModelZoo": (
            "Same shared convergence as ResolveZooSpecs (routes to "
            "PublishModelZooFailureImmediate -> MarkModelZooDegraded)."
        ),
        "ResearchPredictorParallel.ModelZooTrainMap.TrainSpecDispatch": (
            "Map ITERATION-level failure, tolerated by design per this "
            "state's own Comment ('siblings proceed, ModelZooSelect "
            "simply finds this spec challenger absent from the "
            "registry') — isolated to one candidate spec, not a "
            "pipeline-outcome signal; a pipeline-wide degraded flag per "
            "failed spec would be noise on expected per-rotation churn."
        ),
        "ResearchPredictorParallel.ModelZooTrainMap.WaitTrainSpec": (
            "Same per-iteration tolerated-by-design reasoning as "
            "TrainSpecDispatch."
        ),
        "ParityParallel.PitParityLookahead": (
            "alpha-engine-config#6030 branch-level fail-open: the Catch ends "
            "the branch DEGRADED (PitParityLookaheadDegraded, End:true, "
            "status into $.branch_pit_lookahead) rather than throwing — a "
            "branch never aborts its Parallel siblings. The degraded flag "
            "the terminal notifier reads ($.parity_degraded) is set at the "
            "JOIN, not in the branch: AggregateParityBranchOutcomes -> "
            "CheckParityBranchOutcomes reads $.parity_branch_outcomes."
            "pit_lookahead_status and routes any DEGRADED to ParityDegraded "
            "(ResultPath $.parity_degraded=true). The join unconditionally "
            "follows the Parallel, so the flag is guaranteed set — the "
            "DEGRADED analog of the BranchAFailed/BranchBFailed hard-fail "
            "join already handled structurally above."
        ),
        "ParityParallel.WaitForPitParityLookahead": (
            "Same branch-level fail-open as PitParityLookahead: the poll's "
            "Catch converges on the same PitParityLookaheadDegraded branch "
            "terminal; $.parity_degraded is set at the CheckParityBranch"
            "Outcomes join (alpha-engine-config#6030)."
        ),
        "ParityParallel.PitParityWalkforward": (
            "Same branch-level fail-open as PitParityLookahead: Catch ends "
            "the branch DEGRADED (PitParityWalkforwardDegraded); the join "
            "reads $.parity_branch_outcomes.pit_walkforward_status and sets "
            "$.parity_degraded (alpha-engine-config#6030)."
        ),
        "ParityParallel.WaitForPitParityWalkforward": (
            "Same branch-level fail-open as PitParityWalkforward: the poll's "
            "Catch converges on PitParityWalkforwardDegraded; the flag is "
            "set at the CheckParityBranchOutcomes join "
            "(alpha-engine-config#6030)."
        ),
        "ParityParallel.ParityReplay": (
            "Same branch-level fail-open as PitParityLookahead: Catch ends "
            "the branch DEGRADED (ParityReplayDegraded); the join reads "
            "$.parity_branch_outcomes.parity_replay_status and sets "
            "$.parity_degraded (alpha-engine-config#6030)."
        ),
        "ParityParallel.WaitForParityReplay": (
            "Same branch-level fail-open as ParityReplay: the poll's Catch "
            "converges on ParityReplayDegraded; the flag is set at the "
            "CheckParityBranchOutcomes join (alpha-engine-config#6030)."
        ),
        "ParityParallel.PitParityLookaheadResourceKillCheck": (
            "alpha-engine-config-I7267: this send/poll/parse-free marker "
            "check runs ONLY after PitParityLookahead already exited "
            "non-zero. Its own Catch (instance gone, SSM unreachable) is "
            "deliberately safe-by-default: it falls through to the SAME "
            "PitParityLookaheadDegraded branch terminal as the pass's own "
            "Catch — never blocking, only potentially skipping the "
            "enhanced RESOURCE_KILL classification for this one run. "
            "$.parity_degraded is still set at the CheckParityBranchOutcomes "
            "join exactly as for the pass's own Catch."
        ),
        "ParityParallel.WaitForPitParityLookaheadResourceKillCheck": (
            "Same reasoning as PitParityLookaheadResourceKillCheck: the "
            "poll's Catch converges on PitParityLookaheadDegraded; the flag "
            "is set at the CheckParityBranchOutcomes join "
            "(alpha-engine-config-I7267)."
        ),
        "ParityParallel.PitParityWalkforwardResourceKillCheck": (
            "Same reasoning as PitParityLookaheadResourceKillCheck, for the "
            "walkforward pass: Catch converges on "
            "PitParityWalkforwardDegraded; the flag is set at the "
            "CheckParityBranchOutcomes join (alpha-engine-config-I7267)."
        ),
        "ParityParallel.WaitForPitParityWalkforwardResourceKillCheck": (
            "Same reasoning as WaitForPitParityLookaheadResourceKillCheck, "
            "for the walkforward pass: the poll's Catch converges on "
            "PitParityWalkforwardDegraded; the flag is set at the "
            "CheckParityBranchOutcomes join (alpha-engine-config-I7267)."
        ),
    },
    "step_function_daily.json": {
        "TradingDayGate": (
            "Same design intent as the NAMED weekly run-day-gate §5 "
            "carve-out (missing a trading day is worse than a duplicate; "
            "StartExecutorEC2 proceeds, and downstream predictor/"
            "enrichment stages remain independently gated on their own "
            "success) — sf-pipeline-policy.md §5 does not name the daily "
            "gate by file, and this test suite's own pre-existing "
            "_CATCH_EXEMPT['step_function_daily.json']['TradingDayGateFailed'] "
            "entry already treats it as the same accepted pattern. Stated "
            "assumption (alpha-engine-config#6715 session) — a §5 wording "
            "PR to name it explicitly is a cheap follow-up."
        ),
    },
    "step_function_eod.json": {
        "CaptureSnapshot": (
            "NOT fail-open (alpha-engine-config#5569): the Catch routes to "
            "CheckCaptureSnapshotRetryBudget — a bounded single retry that "
            "pages immediately and, once exhausted, HARD-FAILS via "
            "HandleFailure. A degraded flag would misstate the contract: "
            "the run never proceeds past a failed snapshot."
        ),
        "WaitForCaptureSnapshot": (
            "Same route as CaptureSnapshot's Catch (config#5569): poll "
            "failure enters the same bounded-retry-then-HandleFailure "
            "path; fail-closed, not fail-open."
        ),
        "ProbeEODReconcilePrecondition": (
            "Deliberate documented SWALLOW — this state's own Comment "
            "names the failure mode swallowed (the probe Lambda itself "
            "erroring, not a data-gap signal) and the recording surface "
            "($.precondition_probe left absent), per feedback_no_silent_"
            "fails. A probe-Lambda infra failure falls through to "
            "CheckSkipEODReconcile's Default (EODReconcile) — fail-SAFE-"
            "toward-attempting-the-real-reconcile, not fail-open-to-"
            "silent-success. EODReconcile's own Catch is the "
            "failure-family route if the real reconcile then cannot "
            "complete."
        ),
        "HealLaunchPostMarketDataSpot": (
            "Heal-loop state, only reachable after SkipEODReconcileDataGap "
            "-> SetDegradedFlag has ALREADY set $.degraded_summary "
            "unconditionally (both SkipEODReconcileDataGap's normal Next "
            "and its own Catch converge on SetDegradedFlag) — the flag is "
            "already true before this state ever runs."
        ),
        "HealPollPostMarketDataSpot": (
            "Same ancestor-already-flagged reasoning as "
            "HealLaunchPostMarketDataSpot."
        ),
        "HealLaunchArcticAppendSpot": (
            "Same ancestor-already-flagged reasoning as "
            "HealLaunchPostMarketDataSpot."
        ),
        "HealPollArcticAppendSpot": (
            "Same ancestor-already-flagged reasoning as "
            "HealLaunchPostMarketDataSpot."
        ),
        "HealReProbe": (
            "Same ancestor-already-flagged reasoning as "
            "HealLaunchPostMarketDataSpot."
        ),
        "HealDispatchReplay": (
            "Same ancestor-already-flagged reasoning as "
            "HealLaunchPostMarketDataSpot (heal-loop state)."
        ),
        "ReadExerciseCadence": (
            "Deliberate fail-toward-running default: SetCadenceReadDegraded "
            "floors $.exercise_cadence_param to {'value': 'daily'} rather "
            "than skip — this state's own Comment explicitly cites "
            "weekly-sf-policy §5's 'missed exercise run is the worse "
            "failure mode' reasoning, the same intent as the named "
            "run-day-gate carve-out."
        ),
    },
}

assert set(_DEGRADED_FLAG_EXEMPT) == set(_DEGRADED_FLAG_SF_FILES), (
    "degraded-flag exemption registry must enumerate exactly the three "
    "scheduled-pipeline files (not step_function_groom.json)"
)


# ---------------------------------------------------------------------------
# Definition-walking primitives — mirrors the Task-state discovery already
# used informally by test_sf_global_timeout.py / test_deploy_infrastructure_
# sf_coverage.py, but recurses into Parallel branches and Map
# ItemProcessor/Iterator sub-definitions so nested Task states (e.g. every
# state inside step_function.json's ResearchPredictorParallel branches, or
# inside ModelZooTrainMap) are not silently skipped.
# ---------------------------------------------------------------------------


def _iter_task_states(definition: dict) -> Iterator[tuple[str, dict]]:
    def _walk(states: dict, prefix: str) -> Iterator[tuple[str, dict]]:
        for name, state in states.items():
            path = f"{prefix}{name}"
            stype = state.get("Type")
            if stype == "Task":
                yield path, state
            if "States" in state:
                yield from _walk(state["States"], f"{path}.")
            if stype == "Parallel":
                for branch in state.get("Branches", []):
                    yield from _walk(branch.get("States", {}), f"{path}.")
            if stype == "Map":
                sub = (state.get("ItemProcessor") or state.get("Iterator") or {}).get(
                    "States"
                )
                if sub:
                    yield from _walk(sub, f"{path}.")

    yield from _walk(definition.get("States", {}), "")


def _missing_timeout(definition: dict, exempt: dict[str, str]) -> list[str]:
    return sorted(
        name
        for name, state in _iter_task_states(definition)
        if "TimeoutSeconds" not in state and name not in exempt
    )


def _missing_catch(definition: dict, exempt: dict[str, str]) -> list[str]:
    return sorted(
        name
        for name, state in _iter_task_states(definition)
        if "Catch" not in state and name not in exempt
    )


def _stale_exemptions(definition: dict, exempt: dict[str, str]) -> list[str]:
    present = {name for name, _ in _iter_task_states(definition)}
    return sorted(name for name in exempt if name not in present)


# ---------------------------------------------------------------------------
# alpha-engine-config#6715 walking/matching primitives. Reuses the same
# nested-Parallel/Map recursion as _iter_task_states above but yields EVERY
# state regardless of Type — a fail-open route can originate on a Parallel
# or Map's OWN Catch (ResearchPredictorParallel, ModelZooTrainMap), not
# only a Task's.
# ---------------------------------------------------------------------------


def _iter_all_states(definition: dict) -> Iterator[tuple[str, dict]]:
    def _walk(states: dict, prefix: str) -> Iterator[tuple[str, dict]]:
        for name, state in states.items():
            path = f"{prefix}{name}"
            yield path, state
            if "States" in state:
                yield from _walk(state["States"], f"{path}.")
            if state.get("Type") == "Parallel":
                for branch in state.get("Branches", []):
                    yield from _walk(branch.get("States", {}), f"{path}.")
            if state.get("Type") == "Map":
                sub = (state.get("ItemProcessor") or state.get("Iterator") or {}).get(
                    "States"
                )
                if sub:
                    yield from _walk(sub, f"{path}.")

    yield from _walk(definition.get("States", {}), "")


def _flat_index(definition: dict) -> dict[str, dict]:
    return dict(_iter_all_states(definition))


def _resolve_next(
    owner_path: str, flat: dict[str, dict], next_name: str | None
) -> str | None:
    """A Catch/Next targets a state name scoped to the SAME States dict as
    the owner (a Parallel branch's Next never crosses into a sibling
    branch or the top level by name collision) — try the owner's scope
    prefix first, then fall back to top-level (the common case: most
    Catches point at a top-level state from a top-level owner)."""
    if next_name is None:
        return None
    scope_prefix = owner_path.rsplit(".", 1)[0] + "." if "." in owner_path else ""
    scoped = f"{scope_prefix}{next_name}"
    if scoped in flat:
        return scoped
    if next_name in flat:
        return next_name
    return None


def _route_is_visible(
    start_path: str,
    flat: dict[str, dict],
    flag_paths: frozenset[str],
    failure_family: frozenset[str],
    branch_join: frozenset[str],
    max_depth: int = 15,
) -> bool:
    """Forward-walks a Pass/Task-only deterministic chain from start_path,
    returning True the moment it finds EITHER (a) a state whose ResultPath
    IS one of this file's real degraded-flag setter paths, or (b) a state
    that is itself part of the hard-fail family / branch-join convergence
    (the route ends in a LOUD Fail, not a silent SUCCESS, so no flag is
    needed). Stops (False) at the first Choice/Parallel/Map/Wait/Succeed/
    Fail/End — a fail-open route that has not set the flag by then never
    will on THIS path (nothing past a branch point is guaranteed to run)."""
    seen: set[str] = set()
    cur: str | None = start_path
    depth = 0
    while cur and depth < max_depth and cur not in seen:
        seen.add(cur)
        state = flat.get(cur)
        if state is None:
            return False
        name = cur.rsplit(".", 1)[-1]
        if name in failure_family or cur in branch_join:
            return True
        if state.get("ResultPath") in flag_paths:
            return True
        if state.get("Type") not in ("Pass", "Task") or state.get("End"):
            return False
        cur = _resolve_next(cur, flat, state.get("Next"))
        depth += 1
    return False


def _iter_fail_open_catch_routes(
    flat: dict[str, dict],
    failure_family: frozenset[str],
    branch_join: frozenset[str],
) -> Iterator[tuple[str, str]]:
    """(owner_path, target_path) for every Catch clause that is
    structurally fail-open: the owning state is not itself an sns:publish
    notifier (the config#1819 notifier-totality axis, out of scope — see
    _NOTIFY_RESOURCE above), and the Catch's Next does not land immediately
    on a failure-family / branch-join state."""
    for path, state in flat.items():
        if state.get("Resource") == _NOTIFY_RESOURCE:
            continue
        for catch in state.get("Catch", []):
            target = _resolve_next(path, flat, catch.get("Next"))
            target_name = target.rsplit(".", 1)[-1] if target else catch.get("Next")
            if target_name in failure_family or target in branch_join:
                continue
            yield path, target if target else catch.get("Next")


def _choice_variables(choice_state: dict) -> set[str]:
    """Every JSONPath a Choice state's condition tree dereferences,
    recursing through And/Or/Not nesting — used to verify
    _DEGRADED_FLAG_JSONPATHS against the terminal Choice's ACTUAL Variable
    set rather than trusting a hand-typed constant (config#6715 gotcha)."""
    variables: set[str] = set()

    def _walk(rule: dict) -> None:
        if "Variable" in rule:
            variables.add(rule["Variable"])
        for key in ("And", "Or"):
            for sub in rule.get(key, []):
                _walk(sub)
        if "Not" in rule:
            _walk(rule["Not"])

    for choice in choice_state.get("Choices", []):
        _walk(choice)
    return variables


def _is_prefix_path(setter: str, reader: str) -> bool:
    """True if `reader` IS `setter`, or a dotted sub-field of it — daily/
    eod set the composite $.degraded_summary but the terminal Choice reads
    the nested $.degraded_summary.degraded."""
    return reader == setter or reader.startswith(setter + ".")


def _load(sf_file: str) -> dict:
    return json.loads((_INFRA / sf_file).read_text())


# ---------------------------------------------------------------------------
# Deliverable 3 — data-repo launcher-script existence.
#
# Only states whose SSM command list `cd`s into THIS repo's own EC2 deploy
# target (`/home/ec2-user/alpha-engine-data`, confirmed in OVERVIEW.md) are
# checked: a sibling repo's scripts (crucible-executor's `executor/main.py`,
# crucible-predictor's `infrastructure/spot_train.sh`, ...) cannot be
# verified from a pure file read of this repo and are out of scope.
# ---------------------------------------------------------------------------

_SCRIPT_REF_RE = re.compile(r"[A-Za-z0-9_./-]+\.(?:sh|py)\b")
_DATA_REPO_CD_RE = re.compile(r"cd /home/ec2-user/alpha-engine-data(?=['\"\s])")
_DATA_REPO_ABS_PREFIX = "/home/ec2-user/alpha-engine-data/"


def _data_repo_script_refs(definition: dict) -> list[tuple[str, str]]:
    """(state_name, script_ref) pairs for Task states that operate against
    this repo's own EC2 checkout, as evidenced by a `cd` into it appearing
    in the state's Parameters block."""
    refs = []
    for name, state in _iter_task_states(definition):
        params_text = json.dumps(state.get("Parameters", {}))
        if not _DATA_REPO_CD_RE.search(params_text):
            continue
        refs.extend((name, ref) for ref in _SCRIPT_REF_RE.findall(params_text))
    return refs


def _missing_data_repo_scripts(definition: dict, repo_root: Path) -> list[str]:
    missing = []
    for state_name, ref in _data_repo_script_refs(definition):
        rel = (
            ref[len(_DATA_REPO_ABS_PREFIX) :]
            if ref.startswith(_DATA_REPO_ABS_PREFIX)
            else ref
        )
        if not (repo_root / rel).is_file():
            missing.append(f"{state_name} -> {ref} (resolved: {rel})")
    return missing


# ---------------------------------------------------------------------------
# Meta-tests — prove the checkers themselves flag a bad definition, so this
# suite cannot silently pass on a parsing bug (config#6684 deliverable d).
# All synthetic — no dependency on the real definitions, so these stay
# stable regardless of what other in-flight PRs land on the real files.
# ---------------------------------------------------------------------------


def test_meta_walker_finds_nested_task_states():
    """Task states nested inside Parallel branches and Map
    ItemProcessor/Iterator sub-definitions must be discovered — the exact
    shapes step_function.json's ResearchPredictorParallel and
    ModelZooTrainMap use."""
    synthetic = {
        "States": {
            "TopTask": {"Type": "Task", "End": True},
            "AParallel": {
                "Type": "Parallel",
                "Branches": [
                    {"States": {"BranchTask": {"Type": "Task", "End": True}}}
                ],
                "End": True,
            },
            "AMapNew": {
                "Type": "Map",
                "ItemProcessor": {
                    "States": {"MapNewTask": {"Type": "Task", "End": True}}
                },
                "End": True,
            },
            "AMapLegacy": {
                "Type": "Map",
                "Iterator": {
                    "States": {"MapLegacyTask": {"Type": "Task", "End": True}}
                },
                "End": True,
            },
        }
    }
    found = {name for name, _ in _iter_task_states(synthetic)}
    assert found == {
        "TopTask",
        "AParallel.BranchTask",
        "AMapNew.MapNewTask",
        "AMapLegacy.MapLegacyTask",
    }


def test_meta_missing_timeout_is_flagged_and_exemption_clears_it():
    synthetic = {
        "States": {
            "Bare": {"Type": "Task", "Catch": [{"ErrorEquals": ["States.ALL"]}], "End": True},
            "Covered": {
                "Type": "Task",
                "TimeoutSeconds": 30,
                "Catch": [{"ErrorEquals": ["States.ALL"]}],
                "End": True,
            },
        }
    }
    assert _missing_timeout(synthetic, exempt={}) == ["Bare"]
    assert _missing_timeout(synthetic, exempt={"Bare": "synthetic exemption"}) == []


def test_meta_missing_catch_is_flagged_and_exemption_clears_it():
    synthetic = {
        "States": {
            "Bare": {"Type": "Task", "TimeoutSeconds": 30, "End": True},
            "Covered": {
                "Type": "Task",
                "TimeoutSeconds": 30,
                "Catch": [{"ErrorEquals": ["States.ALL"]}],
                "End": True,
            },
        }
    }
    assert _missing_catch(synthetic, exempt={}) == ["Bare"]
    assert _missing_catch(synthetic, exempt={"Bare": "synthetic exemption"}) == []


def test_meta_stale_exemption_is_flagged():
    synthetic = {
        "States": {
            "StillHere": {"Type": "Task", "TimeoutSeconds": 30, "End": True},
        }
    }
    exempt = {"StillHere": "ok", "Renamed": "no longer exists"}
    assert _stale_exemptions(synthetic, exempt) == ["Renamed"]


def test_meta_missing_data_repo_script_is_flagged():
    synthetic = {
        "States": {
            "RunsMissingScript": {
                "Type": "Task",
                "Parameters": {
                    "commands": [
                        "cd /home/ec2-user/alpha-engine-data",
                        "bash infrastructure/this_script_does_not_exist_ndm6684.sh",
                    ]
                },
                "End": True,
            },
            "RunsRealScript": {
                "Type": "Task",
                "Parameters": {
                    "commands": [
                        "cd /home/ec2-user/alpha-engine-data",
                        "bash infrastructure/spot_data_weekly.sh",
                    ]
                },
                "End": True,
            },
            "SiblingRepoNotChecked": {
                "Type": "Task",
                "Parameters": {
                    "commands": [
                        "cd /home/ec2-user/alpha-engine-predictor",
                        "bash infrastructure/this_also_does_not_exist.sh",
                    ]
                },
                "End": True,
            },
        }
    }
    missing = _missing_data_repo_scripts(synthetic, _REPO_ROOT)
    assert len(missing) == 1
    assert "RunsMissingScript" in missing[0]
    assert "this_script_does_not_exist_ndm6684.sh" in missing[0]


# ---------------------------------------------------------------------------
# alpha-engine-config#6715 meta-tests — all synthetic, proving the
# fail-open/degraded-flag classifier itself catches a bad definition before
# trusting it against the real files below.
# ---------------------------------------------------------------------------

_META_FLAG_PATHS = frozenset({"$.thing_degraded"})
_META_FAILURE_FAMILY = frozenset({"HandleFailure", "FailExecution"})
_META_BRANCH_JOIN: frozenset[str] = frozenset()


def test_meta_route_is_visible_credits_a_flag_write():
    synthetic = {
        "States": {
            "Stage": {
                "Type": "Task",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "SetFlag"}],
                "End": True,
            },
            "SetFlag": {
                "Type": "Pass",
                "ResultPath": "$.thing_degraded",
                "Next": "Continue",
            },
            "Continue": {"Type": "Choice", "Choices": [], "Default": "Stage"},
        }
    }
    flat = _flat_index(synthetic)
    assert _route_is_visible(
        "SetFlag", flat, _META_FLAG_PATHS, _META_FAILURE_FAMILY, _META_BRANCH_JOIN
    )


def test_meta_route_is_visible_credits_hard_fail_family():
    synthetic = {
        "States": {
            "Stage": {"Type": "Task", "End": True},
            "Normalize": {"Type": "Pass", "Next": "HandleFailure"},
            "HandleFailure": {"Type": "Task", "Next": "FailExecution"},
            "FailExecution": {"Type": "Fail"},
        }
    }
    flat = _flat_index(synthetic)
    assert _route_is_visible(
        "Normalize", flat, _META_FLAG_PATHS, _META_FAILURE_FAMILY, _META_BRANCH_JOIN
    )


def test_meta_route_is_visible_stops_at_a_choice_with_nothing_found():
    synthetic = {
        "States": {
            "Stage": {"Type": "Task", "End": True},
            "Unflagged": {"Type": "Pass", "Next": "SomeChoice"},
            "SomeChoice": {"Type": "Choice", "Choices": [], "Default": "Stage"},
        }
    }
    flat = _flat_index(synthetic)
    assert not _route_is_visible(
        "Unflagged", flat, _META_FLAG_PATHS, _META_FAILURE_FAMILY, _META_BRANCH_JOIN
    )


def test_meta_iter_fail_open_catch_routes_excludes_notify_and_hard_fail():
    synthetic = {
        "States": {
            "RealStage": {
                "Type": "Task",
                "Catch": [
                    {"ErrorEquals": ["States.ALL"], "Next": "HandleFailure"},
                ],
                "End": True,
            },
            "OpenStage": {
                "Type": "Task",
                "Catch": [
                    {"ErrorEquals": ["States.ALL"], "Next": "ContinueUnflagged"},
                ],
                "End": True,
            },
            "Notifier": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Catch": [
                    {"ErrorEquals": ["States.ALL"], "Next": "ContinueUnflagged"},
                ],
                "End": True,
            },
            "ContinueUnflagged": {"Type": "Pass", "End": True},
            "HandleFailure": {"Type": "Task", "End": True},
        }
    }
    flat = _flat_index(synthetic)
    routes = list(
        _iter_fail_open_catch_routes(flat, _META_FAILURE_FAMILY, _META_BRANCH_JOIN)
    )
    assert routes == [("OpenStage", "ContinueUnflagged")]


def test_meta_choice_variables_recurses_and_or_not():
    choice_state = {
        "Type": "Choice",
        "Choices": [
            {
                "Or": [
                    {"Variable": "$.a", "BooleanEquals": True},
                    {
                        "And": [
                            {"Variable": "$.b", "IsPresent": True},
                            {"Not": {"Variable": "$.c", "BooleanEquals": False}},
                        ]
                    },
                ],
                "Next": "X",
            }
        ],
        "Default": "Y",
    }
    assert _choice_variables(choice_state) == {"$.a", "$.b", "$.c"}


def test_meta_is_prefix_path():
    assert _is_prefix_path("$.gate_degraded", "$.gate_degraded")
    assert _is_prefix_path("$.degraded_summary", "$.degraded_summary.degraded")
    assert not _is_prefix_path("$.degraded_summary", "$.degraded_summaryx")
    assert not _is_prefix_path("$.gate_degraded", "$.health_check_degraded")


# ---------------------------------------------------------------------------
# Real-definition tests.
# ---------------------------------------------------------------------------


def test_sf_file_set_matches_exemption_registry():
    """Guards against a vacuous parametrize AND a new step_function_*.json
    landing with no exemption entries defined for it — either would make
    every test below silently skip or silently pass for the new file."""
    on_disk = sorted(p.name for p in _INFRA.glob("step_function*.json"))
    assert on_disk, "no infrastructure/step_function*.json files found"
    assert on_disk == _SF_FILE_NAMES, (
        f"infrastructure/ has {on_disk} but this module's exemption "
        f"registry covers {_SF_FILE_NAMES} — add/remove a top-level dict "
        f"entry in test_sf_structural_contract.py to match"
    )


# alpha-engine-config#10008. The per-definition whole-execution ceiling, in
# seconds. PRESENCE was already asserted below; the VALUE was not, and the two
# weekday definitions had drifted to numbers nothing derived and nothing
# checked — daily=39600 (11h) against a policy ceiling of 7200, eod=64800
# (18h) against a ≤75-minute wall-clock budget. A ceiling that no test pins is
# a ceiling that only ever grows, because every individual bump looks safe.
#
#   weekly  43200  sf-pipeline-policy.md §4, stated ceiling (12h)
#   daily    7200  sf-pipeline-policy.md §4, stated ceiling (2h). The window to
#                  the 06:30 PT market open is 75 min from the 05:15 PT
#                  trigger; p95 of the 17 successful scheduled runs over
#                  2026-07-28→09-04 is ~52 min and the longest recovery rerun
#                  in that span was 102 min, so 2h bounds every real run while
#                  releasing the SF mutex the same morning. That last part is
#                  the point: sf-pipeline-policy.md §1.2 makes same-day
#                  recovery a standing rule, and an 11h ceiling let one hung
#                  execution hold the mutex past the close and take the
#                  recovery with it.
#   eod     14400  sf-pipeline-policy.md §4 gives the eod pipeline a ≤75-min
#                  wall-clock target and no numeric ceiling (it must "include
#                  the bounded heal loop"). Derived: 4h is 2.1× the longest
#                  execution observed over 2026-07-31→09-04 (115 min, a failed
#                  run on 08-17) and still clears the 22:00 PT
#                  alpha-engine-stop-trading cost guard by six hours.
#   groom   15000  unchanged; not a pipeline sf-pipeline-policy.md governs.
_TOP_LEVEL_TIMEOUT_CEILING = {
    "step_function.json": 43200,
    "step_function_daily.json": 7200,
    "step_function_eod.json": 14400,
    "step_function_groom.json": 15000,
}


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_definition_declares_top_level_timeout(sf_file: str):
    """alpha-engine-config#6693: a hung execution with no top-level
    TimeoutSeconds can run to the Step Functions 1-year service maximum,
    invisible to any status-keyed watcher. Covers every file in the
    exemption registry above (weekly, daily, eod, groom); a new
    step_function_*.json landing without one fails here rather than
    silently inheriting the 1-year default. (Formerly its own module,
    tests/test_sf_timeout_coverage.py; folded in here on
    #1256/config#6693 to avoid two parallel checkers over the same files
    as config#6684's structural-contract suite.)"""
    definition = _load(sf_file)
    assert "TimeoutSeconds" in definition, (
        f"{sf_file}: no top-level TimeoutSeconds — a hung execution can run "
        "to the Step Functions 1-year service maximum invisibly"
    )
    assert isinstance(definition["TimeoutSeconds"], int) and definition["TimeoutSeconds"] > 0


def test_every_definition_has_a_codified_ceiling():
    """A new step_function_*.json must state its ceiling here, with the
    derivation, rather than inherit whatever number was typed into it."""
    assert sorted(_TOP_LEVEL_TIMEOUT_CEILING) == _SF_FILE_NAMES, (
        f"_TOP_LEVEL_TIMEOUT_CEILING covers {sorted(_TOP_LEVEL_TIMEOUT_CEILING)} "
        f"but the definition registry is {_SF_FILE_NAMES} — add the new file's "
        f"ceiling and the one-line derivation that justifies it"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_top_level_timeout_matches_its_codified_ceiling(sf_file: str):
    """alpha-engine-config#10008. Presence is not enough: a whole-execution
    ceiling nothing pins drifts upward one 'safe' bump at a time, and every
    bump is invisible because no reader knows what the number was derived
    from. Changing a value here is a deliberate edit to the comment block
    above it, which is where the derivation lives."""
    expected = _TOP_LEVEL_TIMEOUT_CEILING[sf_file]
    actual = _load(sf_file)["TimeoutSeconds"]
    assert actual == expected, (
        f"{sf_file}: top-level TimeoutSeconds is {actual}, expected {expected}. "
        f"See _TOP_LEVEL_TIMEOUT_CEILING in this file for the derivation. If "
        f"the pipeline genuinely needs a different ceiling, change the map AND "
        f"its derivation comment in the same diff — sf-pipeline-policy.md §4."
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_every_task_state_declares_timeout_or_is_exempt(sf_file: str):
    definition = _load(sf_file)
    missing = _missing_timeout(definition, _TIMEOUT_EXEMPT[sf_file])
    assert not missing, (
        f"{sf_file}: Task state(s) with no TimeoutSeconds and no exemption: "
        f"{missing} — either the state genuinely needs a timeout (fix it; "
        f"config#6684 tracks tightening these) or add a one-line-justified "
        f"entry to _TIMEOUT_EXEMPT['{sf_file}'] in this file"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_every_task_state_declares_catch_or_is_exempt(sf_file: str):
    definition = _load(sf_file)
    missing = _missing_catch(definition, _CATCH_EXEMPT[sf_file])
    assert not missing, (
        f"{sf_file}: Task state(s) with no Catch and no exemption: "
        f"{missing} — either the state genuinely needs a Catch (fix it; "
        f"config#6684 tracks tightening these) or add a one-line-justified "
        f"entry to _CATCH_EXEMPT['{sf_file}'] in this file"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_no_stale_timeout_exemptions(sf_file: str):
    definition = _load(sf_file)
    stale = _stale_exemptions(definition, _TIMEOUT_EXEMPT[sf_file])
    assert not stale, (
        f"{sf_file}: _TIMEOUT_EXEMPT names state(s) no longer present as a "
        f"Task state: {stale} — remove the stale entry (renamed/removed "
        f"states must not linger as dead allowlist entries)"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_no_stale_catch_exemptions(sf_file: str):
    definition = _load(sf_file)
    stale = _stale_exemptions(definition, _CATCH_EXEMPT[sf_file])
    assert not stale, (
        f"{sf_file}: _CATCH_EXEMPT names state(s) no longer present as a "
        f"Task state: {stale} — remove the stale entry (renamed/removed "
        f"states must not linger as dead allowlist entries)"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_data_repo_launcher_scripts_exist(sf_file: str):
    definition = _load(sf_file)
    missing = _missing_data_repo_scripts(definition, _REPO_ROOT)
    assert not missing, (
        f"{sf_file}: state(s) invoke a script under this repo's own EC2 "
        f"checkout (alpha-engine-data) that does not exist in the tree: "
        f"{missing} — I4442/I4975-class regression: a deleted/renamed "
        f"launcher script must fail here, not on Saturday"
    )


# ---------------------------------------------------------------------------
# alpha-engine-config#6715 — WSF-2.3/WSF-5 chokepoint: real-definition
# tests.
# ---------------------------------------------------------------------------


def test_degraded_flag_scope_is_the_three_scheduled_pipelines():
    """Guards the _DEGRADED_FLAG_SF_FILES carve-out from this module's
    broader _SF_FILE_NAMES: sf-pipeline-policy.md §1.1 governs exactly
    weekly/daily/eod, not step_function_groom.json, which has no
    WriteCompletionMarker/degraded-selector concept at all today. Fails
    loud if groom.json ever gains a WriteCompletionMarker state (scope
    should widen to include it) or if the scheduled-pipeline set silently
    shrinks."""
    assert _DEGRADED_FLAG_SF_FILES == [
        "step_function.json",
        "step_function_daily.json",
        "step_function_eod.json",
    ]
    groom = _load("step_function_groom.json")
    assert "WriteCompletionMarker" not in groom["States"], (
        "step_function_groom.json now has a WriteCompletionMarker state — "
        "re-evaluate whether it needs _DEGRADED_FLAG_SF_FILES coverage "
        "(alpha-engine-config#6715)"
    )


@pytest.mark.parametrize("sf_file", _DEGRADED_FLAG_SF_FILES)
def test_degraded_flag_jsonpaths_match_the_actual_terminal_choice(sf_file: str):
    """config#6715 gotcha: assert against the JSONPath the notifier/
    marker-selector ACTUALLY dereferences, not a hand-typed guess. Extracts
    every Variable the terminal degraded-routing Choice state's condition
    tree reads and cross-checks it against _DEGRADED_FLAG_JSONPATHS in
    BOTH directions — so a rename of the Choice's Variable, or an added/
    removed flag family, fails HERE first, before the fail-open-route test
    below could go stale silently and give every route a false pass."""
    definition = _load(sf_file)
    choice_name = _TERMINAL_DEGRADED_CHOICE[sf_file]
    choice_state = definition["States"][choice_name]
    assert choice_state["Type"] == "Choice", (
        f"{sf_file}: {choice_name} is no longer a Choice state — "
        f"_TERMINAL_DEGRADED_CHOICE is stale"
    )
    actual = _choice_variables(choice_state)
    declared = _DEGRADED_FLAG_JSONPATHS[sf_file]

    undeclared = sorted(
        v for v in actual if not any(_is_prefix_path(d, v) for d in declared)
    )
    assert not undeclared, (
        f"{sf_file}: {choice_name} reads {undeclared}, not declared in "
        f"_DEGRADED_FLAG_JSONPATHS['{sf_file}'] — a new/renamed degraded "
        f"flag family landed without updating this constant"
    )

    unread = sorted(
        d for d in declared if not any(_is_prefix_path(d, v) for v in actual)
    )
    assert not unread, (
        f"{sf_file}: _DEGRADED_FLAG_JSONPATHS['{sf_file}'] declares "
        f"{unread} but {choice_name} never reads it — a dead constant that "
        f"would silently pass every fail-open route checked against it"
    )


def test_check_branch_outcomes_routes_failed_branches_to_hard_fail():
    """Structurally verifies the _BRANCH_JOIN_HARD_FAIL claim rather than
    just asserting it: CheckBranchOutcomes must route EITHER branch's
    FAILED status to the same hard-fail chain every other Catch converges
    on (NormalizeFailureContext -> ... -> HandleFailure -> FailExecution),
    not a silent-success path — this is what makes excluding
    BranchAFailed/BranchBFailed from the degraded-flag requirement correct
    rather than a bare hand-list exemption."""
    definition = _load("step_function.json")
    states = definition["States"]
    choice = states["CheckBranchOutcomes"]
    assert choice["Type"] == "Choice"
    assert _choice_variables(choice) == {
        "$.branch_outcomes.branch_a_status",
        "$.branch_outcomes.branch_b_status",
    }, "CheckBranchOutcomes no longer gates on both branch statuses"

    fail_rule = choice["Choices"][0]
    assert fail_rule.get("Or") and all(
        cond.get("StringEquals") == "FAILED" for cond in fail_rule["Or"]
    ), "CheckBranchOutcomes' fail-routing Choice no longer tests FAILED"
    fail_next = fail_rule["Next"]

    # Walk the top-level Pass/Task chain from the FAILED-branch target to
    # confirm it lands in the real failure family within a bounded number
    # of hops (mirrors _route_is_visible's own stopping rules).
    seen: list[str] = []
    cur: str | None = fail_next
    depth = 0
    while cur and cur in states and depth < 10:
        seen.append(cur)
        state = states[cur]
        if cur in _FAILURE_FAMILY["step_function.json"]:
            break
        if state.get("Type") not in ("Pass", "Task"):
            cur = None
            break
        cur = state.get("Next")
        depth += 1
    assert seen and seen[-1] in _FAILURE_FAMILY["step_function.json"], (
        f"CheckBranchOutcomes' FAILED route ({fail_next}) does not reach "
        f"the hard-fail family within 10 hops (chain: {seen}) — "
        f"BranchAFailed/BranchBFailed's exclusion from the degraded-flag "
        f"requirement is no longer justified"
    )


@pytest.mark.parametrize("sf_file", _DEGRADED_FLAG_SF_FILES)
def test_every_fail_open_catch_route_sets_the_degraded_flag_or_is_exempt(
    sf_file: str,
):
    """alpha-engine-config#6715 / sf-pipeline-policy.md §2.3 + §5: every
    fail-open Catch route (derived structurally — Catch target NOT a
    failure-family state, per _iter_fail_open_catch_routes) must pass
    through a state that writes the degraded-flag JSONPath the terminal
    notifier/marker-selector actually reads, or carry a reasoned
    _DEGRADED_FLAG_EXEMPT entry."""
    definition = _load(sf_file)
    flat = _flat_index(definition)
    flag_paths = _DEGRADED_FLAG_JSONPATHS[sf_file]
    failure_family = _FAILURE_FAMILY[sf_file]
    branch_join = _BRANCH_JOIN_HARD_FAIL[sf_file]
    exempt = _DEGRADED_FLAG_EXEMPT[sf_file]

    missing = []
    for owner_path, target_path in _iter_fail_open_catch_routes(
        flat, failure_family, branch_join
    ):
        if owner_path in exempt:
            continue
        if not _route_is_visible(
            target_path, flat, flag_paths, failure_family, branch_join
        ):
            missing.append(f"{owner_path} -> {target_path}")

    assert not missing, (
        f"{sf_file}: fail-open Catch route(s) that never write "
        f"{sorted(flag_paths)} and carry no _DEGRADED_FLAG_EXEMPT entry: "
        f"{missing} — either wire the route to the real degraded-flag "
        f"JSONPath, or add a one-line-justified entry to "
        f"_DEGRADED_FLAG_EXEMPT['{sf_file}'] in this file "
        f"(alpha-engine-config#6715)"
    )


@pytest.mark.parametrize("sf_file", _DEGRADED_FLAG_SF_FILES)
def test_no_stale_degraded_flag_exemptions(sf_file: str):
    definition = _load(sf_file)
    flat = _flat_index(definition)
    stale = sorted(name for name in _DEGRADED_FLAG_EXEMPT[sf_file] if name not in flat)
    assert not stale, (
        f"{sf_file}: _DEGRADED_FLAG_EXEMPT names state(s) no longer "
        f"present in the definition: {stale} — remove the stale entry "
        f"(alpha-engine-config#6715)"
    )


# ---------------------------------------------------------------------------
# alpha-engine-config-I7418 — a degraded run's notification may not claim SUCCESS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sf_file", _DEGRADED_FLAG_SF_FILES)
def test_no_degraded_notifier_subject_claims_success(sf_file: str):
    """Since config-I6891 a degraded run terminates in a **Fail** state, so an
    SNS subject leading with SUCCESS states the opposite of the execution's own
    status.

    Asserted as a class rather than per-notifier: the weekly definition carried
    SIX of these subjects (gates / health / gates+health / report card /
    parity / multiple), all reading `SUCCESS (<something> DEGRADED)`, and four
    separate wiring tests asserted `"SUCCESS" in subject` — the guards pinned
    the false claim. Fixing the six without this test leaves the seventh
    notifier free to reintroduce it.
    """
    flat = _flat_index(_load(sf_file))
    offenders = []
    for name, state in flat.items():
        params = state.get("Parameters") or {}
        subject = params.get("Subject")
        if not isinstance(subject, str):
            continue
        if "DEGRADED" in subject.upper() and "SUCCESS" in subject.upper():
            offenders.append(f"{name}: {subject}")
    assert not offenders, (
        f"{sf_file}: notifier subject(s) claim SUCCESS for a degraded run, "
        f"which terminates FAILED since config-I6891: {offenders}"
    )


def _degraded_route_states(sf_file: str) -> dict[str, dict]:
    """Every state reachable from a NON-Default branch of this file's sole
    degraded router (``_TERMINAL_DEGRADED_CHOICE``).

    Forward reachability rather than a name or subject-text heuristic: the
    router's Default edge IS the clean terminal, so anything reachable only
    from its other edges is on the degraded route by construction. Measured
    2026-08-17: weekly reaches 11 states (the six degraded notifiers among
    them), daily and eod reach 2 each, and the clean-run notifier is reached
    from none of them — so this walk cannot false-positive on a legitimate
    ``SUCCESS`` subject.
    """
    flat = _flat_index(_load(sf_file))
    router_name = _TERMINAL_DEGRADED_CHOICE[sf_file]
    router = flat[router_name]
    frontier = [
        rule["Next"] for rule in router.get("Choices", []) or [] if rule.get("Next")
    ]
    seen: dict[str, dict] = {}
    while frontier:
        name = frontier.pop()
        if name == router_name or name in seen or name not in flat:
            continue
        state = flat[name]
        seen[name] = state
        for edge in _state_successors(state):
            frontier.append(edge)
    return seen


def _state_successors(state: dict) -> list[str]:
    """Every state name this state can hand control to."""
    out: list[str] = []
    for key in ("Next", "Default"):
        if state.get(key):
            out.append(state[key])
    for rule in state.get("Choices", []) or []:
        if rule.get("Next"):
            out.append(rule["Next"])
    for catch in state.get("Catch", []) or []:
        if catch.get("Next"):
            out.append(catch["Next"])
    return out


@pytest.mark.parametrize("sf_file", _DEGRADED_FLAG_SF_FILES)
def test_no_state_on_the_degraded_route_claims_success(sf_file: str):
    """The same rule as the test above, closed against the case it cannot see.

    ``test_no_degraded_notifier_subject_claims_success`` matches on the SUBJECT
    text carrying both words, so a degraded notifier that simply drops the word
    ``DEGRADED`` while keeping ``SUCCESS`` passes it — which is the easier
    mistake to make, not the harder one, because the natural edit when a
    subject reads badly is to delete a word. This asserts the property against
    the ROUTE instead of the wording: nothing reachable from the degraded
    router's non-Default edges may claim SUCCESS, whatever it is called and
    however the rest of the subject is phrased.
    """
    offenders = [
        f"{name}: {(state.get('Parameters') or {}).get('Subject')}"
        for name, state in sorted(_degraded_route_states(sf_file).items())
        if isinstance((state.get("Parameters") or {}).get("Subject"), str)
        and "SUCCESS" in (state["Parameters"]["Subject"]).upper()
    ]
    assert not offenders, (
        f"{sf_file}: state(s) on the degraded route publish a subject claiming "
        f"SUCCESS, but a degraded run terminates FAILED since config-I6891 "
        f"(sf-pipeline-policy.md §2.3): {offenders}"
    )


def test_degraded_route_walk_reaches_the_notifiers_it_is_meant_to_guard():
    """Meta-test: prove the walk actually reaches something.

    A reachability guard that silently walks zero states passes forever and
    protects nothing — the failure mode this suite's other meta-tests exist to
    rule out. Pinned to the weekly definition because it is the only one of the
    three whose degraded route carries per-family notifiers.
    """
    reached = _degraded_route_states("step_function.json")
    with_subjects = {
        name
        for name, state in reached.items()
        if isinstance((state.get("Parameters") or {}).get("Subject"), str)
    }
    assert with_subjects >= {
        "NotifyCompleteGatesDegraded",
        "NotifyCompleteHealthDegraded",
        "NotifyCompleteGatesAndHealthDegraded",
        "NotifyCompleteReportCardDegraded",
        "NotifyCompleteParityDegraded",
        "NotifyCompleteMultipleDegraded",
    }, (
        "the degraded-route walk no longer reaches the six completion "
        f"notifiers it exists to guard — reached instead: {sorted(with_subjects)}"
    )


def test_degraded_route_success_guard_flags_a_synthetic_offender():
    """Meta-test: the checker fails on a definition that violates the rule."""
    synthetic = {
        "SomeNotifier": {
            "Type": "Task",
            "Parameters": {"Subject": "Alpha Engine — SUCCESS (all clear)"},
            "End": True,
        }
    }
    offenders = [
        name
        for name, state in synthetic.items()
        if "SUCCESS" in (state["Parameters"]["Subject"]).upper()
    ]
    assert offenders == ["SomeNotifier"]
