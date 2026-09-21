#!/usr/bin/env python3
"""Mechanical weekly-SF recovery helper (config#2277).

Derives, from a FAILED ``ne-weekly-freshness-pipeline`` execution, the exact
``StartExecution`` input for a correctly-scoped recovery rerun:

- the ORIGINAL ``run_date`` (from the failed execution's ``InitializeInput``
  output — a fresh manual rerun without an explicit run_date gets a NEW date
  stamped from Execution.StartTime and writes to different artifact prefixes,
  orphaning the prior partial run);
- the derived ``skip_*`` flag set for every stage the recovery CHAIN
  completed CLEANLY — the scheduled run plus every prior
  ``watch-rerun-<run_date>-N``, resolved LATEST-ATTEMPT-WINS
  (alpha-engine-config-I8161: deriving from the latest failed execution alone
  means deriving from the last RERUN, whose history contains only the stages
  it did not skip, so each successive recovery lost more of the original
  run's progress). Read from execution HISTORIES, never from a previous
  input's flags, which is what keeps it immune to
  alpha-engine-config-I7259 (re-running a succeeded side-effecting stage duplicates
  its effects — 2026-07-11: duplicate model-zoo promotion emails,
  config#2252). A stage that DEGRADED — ran, failed, and was absorbed by a
  ``Publish*Degraded`` route so the pipeline could continue (alpha-engine-
  config-I6055: observed 2026-08-01 when the Director hard-failed on
  ``No module named 'openai'`` but the run kept going) — is recorded as
  degraded and NEVER skipped: it is exactly the thing a mechanical rerun
  exists to retry, and skipping it would make the rerun's green
  indistinguishable from a real one;
- ``pipeline_role="watch-rerun"`` (see ROLE GATING below);
- ``sns_topic_arn`` / ``ec2_instance_id`` passthrough (the emitted input
  starts from the failed execution's own input, so both carry over).

``--dry-run`` (default) prints the derived plan + input; ``--start`` runs the
pre-start guards (mutex steal, running-execution check) and starts the
execution under the ``watch-rerun-{run_date}-{n}`` naming convention — the
name the saturday-sf-watch dispatcher's operator-recovery suppression keys on
(config#2003 / data-PR705: this script and that suppression are two halves of
one contract).

ROLE GATING (config#2277 deliverable 2)
---------------------------------------
Verified against the live definition at runtime (``_verify_skip_flags_live``):
unlike the EOD SF — whose skip gates are structurally conjunct on
``pipeline_role == "operator-replay"`` (config#1614) — the weekly SF's
``CheckSkip*`` gates test ONLY the flag itself, so skip flags are live under
ANY pipeline_role. The script emits ``pipeline_role="watch-rerun"`` (the SF's
own documented recovery-role convention) because the two states that DO
consume the role make cadence roles actively wrong for a recovery rerun:

- ``CheckWeeklyRunDayGate``: role ``weekly`` triggers the NYSE run-day gate —
  a Sunday/Monday recovery under role ``weekly`` would silently Succeed-skip
  the whole pipeline (observed latent bug: the 2026-07-11 watch reruns
  carried role ``weekly`` and only ran because they happened on Saturday).
- ``CheckMutexRole``/``AcquireMutex``: cadence roles acquire the run-slot
  mutex (config#2280) — role ``weekly`` would ConditionalCheckFail against
  the failed run's own stale item. Role ``watch-rerun`` bypasses the mutex
  entirely (operator-initiated runs are deliberately concurrent by design).

If the weekly SF ever adopts EOD-style role-gated skip flags without
including ``watch-rerun`` in the live set, ``_verify_skip_flags_live`` fails
LOUDLY instead of emitting inert flags (a helper that silently emits inert
skip flags re-burns every completed spot stage — worse than no helper).

MUTEX INTERPLAY (the config#2280 contract)
------------------------------------------
The weekly mutex keys on the RUN-SLOT ``{SM-name}#{pipeline_role}#{run_date}``
with a ~24h ttl_epoch backstop. This script is the PRIMARY stale-item
mechanism: before ``--start`` it looks up the failed execution's own run-slot
item and applies the decision matrix (``decide_mutex_action``):

- no item                      -> proceed (nothing held);
- holder RUNNING               -> ABORT — never steal from, or rerun beside,
                                  a live execution;
- holder SUCCEEDED             -> ABORT — the run-slot's work actually
                                  completed (duplicate-trigger loser shape);
                                  a rerun would duplicate the week's
                                  artifacts. Operator judgment required;
- holder terminal-failed       -> STEAL: delete the stale item, loudly naming
  (FAILED/TIMED_OUT/ABORTED)      what was deleted and why it is safe (the
                                  holder can no longer write artifacts);
- item present, no holder arn  -> ABORT with the manual delete command;
- DDB AccessDenied             -> WARN + print the manual delete command +
                                  proceed. Deliberate non-fatal swallow
                                  (feedback_no_silent_fails rationale): the
                                  rerun itself bypasses the mutex (role
                                  ``watch-rerun``), so the stale item is
                                  hygiene, not a correctness gate; the
                                  running-execution guard below still blocks
                                  the unsafe case; recording surface = loud
                                  stderr WARN + the printed manual command.

Independently of DynamoDB, ``--start`` ABORTS if ANY execution of the state
machine is currently RUNNING with the same effective run_date (that is the
actual double-writer hazard, and it needs no DDB permissions).

Read-only by default; nothing is mutated without ``--start``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# sys.path insertion (not a package import) so this resolves identically
# whether run directly (`python scripts/weekly_sf_rerun.py`) or loaded by
# spec_from_file_location the way tests/test_weekly_sf_rerun.py does.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sf_rerun_common import (  # noqa: E402 — see sys.path insertion above
    derive_calendar_date,
    derive_run_date,
    effective_run_date_of,
    entered_states,
    execution_input,
    fetch_history,
    list_all_executions,
    verify_skip_flags_live,
    _walk_states,  # noqa: F401 — re-exported: tests/test_weekly_sf_rerun.py calls mod._walk_states directly
)

# The definition-derived vacuity analysis (alpha-engine-config-I11075) lives
# with the other definition derivations, in the run-scope Lambda module — the
# same sys.path-insert shape scripts/fault_injection_run.py uses for
# groom-inject-mock. It is imported, never re-implemented: a second copy of a
# reachability walk is a second thing to keep true of the live definition.
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent
        / "infrastructure" / "lambdas" / "weekly-run-scope"),
)
from run_scope import (  # noqa: E402 — see sys.path insertion above
    disabling_flags_for_input,
    enabled_spine_stages,
)
from nousergon_lib.pipeline_status.registry import (  # noqa: E402
    pending_definition_stages_for,
    stage_order_for,
)


def _landed_spine(sm_arn: str) -> tuple[str, ...]:
    """The declared spine, MINUS stages the pinned library has declared ahead
    of this repo's own definitions (PENDING_DEFINITION_STAGES).

    alpha-engine-config-I11267 added ``PENDING_DEFINITION_STAGES`` precisely
    so a consumer repo is not forced into merge-order lockstep with the
    library — a pending stage the pinned lib names is one this repo has not
    landed a state for YET, and a stage that cannot exist cannot be entered.
    Feeding the RAW ``stage_order_for()`` tuple into the vacuity guard would
    count that stage as part of "what a substantive run must enter" and
    understate every real execution's coverage by exactly the number of
    not-yet-landed stages — a false vacuity/coverage refusal, the identical
    failure mode this guard exists to prevent, just introduced from the other
    direction. The library's OWN ``undefined_spine_stages`` already applies
    this same tolerance (`tests/test_pipeline_stage_order_contract.py` pins
    it); this mirrors it for the two vacuity-guard call sites in this script.

    Deliberately does NOT also exclude ``RETIRING_DEFINITION_STAGES``: a
    retiring stage is the OPPOSITE direction — its definition "may be either
    still present or already dropped" (registry.py's own docstring), i.e. it
    is real and enterable TODAY and only tolerated as ABSENT once the repo
    actually removes it. Excluding it here would understate today's
    substantive spine by a stage the live definition still has, which a
    first cut of this fix did (MorningEnrich/DataPhase1 are the two live
    RETIRING_DEFINITION_STAGES entries for this pipeline right now) —
    measured via test_every_declared_spine_stage_is_switchable_off's
    hardcoded count regressing from 17 to 15.
    """
    spine = stage_order_for(sm_arn)
    pending = pending_definition_stages_for(sm_arn)
    return tuple(s for s in spine if s not in pending)


DEFAULT_STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
)
MUTEX_TABLE = "alpha-engine-sf-execution-mutex"
EMITTED_ROLE = "watch-rerun"
# Roles that acquire the run-slot mutex (CheckMutexRole allowlist — kept in
# lockstep by tests/test_weekly_sf_rerun.py against the SF definition).
CADENCE_ROLES = frozenset({"daily", "weekly", "eod", "shell-run", "exercise"})
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})
RERUNNABLE_SOURCE_STATUSES = frozenset({"FAILED", "TIMED_OUT", "ABORTED"})

# The CADENCE trigger's declared input — the single source of truth for the
# scheduled run's own skip set (tests/test_saturday_trigger_skip_parity.py
# pins that this is where it lives, and that a disable here names its
# re-enable issue).
_CFN_ORCHESTRATION = (
    Path(__file__).resolve().parent.parent
    / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
)


# ---------------------------------------------------------------------------
# Declarative stage table — pinned against infrastructure/step_function.json
# by tests/test_weekly_sf_rerun.py (witness = the state the SF enters iff the
# stage completed successfully OR was skipped; either way the rerun must not
# re-run it, and originally-skipped stages carry their flag from the
# preserved original input anyway). degraded_witness = a *Degraded /
# Publish*Degraded state entered iff the stage ran but FAILED and was
# absorbed fail-open so the pipeline could continue (weekly-sf-policy §2.3);
# entering one OVERRIDES witness: the stage is re-run, never skipped — the
# whole point of a mechanical rerun is to retry exactly what degraded
# (alpha-engine-config-I6055).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Stage:
    name: str
    flag: str
    gate: str                      # the CheckSkip* Choice state
    work: str                      # the stage's first work state
    witness: frozenset             # entered => completed-or-skipped
    degraded_witness: frozenset = frozenset()
    # A *Degraded / Publish*Degraded state entered iff the stage RAN BUT
    # FAILED and was absorbed fail-open (weekly-sf-policy §2.3) so the
    # pipeline could continue — entering one OVERRIDES witness: the stage
    # must RE-RUN on a rerun, never be skipped as complete
    # (alpha-engine-config-I6055 — the 2026-08-01 Director hard-fail that
    # the next rerun skipped; extended to Parity by alpha-engine-config-I6025).
    emit_skip: bool = True         # False => never emit the flag (see notes)
    historical_work: frozenset = frozenset()
    # Work-state names this stage carried in a PRIOR SF definition. Failure
    # detection reads `work in entered`, and this script runs against captured
    # execution histories — including ones written before a rename or a split.
    # Without this, renaming a work state silently blinds the rerun planner to
    # every failure in every history older than the rename: the stage stops
    # appearing in plan.failed, and derive_plan's own "no failed or degraded
    # WORK stage identified" warning is the only trace, which reads as a
    # pre-workload failure rather than as a lost signal. Same reasoning as
    # HISTORICAL_DEGRADED_WITNESS below, applied to the work state.
    detect_failure: bool = True    # False => another Stage row already owns
                                    # this `work` state's failure detection
                                    # (config#2362: skip_backtester_stage_only
                                    # shares Backtester's work state with the
                                    # "backtester" row above it)
    note: str = ""


# Degraded states that existed in a prior SF definition but have since been
# removed. Retained in degraded_witness for backward compatibility with pre-fix
# execution histories — derive_plan still needs to recognise them as DEGRADED
# (not completed) so they're re-run, but they're no longer reachable by new
# executions (config#6408).
HISTORICAL_DEGRADED_WITNESS: frozenset[str] = frozenset({
    "PublishDirectorDegraded",
})

# Degraded-named states that belong to the TERMINAL degraded-completion family
# rather than to any stage (alpha-engine-config-I6891). Entering one says the
# run ended degraded — which is the whole execution's verdict, not a signal
# that some stage must be re-run — so mapping them to a Stage would make every
# degraded run re-run an arbitrary stage. Same reasoning that already excludes
# the Notify*Degraded family, extended to the terminal states I6891 added.
TERMINAL_DEGRADED_FAMILY: frozenset[str] = frozenset({
    "CheckGateDegradedNotify",
    "CheckDegradedOutcome",
    "CheckShellRunDegradedOutcome",
    "WriteCompletionMarkerDegraded",
    # alpha-engine-config-I8809: the legacy-partition COPY of the degraded
    # envelope marker, written for the migration window only. Same terminal
    # family as its canonical twin above and for the same reason — it records
    # a run that already ended, and re-running a stage on the strength of it
    # would re-run the whole pipeline. Deleted at the 2026-09-05 cutover.
    "WriteCompletionMarkerDegradedCalendar",
    "DegradedRun",
})

STAGES: tuple[Stage, ...] = (
    Stage(
        "lib_pin_drift_check", "skip_lib_pin_drift_check",
        "CheckSkipLibPinDriftCheck", "LibPinDriftCheck",
        frozenset({"PipelineContractCheck"}),
        # Witness is the first sibling gate the chain re-enters on either the
        # skip path (I4494: skipping lib-pin no longer jumps straight to
        # CheckMutexRole — it re-enters at PipelineContractCheck) or the clean
        # pass path (LibPinDriftGate.Default). The whole pre-workload gate
        # chain (lib-pin / pipeline-contract / evaluator / evaluator-director
        # drift gates, config#2278 + config#2348) degrades fail-open through
        # these Pass+Publish pairs — no skip flag is emitted either way
        # (emit_skip=False), but the summary must say "degraded", not
        # "completed", when one was hit.
        # alpha-engine-config-I7302 added the *GateDegradedFromProbe /
        # *GateDegradedFromError entry pair per gate: the cause-recording
        # normalizers the Choice absence arm and the Task Catch now route
        # through on their way to the unchanged *GateDegraded Pass. They are
        # part of the same degraded route, so they belong in the same witness
        # set — a run that stopped ON one of them is a degraded gate, not a
        # completed one.
        degraded_witness=frozenset({
            "LibPinGateDegradedFromProbe", "LibPinGateDegradedFromError",
            "LibPinGateDegraded", "SetLibPinGateDegradedSummary", "PublishLibPinGateDegraded",
            "PipelineContractGateDegradedFromProbe", "PipelineContractGateDegradedFromError",
            "PipelineContractGateDegraded", "SetPipelineContractGateDegradedSummary",
            "PublishPipelineContractGateDegraded",
            "EvaluatorGateDegradedFromProbe", "EvaluatorGateDegradedFromError",
            "EvaluatorGateDegraded", "SetEvaluatorGateDegradedSummary",
            "PublishEvaluatorGateDegraded",
            "EvaluatorDirectorGateDegradedFromProbe", "EvaluatorDirectorGateDegradedFromError",
            "EvaluatorDirectorGateDegraded", "SetEvaluatorDirectorGateDegradedSummary",
            "PublishEvaluatorDirectorGateDegraded",
            # alpha-engine-config#6722: AcquireMutex's mutex-acquire
            # infra-error fail-open (DynamoDB outage/IAM drift/transient SDK
            # error — the SEPARATE ConditionalCheckFailedException conflict
            # case still hard-Fails via MutexConflict) is the same KIND of
            # pre-workload precondition check as the four gate pairs above,
            # even though it sits later in the chain (after
            # EvaluatorDeployDriftCheck) — folded into this same bucket
            # rather than a new single-purpose Stage row.
            "SetMutexAcquireDegradedFlag", "SetMutexAcquireDegradedFlagSummary",
            "PublishMutexAcquireDegraded",
            # alpha-engine-config-I11112: WeeklyPreflightBlindSpotDeclared and
            # its Pass/Publish siblings are DELIBERATELY NOT here (and not
            # named "*Degraded") — a capability gap this probe cannot reach
            # is a VERDICT about the environment (mirrors
            # ModelZooUnservableDeclared, sf-pipeline-policy.md §2.3a), not a
            # stage degradation, and a rerun has nothing to redo on the
            # strength of it: the gap is a static fact about the Lambda's
            # environment that a rerun does not change. See
            # WeeklyPreflightGate's Choice comment in step_function.json.
        }),
        emit_skip=False,
        note=(
            "deliberately NEVER skipped on a rerun: the lib-pin drift +"
            " pipeline-contract pair are cheap, side-effect-free Lambda"
            " checks that re-validate exactly the environment a recovery"
            " fix most likely touched (lib pin bumps / redeploys)."
        ),
    ),
    Stage(
        "morning_enrich", "skip_morning_enrich",
        "CheckSkipMorningEnrich", "MorningEnrich",
        frozenset({"CheckSkipDataPhase1"}),
    ),
    Stage(
        "data_phase1", "skip_data_phase1",
        "CheckSkipDataPhase1", "DataPhase1",
        frozenset({"ResearchPredictorParallel"}),
    ),
    # --- ResearchPredictorParallel branch A -------------------------------
    # config#3134: Scanner, SignalsEnvelope, ChallengerShadow, and
    # ThinkTankCoverage each got their own CheckSkip* gate (previously NONE
    # of the four had one — every partial rerun unconditionally re-scanned,
    # re-called the ChallengerShadow producer, and re-attempted ThinkTank's
    # gap_fill thesis generation regardless of flags). skip_signals_envelope
    # DEFAULTS FALSE at the SF layer (SignalsEnvelope is LOAD-BEARING, I2880
    # staleness guard) — this helper still emits it like any other completed
    # stage's flag on a rerun, which is safe: a rerun that witnessed
    # SignalsEnvelope already ran successfully this run_date, so skipping a
    # second identical invocation does not create staleness the executor
    # would ever observe.
    Stage(
        "scanner", "skip_scanner",
        "CheckSkipScanner", "Scanner",
        frozenset({"CheckSkipRegimeSubstrate"}),
        # alpha-engine-config#6722: Scanner's fail-open Catch previously set
        # no flag at all — MarkScannerDegraded now threads
        # $.research_degraded_local (folded into the top-level
        # $.research_predictor_degraded post-join) without changing the
        # continuation.
        # alpha-engine-config-I7812: a Scanner RESOURCE KILL (States.Timeout /
        # Lambda.Unknown) that is allowed to fail-open — i.e. the run's
        # universe_membership pointer was proven fresh before the kill —
        # continues to the SAME CheckSkipRegimeSubstrate convergence point via
        # ScannerResourceKillDegraded, so it is a degraded witness exactly like
        # MarkScannerDegraded. The two HALT routes (ScannerResourceKillHalt,
        # ScannerMembershipProbeUnknownHalt) are deliberately NOT witnesses: they
        # fail the branch, and a rerun must re-run the scan.
        degraded_witness=frozenset({
            "MarkScannerDegraded",
            "ScannerResourceKillDegraded",
            "SetScannerResourceKillDegradedSummary",
        }),
    ),
    Stage(
        "regime_substrate", "skip_regime_substrate",
        "CheckSkipRegimeSubstrate", "RegimeSubstrate",
        frozenset({"CheckSkipSignalsEnvelope"}),
        degraded_witness=frozenset({"MarkRegimeSubstrateDegraded"}),
    ),
    Stage(
        "signals_envelope", "skip_signals_envelope",
        "CheckSkipSignalsEnvelope", "SignalsEnvelope",
        # alpha-engine-config-I7726 inserted CheckSkipResearchSelfTest between
        # SignalsEnvelope and CheckSkipChallengerShadow. BOTH are kept as
        # witnesses: this helper derives a rerun plan from the history of a PAST
        # execution, and every execution recorded before that change witnesses
        # SignalsEnvelope's completion by entering CheckSkipChallengerShadow.
        # Replacing rather than adding made `derive_plan` report signals_envelope
        # as FAILED on every pre-change history — which is the exact class
        # TestHistoricalWorkStateNames exists to catch, and it caught it.
        frozenset({"CheckSkipResearchSelfTest", "CheckSkipChallengerShadow"}),
        note=(
            "SignalsEnvelope is LOAD-BEARING for a real weekly run (I2880"
            " staleness guard; the executor hard-fails Monday without a"
            " fresh signals.json) — its SF gate defaults false. This"
            " helper only emits skip_signals_envelope=true when the failed"
            " execution's history shows SignalsEnvelope already ran (this"
            " witness), which is always safe to skip on the rerun."
        ),
    ),
    Stage(
        # alpha-engine-config-I7726 — the module's own correctness verdict.
        # No degraded_witness: its fail-open Catch deliberately sets no degraded
        # flag (the battery never raises, so a Catch means the INVOCATION failed,
        # and folding that would terminate a run that produced real trading
        # artifacts). Its absence is detected by the freshness monitor on
        # research/{date}/self_test.json instead — see the state's Catch comment
        # and the _DEGRADED_FLAG_EXEMPT entry in tests/test_sf_structural_contract.py.
        "research_self_test", "skip_research_self_test",
        "CheckSkipResearchSelfTest", "ResearchSelfTest",
        frozenset({"CheckSkipChallengerShadow"}),
    ),
    Stage(
        "challenger_shadow", "skip_challenger_shadow",
        "CheckSkipChallengerShadow", "ChallengerShadow",
        frozenset({"CheckSkipRAGIngestion"}),
        degraded_witness=frozenset({"MarkChallengerShadowDegraded"}),
    ),
    Stage(
        "rag_ingestion", "skip_rag_ingestion",
        "CheckSkipRAGIngestion", "RAGIngestion",
        frozenset({"CheckSkipRegimeRetrospectiveEval"}),
    ),
    Stage(
        "regime_retrospective_eval", "skip_regime_retrospective_eval",
        "CheckSkipRegimeRetrospectiveEval", "RegimeRetrospectiveEval",
        frozenset({"CheckSkipDataPhase2"}),
        degraded_witness=frozenset({"MarkRegimeRetrospectiveEvalDegraded"}),
    ),
    Stage(
        "data_phase2", "skip_data_phase2",
        "CheckSkipDataPhase2", "DataPhase2",
        frozenset({"CheckSkipEvalJudge"}),
    ),
    Stage(
        "eval_judge", "skip_eval_judge",
        "CheckSkipEvalJudge", "ComputeEvalCadence",
        frozenset({"CheckSkipRationaleClustering"}),
        # alpha-engine-config#6722: MarkEvalJudgeDegraded is the shared
        # convergence for all four submit/poll/process fail-opens;
        # MarkEvalRollingMeanDegraded covers EvalRollingMean's own Catch —
        # both sit between this stage's gate and its witness, so both
        # belong to this row (there is no separate eval_rolling_mean row).
        degraded_witness=frozenset({
            "MarkEvalJudgeDegraded", "MarkEvalRollingMeanDegraded",
        }),
    ),
    # alpha-engine-config-I10539 (Brian ruling 2026-09-16, option b): the
    # replay_concordance stage was retired from the weekly SF, so
    # rationale_clustering's successor gate is now CheckSkipCounterfactual.
    Stage(
        "rationale_clustering", "skip_rationale_clustering",
        "CheckSkipRationaleClustering", "RationaleClustering",
        frozenset({"CheckSkipCounterfactual"}),
        degraded_witness=frozenset({"MarkRationaleClusteringDegraded"}),
    ),
    Stage(
        "counterfactual", "skip_counterfactual",
        "CheckSkipCounterfactual", "Counterfactual",
        # alpha-engine-config-I7194: was CheckSkipAggregateCosts until the
        # aggregator left this branch for the top level. Counterfactual is now
        # the last work state of Branch A, so its success edge, its Catch fold
        # and the skip route all land on BranchAComplete.
        frozenset({"BranchAComplete"}),
        degraded_witness=frozenset({"MarkCounterfactualDegraded"}),
    ),
    # --- ResearchPredictorParallel branch B -------------------------------
    Stage(
        "predictor_training", "skip_predictor_training",
        "CheckSkipPredictorTraining", "PredictorTraining",
        # ResolveZooSpecs entered <=> training succeeded (model-zoo rotation
        # downstream is best-effort and cannot hard-fail the branch);
        # PredictorTrainingSkipped <=> skip flag honored after the
        # ValidatePredictorSkipWeightsFresh freshness proof. On the rerun
        # the SF re-proves weights/meta freshness for run_date before
        # honoring the flag — the helper does not need to.
        frozenset({"ResolveZooSpecs", "PredictorTrainingSkipped"}),
        note=(
            "skip_predictor_training also skips the best-effort model-zoo"
            " rotation (the flag ends branch B; zoo has no separate gate)."
        ),
        # alpha-engine-config#6722: the model-zoo rotation group's five
        # fail-open Catches (ResolveZooSpecs/WaitResolveZoo/ModelZooTrainMap/
        # ModelZooSelect/WaitForModelZoo) all converge on ONE shared
        # MarkModelZooDegraded. ACCEPTED trade-off (same granularity limit
        # this row's note already documents — zoo has no separate gate): a
        # degraded rotation marks the WHOLE predictor_training stage
        # degraded, so the rerun sets skip_predictor_training=false and
        # re-trains the champion even though training itself already
        # succeeded — there is no finer lever to retry only the rotation.
        # Correct-but-wasteful is preferred over the pre-#6722 status quo of
        # the rerun silently skipping a degraded rotation as fully clean.
        degraded_witness=frozenset({"MarkModelZooDegraded"}),
    ),
    # --- post-parallel tail ------------------------------------------------
    Stage(
        "backtester", "skip_backtester",
        "CheckSkipBacktester", "Backtester",
        frozenset({"CheckSkipPredictorBacktest"}),
        note=(
            "skip_backtester's skip route jumps straight to"
            " CheckSkipEvaluator (legacy whole-pair semantics), bypassing"
            " the predictor-backtest / portfolio-optimizer / parity gates."
        ),
    ),
    Stage(
        # config#2362 Option A (operator-ruled 2026-07-21): the additive
        # stage-only skip gate CheckSkipBacktesterStageOnly, inserted
        # between CheckSkipBacktester and the Backtester task itself. It
        # shares Backtester's `work` state with the "backtester" row above,
        # so it carries empty witness + detect_failure=False — completion
        # and failure for the physical Backtester task are detected exactly
        # once, by the "backtester" row. This row exists purely so (a) the
        # TestStageTableLockstep completeness guard sees the new gate
        # covered and (b) _simulate_reachable_works can look up
        # effective["backtester_stage_only"] from plan.skip_flags /
        # original_input like any other flag. derive_plan sets
        # skip_backtester_stage_only explicitly (see the BACKTESTER_OVERSHADOWED
        # replacement logic below) rather than via witness-driven emission.
        "backtester_stage_only", "skip_backtester_stage_only",
        "CheckSkipBacktesterStageOnly", "Backtester",
        frozenset(),
        emit_skip=False,
        detect_failure=False,
    ),
    Stage(
        "predictor_backtest", "skip_predictor_backtest",
        "CheckSkipPredictorBacktest", "PredictorBacktest",
        frozenset({"CheckSkipPortfolioOptimizerBacktest"}),
    ),
    Stage(
        "portfolio_optimizer_backtest", "skip_portfolio_optimizer_backtest",
        "CheckSkipPortfolioOptimizerBacktest", "PortfolioOptimizerBacktest",
        frozenset({"CheckSkipParity"}),
    ),
    # --- parity family (alpha-engine-config#6030 split) --------------------
    Stage(
        # Family gate row: CheckSkipParity bypasses the WHOLE parity family
        # (ParityParallel + PitParityCompare). The four fine-grained rows
        # below own witness/failure detection and skip emission, so this row
        # emits nothing and detects nothing — it exists so the lockstep
        # completeness guard sees the gate covered, and so a post-join
        # branch-degraded fold (ParityDegraded/PublishParityDegraded, which
        # no single fine-grained row owns) is still reported as degraded.
        "parity", "skip_parity",
        "CheckSkipParity", "ParityParallel",
        # CheckSkipEvaluator is only reachable through/past the parity
        # family (every family path converges there), so it remains a valid
        # completed-or-skipped witness — INCLUDING for pre-#6030 execution
        # histories whose Parity/WaitForParity states no longer exist. Any
        # degraded state in the family overrides it (degraded beats
        # completed), so a compare-degraded or branch-degraded run never
        # emits skip_parity.
        frozenset({"CheckSkipEvaluator"}),
        degraded_witness=frozenset({
            "ParityDegraded", "SetParityDegradedSummary", "PublishParityDegraded",
        }),
        emit_skip=False,
        detect_failure=False,
        note=(
            "family row (alpha-engine-config#6030): skip_parity bypasses the"
            " WHOLE family but is NEVER auto-emitted — the fine-grained rows"
            " below own per-branch emission and failure detection, so a"
            " single failed branch reruns ALONE (the #6030 closes-when)."
            " Witness CheckSkipEvaluator keeps 'completed' reporting valid"
            " for pre-#6030 execution histories; a rerun of such a history"
            " re-runs the parity family (conservative and honest — the old"
            " bundled artifacts cannot witness the new per-stage set)."
        ),
    ),
    Stage(
        # Branch rows: witness = the branch's own Complete/Skipped terminal
        # (entered => completed-or-skipped); degraded = the branch's own
        # fail-open Degraded terminal (entered => ran-and-failed => must
        # RE-RUN, never be skipped as completed — I6025 extended per-branch).
        "pit_parity_lookahead", "skip_pit_parity_lookahead",
        "CheckSkipPitParityLookahead", "PitParityLookahead",
        frozenset({"PitParityLookaheadComplete", "PitParityLookaheadSkipped"}),
        degraded_witness=frozenset({"PitParityLookaheadDegraded"}),
    ),
    Stage(
        "pit_parity_walkforward", "skip_pit_parity_walkforward",
        "CheckSkipPitParityWalkforward", "PitParityWalkforward",
        frozenset({"PitParityWalkforwardComplete", "PitParityWalkforwardSkipped"}),
        degraded_witness=frozenset({"PitParityWalkforwardDegraded"}),
    ),
    Stage(
        "parity_replay", "skip_parity_replay",
        "CheckSkipParityReplay", "ParityReplay",
        frozenset({"ParityReplayComplete", "ParityReplaySkipped"}),
        degraded_witness=frozenset({"ParityReplayDegraded"}),
    ),
    Stage(
        "pit_parity_compare", "skip_pit_parity_compare",
        "CheckSkipPitParityCompare", "PitParityCompare",
        frozenset({"PitParityCompareComplete"}),
        degraded_witness=frozenset({
            "ParityCompareDegraded", "SetParityCompareDegradedSummary",
            "PublishParityCompareDegraded",
        }),
        note=(
            "the compare join emits verdict UNKNOWN (never pass) when a pass"
            " artifact is missing (§2.3a); skipping it on a rerun is only"
            " legal when it already completed for this run_date."
        ),
    ),
    Stage(
        "evaluator", "skip_evaluator",
        # config-I3112 deliverable 3: one Evaluator state became
        # EvaluatorDiagnostics -> EvaluatorOptimize. `work` is the FIRST half
        # deliberately, and one row still covers both:
        #   * skip_evaluator gates the pair at CheckSkipEvaluator, so the flag
        #     semantics are unchanged — it was never per-half.
        #   * the witness (CheckSkipPostEval) is reached only after BOTH halves
        #     succeed, so a failure in either leaves it un-entered.
        #   * detect_failure reads `work in entered`, and EvaluatorDiagnostics
        #     is entered on every path that reaches the optimize half — so a
        #     failure in the SECOND half is still attributed to this stage and
        #     still re-runs the pair.
        # A second row would double-count the same stage without changing any
        # outcome; the halves are not independently re-runnable anyway, because
        # optimize consumes the S3 snapshot diagnostics produces.
        "CheckSkipEvaluator", "EvaluatorDiagnostics",
        frozenset({"CheckSkipPostEval"}),
        # Pre-2026-08-11 histories entered the merged "Evaluator" state; a
        # rerun derived from one of those must still attribute the failure.
        historical_work=frozenset({"Evaluator"}),
    ),
    # config#6054: the coarse post_eval span is SPLIT — ReportCard/Director
    # got their own rows below. alpha-engine-config-I8167 splits it FURTHER:
    # the two health checks now sit behind their OWN gate
    # (CheckSkipSaturdayHealthCheck / skip_saturday_health_check, this row),
    # reached one hop downstream of the deprecated whole-tail
    # CheckSkipPostEval alias (the post_eval row right below). Root cause:
    # skip_post eval could never be the flag the I8162 spot-dispatch bypass
    # emits, because setting it ALSO bypasses ReportCard/Director/
    # ScannerLeaderboard — flags a mechanical recovery must never silently
    # set. This row owns emission (emit_skip=True, the default): a recovery
    # chain that witnessed both health checks complete (RunScope entered)
    # emits skip_saturday_health_check=true, the flag CheckSpotDispatchNeeded's
    # bypass conjunction now tests instead of skip_post_eval.
    Stage(
        "saturday_health_check", "skip_saturday_health_check",
        "CheckSkipSaturdayHealthCheck", "SaturdayHealthCheck",
        frozenset({"RunScope"}),
        # Same degraded routes the coarse post_eval row used to own — a
        # health-check degradation is fail-open and continues to RunScope,
        # so entering one of these must still force a re-run rather than
        # read as "completed" (I6055). Ownership moved here from post_eval;
        # never duplicate a degraded route across two rows
        # (test_every_degraded_state_is_mapped enforces exactly one owner).
        degraded_witness=frozenset({
            "SaturdayHealthCheckDegraded", "SetSaturdayHealthCheckDegradedSummary",
            "SubstrateHealthCheckDegraded", "SetSubstrateHealthCheckDegradedSummary",
        }),
    ),
    Stage(
        "post_eval", "skip_post_eval",
        "CheckSkipPostEval", "SaturdayHealthCheck",
        # alpha-engine-config-I7194: was CheckShellRunNotify. The whole-tail
        # skip route now lands one state earlier, on the cost-aggregation
        # gate — skip_post_eval bypasses the post-Evaluator ADVISORIES and
        # has never bypassed cost telemetry, which carries its own
        # skip_aggregate_costs flag.
        frozenset({"CheckSkipAggregateCosts"}),
        emit_skip=False,
        # detect_failure=False (alpha-engine-config-I8167): shares its `work`
        # state with "saturday_health_check" above, which now owns
        # completion/failure/degraded detection for the physical
        # SaturdayHealthCheck/WeeklySubstrateHealthCheck pair — same shared-work
        # convention as "backtester_stage_only" sharing "backtester"'s work.
        # A second row detecting the same work state would double-count it
        # without changing any outcome.
        detect_failure=False,
        note=(
            "skip_post_eval is a DEPRECATED whole-tail alias (config#6054, "
            "config-I8167): the deriver emits skip_report_card / "
            "skip_director / skip_saturday_health_check instead. Kept in "
            "the DEFINITION only for in-flight inputs that still set it — "
            "its meaning (bypass the entire post-Evaluator advisory tail, "
            "including ReportCard/Director/ScannerLeaderboard, but NOT the "
            "cost aggregation behind its own gate) is UNCHANGED. Remove "
            "the alias once a full cycle has passed on the split flags."
        ),
    ),
    Stage(
        "report_card", "skip_report_card",
        "CheckSkipReportCard", "ReportCard",
        # Success-only witness: ReportCard's success edge now lands on
        # CheckSkipDirector (config#6054). Pre-split histories entered
        # "Director" directly on ReportCard success. A skipped ReportCard
        # ALSO enters CheckSkipDirector — same convention as evaluator's
        # CheckSkipPostEval witness: a skip in the original run implies a
        # completion the earlier run derived.
        frozenset({"CheckSkipDirector", "Director"}),
        # config#6685: ReportCardDegraded is the Pass state ReportCard's
        # Catch routes to FIRST (sets $.report_card_degraded), then
        # PublishReportCardDegraded — degraded overrides witness (I6055).
        degraded_witness=frozenset({
            "ReportCardDegraded", "SetReportCardDegradedSummary",
            "PublishReportCardDegraded",
        }),
    ),
    Stage(
        "director", "skip_director",
        "CheckSkipDirector", "Director",
        # DirectorComplete is entered ONLY via Director's success edge
        # (config#6054) — deliberately NOT CheckShellRunNotify, which every
        # bypass path also enters: witnessing on it would mark a bypassed
        # Director complete and skip it on the rerun (the I6055 trap, and
        # the exact 2026-08-01 incident). Pre-split histories have no
        # success-only witness, so the deriver conservatively RE-RUNS
        # Director for them — re-running the advisory is the safe
        # direction.
        frozenset({"DirectorComplete"}),
        # config#6408: Director failure is TERMINAL (NormalizeFailureContext
        # → FailExecution) — no witness is entered, so the stage re-runs.
        # PublishDirectorDegraded is RETAINED for pre-fix execution
        # histories only; new executions never enter it.
        # alpha-engine-config-I11299: DirectorSubStatusDegraded is the §2.3b
        # fold — the stage SUCCEEDED and wrote its plan while one of its
        # named legs errored. It is a degraded witness, not a re-run
        # trigger: DirectorComplete is still entered on that route, so the
        # deriver reads the stage as complete. Listed so the lockstep test
        # sees every *Degraded state on the Director path, which is what
        # stops a future fold from silently meaning "re-run the advisory".
        degraded_witness=frozenset({
            "PublishDirectorDegraded", "DirectorSubStatusDegraded",
            "SetDirectorSubStatusDegradedSummary",
        }),
    ),
    Stage(
        "scanner_leaderboard", "skip_scanner_leaderboard",
        "CheckSkipScannerLeaderboard", "ScannerLeaderboard",
        # alpha-engine-config-I7813. Success-only witness, the DirectorComplete
        # pattern for the same reason: the leaf's success edge and every bypass
        # path would otherwise converge on CheckShellRunNotify, so witnessing
        # there would mark a bypassed leaf complete and skip it on the rerun
        # (the I6055 trap). Skipping it lands on CheckShellRunNotify, so this
        # row shares director's inverted-convention exception.
        frozenset({"ScannerLeaderboardComplete"}),
        # alpha-engine-config-I7812 adds the resource-kill fork: same fail-open,
        # same PublishScannerLeaderboardDegraded, only the degraded_summary
        # reason differs — so both of its states witness the same stage.
        degraded_witness=frozenset({
            "ScannerLeaderboardDegraded",
            "SetScannerLeaderboardDegradedSummary",
            "PublishScannerLeaderboardDegraded",
            "ScannerLeaderboardResourceKill",
            "SetScannerLeaderboardResourceKillSummary",
        }),
    ),
    Stage(
        "aggregate_costs", "skip_aggregate_costs",
        "CheckSkipAggregateCosts", "AggregateCosts",
        # alpha-engine-config-I7194: this row modeled a Branch A stage until
        # the aggregator moved to the top level so it could see Director's
        # director-plan cost rows. It is now the LAST gate before the terminal
        # notify, and its success edge, its fail-open and its skip route all
        # land on CheckShellRunNotify — the ordinary skip-lands-in-witness
        # convention, exactly as the shared BranchAComplete witness worked
        # before the move, NOT director's/scanner_leaderboard's inverted one.
        frozenset({"CheckShellRunNotify"}),
        # alpha-engine-config#6722: matches sf-pipeline-policy.md §5's named
        # cost-aggregation carve-out, which REQUIRES a degraded flag. I7194
        # splits that flag into its own top-level family, so the summary Pass
        # is a second degraded witness (the ReportCard/ScannerLeaderboard
        # shape) — the branch-local $.research_degraded_local fold it used
        # does not exist outside the Parallel.
        degraded_witness=frozenset({
            "MarkAggregateCostsDegraded",
            "SetAggregateCostsDegradedSummary",
            # alpha-engine-config-I8336: the named WARNING publish now sits
            # between the summary and CheckShellRunNotify, mirroring
            # PublishScannerLeaderboardDegraded. Entering it means this stage
            # fail-opened, so a mechanical rerun must re-run it rather than
            # read the shared CheckShellRunNotify witness as completion.
            "PublishAggregateCostsDegraded",
        }),
    ),
)

STAGES_BY_NAME = {s.name: s for s in STAGES}
STAGES_BY_FLAG = {s.flag: s for s in STAGES}
BRANCH_A_STAGES = frozenset({
    # alpha-engine-config-I2515 Phase B: "research" removed (the
    # multi-agent Research state — and its skip_research flag /
    # CheckSkipResearch gate — no longer exists). config#3134: scanner,
    # signals_envelope, challenger_shadow added once each got its own
    # CheckSkip* gate. thinktank_coverage was one of them until 2026-08-10,
    # when the ThinkTankCoverage chain was removed from the weekly SF
    # (Brian ruling: the Think Tank runs daily in shadow mode, outside this
    # pipeline) — the daily EventBridge cadence owns it now.
    "scanner", "regime_substrate", "signals_envelope", "challenger_shadow",
    "rag_ingestion", "regime_retrospective_eval",
    "data_phase2", "eval_judge", "rationale_clustering",
    "counterfactual",
    # alpha-engine-config-I7194: aggregate_costs left this set when the
    # aggregator moved out of ResearchPredictorParallel branch 0 to the
    # top-level tail — it is modeled with the other tail stages below.
})
# Stages whose gate is only reachable THROUGH CheckSkipBacktester's run path
# (the skip route overshoots them — see Stage("backtester").note).
BACKTESTER_OVERSHADOWED = (
    "predictor_backtest", "portfolio_optimizer_backtest",
    # alpha-engine-config#6030: the parity family's fine-grained stages —
    # skip_backtester's whole-pair route jumps past CheckSkipParity, so a
    # failed parity sub-stage behind skip_backtester needs the
    # stage-only replacement exactly like the pre-split "parity" did.
    "pit_parity_lookahead", "pit_parity_walkforward", "parity_replay",
    "pit_parity_compare",
)


# ---------------------------------------------------------------------------
# Spot-dispatch necessity — which stages actually need the weekly box
# (alpha-engine-config-I8162)
# ---------------------------------------------------------------------------
# Every recovery used to boot a spot instance unconditionally: CheckSpotDispatchNeeded
# asked only "is $.ec2_instance_id already present?", so a rerun whose derived skip
# set left no box stage standing still spent ~4 minutes in
# DispatchWeeklyFreshnessSpot -> WaitForWeeklyFreshnessSpotBootstrap booting a box
# nothing used (measured on watch-rerun-2026-08-22-1). Recovery is exactly when the
# skip set is largest, so the most-often-unnecessary stage was the one that always ran.
#
# The predicate the SF now carries is DERIVED here rather than hand-listed there, and
# tests/test_weekly_sf_rerun.py pins the two against each other. Hand-listing would
# drift in the asymmetric direction: a stage that needs the box behind a flag the SF
# thinks covers it FAILS the run, where an unnecessary boot only costs four minutes.
#
# Nothing below is a literal list of box stages. The box states are found by their
# reference to $.ec2_instance_id in the live definition; ownership by a skip flag is
# established by REACHABILITY — a flag owns a box state when setting that flag (and
# nothing else) makes the state unreachable.

SPOT_DISPATCH_GATE = "CheckSpotDispatchNeeded"
# The state every path through the dispatch gate converges on: the IsPresent
# passthrough (via NormalizeEc2InstanceId), the fresh-boot path (via
# RouteAfterBootstrapSuccess), and the new no-box-stage bypass. Reachability is
# measured from HERE so the boot machinery's own $.ec2_instance_id references
# (WaitForWeeklyFreshnessSpotBootstrap et al.) are structurally excluded: they are
# how the box is acquired, never a reason to acquire it.
SPOT_DISPATCH_CONVERGENCE = "CheckShellRun"


def _flat_states(sm_def: dict) -> dict:
    """Every state in the definition by name, Parallel branches and Map
    iterators flattened in (state names are unique across the definition)."""
    return dict(_walk_states(sm_def.get("States", {})))


def _rule_skip_flag(rule: dict) -> str | None:
    """The single ``skip_*`` key a Choice rule tests, or None if the rule
    tests anything else (or more than one thing)."""
    flags = set()
    other = False

    def rec(node):
        nonlocal other
        if not isinstance(node, dict):
            return
        var = node.get("Variable")
        if isinstance(var, str) and var.startswith("$.skip_"):
            flags.add(var.removeprefix("$."))
        elif var is not None:
            other = True
        for key in ("And", "Or"):
            for sub in node.get(key, []) or []:
                rec(sub)
        if "Not" in node:
            rec(node["Not"])

    rec(rule)
    if other or len(flags) != 1:
        return None
    return flags.pop()


def _reachable_from(sm_def: dict, start: str, flags: dict) -> set:
    """States reachable from ``start`` when the ``skip_*`` keys in ``flags``
    are true on the execution input.

    A three-valued walk: a Choice rule testing a flag in ``flags`` is taken
    definitively (and its Default becomes unreachable); every other branch is
    UNKNOWN and both arms are followed. Unknown-as-both makes the result an
    over-approximation of what runs, which is the safe direction for the one
    question this answers — "could any state that needs the box still run?"
    An over-approximation can only make the pipeline boot a box it did not
    need; an under-approximation would deny one a stage then SSM-invokes onto.

    Catch/Retry targets are followed too: a box state reached only from a
    failure route still needs a box.
    """
    states = _flat_states(sm_def)
    seen: set = set()
    stack = [start]
    while stack:
        name = stack.pop()
        if name in seen or name not in states:
            continue
        seen.add(name)
        state = states[name]
        nxt: set = set()
        if state.get("Type") == "Choice":
            for rule in state.get("Choices", []):
                flag = _rule_skip_flag(rule)
                if flag is not None and flags.get(flag) is True:
                    nxt.add(rule["Next"])
                    break          # this rule matches; Default is unreachable
                if flag is not None and flag in flags:
                    continue       # known false — this arm cannot be taken
                nxt.add(rule["Next"])
            else:
                if "Default" in state:
                    nxt.add(state["Default"])
        else:
            if "Next" in state:
                nxt.add(state["Next"])
        for catch in state.get("Catch", []) or []:
            if "Next" in catch:
                nxt.add(catch["Next"])
        if state.get("Type") == "Parallel":
            for branch in state.get("Branches", []):
                if branch.get("StartAt"):
                    nxt.add(branch["StartAt"])
        if state.get("Type") == "Map":
            it = state.get("Iterator") or state.get("ItemProcessor") or {}
            if it.get("StartAt"):
                nxt.add(it["StartAt"])
        stack.extend(nxt)
    return seen


def _states_referencing_instance(sm_def: dict) -> set:
    """Every state whose own body dereferences ``$.ec2_instance_id`` — the
    SSM sendCommand / getCommandInvocation pairs and the Lambda payloads that
    address the box. Read from the definition, never listed."""
    # Comment is prose; Branches/Iterator/ItemProcessor hold OTHER states, whose
    # references belong to them and are counted when the walk reaches them —
    # folding a container's children into the container would make every Parallel
    # a box state and no skip flag could ever clear it.
    nested = {"Comment", "Branches", "Iterator", "ItemProcessor"}
    out = set()
    for name, state in _flat_states(sm_def).items():
        body = json.dumps({k: v for k, v in state.items() if k not in nested})
        if "ec2_instance_id" in body:
            out.add(name)
    return out


def box_states_needing_dispatch(sm_def: dict, flags: dict | None = None) -> set:
    """Box-addressing states still reachable past the dispatch gate under
    ``flags``. Empty => this execution has no use for a spot instance."""
    reachable = _reachable_from(sm_def, SPOT_DISPATCH_CONVERGENCE, flags or {})
    return _states_referencing_instance(sm_def) & reachable


def box_dispatch_flags(sm_def: dict) -> tuple:
    """The ``skip_*`` conjunction under which no box-addressing state can run.

    Derived from the definition, never listed. ``STAGES`` supplies the flag
    universe — it is already pinned against ``infrastructure/step_function.json``
    by ``tests/test_weekly_sf_rerun.py`` — and the definition decides which of
    those flags the conjunction needs:

    1. START from every stage flag and PROVE the conjunction sound: with all of
       them true, no box-addressing state may remain reachable. It raises if one
       does, because a box stage behind no skip flag must fail the build here
       rather than be denied a boot it then SSM-invokes onto.
    2. MINIMISE finest-to-coarsest: drop a flag when the ones that remain still
       leave zero box states reachable. Every drop is PROVEN redundant by
       re-running the reachability check, so the surviving conjunction is exactly
       as strong as the full one — it just spells the condition in the coarse
       flags a recovery plan actually emits (``skip_backtester`` rather than
       ``skip_backtester_stage_only`` plus the four parity branch flags).

    Reachability, not topology reading, is what establishes ownership; and
    because the walk over-approximates (§``_reachable_from``), every flag this
    returns is genuinely load-bearing for the bypass.
    """
    kept = list(dict.fromkeys(s.flag for s in STAGES))
    uncovered = box_states_needing_dispatch(sm_def, {f: True for f in kept})
    if uncovered:
        raise SystemExit(
            f"FATAL: box-addressing state(s) {sorted(uncovered)} are reachable "
            f"even with EVERY stage skip flag set — they belong to no skippable "
            "stage, so CheckSpotDispatchNeeded cannot be given a sound bypass "
            "predicate. Add the stage's CheckSkip* gate (and its STAGES row) "
            "before extending the gate."
        )
    for flag in reversed(list(kept)):
        trial = [f for f in kept if f != flag]
        if not box_states_needing_dispatch(sm_def, {f: True for f in trial}):
            kept = trial
    # alpha-engine-config-I8167: every surviving conjunct must be a flag this
    # SCRIPT can actually set — a conjunct backed only by a stage with
    # emit_skip=False is a predicate no producer emits, which reads as live
    # in the definition (CheckSpotDispatchNeeded's Choice tests it) while
    # being permanently unreachable from any input this script derives. This
    # is exactly the shape I8162 shipped: skip_post_eval was the sole
    # coverer of the health-check span and carried emit_skip=False, so the
    # bypass could never fire from a mechanical recovery's derived input.
    # Fail the build here rather than let it recur silently.
    non_emittable = [f for f in kept if not STAGES_BY_FLAG[f].emit_skip]
    if non_emittable:
        raise SystemExit(
            f"FATAL: CheckSpotDispatchNeeded's bypass conjunction would "
            f"include {non_emittable}, whose STAGES row(s) carry "
            "emit_skip=False — this script's deriver can never SET that "
            "flag, so the bypass predicate reads as live but is dead code "
            "for every input derive_plan() produces (alpha-engine-config-"
            "I8162 / I8167). Either give the stage that uniquely covers "
            "this span its own emit_skip=True STAGES row, or prove it "
            "genuinely redundant (drop it) so the minimization above "
            "removes it instead."
        )
    return tuple(kept)


def spot_dispatch_bypass_rule(sm_def: dict) -> dict:
    """The Choice rule ``CheckSpotDispatchNeeded`` carries for the bypass:
    every box stage skipped => route straight to the convergence, booting
    nothing. Each conjunct is the repo's canonical
    ``And[IsPresent, BooleanEquals]`` pair (tests/test_sf_choice_guards.py:
    an unguarded absent path is a States.Runtime that bypasses the failure
    normalizers)."""
    return {
        "And": [
            {
                "And": [
                    {"Variable": f"$.{flag}", "IsPresent": True},
                    {"Variable": f"$.{flag}", "BooleanEquals": True},
                ]
            }
            for flag in box_dispatch_flags(sm_def)
        ],
        "Next": SPOT_DISPATCH_CONVERGENCE,
    }


class CadenceSkipsUnreadable(RuntimeError):
    """The cadence trigger's declared input could not be read.

    Raised rather than defaulted. An empty cadence-skip set and an unreadable
    one are the same value and opposite facts: the first says the scheduled run
    skips nothing, the second says we do not know — and defaulting the second to
    the first silently re-enables every stage the cadence has deliberately
    disabled, which is precisely the failure this loader exists to prevent.
    """


def cadence_declared_skips(path: Path = None) -> dict:
    """The ``skip_*`` flags the SCHEDULED weekly run declares for itself.

    Parsed out of ``SaturdayTrigger.Targets[saturday-pipeline].Input`` in
    ``infrastructure/cloudformation/alpha-engine-orchestration.yaml`` — the
    declared source of truth for the cadence input, and the same block
    ``tests/test_saturday_trigger_skip_parity.py`` reads. Textual rather than
    via a YAML loader because the value is a ``!Sub`` block scalar whose CFN
    intrinsics ``yaml.safe_load`` cannot resolve; only ``skip_*`` keys are read,
    and every ``${...}`` placeholder sits in a value this function ignores.
    """
    p = _CFN_ORCHESTRATION if path is None else path
    try:
        text = p.read_text()
    except OSError as e:
        raise CadenceSkipsUnreadable(f"cannot read {p}: {e}") from e
    try:
        start = text.index("SaturdayTrigger:")
    except ValueError as e:
        raise CadenceSkipsUnreadable(
            f"{p} has no SaturdayTrigger resource — the cadence trigger moved "
            f"or was renamed; retarget this loader rather than defaulting it"
        ) from e
    m = re.search(r"\n          Input: !Sub \|\n(.*?)\n\n", text[start:], re.S)
    if not m:
        raise CadenceSkipsUnreadable(
            f"{p}: SaturdayTrigger target Input block not found — the CFN shape "
            f"changed; retarget this loader rather than defaulting it"
        )
    try:
        payload = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        raise CadenceSkipsUnreadable(
            f"{p}: SaturdayTrigger Input is not parseable JSON: {e}"
        ) from e
    return {
        k: v for k, v in payload.items()
        if k.startswith("skip_") and v is True
    }


@dataclass
class RerunPlan:
    run_date: str
    run_date_provenance: str
    original_input: dict
    calendar_date: str = ""
    calendar_date_provenance: str = ""
    completed: list = field(default_factory=list)   # stage names
    degraded: list = field(default_factory=list)    # stage names (re-run!)
    failed: list = field(default_factory=list)      # stage names
    skip_flags: dict = field(default_factory=dict)  # flag -> True
    warnings: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    # skip_* keys inherited from the source execution's own input, dropped by
    # rerun_input() and reported by the plan printer. See its docstring.
    dropped_inherited_skips: list = field(default_factory=list)

    # skip_* keys the CADENCE trigger declares for itself, re-applied by
    # rerun_input() and reported separately from the derived set.
    cadence_skips: list = field(default_factory=list)

    # The recovery CHAIN this plan was derived from, oldest first, source last
    # (alpha-engine-config-I8161) — and, per stage, WHICH of those executions
    # witnessed the surviving verdict. Auditability is the property that makes
    # a derived skip set trustworthy at all: a skip whose evidence cannot be
    # named is indistinguishable from an inherited flag.
    chain: list = field(default_factory=list)
    witnessed_by: dict = field(default_factory=dict)

    # alpha-engine-config-I11075: the operator's named reason for dispatching a
    # rerun that can enter no spine stage. Emitted into the execution input so
    # describe-execution answers "who decided this empty run was right, and
    # why" without anyone having to find the terminal that launched it.
    accept_vacuous_reason: str = ""

    def rerun_input(self) -> dict:
        """The emitted StartExecution input.

        Non-``skip_*`` keys pass through from the source execution
        (``sns_topic_arn``, ``ec2_instance_id``, …). Every ``skip_*`` key does
        NOT: the skip set is *derived* from what the source execution actually
        completed, and that derivation is the whole product of this script.

        Inheriting them was a silent stage-disabler (alpha-engine-config-I7259,
        observed 2026-08-13). ``watch-rerun-2026-08-13-3``'s input carried
        ``skip_pit_parity_compare: true``; the derivation did NOT produce that
        flag — ``pit_parity_compare`` appears in neither the completed nor the
        degraded set — yet ``dict(self.original_input)`` carried it into
        ``-4``'s input. The plan printed *"3 parity passes re-run"* while the
        emitted input disabled the compare stage that consumes them, and the
        printed ``derived skips`` line did not mention it, so the operator had
        no way to see it from the plan. Because each rerun starts from the
        previous rerun's input, one such flag survives every subsequent
        recovery indefinitely.

        Derived flags are ADDITIVE — they only ever set True — so they could
        never clear an inherited one. Dropping them is what makes the emitted
        input equal the printed plan. A skip the operator genuinely wants is
        re-passed explicitly, which is also the only form that leaves a record.

        **Except the ones the cadence declares for itself** (2026-08-22,
        alpha-engine-config-I8153). I7259's rule is right about *ad-hoc* flags
        and wrong about *declared* ones, and until this change it could not tell
        them apart. ``skip_parity`` is not an operator's stray flag: it is a
        standing disable declared in the cadence trigger's own CFN Input under a
        Brian ruling, with a tracked re-enable issue (I7309) and two blockers
        that make the stage unable to finish in any budget. Dropping it meant
        every mechanical rerun of a scheduled run silently RE-ENABLED a stage
        the cadence has disabled — so ``--start`` launched a doomed parity
        family, and the operator's only defence was reading a NOTE and
        hand-editing the emitted JSON. That is three actions and a piece of
        tribal knowledge against `sf-pipeline-policy.md` §2.5's target of one.

        The distinction is DECLARED versus AD-HOC, not inherited versus derived.
        Cadence-declared skips are re-applied from their source of truth and
        printed on their own line; every other inherited flag is still dropped,
        exactly as I7259 requires. An unreadable declaration RAISES — see
        ``CadenceSkipsUnreadable``.
        """
        out = {
            k: v for k, v in self.original_input.items()
            if not k.startswith("skip_")
        }
        declared = cadence_declared_skips()
        self.cadence_skips = sorted(declared)
        self.dropped_inherited_skips = sorted(
            k for k in self.original_input
            if k.startswith("skip_")
            and k not in self.skip_flags
            and k not in declared
        )
        out["run_date"] = self.run_date
        if self.calendar_date:
            out["calendar_date"] = self.calendar_date
        out["pipeline_role"] = EMITTED_ROLE
        out.update(declared)
        out.update(self.skip_flags)
        if self.accept_vacuous_reason:
            out["vacuous_rerun_accepted_reason"] = self.accept_vacuous_reason
        return out


def _simulate_reachable_works(flags: dict, original_input: dict) -> set:
    """Walk the skip-gate topology with the proposed flags (merged over the
    preserved original input, mirroring the SF's input semantics) and return
    the set of stage names whose WORK state would run."""
    effective = {}
    for stage in STAGES:
        v = flags.get(stage.flag, original_input.get(stage.flag))
        effective[stage.name] = bool(v is True)

    ran: set = set()

    def run_linear(names: list):
        for n in names:
            if not effective[n]:
                ran.add(n)

    # main chain (lib-pin gate first, then the enrich/phase1 pair)
    run_linear(["lib_pin_drift_check", "morning_enrich", "data_phase1"])
    # parallel branches (always entered)
    run_linear(sorted(BRANCH_A_STAGES, key=lambda n: [s.name for s in STAGES].index(n)))
    run_linear(["predictor_training"])
    # tail: CheckSkipBacktester's skip route OVERSHOOTS to CheckSkipEvaluator
    def run_parity_family():
        # alpha-engine-config#6030: skip_parity bypasses the WHOLE family
        # (ParityParallel + compare); otherwise each fine-grained stage runs
        # per its own flag (the three branch gates + the compare gate).
        if effective["parity"]:
            return
        # the family row itself: ParityParallel is entered whenever the
        # family gate is not skipped (its branches then honor their own
        # flags) — needed so a degraded family fold passes the
        # reachability guard.
        ran.add("parity")
        run_linear([
            "pit_parity_lookahead", "pit_parity_walkforward", "parity_replay",
            "pit_parity_compare",
        ])

    if effective["backtester"]:
        pass  # backtester, predictor_backtest, portfolio_optimizer_backtest + parity family all bypassed
    elif effective["backtester_stage_only"]:
        # config#2362 Option A: only the Backtester SSM task is bypassed;
        # the tail gates still compose orthogonally past it.
        run_linear(["predictor_backtest", "portfolio_optimizer_backtest"])
        run_parity_family()
    else:
        run_linear(["backtester", "predictor_backtest", "portfolio_optimizer_backtest"])
        run_parity_family()
    # config#6054 split the coarse post_eval span: post_eval survives as the
    # deprecated whole-tail alias while report_card / director are
    # independently gated. alpha-engine-config-I8167 split it further —
    # saturday_health_check now owns the two health-check states behind its
    # own gate, one hop downstream of post_eval's (still-live) whole-tail
    # alias. All four are modeled here so the degraded-aware reachability
    # guard (which folds DEGRADED stages into must_rerun) can see
    # saturday_health_check — its health-check degraded_witness — as
    # reachable when a tail rerun re-runs the span.
    # alpha-engine-config-I7194: aggregate_costs joins the tail — it now runs
    # after Director, on the single edge every real-completion path takes
    # into CheckShellRunNotify.
    run_linear([
        "evaluator", "saturday_health_check", "post_eval", "report_card",
        "director", "aggregate_costs",
    ])
    return ran


def classify_stages(entered: set) -> dict:
    """One execution's verdict per stage: ``"degraded"`` | ``"completed"`` |
    ``"failed"``. A stage the execution never attempted is ABSENT from the
    result — "not observed here" and "did not complete" are different facts,
    and collapsing them is what made a chained recovery lose the original
    run's progress (alpha-engine-config-I8161).

    Reads only the set of states an execution ENTERED. It never reads an
    execution's ``skip_*`` input, which is what keeps the chain derivation
    immune to alpha-engine-config-I7259's flag-propagation trap.
    """
    out: dict = {}
    for stage in STAGES:
        if entered & stage.degraded_witness:
            # Ran, failed, absorbed fail-open (Publish*Degraded route): the
            # stage must RE-RUN. Degraded overrides witness — the pipeline
            # continuing past a degradation is NOT evidence of completion
            # (I6055: the 2026-08-01 Director hard-fail recorded as
            # "post_eval complete", then skipped by the next rerun).
            out[stage.name] = "degraded"
        elif entered & stage.witness:
            out[stage.name] = "completed"
        elif stage.detect_failure and (
            stage.work in entered or (entered & stage.historical_work)
        ):
            out[stage.name] = "failed"
    return out


def resolve_chain(links: list) -> tuple:
    """Fold a recovery CHAIN into one verdict per stage, latest attempt wins.

    ``links`` is ``[(label, entered_states), ...]`` in CHRONOLOGICAL order,
    the source execution last. Returns ``(outcome_by_stage, witness_by_stage)``
    where the witness names the execution whose history established the
    surviving verdict.

    alpha-engine-config-I8161. ``weekly_sf_rerun.py`` defaults to the LATEST
    failed execution, which after one recovery is the RERUN — and a rerun's
    history contains only the stages it did not skip, so everything the
    ORIGINAL run completed reads as not-completed. Measured 2026-08-22:
    ``skip_backtester``, ``skip_predictor_backtest`` and
    ``skip_portfolio_optimizer_backtest`` moved from *derived skips* (deriving
    from the scheduled run) to *dropped skips* (deriving from
    ``watch-rerun-2026-08-22-1``), so a second recovery would have re-run the
    whole backtester family — hours of spot compute — for stages that had
    completed cleanly four hours earlier. Each successive recovery lost more of
    the original run's progress, inverting the point of the helper.

    The completed set is a property of the ``run_date``, not of one execution.
    LATEST-ATTEMPT-WINS is the resolution rule and it cuts both ways: a stage
    that completed in run 1 and failed in run 3 must re-run; a stage that
    completed in run 1 and was never attempted again stays completed.

    This is evidence-based and therefore immune to alpha-engine-config-I7259:
    it reads execution HISTORIES, never a previous input's flags. Inheriting
    ``skip_*`` keys propagates a hand-added flag forever; witnessing a state
    entry cannot, because a flag nobody's execution acted on leaves no trace in
    any history.
    """
    outcome: dict = {}
    witness: dict = {}
    for label, entered in links:
        for name, verdict in classify_stages(entered).items():
            outcome[name] = verdict
            witness[name] = label
    return outcome, witness


def derive_plan(
    events: list[dict],
    start_time: datetime | None = None,
    prior_histories: list | None = None,
    source_label: str = "source execution",
    force_rerun: frozenset[str] = frozenset(),
) -> RerunPlan:
    """Derive the recovery plan from the source execution, optionally folding
    in the EARLIER executions of the same ``run_date`` (``prior_histories`` as
    ``[(label, events), ...]``, chronological). See ``resolve_chain``: without
    the chain, every recovery after the first loses the original run's
    progress (alpha-engine-config-I8161).
    """
    entered = entered_states(events)
    original_input = execution_input(events)
    run_date, provenance = derive_run_date(events, start_time)
    calendar_date, cal_provenance = derive_calendar_date(events, start_time)
    plan = RerunPlan(
        run_date=run_date,
        run_date_provenance=provenance,
        original_input=original_input,
        calendar_date=calendar_date,
        calendar_date_provenance=cal_provenance,
    )

    links = [(label, entered_states(evs)) for label, evs in (prior_histories or [])]
    links.append((source_label, entered))
    plan.chain = [label for label, _ in links]
    entered_by_label = dict(links)
    outcome, plan.witnessed_by = resolve_chain(links)

    for stage in STAGES:
        verdict = outcome.get(stage.name)
        if verdict == "degraded":
            plan.degraded.append(stage.name)
            saw = plan.witnessed_by[stage.name]
            plan.notes.append(
                f"{stage.name}: DEGRADED in {saw} (entered "
                f"{sorted(entered_by_label[saw] & stage.degraded_witness)}) "
                "— NOT skipped; the rerun re-runs it to retry the absorbed "
                "failure"
            )
        elif verdict == "completed":
            plan.completed.append(stage.name)
            if stage.emit_skip:
                plan.skip_flags[stage.flag] = True
            elif stage.note:
                plan.notes.append(f"{stage.name}: {stage.note}")
        elif verdict == "failed":
            plan.failed.append(stage.name)

    if not plan.failed and not plan.degraded:
        plan.warnings.append(
            "no failed or degraded WORK stage identified — the failure was "
            "pre-workload (gate / mutex / notifier). Fix the root cause "
            "first; this rerun input re-runs everything not witnessed "
            "complete."
        )

    # Anti-swallow / reachability guard: every failed stage's work must
    # actually run under the derived flags. The only overshooting gate is
    # skip_backtester (its skip route jumps the predictor-backtest /
    # portfolio-optimizer / parity gates), so replace it with
    # skip_backtester_stage_only when it would bypass the failed stage
    # (config#2362 Option A, operator-ruled 2026-07-21): the additive
    # CheckSkipBacktesterStageOnly gate skips only the Backtester SSM task
    # (its backtest/{run_date}/ artifacts already exist and are reused) while
    # still routing through the predictor-backtest/portfolio-optimizer/parity
    # gates, so the failed stage reruns without re-burning Backtester.
    # A stage that must RE-RUN is a failed OR degraded one (I6055: degraded
    # is exactly what a mechanical rerun exists to retry) — both classes get
    # the overshoot replacement and the reachability guard. Meta rows
    # (emit_skip=False AND detect_failure=False: the parity family fold,
    # backtester_stage_only) are excluded — their fine-grained rows carry
    # the actual work-state guarantee.
    def _is_meta(name: str) -> bool:
        st = STAGES_BY_NAME[name]
        return not st.emit_skip and not st.detect_failure

    must_rerun = [f for f in (*plan.failed, *plan.degraded) if not _is_meta(f)]

    # --rerun-stage (alpha-engine-config-I11106): force a stage the deriver
    # witnessed as COMPLETED to run again. The deriver's job is to reuse work
    # that is still valid; it cannot know that a stage's *output* is no longer
    # trustworthy for a reason outside the execution history — a dependency
    # redeployed since, a producer contract fixed, a fix whose only proof is
    # that the stage runs again. Measured case: the 2026-09-19 recovery had to
    # re-enter Director so `director-plan` would emit the cost record that
    # AggregateCosts is graded on (alpha-engine-config-I11100). Without this
    # flag the alternative is hand-editing the derived JSON, which bypasses
    # every coherence guard below — the deriver exists so recovery is ONE
    # mechanical command (sf-pipeline-policy §2.5), and an operator editing
    # its output by hand is that guarantee already broken.
    #
    # A forced stage joins must_rerun deliberately: the reachability guard
    # below then PROVES the emitted skip set can actually enter it, instead of
    # trusting that dropping a flag was sufficient. Forcing a stage whose skip
    # flag is not the only thing gating it fails loudly there rather than
    # producing a rerun that silently does not run it.
    for _stage in sorted(force_rerun):
        if _stage not in STAGES_BY_NAME:
            raise SystemExit(
                f"FATAL: --rerun-stage {_stage!r} is not a declared stage. "
                f"Known: {', '.join(sorted(STAGES_BY_NAME))}"
            )
        _flag = STAGES_BY_NAME[_stage].flag
        _had_flag = plan.skip_flags.pop(_flag, None) is not None
        _was_completed = _stage in plan.completed
        if _was_completed:
            plan.completed.remove(_stage)
        if _stage not in must_rerun and not _is_meta(_stage):
            must_rerun.append(_stage)
        plan.notes.append(
            f"{_stage}: FORCED to re-run by --rerun-stage"
            + (f" (dropped {_flag})" if _had_flag else f" ({_flag} was not set)")
            + (" — was witnessed completed" if _was_completed else "")
            + ". The operator asserted this stage's prior output is not "
            "trustworthy for this recovery; the reachability guard below "
            "proves the emitted input can enter it."
        )

    if "skip_backtester" in plan.skip_flags and any(
        f in must_rerun for f in BACKTESTER_OVERSHADOWED
    ):
        del plan.skip_flags["skip_backtester"]
        plan.skip_flags["skip_backtester_stage_only"] = True
        plan.notes.append(
            "skip_backtester replaced with skip_backtester_stage_only: "
            "Backtester completed but its whole-pair skip route would bypass "
            f"failed/degraded stage(s) {[f for f in must_rerun if f in BACKTESTER_OVERSHADOWED]} "
            "— skipping only the Backtester SSM task (reusing its "
            "already-written artifacts) instead of re-burning it. config#2362."
        )

    # Simulate the EMITTED input, not the source input. rerun_input() drops
    # inherited skip_* keys, so passing original_input here would check a
    # different flag set than the one actually started (config-I7259).
    reachable = _simulate_reachable_works(plan.skip_flags, {})
    unreachable = [f for f in must_rerun if f not in reachable]
    if unreachable:
        raise SystemExit(
            f"FATAL: derived skip set would make failed/degraded stage(s) "
            f"{unreachable} unreachable — refusing to emit an input "
            f"that silently skips a stage that must re-run. Flags: "
            f"{sorted(plan.skip_flags)}; original input flags: "
            f"{ {k: v for k, v in original_input.items() if k.startswith('skip_')} }. "
            f"This means the skip-gate topology changed — update STAGES / "
            f"_simulate_reachable_works in scripts/weekly_sf_rerun.py."
        )
    for f in plan.failed:
        if plan.skip_flags.get(STAGES_BY_NAME[f].flag):
            raise SystemExit(
                f"FATAL: internal contradiction — failed stage {f!r} ended up "
                f"with its own skip flag set. Refusing (forbidden swallow)."
            )

    # Director/ReportCard freshness coupling (alpha-engine-config-I8382,
    # measured 2026-08-22 watch-rerun-2026-08-22-3). Director's input contract
    # is a report_card.json produced by THIS execution — but report_card and
    # director are independently-witnessed stages, and LATEST-ATTEMPT-WINS
    # (resolve_chain, above) can legitimately witness report_card as
    # "completed" from an EARLIER, possibly-failed chain link while director
    # itself was never witnessed complete anywhere in the chain (its own
    # failure is terminal — config#6408 — so DirectorComplete is never
    # entered by a failing attempt, and the stage keeps re-running on every
    # rerun until one succeeds). The two facts compose into a bug: a rerun
    # skips ReportCard (reusing an old evaluator/{date}/report_card.json,
    # possibly the very run whose Director hard-failed and whose upstream
    # tiles were still degraded) while re-running Director fresh against that
    # STALE card. Measured: watch-rerun-2026-08-22-3 graded a card written by
    # the FAILED 02:00 execution (report_card.json at 07:04) with its freshly
    # re-run Director (action_plan.json at 09:04) — a card with
    # status:"partial", tiles_overall_status:"RED", degraded_attestation:true,
    # scope_unknown:true, none of which reflect what the intervening reruns
    # actually fixed upstream.
    #
    # Fix: whenever Director will actually RUN this rerun (skip_director is
    # not True), ReportCard must run too — forcing a fresh card from THIS
    # execution rather than letting Director grade one carried over from a
    # different, earlier attempt. This is independent of the failed/degraded
    # reachability guard above (report_card is neither failed nor degraded
    # here — it is "completed", just stale relative to what Director is about
    # to consume), so it is not covered by must_rerun / BACKTESTER_OVERSHADOWED
    # and needs its own rule. The safe direction is to over-run (re-run a
    # ReportCard that was actually still fine) rather than under-run (skip one
    # that no longer reflects reality) — same reasoning _reachable_from uses
    # for the spot-dispatch predicate.
    director_will_run = not plan.skip_flags.get(STAGES_BY_NAME["director"].flag)
    if director_will_run and plan.skip_flags.pop(STAGES_BY_NAME["report_card"].flag, None):
        if "report_card" in plan.completed:
            plan.completed.remove("report_card")
        plan.notes.append(
            "skip_report_card dropped: Director will run this rerun and its "
            "input contract is a ReportCard produced by THIS execution — "
            "skipping ReportCard would let Director grade a card written by "
            "an earlier, possibly-stale attempt (alpha-engine-config-I8382, "
            "measured 2026-08-22 watch-rerun-2026-08-22-3)."
        )

    orig_role = original_input.get("pipeline_role")
    if orig_role != EMITTED_ROLE:
        plan.notes.append(
            f"pipeline_role: {orig_role!r} -> {EMITTED_ROLE!r} — bypasses the "
            "weekly run-day gate (a Sunday recovery under role 'weekly' would "
            "silently Succeed-skip) and the run-slot mutex (config#2280); "
            "skip flags remain live (weekly gates are role-unconditional)."
        )
    return plan


# ---------------------------------------------------------------------------
# Role-gating verification against the live definition (config#2277 D2)
# ---------------------------------------------------------------------------
# _walk_states / _rule_role_values / verify_skip_flags_live now live in
# sf_rerun_common.py (imported above) — lifted unchanged on the weekday_sf_
# rerun.py second adoption (alpha-engine-config#6694, shared-code-policy).

# ---------------------------------------------------------------------------
# Mutex-steal decision matrix (config#2280 contract)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MutexDecision:
    action: str          # "proceed" | "steal" | "abort"
    reason: str
    key: str = ""
    holder_arn: str = ""
    holder_status: str = ""
    manual_cmd: str = ""


def _manual_delete_cmd(key: str) -> str:
    return (
        f"aws dynamodb delete-item --table-name {MUTEX_TABLE} "
        f"--key '{{\"mutex_key\": {{\"S\": \"{key}\"}}}}'"
    )


def decide_mutex_action(
    item: dict | None,
    holder_status: str | None,
    key: str,
    source_arn: str,
) -> MutexDecision:
    """Pure decision matrix (unit-tested). `item` is the raw DynamoDB item
    (attribute-value encoded) or None; `holder_status` is the holder
    execution's status, or None when the holder could not be described."""
    if item is None:
        return MutexDecision(
            "proceed",
            "no run-slot mutex item exists for the failed run's key — "
            "nothing to steal (non-cadence source role, mutex fail-open, or "
            "already cleaned).",
            key=key,
        )
    holder_arn = (item.get("execution_arn") or {}).get("S", "")
    if not holder_arn:
        return MutexDecision(
            "abort",
            "run-slot mutex item exists but carries no execution_arn — "
            "cannot verify the holder is terminal, refusing to steal blind. "
            "Inspect and delete manually if appropriate.",
            key=key,
            manual_cmd=_manual_delete_cmd(key),
        )
    if holder_status is None:
        return MutexDecision(
            "abort",
            f"could not describe the holder execution {holder_arn} — "
            "refusing to steal without terminal proof.",
            key=key,
            holder_arn=holder_arn,
            manual_cmd=_manual_delete_cmd(key),
        )
    if holder_status == "RUNNING":
        return MutexDecision(
            "abort",
            f"holder execution {holder_arn} is STILL RUNNING — never steal "
            "from, or start a rerun beside, a live execution (artifact "
            "write races on the same run_date prefixes).",
            key=key,
            holder_arn=holder_arn,
            holder_status=holder_status,
        )
    if holder_status == "SUCCEEDED":
        return MutexDecision(
            "abort",
            f"holder execution {holder_arn} SUCCEEDED — the run-slot's work "
            "completed (the failed source was likely the duplicate-trigger "
            "LOSER). A rerun would duplicate the week's artifacts. If you "
            "truly intend to re-run this slot, delete the item and craft "
            "the input by hand.",
            key=key,
            holder_arn=holder_arn,
            holder_status=holder_status,
            manual_cmd=_manual_delete_cmd(key),
        )
    # FAILED / TIMED_OUT / ABORTED
    return MutexDecision(
        "steal",
        f"holder execution {holder_arn} is TERMINAL ({holder_status}) — it "
        "can no longer write artifacts, so deleting its stale run-slot item "
        "is safe and frees the slot for the recovery arc.",
        key=key,
        holder_arn=holder_arn,
        holder_status=holder_status,
    )


# ---------------------------------------------------------------------------
# AWS plumbing (thin, injectable)
# ---------------------------------------------------------------------------
# fetch_history / list_all_executions now live in sf_rerun_common.py
# (imported above) — lifted unchanged on the weekday_sf_rerun.py second
# adoption (alpha-engine-config#6694, shared-code-policy).

def resolve_default_execution(sf, sm_arn: str) -> dict:
    """Latest terminal-failed (FAILED or TIMED_OUT) execution."""
    for ex in list_all_executions(sf, sm_arn):
        if ex["status"] in ("FAILED", "TIMED_OUT"):
            return ex
    raise SystemExit(
        f"FATAL: no FAILED/TIMED_OUT execution found on {sm_arn} — nothing "
        "to recover. Pass --execution-arn explicitly (e.g. for an ABORTED "
        "run)."
    )


def next_rerun_name(sf, sm_arn: str, run_date: str) -> str:
    prefix = f"watch-rerun-{run_date}-"
    ns = []
    for ex in list_all_executions(sf, sm_arn):
        m = re.fullmatch(re.escape(prefix) + r"(\d+)", ex["name"])
        if m:
            ns.append(int(m.group(1)))
    return f"{prefix}{(max(ns) if ns else 0) + 1}"


# effective_run_date_of now lives in sf_rerun_common.py (imported above) —
# lifted unchanged on the weekday_sf_rerun.py second adoption
# (alpha-engine-config#6694, shared-code-policy).


# ---------------------------------------------------------------------------
# The recovery CHAIN (alpha-engine-config-I8161)
# ---------------------------------------------------------------------------
# The completed set for a recovery is a property of the RUN_DATE, not of one
# execution: the scheduled run plus every watch-rerun-<run_date>-N. Deriving
# from the last link alone loses everything the original run completed, because
# a rerun's history contains only the stages it did not skip.

# How far either side of run_date a same-run_date execution may have STARTED.
# The scheduled run stamps run_date from its own UTC start date, and recoveries
# follow over the next days (a cross-UTC-midnight recovery of a Saturday cycle
# is routine — alpha-engine-config-I7443). Purely a bound on how many
# executions get described; the run_date comparison below is what decides
# membership.
CHAIN_WINDOW_DAYS_BEFORE = 1
CHAIN_WINDOW_DAYS_AFTER = 6


def chain_candidates(executions: list, run_date: str, source_arn: str, source_start) -> tuple:
    """Split ``executions`` into (candidates, non_terminal) for the chain of
    ``run_date``, oldest first.

    A candidate is TERMINAL, is not the source itself, and started no later
    than the source — the source is the frontier the operator chose (with
    ``--execution-arn``, deliberately), and evidence from after it is not part
    of the chain being recovered. Non-terminal executions in the window are
    returned separately so the caller can say so rather than silently treat a
    RUNNING execution's partial history as evidence.
    """
    lo = date.fromisoformat(run_date) - timedelta(days=CHAIN_WINDOW_DAYS_BEFORE)
    hi = date.fromisoformat(run_date) + timedelta(days=CHAIN_WINDOW_DAYS_AFTER)
    cands, non_terminal = [], []
    for ex in executions:
        if ex["executionArn"] == source_arn:
            continue
        started = ex["startDate"]
        if not (lo <= started.astimezone(timezone.utc).date() <= hi):
            continue
        if source_start is not None and started > source_start:
            continue
        (cands if ex["status"] in TERMINAL_STATUSES else non_terminal).append(ex)
    cands.sort(key=lambda e: e["startDate"])
    return cands, non_terminal


def collect_chain_histories(sf, sm_arn: str, run_date: str, source: dict) -> tuple:
    """``([(label, events), ...], warnings)`` for every EARLIER terminal
    execution of ``run_date``, oldest first.

    Fails LOUDLY if a candidate's history cannot be read. A dropped link is not
    the conservative direction it looks like: a stage that completed in run 1
    and FAILED in run 2 reverts to "completed" the moment run 2 goes missing,
    and the recovery then skips a stage that must re-run.
    """
    cands, non_terminal = chain_candidates(
        list_all_executions(sf, sm_arn), run_date,
        source["executionArn"], source.get("startDate"),
    )
    histories, warnings = [], []
    if non_terminal:
        warnings.append(
            "chain: execution(s) "
            f"{[e['name'] for e in non_terminal]} are non-terminal in this "
            "run_date's window and were EXCLUDED — a partial history is not "
            "evidence. Re-derive once they finish."
        )
    for ex in cands:
        if effective_run_date_of(sf, ex) != run_date:
            continue
        try:
            events = fetch_history(sf, ex["executionArn"])
        except Exception as exc:  # noqa: BLE001 — an unreadable link is a WRONG plan, not a smaller one
            raise SystemExit(
                f"FATAL: could not read the execution history of chain member "
                f"{ex['name']} ({ex['executionArn']}): {exc!r}. Refusing to "
                "derive a skip set from an INCOMPLETE chain — a missing link "
                "can silently turn a stage that FAILED in a later run back "
                "into 'completed' from an earlier one, which is exactly the "
                "swallow this helper exists to prevent "
                "(alpha-engine-config-I8161)."
            ) from exc
        histories.append((ex["name"], events))
    return histories, warnings


# ---------------------------------------------------------------------------
# Skip-coherence pre-flight (alpha-engine-config-I7443, sf-pipeline-policy
# §2.5: "a skip-set that leaves a downstream JSONPath unresolvable must be
# rejected by the helper, not discovered as a States.Runtime error thirty
# seconds into the rerun.")
# ---------------------------------------------------------------------------

PREDICTOR_MANIFEST_BUCKET = "alpha-engine-research"
PREDICTOR_MANIFEST_KEY = "predictor/weights/meta/manifest.json"


class SkipCoherenceError(Exception):
    """A derived (or carried-forward) skip_* flag is incoherent with the
    plan's run_date: the artifact it claims already exists does not, FOR
    THIS run_date. Raised before any dispatch is spent finding out.

    2026-08-15 recovery of weekly cycle 2026-08-15 burned two full
    ne-weekly-freshness-pipeline executions (watch-rerun-2026-08-16-1,
    -2; ~50 minutes) on skip_predictor_training=true paired with a
    run_date the predictor never trained for — the SF's own
    ValidatePredictorSkipWeightsFresh / CheckPredictorSkipWeightsFresh gate
    (infrastructure/step_function.json) caught it correctly, but four hours
    and two dispatches into the recovery arc instead of at --dry-run. This
    mirrors that exact predicate here, ahead of --start.
    """


def refuse_fallback_run_date_without_acceptance(plan: "RerunPlan", accept: bool) -> None:
    """Refuse (SystemExit) a --start whose run_date was MINTED from the
    source execution's own UTC start time rather than carried from an
    explicit input or InitializeInput output, unless the operator passed
    ``--accept-fallback-run-date``. alpha-engine-config-I7443: the printed
    FALLBACK provenance note went unnoticed/unacted-on for exactly this
    reason on 2026-08-16, and the resulting rerun burned two dispatches on
    PredictorSkipWeightsStale."""
    if not plan.run_date_provenance.startswith("FALLBACK") or accept:
        return
    raise SystemExit(
        f"FATAL: run_date {plan.run_date!r} was MINTED from the source "
        f"execution's own UTC start time ([{plan.run_date_provenance}]), "
        "not carried from an explicit input or InitializeInput output — "
        "refusing to --start on it. If this really is a pre-workload "
        "failure (the source never reached InitializeInput) and "
        f"{plan.run_date!r} is correct, re-run with "
        "--accept-fallback-run-date. Otherwise pass --execution-arn to "
        "point at the right source, or pass the cycle's ORIGINAL "
        "run_date by hand-crafting the input (alpha-engine-config-I7443)."
    )


class VacuousRerunError(Exception):
    """The proposed rerun can enter NO declared spine stage.

    alpha-engine-config-I9693: three straight Saturday runs FAILED and every
    recovery was an all-skipped rerun that terminated SUCCEEDED, so the weekly
    cadence produced nothing between 2026-08-15 and the fix while every surface
    reading the terminal state showed green. The recovery loop converges here
    naturally — each rerun derives skips from what the previous one witnessed,
    so the skip set only ever grows, and the last rerun in a chain is the one
    that runs nothing at all.

    nousergon-data-PR1808 made the SF itself terminate ``VacuousRun`` (a Fail)
    on an execution that enters no spine stage, so it pages rather than
    resolves. This is the same predicate moved AHEAD of the dispatch: a ~50
    minute execution and a CRITICAL page, both spent establishing something the
    input already said.
    """


def refuse_vacuous_rerun(
    sm_def: dict, sm_arn: str, rerun_input: dict, accept_reason: str | None
) -> tuple:
    """Refuse (SystemExit) a --start whose input can enter no spine stage.

    Returns the enterable spine stages so the caller can report them. The
    escape hatch is ``--accept-vacuous-rerun REASON``: named and reasoned, per
    I9693 deliverable 3, and the reason rides into the execution input so
    ``describe-execution`` carries it for as long as the execution is retained.
    Accepting does NOT make the run substantive — the SF still terminates
    ``VacuousRun`` — it records who decided to spend the dispatch and why.
    """
    spine = _landed_spine(sm_arn)
    if not spine:
        raise VacuousRerunError(
            f"no declared spine for {sm_arn.rsplit(':', 1)[-1]!r} in "
            "nousergon_lib.pipeline_status.registry.PIPELINE_STAGE_ORDER — "
            "a pipeline whose substantive stages are undeclared cannot be "
            "judged vacuous, and treating that as 'nothing was expected, so "
            "it passed' is the exact defect this guard exists for. Declare "
            "the spine before re-running this pipeline through this script."
        )
    enterable = enabled_spine_stages(sm_def, rerun_input, spine)
    if enterable:
        return enterable
    attribution = disabling_flags_for_input(sm_def, rerun_input, spine)
    lines = []
    for stage in spine:
        flags = attribution.get(stage, ())
        lines.append(
            f"    {stage}: {', '.join(flags) if flags else 'the skip set as a whole'}"
        )
    detail = "\n".join(lines)
    if accept_reason:
        return enterable
    raise SystemExit(
        "FATAL (vacuous rerun, alpha-engine-config-I11075 / I9693): this "
        f"input can enter NONE of the {len(spine)} declared spine stages of "
        f"{sm_arn.rsplit(':', 1)[-1]} — it would run for its full wall clock "
        "and produce nothing, then terminate VacuousRun and page.\n"
        "  disabled by:\n"
        f"{detail}\n"
        "  Every stage the cycle needs is already switched off, which means "
        "the recovery is FINISHED, not pending — the remaining failure is not "
        "re-runnable by skipping more. Investigate what the chain never "
        "completed (the plan above names it), or, if dispatching an empty run "
        "really is what you want, re-run with "
        "--accept-vacuous-rerun 'why this empty run is the right action'."
    )


def _predictor_manifest_date(s3) -> str:
    """HeadObject the live weights manifest; return LastModified DATE."""
    try:
        head = s3.head_object(Bucket=PREDICTOR_MANIFEST_BUCKET, Key=PREDICTOR_MANIFEST_KEY)
    except Exception as exc:  # noqa: BLE001 — cannot PROVE freshness on any S3 error; fail loud, mirrors the SF's own Catch
        raise SkipCoherenceError(
            f"skip_predictor_training=true but HeadObject on "
            f"s3://{PREDICTOR_MANIFEST_BUCKET}/{PREDICTOR_MANIFEST_KEY} "
            f"failed ({exc!r}) — cannot verify weights freshness. Refusing "
            "to emit a plan the SF's own ValidatePredictorSkipWeightsFresh "
            "would also reject. Re-run WITHOUT skip_predictor_training, or "
            "investigate the manifest."
        ) from exc
    last_modified = head["LastModified"]
    if hasattr(last_modified, "astimezone"):
        return last_modified.astimezone(timezone.utc).date().isoformat()
    return str(last_modified).split("T", 1)[0]


def coerce_calendar_date_for_predictor_skip(
    s3, calendar_date: str, skip_flags: dict
) -> tuple[str, str | None]:
    """When skip_predictor_training, clamp calendar_date to manifest
    LastModified if the derived execution day is later — the SF predicate
    is manifest DATE >= calendar_date, and a recovery launched on a later
    wall-clock day must not re-stamp a calendar_date the weights cannot satisfy."""
    if not skip_flags.get("skip_predictor_training"):
        return calendar_date, None
    manifest_date = _predictor_manifest_date(s3)
    if manifest_date < calendar_date:
        return manifest_date, (
            f"calendar_date clamped {calendar_date} -> {manifest_date} for "
            f"skip_predictor_training (manifest LastModified is the latest "
            f"provable weights date; SF CheckPredictorSkipWeightsFresh compares "
            f"against calendar_date, not run_date — alpha-engine-config-I8809)"
        )
    return calendar_date, None


def check_predictor_skip_freshness(s3, calendar_date: str, skip_flags: dict) -> None:
    """Mirror ValidatePredictorSkipWeightsFresh: if skip_predictor_training
    is set, HeadObject the live weights manifest and require its
    LastModified DATE >= calendar_date (lexicographic YYYY-MM-DD compare, same as
    the SF's CheckPredictorSkipWeightsFresh Choice state). Raises
    SkipCoherenceError rather than emitting an input the SF will only reject
    after a dispatch is already spent.

    Any S3 error (missing manifest, AccessDenied, throttling exhausted) is
    treated the same as the SF's own Catch on ValidatePredictorSkipWeightsFresh
    — fail loud rather than silently trust an unverifiable freshness claim.
    """
    if not skip_flags.get("skip_predictor_training"):
        return
    manifest_date = _predictor_manifest_date(s3)
    if manifest_date < calendar_date:
        raise SkipCoherenceError(
            f"skip_predictor_training=true but "
            f"s3://{PREDICTOR_MANIFEST_BUCKET}/{PREDICTOR_MANIFEST_KEY} "
            f"LastModified date {manifest_date} < calendar_date {calendar_date} — no "
            "completed predictor training for this cycle; refusing to "
            "launch onto stale weights (same predicate as the SF's own "
            "ValidatePredictorSkipWeightsFresh / PredictorSkipWeightsStale — "
            "checked here BEFORE a dispatch, not after one; "
            "alpha-engine-config-I7443). Either re-run WITHOUT "
            "skip_predictor_training, or pass an explicit calendar_date "
            "at or before the manifest date if this is a cross-UTC-midnight "
            "recovery rerun."
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_plan(plan: RerunPlan, source_arn: str, source_status: str, name: str, sm_arn: str) -> None:
    print(f"source execution : {source_arn} ({source_status})")
    print(f"run_date         : {plan.run_date}  [{plan.run_date_provenance}]")
    if plan.calendar_date:
        print(f"calendar_date    : {plan.calendar_date}  [{plan.calendar_date_provenance}]")
    # alpha-engine-config-I8161: the chain, and which link witnessed what. A
    # derived skip set whose evidence cannot be named is indistinguishable from
    # an inherited flag, and auditability is the property that makes this
    # script trustworthy at all.
    print(f"chain            : {' -> '.join(plan.chain)}")
    print(f"rerun name       : {name}")
    print(f"pipeline_role    : {EMITTED_ROLE}")
    print(f"completed stages : {', '.join(plan.completed) or '(none)'}")
    print(f"degraded stages  : {', '.join(plan.degraded) or '(none)'}")
    print(f"failed stages    : {', '.join(plan.failed) or '(none identified)'}")
    print(f"derived skips    : {', '.join(sorted(plan.skip_flags)) or '(none)'}")
    for label in plan.chain:
        seen = [
            f"{name} ({plan_verdict})"
            for name, plan_verdict in (
                *((n, "completed") for n in plan.completed),
                *((n, "degraded") for n in plan.degraded),
                *((n, "failed") for n in plan.failed),
            )
            if plan.witnessed_by.get(name) == label
        ]
        print(f"  witnessed by {label}: {', '.join(seen) or '(nothing that survives)'}")
    # Populates plan.dropped_inherited_skips as a side effect; call before
    # reporting them.
    _emitted = plan.rerun_input()
    if plan.cadence_skips:
        print(
            f"cadence skips    : {', '.join(plan.cadence_skips)} "
            "[declared by the scheduled trigger's own CFN Input, re-applied so "
            "a recovery runs what the cadence runs — NOT derived from what the "
            "source execution completed]"
        )
    if plan.dropped_inherited_skips:
        print(
            f"dropped skips    : {', '.join(plan.dropped_inherited_skips)} "
            "[carried by the source execution's own input, NOT derived from "
            "what it completed — re-pass explicitly if still wanted]"
        )
    for n in plan.notes:
        print(f"NOTE : {n}")
    for w in plan.warnings:
        print(f"WARN : {w}", file=sys.stderr)
    print("\nStartExecution input:")
    print(json.dumps(_emitted, indent=2, sort_keys=True))
    print(
        "\nequivalent CLI (name and input.run_date are DERIVED TOGETHER — "
        "alpha-engine-config-I7443: watch-rerun-{run_date}-N must never "
        "carry a different run_date. Do not hand-edit the JSON below "
        "without also renaming; re-run this script with --execution-arn "
        "instead):"
    )
    print(
        f"aws stepfunctions start-execution --state-machine-arn {sm_arn} "
        f"--name {name} --input '{json.dumps(_emitted, sort_keys=True)}'"
    )


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--execution-arn", help="failed execution to recover (default: latest FAILED/TIMED_OUT)")
    ap.add_argument("--state-machine-arn", default=DEFAULT_STATE_MACHINE_ARN)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="derive + print only (default)")
    mode.add_argument("--start", action="store_true", help="run pre-start guards, steal stale mutex item if safe, StartExecution")
    ap.add_argument(
        "--accept-fallback-run-date", action="store_true",
        help=(
            "required to --start when run_date could not be resolved from "
            "an explicit input or InitializeInput output and fell back to "
            "the source execution's own UTC start date (provenance "
            "'FALLBACK: ...'). Without it, --start refuses rather than "
            "silently minting a run_date that may not match the cycle "
            "being recovered (alpha-engine-config-I7443)."
        ),
    )
    ap.add_argument(
        "--accept-vacuous-rerun", metavar="REASON", default="",
        help=(
            "required to --start an input that can enter NONE of the declared "
            "spine stages. Such a run produces nothing and terminates "
            "VacuousRun; thirteen of them are what made the weekly cadence "
            "produce nothing for four weeks (alpha-engine-config-I9693). The "
            "REASON is mandatory and rides into the execution input as "
            "vacuous_rerun_accepted_reason (alpha-engine-config-I11075)."
        ),
    )
    ap.add_argument(
        "--rerun-stage",
        action="append",
        default=[],
        metavar="STAGE",
        help="force a stage to re-run even though the deriver witnessed it "
             "completed (repeatable). Use when a stage's prior output is "
             "untrustworthy for a reason outside the execution history — a "
             "redeployed dependency, a producer-contract fix whose only proof "
             "is a fresh run. The reachability guard still applies, so a stage "
             "that cannot be entered by the emitted input fails loudly — "
             "measured: research_self_test, scanner_leaderboard and "
             "backtester_stage_only are gated by more than their own skip "
             "flag and cannot be forced this way.",
    )
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args(argv)

    import boto3  # deferred so the pure functions above stay import-light for tests

    sf = boto3.client("stepfunctions", region_name=args.region)
    ddb = boto3.client("dynamodb", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    if args.execution_arn:
        desc = sf.describe_execution(executionArn=args.execution_arn)
        source = {"executionArn": args.execution_arn, "status": desc["status"], "startDate": desc["startDate"]}
    else:
        source = resolve_default_execution(sf, args.state_machine_arn)
    source_arn, source_status = source["executionArn"], source["status"]
    if source_status not in RERUNNABLE_SOURCE_STATUSES:
        raise SystemExit(
            f"FATAL: source execution {source_arn} is {source_status} — only "
            f"{sorted(RERUNNABLE_SOURCE_STATUSES)} executions can be recovered "
            "(a RUNNING one may still finish; a SUCCEEDED one needs no recovery)."
        )

    # Role-gating check against the LIVE definition (config#2277 D2).
    sm_def = json.loads(sf.describe_state_machine(stateMachineArn=args.state_machine_arn)["definition"])
    verify_skip_flags_live(sm_def, EMITTED_ROLE)

    events = fetch_history(sf, source_arn)
    # alpha-engine-config-I8161: derive from the CHAIN of executions sharing
    # this run_date, not from the source alone. run_date is resolved first
    # (cheaply, from the source's own history) because it is what defines the
    # chain's membership.
    chain_run_date, _prov = derive_run_date(events, source.get("startDate"))
    prior, chain_warnings = collect_chain_histories(
        sf, args.state_machine_arn, chain_run_date, source
    )
    plan = derive_plan(
        events,
        start_time=source.get("startDate"),
        prior_histories=prior,
        source_label=source_arn.rsplit(":", 1)[-1],
        force_rerun=frozenset(args.rerun_stage),
    )
    plan.warnings.extend(chain_warnings)
    name = next_rerun_name(sf, args.state_machine_arn, plan.run_date)
    sm_name = args.state_machine_arn.rsplit(":", 1)[-1]
    source_role = execution_input(events).get("pipeline_role")

    # Skip-coherence pre-flight (alpha-engine-config-I7443): reject an
    # incoherent skip-set BEFORE any mutex work or dispatch, not after one —
    # sf-pipeline-policy §2.5. Checked ahead of the mutex inspection below so
    # a doomed plan never touches DynamoDB or prints a false "OK" mutex line.
    try:
        plan.calendar_date, clamp_note = coerce_calendar_date_for_predictor_skip(
            s3, plan.calendar_date, plan.skip_flags
        )
        if clamp_note:
            plan.notes.append(clamp_note)
        check_predictor_skip_freshness(s3, plan.calendar_date, plan.skip_flags)
    except SkipCoherenceError as exc:
        _print_plan(plan, source_arn, source_status, name, args.state_machine_arn)
        raise SystemExit(f"\nFATAL (skip-coherence, alpha-engine-config-I7443): {exc}")

    # Mutex inspection (read-only here; delete only under --start).
    decision = None
    if source_role in CADENCE_ROLES:
        key = f"{sm_name}#{source_role}#{plan.run_date}"
        item, holder_status = None, None
        try:
            resp = ddb.get_item(TableName=MUTEX_TABLE, Key={"mutex_key": {"S": key}}, ConsistentRead=True)
            item = resp.get("Item")
            if item and (item.get("execution_arn") or {}).get("S"):
                try:
                    holder_status = sf.describe_execution(
                        executionArn=item["execution_arn"]["S"]
                    )["status"]
                except Exception as exc:  # noqa: BLE001 — matrix aborts on unknown holder; recorded via decision.reason
                    print(f"WARN: describe holder failed: {exc}", file=sys.stderr)
            decision = decide_mutex_action(item, holder_status, key, source_arn)
        except ddb.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("AccessDeniedException", "AccessDenied"):
                # Deliberate non-fatal path (see module docstring MUTEX
                # INTERPLAY): the rerun bypasses the mutex (non-cadence
                # role), so the stale item is hygiene, not a correctness
                # gate; the running-execution guard below still blocks the
                # unsafe case. Recording surface: this WARN + manual cmd.
                print(
                    f"WARN: no DynamoDB access to {MUTEX_TABLE} "
                    f"(AccessDenied) — cannot inspect/steal the stale "
                    f"run-slot item for key {key}. The rerun itself is "
                    f"unaffected (role {EMITTED_ROLE!r} bypasses the mutex). "
                    f"Clean it up manually once the holder is terminal:\n"
                    f"  {_manual_delete_cmd(key)}",
                    file=sys.stderr,
                )
            else:
                raise
    else:
        print(
            f"mutex: source role {source_role!r} is non-cadence — no run-slot "
            "item can exist for it; nothing to steal."
        )

    if decision is not None:
        tag = {"proceed": "OK", "steal": "STEAL", "abort": "ABORT"}[decision.action]
        print(f"mutex [{tag}]: {decision.reason}")
        if decision.manual_cmd:
            print(f"  manual: {decision.manual_cmd}")
        if decision.action == "abort":
            _print_plan(plan, source_arn, source_status, name, args.state_machine_arn)
            raise SystemExit(2)

    # Vacuity pre-flight (alpha-engine-config-I11075). Computed against the
    # EMITTED input — cadence-declared skips included — so the verdict is about
    # the document that would actually be dispatched, not the derived subset.
    # Reported here so --dry-run shows it; ENFORCED below, after the plan is
    # printed, because a refusal an operator cannot read the plan behind is a
    # refusal they will work around.
    spine = _landed_spine(args.state_machine_arn)
    if not spine:
        raise VacuousRerunError(
            f"no declared spine for {args.state_machine_arn.rsplit(':', 1)[-1]!r} "
            "in nousergon_lib.pipeline_status.registry.PIPELINE_STAGE_ORDER — "
            "declare it before re-running this pipeline through this script "
            "(alpha-engine-config-I11075)."
        )
    enterable = enabled_spine_stages(sm_def, plan.rerun_input(), spine)
    if enterable:
        plan.notes.append(
            "spine stages this input can still enter: " + ", ".join(enterable)
        )
    else:
        plan.accept_vacuous_reason = args.accept_vacuous_rerun
        plan.warnings.append(
            "VACUOUS: this input can enter NO declared spine stage — it would "
            "run its full wall clock, produce nothing, and terminate "
            "VacuousRun (alpha-engine-config-I11075 / I9693)."
            + (
                f" ACCEPTED: {args.accept_vacuous_rerun!r}, emitted as "
                "vacuous_rerun_accepted_reason."
                if args.accept_vacuous_rerun else
                " --start refuses it without --accept-vacuous-rerun REASON."
            )
        )

    _print_plan(plan, source_arn, source_status, name, args.state_machine_arn)

    if not args.start:
        print("\n(dry-run — nothing mutated; re-run with --start to execute)")
        return 0

    # A FALLBACK-provenance run_date was MINTED from the source execution's
    # own start time, not carried from an explicit input or InitializeInput
    # output — never let --start launch on one silently (alpha-engine-
    # config-I7443: the printed provenance line went unnoticed/unacted-on
    # for exactly this reason on 2026-08-16). --dry-run always shows it
    # above; --start additionally refuses unless explicitly acknowledged.
    refuse_fallback_run_date_without_acceptance(plan, args.accept_fallback_run_date)

    # alpha-engine-config-I11075: a rerun that can enter no spine stage is the
    # skip-to-green shape I9693 measured thirteen times — refused before the
    # dispatch, not diagnosed ~50 minutes later by the SF's own VacuousRun
    # terminal (nousergon-data-PR1808).
    refuse_vacuous_rerun(
        sm_def, args.state_machine_arn, plan.rerun_input(), args.accept_vacuous_rerun
    )

    # --- pre-start guards ---------------------------------------------------
    running = list_all_executions(sf, args.state_machine_arn, status_filter="RUNNING")
    clashing = [
        ex["executionArn"] for ex in running
        if effective_run_date_of(sf, ex) == plan.run_date
    ]
    if clashing:
        raise SystemExit(
            f"FATAL: execution(s) {clashing} are RUNNING with the same "
            f"run_date {plan.run_date} — starting a rerun beside a live "
            "execution races artifact writes. Wait for terminal state or "
            "abort them deliberately first."
        )

    if decision is not None and decision.action == "steal":
        ddb.delete_item(TableName=MUTEX_TABLE, Key={"mutex_key": {"S": decision.key}})
        print(
            f"STOLE run-slot mutex item {decision.key!r}: deleted because "
            f"holder {decision.holder_arn} is terminal "
            f"({decision.holder_status}) and can no longer write artifacts."
        )

    resp = sf.start_execution(
        stateMachineArn=args.state_machine_arn,
        name=name,
        input=json.dumps(plan.rerun_input(), sort_keys=True),
    )
    print(f"\nSTARTED {resp['executionArn']}")
    print(
        "Do not block on it — the sf-telegram-notifier + Fleet-SF Watch "
        "track the outcome."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
