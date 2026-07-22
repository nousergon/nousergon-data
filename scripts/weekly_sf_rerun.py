#!/usr/bin/env python3
"""Mechanical weekly-SF recovery helper (config#2277).

Derives, from a FAILED ``ne-weekly-freshness-pipeline`` execution, the exact
``StartExecution`` input for a correctly-scoped recovery rerun:

- the ORIGINAL ``run_date`` (from the failed execution's ``InitializeInput``
  output — a fresh manual rerun without an explicit run_date gets a NEW date
  stamped from Execution.StartTime and writes to different artifact prefixes,
  orphaning the prior partial run);
- the derived ``skip_*`` flag set for every stage the failed execution
  completed (re-running a succeeded side-effecting stage duplicates its
  effects — 2026-07-11: duplicate model-zoo promotion emails, config#2252);
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
from datetime import datetime, timezone

DEFAULT_STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
)
MUTEX_TABLE = "alpha-engine-sf-execution-mutex"
EMITTED_ROLE = "watch-rerun"
# Roles that acquire the run-slot mutex (CheckMutexRole allowlist — kept in
# lockstep by tests/test_weekly_sf_rerun.py against the SF definition).
CADENCE_ROLES = frozenset({"daily", "weekly", "eod", "shell-run"})
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})
RERUNNABLE_SOURCE_STATUSES = frozenset({"FAILED", "TIMED_OUT", "ABORTED"})


# ---------------------------------------------------------------------------
# Declarative stage table — pinned against infrastructure/step_function.json
# by tests/test_weekly_sf_rerun.py (witness = the state the SF enters iff the
# stage completed successfully OR was skipped; either way the rerun must not
# re-run it, and originally-skipped stages carry their flag from the
# preserved original input anyway).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Stage:
    name: str
    flag: str
    gate: str                      # the CheckSkip* Choice state
    work: str                      # the stage's first work state
    witness: frozenset             # entered => completed-or-skipped
    emit_skip: bool = True         # False => never emit the flag (see notes)
    detect_failure: bool = True    # False => another Stage row already owns
                                    # this `work` state's failure detection
                                    # (config#2362: skip_backtester_stage_only
                                    # shares Backtester's work state with the
                                    # "backtester" row above it)
    note: str = ""


STAGES: tuple[Stage, ...] = (
    Stage(
        "lib_pin_drift_check", "skip_lib_pin_drift_check",
        "CheckSkipLibPinDriftCheck", "LibPinDriftCheck",
        frozenset({"CheckMutexRole"}),
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
    ),
    Stage(
        "regime_substrate", "skip_regime_substrate",
        "CheckSkipRegimeSubstrate", "RegimeSubstrate",
        frozenset({"CheckSkipSignalsEnvelope"}),
    ),
    Stage(
        "signals_envelope", "skip_signals_envelope",
        "CheckSkipSignalsEnvelope", "SignalsEnvelope",
        frozenset({"CheckSkipChallengerShadow"}),
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
        "challenger_shadow", "skip_challenger_shadow",
        "CheckSkipChallengerShadow", "ChallengerShadow",
        frozenset({"CheckSkipRAGIngestion"}),
    ),
    Stage(
        "rag_ingestion", "skip_rag_ingestion",
        "CheckSkipRAGIngestion", "RAGIngestion",
        frozenset({"CheckSkipThinkTankCoverage"}),
    ),
    Stage(
        "thinktank_coverage", "skip_thinktank_coverage",
        "CheckSkipThinkTankCoverage", "ThinkTankCoverage",
        frozenset({"CheckSkipRegimeRetrospectiveEval"}),
    ),
    Stage(
        "regime_retrospective_eval", "skip_regime_retrospective_eval",
        "CheckSkipRegimeRetrospectiveEval", "RegimeRetrospectiveEval",
        frozenset({"CheckSkipDataPhase2"}),
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
    ),
    Stage(
        "rationale_clustering", "skip_rationale_clustering",
        "CheckSkipRationaleClustering", "RationaleClustering",
        frozenset({"CheckSkipReplayConcordance"}),
    ),
    Stage(
        "replay_concordance", "skip_replay_concordance",
        "CheckSkipReplayConcordance", "ReplayConcordance",
        frozenset({"CheckSkipCounterfactual"}),
    ),
    Stage(
        "counterfactual", "skip_counterfactual",
        "CheckSkipCounterfactual", "Counterfactual",
        frozenset({"CheckSkipAggregateCosts"}),
    ),
    Stage(
        "aggregate_costs", "skip_aggregate_costs",
        "CheckSkipAggregateCosts", "AggregateCosts",
        frozenset({"BranchAComplete"}),
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
    Stage(
        "parity", "skip_parity",
        "CheckSkipParity", "Parity",
        frozenset({"CheckSkipEvaluator"}),
    ),
    Stage(
        "evaluator", "skip_evaluator",
        "CheckSkipEvaluator", "Evaluator",
        frozenset({"CheckSkipPostEval"}),
    ),
    Stage(
        "post_eval", "skip_post_eval",
        "CheckSkipPostEval", "SaturdayHealthCheck",
        frozenset({"CheckShellRunNotify"}),
        note=(
            "skip_post_eval covers the whole health-check/report-card/"
            "director tail; a failure inside it re-runs the whole tail"
            " (no finer-grained flags exist)."
        ),
    ),
)

STAGES_BY_NAME = {s.name: s for s in STAGES}
BRANCH_A_STAGES = frozenset({
    # alpha-engine-config-I2515 Phase B: "research" removed (the
    # multi-agent Research state — and its skip_research flag /
    # CheckSkipResearch gate — no longer exists). config#3134: scanner,
    # signals_envelope, challenger_shadow, thinktank_coverage added once
    # each got its own CheckSkip* gate.
    "scanner", "regime_substrate", "signals_envelope", "challenger_shadow",
    "rag_ingestion", "thinktank_coverage", "regime_retrospective_eval",
    "data_phase2", "eval_judge", "rationale_clustering",
    "replay_concordance", "counterfactual", "aggregate_costs",
})
# Stages whose gate is only reachable THROUGH CheckSkipBacktester's run path
# (the skip route overshoots them — see Stage("backtester").note).
BACKTESTER_OVERSHADOWED = ("predictor_backtest", "portfolio_optimizer_backtest", "parity")


# ---------------------------------------------------------------------------
# Execution-history parsing (pure functions — unit-tested over fixtures)
# ---------------------------------------------------------------------------

def entered_states(events: list[dict]) -> set:
    return {
        e["stateEnteredEventDetails"]["name"]
        for e in events
        if "stateEnteredEventDetails" in e
    }


def execution_input(events: list[dict]) -> dict:
    for e in events:
        d = e.get("executionStartedEventDetails")
        if d is not None:
            return json.loads(d.get("input") or "{}")
    raise SystemExit("FATAL: history carries no ExecutionStarted event — cannot recover the original input.")


def initialize_input_output(events: list[dict]) -> dict | None:
    """The merged object InitializeInput emitted — the authoritative source
    of the run_date every subsequent stage actually keyed its artifacts on."""
    for e in events:
        d = e.get("stateExitedEventDetails")
        if d is not None and d.get("name") == "InitializeInput":
            try:
                return json.loads(d.get("output") or "null")
            except json.JSONDecodeError:
                return None
    return None


def derive_run_date(events: list[dict], start_time: datetime | None) -> tuple[str, str]:
    """Return (run_date, provenance). Precedence: explicit input run_date >
    InitializeInput's merged output > date(Execution start time)."""
    orig = execution_input(events)
    if isinstance(orig.get("run_date"), str) and orig["run_date"]:
        return orig["run_date"], "explicit run_date in the failed execution's input"
    init = initialize_input_output(events)
    if isinstance(init, dict) and isinstance(init.get("run_date"), str) and init["run_date"]:
        return init["run_date"], "InitializeInput merged output of the failed execution"
    if start_time is not None:
        rd = start_time.astimezone(timezone.utc).date().isoformat()
        return rd, (
            "FALLBACK: UTC date of the failed execution's start time"
            " (InitializeInput never exited — pre-workload failure)"
        )
    raise SystemExit(
        "FATAL: cannot derive run_date — no explicit input run_date, no "
        "InitializeInput output in history, and no execution start time "
        "was supplied."
    )


@dataclass
class RerunPlan:
    run_date: str
    run_date_provenance: str
    original_input: dict
    completed: list = field(default_factory=list)   # stage names
    failed: list = field(default_factory=list)      # stage names
    skip_flags: dict = field(default_factory=dict)  # flag -> True
    warnings: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    def rerun_input(self) -> dict:
        out = dict(self.original_input)
        out["run_date"] = self.run_date
        out["pipeline_role"] = EMITTED_ROLE
        out.update(self.skip_flags)
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
    if effective["backtester"]:
        pass  # backtester, predictor_backtest, portfolio_optimizer_backtest, parity all bypassed
    elif effective["backtester_stage_only"]:
        # config#2362 Option A: only the Backtester SSM task is bypassed;
        # the tail gates still compose orthogonally past it.
        run_linear(["predictor_backtest", "portfolio_optimizer_backtest", "parity"])
    else:
        run_linear(["backtester", "predictor_backtest", "portfolio_optimizer_backtest", "parity"])
    run_linear(["evaluator", "post_eval"])
    return ran


def derive_plan(events: list[dict], start_time: datetime | None = None) -> RerunPlan:
    entered = entered_states(events)
    original_input = execution_input(events)
    run_date, provenance = derive_run_date(events, start_time)
    plan = RerunPlan(run_date=run_date, run_date_provenance=provenance,
                     original_input=original_input)

    for stage in STAGES:
        if entered & stage.witness:
            plan.completed.append(stage.name)
            if stage.emit_skip:
                plan.skip_flags[stage.flag] = True
            elif stage.note:
                plan.notes.append(f"{stage.name}: {stage.note}")
        elif stage.detect_failure and stage.work in entered:
            plan.failed.append(stage.name)

    if not plan.failed:
        plan.warnings.append(
            "no failed WORK stage identified — the failure was pre-workload "
            "(gate / mutex / notifier). Fix the root cause first; this rerun "
            "input re-runs everything not witnessed complete."
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
    if "skip_backtester" in plan.skip_flags and any(
        f in plan.failed for f in BACKTESTER_OVERSHADOWED
    ):
        del plan.skip_flags["skip_backtester"]
        plan.skip_flags["skip_backtester_stage_only"] = True
        plan.notes.append(
            "skip_backtester replaced with skip_backtester_stage_only: "
            "Backtester completed but its whole-pair skip route would bypass "
            f"failed stage(s) {[f for f in plan.failed if f in BACKTESTER_OVERSHADOWED]} "
            "— skipping only the Backtester SSM task (reusing its "
            "already-written artifacts) instead of re-burning it. config#2362."
        )

    reachable = _simulate_reachable_works(plan.skip_flags, original_input)
    unreachable_failed = [f for f in plan.failed if f not in reachable]
    if unreachable_failed:
        raise SystemExit(
            f"FATAL: derived skip set would make failed stage(s) "
            f"{unreachable_failed} unreachable — refusing to emit an input "
            f"that silently skips a failed stage. Flags: "
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

def _walk_states(states: dict):
    for name, state in states.items():
        yield name, state
        if state.get("Type") == "Parallel":
            for branch in state.get("Branches", []):
                yield from _walk_states(branch.get("States", {}))
        if state.get("Type") == "Map":
            it = state.get("Iterator") or state.get("ItemProcessor") or {}
            yield from _walk_states(it.get("States", {}))


def _rule_role_values(rule: dict) -> tuple[bool, set]:
    """Return (references_pipeline_role, {StringEquals values on it})."""
    refs = False
    values: set = set()

    def rec(node):
        nonlocal refs
        if isinstance(node, dict):
            if node.get("Variable") == "$.pipeline_role":
                refs = True
                if "StringEquals" in node:
                    values.add(node["StringEquals"])
            for key in ("And", "Or"):
                for sub in node.get(key, []) or []:
                    rec(sub)
            if "Not" in node:
                rec(node["Not"])

    rec(rule)
    return refs, values


def verify_skip_flags_live(definition: dict, role: str) -> None:
    """Fail LOUDLY if any CheckSkip* gate structurally conjuncts
    pipeline_role in a way that would render our skip flags inert under
    `role` (the EOD SF's config#1614 pattern). A helper that silently emits
    inert skip flags re-burns every completed spot stage."""
    offenders = []
    for name, state in _walk_states(definition.get("States", {})):
        if not name.startswith("CheckSkip") or state.get("Type") != "Choice":
            continue
        for rule in state.get("Choices", []):
            refs, values = _rule_role_values(rule)
            if refs and role not in values:
                offenders.append((name, sorted(values)))
    if offenders:
        raise SystemExit(
            "FATAL (role gating): the weekly SF now conjuncts pipeline_role "
            f"inside skip gates {offenders}, and role {role!r} is not in the "
            "live set — the skip flags this helper emits would be silently "
            "IGNORED and every completed spot stage would re-burn. Update "
            "scripts/weekly_sf_rerun.py's EMITTED_ROLE / derivation to match "
            "the SF's new role-gate semantics before rerunning."
        )


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

def fetch_history(sf, execution_arn: str) -> list[dict]:
    events, token = [], None
    while True:
        kwargs = {"executionArn": execution_arn, "maxResults": 1000}
        if token:
            kwargs["nextToken"] = token
        resp = sf.get_execution_history(**kwargs)
        events.extend(resp["events"])
        token = resp.get("nextToken")
        if not token:
            return events


def list_all_executions(sf, sm_arn: str, status_filter: str | None = None, cap: int = 1000) -> list[dict]:
    out, token = [], None
    while len(out) < cap:
        kwargs = {"stateMachineArn": sm_arn, "maxResults": 200}
        if status_filter:
            kwargs["statusFilter"] = status_filter
        if token:
            kwargs["nextToken"] = token
        resp = sf.list_executions(**kwargs)
        out.extend(resp["executions"])
        token = resp.get("nextToken")
        if not token:
            break
    return out[:cap]


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


def effective_run_date_of(sf, execution: dict) -> str:
    try:
        desc = sf.describe_execution(executionArn=execution["executionArn"])
        inp = json.loads(desc.get("input") or "{}")
        if isinstance(inp.get("run_date"), str) and inp["run_date"]:
            return inp["run_date"]
    except Exception as exc:  # noqa: BLE001 — guard is best-effort per-exec; date fallback below is conservative
        print(f"WARN: could not read input of {execution['executionArn']}: {exc}", file=sys.stderr)
    return execution["startDate"].astimezone(timezone.utc).date().isoformat()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_plan(plan: RerunPlan, source_arn: str, source_status: str, name: str, sm_arn: str) -> None:
    print(f"source execution : {source_arn} ({source_status})")
    print(f"run_date         : {plan.run_date}  [{plan.run_date_provenance}]")
    print(f"rerun name       : {name}")
    print(f"pipeline_role    : {EMITTED_ROLE}")
    print(f"completed stages : {', '.join(plan.completed) or '(none)'}")
    print(f"failed stages    : {', '.join(plan.failed) or '(none identified)'}")
    print(f"derived skips    : {', '.join(sorted(plan.skip_flags)) or '(none)'}")
    for n in plan.notes:
        print(f"NOTE : {n}")
    for w in plan.warnings:
        print(f"WARN : {w}", file=sys.stderr)
    rerun_input = json.dumps(plan.rerun_input(), indent=2, sort_keys=True)
    print("\nStartExecution input:")
    print(rerun_input)
    print("\nequivalent CLI:")
    print(
        f"aws stepfunctions start-execution --state-machine-arn {sm_arn} "
        f"--name {name} --input '{json.dumps(plan.rerun_input(), sort_keys=True)}'"
    )


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--execution-arn", help="failed execution to recover (default: latest FAILED/TIMED_OUT)")
    ap.add_argument("--state-machine-arn", default=DEFAULT_STATE_MACHINE_ARN)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="derive + print only (default)")
    mode.add_argument("--start", action="store_true", help="run pre-start guards, steal stale mutex item if safe, StartExecution")
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args(argv)

    import boto3  # deferred so the pure functions above stay import-light for tests

    sf = boto3.client("stepfunctions", region_name=args.region)
    ddb = boto3.client("dynamodb", region_name=args.region)

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
    plan = derive_plan(events, start_time=source.get("startDate"))
    name = next_rerun_name(sf, args.state_machine_arn, plan.run_date)
    sm_name = args.state_machine_arn.rsplit(":", 1)[-1]
    source_role = execution_input(events).get("pipeline_role")

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

    _print_plan(plan, source_arn, source_status, name, args.state_machine_arn)

    if not args.start:
        print("\n(dry-run — nothing mutated; re-run with --start to execute)")
        return 0

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
