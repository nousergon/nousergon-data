"""Fleet-level per-stage timeout budget table for the Saturday weekly SF.

Generalizes the universe-size budget precedent (config#2938 → fetch_budget.py)
across EVERY weekly-SF stage with an SSM executionTimeout, so a universe jump
re-scales ALL stage ceilings in lockstep instead of surfacing serially at each
site (config#3095 Option A, operator-ruled 2026-07-25).

Why DERIVED, not hardcoded. Three separate incidents at the same ~9x universe
jump — Polygon news sweep (config#2938, fixed), thinktank pillar Lambda
(config#3072), and the backtester spot-eval SSM ceiling (config#3095) — all
followed the same pattern: a static timeout was silently outgrown by universe
growth, surfacing as a clean SIGKILL at the boundary. A shared budget function
makes every future universe jump re-scale all ceilings at once.

Usage:
    from infrastructure.sf_budgets import stage_budgets, recommend_timeout

    budget = recommend_timeout("EvaluatorDiagnostics", universe_size=944)
    # => 1800 (current timeout for the ~944-ticker universe)

Anti-regression guard:
    from infrastructure.sf_budgets import regression_guard

    regression_guard(universe_size=2000)
    # => raises UniverseBudgetExceeded if any stage's required timeout
    #    exceeds its maximum allowed budget, or the pipeline total exceeds
    #    the 43200s global ceiling.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


# ── Current universe reference ────────────────────────────────────────────────
# The ~9x jump from ~100 to ~944 tickers (config#2938 incident universe:
# 79 holdings ∪ 903 AE-signals). This is the "known-good" reference for the
# current timeout floors. Update as the universe changes.
CURRENT_UNIVERSE_SIZE = 944

# Global SF ceiling (config#2274) — the hard upper bound for the sequential
# critical path (post-join stages after the ResearchPredictorParallel).
GLOBAL_SF_CEILING_SECONDS = 43_200  # 12h


@dataclass(frozen=True)
class StageBudget:
    """Budget definition for one weekly-SF stage with an SSM executionTimeout.

    Fields are deliberately named so CI guard tests can pin each one against
    the live ``step_function.json``.
    """

    # Human-readable name for error messages.
    name: str
    # The current SSM ``executionTimeout`` in seconds — the known-good floor
    # for the current universe. CI-guarded against the JSON value.
    current_timeout_seconds: int
    # Estimated per-ticker cost in seconds (None if stage does NOT scale with
    # universe size — e.g. parity checks, health checks, model-only stages).
    # Where None, the budget is constant and has no universe-scaling formula.
    per_ticker_cost_seconds: Optional[float]
    # Constant overhead for this stage not attributable to per-ticker work
    # (bootstrap, fixed init, aggregation, etc.) — the floor even for a
    # single-ticker universe.
    fixed_overhead_seconds: int
    # Absolute cap — the timeout MUST NEVER exceed this (enforced by the
    # anti-regression guard). May be larger than current_timeout_seconds if
    # the operational ceiling allows headroom for known growth.
    max_budget_seconds: int
    # The applicable SF branch for capacity planning: "branch_a" (research
    # side), "branch_b" (predictor side), or "sequential" (post-join tail).
    # Used by the anti-regression guard to sum budgets per pipeline segment.
    pipeline_segment: str

    def recommended_timeout(self, universe_size: int) -> int:
        """Recommended SSM executionTimeout for a given universe size."""
        if self.per_ticker_cost_seconds is None:
            return self.current_timeout_seconds
        raw = self.fixed_overhead_seconds + math.ceil(
            max(universe_size, 0) * self.per_ticker_cost_seconds
        )
        return min(
            max(raw, self.fixed_overhead_seconds),
            self.max_budget_seconds,
        )

    def raw_formula_timeout(self, universe_size: int) -> int:
        """Un-capped formula value — used by the anti-regression guard.

        Returns the raw budget before ``max_budget_seconds`` clamping, so
        the guard can detect when universe growth has pushed the formula
        past what the max budget allows.
        """
        if self.per_ticker_cost_seconds is None:
            return self.current_timeout_seconds
        return self.fixed_overhead_seconds + math.ceil(
            max(universe_size, 0) * self.per_ticker_cost_seconds
        )


# ── Stage budget definitions ─────────────────────────────────────────────────
# Each entry below represents one SSM ``executionTimeout`` in the SF.
# Per-ticker costs where present are ESTIMATED from the current timeout / known
# universe — refined measurements (from live SSM logs with operator creds per
# config#3095's own profiling note) replace these estimates.

STAGE_BUDGETS: dict[str, StageBudget] = {
    # ── Data-collection stages (universe-scaling) ──────────────────────────
    "MorningEnrich": StageBudget(
        name="MorningEnrich",
        current_timeout_seconds=5_400,
        per_ticker_cost_seconds=2.2,  # estimated from current=5400s, fixed=3300s
        fixed_overhead_seconds=3_300,
        max_budget_seconds=10_800,
        pipeline_segment="sequential",
    ),
    # alpha-engine-config-I7176 / -I9201 (2026-08-28): 5_400 -> 6_600. Measured
    # DataPhase1 wall clock on the four most recent real scheduled runs —
    # 2026-08-01 2388s, 08-08 2418s, 08-15 4926s, 08-22 5018s. The step between
    # 08-08 and 08-15 is the retirement of the daily exercise cadence
    # (4159239d, Brian ruling 2026-08-13), whose Friday pass had been writing
    # the .phases/ markers Saturday auto-skipped on; 4926/5018s is the true
    # cold cost, not growth. 6_600 = 5018 x 1.31, well inside max_budget_seconds.
    # Mirrored in infrastructure/spot_data_phase1.sh (MAX_RUNTIME_SECONDS) and
    # step_function.json (executionTimeout / TimeoutSeconds / poll cap 240).
    "DataPhase1": StageBudget(
        name="DataPhase1",
        current_timeout_seconds=6_600,
        per_ticker_cost_seconds=2.2,  # estimated (same dispatch pattern)
        fixed_overhead_seconds=3_300,
        max_budget_seconds=10_800,
        pipeline_segment="sequential",
    ),
    "RAGIngestion": StageBudget(
        name="RAGIngestion",
        current_timeout_seconds=21_600,
        # Polygon free tier: 5 req/min = 12s per request. Each request covers
        # multiple tickers (batch fetch), so the effective per-ticker cost is
        # lower than the raw API rate. Estimated at ~8.5s/ticker averaged across
        # the full ticker universe (batched requests + cache hits + empty tickers
        # that short-circuit). The SEC-filings phase accounts for most of the
        # fixed overhead.
        per_ticker_cost_seconds=8.5,
        fixed_overhead_seconds=5_500,
        max_budget_seconds=21_600,  # config#2938 ruling 2: hard 6h cap
        pipeline_segment="branch_a",
    ),
    # alpha-engine-config-I9329. EvalJudgeProcess became SSM-bearing on
    # 2026-08-29 when the judge moved off a 900s Lambda onto a dedicated EC2
    # spot: it covered 8-15 of an ~83-artifact corpus inside that ceiling,
    # reported complete=False honestly, and returned SUCCESS.
    #
    # This stage does NOT scale with the ticker universe — it scales with the
    # DECISION-ARTIFACT corpus, which is why per_ticker_cost_seconds is 0 and
    # the whole budget sits in fixed_overhead_seconds. Recording it as a
    # per-ticker cost would make the recommendation move with a number that
    # has nothing to do with it.
    #
    # MEASURED (crucible-research-PR766, alpha-engine-config-I9309): 83
    # artifacts at 45-105s per synchronous judge call = 60-145 minutes serial.
    # 10800s (3h) is that worst case plus headroom for corpus growth. It is a
    # budget, not an accommodation: coverage is a HARD failure by Brian's
    # 2026-08-29 ruling, so a run that would exceed this must surface as a
    # stage failure rather than be accommodated by a larger ceiling.
    "EvalJudgeProcess": StageBudget(
        name="EvalJudgeProcess",
        current_timeout_seconds=10_800,
        per_ticker_cost_seconds=0.0,
        fixed_overhead_seconds=8_700,
        max_budget_seconds=10_800,
        pipeline_segment="branch_a",
    ),
    "DataPhase2": StageBudget(
        name="DataPhase2",
        current_timeout_seconds=5_400,
        # MEASURED, not estimated — the only entry in this table that is.
        # alpha-engine-config-I5759, execution 1e856026 (2026-07-30): 402 of
        # 903 tickers in 900s of steady state, 2.16-2.24 s/ticker, FLAT for
        # the whole run. The number is not a throughput estimate, it is a
        # provider-imposed serial FLOOR: _fetch_all_alternative makes two
        # _finnhub_get calls per ticker and collectors/finnhub_client.py
        # sleeps _FINNHUB_MIN_INTERVAL (1.1s) while HOLDING _finnhub_lock,
        # so 2 x 1.1 = 2.2 s/ticker no matter how many workers
        # collectors/alternative.py's ThreadPoolExecutor runs. Concurrency
        # cannot move it; only fewer calls per ticker or a faster provider
        # tier can. That is why this stage left Lambda rather than being
        # parallelised harder.
        per_ticker_cost_seconds=2.2,
        # Spot launch + AL2023 bootstrap (python/git/clone/config) measured at
        # ~7 min across the sibling stages, plus margin for the ArcticDB and
        # S3 preflight legs.
        fixed_overhead_seconds=900,
        max_budget_seconds=10_800,
        pipeline_segment="branch_a",
    ),
    # ── Model/training stages (NOT universe-scaling — scale with
    #    model count and strategy complexity, not ticker count) ──────────
    "PredictorTraining": StageBudget(
        name="PredictorTraining",
        current_timeout_seconds=5_400,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=5_400,
        max_budget_seconds=10_800,
        pipeline_segment="branch_b",
    ),
    "ModelZooSelect": StageBudget(
        name="ModelZooSelect",
        current_timeout_seconds=5_400,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=5_400,
        max_budget_seconds=10_800,
        pipeline_segment="branch_b",
    ),
    "ResolveZooSpecs": StageBudget(
        name="ResolveZooSpecs",
        current_timeout_seconds=600,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=600,
        max_budget_seconds=1_200,
        pipeline_segment="branch_b",
    ),
    "TrainSpecDispatch": StageBudget(
        name="TrainSpecDispatch",
        current_timeout_seconds=5_400,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=5_400,
        max_budget_seconds=10_800,
        pipeline_segment="branch_b",
    ),
    # ── Model/inference stages (NOT universe-scaling — scale with
    #    position count and strategy complexity, not ticker count) ──────────
    "Backtester": StageBudget(
        name="Backtester",
        current_timeout_seconds=7_200,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=7_200,
        max_budget_seconds=14_400,
        pipeline_segment="branch_b",
    ),
    "PredictorBacktest": StageBudget(
        name="PredictorBacktest",
        current_timeout_seconds=7_200,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=7_200,
        max_budget_seconds=14_400,
        pipeline_segment="sequential",
    ),
    "PortfolioOptimizerBacktest": StageBudget(
        name="PortfolioOptimizerBacktest",
        current_timeout_seconds=7_200,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=7_200,
        max_budget_seconds=14_400,
        pipeline_segment="sequential",
    ),
    # ── Quality-assurance stages (NOT universe-scaling) ────────────────────
    # alpha-engine-config#6030: the bundled Parity stage (pit_parity
    # lookahead + walkforward + parity replay behind one launcher) is split
    # into a Parallel of three branch quartets + a PitParityCompare join.
    # CALIBRATION (alpha-engine-config-I6026/I6027 — measured from SSM logs
    # of the two healthy bundled completions, 2026-07-03 + 2026-07-11):
    #   pass1 (lookahead)   12m31s/12m59s  (~780s)
    #   pass2 (walkforward) 10m11s/10m12s  (~610s)
    #   parity replay        2m44s/2m26s   (~160s)
    # Each split stage additionally pays its OWN spot boot/deps (~15 min,
    # ~900s — the bundled stage paid it once). Per-stage budgets below are
    # generous per the calibration philosophy ("start generous, tighten as
    # baselines stabilise") because the 2026-08-01 run took >5x healthy
    # (anomaly under profile, alpha-engine-config-I6029) and was SIGKILLed
    # at the bound. The next tightening is seeded from the per-pass
    # wall_clock_seconds series each pass artifact now records
    # (parity/{date}/pit_stats_{pass}.json, crucible-backtester
    # contracts/pit_stats_pass.schema.json) — measured per stage, not per
    # bundle (I6026 deliverable 2).
    # pipeline_segment="parity_parallel": the three branches run
    # CONCURRENTLY inside ParityParallel — the critical path through them is
    # max(branch), not sum(branch), so they must not inflate the
    # "sequential" capacity sum.
    "PitParityLookahead": StageBudget(
        name="PitParityLookahead",
        current_timeout_seconds=5_400,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=5_400,
        max_budget_seconds=10_800,
        pipeline_segment="parity_parallel",
    ),
    "PitParityWalkforward": StageBudget(
        name="PitParityWalkforward",
        current_timeout_seconds=5_400,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=5_400,
        max_budget_seconds=10_800,
        pipeline_segment="parity_parallel",
    ),
    "ParityReplay": StageBudget(
        name="ParityReplay",
        current_timeout_seconds=2_700,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=2_700,
        max_budget_seconds=7_200,
        pipeline_segment="parity_parallel",
    ),
    # alpha-engine-config-I7267: trivial `aws s3api head-object` marker
    # check, dispatched on the SAME instance only after the pass already
    # exited non-zero — seconds of runtime, no spot boot cost (the box is
    # already up). Budget is deliberately tiny; still inside
    # parity_parallel since it runs concurrently with the sibling branches.
    "PitParityLookaheadResourceKillCheck": StageBudget(
        name="PitParityLookaheadResourceKillCheck",
        current_timeout_seconds=60,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=60,
        max_budget_seconds=90,
        pipeline_segment="parity_parallel",
    ),
    "PitParityWalkforwardResourceKillCheck": StageBudget(
        name="PitParityWalkforwardResourceKillCheck",
        current_timeout_seconds=60,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=60,
        max_budget_seconds=90,
        pipeline_segment="parity_parallel",
    ),
    # The compare join reads two small JSONs and computes numpy stats —
    # seconds of compute; the ~15 min spot boot/deps dominates.
    "PitParityCompare": StageBudget(
        name="PitParityCompare",
        current_timeout_seconds=2_700,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=2_700,
        max_budget_seconds=7_200,
        pipeline_segment="sequential",
    ),
    # ── Evaluation stages (split by config-I3112 deliverable 3) ────────────
    #
    # THE PROFILING NOTE IS DISCHARGED. config#3095 left this stage with an
    # explicitly UNMEASURED 6.9 s/ticker estimate and a note that measuring it
    # needed operator ssm:GetCommandInvocation to pull per-stage timing. Both
    # sources now exist and were read on 2026-08-11:
    #
    #   * SF execution history, the 2026-08-08 SUCCEEDED weekly run:
    #     CheckSkipEvaluator 06:06:03 -> CheckSkipPostEval 06:14:06 = 482s
    #     wall-clock, against the 7200s ceiling — 6.7% utilisation.
    #   * Phase markers under backtest/{date}/.phases/, three consecutive
    #     weekly runs (2026-07-31 / 2026-08-04 / 2026-08-07):
    #         evaluator_signal_quality        7.85 /   8.43 /   9.64
    #         evaluator_diagnostics         198.67 / 231.43 / 230.18
    #         evaluator_optimizers           29.71 /  33.86 /  33.82
    #         assembler+apply_audit+champion+regression      ~1.7 / ~1.9 / ~2.0
    #   * evaluate.py's own span in the stage log: 282s. The remaining ~200s
    #     of the 482s is the spot request, boot, bootstrap, smoke and one 60s
    #     poll tick — a FLOOR both halves pay independently, because
    #     spot_evaluator.sh dispatches and terminates its own instance.
    #
    # The old estimate predicted 944 * 6.9 + 600 = 7114s for a stage that
    # takes 482s: ~15x too high. It was never wrong in a way anything could
    # notice, because a ceiling is only ever observed when it is hit.
    #
    # PER-TICKER COST IS AN UPPER BOUND, NOT A FIT. All three runs sit at the
    # same ~944-ticker universe, so these measurements contain no slope. Each
    # half's per-ticker figure below attributes 100% of its measured phase
    # time to universe scaling — deliberately pessimistic, since the true
    # slope cannot be below zero and cannot be above this. Re-derive the day
    # the universe actually moves; until then, treat the slope as unmeasured
    # and the intercept as measured.
    "EvaluatorDiagnostics": StageBudget(
        name="EvaluatorDiagnostics",
        current_timeout_seconds=1_800,
        # (9.64 + 230.18) / 944, the worst observed run.
        per_ticker_cost_seconds=0.254,
        # Spot request + boot + bootstrap + smoke + one poll tick, plus the
        # S3 handoff snapshot write this half owns.
        fixed_overhead_seconds=300,
        max_budget_seconds=7_200,
        pipeline_segment="sequential",
    ),
    "EvaluatorOptimize": StageBudget(
        name="EvaluatorOptimize",
        current_timeout_seconds=1_200,
        # (33.86 + ~2.0) / 944, the worst observed run.
        per_ticker_cost_seconds=0.038,
        # Same boot floor, plus the snapshot read. This half's ceiling is
        # made of its overhead, not of its work.
        fixed_overhead_seconds=300,
        max_budget_seconds=7_200,
        pipeline_segment="sequential",
    ),
    # ── Health-check stages (constant, small) ──────────────────────────────
    "SaturdayHealthCheck": StageBudget(
        name="SaturdayHealthCheck",
        current_timeout_seconds=300,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=300,
        max_budget_seconds=600,
        pipeline_segment="sequential",
    ),
    "WeeklySubstrateHealthCheck": StageBudget(
        name="WeeklySubstrateHealthCheck",
        current_timeout_seconds=240,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=240,
        max_budget_seconds=480,
        pipeline_segment="sequential",
    ),
    # alpha-engine-config-I11312: the observe-mode on-spot preflight pass.
    # Constant, not per-ticker: its universe-touching checks SAMPLE
    # (universe_sample_freshness reads 20 symbols) or make ONE grouped call
    # (polygon_grouped_coverage). sf_preflight_on_spot.BUDGET_SECONDS (420)
    # sits inside this so a hang is a recorded ERROR, not an SSM TimedOut.
    "WeeklyPreflightOnSpot": StageBudget(
        name="WeeklyPreflightOnSpot",
        current_timeout_seconds=600,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=600,
        max_budget_seconds=900,
        pipeline_segment="sequential",
    ),
}


def recommend_timeout(stage_name: str, universe_size: int = CURRENT_UNIVERSE_SIZE) -> int:
    """Return the recommended SSM executionTimeout for *stage_name*.

    Raises ``KeyError`` if the stage is unknown.
    """
    return STAGE_BUDGETS[stage_name].recommended_timeout(universe_size)


# ── Anti-regression guard ────────────────────────────────────────────────────

class UniverseBudgetExceeded(Exception):
    """Raised when a projected universe size exceeds the available budget."""


def regression_guard(
    universe_size: int,
    _global_ceiling: int = GLOBAL_SF_CEILING_SECONDS,
) -> None:
    """Fail LOUD if *universe_size* would push any stage past its max budget.

    The 43200s global SF ceiling (config#2274) is a HANG-PROTECTION backstop,
    not a budget pool — the actual pipeline completes in <4h even at 944
    tickers. The individual-stage cap check below is the real anti-regression:
    a universe jump that would require a stage to exceed its ``max_budget``
    MUST fail CI (e.g. a 3x universe making the Evaluator require >14400s).
    """
    for name, budget in STAGE_BUDGETS.items():
        required = budget.raw_formula_timeout(universe_size)
        if required > budget.max_budget_seconds:
            extra: str = ""
            if budget.per_ticker_cost_seconds is not None:
                extra = (
                    f"  Per-ticker cost: {budget.per_ticker_cost_seconds}s "
                    f"(estimated; needs live measurement)\n"
                    f"  Fixed overhead:  {budget.fixed_overhead_seconds}s\n"
                    f"  Universe:        {universe_size} tickers\n"
                    f"  To fit:          increase max_budget_seconds or "
                    f"optimize {budget.name} stage"
                )
            raise UniverseBudgetExceeded(
                f"Universe growth budget exceeded for stage '{name}':\n"
                f"  Required:  {required}s\n"
                f"  Max budget: {budget.max_budget_seconds}s\n"
                f"{extra}"
            )


def pipeline_budget_summary(universe_size: int = CURRENT_UNIVERSE_SIZE) -> dict:
    """Human-readable budget summary for all stages at a given universe size."""
    rows = []
    for name in sorted(STAGE_BUDGETS):
        b = STAGE_BUDGETS[name]
        rec = b.recommended_timeout(universe_size)
        rows.append({
            "stage": name,
            "current": b.current_timeout_seconds,
            "recommended": rec,
            "max": b.max_budget_seconds,
            "segment": b.pipeline_segment,
            "scales": b.per_ticker_cost_seconds is not None,
        })
    # Sequential path total (post-join: sum of all sequential stages).
    seq_total = sum(
        b.recommended_timeout(universe_size)
        for b in STAGE_BUDGETS.values()
        if b.pipeline_segment == "sequential"
    )
    return {
        "universe_size": universe_size,
        "global_ceiling": GLOBAL_SF_CEILING_SECONDS,
        "sequential_total": seq_total,
        "stages": rows,
    }
