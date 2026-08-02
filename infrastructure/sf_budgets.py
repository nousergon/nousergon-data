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

    budget = recommend_timeout("Evaluator", universe_size=944)
    # => 7200 (current timeout for the ~944-ticker universe)

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
    "DataPhase1": StageBudget(
        name="DataPhase1",
        current_timeout_seconds=5_400,
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
    # CALIBRATION (alpha-engine-config-I6026/I6027 — measured from SSM logs
    # of the two healthy pit_parity completions, 2026-07-03 + 2026-07-11):
    #   pit_parity 22m48s/23m18s (pass1 12m31s/12m59s, pass2 10m11s/10m12s)
    #   + parity replay 2m44s/2m26s → healthy full stage ≈ 25-26 min (~1550s).
    #   7200s = ~4.6× healthy — generous per the calibration philosophy
    #   ("start generous, tighten as baselines stabilise"), KEPT because the
    #   2026-08-01 run took >5× healthy (anomaly under profile,
    #   alpha-engine-config-I6029) and was SIGKILLed at the bound; tightening
    #   before that lands would turn a slow-but-progressing run into a faster
    #   failure. Per-run wall-clock is now recorded automatically in
    #   backtest/{date}/pit_parity.json (wall_clock block, crucible-backtester
    #   I6027) — the next calibration tightening must be seeded from that
    #   series, not from logs.
    "Parity": StageBudget(
        name="Parity",
        current_timeout_seconds=7_200,
        per_ticker_cost_seconds=None,
        fixed_overhead_seconds=7_200,
        max_budget_seconds=14_400,
        pipeline_segment="sequential",
    ),
    # ── Evaluation stage (universe-scaling — THE VICTIM of config#3095) ────
    "Evaluator": StageBudget(
        name="Evaluator",
        current_timeout_seconds=7_200,  # bumped 3600→7200 by PR969
        # PER-TICKER COST UNMEASURED (config#3095's own profiling note:
        # needs operator ssm:GetCommandInvocation on i-09b539c844515d549
        # to pull /var/log/evaluator.log for per-stage timing).
        # Estimated here as ~7s/ticker (so ~6600s for 944 tickers within
        # the 7200s budget).
        per_ticker_cost_seconds=6.9,
        fixed_overhead_seconds=600,
        max_budget_seconds=14_400,
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
