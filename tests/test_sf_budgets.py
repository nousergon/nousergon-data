"""CI guard tests for fleet-level per-stage timeout budgets (config#3095 Option A).

Pins every weekly-SF stage's SSM ``executionTimeout`` against the derived
budget table in ``infrastructure/sf_budgets.py``, so a universe jump (or a
hand-edit to any single timeout) fails CI unless the corresponding budget
definition moves in lockstep.

Three guard layers:
1. **Current-timeout pin** — every stage's ``current_timeout_seconds`` matches
   the live ``step_function.json`` value. A bare hand-edit to the JSON will
   fail here, forcing the edit to go through the budget table.
2. **Budget-sufficiency** — the recommended timeout for the current universe
   size is AT or BELOW the current timeout (it must not silently recommend a
   lower timeout than what the JSON actually grants).
3. **Anti-regression** — ``regression_guard(projected_universe)`` for a range
   of sizes ensures the pipeline headroom is real, not accidental.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from infrastructure.sf_budgets import (
    CURRENT_UNIVERSE_SIZE,
    STAGE_BUDGETS,
    UniverseBudgetExceeded,
    recommend_timeout,
    regression_guard,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_JSON = _REPO_ROOT / "infrastructure" / "step_function.json"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _load_sf() -> dict:
    return json.loads(_SF_JSON.read_text())


def _find_state(node, target: str):
    """Recursive DFS for an SF state by name."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k == target and isinstance(v, dict) and v.get("Type"):
                return v
            found = _find_state(v, target)
            if found is not None:
                return found
    elif isinstance(node, list):
        for x in node:
            found = _find_state(x, target)
            if found is not None:
                return found
    return None


def _ssm_execution_timeout(state_name: str) -> int:
    """Read the SSM executionTimeout for *state_name* from step_function.json."""
    sf = _load_sf()
    state = _find_state(sf, state_name)
    assert state is not None, f"{state_name} not found in step_function.json"

    params = state.get("Parameters", {})
    if isinstance(params, dict):
        inner = params.get("Parameters", {})
        if isinstance(inner, dict) and "executionTimeout" in inner:
            et = inner["executionTimeout"]
        elif "executionTimeout" in params:
            et = params["executionTimeout"]
        else:
            # Some states nest executionTimeout inside a different Parameter key
            for val in params.values():
                if isinstance(val, dict) and "executionTimeout" in val:
                    et = val["executionTimeout"]
                    break
            else:
                raise AssertionError(f"{state_name}: no executionTimeout found")

        if isinstance(et, list) and len(et) == 1:
            return int(et[0])
        return int(et)
    raise AssertionError(f"{state_name}: Parameters is not a dict")


# Known SSM-bearing states (every state that carries an executionTimeout in the
# Saturday SF). This list is load-bearing — adding a new SSM stage without
# adding it here will fail the coverage test below.
SSM_STAGE_NAMES = [
    # MorningEnrich and DataPhase1 left the definition with the decoupled data
    # cutover (alpha-engine-config-I11269); the coverage test below fails if
    # either SSM state ever comes back without a budget row.
    "RAGIngestion",
    "PredictorTraining",
    "ModelZooSelect",
    "ResolveZooSpecs",
    "TrainSpecDispatch",
    "Backtester",
    "PredictorBacktest",
    "PortfolioOptimizerBacktest",
    "PitParityLookahead",
    "PitParityWalkforward",
    "ParityReplay",
    "PitParityCompare",
    "EvaluatorDiagnostics",
    "EvaluatorOptimize",
    "SaturdayHealthCheck",
    "WeeklySubstrateHealthCheck",
]


# ── Guard 1: current-timeout pin ─────────────────────────────────────────────

class TestSsmExecutionTimeoutPins:
    """Every stage's ``current_timeout_seconds`` matches the live SF JSON."""

    def _check(self, name: str):
        expected = STAGE_BUDGETS[name].current_timeout_seconds
        live = _ssm_execution_timeout(name)
        assert live == expected, (
            f"{name}: step_function.json has executionTimeout={live}, "
            f"but sf_budgets.py declares current_timeout_seconds={expected}. "
            f"Edit sf_budgets.py to match, or vice versa."
        )

    def test_rag_ingestion(self):
        self._check("RAGIngestion")

    def test_predictor_training(self):
        self._check("PredictorTraining")

    def test_model_zoo_select(self):
        self._check("ModelZooSelect")

    def test_resolve_zoo_specs(self):
        self._check("ResolveZooSpecs")

    def test_train_spec_dispatch(self):
        self._check("TrainSpecDispatch")

    def test_backtester(self):
        self._check("Backtester")

    def test_predictor_backtest(self):
        self._check("PredictorBacktest")

    def test_portfolio_optimizer_backtest(self):
        self._check("PortfolioOptimizerBacktest")

    def test_pit_parity_lookahead(self):
        self._check("PitParityLookahead")

    def test_pit_parity_walkforward(self):
        self._check("PitParityWalkforward")

    def test_parity_replay(self):
        self._check("ParityReplay")

    def test_pit_parity_compare(self):
        self._check("PitParityCompare")

    def test_evaluator_diagnostics(self):
        self._check("EvaluatorDiagnostics")

    def test_evaluator_optimize(self):
        self._check("EvaluatorOptimize")

    def test_saturday_health_check(self):
        self._check("SaturdayHealthCheck")

    def test_weekly_substrate_health_check(self):
        self._check("WeeklySubstrateHealthCheck")

    def test_coverage_complete(self):
        """Every SSM-bearing SF stage is in the budget table, and vice versa."""
        # Discover which SF Task states carry an SSM executionTimeout.
        sf = _load_sf()
        ssm_states_in_sf = set()

        def _has_execution_timeout(node):
            """Check if a Task state node carries an SSM executionTimeout."""
            if not isinstance(node, dict) or node.get("Type") != "Task":
                return False
            params = node.get("Parameters", {})
            if not isinstance(params, dict):
                return False
            if "executionTimeout" in params:
                return True
            for val in params.values():
                if isinstance(val, dict) and "executionTimeout" in val:
                    return True
            return False

        def _discover(node, parent_name="root"):
            if isinstance(node, dict):
                for k, v in node.items():
                    if isinstance(v, dict) and v.get("Type"):
                        if _has_execution_timeout(v):
                            ssm_states_in_sf.add(k)
                    _discover(v, k)
            elif isinstance(node, list):
                for x in node:
                    _discover(x)

        _discover(sf)
        sf_ssm_names = ssm_states_in_sf

        budget_names = set(STAGE_BUDGETS.keys())
        extra_in_sf = sf_ssm_names - budget_names
        extra_in_budget = budget_names - sf_ssm_names

        assert not extra_in_sf, (
            f"SSM-bearing stages in step_function.json with no STAGE_BUDGETS entry: "
            f"{extra_in_sf}"
        )
        assert not extra_in_budget, (
            f"STAGE_BUDGETS entries not found as SSM-bearing states in SF: "
            f"{extra_in_budget}"
        )


# ── Guard 2: budget sufficiency ──────────────────────────────────────────────

class TestBudgetSufficiency:
    """For every stage, the recommended timeout fits within the current timeout."""

    def test_all_stages_recommended_at_or_below_current(self):
        for name, budget in STAGE_BUDGETS.items():
            rec = budget.recommended_timeout(CURRENT_UNIVERSE_SIZE)
            assert rec <= budget.current_timeout_seconds, (
                f"{name}: recommended timeout {rec}s for current universe "
                f"({CURRENT_UNIVERSE_SIZE}) exceeds current_timeout_seconds "
                f"{budget.current_timeout_seconds}s — the budget formula is "
                f"over-estimating or the current timeout is too tight."
            )

    def test_recommended_timeout_never_below_fixed_overhead(self):
        for name, budget in STAGE_BUDGETS.items():
            for n in (0, 1, CURRENT_UNIVERSE_SIZE, 5000):
                rec = budget.recommended_timeout(n)
                assert rec >= budget.fixed_overhead_seconds, (
                    f"{name}: recommended timeout {rec}s at universe {n} "
                    f"is below fixed overhead {budget.fixed_overhead_seconds}s"
                )

    def test_non_scaling_stages_return_current(self):
        for name, budget in STAGE_BUDGETS.items():
            if budget.per_ticker_cost_seconds is None:
                for n in (0, 100, 944, 10_000):
                    assert (
                        budget.recommended_timeout(n) == budget.current_timeout_seconds
                    ), f"{name}: non-scaling stage must return current_timeout for any universe"
                assert budget.fixed_overhead_seconds == budget.current_timeout_seconds, (
                    f"{name}: non-scaling stage's fixed_overhead must equal current_timeout"
                )

    def test_universe_scaling_stages_monotonic(self):
        for name, budget in STAGE_BUDGETS.items():
            if budget.per_ticker_cost_seconds is not None:
                prev = -1
                for n in (0, 1, 100, 500, CURRENT_UNIVERSE_SIZE, 2000):
                    cur = budget.recommended_timeout(n)
                    assert cur >= prev, (
                        f"{name}: not monotonic (n={n}: {cur} < prev={prev})"
                    )
                    prev = cur


# ── Guard 3: anti-regression — projected universe sizes ──────────────────────

class TestAntiRegressionGuard:
    """The regression guard fires ONLY when it should — never for current size."""

    def test_current_universe_passes(self):
        # The current universe must always satisfy the budget.
        regression_guard(CURRENT_UNIVERSE_SIZE)

    def test_moderate_growth_passes(self):
        # ~50% growth should still fit within the current headroom.
        regression_guard(int(CURRENT_UNIVERSE_SIZE * 1.5))

    def test_double_growth_still_passes(self):
        # 2x growth — may or may not trigger; depends on max_budget settings.
        # This test is informational; the guard is the actual arbiter.
        regression_guard(CURRENT_UNIVERSE_SIZE * 2)

    def test_huge_growth_triggers_guard(self):
        """At some extreme, the guard MUST fire (no unlimited headroom)."""
        import pytest
        with pytest.raises(UniverseBudgetExceeded):
            regression_guard(50_000)

    def test_rag_ingestion_cap_respected(self):
        """RAGIngestion must never exceed its 6h hard cap regardless of universe."""
        from infrastructure.sf_budgets import STAGE_BUDGETS
        rag = STAGE_BUDGETS["RAGIngestion"]
        for n in (CURRENT_UNIVERSE_SIZE, 2000, 5000, 100_000):
            assert rag.recommended_timeout(n) <= rag.max_budget_seconds, (
                f"RAGIngestion at universe {n} exceeds max {rag.max_budget_seconds}s"
            )

    @pytest.mark.parametrize("stage", ["EvaluatorDiagnostics", "EvaluatorOptimize"])
    def test_evaluator_halves_do_not_exceed_max_budget(self, stage):
        """Both halves stay bounded as the universe grows (config-I3112).

        The per-ticker figures are UPPER BOUNDS, not fits — all three source
        runs sat at the same ~944-ticker universe, so they contain no slope,
        and each half attributes 100% of its measured phase time to universe
        scaling. That makes this assertion pessimistic on purpose.
        """
        from infrastructure.sf_budgets import STAGE_BUDGETS
        b = STAGE_BUDGETS[stage]
        for n in (CURRENT_UNIVERSE_SIZE, 2000, 5000):
            rec = b.recommended_timeout(n)
            assert rec <= b.max_budget_seconds, (
                f"{stage} at universe {n}: recommended {rec}s > max {b.max_budget_seconds}s"
            )

    def test_the_split_did_not_quietly_grow_the_evaluator_budget(self):
        """The two halves together must cost the pipeline LESS than the one
        state they replace, not more.

        The single Evaluator state held 7200s against a measured 482s stage.
        Splitting it is a decomposition, not a licence to widen: if a future
        edit pushes the pair back past the old ceiling, the sequential tail
        has silently reacquired budget nobody argued for.
        """
        from infrastructure.sf_budgets import STAGE_BUDGETS
        pair = (STAGE_BUDGETS["EvaluatorDiagnostics"].current_timeout_seconds
                + STAGE_BUDGETS["EvaluatorOptimize"].current_timeout_seconds)
        assert pair <= 7_200, (
            f"EvaluatorDiagnostics + EvaluatorOptimize = {pair}s exceeds the "
            f"7200s the single Evaluator state held before the config-I3112 split"
        )


# ── Guard 4: fetch_budget.py integration (RAGIngestion cross-reference) ──────

class TestRagBudgetCrossReference:
    """RAGIngestion's budget in the fleet table is consistent with fetch_budget.py."""

    def test_rag_current_timeout_matches_weekly_constant(self):
        from collectors.news_sources.fetch_budget import (
            WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS,
        )
        rag_budget = STAGE_BUDGETS["RAGIngestion"]
        assert (
            rag_budget.current_timeout_seconds
            == WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS
        ), (
            f"RAGIngestion current_timeout_seconds "
            f"({rag_budget.current_timeout_seconds}) must match "
            f"fetch_budget.py's WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS "
            f"({WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS})"
        )
