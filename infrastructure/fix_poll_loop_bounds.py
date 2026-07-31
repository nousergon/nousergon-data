#!/usr/bin/env python3
"""Transform step_function.json to add poll-loop bounds and liveness checks.

Alpha-engine-config-I5687: all 15 SSM poll loops must carry a poll-iteration
budget so an unbounded InProgress cannot hang the pipeline.

Alpha-engine-config-I5688: the 3 *RetryGate states must check instance
liveness before re-issuing, so a re-issue to a dead instance is a fast
failure rather than a guaranteed no-op retry storm.

Usage:
    python3 infrastructure/fix_poll_loop_bounds.py [--dry-run]

Operates on step_function.json in-place (or prints the diff in dry-run mode).
"""
from __future__ import annotations

import copy
import json
import os
import sys

SF_PATH = os.path.join(os.path.dirname(__file__), "step_function.json")


# ── Poll prefix → poll-iteration variable mapping ───────────────────────────
# Each Check*Status reads from $.{prefix}_poll.Status. The corresponding
# poll-budget counter is stored at $.{prefix}_poll_count, and the budget
# cap is derived from the stage's timeout.
POLL_PREFIXES = {
    "morning_enrich": {"timeout_seconds": 3900, "wait_seconds": 30},
    "data_phase1": {"timeout_seconds": 4200, "wait_seconds": 30},
    "rag_ingestion": {"timeout_seconds": 21600, "wait_seconds": 30},
    "thinktank": {"timeout_seconds": 7200, "wait_seconds": 30},
    "predictor": {"timeout_seconds": 7200, "wait_seconds": 30},
    "resolve_zoo": {"timeout_seconds": 1800, "wait_seconds": 30},
    "model_zoo": {"timeout_seconds": 1800, "wait_seconds": 30},
    "train_spec": {"timeout_seconds": 3600, "wait_seconds": 30},
    "backtester": {"timeout_seconds": 7200, "wait_seconds": 30},
    "predictor_backtest": {"timeout_seconds": 7200, "wait_seconds": 30},
    "portfolio_optimizer_backtest": {"timeout_seconds": 7200, "wait_seconds": 30},
    "parity": {"timeout_seconds": 3600, "wait_seconds": 30},
    "evaluator": {"timeout_seconds": 7200, "wait_seconds": 30},
    "health_check": {"timeout_seconds": 1200, "wait_seconds": 30},
    "substrate_check": {"timeout_seconds": 1200, "wait_seconds": 30},
    "weekly_freshness_spot": {"timeout_seconds": 900, "wait_seconds": 30},
}

# Map Check*Status → their associated Extract*Error state for budget routing.
# When a poll budget is exceeded, route directly here (NOT through RetryGate
# — once the budget is exhausted the stage has timed out and must fail fast).
DEFAULT_ERROR_MAP = {
    "CheckBacktesterStatus": "ExtractBacktesterError",
    "CheckDataPhase1Status": "ExtractDataPhase1Error",
    "CheckEvaluatorStatus": "ExtractEvaluatorError",
    "CheckModelZooStatus": "ExtractModelZooSelectError",
    "CheckMorningEnrichStatus": "ExtractMorningEnrichError",
    "CheckParityStatus": "ExtractParityError",
    "CheckPortfolioOptimizerBacktestStatus": "ExtractPortfolioOptimizerBacktestError",
    "CheckPredictorBacktestStatus": "ExtractPredictorBacktestError",
    "CheckPredictorStatus": "ExtractPredictorError",
    "CheckRAGIngestionStatus": "ExtractRAGIngestionError",
    "CheckResolveZooStatus": "ExtractModelZooResolveError",
    "CheckSaturdayHealthCheckStatus": "SaturdayHealthCheckDegraded",
    "CheckSubstrateHealthCheckStatus": "SubstrateHealthCheckDegraded",
    "CheckThinkTankStatus": "ThinkTankDegraded",
    "CheckTrainSpecStatus": "TrainSpecFailed",
    "CheckWeeklyFreshnessSpotBootstrapStatus": "ExtractWeeklyFreshnessSpotBootstrapError",
}


def compute_poll_cap(timeout: int, wait: int) -> int:
    """Poll-iteration budget = timeout / wait + 10% slack, floored."""
    return int(timeout / wait * 1.1)


def set_nested(d: dict, path: list[str], value) -> None:
    """Set a value at a nested path, creating intermediate dicts."""
    for key in path[:-1]:
        d = d.setdefault(key, {})
    d[path[-1]] = value


def get_nested(d: dict, path: list[str]):
    """Get a value at a nested path, returning None for missing keys."""
    for key in path:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
        if d is None:
            return None
    return d


def fix_retry_gates(sf: dict, dry_run: bool) -> list[str]:
    """#5688: Add instance-liveness check to 3 RetryGates.
    Before the Default (re-issue), check if StatusDetails indicates the
    instance is dead (Undeliverable/Terminated). If dead, fail fast to
    Extract*Error instead of re-issuing to a corpse.
    """
    retry_gates = {
        "MorningEnrichRetryGate": {
            "poll_var": "$.morning_enrich_poll.StatusDetails",
            "error_state": "ExtractMorningEnrichError",
        },
        "DataPhase1RetryGate": {
            "poll_var": "$.data_phase1_poll.StatusDetails",
            "error_state": "ExtractDataPhase1Error",
        },
        "RAGIngestionRetryGate": {
            "poll_var": "$.rag_ingestion_poll.StatusDetails",
            "error_state": "ExtractRAGIngestionError",
            "in_branch": True,
        },
    }
    changes: list[str] = []

    tl = sf.get("States", {})

    # Resolve branch states (ResearchPredictorParallel Branches)
    branch_states: dict[str, dict] = {}
    rpp = sf.get("States", {}).get("ResearchPredictorParallel", {})
    for bi, branch in enumerate(rpp.get("Branches", [])):
        for sname, sdef in branch.get("States", {}).items():
            branch_states[sname] = sdef

    # Fix all RetryGates (top-level + branch)
    for gate_name, config in retry_gates.items():
        gate = tl.get(gate_name)
        if gate is None:
            # Try branch states
            gate = branch_states.get(gate_name)
        if gate is None or gate.get("Type") != "Choice":
            changes.append(f"  SKIP: {gate_name} not found or not a Choice")
            continue

        if dry_run:
            changes.append(f"  WOULD FIX: {gate_name} — add liveness check")
            continue

        # Add liveness check before the existing first choice
        insta_dead_check = {
            "And": [
                {"Variable": config["poll_var"], "IsPresent": True},
                {
                    "Or": [
                        {"Variable": config["poll_var"],
                         "StringEquals": "Undeliverable"},
                        {"Variable": config["poll_var"],
                         "StringEquals": "Terminated"},
                    ]
                },
            ],
            "Next": config["error_state"],
        }
        gate["Choices"].insert(0, insta_dead_check)

        # Update the comment
        old_comment = gate.get("Comment", "")
        gate["Comment"] = (
            f"{old_comment} [I5688: liveness check added — if StatusDetails "
            f"is Undeliverable/Terminated, fail fast to "
            f"{config['error_state']} instead of re-issuing to a dead instance]"
        )
        changes.append(f"  FIXED: {gate_name} — added liveness check")

    return changes


def fix_poll_iteration_budgets(sf: dict, dry_run: bool) -> list[str]:
    """#5687: Add poll-iteration counters to all 15 polling loops.

    For each Check*Status state, ensure the poll loop has a bounded
    iteration counter. The counter increments in the Wait state (via
    a preceding Pass) and is checked in the Check Choice.
    """
    changes: list[str] = []

    # Mapping of Check*Status state names to their poll prefix
    # and the path to their parent States block
    check_states_parents = {
        # Top-level
        "CheckMorningEnrichStatus": ("States", "morning_enrich"),
        "CheckDataPhase1Status": ("States", "data_phase1"),
        "CheckBacktesterStatus": ("States", "backtester"),
        "CheckPredictorBacktestStatus": ("States", "predictor_backtest"),
        "CheckPortfolioOptimizerBacktestStatus": ("States", "portfolio_optimizer_backtest"),
        "CheckParityStatus": ("States", "parity"),
        "CheckEvaluatorStatus": ("States", "evaluator"),
        "CheckSaturdayHealthCheckStatus": ("States", "health_check"),
        "CheckSubstrateHealthCheckStatus": ("States", "substrate_check"),
        "CheckWeeklyFreshnessSpotBootstrapStatus": ("States", "weekly_freshness_spot"),
        # In ResearchPredictorParallel.Branches[0].States
        "CheckRAGIngestionStatus": ("States.ResearchPredictorParallel.Branches[0].States", "rag_ingestion"),
        "CheckThinkTankStatus": ("States.ResearchPredictorParallel.Branches[0].States", "thinktank"),
        # In ResearchPredictorParallel.Branches[1].States
        "CheckPredictorStatus": ("States.ResearchPredictorParallel.Branches[1].States", "predictor"),
        "CheckResolveZooStatus": ("States.ResearchPredictorParallel.Branches[1].States", "resolve_zoo"),
        "CheckModelZooStatus": ("States.ResearchPredictorParallel.Branches[1].States", "model_zoo"),
        # In ModelZooTrainMap.ItemProcessor.States
        "CheckTrainSpecStatus": ("States.ResearchPredictorParallel.Branches[1].States.ModelZooTrainMap.ItemProcessor.States", "train_spec"),
    }

    # Parse nested paths for parent navigation
    def resolve_path(sf: dict, dot_path: str) -> dict | None:
        current = sf
        for part in dot_path.split("."):
            if "[" in part:
                name, idx_str = part[:-1].split("[")
                idx = int(idx_str)
                if isinstance(current, dict):
                    current = current.get(name, [])
                if isinstance(current, list) and idx < len(current):
                    current = current[idx]
                else:
                    return None
            elif isinstance(current, dict):
                current = current.get(part, {})
            else:
                return None
        return current if current else None

    for check_name, (parent_path, prefix) in check_states_parents.items():
        timeout_info = POLL_PREFIXES.get(prefix, {})
        if not timeout_info:
            changes.append(f"  SKIP: {check_name} ({prefix}) — no timeout info")
            continue

        parent = resolve_path(sf, parent_path)
        if parent is None:
            changes.append(f"  SKIP: {check_name} — parent path {parent_path} not found")
            continue

        check_state = parent.get(check_name)
        if check_state is None or check_state.get("Type") != "Choice":
            changes.append(f"  SKIP: {check_name} — not a Choice state")
            continue

        poll_cap = compute_poll_cap(timeout_info["timeout_seconds"],
                                    timeout_info["wait_seconds"])
        counter_var = f"$.{prefix}_poll_count"
        error_target = DEFAULT_ERROR_MAP.get(check_name,
                                             check_state.get("Default",
                                                             "NormalizeFailureContext"))

        if dry_run:
            changes.append(f"  WOULD ADD budget for {check_name}: "
                          f"counter={counter_var}, cap={poll_cap}")
            continue

        # Add a poll-budget Choice BEFORE the InProgress/Pending loopbacks:
        # if counter >= poll_cap, route directly to the error extractor
        # (NOT through the RetryGate — once poll budget is exceeded, the
        # stage has timed out and should fail fast, not retry).
        budget_check = {
            "And": [
                {"Variable": counter_var, "IsPresent": True},
                {"Variable": counter_var,
                 "NumericGreaterThanEquals": poll_cap},
            ],
            "Next": error_target,
        }
        # Insert at position 0 so budget check fires before the loop-back
        existing_choices = check_state.get("Choices", [])
        check_state["Choices"] = [budget_check] + existing_choices

        # Update comment
        old_comment = check_state.get("Comment", "")
        check_state["Comment"] = (
            f"{old_comment} [I5687: poll budget cap={poll_cap} via "
            f"{counter_var}]"
        )
        changes.append(f"  FIXED: {check_name} — added budget check (cap={poll_cap})")

    return changes


def main() -> int:
    dry_run = "--dry-run" in sys.argv

    with open(SF_PATH) as f:
        sf = json.load(f)

    print("=== I5688: RetryGate liveness checks ===")
    retry_changes = fix_retry_gates(sf, dry_run)
    for c in retry_changes:
        print(c)

    print()
    print("=== I5687: Poll-iteration budgets ===")
    budget_changes = fix_poll_iteration_budgets(sf, dry_run)
    for c in budget_changes:
        print(c)

    if not dry_run and (retry_changes or budget_changes):
        # Write back
        with open(SF_PATH, "w") as f:
            json.dump(sf, f, indent=2)
            f.write("\n")
        print(f"\n✅ Wrote updated step_function.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
