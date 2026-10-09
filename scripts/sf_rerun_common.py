#!/usr/bin/env python3
"""Shared execution-history parsing + role-gating verification for the Step
Functions mechanical-rerun helpers (alpha-engine-config#6694, second adoption
of the config#2277 idiom `scripts/weekly_sf_rerun.py` established).

Lifted here, UNCHANGED in behavior, once a second rerun helper
(``scripts/weekday_sf_rerun.py``, covering the preopen/EOD weekday SFs)
needed the exact same execution-history parsing + role-gating verification
`weekly_sf_rerun.py` already had (shared-code-policy second-adoption
trigger). Every function below is a pure function over a raw ``describe_
execution_history`` events list, an ``execution_input`` dict, or a raw SF
``definition`` dict — nothing here is weekly-SF-specific, and nothing here
mutates AWS state (the AWS plumbing functions are read-only Describe/List
calls; ``start_execution`` / mutation stays in each caller).

``weekly_sf_rerun.py`` imports these names rather than redefining them;
``tests/test_weekly_sf_rerun.py`` was NOT changed to make this true — it
still asserts against ``weekly_sf_rerun`` module attributes, which resolve
to these same function objects via the import.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone


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
    """The merged object an ``InitializeInput`` Pass state emitted — the
    authoritative source of the run_date every subsequent stage actually
    keyed its artifacts on, for the SFs that have one (weekly, daily). The
    EOD SF has no InitializeInput state (its StartAt is CheckMutexRole), so
    this always returns None there and callers fall through to the next
    precedence tier in ``derive_run_date``."""
    for e in events:
        d = e.get("stateExitedEventDetails")
        if d is not None and d.get("name") == "InitializeInput":
            try:
                return json.loads(d.get("output") or "null")
            except json.JSONDecodeError:
                return None
    return None


def apply_normalized_run_date_output(events: list[dict]) -> dict | None:
    """The merged object ``ApplyNormalizedRunDate`` emitted after
    ``NormalizeRunDates`` — the cycle's TRADING day on ``$.run_date``
    (alpha-engine-config-I8809). This is the key every artifact prefix and
    skip-coherence predicate actually use; ``InitializeInput`` only stamps the
    calendar date before normalization."""
    for e in events:
        d = e.get("stateExitedEventDetails")
        if d is not None and d.get("name") == "ApplyNormalizedRunDate":
            try:
                return json.loads(d.get("output") or "null")
            except json.JSONDecodeError:
                return None
    return None


def derive_run_date(events: list[dict], start_time: datetime | None) -> tuple[str, str]:
    """Return (run_date, provenance). Precedence: explicit input run_date >
    ApplyNormalizedRunDate's merged output (trading day) >
    InitializeInput's merged output (calendar day, pre-normalizer) >
    date(Execution start time)."""
    orig = execution_input(events)
    if isinstance(orig.get("run_date"), str) and orig["run_date"]:
        return orig["run_date"], "explicit run_date in the failed execution's input"
    normalized = apply_normalized_run_date_output(events)
    if isinstance(normalized, dict) and isinstance(normalized.get("run_date"), str) and normalized["run_date"]:
        return normalized["run_date"], "ApplyNormalizedRunDate merged output of the failed execution (trading day)"
    init = initialize_input_output(events)
    if isinstance(init, dict) and isinstance(init.get("run_date"), str) and init["run_date"]:
        return init["run_date"], "InitializeInput merged output of the failed execution"
    if start_time is not None:
        rd = start_time.astimezone(timezone.utc).date().isoformat()
        return rd, (
            "FALLBACK: UTC date of the failed execution's start time"
            " (InitializeInput never exited, or this SF has no"
            " InitializeInput state — pre-workload failure)"
        )
    raise SystemExit(
        "FATAL: cannot derive run_date — no explicit input run_date, no "
        "ApplyNormalizedRunDate output, no InitializeInput output in history, "
        "and no execution start time was supplied."
    )


def derive_calendar_date(events: list[dict], start_time: datetime | None) -> tuple[str, str]:
    """Return (calendar_date, provenance). Precedence: explicit input
    calendar_date > ApplyNormalizedRunDate's preserved calendar_date >
    InitializeInput's calendar_date (or its pre-normalizer run_date stamp) >
    date(Execution start time).

    The weekly SF's ``NormalizeRunDates`` derives ``$.run_date`` (the trading
    day every date-keyed stage reads, ``ValidatePredictorSkipWeightsFresh``'s
    arena verdict key included) FROM ``$.calendar_date`` — a recovery rerun
    must emit this field or ``InitializeInput`` re-stamps it from the NEW
    execution's wall clock and the rerun keys on a different cycle
    (alpha-engine-config-I8809, I12050).
    """
    orig = execution_input(events)
    if isinstance(orig.get("calendar_date"), str) and orig["calendar_date"]:
        return orig["calendar_date"], "explicit calendar_date in the failed execution's input"
    normalized = apply_normalized_run_date_output(events)
    if isinstance(normalized, dict) and isinstance(normalized.get("calendar_date"), str) and normalized["calendar_date"]:
        return normalized["calendar_date"], "ApplyNormalizedRunDate merged output (immutable execution day)"
    init = initialize_input_output(events)
    if isinstance(init, dict):
        if isinstance(init.get("calendar_date"), str) and init["calendar_date"]:
            return init["calendar_date"], "InitializeInput merged output calendar_date"
        if isinstance(init.get("run_date"), str) and init["run_date"]:
            return init["run_date"], "InitializeInput merged output run_date (pre-normalizer calendar day)"
    if isinstance(orig.get("run_date"), str) and orig["run_date"]:
        return orig["run_date"], (
            "explicit run_date in the failed execution's input"
            " (calendar day when InitializeInput never exited)"
        )
    if start_time is not None:
        cd = start_time.astimezone(timezone.utc).date().isoformat()
        return cd, (
            "FALLBACK: UTC date of the failed execution's start time"
            " (InitializeInput never exited, or this SF has no"
            " InitializeInput state — pre-workload failure)"
        )
    raise SystemExit(
        "FATAL: cannot derive calendar_date — no explicit input calendar_date, "
        "no ApplyNormalizedRunDate output, no InitializeInput output in history, "
        "and no execution start time was supplied."
    )


# ---------------------------------------------------------------------------
# Role-gating verification against a live SF definition
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


def verify_skip_flags_live(
    definition: dict,
    role: str,
    *,
    sf_label: str = "the weekly SF",
    script_path: str = "scripts/weekly_sf_rerun.py",
) -> None:
    """Fail LOUDLY if any CheckSkip* gate structurally conjuncts
    pipeline_role in a way that would render the emitted skip flags inert
    under ``role``. A helper that silently emits inert skip flags re-burns
    every completed spot stage.

    Generic over BOTH gating shapes in the fleet: the weekly SF's skip gates
    test only the flag itself (role-unconditional — passing here for any
    role), and the EOD SF's skip gates structurally conjunct
    ``pipeline_role == 'operator-replay'`` (config#1614) — passing here only
    when the caller's ``role`` IS ``'operator-replay'``. ``sf_label`` /
    ``script_path`` customize the error text per caller; defaults match
    weekly_sf_rerun.py's original wording so its own call site (and
    tests/test_weekly_sf_rerun.py, which only asserts the 'role gating'
    substring) are unaffected by this parameterization.
    """
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
            f"FATAL (role gating): {sf_label} now conjuncts pipeline_role "
            f"inside skip gates {offenders}, and role {role!r} is not in the "
            "live set — the skip flags this helper emits would be silently "
            "IGNORED and every completed spot stage would re-burn. Update "
            f"{script_path}'s role handling to match "
            "the SF's new role-gate semantics before rerunning."
        )


# ---------------------------------------------------------------------------
# AWS plumbing (thin, injectable, read-only)
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


def effective_run_date_of(sf, execution: dict) -> str:
    """Best-effort run_date of a (typically RUNNING) execution, for the
    same-run_date double-writer guard every rerun helper's ``--start`` path
    runs before dispatch. Falls back to the UTC date of the execution's own
    start time if its input carries no explicit run_date or can't be read."""
    try:
        desc = sf.describe_execution(executionArn=execution["executionArn"])
        inp = json.loads(desc.get("input") or "{}")
        if isinstance(inp.get("run_date"), str) and inp["run_date"]:
            return inp["run_date"]
    except Exception as exc:  # noqa: BLE001 — guard is best-effort per-exec; date fallback below is conservative
        print(f"WARN: could not read input of {execution['executionArn']}: {exc}", file=sys.stderr)
    return execution["startDate"].astimezone(timezone.utc).date().isoformat()
