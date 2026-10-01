#!/usr/bin/env python3
"""weekly_sf_silence_deadman.py — silence deadman for the weekly pipeline's
two trigger roles (alpha-engine-config#6689 deliverable 3, resolving #5599).

**Background.** The weekly pipeline (``ne-weekly-freshness-pipeline``) is
launched two ways: the Saturday EventBridge cron (``pipeline_role=weekly``,
self-gated by ``step_function.json``'s ``WeeklyRunDayGateChoice`` to the one
correct calendar day per week) and, when ``infrastructure/weekly_cadence.json``
declares ``exercise_cadence=daily``, a chained launch from every trading day's
postclose (``pipeline_role=exercise``, ``LaunchWeeklyExerciseRun`` — in
``step_function_eod_reconcile.json`` since the 2026-09-30 post-close split,
``step_function_eod.json`` before it). Measured over 2026-07-26 -> 08-09: 4 of 10
trading days got no exercise run with ZERO signal — postclose FAILED before
the launch state on 07-27/07-28, and postclose never fired AT ALL on 08-05/
08-06. Nothing paged on the silent days because the only prior deadman
candidate, the CloudWatch ``AWS/States`` ``ExecutionsStarted`` metric, carries
no ``pipeline_role`` dimension — it cannot tell a gated-off day from a dead
one. **This script supersedes that metric-dimension approach** (alpha-engine-
config#5599's proposed resolution): it reads the declared cadence and the
NYSE trading calendar directly, derives the run-slots that SHOULD exist, and
checks the pipeline's own execution history for each one — no new metric
dimension needed.

**Classification, per expected slot:**

  * ``GATED_OFF`` (OK, exit 0) — the declared cadence says this slot should
    NOT fire (e.g. an exercise slot on a trading day when
    ``exercise_cadence`` is ``weekly-only`` or ``off``).
  * ``OK`` (exit 0) — expected, and a matching execution exists.
  * ``CRITICAL`` (pages, exit 1) — expected, and none does. This is the
    silence this script exists to catch: a day the pipeline should have run
    and produced nothing, indistinguishable from a holiday until now.

**Gotchas this script is built around (measured, not theoretical):**

  * A SUCCEEDED ``pipeline_role=weekly`` execution whose OUTPUT carries
    ``weekly_run_day_gate.Payload.is_weekly_run_day is False`` is
    ``WeeklyRunDayGateChoice``'s designed no-op skip on a non-run day (2 of
    every 3 THU-SAT cron firings), NOT a real weekly run. Counting it as
    "ran" would hide a genuinely silent week. Read directly from the
    execution's output, never inferred from duration — corrected
    2026-08-29 (alpha-engine-config-I8057) after a
    ``duration < 90s`` heuristic misclassified real data in both
    directions.
  * Neither launch path passes an explicit Step Functions ``Name`` — every
    execution gets an AWS-generated UUID name. ``pipeline_role`` is read from
    each execution's OWN INPUT via ``describe_execution``, never inferred
    from the name.
  * The weekly cron fires THU-SAT and self-gates true on the day AFTER the
    week's last trading session (Saturday on a normal week; Friday/Thursday
    on a holiday-shortened one — real precedent Fri 2026-07-03). This script
    therefore expects the REAL weekly run on ``last_session + 1 calendar
    day``, not on the last session itself — a deliberate refinement of the
    filing issue's "weekly for the week's last session" wording, recorded as
    a documented deviation in this change's PR body.

  * A slot on TODAY is usually not-yet-due rather than missing — every
    launch path fires late in the UTC day. See ``last_due_day``; the CLI
    stops at yesterday by default and ``--through today`` opts back in.

**Scheduling (alpha-engine-config#6738).** This module is BOTH an operator
CLI and the slot-derivation library for the scheduled check. The
``alpha-engine-pipeline-watchdog`` Lambda imports ``compute_expected_slots``
/ ``evaluate`` / ``fetch_execution_records`` / ``load_cadence_from_ssm`` /
``last_due_day`` from here (its ``deploy.sh`` packages this file flat into
the zip) and runs them on the already-un-paused
``alpha-engine-pipeline-watchdog-daily`` 14:00-UTC trigger, paging the
independent ``alpha-engine-watchdog-alerts`` topic. One implementation, two
entry points — a manual rerun and the scheduled check cannot disagree about
which slots the declaration expects.

Run manually, this script still best-effort publishes a CRITICAL finding to
the fleet's ``alpha-engine-alerts`` SNS topic and always exits non-zero
regardless of whether the publish succeeds — the exit code is the durable
signal, and the scheduled path uses the watchdog topic instead.

Usage:
  ./scripts/weekly_sf_silence_deadman.py                     # manifest cadence, 5-day window, through yesterday
  ./scripts/weekly_sf_silence_deadman.py --live               # cadence read from live SSM instead
  ./scripts/weekly_sf_silence_deadman.py --window-days 10
  ./scripts/weekly_sf_silence_deadman.py --through today      # include today (only after its postclose)
  ./scripts/weekly_sf_silence_deadman.py --json --no-notify   # for scripted/CI consumption
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "infrastructure" / "weekly_cadence.json"
SSM_PARAM_NAME = "/alpha-engine/weekly-sf/exercise-cadence"
STATE_MACHINE_NAME = "ne-weekly-freshness-pipeline"
REGION = "us-east-1"
ACCOUNT_ID = "711398986525"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"

ALLOWED_CADENCE = {"daily", "weekly-only", "off"}
DEFAULT_WINDOW_DAYS = 5


def _is_weekly_gate_out(output_json: dict) -> bool:
    """True iff a SUCCEEDED weekly-role execution's own OUTPUT proves it did
    no work (alpha-engine-config-I8057, mirrors
    ``gate_sf_run_sweep.py::_is_weekly_gate_out`` from
    alpha-engine-config-PR8055 — the reference implementation for this
    class, also mirrored in ``weekly_sf_recovery_metric.py``). Positive test
    only: excludes on ``weekly_run_day_gate.Payload.is_weekly_run_day is
    False``, never on the key's absence — a real run does not carry it at
    all.
    """
    gate = output_json.get("weekly_run_day_gate")
    if not isinstance(gate, dict):
        return False
    inner = gate.get("Payload")
    if not isinstance(inner, dict):
        return False
    return inner.get("is_weekly_run_day") is False


# ---------------------------------------------------------------------------
# Cadence source
# ---------------------------------------------------------------------------

def _validate_cadence(value: object, source: str) -> str:
    if value not in ALLOWED_CADENCE:
        raise ValueError(f"{source} declared exercise_cadence={value!r}, not one of {sorted(ALLOWED_CADENCE)}")
    return value  # type: ignore[return-value]


@dataclass(frozen=True)
class CadenceEntry:
    """One dated declaration in ``weekly_cadence.json``'s ``exercise_cadence``
    history (alpha-engine-config-I7175). ``effective_from`` is the first date
    the value is IN FORCE, inclusive."""
    value: str
    effective_from: date
    why: str = ""


def _parse_cadence_history(raw: object, source: str) -> list[CadenceEntry]:
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            f"{source} exercise_cadence must be a non-empty list of "
            f"{{value, effective_from}} entries, got {raw!r}"
        )
    entries: list[CadenceEntry] = []
    for item in raw:
        if not isinstance(item, dict) or "value" not in item or "effective_from" not in item:
            raise ValueError(f"{source} exercise_cadence entry malformed (needs value + effective_from): {item!r}")
        value = _validate_cadence(item["value"], source)
        try:
            effective_from = date.fromisoformat(item["effective_from"])
        except ValueError as exc:
            raise ValueError(
                f"{source} exercise_cadence entry has an invalid effective_from: {item['effective_from']!r}"
            ) from exc
        entries.append(CadenceEntry(value=value, effective_from=effective_from, why=item.get("_why", "")))
    entries.sort(key=lambda e: e.effective_from)
    return entries


def cadence_for_date(history: list[CadenceEntry], d: date) -> str | None:
    """The cadence value in force on ``d``, or ``None`` if ``d`` precedes the
    first declared entry — "the declaration did not exist yet" is a distinct
    state from "the run was expected and absent" (alpha-engine-config-I7175,
    sf-pipeline-policy.md §2.6 rule 1's GATED_OFF/CRITICAL distinction)."""
    in_force: str | None = None
    for entry in history:  # history is sorted ascending by _parse_cadence_history
        if entry.effective_from <= d:
            in_force = entry.value
        else:
            break
    return in_force


def load_cadence_history(path: Path = MANIFEST_PATH) -> list[CadenceEntry]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    return _parse_cadence_history(manifest.get("exercise_cadence"), str(path))


def load_cadence_from_manifest(path: Path = MANIFEST_PATH) -> str:
    """The value in force TODAY (real wall-clock today), i.e. the manifest
    history's last entry as of now — used only for the CLI header/JSON
    display and as the non-``--live`` current-cadence source. Per-day
    classification never calls this; it calls ``cadence_for_date`` against
    the full history instead (see ``compute_expected_slots``)."""
    history = load_cadence_history(path)
    today = _today()
    value = cadence_for_date(history, today)
    if value is None:
        raise ValueError(f"{path}: no exercise_cadence entry is in force on {today.isoformat()}")
    return value


def load_cadence_from_ssm(ssm_client) -> str:
    resp = ssm_client.get_parameter(Name=SSM_PARAM_NAME)
    return _validate_cadence(resp["Parameter"]["Value"], SSM_PARAM_NAME)


# ---------------------------------------------------------------------------
# Expected run-slot derivation (pure — no AWS)
# ---------------------------------------------------------------------------

def last_due_day(today: date) -> date:
    """The most recent day whose expected run-slots are unambiguously DUE.

    Every launch path for this state machine fires LATE in the UTC day: the
    exercise leg is chained off that trading day's postclose (~20:00-20:30
    UTC) and the Saturday cron fires 09:00 UTC on the day after the week's
    last session. So "today" is, for most of any given UTC day, a slot that
    has not happened YET rather than a slot that is missing — and a detector
    that cannot tell those apart reports CRITICAL on a healthy pipeline.

    Measured 2026-08-09 (alpha-engine-config#6738): a live run of this script
    at 21:00 PT — 2026-08-10 in UTC — classified the 2026-08-10 exercise slot
    CRITICAL alongside the two genuinely silent days, because Monday's
    postclose was ~23h in the future. Three CRITICALs, one of them false.

    Anchoring evaluation one day back removes the whole ambiguity class
    rather than racing it with a cutoff hour: yesterday's exercise slot is
    18h+ old by the time any consumer reads it, and Saturday's 09:00-UTC
    weekly slot is 29h+ old when evaluated on Sunday. Both are overdue, not
    early. Shared by BOTH entry points — the CLI's default anchor and the
    pipeline-watchdog Lambda's — so neither can drift back into the trap.
    """
    return today - timedelta(days=1)


@dataclass(frozen=True)
class ExpectedSlot:
    day: date
    role: str  # "exercise" | "weekly"
    gated_off: bool = False
    note: str = ""


def _is_last_session_of_week(
    d: date,
    is_trading_day: Callable[[date], bool],
    next_trading_day: Callable[[date], date],
) -> bool:
    """True iff d is a trading day and the NEXT trading day falls in a
    different ISO week — i.e. d is the last trading session of its week."""
    if not is_trading_day(d):
        return False
    nxt = next_trading_day(d)
    return nxt.isocalendar()[:2] != d.isocalendar()[:2]


def compute_expected_slots(
    today: date,
    cadence: str,
    window_days: int,
    is_trading_day: Callable[[date], bool],
    next_trading_day: Callable[[date], date],
    cadence_history: list[CadenceEntry] | None = None,
) -> list[ExpectedSlot]:
    """Derive every run-slot expected in [today - window_days, today].

    Exercise slots: one per trading day in the window.

    ``cadence_history`` — when given, EVERY exercise day is resolved against
    it via ``cadence_for_date`` instead of the flat ``cadence`` scalar, so a
    day before the declaration's first entry is ``GATED_OFF`` rather than
    judged against a cadence that did not exist yet on that day
    (alpha-engine-config-I7175). ``None`` (the default) preserves the
    original flat-``cadence`` behavior unchanged — the pipeline-watchdog
    Lambda calls this positionally without it and must keep working; it only
    ever evaluates a window anchored at "now", never retroactively, so the
    distinction does not apply to it. A day gated on either path is skipped
    when ``cadence != "daily"`` — LaunchWeeklyExerciseRun's
    CheckExerciseCadence only fires the launch on 'daily'
    (step_function_eod_reconcile.json since the 2026-09-30 post-close split).

    Weekly slots: one per week whose last-trading-session-plus-one-day run
    date falls within the window. NEVER gated by cadence (declared or
    historical) — the Saturday cron is a separate trigger this knob
    deliberately does not touch.
    """
    slots: list[ExpectedSlot] = []
    d = today - timedelta(days=window_days)
    while d <= today:
        if is_trading_day(d):
            if cadence_history is not None:
                day_cadence = cadence_for_date(cadence_history, d)
            else:
                day_cadence = cadence
            if day_cadence is None:
                first_entry = cadence_history[0]  # non-empty by _parse_cadence_history
                slots.append(ExpectedSlot(
                    day=d,
                    role="exercise",
                    gated_off=True,
                    note=(
                        f"no exercise_cadence declaration was in force on {d.isoformat()} — "
                        f"the earliest entry takes effect {first_entry.effective_from.isoformat()}. "
                        "GATED_OFF, not CRITICAL: the declaration did not exist yet, which is a "
                        "different state from 'expected and absent'."
                    ),
                ))
            else:
                gated = day_cadence != "daily"
                slots.append(ExpectedSlot(
                    day=d,
                    role="exercise",
                    gated_off=gated,
                    note=(
                        f"cadence={day_cadence} — LaunchWeeklyExerciseRun is skipped on trading days"
                        if gated else
                        f"cadence={day_cadence} — LaunchWeeklyExerciseRun fires after this trading day's postclose"
                    ),
                ))
        if _is_last_session_of_week(d, is_trading_day, next_trading_day):
            run_day = d + timedelta(days=1)
            if today - timedelta(days=window_days) <= run_day <= today:
                slots.append(ExpectedSlot(
                    day=run_day,
                    role="weekly",
                    gated_off=False,
                    note=(
                        f"day after the week's last trading session ({d.isoformat()}) — "
                        "Saturday-cron WeeklyRunDayGateChoice self-gates true on this day"
                    ),
                ))
        d += timedelta(days=1)
    return slots


# ---------------------------------------------------------------------------
# Execution matching (pure — takes already-fetched records)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExecutionRecord:
    name: str
    role: str | None
    status: str
    start: datetime  # tz-aware UTC
    stop: datetime | None
    # The execution's own ``run_date`` input — the CYCLE it belongs to, which
    # is NOT derivable from ``start``: a recovery rerun of Saturday's cycle
    # legitimately starts on Sunday (or later) and still carries
    # run_date=<Saturday>. This is the same key the run-slot mutex and the
    # completion marker's ``cycle_key`` use, so a consumer asking "did cycle
    # D succeed?" must match on this and never on a wall-clock date in any
    # zone (alpha-engine-config-I7440). ``None`` when the input carried no
    # usable run_date; consumers fall back to ``start`` and say so.
    run_date: date | None = None
    # True iff this SUCCEEDED weekly-role execution's own OUTPUT proves it
    # gated out (``weekly_run_day_gate.Payload.is_weekly_run_day is False``)
    # — never inferred from duration (alpha-engine-config-I8057). ``False``
    # by default and for every non-weekly role, since only the weekly
    # pipeline carries a run-day gate.
    is_gate_out: bool = False


# The weekly SF's pipeline_role vocabulary, as emitted by its launch paths:
#   weekly      — the alpha-engine-saturday EventBridge cron (the real cycle)
#   exercise    — the daily postclose-chained dry exercise run
#   watch-rerun — scripts/weekly_sf_rerun.py, the §2.5 mechanical recovery
#   recovery    — sf-watch's substrate-reclaim relaunch
#   shell-run   — the Friday-PM off-cycle preflight shell
# Closed on purpose: an unrecognized role normalizes to None so a new launch
# path cannot silently start counting as a cycle nobody declared.
KNOWN_ROLES = ("exercise", "weekly", "watch-rerun", "recovery", "shell-run")


def _parse_run_date(raw: object) -> date | None:
    """``run_date`` input → ``date``; ``None`` on anything unparseable.

    Never raises: an execution whose input carries a malformed run_date is a
    record with ``run_date=None``, which consumers treat as "cycle unknown"
    and fall back on — it must not take down the whole fetch for the other
    executions in the window.
    """
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def normalize_role(raw: object) -> str | None:
    """Pass a recognized ``pipeline_role`` through unchanged; unknown → None.

    Until 2026-08-16 this collapsed EVERYTHING except ``exercise``/``weekly``
    to ``None``, which made ``watch-rerun`` and ``recovery`` indistinguishable
    from an unrecognized launch path. That is why
    ``pipeline-watchdog``'s ``WEEKLY_CADENCE_ROLES`` filter — which names both
    — could never match a record produced by ``fetch_execution_records``, and
    why its weekly failed-day check counted 1 execution on cycle day
    2026-08-15 while its own liveness check, reading roles by a second
    unrelated path, counted 24 and returned CLEAR (alpha-engine-config-I7440).

    Widening is behaviour-preserving for this module's own slot matching:
    ``evaluate_slot`` compares ``e.role == slot.role`` and slot roles are only
    ``weekly``/``exercise``, so records that were ``None`` and matched nothing
    are now named and still match nothing.
    """
    return raw if raw in KNOWN_ROLES else None


@dataclass(frozen=True)
class SlotResult:
    slot: ExpectedSlot
    classification: str  # "OK" | "GATED_OFF" | "CRITICAL"
    matched_execution: str | None
    detail: str


def _is_gate_noop(e: ExecutionRecord) -> bool:
    """True iff this SUCCEEDED weekly-role execution's own OUTPUT proves it
    did no work — never a duration heuristic (corrected 2026-08-29,
    alpha-engine-config-I8057, same class fixed in
    ``gate_sf_run_sweep.py::_is_weekly_gate_out`` /
    alpha-engine-config-PR8055). The prior ``duration < GATE_NOOP_MAX_SECONDS``
    (90s) test was wrong in both directions: measured 2026-08-22,
    ``watch-rerun-2026-08-22-3`` ran 871.8s and entered 1 of 16 stages — long,
    and vacuous — while the fleet's shortest genuine weekly run is 12.6s,
    well inside a 90s window. ``is_gate_out`` is computed once in
    ``fetch_execution_records`` from the same ``describe_execution`` call
    already made for ``role``/``run_date`` — no extra AWS call, no clock.
    """
    if e.status != "SUCCEEDED":
        return False
    return e.is_gate_out


def evaluate_slot(slot: ExpectedSlot, executions: list[ExecutionRecord]) -> SlotResult:
    if slot.gated_off:
        return SlotResult(slot, "GATED_OFF", None, slot.note)

    candidates = [e for e in executions if e.role == slot.role and e.start.date() == slot.day]

    if slot.role == "weekly":
        real = [e for e in candidates if not _is_gate_noop(e)]
        if real:
            e = real[0]
            return SlotResult(slot, "OK", e.name, f"found {e.name} status={e.status}")
        if candidates:
            return SlotResult(
                slot, "CRITICAL", None,
                f"only run-day-gate no-op(s) found on {slot.day.isoformat()} "
                f"({len(candidates)}) — WeeklyRunDayGateChoice's skip is not a real run",
            )
        return SlotResult(
            slot, "CRITICAL", None,
            f"no weekly-role execution found on {slot.day.isoformat()} — expected the Saturday-cron run",
        )

    # exercise: not gated by the run-day gate (bypasses it by design), so any
    # matching execution is a real run.
    if candidates:
        e = candidates[0]
        return SlotResult(slot, "OK", e.name, f"found {e.name} status={e.status}")
    return SlotResult(
        slot, "CRITICAL", None,
        f"no exercise-role execution found on {slot.day.isoformat()} — postclose should have chained it (cadence=daily)",
    )


def evaluate(slots: list[ExpectedSlot], executions: list[ExecutionRecord]) -> list[SlotResult]:
    return [evaluate_slot(s, executions) for s in slots]


# ---------------------------------------------------------------------------
# AWS plumbing (thin, injectable — mirrors weekly_sf_rerun.py's shape)
# ---------------------------------------------------------------------------

def _list_all_executions(sf_client, sm_arn: str, cap: int = 500) -> list[dict]:
    out: list[dict] = []
    token = None
    while len(out) < cap:
        kwargs = {"stateMachineArn": sm_arn, "maxResults": 200}
        if token:
            kwargs["nextToken"] = token
        resp = sf_client.list_executions(**kwargs)
        out.extend(resp["executions"])
        token = resp.get("nextToken")
        if not token:
            break
    return out[:cap]


def fetch_execution_records(
    sf_client, window_days: int, today: date | None = None
) -> list[ExecutionRecord]:
    """Live AWS fetch: list recent executions, then describe each one for its
    pipeline_role (never present on the list-executions summary itself —
    every launch path here omits an explicit Name, so role must come from
    input, not the name).

    ``today`` — the anchor the lookback is measured back from. Defaults to
    ``_today()`` (the CLI's behaviour, unchanged). The pipeline-watchdog
    Lambda passes its own ``now_utc.date()`` instead (alpha-engine-config
    #6738) so the fetch window and the slot window are anchored to the SAME
    instant: a handler that pins ``now`` while this function read the wall
    clock would silently evaluate slots the fetch never covered.
    """
    sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"
    # +2 day pad: a weekly slot's run_day can land one calendar day after the
    # window's nominal start when the window boundary splits a week. Derived
    # from the anchor (not a fresh datetime.now() call) so a test that pins
    # the anchor gets a fully deterministic window.
    anchor = today if today is not None else _today()
    since = datetime.combine(anchor, datetime.min.time(), tzinfo=timezone.utc) - timedelta(days=window_days + 2)

    records: list[ExecutionRecord] = []
    for ex in _list_all_executions(sf_client, sm_arn):
        start = ex["startDate"]
        if start < since:
            continue
        role = None
        run_date = None
        is_gate_out = False
        try:
            desc = sf_client.describe_execution(executionArn=ex["executionArn"])
            inp = json.loads(desc.get("input") or "{}")
            role = normalize_role(inp.get("pipeline_role"))
            run_date = _parse_run_date(inp.get("run_date"))
            if role == "weekly" and ex["status"] == "SUCCEEDED":
                # Same describe_execution call already made above — no extra
                # AWS call. Never duration-based (alpha-engine-config-I8057).
                output_json = json.loads(desc.get("output") or "{}")
                is_gate_out = _is_weekly_gate_out(output_json)
        except Exception as exc:  # noqa: BLE001 — best-effort per-execution; unrecognized role is a safe fallback, not a hidden failure (surfaced below)
            print(f"WARN: could not read input of {ex['executionArn']}: {exc}", file=sys.stderr)
        records.append(ExecutionRecord(
            name=ex["name"],
            role=role,
            status=ex["status"],
            start=start,
            stop=ex.get("stopDate"),
            run_date=run_date,
            is_gate_out=is_gate_out,
        ))
    return records


# ---------------------------------------------------------------------------
# Reporting + CLI
# ---------------------------------------------------------------------------

def _today() -> date:
    """Isolated seam so CLI-level tests can pin 'today' deterministically
    without depending on the real wall clock."""
    return datetime.now(timezone.utc).date()


def format_report(results: list[SlotResult]) -> str:
    marks = {"OK": "✓", "GATED_OFF": "·", "CRITICAL": "✗"}
    lines = []
    for r in sorted(results, key=lambda r: (r.slot.day, r.slot.role)):
        lines.append(
            f"  {marks[r.classification]} {r.slot.day.isoformat()} {r.slot.role:8s} "
            f"[{r.classification}] {r.detail}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS, help=f"trailing days to check (default {DEFAULT_WINDOW_DAYS})")
    ap.add_argument("--live", action="store_true", help="read the declared cadence from live SSM instead of the local manifest")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--no-notify", action="store_true", help="never publish to SNS, even on CRITICAL (dry runs / CI)")
    ap.add_argument(
        "--through",
        choices=("last-due", "today"),
        default="last-due",
        help=(
            "newest day to evaluate. 'last-due' (default) stops at yesterday, "
            "because today's exercise slot is chained off today's ~20:00 UTC "
            "postclose and is therefore not-yet-due, not missing. 'today' "
            "includes it — only correct when invoked AFTER today's postclose "
            "has had time to chain."
        ),
    )
    args = ap.parse_args(argv)

    import boto3  # local import: keeps the pure functions above importable/testable with zero AWS deps
    from nousergon_lib.trading_calendar import is_trading_day, next_trading_day

    sf = boto3.client("stepfunctions", region_name=REGION)

    if args.live:
        ssm = boto3.client("ssm", region_name=REGION)
        cadence = load_cadence_from_ssm(ssm)
    else:
        cadence = load_cadence_from_manifest()

    # The dated history is always read from the repo manifest, --live or not:
    # SSM holds a single current scalar and structurally cannot answer "what
    # was declared on 2026-07-15" (alpha-engine-config-I7175). `cadence`
    # above stays the display/JSON value of TODAY's declaration (live SSM
    # when --live, else the manifest's current entry); `cadence_history`
    # drives per-day classification for every day in the window, including
    # today, so a manifest edit not yet deployed can never desync what the
    # deadman expects from what --live merely displays.
    cadence_history = load_cadence_history()

    today = _today()
    through = today if args.through == "today" else last_due_day(today)
    executions = fetch_execution_records(sf, args.window_days, today=today)
    slots = compute_expected_slots(
        through, cadence, args.window_days, is_trading_day, next_trading_day,
        cadence_history=cadence_history,
    )
    results = evaluate(slots, executions)
    critical = [r for r in results if r.classification == "CRITICAL"]

    if args.json:
        print(json.dumps({
            "cadence": cadence,
            "evaluated_through": through.isoformat(),
            "window_days": args.window_days,
            "checked": len(results),
            "critical": len(critical),
            "results": [
                {
                    "day": r.slot.day.isoformat(),
                    "role": r.slot.role,
                    "classification": r.classification,
                    "matched_execution": r.matched_execution,
                    "detail": r.detail,
                }
                for r in results
            ],
        }, indent=2))
    else:
        print(
            f"weekly-sf silence deadman — cadence={cadence} "
            f"window={args.window_days}d through={through.isoformat()}"
            + ("" if args.through == "today" else " (today excluded: not yet due — --through today to include)")
        )
        print(format_report(results))
        print(f"\n✗ {len(critical)} CRITICAL slot(s) — expected and absent" if critical else "\n✓ no silent slots")

    if critical and not args.no_notify:
        try:
            sns = boto3.client("sns", region_name=REGION)
            detail = "\n".join(f"- {r.slot.day.isoformat()} {r.slot.role}: {r.detail}" for r in critical)
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="Alpha Engine — weekly-SF silence deadman: CRITICAL",
                Message=(
                    "weekly_sf_silence_deadman.py found expected run-slot(s) with no matching "
                    f"execution on {STATE_MACHINE_NAME} (cadence={cadence}):\n\n{detail}\n\n"
                    "This means a day the pipeline should have run produced NO execution at "
                    "all — not a failure, silence. See alpha-engine-config#6689."
                ),
            )
        except Exception as exc:  # noqa: BLE001 — best-effort page; exit code below is the durable signal regardless
            print(f"WARN: SNS publish failed: {exc}", file=sys.stderr)

    return 1 if critical else 0


if __name__ == "__main__":
    sys.exit(main())
