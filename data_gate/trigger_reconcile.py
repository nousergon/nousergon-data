"""Reconcile each unit's DECLARED trigger against the LIVE one.

`alpha-engine-config-I11189` deliverable 2, carrying `-I11194`'s remaining
deliverable (the DISABLED-annotation check).

**The defect class.** Every run-evidence reader grades a scheduled unit from the
descriptor's declared ``trigger.schedule`` (`cadence.py::latest_due_fire`), and
nothing checked that declaration against the thing that actually fires. Fourteen
postclose descriptors declared 16:45 ET for a pipeline that starts at 16:00 ET,
so thirteen healthy units read UNMET for a week. Correcting the fourteen values
(nousergon-data-PR1824) closed the instance; this module closes the class: a
declaration that drifts from its live trigger is a FINDING the next day, not a
silent mis-grade until somebody measures by hand.

**A producer publishes, a clause reads** (`alpha-engine-config-I11035`'s rule).
`data_gate/producers/trigger_observation.py` surveys the live surfaces —
EventBridge rules, EventBridge Scheduler entries, and the owning state machines'
execution starts — and publishes what it SAW, with no verdict. The reconciliation
happens here, at read time, against the descriptors on the gate's own checkout.
That split is deliberate: a descriptor edited away from its live trigger turns
the clause red on the very next read, without waiting for the producer to run
again (the issue's closes-when).

**Per trigger kind:**

* ``step-functions`` — there is not always a schedule object to read (no enabled
  rule or Scheduler entry targets ``ne-postclose-trading-pipeline``; measured
  2026-09-20), so execution-start history is the first-class source. Every
  declared fire inside the observed window must have an execution start within
  :data:`FIRE_TOLERANCE`. Manual reruns scatter across the day and never make a
  declared fire match; they are simply not counted.
* ``eventbridge-rule`` / ``eventbridge-scheduler`` — the live ``State`` and
  ``ScheduleExpression`` (+ timezone). The DISABLED annotation after the em dash
  of ``trigger.schedule`` must agree with the live state in BOTH directions: an
  annotation that says DISABLED over an ENABLED trigger hides a live trigger
  from every run-record reader (`DisabledTriggerClause` grades by no gate), and
  an unannotated descriptor over a DISABLED trigger demands runs nobody asked
  for. While enabled, declared and live fire instants must match both ways.
* ``github-actions`` — the workflow file on this checkout IS the live schedule
  (GitHub runs the default branch's file, and the gate runs from the default
  branch), so its ``on.schedule`` cron is read from the tree, no AWS involved.

**Scope, stated rather than implied.** A unit is in scope when it is not retired
and declares a ``trigger.schedule``. A unit in scope whose declaration cannot be
reconciled against any live observation is UNMEASURABLE — never MET: *no data is
not green* (`principles.md` §2.7). Units that declare no ``trigger.schedule`` at
all (manual, on-demand dispatch, ``cadence_minutes`` systemd timers) have no
declared instant to drift and are named in the detail as out of scope.

**No wider selection window.** Deliverable 3: this module never loosens
`evidence.manifests_since`. The tolerance here is on the declared-vs-live
comparison only, and a divergence past it grades as a finding.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from dataclasses import dataclass
from zoneinfo import ZoneInfo

import yaml
from nousergon_lib.gates import GateStore, read_store_document
from nousergon_lib.trading_calendar import is_trading_day  # pyright: ignore[reportAttributeAccessIssue]

from data_gate.cadence import _DISABLED_ANNOTATION_RE, Cadence, parse_cron, unit_cadence
from data_gate.descriptors import Unit
from data_gate.evidence import Reading

__all__ = [
    "FIRE_TOLERANCE",
    "MAX_OBSERVATION_AGE",
    "OBSERVABLE_KINDS",
    "OBSERVATION_KEY",
    "OBSERVATION_SCHEMA",
    "UnitReconciliation",
    "declared_disabled",
    "fires_between",
    "owner_key",
    "read_triggers_reconciled",
    "reconcile_unit",
    "units_in_scope",
]

#: Under ``s3://alpha-engine-research/data_collection/`` — the store root every
#: gate reader is relative to.
OBSERVATION_KEY = "metrics/trigger_observation/latest.json"
OBSERVATION_SCHEMA = "trigger_observation.v1"

#: How far a live fire may sit from its declared instant and still be the SAME
#: fire. Measured 2026-09-25: `ne-postclose-trading-pipeline` starts between
#: 20:00:02Z and 20:00:58Z, `ne-preopen-trading-pipeline` at 12:15:41Z and
#: `ne-weekly-freshness-pipeline`'s Saturday run at 09:00:49Z — each under a
#: minute from its schedule. Fifteen minutes absorbs Scheduler jitter with a wide
#: margin while staying well under the 45-minute drift that caused I11189.
FIRE_TOLERANCE = dt.timedelta(minutes=15)

#: An observation older than this is not "live" any more. The producer runs
#: daily (`phase-exit-metrics.yml`, 22:00 UTC), so two missed runs plus a margin.
MAX_OBSERVATION_AGE = dt.timedelta(hours=50)

#: Trigger kinds the producer observes on AWS.
OBSERVABLE_KINDS: frozenset[str] = frozenset(
    {"step-functions", "eventbridge-rule", "eventbridge-scheduler"}
)

#: Trigger kinds reconciled from this checkout, with no AWS read at all.
_REPO_KINDS: frozenset[str] = frozenset({"github-actions"})

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

_GH_DOW = {"SUN": 6, "MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5}


# ---------------------------------------------------------------------------
# Declared side
# ---------------------------------------------------------------------------


def _trigger(unit: Unit) -> dict:
    return unit.raw.get("trigger") or {}


def _schedule_parts(unit: Unit) -> tuple[str, str]:
    text, _, annotation = str(_trigger(unit).get("schedule") or "").partition(" — ")
    return text.strip(), annotation.strip()


def declared_disabled(unit: Unit) -> bool:
    """The descriptor's own DISABLED annotation — the same predicate
    `cadence.unit_cadence` grades ``run_record`` by (`-I11194`), so the two can
    never disagree about what the annotation says."""
    return bool(_DISABLED_ANNOTATION_RE.search(_schedule_parts(unit)[1]))


def units_in_scope(units: list[Unit]) -> tuple[list[Unit], list[Unit]]:
    """``(in_scope, out_of_scope)``: in scope = not retired and declares a
    ``trigger.schedule``."""
    in_scope: list[Unit] = []
    out: list[Unit] = []
    for unit in units:
        if unit.retired:
            continue
        (in_scope if _schedule_parts(unit)[0] else out).append(unit)
    return in_scope, out


def owner_key(unit: Unit) -> str | None:
    """The observation-document key for the live surface a unit's trigger names.

    ``step-functions:<machine>`` (the ``:State`` suffix names a state INSIDE the
    machine; the machine is what starts), ``eventbridge-rule:<rule>`` on the
    default bus, ``eventbridge-scheduler:<group>/<name>`` (a bare name is the
    ``default`` group). ``None`` for a kind the producer does not observe.
    """
    trigger = _trigger(unit)
    kind = str(trigger.get("kind") or "")
    owner = str(trigger.get("owner") or "").strip()
    if kind not in OBSERVABLE_KINDS or not owner:
        return None
    if kind == "step-functions":
        return f"step-functions:{owner.split(':', 1)[0]}"
    if kind == "eventbridge-rule":
        return f"eventbridge-rule:{owner}"
    group, _, name = owner.rpartition("/")
    return f"eventbridge-scheduler:{group or 'default'}/{name}"


# ---------------------------------------------------------------------------
# Fire arithmetic
# ---------------------------------------------------------------------------


def fires_between(
    cadence: Cadence,
    start: dt.datetime,
    end: dt.datetime,
    *,
    respect_calendar: bool = True,
) -> list[dt.datetime]:
    """Every fire of a scheduled cadence in ``[start, end]``, UTC, DST-correct.

    ``respect_calendar`` drops NYSE holidays for a trading-day schedule, exactly
    as `cadence.latest_due_fire` does. Comparing two SCHEDULE EXPRESSIONS passes
    ``False`` on both sides, because a Scheduler cron fires on a holiday whatever
    the machine then does with it.
    """
    if cadence.kind != "scheduled":
        raise ValueError(f"fires_between needs a scheduled cadence, got {cadence.kind}")
    zone = ZoneInfo(cadence.tz)
    day = start.astimezone(zone).date() - dt.timedelta(days=1)
    last = end.astimezone(zone).date() + dt.timedelta(days=1)
    fires: list[dt.datetime] = []
    while day <= last:
        if day.weekday() in cadence.weekdays and not (
            respect_calendar and cadence.trading_days_only and not is_trading_day(day)
        ):
            fire = dt.datetime.combine(
                day, dt.time(cadence.hour, cadence.minute), tzinfo=zone
            )
            fire = fire.astimezone(dt.timezone.utc)
            if start <= fire <= end:
                fires.append(fire)
        day += dt.timedelta(days=1)
    return fires


def _unmatched(want: list[dt.datetime], have: list[dt.datetime]) -> list[dt.datetime]:
    return [w for w in want if not any(abs(h - w) <= FIRE_TOLERANCE for h in have)]


def _fmt(instant: dt.datetime, tz: str) -> str:
    local = instant.astimezone(ZoneInfo(tz))
    return f"{local:%a %Y-%m-%d %H:%M} {tz}"


def _modal_local_time(starts: list[dt.datetime], tz: str) -> str | None:
    if not starts:
        return None
    counts: dict[str, int] = {}
    for start in starts:
        key = start.astimezone(ZoneInfo(tz)).strftime("%H:%M")
        counts[key] = counts.get(key, 0) + 1
    best = max(sorted(counts), key=lambda k: counts[k])
    return f"{best} {tz} ({counts[best]} of {len(starts)} starts)"


def _parse_instant(value: object) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


# ---------------------------------------------------------------------------
# Live side
# ---------------------------------------------------------------------------


def _live_cadence(expression: str, tz: str | None) -> Cadence | None:
    """A live ``cron(...)`` as a Cadence, or ``None`` for ``rate(...)``/unknown."""
    expression = expression.strip()
    if not expression.startswith("cron("):
        return None
    return parse_cron(expression, tz=tz or None, trading_days_only=False)


def _gh_dow(field: str) -> frozenset[int]:
    if field == "*":
        return frozenset(range(7))
    days: set[int] = set()
    for part in field.upper().split(","):
        lo, _, hi = part.partition("-")

        def one(token: str) -> int:
            if token.isdigit():
                return (int(token) - 1) % 7  # GitHub/POSIX: 0 and 7 = Sunday
            return _GH_DOW[token[:3]]

        if hi:
            a, b = int(lo) if lo.isdigit() else None, int(hi) if hi.isdigit() else None
            if a is not None and b is not None:
                days.update((d - 1) % 7 for d in range(a, b + 1))
            else:
                start, stop = one(lo), one(hi)
                d = start
                while True:
                    days.add(d)
                    if d == stop:
                        break
                    d = (d + 1) % 7
        else:
            days.add(one(lo))
    return frozenset(days)


def _workflow_cadences(unit: Unit, root: pathlib.Path) -> tuple[list[Cadence], str]:
    """The ``on.schedule`` crons of the workflow a ``github-actions`` unit names."""
    detail = str(_trigger(unit).get("detail") or "")
    workflow = detail.split(",")[0].strip()
    if not workflow.startswith(".github/workflows/"):
        raise ValueError(f"trigger.detail names no workflow file ({detail!r})")
    path = root / workflow
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    # PyYAML reads the bare key `on` as boolean True (YAML 1.1).
    triggers = document.get("on", document.get(True)) or {}
    schedules = triggers.get("schedule") if isinstance(triggers, dict) else None
    cadences: list[Cadence] = []
    for entry in schedules or []:
        fields = str(entry.get("cron") or "").split()
        if len(fields) != 5 or fields[2] != "*" or fields[3] != "*":
            raise ValueError(f"{workflow}: unsupported cron {entry.get('cron')!r}")
        minute, hour, _, _, dow = fields
        cadences.append(
            Cadence(
                kind="scheduled",
                source=f"{workflow} cron {entry.get('cron')}",
                weekdays=_gh_dow(dow),
                hour=int(hour),
                minute=int(minute),
                tz="UTC",
            )
        )
    return cadences, workflow


# ---------------------------------------------------------------------------
# One unit
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UnitReconciliation:
    unit_id: str
    outcome: str  # "reconciled" | "divergent" | "unreconcilable"
    detail: str


def _ok(unit: Unit, detail: str) -> UnitReconciliation:
    return UnitReconciliation(unit.unit_id, "reconciled", detail)


def _divergent(unit: Unit, detail: str) -> UnitReconciliation:
    return UnitReconciliation(unit.unit_id, "divergent", detail)


def _unreconcilable(unit: Unit, detail: str) -> UnitReconciliation:
    return UnitReconciliation(unit.unit_id, "unreconcilable", detail)


def _compare_schedules(
    unit: Unit,
    declared: Cadence,
    declared_text: str,
    live: list[Cadence],
    live_label: str,
    *,
    start: dt.datetime,
    end: dt.datetime,
) -> UnitReconciliation:
    """Declared vs live SCHEDULE EXPRESSIONS: fire instants must match both ways."""
    want = fires_between(declared, start, end, respect_calendar=False)
    have = sorted(
        f for c in live for f in fires_between(c, start, end, respect_calendar=False)
    )
    if not want and not have:
        return _unreconcilable(
            unit, f"neither {declared_text!r} nor live {live_label} fires in the window"
        )
    missing = _unmatched(want, have)
    extra = _unmatched(have, want)
    if missing or extra:
        parts = [f"declares {declared_text!r} but the live trigger is {live_label}"]
        if missing:
            parts.append(
                f"{len(missing)} of {len(want)} declared fire(s) have no live fire within "
                f"{int(FIRE_TOLERANCE.total_seconds() // 60)} min (first: {_fmt(missing[0], declared.tz)})"
            )
        if extra:
            parts.append(
                f"{len(extra)} live fire(s) the declaration does not account for "
                f"(first: {_fmt(extra[0], declared.tz)})"
            )
        return _divergent(unit, "; ".join(parts))
    return _ok(
        unit,
        f"{declared_text!r} matches live {live_label} ({len(want)} fire(s) compared)",
    )


def _reconcile_schedule_object(
    unit: Unit,
    declared: Cadence,
    observation: dict,
    *,
    start: dt.datetime,
    end: dt.datetime,
) -> UnitReconciliation:
    """An EventBridge rule or Scheduler entry: state first, then expression."""
    text, _ = _schedule_parts(unit)
    state = str(observation.get("state") or "").upper()
    expression = str(observation.get("schedule_expression") or "").strip()
    tz = observation.get("timezone") or None
    if state not in {"ENABLED", "DISABLED"}:
        return _unreconcilable(unit, f"live trigger reports state {state or 'none'!r}")
    live_disabled = state == "DISABLED"
    if declared_disabled(unit) and not live_disabled:
        return _divergent(
            unit,
            f"descriptor annotates its trigger DISABLED but the live trigger is ENABLED "
            f"({expression}) — a stale annotation hiding a live trigger from run_record, which "
            "grades a declared-DISABLED unit by no gate",
        )
    if live_disabled and not declared_disabled(unit):
        return _divergent(
            unit,
            f"the live trigger is DISABLED but the descriptor declares {text!r} with no DISABLED "
            "annotation — run_record demands runs nothing is asking for",
        )
    if live_disabled:
        return _ok(
            unit,
            f"DISABLED annotation matches the live trigger's DISABLED state ({expression})",
        )
    live_label = f"{expression}" + (f" [{tz}]" if tz else "")
    if text.startswith("rate(") or expression.startswith("rate("):
        if " ".join(text.split()) == " ".join(expression.split()):
            return _ok(unit, f"{text!r} matches live {live_label}")
        return _divergent(
            unit, f"declares {text!r} but the live trigger is {live_label}"
        )
    if declared.kind != "scheduled":
        return _unreconcilable(
            unit, f"declared schedule {text!r} does not parse ({declared.source})"
        )
    try:
        live = _live_cadence(expression, tz)
    except ValueError as exc:
        return _unreconcilable(unit, f"live expression does not parse: {exc}")
    if live is None:
        return _unreconcilable(
            unit, f"live expression {expression!r} is not a cron this reader compares"
        )
    return _compare_schedules(
        unit, declared, text, [live], live_label, start=start, end=end
    )


def _reconcile_executions(
    unit: Unit,
    declared: Cadence,
    observation: dict,
    *,
    start: dt.datetime,
    end: dt.datetime,
) -> UnitReconciliation:
    """A state machine: every declared fire needs an execution start near it.

    Tolerated: a MINORITY of declared fires with no start (a missed or failed
    trigger is a real fact, but it is run_record's to grade, not a declared/live
    divergence). Not tolerated: a majority unmatched, or the two most recent
    declared fires both unmatched — the second catches a schedule moved inside
    the window before the majority has shifted.
    """
    text, _ = _schedule_parts(unit)
    if declared.kind == "disabled":
        return _unreconcilable(
            unit,
            "a DISABLED annotation on a step-functions trigger cannot be checked from execution "
            "history (a quiet schedule and a missed one look alike); declare the owning rule or "
            "schedule instead",
        )
    if declared.kind != "scheduled":
        return _unreconcilable(
            unit, f"declared schedule {text!r} does not parse ({declared.source})"
        )
    starts = sorted(
        _parse_instant(s) for s in observation.get("execution_starts") or []
    )
    want = fires_between(declared, start, end)
    if not want:
        return _unreconcilable(
            unit, f"no declared fire of {text!r} falls inside the observed window"
        )
    missing = _unmatched(want, starts)
    recent_missing = len(want) >= 2 and all(w in missing for w in want[-2:])
    machine = (owner_key(unit) or "").split(":", 1)[-1]
    if len(missing) * 2 >= len(want) or recent_missing:
        observed_on_days = [
            s
            for s in starts
            if s.astimezone(ZoneInfo(declared.tz)).weekday() in declared.weekdays
        ]
        modal = _modal_local_time(observed_on_days, declared.tz)
        return _divergent(
            unit,
            f"declares {text!r} but {len(missing)} of {len(want)} declared fire(s) have no "
            f"{machine} execution start within {int(FIRE_TOLERANCE.total_seconds() // 60)} min"
            + (
                f"; starts on declared days cluster at {modal}"
                if modal
                else "; no starts observed"
            )
            + (" (the two most recent fires both unmatched)" if recent_missing else ""),
        )
    note = (
        f"; {len(missing)} fire(s) with no start: {[_fmt(m, declared.tz) for m in missing]}"
        if missing
        else ""
    )
    return _ok(
        unit,
        f"{len(want) - len(missing)} of {len(want)} declared fire(s) of {text!r} matched by a "
        f"{machine} execution start{note}",
    )


def reconcile_unit(
    unit: Unit,
    observations: dict,
    *,
    window_start: dt.datetime,
    window_end: dt.datetime,
    repo_root: pathlib.Path = _REPO_ROOT,
) -> UnitReconciliation:
    """One in-scope unit's declared trigger against what was observed live."""
    trigger = _trigger(unit)
    kind = str(trigger.get("kind") or "")
    declared = unit_cadence(unit.raw)
    text, _ = _schedule_parts(unit)
    if kind in _REPO_KINDS:
        try:
            live, workflow = _workflow_cadences(unit, repo_root)
        except (OSError, ValueError, KeyError) as exc:
            return _unreconcilable(unit, f"workflow schedule unreadable: {exc}")
        if declared.kind != "scheduled":
            return _unreconcilable(
                unit, f"declared schedule {text!r} does not parse ({declared.source})"
            )
        if not live:
            return _divergent(
                unit, f"declares {text!r} but {workflow} has no on.schedule"
            )
        label = (
            f"{workflow} "
            + ", ".join(c.source.rsplit(" cron ", 1)[-1] for c in live)
            + " [UTC]"
        )
        return _compare_schedules(
            unit, declared, text, live, label, start=window_start, end=window_end
        )
    key = owner_key(unit)
    if key is None:
        return _unreconcilable(
            unit,
            f"trigger.kind={kind or '?'} owner={trigger.get('owner')!r} names no live surface the "
            "trigger-observation producer surveys",
        )
    observation = observations.get(key)
    if not isinstance(observation, dict):
        return _unreconcilable(unit, f"{key} is absent from the observation document")
    status = observation.get("status")
    if status != "observed":
        return _unreconcilable(
            unit,
            f"{key} was not observed ({status}: {observation.get('error') or 'no detail'})",
        )
    if kind == "step-functions":
        return _reconcile_executions(
            unit, declared, observation, start=window_start, end=window_end
        )
    return _reconcile_schedule_object(
        unit, declared, observation, start=window_start, end=window_end
    )


# ---------------------------------------------------------------------------
# The clause's reading
# ---------------------------------------------------------------------------

_SOURCE = "data_collection store + registry.d/units"


def _unmeasurable(detail: str) -> Reading:
    return Reading(
        met=False,
        detail=detail,
        evidence=(OBSERVATION_KEY, "registry.d/units/"),
        unmeasurable=True,
        source=_SOURCE,
    )


def read_triggers_reconciled(
    store: GateStore,
    units: list[Unit],
    *,
    as_of: dt.datetime,
    repo_root: pathlib.Path = _REPO_ROOT,
) -> Reading:
    """Every in-scope unit's declared trigger reconciles with its live trigger.

    UNMET on any divergence (the finding), else UNMEASURABLE while any in-scope
    unit could not be reconciled, else MET.
    """
    read = read_store_document(store, OBSERVATION_KEY)
    if read.problem is not None:
        return _unmeasurable(f"could not read {OBSERVATION_KEY}: {read.problem}")
    if read.absent:
        return _unmeasurable(
            f"{OBSERVATION_KEY} does not exist: nothing has observed the live triggers yet, so no "
            "declared trigger.schedule has been reconciled. Written daily by "
            "data_gate.producers.trigger_observation (phase-exit-metrics.yml). No data is not green."
        )
    document = read.document or {}
    if document.get("schema_version") != OBSERVATION_SCHEMA:
        return _unmeasurable(
            f"{OBSERVATION_KEY} carries schema_version={document.get('schema_version')!r}, "
            f"not {OBSERVATION_SCHEMA!r}"
        )
    try:
        observed_at = _parse_instant(document["as_of"])
        window_start = _parse_instant(document["window_start"])
    except (KeyError, ValueError) as exc:
        return _unmeasurable(
            f"{OBSERVATION_KEY} has no parseable as_of/window_start ({exc})"
        )
    age = as_of - observed_at
    if age > MAX_OBSERVATION_AGE:
        return _unmeasurable(
            f"{OBSERVATION_KEY} was observed at {document['as_of']}, {age} before this reading "
            f"(limit {MAX_OBSERVATION_AGE}); a stale observation is not the live trigger"
        )
    observations = document.get("owners") or {}
    in_scope, out_of_scope = units_in_scope(units)
    results = [
        reconcile_unit(
            unit,
            observations,
            window_start=window_start,
            window_end=observed_at,
            repo_root=repo_root,
        )
        for unit in in_scope
    ]
    divergent = [r for r in results if r.outcome == "divergent"]
    unreconcilable = [r for r in results if r.outcome == "unreconcilable"]
    reconciled = [r for r in results if r.outcome == "reconciled"]
    summary = (
        f"{len(reconciled)} reconciled, {len(divergent)} divergent, {len(unreconcilable)} "
        f"unreconcilable of {len(in_scope)} unit(s) declaring a trigger.schedule (window "
        f"{document['window_start']} .. {document['as_of']}, tolerance "
        f"{int(FIRE_TOLERANCE.total_seconds() // 60)} min). Out of scope, no trigger.schedule "
        f"declared: {sorted(u.unit_id for u in out_of_scope)}."
    )
    lines = [f"DIVERGENT {r.unit_id}: {r.detail}" for r in divergent]
    lines += [f"UNRECONCILABLE {r.unit_id}: {r.detail}" for r in unreconcilable]
    detail = summary + ("" if not lines else " " + " | ".join(lines))
    evidence = (OBSERVATION_KEY, "registry.d/units/")
    as_of_text = str(document.get("as_of") or "")
    if divergent:
        return Reading(
            met=False,
            detail=detail,
            evidence=evidence,
            source=_SOURCE,
            as_of=as_of_text,
        )
    if unreconcilable or not in_scope:
        return Reading(
            met=False,
            detail=detail,
            evidence=evidence,
            unmeasurable=True,
            source=_SOURCE,
            as_of=as_of_text,
        )
    return Reading(
        met=True, detail=detail, evidence=evidence, source=_SOURCE, as_of=as_of_text
    )
