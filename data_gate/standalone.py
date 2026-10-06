"""`data.<unit>.survives_phase4` — read from live state, never from the template.

`alpha-engine-config-I10870`, plan §6 phase-1 exit: survives_phase4 is "read
from live schedule state + a successful standalone-stack manifest per unit, not
from the template". The template says what WILL run; only live state says what
does.

**Which schedule is a unit's** comes from the stack definition itself — each
schedule's ``verify_units`` input, the list the machine's own completion check
grades (`infrastructure/data_collection_stack.py::schedules`). Never a hand
list, and never a descriptor field this module would have to keep in step.

A unit the stack covers reads MET only when, for EVERY schedule that lists it:

1. the schedule is ``ENABLED`` in live EventBridge Scheduler state;
2. the schedule's state machine (the live schedule's own ``Target.Arn``) has a
   ``SUCCEEDED`` execution that started for the most recent due fire; and
3. a ``scheduled``-trigger manifest for the unit with ``status: ok`` started
   inside that execution — a manifest from a hand run, or from the v1 pipeline,
   cannot stand in, because it did not run inside the standalone machine.

One declared exception (alpha-engine-config-I11812): a fire named in
``data_gate/config/recovered_fires.yaml`` with a recorded ruling, whose own
execution started in the window but did not SUCCEED, is graded by the cycle
counter's rule instead of 2-3 — the declared recovery execution SUCCEEDED inside
``[fire, fire + COMPLETION_GRACE]`` and the unit holds an ok scheduled manifest
that started in that interval. Every other fire is graded as above.

UNMET with the reason otherwise; UNMEASURABLE only when a read was denied or
failed (or the store carries no AWS clients at all — every test fixture).

A unit the stack does NOT cover is graded by what its trigger is:

* a Step Functions trigger (the v1 pipelines phase 4 tears down) with no
  standalone schedule is UNMET — that is the gap the requirement names;
* an on-demand/manual trigger has no schedule to lose: a declared
  not-applicable, citing the descriptor;
* any other trigger (systemd timer, GitHub Actions, the v2 scheduler) is
  outside the teardown, and is MET only on an ``ok`` manifest for its latest
  due run — a trigger that "survives" but no longer produces anything does not.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib
from dataclasses import dataclass
from functools import lru_cache
from zoneinfo import ZoneInfo

import yaml
from nousergon_lib.gates import GateStore
from nousergon_lib.trading_calendar import is_trading_day  # pyright: ignore[reportAttributeAccessIssue]

from data_gate.cadence import COMPLETION_GRACE, fire_selection_moment, latest_due_fire, parse_cron, unit_cadence
from data_gate.descriptors import REPO_ROOT, Unit
from data_gate.evidence import Reading, manifests_since, read_run_record

__all__ = [
    "EXECUTION_START_WINDOW",
    "RECOVERED_FIRES_PATH",
    "RecoveredFire",
    "covering_schedules",
    "load_recovered_fires",
    "stack_schedules",
    "read_standalone_workload_declared",
    "read_survives_phase4",
]

_STACK_HELPER = REPO_ROOT / "infrastructure" / "data_collection_stack.py"

#: Scheduler runs with FlexibleTimeWindow OFF (the lint enforces it), so an
#: execution starts within seconds of its fire. Fifteen minutes absorbs a
#: Scheduler retry without admitting the next day's run.
EXECUTION_START_WINDOW = dt.timedelta(minutes=15)

_SOURCE_LIVE = "scheduler:GetSchedule + states:ListExecutions + data_collection store"

#: Declared recoveries of one missed fire each (alpha-engine-config-I11812).
#: The file's header is the contract; `_grade_recovered_fire` is its reader.
RECOVERED_FIRES_PATH = REPO_ROOT / "data_gate" / "config" / "recovered_fires.yaml"
_RECOVERED_FIRES_SCHEMA = "data_recovered_fires.v1"
_RECOVERED_FIRE_FIELDS = (
    "schedule",
    "fire",
    "machine",
    "fire_execution",
    "recovery_execution",
    "root_cause_fix",
    "tracker",
    "ruling",
)
#: The `ruling:` value that keeps a declared recovery inert.
_RULING_PENDING = "pending"

#: `read_standalone_workload_declared` reads the committed stack definition and
#: the committed descriptor, and NOTHING else. Named so a reader can tell the
#: two questions apart on the source line alone.
_SOURCE_DECLARED = "infrastructure/data_collection_stack.py (verify_units) + registry.d/units"


@lru_cache(maxsize=1)
def _stack_schedules() -> tuple[dict, ...]:
    # A standalone script, not a package: loaded by path, the same way its own
    # tests load it, so the gate and the deploy tool share one parser.
    spec = importlib.util.spec_from_file_location("data_collection_stack", _STACK_HELPER)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return tuple(module.schedules(module.load_template()))


def stack_schedules() -> tuple[dict, ...]:
    """Every schedule the COMMITTED stack declares.

    The public reading of the parsed template, so a caller that needs a
    schedule by name (`data_gate.exit_criteria`, which counts a schedule's
    cycles) reads the same parse the deploy tool does instead of re-parsing
    the template or hand-listing the names.
    """
    return _stack_schedules()


@dataclass(frozen=True)
class RecoveredFire:
    """One declared recovery of one missed fire — see `recovered_fires.yaml`."""

    schedule: str
    fire: dt.datetime
    machine: str
    fire_execution: str
    recovery_execution: str
    root_cause_fix: str
    tracker: str
    ruling: str

    @property
    def ruled(self) -> bool:
        return self.ruling.strip().lower() != _RULING_PENDING

    def names(self, execution: dict, which: str) -> bool:
        """Whether ``execution`` is this declaration's ``which`` execution of its machine."""
        return str(execution.get("executionArn") or "").endswith(f":{self.machine}:{getattr(self, which)}")


def _is_fire_of(schedule: dict, fire: dt.datetime) -> bool:
    cadence = parse_cron(
        schedule["expression"],
        tz=schedule["timezone"],
        trading_days_only=bool(schedule["input"].get("require_trading_day")),
    )
    local = fire.astimezone(ZoneInfo(cadence.tz))
    if cadence.trading_days_only and not is_trading_day(local.date()):
        return False
    return (
        local.weekday() in cadence.weekdays
        and (local.hour, local.minute, local.second, local.microsecond) == (cadence.hour, cadence.minute, 0, 0)
    )


def load_recovered_fires(path: pathlib.Path | None = None) -> tuple[RecoveredFire, ...]:
    """Every declared recovery, refusing any shape that could widen one.

    Raises rather than skipping, and `contain_clause_exceptions` turns the raise
    into UNMEASURABLE rows: a declaration this loader silently dropped, or read
    more broadly than written, is a gate reading nobody ruled on.
    """
    if path is None:
        return _committed_recovered_fires()
    return _parse_recovered_fires(path)


@lru_cache(maxsize=1)
def _committed_recovered_fires() -> tuple[RecoveredFire, ...]:
    return _parse_recovered_fires(RECOVERED_FIRES_PATH)


def _parse_recovered_fires(path: pathlib.Path) -> tuple[RecoveredFire, ...]:
    if not path.exists():
        return ()
    doc = yaml.safe_load(path.read_text()) or {}
    if doc.get("schema_version") != _RECOVERED_FIRES_SCHEMA:
        raise ValueError(f"{path.name}: schema_version is {doc.get('schema_version')!r}, not {_RECOVERED_FIRES_SCHEMA!r}")
    entries = doc.get("recovered_fires") or []
    if not isinstance(entries, list):
        raise ValueError(f"{path.name}: recovered_fires is {type(entries).__name__}, not a list")
    by_name = {s["name"]: s for s in _stack_schedules()}
    seen: set[tuple[str, dt.datetime]] = set()
    out: list[RecoveredFire] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"{path.name}: an entry is {type(entry).__name__}, not a mapping: {entry!r}")
        missing = [f for f in _RECOVERED_FIRE_FIELDS if not str(entry.get(f) or "").strip()]
        extra = sorted(set(entry) - set(_RECOVERED_FIRE_FIELDS))
        if missing or extra:
            raise ValueError(f"{path.name}: entry {entry.get('fire')!r} is missing {missing} / carries unknown {extra}")
        schedule = by_name.get(str(entry["schedule"]))
        if schedule is None:
            raise ValueError(f"{path.name}: schedule {entry['schedule']!r} is not in the committed stack")
        try:
            fire = dt.datetime.strptime(str(entry["fire"]), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
        except ValueError as exc:
            raise ValueError(f"{path.name}: fire {entry['fire']!r} is not YYYY-MM-DDTHH:MM:SSZ") from exc
        if not _is_fire_of(schedule, fire):
            raise ValueError(
                f"{path.name}: {entry['fire']} is not a fire of {schedule['name']} ({schedule['expression']} "
                f"{schedule['timezone']}) — a declaration names one real fire, never a window"
            )
        if (schedule["name"], fire) in seen:
            raise ValueError(f"{path.name}: {schedule['name']} {entry['fire']} is declared twice")
        seen.add((schedule["name"], fire))
        out.append(
            RecoveredFire(
                schedule=schedule["name"],
                fire=fire,
                **{f: str(entry[f]).strip() for f in _RECOVERED_FIRE_FIELDS if f not in ("schedule", "fire")},
            )
        )
    return tuple(out)


def _recovery_for(schedule: dict, fire: dt.datetime) -> RecoveredFire | None:
    return next(
        (r for r in load_recovered_fires() if r.schedule == schedule["name"] and r.fire == fire),
        None,
    )


def covering_schedules(unit_id: str) -> list[dict]:
    """Every standalone schedule whose ``verify_units`` names this unit."""
    return [s for s in _stack_schedules() if unit_id in (s["input"].get("verify_units") or [])]


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or "")
    return ""


def _unmeasurable(detail: str, evidence: tuple[str, ...]) -> Reading:
    return Reading(met=False, detail=detail, evidence=evidence, unmeasurable=True, source=_SOURCE_LIVE)


def _executions_since(sfn, machine_arn: str, since: dt.datetime) -> list[dict]:
    """Every execution of the machine that started at or after ``since``."""
    kwargs = {"stateMachineArn": machine_arn, "maxResults": 100}
    found: list[dict] = []
    while True:
        page = sfn.list_executions(**kwargs)
        for execution in page.get("executions", []):
            if execution["startDate"].astimezone(dt.timezone.utc) < since:
                return found
            found.append(execution)
        token = page.get("nextToken")
        if not token:
            return found
        kwargs["nextToken"] = token


def _parse_started(stamp: object) -> dt.datetime | None:
    try:
        return dt.datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    except (TypeError, ValueError):
        return None


def _grade_recovered_fire(
    store: GateStore,
    unit: Unit,
    recovery: RecoveredFire,
    machine: str,
    fire_execution: dict,
    as_of: dt.datetime,
    evidence: tuple[str, ...],
) -> Reading:
    """A declared, ruled recovery of ``recovery.fire`` — see `recovered_fires.yaml`.

    Reached only when the fire's own execution started inside the window and is
    not SUCCEEDED. Three conditions, every one read from live state or the
    store, never from the declaration's prose:

    1. the execution that started for the fire IS the declared one;
    2. the declared recovery execution is SUCCEEDED, on the same machine, and
       started inside [fire, fire + COMPLETION_GRACE] and before ``as_of``;
    3. the unit holds an ok scheduled-trigger manifest that started inside
       [fire, fire + COMPLETION_GRACE] — the cycle counter's rule.
    """
    fire_s = recovery.fire.strftime("%Y-%m-%dT%H:%MZ")
    declared = f"data_gate/config/recovered_fires.yaml ({recovery.tracker}; ruling {recovery.ruling})"
    evidence = evidence + ("data_gate/config/recovered_fires.yaml",)
    if not machine.endswith(f":{recovery.machine}") or not recovery.names(fire_execution, "fire_execution"):
        return Reading(
            met=False,
            detail=(
                f"a recovery is declared for the {fire_s} fire of {recovery.machine}/{recovery.fire_execution}, "
                f"but the execution that started for it is {fire_execution.get('executionArn')} on {machine}"
            ),
            evidence=evidence,
            source=_SOURCE_LIVE,
        )
    window_end = recovery.fire + COMPLETION_GRACE
    try:
        candidates = _executions_since(store.sfn_client, machine, recovery.fire)  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
        return _unmeasurable(f"could not list executions of {machine}: {type(exc).__name__}: {exc}", evidence)
    rec = next((e for e in candidates if recovery.names(e, "recovery_execution")), None)
    if rec is None:
        return Reading(
            met=False,
            detail=(
                f"the declared recovery {recovery.recovery_execution} of the {fire_s} fire is not an execution "
                f"of {machine} started at or after the fire"
            ),
            evidence=evidence,
            source=_SOURCE_LIVE,
        )
    evidence = evidence + (str(rec.get("executionArn")),)
    rec_start = rec["startDate"].astimezone(dt.timezone.utc)
    rec_stop = rec.get("stopDate")
    if str(rec.get("status")) != "SUCCEEDED" or rec_stop is None:
        return Reading(
            met=False,
            detail=f"the declared recovery {recovery.recovery_execution} of the {fire_s} fire is {rec.get('status')}, not SUCCEEDED",
            evidence=evidence,
            source=_SOURCE_LIVE,
        )
    rec_stop = rec_stop.astimezone(dt.timezone.utc)
    if rec_start > window_end or rec_stop > as_of:
        return Reading(
            met=False,
            detail=(
                f"the declared recovery {recovery.recovery_execution} started {rec_start:%Y-%m-%dT%H:%MZ} and stopped "
                f"{rec_stop:%Y-%m-%dT%H:%MZ}; it must start by {window_end:%Y-%m-%dT%H:%MZ} (fire + completion grace) "
                "and finish before the reading"
            ),
            evidence=evidence,
            source=_SOURCE_LIVE,
        )
    ceiling = min(window_end, as_of)
    try:
        docs, problems, where = manifests_since(store, unit, since=recovery.fire, as_of=ceiling)
    except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
        return _unmeasurable(f"could not list {unit.run_manifest_prefix}: {type(exc).__name__}: {exc}", evidence)
    if problems:
        return _unmeasurable(f"manifest(s) unreadable under {where}: {problems[:4]}", evidence)
    unparsed = [k for k, d in docs if _parse_started(d.get("started")) is None]
    if unparsed:
        return _unmeasurable(f"`started` does not parse on {unparsed[:4]} under {where}", evidence)
    ok = [k for k, d in docs if d.get("trigger") == "scheduled" and d.get("status") == "ok"]
    if not ok:
        seen = sorted({f"{d.get('trigger')}/{d.get('status')}" for _, d in docs}) or ["none"]
        return Reading(
            met=False,
            detail=(
                f"a ruled recovery of the {fire_s} fire is declared ({declared}) and "
                f"{recovery.recovery_execution} SUCCEEDED, but {unit.unit_id} has no ok scheduled-trigger manifest "
                f"started in [{fire_s}, {window_end:%Y-%m-%dT%H:%MZ}] (seen: {seen}; under {where})"
            ),
            evidence=evidence + tuple(k for k, _ in docs),
            source=_SOURCE_LIVE,
        )
    return Reading(
        met=True,
        detail=(
            f"the {fire_s} execution {recovery.fire_execution} started in the window and was {fire_execution.get('status')}; "
            f"counted under the declared recovery {recovery.recovery_execution} (SUCCEEDED "
            f"{rec_stop:%Y-%m-%dT%H:%MZ}; {declared}): {len(ok)} ok scheduled manifest(s) in "
            f"[{fire_s}, {window_end:%Y-%m-%dT%H:%MZ}]"
        ),
        evidence=evidence + tuple(ok),
        source=_SOURCE_LIVE,
        as_of=str(rec_stop.isoformat()),
    )


def _execution_for(sfn, machine_arn: str, fire: dt.datetime) -> dict | None:
    """The execution that started for ``fire``, newest-first, stopping once past it."""
    kwargs = {"stateMachineArn": machine_arn, "maxResults": 100}
    while True:
        page = sfn.list_executions(**kwargs)
        for execution in page.get("executions", []):
            started = execution["startDate"].astimezone(dt.timezone.utc)
            if fire <= started <= fire + EXECUTION_START_WINDOW:
                return execution
            if started < fire:
                return None
        token = page.get("nextToken")
        if not token:
            return None
        kwargs["nextToken"] = token


def _grade_schedule(store: GateStore, unit: Unit, schedule: dict, live: dict, as_of: dt.datetime) -> Reading:
    """Conditions 2 and 3 for one ENABLED schedule."""
    name = schedule["qualified_name"]
    cadence = parse_cron(
        str(live.get("ScheduleExpression") or schedule["expression"]),
        tz=live.get("ScheduleExpressionTimezone") or schedule["timezone"],
        trading_days_only=bool(schedule["input"].get("require_trading_day")),
    )
    fire = latest_due_fire(cadence, as_of=as_of, grace=COMPLETION_GRACE)
    machine = str((live.get("Target") or {}).get("Arn") or "")
    evidence = (f"scheduler:{name}", f"states:{machine or '?'}")
    if not machine:
        return Reading(
            met=False, detail=f"{name} is ENABLED but live state names no target", evidence=evidence, source=_SOURCE_LIVE
        )
    try:
        execution = _execution_for(store.sfn_client, machine, fire)  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
        # The failure mode is a denied/throttled ListExecutions; the rest of
        # the board survives; the recording surface is this UNMEASURABLE row.
        return _unmeasurable(f"could not list executions of {machine}: {type(exc).__name__}: {exc}", evidence)
    fire_s = fire.strftime("%Y-%m-%dT%H:%MZ")
    if execution is None:
        return Reading(
            met=False,
            detail=f"{name} is ENABLED but {machine} has no execution started for the fire due at {fire_s}",
            evidence=evidence,
            source=_SOURCE_LIVE,
        )
    status = str(execution.get("status"))
    evidence = evidence + (str(execution.get("executionArn")),)
    if status != "SUCCEEDED":
        # alpha-engine-config-I11812: one declared, ruled recovery of exactly
        # this (schedule, fire) may stand in for the SUCCEEDED half. Any other
        # fire — and this one while its ruling is pending — reads as before.
        recovery = _recovery_for(schedule, fire)
        if recovery is not None and recovery.ruled:
            return _grade_recovered_fire(store, unit, recovery, machine, execution, as_of, evidence)
        pending = (
            f" (a recovery is declared for this fire in data_gate/config/recovered_fires.yaml but its ruling "
            f"is pending: {recovery.tracker})"
            if recovery is not None
            else ""
        )
        return Reading(
            met=False,
            detail=f"the {fire_s} execution of {machine} is {status}, not SUCCEEDED{pending}",
            evidence=evidence,
            source=_SOURCE_LIVE,
        )
    start = execution["startDate"].astimezone(dt.timezone.utc)
    stop = execution["stopDate"].astimezone(dt.timezone.utc)
    try:
        docs, problems, where = manifests_since(store, unit, since=start, as_of=stop)
    except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
        return _unmeasurable(f"could not list {unit.run_manifest_prefix}: {type(exc).__name__}: {exc}", evidence)
    if problems:
        return _unmeasurable(f"manifest(s) unreadable under {where}: {problems[:4]}", evidence)
    inside = [(k, d) for k, d in docs if d.get("trigger") == "scheduled"]
    if not inside:
        return Reading(
            met=False,
            detail=(
                f"the {fire_s} execution of {machine} SUCCEEDED but no scheduled-trigger manifest "
                f"for {unit.unit_id} started inside it (under {where})"
            ),
            evidence=evidence,
            source=_SOURCE_LIVE,
        )
    not_ok = sorted({str(d.get("status")) for _, d in inside if d.get("status") != "ok"})
    keys = tuple(k for k, _ in inside)
    if not_ok:
        return Reading(
            met=False,
            detail=f"{unit.unit_id} ran inside the {fire_s} execution of {machine} with status {not_ok}, not ok",
            evidence=evidence + keys,
            source=_SOURCE_LIVE,
        )
    return Reading(
        met=True,
        detail=f"{name} ENABLED; the {fire_s} execution SUCCEEDED and recorded {len(keys)} ok manifest(s)",
        evidence=evidence + keys,
        source=_SOURCE_LIVE,
        as_of=str(stop.isoformat()),
    )


def read_survives_phase4(
    store: GateStore, unit: Unit, *, trading_day: dt.date, now: dt.datetime | None = None
) -> Reading:
    """See the module docstring.

    A unit declaring `partial_exclusion` naming ``survives_phase4`` (see
    `unit_readers.partial_exclusion_reading`) is graded from that
    declaration alone. Checked FIRST, not left to fall out of the
    ``read_run_record`` reuse below: D47 (component 2, plan §8.1) reused this
    module's own `read_run_record(...)` call to decide "does it still fire",
    so once `read_run_record` started honoring D47's `partial_exclusion` for
    `run_record` (`alpha-engine-config-I11245`/`-I10810`), this reader would
    otherwise have inherited that MET incidentally, worded as if a "latest
    due run recorded" — true in outcome, wrong in reasoning. Explicit beats
    inherited (imported here, not at module top: `unit_readers` imports
    `Reading` from `evidence`, which this module also imports from).
    """
    from data_gate import unit_readers

    excluded = unit_readers.partial_exclusion_reading(unit, "survives_phase4")
    if excluded is not None:
        return excluded
    schedules = covering_schedules(unit.unit_id)
    trigger = unit.raw.get("trigger") or {}
    kind = str(trigger.get("kind") or "")
    descriptor = unit.path.relative_to(REPO_ROOT).as_posix()

    if not schedules:
        cadence = unit_cadence(unit.raw)
        if kind == "step-functions":
            return Reading(
                met=False,
                detail=(
                    f"{unit.unit_id}'s trigger is the v1 Step Functions pipeline "
                    f"{trigger.get('owner')!r}, which v2 phase 4 tears down, and no schedule in the "
                    "nousergon-data-collection stack lists it in verify_units. It needs a standalone "
                    "workload or a recorded retirement decision."
                ),
                evidence=(descriptor, "infrastructure/cloudformation/nousergon-data-collection.yaml"),
                source="registry.d/units + stack definition",
            )
        if cadence.kind == "on_demand":
            return Reading(
                met=True,
                detail=(
                    f"not applicable: {unit.unit_id} runs on demand ({cadence.source}; "
                    f"{trigger.get('successor')!r}) — it has no scheduled trigger for phase 4 to remove"
                ),
                evidence=(descriptor,),
                source="registry.d/units",
            )
        record = read_run_record(store, unit, trading_day=trading_day, now=now)
        if record.unmeasurable or not record.met:
            return Reading(
                met=False,
                detail=(
                    f"{unit.unit_id}'s trigger ({kind}, {trigger.get('successor')!r}) is outside the "
                    f"phase-4 teardown, but it has no current run to show it still fires: {record.detail}"
                ),
                evidence=record.evidence,
                unmeasurable=record.unmeasurable,
                source=record.source,
            )
        return Reading(
            met=True,
            detail=(
                f"{unit.unit_id}'s trigger ({kind}, {trigger.get('successor')!r}) is outside the "
                f"phase-4 teardown and its latest due run recorded: {record.detail}"
            ),
            evidence=record.evidence,
            source=record.source,
            as_of=record.as_of,
        )

    names = [s["qualified_name"] for s in schedules]
    evidence = tuple(f"scheduler:{n}" for n in names)
    scheduler = getattr(store, "scheduler_client", None)
    if scheduler is None or getattr(store, "sfn_client", None) is None:
        return _unmeasurable(
            f"this store backend supplies no Scheduler/Step Functions client (only a live S3Store "
            f"does); {unit.unit_id} is covered by {names}",
            evidence,
        )

    live: dict[str, dict] = {}
    for schedule in schedules:
        try:
            live[schedule["qualified_name"]] = scheduler.get_schedule(
                GroupName=schedule["group"], Name=schedule["name"]
            )
        except Exception as exc:  # noqa: BLE001 - classified below
            if _error_code(exc) == "ResourceNotFoundException":
                return Reading(
                    met=False,
                    detail=(
                        f"schedule {schedule['qualified_name']} does not exist live: the "
                        "nousergon-data-collection stack is not applied"
                    ),
                    evidence=evidence,
                    source=_SOURCE_LIVE,
                )
            # Denied or throttled: we could not look. Recording surface: this row.
            return _unmeasurable(
                f"could not read schedule {schedule['qualified_name']}: {type(exc).__name__}: {exc}", evidence
            )

    disabled = sorted(n for n, doc in live.items() if doc.get("State") != "ENABLED")
    if disabled:
        return Reading(
            met=False,
            detail=(
                f"schedule(s) {disabled} are {sorted({live[n].get('State') for n in disabled})} in live "
                f"state, so {unit.unit_id} has no standalone manifest and cannot have one until the "
                "cutover enables them (plan §6.2)"
            ),
            evidence=evidence,
            source=_SOURCE_LIVE,
        )

    # alpha-engine-config-I11354, lifted into `cadence.fire_selection_moment` by
    # alpha-engine-config-I11838 so `run_record`, `completeness` and the
    # exit-criteria cycle counters select the same fire this clause does: the
    # end of the trading day plus the completion grace, bounded by the clock. A
    # schedule firing after 17:59 ET (the 18:15 ET EOD) is graded for its own
    # day. Widened for fire selection only — `_grade_schedule` bounds manifests
    # by the execution's own start/stop, so nothing from a later day counts.
    as_of = fire_selection_moment(trading_day, now)
    readings = [_grade_schedule(store, unit, s, live[s["qualified_name"]], as_of) for s in schedules]
    evidence = tuple(e for r in readings for e in r.evidence)
    failing = [r for r in readings if not r.met]
    if any(r.unmeasurable for r in failing):
        return _unmeasurable("; ".join(r.detail for r in failing), evidence)
    if failing:
        return Reading(met=False, detail="; ".join(r.detail for r in failing), evidence=evidence, source=_SOURCE_LIVE)
    return Reading(
        met=True,
        detail="; ".join(r.detail for r in readings),
        evidence=evidence,
        source=_SOURCE_LIVE,
        as_of=max(str(r.as_of or "") for r in readings),
    )


def read_standalone_workload_declared(unit: Unit) -> Reading:
    """Is a standalone workload DECLARED for this unit — plan §6.2 item 3.

    A static question, answerable with every schedule off, and deliberately so:
    `data-cutover-ready` is read BEFORE the maintenance window that enables the
    schedules, so a leg of it that needs an ENABLED schedule is satisfiable only
    by the action it guards (`alpha-engine-config-I10989`). The dynamic question
    — has the unit PRODUCED under its own enabled schedule — is
    `read_survives_phase4`, and it belongs at the phase-1 exit, "read after the
    cutover's first 5 trading days" (plan §6.2 item 7).

    MET when some schedule in the `nousergon-data-collection` stack names the
    unit in its ``verify_units`` — the same list the machine's own completion
    check grades, so "declared" means the workload will actually verify it, not
    that a descriptor mentions a successor. Schedule STATE is not read here, and
    no AWS client is touched at all.

    Retirement is not handled here: a retired unit is satisfied by its recorded
    retirement decision and is dropped before this is called.
    """
    descriptor = unit.path.relative_to(REPO_ROOT).as_posix()
    schedules = covering_schedules(unit.unit_id)
    if schedules:
        names = [s["qualified_name"] for s in schedules]
        return Reading(
            met=True,
            detail=(
                f"{unit.unit_id} is named in the verify_units of {names} in the "
                "nousergon-data-collection stack definition (schedule state not read: this leg "
                "is answered before the cutover enables them)"
            ),
            evidence=(descriptor, "infrastructure/data_collection_stack.py") + tuple(f"verify_units:{n}" for n in names),
            source=_SOURCE_DECLARED,
        )
    trigger = unit.raw.get("trigger") or {}
    return Reading(
        met=False,
        detail=(
            f"no schedule in the nousergon-data-collection stack names {unit.unit_id} in its "
            f"verify_units, although its descriptor declares the successor "
            f"{trigger.get('successor')!r}. It needs a standalone workload or a recorded "
            "retirement decision (plan §6.2 item 3)"
        ),
        evidence=(descriptor, "infrastructure/cloudformation/nousergon-data-collection.yaml"),
        source=_SOURCE_DECLARED,
    )
