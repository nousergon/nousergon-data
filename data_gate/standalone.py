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
from functools import lru_cache

from nousergon_lib.gates import GateStore

from data_gate.cadence import COMPLETION_GRACE, gate_moment, latest_due_fire, parse_cron, unit_cadence
from data_gate.descriptors import REPO_ROOT, Unit
from data_gate.evidence import Reading, manifests_since, read_run_record

__all__ = [
    "EXECUTION_START_WINDOW",
    "covering_schedules",
    "read_standalone_workload_declared",
    "read_survives_phase4",
]

_STACK_HELPER = REPO_ROOT / "infrastructure" / "data_collection_stack.py"

#: Scheduler runs with FlexibleTimeWindow OFF (the lint enforces it), so an
#: execution starts within seconds of its fire. Fifteen minutes absorbs a
#: Scheduler retry without admitting the next day's run.
EXECUTION_START_WINDOW = dt.timedelta(minutes=15)

_SOURCE_LIVE = "scheduler:GetSchedule + states:ListExecutions + data_collection store"

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
        return Reading(
            met=False,
            detail=f"the {fire_s} execution of {machine} is {status}, not SUCCEEDED",
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
    """See the module docstring."""
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

    as_of = gate_moment(trading_day, now)
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
