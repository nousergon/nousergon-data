"""Producer for the 16 ``data.slo.{freshness,completeness}.<family>`` clauses
(`alpha-engine-config-I10789`, plan item P-22; `data_collection_plan_260914.md`
§2 rows 1 and 2).

Writes, under ``s3://alpha-engine-research/data_collection``::

    metrics/slo/freshness/<family>/latest.json
    metrics/slo/completeness/<family>/latest.json

one pair per freshness family the unit descriptors declare — the exact keys
`data_gate/clauses.py::generate` already reads through
`evidence.read_objective`. Until this module those keys had no writer, so all
16 rows read "no metric document ... an objective with no emitter is
unobserved, not met".

**Computed from the run manifests, never from S3 HEAD** (plan §2 row 1:
"computed from run manifests, not HEAD alone"). A HEAD on a published key says
an object was written at some time; it cannot say which session the content
describes, whether the run that wrote it succeeded, or whether a scheduler or a
human started it. The manifest (`data_run_manifest.v1`) carries all three.

**What a cycle is.** One occurrence of the unit's declared DEADLINE
(``freshness.deadline``), not one fire of its trigger: the SLO is about when
the artifact is usable, and a 15-minute trigger with a once-a-day deadline has
one cycle a day. The cycle's trading day is derived from the deadline's own
local date through the family's declared session convention
(:data:`SESSION_CONVENTION`), independently of what any run claims — so the
partition read for a cycle is ``runs/<unit>/<that trading day>/``, and a run
that recorded a different session cannot satisfy it.

**Freshness** — per cycle, per unit: an ``ok`` manifest from a counted trigger
(:data:`COUNTED_TRIGGERS`) whose ``finished`` is at or before the deadline and
whose own ``trading_day`` equals the cycle's. A family cycle is met when every
graded unit met it. ``status: ok`` needs >= 19 of the last 20 cycles met and
the as-of check holding on every cycle that had a run, over a full 20-cycle
window (plan §2 row 1). The intraday family's deadline is a slot rule instead
(``age <= N minutes in >= P% of session slots``): each session slot is fresh
when an ``ok`` run finished in the N minutes before it.

**Completeness** — per cycle, per unit: the newest ``ok`` counted manifest of
the cycle carries ``data_cardinality`` guard records (`validators/
expectations.py::check_cardinality`, the covered ÷ (denominator − declared
exclusions) >= floor measurement) and every one is clean. A unit whose
descriptor declares its completeness ``not_applicable`` is a DECLARED
exclusion, named in the document; a unit that declares a floor but whose run
recorded no cardinality guard is a MISS, never an exclusion — "we looked and
the measurement is not there" is the answer, not a shrug. ``status: ok`` needs
every cycle of the window met with at least one graded unit; a family with no
graded unit at all is a breach naming that, never a vacuous ``ok``.

**A document says when it stops being evidence.** Each carries
``stale_after_utc``; `evidence.read_objective` renders a document past it
UNMEASURABLE, so an emitter that stops leaves the clause red rather than its
last answer standing.

**Read failures are never a verdict.** A listing or manifest read that fails
for a family leaves that family's documents UNWRITTEN (the previous ones age
out through ``stale_after_utc``), the run record is ``error``, and the process
exits non-zero after writing every family it could measure.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import logging
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

from nousergon_lib.gates import read_store_document
from nousergon_lib.trading_calendar import (  # pyright: ignore[reportAttributeAccessIssue]
    is_trading_day,
    next_trading_day,
    previous_trading_day,
    session_close_et,
)

from data_gate.cadence import Cadence, latest_trading_day_on_or_before, unit_cadence
from data_gate.descriptors import Unit, load_units

__all__ = [
    "COUNTED_TRIGGERS",
    "DEFAULT_STORE",
    "FRESHNESS_REQUIRED",
    "PRODUCER",
    "SESSION_CONVENTION",
    "SLO_SCHEMA",
    "STALE_AFTER",
    "WINDOW",
    "Deadline",
    "build_documents",
    "deadline_instants",
    "expected_trading_day",
    "main",
    "parse_deadline",
    "slo_key",
]

logger = logging.getLogger(__name__)

SLO_SCHEMA = "data_slo.v1"
PRODUCER = "slo"
DEFAULT_STORE = "s3://alpha-engine-research/data_collection"

#: Plan §2 row 1: "met on >= 19 of every rolling 20 scheduled cycles".
WINDOW = 20
FRESHNESS_REQUIRED = 19

#: A run started by a schedule. ``gha`` is how a GitHub-Actions-cron unit
#: (D39) records its scheduled run. ``manual``, ``on_demand`` and ``backfill``
#: are deliberately NOT counted: plan §2 row 11 puts no operator on the success
#: path, so a hand repair is a human touch, not an SLO cycle met. The counts of
#: what was excluded are written into every document, so a family carried by
#: repairs is visible rather than silently red or silently green.
COUNTED_TRIGGERS: frozenset[str] = frozenset({"scheduled", "gha"})

#: Which trading session a cycle whose deadline falls on local date ``L``
#: describes. Declared per family, because the producers genuinely differ and
#: the difference is not derivable from the clock (measured on the 2026-10-01..
#: 2026-10-03 manifests): the 07:30 ET morning enrich files the PREVIOUS
#: session (D17/D18 run 10-02, ``trading_day`` 10-01); the 04:00 PT news run
#: files the session it precedes (D36 Saturday 10-03, ``trading_day`` 10-05);
#: everything else files the latest session on or before its run date (the
#: Saturday weekly files Friday). A family missing here is a breach naming the
#: gap — `tests/test_slo_producer.py` asserts every declared family has one.
SESSION_CONVENTION: dict[str, str] = {
    "eod-spine": "on_or_before",
    "weekly": "on_or_before",
    "weekly-membership": "on_or_before",
    "inst_ownership": "on_or_before",
    "crypto": "on_or_before",
    "intraday": "on_or_before",
    "morning": "previous",
    "daily-news": "on_or_after",
}

#: A document older than this is no longer evidence (`read_objective`). The
#: producer runs daily; 50 h is one missed run plus slack, the same ceiling the
#: trigger-observation row uses.
STALE_AFTER = dt.timedelta(hours=50)

#: The regular session the intraday slot rule is graded over. The close comes
#: from the NYSE calendar, so an early-close day has fewer slots, not misses.
_SESSION_OPEN = dt.time(9, 30)
_SESSION_TZ = "America/New_York"

_DOW = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
_WEEKLY = re.compile(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (\d{2}):(\d{2}) (\S+)")
_DAILY = re.compile(r"^(\d{2}):(\d{2}) (\S+)")
_SLOTS = re.compile(r"^age <= (\d+) minutes in >= (\d+(?:\.\d+)?)% of session slots")
_TRADING_DAYS = frozenset(range(5))
_ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_STORE_PREFIX = "data_collection/"
_READ_WORKERS = 10  # botocore's default connection pool size


def slo_key(objective: str, family: str) -> str:
    """The store-relative key `clauses.py` reads for one objective row."""
    return f"metrics/slo/{objective}/{family}/latest.json"


# ---------------------------------------------------------------------------
# Deadlines and cycles.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Deadline:
    """A unit's declared freshness deadline, parsed."""

    kind: str  # "daily" | "weekly" | "slots" | "undeclared"
    source: str
    weekdays: frozenset[int] = frozenset()
    hour: int = 0
    minute: int = 0
    tz: str = "UTC"
    trading_days_only: bool = False
    slot_minutes: int = 0
    max_age_minutes: int = 0
    slot_share: float = 0.0


def parse_deadline(text: str | None, cadence: Cadence, *, slot_minutes: int | None = None) -> Deadline:
    """``freshness.deadline`` as a recurrence. Never guesses a shape it does not know.

    ``"Sat 13:00 America/New_York"`` recurs weekly. ``"18:15 America/New_York"``
    recurs on the days the unit's schedule fires (a weekday-only schedule is a
    trading-day schedule, as `cadence.unit_cadence` already rules); a unit with
    no fixed fire time recurs on trading days. Trailing prose after the
    timezone (D38's ruling citation) is ignored. The intraday slot form is its
    own kind.
    """
    raw = " ".join(str(text or "").split())
    if not raw:
        return Deadline(kind="undeclared", source="freshness.deadline is not declared")
    slots = _SLOTS.match(raw)
    if slots:
        return Deadline(
            kind="slots",
            source=raw,
            weekdays=_TRADING_DAYS,
            tz=_SESSION_TZ,
            trading_days_only=True,
            slot_minutes=int(slot_minutes or 5),
            max_age_minutes=int(slots.group(1)),
            slot_share=float(slots.group(2)) / 100.0,
        )
    weekly = _WEEKLY.match(raw)
    if weekly:
        day, hh, mm, tz = weekly.groups()
        ZoneInfo(tz)  # raises on an unknown zone, which is a descriptor defect
        return Deadline(
            kind="weekly",
            source=raw,
            weekdays=frozenset({_DOW[day.upper()]}),
            hour=int(hh),
            minute=int(mm),
            tz=tz,
        )
    daily = _DAILY.match(raw)
    if daily:
        hh, mm, tz = daily.groups()
        ZoneInfo(tz)
        if cadence.kind == "scheduled":
            weekdays, trading_only = cadence.weekdays, cadence.trading_days_only
        else:
            weekdays, trading_only = _TRADING_DAYS, True
        return Deadline(
            kind="daily",
            source=raw,
            weekdays=weekdays,
            hour=int(hh),
            minute=int(mm),
            tz=tz,
            trading_days_only=trading_only,
        )
    return Deadline(kind="undeclared", source=f"freshness.deadline={raw!r} is not a recognised shape")


def _session_bounds(day: dt.date) -> tuple[dt.datetime, dt.datetime]:
    zone = ZoneInfo(_SESSION_TZ)
    opened = dt.datetime.combine(day, _SESSION_OPEN, tzinfo=zone)
    closed = dt.datetime.combine(day, session_close_et(day), tzinfo=zone)
    return opened.astimezone(dt.timezone.utc), closed.astimezone(dt.timezone.utc)


def deadline_instants(deadline: Deadline, *, as_of: dt.datetime, count: int) -> list[tuple[dt.datetime, dt.date]]:
    """The ``count`` most recent deadline instants at or before ``as_of``, newest
    first, each with its LOCAL date. For the slot kind the instant is the
    session close. Bounded walk: no deadline recurs less than weekly."""
    if deadline.kind not in {"daily", "weekly", "slots"}:
        return []
    zone = ZoneInfo(deadline.tz)
    local_today = as_of.astimezone(zone).date()
    out: list[tuple[dt.datetime, dt.date]] = []
    for back in range(0, count * 7 + 14):
        day = local_today - dt.timedelta(days=back)
        if day.weekday() not in deadline.weekdays:
            continue
        if deadline.trading_days_only and not is_trading_day(day):
            continue
        if deadline.kind == "slots":
            instant = _session_bounds(day)[1]
        else:
            instant = dt.datetime.combine(day, dt.time(deadline.hour, deadline.minute), tzinfo=zone).astimezone(
                dt.timezone.utc
            )
        if instant <= as_of:
            out.append((instant, day))
            if len(out) == count:
                break
    return out


def expected_trading_day(convention: str, local_date: dt.date) -> dt.date:
    """The session a cycle whose deadline falls on ``local_date`` describes."""
    if convention == "on_or_before":
        return latest_trading_day_on_or_before(local_date)
    if convention == "previous":
        return previous_trading_day(local_date)
    if convention == "on_or_after":
        return local_date if is_trading_day(local_date) else next_trading_day(local_date)
    raise ValueError(f"unknown session convention {convention!r}")


# ---------------------------------------------------------------------------
# Manifest reads.
# ---------------------------------------------------------------------------


def _store_relative(prefix: str) -> str:
    return prefix[len(_STORE_PREFIX):] if prefix.startswith(_STORE_PREFIX) else prefix


def _ulid_instant(key: str) -> dt.datetime | None:
    """A run_id ULID's creation instant (= the run's ``started``), from the key
    alone — so the intraday reader GETs only the runs near the session, not
    the ~400 a 5-minute unit records per day."""
    stem = key.rsplit("/", 1)[-1].removesuffix(".json")
    if len(stem) != 26:
        return None
    value = 0
    for char in stem[:10]:
        index = _ULID_ALPHABET.find(char.upper())
        if index < 0:
            return None
        value = value * 32 + index
    return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc)


def _parse_utc(stamp: object) -> dt.datetime | None:
    if not isinstance(stamp, str) or not stamp:
        return None
    try:
        parsed = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


class _Reader:
    """Lists one partition and reads its manifests, in parallel, recording every
    failure instead of swallowing it. One instance per run, so a partition two
    families share is read once."""

    def __init__(self, store: Any) -> None:
        self.store = store
        self.problems: list[str] = []
        self._docs: dict[str, dict | None] = {}
        self._listings: dict[str, list[str]] = {}
        # Create a lazily built boto3 client BEFORE the worker threads race to.
        getattr(store, "client", None)

    def list_partition(self, prefix: str) -> list[str] | None:
        if prefix not in self._listings:
            try:
                self._listings[prefix] = sorted(k for k in self.store.list_keys(prefix) if k.endswith(".json"))
            except Exception as exc:  # noqa: BLE001 - recorded, then the family is not written
                self.problems.append(f"could not list {prefix}: {type(exc).__name__}: {exc}")
                return None
        return self._listings[prefix]

    def read(self, keys: Iterable[str]) -> list[tuple[str, dict]]:
        wanted = [k for k in keys if k not in self._docs]
        if wanted:
            with concurrent.futures.ThreadPoolExecutor(max_workers=_READ_WORKERS) as pool:
                for key, read in zip(wanted, pool.map(lambda k: read_store_document(self.store, k), wanted)):
                    if read.problem is not None:
                        self.problems.append(f"{key}: {read.problem}")
                        self._docs[key] = None
                    elif read.absent:
                        self.problems.append(f"{key}: vanished between listing and read")
                        self._docs[key] = None
                    else:
                        self._docs[key] = read.document or {}
        return [(k, self._docs[k]) for k in keys if self._docs.get(k) is not None]  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Grading.
# ---------------------------------------------------------------------------


@dataclass
class UnitCycle:
    """One unit's answer for one cycle."""

    unit_id: str
    trading_day: dt.date
    deadline: dt.datetime
    freshness: str  # met | late | missing | as_of_mismatch | unreadable
    completeness: str  # met | no_run | no_cardinality_record | below_floor | excluded | unreadable
    detail: str = ""
    finished: str | None = None
    excluded_triggers: dict[str, int] = field(default_factory=dict)
    had_run: bool = False
    as_of_ok: bool = True


def _counted_ok(docs: list[tuple[str, dict]]) -> tuple[list[dict], dict[str, int]]:
    counted, excluded = [], {}
    for _key, doc in docs:
        if doc.get("status") != "ok":
            continue
        trigger = str(doc.get("trigger") or "")
        if trigger in COUNTED_TRIGGERS:
            counted.append(doc)
        else:
            excluded[trigger or "?"] = excluded.get(trigger or "?", 0) + 1
    return counted, excluded


def _completeness_verdict(unit: Unit, newest: dict | None) -> tuple[str, str]:
    block = unit.completeness or {}
    if str(block.get("status") or "") == "not_applicable":
        return "excluded", f"declared not_applicable ({block.get('na_code')})"
    if newest is None:
        return "no_run", "no ok run from a counted trigger for this cycle"
    records = [g for g in newest.get("guards") or [] if g.get("guard") == "data_cardinality"]
    if not records:
        return "no_cardinality_record", (
            f"run {newest.get('run_id')} recorded no data_cardinality guard, against the declared "
            f"floor {block.get('floor')} over {block.get('denominator')}"
        )
    unclean = [g for g in records if g.get("verdict") not in ("ok", "not_applicable")]
    if unclean:
        return "below_floor", "; ".join(
            f"{g.get('key') or '-'}: {g.get('verdict')} value={g.get('value')} floor={g.get('baseline')}" for g in unclean
        )[:400]
    return "met", f"{len(records)} cardinality record(s) clean"


def _grade_point(
    unit: Unit, docs: list[tuple[str, dict]], *, trading_day: dt.date, deadline: dt.datetime
) -> UnitCycle:
    counted, excluded = _counted_ok(docs)
    newest = max(counted, key=lambda d: str(d.get("finished") or ""), default=None)
    completeness, c_detail = _completeness_verdict(unit, newest)
    cycle = UnitCycle(
        unit_id=unit.unit_id,
        trading_day=trading_day,
        deadline=deadline,
        freshness="missing",
        completeness=completeness,
        excluded_triggers=excluded,
        had_run=bool(counted),
    )
    if not counted:
        failed = sum(1 for _, d in docs if d.get("status") == "failed")
        cycle.detail = f"no ok counted run in the partition ({len(docs)} manifest(s), {failed} failed)"
        return cycle
    wrong_day = [d for d in counted if str(d.get("trading_day") or "") != trading_day.isoformat()]
    if wrong_day:
        cycle.as_of_ok = False
        cycle.freshness = "as_of_mismatch"
        cycle.detail = (
            f"run {wrong_day[0].get('run_id')} is filed under {trading_day} but records "
            f"trading_day {wrong_day[0].get('trading_day')!r}"
        )
        return cycle
    on_time = [d for d in counted if (f := _parse_utc(d.get("finished"))) is not None and f <= deadline]
    if on_time:
        first = min(on_time, key=lambda d: str(d.get("finished")))
        cycle.freshness = "met"
        cycle.finished = str(first.get("finished"))
        cycle.detail = f"finished {cycle.finished}"
    else:
        cycle.freshness = "late"
        cycle.finished = str(newest.get("finished")) if newest else None
        cycle.detail = f"first ok run finished {min(str(d.get('finished')) for d in counted)}, after the deadline"
    if completeness != "met":
        cycle.detail += f"; completeness: {c_detail}"
    return cycle


def _grade_slots(
    unit: Unit, docs: list[tuple[str, dict]], *, trading_day: dt.date, deadline: Deadline
) -> UnitCycle:
    opened, closed = _session_bounds(trading_day)
    counted, excluded = _counted_ok(docs)
    finishes = sorted(f for d in counted if (f := _parse_utc(d.get("finished"))) is not None)
    step = dt.timedelta(minutes=deadline.slot_minutes)
    age = dt.timedelta(minutes=deadline.max_age_minutes)
    slots, fresh = 0, 0
    moment = opened + step
    while moment <= closed:
        slots += 1
        if any(moment - age <= f <= moment for f in finishes):
            fresh += 1
        moment += step
    share = fresh / slots if slots else 0.0
    newest = max(counted, key=lambda d: str(d.get("finished") or ""), default=None)
    completeness, c_detail = _completeness_verdict(unit, newest)
    wrong_day = [d for d in counted if str(d.get("trading_day") or "") != trading_day.isoformat()]
    cycle = UnitCycle(
        unit_id=unit.unit_id,
        trading_day=trading_day,
        deadline=closed,
        freshness="met" if share >= deadline.slot_share and not wrong_day else "late",
        completeness=completeness,
        excluded_triggers=excluded,
        had_run=bool(counted),
        as_of_ok=not wrong_day,
        detail=f"{fresh}/{slots} session slots fresh ({share:.1%}, needs >= {deadline.slot_share:.0%})",
    )
    if wrong_day:
        cycle.freshness = "as_of_mismatch"
    if not counted:
        cycle.freshness = "missing"
    if completeness != "met":
        cycle.detail += f"; completeness: {c_detail}"
    return cycle


@dataclass
class _UnitPlan:
    unit: Unit
    deadline: Deadline
    points: list[tuple[dt.datetime, dt.date]]


def _family_deadline_fallback(members: list[Unit]) -> str | None:
    declared = {str((u.raw.get("freshness") or {}).get("deadline") or "").strip() for u in members}
    declared.discard("")
    return next(iter(declared)) if len(declared) == 1 else None


def _grade_family(
    family: str, members: list[Unit], reader: _Reader, *, as_of: dt.datetime
) -> tuple[list[UnitCycle], dict[str, str], list[dt.date], int]:
    """Every graded unit's cycles, the units not graded (with why), the family's
    cycle trading days newest first, and how many read problems it hit."""
    convention = SESSION_CONVENTION.get(family)
    not_graded: dict[str, str] = {}
    plans: list[_UnitPlan] = []
    fallback = _family_deadline_fallback(members)
    for unit in members:
        cadence = unit_cadence(unit.raw)
        if cadence.kind in {"on_demand", "disabled", "undeclared"}:
            not_graded[unit.unit_id] = f"cadence {cadence.kind} ({cadence.source}): no scheduled cycle to grade"
            continue
        text = (unit.raw.get("freshness") or {}).get("deadline") or fallback
        deadline = parse_deadline(text, cadence, slot_minutes=unit.cadence_minutes)
        if deadline.kind == "undeclared":
            not_graded[unit.unit_id] = deadline.source
            continue
        plans.append(_UnitPlan(unit, deadline, deadline_instants(deadline, as_of=as_of, count=WINDOW)))
    if convention is None:
        return [], {**not_graded, "*": f"family {family!r} declares no session convention"}, [], 0
    days: set[dt.date] = set()
    for plan in plans:
        plan.points = [(instant, expected_trading_day(convention, local)) for instant, local in plan.points]
        days.update(td for _, td in plan.points)
    cycle_days = sorted(days, reverse=True)[:WINDOW]
    problems_before = len(reader.problems)
    results: list[UnitCycle] = []
    for plan in plans:
        base = f"{_store_relative(plan.unit.run_manifest_prefix)}/"
        for instant, trading_day in plan.points:
            if trading_day not in cycle_days:
                continue
            prefix = f"{base}{trading_day.isoformat()}/"
            keys = reader.list_partition(prefix)
            if keys is None:
                results.append(
                    UnitCycle(plan.unit.unit_id, trading_day, instant, "unreadable", "unreadable", f"could not list {prefix}")
                )
                continue
            if plan.deadline.kind == "slots":
                opened, closed = _session_bounds(trading_day)
                lead = dt.timedelta(minutes=plan.deadline.max_age_minutes)
                keys = [k for k in keys if (t := _ulid_instant(k)) is None or opened - lead <= t <= closed]
                docs = reader.read(keys)
                results.append(_grade_slots(plan.unit, docs, trading_day=trading_day, deadline=plan.deadline))
            else:
                docs = reader.read(keys)
                results.append(_grade_point(plan.unit, docs, trading_day=trading_day, deadline=instant))
    return results, not_graded, cycle_days, len(reader.problems) - problems_before


def _iso(instant: dt.datetime) -> str:
    return instant.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _family_documents(
    family: str,
    results: list[UnitCycle],
    not_graded: dict[str, str],
    cycle_days: list[dt.date],
    *,
    now: dt.datetime,
    code_sha: str | None,
) -> dict[str, dict]:
    graded_units = sorted({r.unit_id for r in results})
    by_day: dict[dt.date, list[UnitCycle]] = {}
    for r in results:
        by_day.setdefault(r.trading_day, []).append(r)
    excluded_triggers: dict[str, int] = {}
    for r in results:
        for trigger, n in r.excluded_triggers.items():
            excluded_triggers[trigger] = excluded_triggers.get(trigger, 0) + n

    fresh_rows, comp_rows = [], []
    fresh_met = asof_checked = asof_ok = comp_met = 0
    comp_excluded = sorted({r.unit_id for r in results if r.completeness == "excluded"})
    for day in cycle_days:
        rows = by_day.get(day, [])
        misses = [r for r in rows if r.freshness != "met"]
        met = bool(rows) and not misses
        fresh_met += met
        ran = [r for r in rows if r.had_run]
        if ran:
            asof_checked += 1
            asof_ok += all(r.as_of_ok for r in ran)
        fresh_rows.append(
            {
                "trading_day": day.isoformat(),
                "deadline_utc": _iso(max(r.deadline for r in rows)) if rows else None,
                "met": met,
                "misses": {r.unit_id: f"{r.freshness}: {r.detail}"[:300] for r in misses},
            }
        )
        graded = [r for r in rows if r.completeness != "excluded"]
        c_misses = [r for r in graded if r.completeness != "met"]
        c_met = bool(graded) and not c_misses
        comp_met += c_met
        comp_rows.append(
            {
                "trading_day": day.isoformat(),
                "met": c_met,
                "misses": {r.unit_id: r.completeness for r in c_misses},
            }
        )

    window_full = len(cycle_days) == WINDOW
    newest = _iso(max(r.deadline for r in results)) if results else _iso(now)
    common = {
        "schema_version": SLO_SCHEMA,
        "family": family,
        "generated_utc": _iso(now),
        "stale_after_utc": _iso(now + STALE_AFTER),
        "as_of": newest,
        "window_cycles": WINDOW,
        "cycles_graded": len(cycle_days),
        "graded_units": graded_units,
        "not_graded": dict(sorted(not_graded.items())),
        "counted_triggers": sorted(COUNTED_TRIGGERS),
        "excluded_trigger_runs": dict(sorted(excluded_triggers.items())),
        "source": "data_collection/runs/<unit>/<cycle trading day>/ (data_run_manifest.v1); never S3 HEAD",
        "producer": "data_gate.producers.slo",
        "code_sha": code_sha,
    }

    if not graded_units:
        why = f"no unit in the {family} family has a scheduled cycle and a declared deadline to grade"
        fresh_status, fresh_summary = "breach", why
    elif not window_full:
        fresh_status = "breach"
        fresh_summary = f"{fresh_met}/{len(cycle_days)} cycles met; only {len(cycle_days)} of {WINDOW} cycles exist"
    else:
        ok = fresh_met >= FRESHNESS_REQUIRED and asof_ok == asof_checked
        fresh_status = "ok" if ok else "breach"
        fresh_summary = (
            f"{fresh_met}/{WINDOW} cycles met the deadline (needs {FRESHNESS_REQUIRED}); "
            f"as_of matched on {asof_ok}/{asof_checked} cycles with a run (needs all)"
        )
    freshness = {
        **common,
        "objective": "freshness",
        "status": fresh_status,
        "value": fresh_met,
        "baseline": FRESHNESS_REQUIRED,
        "attainment": round(fresh_met / len(cycle_days), 4) if cycle_days else None,
        "as_of_matched": asof_ok,
        "as_of_checked": asof_checked,
        "as_of_source": (
            "the manifest's own trading_day against the cycle's derived session; a payload-level as_of "
            "is graded by data.<unit>.guard.pit, not here"
        ),
        "summary": fresh_summary,
        "cycles": fresh_rows,
    }

    comp_graded = [u for u in graded_units if u not in comp_excluded]
    if not comp_graded:
        comp_status = "breach"
        comp_summary = (
            f"no unit in the {family} family declares a completeness floor to grade "
            f"(declared exclusions: {comp_excluded or 'none'}); an objective with nothing measured is not met"
        )
    elif not window_full:
        comp_status = "breach"
        comp_summary = f"{comp_met}/{len(cycle_days)} cycles complete; only {len(cycle_days)} of {WINDOW} cycles exist"
    else:
        comp_status = "ok" if comp_met == WINDOW else "breach"
        comp_summary = f"{comp_met}/{WINDOW} cycles with every graded unit at its declared floor (needs all)"
    completeness = {
        **common,
        "objective": "completeness",
        "status": comp_status,
        "value": comp_met,
        "baseline": WINDOW,
        "attainment": round(comp_met / len(cycle_days), 4) if cycle_days else None,
        "declared_exclusions": comp_excluded,
        "summary": comp_summary,
        "cycles": comp_rows,
    }
    return {"freshness": freshness, "completeness": completeness}


def build_documents(
    store: Any, units: list[Unit], *, now: dt.datetime, code_sha: str | None = None
) -> tuple[dict[str, dict], dict[str, list[str]]]:
    """``({key: document}, {family: [problems]})``. A family with read problems
    contributes no documents — the caller must not write a verdict built on a
    partial read."""
    families: dict[str, list[Unit]] = {}
    for unit in units:
        if unit.freshness_family and not unit.retired:
            families.setdefault(unit.freshness_family, []).append(unit)
    reader = _Reader(store)
    documents: dict[str, dict] = {}
    problems: dict[str, list[str]] = {}
    for family in sorted(families):
        start = len(reader.problems)
        results, not_graded, days, n_problems = _grade_family(family, families[family], reader, as_of=now)
        if n_problems or any(r.freshness == "unreadable" for r in results):
            problems[family] = reader.problems[start:] or ["a partition could not be listed"]
            continue
        for objective, document in _family_documents(
            family, results, not_graded, days, now=now, code_sha=code_sha
        ).items():
            documents[slo_key(objective, family)] = document
    return documents, problems


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


class _StoreS3Adapter:
    """`write_run_record` speaks boto3's ``put_object``; route it through the
    store so the record lands beside the metric documents (and a local store
    or ``--dry-run`` behaves exactly like the documents do)."""

    def __init__(self, store: Any) -> None:
        self.store = store

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:  # noqa: N803 - boto3 shape
        del Bucket, ContentType
        self.store.put_bytes(_store_relative(Key), Body)


def _code_sha() -> str | None:
    try:
        from nousergon_lib.run_identity import resolve_code_sha  # noqa: PLC0415

        return resolve_code_sha()
    except Exception:  # noqa: BLE001 - provenance only; never blocks the measurement
        return None


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--store", default=DEFAULT_STORE, help="s3://bucket/prefix or a local directory")
    ap.add_argument(
        "--now",
        default=None,
        help="ISO-8601 UTC instant to grade AS OF (a replay); defaults to the current time",
    )
    ap.add_argument("--no-write", action="store_true", help="print the documents instead of writing them")
    args = ap.parse_args(argv)

    from data_gate.producers._run_record import RUN_RECORD_BUCKET, write_run_record  # noqa: PLC0415
    from data_gate.store import open_store  # noqa: PLC0415

    store = open_store(args.store, dry_run=args.no_write)
    started_at = dt.datetime.now(dt.timezone.utc)
    now = _parse_utc(args.now) if args.now else started_at
    if now is None:
        ap.error(f"--now {args.now!r} does not parse as ISO-8601")
    recorder = _StoreS3Adapter(store)
    try:
        documents, problems = build_documents(store, load_units(), now=now, code_sha=_code_sha())
    except Exception as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                recorder,
                bucket=RUN_RECORD_BUCKET,
                producer=PRODUCER,
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=f"{type(exc).__name__}: {exc}",
            )
        raise

    # Counts and verdicts only: this repository's Actions logs are public.
    for key, document in sorted(documents.items()):
        print(f"{key}: status={document['status']} value={document['value']}/{document['cycles_graded']}")
    for family, found in sorted(problems.items()):
        print(f"NOT WRITTEN {family}: {len(found)} read problem(s); first: {found[0][:200]}")

    if args.no_write:
        print(json.dumps(documents, indent=2, sort_keys=True))
        return 1 if problems else 0

    for key, document in sorted(documents.items()):
        store.put_bytes(key, json.dumps(document, sort_keys=True).encode("utf-8"))
    write_run_record(
        recorder,
        bucket=RUN_RECORD_BUCKET,
        producer=PRODUCER,
        status="error" if problems else "ok",
        started_at=started_at,
        finished_at=dt.datetime.now(dt.timezone.utc),
        error=(f"read problems in {sorted(problems)}" if problems else None),
        detail={
            "documents_written": len(documents),
            "families_unwritten": sorted(problems),
            "statuses": {k: d["status"] for k, d in sorted(documents.items())},
        },
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
