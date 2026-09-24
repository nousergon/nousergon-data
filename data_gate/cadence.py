"""When a unit was last DUE — the cadence every run-evidence reader grades against.

`alpha-engine-config-I10871`. `read_run_record` used to grade every unit against
the gate's own trading day, so a Saturday-weekly unit read UNMET four weekdays
in five and an on-demand unit read UNMET on every day nobody invoked it. The
phase-1 exit ("run_record 0 red for every non-retired unit") was unmeetable by
construction. The fix is not a per-unit exception list: it is one question,
asked the same way by every reader — *which execution of this unit should
exist by now?* — answered from what the unit DECLARES.

Three shapes, each derived from a declared field, never a hand list:

* **scheduled** — ``trigger.schedule`` parses (``cron(m h ? * DOW *)``,
  ``"Sat 05:00 America/New_York"``, ``"weekdays 16:45 America/New_York"``).
  The evidence is the most recent fire at or before the gate's moment, less a
  completion grace.
* **on_demand** — ``trigger.kind`` is ``manual`` or ``on-demand-dispatch``, OR
  ``trigger.on_demand`` is ``true``. The evidence is the most recent
  invocation, whenever it was; no invocation at all is a declared
  not-applicable, not a failure. The explicit flag exists because ``kind`` is
  also how ``scripts/gen_observability_rows.py`` derives SUBSTRATE, log
  location and alert channel (``SUBSTRATE_BY_TRIGGER_KIND``) — a GHA-hosted
  unit whose trigger is push/``workflow_dispatch``-only (D42) is genuinely
  on-demand for CADENCE purposes but must keep ``kind: github-actions`` for
  substrate purposes, so cadence declares it independently rather than
  overloading ``kind`` with a second meaning.
* **continuous** — ``trigger.cadence_minutes`` or a ``rate(...)`` schedule: runs
  at least daily, so the gate's own trading day is the right day.

A descriptor that declares none of these is **undeclared**, and that is named
in the reading rather than guessed from prose (``trigger.detail`` is free text
and is deliberately NOT parsed). An undeclared unit is graded against the gate
day — the strictest reading — so a missing declaration can never make a row
greener than it would be with one.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from nousergon_lib.trading_calendar import (  # pyright: ignore[reportAttributeAccessIssue]
    is_trading_day,
    previous_trading_day,
)

def latest_trading_day_on_or_before(calendar_date: dt.date) -> dt.date:
    """The trading day a reading taken on ``calendar_date`` is ABOUT.

    On a trading day that is the day itself: the board publishes at 23:30 UTC,
    after the close, so the day's own manifests exist by then. Only a weekend
    or a market holiday has no trading day of its own, and those resolve to the
    preceding one.

    `alpha-engine-config-I11191`. `data_gate read` previously defaulted to the
    bare UTC calendar date, so a weekend reading graded every unit whose cadence
    is neither `scheduled` nor `on_demand` against a `{run_manifest_prefix}/
    {trading_day}/` partition that cannot exist. Measured 2026-09-20 (a Sunday):
    D36 daily-news read UNMET -- "no run manifest for this trading day, under
    runs/D36/2026-09-20/" -- while holding a clean `ok` manifest from that same
    morning carrying `trading_day 2026-09-18`, `calendar_date 2026-09-20` and
    273 rows. The producer's mapping was right and the reader's was absent.

    Deliberately NOT `data_gate.report.previous_trading_day`, which is
    STRICTLY-before. That is correct for the daily report, which reports on a
    completed prior day; reusing it here would regress every weekday reading by
    one day and grade Friday's manifests against Thursday's partitions.

    Holiday-aware, because it defers to `nousergon_lib.trading_calendar` rather
    than stepping over Saturdays and Sundays only.
    """
    return calendar_date if is_trading_day(calendar_date) else previous_trading_day(calendar_date)


__all__ = [
    "COMPLETION_GRACE",
    "Cadence",
    "latest_due_fire",
    "latest_trading_day_on_or_before",
    "parse_cron",
    "unit_cadence",
]

#: The annotation, after the em dash of `trigger.schedule`, that marks a
#: trigger as not firing. Matched case-insensitively on the WORD so
#: "DISABLED live", "disabled until cutover" and "DISABLED (I1701)" all read
#: alike; a schedule that merely mentions the word inside a longer sentence
#: before the em dash is unaffected, because only the annotation is searched.
_DISABLED_ANNOTATION_RE = re.compile(r"\bDISABLED\b", re.IGNORECASE)

#: How long after a fire the run's manifest may legitimately still be absent.
#: The longest standalone collection (weekly: phase-one + alternative-phase-two
#: + RAG ingestion) is measured at roughly 3 hours end to end; 6 hours keeps a
#: running execution from reading as a missed one without letting a whole
#: missed cycle hide (the next-shortest cycle is a weekday, 24 hours).
COMPLETION_GRACE = dt.timedelta(hours=6)

#: EventBridge rules and Scheduler cron without a declared timezone are UTC.
_DEFAULT_CRON_TZ = "UTC"

_DOW = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
_ON_DEMAND_KINDS = frozenset({"manual", "on-demand-dispatch"})

_CRON = re.compile(r"^cron\((\d{1,2}) (\d{1,2}) \? \* ([A-Z,\-]+) \*\)$")
_NAMED_DAY = re.compile(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (\d{2}):(\d{2}) (\S+)$")
_WEEKDAYS = re.compile(r"^weekdays (\d{2}):(\d{2}) (\S+)$")
_RATE = re.compile(r"^rate\(")


@dataclass(frozen=True)
class Cadence:
    """What a unit declares about when it runs."""

    kind: str  # "scheduled" | "on_demand" | "continuous" | "undeclared" | "disabled"
    source: str
    weekdays: frozenset[int] = frozenset()
    hour: int = 0
    minute: int = 0
    tz: str = _DEFAULT_CRON_TZ
    #: A fire on a non-trading day is not due (the machine checks and exits).
    trading_days_only: bool = False


def _dow_set(field: str) -> frozenset[int]:
    days: set[int] = set()
    for part in field.split(","):
        if "-" in part:
            lo, hi = part.split("-", 1)
            days.update(range(_DOW[lo], _DOW[hi] + 1))
        else:
            days.add(_DOW[part])
    return frozenset(days)


def parse_cron(expression: str, *, tz: str | None, trading_days_only: bool) -> Cadence:
    """An EventBridge ``cron(m h ? * DOW *)``. Raises on any other shape.

    Only the day-of-week form every schedule in this fleet uses is accepted. A
    shape this does not understand is a code defect to fix here, not a row to
    grade by approximation — the raise is contained into one UNMEASURABLE row by
    `contain_clause_exceptions`.
    """
    match = _CRON.match(expression.strip())
    if not match:
        raise ValueError(f"unsupported cron expression {expression!r}")
    minute, hour, dow = match.groups()
    return Cadence(
        kind="scheduled",
        source=expression,
        weekdays=_dow_set(dow),
        hour=int(hour),
        minute=int(minute),
        tz=tz or _DEFAULT_CRON_TZ,
        trading_days_only=trading_days_only,
    )


def unit_cadence(raw: dict) -> Cadence:
    """The cadence a unit descriptor declares — see the module docstring."""
    trigger = raw.get("trigger") or {}
    kind = str(trigger.get("kind") or "")
    if trigger.get("on_demand") is True:
        return Cadence(kind="on_demand", source=f"trigger.on_demand=true (kind={kind or '?'})")
    if kind in _ON_DEMAND_KINDS:
        return Cadence(kind="on_demand", source=f"trigger.kind={kind}")
    if trigger.get("cadence_minutes"):
        return Cadence(kind="continuous", source=f"trigger.cadence_minutes={trigger['cadence_minutes']}")
    raw_schedule = str(trigger.get("schedule") or "")
    text, _, annotation = raw_schedule.partition(" — ")
    text = text.strip()

    # A DECLARED-DISABLED trigger (alpha-engine-config-I11194).
    #
    # Two descriptors record their live state in the annotation after the em
    # dash: D33 `"cron(0 9 ? * MON-FRI *) — DISABLED live"` and D38
    # `"rate(15 minutes) — DISABLED live"`. Both were verified DISABLED on the
    # live EventBridge rule / Scheduler entry, 2026-09-20.
    #
    # This parser used to `.split(" — ")[0]` and throw the annotation away, so
    # it computed a due instant from a trigger that does not fire, found no
    # manifest, and reported the unit as "either did not execute or executed
    # without recording itself". It executed NEITHER way: it was not asked to.
    # The descriptor held the fact that would have made the clause correct and
    # the reader discarded it before looking.
    #
    # Graded in `clauses.py` as its own state, never MET: a unit that is off is
    # not healthy, it is off (`observability-policy` §8.3 DISABLED).
    if _DISABLED_ANNOTATION_RE.search(annotation):
        return Cadence(
            kind="disabled",
            source=f"trigger.schedule={raw_schedule.strip()!r}",
        )
    if not text:
        return Cadence(
            kind="undeclared",
            source=(
                f"trigger.kind={kind or '?'} declares no machine-readable trigger.schedule or "
                "trigger.cadence_minutes"
            ),
        )
    if _RATE.match(text):
        return Cadence(kind="continuous", source=f"trigger.schedule={text}")
    if text.startswith("cron("):
        cron = parse_cron(text, tz=None, trading_days_only=False)
        weekday_only = cron.weekdays <= frozenset(range(5))
        return Cadence(**{**cron.__dict__, "source": f"trigger.schedule={text}", "trading_days_only": weekday_only})
    named = _NAMED_DAY.match(text)
    if named:
        day, hh, mm, tz = named.groups()
        weekdays = frozenset({_DOW[day.upper()]})
    else:
        every = _WEEKDAYS.match(text)
        if not every:
            return Cadence(kind="undeclared", source=f"trigger.schedule={text!r} is not a recognised shape")
        hh, mm, tz = every.groups()
        weekdays = frozenset(range(5))
    return Cadence(
        kind="scheduled",
        source=f"trigger.schedule={text}",
        weekdays=weekdays,
        hour=int(hh),
        minute=int(mm),
        tz=tz,
        # A weekday-only schedule is a trading-day schedule: every weekday
        # machine in this fleet checks the NYSE calendar and exits on a holiday
        # (`require_trading_day: true`). A weekend schedule runs regardless.
        trading_days_only=weekdays <= frozenset(range(5)),
    )


def gate_moment(trading_day: dt.date, now: dt.datetime | None = None) -> dt.datetime:
    """The instant a reading for ``trading_day`` is taken AS OF.

    The end of that day in New York, or now if that is earlier — a gate reading
    today cannot demand a run scheduled for tonight, and a gate re-reading a past
    day must not be graded against runs that happened after it.
    """
    end = dt.datetime.combine(trading_day, dt.time(23, 59, 59), tzinfo=ZoneInfo("America/New_York"))
    moment = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    return min(moment, end.astimezone(dt.timezone.utc))


def latest_due_fire(
    cadence: Cadence, *, as_of: dt.datetime, grace: dt.timedelta = COMPLETION_GRACE
) -> dt.datetime:
    """The most recent fire of a scheduled cadence whose run should be finished by ``as_of``.

    Walks back day by day in the schedule's own timezone (DST-correct), skipping
    days the schedule does not fire and — for a trading-day schedule — NYSE
    holidays. Bounded at 21 days: no schedule here fires less than weekly.
    """
    if cadence.kind != "scheduled":
        raise ValueError(f"latest_due_fire needs a scheduled cadence, got {cadence.kind}")
    zone = ZoneInfo(cadence.tz)
    local = as_of.astimezone(zone)
    for back in range(0, 22):
        day = local.date() - dt.timedelta(days=back)
        if day.weekday() not in cadence.weekdays:
            continue
        if cadence.trading_days_only and not is_trading_day(day):
            continue
        fire = dt.datetime.combine(day, dt.time(cadence.hour, cadence.minute), tzinfo=zone)
        if fire + grace <= as_of:
            return fire.astimezone(dt.timezone.utc)
    raise ValueError(f"no due fire of {cadence.source} within 21 days of {as_of.isoformat()}")
