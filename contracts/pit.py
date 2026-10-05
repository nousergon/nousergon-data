"""Point-in-time stamps: ``as_of`` and ``available_at`` on every published record.

`data_collection_plan_260914.md` §2 objective 4 and plan item P-15
(`alpha-engine-config-I10782`):

    Every published record carries ``as_of`` (the trading day it describes) and
    ``available_at`` (UTC time the producer could first have known it), and
    ``available_at <= manifest.finished_at``.

This module is the ONE definition of those two fields, used from three places:

* **the producer** — :func:`stamp` puts both fields on a payload, and
  :func:`pit_guard_entry` turns the payload it just wrote into one
  ``guards[]`` entry for its run manifest. A collector returns that entry in its
  result's ``guards`` list, and ``weekly_collector._record_collector_guards``
  (the generic hook every collector's guard readings already go through) folds
  it onto the manifest. Nothing in the shared write path has to learn about it.
* **the gate** — ``data_gate.evidence.read_pit`` reads those entries back off
  the unit's run manifests and grades ``as_of <= available_at <= finished`` for
  every output the run published (the ``data.<unit>.guard.pit`` clause).
* **the contracts** — :data:`PIT_PROPERTIES` is the JSON Schema both fields
  take in a ``contracts/*.schema.json``, and
  ``tests/test_pit_contract.py`` holds every schema that declares either field
  to exactly that shape.

**What the two fields mean, precisely.**

``as_of``
    The trading day the record DESCRIBES, as an ISO date on the NYSE calendar
    (``America/New_York``). Not the day it was written: the 07:30 ET morning run
    describes T-1, and the Saturday weekly run describes Friday.

``available_at``
    The UTC instant (RFC 3339, ``Z`` suffix) at which the producer could FIRST
    have known the record: when the value was read from its source, not when a
    later job re-wrote it. A re-publication of an unchanged record keeps its
    original ``available_at``. It can never precede the start of the ``as_of``
    day (a record cannot be known before the day it describes begins), and it
    can never follow the ``finished`` stamp of the run that published it.

**The manifest entry** (``data_run_manifest.v1``'s closed ``GuardVerdict``
shape, so no schema change in ``nousergon-lib`` is needed to carry it):

* ``guard`` — :data:`PIT_GUARD_NAME`;
* ``key`` — the published key the stamps describe;
* ``value`` — ``available_at`` as POSIX seconds, UTC;
* ``baseline`` — the start of the ``as_of`` day (00:00 ``America/New_York``)
  as POSIX seconds: the bound ``value`` is graded against from below;
* ``verdict`` — ``ok`` when both stamps were present and consistent,
  ``unmeasurable`` when either was missing, unparseable or contradictory (the
  guard could not establish when the record became knowable, which is red and
  never a pass), ``not_applicable`` for a key :func:`pit_exemption` exempts.

Both numbers are machine-readable on purpose: the gate grades from them and
never parses ``detail``, which repeats the stamps for a human.

**OBSERVE mode** (`sf-pipeline-policy` §7a). Recording a stamp never fails a
run; the board grades it. Promotion to enforcing is a separate, deliberate
change with its own `Re-exam:` on `alpha-engine-config-I10782`.
"""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

__all__ = [
    "AS_OF",
    "AVAILABLE_AT",
    "PIT_GUARD_MODE",
    "PIT_GUARD_NAME",
    "PIT_PROPERTIES",
    "PitStamp",
    "as_of_floor",
    "format_available_at",
    "parse_as_of",
    "parse_available_at",
    "pit_exemption",
    "pit_guard_entry",
    "read_pit_entry",
    "stamp",
]

AS_OF = "as_of"
AVAILABLE_AT = "available_at"

#: The manifest ``guards[].guard`` name. ``data_`` prefixed like
#: ``data_empty_fresh`` / ``data_cardinality``, so the recorded vocabulary
#: stays one namespace.
PIT_GUARD_NAME = "data_pit"
PIT_GUARD_MODE = "observe"

#: The trading calendar's own zone: ``as_of`` is a NYSE session date.
MARKET_TZ = ZoneInfo("America/New_York")

#: The JSON Schema each field takes in a contract. ``available_at`` is a run
#: timestamp, so it is declared provenance: two correct re-productions of the
#: same record carry different ones, and shadow parity must not grade that as a
#: data breach (`tests/test_declared_contracts_are_reachable_from_their_keys.py`).
PIT_PROPERTIES: dict[str, dict[str, Any]] = {
    AS_OF: {
        "type": "string",
        "format": "date",
        "description": (
            "Point-in-time: the trading day this record describes (ISO date, NYSE calendar). "
            "data_collection_plan_260914.md §2 objective 4; contracts/pit.py."
        ),
    },
    AVAILABLE_AT: {
        "type": "string",
        "format": "date-time",
        "pattern": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z$",
        "description": (
            "Point-in-time: the UTC instant the producer could first have known this record "
            "(RFC 3339, Z suffix). Never before the as_of day begins, never after the publishing "
            "run's manifest `finished`. contracts/pit.py."
        ),
        "x-provenance": True,
    },
}

#: Published-key prefixes whose point-in-time property is held somewhere other
#: than a per-record stamp, each with the reason the board shows. Plan §2
#: objective 4, verbatim: "ArcticDB appends keep `UniverseFreshnessViolation`
#: enforcing. v2's `crucible/data/point_in_time.py` stays the consumer-side
#: check." A library ref is not an object with a body to stamp.
_EXEMPT_PREFIXES: tuple[tuple[str, str], ...] = (
    (
        "arcticdb/",
        "an ArcticDB library, not a stamped object: point-in-time is enforced at append by "
        "builders/daily_append.py::UniverseFreshnessViolation and read-side by crucible's "
        "crucible/data/point_in_time.py (plan §2 objective 4)",
    ),
)


def pit_exemption(key: str) -> str | None:
    """Why ``key`` carries no per-record stamp, or ``None`` when it must carry one."""
    for prefix, reason in _EXEMPT_PREFIXES:
        if str(key).startswith(prefix):
            return reason
    return None


def format_available_at(moment: dt.datetime) -> str:
    """``moment`` as the RFC 3339 UTC ``Z`` form :data:`PIT_PROPERTIES` requires."""
    if moment.tzinfo is None:
        raise ValueError("available_at must be timezone-aware; a naive time has no instant")
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_as_of(value: object) -> dt.date | None:
    """An ``as_of`` value as a date, or ``None`` when it is not an ISO date."""
    if isinstance(value, dt.datetime):
        return None
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError:
        return None


def parse_available_at(value: object) -> dt.datetime | None:
    """An ``available_at`` value as an aware UTC datetime, or ``None``.

    Only the ``Z``-suffixed form is accepted: an offset-less stamp is a guess
    about which clock wrote it, which is the exact ambiguity the field exists
    to remove.
    """
    text = str(value) if value is not None else ""
    if not text.endswith("Z"):
        return None
    try:
        parsed = dt.datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError:
        return None
    return parsed.astimezone(dt.timezone.utc)


def as_of_floor(as_of: dt.date) -> dt.datetime:
    """The first instant of the ``as_of`` session day: 00:00 ``America/New_York``, in UTC."""
    return dt.datetime(as_of.year, as_of.month, as_of.day, tzinfo=MARKET_TZ).astimezone(dt.timezone.utc)


def stamp(
    payload: Mapping[str, Any],
    *,
    as_of: dt.date | str,
    available_at: dt.datetime | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """A copy of ``payload`` carrying both point-in-time fields.

    ``available_at`` defaults to ``now`` (or the wall clock): call this when
    the source was READ, not when the object is about to be written, if those
    differ. Refuses to overwrite an existing stamp that disagrees: a payload
    that already says it describes another day, or was knowable at another
    time, is a producer bug to surface, not to paper over.
    """
    day = parse_as_of(as_of)
    if day is None:
        raise ValueError(f"as_of must be an ISO date, got {as_of!r}")
    moment = available_at or now or dt.datetime.now(dt.timezone.utc)
    if moment.tzinfo is None:
        raise ValueError("available_at must be timezone-aware")
    if moment < as_of_floor(day):
        raise ValueError(
            f"available_at {format_available_at(moment)} precedes the as_of day {day.isoformat()}: "
            "a record cannot be known before the day it describes begins"
        )
    out = dict(payload)
    for field, new in ((AS_OF, day.isoformat()), (AVAILABLE_AT, format_available_at(moment))):
        old = out.get(field)
        if old is not None and str(old) != new:
            raise ValueError(f"payload already carries {field}={old!r}; refusing to restamp it as {new!r}")
        out[field] = new
    return out


def _entry(key: str, verdict: str, detail: str, value: float | None, baseline: float | None) -> dict[str, Any]:
    return {
        "guard": PIT_GUARD_NAME,
        "mode": PIT_GUARD_MODE,
        "verdict": verdict,
        "detail": detail[:2000],
        "key": key,
        "value": value,
        "baseline": baseline,
    }


def pit_guard_entry(key: str, document: Mapping[str, Any] | None) -> dict[str, Any]:
    """The manifest ``guards[]`` entry for the stamps ``document`` carries.

    ``document`` is the payload the producer just wrote at ``key`` (for a row
    contract, the row whose stamps stand for the object). Shaped for
    ``weekly_collector._record_collector_guards``: return it in a collector
    result's ``guards`` list.
    """
    exempt = pit_exemption(key)
    if exempt is not None:
        return _entry(key, "not_applicable", f"{key}: {exempt}", None, None)
    doc = document if isinstance(document, Mapping) else {}
    missing = [f for f in (AS_OF, AVAILABLE_AT) if doc.get(f) in (None, "")]
    if missing:
        return _entry(
            key,
            "unmeasurable",
            f"{key} carries no {' or '.join(missing)}: when this record became knowable is not "
            "recorded, so it cannot be graded point-in-time (plan §2 objective 4)",
            None,
            None,
        )
    day = parse_as_of(doc[AS_OF])
    moment = parse_available_at(doc[AVAILABLE_AT])
    if day is None or moment is None:
        return _entry(
            key,
            "unmeasurable",
            f"{key} carries as_of={doc[AS_OF]!r} available_at={doc[AVAILABLE_AT]!r}, which do not "
            "parse as an ISO date and an RFC 3339 UTC `Z` instant",
            None,
            None,
        )
    floor = as_of_floor(day)
    value, baseline = moment.timestamp(), floor.timestamp()
    stamps = f"as_of={day.isoformat()} available_at={format_available_at(moment)}"
    if moment < floor:
        return _entry(
            key,
            "unmeasurable",
            f"{key}: {stamps} — available_at precedes the start of the day the record describes, "
            "so the stamps contradict each other and neither can be trusted",
            value,
            baseline,
        )
    return _entry(key, "ok", f"{key}: {stamps}", value, baseline)


@dataclass(frozen=True)
class PitStamp:
    """One manifest entry read back: what the gate grades."""

    key: str
    verdict: str
    available_at: dt.datetime | None
    as_of_floor: dt.datetime | None
    detail: str


def _seconds(value: object) -> dt.datetime | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)


def read_pit_entry(entry: Mapping[str, Any]) -> PitStamp | None:
    """A manifest ``guards[]`` entry as a :class:`PitStamp`, or ``None`` if it is not one."""
    if str(entry.get("guard") or "") != PIT_GUARD_NAME or not entry.get("key"):
        return None
    return PitStamp(
        key=str(entry["key"]),
        verdict=str(entry.get("verdict") or ""),
        available_at=_seconds(entry.get("value")),
        as_of_floor=_seconds(entry.get("baseline")),
        detail=str(entry.get("detail") or ""),
    )
