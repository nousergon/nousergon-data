"""Decision-time and vintage contracts: the ``x-pit`` declaration on a contract schema.

`alpha-engine-config-I12567` (audit DATA-01, `nous-ergon-ops/audits/2026-10-22-core-ne`),
SYSTEM_OPTIMIZED §4.1: *separate event, first availability, ingestion and revision;
replay exact decision-time vintages.*

`contracts/pit.py` gives every published record two stamps, ``as_of`` and
``available_at``, and its ``available_at`` defaults to *when the collector read
the source*. That is one of four times, and for a revised series (FRED) or a
filing-dated dataset (EDGAR) it is not the one a replay needs. This module is
the one definition of the other two, declared per contract so a consumer and
the gate read the same thing:

* **event time** — what the record describes (a session, an observation date,
  a filing's knowledge date). The ``event_time`` field.
* **first availability** — when the record could first have been known, and
  ON WHAT BASIS that time is asserted (``available_at_basis``): the source's
  own timestamp, a source-published date rule, a publication calendar, the
  collection clock, or ``unknown``. A collection-time basis is honest but
  weak: it is an upper bound on knowability, never evidence of when the
  source published.
* **ingestion** — the run that wrote it. Not declared here: the run manifest
  (``started``/``finished``) is already the record of it.
* **revision** — whether a later correction of the same record is kept as a
  new vintage (``preserved`` / ``superseding``, which need a ``vintage_field``),
  overwrites it in place (``overwritten``), or cannot happen (``final``).

**The declaration** is a schema-level annotation, so it is inert to
``jsonschema`` validation and to consumer shape pins
(`data_gate/consumer_pins.py::schema_shape` drops every ``x-`` key)::

    "x-pit": {
      "event_time": "knowledge_date",
      "available_at_field": "knowledge_date",
      "available_at_basis": "source_date_rule",
      "admissible_when": "available_before_session",
      "vintage_field": "latest_accession",
      "revisions": "superseding",
      "rule": "...one sentence: why the basis holds...",
      "tracker": "alpha-engine-config-I12567"
    }

**Replay** (:func:`admit`): a consumer deciding at instant T admits a row only
when its availability is KNOWN and on or before T; a row with unknown
availability is excluded and COUNTED, never guessed in; and where revisions are
kept, only the newest vintage available by T stands for its record. So a later
correction cannot change what a replay at T admits — the closes-when of
I12567 ("replay at T is unchanged by later corrections").

**OBSERVE mode.** The gate (`data_gate.evidence.read_pit`) enforces a
declaration that is PRESENT — one that names a field its own schema does not
define, or claims revisions are kept without naming the vintage field, fails
the `guard.pit` clause — and reports a contract with NO declaration as an
observation, not a failure, until each producer carries one. The register
:data:`PENDING_DECLARATIONS` names every stamped contract still without one and
why; it only shrinks.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

__all__ = [
    "ADMISSIBLE_WHEN",
    "AVAILABILITY_BASES",
    "DECLARATION_KEY",
    "PENDING_DECLARATIONS",
    "REVISION_POLICIES",
    "UNCONTRACTED_PIT_DATASETS",
    "VINTAGE_MODE",
    "Admission",
    "VintageDeclaration",
    "admit",
    "declaration_errors",
    "load_declaration",
    "replay_supported",
]

#: The schema-level annotation key.
DECLARATION_KEY = "x-pit"

#: How this module's findings act today (`sf-pipeline-policy` §7a): the gate
#: grades a present declaration; an absent one is reported, not failed.
VINTAGE_MODE = "observe"

MARKET_TZ = ZoneInfo("America/New_York")

#: On what basis a record's first-availability time is asserted, strongest first.
AVAILABILITY_BASES: dict[str, str] = {
    "source_timestamp": "the source's own instant for the record (e.g. an SEC acceptance datetime)",
    "source_date_rule": "a date the source publishes per record, plus a documented cutoff rule",
    "publication_calendar": "a published release schedule; the per-record publication time is not observed",
    "collection_time": "when the collector read the source: an upper bound on knowability, not a publication time",
    "unknown": "not established; every row is excluded from replay and counted",
}

#: What happens when the source corrects a record already published.
REVISION_POLICIES: dict[str, str] = {
    "preserved": "every vintage is kept side by side; vintage_field names it",
    "superseding": "a correction is a new row from its own availability on; vintage_field names it",
    "overwritten": "a correction replaces the earlier value in place; replay at a past T is not reproducible",
    "final": "the source does not revise a published record",
}

#: How :func:`admit` compares availability with the decision.
ADMISSIBLE_WHEN: dict[str, str] = {
    "available_at_or_before_decision": "available_at <= the decision instant T",
    "available_before_session": "the availability DATE is strictly before the decision session S",
}

#: Stamped contracts (`contracts/pit.py` ``as_of``) that carry no ``x-pit`` yet,
#: each with the reason. Shrink-only: `tests/test_vintage_contract.py` fails on
#: a stamped contract missing from both this register and a declaration, and
#: on an entry whose contract has since declared one.
PENDING_DECLARATIONS: dict[str, str] = {
    "universe_classification.schema.json": (
        "written by the Saturday weekly (D07); held behind the 2026-10-24 weekly-path freeze, "
        "declared in the next slice"
    ),
    "metron_valuation_medians.schema.json": (
        "written by the Saturday weekly (D11); held behind the 2026-10-24 weekly-path freeze, "
        "declared in the next slice"
    ),
    "arctic_universe.schema.json": (
        "ArcticDB library rows, exempt from per-record stamps (contracts/pit.py::pit_exemption); "
        "their availability is enforced at append by UniverseFreshnessViolation. A declaration "
        "belongs with the append's revision policy (ArcticDB versions), next slice"
    ),
}

#: PIT datasets the audit names that publish NO contract schema yet, so there
#: is nowhere to declare ``x-pit``. Counted, not hidden: each is a contract
#: the next slice owes. The availability basis is the one the collector's own
#: docstring states, and none is ``source_timestamp`` today.
UNCONTRACTED_PIT_DATASETS: dict[str, dict[str, str]] = {
    "collectors/fred_history.py": {
        "basis": "collection_time",
        "revisions": "overwritten",
        "gap": "requests observation_start/end only, no realtime_start/realtime_end (ALFRED vintages)",
    },
    "collectors/fails_to_deliver_pit.py": {
        "basis": "publication_calendar",
        "revisions": "overwritten",
        "gap": "known_at is calendar-derived (7th / 22nd of the next month); the publication instant is unobserved",
    },
    "collectors/short_volume_pit.py": {
        "basis": "publication_calendar",
        "revisions": "final",
        "gap": "FINRA's 6 pm ET same-day publication rule, not an observed instant",
    },
    "collectors/insider_transactions_pit.py": {
        "basis": "source_date_rule",
        "revisions": "superseding",
        "gap": "the quarterly data sets carry filing_date, not the acceptance instant",
    },
    "collectors/holdings_13f_pit.py": {
        "basis": "source_date_rule",
        "revisions": "superseding",
        "gap": "amendments supersede by amendment_type; no contract names the vintage field",
    },
    "collectors/filing_8k_items_pit.py": {
        "basis": "source_timestamp",
        "revisions": "final",
        "gap": "acceptance_datetime is kept but no contract declares it as available_at",
    },
    "collectors/filing_text_pit.py": {
        "basis": "source_date_rule",
        "revisions": "final",
        "gap": "filing date with no acceptance time",
    },
    "collectors/wiki_pageviews_pit.py": {
        "basis": "publication_calendar",
        "revisions": "final",
        "gap": "known_at = view_date + 2 days by rule",
    },
}

_REQUIRED = ("event_time", "available_at_basis", "admissible_when", "revisions", "rule")
_FIELD_KEYS = ("event_time", "available_at_field", "vintage_field")
_KNOWN_KEYS = frozenset(_REQUIRED + _FIELD_KEYS + ("tracker",))


@dataclass(frozen=True)
class VintageDeclaration:
    """One contract's ``x-pit`` declaration, parsed."""

    event_time: str
    available_at_basis: str
    admissible_when: str
    revisions: str
    rule: str
    available_at_field: str | None = None
    vintage_field: str | None = None
    tracker: str = ""


def _row_properties(schema: Mapping[str, Any]) -> dict[str, Any]:
    """The properties a declared field may name: the schema's own, or its items'."""
    props = dict(schema.get("properties") or {})
    items = schema.get("items")
    if isinstance(items, Mapping):
        props.update(items.get("properties") or {})
    return props


def declaration_errors(schema: Mapping[str, Any]) -> list[str]:
    """Why ``schema``'s ``x-pit`` cannot be trusted; empty when it is sound or absent.

    A declaration is checked against ITS OWN schema: every field it names must
    be a property the schema defines, and a revision policy that keeps
    vintages must name the field that tells them apart.
    """
    raw = schema.get(DECLARATION_KEY)
    if raw is None:
        return []
    if not isinstance(raw, Mapping):
        return [f"{DECLARATION_KEY} must be an object, got {type(raw).__name__}"]
    errors = [f"{DECLARATION_KEY}.{k} is required" for k in _REQUIRED if raw.get(k) in (None, "")]
    errors += [f"{DECLARATION_KEY}.{k} is not a declaration key" for k in sorted(set(raw) - _KNOWN_KEYS)]
    for key, vocabulary in (
        ("available_at_basis", AVAILABILITY_BASES),
        ("revisions", REVISION_POLICIES),
        ("admissible_when", ADMISSIBLE_WHEN),
    ):
        value = raw.get(key)
        if value not in (None, "") and value not in vocabulary:
            errors.append(f"{DECLARATION_KEY}.{key}={value!r} is not one of {sorted(vocabulary)}")
    props = _row_properties(schema)
    for key in _FIELD_KEYS:
        name = raw.get(key)
        if name not in (None, "") and name not in props:
            errors.append(f"{DECLARATION_KEY}.{key}={name!r} names no property of this schema")
    if raw.get("revisions") in ("preserved", "superseding") and not raw.get("vintage_field"):
        errors.append(f"{DECLARATION_KEY}.revisions={raw['revisions']!r} keeps vintages, so vintage_field is required")
    basis = raw.get("available_at_basis")
    if basis not in (None, "", "collection_time", "unknown") and not raw.get("available_at_field"):
        errors.append(
            f"{DECLARATION_KEY}.available_at_basis={basis!r} asserts a per-record source time, so "
            "available_at_field is required"
        )
    return errors


def load_declaration(schema: Mapping[str, Any]) -> VintageDeclaration | None:
    """``schema``'s declaration, ``None`` when it has none; raises on an unsound one."""
    if schema.get(DECLARATION_KEY) is None:
        return None
    errors = declaration_errors(schema)
    if errors:
        raise ValueError("; ".join(errors))
    raw = schema[DECLARATION_KEY]
    return VintageDeclaration(
        event_time=str(raw["event_time"]),
        available_at_basis=str(raw["available_at_basis"]),
        admissible_when=str(raw["admissible_when"]),
        revisions=str(raw["revisions"]),
        rule=str(raw["rule"]),
        available_at_field=raw.get("available_at_field") or None,
        vintage_field=raw.get("vintage_field") or None,
        tracker=str(raw.get("tracker") or ""),
    )


def replay_supported(declaration: VintageDeclaration) -> bool:
    """Whether a replay at a past T reproduces what was knowable then.

    Not when corrections overwrite in place, and not when availability is
    unknown: both are the gaps I12567 says must be explicit, never assumed.
    """
    return declaration.revisions != "overwritten" and declaration.available_at_basis != "unknown"


@dataclass(frozen=True)
class Admission:
    """What a replay at one decision admitted, and what it left out and why."""

    admitted: tuple[Mapping[str, Any], ...]
    unknown_availability: int = 0
    not_yet_available: int = 0
    superseded: int = 0
    reasons: tuple[str, ...] = field(default_factory=tuple)


def _as_date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.astimezone(MARKET_TZ).date() if value.tzinfo else None
    if isinstance(value, dt.date):
        return value
    text = str(value or "")
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def _as_instant(value: object) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.timezone.utc) if value.tzinfo else None
    if isinstance(value, dt.date):
        # A bare date's first knowable instant: its own start in New York.
        return dt.datetime(value.year, value.month, value.day, tzinfo=MARKET_TZ).astimezone(dt.timezone.utc)
    text = str(value or "")
    if not text:
        return None
    if not text.endswith("Z"):
        # An offset-less or non-UTC instant is a guess about which clock wrote
        # it (contracts/pit.py's rule); a bare ISO date is its own first instant.
        if "T" in text:
            return None
        day = _as_date(text)
        return _as_instant(day) if day is not None else None
    try:
        return dt.datetime.fromisoformat(text[:-1] + "+00:00").astimezone(dt.timezone.utc)
    except ValueError:
        return None


def admit(
    rows: Iterable[Mapping[str, Any]],
    declaration: VintageDeclaration,
    *,
    decision: dt.datetime,
    record_key: Callable[[Mapping[str, Any]], Any] | Sequence[str] | None = None,
    stamp_available_at: dt.datetime | None = None,
) -> Admission:
    """The rows a consumer deciding at ``decision`` may use, per ``declaration``.

    ``decision`` is an aware instant. For ``available_before_session`` the
    decision session is its New York date, and a row is admissible only when
    its availability date is strictly earlier.

    ``stamp_available_at`` is the publishing run's ``available_at``
    (`contracts/pit.py`) for a contract whose availability is not per row
    (``available_at_field`` absent); without it such rows are UNKNOWN, never
    admitted. ``record_key`` (field names or a function) identifies one
    record across its vintages; when revisions are kept, only the newest
    vintage available by ``decision`` is admitted per record.
    """
    if decision.tzinfo is None:
        raise ValueError("decision must be timezone-aware")
    session = decision.astimezone(MARKET_TZ).date()
    unknown = later = 0
    candidates: list[tuple[Mapping[str, Any], dt.datetime]] = []
    for row in rows:
        if declaration.available_at_basis == "unknown":
            unknown += 1
            continue
        raw = row.get(declaration.available_at_field) if declaration.available_at_field else stamp_available_at
        if declaration.admissible_when == "available_before_session":
            day = _as_date(raw)
            if day is None:
                unknown += 1
                continue
            if not day < session:
                later += 1
                continue
            when = _as_instant(day)
        else:
            when = _as_instant(raw)
            if when is None:
                unknown += 1
                continue
            if when > decision:
                later += 1
                continue
        candidates.append((row, when))

    superseded = 0
    if declaration.vintage_field and declaration.revisions in ("preserved", "superseding") and record_key:
        key_of = record_key if callable(record_key) else (lambda r, f=tuple(record_key): tuple(r.get(x) for x in f))
        newest: dict[Any, tuple[Mapping[str, Any], dt.datetime]] = {}
        for row, when in candidates:
            k = key_of(row)
            held = newest.get(k)
            rank = (when, str(row.get(declaration.vintage_field) or ""))
            if held is None or rank > (held[1], str(held[0].get(declaration.vintage_field) or "")):
                newest[k] = (row, when)
        superseded = len(candidates) - len(newest)
        admitted = tuple(r for r, _ in sorted(newest.values(), key=lambda p: (p[1], str(key_of(p[0])))))
    else:
        admitted = tuple(r for r, _ in candidates)

    reasons = []
    if unknown:
        reasons.append(f"{unknown} row(s) excluded: availability unknown ({declaration.available_at_basis})")
    if later:
        reasons.append(f"{later} row(s) excluded: not yet available at {decision.isoformat()}")
    if superseded:
        reasons.append(f"{superseded} older vintage(s) superseded by a correction available by the decision")
    return Admission(admitted, unknown, later, superseded, tuple(reasons))
