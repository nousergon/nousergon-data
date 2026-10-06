"""The `console_entity` base-column reader — what replaced the phase-0 stub.

`alpha-engine-config-I10795` (audit gap A3, part 2; part 1 is
`data_gate/detector_readers.py`). The requirement, read literally: the unit is
reachable on the console as a Component **by name, by structure and by
relation** (`console-policy` §3.1), and it **never renders green when it has
nothing to say** (`observability-policy` §8.3: UNREPORTED vs HEALTHY, never
collapsed). A registry row on disk, a descriptor that names a component, or the
console's own config listing an adapter are all equally true of a unit the
console cannot find, so none of them is evidence here.

**The evidence is one console reachability receipt per unit**, at
:data:`RECEIPT_KEY` under the data-collection store (the gate role already
reads ``data_collection/*``, so no new grant). It carries the console's OWN
diagnosis of the unit's ``component_id`` — `nousergon-console`'s
``console.diagnose.doctor`` rendered with ``as_dict``, the chain

    registry row -> adapter claim -> merged entity
                 -> search-reachable -> structure-reachable -> relation-reachable

— plus the state the merged entity renders in:

.. code-block:: json

    {
      "schema": "data_console_entity.v1",
      "unit_id": "D03",
      "component_id": "data-collector-d03-prices",
      "checked_at": "2026-10-07T14:00:00Z",
      "doctor": {"identifier": "data-collector-d03-prices", "ok": true,
                 "steps": [{"name": "registry row", "ok": true, "detail": "..."}, "..."]},
      "entity": {"kind": "component", "state": "HEALTHY", "reporting_claims": 2}
    }

The gate cannot reach the console itself (it runs on GitHub Actions; the console
serves on its box), so the receipt is what crosses that boundary — the same
shape as part 1's commissioning receipts and the guard-commissioning record.

The checks are batched by the property they grade, each with its own fixture
tests in `tests/test_console_entity_readers.py`:

* **Identity** — the receipt is this unit's, for this unit's ``component_id``.
* **Current** — ``checked_at`` is no older than :data:`RECEIPT_MAX_AGE`; a
  reachability reading from last week says nothing about the console today.
* **Reachable** — every link of :data:`REQUIRED_LINKS` is present and ``ok``,
  and no other link the doctor reported (a descriptor ``binding``) failed.
* **Honest** — the entity is a Component, renders a state from §8.3's closed
  vocabulary, never ``HEALTHY`` on zero reporting claims, and agrees with the
  descriptor's declared ``lifecycle``.

Same three-way honesty as every reader in this package:

* **MET** only when a receipt was read and proves every property above.
* **UNMET** when we looked and it is not there (no receipt), or the receipt
  does not prove what it claims — each failure named.
* **UNMEASURABLE** when we could not look (a denied or unparseable read).
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from nousergon_lib.gates import read_store_document

from data_gate.descriptors import Unit
from data_gate.evidence import GateStore, Reading, _parse_utc

__all__ = [
    "COMPONENT_STATES",
    "RECEIPT_KEY",
    "RECEIPT_MAX_AGE",
    "RECEIPT_SCHEMA",
    "REQUIRED_LINKS",
    "read_console_entity",
    "receipt_key",
]

RECEIPT_SCHEMA = "data_console_entity.v1"

#: Relative to the store root (`s3://alpha-engine-research/data_collection`).
RECEIPT_KEY = "console_entity/{unit_id}/latest.json"

#: The ladder's own freshness bound (`clauses.py`, `read_ladder_freshness`):
#: the gate reads daily, so a receipt one missed day old is still current and
#: one older than that is a probe that stopped.
RECEIPT_MAX_AGE = dt.timedelta(hours=26)

#: `nousergon-console` `console/diagnose.py::doctor`'s chain, by the names it
#: gives each link. All six, because §3.1's three paths are "independently
#: sufficient" only when each is actually there — reachable by name and by
#: structure but not by relation is findable only by someone who already knows.
REQUIRED_LINKS: tuple[str, ...] = (
    "registry row",
    "adapter claim",
    "merged entity",
    "search-reachable",
    "structure-reachable",
    "relation-reachable",
)

#: `observability-policy` §8.3's closed vocabulary, as `nousergon-console`
#: `console/model/kinds.py::State` renders it. Closed: a state outside it is a
#: fall-through, and §8.3 exists because a fall-through is eventually green.
COMPONENT_STATES: frozenset[str] = frozenset(
    {
        "HEALTHY",
        "RUNNING",
        "DEGRADED",
        "FAILED",
        "STALLED",
        "MISSED",
        "NEVER_RAN",
        "DISABLED",
        "DEPRECATED",
        "RETIRED",
        "ABSENT",
        "UNREGISTERED",
        "UNREPORTED",
        "ARMED",
    }
)

#: The one state that reads as "fine". Rendered on no reporting claim it is the
#: aggregate-green defect the requirement names: a Component with nothing to say
#: must render UNREPORTED.
_GREEN = "HEALTHY"

#: Descriptor `lifecycle` values the console must render AS DECLARED
#: (`kinds.py::DECLARED_LIFECYCLE_STATES`) — never inferred, never overridden.
_DECLARED_LIFECYCLE_STATES: dict[str, str] = {
    "disabled": "DISABLED",
    "deprecated": "DEPRECATED",
    "retired": "RETIRED",
}

#: States that contradict a unit that has a registry row and is in service:
#: a declared-only state it never declared, or "no registry row" itself.
_CONTRADICTS_IN_SERVICE: frozenset[str] = frozenset({"DISABLED", "DEPRECATED", "RETIRED", "UNREGISTERED"})

_SOURCE = "data_collection store (console reachability receipts)"


def receipt_key(unit: Unit) -> str:
    return RECEIPT_KEY.format(unit_id=unit.unit_id)


def _identity_problems(unit: Unit, document: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if document.get("schema") != RECEIPT_SCHEMA:
        problems.append(f"schema {document.get('schema')!r}, expected {RECEIPT_SCHEMA!r}")
    if str(document.get("unit_id") or "") != unit.unit_id:
        problems.append(f"unit_id {document.get('unit_id')!r} is not {unit.unit_id!r}")
    if str(document.get("component_id") or "") != unit.component_id:
        problems.append(f"component_id {document.get('component_id')!r} is not {unit.component_id!r}")
    return problems


def _currency_problems(document: dict[str, Any], now: dt.datetime) -> list[str]:
    stamp = document.get("checked_at")
    checked = _parse_utc(stamp) if stamp else None
    if checked is None:
        return [f"`checked_at` {stamp!r} is not an ISO-8601 timestamp: an undated reading is not a current one"]
    if now - checked > RECEIPT_MAX_AGE:
        hours = int(RECEIPT_MAX_AGE.total_seconds() // 3600)
        return [f"checked_at {stamp} is older than {hours} hours: the console probe has stopped"]
    return []


def _reachability_problems(unit: Unit, document: dict[str, Any]) -> list[str]:
    doctor = document.get("doctor")
    if not isinstance(doctor, dict):
        return ["no `doctor` record: nothing shows the console was asked where this unit is"]
    problems: list[str] = []
    if str(doctor.get("identifier") or "") != unit.component_id:
        problems.append(f"doctor.identifier {doctor.get('identifier')!r} is not {unit.component_id!r}")
    steps = [s for s in (doctor.get("steps") or []) if isinstance(s, dict)]
    by_name = {str(s.get("name") or ""): s for s in steps}
    missing = [name for name in REQUIRED_LINKS if name not in by_name]
    if missing:
        problems.append(f"the doctor did not report {missing}: an unwalked link is not a reachable one")
    for step in steps:
        if step.get("ok") is not True:
            problems.append(f"`{step.get('name')}` failed: {step.get('detail') or 'no detail'}")
    if doctor.get("ok") is not True and not any(s.get("ok") is not True for s in steps):
        problems.append(f"doctor.ok is {doctor.get('ok')!r} with every reported link ok: the receipt contradicts itself")
    return problems


def _honesty_problems(unit: Unit, document: dict[str, Any]) -> list[str]:
    entity = document.get("entity")
    if not isinstance(entity, dict):
        return ["no `entity` record: nothing shows what state the console renders this unit in"]
    problems: list[str] = []
    if str(entity.get("kind") or "") != "component":
        problems.append(f"entity.kind {entity.get('kind')!r}: the unit must be a Component, not another kind")
    state = str(entity.get("state") or "")
    claims = entity.get("reporting_claims")
    if state not in COMPONENT_STATES:
        problems.append(f"entity.state {state!r} is outside §8.3's closed vocabulary")
    if not isinstance(claims, int) or isinstance(claims, bool) or claims < 0:
        problems.append(f"entity.reporting_claims {claims!r} is not a count")
    elif state == _GREEN and claims == 0:
        problems.append(
            "renders HEALTHY on zero reporting claims: green with nothing to say "
            "(a Component nobody reports on must render UNREPORTED)"
        )
    declared = _DECLARED_LIFECYCLE_STATES.get(unit.lifecycle)
    if declared is not None and state != declared:
        problems.append(f"lifecycle {unit.lifecycle!r} must render {declared}, renders {state!r}")
    if declared is None and state in _CONTRADICTS_IN_SERVICE:
        problems.append(f"renders {state} for a unit declared {unit.lifecycle!r}")
    return problems


def read_console_entity(store: GateStore, unit: Unit, *, now: dt.datetime | None = None) -> Reading:
    """Whether the console finds this unit by all three paths and renders it honestly."""
    from data_gate.unit_readers import partial_exclusion_reading

    excluded = partial_exclusion_reading(unit, "console_entity")
    if excluded is not None:
        return excluded
    now = now or dt.datetime.now(dt.timezone.utc)
    key = receipt_key(unit)
    read = read_store_document(store, key)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {key}: {read.problem}",
            evidence=(key,),
            unmeasurable=True,
            source=_SOURCE,
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"no console reachability receipt at {key}. Nothing shows the console finds "
                f"{unit.component_id!r} by name, by structure and by relation (console-policy §3.1), "
                "and a unit nobody has looked for renders as nothing at all"
            ),
            evidence=(key, f"registry.d/units/{unit.path.name}"),
            source=_SOURCE,
        )
    document = read.document or {}
    as_of = str(document.get("checked_at") or "") or None
    problems = (
        _identity_problems(unit, document)
        + _currency_problems(document, now)
        + _reachability_problems(unit, document)
        + _honesty_problems(unit, document)
    )
    if problems:
        return Reading(
            met=False,
            detail=f"{key} does not prove {unit.component_id!r} is on the console: {'; '.join(problems)}",
            evidence=(key,),
            source=_SOURCE,
            as_of=as_of,
        )
    entity = document["entity"]
    return Reading(
        met=True,
        detail=(
            f"{unit.component_id!r} is reachable on the console by name, structure and relation, "
            f"renders {entity['state']} on {entity['reporting_claims']} reporting claim(s), "
            f"checked {document['checked_at']} ({key})"
        ),
        evidence=(key,),
        source=_SOURCE,
        as_of=as_of,
    )
