"""The `detector` base-column reader — what replaced the phase-0 stub.

`alpha-engine-config-I10795` (audit gap A3, part 1). The requirement is
`observability-policy` §9.1, read literally: a unit's detector has been made to
FIRE by inducing the real condition, the alert **named the right subject**, and
it **stood down when the condition cleared** — and all three were recorded.
Reading the code, seeing the alarm exist, or seeing it sit in ``OK`` are equally
true of a detector that will never fire, so none of them is evidence here.

**The evidence is one commissioning receipt per declared detector**, at
:data:`RECEIPT_KEY` under the data-collection store (the gate role already
reads ``data_collection/*``, so no new grant). The guard-commissioning record
(`faults/<unit>/<guard>/latest.json`, :func:`data_gate.evidence.read_guard_commissioning`)
is the precedent; a detector receipt carries more because §9.1 asks for three
facts, not one:

.. code-block:: json

    {
      "schema": "data_detector_commissioning.v1",
      "unit_id": "D03",
      "detector": {"kind": "freshness-monitor", "via": "artifact-registry-row"},
      "outcome": "induced",
      "induced": {"at": "2026-10-07T14:00:00Z", "condition": "withheld ..."},
      "fired":   {"at": "2026-10-07T14:20:00Z", "subject": "price_cache_freshness_sentinel",
                  "evidence": "alert id / message ref"},
      "cleared": {"at": "2026-10-07T15:05:00Z", "evidence": "stand-down ref"}
    }

The reader is batched by detector FAMILY (the descriptor's ``detectors[].kind``)
because the families differ in one thing only: what the right SUBJECT is.

* ``freshness-monitor`` pages per ARTIFACT_REGISTRY row, so the subject must be
  one of the unit's own ``registry_rows``.
* ``sf-execution`` and ``eventbridge-scheduler`` name an exact resource in
  ``via`` (a state machine, a scheduled check), so the subject must equal it.
* ``cloudwatch-alarm``, ``box-timer-health``, ``code-refusal`` and
  ``crucible-gate`` describe their detector in prose (``via: "no-heartbeat"``),
  so the subject is whatever the detector entry declares as ``subject:``. A
  receipt for one that declares none is UNMEASURABLE — the reader will not guess
  which alarm "no-heartbeat" means.

Same three-way honesty as every reader in this package:

* **MET** only when a receipt was read and proves all three facts.
* **UNMET** when we looked and it is not there (no detector declared, no
  receipt), or the receipt does not prove what it claims — each named.
* **UNMEASURABLE** when we could not look (a denied or unparseable read), or a
  receipt exists whose subject the descriptor gives us no way to check.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from typing import TypeGuard, cast

from nousergon_lib.gates import read_store_document

from data_gate.descriptors import Unit
from data_gate.evidence import GateStore, Reading

__all__ = [
    "DETECTOR_FAMILIES",
    "RECEIPT_KEY",
    "RECEIPT_SCHEMA",
    "expected_subjects",
    "read_detector",
    "receipt_key",
]

RECEIPT_SCHEMA = "data_detector_commissioning.v1"

#: Relative to the store root (`s3://alpha-engine-research/data_collection`).
#: One receipt per (unit, detector kind): the freshness monitor covers dozens of
#: units, and commissioning it for D03 says nothing about whether it names D31.
RECEIPT_KEY = "commissioning/{unit_id}/{kind}/latest.json"

#: The only outcome that proves a detector fired. Mirrors the guard record's
#: vocabulary (`nousergon_lib.gates.faults.FAULT_OUTCOME_INDUCED`): ``absorbed``
#: names a fault the detector never had to catch.
_INDUCED = "induced"

#: How each family's subject is resolved. Closed: a descriptor declaring a kind
#: outside it reads UNMEASURABLE naming the kind, never MET on a guessed rule.
_SUBJECT_FROM_REGISTRY_ROWS = "registry_rows"
_SUBJECT_FROM_VIA = "via"
_SUBJECT_DECLARED = "subject"
DETECTOR_FAMILIES: dict[str, str] = {
    "freshness-monitor": _SUBJECT_FROM_REGISTRY_ROWS,
    "sf-execution": _SUBJECT_FROM_VIA,
    "eventbridge-scheduler": _SUBJECT_FROM_VIA,
    "cloudwatch-alarm": _SUBJECT_DECLARED,
    "box-timer-health": _SUBJECT_DECLARED,
    "code-refusal": _SUBJECT_DECLARED,
    "crucible-gate": _SUBJECT_DECLARED,
}

_SOURCE = "data_collection store (commissioning receipts)"


def receipt_key(unit: Unit, kind: str) -> str:
    return RECEIPT_KEY.format(unit_id=unit.unit_id, kind=kind)


def _is_json_object(value: object) -> TypeGuard[dict[str, object]]:
    """``isinstance(value, dict)``: a decoded JSON/YAML mapping, keyed by field name."""
    return isinstance(value, dict)


def _row_id(entry: object) -> str:
    """`feature_store_freshness_sentinel (indirect, depends_on)` -> the id."""
    text = str(entry).strip()
    return text.split()[0].rstrip(",") if text else ""


def _as_list(value: object) -> list[str]:
    if value is None or value == "":
        return []
    # A list/tuple out of the descriptor YAML: its items are whatever YAML decoded.
    values: Sequence[object] = (
        cast("Sequence[object]", value) if isinstance(value, (list, tuple)) else [value]
    )
    return [str(v).strip() for v in values if str(v).strip()]


def expected_subjects(unit: Unit, detector: Mapping[str, object]) -> tuple[str, ...]:
    """The subjects a receipt for ``detector`` may name. Empty: none is checkable.

    An explicit ``subject:`` on the detector entry always wins, for every family:
    it is the descriptor's own, narrower statement.
    """
    declared = _as_list(detector.get("subject"))
    if declared:
        return tuple(declared)
    rule = DETECTOR_FAMILIES.get(str(detector.get("kind") or ""))
    if rule == _SUBJECT_FROM_REGISTRY_ROWS:
        rows: Iterable[object] = unit.raw.get("registry_rows") or []
        return tuple(r for r in (_row_id(e) for e in rows) if r)
    if rule == _SUBJECT_FROM_VIA:
        return tuple(_as_list(detector.get("via")))
    return ()


def _parse_utc(stamp: object) -> dt.datetime | None:
    if not stamp:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _receipt_problems(unit: Unit, kind: str, document: Mapping[str, object], subjects: tuple[str, ...]) -> list[str]:
    """Every way ``document`` fails to prove induce -> fire on the right subject -> clear."""
    problems: list[str] = []
    if document.get("schema") != RECEIPT_SCHEMA:
        problems.append(f"schema {document.get('schema')!r}, expected {RECEIPT_SCHEMA!r}")
    if str(document.get("unit_id") or "") != unit.unit_id:
        problems.append(f"unit_id {document.get('unit_id')!r} is not {unit.unit_id!r}")
    # Read unchecked, as it always was: a non-mapping `detector` raises here.
    recorded_detector = cast("Mapping[str, object]", document.get("detector") or {})
    recorded_kind = str(recorded_detector.get("kind") or "")
    if recorded_kind != kind:
        problems.append(f"detector.kind {recorded_kind!r} is not {kind!r}")
    outcome = str(document.get("outcome") or "")
    if outcome != _INDUCED:
        problems.append(
            f"outcome {outcome!r}: only an `induced` record is evidence the detector fired"
        )

    blocks: dict[str, object] = {name: document.get(name) for name in ("induced", "fired", "cleared")}
    stamps: dict[str, dt.datetime] = {}
    for name, block in blocks.items():
        if not _is_json_object(block):
            problems.append(f"no `{name}` record (§9.1 records all three: induced, fired, cleared)")
            continue
        at = _parse_utc(block.get("at"))
        if at is None:
            problems.append(f"`{name}.at` {block.get('at')!r} is not an ISO-8601 timestamp")
        else:
            stamps[name] = at
    induced, fired, cleared = blocks["induced"], blocks["fired"], blocks["cleared"]
    if _is_json_object(induced) and not str(induced.get("condition") or "").strip():
        problems.append("`induced.condition` is empty: the record does not say what was broken")
    if _is_json_object(fired):
        if not str(fired.get("evidence") or "").strip():
            problems.append("`fired.evidence` is empty: nothing shows the alert arrived")
        subject = str(fired.get("subject") or "").strip()
        if not subject:
            problems.append("`fired.subject` is empty: the alert's subject was not recorded")
        elif subject not in subjects:
            problems.append(f"fired on subject {subject!r}, which is not {unit.unit_id}'s ({list(subjects)})")
    if _is_json_object(cleared) and not str(cleared.get("evidence") or "").strip():
        problems.append("`cleared.evidence` is empty: nothing shows the detector stood down")
    if "induced" in stamps and "fired" in stamps and stamps["fired"] < stamps["induced"]:
        problems.append("fired before the fault was induced: the alert was not caused by it")
    if "fired" in stamps and "cleared" in stamps and stamps["cleared"] <= stamps["fired"]:
        problems.append("cleared at or before it fired: no stand-down was observed")
    return problems


def _read_one(store: GateStore, unit: Unit, detector: Mapping[str, object]) -> Reading:
    """One declared detector's receipt."""
    kind = str(detector.get("kind") or "").strip()
    key = receipt_key(unit, kind or "<undeclared-kind>")
    label = f"{kind or '?'} via {detector.get('via')!r}"
    if kind not in DETECTOR_FAMILIES:
        return Reading(
            met=False,
            detail=(
                f"{label}: detector kind {kind!r} is not a known family {sorted(DETECTOR_FAMILIES)}, "
                "so there is no rule for which subject it must name"
            ),
            evidence=(key,),
            unmeasurable=True,
            source=_SOURCE,
        )
    read = read_store_document(store, key)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"{label}: could not read {key}: {read.problem}",
            evidence=(key,),
            unmeasurable=True,
            source=_SOURCE,
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"{label}: no commissioning receipt at {key}. A detector that has never been made "
                "to fire is not in service (observability-policy §9.1)"
            ),
            evidence=(key,),
            source=_SOURCE,
        )
    document = read.document or {}
    subjects = expected_subjects(unit, detector)
    cleared = document.get("cleared")
    as_of = str((cleared or {}).get("at") or "") if _is_json_object(cleared) else ""
    if not subjects:
        return Reading(
            met=False,
            detail=(
                f"{label}: a receipt exists at {key}, but the descriptor names no subject it can be "
                f"checked against (family {kind!r} resolves its subject from "
                f"`{DETECTOR_FAMILIES[kind]}`; declare `subject:` on this detector entry)"
            ),
            evidence=(key,),
            unmeasurable=True,
            source=_SOURCE,
            as_of=as_of or None,
        )
    problems = _receipt_problems(unit, kind, document, subjects)
    if problems:
        return Reading(
            met=False,
            detail=f"{label}: {key} does not prove commissioning: {'; '.join(problems)}",
            evidence=(key,),
            source=_SOURCE,
            as_of=as_of or None,
        )
    fired = document["fired"]
    return Reading(
        met=True,
        detail=(
            f"{label}: commissioned — induced {document['induced']['at']}, fired "
            f"{fired['at']} on {fired['subject']!r}, cleared {document['cleared']['at']} ({key})"
        ),
        evidence=(key,),
        source=_SOURCE,
        as_of=as_of or None,
    )


def read_detector(store: GateStore, unit: Unit) -> Reading:
    """Whether at least one of the unit's declared detectors is commissioned.

    The requirement is "has A detector that has been made to fire", so one
    commissioned detector is enough; the others are named in the detail either
    way, so a unit with one commissioned and one dead detector shows both.
    UNMEASURABLE only when nothing is MET and some detector could not be read —
    "could not look" never hides behind a sibling's "looked, absent".
    """
    from data_gate.unit_readers import partial_exclusion_reading

    excluded = partial_exclusion_reading(unit, "detector")
    if excluded is not None:
        return excluded
    declared: Iterable[object] = unit.raw.get("detectors") or []
    detectors = [d for d in declared if _is_json_object(d)]
    ref = f"registry.d/units/{unit.path.name}"
    if not detectors:
        return Reading(
            met=False,
            detail=(
                f"{unit.unit_id} declares no detector (`detectors: []`), so nothing exists that could "
                "be made to fire. Phase 3 adds one and commissions it by induction"
            ),
            evidence=(ref,),
            source="registry.d/units",
        )
    readings = [_read_one(store, unit, d) for d in detectors]
    evidence = tuple(dict.fromkeys(e for r in readings for e in r.evidence)) + (ref,)
    detail = " | ".join(r.detail for r in readings)
    met = [r for r in readings if r.met]
    if met:
        return Reading(met=True, detail=detail, evidence=evidence, source=_SOURCE, as_of=met[0].as_of)
    return Reading(
        met=False,
        detail=detail,
        evidence=evidence,
        unmeasurable=any(r.unmeasurable for r in readings),
        source=_SOURCE,
    )
