"""Load and validate the unit descriptors under ``registry.d/units/``.

`data_collection_plan_260914.md` §4.1: the descriptors are the ONLY source of
units. Nothing downstream hand-lists one — a hand-maintained monitored-things
list drifts, and its drift is invisible because the missing rows produce no
signal (`observability-policy` §2.2).

So this module's job is not "parse YAML", it is **refuse a descriptor set that
would silently grade less than it claims**: a missing column, an unknown state,
a guard marked N/A with no code from the closed taxonomy, a duplicate unit id.
Every refusal here is loud at import, because the alternative is a board that
renders complete over a unit nobody declared.
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass
from typing import Any

import yaml

__all__ = [
    "AUDIT_CELL_STATES",
    "AUDIT_COLUMNS",
    "GUARD_CLASSES",
    "NA_TAXONOMY",
    "UNITS_DIR",
    "Unit",
    "load_units",
]

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
UNITS_DIR = REPO_ROOT / "registry.d" / "units"

#: The nine scored columns of the audit's §3 unit register, in its own order.
#: One base clause is generated per unit per column, and
#: `tests/test_every_audit_cell_is_a_clause.py` grades that mapping as a
#: bijection. Changing this tuple changes the clause count, which is why it is
#: declared once and read everywhere.
AUDIT_COLUMNS: tuple[str, ...] = (
    "identity",
    "consumers",
    "schema_contract",
    "artifact_registry",
    "observability_row",
    "run_record",
    "detector",
    "console_entity",
    "survives_phase4",
)

#: The audit's own cell vocabulary (§2, "Cell vocabulary"). Closed: a tenth
#: value would be a reading nobody defined, and a descriptor carrying one is
#: refused rather than rendered.
AUDIT_CELL_STATES: frozenset[str] = frozenset({"PRESENT", "ABSENT", "BROKEN", "UNVERIFIED"})

#: The six silent-degradation classes of audit §4.1, plus the cross-check the
#: plan adds on the two units that write the same key (§2 row 5).
GUARD_CLASSES: tuple[str, ...] = (
    "empty_fresh",
    "cardinality",
    "units",
    "pit",
    "vendor_fallback",
    "success_without_output",
)
OPTIONAL_GUARD_CLASSES: tuple[str, ...] = ("vendor_crosscheck",)

#: `observability-policy` §3.5's CLOSED N/A taxonomy. A guard class, a freshness
#: deadline or a completeness floor may be marked not-applicable ONLY with one
#: of these codes beside it. The plan (§4.1) states the rule and the consequence
#: in one line: *an N/A with no reason is UNMET*. A free-text excuse is how a
#: class quietly stops being graded.
NA_TAXONOMY: frozenset[str] = frozenset(
    {
        "N/A-NOT-IMPL",  # the thing is not built
        "N/A-NOT-RUN",  # it did not run this cycle (disabled, manual, on-demand)
        "N/A-LOW-N",  # it ran, and the sample is below its floor
        "N/A-MISSING-INPUT",  # it ran, and a required input was absent
    }
)

#: The shape `run_manifest_retention` must take: a positive integer count of
#: days (`alpha-engine-config-I10831` deliverable 3; precedent: D37's
#: `run_manifest_retention: 400 days`). A free-text value is exactly the kind
#: of undeclared knob the field exists to end.
_RETENTION_PATTERN = re.compile(r"^\d+\s+days$")

#: A unit firing more than once an hour takes one manifest per tick, not one
#: per day — its `run_manifest_prefix` is high-cardinality and its S3 cost is
#: a decision, not a default. `trigger.cadence_minutes` mirrors the field name
#: `nous-ergon-ops` registry rows already use for the same fact.
_SUB_HOURLY_MINUTES = 60

_REQUIRED_TOP_LEVEL = (
    "unit_id",
    "component_id",
    "owning_repo",
    "title",
    "lifecycle",
    "trigger",
    "writes",
    "consumers",
    "contract",
    "registry_rows",
    "run_manifest_prefix",
    "detectors",
    "freshness",
    "completeness",
    "guards",
    "audit",
    "clause_phase",
)


class DescriptorError(ValueError):
    """A descriptor that cannot be graded. Always fatal, never skipped.

    Skipping a malformed descriptor would drop its unit from the board's
    denominator, and a board whose denominator shrinks when a file breaks
    reports improving coverage as its coverage disappears.
    """


@dataclass(frozen=True)
class Unit:
    """One audit unit, as its committed descriptor declares it."""

    unit_id: str
    path: pathlib.Path
    raw: dict[str, Any]

    @property
    def component_id(self) -> str:
        return str(self.raw["component_id"])

    @property
    def title(self) -> str:
        return str(self.raw["title"])

    @property
    def lifecycle(self) -> str:
        return str(self.raw["lifecycle"])

    @property
    def component(self) -> int:
        return int(self.raw.get("component", 1))

    @property
    def graded_on(self) -> str | None:
        value = self.raw.get("graded_on")
        return None if value is None else str(value)

    @property
    def cells(self) -> dict[str, str]:
        return dict(self.raw["audit"]["cells"])

    @property
    def clause_phase(self) -> dict[str, int]:
        return {k: int(v) for k, v in self.raw["clause_phase"].items()}

    @property
    def writes(self) -> list[str]:
        return list(self.raw["writes"])

    @property
    def guards(self) -> dict[str, dict[str, Any]]:
        return dict(self.raw["guards"])

    @property
    def freshness_family(self) -> str | None:
        freshness = self.raw.get("freshness") or {}
        return freshness.get("family")

    @property
    def run_manifest_prefix(self) -> str:
        return str(self.raw["run_manifest_prefix"])

    @property
    def cadence_minutes(self) -> int | None:
        trigger = self.raw.get("trigger") or {}
        value = trigger.get("cadence_minutes")
        return None if value is None else int(value)

    @property
    def run_manifest_retention_days(self) -> int | None:
        retention = self.raw.get("run_manifest_retention")
        if retention is None:
            return None
        return int(str(retention).strip().split()[0])


def _check_na(where: str, block: dict[str, Any], unit_id: str) -> None:
    """A not-applicable declaration carries a code from the closed taxonomy."""
    code = block.get("na_code")
    if code is None:
        raise DescriptorError(
            f"{unit_id}: {where} is marked not_applicable with no `na_code`. "
            "observability-policy §3.5's N/A taxonomy is CLOSED and the plan's rule is "
            "explicit: an N/A with no reason is UNMET. A free-text excuse is how a class "
            f"quietly stops being graded. Use one of {sorted(NA_TAXONOMY)}."
        )
    if code not in NA_TAXONOMY:
        raise DescriptorError(
            f"{unit_id}: {where} declares na_code {code!r}, which is not in "
            f"observability-policy §3.5's closed taxonomy {sorted(NA_TAXONOMY)}. A code "
            "outside the set is a new engineering state nobody has defined a rendering "
            "for."
        )


def _check_retention(unit_id: str, document: dict[str, Any], path: pathlib.Path) -> None:
    """A sub-hourly unit declares a validated `run_manifest_retention`.

    `alpha-engine-config-I10831` deliverable 3. D37 (metron-intraday, 5-minute
    cadence) is the precedent this generalizes: ~288 manifests/day against
    ~1/day for every other unit is a bucket-cost decision that belongs on the
    record, not left to the bucket's default. A cadence at or above 60 minutes
    is exempt — the field is optional there, but if declared anyway its shape
    is still checked, so a stray declaration can never silently mean nothing.
    """
    trigger = document.get("trigger") or {}
    raw_cadence = trigger.get("cadence_minutes")
    cadence_minutes: int | None = None
    if raw_cadence is not None:
        try:
            cadence_minutes = int(raw_cadence)
        except (TypeError, ValueError):
            raise DescriptorError(
                f"{path.name}: trigger.cadence_minutes must be an integer number of "
                f"minutes, got {raw_cadence!r}."
            )
        if cadence_minutes <= 0:
            raise DescriptorError(
                f"{path.name}: trigger.cadence_minutes must be positive, got {cadence_minutes}."
            )

    retention = document.get("run_manifest_retention")
    reason = document.get("run_manifest_retention_reason")
    sub_hourly = cadence_minutes is not None and cadence_minutes < _SUB_HOURLY_MINUTES

    if retention is None:
        if sub_hourly:
            raise DescriptorError(
                f"{unit_id}: trigger.cadence_minutes={cadence_minutes} is sub-hourly — its "
                "run_manifest_prefix takes one manifest per tick, not one per day — so it "
                "must declare `run_manifest_retention` (`<int> days`) and "
                "`run_manifest_retention_reason`. A missing declaration leaves the bucket's "
                "default to decide a cost nobody chose (precedent: D37)."
            )
        return

    if not _RETENTION_PATTERN.match(str(retention).strip()):
        raise DescriptorError(
            f"{unit_id}: run_manifest_retention {retention!r} is not `<positive integer> "
            "days` — the only shape this field is declared to accept."
        )
    if not str(reason or "").strip():
        raise DescriptorError(
            f"{unit_id}: run_manifest_retention is declared with no "
            "run_manifest_retention_reason. A retention number with no rationale is a knob "
            "nobody can safely change later."
        )


def _validate(unit_id: str, document: dict[str, Any], path: pathlib.Path) -> None:
    missing = [key for key in _REQUIRED_TOP_LEVEL if key not in document]
    if missing:
        raise DescriptorError(f"{path.name}: missing required field(s) {missing}")

    if document["unit_id"] != unit_id:
        raise DescriptorError(
            f"{path.name}: declares unit_id {document['unit_id']!r} but its filename says "
            f"{unit_id!r}. The filename is how a human finds the unit and the field is how "
            "the generator names it; a mismatch means one of the two is a lie."
        )

    cells = (document.get("audit") or {}).get("cells")
    if not isinstance(cells, dict):
        raise DescriptorError(f"{path.name}: audit.cells is missing or is not a mapping")
    if set(cells) != set(AUDIT_COLUMNS):
        raise DescriptorError(
            f"{path.name}: audit.cells declares {sorted(cells)}, but the audit's §3 register "
            f"scores exactly {list(AUDIT_COLUMNS)}. A unit with a missing column is a unit "
            "whose board row is complete over a question nobody asked."
        )
    for column, state in cells.items():
        if state not in AUDIT_CELL_STATES:
            raise DescriptorError(
                f"{path.name}: audit.cells.{column} is {state!r}, which is not one of the "
                f"audit's four cell states {sorted(AUDIT_CELL_STATES)}."
            )

    if set(document["clause_phase"]) != set(AUDIT_COLUMNS):
        raise DescriptorError(
            f"{path.name}: clause_phase must name every scored column; a column with no "
            "phase is a clause that can never hold a gate."
        )

    guards = document["guards"]
    if not isinstance(guards, dict):
        raise DescriptorError(f"{path.name}: guards is not a mapping")
    missing_guards = [g for g in GUARD_CLASSES if g not in guards]
    if missing_guards:
        raise DescriptorError(
            f"{path.name}: guards is missing {missing_guards}. Every unit answers every "
            "audit §4.1 class — with a state, or with an explicit N/A and its code. "
            "Silence on a degradation class is the degradation going unnamed."
        )
    unknown = [g for g in guards if g not in GUARD_CLASSES + OPTIONAL_GUARD_CLASSES]
    if unknown:
        raise DescriptorError(f"{path.name}: guards declares unknown class(es) {unknown}")
    for name, block in guards.items():
        if not isinstance(block, dict) or "state" not in block:
            raise DescriptorError(f"{path.name}: guards.{name} has no `state`")
        if block["state"] == "not_applicable":
            _check_na(f"guards.{name}", block, unit_id)
        if not str(block.get("note") or "").strip():
            raise DescriptorError(
                f"{path.name}: guards.{name} has no note. A guard state with no evidence "
                "behind it is a claim, and the board renders claims as findings."
            )

    freshness = document["freshness"] or {}
    if freshness.get("status") == "not_applicable":
        _check_na("freshness", freshness, unit_id)
    completeness = document["completeness"] or {}
    if completeness.get("status") == "not_applicable":
        _check_na("completeness", completeness, unit_id)

    if not document["consumers"] and not str(document.get("consumers_reason") or "").strip():
        raise DescriptorError(
            f"{path.name}: declares no consumers and gives no reason. Plan §3's rule: a key "
            "with no surviving consumer gets a retirement decision, or a declared "
            "`consumers: []` WITH a reason, which renders as a finding. An empty list with "
            "no reason renders as nothing at all."
        )

    _check_retention(unit_id, document, path)


def load_units(directory: pathlib.Path | None = None) -> list[Unit]:
    """Every unit descriptor, validated, ordered by unit id.

    Raises on an empty directory: a clause generator that finds no units would
    publish a gate over an empty set and call it met.
    """
    root = directory or UNITS_DIR
    paths = sorted(root.glob("*.yaml"))
    if not paths:
        raise DescriptorError(
            f"no unit descriptors under {root}. A clause generator over an empty unit set "
            "would publish a gate that grades nothing and reads as a pass."
        )
    units: list[Unit] = []
    seen: set[str] = set()
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise DescriptorError(f"{path.name}: not a YAML mapping")
        unit_id = path.name.split("-", 1)[0]
        _validate(unit_id, document, path)
        if unit_id in seen:
            raise DescriptorError(f"duplicate unit id {unit_id!r} under {root}")
        seen.add(unit_id)
        units.append(Unit(unit_id=unit_id, path=path, raw=document))
    return units
