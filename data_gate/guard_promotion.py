"""Per-guard, risk-based promotion criteria — Brian's 2026-10-04 option (c).

`alpha-engine-config-I11973`, ruling comment 5983159736: *"The adopted
ten-clean-cycle guard promotion is replaced by per-guard risk-based criteria,
codified per guard, plus at least one actual scheduled observe execution."*
Promotion stays a deliberate PR flipping the guard's
`nousergon_lib.guard_mode.GuardStaging` to ``ENFORCE``; this module is the
codified criterion that PR cites, and the gate reads it.

**What replaced the ten cycles.** A clean-cycle COUNT was the same bar for a
guard whose raise halts every scheduled collector and for one whose verdict has
no raise site at all. The criterion here scales with BLAST RADIUS instead — what
an ENFORCE verdict halts, read from the code — and the evidence each tier needs
is coverage, not elapsed time:

* **every guard**: at least one actual scheduled execution, and it is the
  CURRENT one — the latest due cycle of every schedule a required unit runs on
  recorded this guard's verdict, clean, for that unit (option (c): a current
  failure, or stale or missing proof, blocks). Where the guard maps to a
  descriptor guard class, every required unit also holds an INDUCED-fault
  commissioning record (`observability-policy` §9.1), which is the replay
  receipt that the predicate fires on the fault it exists for.
* **high** risk (a shared chokepoint: the raise halts many units' phases):
  the required units are EVERY unit whose descriptor records this guard
  (``records_as``), across every schedule — no sampling.
* **moderate** risk (one unit's own output): the declared units only.

No count above the ruling's own floor of one is set here. Raising one for a
specific guard is a threshold decision, reserved to Brian with every other
threshold — it is an edit to that guard's entry below, with the ruling cited.

**Lockstep with the code.** `data_gate` ships inside Lambda zips that carry no
`nousergon_lib`, so each entry names its staging object and repeats its mode as
a literal (the `descriptors.GUARD_RECORDED_NAMES` pattern), and
`tests/test_guard_promotion.py` pins both to the producing modules: a promotion
PR that flips a mode without this table, or a staged guard with no entry, is a
red test.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from data_gate.descriptors import Unit
    from data_gate.evidence import GateStore, Reading
    from data_gate.exit_criteria import CycleSet

__all__ = [
    "CLAUSE_PREFIX",
    "GUARD_PROMOTIONS",
    "PROMOTION_RULING",
    "RISK_TIERS",
    "GuardPromotion",
    "clause_name",
    "read_promotion",
    "required_units",
]

CLAUSE_PREFIX = "data.guard_promotion"

PROMOTION_RULING = (
    "Brian, 2026-10-04 (option (c), alpha-engine-config-I11973): the adopted ten-clean-cycle "
    "guard promotion is replaced by per-guard risk-based criteria, codified per guard, plus at "
    "least one actual scheduled observe execution; promotion stays a deliberate PR."
)

#: What each tier requires, rendered on the row so the bar is read, not recalled.
RISK_TIERS: dict[str, str] = {
    "high": (
        "a shared chokepoint: every unit that records this guard must show a clean verdict on "
        "the latest due cycle of its schedule, and an induced-fault commissioning record"
    ),
    "moderate": (
        "one unit's own output: the unit(s) that record this guard must show a clean verdict on "
        "the latest due cycle of their schedule, and an induced-fault commissioning record where "
        "the guard maps to a descriptor guard class"
    ),
}


@dataclass(frozen=True)
class GuardPromotion:
    """One staged guard's codified promotion criterion."""

    #: The name the guard records on a manifest (`guards[].guard`) — its
    #: `GuardStaging.name`.
    name: str
    #: Where the `GuardStaging` lives, ``<path>::<symbol>``.
    staging: str
    #: The staging's mode, as a literal: ``observe`` or ``enforce``.
    mode: str
    #: A key of :data:`RISK_TIERS`.
    risk: str
    #: What an ENFORCE verdict halts, read from the code.
    blast_radius: str
    #: The descriptor guard class whose per-unit commissioning record is
    #: required, or ``None`` for a guard no descriptor class carries.
    guard_class: str | None
    #: The units whose evidence is required. Empty: every non-retired unit
    #: whose descriptor's ``guards.<guard_class>.records_as`` names this guard.
    units: tuple[str, ...]
    #: Verdicts that count as clean. Never ``unmeasurable``: a guard that could
    #: not look did not pass.
    clean_verdicts: frozenset[str]
    #: Conditions the staging already declared that no reader grades. Rendered
    #: on the row, never assumed met; the promotion PR confirms each.
    preconditions: tuple[str, ...]
    tracked_issue: str


GUARD_PROMOTIONS: tuple[GuardPromotion, ...] = (
    GuardPromotion(
        name="data_empty_fresh",
        staging="validators/expectations.py::EMPTY_FRESH_GUARD",
        mode="observe",
        risk="high",
        blast_radius=(
            "ENFORCE raises _CollectorError in weekly_collector._phase_collect — the chokepoint "
            "every scheduled collector passes — failing that collector's phase on every schedule"
        ),
        guard_class="empty_fresh",
        units=(),
        clean_verdicts=frozenset({"ok", "not_applicable"}),
        preconditions=(),
        tracked_issue="alpha-engine-config-I10785",
    ),
    GuardPromotion(
        name="data_cardinality",
        staging="validators/expectations.py::CARDINALITY_GUARD",
        mode="observe",
        risk="moderate",
        blast_radius=(
            "the EOD spine's (D20) own run; no ENFORCE raise site exists yet, so promotion also "
            "writes one — the verdict is recorded and published, never acted on, today"
        ),
        guard_class="cardinality",
        units=(),
        clean_verdicts=frozenset({"ok", "not_applicable"}),
        preconditions=(),
        tracked_issue="alpha-engine-config-I10780",
    ),
    GuardPromotion(
        name="bar_settlement",
        staging="dates.py::BAR_SETTLEMENT_GUARD",
        mode="observe",
        risk="moderate",
        blast_radius=(
            "the D03/D19 daily-close fetches; no ENFORCE raise site exists yet, so promotion "
            "also writes one — the verdict is recorded on the manifest and read by shadow parity"
        ),
        guard_class=None,
        units=("D03", "D19"),
        clean_verdicts=frozenset({"settled"}),
        preconditions=(
            "the standalone postclose schedule fetches at or after the settlement threshold "
            "(Brian's ruling on alpha-engine-config-I11354)",
            "the 3-day settlement-time sample (alpha-engine-config-I11356) confirms the threshold",
        ),
        tracked_issue="alpha-engine-config-I11354",
    ),
)


def clause_name(promotion: GuardPromotion) -> str:
    return f"{CLAUSE_PREFIX}.{promotion.name}"


def required_units(promotion: GuardPromotion, units: list[Unit]) -> list[str]:
    """The units whose evidence this guard's promotion needs (see :class:`GuardPromotion`)."""
    if promotion.units:
        return list(promotion.units)
    return [
        u.unit_id
        for u in units
        if not u.retired
        and str((u.guards.get(promotion.guard_class or "") or {}).get("records_as") or "")
        == promotion.name
    ]


def read_promotion(
    store: GateStore,
    promotion: GuardPromotion,
    units: list[Unit],
    cycle_sets: list[CycleSet],
    *,
    trading_day: dt.date,
) -> Reading:
    """Whether ``promotion``'s codified criterion holds, and whether the guard is promoted.

    ``met`` only when the criterion holds AND the staging is ``enforce``: the
    phase-2 exit asks for every guard ENFORCING, and an enforcing guard whose
    criterion does not hold is the undeliberate promotion this exists to show.
    A guard that is READY but still observing reads UNMET with "READY", so the
    promotion PR has its evidence on the row.
    """
    from data_gate.evidence import Reading, read_guard_commissioning

    source = "data_collection/runs/<unit>/<trading_day>/ + faults/<unit>/<guard>/latest.json"
    needed = required_units(promotion, units)
    evidence: list[str] = [promotion.staging, "data_gate/guard_promotion.py"]
    if not needed:
        return Reading(
            met=False,
            detail=(
                f"no unit records {promotion.name} (no descriptor declares "
                f"guards.{promotion.guard_class}.records_as: {promotion.name}), so nothing can "
                "show the guard ran on a scheduled path"
            ),
            evidence=tuple(evidence),
            source=source,
        )
    by_id = {u.unit_id: u for u in units}
    missing: list[str] = []
    passed: list[str] = []
    for unit_id in needed:
        sets = [cs for cs in cycle_sets if unit_id in cs.unit_ids]
        if not sets:
            missing.append(f"{unit_id}: no declared schedule verifies it, so no scheduled run exists")
            continue
        for cs in sets:
            if cs.unmeasurable:
                return Reading(
                    met=False,
                    detail=f"cannot read {cs.schedule}'s cycles for {unit_id}: {cs.reason}",
                    evidence=tuple(evidence + [cs.schedule]),
                    unmeasurable=True,
                    source=source,
                )
            if not cs.cycles:
                missing.append(f"{unit_id}@{cs.schedule}: no due cycle")
                continue
            latest = cs.cycles[0]
            verdicts = [
                str(g.get("verdict"))
                for _key, doc in latest.manifests.get(unit_id, [])
                if doc.get("trigger") == "scheduled"
                for g in (doc.get("guards") or [])
                if str(g.get("guard") or g.get("name") or "") == promotion.name
            ]
            where = f"{unit_id}@{cs.schedule} latest due cycle {latest.label}"
            if not verdicts:
                missing.append(f"{where}: no scheduled {promotion.name} verdict recorded")
            elif not set(verdicts) <= promotion.clean_verdicts:
                missing.append(f"{where}: verdict(s) {sorted(set(verdicts))} not clean")
            else:
                passed.append(where)
        if promotion.guard_class and unit_id in by_id:
            commissioning = read_guard_commissioning(
                store, by_id[unit_id], promotion.guard_class, trading_day=trading_day
            )
            evidence.extend(commissioning.evidence)
            if commissioning.unmeasurable:
                return Reading(
                    met=False,
                    detail=commissioning.detail,
                    evidence=tuple(evidence),
                    unmeasurable=True,
                    source=source,
                )
            if not commissioning.met:
                missing.append(f"{unit_id}: not commissioned — {commissioning.detail}")
    ready = not missing
    enforcing = promotion.mode == "enforce"
    tier = f"{promotion.risk} risk ({RISK_TIERS[promotion.risk]})"
    if ready and enforcing:
        verdict = "PROMOTED on its codified criterion"
    elif ready:
        verdict = (
            f"READY TO PROMOTE: the codified criterion holds; promotion is a deliberate PR "
            f"flipping {promotion.staging} to ENFORCE"
        )
    elif enforcing:
        verdict = "ENFORCING WITHOUT ITS CODIFIED EVIDENCE"
    else:
        verdict = "OBSERVING, criterion not yet met"
    detail = (
        f"{verdict}. {promotion.name} is {promotion.mode.upper()}; {tier}; blast radius: "
        f"{promotion.blast_radius}. {len(needed)} required unit(s); clean on the current "
        f"scheduled run: {len(passed)} path(s)"
    )
    if missing:
        detail += f"; missing: {missing[:6]}" + (f" (+{len(missing) - 6} more)" if len(missing) > 6 else "")
    if promotion.preconditions:
        detail += f"; declared preconditions the promotion PR confirms: {list(promotion.preconditions)}"
    return Reading(
        met=ready and enforcing,
        detail=detail,
        evidence=tuple(dict.fromkeys(evidence)),
        source=source,
    )
