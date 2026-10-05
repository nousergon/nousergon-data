"""Per-guard, risk-based promotion — Brian's 2026-10-04 option (c), codified.

`data_gate/guard_promotion.py`. The adopted "10 consecutive clean cycles" is
replaced by a criterion codified per guard and scaled by blast radius, plus at
least one actual scheduled execution. Pinned here:

* the table is in lockstep with the code: every `GuardStaging` in the repo has
  an entry, and each entry's name and mode match its staging object;
* a guard is MET only when ENFORCING on evidence — READY-but-observing and
  enforcing-without-evidence both read UNMET, never green;
* the current scheduled run is the evidence: a missing, silent or unclean
  verdict on the latest due cycle blocks, and `unmeasurable` is never clean;
* no criterion is a clean-cycle count.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib
import re

import pytest

import dates
from data_gate import clauses as clause_module
from data_gate import exit_criteria as xc
from data_gate import guard_promotion as gp
from data_gate.descriptors import REPO_ROOT, load_units
from data_gate.read import evaluate, load_phases
from validators import expectations

from tests.data_gate_support import DeniedStore, EmptyStore, TRADING_DAY

_STAGINGS = {
    "validators/expectations.py::EMPTY_FRESH_GUARD": expectations.EMPTY_FRESH_GUARD,
    "validators/expectations.py::CARDINALITY_GUARD": expectations.CARDINALITY_GUARD,
    "dates.py::BAR_SETTLEMENT_GUARD": dates.BAR_SETTLEMENT_GUARD,
}


@pytest.fixture(scope="module")
def units():
    return load_units()


def _by_name(name: str) -> gp.GuardPromotion:
    return next(p for p in gp.GUARD_PROMOTIONS if p.name == name)


# --- lockstep with the code -----------------------------------------------------


def test_every_staged_guard_in_the_repo_has_a_codified_criterion():
    """A new `GuardStaging(` anywhere outside tests needs an entry, or this fails."""
    declared = set()
    for path in REPO_ROOT.rglob("*.py"):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel.startswith(("tests/", ".venv", "infrastructure/lambdas/")) or "/." in rel:
            continue
        for match in re.finditer(r"^([A-Z_]+)\s*=\s*GuardStaging\(", path.read_text(errors="ignore"), re.M):
            declared.add(f"{rel}::{match.group(1)}")
    assert declared == {p.staging for p in gp.GUARD_PROMOTIONS} == set(_STAGINGS)


@pytest.mark.parametrize("promotion", gp.GUARD_PROMOTIONS, ids=lambda p: p.name)
def test_each_entry_matches_its_staging_name_and_mode(promotion):
    staging = _STAGINGS[promotion.staging]
    assert promotion.name == staging.name
    assert promotion.mode == staging.mode.value, (
        f"{promotion.staging} is {staging.mode.value} but data_gate/guard_promotion.py says "
        f"{promotion.mode}: a promotion PR flips both"
    )
    assert "data_gate/guard_promotion.py" in staging.promotion_criterion
    assert promotion.tracked_issue == staging.tracked_issue


@pytest.mark.parametrize("promotion", gp.GUARD_PROMOTIONS, ids=lambda p: p.name)
def test_each_entry_is_risk_tiered_and_never_a_cycle_count(promotion):
    assert promotion.risk in gp.RISK_TIERS
    assert promotion.blast_radius.strip()
    assert "unmeasurable" not in promotion.clean_verdicts
    text = " ".join([promotion.blast_radius, *promotion.preconditions, gp.RISK_TIERS[promotion.risk]])
    assert not re.search(r"\b(10|ten)\b.*\b(consecutive|clean|cycles?|runs?|days?)\b", text, re.I)


def test_the_ruling_is_dated_and_cites_its_record():
    assert "Brian, 2026-10-04" in gp.PROMOTION_RULING
    assert "alpha-engine-config-I11973" in gp.PROMOTION_RULING
    assert "at least one actual scheduled observe execution" in gp.PROMOTION_RULING


def test_the_required_units_come_from_the_descriptors_records_as(units):
    assert gp.required_units(_by_name("data_cardinality"), units) == ["D20"]
    empty_fresh = gp.required_units(_by_name("data_empty_fresh"), units)
    assert len(empty_fresh) >= 20
    assert gp.required_units(_by_name("bar_settlement"), units) == ["D03", "D19"]


# --- the board ------------------------------------------------------------------


@pytest.fixture(scope="module")
def board(units):
    return clause_module.generate(EmptyStore(), units, load_phases(), trading_day=TRADING_DAY)


def test_one_graded_phase_2_row_per_staged_guard(board):
    rows = [c for c in board if c.name.startswith(gp.CLAUSE_PREFIX + ".")]
    assert {c.name for c in rows} == {gp.clause_name(p) for p in gp.GUARD_PROMOTIONS}
    phase2 = {
        c.name
        for c in evaluate(EmptyStore(), gate="data-phase2", trading_day=TRADING_DAY, all_clauses=board).clauses
    }
    for clause in rows:
        assert clause.phase == "data-phase2" and clause.name in phase2
        assert not clause.met, f"{clause.name} MET over an empty store"
        assert gp.PROMOTION_RULING in clause.requirement


# --- the reader -------------------------------------------------------------------


def _cycle_set(unit_id: str, verdicts: list[str] | None, *, guard: str, units) -> xc.CycleSet:
    """One schedule, one due cycle (the latest), one unit's scheduled manifest."""
    unit = next(u for u in units if u.unit_id == unit_id)
    result = xc.CycleSet(schedule="nousergon-data-collection/data-collection-eod", units=[unit])
    cycle = xc.Cycle(fire=dt.datetime(2026, 9, 14, 22, 15, tzinfo=dt.timezone.utc))
    cycle.manifests[unit_id] = []
    if verdicts is not None:
        cycle.manifests[unit_id].append(
            (
                "k",
                {
                    "trigger": "scheduled",
                    "status": "ok",
                    "guards": [{"guard": guard, "mode": "observe", "verdict": v} for v in verdicts],
                },
            )
        )
    result.cycles.append(cycle)
    return result


def _commissioned(unit_id: str, guard_class: str) -> dict[str, bytes]:
    return {
        f"faults/{unit_id}/{guard_class}/latest.json": json.dumps(
            {"outcome": "induced", "as_of": "2026-09-14"}
        ).encode()
    }


def _read(promotion, store, units, verdicts):
    sets = [_cycle_set("D20", verdicts, guard=promotion.name, units=units)]
    return gp.read_promotion(store, promotion, units, sets, trading_day=TRADING_DAY)


def test_ready_but_observing_reads_unmet_and_says_ready(units):
    promotion = _by_name("data_cardinality")
    reading = _read(promotion, EmptyStore(_commissioned("D20", "cardinality")), units, ["ok"])
    assert not reading.met and reading.detail.startswith("READY TO PROMOTE")


def test_promoted_on_evidence_is_met(units):
    promotion = dataclasses.replace(_by_name("data_cardinality"), mode="enforce")
    reading = _read(promotion, EmptyStore(_commissioned("D20", "cardinality")), units, ["ok"])
    assert reading.met and reading.detail.startswith("PROMOTED")


def test_enforcing_without_evidence_is_named(units):
    promotion = dataclasses.replace(_by_name("data_cardinality"), mode="enforce")
    reading = _read(promotion, EmptyStore(), units, ["ok"])
    assert not reading.met and reading.detail.startswith("ENFORCING WITHOUT")


@pytest.mark.parametrize(
    ("verdicts", "why"),
    [
        (None, "no scheduled"),
        ([], "no scheduled"),
        (["unmeasurable"], "not clean"),
        (["below_floor"], "not clean"),
    ],
)
def test_the_current_scheduled_run_must_be_clean(units, verdicts, why):
    promotion = _by_name("data_cardinality")
    reading = _read(promotion, EmptyStore(_commissioned("D20", "cardinality")), units, verdicts)
    assert not reading.met and why in reading.detail
    assert not reading.detail.startswith("READY")


def test_commissioning_is_required(units):
    reading = _read(_by_name("data_cardinality"), EmptyStore(), units, ["ok"])
    assert "not commissioned" in reading.detail and not reading.detail.startswith("READY")


def test_a_denied_commissioning_read_is_unmeasurable(units):
    reading = _read(_by_name("data_cardinality"), DeniedStore(), units, ["ok"])
    assert reading.unmeasurable and not reading.met


def test_a_unit_no_schedule_verifies_cannot_be_ready(units):
    promotion = _by_name("bar_settlement")
    reading = gp.read_promotion(EmptyStore(), promotion, units, [], trading_day=TRADING_DAY)
    assert not reading.met and "no declared schedule verifies" in reading.detail
    assert "declared preconditions" in reading.detail


def test_the_phases_file_grades_the_enforcing_half():
    text = pathlib.Path(REPO_ROOT, "data_gate/config/phases.yaml").read_text()
    assert "exit_measured_by: data.guard_promotion.*" in text
