"""The five calendar-floored phase 2-3 clauses are STANDING rows.

`alpha-engine-config-I11305`, applying Brian's 2026-09-21 time-gate ruling
(recorded on `alpha-engine-config-I10793`): a clause that can only become true
by the calendar advancing is read and rendered every day with its REAL state
against its ratified target, and counted in no data-phase gate.
`data_gate/clauses.py::CALENDAR_FLOOR_STANDING_CLAUSES` / `StandingClause`.

The properties pinned here, each from both sides:

* a standing row never moves a gate's verdict, whether it reads MET or UNMET;
* a standing row still renders its true state — MET only when the ORIGINAL
  ratified target is met, and absent data is never green.
"""

from __future__ import annotations

import dataclasses
import json

import pytest

from data_gate import clauses as clause_module
from data_gate import exit_criteria as xc
from data_gate.descriptors import load_units
from data_gate.read import GATES, _board_document, evaluate, load_phases

from tests.data_gate_support import DeniedStore, EmptyStore, TRADING_DAY

FIVE = {
    "data.phase2.executor_collection_writes_zero",
    "data.cost.monthly",
    "data.pages.monthly",
    "data.human_touch.monthly",
    "data.phase3.sustained_window",
}


@pytest.fixture(scope="module")
def board():
    return clause_module.generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)


def _doc_store(key: str, document: dict) -> EmptyStore:
    return EmptyStore({key: json.dumps(document).encode()})


# --- scope ------------------------------------------------------------------


def test_exactly_the_five_named_by_i11305_are_calendar_floor_standing(board):
    """Named, not derived, so adding or dropping one is a visible edit here."""
    assert set(clause_module.CALENDAR_FLOOR_STANDING_CLAUSES) == FIVE
    by_name = {c.name: c for c in board}
    for name in FIVE:
        clause = by_name[name]
        assert clause_module.is_standing(clause), name
        assert not clause_module.is_observation_window(clause), name
        assert clause.ruling == clause_module.CALENDAR_FLOOR_STANDING_RULING, name
    ruling = clause_module.CALENDAR_FLOOR_STANDING_RULING
    assert "Brian, 2026-09-21" in ruling
    assert "alpha-engine-config-I11305" in ruling


def test_the_five_keep_their_phase_tag(board):
    """Rendered in their own phase's standing section, not moved to another gate."""
    by_name = {c.name: c for c in board}
    assert by_name["data.phase2.executor_collection_writes_zero"].phase == "data-phase2"
    for name in FIVE - {"data.phase2.executor_collection_writes_zero"}:
        assert by_name[name].phase == "data-phase3", name


def test_no_numbered_gate_grades_any_of_the_five(board):
    for gate, ceiling in GATES.items():
        if ceiling is None:
            continue
        graded = {
            c.name for c in evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board).clauses
        }
        assert not graded & FIVE, (gate, sorted(graded & FIVE))


def test_no_phase_exit_criterion_names_any_of_the_five():
    import fnmatch

    for phase in load_phases():
        for criterion in phase.exit_criteria:
            for pattern in criterion.patterns:
                assert not fnmatch.filter(sorted(FIVE), pattern), (phase.id, criterion.text)


def test_the_migration_half_stays_gating_in_phase_2(board):
    """I11305 deliverable 2: only the 7-day WINDOW is standing; the boxes running
    under their own workload identity (I10756) is still a phase-2 exit, graded
    per unit by the `identity` column."""
    graded = {
        c.name
        for c in evaluate(EmptyStore(), gate="data-phase2", trading_day=TRADING_DAY, all_clauses=board).clauses
    }
    identity = [n for n in graded if n.endswith(".identity")]
    assert identity, "no per-unit identity clause is graded by data-phase2"


# --- a standing row never moves a gate ---------------------------------------


@pytest.mark.parametrize("gate", ["data-phase2", "data-phase3"])
@pytest.mark.parametrize("forced", [True, False])
def test_a_standing_row_never_moves_a_gates_verdict(board, gate, forced):
    """Flip the five to MET (or UNMET): the gate's clause set and verdict are unchanged."""
    flipped = [
        dataclasses.replace(c, met=forced, unmeasurable=False) if c.name in FIVE else c for c in board
    ]
    before = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board)
    after = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=flipped)
    assert [c.name for c in before.clauses] == [c.name for c in after.clauses]
    assert before.met == after.met


def test_the_board_counts_them_as_standing_not_graded(board):
    document = _board_document(
        board, trading_day=TRADING_DAY, generated_utc="2026-10-04T00:00:00Z", store_uri=None
    )
    rows = {r["clause"]: r for r in document["rows"]}
    for name in FIVE:
        assert rows[name]["standing"] is True, name
        assert rows[name]["standing_ruling"] == clause_module.CALENDAR_FLOOR_STANDING_RULING
        assert rows[name]["state"] in {"UNMET", "UNMEASURABLE"}, (name, rows[name]["state"])


# --- each row renders its true state ------------------------------------------


def test_executor_writes_row_reads_its_ratified_target():
    build = clause_module._clause_phase2_executor_collection_writes_zero
    key = xc.EXECUTOR_WRITES_KEY
    assert build(_doc_store(key, {"collection_writes": 0, "days_covered": 7})).met
    clause = build(_doc_store(key, {"collection_writes": 0, "days_covered": 3}))
    assert not clause.met, "3 of 7 days is not the ratified 7-day window"
    assert not build(_doc_store(key, {"collection_writes": 5, "days_covered": 7})).met
    absent = build(EmptyStore())
    assert not absent.met
    denied = build(DeniedStore())
    assert denied.unmeasurable and not denied.met


@pytest.mark.parametrize(
    ("fn", "key"),
    [
        ("_clause_cost_monthly", "metrics/cost/monthly/latest.json"),
        ("_clause_pages_monthly", "metrics/pages/monthly/latest.json"),
        ("_clause_human_touch_monthly", "metrics/human_touch/monthly/latest.json"),
    ],
)
def test_monthly_rows_read_their_ratified_target(fn, key):
    build = getattr(clause_module, fn)
    full = build(_doc_store(key, {"status": "ok", "days_observed": 31, "days_in_month": 31}))
    assert full.met, full.detail
    partial = build(_doc_store(key, {"status": "ok", "days_observed": 4, "days_in_month": 31}))
    assert not partial.met and "full calendar month" in partial.detail
    assert not build(_doc_store(key, {"status": "breach", "days_observed": 31, "days_in_month": 31})).met
    assert not build(_doc_store(key, {"status": ""})).met
    assert not build(EmptyStore()).met
    assert build(DeniedStore()).unmeasurable


def _sustain(clean_days: int, saturdays: int):
    from nousergon_lib.trading_calendar import subtract_trading_days  # pyright: ignore[reportAttributeAccessIssue]

    from tests.test_observation_window_ruling import _cycles

    objects: dict[str, bytes] = {}
    day = TRADING_DAY
    for _ in range(clean_days):
        rows = [{"name": "data.x", "met": True, "unmeasurable": False}]
        objects[f"gates/data-phase3/{day.isoformat()}/gate.json"] = json.dumps({"clauses": rows}).encode()
        day = subtract_trading_days(day, 1)
    weekly = _cycles([["ok"]] * saturdays, guard="empty_fresh")
    return clause_module._clause_phase3_sustained_window(EmptyStore(objects), weekly, trading_day=TRADING_DAY)


def test_sustain_row_reads_its_ratified_target():
    assert not _sustain(0, 0).met
    partial = _sustain(3, 1)
    assert not partial.met, "3 clean days is not the ratified 20-trading-day sustain"
    full = _sustain(xc.PHASE3_TRADING_DAYS, xc.PHASE3_SATURDAYS)
    assert full.met, full.detail
