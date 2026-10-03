"""Brian's 2026-10-03 extension of the time-gate ruling, graded.

Decision card ``cmsg_01N3B1WSaV2QaHy37cD37vyxEhCi3VxEhH8FEwvdjdfxfG`` ("Extend
ruling"), extending his 2026-09-21 ruling (`alpha-engine-config-I11305`): a
phase-2/3 clause that counts elapsed time passes once its check is BUILT and
LIVE, the observation window runs to completion behind it, and a later failed
observation reopens it. `data_gate/clauses.py::ObservationWindowClause`.

The two properties that must stay true, each pinned below from both sides:

* a clause passes only once its check has produced a real reading — never on
  an empty store, a missing document, or an UNMEASURABLE read;
* a failed observation anywhere in the trailing window reopens it.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import json

import pytest

from data_gate import clauses as clause_module
from data_gate import evidence as ev
from data_gate import exit_criteria as xc
from data_gate.descriptors import load_units
from data_gate.read import _board_document, evaluate, load_phases

from tests.data_gate_support import DeniedStore, EmptyStore, TRADING_DAY


@pytest.fixture(scope="module")
def units():
    return load_units()


@pytest.fixture(scope="module")
def board(units):
    return clause_module.generate(EmptyStore(), units, load_phases(), trading_day=TRADING_DAY)


def _ruled(board):
    return [
        c
        for c in board
        if any(fnmatch.fnmatch(c.name, p) for p in clause_module.OBSERVATION_WINDOW_CLAUSE_PATTERNS)
    ]


# --- scope ------------------------------------------------------------------


def test_the_ruling_covers_exactly_the_time_counting_clauses_i11305_does_not(board, units):
    """3 phase-2 counters + one freshness SLO per family; nothing else.

    Named rather than derived from the wrapper type, so dropping a clause from
    the ruling (or adding one) is a visible edit here.
    """
    families = sorted({u.freshness_family for u in units if u.freshness_family})
    expected = {
        "data.phase2.eod_universe_covered",
        "data.phase2.empty_fresh_free",
        "data.phase2.vendor_divergence_emitted",
    } | {f"data.slo.freshness.{family}" for family in families}
    ruled = {c.name for c in board if clause_module.is_observation_window(c)}
    assert ruled == expected
    assert {c.name for c in _ruled(board)} == expected
    # 2026-10-03: eight families, so eleven clauses.
    assert len(families) == 8 and len(ruled) == 11


def test_i11305s_five_and_the_completeness_slos_are_not_converted_here(board):
    by_name = {c.name: c for c in board}
    untouched = [
        "data.phase2.executor_collection_writes_zero",
        "data.cost.monthly",
        "data.pages.monthly",
        "data.human_touch.monthly",
        "data.phase3.sustained_window",
    ] + [n for n in by_name if n.startswith("data.slo.completeness.")]
    for name in untouched:
        assert not clause_module.is_observation_window(by_name[name]), name


def test_ruled_clauses_stay_graded_by_their_phases_gate(board):
    """Unlike a STANDING clause, the ruling never removes a clause from a gate."""
    for clause in _ruled(board):
        assert not clause_module.is_ungraded(clause), clause.name
    phase2, phase3 = (
        {c.name for c in evaluate(EmptyStore(), gate=g, trading_day=TRADING_DAY, all_clauses=board).clauses}
        for g in ("data-phase2", "data-phase3")
    )
    for clause in _ruled(board):
        assert clause.name in phase3
        if clause.phase == "data-phase2":
            assert clause.name in phase2


def test_every_ruled_clause_carries_the_ruling_with_its_date_and_decision(board):
    for clause in _ruled(board):
        assert clause.ruling == clause_module.OBSERVATION_WINDOW_RULING
    ruling = clause_module.OBSERVATION_WINDOW_RULING
    assert "Brian, 2026-10-03" in ruling
    assert "cmsg_01N3B1WSaV2QaHy37cD37vyxEhCi3VxEhH8FEwvdjdfxfG" in ruling
    assert "alpha-engine-config-I11305" in ruling


# --- invariant 1: built AND live --------------------------------------------


def test_nothing_ruled_is_met_over_an_empty_store(board):
    green = [c.name for c in _ruled(board) if c.met]
    assert not green, f"MET with no check live: {green}"
    for clause in _ruled(board):
        assert "NOT LIVE" in clause.detail, clause.name


def test_a_denied_store_is_unmeasurable_never_met(units):
    clause = clause_module._clause_slo_freshness(DeniedStore(), "eod-spine")
    assert clause.unmeasurable and not clause.met
    clause = clause_module._clause_phase2_eod_universe_covered(DeniedStore(), trading_day=TRADING_DAY)
    assert clause.unmeasurable and not clause.met


def test_a_reading_with_no_window_fails_closed():
    reading = ev.Reading(met=True, detail="a reader that forgot its window", evidence=("k",))
    clause = clause_module._observation_window_clause("data.x", "req", reading, phase="data-phase2")
    assert not clause.met
    assert "fails closed" in clause.detail


# --- eod_universe_covered ---------------------------------------------------


def _completeness_store(statuses_newest_first: list[str | None]) -> EmptyStore:
    objects: dict[str, bytes] = {}
    day = TRADING_DAY
    from nousergon_lib.trading_calendar import subtract_trading_days  # pyright: ignore[reportAttributeAccessIssue]

    for status in statuses_newest_first:
        if status is not None:
            objects[f"metrics/eod_completeness/{day.isoformat()}.json"] = json.dumps(
                {"status": status}
            ).encode()
        day = subtract_trading_days(day, 1)
    return EmptyStore(objects)


def test_eod_coverage_passes_on_a_partial_clean_window():
    store = _completeness_store(["GREEN", "GREEN", "GREEN"])  # 3 of 10, nothing older
    clause = clause_module._clause_phase2_eod_universe_covered(store, trading_day=TRADING_DAY)
    assert clause.met, clause.detail
    assert clause.window_observed == 3 and clause.window_required == 10
    assert not clause.window_complete, "the ORIGINAL 10-day criterion is not yet met"


def test_eod_coverage_reopens_on_a_red_day_inside_the_window():
    store = _completeness_store(["GREEN", "RED", "GREEN"])
    clause = clause_module._clause_phase2_eod_universe_covered(store, trading_day=TRADING_DAY)
    assert not clause.met and "REOPENED" in clause.detail


def test_eod_coverage_treats_a_missing_day_after_going_live_as_a_failure():
    """A missing measurement is not a passing one — once the guard is live."""
    store = _completeness_store([None, "GREEN", "GREEN"])
    clause = clause_module._clause_phase2_eod_universe_covered(store, trading_day=TRADING_DAY)
    assert not clause.met and "ABSENT" in clause.detail


def test_eod_coverage_full_clean_window_is_also_the_original_verdict():
    store = _completeness_store(["GREEN"] * 10)
    clause = clause_module._clause_phase2_eod_universe_covered(store, trading_day=TRADING_DAY)
    assert clause.met and clause.window_complete


# --- empty_fresh_free -------------------------------------------------------


def _cycles(verdict_sets: list[list[str] | None], *, guard: str, unit_id: str = "D19") -> xc.CycleSet:
    """Newest first, like `collect_cycles`; ``None`` is a cycle that recorded nothing."""
    units = [u for u in load_units() if u.unit_id == unit_id]
    result = xc.CycleSet(schedule="nousergon-data-collection/data-collection-eod", units=units)
    base = dt.datetime(2026, 9, 14, 22, 15, tzinfo=dt.timezone.utc)
    for i, verdicts in enumerate(verdict_sets):
        cycle = xc.Cycle(fire=base - dt.timedelta(days=i))
        cycle.manifests[unit_id] = []
        if verdicts is not None:
            doc = {
                "run_id": f"run{i}",
                "trigger": "scheduled",
                "status": "ok",
                "guards": [{"guard": guard, "verdict": v} for v in verdicts],
            }
            cycle.manifests[unit_id].append((f"k{i}", doc))
        result.cycles.append(cycle)
    return result


def test_empty_fresh_is_not_live_until_the_guard_records_a_verdict():
    clause = clause_module._clause_phase2_empty_fresh_free(_cycles([[], [], []], guard="empty_fresh"))
    assert not clause.met and "NOT LIVE" in clause.detail


def test_empty_fresh_passes_once_the_guard_is_live_and_clean():
    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([["ok"], ["not_applicable"], ["ok"]], guard="empty_fresh")
    )
    assert clause.met, clause.detail
    assert clause.window_observed == 3 and not clause.window_complete


def test_empty_fresh_reopens_on_one_empty_write_in_the_window():
    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([["ok"], ["empty_fresh"], ["ok"]], guard="empty_fresh")
    )
    assert not clause.met and "REOPENED" in clause.detail


# --- vendor_divergence_emitted ----------------------------------------------


def _vendor_sets(eod_verdicts: list[list[str] | None]) -> list[xc.CycleSet]:
    eod = _cycles(eod_verdicts, guard=xc.VENDOR_GUARD, unit_id="D19")
    return [eod]


def test_vendor_divergence_is_not_live_until_a_verdict_is_emitted():
    clause = clause_module._clause_phase2_vendor_divergence_emitted(_vendor_sets([None, None]))
    assert not clause.met and "NOT LIVE" in clause.detail


def test_vendor_divergence_forgives_silence_before_the_guard_shipped_only():
    # Newest first: two emitted cycles, then three silent ones from before the guard existed.
    clause = clause_module._clause_phase2_vendor_divergence_emitted(
        _vendor_sets([["ok"], ["breach_named"], None, None, None])
    )
    assert clause.met, clause.detail


def test_vendor_divergence_reopens_on_a_silent_cycle_after_going_live():
    clause = clause_module._clause_phase2_vendor_divergence_emitted(
        _vendor_sets([None, ["ok"], ["ok"]])
    )
    assert not clause.met and "REOPENED" in clause.detail


def test_vendor_divergence_reopens_on_a_blind_cycle_after_going_live():
    clause = clause_module._clause_phase2_vendor_divergence_emitted(
        _vendor_sets([["unmeasurable"], ["ok"]])
    )
    assert not clause.met and "REOPENED" in clause.detail


# --- freshness SLO ----------------------------------------------------------


def _slo_store(document: dict | None) -> EmptyStore:
    if document is None:
        return EmptyStore()
    return EmptyStore(
        {"metrics/slo/freshness/eod-spine/latest.json": json.dumps(document).encode()}
    )


def test_freshness_slo_passes_on_a_live_ok_verdict_over_a_partial_window():
    clause = clause_module._clause_slo_freshness(
        _slo_store({"status": "ok", "value": 1.0, "cycles_observed": 4}), "eod-spine"
    )
    assert clause.met, clause.detail
    assert clause.window_observed == 4 and not clause.window_complete


def test_freshness_slo_reopens_on_breach():
    clause = clause_module._clause_slo_freshness(
        _slo_store({"status": "breach", "value": 0.8, "cycles_observed": 6}), "eod-spine"
    )
    assert not clause.met and "REOPENED" in clause.detail


def test_freshness_slo_with_an_undefined_status_is_not_live():
    clause = clause_module._clause_slo_freshness(_slo_store({"status": ""}), "eod-spine")
    assert not clause.met and "NOT LIVE" in clause.detail


# --- rendering --------------------------------------------------------------


def test_board_rows_keep_the_window_and_the_strict_answer_visible(board):
    document = _board_document(
        board, trading_day=TRADING_DAY, generated_utc="2026-10-03T00:00:00Z", store_uri=None
    )
    rows = {r["clause"]: r for r in document["rows"]}
    for clause in _ruled(board):
        window = rows[clause.name]["observation_window"]
        assert window["ruling"] == clause_module.OBSERVATION_WINDOW_RULING
        assert window["complete"] is False
        assert window["required"] > 0
