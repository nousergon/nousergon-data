"""Brian's 2026-10-04 option (c), graded: version-bound evidence gates release.

Ruling recorded on `alpha-engine-config-I11973` (comment 5983159736): release
acceptance is gated by CURRENT, VERSION-BOUND evidence. Any current failure, or
stale or missing proof, blocks; a verified remedy does not wait for old
failures to age out; the long window stays visible as an observation.
`data_gate/clauses.py::VERSION_BOUND_ACCEPTANCE_RULING`.

Each property is pinned from both sides:

* a failure on the code running now blocks, and a failure on an EARLIER code
  version does not — but stays on the row;
* missing proof (the newest due observation recorded nothing) blocks;
* an unknown version binds only the newest observation, never a longer run;
* the five long windows option (c) makes STANDING (nousergon-data PR2047) keep
  the 2026-10-03 rule until that lands — stricter, never laxer.
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
from data_gate.read import _board_document, load_phases

from tests.data_gate_support import EmptyStore, TRADING_DAY


@pytest.fixture(scope="module")
def board():
    return clause_module.generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)


# --- the helper -------------------------------------------------------------


def _obs(label: str, version: str = "", failure: str | None = None, present: bool = True):
    return ev.Observation(label=label, version=version, failure=failure, present=present)


def test_no_observation_is_stale_never_clean():
    current = ev.current_evidence([])
    assert current.stale and not current.clean


def test_a_missing_newest_observation_is_stale():
    current = ev.current_evidence([_obs("d3", present=False), _obs("d2", "abc"), _obs("d1", "abc")])
    assert current.stale and current.observed == 0 and not current.clean


def test_the_run_is_the_newest_version_back_to_the_first_other_version():
    current = ev.current_evidence(
        [_obs("d4", "new"), _obs("d3", "new"), _obs("d2", "old", failure="RED"), _obs("d1", "new")]
    )
    assert current.observed == 2 and current.version == "new"
    assert current.clean, "the RED day ran other code — a remedy does not wait for it"


def test_a_failure_on_the_current_version_blocks_even_when_older():
    current = ev.current_evidence(
        [_obs("d3", "abc"), _obs("d2", "abc", failure="RED"), _obs("d1", "old")]
    )
    assert not current.clean and current.failures == ("d2: RED",)


def test_an_unknown_version_binds_only_the_newest_observation():
    current = ev.current_evidence([_obs("d2"), _obs("d1", failure="RED")])
    assert current.observed == 1 and current.clean
    current = ev.current_evidence([_obs("d2", failure="RED"), _obs("d1")])
    assert not current.clean


def test_a_missing_observation_ends_the_run():
    current = ev.current_evidence(
        [_obs("d3", "abc"), _obs("d2", present=False), _obs("d1", "abc", failure="RED")]
    )
    assert current.observed == 1 and current.clean


def test_combined_paths_each_must_be_clean():
    clean = ev.current_evidence([_obs("c1", "abc")])
    dirty = ev.current_evidence([_obs("c1", "abc", failure="no verdict")])
    stale = ev.current_evidence([_obs("c1", present=False)])
    assert ev.combine_current([("eod", clean), ("morning", clean)]).clean
    assert not ev.combine_current([("eod", clean), ("morning", dirty)]).clean
    assert not ev.combine_current([("eod", clean), ("morning", stale)]).clean
    assert ev.combine_current([]) is None


# --- the cycle readers bind to code_sha -------------------------------------


def _cycles(rows: list[tuple[list[str] | None, str]], *, guard: str, unit_id: str = "D19") -> xc.CycleSet:
    """Newest first. Each row: (verdicts or None for a cycle that recorded nothing, code_sha)."""
    units = [u for u in load_units() if u.unit_id == unit_id]
    result = xc.CycleSet(schedule="nousergon-data-collection/data-collection-eod", units=units)
    base = dt.datetime(2026, 9, 14, 22, 15, tzinfo=dt.timezone.utc)
    for i, (verdicts, sha) in enumerate(rows):
        cycle = xc.Cycle(fire=base - dt.timedelta(days=i))
        cycle.manifests[unit_id] = []
        if verdicts is not None:
            doc = {
                "run_id": f"run{i}",
                "trigger": "scheduled",
                "status": "ok",
                "code_sha": sha,
                "guards": [{"guard": guard, "verdict": v} for v in verdicts],
            }
            cycle.manifests[unit_id].append((f"k{i}", doc))
        result.cycles.append(cycle)
    return result


def test_empty_fresh_on_the_same_code_still_counts_an_older_failure():
    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([(["ok"], "abc"), (["empty_fresh"], "abc"), (["ok"], "old")], guard="empty_fresh")
    )
    assert not clause.met and "CURRENT FAILURE" in clause.detail
    assert clause.current is not None and clause.current.version == "abc"


def test_empty_fresh_after_a_code_change_does_not_wait_for_the_old_failure():
    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([(["ok"], "fix"), (["empty_fresh"], "old"), (["ok"], "old")], guard="empty_fresh")
    )
    assert clause.met, clause.detail
    assert clause.window_failures and not clause.window_complete


def test_empty_fresh_blocks_when_the_current_cycle_is_silent():
    """Live non-silent telemetry: manifests with no `empty_fresh` verdict are a failure."""
    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([([], "abc"), (["ok"], "abc")], guard="empty_fresh")
    )
    assert not clause.met and "silent" in clause.detail


def test_empty_fresh_blocks_when_the_current_cycle_recorded_nothing():
    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([(None, ""), (["ok"], "abc")], guard="empty_fresh")
    )
    assert not clause.met and "STALE" in clause.detail


def test_vendor_divergence_binds_to_the_vendor_units_code():
    clause = clause_module._clause_phase2_vendor_divergence_emitted(
        [_cycles([(["ok"], "fix"), (["unmeasurable"], "old")], guard=xc.VENDOR_GUARD)]
    )
    assert clause.met, clause.detail
    clause = clause_module._clause_phase2_vendor_divergence_emitted(
        [_cycles([(["ok"], "abc"), (["unmeasurable"], "abc")], guard=xc.VENDOR_GUARD)]
    )
    assert not clause.met and "CURRENT FAILURE" in clause.detail


# --- the freshness SLO reads the producer's per-cycle rows ------------------


def _slo(document: dict) -> EmptyStore:
    return EmptyStore({"metrics/slo/freshness/eod-spine/latest.json": json.dumps(document).encode()})


def test_freshness_slo_with_cycle_rows_grades_the_newest_observed_cycle():
    rows = [
        {"trading_day": "2026-09-10", "observed": True, "met": False, "misses": {"D20": "late"}},
        {"trading_day": "2026-09-11", "observed": True, "met": True, "misses": {}},
        {"trading_day": "2026-09-14", "observed": False, "met": False, "misses": {}},
    ]
    clause = clause_module._clause_slo_freshness(
        _slo({"status": "breach", "value": 1, "cycles_observed": 2, "cycles": rows}), "eod-spine"
    )
    assert clause.met, clause.detail
    assert clause.current is not None and clause.current.latest == "2026-09-11"
    assert clause.window_failures, "the window's breach stays on the row"


def test_freshness_slo_blocks_when_the_newest_observed_cycle_missed():
    rows = [
        {"trading_day": "2026-09-11", "observed": True, "met": True},
        {"trading_day": "2026-09-14", "observed": True, "met": False, "misses": {"D20": "late"}},
    ]
    clause = clause_module._clause_slo_freshness(
        _slo({"status": "breach", "cycles_observed": 2, "cycles": rows}), "eod-spine"
    )
    assert not clause.met and "CURRENT FAILURE" in clause.detail


def test_freshness_slo_without_cycle_rows_keeps_the_stricter_window_rule():
    clause = clause_module._clause_slo_freshness(
        _slo({"status": "breach", "cycles_observed": 4}), "eod-spine"
    )
    assert not clause.met and "REOPENED" in clause.detail
    assert "stricter" in clause.detail


# --- scope -------------------------------------------------------------------


def test_exactly_the_eleven_current_evidence_clauses_are_version_bound(board):
    units = load_units()
    families = sorted({u.freshness_family for u in units if u.freshness_family})
    expected = {
        "data.phase2.eod_universe_covered",
        "data.phase2.empty_fresh_free",
        "data.phase2.vendor_divergence_emitted",
    } | {f"data.slo.freshness.{f}" for f in families}
    bound = {
        c.name
        for c in board
        if clause_module.is_observation_window(c)
        and c.ruling == clause_module.VERSION_BOUND_ACCEPTANCE_RULING
    }
    assert bound == expected and len(bound) == 11
    assert {
        c.name
        for c in board
        if any(fnmatch.fnmatch(c.name, p) for p in clause_module.VERSION_BOUND_CLAUSE_PATTERNS)
    } == expected


@pytest.mark.parametrize(
    "name",
    [
        "data.phase2.executor_collection_writes_zero",
        "data.cost.monthly",
        "data.pages.monthly",
        "data.human_touch.monthly",
        "data.phase3.sustained_window",
    ],
)
def test_the_five_long_windows_are_untouched_by_this_change(board, name):
    """Option (c) makes these STANDING; that conversion is PR2047's, not this one's."""
    clause = next(c for c in board if c.name == name)
    assert getattr(clause, "current", None) is None
    assert getattr(clause, "ruling", "") != clause_module.VERSION_BOUND_ACCEPTANCE_RULING


def test_the_ruling_is_dated_and_cites_its_record():
    ruling = clause_module.VERSION_BOUND_ACCEPTANCE_RULING
    assert "Brian, 2026-10-04" in ruling and "option (c)" in ruling
    assert "alpha-engine-config-I11973" in ruling
    # The 2026-10-03 ruling it amends stays recorded, verbatim and unchanged.
    assert "Brian, 2026-10-03" in clause_module.OBSERVATION_WINDOW_RULING


def test_nothing_version_bound_is_met_over_an_empty_store(board):
    green = [
        c.name
        for c in board
        if getattr(c, "ruling", "") == clause_module.VERSION_BOUND_ACCEPTANCE_RULING and c.met
    ]
    assert not green


# --- rendering ----------------------------------------------------------------


def test_the_board_row_names_what_gated_it_and_keeps_the_window():
    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([(["ok"], "fix"), (["empty_fresh"], "old")], guard="empty_fresh")
    )
    document = _board_document(
        [clause], trading_day=TRADING_DAY, generated_utc="2026-10-05T00:00:00Z", store_uri=None
    )
    window = document["rows"][0]["observation_window"]
    assert window["acceptance_ruling"] == clause_module.VERSION_BOUND_ACCEPTANCE_RULING
    assert window["ruling"] == clause_module.OBSERVATION_WINDOW_RULING
    assert window["current"]["version"] == "fix" and window["current"]["observed"] == 1
    assert window["failures"], "the long window's failure stays visible"
    assert document["rows"][0]["state"] == "MET"


def test_empty_fresh_reads_the_guard_under_the_name_the_producer_records():
    """`data_empty_fresh` is what `weekly_collector` files; the class spelling is a fixture's."""
    from validators import expectations

    clause = clause_module._clause_phase2_empty_fresh_free(
        _cycles([(["ok"], "abc")], guard=expectations.EMPTY_FRESH_GUARD.name)
    )
    assert clause.met, clause.detail
    assert "NOT LIVE" not in clause.detail
