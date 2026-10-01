"""Phase 0's audit clauses grade the audit's own units, not units registered later.

D48 and D49 were registered on 2026-09-21 with ``audit.baseline_date: 2026-09-21``.
Counting them against the 2026-09-14 audit's fixed totals (46 units, 414 cells)
turned data-phase0 red from 2026-09-21 for registering MORE than the audit saw.
"""

from __future__ import annotations

import copy
import dataclasses

from data_gate import clauses
from data_gate.descriptors import load_units


def test_audit_clauses_are_met_on_the_committed_descriptors():
    units = load_units()
    assert clauses._clause_board_population_complete(None, units).met
    assert clauses._clause_board_cells_reconciled(None, units).met


def test_a_unit_added_after_the_audit_is_named_not_counted():
    units = load_units()
    pop = clauses._clause_board_population_complete(None, units)
    assert "registered after the 2026-09-14 audit" in pop.detail
    assert "D48" in pop.detail and "D49" in pop.detail


def test_losing_an_audit_unit_still_fails():
    units = load_units()
    audited = [
        u for u in units if str(u.raw["audit"]["baseline_date"]) == clauses.AUDIT_BASELINE_DATE
    ]
    dropped = [u for u in units if u is not audited[0]]
    assert not clauses._clause_board_population_complete(None, dropped).met
    assert not clauses._clause_board_cells_reconciled(None, dropped).met


def test_a_relabelled_unit_cannot_hide_a_transcription_slip():
    """Moving an audit unit's baseline date does not keep the count green."""
    units = load_units()
    victim = next(
        u for u in units if str(u.raw["audit"]["baseline_date"]) == clauses.AUDIT_BASELINE_DATE
    )
    raw = copy.deepcopy(victim.raw)
    raw["audit"]["baseline_date"] = "2026-09-21"
    relabelled = [dataclasses.replace(u, raw=raw) if u is victim else u for u in units]
    assert not clauses._clause_board_population_complete(None, relabelled).met
