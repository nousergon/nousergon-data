"""The data collector's red board: a `data` gate ladder over 46 audit units.

`alpha-engine-config` `private-docs/data_collection_plan_260914.md` §4.1, items
P-01 and P-02. Brian's framing, verbatim: *"we need to make sure we have a 'red
board' that we will light up with every component of the data collection to make
sure every aspect is accounted for, is being logged and is being monitored by a
console/dashboard."*

The engine — the clause state machine, the ladder document and its schema, fault
exclusion — lives in `nousergon_lib.gates`, shared with `crucible`. The clause
DEFINITIONS live here, because `architecture.d/146` rule 1 puts them in the repo
that owns the thing being graded.

What the board is made of, all generated from `registry.d/units/*.yaml`:

* 414 base clauses, one per audit cell (46 units x 9 scored columns);
* one guard clause per applicable audit §4.1 silent-degradation class;
* the objective/SLO clauses of plan §2;
* `data.inventory.writers_declared`, the AST reconciliation that notices a
  producer nobody registered.

**Red by default.** At birth every clause is red, including the 214 cells the
audit scored PRESENT. The phase-0 gate measures that the board is HONEST, not
that it is green.
"""

from __future__ import annotations

__all__ = ["__doc__"]
