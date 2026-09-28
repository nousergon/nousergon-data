"""The decoupled data cutover's instant, as ONE committed fact (alpha-engine-config-I11269).

Two readers depend on when the v1 state machines stopped running their own data
stages, and they must agree on it:

* ``data_gate/producers/v1_data_stage.py`` counts v1 data-stage executions
  STARTED at or after it (``data.phase1.v1_data_stage_quiet``,
  alpha-engine-config-I11265);
* ``data_gate/evidence.py::read_parity`` stops refreshing
  ``data.cutover_ready.parity`` after it: once v1 no longer writes the compared
  keys, a shadow-vs-v1 report compares the collector with itself, so the clause
  is FROZEN at the last report dated on or before the cutover's trading day.

**PLANNED, not measured — read this before merging the cutover PR.** The real
instant is the moment the cutover PR's definitions deploy, which is unknown
until it merges. Brian's hold (2026-09-21) sets the earliest merge window at a
weekday evening, 18:30-23:00 America/New_York; the first is Monday 2026-09-28.
``CUTOVER_UTC`` is the START of that window, and ``CUTOVER_STATUS`` says so.

Why the window START is the correct value for any merge inside that window, not
merely an approximation: every v1 state machine that could run a data stage
starts OUTSIDE it — preopen at 08:15 ET, postclose at ~16:00 ET, weekly on
Saturday at 09:00 UTC — so between 18:30 ET and a merge before 23:00 ET the
same day no v1 execution can start, and "started at or after CUTOVER_UTC" and
"started after the deploy" select the same executions. An execution that STARTED
before it and was still running at merge (the postclose SF on the old
definition) is correctly not counted: it began on the old definition.

**If the merge slips to another evening, change ``CUTOVER_UTC`` to that
evening's 18:30 ET in the same PR before merging.** Both failure directions are
LOUD, never vacuous:

* a stale value (too early) counts the slipped days' real v1 data-stage runs,
  so ``v1_data_stage_quiet`` reads non-zero and blocks the phase-1 exit — which
  is true: v1 data stages DID run after the declared instant;
* a future value is refused by the producer outright (a survey of a window that
  has not begun counts zero executions and would read quiet), and a document
  whose ``as_of`` precedes its own ``cutover_utc`` reads UNMEASURABLE.

``tests/test_cutover_instant.py`` pins the value to a weekday 18:30 ET instant,
so an edit that lands inside the trading day (where a v1 start could fall
between the declared instant and the deploy) fails review.
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

__all__ = [
    "CUTOVER_STATUS",
    "CUTOVER_UTC",
    "V1_STATE_MACHINE_NAMES",
    "cutover_instant",
    "cutover_trading_day",
]

#: The start of the planned merge window, Mon 2026-09-28 18:30 America/New_York
#: (EDT, UTC-4). See the module docstring before editing.
CUTOVER_UTC = "2026-09-28T22:30:00Z"

#: "planned": the start of the declared merge window, correct for any merge
#: inside it (module docstring). There is no "measured" state to flip to: once
#: merged inside the window, the planned value IS the instant both readers need.
CUTOVER_STATUS = "planned"

#: The three v1 state machines the decoupled cutover leaves RUNNING with their
#: data stages removed (Brian's ruling (b), 2026-09-21). The producer surveys
#: exactly these, and the reader refuses a document that does not name each.
V1_STATE_MACHINE_NAMES: tuple[str, ...] = (
    "ne-weekly-freshness-pipeline",
    "ne-preopen-trading-pipeline",
    "ne-postclose-trading-pipeline",
)

_ET = ZoneInfo("America/New_York")


def cutover_instant(value: str = CUTOVER_UTC) -> dt.datetime:
    """``value`` as an aware UTC datetime. RAISES on anything unparseable."""
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"cutover instant {value!r} carries no offset; it must be UTC")
    return parsed.astimezone(dt.timezone.utc)


def cutover_trading_day(value: str = CUTOVER_UTC) -> dt.date:
    """The America/New_York date the cutover happened on.

    A parity report is filed under the trading day it compares, and the shadow
    runs for that day compare against v1 output written BEFORE the evening
    cutover, so a report dated on or before this day is pre-cutover evidence and
    one dated after it is not.
    """
    return cutover_instant(value).astimezone(_ET).date()
