"""Follow a rehearsal deadline checkpoint to the stage it guards.

alpha-engine-config-I12351 (B): before each long stage of the weekly SF, a
REHEARSAL reads the clock (``RehearsalClockBefore<Stage>``, a Pass) and a
Choice (``CheckRehearsalDeadlineBefore<Stage>``) stops it once it is past
``$.rehearsal_deadline``. Both sit on the gate's Default edge, and the
Choice's Default is the stage, so for production the RUN path is unchanged.
Wiring tests that pin "gate X defaults to running stage S" follow the pair
through this helper instead of pinning its first hop.
"""

from __future__ import annotations

CLOCK_PREFIX = "RehearsalClockBefore"
CHECK_PREFIX = "CheckRehearsalDeadlineBefore"


def past_rehearsal_checkpoint(states: dict, name: str) -> str:
    """``name`` itself, or — when it is a checkpoint's clock Pass — the stage
    that checkpoint guards (asserting the pair's shape on the way)."""
    if not name.startswith(CLOCK_PREFIX):
        return name
    stage = name[len(CLOCK_PREFIX):]
    check = states[name]["Next"]
    assert check == f"{CHECK_PREFIX}{stage}", (name, check)
    assert states[check]["Type"] == "Choice"
    assert states[check]["Default"] == stage, (check, states[check]["Default"])
    return stage
