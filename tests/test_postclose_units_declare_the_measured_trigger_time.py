"""Every unit owned by `ne-postclose-trading-pipeline` declares the time that
pipeline actually fires.

`alpha-engine-config-I11189`. Fourteen descriptors declared
`trigger.schedule: "weekdays 16:45 America/New_York"` while the pipeline they
all name as `trigger.owner` starts at **16:00 ET**. Measured 2026-09-20 from
`ne-postclose-trading-pipeline`'s own execution history, four consecutive
weekdays, every start within 40 s of the top of the hour::

    eod-2026-09-18-1789761639  SUCCEEDED  2026-09-18T13:00:39-07:00  (20:00:39Z)
    eod-2026-09-17-1789675210  SUCCEEDED  2026-09-17T13:00:10-07:00
    eod-2026-09-16-1789588833  SUCCEEDED  2026-09-16T13:00:33-07:00
    eod-2026-09-15-1789502415  SUCCEEDED  2026-09-15T13:00:15-07:00

The 45-minute error was not cosmetic. `data_gate/evidence.py::_cycle` grades a
scheduled unit by selecting only manifests whose ``started`` falls at or after
the DECLARED fire instant, so every stage inside the SF started before its own
declared due time, was discarded, and thirteen units read `run_record` UNMET
while holding clean `ok` manifests with real row counts.

**What this test can and cannot do.** It pins the declared value against the
measurement above, so the 45-minute drift cannot silently return in an edit.
It does NOT reconcile the declaration against the live trigger — nothing in
this repository can, because the reconciliation has to read a live AWS surface
and a gate reads published artifacts rather than running a survey of its own
(`alpha-engine-config-I11035`). That is deliverable 2 of I11189 and it stays
open; this test is the interim floor, not the fix for the class. Do not read a
green here as "declared and live agree" — it means "declared has not moved away
from what was measured on 2026-09-20".
"""

from __future__ import annotations

from data_gate.descriptors import load_units

#: The owning pipeline whose fire time the units below inherit.
POSTCLOSE_OWNER = "ne-postclose-trading-pipeline"

#: Measured 2026-09-20 from the pipeline's execution history (see module
#: docstring). A constant, not a lookup: a value derived from the thing it
#: checks would check nothing.
MEASURED_SCHEDULE = "weekdays 16:00 America/New_York"


def _postclose_units():
    return [u for u in load_units() if (u.raw.get("trigger") or {}).get("owner") == POSTCLOSE_OWNER]


def test_there_are_postclose_owned_units():
    """Non-vacuity guard: if the owner string is ever renamed, the assertion
    below would iterate nothing and report green over an empty set."""
    units = _postclose_units()
    assert units, f"no descriptor declares trigger.owner == {POSTCLOSE_OWNER!r}"
    assert len(units) >= 14, (
        f"expected at least the 14 units measured on 2026-09-20, found {len(units)}: "
        f"{sorted(u.unit_id for u in units)}"
    )


def test_every_postclose_unit_declares_the_measured_fire_time():
    wrong = {
        u.unit_id: u.raw["trigger"].get("schedule")
        for u in _postclose_units()
        if u.raw["trigger"].get("schedule") != MEASURED_SCHEDULE
    }
    assert not wrong, (
        f"these units are triggered by {POSTCLOSE_OWNER}, which was MEASURED starting at "
        f"{MEASURED_SCHEDULE!r} on four consecutive weekdays, but declare something else: "
        f"{wrong}. data_gate/evidence.py::_cycle discards every manifest that started before "
        "the DECLARED fire instant, so a declaration later than the real trigger makes a "
        "healthy unit read run_record UNMET (alpha-engine-config-I11189). If the pipeline's "
        "trigger genuinely moved, re-measure it from the state machine's execution history "
        "and update MEASURED_SCHEDULE here in the same commit as the descriptors."
    )
