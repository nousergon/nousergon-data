"""metron-intraday.service must declare a measured memory ceiling.

`shared-application-host-policy.md` §4.3: a service on the shared dashboard box
needs a "bounded, declared resource footprint — a steady-state memory ceiling
measured before install and enforced as a `MemoryMax=` in its unit", and "a
service whose footprint is unknown is measured first, not installed first."

This unit was installed on 2026-07-29 with no cap of any kind. Its peak was then
measured across three real runs on the box: **184 MB RSS, 3.2-3.7s wall**. The
caps here are sized from that (260M soft / 350M hard, ~1.4x / ~1.9x).

The interpreter assertion belongs with the footprint one: both are properties of
"this unit can actually run here", and the venv it names is the thing that made
the footprint zero in the first place — an ImportError path returns empty
results without allocating anything.
"""

import re
from pathlib import Path

UNIT = (
    Path(__file__).parent.parent / "infrastructure" / "systemd" / "metron-intraday.service"
).read_text()


def _directive(name: str) -> str | None:
    m = re.search(rf"^{name}=(.+)$", UNIT, re.M)
    return m.group(1).strip() if m else None


def test_declares_a_hard_memory_ceiling():
    """Policy §4.3 — a service with no MemoryMax has an unbounded footprint."""
    assert _directive("MemoryMax"), (
        "no MemoryMax=. On the shared box an uncapped unit is exactly what T1-1 "
        "exists to prevent: per-service caps are the only thing standing between "
        "one runaway and the other thirteen services."
    )


def test_declares_a_reclaim_window_below_the_hard_cap():
    """MemoryHigh throttles and reclaims; MemoryMax kills.

    A unit with only a hard cap goes from unconstrained straight to a kill — the
    same reasoning budget.yaml records for every long-running service here.
    """
    high, mx = _directive("MemoryHigh"), _directive("MemoryMax")
    assert high, "no MemoryHigh= — no reclaim window before the hard cap"

    def mb(v: str) -> int:
        return int(v.rstrip("MG")) * (1024 if v.endswith("G") else 1)

    assert mb(high) < mb(mx), f"MemoryHigh ({high}) must sit below MemoryMax ({mx})"


def test_caps_are_clear_of_the_measured_peak():
    """Sized ABOVE the measurement, not fitted to it.

    A cap set just above an observed number re-pins the service the moment
    anything moves — the mistake budget.yaml records twice on this box
    (config-I5216, config-I5237), where a cap raised to just over a censored
    reading was throttling again within a day.
    """
    measured_peak_mb = 184  # three runs, 2026-07-29, /usr/bin/time -v
    high = int(_directive("MemoryHigh").rstrip("M"))
    assert high >= measured_peak_mb * 1.25, (
        f"MemoryHigh={high}M leaves under 25% over the measured {measured_peak_mb} MB peak"
    )


def test_the_measurement_is_recorded_in_the_unit():
    """A bare number is unreviewable; the next person needs to know what it came from."""
    assert "184" in UNIT and re.search(r"peak|RSS", UNIT), (
        "the unit must record the measurement its caps derive from, so a future "
        "change knows whether it is re-deriving or guessing"
    )


def test_runs_on_the_dedicated_venv_not_the_shared_one():
    """The shared .venv is Python 3.9 without pandas/yfinance.

    Pointed there, every fetch helper returns {} and the collector writes an
    empty artifact over a good one while reporting success — the 2026-07-29
    defect. This assertion is what stops a future edit "simplifying" the path
    back to the shared venv.
    """
    exec_start = _directive("ExecStart")
    assert ".venv-intraday/bin/python" in exec_start, (
        f"ExecStart names {exec_start!r}; the shared .venv cannot run this job"
    )
