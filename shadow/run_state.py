"""The phase registry, with its auto-skip decision read as RUN STATE.

`alpha-engine-config-I10891`. ``nousergon_lib.phase_registry.PhaseRegistry``
decides a same-date auto-skip from two reads: the phase's completion marker and
a ``head_object`` on every artifact that marker claims. Both are the run's own
state. Under an active shadow root they must resolve to the shadow prefix, or a
shadow run reads v1's live markers, concludes its work is done, and publishes
nothing — which is what the 2026-09-14 shadow run did for twelve units.

The classification itself lives in ``shadow/interceptor.py``; this subclass only
marks the decision as run state by entering :func:`own_state_reads`. With no
shadow root active the interceptor is not consulted, so production behaviour is
byte-identical to the base class.
"""

from __future__ import annotations

from nousergon_lib.phase_registry import PhaseRegistry

from shadow.interceptor import own_state_reads

__all__ = ["RunStatePhaseRegistry"]


class RunStatePhaseRegistry(PhaseRegistry):
    """``PhaseRegistry`` whose marker and artifact-probe reads are run state."""

    def should_run(self, phase_name: str, supports_auto_skip: bool = False) -> tuple[bool, str]:
        with own_state_reads():
            return super().should_run(phase_name, supports_auto_skip=supports_auto_skip)

    def load_marker(self, phase_name: str) -> dict | None:
        with own_state_reads():
            return super().load_marker(phase_name)
