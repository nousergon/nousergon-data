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

It also lets a phase ANNOTATE its END marker (alpha-engine-config-I11812): the
base class writes a fixed set of fields, and D02's provisional spin-off
additions must be visible on the marker, not only in the artifact and the run
manifest. :meth:`RunStatePhaseRegistry.annotate_marker` records extra fields
for one phase; the next marker written for that phase carries them, and the
annotation is consumed so it cannot leak onto a later run of the phase. The
base fields always win a name collision — an annotation can add to a marker,
never rewrite its status or artifact list.
"""

from __future__ import annotations

from nousergon_lib.phase_registry import PhaseRegistry

from shadow.interceptor import own_state_reads

__all__ = ["RunStatePhaseRegistry"]


class RunStatePhaseRegistry(PhaseRegistry):
    """``PhaseRegistry`` whose marker and artifact-probe reads are run state."""

    def annotate_marker(self, phase_name: str, **fields) -> None:
        """Add ``fields`` to the next END marker written for ``phase_name``."""
        annotations = self.__dict__.setdefault("_marker_annotations", {})
        annotations.setdefault(phase_name, {}).update(fields)

    def _write_marker(self, marker: dict) -> None:
        extra = self.__dict__.get("_marker_annotations", {}).pop(marker.get("phase"), None)
        if extra:
            marker = {**extra, **marker}
        super()._write_marker(marker)

    def should_run(self, phase_name: str, supports_auto_skip: bool = False) -> tuple[bool, str]:
        with own_state_reads():
            return super().should_run(phase_name, supports_auto_skip=supports_auto_skip)

    def load_marker(self, phase_name: str) -> dict | None:
        with own_state_reads():
            return super().load_marker(phase_name)
