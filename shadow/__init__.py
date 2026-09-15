"""Pre-cutover shadow runs: the output-root override and the parity diff.

`alpha-engine-config-I10778`, plan `data_collection_plan_260914.md` §6.2 step 4.

Two halves, deliberately separate:

* ``shadow.root`` + ``shadow.interceptor`` — the **writer** side. One standalone
  execution writes every published key under ``staging/shadow/{trading_day}/``
  and every ArcticDB library under ``shadow_{YYYYMMDD}_``, and cannot write
  anywhere else. Entered through ``python -m shadow run``.
* ``shadow.parity`` — the **reader** side. Diffs each shadow key against the
  same trading day's v1 output and publishes
  ``data_collection/parity/{trading_day}.json``, which
  ``data_gate.evidence.read_parity`` reads as the ``data-cutover-ready``
  gate's parity evidence.

This package deliberately sits OUTSIDE ``registry.d/writer_inventory.yaml``'s
walked roots, for the same reason ``data_gate`` does: it writes a gate-evidence
artifact, not a published data key, and putting it in the roots would add a row
to the producer board for something that produces no data.
"""

from shadow.root import (
    ShadowGuardViolation,
    ShadowRoot,
    activate,
    activate_from_env,
    active_root,
    deactivate,
    root_from_env,
    shadow_arctic_library,
)

__all__ = [
    "ShadowGuardViolation",
    "ShadowRoot",
    "activate",
    "activate_from_env",
    "active_root",
    "deactivate",
    "root_from_env",
    "shadow_arctic_library",
]
