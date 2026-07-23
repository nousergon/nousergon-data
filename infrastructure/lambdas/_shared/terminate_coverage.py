"""Source-derived guard: every box-launching Lambda must terminate on failure
(alpha-engine-config#3189, generalizing ARCHITECTURE.md #115 rule 1).

## Why this exists

The 2026-07-20 triple-orphan incident left three spot boxes running after
their Step Function's failure path returned with nothing issuing a terminate.
Investigation found the fleet's actual termination architecture is box-local,
not SF-local: every dispatcher Lambda that launches a box (via
``nousergon_lib.ec2_spot.launch`` or ``spot_dispatch.launch_with_fallback``)
calls a terminate-on-failure helper in its own error path — either the shared
``spot_dispatch.terminate_on_failure`` or a local wrapper that itself calls
EC2 ``terminate_instances`` — on top of each box's self-arming
``InstanceInitiatedShutdownBehavior=terminate`` watchdog. The Step Functions'
``Catch`` transitions correctly route to notification only
(``NormalizeFailureContext``), not a terminate state, by design.

That per-dispatcher coverage was never *pinned* anywhere: nothing stopped a
new box-launching dispatcher from being added without the matching
terminate-on-failure call, silently reintroducing the orphan-on-failure gap.
This module derives the invariant from the live source, the same way
``hermetic_import_guard.py`` derives the stub-lockstep invariant: walk every
Lambda handler's module body for the launch call sites, and assert each one
that launches also terminates.

The fleet-wide ``spot-orphan-reaper`` Lambda's 6.5h age-cap
(``MAX_SPOT_BUDGET_SECONDS`` + ``GRACE_SECONDS``) remains a true
backstop-of-last-resort — it is deliberately NOT the mechanism this guard
checks; a launcher relying on the reaper alone (no terminate-on-failure call)
still fails this guard.
"""

from __future__ import annotations

import ast
from pathlib import Path

_LAUNCH_ATTRS = frozenset({"launch", "launch_with_fallback"})
_TERMINATE_ATTRS = frozenset({"terminate_on_failure", "terminate_instances"})


def _call_attr_names(source: str) -> tuple[set[str], set[str]]:
    """Return (launch call attr names, terminate call attr names) found anywhere
    in ``source`` (module scope or nested — a local ``_terminate_instance()``
    helper called from a nested ``except`` block must still count)."""
    launch_attrs: set[str] = set()
    terminate_attrs: set[str] = set()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        attr = node.func.attr
        if attr in _LAUNCH_ATTRS:
            launch_attrs.add(attr)
        if attr in _TERMINATE_ATTRS:
            terminate_attrs.add(attr)
    return launch_attrs, terminate_attrs


def find_box_launchers(lambdas_root: Path) -> dict[str, bool]:
    """Map each Lambda dir under ``lambdas_root`` that launches EC2 boxes
    (calls ``ec2_spot.launch`` or ``spot_dispatch.launch_with_fallback``) to
    whether it also has a terminate-on-failure call site anywhere in its
    ``index.py`` (``spot_dispatch.terminate_on_failure`` or a local
    ``terminate_instances`` wrapper)."""
    result: dict[str, bool] = {}
    for index_py in sorted(lambdas_root.glob("*/index.py")):
        launch_attrs, terminate_attrs = _call_attr_names(index_py.read_text())
        if launch_attrs:
            result[index_py.parent.name] = bool(terminate_attrs)
    return result


def assert_every_launcher_terminates_on_failure(lambdas_root: Path) -> None:
    """Fail loud if any box-launching Lambda under ``lambdas_root`` has no
    terminate-on-failure call site.

    Raises:
        AssertionError: naming the exact dispatcher(s) missing coverage.
    """
    launchers = find_box_launchers(lambdas_root)
    missing = sorted(name for name, covered in launchers.items() if not covered)
    if missing:
        raise AssertionError(
            "terminate_coverage (alpha-engine-config#3189): "
            f"{missing} call ec2_spot.launch()/spot_dispatch.launch_with_fallback() "
            "but have no terminate-on-failure call site in index.py. A box "
            "launched by this dispatcher and left running on error will sit "
            "orphaned until the spot-orphan-reaper's 6.5h age-cap backstop "
            "reaps it. Add `spot_dispatch.terminate_on_failure(instance_id, "
            "region=REGION, label=<name>)` to the failure path — see "
            "ci-watch-dispatcher/index.py's `_terminate_instance` for the "
            "pattern."
        )
