"""
tests/test_pipeline_status_registry_source_check.py — source-side twin of
``crucible-dashboard``'s ``tests/test_pipeline_status_registry_drift.py``.

nousergon/alpha-engine-config#2480: 5 recurrences of the same drift class
(config#1115, #1120, #2372, #2430, and the EOD instance nousergon-lib#201
fixed) — a new Task state lands in one of THIS repo's SF JSONs without a
paired ``nousergon_lib.pipeline_status.registry.STATE_TO_ARCHIVE_PAGE``
entry, and it's always caught downstream in crucible-dashboard's CI,
days/weeks late, because the SF JSON that ADDS the new state lives HERE,
not there.

crucible-dashboard's test walks these same JSONs from a sibling checkout
path (``~/Development/alpha-engine-data/...``) and SKIPs when that path
doesn't exist — which is always true in its own CI (no sibling checkout
there), so the invariant has never actually been enforced pre-merge on
either side. This test closes that gap: it reads the JSONs directly from
THIS repo's working tree, so it always runs (never skips) in nousergon-data
CI, and fails the PR that introduces the drift instead of surfacing it
later as a dashboard "Registry drift" cell.

The walk/filter logic below is copied verbatim (not reinvented) from
crucible-dashboard's ``_walk_substantive_task_states`` so the two checks
can never quietly disagree on what counts as "substantive."

Coverage note (beyond dashboard-test parity): the dashboard test only
walks Saturday / Weekday / EOD. This repo also owns
``infrastructure/step_function_groom.json`` (the groom-pipeline SF), which
the dashboard test has never covered. The groom SF currently has several
substantive states with no registry entry at all (pre-existing gap, not
introduced by this PR) — asserting on it here would fail on unrelated,
already-existing drift rather than catching new drift, so it is walked and
reported via a non-asserting visibility test, pending a separate
registry-population pass for the groom pipeline. The 3 dashboard-covered
files (Saturday/Weekday/EOD) ARE asserted on hard, per this issue's scope.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nousergon_lib.pipeline_status.registry import (
    STATE_TO_ARCHIVE_PAGE,
    SUBSTANTIVE_RESOURCES,
    WAIT_GROUPING,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

_SF_JSON_FILES = [
    ("Saturday", REPO_ROOT / "infrastructure" / "step_function.json"),
    ("Weekday", REPO_ROOT / "infrastructure" / "step_function_daily.json"),
    ("EOD", REPO_ROOT / "infrastructure" / "step_function_eod.json"),
]

# Not covered by crucible-dashboard's test at all. Walked + reported below
# for visibility, not asserted on hard (see module docstring coverage note).
_GROOM_SF_JSON = REPO_ROOT / "infrastructure" / "step_function_groom.json"


def _walk_substantive_task_states(states: dict, found: set) -> set:
    """Verbatim port of crucible-dashboard's
    ``tests/test_pipeline_status_registry_drift.py::_walk_substantive_task_states``.
    Walk an SF JSON ``States`` map, descending into Parallel + Map branches,
    and collect every Task state name whose Resource is in
    SUBSTANTIVE_RESOURCES."""
    for name, body in states.items():
        if not isinstance(body, dict):
            continue
        type_ = body.get("Type")
        if type_ == "Task":
            resource = body.get("Resource")
            if isinstance(resource, str) and resource in SUBSTANTIVE_RESOURCES:
                found.add(name)
        elif type_ == "Parallel":
            for branch in body.get("Branches", []):
                _walk_substantive_task_states(branch.get("States", {}), found)
        elif type_ == "Map":
            iterator = body.get("Iterator") or body.get("ItemProcessor", {})
            _walk_substantive_task_states(iterator.get("States", {}), found)
    return found


def _all_substantive_states(json_path: Path) -> set:
    sf = json.loads(json_path.read_text())
    return _walk_substantive_task_states(sf.get("States", {}), set())


@pytest.mark.parametrize("label,json_path", _SF_JSON_FILES)
def test_every_substantive_state_has_registry_entry(label, json_path):
    """The load-bearing cross-repo invariant, enforced from the SOURCE side
    (config#2480) so the PR that introduces a new substantive Task state
    fails here instead of days/weeks later as a dashboard "Registry drift"
    cell. Fix: add the new state name + ArchivePageRef or ArtifactReason to
    ``nousergon_lib.pipeline_status.registry`` (companion nousergon-lib PR),
    bump the lib version, then bump this repo's pin in requirements.txt."""
    assert json_path.exists(), f"{label} SF JSON not found at {json_path}"

    substantive = _all_substantive_states(json_path)
    # WAIT_GROUPING members roll up into their parent row and never need
    # their own registry entry (mirrors the dashboard test's exclusion).
    substantive -= set(WAIT_GROUPING.keys())
    missing = substantive - set(STATE_TO_ARCHIVE_PAGE.keys())

    assert not missing, (
        f"{label} SF ({json_path.relative_to(REPO_ROOT)}) has {len(missing)} "
        f"substantive Task state(s) NOT in "
        f"nousergon_lib.pipeline_status.registry.STATE_TO_ARCHIVE_PAGE: "
        f"{sorted(missing)}. This is the config#1115/#1120/#2372/#2430 drift "
        f"class recurring again. Add each state to the registry in "
        f"nousergon-lib with an ArchivePageRef deep-link or an explicit "
        f"ArtifactReason string, bump the lib version, then bump this "
        f"repo's requirements.txt pin in the SAME PR that adds the state — "
        f"do not merge the SF JSON change ahead of the registry entry."
    )


@pytest.mark.parametrize("label,json_path", _SF_JSON_FILES)
def test_wait_companions_in_json_are_in_wait_grouping(label, json_path):
    """Every state named ``WaitFor*`` in the SF JSON must appear in
    WAIT_GROUPING — otherwise it would render as its own row instead of
    rolling into its parent. Verbatim port of the dashboard test's
    companion assertion."""
    assert json_path.exists(), f"{label} SF JSON not found at {json_path}"

    sf = json.loads(json_path.read_text())

    def _collect_wait_states(states: dict, found: set) -> set:
        for name, body in states.items():
            if not isinstance(body, dict):
                continue
            if name.startswith("WaitFor"):
                found.add(name)
            if body.get("Type") == "Parallel":
                for branch in body.get("Branches", []):
                    _collect_wait_states(branch.get("States", {}), found)
            elif body.get("Type") == "Map":
                iterator = body.get("Iterator") or body.get("ItemProcessor", {})
                _collect_wait_states(iterator.get("States", {}), found)
        return found

    wait_states = _collect_wait_states(sf.get("States", {}), set())
    missing = wait_states - set(WAIT_GROUPING.keys())

    assert not missing, (
        f"{label} SF has {len(missing)} ``WaitFor*`` state(s) NOT in "
        f"nousergon_lib.pipeline_status.registry.WAIT_GROUPING: "
        f"{sorted(missing)}. Each must map to its parent Task state name; "
        f"otherwise the wait companion will render as its own row instead "
        f"of rolling up."
    )


def test_groom_sf_registry_coverage_visibility():
    """Non-blocking visibility check for the groom-pipeline SF (never
    covered by crucible-dashboard's test). Reports current registry
    coverage rather than asserting, since the groom SF has pre-existing
    unregistered substantive states this PR does not attempt to fix (that
    is a separate registry-population pass, tracked outside config#2480's
    scope). Fails only if the file goes missing entirely (a repo-layout
    regression), not on registry gaps."""
    assert _GROOM_SF_JSON.exists(), f"groom SF JSON not found at {_GROOM_SF_JSON}"

    substantive = _all_substantive_states(_GROOM_SF_JSON)
    substantive -= set(WAIT_GROUPING.keys())
    missing = substantive - set(STATE_TO_ARCHIVE_PAGE.keys())

    if missing:
        print(
            f"\ngroom SF (step_function_groom.json) has {len(missing)} "
            f"substantive Task state(s) not yet in STATE_TO_ARCHIVE_PAGE: "
            f"{sorted(missing)} — pre-existing gap, not asserted on here "
            f"(dashboard test never covered groom either). See this test "
            f"file's module docstring."
        )
