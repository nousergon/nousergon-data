"""`PIPELINE_STAGE_ORDER` names states this repo's Step Functions actually have.

`nousergon_lib.pipeline_status.registry.PIPELINE_STAGE_ORDER`'s own docstring
says:

    Kept in sync with the live SF JSONs by a producer-side contract test in
    ``nousergon-data`` — a stage rename that is not mirrored here blinds every
    reader matching on the old name, and that blindness reports as the benign
    "that stage did not run" (the I6857 defect).

**That test did not exist.** Verified 2026-09-08 by grepping this entire
repository for ``PIPELINE_STAGE_ORDER`` and ``stage_order_for``: no hits. The docstring named the failure mode, named the guard that prevents
it, and the guard was never built — so the constant has been unguarded against
a rename in the one repository that owns the definitions, which is the only
place a rename can happen (`alpha-engine-config-I10201`).

WHY THE DIRECTION OF THE ASSERTION IS THE WHOLE TEST
----------------------------------------------------
The spine is a **judgment**: a small ordered subset of substantive stages that
`cycles._depth_of` reads as progress. It is deliberately NOT the state list —
`ne-weekly-freshness-pipeline` declares 465 states and a 16-stage spine.

So this asserts one direction only: **every spine stage must exist as a state.**
The converse — every state appearing in the spine — would be false by design and
would make the constant unmaintainable.

That single direction is exactly the I6857 protection. A rename in
`step_function.json` that is not mirrored in the library leaves a reader
matching on a name nothing will ever emit, and the resulting silence is
indistinguishable from "that stage did not run this week".

STATES NEST, AND A SHALLOW READ IS A FALSE PASS
-----------------------------------------------
Seven of the sixteen weekly spine stages are not top-level `States` keys — they
live inside `Parallel.Branches[].States` and `Map` iterators. A first cut of
this file read only the top level and reported all seven as missing, which
would have been a guard reporting a defect that did not exist. `_state_names`
therefore recurses through `Branches`, `Iterator` and `ItemProcessor`; the
depth is asserted below so a future shallow regression fails here rather than
becoming a silent false pass.

The library version this runs against is the PINNED one in CI, which is the
version whose constant is actually shipped.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nousergon_lib.pipeline_status import registry
from nousergon_lib.pipeline_status.registry import (
    PIPELINE_STAGE_ORDER,
    stale_pending_stages,
    undefined_spine_stages,
)

REPO = Path(__file__).resolve().parents[1]

#: pipeline -> the definition this repo owns. Mirrors the map
#: `tests/test_sf_definition_s3_consumer_contract.py::_CONSUMER_PATHS` already
#: uses for the two trading pipelines, extended with the weekly one.
DEFINITIONS = {
    "ne-weekly-freshness-pipeline": "infrastructure/step_function.json",
    "ne-preopen-trading-pipeline": "infrastructure/step_function_daily.json",
    "ne-postclose-trading-pipeline": "infrastructure/step_function_eod.json",
}


def _state_names(states: dict | None, out: set[str]) -> None:
    """Every state name at any depth: Parallel branches and Map iterators too."""
    for name, body in (states or {}).items():
        out.add(name)
        if not isinstance(body, dict):
            continue
        for branch in body.get("Branches") or []:
            _state_names(branch.get("States"), out)
        for key in ("Iterator", "ItemProcessor"):
            nested = body.get(key)
            if isinstance(nested, dict):
                _state_names(nested.get("States"), out)


def _names_for(pipeline: str) -> set[str]:
    doc = json.loads((REPO / DEFINITIONS[pipeline]).read_text(encoding="utf-8"))
    names: set[str] = set()
    _state_names(doc.get("States"), names)
    return names


def test_every_pipeline_in_the_spine_has_a_definition_here():
    """A pipeline the library declares and this repo cannot show is unguarded.

    Derived from the library, not from `DEFINITIONS`: adding a fourth pipeline
    to `PIPELINE_STAGE_ORDER` must fail here until its definition is mapped,
    rather than silently going unchecked.
    """
    missing = sorted(set(PIPELINE_STAGE_ORDER) - set(DEFINITIONS))
    assert not missing, (
        f"PIPELINE_STAGE_ORDER declares {missing} and this test maps no "
        f"definition for it, so that pipeline's spine is unguarded against a "
        f"rename. Add it to DEFINITIONS."
    )


@pytest.mark.parametrize("pipeline", sorted(PIPELINE_STAGE_ORDER))
def test_every_spine_stage_exists_in_the_live_definition(pipeline):
    names = _names_for(pipeline)
    # A stage the library marks PENDING_DEFINITION_STAGES is declared ahead of
    # this repo's definition and is not reported here (alpha-engine-config-I10762);
    # the stale-marker test below is what keeps that tolerance honest.
    missing = list(undefined_spine_stages(pipeline, names))
    assert not missing, (
        f"{pipeline}: nousergon-lib's PIPELINE_STAGE_ORDER names {missing}, "
        f"which {DEFINITIONS[pipeline]} does not contain at any depth. Either "
        f"a state was renamed here without mirroring it in the library — which "
        f"leaves every reader matching a name nothing will emit, reported as "
        f"the benign 'that stage did not run' (the I6857 defect) — or the "
        f"library declares a stage this pipeline never had."
    )


@pytest.mark.parametrize("pipeline", sorted(PIPELINE_STAGE_ORDER))
def test_no_spine_stage_is_still_marked_pending_once_this_repo_defines_it(pipeline):
    """This repo OWNS the definitions, so it alone is strict about a stale marker.

    ``PENDING_DEFINITION_STAGES`` lets a consumer tolerate a spine stage whose
    state has not landed here yet — in either merge order — instead of two PRs
    each waiting on the other's ``main`` (alpha-engine-config-I10762). The PR
    that ADDS the state is the change that makes the marker stale, so it is the
    PR that must take a library release without the marker. Consumers reading
    this repo's ``main`` must NOT fail on this, or the deadlock returns.
    """
    stale = stale_pending_stages(pipeline, _names_for(pipeline))
    assert not stale, (
        f"{pipeline}: {list(stale)} exist in {DEFINITIONS[pipeline]} but the "
        f"pinned nousergon-lib still marks them PENDING_DEFINITION_STAGES. Remove "
        f"the marker in nousergon-lib and bump the pin in this PR — a marker that "
        f"outlives its definition hides a future rename of that stage."
    )


# ── Both merge orders (alpha-engine-config-I10762) ───────────────────────────
# Mutation tests: each simulates the mirror PR's view by patching the library
# registry, and asserts the verdict this repo's guard must return for it.

_EOD = "ne-postclose-trading-pipeline"
_NEW = "LaunchSomeNewDailySpot"


#: A synthetic definition built from the spine as imported, so these tests
#: grade the merge-order LOGIC and never the live state of the definitions.
#: alpha-engine-config-I11267: excludes any REAL pending stage the pinned
#: library already declares for _EOD (e.g. WaitForCollectionManifests) — this
#: baseline must represent "landed states only" so the synthetic `_NEW`
#: mutation below is the ONLY not-yet-landed stage these tests reason about.
#: Without the exclusion, a real pending declaration landing in the library
#: makes `_BASE_DEFINITION` silently include a stage that is NOT actually
#: defined, and `stale_pending_stages` correctly (per its own contract) flags
#: it as stale — a false failure with no code change in this repo at all.
_BASE_DEFINITION = (
    frozenset(PIPELINE_STAGE_ORDER[_EOD]) - registry.pending_definition_stages_for(_EOD)
) | {"MarketHoursBlocked"}


def _declare(monkeypatch, *, pending: bool) -> None:
    order = dict(registry.PIPELINE_STAGE_ORDER)
    order[_EOD] = (*order[_EOD], _NEW)
    monkeypatch.setattr(registry, "PIPELINE_STAGE_ORDER", order)
    marks = {k: dict(v) for k, v in registry.PENDING_DEFINITION_STAGES.items()}
    if pending:
        marks[_EOD][_NEW] = "alpha-engine-config-I10762 (test fixture)"
    monkeypatch.setattr(registry, "PENDING_DEFINITION_STAGES", marks)


def test_library_first_with_a_pending_marker_is_not_red_here(monkeypatch):
    _declare(monkeypatch, pending=True)
    names = set(_BASE_DEFINITION)
    assert undefined_spine_stages(_EOD, names) == ()
    assert stale_pending_stages(_EOD, names) == ()


def test_library_first_without_a_marker_is_red_here(monkeypatch):
    _declare(monkeypatch, pending=False)
    assert undefined_spine_stages(_EOD, set(_BASE_DEFINITION)) == (_NEW,)


def test_definition_landing_with_the_marker_still_set_is_red_here(monkeypatch):
    _declare(monkeypatch, pending=True)
    names = set(_BASE_DEFINITION) | {_NEW}
    assert undefined_spine_stages(_EOD, names) == ()
    assert stale_pending_stages(_EOD, names) == (_NEW,)


def test_definition_landing_with_the_marker_cleared_is_green_here(monkeypatch):
    _declare(monkeypatch, pending=False)
    names = set(_BASE_DEFINITION) | {_NEW}
    assert undefined_spine_stages(_EOD, names) == ()
    assert stale_pending_stages(_EOD, names) == ()


@pytest.mark.parametrize("pipeline", sorted(PIPELINE_STAGE_ORDER))
def test_the_spine_is_a_subset_and_not_the_state_list(pipeline):
    """Guards the assertion's DIRECTION, which is the point of the test.

    If someone later 'tightens' this file into an equality check, the spine
    becomes unmaintainable and the pressure will be to delete the guard. The
    asymmetry is deliberate and is asserted so it survives.

    alpha-engine-config-I11267: a PENDING_DEFINITION_STAGES entry is declared
    in the library AHEAD of its state landing here — by design, per
    `test_every_spine_stage_exists_in_the_live_definition`'s own tolerance —
    so it must be excluded from "the spine" this subset check reasons about,
    or a library-first pending declaration alone (no code change in THIS
    repo at all) fails this test. A RETIRING_DEFINITION_STAGES entry is NOT
    excluded: it is still live in the current definition today and only
    tolerated as absent once this repo actually removes it.
    """
    names = _names_for(pipeline)
    pending = registry.pending_definition_stages_for(pipeline)
    spine = set(PIPELINE_STAGE_ORDER[pipeline]) - pending
    assert spine < names, (
        f"{pipeline}: the spine is not a PROPER subset of the definition's "
        f"states ({len(spine)} vs {len(names)}). The spine is a judgment about "
        f"which stages represent progress, never the full state list."
    )


def test_the_reader_descends_into_parallel_branches_and_maps():
    """A shallow read is a FALSE PASS, and it was the first cut of this file.

    Seven of the sixteen weekly spine stages live inside `Parallel` branches or
    `Map` iterators. A top-level-only `States` read reported all seven as
    missing. Asserting the depth keeps a future regression to the shallow form
    failing here instead of silently passing over nested states.
    """
    doc = json.loads(
        (REPO / DEFINITIONS["ne-weekly-freshness-pipeline"]).read_text(encoding="utf-8")
    )
    top_level = set(doc.get("States") or {})
    all_names = _names_for("ne-weekly-freshness-pipeline")

    assert len(all_names) > len(top_level), (
        "the weekly definition has no nested states, which contradicts its "
        "Parallel/Map structure — _state_names has stopped descending"
    )
    nested_spine = [
        s for s in PIPELINE_STAGE_ORDER["ne-weekly-freshness-pipeline"]
        if s in all_names and s not in top_level
    ]
    assert nested_spine, (
        "no weekly spine stage is nested any more. If that is a real "
        "restructuring, this assertion should be updated deliberately; if it "
        "is a reader regression, the guard above has become a false pass."
    )
