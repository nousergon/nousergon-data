"""Total-coverage detector for the run-manifest lift (`alpha-engine-config-I10773`).

`tests/test_run_units.py` grades ``run_units.PHASE_UNITS`` against the
descriptor set two ways, and `tests/test_unit_manifests.py` grades five NAMED
standalone entry points. Neither asserts coverage is TOTAL: a new
``lifecycle: in-service`` descriptor added under ``registry.d/units/`` with no
manifest-emitting call site anywhere is invisible to both — the first only
grades units already in its table, the second only grades units someone
remembered to add a fixture for. That is detection blindness, which outranks
the defects it hides (`engagement-protocol-policy` §5).

This module enumerates every committed descriptor and asserts every
``lifecycle: in-service`` unit owned by THIS repo (``owning_repo:
nousergon-data``) has a manifest-emitting call site, found one of three ways:

  * a row in ``run_units.PHASE_UNITS`` (a ``_phase_collect`` phase);
  * a value in ``run_units.MODE_UNITS`` (a whole-mode dispatch);
  * a literal unit id passed to ``run_units.recorded_entry(...)``,
    ``run_units.manual_run(...)``, or ``nousergon_lib.run_manifest.run_unit(...)``
    (directly, keyed off a module-level ``UNIT_ID = "..."`` constant — D35's
    shape) anywhere in this repo's non-test Python.

A unit legitimately without a call site here is named in
:data:`OWNING_REPO_WITH_NO_LOCAL_CALL_SITE` with its reason and descriptor
evidence — never a silent skip. A ``lifecycle: retired`` (or any lifecycle
other than ``in-service``, e.g. ``disabled``/``pending``) unit is out of scope
by that field alone, not by being listed here: this test only ever asserts on
what claims to be running.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import run_units
from data_gate.descriptors import Unit, load_units

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Directories whose Python is never a manifest call site worth scanning:
#: the venv, git internals, worktree admin, and the test suite itself (a test
#: exercising ``recorded_entry("D36", ...)`` must not count as THE call site
#: for D36 — that would let a real call site be deleted with this test still
#: green).
_EXCLUDED_DIR_PARTS = frozenset({".venv", ".git", "node_modules", "tests", ".worktrees"})

#: ``recorded_entry("D36", ...)`` / ``manual_run("D43", ...)`` — first
#: positional arg a string literal, across a possible line break.
_WRAPPER_CALL = re.compile(
    r"\b(?:recorded_entry|manual_run)\(\s*\n?\s*\"([A-Z][A-Za-z0-9]*)\"", re.MULTILINE
)

#: ``run_manifest.run_unit(UNIT_ID, ...)`` — D35's shape (``scripts/
#: backfill_benchmark_proxies.py``): the literal id isn't at the call site, it
#: is a module constant named ``UNIT_ID`` resolved separately below. A literal
#: string at this call site (rather than the module constant) is also matched,
#: for a future call site that inlines it.
_RUN_UNIT_CALL = re.compile(
    r"run_manifest\.run_unit\(\s*\n?\s*(?:UNIT_ID|\"([A-Z][A-Za-z0-9]*)\")", re.MULTILINE
)
_UNIT_ID_CONST = re.compile(r"^UNIT_ID\s*=\s*\"([A-Z][A-Za-z0-9]*)\"", re.MULTILINE)


def _repo_python_files(root: Path) -> list[Path]:
    out = []
    for path in root.rglob("*.py"):
        if any(part in _EXCLUDED_DIR_PARTS for part in path.relative_to(root).parts):
            continue
        out.append(path)
    return out


def call_site_units(root: Path = REPO_ROOT) -> set[str]:
    """Every unit id named at a manifest-emitting call site in ``root``.

    Combines the two declared tables (``PHASE_UNITS``/``MODE_UNITS``, already
    graded two-way by ``test_run_units.py``) with a scan of every other
    non-test ``.py`` file for the wrapper calls those tables don't cover.
    """
    units = {pu.unit_id for pu in run_units.PHASE_UNITS.values()}
    units |= set(run_units.MODE_UNITS.values())
    for path in _repo_python_files(root):
        source = path.read_text(encoding="utf-8")
        units |= set(_WRAPPER_CALL.findall(source))
        run_unit_matches = _RUN_UNIT_CALL.findall(source)
        if not run_unit_matches:
            continue
        # A literal string at the run_unit() call site itself.
        units |= {m for m in run_unit_matches if m}
        # The UNIT_ID-constant shape: at least one match used the bare
        # `UNIT_ID` keyword (empty capture group), so resolve the module's own
        # constant.
        if any(m == "" for m in run_unit_matches):
            const_match = _UNIT_ID_CONST.search(source)
            if const_match:
                units.add(const_match.group(1))
    return units


#: Units that are ``lifecycle: in-service`` and ``owning_repo: nousergon-data``
#: yet legitimately have NO manifest call site in this repo — an explicit,
#: commented exclusion, never a silent skip. Each entry names the descriptor
#: evidence backing the exclusion so a future reader can re-verify it rather
#: than trust the comment.
OWNING_REPO_WITH_NO_LOCAL_CALL_SITE: dict[str, str] = {
    # D47 — "crucible v2 data.daily / data.weekly / data.heal". Descriptor
    # declares `owning_repo: "nousergon-data"` (which is why it is in this
    # repo's descriptor set at all — the audit's unit register is shared
    # across the whole data-collection ladder) but its OWN code_path is
    # `"crucible:crucible/track_a.py:702-705, crucible/data/daily.py:172"` and
    # its `graded_on` is `"crucible-board"` — component 2, the first-class
    # CONSUMER of this repo's ArcticDB universe, not a producer running in
    # this repo's process tree. Its `notes` block states the exclusion in
    # full: "It keeps a descriptor here for exactly one reason: its nine
    # audit cells are nine of the 414 ... Every clause on this unit reads the
    # CRUCIBLE board as its evidence, never a data-collector artifact." A
    # manifest for D47's executions, if one is ever emitted, is a `crucible`
    # repo concern — out of scope for the sibling `alpha-engine-config-I10953`
    # session too (that session's brief names `crucible`/`nousergon-lib` only,
    # not this unit).
    "D47": (
        "registry.d/units/D47-v2-data-daily.yaml: code_path="
        '"crucible:crucible/track_a.py:702-705, crucible/data/daily.py:172", '
        'graded_on="crucible-board"'
    ),
}


_MANIFEST_PREFIX_UNIT = re.compile(r"^data_collection/runs/([A-Z][A-Za-z0-9]*)$")


def _piggybacked_units(units: list[Unit]) -> dict[str, str]:
    """unit_id -> the OTHER unit_id whose manifest carries its keys.

    A unit's own descriptor declares ``run_manifest_prefix: data_collection/
    runs/<other-unit>`` instead of its own id when a second manifest for it
    would mean re-running the identical upstream work a second time — D46's
    documented shape (``rag/pipelines/run_weekly_ingestion_recorded.py``'s
    module docstring; ``registry.d/units/D46-insider-transactions.yaml``'s
    ``run_manifest_prefix: data_collection/runs/D16``). This is the SAME field
    ``infrastructure/lambdas/data-spot-dispatcher/index.py::_check_unit``
    reads to find a unit's completion evidence, so a descriptor pointing
    elsewhere is a real, machine-checked coverage claim — not an assumption
    this test invents.
    """
    out: dict[str, str] = {}
    for unit in units:
        match = _MANIFEST_PREFIX_UNIT.match(str(unit.raw.get("run_manifest_prefix") or ""))
        if match and match.group(1) != unit.unit_id:
            out[unit.unit_id] = match.group(1)
    return out


def uncovered_units(
    units: list[Unit],
    known_call_site_units: set[str],
    *,
    piggybacked: dict[str, str] | None = None,
) -> list[str]:
    """Every in-service, nousergon-data-owned unit with no known call site.

    Pure function of its inputs — no descriptor-directory or filesystem
    access — so a fake unit set can prove this actually catches a gap
    (see ``test_a_new_uncovered_unit_is_caught_by_this_test`` below) without
    needing to write a throwaway descriptor file to disk.

    ``piggybacked`` is a unit id -> unit id map: a unit declaring itself
    covered by ANOTHER unit's manifest (see :func:`_piggybacked_units`) is
    covered iff that OTHER unit is itself covered — a descriptor pointing at
    an equally-uncovered sibling is not evidence of anything.
    """
    piggybacked = piggybacked or {}
    gaps = []
    for unit in units:
        if unit.lifecycle != "in-service":
            continue
        if str(unit.raw.get("owning_repo") or "") != "nousergon-data":
            continue
        if unit.unit_id in OWNING_REPO_WITH_NO_LOCAL_CALL_SITE:
            continue
        if unit.unit_id in known_call_site_units:
            continue
        host = piggybacked.get(unit.unit_id)
        if host is not None and host in known_call_site_units:
            continue
        gaps.append(unit.unit_id)
    return sorted(gaps)


def test_every_in_service_unit_has_a_manifest_call_site():
    units = load_units()
    known = call_site_units()
    gaps = uncovered_units(units, known, piggybacked=_piggybacked_units(units))
    assert not gaps, (
        f"unit(s) {gaps} are lifecycle=in-service, owning_repo=nousergon-data, and have NO "
        "manifest-emitting call site — not a row in run_units.PHASE_UNITS or MODE_UNITS, and "
        "no recorded_entry()/manual_run()/run_manifest.run_unit() call anywhere in this repo's "
        "non-test Python. Every execution of every unit must write a data_run_manifest.v1 "
        "record (alpha-engine-config-I10773, plan §2 row 7); add the call site, or if this unit "
        "legitimately runs outside this repo, add it to "
        "OWNING_REPO_WITH_NO_LOCAL_CALL_SITE with the descriptor evidence."
    )


def test_excluded_unit_d47_is_declared_not_a_silent_gap():
    """The one exclusion in force today is committed WITH evidence, and is
    still a real descriptor (not a typo pointing at nothing)."""
    units = {u.unit_id: u for u in load_units()}
    assert set(OWNING_REPO_WITH_NO_LOCAL_CALL_SITE) <= set(units), (
        "an exclusion names a unit id with no descriptor at all — that is a stale exclusion, "
        "not a real one"
    )
    d47 = units["D47"]
    assert d47.lifecycle == "in-service"
    assert d47.raw.get("owning_repo") == "nousergon-data"
    assert "crucible" in str(d47.raw.get("code_path") or "")


def _fake_unit(unit_id: str, *, lifecycle: str = "in-service", owning_repo: str = "nousergon-data") -> Unit:
    return Unit(
        unit_id=unit_id,
        path=Path(f"registry.d/units/{unit_id}-fake.yaml"),
        raw={
            "unit_id": unit_id,
            "lifecycle": lifecycle,
            "owning_repo": owning_repo,
            "title": f"fake unit {unit_id} for the coverage-detector proof",
        },
    )


def test_a_new_uncovered_unit_is_caught_by_this_test():
    """Proves the detector actually detects — the whole point of this file.

    A fake ``lifecycle: in-service``, ``owning_repo: nousergon-data`` unit with
    no call site MUST come back as a gap. If this test ever goes green with an
    empty gap list, ``uncovered_units`` has stopped detecting anything and the
    completeness assertion above is a tautology.
    """
    known = call_site_units()
    assert "DFAKE" not in known, "the fake id collided with a real call site — pick another id"

    fake_units = [_fake_unit("DFAKE"), _fake_unit("D01")]  # D01 IS covered, for contrast
    gaps = uncovered_units(fake_units, known)
    assert gaps == ["DFAKE"], (
        f"expected exactly the uncovered fake unit, got {gaps} — the detector did not fire on "
        "an unquestionably uncovered in-service unit"
    )


def test_a_retired_unit_is_out_of_scope_regardless_of_call_site():
    """Lifecycle gates scope, not the exclusion table — retired needs no entry."""
    fake_units = [_fake_unit("DRETIRED", lifecycle="retired")]
    assert uncovered_units(fake_units, set()) == []


def test_a_unit_owned_by_another_repo_is_out_of_scope():
    fake_units = [_fake_unit("DOTHER", owning_repo="crucible")]
    assert uncovered_units(fake_units, set()) == []


def test_d46_is_covered_via_its_own_piggyback_declaration_not_the_exclusion_table():
    """D46 has NO call site naming it directly (`insider_transactions` is step
    6 of the SAME script D16 runs, folded into D16's one manifest — see
    ``rag/pipelines/run_weekly_ingestion_recorded.py``'s module docstring).
    Its descriptor declares this itself (``run_manifest_prefix: data_collection
    /runs/D16``), so it must be covered through :func:`_piggybacked_units`,
    never by adding it to OWNING_REPO_WITH_NO_LOCAL_CALL_SITE — that table is
    for units with NO manifest at all, and D46's manifest genuinely exists,
    just under D16's id."""
    assert "D46" not in OWNING_REPO_WITH_NO_LOCAL_CALL_SITE
    units = load_units()
    piggy = _piggybacked_units(units)
    assert piggy.get("D46") == "D16"
    assert "D16" in call_site_units()


def test_a_piggybacked_unit_pointing_at_an_uncovered_host_is_still_a_gap():
    """A descriptor's `run_manifest_prefix` pointing elsewhere is not, on its
    own, proof of anything — the pointed-at unit must ITSELF be covered."""
    host = _fake_unit("DHOST")
    rider = _fake_unit("DRIDER")
    rider.raw["run_manifest_prefix"] = "data_collection/runs/DHOST"
    piggy = _piggybacked_units([host, rider])
    assert piggy == {"DRIDER": "DHOST"}
    # DHOST itself has no call site (not in `known`), so DRIDER stays a gap.
    gaps = uncovered_units([host, rider], known_call_site_units=set(), piggybacked=piggy)
    assert gaps == ["DHOST", "DRIDER"]


def test_call_site_units_covers_the_run_unit_constant_shape():
    """D35 (scripts/backfill_benchmark_proxies.py) calls
    ``run_manifest.run_unit(UNIT_ID, ...)`` directly — not `recorded_entry`/
    `manual_run` — keyed off a module-level ``UNIT_ID = "D35"`` constant. This
    is the one call-site shape neither PHASE_UNITS/MODE_UNITS nor the
    wrapper-call regex catches on its own; assert it is resolved."""
    assert "D35" in call_site_units()
