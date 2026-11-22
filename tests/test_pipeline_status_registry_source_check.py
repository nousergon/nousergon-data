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

import importlib.metadata
import json
import re
from pathlib import Path

import pytest

from nousergon_lib.pipeline_status.registry import (
    STATE_TO_ARCHIVE_PAGE,
    SUBSTANTIVE_RESOURCES,
    WAIT_GROUPING,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

# ── Installed-versus-pinned provenance (alpha-engine-config-I9116) ───────────
#
# This test reads the SF JSONs from the working tree and the registry from the
# INSTALLED `nousergon_lib`. Those are two different clocks. When the installed
# lib is behind `requirements.txt`, the invariant fails for a reason that has
# nothing to do with the SF definition — and the message below used to say, with
# no hedge, "the Saturday SF has 1 substantive Task state NOT in the registry …
# add each state to the registry in nousergon-lib". Measured 2026-08-28: the
# venv held v0.124.88 (137 registry entries, no `NormalizeRunDates`) while
# requirements.txt pinned v0.124.96 (138, with it). CI installs the pin and
# passes; the reader on the laptop is sent to nousergon-lib to add an entry that
# already exists there.
#
# A test whose failure text sends the reader to the wrong place is a defect in
# its own right, so the provenance is now part of the verdict. It is a
# QUALIFIER, never a suppressor: a version mismatch still fails, it just fails
# saying which of the two things to go fix. The environment half is tracked
# separately as alpha-engine-config-I9070; nothing here tries to repair it.
_PIN_RE = re.compile(r"nousergon-lib(?:\[[^\]]*\])?\s*@\s*\S+@(v[0-9][^\s#]*)")


def _pinned_lib_version(requirements: str) -> str | None:
    """The `nousergon-lib@vX.Y.Z` tag from requirements.txt text.

    FULL-LINE comments are stripped first, and that is load-bearing, not
    tidiness: this repo's requirements.txt carries at least four comment lines
    naming superseded tags (`v0.124.53`, `v0.124.78`, …) directly above the real
    pin, explaining why each was bumped. A matcher that reads those would report
    a mismatch against a version nobody installed — the same shape as the
    scheduled-identity scan that matched a role name inside a YAML comment and
    went red on main. Only whole comment lines are removed: a `#` mid-line is
    left alone, because a pin may legitimately carry a trailing note.
    """
    code = "\n".join(
        line for line in requirements.splitlines() if not line.lstrip().startswith("#")
    )
    match = _PIN_RE.search(code)
    return match.group(1) if match else None


def _installed_lib_version() -> str | None:
    try:
        return "v" + importlib.metadata.version("nousergon-lib")
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover - env-only
        return None


def _provenance_note(installed: str | None, pinned: str | None) -> str:
    """The paragraph prepended to a drift failure when the two clocks disagree.

    Empty string when they agree or when either is unreadable — in which case
    the failure is the SF-definition drift the assertion was written for.
    """
    if installed is None or pinned is None or installed == pinned:
        return ""
    return (
        f"READ THIS FIRST — your installed nousergon-lib does NOT match the pin.\n"
        f"  installed: {installed}\n"
        f"  pinned in requirements.txt: {pinned}\n"
        f"This check reads the SF JSONs from the working tree but the registry "
        f"from the INSTALLED lib, so a stale install produces exactly the "
        f"failure below WITHOUT any drift existing. Reinstall the pin first and "
        f"re-run:\n"
        f"    python3 -m pip install -r requirements.txt\n"
        f"Only if it still fails afterwards is the SF-definition drift described "
        f"below real. (alpha-engine-config-I9116; the environment drift itself is "
        f"tracked as alpha-engine-config-I9070.)\n\n"
    )


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
def test_every_substantive_state_has_registry_entry(
    label: str, json_path: Path
) -> None:
    """The load-bearing cross-repo invariant, enforced from the SOURCE side
    (config#2480) so the PR that introduces a new substantive Task state
    fails here instead of days/weeks later as a dashboard "Registry drift"
    cell. Fix: add the new state name + ArchivePageRef or ArtifactReason to
    ``nousergon_lib.pipeline_status.registry`` (companion nousergon-lib PR),
    let the lib's merge-time auto-bump tag the new version, then bump this
    repo's pin in requirements.txt to that tag."""
    assert json_path.exists(), f"{label} SF JSON not found at {json_path}"

    substantive = _all_substantive_states(json_path)
    # WAIT_GROUPING members roll up into their parent row and never need
    # their own registry entry (mirrors the dashboard test's exclusion).
    substantive -= set(WAIT_GROUPING.keys())
    missing = substantive - set(STATE_TO_ARCHIVE_PAGE.keys())

    assert not missing, _provenance_note(
        _installed_lib_version(),
        _pinned_lib_version((REPO_ROOT / "requirements.txt").read_text()),
    ) + (
        f"{label} SF ({json_path.relative_to(REPO_ROOT)}) has {len(missing)} "
        f"substantive Task state(s) NOT in "
        f"nousergon_lib.pipeline_status.registry.STATE_TO_ARCHIVE_PAGE: "
        f"{sorted(missing)}. This is the config#1115/#1120/#2372/#2430 drift "
        f"class recurring again. Add each state to the registry in "
        f"nousergon-lib with an ArchivePageRef deep-link or an explicit "
        f"ArtifactReason string, merge that lib PR (auto-version-bump.yml is "
        f"the single version writer — config-I2716 — and version-bump-check.yml "
        f"FORBIDS version edits inside a lib PR), then bump this "
        f"repo's requirements.txt pin in the SAME PR that adds the state — "
        f"do not merge the SF JSON change ahead of the new registry key. "
        f"For removals or renames, keep the old registry key until AFTER the "
        f"consuming SF change merges: a lib pin bump can carry a premature "
        f"removal or rename just as easily as an addition. Registry editors "
        f"must use the additive-then-subtractive protocol."
    )


@pytest.mark.parametrize("label,json_path", _SF_JSON_FILES)
def test_wait_companions_in_json_are_in_wait_grouping(
    label: str, json_path: Path
) -> None:
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

    assert not missing, _provenance_note(
        _installed_lib_version(),
        _pinned_lib_version((REPO_ROOT / "requirements.txt").read_text()),
    ) + (
        f"{label} SF has {len(missing)} ``WaitFor*`` state(s) NOT in "
        f"nousergon_lib.pipeline_status.registry.WAIT_GROUPING: "
        f"{sorted(missing)}. Each must map to its parent Task state name; "
        f"otherwise the wait companion will render as its own row instead "
        f"of rolling up."
    )


def test_groom_sf_registry_coverage_visibility() -> None:
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


# ── The provenance qualifier's own tests (alpha-engine-config-I9116) ─────────
#
# Both halves, or the change is worse than what it replaced: the corrected
# message must appear under a stale-lib condition, AND a genuine drift with the
# versions in lockstep must still fail loudly and still name the states.


def test_pin_parser_reads_the_pin_and_not_the_comments_above_it() -> None:
    """The comment trap. requirements.txt names superseded tags in prose
    directly above the live pin; a matcher that harvested those would report a
    mismatch against a version nobody installed."""
    text = (
        "# nousergon-lib-PR311 adds all five and MUST merge first; v0.124.53 was\n"
        "# the floor. Superseded by nousergon-lib @ git+https://x/y@v0.124.78.\n"
        "some-other-pkg==1.2.3\n"
        "nousergon-lib[arcticdb,rag] @ git+https://github.com/nousergon/"
        "nousergon-lib@v0.124.96\n"
    )
    assert _pinned_lib_version(text) == "v0.124.96"


def test_pin_parser_reads_the_real_requirements_file() -> None:
    """A parser proven only on a fixture is a parser nobody ran on the real
    input. The live file must yield a pin, or the qualifier silently degrades to
    'unreadable' and never fires."""
    pinned = _pinned_lib_version((REPO_ROOT / "requirements.txt").read_text())
    assert pinned is not None and pinned.startswith("v"), (
        "requirements.txt no longer yields a nousergon-lib pin this parser can "
        "read. Fix _PIN_RE — a silently unparseable pin turns the provenance "
        "qualifier off without failing anything."
    )


def test_a_stale_install_is_named_first_in_the_failure_message() -> None:
    """THE case that made this a defect. On 2026-08-28 a laptop venv held
    v0.124.88 against a v0.124.96 pin, and the message sent the reader to
    nousergon-lib to add a `NormalizeRunDates` entry that already existed
    there."""
    note = _provenance_note("v0.124.88", "v0.124.96")
    assert note, "a version mismatch produced no qualifier at all"
    assert note.startswith("READ THIS FIRST"), (
        "the qualifier must LEAD. Appended below a 10-line accusation about the "
        "SF definition, it is not read."
    )
    assert "v0.124.88" in note and "v0.124.96" in note, (
        f"the qualifier names neither version, so it cannot be acted on:\n{note}"
    )
    assert "pip install -r requirements.txt" in note, (
        f"the qualifier does not say what to actually do:\n{note}"
    )


def test_matched_versions_produce_no_qualifier() -> None:
    """It is a qualifier, not a suppressor. With the clocks in lockstep the
    failure must read exactly as it did before — an accusation against the SF
    definition, which is then correct."""
    assert _provenance_note("v0.124.96", "v0.124.96") == ""


def test_unreadable_provenance_produces_no_qualifier() -> None:
    """An unreadable pin or a missing distribution must not invent a mismatch.
    Absence of evidence is not a version skew — the same conflation that made
    a denied `iam:GetRole` read as an absent role."""
    assert _provenance_note(None, "v0.124.96") == ""
    assert _provenance_note("v0.124.96", None) == ""


def test_real_drift_still_fails_loudly_when_versions_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half. Inject a substantive Task state that no registry entry
    covers, with installed == pinned, and assert the check still fails AND still
    names the offending state. Without this, the change could have turned a real
    invariant into a version-mismatch reporter."""
    real_pin = _pinned_lib_version((REPO_ROOT / "requirements.txt").read_text())
    monkeypatch.setattr(
        "tests.test_pipeline_status_registry_source_check._installed_lib_version",
        lambda: real_pin,
        raising=False,
    )
    resource = next(iter(SUBSTANTIVE_RESOURCES))
    invented = "AStateNoRegistryEverHeardOf"
    assert invented not in STATE_TO_ARCHIVE_PAGE
    states = {invented: {"Type": "Task", "Resource": resource}}
    found = _walk_substantive_task_states(states, set())
    assert found == {invented}, (
        "the walker no longer recognises a plain substantive Task state, so the "
        "invariant would pass vacuously on real drift"
    )
    missing = found - set(STATE_TO_ARCHIVE_PAGE.keys()) - set(WAIT_GROUPING.keys())
    note = _provenance_note(real_pin, real_pin)
    message = note + f"has {len(missing)} substantive Task state(s): {sorted(missing)}"
    assert note == "", "versions match, so no qualifier should precede the drift text"
    assert invented in message, (
        "a genuine drift failure no longer names the offending state"
    )


def test_the_real_assertion_renders_the_qualifier_under_a_stale_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end, through the REAL assertion — not the helper in isolation.

    Reproduces the measured 2026-08-28 condition: the working tree's Saturday SF
    is unchanged and correct, but the installed lib is behind the pin and its
    registry is missing a state the SF has. The rendered AssertionError must LEAD
    with the version skew, because that is where the reader has to go.
    """
    label, json_path = _SF_JSON_FILES[0]
    substantive = _all_substantive_states(json_path) - set(WAIT_GROUPING.keys())
    assert substantive, "the Saturday SF walked to zero substantive states"
    dropped = sorted(substantive)[0]

    stale_registry = {k: v for k, v in STATE_TO_ARCHIVE_PAGE.items() if k != dropped}
    monkeypatch.setattr(
        "tests.test_pipeline_status_registry_source_check.STATE_TO_ARCHIVE_PAGE",
        stale_registry,
    )
    monkeypatch.setattr(
        "tests.test_pipeline_status_registry_source_check._installed_lib_version",
        lambda: "v0.124.88",
    )

    with pytest.raises(AssertionError) as excinfo:
        test_every_substantive_state_has_registry_entry(label, json_path)

    rendered = str(excinfo.value)
    assert rendered.startswith("READ THIS FIRST"), (
        "under a stale install the failure still opens by accusing the SF "
        f"definition:\n{rendered}"
    )
    assert "v0.124.88" in rendered, rendered
    assert dropped in rendered, (
        "the qualifier swallowed the underlying finding — it must ADD context, "
        f"never replace it:\n{rendered}"
    )


def test_the_real_assertion_accuses_the_sf_when_versions_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """And the inverse: with the clocks in lockstep, a genuine gap must produce
    the original, correct accusation with no hedge in front of it."""
    label, json_path = _SF_JSON_FILES[0]
    substantive = _all_substantive_states(json_path) - set(WAIT_GROUPING.keys())
    dropped = sorted(substantive)[0]

    monkeypatch.setattr(
        "tests.test_pipeline_status_registry_source_check.STATE_TO_ARCHIVE_PAGE",
        {k: v for k, v in STATE_TO_ARCHIVE_PAGE.items() if k != dropped},
    )
    pinned = _pinned_lib_version((REPO_ROOT / "requirements.txt").read_text())
    monkeypatch.setattr(
        "tests.test_pipeline_status_registry_source_check._installed_lib_version",
        lambda: pinned,
    )

    with pytest.raises(AssertionError) as excinfo:
        test_every_substantive_state_has_registry_entry(label, json_path)

    rendered = str(excinfo.value)
    assert "READ THIS FIRST" not in rendered, (
        f"a version qualifier appeared with the versions in lockstep:\n{rendered}"
    )
    assert dropped in rendered and "STATE_TO_ARCHIVE_PAGE" in rendered, rendered
    assert "do not merge the SF JSON change ahead of the new registry key" in rendered
    assert (
        "keep the old registry key until AFTER the consuming SF change merges"
        in rendered
    )
    assert "a lib pin bump can carry a premature removal or rename" in rendered
    assert "additive-then-subtractive protocol" in rendered
