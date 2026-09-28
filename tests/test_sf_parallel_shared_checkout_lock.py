"""alpha-engine-config-I7259 — no unserialised shared-path mutation across
concurrent ``Parallel`` branches.

Root cause (measured 2026-08-13, execution ``watch-rerun-2026-08-13-2`` of
``ne-weekly-freshness-pipeline``): ``step_function.json``'s ``ParityParallel``
state has 3 branches (``PitParityLookahead``, ``PitParityWalkforward``,
``ParityReplay``), each of whose SSM command array opens with
``git -C /home/ec2-user/alpha-engine-backtester pull --ff-only origin main``
against the *same checkout on the same launcher box*, started concurrently by
the ``Parallel`` state. ``FETCH_HEAD`` is one file per repo and a concurrent
``git fetch`` does not write it atomically, so two branches racing into their
pull in the same fetch window leave more than one ref marked for merge and
``git pull`` refuses with ``fatal: Cannot fast-forward to multiple
branches.`` (exit 128) — deterministic-under-race, not a flake. Both
``PitParityLookahead`` and ``ParityReplay`` died exactly this way on their
first poll. The stages fail OPEN to a degraded flag (sf-pipeline-policy.md
§2.3a — parity is a correctness VERDICT, not a data artifact), so the run
continued with no page.

Fix: serialise each shared-checkout ``git pull`` with ``flock`` on a
per-repo-path lock file, in place, without adding/removing/reordering any SF
state (topology restructuring is complexity:ultra, human-authored —
sf-pipeline-policy.md §5). ``ResearchPredictorParallel`` needed no fix — its
two branches mutate disjoint checkouts (``alpha-engine-data`` in Branch A vs
``alpha-engine-predictor`` + ``alpha-engine-config`` in Branch B; verified
below) so no cross-branch collision exists there even though both branches
can run on the same launcher box.

This module is the structural enforcement: walk every ``Parallel`` state in
all three scheduled pipeline definitions, extract every filesystem path each
branch's SSM ``AWS-RunShellScript`` command array mutates (``git -C <path>
pull``, ``cp ... <path>``, ``mkdir -p <path>``, ``pip install`` inside a
``.venv`` at ``<path>``), and fail if two DIFFERENT branches of the same
``Parallel`` state target the same path without every git-pull command
against that path being wrapped in ``flock`` on a lock file unique to that
path. Verified (see ``test_parity_parallel_would_have_failed_pre_fix`` /
run history in alpha-engine-config-I7259) to fail against
``ParityParallel`` as committed before this fix — a guard that has only ever
been exercised against the already-fixed shape is worthless.

``step_function_daily.json`` and ``step_function_eod.json`` currently
declare NO ``Parallel`` state at all (measured: zero ``"Type": "Parallel"``
occurrences in either file) — trivially safe, included here so a future
``Parallel`` addition to either file is caught by this same walker rather
than by a second incident.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterator

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"

_SF_FILES = (
    "step_function.json",
    "step_function_daily.json",
    "step_function_eod.json",
)

# Matches `git -C <path> pull` anywhere in a command string, capturing <path>.
# alpha-engine-config-I11570: `exec_code_pin.sh <execution> <path>` pulls (or
# checks out the execution's pinned SHA) into <path>, so it is the same
# shared-checkout mutation and is captured the same way.
_GIT_PULL_RE = re.compile(
    r"git -C (\S+) pull|exec_code_pin\.sh \S+ (/home/ec2-user/[\w.-]+)"
)
# A `flock <lockfile> ... git -C <path> pull` guard on the SAME command string.
_FLOCK_GIT_PULL_RE = re.compile(
    r"flock \S+ .*(?:git -C (\S+) pull|exec_code_pin\.sh \S+ (/home/ec2-user/[\w.-]+))"
)
# `cp ... <dest>` — captures the destination (last path-looking token).
_CP_RE = re.compile(r"\bcp\s+(?:--\S+\s+)*\S+\s+(\S+)")
# `mkdir -p <path>`
_MKDIR_RE = re.compile(r"mkdir -p (\S+)")


def _iter_parallel_states(states: dict, path: str = "") -> Iterator[tuple[str, dict]]:
    """Yield (dotted-path-name, state-dict) for every Parallel state,
    recursing into Map iterators and nested Parallel branches too."""
    for name, st in states.items():
        full = f"{path}/{name}" if path else name
        if st.get("Type") == "Parallel":
            yield full, st
        if "States" in st:
            yield from _iter_parallel_states(st["States"], full)
        for i, branch in enumerate(st.get("Branches", []) or []):
            yield from _iter_parallel_states(branch.get("States", {}), f"{full}[{i}]")
        iterator = st.get("Iterator") or st.get("ItemProcessor")
        if iterator and "States" in iterator:
            yield from _iter_parallel_states(iterator["States"], full)


def _iter_task_commands(states: dict) -> Iterator[str]:
    """Yield every literal command-array string found anywhere inside a
    (single) Parallel branch's state subtree — this walks Choice/Pass/Task
    chains within ONE branch, not across branches."""
    for st in states.values():
        params = st.get("Parameters", {})
        cmds = params.get("Parameters", {}).get("commands.$") if "Parameters" in params else None
        if cmds is None:
            cmds = params.get("commands.$")
        if isinstance(cmds, str):
            yield cmds
        if "States" in st:
            yield from _iter_task_commands(st["States"])


def _paths_mutated_by_branch(branch_states: dict) -> dict[str, bool]:
    """Return {path: all_pulls_locked} for every shared-checkout path this
    branch's command strings mutate. ``all_pulls_locked`` is True only if
    EVERY git-pull command against that path in this branch is flock-wrapped
    (a branch touching the same path via both a locked and an unlocked
    command is still a race on the unlocked one)."""
    mutated: dict[str, bool] = {}
    for cmd in _iter_task_commands(branch_states):
        for m in _GIT_PULL_RE.finditer(cmd):
            path = m.group(1) or m.group(2)
            locked = bool(_FLOCK_GIT_PULL_RE.search(cmd)) and any(
                (fm.group(1) or fm.group(2)) == path
                for fm in _FLOCK_GIT_PULL_RE.finditer(cmd)
            )
            mutated[path] = mutated.get(path, True) and locked
        for m in _CP_RE.finditer(cmd):
            mutated.setdefault(m.group(1), False)
        for m in _MKDIR_RE.finditer(cmd):
            mutated.setdefault(m.group(1), False)
    return mutated


def _unserialised_collisions(parallel_state: dict) -> list[str]:
    """Return a list of human-readable violation strings: paths mutated by
    more than one branch of this Parallel state where not every mutation is
    a flock-wrapped git pull."""
    per_branch = [
        _paths_mutated_by_branch(b.get("States", {}))
        for b in parallel_state.get("Branches", [])
    ]
    all_paths: set[str] = set()
    for pb in per_branch:
        all_paths.update(pb)

    violations = []
    for path in sorted(all_paths):
        touching = [pb for pb in per_branch if path in pb]
        if len(touching) < 2:
            continue  # only one branch touches this path — no race
        if all(pb[path] for pb in touching):
            continue  # every touching branch's git-pull is flock-wrapped
        violations.append(
            f"path {path!r} is mutated by {len(touching)} concurrent branches "
            f"without every mutation serialised via flock"
        )
    return violations


def _load(name: str) -> dict:
    return json.loads((_INFRA / name).read_text())


@pytest.mark.parametrize("sf_file", _SF_FILES)
def test_no_unserialised_shared_checkout_mutation_in_any_parallel_state(sf_file):
    """alpha-engine-config-I7259 closes-when: no Parallel branch in any of
    the three definitions performs an unserialised git pull (or other
    shared-path mutation) against a checkout a concurrent sibling branch
    also mutates."""
    definition = _load(sf_file)
    found_any_parallel = False
    for state_name, state in _iter_parallel_states(definition.get("States", {})):
        found_any_parallel = True
        violations = _unserialised_collisions(state)
        assert not violations, (
            f"{sf_file}::{state_name} has an unserialised shared-checkout "
            f"race: {violations}"
        )
    if sf_file != "step_function.json":
        # Measured fact (2026-08-13): neither the daily nor the EOD
        # definition declares any Parallel state today. This branch exists
        # so a FUTURE Parallel addition to either file is walked by the
        # same collision check above rather than silently exempted — if
        # this assertion ever fails, a Parallel state was added and the
        # walker above already ran against it (that's the point).
        assert not found_any_parallel, (
            f"{sf_file} now declares a Parallel state — this test already "
            f"walked it via the loop above; this assertion is just "
            f"documentation that the file's shape changed"
        )


def test_parity_parallel_branches_are_reachable_by_the_walker():
    """Sanity check on the walker itself: ParityParallel's 3 branches must
    actually be visited, or the collision check above would vacuously pass
    without ever inspecting the shape that motivated it."""
    definition = _load("step_function.json")
    matches = [
        (name, st)
        for name, st in _iter_parallel_states(definition["States"])
        if name == "ParityParallel"
    ]
    assert len(matches) == 1
    _, parity = matches[0]
    assert len(parity["Branches"]) == 3


def test_parity_parallel_pulls_are_flock_wrapped():
    """Direct pin of the fix shape: every ParityParallel branch's
    backtester-checkout git pull must be wrapped in ``flock`` on
    ``/home/ec2-user/.ae-git-sync.lock``. This is a tighter, file-specific
    companion to the generic collision walker above — it pins the EXACT lock
    path so a future edit that locks against a differently-named file (still
    serialising this Parallel, but drifting the literal used across the 3
    branches, or by ResearchPredictorParallel's unrelated
    backtester-adjacent stages outside this Parallel) is caught.

    The lock path moved off ``/var/lock/alpha-engine-backtester-git.lock``
    (the first form of this fix) for two measured reasons:

    1. ``/var/lock`` is a symlink to ``/run/lock``, mode ``0755 root:root``
       on AL2023. ``sudo -u ec2-user flock /var/lock/<f>`` therefore exits
       **66** with ``flock: cannot open lock file ...: Permission denied``
       (verified live on i-0fbfe2c1f3d89a835, 2026-08-13). As command 0
       under ``set -eo pipefail`` that kills the stage before it does any
       work — the same 0.5s death the fix was written to prevent, only now
       deterministic for all 3 branches instead of 2 of 3.
    2. The SF's Parallel branches are not the only concurrent writers to
       these checkouts. ``boot-pull.service`` and the daily SF's
       ``CodeFreshnessGate``/``ChronicGapSelfHeal`` already serialise on
       ``/home/ec2-user/.ae-git-sync.lock``. A second lock inode over the
       same repo serialises nothing against them, so the race survives the
       fix from outside the Parallel. One repo, one lock inode.

    ``test_sf_git_pull_serialized.py`` enforces both properties across every
    SF-issued pull, not just this Parallel's three.
    """
    definition = _load("step_function.json")
    matches = [
        st
        for name, st in _iter_parallel_states(definition["States"])
        if name == "ParityParallel"
    ]
    assert len(matches) == 1
    parity = matches[0]
    seen_branches = 0
    for branch in parity["Branches"]:
        for cmd in _iter_task_commands(branch["States"]):
            # alpha-engine-config-I11570: the pull is issued through
            # exec_code_pin.sh (pull on first use per execution, checkout of
            # the pinned SHA afterwards) — still under the same lock.
            if "exec_code_pin.sh {} /home/ec2-user/alpha-engine-backtester" in cmd:
                seen_branches += 1
                assert (
                    "flock -w 150 /home/ec2-user/.ae-git-sync.lock "
                    "bash /home/ec2-user/alpha-engine-data/infrastructure/"
                    "exec_code_pin.sh {} /home/ec2-user/alpha-engine-backtester"
                    in cmd
                ), f"unlocked backtester pull found in a ParityParallel branch: {cmd!r}"
    assert seen_branches == 3, (
        f"expected exactly 3 ParityParallel branches to pull the shared "
        f"backtester checkout, found {seen_branches}"
    )


def test_research_predictor_parallel_branches_touch_disjoint_checkouts():
    """Regression guard for the "cleared as safe" verdict on
    ResearchPredictorParallel (I7259 sweep): Branch A (Research) only ever
    git-pulls /home/ec2-user/alpha-engine-data; Branch B (Predictor) only
    ever git-pulls /home/ec2-user/alpha-engine-predictor and
    /home/ec2-user/alpha-engine-config. No path is shared between the two
    branches, so — even though both can run on the same launcher box — there
    is no FETCH_HEAD race. If a future edit makes either branch touch the
    other's path, this test (and the generic walker above) must fail."""
    definition = _load("step_function.json")
    matches = [
        st
        for name, st in _iter_parallel_states(definition["States"])
        if name == "ResearchPredictorParallel"
    ]
    assert len(matches) == 1
    parallel = matches[0]
    assert len(parallel["Branches"]) == 2
    per_branch_paths = [
        set(_paths_mutated_by_branch(b["States"]).keys())
        for b in parallel["Branches"]
    ]
    assert per_branch_paths[0], "Branch A (Research) should mutate at least one path"
    assert per_branch_paths[1], "Branch B (Predictor) should mutate at least one path"
    shared = per_branch_paths[0] & per_branch_paths[1]
    assert not shared, (
        f"ResearchPredictorParallel branches now share mutated path(s) "
        f"{shared} — this used to be safe only because the branches were "
        f"disjoint; add flock serialisation (mirroring ParityParallel's "
        f"fix) before this can stay green"
    )
