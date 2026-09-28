"""Every SF-issued `git pull` into a shared box checkout is flock-serialized
on the ONE fleet-wide lock inode, and that inode is writable by ec2-user.

2026-08-13 weekly run ``watch-rerun-2026-08-13-2``: ``ParityParallel`` fans out
three SSM commands (``PitParityLookahead``, ``PitParityWalkforward``,
``ParityReplay``) that each open with

    sudo -u ec2-user git -C /home/ec2-user/alpha-engine-backtester pull --ff-only origin main

against the SAME checkout at the same instant. Three concurrent writers to one
``FETCH_HEAD`` produced::

    From https://github.com/nousergon/crucible-backtester
     * branch            main       -> FETCH_HEAD
    fatal: Cannot fast-forward to multiple branches.
    failed to run commands: exit status 128

With ``set -eo pipefail`` as command 0, PitParityLookahead and ParityReplay
died 0.5s in — before either stage did any work. The lookahead pass artifact
was therefore never published, and ``PitParityCompare`` emitted verdict UNKNOWN
on ``{'lookahead': 'missing', 'walkforward': 'failed'}``. The same race killed
``EvaluatorDiagnostics`` and failed the whole execution.

This is a recurring class, not a new bug: the identical two-writer race was
diagnosed on the daily SF 2026-06-19 -> 07-11 (see the
``CheckSaturdayHealthCheckStatus`` comment in ``step_function_daily.json``) and
on the 2026-07-08 preopen failure (``test_sf_code_freshness_lock_retry_wiring``).
Both earlier fixes were per-state. Mutual exclusion — not retry-on-message — is
the fix that covers every signature, including ones not yet observed: the
``CodeFreshnessGate`` retry helper keys on git's ``index.lock`` phrase
("Another git process seems to be running") and is structurally blind to the
``Cannot fast-forward to multiple branches`` variant seen here.

Two clauses, because the first fix of this on the weekly SF (#1366) met one and
broke on the other:

1. **No bare pull.** Every ``git ... pull`` in every SF definition is behind
   ``flock``.
2. **One inode, ec2-user-writable.** All of them use
   ``/home/ec2-user/.ae-git-sync.lock`` — the same advisory-lock inode
   ``boot-pull.sh``, ``ChronicGapSelfHeal`` and the daily SF already acquire.
   Two lock files over one repo serialize nothing against each other, and
   ``/var/lock`` is root-owned ``0755`` on AL2023 (verified live on
   i-0fbfe2c1f3d89a835, 2026-08-13): ``sudo -u ec2-user flock
   /var/lock/<f>`` exits **66** with ``Permission denied``, which under
   ``set -eo pipefail`` kills the stage on command 0 — every time, for every
   parity stage.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_DIR = _REPO_ROOT / "infrastructure"

# The one fleet-wide git-sync advisory lock. Lives under /home/ec2-user so the
# `sudo -u ec2-user` open always succeeds (see module docstring clause 2).
CANONICAL_LOCK = "/home/ec2-user/.ae-git-sync.lock"

# alpha-engine-config-I11570: the weekly SF's pulls go through
# infrastructure/exec_code_pin.sh, which pulls on the first call of an
# execution and checks out the pinned SHA after that. It writes the same
# checkout, so it is held to the same lock clause as a bare `git pull`.
_PIN_HELPER = r"bash \S*/infrastructure/exec_code_pin\.sh\b"
_PULL = re.compile(r"git (?:-C \S+ )?pull\b|" + _PIN_HELPER)
# The guarded form: flock, any -w budget, the canonical inode, then `git` or
# the pin helper.
_GUARDED = re.compile(
    r"flock (?:-w \d+ )?"
    + re.escape(CANONICAL_LOCK)
    + r" (?:git\b|"
    + _PIN_HELPER
    + r")"
)


def _sf_definitions() -> list[Path]:
    paths = sorted(_SF_DIR.glob("step_function*.json"))
    assert paths, f"no SF definitions found under {_SF_DIR}"
    return paths


def _command_strings(node: object) -> list[str]:
    """Every SSM *command* string in the definition.

    Scoped to ``commands`` / ``commands.$`` values so a ``Comment`` that merely
    describes a git pull is not mistaken for one. Walks the whole document
    rather than reaching for known state names: a new state added later must be
    covered without editing this test.
    """
    found: list[str] = []
    if isinstance(node, dict):
        for k, v in node.items():
            if k in ("commands", "commands.$"):
                found += [s for s in _flatten_strings(v)]
            else:
                found += _command_strings(v)
    elif isinstance(node, list):
        for v in node:
            found += _command_strings(v)
    return found


def _flatten_strings(node: object) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, list):
        return [s for v in node for s in _flatten_strings(v)]
    if isinstance(node, dict):
        return [s for v in node.values() for s in _flatten_strings(v)]
    return []


def test_no_bare_git_pull_in_any_sf_definition() -> None:
    """Clause 1: no `git pull` runs outside the lock."""
    offenders: list[str] = []
    for path in _sf_definitions():
        for cmd in _command_strings(json.loads(path.read_text())):
            # Split on the ASL intrinsic's single-quoted command boundaries so
            # one flock earlier in a States.Array cannot vouch for a later
            # unguarded pull in the same string.
            for piece in cmd.split("','"):
                if _PULL.search(piece) and not _GUARDED.search(piece):
                    offenders.append(f"{path.name}: {piece.strip()[:160]}")
    assert not offenders, (
        "unserialized `git pull` into a shared box checkout — concurrent SSM "
        "commands race FETCH_HEAD and the stage dies on command 0. Wrap it as "
        f"`sudo -u ec2-user flock -w 150 {CANONICAL_LOCK} git -C ... pull ...`:\n"
        + "\n".join(offenders)
    )


def test_git_lock_is_the_single_ec2_user_writable_inode() -> None:
    """Clause 2: exactly one lock path, and it is under /home/ec2-user."""
    used: set[str] = set()
    for path in _sf_definitions():
        for m in re.finditer(
            r"flock (?:-w \d+ )?(\S+) (?:git\b|" + _PIN_HELPER + r")",
            path.read_text(),
        ):
            used.add(m.group(1))
    assert used, "no flock-guarded git call found in any SF definition"
    assert used == {CANONICAL_LOCK}, (
        "git-sync lock inode drift — two lock files over one checkout "
        "serialize nothing against each other, and a path outside "
        "/home/ec2-user is not writable by ec2-user on AL2023 "
        f"(flock exits 66). Expected only {CANONICAL_LOCK}, found: {sorted(used)}"
    )


def test_parity_parallel_branches_share_the_lock() -> None:
    """The three states whose race produced the 2026-08-13 failure, by name."""
    doc = json.loads((_SF_DIR / "step_function.json").read_text())
    text = json.dumps(doc)
    for slug in ("pit-lookahead", "pit-walkforward", "parity-replay"):
        # the SSM command that runs this stage, located by its log slug
        stage_cmds = [
            c for c in _command_strings(doc) if f"--slug {slug} " in c
        ]
        assert stage_cmds, f"no SSM command found for stage slug {slug!r}"
        for cmd in stage_cmds:
            assert _GUARDED.search(cmd), (
                f"{slug}: git pull is not flock-guarded on {CANONICAL_LOCK} — "
                "this is the exact 2026-08-13 ParityParallel race"
            )
    assert "ParityParallel" in text, "ParityParallel state disappeared"
