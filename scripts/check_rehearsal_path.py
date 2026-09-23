#!/usr/bin/env python3
"""Enforce weekly-sf-policy.md §7 (Rehearsal path requirement) on PRs that
touch weekly-SF-critical paths (alpha-engine-config-I6688).

Policy summary (nous-ergon-ops/policies/weekly-sf-policy.md §7): a weekly-SF
change whose validation "needs a live Saturday" must, in the SAME PR, take
one of four routes instead of waiting for one:

  1. Nominate a rehearsal path (§7.1) — a `Rehearsal-path:` trailer naming
     one of the two existing mechanisms (the Friday shell-run,
     `infrastructure/run_weekly_offcycle.sh`; or the Replay canary,
     `.github/workflows/canary-replay.yml` / the `canary:replay` label /
     `alpha-engine-canary-replay-dispatcher`), OR a CI-validatable
     alternative — a repo-relative test path that actually exists in the
     checked-out tree (§7.2 route 1).
  2. Declare one of the two §7.3 exemptions, verbatim: `structural` (CI
     already covers pure structural validation — IAM grants, SF definition
     syntax, static analysis) or `complexity:ultra` (human-authored
     topology changes per §5).
  3. Record a `Rehearsal-risk-acceptance:` paragraph (§7.2 route 3) — the
     specific constraint that prevents any rehearsal (a live-only IAM
     boundary, an irreversible mutation, an externally-triggered
     dependency), for a reviewer to evaluate as a risk-acceptance decision.

A body reading only "needs a Saturday run" satisfies none of these and the
check fails — that is the exact discipline gap I6688 measured (zero
`Rehearsal-path:` hits across the fleet at origin/main, 2026-08-09).

This module is deliberately import-only logic with no I/O beyond the CLI
entrypoint, so `tests/test_check_rehearsal_path.py` can exercise every
branch directly. The invoking workflow
(`.github/workflows/rehearsal-path-guard.yml`) passes the PR body via the
`PR_BODY` env var (never spliced into a shell `run:` block — PR bodies are
attacker-controlled text on `pull_request`) and the checked-out repo root as
argv[1].

``WEEKLY_CRITICAL_GLOBS`` is the canonical path list for this check's own
paths-filter AND must stay in lockstep with the equivalent entries in
canary-replay.yml's filter (deliverable 2 of I6688) — pinned by
tests/test_rehearsal_path_guard_lockstep.py so the two workflows cannot
silently drift apart the way the duplicate §7/§8 sections of the policy
itself once did (§7.4).
"""

from __future__ import annotations

import fnmatch
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# Kept identical, in both entries and order, to the `weekly_critical` filter
# in .github/workflows/rehearsal-path-guard.yml and to the weekly-critical
# subset of .github/workflows/canary-replay.yml's filter. Pinned by
# tests/test_rehearsal_path_guard_lockstep.py.
WEEKLY_CRITICAL_GLOBS: tuple[str, ...] = (
    "infrastructure/step_function*.json",
    "infrastructure/spot_*.sh",
    "infrastructure/run_weekly_offcycle.sh",
    "scripts/weekly_sf_rerun.py",
    # alpha-engine-config-I11435: the in-repo Lambdas the weekly SF
    # (infrastructure/step_function.json) invokes whose failure fails or
    # degrades the Saturday run. nousergon-data-PR1879 changed the weekly
    # box's spot rotation in the first of these and this check reported
    # `skipping`. Every in-repo Lambda the weekly SF invokes must be either
    # here or in WEEKLY_SF_LAMBDAS_NOT_CRITICAL below, pinned by
    # tests/test_rehearsal_path_guard_lockstep.py.
    "infrastructure/lambdas/weekly-freshness-spot-dispatcher/**",
    "infrastructure/lambdas/eval-judge-spot-dispatcher/**",
    "infrastructure/lambdas/weekly-preflight/**",
    "infrastructure/lambdas/substrate-health-gate/**",
    "infrastructure/lambdas/weekly-run-scope/**",
)

# In-repo Lambdas the weekly SF invokes that are deliberately NOT
# weekly-critical, each with the reason. A Lambda directory the weekly SF
# reaches must appear here or in WEEKLY_CRITICAL_GLOBS, so a new weekly
# stage that moves into a Lambda is decided explicitly rather than by
# omission (alpha-engine-config-I11435 deliverable 4).
WEEKLY_SF_LAMBDAS_NOT_CRITICAL: dict[str, str] = {
    "weekly-coverage-sweep": (
        "post-run observer: it reads what the run produced and pages on "
        "gaps. Its Catch routes to WeeklyCoverageSweepUnavailable, which "
        "pages; a regression cannot change what the run computes."
    ),
}

# §7.1 mechanism names/aliases a `Rehearsal-path:` trailer may cite. Matched
# case-insensitively as a substring so both the artifact and trigger names
# from the policy table are accepted (e.g. "canary-replay" or
# "canary:replay" or "alpha-engine-canary-replay-dispatcher").
_MECHANISM_KEYWORDS = (
    "run_weekly_offcycle.sh",
    "canary-replay",
    "canary:replay",
    "canary_replay",
)

# §7.3, exhaustive. Matched exactly (case-insensitive), not by substring —
# these are declared exemptions, not free text.
_EXEMPT_VALUES = {"structural", "complexity:ultra"}

# Trailers are matched as `Key: value` at the start of a line, mirroring how
# `Verified-when:` / `Closes-when:` trailers are already written across the
# fleet's PR bodies.
_REHEARSAL_PATH_RE = re.compile(r"^Rehearsal-path:[ \t]*(.+)$", re.MULTILINE)
_REHEARSAL_EXEMPT_RE = re.compile(r"^Rehearsal-exempt:[ \t]*(.+)$", re.MULTILINE)
# Risk-acceptance is a *paragraph* (§7.2 route 3), not a single trailer
# value — capture the rest of the line plus every following line up to the
# next blank line or the end of the body.
_REHEARSAL_RISK_RE = re.compile(
    r"^Rehearsal-risk-acceptance:[ \t]*(.*(?:\n(?![ \t]*\n)[^\n]*)*)",
    re.MULTILINE,
)

# A risk-acceptance paragraph shorter than this is treated as a placeholder,
# not a recorded constraint — the policy requires "the specific constraint",
# not an empty trailer.
_MIN_RISK_ACCEPTANCE_CHARS = 20

_ACCEPTED_FORMS = """Accepted forms (weekly-sf-policy.md §7), any ONE of:
  1. `Rehearsal-path: <mechanism or test path>` naming one of the §7.1
     mechanisms (a value containing "run_weekly_offcycle.sh" or
     "canary-replay"/"canary:replay"), OR a repo-relative path to a test
     that exists in the checked-out tree (§7.2 route 1).
  2. `Rehearsal-exempt: structural` or `Rehearsal-exempt: complexity:ultra`
     (§7.3, exhaustive — exactly these two values).
  3. A `Rehearsal-risk-acceptance:` paragraph naming the specific constraint
     that prevents any rehearsal (§7.2 route 3) — a live-only IAM boundary,
     an irreversible mutation, an externally-triggered dependency.

"Needs a Saturday run" alone is not a legitimate gate (§7.2)."""


@dataclass
class CheckResult:
    ok: bool
    message: str


def weekly_critical_paths_touched(
    changed_paths: list[str], globs: tuple[str, ...] = WEEKLY_CRITICAL_GLOBS
) -> list[str]:
    """Return the subset of ``changed_paths`` matching a weekly-critical glob.

    Mirrors dorny/paths-filter's own glob semantics closely enough for unit
    testing; the workflow itself is the source of truth for the real gate
    (this function exists so the glob list is testable without a live CI
    run, and so a future non-GHA caller — e.g. a pre-commit hook — can reuse
    it).
    """
    hits = []
    for raw in changed_paths:
        normalized = raw.replace(os.sep, "/").lstrip("/")
        if any(fnmatch.fnmatch(normalized, glob) for glob in globs):
            hits.append(normalized)
    return hits


def _extract(pattern: re.Pattern, body: str) -> str | None:
    match = pattern.search(body)
    if not match:
        return None
    return match.group(1).strip()


def _rehearsal_path_is_valid(value: str, repo_root: Path) -> bool:
    if not value:
        return False
    lowered = value.lower()
    if any(keyword in lowered for keyword in _MECHANISM_KEYWORDS):
        return True

    # Treat the trailer value as a repo-relative path (strip surrounding
    # backticks/quotes and any trailing prose after the first whitespace
    # token, e.g. "Rehearsal-path: tests/test_foo.py (new)").
    candidate = value.strip().strip("`\"'").split()[0] if value.split() else ""
    if not candidate:
        return False

    resolved_root = repo_root.resolve()
    candidate_path = (resolved_root / candidate).resolve()
    try:
        candidate_path.relative_to(resolved_root)
    except ValueError:
        return False  # path escapes the repo (e.g. "../../etc/passwd")
    return candidate_path.is_file()


def evaluate(body: str, repo_root: Path) -> CheckResult:
    """Evaluate a PR body against weekly-sf-policy.md §7's accepted forms."""
    body = body or ""

    rehearsal_path = _extract(_REHEARSAL_PATH_RE, body)
    if rehearsal_path is not None:
        if _rehearsal_path_is_valid(rehearsal_path, repo_root):
            return CheckResult(True, f"Rehearsal-path OK: {rehearsal_path!r}")
        return CheckResult(
            False,
            f"Rehearsal-path {rehearsal_path!r} names neither a §7.1 "
            "mechanism nor an existing repo-relative test path.\n\n"
            + _ACCEPTED_FORMS,
        )

    exempt = _extract(_REHEARSAL_EXEMPT_RE, body)
    if exempt is not None:
        if exempt.strip().lower() in _EXEMPT_VALUES:
            return CheckResult(True, f"Rehearsal-exempt OK: {exempt.strip()}")
        return CheckResult(
            False,
            f"Rehearsal-exempt {exempt!r} is not one of the two §7.3 "
            "exemptions ('structural' or 'complexity:ultra').\n\n"
            + _ACCEPTED_FORMS,
        )

    risk = _extract(_REHEARSAL_RISK_RE, body)
    if risk is not None:
        if len(risk.strip()) >= _MIN_RISK_ACCEPTANCE_CHARS:
            return CheckResult(True, "Rehearsal-risk-acceptance OK")
        return CheckResult(
            False,
            "Rehearsal-risk-acceptance paragraph is too short to name a "
            "specific constraint (§7.2 route 3 needs the actual boundary, "
            "not a placeholder).\n\n" + _ACCEPTED_FORMS,
        )

    return CheckResult(
        False,
        "This PR touches a weekly-SF-critical path but its body carries "
        "none of the required trailers.\n\n" + _ACCEPTED_FORMS,
    )


def main(argv: list[str]) -> int:
    repo_root = Path(argv[1]) if len(argv) > 1 else Path(".")
    body = os.environ.get("PR_BODY", "")
    result = evaluate(body, repo_root)
    print(result.message)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
