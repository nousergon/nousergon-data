"""Catches the nousergon-data-PR936 unbound-variable class: a variable
assigned only inside one `case` arm of a script's `--flag` arg-parse loop,
then read unconditionally after the loop with no top-level default-init.

PR936 added `--max-runtime-seconds) MAX_RUNTIME_SECONDS="$2";
MAX_RUNTIME_EXPLICIT=1; shift 2 ;;` to infrastructure/spot_data_weekly.sh's
arg-parse loop but only ever assigned MAX_RUNTIME_EXPLICIT there. Every
SF-driven invocation passes zero flags, so the loop body never runs and the
later unconditional `[ "$MAX_RUNTIME_EXPLICIT" != "1" ]` read died under
`set -u`: 'MAX_RUNTIME_EXPLICIT: unbound variable' (watch-rerun-2026-07-18-1,
fixed in PR937 by default-initializing before the read).

shellcheck does NOT catch this shape (verified locally against the pre-fix
PR936 revision with `shellcheck --severity=error` and even the optional
`check-unassigned-uppercase` check: zero findings) — its dataflow analysis
is whole-scope, not branch-path-sensitive, so a var assigned anywhere in the
case block reads as "assigned" regardless of whether that arm is reachable
on the zero-flag path. This test is the branch-path-sensitive check that
closes that gap: for each `infrastructure/*.sh` script that declares
`set -u`/`set -eu`/`set -euo pipefail`, find its `while [[ $# -gt 0 ]]; do
case "$1" in ... esac; done` arg-parse loop, collect every UPPER_CASE
variable assigned inside a case arm, and — for each such variable that is
also read after the loop — require an unconditional top-level (column-0,
so not itself inside an `if`/case) default-init assignment somewhere before
that read. `ID_ARTIFACT_KEY="${ID_ARTIFACT_KEY:-}"` right after the loop in
spot_data_weekly.sh is the correct pattern this test accepts; PR936's
MAX_RUNTIME_EXPLICIT (no default-init anywhere) is exactly what it flags.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA_SCRIPTS = sorted((_REPO_ROOT / "infrastructure").glob("*.sh"))

_SET_U_RE = re.compile(r"^\s*set\s+-[a-z]*u[a-z]*\b", re.MULTILINE)
_LOOP_RE = re.compile(
    r'while\s*\[\[?\s*\$#\s*-gt\s*0\s*\]\]?\s*;?\s*do\s*\n'
    r'\s*case\s+"\$1"\s+in\n(.*?)\n\s*esac\s*\ndone',
    re.DOTALL,
)
_ARM_ASSIGN_RE = re.compile(r'\b([A-Z][A-Z0-9_]*)=')


def _find_unguarded_loop_vars(text: str) -> list[tuple[str, int]]:
    """Return (var, 1-based line number of the offending read) for every
    case-arm-only variable read after the loop with no prior default-init."""
    if not _SET_U_RE.search(text):
        return []
    loop_m = _LOOP_RE.search(text)
    if not loop_m:
        return []
    loop_start, loop_end = loop_m.start(), loop_m.end()
    loop_vars = set(_ARM_ASSIGN_RE.findall(loop_m.group(1)))

    after = text[loop_end:]
    findings = []
    for var in sorted(loop_vars):
        read_m = re.search(rf'\$\{{?{var}\b', after)
        if not read_m:
            continue  # never read outside the loop -> not this bug class
        default_re = re.compile(rf'^{var}=', re.MULTILINE)
        already_defaulted = default_re.search(
            text[:loop_start]
        ) or default_re.search(after[: read_m.start()])
        if already_defaulted:
            continue
        line_no = text.count("\n", 0, loop_end + read_m.start()) + 1
        findings.append((var, line_no))
    return findings


@pytest.mark.parametrize(
    "script", _INFRA_SCRIPTS, ids=[s.name for s in _INFRA_SCRIPTS]
)
def test_no_unguarded_case_arm_only_vars(script: Path):
    findings = _find_unguarded_loop_vars(script.read_text())
    assert not findings, (
        f"{script.name}: variable(s) assigned only inside a `case` arm of "
        f"the arg-parse loop, then read after the loop with no top-level "
        f"default-init before that read (config#2949, PR936/PR937 bug "
        f"class — the zero-flag SF-driven invocation never runs the loop "
        f"body, so these die 'unbound variable' under set -u): {findings}. "
        f"Fix: default-init each one right after the loop, e.g. "
        f'VAR="${{VAR:-<default>}}" (see ID_ARTIFACT_KEY in '
        f"spot_data_weekly.sh for the pattern)."
    )


def test_harness_catches_the_pr936_regression_shape(tmp_path):
    """Meta-test: pin the checker itself against a minimal repro of the
    actual PR936 bug, so a future refactor of the regex/logic above can't
    silently stop catching the incident it exists for."""
    script = tmp_path / "repro.sh"
    script.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "while [[ $# -gt 0 ]]; do\n"
        "    case \"$1\" in\n"
        "        --max-runtime-seconds) MAX_RUNTIME_SECONDS=\"$2\"; "
        "MAX_RUNTIME_EXPLICIT=1; shift 2 ;;\n"
        "        *) echo \"Unknown flag: $1\"; exit 1 ;;\n"
        "    esac\n"
        "done\n"
        'if [ "$MAX_RUNTIME_EXPLICIT" != "1" ]; then\n'
        "    MAX_RUNTIME_SECONDS=5400\n"
        "fi\n"
    )
    findings = _find_unguarded_loop_vars(script.read_text())
    assert [var for var, _ in findings] == ["MAX_RUNTIME_EXPLICIT"]


def test_harness_accepts_the_pr937_fix_shape(tmp_path):
    """The default-initialized fix must NOT be flagged."""
    script = tmp_path / "fixed.sh"
    script.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'MAX_RUNTIME_EXPLICIT="${MAX_RUNTIME_EXPLICIT:-0}"\n'
        "while [[ $# -gt 0 ]]; do\n"
        "    case \"$1\" in\n"
        "        --max-runtime-seconds) MAX_RUNTIME_SECONDS=\"$2\"; "
        "MAX_RUNTIME_EXPLICIT=1; shift 2 ;;\n"
        "        *) echo \"Unknown flag: $1\"; exit 1 ;;\n"
        "    esac\n"
        "done\n"
        'if [ "$MAX_RUNTIME_EXPLICIT" != "1" ]; then\n'
        "    MAX_RUNTIME_SECONDS=5400\n"
        "fi\n"
    )
    assert _find_unguarded_loop_vars(script.read_text()) == []
