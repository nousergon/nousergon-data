"""`requirement_pin` (infrastructure/lambdas/_shared/run_handler_tests.sh), EXECUTED.

WHY (alpha-engine-config-I10337, eval-judge-spot-dispatcher runs 34711758231
and 34312894868): eight deploy.sh scripts each did

    KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)

and handed the WHOLE matched requirements.txt line — trailing lockstep-bump
comment included — to `run_handler_tests`, which installs it as a bare pip
command-line argument. A requirements FILE tolerates an inline `# ...`
comment; a pip argument does not:

    ERROR: Invalid requirement: 'krepis==0.59.54  # bumped 0.59.41 -> 0.59.54:
    nousergon-lib v0.124.116 (root requirements.txt) floors krepis>=0.59.52;
    alpha-engine-config-I10226 lockstep gap, run 34309659890'

eval-judge-spot-dispatcher's pin happened to carry the comment on the day
this broke; the other seven sites (and the co-located `nousergon-lib` pins in
six of them) carried the identical unguarded `grep | head -1` and would break
the next time a lockstep bump annotated THEIR line. `requirement_pin` is the
one place this now lives.

These tests execute the real shell function (sourced from the real file, not
a copy) against real fixture requirements.txt files — a test asserting on the
shell SOURCE would pass against the broken version too, since the broken
version also contains the string `head -1`.
"""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_HELPER = _REPO_ROOT / "infrastructure" / "lambdas" / "_shared" / "run_handler_tests.sh"


def _run_requirement_pin(req_body: str, pkg: str, tmp_path: Path) -> subprocess.CompletedProcess:
    req_file = tmp_path / "requirements.txt"
    req_file.write_text(textwrap.dedent(req_body))
    script = f'source "{_HELPER}"\nrequirement_pin "{req_file}" "{pkg}"\n'
    return subprocess.run(  # noqa: S603
        ["bash", "-c", script],  # noqa: S607
        capture_output=True, text=True, check=False,
    )


def test_strips_trailing_inline_comment(tmp_path: Path) -> None:
    """The exact eval-judge-spot-dispatcher shape: a lockstep-bump comment
    after two spaces must not survive into the printed pin."""
    body = """\
        nousergon-lib @ git+https://github.com/nousergon/nousergon-lib@v0.124.120
        krepis==0.59.54  # bumped 0.59.41 -> 0.59.54: nousergon-lib v0.124.116 (root requirements.txt) floors krepis>=0.59.52; alpha-engine-config-I10226 lockstep gap, run 34309659890
    """
    result = _run_requirement_pin(body, "krepis", tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "krepis==0.59.54"


def test_line_without_a_comment_is_unchanged(tmp_path: Path) -> None:
    body = """\
        nousergon-lib @ git+https://github.com/nousergon/nousergon-lib@v0.124.120
        krepis>=0.59.6
    """
    result = _run_requirement_pin(body, "krepis", tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "krepis>=0.59.6"


def test_matches_first_of_multiple_and_ignores_bracket_extras(tmp_path: Path) -> None:
    """`nousergon-lib[flow-doctor] @ git+...` (pipeline-watchdog's actual pin
    shape) must still match `^nousergon-lib` the way the old grep did."""
    body = """\
        nousergon-lib[flow-doctor] @ git+https://github.com/nousergon/nousergon-lib@v0.124.120
        krepis>=0.15.0
    """
    result = _run_requirement_pin(body, "nousergon-lib", tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "nousergon-lib[flow-doctor] @ git+https://github.com/nousergon/nousergon-lib@v0.124.120"


def test_missing_package_fails_loud(tmp_path: Path) -> None:
    """The old `grep | head -1` silently produced an empty string on no
    match, which `run_handler_tests` would then install as nothing extra at
    all — a missing pin read as 'no deps needed'. The helper must exit
    non-zero instead."""
    body = "nousergon-lib @ git+https://github.com/nousergon/nousergon-lib@v0.124.120\n"
    result = _run_requirement_pin(body, "krepis", tmp_path)
    assert result.returncode != 0
    assert "no 'krepis' requirement found" in result.stderr
    assert result.stdout.strip() == ""


def test_bare_name_with_trailing_comment_still_prints_the_bare_name(tmp_path: Path) -> None:
    """An unpinned-but-declared line (`krepis  # TODO pin this properly`) is a
    valid pip spec once the comment is stripped — the package name alone —
    and must not be reported as an error just because nothing follows it."""
    body = "krepis  # TODO pin this properly\n"
    result = _run_requirement_pin(body, "krepis", tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "krepis"


@pytest.mark.parametrize("pkg", ["krepis", "nousergon-lib"])
def test_real_lambda_requirements_files_parse_clean(pkg: str, tmp_path: Path) -> None:
    """Run the helper against every real requirements.txt under
    infrastructure/lambdas that declares this package, and confirm the
    printed pin is a valid pip requirement spec (no stray `#`)."""
    lambdas_dir = _REPO_ROOT / "infrastructure" / "lambdas"
    checked = 0
    for req_file in lambdas_dir.glob("*/requirements.txt"):
        text = req_file.read_text()
        if not any(line.strip().startswith(pkg) for line in text.splitlines()):
            continue
        script = f'source "{_HELPER}"\nrequirement_pin "{req_file}" "{pkg}"\n'
        result = subprocess.run(  # noqa: S603
            ["bash", "-c", script],  # noqa: S607
            capture_output=True, text=True, check=False,
        )
        assert result.returncode == 0, f"{req_file}: {result.stderr}"
        pin = result.stdout.strip()
        assert "#" not in pin, f"{req_file}: pin still carries a comment: {pin!r}"
        checked += 1
    assert checked > 0, f"no lambda requirements.txt declared {pkg} — glob or fixture drifted"


def test_no_deploy_script_still_hand_rolls_the_unguarded_grep() -> None:
    """Guard against the class coming back: no `infrastructure/lambdas/*/deploy.sh`
    may reach for `grep -E '^krepis'` / `grep -E '^nousergon-lib'` (or any
    quoting variant of the same pattern) directly — every pin must go through
    `requirement_pin`, which strips the inline comment before the value ever
    becomes a pip argument."""
    out = subprocess.run(  # noqa: S603
        ["git", "ls-files", "infrastructure/lambdas/*/deploy.sh"],  # noqa: S607
        cwd=_REPO_ROOT, capture_output=True, text=True, check=True,
    )
    paths = [_REPO_ROOT / line for line in out.stdout.split() if line]
    assert paths, "no deploy.sh discovered — the glob or cwd is wrong"

    offenders: list[str] = []
    for path in paths:
        body = path.read_text()
        for needle in ("grep -E '^krepis'", "grep -E '^nousergon-lib'",
                       'grep -E "^krepis"', 'grep -E "^nousergon-lib"'):
            if needle in body:
                offenders.append(f"{path.parent.name}: still has `{needle}`")
    assert not offenders, (
        "deploy.sh scripts hand-rolling a requirement-pin grep instead of using "
        "the shared requirement_pin helper (alpha-engine-config-I10337):\n  "
        + "\n  ".join(offenders)
        + "\n\nUse:\n"
        '  KREPIS_REQ=$(requirement_pin "${SCRIPT_DIR}/requirements.txt" krepis)\n'
    )
