"""No code in this repo may write a bucket's lifecycle or versioning document.

alpha-engine-config-I10897. This repo used to carry a script that applied a
lifecycle JSON to the research bucket. ``put-bucket-lifecycle-configuration``
replaces the WHOLE lifecycle document, so a second writer silently deletes
every rule it does not list. The bucket's versioning and lifecycle are now
declared, applied on merge and drift-checked by the nous-ergon-ops repo's
bucket-config lane, and that lane is the only writer.

This guard fails when an imperative writer comes back: the CLI verbs or the
boto3 methods for bucket lifecycle and versioning, in any shell, Python or
workflow file outside ``tests/``. Tests are exempt because they call the
boto3 methods against moto's in-memory S3 to set up fixtures; they never
reach AWS. The fleet-wide scan in nous-ergon-ops applies the same verbs and
the same ``tests`` exemption across every roster repo; this one fails in this
repo's own PR, before merge.
"""

from __future__ import annotations

import io
import re
import tokenize
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Built from parts so this file is not itself a match for a plain grep.
_VERBS = ("lifecycle" + "[-_]configuration", "versioning")
PATTERN = re.compile(r"put[-_]bucket[-_](?:" + "|".join(_VERBS) + r")")

SCANNED_SUFFIXES = {".sh", ".bash", ".py", ".yml", ".yaml"}
SKIP_DIRS = {
    ".git", "tests", "__pycache__", ".venv", "venv", "node_modules",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", "site-packages",
}


def _python_hits(text: str) -> list[tuple[int, str]]:
    """Match code and string tokens, not comments, so prose cannot be a finding."""
    hits: list[tuple[int, str]] = []
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return _text_hits(text)
    lines = text.splitlines()
    for tok in tokens:
        if tok.type == tokenize.COMMENT:
            continue
        if PATTERN.search(tok.string):
            n = tok.start[0]
            hits.append((n, lines[n - 1].strip() if n - 1 < len(lines) else ""))
    return sorted(set(hits))


def _text_hits(text: str) -> list[tuple[int, str]]:
    """Shell and YAML: skip whole-line ``#`` comments, match everything else."""
    return [
        (n, line.strip())
        for n, line in enumerate(text.splitlines(), start=1)
        if not line.lstrip().startswith("#") and PATTERN.search(line)
    ]


def _scan_files() -> list[Path]:
    files = []
    for path in REPO_ROOT.rglob("*"):
        rel = path.relative_to(REPO_ROOT)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        if path.is_file() and path.suffix in SCANNED_SUFFIXES:
            files.append(path)
    return files


def find_writers() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for path in _scan_files():
        try:
            text = path.read_text()
        except UnicodeDecodeError:  # binary with a text suffix
            continue
        hits = _python_hits(text) if path.suffix == ".py" else _text_hits(text)
        if hits:
            rel = path.relative_to(REPO_ROOT).as_posix()
            found[rel] = [f"{rel}:{n}: {line}" for n, line in hits]
    return found


def test_scan_is_not_vacuous():
    files = _scan_files()
    assert len(files) >= 100, (
        f"only {len(files)} files scanned under {REPO_ROOT}; the guard would "
        f"pass by seeing nothing"
    )


def test_pattern_matches_every_verb_form():
    for verb in ("lifecycle-configuration", "lifecycle_configuration", "versioning"):
        for sep in ("-", "_"):
            sample = f"put{sep}bucket{sep}{verb}"
            assert PATTERN.search(sample), sample


def test_detector_ignores_comments_and_finds_code():
    verb = "put_bucket_" + "lifecycle_configuration"
    src = f"# {verb} in prose\nclient.{verb}(Bucket='b')\n"
    assert [n for n, _ in _python_hits(src)] == [2]
    cli = "put-bucket-" + "lifecycle-configuration"
    sh = f"# aws s3api {cli}\naws s3api {cli} --bucket b\n"
    assert [n for n, _ in _text_hits(sh)] == [2]


def test_no_imperative_bucket_lifecycle_or_versioning_writer():
    found = find_writers()
    assert not found, (
        "Imperative bucket lifecycle/versioning writer(s) found. The "
        "research bucket's lifecycle and versioning are owned by "
        "the nous-ergon-ops repo's declared bucket-config lane "
        "(alpha-engine-config-I10897); a whole-document put from here drops "
        "every rule it does not list. Change the declared rule there instead.\n"
        + "\n".join(line for lines in found.values() for line in lines)
    )
