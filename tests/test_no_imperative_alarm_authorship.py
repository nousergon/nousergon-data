#!/usr/bin/env python3
"""A CloudWatch alarm may only come into existence from a file — and no
longer from this repo at all.

`alpha-engine-config-I7359`, executing the `I7359` ownership ruling
(`nous-ergon-ops` owns the CloudWatch alarm apply; this repo, being PUBLIC
while the alarm definitions and applier are PRIVATE, stops creating alarms
entirely rather than shallow-cloning `nous-ergon-ops` for them —
`infrastructure-ownership-policy.md` §2's corollary names that shallow-clone
shape as the anti-pattern). Mirrors
`nous-ergon-ops/tests/test_no_imperative_alarm_authorship.py`, adapted to this
repo's stricter target: THIS list is expected to be EMPTY, not merely
shrinking, because every alarm this repo used to create is now codified in
`nous-ergon-ops/infrastructure/cloudwatch/alarms/`.

Seven `setup_*_alarms.sh` scripts and four Lambda `deploy.sh` files created
alarms imperatively before this PR. All eleven are converted: the six
alarm-only setup scripts are reduced to pointer stubs (one,
`setup_pipeline_deadman_alarms.sh`, keeps its non-alarm SNS
topic/subscription provisioning — that is not alarm authorship and is not
migrating), and the four Lambda deploy.sh files' `put-metric-alarm` calls are
removed or turned into no-op branches.

Detection is the shared ``nousergon_lib.alarm_authorship`` detector
(alpha-engine-config-I14032), which tokenises Python, shell and the
executable fields of YAML, so it finds a continued or option-prefixed
``aws cloudwatch put-metric-alarm`` and an f-string subprocess command, and
ignores comments, docstrings and here-document bodies. The file walk, the
``tests`` exclusion and GRANDFATHERED stay here.

The detector only proves the forms it can resolve statically. This repo's
commonest AWS call shape is ``run aws ...`` through a local dry-run wrapper
(``run()`` in several infrastructure scripts), which the detector does not
see through, and neither does it follow ``timeout``, ``bash -c``, a
``$AWS`` command variable or a bound ``cw.put_metric_alarm`` reference. So
the guard's previous mention check is kept as a fail-closed backstop: a line
naming the call that the detector did not report as a call is still a
finding. Replacing it outright would have let those forms through.
"""

from __future__ import annotations

import io
import re
import tokenize
from pathlib import Path

import pytest
from nousergon_lib.alarm_authorship import SourceLanguage, find_imperative_alarm_calls

ROOT = Path(__file__).resolve().parent.parent

#: Nothing in this repo may author a CloudWatch alarm imperatively any more.
#: THIS LIST MUST STAY EMPTY. A PR adding a name to it is a PR that has to
#: argue for it — same shape as nous-ergon-ops's GRANDFATHERED, but here the
#: bar is zero, not "may only shrink", because I7359 retired every known
#: imperative alarm-creation site in one PR rather than staging a shrink.
GRANDFATHERED: frozenset[str] = frozenset()

SHELL_CREATE = re.compile(r"\bput-metric-alarm\b")

#: Suffix -> the language the shared detector parses the file as.
LANGUAGE_BY_SUFFIX: dict[str, SourceLanguage] = {
    ".sh": "shell",
    ".bash": "shell",
    ".py": "python",
    ".yml": "yaml",
    ".yaml": "yaml",
}

SCANNED_SUFFIXES = set(LANGUAGE_BY_SUFFIX)

#: Same reasoning as nous-ergon-ops's guard: `tests` is skipped because this
#: file and its siblings talk ABOUT put-metric-alarm in prose/fixtures, never
#: call it, and a guard whose findings are its own sentences gets suppressed.
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "node_modules", "tests"}


def _python_hits(text: str, verb: str, method: str) -> list[tuple[int, str]]:
    """Backstop: Python lines naming the call outside docstrings and comments.

    Tokenized rather than regex'd over raw text — a regex over raw text
    reports docstrings and assertion messages as findings, which is how the
    nous-ergon-ops sibling guard's first draft reported six of its own
    sentences.
    """
    hits: list[tuple[int, str]] = []
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except (tokenize.TokenError, IndentationError):  # pragma: no cover
        return [(n, line) for n, line in enumerate(text.splitlines(), 1)
                if verb in line or method in line]
    for tok in tokens:
        if tok.type == tokenize.NAME and tok.string == method:
            hits.append((tok.start[0], tok.string))
        elif tok.type == tokenize.STRING:
            inner = tok.string.lstrip("bBrRuUfF").strip("\"'")
            if inner == verb or f"cloudwatch {verb}" in inner:
                hits.append((tok.start[0], tok.string))
    return hits


def _shell_hits(text: str) -> list[tuple[int, str]]:
    return [
        (n, line.strip())
        for n, line in enumerate(text.splitlines(), 1)
        if not line.strip().startswith("#") and SHELL_CREATE.search(line)
    ]


def _mentions(path: Path, text: str) -> list[tuple[int, str]]:
    """The fail-closed backstop: every non-prose line naming the call."""
    if path.suffix == ".py":
        return _python_hits(text, "put-metric-alarm", "put_metric_alarm")
    return _shell_hits(text)


def _hits(path: Path) -> list[tuple[int, str]]:
    """Calls the shared detector proves, then mentions it could not classify.

    A mention on a line the detector reported is the same finding and is not
    repeated; one it did not report stays a finding, so this guard reports at
    least every line its pre-I14032 detector did.
    """
    text = path.read_text(encoding="utf-8")
    calls = find_imperative_alarm_calls(text, LANGUAGE_BY_SUFFIX[path.suffix])
    hits = [(call.line, f"col {call.column}: {call.text}") for call in calls]
    reported = {call.line for call in calls}
    hits += [
        (n, f"unclassified mention (fail-closed): {line}")
        for n, line in _mentions(path, text)
        if n not in reported
    ]
    return sorted(hits)


def _scanned_files() -> list[Path]:
    out: list[Path] = []
    for path in (ROOT / "infrastructure").rglob("*"):
        if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
            continue
        relative = path.relative_to(ROOT)
        if any(part in SKIP_DIRS for part in relative.parts):
            continue
        out.append(path)
    return out


def _offenders() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for path in _scanned_files():
        rel = path.relative_to(ROOT).as_posix()
        if rel in GRANDFATHERED:
            continue
        try:
            hits = _hits(path)
        except UnicodeDecodeError:  # pragma: no cover — binary with a text suffix
            continue
        if hits:
            found[rel] = [f"{rel}:{n}: {text}" for n, text in hits]
    return found


def test_the_walk_reaches_the_repo():
    """`principles.md` §2.7 — a check emitting nothing is unobserved, not
    healthy. Asserted first because every assertion below passes vacuously on
    an empty walk."""
    scanned = _scanned_files()
    assert len(scanned) > 20, (
        f"the guard scanned only {len(scanned)} files under {ROOT / 'infrastructure'} "
        f"— an empty walk is indistinguishable from a clean repo"
    )


def test_no_imperative_alarm_authorship():
    offenders = _offenders()
    assert not offenders, (
        "these files create CloudWatch alarms imperatively:\n  "
        + "\n  ".join(line for lines in offenders.values() for line in lines)
        + "\n\nSince alpha-engine-config-I7359 this repo does not create "
          "CloudWatch alarms at all. The definition and applier live in the "
          "PRIVATE nous-ergon-ops repo: "
          "infrastructure/cloudwatch/alarms/*.json + apply.py. Add the "
          "definition there, not put-metric-alarm here."
    )


@pytest.mark.parametrize("rel", sorted(GRANDFATHERED))
def test_every_grandfathered_entry_still_exists(rel: str):  # pragma: no cover — empty set
    assert (ROOT / rel).is_file(), (
        f"{rel} is listed as grandfathered imperative alarm authorship and no "
        f"longer exists. Remove it from GRANDFATHERED."
    )


class TestTheGuardCanActuallyFail:
    """A check that always passes and a check that always fails read the same
    if nothing ever exercises the failing branch."""

    def test_a_shell_invocation_is_found_and_a_shell_comment_is_not(self, tmp_path):
        fixture = tmp_path / "offender.sh"
        fixture.write_text(
            "#!/usr/bin/env bash\n"
            "# aws cloudwatch put-metric-alarm  <- a comment, not a finding\n"
            '  aws cloudwatch put-metric-alarm --alarm-name "x" \\\n',
            encoding="utf-8",
        )
        assert [n for n, _ in _hits(fixture)] == [3]

    def test_a_boto3_call_is_found(self, tmp_path):
        fixture = tmp_path / "offender.py"
        fixture.write_text(
            'import boto3\n'
            'boto3.client("cloudwatch").put_metric_alarm(AlarmName="x")\n',
            encoding="utf-8",
        )
        assert [n for n, _ in _hits(fixture)] == [2]

    def test_python_prose_is_not_a_finding(self, tmp_path):
        fixture = tmp_path / "offender.py"
        fixture.write_text(
            '"""A docstring mentioning put-metric-alarm is not a call."""\n'
            'MSG = "put-metric-alarm rejects an over-long description"\n',
            encoding="utf-8",
        )
        assert _hits(fixture) == []

    def test_the_shared_detector_classifies_a_boto3_call(self, tmp_path):
        fixture = tmp_path / "offender.py"
        fixture.write_text(
            'import boto3\n'
            'boto3.client("cloudwatch").put_metric_alarm(AlarmName="x")\n',
            encoding="utf-8",
        )
        assert _hits(fixture) == [
            (2, 'col 1: boto3.client("cloudwatch").put_metric_alarm(AlarmName="x")')
        ]

    def test_an_fstring_subprocess_command_is_found(self, tmp_path):
        """Python 3.12 tokenises an f-string into FSTRING_* tokens, which the
        backstop's STRING check never saw; the shared detector does."""
        fixture = tmp_path / "offender.py"
        fixture.write_text(
            "import subprocess\n"
            'subprocess.run(f"aws cloudwatch put-metric-alarm --alarm-name {name}", shell=True)\n',
            encoding="utf-8",
        )
        assert [n for n, _ in _hits(fixture)] == [2]

    def test_a_call_through_this_repos_run_wrapper_is_found(self, tmp_path):
        """`run aws ...` is how this repo's scripts call AWS; the shared
        detector does not see through the wrapper, the backstop does."""
        fixture = tmp_path / "offender.sh"
        fixture.write_text(
            "#!/usr/bin/env bash\n"
            'run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }\n'
            'run aws cloudwatch put-metric-alarm --alarm-name "x"\n',
            encoding="utf-8",
        )
        assert [n for n, _ in _hits(fixture)] == [3]

    def test_a_yaml_run_step_is_found_and_its_comment_is_not(self, tmp_path):
        fixture = tmp_path / "workflow.yml"
        fixture.write_text(
            "jobs:\n"
            "  x:\n"
            "    steps:\n"
            "      - run: |\n"
            "          # aws cloudwatch put-metric-alarm  <- a comment\n"
            "          aws cloudwatch put-metric-alarm --alarm-name x\n",
            encoding="utf-8",
        )
        assert [n for n, _ in _hits(fixture)] == [6]
