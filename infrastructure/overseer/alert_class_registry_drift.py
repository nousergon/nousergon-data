#!/usr/bin/env python3
"""
Cross-repo drift guard: assert every ``krepis.alerts.publish(source=...)`` /
``publish_ops_alert`` source literal across the fleet repos has a matching row
in ``nousergon-data/infrastructure/overseer/playbooks.yaml``'s ``alert_classes:``
section (alpha-engine-config-I3211 final leg, alpha-engine-config#3305).

A NEW emitter cannot merge without declaring its alert class — this chokepoint
enforces that by scanning all Python files in the checked-out fleet repos for
source literals and cross-referencing against the registry's source patterns.

Two emission shapes are covered (alpha-engine-config-I6753 added the second):
  1. In-process Python calls: ``alerts.publish(source="...")`` and the
     ``publish_ops_alert`` / ``publish_observe_alert`` / ``publish_ops_digest``
     wrappers (``*.py`` only).
  2. CLI-shaped invocations: ``python -m krepis.alerts publish ... --source X``
     built in subprocess arg-lists, f-strings, or shell scripts (``*.py`` and
     ``*.sh``). A CLI invocation with NO ``--source`` fails the check outright
     (reported as ``<missing --source>``) — it is unattributable, so no
     registry row can ever cover it.

Pattern matching:
  - Direct literal equality: source=="foo" matches registry row where source=="foo"
  - Wildcard patterns: registry rows ending with ``:*`` (e.g. ``groom:*``,
    ``cloudwatch-alarm:*``) match any source with the matching prefix. Rows
    ending with ``/*`` (e.g. ``alert-transport-liveness/*``) are prefix claims
    too, and keep their slash, so ``foo/*`` never claims ``foobar``. Both
    shapes are a subset of what the runtime resolver accepts
    (``krepis.alert_tiers._match`` treats any trailing ``*`` as a prefix).
  - ``raw-sns:*``, ``raw-telegram:*``, ``flow-doctor:*``, ``email:*`` rows match
    any source with that prefix (drain-blind non-bus sources).

Exits 0 on success; exits 1 with a list of uncovered source literals.

**Where this lives, and why it moved** (``alpha-engine-config-I7896``). It was
``alpha-engine-config/scripts/check_alert_class_registry_drift.py``: a scanner
in a PRIVATE repo, reading a registry in THIS one, grading emitters in eight
PUBLIC ones, running only on ``alpha-engine-config``'s push-to-``main``. That
placement is what made the failure land late and in the wrong repo — twice on
2026-08-20 (``alpha-engine-config-I7876``, ``-I7896``), each time reddening
``alpha-engine-config``'s ``main``, which silently ejects every entry from that
repo's merge queue while each queued PR keeps reporting a healthy state.

It now lives beside the registry it grades, so ``alert_class_pr_guard.py`` can
run the SAME scanner from an emitter repo's own ``pull_request`` CI over a
public checkout of this repo — no private checkout, no new credential.
``alpha-engine-config``'s fleet-wide sweep invokes this file directly and stays
exactly as it was in every other respect: the backstop, not the first detector.

Usage:
  python infrastructure/overseer/alert_class_registry_drift.py \\
    --playbooks infrastructure/overseer/playbooks.yaml \\
    --repos ../crucible-research crucible-executor crucible-predictor ...
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

# EXCLUDED_DIR_NAMES is defined HERE rather than imported. This scanner moved
# out of `alpha-engine-config/scripts/` (a PRIVATE repo) into this one — the
# repo that owns `playbooks.yaml` — under `alpha-engine-config-I7896`, so that
# the eight PUBLIC emitter repos can run it at their OWN merge time without
# minting a private-repo checkout credential in public CI. `scan_exclusions.py`
# stayed behind with the three other alpha-engine-config scanners that import
# it; `test_alert_class_registry_drift.py::test_excluded_dir_names_matches_the
# _shared_list_when_it_is_reachable` pins the two lists together whenever both
# checkouts are present, so the fork cannot drift silently.
EXCLUDED_DIR_NAMES: frozenset[str] = frozenset({
    ".worktrees",   # a git worktree is a SECOND checkout — its files are copies
    ".git",
    ".venv",
    "venv",
    "env",
    ".tox",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "build",
    "dist",
})

# Function/method name patterns that constitute an alert publish call.
# Covers: alerts.publish, krepis.alerts.publish, publish_ops_alert,
# publish_observe_alert, publish_ops_digest, etc.
_PUBLISH_CALL_PATTERN = re.compile(
    r"""
    (?:
        alerts\.publish\s*\(
        | publish_ops_alert\s*\(
        | publish_observe_alert\s*\(
        | publish_ops_digest\s*\(
    )
    """,
    re.VERBOSE,
)

# Extracts source="..." or source='...' from the call window.
_SOURCE_ARG_PATTERN = re.compile(r"""source\s*=\s*["']([^"']+)["']""")

# CLI-shaped emission: a subprocess/shell invocation of the krepis.alerts CLI
# (`python -m krepis.alerts publish ...`), embedded in a .py f-string/arg-list
# or a .sh script. These never pass through the in-process call shapes above,
# which is how scan-unlisted-state reached production with no registry row
# (alpha-engine-config-I6753). Only the `-m` module-invocation shape is
# matched — krepis ships no console script, so it is the only CLI entry, and
# the looser `krepis.alerts publish` adjacency also matches PROSE (a
# box_health.sh comment naming the channel false-positived as a sourceless
# invocation on the first live run of this check).
_CLI_CALL_PATTERN = re.compile(r"-m['\",\s]+krepis\.alerts")

# Window scanned forward from each CLI match for its --source argument. Long
# enough to span multi-line f-string fragments and backslash-continued shell
# args (box_health.sh's invocation spans ~6 lines); short enough not to
# swallow a neighbouring, unrelated invocation's --source.
_CLI_WINDOW_CHARS = 600

# A literal --source token: `--source scan-unlisted-state` or the same
# quoted/comma-separated inside a Python arg list (`"--source", "router-canary"`).
# The class includes `/` and `:` — registry sources are frequently path-shaped
# (`alpha-engine-research/infrastructure/deploy.sh`, `...py::func`), and a
# class without them truncates at the first slash, mis-reporting a covered
# emitter as an uncovered prefix.
_CLI_SOURCE_LITERAL = re.compile(r"--source[\s'\",]+([A-Za-z0-9][\w.\-/:]*)")

# A dynamic --source (`--source "$SRC"` / `--source {var}`): statically
# unverifiable, skipped — mirroring the in-process scanner, which only
# matches string literals and ignores source=<variable>.
_CLI_SOURCE_DYNAMIC = re.compile(r"""--source[\s'\",]+[{$]""")

# Sentinel reported when a CLI invocation carries no --source at all. Such an
# emitter is unattributable: no registry row can ever match it, and the drain
# ingests its events as unknown-source findings. Live instance: four
# claude-code-config call sites also missing the `publish` subcommand, so
# every invocation died on an argparse error the callers swallowed.
MISSING_SOURCE_SENTINEL = "<missing --source>"

# THE SCANNER MUST NOT SCAN ITSELF (alpha-engine-config-I7896, caught by this
# guard on its OWN first CI run — nousergon-data-PR1479, run 32436244797).
#
# In alpha-engine-config this file was never in the scanned set, so it never
# came up. Here it is: nousergon-data is one of the nine repos the fleet sweep
# walks. And these two modules are the one place in the fleet whose JOB is to
# spell out every publish shape — the docstrings carry
# `alerts.publish(source="...")`, `--source X`, and a bare `python -m
# krepis.alerts publish` with no --source as illustrations. The scanner read
# its own examples as live emitters and reported three: `...`, `X`, and
# `<missing --source>`.
#
# The exclusion is BY EXACT RELATIVE PATH, not by filename and not by "any
# file that talks about krepis.alerts". A rule of the second kind would blind
# the scanner to a real emitter that happens to mention the CLI in a comment
# — the same over-broad instinct that made a box_health.sh PROSE comment
# false-positive as a sourceless invocation on this check's first live run.
SELF_EXCLUDED_RELPATHS: frozenset[str] = frozenset({
    "infrastructure/overseer/alert_class_registry_drift.py",
    "infrastructure/overseer/alert_class_pr_guard.py",
})


def _find_cli_source_literals(file_text: str) -> tuple[set[str], int]:
    """CLI-shaped invocations: (literal sources found, count with no --source).

    Dynamic sources (env vars, format interpolation) are skipped, consistent
    with `_find_publish_source_literals` ignoring non-literal `source=`.
    """
    sources: set[str] = set()
    missing = 0
    for match in _CLI_CALL_PATTERN.finditer(file_text):
        window = file_text[match.start(): match.start() + _CLI_WINDOW_CHARS]
        literal = _CLI_SOURCE_LITERAL.search(window)
        if literal:
            sources.add(literal.group(1))
        elif not _CLI_SOURCE_DYNAMIC.search(window):
            missing += 1
    return sources, missing


def _load_alert_classes(playbooks_path: Path) -> list[dict]:
    """Load alert_classes from playbooks.yaml."""
    data = yaml.safe_load(playbooks_path.read_text())
    return data.get("alert_classes", [])


def is_wildcard_source(source: str) -> bool:
    """True for a registry ``source`` that is a prefix claim (``:*`` or ``/*``)."""
    return source.endswith((":*", "/*"))


def _wildcard_prefix(pattern: str) -> str:
    """The prefix a wildcard row claims.

    ``groom:*`` claims ``groom`` (the historical behaviour, kept unchanged);
    ``alert-transport-liveness/*`` claims ``alert-transport-liveness/``.
    """
    return pattern[:-2] if pattern.endswith(":*") else pattern[:-1]


def _build_registry_patterns(alert_classes: list[dict]) -> list[tuple[str, str, bool]]:
    """Build a list of (class_name, source_pattern, is_wildcard) tuples.

    Wildcard patterns (ending with ``:*`` or ``/*``) are matched by prefix.
    """
    patterns = []
    for cls in alert_classes:
        source = cls["source"]
        patterns.append((cls["class"], source, is_wildcard_source(source)))
    return patterns


def _find_publish_source_literals(file_text: str, file_path: Path) -> set[str]:
    """Find all source= string literals within publish call contexts.

    Uses a multi-line-aware approach:
    1. Find the start of each publish call
    2. Find the matching closing paren (handling nested parens)
    3. Within that call, extract source= literal values

    Returns a set of literal source strings found.
    """
    sources: set[str] = set()
    text = file_text

    for match in _PUBLISH_CALL_PATTERN.finditer(text):
        # Find the matching closing paren for this call.
        paren_start = text.index("(", match.end() - 1)
        depth = 0
        call_end = -1
        for i in range(paren_start, len(text)):
            ch = text[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    call_end = i
                    break

        if call_end == -1:
            continue  # malformed, skip

        call_body = text[paren_start:call_end]
        for src_match in _SOURCE_ARG_PATTERN.finditer(call_body):
            sources.add(src_match.group(1))

    return sources


def _source_matches_registry(source: str, patterns: list[tuple[str, str, bool]]) -> bool:
    """Check if a source value matches any registered alert_class pattern."""
    for _class_name, pattern, is_wildcard in patterns:
        if is_wildcard:
            if source.startswith(_wildcard_prefix(pattern)):
                return True
        else:
            if source == pattern:
                return True
    return False


def _is_test_file(file_path: Path, repo_root: Path) -> bool:
    """Check if a file is a test file (test_ prefix or tests/ directory)."""
    rel = file_path.relative_to(repo_root)
    parts = rel.parts
    return any(p == "tests" or p.startswith("test_") for p in parts) or rel.name.startswith(
        "test_"
    )


def _scan_repo(repo_root: Path, patterns: list[tuple[str, str, bool]]) -> dict[str, set[str]]:
    """Scan a single repository for uncovered publish source literals.

    Covers in-process Python call shapes (`*.py`) and CLI-shaped invocations
    of the krepis.alerts CLI (`*.py` and `*.sh`). A CLI invocation with no
    literal or dynamic ``--source`` at all is reported under
    ``MISSING_SOURCE_SENTINEL`` — it can never match a registry row.

    Returns a dict mapping source literal to set of file paths where it was found.
    """
    uncovered: dict[str, set[str]] = {}
    scan_files = sorted(repo_root.rglob("*.py")) + sorted(repo_root.rglob("*.sh"))

    for scan_file in scan_files:
        # Skip the .git directory, __pycache__, and virtual environments.
        parts = scan_file.relative_to(repo_root).parts
        # alpha-engine-config-I7867: EXCLUDED_DIR_NAMES is now the one shared
        # list (this scanner's private copy was the only one of four that
        # remembered `.worktrees`). `.claude` stays local — agent config is
        # not a second checkout, and no other scanner wants it skipped.
        if any(p in EXCLUDED_DIR_NAMES or p == ".claude" for p in parts):
            continue
        # Skip vendored/lib stubs.
        if scan_file.name.endswith("_pb2.py"):
            continue

        # Skip test files — test source literals are fake/throwaway values.
        if _is_test_file(scan_file, repo_root):
            continue

        # Skip this scanner and its PR-time guard: their docstrings spell out
        # every publish shape by design, and the scanner read those examples
        # as live emitters. See SELF_EXCLUDED_RELPATHS above.
        if scan_file.relative_to(repo_root).as_posix() in SELF_EXCLUDED_RELPATHS:
            continue

        try:
            text = scan_file.read_text(encoding="utf-8", errors="replace")
        except Exception:  # noqa: S112
            continue

        sources = set()
        if scan_file.suffix == ".py":
            sources |= _find_publish_source_literals(text, scan_file)
        cli_sources, cli_missing = _find_cli_source_literals(text)
        sources |= cli_sources

        rel_path = str(scan_file.relative_to(repo_root))
        if cli_missing:
            uncovered.setdefault(MISSING_SOURCE_SENTINEL, set()).add(rel_path)
        for source in sources:
            if not _source_matches_registry(source, patterns):
                uncovered.setdefault(source, set()).add(rel_path)

    return uncovered


def _scan_repos(
    repo_roots: list[Path],
    patterns: list[tuple[str, str, bool]],
) -> dict[str, dict[str, set[str]]]:
    """Scan multiple repos and return uncovered sources grouped by source literal."""
    all_uncovered: dict[str, dict[str, set[str]]] = {}
    for root in repo_roots:
        repo_name = root.name
        uncovered = _scan_repo(root, patterns)
        if uncovered:
            all_uncovered[repo_name] = uncovered
    return all_uncovered


def check(playbooks_path: Path, repo_roots: list[Path]) -> int:
    """Main entry point. Returns 0 on success, 1 on drift found."""
    if not playbooks_path.exists():
        print(f"❌ playbooks.yaml not found at {playbooks_path}", file=sys.stderr)
        return 1

    alert_classes = _load_alert_classes(playbooks_path)
    patterns = _build_registry_patterns(alert_classes)

    print(f"📋 Loaded {len(patterns)} alert class patterns from {playbooks_path}")

    valid_roots = [r for r in repo_roots if r.is_dir()]
    skipped = [str(r) for r in repo_roots if not r.is_dir()]
    if skipped:
        print(f"⚠️  Skipping {len(skipped)} non-existent repo dir(s): {', '.join(skipped)}", file=sys.stderr)

    if not valid_roots:
        print("❌ No valid repo directories to scan", file=sys.stderr)
        return 1

    print(f"🔍 Scanning {len(valid_roots)} repo(s): {', '.join(r.name for r in valid_roots)}")
    all_uncovered = _scan_repos(valid_roots, patterns)

    if all_uncovered:
        total_sources = sum(len(sources) for repo_d in all_uncovered.values() for sources in repo_d.values())
        total_files = sum(len(files) for repo_d in all_uncovered.values() for sources in repo_d.values() for files in [sources])
        print(f"\n❌ DRIFT FOUND: {total_sources} uncovered source literal(s) in {total_files} file(s):\n")
        for repo_name, repo_sources in sorted(all_uncovered.items()):
            print(f"  📁 {repo_name}/")
            for source, files in sorted(repo_sources.items()):
                print(f"    🔴 source={source!r}")
                for f in sorted(files):
                    print(f"       - {repo_name}/{f}")
            print()
        return 1
    else:
        print("\n✅ All publish source literals are registered in alert_classes — no drift.")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cross-repo alert class registry drift check"
    )
    parser.add_argument(
        "--playbooks",
        type=Path,
        required=True,
        help="Path to nousergon-data/infrastructure/overseer/playbooks.yaml",
    )
    parser.add_argument(
        "--repos",
        type=Path,
        nargs="+",
        required=True,
        help="One or more fleet repo root directories to scan",
    )
    args = parser.parse_args()

    return check(args.playbooks, args.repos)


if __name__ == "__main__":
    sys.exit(main())


# ─────────────────────────────────────────────────────────────────────────────
# Severity inference — used by ``alert_class_pr_guard.py`` to emit a row that
# is PASTE-READY rather than merely indicative.
#
# THE GOTCHA THIS EXISTS FOR (alpha-engine-config-I7896). The severity
# vocabulary in ``playbooks.yaml`` is ``warning``; ``publish_observe_alert``'s
# default parameter value is the literal string ``"WARN"``. Measured
# 2026-08-20: no row in the file uses ``warn``, 35 use ``warning``. A guard
# that copied the call-site literal into its suggested row would suggest a
# severity that matches nothing and validates against nothing — the schema's
# ``severities`` enum is closed. So the literal is NORMALIZED, and a call site
# whose severity is not a literal at all resolves to ``dynamic``, which is a
# real enum member (9 rows use it) meaning exactly that.
# ─────────────────────────────────────────────────────────────────────────────

#: Severity enum accepted by ``playbooks.schema.json``'s ``severities`` items.
SEVERITY_VOCAB: frozenset[str] = frozenset(
    {"info", "warning", "error", "critical", "alarm", "dynamic"}
)

#: Call-site spelling -> registry vocabulary. Keys are lowercased.
_SEVERITY_ALIASES: dict[str, str] = {
    "warn": "warning",
    "warning": "warning",
    "err": "error",
    "error": "error",
    "crit": "critical",
    "critical": "critical",
    "fatal": "critical",
    "info": "info",
    "notice": "info",
    "debug": "info",
    "alarm": "alarm",
}

#: Default severity each publish wrapper applies when the CALL SITE passes
#: none. ``None`` means the wrapper has no default (severity is a required
#: keyword-only argument), so a call site omitting it would be a TypeError —
#: nothing to infer, and ``dynamic`` is the honest answer.
_WRAPPER_DEFAULT_SEVERITY: dict[str, str | None] = {
    "publish_observe_alert": "warning",  # observe_alerts.py: severity: str = "WARN"
    "publish_ops_alert": None,           # ops_alerts.py: keyword-only, no default
    "publish_ops_digest": None,
    "alerts.publish": "error",           # krepis.alerts.publish: severity="error"
}

# Same shapes as _PUBLISH_CALL_PATTERN, but capturing WHICH wrapper — the
# wrapper decides the default severity when the call site passes none.
_PUBLISH_CALL_NAMED = re.compile(
    r"(alerts\.publish|publish_ops_alert|publish_observe_alert|publish_ops_digest)\s*\("
)

_SEVERITY_ARG_LITERAL = re.compile(r"""severity\s*=\s*["']([^"']+)["']""")
_SEVERITY_ARG_ANY = re.compile(r"severity\s*=")


def normalize_severity(raw: str) -> str:
    """Call-site severity literal -> a value the registry schema accepts.

    An unrecognised literal becomes ``dynamic`` rather than being passed
    through: an invented enum member would make the emitted row fail schema
    validation, which is a worse failure than an honest "varies".
    """
    return _SEVERITY_ALIASES.get(raw.strip().lower(), "dynamic")


def find_publish_sites(file_text: str) -> dict[str, str]:
    """``{source literal: registry severity}`` for every publish call in a file.

    Severity resolution order, per call: an explicit ``severity="..."`` literal
    (normalized) -> the wrapper's own default -> ``dynamic``. When one source
    literal is published at two different severities in the same file, the
    result is ``dynamic`` — the registry row would need both, and ``dynamic``
    is the value the file already uses for that case.
    """
    found: dict[str, set[str]] = {}
    for match in _PUBLISH_CALL_NAMED.finditer(file_text):
        wrapper = match.group(1)
        paren_start = file_text.index("(", match.end() - 1)
        depth = 0
        call_end = -1
        for i in range(paren_start, len(file_text)):
            if file_text[i] == "(":
                depth += 1
            elif file_text[i] == ")":
                depth -= 1
                if depth == 0:
                    call_end = i
                    break
        if call_end == -1:
            continue  # malformed, skip — same posture as the scanner above
        body = file_text[paren_start:call_end]

        literal = _SEVERITY_ARG_LITERAL.search(body)
        if literal:
            severity = normalize_severity(literal.group(1))
        elif _SEVERITY_ARG_ANY.search(body):
            severity = "dynamic"  # passed, but not statically knowable
        else:
            severity = _WRAPPER_DEFAULT_SEVERITY.get(wrapper) or "dynamic"

        for src in _SOURCE_ARG_PATTERN.finditer(body):
            found.setdefault(src.group(1), set()).add(severity)

    return {
        src: (sevs.pop() if len(sevs) == 1 else "dynamic")
        for src, sevs in ((s, set(v)) for s, v in found.items())
    }
