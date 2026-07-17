"""Burn-down guard — forbid NEW raw ``send_message`` imports outside the
flow-doctor sink (EPIC config#1740 Fleet Telegram consolidation, Phase T5,
config#2400).

Fleet Telegram consolidation made ``flow_doctor_telegram.notify_via_flow_doctor``
(and, downstream of it, ``FlowDoctor.notify_event`` / ``notify_success``) the
single sink for fleet Telegram alerts — forum-topic routing, cross-invocation
DynamoDB dedup, and rate limiting all live there. A producer that instead
imports ``krepis.telegram.send_message`` (or its ``nousergon_lib.telegram``
re-export) directly bypasses every one of those, which is how the fleet ended
up with unrouted/undeduped pings scattered across Lambdas pre-T5.

T0-T4 migrated every producer except one deliberate, still-live exception:
``infrastructure/lambdas/spot-orphan-reaper/index.py``. Verified 2026-07-17
(config#2400): that Lambda's ``requirements.txt`` intentionally omits the
``nousergon-lib[flow-doctor]`` extra (it pulls the full ``flow_doctor``
package — PyYAML, a DynamoDB-backed store client, forum-topic machinery — for
one alert shape), and its ``iam-policy.json`` carries no DynamoDB
permissions for the ``flow-doctor-store`` table, only the narrow
``ssm:GetParameter`` grant the raw Telegram send needs. Migrating would be an
infra change (new IAM statements + a heavier Lambda package), not a code-only
swap, for a single best-effort alert whose own docstring already treats
``send_message`` failure as non-fatal. Kept as a documented, allowlisted
exception rather than force-migrated.

This is this repo's OWN ratchet (unlike the wide-horizon-column guard in
crucible-backtester, there is no shared ``nousergon_lib`` primitive for this
pattern — telegram-sink topology is fleet-Lambda-specific, not a quant
concern) — same shape: scan production files for the forbidden import,
forbid anything outside an explicit allowlist with a one-line rationale per
entry, and an honesty sub-test proving the guard actually fires on an
injected violation.
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent

# Matches `from krepis.telegram import send_message` or
# `from nousergon_lib.telegram import send_message` (the re-export), allowing
# for `as` aliasing and either single/multi-name import lists containing
# send_message, so a sneaky `from krepis.telegram import (foo, send_message)`
# doesn't slip past a naive single-name regex.
_FORBIDDEN_IMPORT_RE = re.compile(
    r"^\s*from\s+(?:krepis|nousergon_lib)\.telegram\s+import\s+"
    r"(?:\([^)]*\bsend_message\b[^)]*\)|[^#\n]*\bsend_message\b)",
    re.MULTILINE,
)

# Directories/files that are not production import paths.
_EXCLUDE_PREFIXES = (
    "tests/",
    ".venv",
    ".git/",
    "venv/",
)

# ---------------------------------------------------------------------------
# Allowlist — every entry needs a one-line rationale. A file leaving this set
# (no longer matching) should be DELETED from here, not left stale (the
# honesty test below enforces that). A NEW match anywhere else is a
# regression back to the pre-T5 unrouted-alert bug class.
# ---------------------------------------------------------------------------
_ALLOWLIST: dict[str, str] = {
    # The flow-doctor sink itself — send_message is its own internal
    # fallback (notify_via_flow_doctor falls back to it when flow-doctor
    # init failed), not a bypass of it. Permanently exempt by definition.
    "infrastructure/lambdas/flow_doctor_telegram.py": (
        "flow-doctor sink's own internal degraded-mode fallback, not a bypass"
    ),
    # Documented, deliberate exception (config#2400) — see module docstring
    # above for the full IAM/packaging reasoning. Best-effort, non-fatal,
    # single alert shape; migrating means adding DynamoDB IAM + the full
    # flow_doctor package to a Lambda that today deliberately excludes it.
    "infrastructure/lambdas/spot-orphan-reaper/index.py": (
        "documented bypass (config#2400): no [flow-doctor] extra / no "
        "DynamoDB IAM on this Lambda by design; best-effort single alert shape"
    ),
}


def _is_excluded(rel_posix: str) -> bool:
    return any(rel_posix.startswith(p) for p in _EXCLUDE_PREFIXES)


def _matching_files(root: Path) -> set[str]:
    """Every production .py file under ``root`` whose text matches the
    forbidden import pattern, as repo-relative posix paths."""
    out: set[str] = set()
    for path in root.rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        if _is_excluded(rel):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if _FORBIDDEN_IMPORT_RE.search(text):
            out.add(rel)
    return out


def test_no_new_raw_send_message_imports_outside_allowlist():
    """The closes-when grep (EPIC config#1740): every raw
    ``send_message`` import in production code must be named in
    ``_ALLOWLIST`` with its rationale. A new, un-allowlisted match means a
    producer bypassed the flow-doctor sink — migrate it onto
    ``notify_via_flow_doctor`` / ``FlowDoctor.notify_event`` instead of
    adding it here."""
    found = _matching_files(_REPO)
    unlisted = found - set(_ALLOWLIST)
    assert not unlisted, (
        "New raw krepis/nousergon_lib telegram.send_message import(s) outside "
        f"the flow-doctor sink: {sorted(unlisted)}. Route through "
        "flow_doctor_telegram.notify_via_flow_doctor (or FlowDoctor.notify_event) "
        "instead, or — only if there's a documented, deliberate reason it can't "
        "use flow-doctor (as spot-orphan-reaper's IAM/packaging constraints "
        "are) — add it to _ALLOWLIST here with a one-line rationale."
    )


def test_allowlist_entries_are_honest():
    """Every allowlist entry must still exist and still actually match the
    forbidden pattern — a stale entry (file deleted, or migrated off
    send_message) must be removed, not left to silently mask a future
    regression at that same path."""
    problems: dict[str, str] = {}
    for rel in _ALLOWLIST:
        path = _REPO / rel
        if not path.exists():
            problems[rel] = "file no longer exists — remove from _ALLOWLIST"
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if not _FORBIDDEN_IMPORT_RE.search(text):
            problems[rel] = (
                "no longer imports send_message directly — migration complete, "
                "remove from _ALLOWLIST"
            )
    assert not problems, f"stale _ALLOWLIST entries: {problems}"


def test_guard_catches_an_injected_violation():
    """Honesty test: prove the scan itself actually fires, using a temp
    fixture file with a synthetic violation — not just that the current
    tree happens to be clean."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        victim = root / "infrastructure" / "lambdas" / "totally-new-lambda" / "index.py"
        victim.parent.mkdir(parents=True)
        victim.write_text(
            "from nousergon_lib.telegram import send_message\n\n"
            "def handler(event, context):\n"
            "    send_message('uh oh', disable_notification=False)\n",
            encoding="utf-8",
        )
        found = _matching_files(root)
        assert "infrastructure/lambdas/totally-new-lambda/index.py" in found

        # And the multi-name / aliased import forms the regex is meant to
        # also catch, each in their own fixture file:
        victim2 = root / "infrastructure" / "lambdas" / "another-lambda" / "index.py"
        victim2.parent.mkdir(parents=True)
        victim2.write_text(
            "from krepis.telegram import (some_helper, send_message)\n",
            encoding="utf-8",
        )
        found2 = _matching_files(root)
        assert "infrastructure/lambdas/another-lambda/index.py" in found2


def test_guard_ignores_test_directory_matches():
    """A test fixture that legitimately references the forbidden import
    string (e.g. this repo's own test_handler.py hermetic-import-guard
    comments/mocks) must not trip the production-only scan."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        test_file = root / "tests" / "test_something.py"
        test_file.parent.mkdir(parents=True)
        test_file.write_text(
            "# from nousergon_lib.telegram import send_message\n"
            "from nousergon_lib.telegram import send_message\n",
            encoding="utf-8",
        )
        found = _matching_files(root)
        assert found == set()


@pytest.mark.parametrize("rel_path", sorted(_ALLOWLIST))
def test_allowlist_path_is_production_not_excluded(rel_path):
    """Every allowlisted path must actually be a production path under this
    scan's own exclude rules — an allowlist entry that the scanner would
    never visit anyway is dead weight that hides nothing."""
    assert not _is_excluded(rel_path), (
        f"{rel_path} is under an excluded prefix {_EXCLUDE_PREFIXES} — "
        "remove it from _ALLOWLIST, the scanner never checks it"
    )
