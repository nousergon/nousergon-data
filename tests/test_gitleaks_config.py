#!/usr/bin/env python3
"""The gitleaks baseline must narrow the scanner, never blind it.

alpha-engine-config-I6622. Secret scanning on this repo failed on every push to
main from 2026-08-04 to 2026-08-07 — eight consecutive runs, six merges, no
alert — on a single false positive: the `_HEX32_TOKEN` fixture in
`infrastructure/lambdas/scheduled-groom-dispatcher/test_handler.py`, which is a
run_token correlation id, not a credential. The finding is nonetheless real in
gitleaks' terms and permanent: the scan walks full history at `fetch-depth: 0`,
so the value cannot be edited out of commit 7f5b4988.

A secret-scanner allowlist is the most dangerous file in the repo, because every
way it can be wrong makes the scan look GREENER and nothing downstream reports
the damage. These tests pin the properties that keep it honest.

Two design traps were found by experiment while fixing I6622, both of which read
as obviously correct:

  * A repo-local `.gitleaks.toml` is the natural place for an allowlist, and it
    silently does nothing: `.gitignore` excludes it (it is a local symlink for
    the pre-commit hook), so it never reaches the CI checkout. Verified by
    committing one and watching it not stage.
  * Inside such a config, a `paths` allowlist excludes the whole file from
    scanning BEFORE any regex runs, and `matchCondition = "AND"` does not
    restrain it — measured against gitleaks 8.30.1, the pinned version. A path
    scope there would have blinded the scanner to every future secret in a
    3,900-line test file while appearing to forgive one line.

Both are why the accepted finding lives in `.gitleaks-baseline.json` as a single
appended record instead.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE = REPO_ROOT / ".gitleaks-baseline.json"
CALLER = REPO_ROOT / ".github" / "workflows" / "gitleaks.yml"


@pytest.fixture(scope="module")
def baseline() -> list[dict]:
    return json.loads(BASELINE.read_text(encoding="utf-8"))


def test_baseline_parses_and_is_non_empty(baseline):
    assert isinstance(baseline, list) and baseline


def test_no_baseline_entry_carries_an_unredacted_secret(baseline):
    """The baseline is committed. A real value in it is a leak by definition."""
    for entry in baseline:
        assert entry.get("Secret") == "REDACTED", (
            f"{entry.get('File')}:{entry.get('StartLine')} has Secret="
            f"{entry.get('Secret')!r}. The baseline must be generated with "
            "--redact; committing the value turns the accept-list into the leak."
        )
        assert "REDACTED" in entry.get("Match", ""), (
            f"{entry.get('File')}:{entry.get('StartLine')} has an unredacted Match"
        )


def test_every_baseline_entry_is_traceable(baseline):
    """An accepted finding nobody can trace back is an unexplained hole."""
    for entry in baseline:
        for field in ("RuleID", "File", "Commit", "Fingerprint"):
            assert entry.get(field), f"baseline entry missing {field}: {entry!r}"


def test_baseline_fingerprints_are_unique(baseline):
    fps = [e["Fingerprint"] for e in baseline]
    dupes = {f for f in fps if fps.count(f) > 1}
    assert not dupes, (
        f"duplicate fingerprints {sorted(dupes)} — a sign the baseline was "
        "regenerated and merged rather than appended to"
    )


def test_gitleaks_toml_stays_gitignored():
    """The trap: it looks like the right place for an allowlist and never ships.

    If this ever becomes tracked, the allowlist mechanism has changed and the
    guidance in the caller workflow and in this file is wrong. Fail loudly
    rather than let a config that CI cannot see look authoritative.
    """
    result = subprocess.run(
        ["git", "check-ignore", "-q", ".gitleaks.toml"],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    assert result.returncode == 0, (
        ".gitleaks.toml is no longer gitignored. Either it is now genuinely "
        "shipped to CI — in which case update the caller workflow's guidance "
        "and this test — or an allowlist has been put somewhere the CI "
        "checkout will never see it."
    )


def test_caller_workflow_does_not_claim_red_is_expected():
    """The stale gate note that made eight consecutive failures invisible."""
    lowered = CALLER.read_text(encoding="utf-8").lower()
    for phrase in ("expected and by design", "gate:dependency"):
        assert phrase not in lowered, (
            f"the caller workflow says {phrase!r}. A security gate documented "
            "as expected-to-be-red is indistinguishable from one that works; "
            "that note is why I6622 went unread for three days."
        )


def test_caller_workflow_says_where_findings_land(baseline):
    src = CALLER.read_text(encoding="utf-8")
    assert "gitleaks-sarif" in src, (
        "the caller must tell a reader where the findings detail is, or a red "
        "run has no next step"
    )
    assert "never regenerate" in src.lower(), (
        "the caller must warn against regenerating the baseline — the one "
        "action that clears a red run while hiding whatever else it accepts"
    )
