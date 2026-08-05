#!/usr/bin/env python3
"""Tests for check-manifest-drift.py (alpha-engine-config-I6461,
groom-sweep-policy.md §2.9/§4.8).

Mirrors test_scheduler_reconcile_is_ci_runnable.py's style for its sibling
check-schedule-drift.py: the manifest and the checker are loaded fresh per
test via importlib, never via a package import (these scripts are invoked
directly, not imported as a package).
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "infrastructure" / "scheduler" / "schedule-manifest.json"
CHECKER_PATH = REPO_ROOT / "infrastructure" / "scheduler" / "check-manifest-drift.py"
DEPLOY_SH = (
    REPO_ROOT / "infrastructure" / "lambdas" / "scheduled-groom-dispatcher" / "deploy.sh"
)


def _load_checker():
    spec = importlib.util.spec_from_file_location("check_manifest_drift", CHECKER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_manifest(tmp_path: Path, triggers: list[dict]) -> Path:
    p = tmp_path / "schedule-manifest.json"
    p.write_text(json.dumps({"triggers": triggers}), encoding="utf-8")
    return p


class TestManifestIsValid:
    def test_manifest_is_valid_json(self):
        json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    def test_checked_repo_manifest_has_zero_drift(self):
        """The real manifest against the real deploy.sh — the check this repo
        ships must itself be clean, or CI would be red on merge."""
        checker = _load_checker()
        findings, checked = checker.check()
        assert not findings, f"manifest drift in the real repo: {findings}"
        assert checked > 0


class TestUndeclaredAndOrphanDetection:
    def test_rule_missing_from_manifest_is_flagged(self, tmp_path):
        # Manifest with the sweep-0000 entry deliberately omitted.
        checker = _load_checker()
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        triggers = [
            t for t in manifest["triggers"]
            if t["name"] != "alpha-engine-groom-sweep-0000-daily"
        ]
        mp = _write_manifest(tmp_path, triggers)
        findings, _ = checker.check(manifest_path=mp)
        kinds = {(f["kind"], f.get("rule")) for f in findings}
        assert ("undeclared-in-manifest", "alpha-engine-groom-sweep-0000-daily") in kinds

    def test_manifest_entry_with_no_codified_rule_is_orphaned(self, tmp_path):
        checker = _load_checker()
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        triggers = list(manifest["triggers"])
        triggers.append({
            "name": "alpha-engine-scheduled-groom-does-not-exist",
            "cron": "cron(0 3 * * ? *)",
            "source_file": "infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh",
            "reason": "fabricated for the test",
            "actuation_tier": "reversible",
            "attended_window": None,
        })
        mp = _write_manifest(tmp_path, triggers)
        findings, _ = checker.check(manifest_path=mp)
        kinds = {(f["kind"], f.get("rule")) for f in findings}
        assert ("manifest-orphan", "alpha-engine-scheduled-groom-does-not-exist") in kinds

    def test_cron_mismatch_is_flagged(self, tmp_path):
        checker = _load_checker()
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        triggers = json.loads(json.dumps(manifest["triggers"]))  # deep copy
        for t in triggers:
            if t["name"] == "alpha-engine-scheduled-groom-0400-daily":
                t["cron"] = "cron(0 5 * * ? *)"
        mp = _write_manifest(tmp_path, triggers)
        findings, _ = checker.check(manifest_path=mp)
        kinds = {(f["kind"], f.get("rule")) for f in findings}
        assert ("cron-mismatch", "alpha-engine-scheduled-groom-0400-daily") in kinds


class TestIncompleteEntryDetection:
    def test_missing_reason_is_flagged(self, tmp_path):
        checker = _load_checker()
        triggers = [{
            "name": "alpha-engine-scheduled-groom-0400-daily",
            "cron": "cron(0 4 * * ? *)",
            "source_file": "infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh",
            "actuation_tier": "reversible",
            "attended_window": None,
        }]
        mp = _write_manifest(tmp_path, triggers)
        findings, _ = checker.check(manifest_path=mp)
        assert any(f["kind"] == "incomplete-manifest-entry" for f in findings)

    def test_irreversible_tier_without_attended_window_is_flagged(self, tmp_path):
        checker = _load_checker()
        triggers = [{
            "name": "alpha-engine-groom-sweep-0000-daily",
            "cron": "cron(0 0 * * ? *)",
            "source_file": "infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh",
            "reason": "test",
            "actuation_tier": "irreversible",
            "attended_window": None,
        }]
        mp = _write_manifest(tmp_path, triggers)
        findings, _ = checker.check(manifest_path=mp, source_file=None)
        offenders = [
            f for f in findings
            if f["kind"] == "incomplete-manifest-entry"
            and "attended_window" in f["detail"]
        ]
        assert offenders, findings


class TestScoping:
    def test_default_scope_excludes_other_dispatchers(self):
        """Unscoped comparison would flag every OTHER deploy.sh's rules as
        undeclared — the manifest only covers the groom dispatcher today."""
        checker = _load_checker()
        findings, _ = checker.check()  # default source_file scoping
        foreign = [
            f for f in findings
            if f["kind"] == "undeclared-in-manifest"
            and "scheduled-groom-dispatcher" not in f.get("detail", "")
        ]
        assert not foreign, (
            f"scoped check should never see other dispatchers' rules: {foreign}"
        )
