#!/usr/bin/env python3
"""The schedule-reconciliation path must stay runnable by CI, forever.

alpha-engine-config-I5805. ``nousergon-data#1179`` declared three independent
sweep schedules on 2026-07-30. They merged, the deploy workflow ran green, and
none of the three was ever created — the block that creates them sat inside
``--bootstrap``, a flag the workflow does not pass and cannot pass, because
``--bootstrap`` also writes IAM and the OIDC role deliberately has no
``iam:PutRolePolicy``.

The fix splits the flag on the IAM boundary. These tests assert the property
that split bought, so it cannot be quietly given back:

  1. The reconcile block writes NO IAM. One ``aws iam`` call inside it and the
     whole block becomes operator-only again — silently, since the failure is an
     AccessDenied on a merge nobody is watching.
  2. The deploy workflow actually invokes it.
  3. The workflow asserts the OUTCOME afterwards, rather than trusting its own
     exit status. A deploy step that succeeds while a schedule is missing is
     exactly what 2026-07-30 looked like.
  4. Every schedule array a deploy.sh declares is reachable by the drift
     checker's discovery — a rule the checker cannot see is a rule nobody
     checks, and reads as "no drift".
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SH = (
    REPO_ROOT
    / "infrastructure"
    / "lambdas"
    / "scheduled-groom-dispatcher"
    / "deploy.sh"
)
WORKFLOW = (
    REPO_ROOT / ".github" / "workflows" / "deploy-scheduled-groom-dispatcher.yml"
)
DRIFT_CHECKER = REPO_ROOT / "infrastructure" / "scheduler" / "check-schedule-drift.py"


def _load_checker():
    spec = importlib.util.spec_from_file_location("check_schedule_drift", DRIFT_CHECKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _reconcile_block() -> str:
    """The body of `if $RECONCILE_SCHEDULES; then ... fi`.

    Located by brace-free line scanning rather than a regex over the whole file,
    because the block contains its own nested if/fi pairs.
    """
    lines = DEPLOY_SH.read_text(encoding="utf-8").splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.strip() == "if $RECONCILE_SCHEDULES; then":
            start = i
            break
    assert start is not None, (
        "deploy.sh has no `if $RECONCILE_SCHEDULES; then` block — the "
        "IAM-free schedule path has been removed or renamed"
    )

    depth = 0
    for i in range(start, len(lines)):
        stripped = lines[i].strip()
        if stripped.startswith("if ") or stripped == "if":
            depth += 1
        elif stripped == "fi" or stripped.startswith("fi "):
            depth -= 1
            if depth == 0:
                return "\n".join(lines[start : i + 1])
    pytest.fail("unterminated `if $RECONCILE_SCHEDULES` block in deploy.sh")


class TestReconcileBlockIsIamFree:
    def test_block_exists(self):
        assert "$RECONCILE_SCHEDULES" in DEPLOY_SH.read_text(encoding="utf-8")

    def test_no_iam_writes_inside_the_reconcile_block(self):
        """The whole point of the split. One `aws iam` call undoes it."""
        block = _reconcile_block()
        offenders = [
            line.strip()
            for line in block.splitlines()
            if re.search(r"\baws iam\b", line) and not line.strip().startswith("#")
        ]
        assert not offenders, (
            "The reconcile block must not write IAM — github-actions-lambda-deploy "
            "has no iam:PutRolePolicy, so an IAM call here silently makes the "
            "whole block operator-only again (alpha-engine-config-I5805).\n"
            "Offending lines:\n  " + "\n  ".join(offenders)
        )

    def test_reconcile_block_still_creates_the_sweep_rules(self):
        """The three rules I5805 exists because of."""
        block = _reconcile_block()
        for name in (
            "alpha-engine-groom-sweep-0000-daily",
            "alpha-engine-groom-sweep-0800-daily",
            "alpha-engine-groom-sweep-1600-daily",
        ):
            assert name in block, f"{name} is not created in the reconcile block"

    def test_bootstrap_still_implies_both_halves(self):
        """`--bootstrap` predates the split and must keep its old meaning."""
        text = DEPLOY_SH.read_text(encoding="utf-8")
        m = re.search(r"--bootstrap\)\s*(.+?);;", text)
        assert m, "no `--bootstrap)` case arm found"
        arm = m.group(1)
        assert "BOOTSTRAP_IAM=true" in arm and "RECONCILE_SCHEDULES=true" in arm, (
            f"--bootstrap must still set both halves; got: {arm!r}"
        )


class TestDeployWorkflowRunsIt:
    def test_workflow_invokes_reconcile_schedules(self):
        wf = WORKFLOW.read_text(encoding="utf-8")
        assert "--reconcile-schedules" in wf, (
            "deploy-scheduled-groom-dispatcher.yml must run "
            "`deploy.sh --reconcile-schedules`, or a merged cadence change is "
            "inert again (alpha-engine-config-I5805)"
        )

    def test_workflow_asserts_the_outcome_after_deploying(self):
        """Not the step's exit status — the live state it was supposed to produce."""
        wf = WORKFLOW.read_text(encoding="utf-8")
        assert "check-schedule-drift.py" in wf, (
            "the deploy workflow must assert every declared schedule exists live "
            "after reconciling; a green deploy with a missing schedule is the "
            "2026-07-30 failure mode"
        )

    def test_workflow_watches_the_sf_definition_path(self):
        wf = WORKFLOW.read_text(encoding="utf-8")
        assert "infrastructure/step_function_groom.json" in wf


class TestDriftCheckerSeesEveryDeclaredRule:
    def test_discovery_finds_the_sweep_rules(self):
        """Indented arrays included — they live inside an `if` block.

        The first draft of the checker anchored its array regex at column 0 and
        found 11 of 14 rules, missing exactly the sweep ones. Zero findings on an
        invisible rule reads as "no drift", which is the same class of bug the
        checker is for.
        """
        checker = _load_checker()
        rules, errors, _ = checker.discover_codified_rules()
        assert not errors, f"source errors in deploy scripts: {errors}"
        names = {r["name"] for r in rules}
        for expected in (
            "alpha-engine-groom-sweep-0000-daily",
            "alpha-engine-groom-sweep-0800-daily",
            "alpha-engine-groom-sweep-1600-daily",
            "alpha-engine-scheduled-groom-0400-daily",
            "alpha-engine-groom-lane-reconciler-5min",
        ):
            assert expected in names, (
                f"{expected} is declared in a deploy.sh but the drift checker "
                f"does not discover it. Found: {sorted(names)}"
            )

    def test_discovery_covers_more_than_the_groom_dispatcher(self):
        """The defect class is fleet-wide; the checker must be too.

        Fixing this on the groom dispatcher alone would be the 'fixed on the
        daily path, not the critical one' pattern — every deploy.sh declaring
        SCHED_NAMES has the same exposure.
        """
        checker = _load_checker()
        rules, _, _ = checker.discover_codified_rules()
        sources = {r["source_file"] for r in rules}
        assert len(sources) >= 3, (
            f"drift checker only discovered rules in {sources} — it should cover "
            "every infrastructure/lambdas/*/deploy.sh that declares SCHED_NAMES"
        )

    def test_every_discovered_rule_has_a_nonempty_expression(self):
        checker = _load_checker()
        rules, _, _ = checker.discover_codified_rules()
        assert rules, "discovered no scheduler rules at all"
        for rule in rules:
            assert rule["expression"].startswith(("cron(", "rate(", "at(")), (
                f"{rule['name']} has expression {rule['expression']!r}, which is "
                "not a valid EventBridge Scheduler expression"
            )

    def test_unbalanced_arrays_are_reported_as_source_errors(self, tmp_path):
        """A name with no cron must fail loudly, not silently drop the rule."""
        checker = _load_checker()
        fake = tmp_path / "infrastructure" / "lambdas" / "broken"
        fake.mkdir(parents=True)
        (fake / "deploy.sh").write_text(
            'SCHED_NAMES=(\n  "a"\n  "b"\n)\nSCHED_CRONS=(\n  "cron(0 1 * * ? *)"\n)\n',
            encoding="utf-8",
        )
        checker.LAMBDAS_DIR = tmp_path / "infrastructure" / "lambdas"
        checker.REPO_ROOT = tmp_path
        _, errors, _ = checker.discover_codified_rules()
        assert errors and errors[0]["kind"] == "source-error", (
            f"unbalanced arrays should produce a source-error, got {errors}"
        )
