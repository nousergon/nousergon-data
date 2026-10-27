"""Every REQUIRED status check here must also report on a `merge_group` event.

alpha-engine-config-I11310 / auto-merge-policy §7: no repo gets a merge queue
before each of its required contexts is produced by a workflow declaring
`merge_group: {types: [checks_requested]}`, and the merge-group
required-check guard is itself requireable. Enabling the queue first is the
sequence that deadlocked the fleet on 2026-07-24: a required context that never
posts on a queue entry leaves every entry AWAITING_CHECKS until it times out.

The live guard (`merge-group-required-check-guard.yml`, nousergon-lib's
validator) reads the required set from the repo's settings. This test pins the
same property offline, against the required set measured from
`GET /repos/nousergon/nousergon-data/branches/main` on 2026-10-27 (classic
protection: `test`, `iam-policy-change-guard`, `rehearsal-path-guard`; no
ruleset adds a required check) plus the guard's own context, which §7 says
must become required before a queue is enabled. Adding a required check in
settings without adding it here is the drift the live guard exists to catch.
"""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"

# required context -> (workflow file, job id that emits it)
REQUIRED = {
    "test": ("ci.yml", "test"),
    "iam-policy-change-guard": ("iam-policy-change-guard.yml", "iam-policy-change-guard"),
    "rehearsal-path-guard": ("rehearsal-path-guard.yml", "rehearsal-path-guard"),
    "guard / merge-group required-check guard": ("merge-group-required-check-guard.yml", "guard"),
}

# PR-scoped guards whose verdict depends on the PR's labels/body: on a queue
# entry the payload has no `pull_request`, so they must resolve the PR from
# `merge_group.head_ref` and fail closed when they cannot.
PR_SCOPED = ("iam-policy-change-guard.yml", "rehearsal-path-guard.yml")


def _load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def _triggers(doc: dict) -> dict:
    # PyYAML parses the bare key `on:` as the boolean True (YAML 1.1).
    return doc.get("on", doc.get(True)) or {}


@pytest.mark.parametrize("context", sorted(REQUIRED))
def test_required_context_workflow_triggers_on_merge_group(context):
    wf, job = REQUIRED[context]
    doc = _load(wf)
    assert job in doc["jobs"], f"{wf} no longer has job {job!r}, which emits {context!r}"
    mg = _triggers(doc).get("merge_group", "absent")
    assert isinstance(mg, dict) and "checks_requested" in (mg.get("types") or []), (
        f"{wf} produces the REQUIRED context {context!r} but does not declare "
        f"`merge_group: {{types: [checks_requested]}}` (got {mg!r}). Under a merge "
        f"queue that context never posts and every entry deadlocks (I11310)."
    )


@pytest.mark.parametrize("context", sorted(REQUIRED))
def test_required_context_pull_request_trigger_is_unfiltered(context):
    """A path-filtered producer cannot report on a PR outside its paths, so a
    required context it serves blocks that PR forever (config#5941)."""
    wf, _ = REQUIRED[context]
    pr = _triggers(_load(wf)).get("pull_request") or {}
    assert "paths" not in pr and "paths-ignore" not in pr, (
        f"{wf} serves the required context {context!r} but filters pull_request: {pr!r}"
    )


@pytest.mark.parametrize("wf", PR_SCOPED)
def test_pr_scoped_guard_resolves_the_pr_from_the_queue_ref_and_fails_closed(wf):
    body = (WORKFLOWS / wf).read_text(encoding="utf-8")
    assert "github.event.merge_group.head_ref" in body
    assert "gh-readonly-queue/" in body
    assert "Failing closed" in body, (
        f"{wf}: an unparseable merge_group.head_ref must fail the check, never pass it"
    )
