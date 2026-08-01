"""The IAM guard must be able to report on EVERY PR, including an empty one.

nousergon-data-I1078 made this guard's context requireable by pairing a
`paths:`-filtered guard with a companion `iam-policy-change-guard-noop.yml`
carrying the complementary `paths-ignore:`, on the reasoning that exactly one
of the pair always runs so the shared context always reports.

That pair is NOT an exhaustive complement (alpha-engine-config#5941). GitHub
evaluates `paths-ignore` as "at least one changed file does not match", which
is FALSE when a PR changes ZERO files. Neither half fires, the required
context never posts, and the PR is BLOCKED permanently — with every other
check green and this context absent from the rollup entirely, which reads as
nothing at all on the PR page rather than as a red check.

Hit live on nousergon-data #784, #1118, #1134 and #1158; #784 sat that way for
18 days.

The fix removes the filter rather than widening it: the guard runs on every
PR and early-exits internally when no `infrastructure/lambdas/*/iam-policy.json`
changed. The noop companion is deleted. These tests pin that shape, because
re-adding a `paths:` filter here silently re-opens the hole.
"""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"
GUARD = WORKFLOWS / "iam-policy-change-guard.yml"
NOOP = WORKFLOWS / "iam-policy-change-guard-noop.yml"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(doc: dict) -> dict:
    # PyYAML parses the bare key `on:` as the boolean True (YAML 1.1).
    return doc.get("on", doc.get(True)) or {}


def test_guard_exists():
    assert GUARD.is_file(), "the IAM policy-change guard is missing"


def test_noop_companion_is_gone():
    """The companion only existed to cover the guard's path filter. With the
    filter removed it would double-report the same context."""
    assert not NOOP.exists(), (
        "iam-policy-change-guard-noop.yml is back — with an unfiltered guard the "
        "pair reports the same context twice, and the pair never covered the "
        "zero-changed-file case anyway (config#5941)"
    )


def test_guard_trigger_is_unfiltered():
    """The whole defect. A required context served by a path-filtered workflow
    cannot report on a PR that changes no files."""
    pr = _triggers(_load(GUARD))["pull_request"]
    assert "paths" not in pr and "paths-ignore" not in pr, (
        f"iam-policy-change-guard is a REQUIRED status check and must run on every "
        f"PR, but its trigger is filtered: {pr!r}. A zero-changed-file PR matches "
        f"neither `paths` nor `paths-ignore`, so the context never posts and the "
        f"PR is blocked forever (config#5941)."
    )


def test_job_id_is_the_required_context_and_stays_specific():
    jobs = list(_load(GUARD)["jobs"])
    assert jobs == ["iam-policy-change-guard"], (
        "the job id IS the required branch-protection context; keep it specific "
        "so a future workflow's generic job name cannot collide with a required "
        "status (nousergon-data-I1078)"
    )


def test_guard_exits_cleanly_when_no_policy_changed():
    """Unfiltered means the guard now runs on PRs touching no IAM at all — it
    must pass them, not fail them."""
    body = GUARD.read_text()
    assert "No iam-policy.json changes detected" in body, (
        "the guard runs on every PR now; it needs its no-policy-changed early "
        "exit or every non-IAM PR goes red"
    )


def test_guard_reacts_to_label_events():
    """The guard's escape hatch is a `gate:operator` label, so it must re-run on
    label changes — otherwise adding the label after the initial run leaves the
    failing status in place with no way to clear it."""
    types = _triggers(_load(GUARD))["pull_request"].get("types") or []
    assert "labeled" in types and "unlabeled" in types, (
        "the guard must trigger on labeled/unlabeled so the gate:operator escape "
        "hatch can actually clear the check"
    )


# ── Branch 3: ALREADY LIVE (alpha-engine-config-I5309) ──────────────────────
#
# The two original branches deadlocked against `gate-label-guard`, which fails
# any PR carrying `gate:*` and was also a required check here: satisfying this
# guard via `gate:operator` guaranteed the other went red, so an IAM-only PR
# could not merge by any route (hit live on nousergon-data-PR1132).
#
# Both original branches also accept an INTENTION ("an operator will apply it
# later") rather than a fact, and the standing rule's PREFERRED path — apply
# in-session before opening the PR — could not satisfy the guard at all. The
# third branch verifies the outcome directly against the account.

_GUARD = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "iam-policy-change-guard.yml"


def test_guard_accepts_an_already_live_policy():
    """Without this branch, the strictly safer workflow (apply first, then open
    the PR) is the one the guard rejects."""
    body = _GUARD.read_text()
    assert "get-role-policy" in body, (
        "the guard must be able to read the live policy to verify it is applied")
    assert "ALREADY LIVE" in body


def test_already_live_comparison_is_normalised_not_a_raw_string_compare():
    """IAM returns its own key order and URL-decodes the document, so a raw
    compare reports drift on every PR — the branch would be dead code that
    always falls through, which looks identical to working."""
    body = _GUARD.read_text()
    assert "jq -S" in body, "both documents must be key-sorted before diffing"


def test_guard_uses_a_read_only_iam_identity():
    """Verification must stay strictly weaker than mutation. The drift-check
    role holds iam:GetRolePolicy and NOT iam:PutRolePolicy, so this guard
    cannot become the fifth IAM-clobber incident."""
    body = _GUARD.read_text()
    assert "github-actions-iam-drift-check" in body
    # Assert on INVOCATIONS, not on the presence of a word anywhere in the file.
    # Three naive versions of this assertion failed while writing it, all the
    # same way: the header comment mentions `PutRolePolicy` while explaining the
    # role LACKS it, references `_shared/apply_iam_policy.sh` as documentation,
    # and the failure message echoes `--apply-iam` as guidance for the operator.
    # A substring test flags all three — text that says the opposite of the
    # thing being guarded against.
    #
    # So: a mutating token is allowed on a comment or an echo line, and nowhere
    # else. That is the real invariant — the guard may *talk about* applying IAM,
    # it may not *do* it.
    mutating = ("aws iam put-role-policy", "aws iam attach-role-policy",
                "aws iam delete-role-policy", "apply_iam_policy.sh", "--apply-iam")
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("echo "):
            continue
        for token in mutating:
            assert token not in stripped, (
                f"guard must not mutate IAM — {token!r} appears on an executable "
                f"line: {stripped!r}")


def test_aws_credential_step_is_non_fatal():
    """An OIDC/STS hiccup must degrade the already-live branch to unavailable,
    never fail a PR that satisfies one of the two original branches."""
    body = _GUARD.read_text()
    aws_step = body.split("Configure AWS credentials", 1)[1].split("- name:", 1)[0]
    assert "continue-on-error: true" in aws_step


def test_failure_message_names_the_apply_command():
    """A guard that blocks without naming the unblocking step makes every hit
    start with archaeology."""
    body = _GUARD.read_text()
    assert "--apply-iam" in body
