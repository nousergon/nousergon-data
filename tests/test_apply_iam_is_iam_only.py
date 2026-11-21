"""`deploy.sh --apply-iam` applies IAM and NOTHING else (config-I7444).

The measured incident, 2026-08-16. `nousergon-data-PR1400` changed
`pipeline-watchdog/iam-policy.json`, and `iam-policy-change-guard` (a
REQUIRED check) went red because the policy was not live. The operator ran
the exact command the guard printed, from `~/Development/nousergon-data`,
which was on `main`. Output ended `✓ IAM applied.` Two things had happened,
neither of them visible:

  1. The policy applied was **main's** — the change lived on an unmerged
     branch — so `put-role-policy` wrote a document identical to what was
     already live. A complete no-op that printed success.
  2. `--apply-iam` did not stop after applying IAM. The script continued into
     `update-function-code` and **reverted the deployed Lambda to main's
     code**, undoing a deploy, from a flag named `--apply-iam`.

Two of the thirty-five deploy scripts (`freshness-monitor`,
`ssm-reachability-probe`) already exited after applying. This module pins
that property for all of them, so the correct behaviour cannot silently
diverge again on a thirty-sixth.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPTS = sorted(REPO.glob("infrastructure/lambdas/*/deploy.sh"))

# This list may only ever SHRINK. It is empty now that every deploy.sh with an
# --apply-iam path uses the shared helper.
GRANDFATHERED_INLINE_APPLY: frozenset[str] = frozenset()


def _apply_iam_scripts() -> "list[Path]":
    return [p for p in DEPLOY_SCRIPTS if "APPLY_IAM=false" in p.read_text()]


def _apply_iam_block(path: Path) -> str:
    m = re.search(r"if \$APPLY_IAM; then\n(.*?)\nfi\n", path.read_text(), re.S)
    assert m, f"{path.parent.name}: no `if $APPLY_IAM; then ... fi` block found"
    return m.group(1)


def test_there_are_apply_iam_scripts_to_check() -> None:
    """A guard that silently matches nothing is not a guard."""
    assert len(_apply_iam_scripts()) >= 30


@pytest.mark.parametrize(
    "script", _apply_iam_scripts(), ids=lambda p: p.parent.name
)
def test_apply_iam_exits_and_never_falls_through_to_a_code_deploy(
    script: Path,
) -> None:
    """`--apply-iam` must terminate the script.

    Falling through means an operator applying a GRANT also ships whatever
    code is in their working tree — which on 2026-08-16 silently reverted a
    deployed Lambda to an older commit.
    """
    body = _apply_iam_block(script)
    assert re.search(r"^\s*exit 0\s*$", body, re.M), (
        f"{script.parent.name}/deploy.sh: the --apply-iam block does not exit, "
        f"so it falls through into the code-deploy path. A flag named "
        f"--apply-iam must not update function code. Mirror "
        f"freshness-monitor/deploy.sh."
    )


@pytest.mark.parametrize(
    "script", _apply_iam_scripts(), ids=lambda p: p.parent.name
)
def test_apply_iam_says_that_nothing_else_was_touched(script: Path) -> None:
    """The operator must be able to tell from the output that this did not
    deploy anything. `✓ IAM applied.` alone does not say that."""
    body = _apply_iam_block(script)
    assert "Nothing else was touched" in body, (
        f"{script.parent.name}/deploy.sh: the --apply-iam block does not state "
        f"that it touched nothing else. The operator reads this line to know "
        f"whether a code deploy happened."
    )


@pytest.mark.parametrize(
    "script", _apply_iam_scripts(), ids=lambda p: p.parent.name
)
def test_apply_iam_uses_the_shared_helper(script: Path) -> None:
    """One applier, so the no-op detection added for I7444 reaches every
    caller. The grandfather list may only shrink."""
    name = script.parent.name
    body = _apply_iam_block(script)
    if name in GRANDFATHERED_INLINE_APPLY:
        pytest.skip(f"{name} is grandfathered; the list may only shrink")
    assert "apply_iam_policy " in body, (
        f"{name}/deploy.sh hand-rolls the IAM apply instead of calling "
        f"apply_iam_policy from _shared/apply_iam_policy.sh. A second copy "
        f"does not get the no-op detection, and duplication here has already "
        f"produced one silent failure."
    )


def test_grandfather_list_names_only_real_scripts() -> None:
    """A stale grandfather entry hides a script that no longer exists, and
    makes the list look smaller than the debt it represents."""
    names = {p.parent.name for p in _apply_iam_scripts()}
    unknown = GRANDFATHERED_INLINE_APPLY - names
    assert not unknown, (
        f"grandfather list names scripts that do not exist or no longer "
        f"support --apply-iam: {sorted(unknown)}"
    )


def test_shared_applier_detects_a_no_op_apply() -> None:
    """The header property: apply_iam_policy compares live against the file
    and says which, so an apply from the wrong checkout is not silent."""
    shared = (REPO / "infrastructure" / "lambdas" / "_shared"
              / "apply_iam_policy.sh").read_text()
    assert "get-role-policy" in shared, (
        "apply_iam_policy no longer reads the live policy, so it cannot tell "
        "a real change from a no-op — the I7444 failure mode is back"
    )
    for marker in ("NO-OP", "wrong checkout", "DIFFERS"):
        assert marker in shared, (
            f"apply_iam_policy no longer reports {marker!r}; the operator "
            f"cannot distinguish an applied change from an idempotent rewrite"
        )


@pytest.mark.parametrize("managed_policy_arn", [None, "arn:aws:iam::aws:policy/example"])
def test_shared_applier_optionally_attaches_managed_policy(
    tmp_path: Path, managed_policy_arn: str | None
) -> None:
    """The fifth argument adds exactly one post-creation attachment."""
    policy_file = tmp_path / "policy.json"
    policy_file.write_text("{}")
    helper = REPO / "infrastructure/lambdas/_shared/apply_iam_policy.sh"
    arguments = "" if managed_policy_arn is None else f" {managed_policy_arn!r}"
    command = f"""
source {str(helper)!r}
DRY_RUN=true
aws() {{
  if [ "$1 $2" = "iam get-role" ]; then
    echo 'NoSuchEntity' >&2
    return 1
  fi
  if [ "$1 $2" = "iam get-role-policy" ]; then
    echo 'NoSuchEntity' >&2
    return 1
  fi
}}
run() {{ printf '%s\\n' "$*"; }}
apply_iam_policy role policy {str(policy_file)!r} '{{}}'{arguments}
"""
    result = subprocess.run(
        ["bash", "-c", command],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )

    attachments = [
        line for line in result.stdout.splitlines() if "attach-role-policy" in line
    ]
    if managed_policy_arn is None:
        assert attachments == []
    else:
        assert attachments == [
            f"aws iam attach-role-policy --role-name role --policy-arn {managed_policy_arn}"
        ]
