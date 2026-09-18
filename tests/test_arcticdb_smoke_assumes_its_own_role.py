"""The live-ArcticDB smoke assumes its own read role, never the deploy role.

alpha-engine-config-I10110 (stage 2 of I10109, ruling I9788 option b).

WHAT THIS EXISTS TO STOP COMING BACK
------------------------------------
`live-arcticdb-smoke.yml` triggers on `pull_request` and assumed
`github-actions-lambda-deploy` — the fleet's widest CI identity (ECR push,
`lambda:UpdateFunctionCode`, CloudFormation stack update, `iam:PassRole`,
`ssm:SendCommand` onto the dashboard box). That single consumer is the ONLY
reason `repo:nousergon/nousergon-data:pull_request` appeared in that role's
trust; every other entry there is `ref:refs/heads/main`, i.e. code a human
merged. So a workflow edit on any unreviewed branch in this repo could reach
the deploy identity's whole authority, and the smoke needed two S3 statements.

The replacement is `github-actions-arcticdb-smoke-read`, codified in
`nous-ergon-ops/infrastructure/iam/github-actions-arcticdb-smoke-read/`.

The same split was made for the SF-definition validator
(`ne-github-sf-validate`, alpha-engine-config-I7798) and
`tests/test_sf_definition_validated_preflight.py` guards it the same way — an
assertion on the assumed role plus a negative on the deploy role, because the
regression shape is a copy-paste of `role-to-assume` from another workflow,
which reads as correct in a diff and is invisible at runtime (the run goes
green either way; only the blast radius changes).
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "live-arcticdb-smoke.yml"

# The account segment is either the literal id or the repository VARIABLE
# holding it — this repo is public and its run logs are public
# (alpha-engine-config-I10973), so the literal form was replaced with
# `${{ vars.AWS_ACCOUNT_ID }}`. Both spellings name the same role.
SMOKE_ROLE_ARN = (
    "arn:aws:iam::711398986525:role/github-actions-arcticdb-smoke-read"
)
SMOKE_ROLE_ARN_VAR = (
    "arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/github-actions-arcticdb-smoke-read"
)
DEPLOY_ROLE = "github-actions-lambda-deploy"


def test_the_workflow_exists() -> None:
    """A rename would make every assertion below pass by vacuum."""
    assert WORKFLOW.is_file(), f"missing {WORKFLOW}"


def test_the_smoke_assumes_the_dedicated_read_role() -> None:
    wf = WORKFLOW.read_text(encoding="utf-8")
    assert f"role-to-assume: {SMOKE_ROLE_ARN}" in wf or f"role-to-assume: {SMOKE_ROLE_ARN_VAR}" in wf, (
        f"live-arcticdb-smoke.yml must assume github-actions-arcticdb-smoke-read "
        f"(literal or {SMOKE_ROLE_ARN_VAR!r}) — the role whose whole grant is a "
        f"read of s3://alpha-engine-research/arcticdb/*. alpha-engine-config-I10110."
    )


def test_the_smoke_never_assumes_the_deploy_role() -> None:
    """Not implied by the assertion above: a workflow can name two roles, and a
    second `configure-aws-credentials` step added later would overwrite the
    first job's credentials without changing the line the test above reads."""
    wf = WORKFLOW.read_text(encoding="utf-8")
    executable = [
        line
        for line in wf.splitlines()
        if DEPLOY_ROLE in line and not line.lstrip().startswith("#")
    ]
    assert not executable, (
        f"live-arcticdb-smoke.yml names {DEPLOY_ROLE} outside a comment: "
        f"{executable}. This PR-triggered workflow must never reach the deploy "
        f"identity — that trust is being removed from it (I10110 stage 3), so "
        f"the run would fail as well as being wrong."
    )


def test_the_workflow_stays_pull_request_triggered() -> None:
    """The role's trust is `repo:nousergon/nousergon-data:pull_request` and
    nothing else, so moving this workflow onto `push` or `schedule` makes it
    unassumable. That failure would read as an IAM outage rather than as the
    trigger change that caused it — assert the coupling where it is legible."""
    wf = WORKFLOW.read_text(encoding="utf-8")
    assert "pull_request:" in wf, (
        "live-arcticdb-smoke.yml no longer triggers on pull_request, but "
        "github-actions-arcticdb-smoke-read trusts only "
        "`repo:nousergon/nousergon-data:pull_request`. Change the role's trust "
        "in nous-ergon-ops in the same arc, or the smoke cannot authenticate."
    )
