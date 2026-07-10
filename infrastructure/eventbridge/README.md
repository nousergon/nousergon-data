# EventBridge SF-ARN drift guard (alpha-engine-config#1464)

`check-drift.py` diffs the `stateMachineArn` values baked into each
script-managed EventBridge rule's `EventPattern` against what's live on
AWS. It's the CI backstop for the drift class that bit this repo on the
2026-06-29 `ne-*` Step Function rename (config#1381): a rule whose
`EventPattern` still matches the OLD SF name/ARN doesn't error — it just
quietly stops firing.

## Scope

Covers EventBridge rules wired by `infrastructure/lambdas/*/deploy.sh`
scripts (managed outside CloudFormation — see each `deploy.sh`'s header
comment for why). This script discovers them by scanning for an
`EVENT_PATTERN=$(cat <<EOF ... EOF)` heredoc keyed on `stateMachineArn`, so
a new Lambda wiring this pattern is picked up automatically — no registry
to maintain here.

**Not covered:** the two CFN-managed cron rules (`SaturdayTrigger` /
`WeekdayTrigger` in
`infrastructure/cloudformation/alpha-engine-orchestration.yaml`). Those are
reconciled on every push to `main` by `deploy-infrastructure.yml`, so they
can't silently drift the way the script-managed rules did.

## Usage

```bash
# Check every discovered rule
./infrastructure/eventbridge/check-drift.py

# Check one rule
./infrastructure/eventbridge/check-drift.py --rule alpha-engine-sf-status-change
```

Requires AWS creds with `events:DescribeRule` on the target rules.

## CI wiring

Not yet wired into a workflow — the PR that added this script includes the
intended `sf-drift-check.yml` diff in its description, marked as
needs-manual-apply (the runner that authored it lacks the `workflow` OIDC
scope to push workflow YAML directly). It also needs
`events:DescribeRule` added to the `github-actions-iam-drift-check` OIDC
role's policy (codified in `crucible-executor`, not this repo).
