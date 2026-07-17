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

Wired via `.github/workflows/sf-arn-drift-check.yml` (PR-triggered on the
paths above, daily 09:30 UTC, `workflow_dispatch`), alongside the SF
LoggingConfiguration guard — see `infrastructure/step-functions/README.md`'s
"CI wiring" note. Requires `events:DescribeRule` / `events:ListRules` on the
`github-actions-iam-drift-check` OIDC role's policy (codified in
`crucible-executor`) — until that policy is applied live (operator-run
`apply.sh`, not automated per that repo's IAM doctrine), this workflow's
steps will fail on AccessDenied rather than report real drift.
