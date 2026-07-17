# alpha-engine-sweep-artifact-monitor

Post-SF sweep-artifact validation for the backlog groom dispatch
(`alpha-engine-config#2392`).

## What it does

The `alpha-engine-groom-dispatch` Step Function's `DispatchEndOfSfSweep`
state (config#2201 / #2311) unconditionally fires one Haiku `run_mode=sweep`
spot box per trigger cycle, fire-and-forget — the SF SUCCEEDS well before the
sweep box finishes and writes its own S3 run artifact
(`groom/{date}/sweep-{HHMMSS}.json`, `run_kind=sweep`). Nothing outside the
box itself previously cross-checked that the artifact actually landed — a
silent sweep-skip (IAM, a Lambda error, an SF Catch misroute, or the sweep
box dying before its own artifact-verify) went undetected until stale open
PRs were noticed by hand.

This Lambda subscribes (EventBridge) to `alpha-engine-groom-dispatch`
**SUCCEEDED** execution-status-change events only. For each:

1. Reads the execution's own output (`states:DescribeExecution`) — the
   `DispatchEndOfSfSweep` state's `$.sweep` result — to determine what this
   cycle actually did:
   - `reason == "concurrent_tier_skip"` (config#1979's concurrent guard —
     a prior cycle's sweep box was still live) → **no artifact expected
     this cycle**; the next cycle's sweep covers it. Not alerted.
   - `dispatched is False` (a genuine sweep-launch failure, already
     recorded + SNS-notified by the SF's own `NotifySweepDispatchFailure`
     state) → not alerted here (would double-notify the same incident).
   - Otherwise → an artifact is expected.
2. For the expected-artifact case, searches
   `s3://alpha-engine-research/groom/{date}/sweep-*.json` (today's UTC date
   and the prior UTC date, to cover a `stopDate` shortly after midnight) for
   a `run_kind=sweep` artifact whose `run_start` falls within
   `SWEEP_GRACE_MINUTES` (default 15) of the execution's completion time.
3. Missing → publishes to the `alpha-engine-alerts` SNS topic.

Never triggered on FAILED/TIMED_OUT/ABORTED executions — the EventBridge rule
itself is scoped to `SUCCEEDED` only, and the handler independently
re-checks the status before doing any work.

## Acceptance criteria (config#2392)

1. Every SUCCEEDED `alpha-engine-groom-dispatch` execution gets checked
   (EventBridge fan-out, one event per execution).
2. A missing sweep artifact alerts via the `alpha-engine-alerts` SNS topic.
3. A FAILED execution never triggers a check (rule-level + handler-level
   filter).

## Fail-loud

`states:DescribeExecution` / `s3:ListObjectsV2` / `s3:GetObject` /
`sns:Publish` failures all **raise** — the EventBridge retry policy + a
CloudWatch alarm on Lambda errors page the operator rather than this check
silently skipping (`feedback_no_silent_fails`).

## Deploy

```bash
bash infrastructure/lambdas/sweep-artifact-monitor/deploy.sh             # update code only
bash infrastructure/lambdas/sweep-artifact-monitor/deploy.sh --bootstrap # first-time create + wire EventBridge
bash infrastructure/lambdas/sweep-artifact-monitor/deploy.sh --dry-run   # show actions, do not apply
```

Managed outside CloudFormation — same rationale as the sibling
`friday-shell-run-report` / `sf-telegram-notifier` Lambdas (keeps the
`github-actions-lambda-deploy` OIDC role's blast radius narrow;
operator-deployed only). The EventBridge rule
(`alpha-engine-sweep-artifact-monitor`) is discovered automatically by
`infrastructure/eventbridge/check-drift.py` (no registry to maintain there).

**Not yet live**: this Lambda + its EventBridge rule need an operator to run
`deploy.sh --bootstrap` against real AWS credentials (this repo's CI OIDC
role cannot create IAM roles) and then observe one real
`alpha-engine-groom-dispatch` SUCCEEDED execution end-to-end to confirm the
alert fires correctly on a genuinely missing artifact.
