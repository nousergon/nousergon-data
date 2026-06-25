# saturday-sf-watch-dispatcher

**Saturday-SF Watch — M1 (OBSERVE).** First slice of the autonomous
Saturday-SF resilience arc. Spec: [nousergon/alpha-engine-config#1227](https://github.com/nousergon/alpha-engine-config/issues/1227).

## What it does (M1)

On a **Saturday SF terminal failure** (`FAILED` / `TIMED_OUT` / `ABORTED`) this
Lambda:

1. Reads the failure cause (`DescribeExecution`) and the **failed state name**
   (`GetExecutionHistory` — the entered-but-not-exited state).
2. Appends an event to the **watch-log artifact**
   `s3://alpha-engine-research/consolidated/saturday_sf_watch/{run_date}.json`
   (the contract the M3 dashboard page reads; repeated failures in one Saturday
   accumulate).
3. Sends a **distinct, SILENT** Telegram receipt naming the failed state + the
   artifact location (the `sf-telegram-notifier` already pinged loud on the same
   FAILED event — this is the additive watch record, not a duplicate alert).

It does **not** invoke any agent, touch any repo/IAM/SF definition, or rerun
anything. `AGENT_DISPATCH_ENABLED` (default `false`) is the M2 seam.

## Why it's not a second notifier

`alpha-engine-sf-telegram-notifier` already covers generic SF notification
(all three SFs, all statuses). This Lambda's distinct concerns are the
**Saturday-failure-only trigger** (the seam the agent will hang off), the
**watch-log artifact contract**, and a Saturday-scoped IAM surface — kept
separate so the notifier's IAM stays narrow.

## Fail-loud posture

- **Primary** (RAISES on failure): the watch-log S3 write — a broken producer
  must surface via the Lambda error metric + CW alarm.
- **Best-effort** (logged WARN, recorded in artifact): `DescribeExecution` /
  `GetExecutionHistory` enrichment and the Telegram receipt.

## Deploy

Managed outside CloudFormation (operator-deployed; keeps the GHA OIDC role's
blast radius narrow). **Merging the PR has zero live effect** — an operator
bootstraps it:

```
bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh --dry-run    # preview
bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh --bootstrap  # first-time: role + Lambda + EventBridge rule
bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh              # code-only update
bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh --smoke      # synthetic FAILED invoke
```

## Test

```
python3 -m pytest infrastructure/lambdas/saturday-sf-watch-dispatcher/test_handler.py -q
```

## Event shape (EventBridge `aws.states` / Step Functions Execution Status Change)

```
detail.status:           FAILED | TIMED_OUT | ABORTED   (rule-scoped)
detail.stateMachineArn:  arn:...:stateMachine:alpha-engine-saturday-pipeline
detail.executionArn:     execution arn
detail.name:             execution id
detail.startDate:        epoch ms
```

## Next milestones (see #1227)

- **M2** — wire `repository_dispatch` (behind `AGENT_DISPATCH_ENABLED`) to the
  agent GHA workflow; run propose-only.
- **M3** — "Saturday SF Watch" dashboard page reading the watch-log artifact.
- **M4** — independent artifact-integrity gate on the weekday pre-run.
- **M5** — flip autonomous merge after the propose-only soak earns trust.
