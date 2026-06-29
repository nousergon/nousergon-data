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

## M2 — agent dispatch (behind a flag, default OFF)

When `AGENT_DISPATCH_ENABLED=true`, **after** writing the watch-log (so the
agent reads fresh context) the Lambda also fires a GitHub `repository_dispatch`
(`event_type=saturday-sf-failure`) to `DISPATCH_REPO`
(`nousergon/alpha-engine-config`), passing the failure context
(`execution_arn`, `failed_state`, `cause`, `run_date`, `watch_log_key`,
`is_preflight`). That triggers the autonomous resilience-agent GHA workflow,
which diagnoses → fixes → **merges** → **reruns the SF from the failed step**,
then reports back (enriches the watch-log + dashboard + Telegram).

The dispatch is **best-effort with a recording surface** (CLAUDE.md
no-silent-fails secondary carve-out): a GitHub/SSM outage logs `WARNING` and is
returned in `agent_dispatch.error`, but does NOT raise — the primary observe
deliverable (watch-log) already landed. The PAT is read from SSM
(`/alpha-engine/saturday_sf_watch/github_pat`, SecureString) at dispatch time
and is never logged.

**Activation (operator steps — keep `AGENT_DISPATCH_ENABLED=false` until done):**
1. Mint a dedicated fine-grained PAT scoped to the Saturday-SF-path repos
   (contents + pull-requests write) and store it:
   `aws ssm put-parameter --name /alpha-engine/saturday_sf_watch/github_pat --type SecureString --value <PAT>`.
2. Re-apply the dispatcher role policy so the new `AgentDispatchPAT` SSM grant
   lands: `bash …/deploy.sh --bootstrap` (idempotent) — or `aws iam
   put-role-policy` directly.
3. Land the agent GHA workflow + scoped OIDC role (M2c, alpha-engine-config).
4. Flip the flag:
   `aws lambda update-function-configuration --function-name alpha-engine-saturday-sf-watch-dispatcher --environment 'Variables={LOG_LEVEL=INFO,AGENT_DISPATCH_ENABLED=true}' --query LastUpdateStatus --output text`.

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
detail.stateMachineArn:  arn:...:stateMachine:alpha-engine-weekly-pipeline
detail.executionArn:     execution arn
detail.name:             execution id
detail.startDate:        epoch ms
```

## Milestones (see #1227)

- **M1** ✅ observe-only watch-log + Telegram receipt.
- **M3** ✅ "Saturday SF Watch" dashboard page reading the watch-log.
- **M2a** ✅ (this) `repository_dispatch` path behind `AGENT_DISPATCH_ENABLED`.
- **M2c** — agent GHA workflow + charter + scoped OIDC role (with
  `states:StartExecution` for the rerun) in alpha-engine-config; one-time
  historical `workflow_dispatch` charter test, then flip the flag.
- **M4** — independent artifact-integrity net on the weekday pre-run (parallel
  swallow safeguard; non-blocking).

**Posture (Brian-ratified):** ask-forgiveness — the agent merges the fix and
reruns the SF from the failed step autonomously, then reports. The ONE
forced-propose class is IAM (`put-role-policy` is human-gated).
