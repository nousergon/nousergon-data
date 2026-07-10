# saturday-sf-success-groom-dispatcher

Runs the backlog groom **after a successful Saturday SF**, so the fresh
post-Director backlog (including the Director's just-filed weekly proposals) gets
groomed immediately rather than waiting for the next scheduled slot.

## How it fits (config#2175 — GHA groom execution retired)

```
ne-weekly-freshness-pipeline  ──SUCCEEDED──▶  EventBridge rule
  (status change event)                         alpha-engine-saturday-succeeded-groom
                                                        │ target
                                                        ▼
                                          THIS Lambda (async boto3 invoke)
                                                        │ InvocationType=Event
                                                        ▼
                            alpha-engine-scheduled-groom-dispatcher (sibling dir)
                              event {"run_mode": "full", "trigger": "demand-all",
                                     "schedule": "saturday-sf-success"}
                                                        │
                                                        ▼
                              0..3 EC2-spot groom boxes (per-tier demand gates)
```

The demand-all trigger evaluates the fresh post-SF backlog per tier — strictly
better than the old unconditional single FULL groom this Lambda used to
`repository_dispatch` to `nousergon/alpha-engine-config :: backlog-groom.yml`
(workflow DELETED, config#2175). The GitHub PAT read + urllib dispatch machinery
were removed with it — this Lambda needs **no GitHub credential**.

## Why this and not an in-SF task

The Saturday SF definition (`step_function.json`) is deliberately **untouched** — an
EventBridge rule on the SF's terminal `SUCCEEDED` status is zero-risk relative to
editing the pipeline, and reuses the proven watch-dispatcher pattern. Functionally
identical to "run the groom once the Saturday SF completes."

## Contract & safety

- **Fires only on `SUCCEEDED`** (the rule scopes it; the handler re-checks
  defensively so a mis-scoped rule can never fire on a non-success).
- **Best-effort, non-fatal**: a Lambda-invoke outage logs WARN + is returned in
  the result, never raises. The scheduled groom cadence is the backstop.
- **Downstream gates still apply**: the scheduled dispatcher runs its own
  pre-boot pace gate + per-tier demand gates on this trigger exactly as on its
  cron slots — a post-SF trigger against a drained/over-pace backlog launches
  nothing, by design.
- **Kill-switch**: set the Lambda env `GROOM_DISPATCH_ENABLED=false` to disable
  without removing the rule.

## Deploy (operator-managed, outside CloudFormation)

```bash
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh --bootstrap  # first time: role + lambda + rule
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh              # update code only
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh --dry-run    # preview
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh --smoke      # ⚠ fires a REAL groom trigger
```

Merging the PR has **zero live effect** until an operator runs `deploy.sh` (code)
and re-applies the IAM policy (the config#2175 reroute swaps the SSM-PAT read for
`lambda:InvokeFunction` on the scheduled dispatcher):

```bash
aws iam put-role-policy --role-name alpha-engine-saturday-sf-success-groom-dispatcher-role --policy-name alpha-engine-saturday-sf-success-groom-dispatcher-policy --policy-document file://infrastructure/lambdas/saturday-sf-success-groom-dispatcher/iam-policy.json
```

`--smoke` invokes the Lambda with a synthetic SUCCEEDED event and (since
`GROOM_DISPATCH_ENABLED` defaults on) **triggers an actual demand-all groom
evaluation** — use intentionally.
