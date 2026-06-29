# saturday-sf-success-groom-dispatcher

Runs the daily backlog groom **after a successful Saturday SF**, so the fresh
post-Director backlog (including the Director's just-filed weekly proposals) gets
groomed immediately rather than waiting for the next scheduled 10pm-PT run.

## How it fits

```
ne-weekly-freshness-pipeline  ──SUCCEEDED──▶  EventBridge rule
  (status change event)                         alpha-engine-saturday-succeeded-groom
                                                        │ target
                                                        ▼
                                          THIS Lambda (repository_dispatch)
                                                        │ type: saturday-sf-success-groom
                                                        ▼
                            nousergon/alpha-engine-config :: backlog-groom.yml
                                          (on: repository_dispatch → FULL groom)
```

It mirrors the failure-side sibling `saturday-sf-watch-dispatcher` and **reuses the
same SSM PAT** (`/alpha-engine/saturday_sf_watch/github_pat`) for the
repository_dispatch — no new credential.

## Why this and not an in-SF task

The Saturday SF definition (`step_function.json`) is deliberately **untouched** — an
EventBridge rule on the SF's terminal `SUCCEEDED` status is zero-risk relative to
editing the pipeline, and reuses the proven watch-dispatcher pattern. Functionally
identical to "run the groom once the Saturday SF completes."

## Contract & safety

- **Fires only on `SUCCEEDED`** (the rule scopes it; the handler re-checks
  defensively so a mis-scoped rule can never fire on a non-success).
- **Best-effort, non-fatal**: a GitHub/SSM outage logs WARN + is returned in the
  result, never raises. The scheduled nightly groom is the backstop.
- **Kill-switch**: set the Lambda env `GROOM_DISPATCH_ENABLED=false` to disable
  without removing the rule.

## Deploy (operator-managed, outside CloudFormation)

```bash
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh --bootstrap  # first time: role + lambda + rule
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh              # update code only
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh --dry-run    # preview
bash infrastructure/lambdas/saturday-sf-success-groom-dispatcher/deploy.sh --smoke      # ⚠ fires a REAL groom run
```

Merging the PR has **zero live effect** until an operator runs `--bootstrap`.
`--smoke` invokes the Lambda with a synthetic SUCCEEDED event and (since
`GROOM_DISPATCH_ENABLED` defaults on) **triggers an actual groom run** — use
intentionally.

## Prerequisite in alpha-engine-config

`backlog-groom.yml` must listen for the trigger:

```yaml
on:
  repository_dispatch:
    types: [saturday-sf-success-groom]
```

(added alongside the existing `schedule` + `workflow_dispatch` triggers).
