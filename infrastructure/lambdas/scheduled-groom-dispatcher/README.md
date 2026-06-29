# scheduled-groom-dispatcher

Fires the autonomous backlog groom on a **reliable, on-time cadence** via
**EventBridge Scheduler → `repository_dispatch`**, replacing the
`backlog-groom.yml` GitHub Actions `schedule:` crons, which are best-effort and
fire late or are silently dropped (config#1322).

> **Status: UNVALIDATED.** This is the IaC + handler only. It has **zero live
> effect** until an operator runs `deploy.sh --bootstrap`, and the GHA
> `schedule:` crons stay live as a belt-and-suspenders backstop until a multi-day
> on-time-firing soak confirms this path (see **Soak** below). Only then can the
> GHA crons be removed.

## How it fits

```
EventBridge Scheduler rules (UTC, cron)              THIS Lambda
  alpha-engine-scheduled-groom-0700-sunfri  ─┐       (repository_dispatch)
  alpha-engine-scheduled-groom-2300-daily   ─┴─────▶  type: scheduled-groom
                                                      client_payload.run_mode
                                                              │
                                                              ▼
                            nousergon/alpha-engine-config :: backlog-groom.yml
                         (on: repository_dispatch[scheduled-groom] → run_mode-routed groom)
```

It mirrors the sibling `saturday-sf-success-groom-dispatcher` and **reuses the
same SSM PAT** (`/alpha-engine/saturday_sf_watch/github_pat`) for the
repository_dispatch — no new credential. The EventBridge Scheduler conventions
(`scheduler.amazonaws.com` execution role, `cron()` expression, OFF
flexible-time-window) mirror `infrastructure/run_weekly_offcycle.sh`.

## Cadence — one-for-one with the GHA crons it replaces

| Scheduler rule | Expression (UTC) | GHA cron replaced | PT | Day mask | run_mode |
|---|---|---|---|---|---|
| `…-0700-sunfri` | `cron(0 7 ? * SUN-FRI *)` | `0 7 * * 0-5` | 12am | Sun–Fri (skips Sat) | full |
| `…-2300-daily` | `cron(0 23 * * ? *)` | `0 23 * * *` | 4pm | daily incl. Sat | full |

> **Reduced 3→2/day on 2026-06-29** (usage pacing): the former
> `…-1500-sunfri` rule (`cron(0 15 ? * SUN-FRI *)`, 8am PT) was dropped.
> `deploy.sh` step 2e prunes it from live on the next `--bootstrap`; the matching
> `0 15 * * 0-5` GHA cron is removed in the same change.

The Sat-skip rationale is carried verbatim from `backlog-groom.yml`: the
`…-0700-sunfri` rule avoids colliding with the 09:00-UTC Crucible Saturday
pipeline; the daily 23:00 rule runs every day (Brian wants the Sat 4pm-PT groom
retained).

EventBridge Scheduler cron uses 6 fields `cron(min hour day-of-month month
day-of-week year)` with `?` for an unspecified day field and `SUN-FRI` for the
day-of-week mask.

## Run-mode routing

Each schedule passes a JSON input `{"run_mode": "...", "schedule": "..."}`. The
Lambda forwards `run_mode` into `client_payload` (also as `phase` for
forward-compat); `backlog-groom.yml`'s run-mode step reads
`github.event.client_payload.run_mode` to route `full` vs `sweep`. Both
current rules are `full` (the drain-phase default). An unknown/missing run_mode
degrades to `full`.

## Contract & safety

- **Fail-loud** (UNLIKE the convenience success-dispatcher): a scheduled groom
  IS the deliverable — it replaces the cron the workflow depends on — so a
  GitHub/SSM failure **RAISES**, letting EventBridge's retries + the Lambda
  error metric surface a dropped pass. (Wire a CloudWatch alarm on the function
  `Errors` metric during bootstrap-ops to page on it.)
- **Kill-switch**: set the Lambda env `GROOM_DISPATCH_ENABLED=false` to disable
  without deleting the Scheduler rules.
- **Narrow IAM**: the Lambda role reads only the one SSM PAT + its own log
  group; the Scheduler execution role can `lambda:InvokeFunction` this function
  only.

## Deploy (operator-managed, outside CloudFormation)

```bash
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh --bootstrap  # roles + lambda + schedules (creates/updates from SCHED_NAMES; prunes orphaned rules)
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh              # update code only
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh --dry-run    # preview
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh --smoke      # ⚠ fires a REAL groom run
```

Merging the PR has **zero live effect** until an operator runs `--bootstrap`.
`--smoke` invokes the Lambda with a synthetic schedule event and (since
`GROOM_DISPATCH_ENABLED` defaults on) **triggers an actual groom run** — use
intentionally.

## Validation — apply + on-time-firing soak (REQUIRED before removing GHA crons)

1. **Prereq**: the `alpha-engine-config` PR adding the `scheduled-groom`
   dispatch type + run-mode routing must be merged (cross-linked below).
2. **Apply**: `bash …/deploy.sh --bootstrap` (operator with AWS creds).
3. **Verify wiring**: `aws scheduler list-schedules --name-prefix
   alpha-engine-scheduled-groom --region us-east-1` shows both; `--smoke`
   produces a backlog-groom run in alpha-engine-config Actions.
4. **Soak (multi-day)**: confirm each rule fires **on time** for a week — compare
   the EventBridge/Lambda CloudWatch invocation timestamps against the cron and
   confirm a matching `repository_dispatch` (`scheduled-groom`) backlog-groom run
   appears within a minute. Watch the Lambda `Errors` metric stays at 0.
5. **Cut over**: only after the soak confirms reliable on-time firing, remove the
   remaining `schedule:` crons from `backlog-groom.yml` (a follow-up PR), leaving
   EventBridge as the sole scheduler. Until then both run (the workflow's
   `concurrency` group serialises any rare overlap, so the belt-and-suspenders
   window is harmless).

## Prerequisite in alpha-engine-config

`backlog-groom.yml` must listen for the trigger and honor the run-mode:

```yaml
on:
  repository_dispatch:
    types: [saturday-sf-success-groom, scheduled-groom]
```

(the run-mode step reads `github.event.client_payload.run_mode`).
