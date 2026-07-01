# scheduled-groom-dispatcher

Fires the autonomous backlog groom on a **reliable, on-time cadence** via
**EventBridge Scheduler → a dedicated EC2 spot box** (config#1322 for the
cadence; config#1432 for the spot move). EventBridge Scheduler replaces the
best-effort GHA `schedule:` crons; the spot box replaces the GitHub-hosted
runner so the ~hours-long groom stops burning the org's 2,000 included
PRIVATE-repo GHA Actions minutes (public repos are free; the groom ran in the
private `alpha-engine-config` repo). The box runs the SAME
`scripts/groom_run.sh` entrypoint the GHA workflow uses (single source of truth),
then self-terminates (~$2/mo).

> **Status: LIVE** (cutover 2026-06-30, config#1432). This is the sole scheduler
> for the backlog groom — the GHA `schedule:` crons in `backlog-groom.yml` have
> been REMOVED; that workflow now runs only on `workflow_dispatch` (manual
> escape hatch). Any code change here needs `deploy.sh` (code-only, or
> `--bootstrap` when a schedule/IAM changes) run by an operator with AWS creds
> before it has live effect — merging the PR alone does not deploy it.

## How it fits

```
EventBridge Scheduler rules (UTC, cron)                    THIS Lambda
  alpha-engine-scheduled-groom-0700-sunfri          ─┐      1. nousergon_lib.ec2_spot.launch()
  alpha-engine-scheduled-groom-2300-daily           ─┼───▶     (spot; on-demand fallback)
  alpha-engine-scheduled-groom-1500-daily-opus-high ─┘      2. wait instance running + SSM Online
                                                             3. async ssm send-command (detached):
                                                                   │
                                                                   ▼
   EC2 spot box (AL2023, alpha-engine-executor-profile)
     prelude: read PAT (SSM) → clone alpha-engine-config
       → export GROOM_MODEL / GROOM_ISSUE_FILTER (from the schedule's event)
       → infrastructure/groom_spot_bootstrap.sh --mode <full|sweep>
         → scripts/groom_run.sh  (budget gate → driver/sweep → digest-verify)
     → shutdown -h now (InstanceInitiatedShutdownBehavior=terminate)
```

The **box** reads the cross-repo PAT (`/alpha-engine/saturday_sf_watch/github_pat`)
and all other secrets itself from SSM via its instance profile — no new
credential, and this Lambda needs no secret access. The EventBridge Scheduler conventions
(`scheduler.amazonaws.com` execution role, `cron()` expression, OFF
flexible-time-window) mirror `infrastructure/run_weekly_offcycle.sh`.

## Cadence

| Scheduler rule | Expression (UTC) | PT | Day mask | run_mode | model | issue_filter |
|---|---|---|---|---|---|---|
| `…-0700-sunfri` | `cron(0 7 ? * SUN-FRI *)` | 12am | Sun–Fri (skips Sat) | full | claude-sonnet-5 | default |
| `…-2300-daily` | `cron(0 23 * * ? *)` | 4pm | daily incl. Sat | full | claude-sonnet-5 | default |
| `…-1500-daily-opus-high` | `cron(0 15 * * ? *)` | 8am | daily (all 7) | full | claude-opus-4-8 | high-only |

> **Reduced 3→2/day on 2026-06-29** (usage pacing): the former
> `…-1500-sunfri` rule (`cron(0 15 ? * SUN-FRI *)`, 8am PT, Sonnet/default) was
> dropped. **Re-added 2026-07-01** at the same 15:00 UTC slot (config#1495
> follow-up) as a DIFFERENT tier, not a reinstatement: `…-1500-daily-opus-high`
> runs **Opus** against **`complexity:high` issues only** — the queue the two
> Sonnet schedules above explicitly exclude. It shares the SAME weekly Max-quota
> reserve-for-interactive budget gate (`scripts/groom_budget.py`) as the Sonnet
> schedules — it competes for the same pool, not a separate one — and shuts
> down immediately/cleanly if the `complexity:high` queue is empty (a
> `total == 0` clean stop, never a floor-breach false-positive; see
> `scripts/groom_driver.py`). An issue an Opus chunk judges to need Brian's own
> judgment (a genuine, irreducible product/architecture fork — not mere
> difficulty) gets relabeled `complexity:ultra`, which permanently exits ALL
> automated grooming (both this schedule and the two Sonnet ones).

The Sat-skip rationale is carried verbatim from `backlog-groom.yml`: the
`…-0700-sunfri` rule avoids colliding with the 09:00-UTC Crucible Saturday
pipeline; the daily 23:00 rule runs every day (Brian wants the Sat 4pm-PT groom
retained).

EventBridge Scheduler cron uses 6 fields `cron(min hour day-of-month month
day-of-week year)` with `?` for an unspecified day field and `SUN-FRI` for the
day-of-week mask.

## Schedule-input routing

Each schedule passes a JSON input, e.g. `{"run_mode": "full", "model":
"claude-opus-4-8", "issue_filter": "high-only", "schedule": "0 15 * * *"}`.
The Lambda resolves each field (unknown/missing values degrade to a safe
default — `full` / `claude-sonnet-5` / `default`) and exports `GROOM_MODEL` +
`GROOM_ISSUE_FILTER` in the SSM bootstrap prelude ahead of invoking
`groom_spot_bootstrap.sh --mode <run_mode>`, which forwards them into
`scripts/groom_run.sh` → `scripts/groom_driver.py` (`--issue-filter` selects
the Sonnet default queue vs. the Opus `complexity:high`-only queue; `--model`
is passed straight to the `claude -p` invocation). `model` is validated against
a conservative allowlist regex before being embedded in the shell command
(defense-in-depth — the event is Lambda-config-controlled, not raw user input,
but injection protection is cheap).

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

## Validating a new/changed schedule

1. **Apply**: `bash …/deploy.sh --bootstrap` (operator with AWS creds) — creates/
   updates the 3 Scheduler rules from `SCHED_NAMES`/`SCHED_CRONS`/`SCHED_INPUTS`
   and prunes any live rule under the prefix no longer listed there.
2. **Verify wiring**: `aws scheduler list-schedules --name-prefix
   alpha-engine-scheduled-groom --region us-east-1` shows all 3.
3. **Bounded manual test**: `aws lambda invoke` with the target schedule's JSON
   input (see the Cadence table) to fire an on-demand run without waiting for
   the cron; add `"soft_limit_min"` handling is NOT read by the Lambda itself —
   cap a manual box's runtime by SSH/SSM-exporting `SOFT_LIMIT_MIN` before
   `groom_spot_bootstrap.sh --soft-limit-min <n>` runs, or invoke the bootstrap
   script directly with that flag on a manually launched box.
4. **Soak**: confirm the rule fires on time and produces (or, for `high-only`,
   cleanly no-ops on) a `groom-digest` issue in `alpha-engine-config`; watch the
   Lambda `Errors` metric stays at 0.
