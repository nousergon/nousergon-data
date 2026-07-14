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
> escape hatch). **CODE auto-deploys on merge to main** (2026-07-02) via
> `.github/workflows/deploy-scheduled-groom-dispatcher.yml`. A **schedule or
> IAM change** (`SCHED_NAMES`/`SCHED_CRONS`/`SCHED_INPUTS`, `iam-policy.json`,
> `step_function_groom.json`, `sf-execution-iam-policy.json`) still has ZERO
> live effect until an operator runs `deploy.sh --bootstrap` by hand — the CI
> OIDC role deliberately cannot create/modify IAM roles (see `deploy.sh`'s
> header comment).

## How it fits

```
EventBridge Scheduler rules (UTC, cron)                    THIS Lambda
  alpha-engine-scheduled-groom-0100-daily-opus-high     ─┐      1. nousergon_lib.ec2_spot.launch()
  alpha-engine-scheduled-groom-0700-daily-mid           ─┼───▶     (spot; on-demand fallback)
  alpha-engine-scheduled-groom-1900-daily-low           ─┘      2. wait instance running + SSM Online
  alpha-engine-scheduled-groom-sun0900-weekly-gated-reverify  (Sun 09:00 UTC, Haiku gated-reverify)
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
| `…-0100-daily-opus-high`¹ | `cron(0 1 * * ? *)` | 6pm | daily (all 7) | full | claude-sonnet-5 | high-only |
| `…-0700-daily-mid` | `cron(0 7 * * ? *)` | 12am | daily (all 7) | full | claude-sonnet-5 | mid-only |
| `…-1900-daily-low` | `cron(0 19 * * ? *)` | 12pm | daily (all 7) | full | claude-haiku-4-5 | low-only |
| `…-sun0900-weekly-gated-reverify` | `cron(0 9 ? * SUN *)` | 2am | Sun only | full | claude-haiku-4-5 | gated-reverify |

> ¹ config#2409 (2026-07-13): the high-only slot's MODEL moved off Opus onto
> Sonnet; the Scheduler rule NAME is left as `-opus-high` deliberately — a
> rename would require an operator-run `--bootstrap` (see the warning above)
> purely for a cosmetic AWS identifier, which isn't worth the live-schedule
> churn. Rename opportunistically the next time this cadence table changes
> for a substantive reason.

> **Tier-split cadence (config#1760/#2409):** three disjoint queues — Haiku
> on `complexity:low`, Sonnet on `complexity:mid` (+ unlabeled) and on
> `complexity:high` (moved off Opus 2026-07-13 — see config#2409; the split
> is queue-isolation/dedicated-attention, not a model-capability step up).
> Requires `alpha-engine-config` driver filters (#1761) on
> the box's cloned `main` before `--bootstrap` deploys these schedules live.

> **Sat-skip removed 2026-07-02, uniform 3x/day/7-days, no exceptions.** The
> former `…-0700-sunfri` rule (`cron(0 7 ? * SUN-FRI *)`) skipped Saturday to
> "avoid colliding with the 09:00-UTC Crucible Saturday pipeline" — a rationale
> carried verbatim from the original `backlog-groom.yml` GHA cron comment with
> no incident or postmortem behind it. Investigated and found no real
> contention: the groom draws from the Claude **Max-plan** OAuth token
> (`CLAUDE_CODE_OAUTH_TOKEN`); the weekly SF's Research/Predictor agents call
> the Anthropic API directly via a separate pay-as-you-go `ANTHROPIC_API_KEY` —
> disjoint quota pools. EC2 spot capacity is also disjoint: the groom uses
> `t3/t3a/t2.medium`; the weekly SF's data/RAG/training stages use
> `c5/m5/c6i/c5a.large` and `r5/r5a/r6i/m5.large`. Renamed `…-0700-sunfri` →
> `…-0700-daily`; `deploy.sh --bootstrap` creates the new rule and PRUNES the
> old one automatically (§ prune reconciliation). This also means the groom
> cadence no longer needs to track which day the weekly SF lands on (e.g. the
> holiday-aware Friday shift, `weekly-schedule-adjuster` #578) — it's simply
> uniform every day now.

EventBridge Scheduler cron uses 6 fields `cron(min hour day-of-month month
day-of-week year)` with `?` for an unspecified day field.

## Schedule-input routing

Each schedule passes a JSON input, e.g. `{"run_mode": "full", "model":
"claude-sonnet-5", "issue_filter": "high-only", "pr_budget": 100, "schedule": "0 1 * * *"}`.
The dedicated high-only schedule alone carries `pr_budget: 100` (config#1769) — forwarded as
`GROOM_PR_BUDGET` on the spot box; Haiku/mid-Sonnet stay at the bootstrap default (50).
The Lambda resolves each field (unknown/missing values degrade to a safe
default — `full` / `claude-sonnet-5` / `default`) and exports `GROOM_MODEL` +
`GROOM_ISSUE_FILTER` in the SSM bootstrap prelude ahead of invoking
`groom_spot_bootstrap.sh --mode <run_mode>`, which forwards them into
`scripts/groom_run.sh` → `scripts/groom_driver.py` (`--issue-filter` selects
the mid default queue vs. the dedicated `complexity:high`-only queue; `--model`
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
- **Pre-boot pace gate (2026-07-04; ceiling wired to the SSoT config-I2461,
  2026-07-14)**: before launching the spot box, compares reset-aligned weekly
  Claude usage (WET) against how much of the current weekly window has
  elapsed (`krepis.usage_pacing.pace_check` — same gate
  `alpha-engine-config/scripts/groom_budget.py` runs on-box); a launch running
  ahead of pace is skipped entirely (`launched: false, reason: "pace_gate_skip"`,
  routed through the existing `CheckLaunched`→`GroomSkipped` Step Function
  branch, no SF changes needed) **and sends its own Telegram ping** — the only
  place a pre-boot skip is ever visible, since a run that never boots has no
  on-box `groom_run.sh` to notify the way the on-box budget-gate skip does.
  The ceiling/anchor are read LIVE from `s3://alpha-engine-research/config/
  usage_pacing.json` (`_load_pacing_config()`) — the SAME SSoT `groom_budget.py`
  and the `usage-pace-alert` Lambda already read, recalibrated daily by
  `alpha-engine-config/scripts/record_usage_pacing_sample.py`. `GROOM_WEEKLY_
  WET_CEILING` / the `WEEKLY_RESET_ANCHOR` constant are FALLBACK ONLY, used
  iff that S3 read fails (previously this Lambda was permanently pinned to
  the 850M literal regardless of recalibration — config-I2461 — which
  false-tripped the gate on every trigger once the SSoT was recalibrated
  upward). Fail-safe on any S3/read error — never blocks a scheduled groom
  (and never pings on a fail-safe pass-through either). `GROOM_PACE_GATE_
  ENABLED=false` disables just this gate (independent of
  `GROOM_DISPATCH_ENABLED`); `CCUSAGE_BUCKET` / `PACING_CONFIG_KEY` override
  the S3 location.
- **Narrow IAM**: the box reads its own run secrets from SSM via its own
  instance profile — this Lambda needs none of those. Its own IAM grants are
  EC2 launch/terminate, `iam:PassRole` for the executor role, SSM
  send-command/describe, its own log group, read-only S3 access to
  `claude_code_usage/*` in `alpha-engine-research` for the pace gate, and
  (2026-07-04) `ssm:GetParameter` on just the two Telegram secret params for
  the pace-gate-skip ping (mirrors `sf-telegram-notifier`'s exact IAM
  pattern). The Scheduler execution role can `lambda:InvokeFunction` this function
  only.

## Deploy

**Code: auto-deploys on merge to main** via
`.github/workflows/deploy-scheduled-groom-dispatcher.yml` (path-filtered to
this directory) — no operator action needed for a code-only PR.

**Schedule / IAM: still operator-managed, outside CloudFormation** — the CI
OIDC role deliberately cannot create/modify IAM roles (fleet-wide policy, see
`deploy.sh`'s header). Run by hand:

```bash
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh --bootstrap  # roles + lambda + schedules (creates/updates from SCHED_NAMES; prunes orphaned rules)
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh              # update code only (same command CI runs)
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh --dry-run    # preview
bash infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh --smoke      # ⚠ fires a REAL groom run
```

A PR that touches `SCHED_NAMES`/`SCHED_CRONS`/`SCHED_INPUTS`, `iam-policy.json`,
`step_function_groom.json`, or `sf-execution-iam-policy.json` has **zero live
effect on the schedule/IAM** once merged until an operator runs `--bootstrap`
— CI will still auto-deploy the CODE, but the cron/IAM change itself sits
inert until then. `--smoke` invokes the Lambda with a synthetic schedule event
and (since `GROOM_DISPATCH_ENABLED` defaults on) **triggers an actual groom
run** — use intentionally.

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
