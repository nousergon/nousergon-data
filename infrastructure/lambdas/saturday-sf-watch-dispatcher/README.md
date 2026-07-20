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
3. Sends a **distinct, SILENT** Telegram receipt **only when recovery work
   actually starts** (agent dispatched or fast-path rerun). Observe-only paths
   are recorded in the watch-log only — `sf-telegram-notifier` already alerted
   on the failure; a Fleet-SF Watch ping with no action is noise.

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

## Dispatch suppression (config#2003, closed out by config#2953)

One carve-out stops a **second** agent from being dispatched for an incident
already being handled. It still writes the watch-log event AND sends the
(SILENT) Telegram receipt — `mode: DISPATCH SUPPRESSED` — recording the
decision in `dispatch_suppressed` (never a silent skip); only the
`repository_dispatch` call itself is withheld.

1. **Same-day post-escalation repeats.** Once this pipeline's watch-log for
   today already has an `action: escalated` event (human-gated, e.g. IAM), a
   subsequent failure of the same pipeline that day suppresses **only if the
   operator has explicitly opted out**. Flag: `SF_WATCH_DISPATCH_AFTER_ESCALATION`
   (`EOD_SF_WATCH_DISPATCH_AFTER_ESCALATION` honored one more release), default
   **true** since config#2953 (Brian's 2026-07-18 shepherd ruling — the
   overseer owns the whole incident arc by default); set `false` to restore
   the pre-shepherd dispatch-suppressed-after-escalation posture.

This carve-out never suppresses the **first** failure of a pipeline/day.

**Retired (config#2953): operator-recovery-rerun name suppression.** Until
2026-07-19 an execution named after the watch's own recommended
recovery-rerun convention (`watch-rerun-<date>-<n>`) or this Lambda's own
fast-path rerun (`fast-path-rerun-<date>-<hms>`) was treated as a recovery
attempt already in progress and never re-dispatched. `watch-rerun-*` started
dispatching on 2026-07-18 (Brian's shepherd ruling, the 2026-07-08 EOD
incident config#1446/#1464 pile-on it originally guarded against is now
bounded by the config#2269 ceiling instead); `fast-path-rerun-*` was the
last suppressed-and-stalled path and config#2953 closed it the same way — a
fast-path rerun failing means the deterministic transient-signature guess
was wrong, a genuine new incident the overseer should shepherd.

## Mechanical per-cadence dispatch ceiling (config#2269)

The charter's attempt budget is honor-system (it depends on the dispatched
agent reading + enriching the watch-log). The dispatcher ALSO enforces it
mechanically: before each agent dispatch it counts prior budget-consuming
events for this (cadence, pipeline, run_date) from its own watch-log —
dispatcher-authored `action` values (`dispatch`, `fast_path_rerun`,
`reclaim_relaunch`) plus the charter's in-place outcome rewrites of dispatch
events (`fixed_merged_rerun`/`rerun`/`proposed`/`refused`/`escalated`), never
the agent-enriched `agent_attempt` field, so an agent crash can't reset the
count. At/over the ceiling the dispatch is suppressed
(`dispatch_suppressed: attempt_budget_exhausted`) and a **LOUD** Telegram
escalation pages — "the watch has given up on today; human needed" — instead
of the silent receipt.

Ceilings (env, config defaults — **not** operator flags, not preserved across
redeploys; change via PR): `SF_WATCH_MAX_DISPATCHES_SATURDAY=8`,
`SF_WATCH_MAX_DISPATCHES_WEEKDAY=2`, `SF_WATCH_MAX_DISPATCHES_EOD=2`
(the charter's Brian-ruled per-cadence budgets, 2026-07-11). The ceiling
composes with the config#2003 suppression and the charter-side budget — it
is the outermost runaway backstop, and it still applies in the default
`SF_WATCH_DISPATCH_AFTER_ESCALATION=true` shepherd posture.

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
detail.stateMachineArn:  arn:...:stateMachine:ne-weekly-freshness-pipeline
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
