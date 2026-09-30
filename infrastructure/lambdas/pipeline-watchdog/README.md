# alpha-engine-pipeline-watchdog

Phase 4 of the pipeline-reporting-revamp arc (ROADMAP L3050). Daily
NYSE-trading-day-aware watchdog for the 3 Alpha Engine Step Functions, plus
(alpha-engine-config#2412) a preopen schedule-buffer canary and
(alpha-engine-config#6738) the weekly-SF silence deadman.

## What it does

Cron-fires daily at 14:00 UTC (≈ 07:00 PT, well after every SF's expected
start time). For each of the 3 Step Functions, checks whether at least one
execution started in the expected window. If a check fails, publishes an
alert via `nousergon_lib.alerts.publish` to a DISTINCT SNS topic
(`alpha-engine-watchdog-alerts`, NOT `alpha-engine-alerts`) AND to Telegram
in parallel — channel independence preserved per plan doc §3.5.

| SF | Window | Watch-day condition |
|---|---|---|
| Weekday SF | 24h | TODAY is a NYSE trading day (via `nousergon_lib.trading_calendar`) |
| EOD SF     | 24h | TODAY is a NYSE trading day |
| EOD reconcile SF (`ne-postclose-reconcile-pipeline`, split out 2026-09-30) | same as EOD SF | TODAY is a NYSE trading day; also in the prior-day failed-run check |
| Saturday SF | 7d  | TODAY is Sunday (Saturday SF fires Sat 09:00 UTC; by Sun 14:00 UTC any missed firing is 24+h overdue) |

## Preopen schedule-buffer canary (alpha-engine-config#2412)

A 4th check, run alongside the 3 above whenever today is a NYSE trading day.
`WeekdayPipelineSchedule`'s trigger has been moved earlier twice after
finishing after the 06:30 AM PT market open — 06:00→05:45 PT (2026-05-19),
then 05:45→05:15 PT (2026-07-13) — both times the buffer erosion was
noticed anecdotally, days after it started. This check reads the finish
(`stopDate`) of the most recently CLOSED trading day's SUCCEEDED Weekday-SF
execution and alerts BEFORE the buffer is consumed again:

- **Hard floor** (severity=error): finish at/after **06:15 PT** (15-min
  buffer floor). A finish at/after the actual **06:30 PT** open gets a
  distinct "MISSED THE OPEN" message.
- **Early warning** (severity=warning): finish at/after **06:10 PT** but
  before the hard floor.
- **5-day trend** (severity=warning): even when today's own reading is
  quiet, a **median** over the last 5 trading days' SUCCEEDED finishes at/
  past the 06:10 PT floor fires a distinct trend alert — catches a creep
  that never individually crosses either threshold on a single day.
- **No SUCCEEDED execution** for the target day → deferred silently to the
  existing Weekday-SF liveness check (0-executions case) or the SF's own
  failure alert — never double-paged.

All thresholds are evaluated in `America/Los_Angeles` via `zoneinfo`
(DST-correct year-round; market open is a fixed 06:30 PT local-clock time).
Reuses the same `WATCHDOG_SNS_TOPIC_ARN` + Telegram fan-out as the 3 checks
above — no new channel. Does NOT filter by `pipeline_role`; it uses the
earliest-started SUCCEEDED execution per PT calendar day as the proxy for
"the scheduled run".

> **Corrected 2026-08-09 (config#6738).** This paragraph used to say the
> role filter was impossible because the exec role lacked
> `states:DescribeExecution`. That is no longer true: `iam-policy.json`
> carries a `DescribeSFExecutions` statement and the **live** role was
> measured to carry it too. Not filtering here is now a cost choice (one
> `DescribeExecution` per row of a 14-day walk), not a permission
> constraint — revisit under `alpha-engine-config-I6748`.

## Weekly-SF silence deadman (alpha-engine-config#6738)

A 5th check, run on **every** calendar day. The Saturday-SF check above asks
"did the weekly cron fire at all in 7 days". This asks the per-day question
that went unanswered on 2026-08-05/06: for **every run-slot the declaration
expects**, does a matching execution exist?

- Reads the declared cadence from SSM
  `/alpha-engine/weekly-sf/exercise-cadence` — the same parameter
  `step_function_eod_reconcile.json`'s `ReadExerciseCadence` task reads at execution
  time, so the detector's expectation is provably the launcher's behaviour.
- Derives every expected slot over a trailing 5 days and classifies each:
  `OK` · `GATED_OFF` (the declaration does not expect this slot — reported,
  never conflated with silence) · `CRITICAL` (expected, absent → pages).
- **Evaluates through YESTERDAY, not today.** Today's exercise slot is chained
  off today's post-close reconcile (evening ET), hours after the 14:00 UTC cron; evaluating
  today would page on every trading day. Same retrospective discipline as the
  EOD window and the preopen canary.
- Dedup is keyed on the **silent slot** (`role`+`day`) with a 7-day window, so
  each genuinely new silent day pages exactly once even though it stays
  visible in the look-back for four more firings.
- Slot derivation is **imported** from `scripts/weekly_sf_silence_deadman.py`
  (packaged flat into the zip by `deploy.sh`, like `flow_doctor_telegram.py`),
  so the scheduled check and the manual rerun
  (`./scripts/weekly_sf_silence_deadman.py --live`) cannot disagree.
- Deliberately **not** rebuilt on `AWS/States ExecutionsStarted`: that metric
  has no `pipeline_role` dimension, so it cannot tell a gated-off day from a
  dead one — which is why the prior CloudWatch deadman could never fire
  (config#5599; alarm deleted 2026-08-09).

**IAM (operator-gated).** The cadence read needs
`ssm:GetParameter` on that parameter — codified in `iam-policy.json`, applied
only by an operator (`deploy.sh --apply-iam`), because
`github-actions-lambda-deploy` has no `iam:PutRolePolicy` by design. Until it
is applied the check reports `checked=false` with a `degraded_reason` and
pages "DEGRADED: cannot read …" naming the exact command — **UNKNOWN, never
healthy**. `nous-ergon-ops/infrastructure/iam/check-drift.py --lambdas-root`
(daily, 09:35 UTC) is the second, automatic detector for the same gap.

## Why this exists (vs. a dumb CW alarm)

Per Phase 0 Q2 SOTA-lock, a naive `AWS/States ExecutionsStarted` alarm with
a 24h window would false-positive every weekend for Weekday + EOD. Alert
hygiene is load-bearing: a watchdog that false-positives twice every weekend
trains the operator to silence it, defeating its purpose. The
`nousergon_lib.trading_calendar` chokepoint encodes NYSE
holiday + weekend awareness so the Lambda fires cleanly only on genuine
missed executions on expected trading days.

## Channel-independence design (plan doc §3.5)

The watchdog publishes to a NEW SNS topic (`alpha-engine-watchdog-alerts`),
NOT the existing `alpha-engine-alerts` topic. Rationale: if the
operator's regular `alpha-engine-alerts` → email path silently breaks,
this watchdog's separate publish path still reaches the operator.
Telegram (delivered via the lib's dual fan-out) is the non-overlapping
second channel.

Subscribers to the watchdog topic are operator choice — email, pagerduty,
slack — without polluting the trade-decision alert channel.

## Dedup

Each per-(SF, date) alert carries a deterministic `dedup_key` and a
12-hour window so a persistent outage doesn't re-page the operator
every cron firing. Once the underlying issue is fixed and the SF runs,
the next cron firing clears the check and stops alerting.

The silence deadman is the one exception: its key is per-(role, silent day)
with a **7-day** window, deliberately longer than its 5-day look-back. A
firing-date key would re-page the same silent day on each of the next four
firings; a slot key with a window shorter than the look-back would do the
same. Both are the same defect — one page per silent slot is the contract.

## Deploy

```bash
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --bootstrap   # first-time
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --apply-iam   # re-apply iam-policy.json
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh                # code update
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --dry-run     # preview
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --smoke       # invoke once
```

Bootstrap is idempotent — re-running creates only missing resources.

**`--apply-iam` is required whenever `iam-policy.json` changes.** Flagless
`deploy.sh` (the CI auto-deploy path in
`.github/workflows/deploy-pipeline-watchdog.yml`) ships CODE ONLY — the
`github-actions-lambda-deploy` OIDC role has no `iam:PutRolePolicy` by design
(`nous-ergon-ops/infrastructure/iam/README.md`, single-writer rule). Run
`--apply-iam` from an admin profile (`AWS_PROFILE=ne-admin`).

## Subscribe email to the watchdog topic (manual, post-deploy)

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:711398986525:alpha-engine-watchdog-alerts \
  --protocol email \
  --notification-endpoint cipher813@gmail.com \
  --region us-east-1
# confirm subscription via the email link AWS sends
```

## Operational

- Cron: `cron(0 14 * * ? *)` — daily 14:00 UTC, MUTABLE via
  `aws events put-rule --schedule-expression ...` if the firing window
  needs to shift.
- Lambda timeout: 60s. `ListExecutions` paginated walks per invocation, plus
  one `DescribeExecution` per weekly-SF execution inside the deadman's 7-day
  fetch (~5-10 rows); in practice completes in < 10s.
- Logs: `/aws/lambda/alpha-engine-pipeline-watchdog` in us-east-1
  CloudWatch Logs. Every check's verdict — including the deadman's
  per-slot `OK` / `GATED_OFF` / `CRITICAL` split — is in the returned
  summary dict, logged at INFO on every firing.
- IAM: minimum-privilege per `iam-policy.json` — list + describe SF
  executions, publish to ONE SNS topic, read the Telegram/flow-doctor SSM
  parameters and the weekly-SF cadence declaration, read/write 1 S3 prefix
  (dedup markers), the flow-doctor DynamoDB store. NO
  `states:StartExecution`, NO `alpha-engine-alerts` publish.
- Trigger state: `alpha-engine-pipeline-watchdog-daily` is listed under
  `not_paused` in `infrastructure/automation_pause.json` (Brian ruling
  2026-08-09, config#6697), so `deploy.sh`'s `pause_state` resolves it to
  ENABLED. The deadman rides this trigger rather than adding a second one.

## Composes with

- `nousergon_lib.alerts` v0.24.0+ (dual-channel publish + dedup)
- `nousergon_lib.trading_calendar` v0.27.0+ (NYSE-holiday-aware gate)
- `sf-telegram-notifier` (data #275) — sibling Lambda using same
  EventBridge → Telegram delivery pattern (this Lambda uses the
  lib chokepoint instead of duplicating the Telegram code)
- `eod-success-friday-shell-trigger` (data #282) — sibling Lambda
  using identical deploy.sh + IAM + cron-Lambda structure (this is
  the mirror pattern per Q2 lock)
- Plan doc §3.5 (channel-independence design)
- ROADMAP L3050 (Phase 4 tracking line)
