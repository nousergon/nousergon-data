# alpha-engine-pipeline-watchdog

Phase 4 of the pipeline-reporting-revamp arc (ROADMAP L3050). Daily
NYSE-trading-day-aware watchdog for the 3 Alpha Engine Step Functions.

## What it does

Cron-fires daily at 14:00 UTC (≈ 07:00 PT, well after every SF's expected
start time). For each of the 3 Step Functions, checks whether at least one
execution started in the expected window. If a check fails, publishes an
alert via `alpha_engine_lib.alerts.publish` to a DISTINCT SNS topic
(`alpha-engine-watchdog-alerts`, NOT `alpha-engine-alerts`) AND to Telegram
in parallel — channel independence preserved per plan doc §3.5.

| SF | Window | Watch-day condition |
|---|---|---|
| Weekday SF | 24h | TODAY is a NYSE trading day (via `alpha_engine_lib.trading_calendar`) |
| EOD SF     | 24h | TODAY is a NYSE trading day |
| Saturday SF | 7d  | TODAY is Sunday (Saturday SF fires Sat 09:00 UTC; by Sun 14:00 UTC any missed firing is 24+h overdue) |

## Why this exists (vs. a dumb CW alarm)

Per Phase 0 Q2 SOTA-lock, a naive `AWS/States ExecutionsStarted` alarm with
a 24h window would false-positive every weekend for Weekday + EOD. Alert
hygiene is load-bearing: a watchdog that false-positives twice every weekend
trains the operator to silence it, defeating its purpose. The
`alpha_engine_lib.trading_calendar` chokepoint encodes NYSE
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

## Deploy

```bash
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --bootstrap   # first-time
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh                # code update
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --dry-run     # preview
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --smoke       # invoke once
```

Bootstrap is idempotent — re-running creates only missing resources.

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
- Lambda timeout: 60s. Three `ListExecutions` paginated walks per
  invocation; in practice completes in < 5s.
- Logs: `/aws/lambda/alpha-engine-pipeline-watchdog` in us-east-1
  CloudWatch Logs.
- IAM: minimum-privilege per `iam-policy.json` — list SF executions,
  publish to ONE SNS topic, read 2 SSM parameters (Telegram creds),
  read/write 1 S3 prefix (dedup markers). NO `states:StartExecution`,
  NO `alpha-engine-alerts` publish.

## Composes with

- `alpha_engine_lib.alerts` v0.24.0+ (dual-channel publish + dedup)
- `alpha_engine_lib.trading_calendar` v0.27.0+ (NYSE-holiday-aware gate)
- `sf-telegram-notifier` (data #275) — sibling Lambda using same
  EventBridge → Telegram delivery pattern (this Lambda uses the
  lib chokepoint instead of duplicating the Telegram code)
- `eod-success-friday-shell-trigger` (data #282) — sibling Lambda
  using identical deploy.sh + IAM + cron-Lambda structure (this is
  the mirror pattern per Q2 lock)
- Plan doc §3.5 (channel-independence design)
- ROADMAP L3050 (Phase 4 tracking line)
