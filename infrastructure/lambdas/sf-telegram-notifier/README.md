# sf-telegram-notifier

Fans EventBridge `Step Functions Execution Status Change` events for the
three Alpha Engine Step Functions into Telegram via the canonical
`nousergon_lib.telegram.send_message` primitive.

**Purely additive.** The existing SNS → email path on every SF
(`NotifyComplete` success + `HandleFailure` failure branches) is unchanged.
This Lambda subscribes to a separate EventBridge rule and never touches the
SF JSON definitions.

## Coverage

| SF | Source ARN suffix | Pretty label |
| --- | --- | --- |
| Saturday weekly pipeline | `ne-weekly-freshness-pipeline` | `Saturday SF` |
| Weekday daily pipeline   | `ne-preopen-trading-pipeline`  | `Weekday SF` |
| EOD post-market pipeline | `ne-postclose-trading-pipeline`      | `EOD SF` |
| EOD post-close reconcile | `ne-postclose-reconcile-pipeline`    | `Post-close Reconcile SF` |

| Status | Emoji | Push? | Extra detail |
| --- | --- | --- | --- |
| `RUNNING`   | 🚀 | silent | execution name only |
| `SUCCEEDED` | ✅ | loud   | run_date, duration, digest |
| `FAILED`    | 🔴 | loud   | run_date, duration, digest naming the last state that was doing work, `Cause:` line — the real diagnostic from the terminal Fail state's ssm-liveness-poller `detail` field when present (truncated at 1000 chars), else `error: cause` via `DescribeExecution` (best-effort, truncated at 280 chars) |
| `TIMED_OUT` | ⏰ | loud   | run_date, duration |
| `ABORTED`   | ⛔ | loud   | run_date, duration |
| `FAILED` + `Error=DegradedRun` | 🟠 rendered as **DEGRADED** | loud | same as FAILED — distinct label/emoji so a degraded EOD run does not read as either a clean SUCCEEDED or a generic crash FAILED (alpha-engine-config#5289) |

`RUNNING` is delivered silently (in-channel awareness, no phone buzz) so the
weekday SF's daily 5:45 AM PT start does not page on every trading day.

`run_date` is parsed from the execution's `input.run_date`, falling back to
the first ISO-8601 date substring in the execution name.

### EOD artifact verification (alpha-engine-config#5289)

A `ne-postclose-reconcile-pipeline` terminal rendering as SUCCEEDED or DEGRADED
(the machine that writes the eod_pnl row since the 2026-09-30 split; it was
`ne-postclose-trading-pipeline` before)
additionally verifies, via two read-only S3 checks against
`alpha-engine-research`:

1. the SF-envelope completion marker
   (`_sf_completion/ne-postclose-reconcile-pipeline/{run_date}.json`), and
2. a `trades/eod_pnl.csv` row for `run_date`.

Both present → one terse line (`Artifacts: ✓ …`). Either missing → an
expanded, loud block naming what's missing — deliberately NOT one line, since
a "SUCCESS" that did not write its ledger row reading as clean is the failure
mode this check exists to catch. See `eod_artifact_verification.py`.

## Architecture

```
SF status transition
       │
       ▼
EventBridge default bus
   (aws.states / Step Functions Execution Status Change,
    filtered to the 4 alpha-engine pipeline SF ARNs)
       │
       ▼
alpha-engine-sf-telegram-notifier  ──►  nousergon_lib.telegram.send_message
                                                │
                                                ▼
                                       Telegram bot API
                                       (alpha-engine primary bot)
```

Telegram credentials are resolved at runtime by the lib from SSM under
`/alpha-engine/TELEGRAM_BOT_TOKEN` + `/alpha-engine/TELEGRAM_CHAT_ID`,
which were provisioned for the executor `notifier.py` arc
(ROADMAP L1067, 2026-05-13). No new secret material is required.

## Deploy

```bash
# First-time bootstrap — creates IAM role, Lambda, EventBridge rule, permission
bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh --bootstrap

# Code-only update (default)
bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh

# Dry-run (validate + package, do not apply)
bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh --dry-run

# Smoke-test (invoke with a synthetic SUCCEEDED event)
bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh --smoke
```

Auth (bootstrap / `--apply-iam` / `--smoke`): uses active AWS CLI creds —
IAM role/policy changes stay operator-run, matching the spot-orphan-reaper /
changelog-cloudwatch-mirror convention. Code-only updates additionally ship
via `.github/workflows/deploy-sf-telegram-notifier.yml` on merge to `main`
(mirrors `deploy-pipeline-watchdog.yml` — code only, no `--bootstrap`/
`--apply-iam` side effects from CI).

## IAM (inline policy)

- `logs:CreateLogGroup/Stream + PutLogEvents` on the Lambda's own log group
- `ssm:GetParameter` on `/alpha-engine/TELEGRAM_BOT_TOKEN` +
  `/alpha-engine/TELEGRAM_CHAT_ID` (no other parameters)
- `states:DescribeExecution` + `states:GetExecutionHistory` on
  `arn:aws:states:…:execution:{alpha-engine-*,ne-*}:*`
- `s3:GetObject` (a HeadObject call is authorized by GetObject — there is no `s3:HeadObject` action; alpha-engine-config-I7571) on
  `alpha-engine-research/_sf_completion/ne-postclose-trading-pipeline/*` +
  `alpha-engine-research/_sf_completion/ne-postclose-reconcile-pipeline/*` +
  `alpha-engine-research/trades/eod_pnl.csv` (EOD artifact verification,
  alpha-engine-config#5289)
