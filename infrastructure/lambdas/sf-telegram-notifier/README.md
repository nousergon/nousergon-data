# sf-telegram-notifier

Fans EventBridge `Step Functions Execution Status Change` events for the
three Alpha Engine Step Functions into Telegram via the canonical
`alpha_engine_lib.telegram.send_message` primitive.

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

| Status | Emoji | Push? | Extra detail |
| --- | --- | --- | --- |
| `RUNNING`   | 🚀 | silent | execution name only |
| `SUCCEEDED` | ✅ | loud   | duration |
| `FAILED`    | 🔴 | loud   | duration + `error: cause` via `DescribeExecution` (best-effort, truncated at 280 chars) |
| `TIMED_OUT` | ⏰ | loud   | duration |
| `ABORTED`   | ⛔ | loud   | duration |

`RUNNING` is delivered silently (in-channel awareness, no phone buzz) so the
weekday SF's daily 5:45 AM PT start does not page on every trading day.

## Architecture

```
SF status transition
       │
       ▼
EventBridge default bus
   (aws.states / Step Functions Execution Status Change,
    filtered to the 3 alpha-engine SF ARNs)
       │
       ▼
alpha-engine-sf-telegram-notifier  ──►  alpha_engine_lib.telegram.send_message
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

Auth: uses active AWS CLI creds. Personal IAM user has enough perms;
deliberately not wired into CI to keep the OIDC role's blast radius narrow,
matching the spot-orphan-reaper / changelog-cloudwatch-mirror convention.

## IAM (inline policy)

- `logs:CreateLogGroup/Stream + PutLogEvents` on the Lambda's own log group
- `ssm:GetParameter` on `/alpha-engine/TELEGRAM_BOT_TOKEN` +
  `/alpha-engine/TELEGRAM_CHAT_ID` (no other parameters)
- `states:DescribeExecution` on `arn:aws:states:…:execution:alpha-engine-*:*`
  — only used to enrich `FAILED` events with the error+cause snippet
