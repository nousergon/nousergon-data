# eod-success-friday-shell-trigger

Subscribes to EventBridge `Step Functions Execution Status Change` events
for `ne-postclose-trading-pipeline` SUCCEEDED transitions and, when the
underlying trading_day is a Friday, starts the Saturday Step Function in
shell-run mode (`shell_run: true`).

**Replaces** the prior fixed-time cron rule `alpha-engine-friday-shell-run`
(`cron(45 20 ? * FRI *)` = 13:45 PT Friday). Disable the cron AFTER this
Lambda's first successful Friday event-driven invocation:

```bash
aws events disable-rule --name alpha-engine-friday-shell-run --region us-east-1
```

## Why event-driven over cron

The cron fires unconditionally at a fixed time. It has three known failure
modes the event-driven design eliminates:

1. **Races `StopTradingInstance`.** The cron fires at 13:45 PT Friday, but
   the EOD SF often runs past that window. The cron would try to boot the
   trading EC2 while EOD's `StopTradingInstance` was still in flight.
2. **Fires even when Friday EOD fails.** A broken upstream EOD does not
   mean we should still preflight the Saturday SF; the shell run depends
   on EOD-produced data substrate being healthy. The cron has no signal
   to check this.
3. **No way to handle late re-runs.** If Friday's EOD is fixed and
   re-triggered later that day, the cron is already gone. The
   event-driven path naturally fires whenever EOD reaches SUCCEEDED.

## trading_day binding (not wall-clock)

The handler derives `trading_day` via the canonical
`alpha_engine_lib.trading_calendar.last_closed_trading_day` helper,
passing `event.detail.stopDate` (epoch ms, UTC) as a tz-aware UTC
datetime. The lib converts to NYSE local time and walks back to the most
recent closed session.

This handles the UTC ↔ ET ↔ PT rollover failure mode that a naive
`datetime.utcnow().weekday()` check would have:

| Scenario | stopDate (UTC) | wall-clock day (UTC) | trading_day (lib) | Fires? |
| --- | --- | --- | --- | --- |
| Normal Fri 13:25 PT EOD success | Fri 20:25 UTC | Fri | Fri | ✅ |
| Fri 18:00 PT fix-and-rerun | Sat 01:00 UTC | Sat ❌ | Fri ✅ | ✅ |
| Sat 10:00 PT fix-and-rerun for Fri's data | Sat 17:00 UTC | Sat ❌ | Fri ✅ | ✅ |
| Normal Wed 13:25 PT EOD success | Wed 20:25 UTC | Wed | Wed | ❌ |

## Fail-loud semantics

Per the `feedback_wire_orphaned_producer_must_fail_loud` discipline,
every failure surface in this Lambda raises:

- Missing `detail.stopDate` on a SUCCEEDED event → `RuntimeError`
  (upstream EventBridge contract violation).
- `last_closed_trading_day` lookup failure → propagates (lib bug).
- `states:StartExecution` boto3 failure → propagates (EventBridge will
  retry; CW Lambda-error alarms page if persistent).

Non-Friday trading_day is the intended skip path and returns
`{"fired": False, "reason": "not_friday", ...}` with a structured log —
this is NOT a swallow.

## Architecture

```
EOD SF reaches SUCCEEDED
       │
       ▼
EventBridge default bus
   (aws.states / Step Functions Execution Status Change,
    filtered to ne-postclose-trading-pipeline + SUCCEEDED only)
       │
       ▼
alpha-engine-eod-success-friday-shell-trigger
       │
       ├──► last_closed_trading_day(stopDate UTC) → trading_day
       │
       └──► if trading_day.weekday() == FRI:
                states:StartExecution on ne-weekly-freshness-pipeline
                with input { ec2_instance_id, sns_topic_arn, shell_run: true }
```

## Deploy

```bash
# First-time bootstrap — creates IAM role, Lambda, EventBridge rule, permission
bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh --bootstrap

# Code-only update (default)
bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh

# Dry-run (validate + package, do not apply)
bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh --dry-run

# Smoke-test — WARNING: triggers a REAL saturday-pipeline shell run
bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh --smoke
```

Auth: uses active AWS CLI creds. Personal IAM user has enough perms;
deliberately not wired into CI to keep the OIDC role's blast radius narrow,
matching the sf-telegram-notifier / spot-orphan-reaper /
changelog-cloudwatch-mirror convention.

## IAM (inline policy)

- `logs:CreateLogGroup/Stream + PutLogEvents` on the Lambda's own log group
- `states:StartExecution` on
  `arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline`
  (single-target scope — cannot start any other SF)

## Cutover plan

1. Merge this PR.
2. `bash deploy.sh --bootstrap` to create the Lambda + EventBridge rule.
   The new rule starts firing on the next EOD SUCCEEDED transition.
3. Leave the old cron rule `alpha-engine-friday-shell-run` ENABLED through
   one successful Friday event-driven invocation (belt + suspenders for
   the first Friday).
4. After the first Friday confirms the event-driven path fired and the
   shell run completed:
   `aws events disable-rule --name alpha-engine-friday-shell-run`.
5. Drop the cron rule entirely in a follow-up commit once a second Friday
   confirms the event-driven path is the canonical trigger.
