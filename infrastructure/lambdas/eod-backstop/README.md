# alpha-engine-eod-backstop

Same-day backstop trigger for the EOD Step Function (`ne-postclose-trading-pipeline`). Phase 2 of the trading-day-gap arc (**config#1229**).

## Why

The EOD SF's **only** normal trigger is the trading daemon's shutdown hook (`daemon.py` finally block) — a deliberate "no-backstop design". If the daemon dies before that hook (crash, SSM `RunDaemon` step killed, daemon never starts), the EOD SF never fires: no `PostMarketData`, no `CaptureSnapshot`, and — the load-bearing failure — **no `eod_pnl` row for the day**. The next day's EOD reconcile then has no adjacent prior-day NAV baseline, and the headline daily return/alpha span multiple sessions (the 2026-06-24 gap → RGEN +14.92% class of bug; see config#1228 for the per-position fix and the executor's gap-aware reconcile, crucible-executor#280).

## What it does

EventBridge fires it **22:30 UTC MON-FRI** (well after the daemon's nominal ~20:15 UTC EOD). It starts the EOD SF **iff both**:

1. **the trading box is still RUNNING** — the daemon never shut it down, so EOD never fired; and `CaptureSnapshot` needs a live IB session, which exists only while the box is up. This is therefore a **same-day-only** recovery.
2. **no EOD execution started today** — never double-run after a daemon-triggered EOD that already completed or is mid-flight.

Otherwise it is a no-op:
- box stopped → EOD already ran (and stopped it) or the box never booted (no trading → nothing to reconcile);
- not a trading day → no EOD expected.

The EOD SF's own DynamoDB mutex (`AcquireMutex`) is the concurrency backstop if a daemon-triggered EOD is racing this one.

**Not** this Lambda's job: the **late-discovery** case (box long gone, gap found days later). That is the IBKR Flex Query `eod_pnl` backfill (config#1229) — it reconstructs a past day's NAV/cash/positions from IBKR's historical statements, no live box required.

## Fail-loud

Per `feedback_no_silent_fails`: any AWS call failure (`ec2:DescribeInstances`, `states:ListExecutions`) raises so the EventBridge retry + Lambda-error CloudWatch alarm page the operator. The check must never be silently skipped on the one day it matters.

## Deploy / safe rollout

```bash
# first-time create (EventBridge rule created DISABLED)
bash infrastructure/lambdas/eod-backstop/deploy.sh --bootstrap
# code update only
bash infrastructure/lambdas/eod-backstop/deploy.sh
```

The rule ships **DISABLED** because this Lambda can start the live trading EOD pipeline. Soak it first (`--smoke` on a non-trading day or with the box down — guaranteed no-op — and review logs), then enable deliberately:

```bash
aws events enable-rule --name alpha-engine-eod-backstop-daily --region us-east-1
```

## Config

| env var | default |
|---|---|
| `EOD_SF_ARN` | `…:stateMachine:ne-postclose-trading-pipeline` |
| `TRADING_INSTANCE_ID` | `i-018eb3307a21329bf` |
| `DASHBOARD_INSTANCE_ID` | `i-09b539c844515d549` |
| `SNS_TOPIC_ARN` | `…:alpha-engine-alerts` |

Cron: `cron(30 22 ? * MON-FRI *)` (mutable via `aws events put-rule`).
