# alpha-engine-eod-backstop

Starts the post-close pipelines when nothing else did. Phase 2 of the trading-day-gap arc (**config#1229**); split into two machines on 2026-09-30 (**alpha-engine-config-I11269** follow-up — Brian: "run all steps in the post close sf that can run immediately after close and just set up the part that relies on the collector as a separate sf").

## The two machines

| machine | definition | does | primary trigger |
|---|---|---|---|
| `ne-postclose-trading-pipeline` | `infrastructure/step_function_eod.json` | market-hours gate, mutex, deploy-drift check, box start, executor refresh, **CaptureSnapshot** | daemon shutdown hook (~16:00 ET) |
| `ne-postclose-reconcile-pipeline` | `infrastructure/step_function_eod_reconcile.json` | collection readiness grade, precondition probe, **EODReconcile** / self-heal loop, box stop, weekly-exercise chain | **this Lambda**, on `ne-data-collection-eod`'s terminal event |

## Three entry points

| rule | schedule / pattern | handler | starts | iff |
|---|---|---|---|---|
| `alpha-engine-eod-backstop-daily` | `cron(30 22 ? * MON-FRI *)` UTC | `_handle_postclose_backstop` | post-close SF | trading day, no post-close RUNNING, **snapshot** `trades/snapshots/{day}.json` missing, not already backstopped today |
| `alpha-engine-eod-reconcile-trigger` | `ne-data-collection-eod` SUCCEEDED / FAILED / TIMED_OUT, name not `v1-eod-heal-*` | `_handle_collection_terminal` | reconcile SF | the collection started on a trading day at/after 16:00 ET and no reconcile is RUNNING; one execution per collection execution (name derived from its ARN, redelivery = `ExecutionAlreadyExists` no-op) |
| `alpha-engine-eod-reconcile-backstop-daily` | `cron(15 2 ? * TUE-SAT *)` UTC, input `{"mode":"reconcile-backstop"}` | `_handle_reconcile_backstop` | reconcile SF | trading day (ET), no reconcile or collection RUNNING, **eod_pnl row** missing, not already backstopped for that day |

Why the post-close predicate moved from the eod_pnl row to the snapshot: the row is the reconcile machine's output now, written after the 18:15 ET collection — keying the 22:30 UTC firing on it would re-dispatch the post-close machine (and a second live-IB capture) on every normal day.

Why a FAILED collection still starts the reconcile: its precondition probe and self-heal loop (which re-runs the collection as `v1-eod-heal-*`) are what act on missing data. Those heal executions are excluded from the trigger in the rule pattern **and** in code, so a heal can never start a reconcile underneath the loop that launched it.

Why 02:15 UTC for the reconcile backstop: after the collection's 18:15 ET cron plus the declared caps of every workload it runs (9600 s → 20:55 ET worst case; derived in `tests/test_v1_collection_readiness_wait.py`), in both DST regimes (22:15 EDT / 21:15 EST). It stands down while a collection is RUNNING because that collection's terminal event will start the reconcile.

A second miss is a page, not another boot: `alpha-engine-eod-snapshot-existence-check` (23:30 ET) pages independently on a missing snapshot and on a missing eod_pnl row, and every AWS error here raises into the Lambda-error alarm.

The two reconcile rules are reconciled on **every** deploy (`deploy.sh` step 2b), so the merge that ships the split ships its trigger. The post-close rule stays `--bootstrap`-created.

**Not** this Lambda's job: the **late-discovery** case (box long gone, gap found days later). That is the IBKR Flex Query `eod_pnl` backfill (config#1229).

## Fail-loud

Per `feedback_no_silent_fails`: any AWS call failure (`ec2:DescribeInstances`, `states:ListExecutions`, `states:StartExecution` other than `ExecutionAlreadyExists`, a non-404 snapshot `HeadObject`) raises so the EventBridge retry + Lambda-error CloudWatch alarm page the operator. The check must never be silently skipped on the one day it matters.

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
| `RECONCILE_SF_ARN` | `…:stateMachine:ne-postclose-reconcile-pipeline` |
| `COLLECTION_SF_ARN` | `…:stateMachine:ne-data-collection-eod` |
| `TRADING_INSTANCE_ID` | `i-018eb3307a21329bf` |
| `DASHBOARD_INSTANCE_ID` | `i-09b539c844515d549` |
| `SNS_TOPIC_ARN` | `…:alpha-engine-alerts` |

Crons: post-close `cron(30 22 ? * MON-FRI *)`, reconcile backstop `cron(15 2 ? * TUE-SAT *)`.
