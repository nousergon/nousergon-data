# alpha-engine-weekly-schedule-adjuster

Holds the weekly research Step Function (`ne-weekly-freshness-pipeline`) to the
**day after the last NYSE trading day of the week**, so a trailing market
holiday shifts the run *earlier* (never later).

- **Normal weeks** → the run day is Saturday, and the CFN-owned
  `alpha-engine-saturday` cron fires it as always. The adjuster does nothing but
  ensure that cron is enabled + reap any spent one-shot.
- **Trailing-holiday weeks** (Good Friday; a Friday-observed July‑4 / Christmas)
  → the last trading day is Thursday, so the run shifts to **Friday**. The
  adjuster disables `alpha-engine-saturday` for that week and stands up a
  one-shot EventBridge rule (`alpha-engine-weekly-oneshot-YYYYMMDD`) that fires
  the weekly SF once on the run day with a **byte-identical weekly input**.

## Why a reconciler, not a daily gate

The research inputs are trading-day-gated — they freeze at the last close, so
running *later* in a market-closed weekend buys no freshness. Run as early as
the last trading day's data has settled (T+1): Saturday normally, Friday on a
trailing-holiday week.

Crucially the reconciler is **fail-safe**: `alpha-engine-saturday` is the
untouched baseline. If this Lambda never runs or errors, that cron stays in
whatever state it was — a normal week leaves it ENABLED, so the weekly run still
happens Saturday. **A broken adjuster degrades to the normal Saturday run, never
a missed run.** (A daily "should I run today?" gate has the opposite, dangerous
failure mode: a broken gate misses the week.) On the holiday branch the one-shot
is created *before* the Saturday cron is disabled, so a mid-run failure also
leaves Saturday firing.

## Trigger

Weekly EventBridge tick — **Wed 06:00 UTC** (`cron(0 6 ? * WED *)`), mid-week so
a holiday shift is in place days before the weekend. Idempotent: safe to run any
number of times per week.

## Deploy (operator-run, outside CloudFormation)

```bash
bash infrastructure/lambdas/weekly-schedule-adjuster/deploy.sh --dry-run    # preview
bash infrastructure/lambdas/weekly-schedule-adjuster/deploy.sh --bootstrap  # first-time: role + fn + weekly tick
bash infrastructure/lambdas/weekly-schedule-adjuster/deploy.sh              # code update only
```

After the first `--bootstrap`, verify on the next Wednesday via CloudWatch Logs
(`/aws/lambda/alpha-engine-weekly-schedule-adjuster`): a normal week logs
`acted=normal`; the next holiday week logs `acted=holiday_shift` with the
one-shot name.

## IAM (`iam-policy.json`)

- `events:EnableRule`/`DisableRule` on `alpha-engine-saturday` only.
- `events:PutRule`/`PutTargets`/`RemoveTargets`/`DeleteRule` on
  `alpha-engine-weekly-oneshot-*` only.
- `events:ListRules`/`ListTargetsByRule`/`DescribeRule` (read, reconcile).
- `iam:PassRole` on `alpha-engine-eventbridge-sfn-role` (attached to the
  one-shot's SF target), constrained to `events.amazonaws.com`.

## Relationship to `run_weekly_offcycle.sh`

This Lambda automates, on the NYSE calendar, exactly what
`run_weekly_offcycle.sh` does by hand (disable the Saturday cron + schedule a
run + restore). The offcycle script remains the manual break-glass path.

## Interaction note

`alpha-engine-saturday` is CFN-owned (`alpha-engine-orchestration.yaml`,
`State: ENABLED`). If that stack redeploys mid-holiday-week it will re-enable the
cron, which would double-fire alongside the one-shot (mutex-protected, low
probability over a holiday weekend). The next Wednesday reconcile re-asserts the
correct state regardless.
