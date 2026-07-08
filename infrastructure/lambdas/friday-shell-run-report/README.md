# alpha-engine-friday-shell-run-report

Consolidated pass/fail report for the **Friday shell-run** (the Friday-PM dry
preflight of the Saturday Step Function). Closes ROADMAP **L658** design point 5
— the scoped follow-on the shell-run SF spine deferred (see
`CheckShellRunNotify`'s comment in `infrastructure/step_function.json`).

## What it does

Subscribes (EventBridge) to `ne-weekly-freshness-pipeline` **terminal**
execution-status transitions (`SUCCEEDED` / `FAILED` / `TIMED_OUT` / `ABORTED`).
For executions that ran in **shell-run mode** (`shell_run: true` /
`pipeline_role: "shell-run"` — started by the sibling
`eod-success-friday-shell-trigger` Lambda), it:

1. reads the execution history (`states:GetExecutionHistory`),
2. reduces it to a per-state `{name, status: PASS|FAIL, duration_s}` summary (a
   state entered-but-never-exited = the failure point),
3. computes a readiness verdict — `GO_SATURDAY` (all states passed) or
   `HOLD_INVESTIGATE`,
4. writes `s3://alpha-engine-research/friday-shell-run/{trading_day}/report.json`,
5. publishes a structured per-state SNS summary to `alpha-engine-alerts`.

Real Saturday runs (no `shell_run`) are the intended **no-op** path
(`{"reported": false}`). The report is the Lambda's deliverable, so
GetExecutionHistory / S3 / SNS failures **raise** (EventBridge retry + the
CloudWatch Lambda-error alarm surface them).

This is the consolidated-report half of L658; the shell-run orchestration
(the `CheckShellRun` SF spine + the Friday trigger Lambda) shipped ~2026-05-29.

## Why event-driven (not an SF state)

Wiring the report as a new terminal SF state would need surgery on both the
success and failure paths. An EventBridge rule on the execution status-change
covers **both** terminal outcomes with zero SF-JSON change, mirroring the
sibling `eod-success-friday-shell-trigger` pattern.

## Deploy (operator-managed, outside CloudFormation)

```
bash infrastructure/lambdas/friday-shell-run-report/deploy.sh --bootstrap  # first-time: role + rule + lambda
bash infrastructure/lambdas/friday-shell-run-report/deploy.sh              # code-only update
bash infrastructure/lambdas/friday-shell-run-report/deploy.sh --dry-run    # show actions
```

`trading_day` is parsed from the execution name (`friday-shell-{YYYY-MM-DD}-…`),
falling back to `nousergon_lib.trading_calendar.last_closed_trading_day` on
`detail.stopDate`.
