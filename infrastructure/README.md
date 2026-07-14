## Relocated to nous-ergon-ops (private)

The following operational files were relocated to the private
`nousergon/nous-ergon-ops` repo (mirrored layout) in the Phase-2 scoped
ops migration (alpha-engine-config#636, 2026-06-11). Each was verified
consumer-free (no workflow/test/SF-literal/box-runtime path) before
removal. Operators: find them at `nous-ergon-ops/<this-repo>/<same-path>`.

- `seed-ssm.sh`, `sync-secrets.sh`, `push-configs.sh`, `add-ssm-policy.sh`, `add-cron.sh` (operator one-shots)
- `backfill_reference_price_cache.sh`, `setup_eval_quality_alarm.sh` (one-shot backfill/alarm setup)
- `iam/github-actions-lambda-deploy.trust.json` (trust doc; excluded from check-drift.py's glob)
- `cloudformation/resources-to-import.json` + `cloudformation/RECOVERY.md` (RECOVERY.md completes its Phase-1 move)

## Off-cycle weekly runs — `run_weekly_offcycle.sh`

Fire the weekly Saturday pipeline **off-schedule** (holiday weeks, schedule
shifts, ad-hoc rehearsals) without hand-building the `start-execution` JSON.
The two production triggers are:

- **Friday shell run** — event-driven via the
  `eod-success-friday-shell-trigger` Lambda (fires on Friday EOD success).
  On a **holiday Friday there is no EOD success → no shell run**, so the
  manual `shell` verb is the only way to get the rehearsal that week.
- **Saturday full run** — the `alpha-engine-saturday` EventBridge cron
  (`cron(0 9 ? * SAT *)`).

```bash
bash infrastructure/run_weekly_offcycle.sh shell      # rehearsal now (additive, safe anytime)
bash infrastructure/run_weekly_offcycle.sh full       # full weekly now + suppress next Sat cron
bash infrastructure/run_weekly_offcycle.sh full --dry-run
bash infrastructure/run_weekly_offcycle.sh status     # cron state + pending re-enable + recent runs
bash infrastructure/run_weekly_offcycle.sh restore    # re-enable Sat cron + drop pending re-enable (escape hatch)
```

**Why `full` touches the cron.** A `full` run is *not* additive — if a
scheduled Saturday fire follows it the weekly pipeline runs twice (wasted
spot $, double model-zoo rotation, artifact clobber). So `full`:

1. ensures a narrowly-scoped EventBridge Scheduler role
   (`alpha-engine-offcycle-cron-role`, `events:EnableRule` on the Saturday
   rule only),
2. registers a **one-time** re-enable schedule at the skipped Saturday
   09:30 UTC (`ActionAfterCompletion=DELETE` → self-cleans),
3. **then** disables the cron, **then** starts the execution.

Re-enable is scheduled+verified *before* the cron is disabled, so any
failure aborts with the rule still ENABLED (re-enabling an enabled rule is
a no-op). Zero manual follow-up — next week's cadence is untouched. The
input contracts are pinned by `tests/test_run_weekly_offcycle.py`.

Holiday-week recipe (e.g. Juneteenth Fri): `shell` first → wait for
SUCCEEDED → `full` (they share the t3.small box, so run sequentially).

## Non-redrivable by design — `ne-weekly-freshness-pipeline` <a name="non-redrivable"></a>

**AWS `redrive-execution` is a deterministic no-op on this state machine.**
Do not attempt it as an incident-response step; it will not retry the
failing stage (config#2156, decided 2026-07-11: "Operator decision
2026-07-11: Option A" — accepted, not a bug to be re-architected away).

**Why.** Every spot-bearing Task in `step_function.json` carries a
`Catch: States.ALL` that routes through `Extract*Error` →
`NormalizeFailureContext(Repin)` → `HandleFailure` (the per-stage SNS
failure alert) → `FailExecution`, a single shared terminal `Fail` state.
AWS's `redrive-execution` resumes from the execution's recorded failure
point — but a *caught* error counts as "handled", so the only
unsuccessful state left on the execution is `FailExecution` itself. A
`Fail` state has no logic to re-run; redriving it just re-enters
`FailExecution` and re-fails immediately (same stale input, no
intervening Task execution). Confirmed live 2026-07-10 against execution
`offcycle-shell-20260710-211603`: `ExecutionRedriven` → `ExecutionFailed`
landed 39ms apart with zero intervening state transitions.

**The real recovery mechanism: fresh execution + `skip_*` flags.** The
pipeline's `Execution.Input` accepts one boolean `skip_*` flag per
spot-bearing stage. Start a new execution with `skip_*: true` for every
stage that already completed successfully before the failure, so the
retry only re-runs the stage that actually failed (and everything after
it). This is the operational equivalent of a targeted redrive:

```
skip_weekly_run_day_gate, skip_morning_enrich, skip_data_phase1, skip_data_phase2,
skip_lib_pin_drift_check, skip_regime_substrate, skip_research, skip_rag_ingestion,
skip_rationale_clustering, skip_eval_judge, skip_evaluator, skip_post_eval,
skip_regime_retrospective_eval, skip_predictor_training, skip_predictor_backtest,
skip_portfolio_optimizer_backtest, skip_backtester, skip_parity, skip_replay_concordance,
skip_counterfactual, skip_aggregate_costs
```

e.g. an execution that failed at `MorningEnrich` retries with
`skip_weekly_run_day_gate=true` (already passed) and every other `skip_*`
left `false`/absent so the pipeline re-runs from `MorningEnrich` forward.

Caution: this is safe for the idempotent/additive `shell` (Friday
rehearsal) path. For a `full` Saturday run, stages are **not** all
additive (see `run_weekly_offcycle.sh`'s "Why `full` touches the cron"
above) — do not blanket-skip stages you have not actually confirmed
completed, and do not let a manual retry race a still-pending scheduled
Saturday fire (double spot spend / model-zoo rotation / artifact
clobber).

**Both operator-facing failure messages point back here:** the
`HandleFailure` SNS alert's `Message` and the `FailExecution` state's
`Cause` field both explain the no-op and name this doc as the recovery
reference. If you're tempted to "fix" this by wiring per-Task `Catch`
targets to distinct `Fail` states, or by making `HandleFailure` a Task
terminal node instead of a Pass-through to `Fail` — don't: neither
restores real redrive-to-the-failing-Task without dropping the in-ASL
catch-all entirely and moving failure notification to an
execution-level EventBridge rule (a substantial re-architecture, Option
B in config#2156, not pursued per the 2026-07-11 ruling).
