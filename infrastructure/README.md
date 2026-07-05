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
