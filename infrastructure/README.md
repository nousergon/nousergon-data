## Relocated to nous-ergon-ops (private)

The following operational files were relocated to the private
`cipher813/nous-ergon-ops` repo (mirrored layout) in the Phase-2 scoped
ops migration (alpha-engine-config#636, 2026-06-11). Each was verified
consumer-free (no workflow/test/SF-literal/box-runtime path) before
removal. Operators: find them at `nous-ergon-ops/<this-repo>/<same-path>`.

- `seed-ssm.sh`, `sync-secrets.sh`, `push-configs.sh`, `add-ssm-policy.sh`, `add-cron.sh` (operator one-shots)
- `backfill_reference_price_cache.sh`, `setup_eval_quality_alarm.sh` (one-shot backfill/alarm setup)
- `iam/github-actions-lambda-deploy.trust.json` (trust doc; excluded from check-drift.py's glob)
- `cloudformation/resources-to-import.json` + `cloudformation/RECOVERY.md` (RECOVERY.md completes its Phase-1 move)
