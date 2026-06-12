# Wave-4 — `predictor/price_cache_slim/` S3 prefix deletion runbook

**Status: GATED — do NOT execute until the gate below passes.**

This PR removes the slim *code* (writer, `load_slim_cache` API, consumer
fallbacks). The destructive S3 prefix deletion is a separate, manual,
gated step documented here so it is reviewable and auditable (it is **not**
run by CI or any pipeline).

## Gate (all must hold before deletion)

1. **This PR merged** — slim writer gone, every consumer ArcticDB-only
   (data macro-breadth + feature-compute; backtester exit_timing #226;
   dashboard health-check retired #88).
2. **Consumer-side ArcticDB-primary soak (≥6 weeks since 2026-04-14
   cutover).** The original gate proposed reading a `WAVE4_PARITY_METRIC
   passed: true` from a Saturday SF run, but the 5/24 emission surfaced
   that the gate is asking the wrong question: ArcticDB writes
   auto-adjusted close while slim writes raw close (yfinance auto-adjust
   policy mismatch). For dividend-paying tickers (e.g. EQIX REIT at $5+
   quarterly dividends) the per-cell delta is dividend-scale, NOT
   float-precision noise, and persists at any reasonable epsilon — the
   `passed` flag in `alpha_engine_lib/reconcile.py:62-68` requires
   `n_cells_over_epsilon == 0`, which the divergence cannot satisfy.
   Both stores are correct representations under their own conventions;
   the canonical store going forward is ArcticDB's auto-adjusted.
   The actual safety case is stronger than the gate could provide: the
   migrated consumers (data #267 macro-breadth, data #268 feature-compute,
   backtester #226 exit_timing, dashboard #88 health-check) have been
   ArcticDB-primary in production since their respective merges — 6 weeks
   of weekly Saturday SF + daily inference + weekly backtest cycles, no
   correctness alerts. Before proceeding, re-confirm: no Saturday SF
   failure in the consumer paths attributable to ArcticDB-missing-data.
3. **No remaining live reader.** `spot_backtest.sh:528`'s slim `aws s3
   sync` was verified dead (predictor_backtest.py loads from ArcticDB;
   only `sector_map.json` is read from the cache dir) and is removed in
   the backtester PR4. Re-confirm no new consumer via the terminal guard
   `tests/test_wave4_slim_arctic_parity.py`.

## Procedure (paper-trading; bounded-reversible)

```
# 1. Pre-deletion byte-equal backup (Wave-5 precedent).
aws s3 cp --recursive \
  s3://alpha-engine-research/predictor/price_cache_slim/ \
  s3://alpha-engine-research/backups/price_cache_slim.pre-deletion-260525/ \
  --only-show-errors

# 2. Verify the backup is byte-equal (object count + total bytes).
aws s3 ls --recursive --summarize \
  s3://alpha-engine-research/predictor/price_cache_slim/ \
  | tail -2
aws s3 ls --recursive --summarize \
  s3://alpha-engine-research/backups/price_cache_slim.pre-deletion-260525/ \
  | tail -2
# Object count + Total Size MUST match before proceeding.

# 3. Delete the prefix.
aws s3 rm --recursive \
  s3://alpha-engine-research/predictor/price_cache_slim/ --only-show-errors

# 4. Confirm empty.
aws s3 ls s3://alpha-engine-research/predictor/price_cache_slim/ \
  | wc -l   # -> 0
```

## Rollback

`git revert` the Wave-4 PR(s) + `aws s3 cp --recursive` the backup prefix
back to `predictor/price_cache_slim/`. The slim writer resumes on the next
weekly SF. Worst case from a missed divergence in this
paper-trading context: degraded features for ~one week until noticed — no
capital or data-loss risk (per the CLAUDE.md severity posture).

## Follow-ups (cosmetic, batch after deletion)

- Dashboard architecture-page slim *labels* (`public/pages/2_Architecture.py`,
  `pages/10_Architecture.py`) — descriptive topology text only.
- Comment-only slim mentions in `builders/backfill.py`,
  `validators/price_validator.py` — historical context, harmless.
