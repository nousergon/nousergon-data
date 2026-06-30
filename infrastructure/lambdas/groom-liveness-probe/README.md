# alpha-engine-groom-liveness-probe

External heartbeat for the EC2-spot backlog groom (config#1432). The groom
self-reports its terminal state (a `groom-digest` issue + Telegram ping) **only
when the box lives long enough to run `groom_run.sh`'s reporting trap**. This
probe covers the modes that file *nothing*:

- spot reclaim mid-run
- OOM / kernel panic before the trap installs
- a lost / failed SSM command
- the `scheduled-groom-dispatcher` Lambda erroring
- a broken / disabled EventBridge schedule (the 2026-06-29 dead-trigger class)

## How it works

Schedule-aware, per-trigger accounting:

1. Enumerate every scheduled groom trigger in the last `GROOM_LOOKBACK_HOURS`
   that has had `GROOM_CEILING_MIN + GROOM_MARGIN_MIN` to finish (so a still-running
   groom never false-alarms). The schedule mirrors the dispatcher's crons
   (07:00 Sun-Fri, 23:00 daily) and is overridable via `GROOM_SCHEDULE` (JSON).
2. Fetch recent `groom-digest`-labeled issues from `nousergon/alpha-engine-config`
   (success digests **and** loud-failure issues both carry the label).
3. For each trigger, assert a digest was created inside its run window
   `[T, T + CEILING + MARGIN]`. A trigger with no digest → that scheduled groom
   filed no terminal report → **LOUD Telegram alert**. Per-trigger windows mean a
   single silent death is **not masked** by the next successful run.
4. S3 dedup state (`consolidated/groom_liveness/alerted.json`) suppresses
   re-pinging a standing miss; generous lookback + dedup → tolerant to schedule /
   ceiling changes (no fragile probe-time tuning).

**Fail-loud:** the digest fetch is the PRIMARY input → a GitHub/SSM error RAISES
(surfaces via the Lambda error metric + a CW alarm); a silently-skipped check is
the exact failure this guards against. The Telegram send and the dedup-state
write are best-effort.

## Relationship to the Fleet-SF Watch

This applies the Fleet-SF Watch philosophy (an external observer of a producer
that can't be trusted to report its own death) to the groom. The groom is **not**
a Step Function, so — unlike the three fleet pipelines — it gets no EventBridge
terminal-failure event for the existing watcher to hang off. Wrapping the groom
dispatch in a Step Function so the existing Fleet-SF Watch covers it natively is
the tracked **"SF later"** follow-up; this Lambda is the **"probe now"** half.

## Deploy (operator-gated, outside CloudFormation)

```
bash deploy.sh --dry-run     # show actions
bash deploy.sh --bootstrap   # first-time: create role + Lambda + 2 Scheduler rules
bash deploy.sh               # update code only
bash deploy.sh --smoke       # invoke once (read-only; pings only on a REAL miss)
```

Merging the PR has **zero** live effect until an operator runs `--bootstrap`.
Recommend wiring a CloudWatch alarm on this function's `Errors` metric (the
fail-loud contract assumes one).

Cadence (UTC): `cron(30 6 * * ? *)` and `cron(30 14 * * ? *)` — each after a
groom's worst-case completion.
