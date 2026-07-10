# alpha-engine-groom-liveness-probe

External heartbeat for the EC2-spot backlog groom (config#1432). The groom
self-reports its terminal state (an S3 run artifact under `groom/{date}/` +
Telegram ping) **only when the box lives long enough to run `groom_run.sh`'s
reporting trap**. This probe covers the modes that file *nothing*:

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
   (01:00 daily Opus high-only, 07:00 daily Sonnet mid-only, 19:00 daily Haiku
   low-only) and is overridable via
   `GROOM_SCHEDULE` (JSON).
2. List recent S3 run artifacts under `groom/{date}/` (`alpha-engine-research`
   bucket — `groom_driver.py::write_run_artifact`'s PRIMARY run record,
   config#1808, written by every completed run: success, floor-breach,
   crash-cascade, turn-budget-exceeded) and read each artifact's `run_start`.
3. For each trigger, assert an artifact's `run_start` fell inside its run window
   `[T, T + CEILING + MARGIN]`. A trigger with no artifact → that scheduled groom
   filed no terminal report → **LOUD Telegram alert**. Per-trigger windows mean a
   single silent death is **not masked** by the next successful run.
4. S3 dedup state (`consolidated/groom_liveness/alerted.json`) suppresses
   re-pinging a standing miss; generous lookback + dedup → tolerant to schedule /
   ceiling changes (no fragile probe-time tuning).

**Fail-loud:** the S3 artifact list/read is the PRIMARY input → an error RAISES
(surfaces via the Lambda error metric + a CW alarm); a silently-skipped check is
the exact failure this guards against. The Telegram send and the dedup-state
write are best-effort.

config#2037: this probe originally read `groom-digest`-labeled GitHub issues
instead. config#1808 retired the routine per-run issue, so the GitHub signal
went permanently empty — switched to reading the S3 artifact directly (the
signal that should have been used from the start).

## Relationship to the Fleet-SF Watch

This applies the Fleet-SF Watch philosophy (an external observer of a producer
that can't be trusted to report its own death) to the groom. The groom is **not**
a Step Function, so — unlike the three fleet pipelines — it gets no EventBridge
terminal-failure event for the existing watcher to hang off. Wrapping the groom
dispatch in a Step Function so the existing Fleet-SF Watch covers it natively is
the tracked **"SF later"** follow-up; this Lambda is the **"probe now"** half.

## Deploy

**Code: auto-deploys on merge to main** (2026-07-02) via
`.github/workflows/deploy-groom-liveness-probe.yml` — no operator action
needed for a code-only PR (e.g. adding a schedule entry to `_DEFAULT_SCHEDULE`,
per config#1571).

**Cadence / IAM: still operator-gated, outside CloudFormation** — the CI OIDC
role deliberately cannot create/modify IAM roles (fleet-wide policy). Run by
hand:

```
bash deploy.sh --dry-run     # show actions
bash deploy.sh --bootstrap   # first-time / cadence change: create role + Lambda + 2 Scheduler rules
bash deploy.sh               # update code only (same command CI runs)
bash deploy.sh --smoke       # invoke once (read-only; pings only on a REAL miss)
```

A PR that changes `SCHED_CRONS` (this probe's own invocation cadence) or
`iam-policy.json` has zero live effect until an operator runs `--bootstrap`.
Recommend wiring a CloudWatch alarm on this function's `Errors` metric (the
fail-loud contract assumes one).

Cadence (UTC): `cron(30 6 * * ? *)` and `cron(30 14 * * ? *)` — each after a
groom's worst-case completion.
