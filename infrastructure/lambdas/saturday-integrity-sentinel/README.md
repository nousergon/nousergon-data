# saturday-integrity-sentinel

**Saturday-SF Watch — M4.** The independent Sat→Monday swallow safeguard for the
autonomous resilience agent. Spec: [nousergon/alpha-engine-config#1227](https://github.com/nousergon/alpha-engine-config/issues/1227).

## What it does

Monday ~12:30 UTC (15 min before the weekday SF), it reads the freshness-monitor's
pre-computed `saturday_sf` cycle-completion verdict
(`s3://alpha-engine-research/_freshness_monitor/cycle_verdict.json`) and pages a
**GO / NO-GO**: are last Saturday's critical artifacts (signals, predictor
weights manifest, constituents, training summary, …) actually present + fresh,
so it's safe to trade on them today?

- **GO** → silent heartbeat Telegram.
- **NO-GO** (cycle incomplete, OR uncertain: verdict missing / monitor stale /
  no `saturday_sf` row) → **LOUD** Telegram naming the missing/stale artifacts +
  a marker at `consolidated/saturday_integrity/{date}.json` (dashboard surface).

## Why it's the real safeguard

The agent (M2c) reports what it fixed; this sentinel does **not** trust that
report. It reads the freshness-monitor's verdict, which is derived by HEAD-ing
the artifacts in S3 directly — **independent of the agent**. A swallow (a "fix"
that left an artifact silently stale/missing while the SF went green) can fool
the agent's self-report but cannot fool an S3 probe. It also fires even when
freshness-monitor *alerting* is in OBSERVE mode (the verdict is computed
regardless of the alert gate), so the Monday GO/NO-GO is never silenced.

**Non-blocking by design** (Brian-ratified): it does not touch the weekday SF —
it pages + marks, within the Sat→Mon buffer, so a miss is caught before/at Monday
open without halting trading.

**Fail-loud on uncertainty:** for a safety check, ambiguity = NO-GO. A missing or
stale `cycle_verdict.json` pages loud rather than assuming GO. The marker write
RAISES on failure (primary); Telegram is best-effort.

## Deploy (operator; zero live effect until --bootstrap)

```
bash infrastructure/lambdas/saturday-integrity-sentinel/deploy.sh --dry-run
bash infrastructure/lambdas/saturday-integrity-sentinel/deploy.sh --bootstrap  # role + Lambda + Monday cron
bash infrastructure/lambdas/saturday-integrity-sentinel/deploy.sh              # code-only update
bash infrastructure/lambdas/saturday-integrity-sentinel/deploy.sh --smoke      # invoke now (reads live verdict)
```

## Test

```
python3 -m pytest infrastructure/lambdas/saturday-integrity-sentinel/test_handler.py -q
```

## Follow-up

A dashboard banner on the **Saturday SF Watch** page (views/37) reading the
latest `consolidated/saturday_integrity/{date}.json` marker — quick additive
surface; the loud Telegram NO-GO is the primary safeguard.
