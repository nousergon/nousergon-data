# alpha-engine-sf-watch-reclaim-sweep-handler

The **action** half of the Fleet-SF Watch watchdog: the mid-run spot-reclaim
checker (config#2270) and the disabled-window dropped-failure sweep (config#2257).

> **Slimmed (alpha-engine-config-I2831):** the read-only **wiring-integrity
> checks** that used to live here (EventBridge rule / registered-SF-ARN /
> dead-state-machine / dispatcher-Lambda-health / spot launch-config drift)
> moved to the registry-driven **`overseer-liveness-probe`**, which iterates
> `infrastructure/overseer/playbooks.yaml` so the watch-plane surface is
> enumerated in ONE place. This Lambda retains ONLY the two action paths below —
> they have their own EC2-event trigger topology and 45 pinned behavioral tests,
> so they were deliberately not migrated in that pass (a follow-up tracks their
> eventual move). The Lambda name / EC2 reclaim rules / scheduler cron are
> unchanged (renaming would re-point live EventBridge targets for zero gain).

## Mid-run spot-reclaim checker (config#2270)

This Lambda is the EventBridge target for `EC2 Spot Instance Interruption
Warning` and `EC2 Instance State-change Notification` (state=terminated) — the
handler branches on event shape (the scheduled sweep payload is `{}`; the EC2
events carry `source: aws.ec2`). The EC2 events carry only an instance-id (the
rules cannot be tag-scoped), so the checker `DescribeTags` the instance:

- `Name` tag ≠ `alpha-engine-sf-watch-spot` → quiet exit (log only).
- Watch box **with** its completion marker
  (`s3://…/sf_watch/_control/completed/{cadence}-{pipeline}-{run_date}.json`) →
  clean run, exit.
- Watch box **without** a marker → died mid-repair: re-invoke
  `alpha-engine-sf-watch-spot-dispatcher` **once** (async) with the dispatch
  fields reconstructed from the discriminator tags + the newest watch-log event,
  plus `force_on_demand: "true"`. The relaunch is recorded as an
  `action: reclaim_relaunch` watch-log event **before** the invoke (the
  exactly-one bound), then a **silent** Telegram note fires.
- A **second** death for the same (cadence, pipeline, run_date), an untagged
  watch-box death, or an unreconstructable dispatch → **LOUD** escalation.

Canary-drill boxes (`run_date` tag `drill-YYYY-MM-DD`) are isolated: their deaths
never relaunch, consume the reclaim budget, or page — the missed `_canary`
heartbeat is their designed signal (config#2223).

## Disabled-window dropped-failure sweep (config#2257)

On the scheduled invocation (`{}`), the sweep checks each registered pipeline's
LATEST execution: a terminal-failed (FAILED/TIMED_OUT/ABORTED) execution with NO
covering watch-log event for its run_date — and dispatch currently ENABLED —
gets a synthesized failure event re-driven through
`alpha-engine-saturday-sf-watch-dispatcher` (which owns the watch-log write that
marks it covered, the attempt ceiling, and the suppression carve-outs). Gated on
the spot dispatcher's live `SF_WATCH_DISPATCH_ENABLED` — read directly now (it
was previously a by-product of the wiring-check spot-leg inspection that moved to
`overseer-liveness-probe`).

**Fail-loud:** every AWS describe/list is a PRIMARY input — an unexpected API
error RAISES (surfaces via the Lambda `Errors` metric + the watch-plane backstop
alarms in `infrastructure/setup_watch_plane_alarms.sh`). The Telegram send and
dedup-state write are best-effort.

## Deploy (operator-gated, outside CloudFormation)

```
bash deploy.sh --dry-run     # show actions
bash deploy.sh --bootstrap   # first-time: role + Lambda + 2 Scheduler rules (sweep) + 2 EC2 reclaim EventBridge rules (config#2270)
bash deploy.sh               # update code only
bash deploy.sh --smoke       # invoke once (read-only sweep; pings only on a REAL problem)
```

Merging the PR has **zero** live effect until an operator runs `--bootstrap`.

Cadence (UTC): `cron(45 6 * * ? *)` and `cron(45 14 * * ? *)` — the sweep runs
twice daily; the reclaim checker is event-driven, not scheduled.
