# alpha-engine-sf-watch-liveness-probe

External wiring-integrity check for Fleet-SF Watch itself (`saturday-sf-watch-dispatcher`).

Fleet-SF Watch is event-driven: it only fires when a registered pipeline's
Step Function reaches a terminal FAILED/TIMED_OUT/ABORTED status via its
EventBridge rule. That means there's no natural "session" to report a
begin/end for — and, critically, **nothing notices if the watcher's own
wiring silently breaks**. That's exactly what happened on 2026-06-29: the
EventBridge rule pointed at a deleted SF ARN for an unknown period before a
real failure exposed it, and the Lambda's own `Errors` metric stayed at zero
the whole time — it simply never got invoked. A "0 errors" signal looked
healthy while the watcher was completely dead.

## How it works

Read-only, schedule-aware config-drift check:

1. The EventBridge rule (`alpha-engine-saturday-sf-watch-failed`) exists, is
   `ENABLED`, and targets the expected dispatcher Lambda.
2. The rule's registered `stateMachineArn` list matches
   `EXPECTED_PIPELINE_NAMES` (kept in lockstep with the dispatcher's own
   `PIPELINES` registry — cross-checked by a test).
3. Every expected pipeline's Step Function actually **exists** — the exact
   2026-06-29 dead-ARN class, caught directly instead of waiting for a real
   failure to expose it.
4. The target dispatcher Lambda is `Active` with a successful last code
   update.
5. **EC2-spot dispatch leg** (the LIVE repair path since the 2026-07-10 spot
   migration, config#2001/#2106): `alpha-engine-sf-watch-spot-dispatcher` and
   `alpha-engine-ci-watch-dispatcher` exist and are `Active`. Their
   kill-switch env values (`SF_WATCH_DISPATCH_ENABLED` /
   `CI_WATCH_DISPATCH_ENABLED`) are read and **reported** in the probe
   record/log — never alerted on: a deliberate operator disable is state, not
   an incident.
6. **Launch-config existence** (the deregistered-AMI silent-break guard,
   config#2265): the AMI, security group, and subnets the spot dispatcher
   would launch with — read from its DEPLOYED live env (`SF_WATCH_AMI_ID` /
   `SF_WATCH_SECURITY_GROUP` / `SF_WATCH_SUBNETS`, pinned by that Lambda's
   `deploy.sh` and lockstep-tested against its in-code defaults) — still
   exist in EC2. A missing expected env key is itself a loud finding, never a
   skip.

Silent-unless-broken (mirrors the groom-liveness-probe's philosophy, one
layer up): a clean check logs and returns, no Telegram noise. Any problem
fires a LOUD alert, deduplicated by the **content** of the problem set (a
hash, not a timestamp) — a standing issue doesn't re-ping every run, and the
alert state clears automatically once the check is clean again.

**Fail-loud:** every AWS describe/list call is the PRIMARY input — an error
code other than the specific "doesn't exist" ones being checked for RAISES,
surfacing via the Lambda `Errors` metric, alarmed by the watch-plane
CloudWatch alarms provisioned in `infrastructure/setup_watch_plane_alarms.sh`
(the dead-probe backstop) — rather than silently skipping the one check that
verifies nothing else is silently broken. The Telegram send and dedup-state
write are best-effort.

## Deploy (operator-gated, outside CloudFormation)

```
bash deploy.sh --dry-run     # show actions
bash deploy.sh --bootstrap   # first-time: create role + Lambda + 2 Scheduler rules
bash deploy.sh               # update code only
bash deploy.sh --smoke       # invoke once (read-only; pings only on a REAL problem)
```

Merging the PR has **zero** live effect until an operator runs `--bootstrap`.
The CloudWatch alarm on this function's `Errors` metric (which the fail-loud
contract assumes) is provisioned by `infrastructure/setup_watch_plane_alarms.sh`.

Cadence (UTC): `cron(45 6 * * ? *)` and `cron(45 14 * * ? *)` — offset 15 min
from the groom-liveness-probe's cadence purely to avoid simultaneous
invocation; this check isn't tied to any pipeline's own schedule.
