# alpha-engine-overseer-liveness-probe

ONE registry-driven liveness probe for the whole fleet watch plane
(alpha-engine-config-I2831, epic I2821). It iterates
`infrastructure/overseer/playbooks.yaml` and runs each playbook's declared
`liveness` checks plus the top-level `watch_plane_liveness` checks — so adding a
playbook (or a check) automatically extends coverage. Replaces the two per-probe
enumerations it consolidated:

- **sf-watch-reclaim-sweep-handler's wiring checks** (EventBridge rule / registered-SF /
  dead-state-machine / dispatcher-Lambda-health / spot launch-config drift) —
  migrated here. That Lambda is now slimmed to its reclaim/sweep **action** paths.
- **groom-liveness-probe** — fully migrated (its per-trigger run-window
  accounting is the `run_window` check type) and **deleted**.

## How it works

Read-only, silent-unless-broken. For every check declared in the registry it
runs the matching checker, aggregates all problems into ONE set, and LOUD-pings
Telegram only when the problem set is NEW or CHANGED (content-hash dedup — a
standing problem doesn't re-ping, and the alert clears automatically when
everything is clean again).

Check types (contract in `playbooks.schema.json`, a discriminated union on `type`):

| type | asserts |
|---|---|
| `eventbridge_rule` | rule exists / ENABLED / targets the expected Lambda **or** SQS queue (custom bus supported) / its registered `stateMachineArn` list matches |
| `state_machines_exist` | each named Step Function actually exists (the 2026-06-29 dead-ARN class) |
| `lambda_active` | function `Active` + `LastUpdateStatus` `Successful`; optional kill-switch **reported** (never alerted) + optional launch-config (AMI/SG/subnet) existence |
| `run_window` | per mature expected trigger (fixed-cron **union** the dispatcher decision log), an S3 run artifact's `run_start` landed in `[T, T+ceiling+margin]` |
| `sqs_queue_exists` | intake queue (+ optional DLQ) exists |

**Kill-switch REPORTED, never alerted:** a deliberate operator disable
(`SF_WATCH_DISPATCH_ENABLED` etc.) is state, not an incident — it is logged and
attached to any alert's context, never a finding on its own.

**Fail-loud (CLAUDE.md no-silent-fails):** every AWS describe/list is a PRIMARY
input — an *unexpected* API error (anything other than the specific "does not
exist" codes each check looks for) RAISES, so a broken probe surfaces via the
Lambda `Errors` metric, alarmed by the watch-plane backstop alarms in
`infrastructure/setup_watch_plane_alarms.sh`. A malformed registry, or a check
`type` the probe doesn't know, also raises (a packaging/config bug, never a
silent skip). The Telegram send + dedup-state write are best-effort.

## What is NOT here

The sf-watch **reclaim-checker** (config#2270, EC2-event-triggered bounded
relaunch) and **disabled-window sweep** (config#2257) are ACTION paths with 45
pinned behavioral tests; they stay in the slimmed `sf-watch-reclaim-sweep-handler`. A
follow-up tracks their eventual migration. This probe never mutates fleet state.

## Deploy (operator-gated, outside CloudFormation)

**Code + registry: auto-deploy on merge to main** via
`.github/workflows/deploy-overseer-liveness-probe.yml` (the registry
`playbooks.yaml` is bundled into the zip, so a registry edit deploys through the
normal code path).

**Cadence / IAM: operator-gated** — the CI OIDC role cannot create IAM roles or
Scheduler rules. First-time / cadence change:

```
bash deploy.sh --dry-run     # show actions
bash deploy.sh --bootstrap   # first-time: role + Lambda + 2 Scheduler rules
bash deploy.sh               # update code only (same command CI runs)
bash deploy.sh --smoke       # invoke once (read-only; pings only on a REAL problem)
```

Merging the PR has **zero** live effect until an operator runs `--bootstrap`.
The CloudWatch alarm on this function's `Errors` metric (which the fail-loud
contract assumes) is provisioned by `infrastructure/setup_watch_plane_alarms.sh`.

Cadence (UTC): `cron(50 6 * * ? *)` and `cron(50 14 * * ? *)` — offset from the
slimmed sf-watch probe's sweep cadence purely to avoid simultaneous invocation;
the `run_window` maturity gate makes exact probe time non-critical.

## Post-merge operator cleanup (one-time)

The old `groom-liveness-probe` Lambda + its role + 2 Scheduler rules are no
longer deployed by any workflow (the dir is deleted). Tear them down by hand:

```
aws lambda delete-function --function-name alpha-engine-groom-liveness-probe
aws scheduler delete-schedule --name alpha-engine-groom-liveness-0630-daily
aws scheduler delete-schedule --name alpha-engine-groom-liveness-1430-daily
# then the role: alpha-engine-groom-liveness-probe-role (+ scheduler role)
```
