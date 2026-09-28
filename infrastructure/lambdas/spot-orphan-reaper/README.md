# alpha-engine-spot-orphan-reaper

Hourly backstop Lambda that terminates orphan `alpha-engine-*` tagged spot
instances whose on-box watchdog failed to arm.

## One number, zero per-workload config (config#1492)

Every alpha-engine spot box self-terminates via its own `systemd-run ... shutdown
-h now` watchdog (+ `InstanceInitiatedShutdownBehavior=terminate`). This reaper is
**only a backstop** for the box whose watchdog never installed. A backstop does
not need per-workload precision — it enforces one invariant:

> no alpha-engine spot box should ever outlive the **longest watchdog in the
> fleet** (plus a grace window).

So there is deliberately **no per-tag budget table**. Any running `alpha-engine-*`
spot older than `MAX_SPOT_BUDGET_SECONDS + GRACE_SECONDS` is terminated:

| Env var                   | Default   | Meaning                                                        |
|---------------------------|-----------|---------------------------------------------------------------|
| `MAX_SPOT_BUDGET_SECONDS` | `21600` (6h) | Longest on-box watchdog in the fleet (backlog groom).      |
| `GRACE_SECONDS`           | `1800` (30m) | Gap between a watchdog firing and the hourly scan noticing. |

Effective reap threshold = **6.5h**.

### Why no table

The previous design kept a `TAG_BUDGETS` dict mapping each launcher's tag prefix
to its `MAX_RUNTIME_SECONDS`. That dict lived in **this repo** but had to stay in
lockstep with launcher budgets defined in **other repos**. On 2026-07-01 the
groom-on-spot migration (config#1432) added `alpha-engine-groom-spot` (6h
watchdog) without adding a table row, so the reaper's 2h default killed a **live
groom mid-run at 2.5h** (config#1492).

A single global cap cannot drift out of lockstep with anything:

- **Adding a new spot workload touches only its own launcher.** The reaper needs
  no change as long as that workload's watchdog ≤ `MAX_SPOT_BUDGET_SECONDS`.
- The cap moves **only** when a workload legitimately needs a *longer* watchdog
  than any today — a rare, deliberate act — and the failure mode if forgotten is
  **loud** (the box is reaped at the cap and logged), never a silent mis-kill at a
  wrong per-workload guess.

Trade-off accepted: a genuinely orphaned *short* workload (e.g. a 30-min drift
box whose watchdog failed) lingers up to 6.5h before the backstop fires instead of
~1h. That is pennies of spot on a rare event — the correct trade for a backstop.

## Defense in depth

1. **Spot-side watchdog** (in each launcher): `systemd-run --on-active=$MAX_RUNTIME_SECONDS` fires `shutdown -h now`. With `InstanceInitiatedShutdownBehavior=terminate` this terminates the instance. Fires regardless of dispatcher state — the primary teardown.
2. **This Lambda**: hourly scan + termination for the case where the watchdog itself never installed (dispatcher SSM cancelled before the `systemd-run` step, package-manager-interrupted bootstrap, AMI issue, etc.).
3. **CloudWatch billing alarm** (`AlphaEngine-Monthly` budget, $50/month): catches anything the other two missed, signals via SNS.

## Scope: spot boxes AND launcher-tagged on-demand boxes (alpha-engine-config-I11108)

Until 2026-09-25 the scan filtered on `instance-lifecycle=spot`, so a box the
launcher fell back to on-demand for was invisible. The 2026-09-19 weekly run's
c5.large leaked ~8h while the reaper logged `Scanned 0`. The scan now runs twice
and merges the results by instance id:

| Scope | Filter (plus `running` and `tag:Name=alpha-engine-*`) |
|---|---|
| spot | `instance-lifecycle=spot` |
| on-demand | `tag:LaunchMarket=on-demand` |

`LaunchMarket` is stamped only by `nousergon_lib.spot_dispatch.launch_with_fallback`
and `_spot_relaunch.sh` (`launch --no-spot`), atomically with RunInstances. That tag
is what separates an ephemeral run box from the long-lived on-demand hosts that
share the Name prefix (`alpha-engine-dashboard`, `alpha-engine-executor`). **Never
widen the scan to `tag:Name` alone:** those hosts carry no `watchdog-deadline`, so
they would be terminated at the 6.5h fallback cap.

## A finished weekly run's box ends early (alpha-engine-config-I11569, I11108)

When a box's `execution-id` tag names an `ne-weekly-freshness-pipeline` execution
that stopped more than `FINISHED_EXECUTION_REAP_GRACE_SECONDS` (default 3600; the
old `REHEARSAL_REAP_GRACE_SECONDS` name is still read) ago, the reaper terminates
it. The reason is `rehearsal-finished` for a `rehearsal-*` execution and
`execution-finished` for any other.

Nothing reuses a finished run's box. Since config#2248 the box is dispatched
inside the execution, `ec2_instance_id` is never in a cadence input, and
`weekly_sf_rerun.py` passes through only the source execution's input, so every
watch-rerun boots a fresh box (the five latest watch-reruns, measured 2026-09-26,
all carried `ec2_instance_id: None`). The weekly definition stamps
`execution-id` on three dispatches, the freshness box, its relaunch and the
eval-judge box, and polls each to completion.

Only the state machines in `FINISHED_REAP_STATE_MACHINES` are looked up. Other
definitions' launches can outlive their execution, so an `execution-id` naming
any other state machine is ignored. Any `DescribeExecution` error, including
AccessDenied before the role carries `ReadWeeklyExecutionStatus`, keeps the box
on its own deadline.

## CloudWatch metric

`AlphaEngine/Infra/spot_orphans_terminated` (Count, sum) with a `name` dimension
(the terminated box's `Name` tag). Zero is the expected steady-state; any non-zero
value is a process-quality signal worth investigating — the most likely cause is a
launcher that shipped without arming its watchdog.

`AlphaEngine/Infra/orphan_reaper_candidates` and `orphan_reaper_terminated` (Count)
with a `market` dimension (`spot` / `on-demand`) are emitted on **every** run,
zeros included, so "nothing matched the filter" is a data point, not an absence.

## Watch-kind incomplete-reap alert (additive, generalized config#2106)

Every other tagged workload's reap path above is unchanged. For a small,
explicit table of "watch" workloads (`WATCH_KINDS` in `index.py` — currently
Fleet CI Watch and Fleet-SF Watch), this reaper ALSO checks — right before
terminating — whether the sibling run script wrote its S3 completion marker
(`s3://alpha-engine-research/{ci_watch,sf_watch}/_control/completed/<key>.json`,
written on every one of that script's exit paths, keyed on each kind's own
discriminator tags: `(repo, sha)` for CI-watch, `(cadence, pipeline, run_date)`
for SF-watch). If the marker is absent, the reap fired because the diagnose+fix
agent never reached a normal exit — something could still be unrepaired with
nobody told — so this Lambda sends one best-effort Telegram ping via
`krepis.telegram.send_message` (through the `nousergon_lib.telegram`
re-export). Fail-safe direction is deliberately the OPPOSITE of the reap
decision itself: any inability to confirm completion (a genuine 404 or an
unrelated S3 error) still fires the alert — an occasional false positive is
safer than silently missing a real incomplete run.

One shared check/notify code path serves every `WATCH_KINDS` entry (config#2106:
SF-watch was about to become a second copy-pasted `_ci_watch_*`-shaped function
pair, which is exactly the duplication class that issue exists to stop — see
`nousergon_lib.spot_dispatch` for the sibling generalization one layer down, in
the dispatcher Lambdas themselves). Adding a THIRD watch-kind later is a new
`WatchKind(...)` row in `index.py`, not a new function pair.

## Deploying

```bash
# First-time: create role, policy, Lambda, EventBridge rule + permission
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --bootstrap

# Subsequent updates — pushes code AND converges the canonical env
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh

# Smoke (flips DRY_RUN=true, invokes once, prints scan output, flips back)
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --smoke

# Dry-run the deploy itself
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --dry-run
```

Every deploy converges the function env to the canonical `PROD_ENV` defined once
in `deploy.sh` (create sets it; `update-function-code` does not touch env, so the
explicit `update-function-configuration` is what lands `MAX_SPOT_BUDGET_SECONDS`
on an already-created reaper).

Managed outside CloudFormation by deliberate choice — same rationale as the
`changelog-cloudwatch-mirror` Lambda: this function has destructive
`ec2:TerminateInstances` permission, so the `github-actions-lambda-deploy` OIDC
role's blast radius stays narrow.

## IAM

The role's inline policy (`iam-policy.json`):

- `ec2:DescribeInstances *` — global read for the scan
- `ec2:TerminateInstances` scoped to instances with `tag:Name` matching `alpha-engine-*` — defence in depth so even a buggy reaper run cannot terminate anything outside the alpha-engine tag prefix
- `cloudwatch:PutMetricData` scoped to namespace `AlphaEngine/Infra`
- `s3:GetObject` scoped to the `_control/completed/` prefix of all four watch kinds (`ci_watch`, `sf_watch`, `overseer`, `thinktank`) — the watch-kind incomplete-reap marker checks (above). A HeadObject call is authorized by `s3:GetObject`; there is no `s3:HeadObject` IAM action, and the `overseer`/`thinktank` prefixes were never granted at all, so all four checks 403'd and every reap of those kinds reported "WITHOUT completing" (alpha-engine-config-I7571)
- `ssm:GetParameter` scoped to the two Telegram secrets (`/alpha-engine/TELEGRAM_BOT_TOKEN`, `/alpha-engine/TELEGRAM_CHAT_ID`) — resolved by `krepis.secrets.get_secret` inside `send_message`
- Standard Lambda logging perms

## Changing the cap

Bump `MAX_SPOT_BUDGET_SECONDS` (in `deploy.sh`'s `PROD_ENV`/`SMOKE_ENV`) **only**
when some spot workload legitimately needs a watchdog longer than the current 6h.
It is one number in one place — never a per-workload table.
