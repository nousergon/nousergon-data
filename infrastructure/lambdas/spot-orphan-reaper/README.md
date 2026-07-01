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

## CloudWatch metric

`AlphaEngine/Infra/spot_orphans_terminated` (Count, sum) with a `name` dimension
(the terminated box's `Name` tag). Zero is the expected steady-state; any non-zero
value is a process-quality signal worth investigating — the most likely cause is a
launcher that shipped without arming its watchdog.

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
- Standard Lambda logging perms

## Changing the cap

Bump `MAX_SPOT_BUDGET_SECONDS` (in `deploy.sh`'s `PROD_ENV`/`SMOKE_ENV`) **only**
when some spot workload legitimately needs a watchdog longer than the current 6h.
It is one number in one place — never a per-workload table.
