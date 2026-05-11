# alpha-engine-spot-orphan-reaper

Hourly backstop Lambda that terminates orphan `alpha-engine-*` tagged spot
instances. Backs up the spot-side `systemd-run` watchdog installed by each
of the four spot launcher scripts:

| Launcher                              | Tag prefix                  | Budget (s) |
|---------------------------------------|-----------------------------|-----------:|
| `spot_data_weekly.sh`                 | `alpha-engine-data-weekly-` |       5400 |
| `spot_drift_detection.sh`             | `alpha-engine-drift-`       |       1800 |
| `spot_train.sh` (predictor)           | `alpha-engine-gbm-train-`   |       5400 |
| `spot_backtest.sh`                    | `alpha-engine-backtest-`    |       7200 |

Plus a default `7200` budget for unrecognised `alpha-engine-*` tags so a new
launcher added without updating the table fails safe.

Each tagged instance whose `LaunchTime` is older than `(budget + 1800s grace)`
is terminated. The grace buffer (default 30 minutes via `GRACE_SECONDS`)
covers the gap between launcher MAX_RUNTIME_SECONDS and the reaper's
hourly cron firing cadence — workloads that legitimately push their budget
get one extra hour before the reaper fires.

## Defense in depth

The reaper is **Layer 2** of the orphan-prevention stack. Layers:

1. **Spot-side watchdog** (in each launcher): `systemd-run --on-active=$MAX_RUNTIME_SECONDS` fires `shutdown -h now`. Combined with `InstanceInitiatedShutdownBehavior=terminate` (also set by each launcher) this terminates the instance. Fires regardless of dispatcher state.
2. **This Lambda**: hourly scan + termination for the case where the watchdog itself never installed (dispatcher SSM cancelled before reaching the `systemd-run` step, package manager interrupted bootstrap, AMI issue, etc.).
3. **CloudWatch billing alarm** (`AlphaEngine-Monthly` budget, $50/month): catches anything the other two missed and signals via SNS.

## CloudWatch metric

`AlphaEngine/Infra/spot_orphans_terminated` (Count, sum) with `tag_prefix`
dimension. Zero is the expected steady-state; any non-zero value is a
process-quality signal worth investigating (most likely cause: a launcher
shipped without the watchdog install step, or the budget table here is
stale).

## Deploying

```bash
# First-time: create role, policy, Lambda, EventBridge rule + permission
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --bootstrap

# Subsequent code-only updates
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh

# Smoke (flips DRY_RUN=true, invokes once, prints scan output, flips back)
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --smoke

# Dry-run the deploy itself
bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --dry-run
```

Managed outside CloudFormation by deliberate choice — same rationale as
the `changelog-cloudwatch-mirror` Lambda: this function has destructive
`ec2:TerminateInstances` permission so the `github-actions-lambda-deploy`
OIDC role's blast radius stays narrow.

## IAM

The role's inline policy (`iam-policy.json`):

- `ec2:DescribeInstances *` — global read for the scan
- `ec2:TerminateInstances` scoped to instances with `tag:Name` matching `alpha-engine-*` — defence in depth so even with a buggy reaper run it cannot terminate anything outside the alpha-engine tag prefix
- `cloudwatch:PutMetricData` scoped to namespace `AlphaEngine/Infra`
- Standard Lambda logging perms

## Updating budgets

When a launcher's `MAX_RUNTIME_SECONDS` default changes, update the
`TAG_BUDGETS` dict in `index.py` in the same commit. Out-of-sync values
will cause the reaper to terminate live workloads.
