# alpha-engine-data-spot-dispatcher

Launches the data-heavy **weekday/EOD enrich workloads** on a dedicated
ephemeral EC2 **spot** box (config#1767, Phase 2), so the always-on trading box
(`i-018eb3307a21329bf`, t3.small) no longer runs the ~30-50 min daily_closes
fetch + ArcticDB append that fills `/tmp` and competes with IB Gateway + the
executor daemon.

This **mirrors the Saturday spot pattern**: it is the SF-invokable twin of
`scheduled-groom-dispatcher` (which itself mirrors the fleet gold-standard
`infrastructure/spot_data_weekly.sh`). It launches a spot via
`nousergon_lib.ec2_spot.launch()` (on-demand fallback on capacity exhaustion OR
account-wide spot quota exhaustion, e.g. `MaxSpotInstanceCountExceeded` —
config#2698 — with an operator page on the quota case since it only clears via
a human-requested service-quota increase), waits for the SSM agent to come
Online, fires an **async detached**
`ssm send-command`, and returns immediately. The box self-terminates
(`InstanceInitiatedShutdownBehavior=terminate` + an in-bootstrap watchdog); the
invoking Step Function polls `ssm:GetCommandInvocation` to a terminal status.

## Contract

Input (from the daily/EOD Step Function `Payload`):

```json
{ "workload": "morning-enrich" }
```

`workload` ∈ `{morning-enrich, morning-arctic-append, post-market-data,
post-market-arctic-append}` — each maps to the SAME `weekly_collector.py`
invocation the old on-trading SSM states ran (M0 data contract preserved:
unchanged paths/schemas).

Return (wrapped under `data_spot`, mirroring the groom dispatcher's `groom` wrap
so the SF JSONPath is `$.<result>.Payload.data_spot.*`):

```json
{ "data_spot": { "launched": true, "instance_id": "i-…", "command_id": "…",
                 "market": "spot", "workload": "morning-enrich", "run_token": "…" } }
```

or `{ "data_spot": { "launched": false, "reason": "disabled" } }` under the
`DATA_SPOT_DISPATCH_ENABLED=false` kill-switch — the SF's
`Check…SpotLaunched → …CaptureSnapshot/ChronicGapHeal` default branch handles it
as an intentional no-op (mirrors the groom SF's `CheckLaunched → GroomSkipped`).

**Fail-loud on launch:** a launch/SSM error RAISES so the SF's `Catch` can
convert it to the **fail-open continue branch** (`ExtractDataSpotError`). The
fail-open decision lives in the SF (config#1767 deliverable #4): a data-spot
failure must NOT block daemon start (weekday) or reconcile + instance-stop (EOD).

## IAM & security group (deliverable #3)

- **Spot box role:** reuses `alpha-engine-executor-profile` /
  `alpha-engine-executor-role` — the SAME profile `spot_data_weekly.sh` grants
  the Saturday data spot, which already has ArcticDB S3 read/write for the
  enrich paths. No new role is minted; the Saturday spot role is mirrored.
- **Security group:** the standard fleet SG (`sg-03cd3c4bd91e610b0`); no IB
  Gateway port (4001/4002) is opened — the data spot only needs egress + SSM.
- **Lambda execution role:** `iam-policy.json` (ec2:RunInstances / CreateTags /
  Describe*, iam:PassRole for the executor role, ssm:SendCommand /
  DescribeInstanceInformation, ec2:TerminateInstances scoped to the
  `alpha-engine-data-spot` tag; plus sns:Publish on `alpha-engine-alerts` +
  ssm:GetParameter on the Telegram secrets + s3:GetObject/PutObject on the
  `_alerts/_dedup/*` marker prefix, config#2698, for the quota-exceeded
  operator page).
- **SF execution role delta:** `sf-execution-iam-policy.json` +
  `../../iam/alpha-engine-step-functions-role.json` grants
  `lambda:InvokeFunction` on `alpha-engine-data-spot-dispatcher*` and reuses the
  existing `ssm:GetCommandInvocation` / `ssm:DescribeInstanceInformation` grants
  for the poll loop.

## Deployment

Managed **outside CloudFormation** (same as `scheduled-groom-dispatcher`).
**Caution: the invoking SF definitions auto-deploy on merge**, so merging a
change that touches `step_function_daily.json` / `step_function_eod.json`
alongside this Lambda goes live immediately on the SF side — the Lambda + IAM
halves must exist BEFORE such a merge, or the SFs 404 on the invoke (this
exact ordering inversion broke the 2026-07-08 EOD run; the original PR #643
wrongly claimed merging had "zero live effect").

First-time bootstrap (operator-only — creates the exec role from
`iam-policy.json` + the function):

1. `bash infrastructure/lambdas/data-spot-dispatcher/deploy.sh --bootstrap`
2. `bash infrastructure/iam/apply.sh --role alpha-engine-step-functions-role`
   (applies the SF execution-role invoke grant)

Code updates after bootstrap auto-deploy on merge via
`.github/workflows/deploy-data-spot-dispatcher.yml` (path-filtered, flagless
`deploy.sh` run — code-only, mirroring the groom-dispatcher twin).
