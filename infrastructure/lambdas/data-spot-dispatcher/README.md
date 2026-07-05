# alpha-engine-data-spot-dispatcher

Launches the data-heavy **weekday/EOD enrich workloads** on a dedicated
ephemeral EC2 **spot** box (config#1767, Phase 2), so the always-on trading box
(`i-018eb3307a21329bf`, t3.small) no longer runs the ~30-50 min daily_closes
fetch + ArcticDB append that fills `/tmp` and competes with IB Gateway + the
executor daemon.

This **mirrors the Saturday spot pattern**: it is the SF-invokable twin of
`scheduled-groom-dispatcher` (which itself mirrors the fleet gold-standard
`infrastructure/spot_data_weekly.sh`). It launches a spot via
`nousergon_lib.ec2_spot.launch()` (on-demand fallback on capacity exhaustion),
waits for the SSM agent to come Online, fires an **async detached**
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
  `alpha-engine-data-spot` tag).
- **SF execution role delta:** `sf-execution-iam-policy.json` +
  `../../iam/alpha-engine-step-functions-role.json` grants
  `lambda:InvokeFunction` on `alpha-engine-data-spot-dispatcher*` and reuses the
  existing `ssm:GetCommandInvocation` / `ssm:DescribeInstanceInformation` grants
  for the poll loop.

## Deployment

Managed **outside CloudFormation** (same as `scheduled-groom-dispatcher`) —
operator-deployed. Merging this PR has **zero live effect** until:

1. the Lambda + `iam-policy.json` are deployed (create the
   `alpha-engine-data-spot-dispatcher` function with the packaged `index.py` +
   `requirements.txt`, attach `iam-policy.json`);
2. the `alpha-engine-step-functions-role` policy is re-applied
   (`infrastructure/iam/apply.sh`) so the SF can invoke the new Lambda;
3. the daily + EOD Step Functions are re-deployed from the updated
   `step_function_daily.json` / `step_function_eod.json`.

This is the **live validation gate** — see the PR body for the exact steps.
