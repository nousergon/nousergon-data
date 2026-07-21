# alpha-engine-weekly-freshness-spot-dispatcher

Launches the **Saturday weekly pipeline's launcher box** on a dedicated
ephemeral EC2 **spot** instead of the always-on dashboard box
(`i-09b539c844515d549` — 12 live services, persistent, stateful).
config#2248: a full disk on that box killed the entire
`ne-weekly-freshness-pipeline` run — it was a structural single point of
failure for the whole SF, purely in its role as the LAUNCHER for the 14
states that SSM-invoke onto `$.ec2_instance_id` (13 `sendCommand` states plus
`SubstrateHealthGate`'s Payload), even though those states mostly launch
their OWN nested spots (`spot_data_weekly.sh` / `spot_backtest.sh`) rather
than doing heavy compute on the launcher itself.

This **mirrors the fleet dispatcher pattern** (`nousergon_lib.spot_dispatch`
— `launch_with_fallback` + `wait_ssm_online` + `send_async_command` +
`terminate_on_failure`, the same chokepoint `alert-drain-dispatcher` /
`ci-watch-dispatcher` / `scheduled-groom-dispatcher` already use): it
launches a spot (on-demand fallback on capacity/quota exhaustion), waits for
SSM Online, fires an **async detached** `ssm send-command` that clones all
four repos the 14 downstream states' `git -C ... pull --ff-only` commands
expect at their dashboard-box paths and builds
`/home/ec2-user/alpha-engine-dashboard/.venv`, and returns immediately with
the new instance id. The Step Function's own poll loop
(`WaitForWeeklyFreshnessSpotBootstrap` / `CheckWeeklyFreshnessSpotBootstrapStatus`
/ `WeeklyFreshnessSpotBootstrapWait`, mirroring the SF's existing
`WaitForMorningEnrich`-style idiom) waits for that bootstrap command to reach
a terminal SSM status BEFORE the SF proceeds — so none of the 14 consumer
states can race an incomplete clone/venv.

## Why this box does NOT self-terminate after one workload

Unlike every sibling dispatcher's nested spot (which runs one workload then
self-terminates), THIS box is the launcher for the **whole** weekly pipeline
— it must stay up for hours (the SF's own top-level `TimeoutSeconds` is
43200s / 12h). Its bootstrap arms a `systemd-run --on-active=46800` (13h)
shutdown watchdog as an orphan-prevention BACKSTOP only, sized to clear the
SF's own 12h ceiling with an hour of headroom — nothing on the happy path
relies on it firing. `InstanceInitiatedShutdownBehavior=terminate` so a
watchdog fire actually terminates (not stops) the box.

## Contract

Input (from the weekly Step Function's `DispatchWeeklyFreshnessSpot` state):

```json
{}
```

(`force_on_demand` is reserved for a future bounded retry-on-relaunch,
mirroring the daily/EOD data-spot pattern — no current caller sets it.)

Return:

```json
{ "instance_id": "i-…", "market": "spot", "command_id": "…", "run_token": "…" }
```

**Fail-loud, no fail-open branch:** a launch/SSM error RAISES, and the SF's
`Catch` routes it into `ExtractWeeklyFreshnessSpotDispatchError` ->
`NormalizeFailureContext` -> `HandleFailure` — the SAME loud SNS-paged
failure path every other Task state in this SF uses. Unlike
`data-spot-dispatcher`'s weekday/EOD fail-open posture (a data-spot failure
there doesn't block the trading-critical path), the weekly pipeline cannot
run AT ALL without a launcher box, so there is no meaningful fail-open
branch here — a dispatch failure must halt the whole run loudly.

## Escape hatch — operator override / redrive against an existing box

The SF's `CheckSpotDispatchNeeded` Choice (inserted right after
`AcquireMutex`, before any of the 14 consumer states) skips this Lambda
entirely when `$.ec2_instance_id` is ALREADY present/non-empty on the
execution input. `scripts/weekly_sf_rerun.py`'s `rerun_input()` passthrough
(unchanged by config#2248) is exactly this path: a `watch-rerun` recovery
execution's emitted input carries the ORIGINAL failed execution's
`ec2_instance_id` (which this Lambda populated on that original run)
verbatim — so a recovery rerun reuses the same still-live launcher box
instead of paying for a second launch. `run_weekly_offcycle.sh`'s `shell`/
`full` off-cycle triggers no longer hardcode an instance id either (config#2248)
— they go through this same dispatcher on every off-cycle fire.

## IAM

- **Launcher box role:** reuses `alpha-engine-executor-profile` /
  `alpha-engine-executor-role` (home repo `alpha-engine`) — the SAME profile
  `spot_data_weekly.sh` / `spot_backtest.sh` already grant the Saturday
  nested spots, and the SAME profile `data-spot-dispatcher`'s launched box
  uses. It already carries `ec2:RunInstances`/`CreateTags`/`Describe*`,
  which is exactly what this box needs to itself launch the nested spots
  `spot_data_weekly.sh`/`spot_backtest.sh` create. No new role.
- **Security group:** the standard fleet SG (`sg-03cd3c4bd91e610b0`) — no IB
  Gateway port opened.
- **Lambda execution role:** `iam-policy.json` (ec2:RunInstances / CreateTags
  / Describe*, iam:PassRole for the executor role, ssm:SendCommand /
  DescribeInstanceInformation, ec2:TerminateInstances scoped to the
  `alpha-engine-weekly-freshness-spot` tag, sns:Publish on
  `alpha-engine-alerts` for the spot-quota-exceeded operator page).
- **SF execution role delta:** `sf-execution-iam-policy.json` +
  `../../iam/alpha-engine-step-functions-role.json` grants
  `lambda:InvokeFunction` on `alpha-engine-weekly-freshness-spot-dispatcher`
  (the `ssm:GetCommandInvocation` grant for the SF's poll loop already exists
  on that role, broadly scoped, from the data-spot-dispatcher wiring).

## Deployment

Managed **outside CloudFormation** (same as every sibling dispatcher).
**Caution: `step_function.json` auto-deploys on merge** — this Lambda + its
IAM must exist BEFORE a merge that also touches `step_function.json`, or the
SF's `DispatchWeeklyFreshnessSpot` state 404s on its very next invoke (the
exact ordering inversion that broke data-spot-dispatcher's 2026-07-08 EOD
rollout — see that Lambda's README).

First-time bootstrap (operator-only):

1. `bash infrastructure/lambdas/weekly-freshness-spot-dispatcher/deploy.sh --bootstrap`
2. `bash infrastructure/iam/apply.sh --role alpha-engine-step-functions-role`
   (applies the SF execution-role invoke grant)
3. Re-deploy `infrastructure/step_function.json` (deploy-infrastructure.sh /
   CI) so the SF's `DispatchWeeklyFreshnessSpot` state exists live.
4. Validate end to end via `bash infrastructure/run_weekly_offcycle.sh shell`
   BEFORE the next real Saturday cron fire — this cannot be validated by a
   live Saturday SF run in code review alone.

Code updates after bootstrap auto-deploy on merge via
`.github/workflows/deploy-weekly-freshness-spot-dispatcher.yml`
(path-filtered, flagless `deploy.sh` run — mirrors every sibling dispatcher).
