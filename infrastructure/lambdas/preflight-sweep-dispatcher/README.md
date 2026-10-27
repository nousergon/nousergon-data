# alpha-engine-preflight-sweep-dispatcher

Daily trigger for the all-stage `--preflight-only` sweep
(alpha-engine-config-I7249).

## Why this exists

Every real execution of `ne-weekly-freshness-pipeline` since 2026-08-10 has
failed — sixteen consecutively — each with a **different** root cause: a
missing `predictor.yaml`, `backtest.py` rc=1, an SSM `Undeliverable` from a
spot reclaim, DataPhase1 rc=1, rc=137 OOM, rc=75, and `No module named
'nousergon_lib'` raised inside the preflight Lambda itself. Every one of
these happened **on the spot box, after boot, at first use** — the substrate
the existing `weekly-preflight` Lambda structurally cannot observe, because
it runs before any box exists. On 2026-08-10 alone the operator burned nine
reruns without converging; `sf-pipeline-policy` §2.5's target is ONE.

The dry instruments already exist (`--preflight-only` on every stage
launcher, `$.preflight_args` threaded through the SF definition), but their
only whole-pipeline caller was the Friday shell-run, which drives them
through the SF's fail-fast chain and therefore surfaces one root cause per
execution. This dispatcher drives them as a non-short-circuiting SUITE
instead, daily, so a broken substrate precondition is found in minutes for
one spot-hour rather than over days of live weekly runs.

Cadence is declared in ONE file, `infrastructure/preflight_sweep_cadence.json`
(`sf-pipeline-policy` §2.6) — the EventBridge schedule and the deadman
alarm's evaluation window both derive from it (the alarm itself is codified in
`nous-ergon-ops/infrastructure/cloudwatch/alarms/alpha-engine-preflight-sweep-no-run.json`
since alpha-engine-config-I9009), and the Lambda itself
re-reads it at invoke time so a cadence of `off` reports itself as a declared
skip rather than as a dead component.

## Reuse decision — invoke, don't re-provision

This Lambda does **not** launch a box of its own. It invokes
`alpha-engine-weekly-freshness-spot-dispatcher`, the Lambda that already
builds the weekly pipeline's shared launcher box, and then sends its own SSM
command to the instance that comes back. This was chosen over writing a
second ~400-line spot-provisioning path for three reasons:

- The sweep runs on a box built by the SAME bootstrap the real weekly run
  uses — same clone set, same venv, same paths, same `config.yaml` symlink.
  The bootstrap is itself a measured failure source, so it has to be part of
  what is swept, not engineered around.
- Changes to how that box is provisioned are inherited automatically.
  `nousergon-data-PR1343` (launch the shared launcher box ON-DEMAND from the
  start, config-I7120) lands in the sweep with no edit here.
- One launcher-box implementation in the fleet, not two
  (`policy-shared-code`).

## Contract

Invoked on the CFN-owned daily schedule with `{}`. Returns:

```json
{ "dispatched": true, "declared_skip": false, "reason": "...", "cadence": "daily", "run_id": "preflight-sweep-...", "instance_id": "i-...", "market": "spot", "bootstrap_command_id": "...", "command_id": "..." }
```

A declared skip (kill-switch off, cadence `off`, or the pre-spend guard
failing) returns `dispatched: false` with a `reason` and never raises. Any
failure past the guards RAISES `SweepDispatchError` — and first writes the
console's `_preflight_sweep/latest.json` "could not measure" row, so silence
on the console cannot be produced by this Lambda failing.

## Rollout order

1. `bash infrastructure/lambdas/preflight-sweep-dispatcher/deploy.sh --bootstrap`
   (operator-only — creates the IAM role, the Lambda function, and the
   `events.amazonaws.com` invoke permission scoped to the rule ARN below).
2. Merge the PR that ships `infrastructure/preflight_sweep.py` /
   `preflight_sweep.sh` / `preflight_sweep_cadence.json` and the CFN template's
   `PreflightSweepDailyTrigger` rule (`alpha-engine-preflight-sweep-daily`,
   `arn:aws:events:us-east-1:711398986525:rule/alpha-engine-preflight-sweep-daily`)
   — `deploy-infrastructure.yml` restamps the CFN stack on every merge to
   main, no path filter, so this is the single merge-button action that
   activates dispatch. Until that code is on `main`, the handler's pre-spend
   guard (`sweep_code_is_deployed`) returns a declared skip on every
   scheduled fire and nothing is spent.
3. No separate validation step is required before the schedule fires for
   real: the pre-spend guard is itself the safety net — a scheduled fire
   against undeployed sweep code costs nothing. An operator wanting to
   validate sooner can `--smoke` (below) once step 2 has merged.

The Lambda function and its IAM MUST exist (step 1) before step 2 merges —
the CFN rule's target is this function's ARN, and a rule with no permission
to invoke its target fails silently on the CloudWatch Events side (the
`add-permission` grant is what step 1 provides; a rule that exists before
that grant simply never successfully invokes until it is applied).

## Kill switch

`PREFLIGHT_SWEEP_DISPATCH_ENABLED` (default `true`). Set to `false` to stop
dispatch without touching the CFN-owned schedule — the handler checks this
before doing anything else, reports a declared skip with its own metric
(`PreflightSweepDispatchSkipped`), and never falls open. `deploy.sh`'s
flagless (CI auto-deploy) path reads the LIVE value and preserves it across a
code-only redeploy; only `--bootstrap` sets the safe first-deployment
default.

Independently, `infrastructure/preflight_sweep_cadence.json`'s
`sweep_cadence: "off"` is a declared-off day (a ruling, not tuning — see that
file's `_meaning.off`), distinct from the kill switch: it reports
`PreflightSweepDeclaredOff` instead.

## Cost

Roughly **$0.20/night**: one shared launcher spot box (the same one the
weekly pipeline pays for on its own cadence, amortized) running for up to
`PREFLIGHT_SWEEP_TIMEOUT_SECONDS` (5400s / 90min ceiling) plus up to sixteen
short-lived nested preflight spots, one per sweepable stage, each a boot +
deps + smoke of a few minutes. `--smoke` fires this for real and warns before
doing so.

## IAM

`iam-policy.json` — least privilege, one Sid per capability:

- **Logs**: the standard Lambda trio, scoped to this function's own log
  group.
- **InvokeWeeklyLauncherBoxDispatcherOnly**: `lambda:InvokeFunction` on
  `alpha-engine-weekly-freshness-spot-dispatcher` ONLY — this Lambda has no
  `ec2:RunInstances` of its own; provisioning is entirely delegated (see
  Reuse decision above).
- **SendSweepCommandViaRunShellScript**: `ssm:SendCommand` scoped to the
  `AWS-RunShellScript` document ARN and to `ec2:instance/*` — both resources
  participate in every `SendCommand` call and IAM must allow the action
  against each.
- **RetagWatchdogDeadlineOnSweepBox**: `ec2:CreateTags`, unconditioned on
  `ec2:instance/*`. A `Condition` keyed on `ec2:ResourceTag/pipeline_role`
  cannot be correctly expressed here: this is the FIRST call that stamps
  `pipeline_role=preflight-sweep` on the box (the launcher dispatcher tags it
  `Name=alpha-engine-weekly-freshness-spot`, not `pipeline_role`), and an
  `ec2:ResourceTag` condition evaluates tags already present on the
  resource — a condition requiring the tag this call is about to set would
  simply deny the call. Scoped to `instance/*` and documented here instead of
  writing a condition that has not been verified to work.
- **TerminateSweepBoxOnSendCommandFailure**: `ec2:TerminateInstances`, scoped
  with `Condition: StringEquals ec2:ResourceTag/pipeline_role =
  preflight-sweep`. This one IS correctly expressible: by the time
  `_terminate()` can run (only reached after `_retag_watchdog_deadline()` has
  already stamped that tag on the same instance, earlier in the same
  invocation), the tag is guaranteed present.
- **PutMetricDataUnderAlphaEngineNamespaceOnly**: `cloudwatch:PutMetricData`
  on `*` (the action is not resource-scopable), `Condition: StringEquals
  cloudwatch:namespace = AlphaEngine`.
- **WriteConsoleUnmeasuredRow**: `s3:PutObject` on
  `arn:aws:s3:::alpha-engine-research/_preflight_sweep/*` — the console's
  freshness view (`report.json` + `latest.json`).

## Deployment

Managed **outside CloudFormation** for the Lambda + IAM half, same as every
sibling dispatcher — keeps the `github-actions-lambda-deploy` OIDC role's
blast radius narrow (it deliberately lacks `iam:CreateRole` /
`iam:PutRolePolicy`). The EventBridge schedule half IS CloudFormation-owned
(see "Why this exists" and "Rollout order" above) — that is the one point of
departure from the sibling pattern, and it exists because the sweep's
schedule is a declared, tested parameter
(`tests/test_preflight_sweep_cadence.py` asserts the CFN template and
`preflight_sweep_cadence.json` cannot disagree) rather than an
operator-applied rule.

Code updates after bootstrap auto-deploy on merge via
`.github/workflows/deploy-preflight-sweep-dispatcher.yml` (path-filtered,
flagless `deploy.sh` run).

First-time bootstrap (operator-only):

```
bash infrastructure/lambdas/preflight-sweep-dispatcher/deploy.sh --bootstrap
```
