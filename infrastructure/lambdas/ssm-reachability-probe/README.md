# alpha-engine-ssm-reachability-probe

Answers one question every five minutes: **is every running fleet instance
reachable over the SSM control plane?**

`alpha-engine-config-I6198`.

## Why

SSM is the single transport by which every unattended workload receives its
work — the backlog groom and sweep, sf-watch, ci-watch, alert-drain,
think-tank, data-spot and the weekly Step Functions pipeline all dispatch by
launching a spot box and sending it an SSM command. It had no health check.

On 2026-08-03T01:50:11Z an interface VPC endpoint for
`com.amazonaws.us-east-1.ssm` was created in `vpc-566f002e` with private DNS
enabled, on a security group allowing inbound 443 only from the Cloudflare
prefix lists. Private DNS overrides `ssm.us-east-1.amazonaws.com` for **every**
instance in the VPC, so the whole fleet resolved the control plane to an ENI
that dropped its packets.

- The always-on dashboard box sat `ConnectionLost` for **2h31m**. Nothing
  alarmed.
- Eleven spot boxes failed to register and were terminated by their
  dispatchers. The only human-visible signal was three lane-DEATH pages naming
  the wrong cause (`alpha-engine-config-I6199`).

## Design decisions worth knowing

**Ground truth is `ec2:DescribeInstances`, not `ssm:DescribeInstanceInformation`.**
The latter lists only instances that have *ever* registered, so a box that
never registers — the actual 2026-08-03 failure mode — is invisible to it.

**A healthy fleet publishes an explicit `0`.** Without that datapoint,
"everything is fine" and "the probe is dead" are the same shape on the metric.

**The probe is itself observed.** `ssm_probe_heartbeat` is published on every
invocation, and the `-dead` alarm treats missing data as breaching.

**Nothing is swallowed.** Every AWS call may raise. A probe that catches its
own errors and returns cleanly would publish `0 unreachable` from a failed
scan — a false green, which is worse than no probe at all.

## Metrics — namespace `AlphaEngine/Infra`

| Metric | Shape | Meaning |
|---|---|---|
| `ssm_unreachable_instances` | aggregate, always emitted | count of running fleet instances not `Online` in SSM |
| `ssm_unreachable_instances` | dimension `name=<Name tag>` | one datapoint per affected box |
| `ssm_probe_heartbeat` | always emitted, value 1 | the probe ran |

## Alarms

- `alpha-engine-ssm-reachability-probe-unreachable` — `> 0` for two
  consecutive 5-minute periods, missing data **breaching**.
- `alpha-engine-ssm-reachability-probe-dead` — heartbeat `< 1` over three
  consecutive periods (15 minutes), missing data **breaching**.

## Deploy

Both **alarms** are applied on every merge by
`.github/workflows/deploy-ssm-reachability-probe.yml` (`--apply-alarms`), and
**code** deploys on every merge once the function exists.

First-time creation of the IAM role, function and EventBridge rule is
operator-gated, because the deploy OIDC role deliberately holds no
`iam:CreateRole` / `iam:PutRolePolicy` (fleet-wide, after four IAM-clobber
incidents). This is `pull-request-policy.md` §4.2 **form 3**: the merge emits
the exact command, and `alpha-engine-ssm-reachability-probe-dead` sits RED from
the moment the alarms land until the command has actually run. Alarm creation
is deliberately NOT part of `--bootstrap` — that would make the detector for
"bootstrap has not run" not exist until bootstrap ran.

```
AWS_PROFILE=ne-admin bash infrastructure/lambdas/ssm-reachability-probe/deploy.sh --bootstrap
```

Preview it first with `--dry-run`, and confirm afterwards with `--smoke`.

## Tuning

| Env var | Default | Notes |
|---|---|---|
| `GRACE_SECONDS` | `300` | must stay above the groom dispatcher's 180s SSM-online budget, or normal boot reads as an outage |
| `NAME_PREFIX` | `alpha-engine-` | scopes the probe to fleet boxes so an unrelated instance cannot page |
