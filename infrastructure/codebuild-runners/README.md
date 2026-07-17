# CodeBuild-managed GitHub Actions runners (cost-I2864)

Replaces the bespoke self-hosted runner subsystem — `*-runner-dispatcher` Lambdas
(webhook → mint JIT config → launch EC2 spot → cold-bootstrap → run one job →
reap) — with **AWS CodeBuild-managed ephemeral runners**.

## Why
Cost is a wash at our volume (~60-115 runner jobs/day); the win is **maintenance
and latency**. CodeBuild deletes the dispatcher Lambda, the x86/arm AMI + bootstrap
scripts, the spot reaper, and the spot-quota-lockout failure mode (7/15 incident).
Measured telos spike: **21s end-to-end on arm64/Graviton** vs. minutes of
boot+bootstrap on the spot box. arm64-native, no host to boot, no AMI to bake.

Chosen over Fargate (still needs the webhook→dispatch glue you'd want to delete)
and over hand-rolled EC2 runners (`philips-labs/terraform-aws-github-runner` is the
mature version, but CodeBuild is more managed). Not ARC-on-EKS (~$73/mo control
plane floor > the entire saving).

## One-time account setup (already done)
- CodeConnections GitHub App connection `nousergon-codebuild` (AVAILABLE), the
  "AWS Connector for GitHub" app installed on the `nousergon` org with access to
  the runner repos. This is the auth for every project — no PATs, no expiry.

## Per-repo setup
```
./deploy_codebuild_runner.sh <repo-short-name>     # telos | vires | metron | alpha-engine-config
```
Then re-point the repo's self-hosted job:
```yaml
runs-on: codebuild-<repo>-runner-${{ github.run_id }}-${{ github.run_attempt }}
```
Per-job compute/image overrides are available via label suffixes (`image:arm-3.0`,
`instance-size:small`) — see AWS docs. Default is arm64 `BUILD_GENERAL1_SMALL`.

If a repo's runner jobs need AWS access (e.g. alpha-engine-config sweeps hitting
S3/SSM), extend that repo's `codebuild-<repo>-runner-role` inline policy — telos's
gitleaks job needs none.

## Rollout order (smallest → largest)
1. **telos** — gitleaks job (done, spike). 2. vires. 3. metron. 4. alpha-engine-config (~28 jobs).

## Decommission (per repo, AFTER its jobs run green on CodeBuild for a cycle)
Once a repo's workflows no longer reference `[self-hosted, <repo>-spot]`:
- Disable + delete the `<repo>-runner-dispatcher` Lambda (nousergon-data
  `infrastructure/lambdas/<repo>-runner-dispatcher/`) and its EventBridge/webhook
  wiring, IAM role, and log group.
- Remove any `<repo>-spot` runner registrations left in GitHub.
- The shared `spot-orphan-reaper` can be retired once ALL repos are migrated.
