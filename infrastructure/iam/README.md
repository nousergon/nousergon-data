# IAM policies (alpha-engine-data — orchestration infra)

Source-of-truth for inline IAM policies on the cross-cutting orchestration
roles. This repo owns these roles because the **grants are derived from
code that lives in this repo**:

- `alpha-engine-step-functions-role` — grants reflect the Lambdas the SF
  JSON invokes + the EC2 instances it sends SSM to + the trading
  instance it starts/stops. Source: `infrastructure/cloudformation/`
  + `infrastructure/deploy_step_function*.sh`.
- `alpha-engine-eventbridge-sfn-role` — grants reflect which Step
  Functions the EventBridge cron rules target. Source: same as above.
- `github-actions-lambda-deploy` — grants reflect the set of Lambdas
  any GitHub Action in any alpha-engine-* repo can deploy. Cross-cutting
  by design.

Module-specific roles live in their owning repo's `infrastructure/iam/`:

| Role | Home repo |
|---|---|
| `alpha-engine-executor-role` | `alpha-engine` |
| `alpha-engine-predictor-role` | `alpha-engine-predictor` |
| `github-actions-iam-drift-check` | `alpha-engine` (workflow-specific) |

This repo's own lambda exec roles (`infrastructure/lambdas/<name>/iam-policy.json`,
one per lambda) are a separate, module-specific layer applied by each
lambda's own `deploy.sh`, not this directory's `apply.sh`. Automated drift
detection for this layer (`check-drift.py --lambdas-only`) is not live yet —
tracked in `nousergon-data#784` (still open) — so `deploy.sh --apply-iam`
is the operator's own responsibility to re-run after editing an
`iam-policy.json`, until #784 lands. Every `deploy.sh --apply-iam`
re-applies just that lambda's inline policy (config#2825) — use it after
editing an `iam-policy.json` instead of the slower, more side-effectful
`--bootstrap`, which previously was the ONLY path that re-applied it and is
why edits routinely drifted from live until the next full bootstrap.

## Layout

Flat one-file-per-role:

```
infrastructure/iam/
├── apply.sh
├── check-drift.py
├── README.md
├── alpha-engine-step-functions-role.json
├── alpha-engine-step-functions-role.trust.json
├── alpha-engine-eventbridge-sfn-role.json
├── alpha-engine-eventbridge-sfn-role.trust.json
└── github-actions-lambda-deploy.json
```

The filename (minus `.json`) is the IAM role name; the inline policy
name on AWS is `{role-name}-policy` (enforced by `apply.sh`). A
`<role-name>.trust.json` file is that role's assume-role (trust) policy
snapshot — see "Trust policies + role creation" below.

## Usage

```bash
# Apply every inline policy + trust snapshot
./infrastructure/iam/apply.sh

# Apply one specific role (both its .json and .trust.json, whichever exist)
./infrastructure/iam/apply.sh alpha-engine-step-functions-role

# Print planned commands without executing
./infrastructure/iam/apply.sh --dry-run

# Check drift against live AWS (inline policies + trust documents)
./infrastructure/iam/check-drift.py
```

`apply.sh` calls `aws iam put-role-policy` for `*.json` (idempotent —
re-running overwrites the existing inline policy on the role) and
`aws iam update-assume-role-policy` for `*.trust.json` (also idempotent
— it writes the full trust document).

## Single-writer rule

Each codified role must have **exactly one writer** — `apply.sh` in the
home repo. Any deploy script that calls `aws iam put-role-policy` against
a codified role from anywhere else is a regression risk.

This rule is enforced by `check-no-foreign-writers.py` in the alpha-engine
repo, which scans every sibling alpha-engine-* repo on every PR + daily.
4 IAM-clobber incidents in 2 months traced to this exact pattern (PR
review missed inline `put-role-policy` blocks in alpha-engine-data deploy
scripts that competed with codified state); the static check closes that
regression class.

## Trust policies + role creation

Initial role creation stays in the deploy script that needs it
(`infrastructure/deploy_step_function.sh` for the SF + EB-SFN roles) —
`apply.sh` never creates a role, only ensures its inline policy + trust
document once the role exists.

**`*.trust.json` snapshots (config#2826).** Every role whose trust is
bootstrapped inline in a deploy script has a version-tracked
`<role-name>.trust.json` snapshot here — the single source of truth the
bootstrapping script(s) read via `file://infrastructure/iam/<role>.trust.json`
instead of keeping their own inline copy:

- `alpha-engine-step-functions-role.trust.json` — read by
  `deploy_step_function.sh`'s one-time `create-role` bootstrap.
- `alpha-engine-eventbridge-sfn-role.trust.json` — read by BOTH
  `deploy_step_function.sh`'s bootstrap AND `deploy-infrastructure.sh`'s
  step 3b idempotent re-assertion (config#2413/#2826 — this role backs a
  CFN `AWS::Scheduler::Schedule` target, so its trust must be re-verified
  on every CI deploy, not just asserted once at creation time).

`apply.sh` applies every `*.trust.json` via `update-assume-role-policy`;
`check-drift.py` diffs each against the role's live
`AssumeRolePolicyDocument`.

**`github-actions-lambda-deploy` is NOT in the set above.** Its OIDC trust
policy is provisioned entirely out-of-band (no deploy script in this repo
creates or re-asserts it — `infrastructure/deploy.sh` explicitly documents
this as a one-time manual operation) and no committed document, code path,
or readable live state on this box exists that this repo can derive it
from. A prior note in this file claimed a `github-actions-lambda-deploy.trust.json`
"reference copy" existed — it did not (`git log --all` shows no such file
was ever committed); that claim is corrected here. Codifying that role's
trust snapshot needs a human with IAM read/console access to run
`aws iam get-role --role-name github-actions-lambda-deploy --query Role.AssumeRolePolicyDocument`
and commit the actual live document as a follow-up.

## Drift detection

`.github/workflows/iam-drift-check.yml` runs `check-drift.py` on every
PR touching `infrastructure/iam/**`, daily at 09:30 UTC, and on
manual `workflow_dispatch`.

Auth: OIDC via the shared `github-actions-iam-drift-check` role (defined
in `crucible-executor`; trust policy permits both `crucible-executor` and
this repo); read-only `iam:ListRolePolicies` + `iam:GetRolePolicy` +
`iam:GetRole` scoped to the codified roles only — confirmed live
(`crucible-executor/infrastructure/iam/github-actions-iam-drift-check/iam-readonly.json`)
already grants `iam:GetRole` on both `alpha-engine-step-functions-role` and
`alpha-engine-eventbridge-sfn-role`, so the new trust-drift check needs no
additional IAM grant.

## When you add a new inline policy

1. Apply it to AWS first (e.g. via `aws iam put-role-policy ...`)
2. Save the JSON document as `<role-name>.json` in this directory
3. Commit the file with a description of why the grant was needed

If the role is module-specific rather than orchestration-shared, codify
it in the owning module's repo instead.

<!-- ci-trigger -->
