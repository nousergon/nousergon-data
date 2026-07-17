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

## Layout

Flat one-file-per-role:

```
infrastructure/iam/
├── apply.sh
├── check-drift.py
├── README.md
├── alpha-engine-step-functions-role.json
├── alpha-engine-eventbridge-sfn-role.json
└── github-actions-lambda-deploy.json
```

The filename (minus `.json`) is the IAM role name; the inline policy
name on AWS is `{role-name}-policy` (enforced by `apply.sh`).

## Usage

```bash
# Apply every policy
./infrastructure/iam/apply.sh

# Apply one specific role
./infrastructure/iam/apply.sh alpha-engine-step-functions-role

# Print planned commands without executing
./infrastructure/iam/apply.sh --dry-run

# Check drift against live AWS
./infrastructure/iam/check-drift.py
```

`apply.sh` calls `aws iam put-role-policy`, which is idempotent —
re-running overwrites the existing inline policy on the role.

## Lambda exec-role drift coverage (config#2340 surface 3)

`check-drift.py` also covers **every lambda exec role** whose inline
permission policy is tracked as a file — i.e. each
`infrastructure/lambdas/<name>/iam-policy.json`. These policies were already
tracked and already applied by each lambda's `deploy.sh --bootstrap`; the only
missing leg was drift-check coverage.

Rather than move the files (which would churn every lambda's deploy path),
`check-drift.py` discovers each lambda's **primary role in place**: it reads the
authoritative `ROLE_NAME=`/`POLICY_NAME=` at the top of the lambda's `deploy.sh`
(the source of truth for what `put-role-policy … --policy-document
file://iam-policy.json` applies) and drift-checks `(ROLE_NAME, POLICY_NAME,
iam-policy.json)`. A tracked `iam-policy.json` whose `deploy.sh` does **not**
define both names is a **coverage gap** and fails the sweep — a policy file can
never silently escape drift-checking (the untracked-policy → outage class this
surface closes).

The live drift run needs the OIDC role `github-actions-iam-drift-check` to hold
`iam:GetRolePolicy` on these lambda roles; that Resource-list extension ships in
the paired `crucible-executor` PR and must be applied by an operator before the
scheduled check passes for the lambda roles.

### Out of scope (documented follow-up)

Some lambdas define **secondary** roles (schedulers/canaries — `SCHED_ROLE_NAME`,
`CANARY_SCHED_ROLE_NAME`, `SF_ROLE_NAME`, …) whose policies are applied from
**inline heredoc variables** (`${SCHED_INVOKE_POLICY}`), not from
`iam-policy.json` files. Those have no file to diff, so they are **not** yet
drift-covered. Lifting each inline scheduler/canary policy into a tracked file
(so `check-drift.py` covers it the same way) is the remaining surface-3
follow-up — it touches each `deploy.sh`'s apply path and needs a live deploy to
validate, so it is deliberately separated from this drift-coverage change.

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

Initial role creation is out of scope (handled inline in the deploy
scripts that need them — `infrastructure/deploy_step_function.sh` for the
SF + EB-SFN roles). `apply.sh` only manages the **inline permission
documents** on roles that already exist.

**Exception — `github-actions-lambda-deploy.trust.json` (reference copy).**
The OIDC trust policy for the shared `github-actions-lambda-deploy` role
(which repos may assume it from GitHub Actions) was historically manual /
uncodified. The `*.trust.json` file is a version-tracked snapshot of that
trust policy so additions are reviewable. `apply.sh` does NOT apply it
(it's an assume-role-policy, not an inline policy). Apply a trust-policy
change explicitly:

```
aws iam update-assume-role-policy --role-name github-actions-lambda-deploy --policy-document file://infrastructure/iam/github-actions-lambda-deploy.trust.json
```

When a new repo needs to auto-deploy, add its
`repo:nousergon/<repo>:ref:refs/heads/main` (+ `:pull_request`) entry to
the trust JSON AND add its resource ARNs to the permission JSON, then
apply both.

## Drift detection

`.github/workflows/iam-drift-check.yml` runs `check-drift.py` on every
PR touching `infrastructure/iam/**`, daily at 09:30 UTC, and on
manual `workflow_dispatch`.

Auth: OIDC via the shared `github-actions-iam-drift-check` role (defined
in alpha-engine; trust policy permits both alpha-engine and
alpha-engine-data); read-only `iam:ListRolePolicies` + `iam:GetRolePolicy`
scoped to the codified roles only.

## When you add a new inline policy

1. Apply it to AWS first (e.g. via `aws iam put-role-policy ...`)
2. Save the JSON document as `<role-name>.json` in this directory
3. Commit the file with a description of why the grant was needed

If the role is module-specific rather than orchestration-shared, codify
it in the owning module's repo instead.

<!-- ci-trigger -->
