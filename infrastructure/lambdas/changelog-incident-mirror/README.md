# `changelog-incident-mirror` Lambda

Subscribed to `arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts`.
For every SNS message, writes one structured incident entry to:

  `s3://alpha-engine-research/changelog/entries/{YYYY-MM-DD}/{event_id}.json`

Schema 1.0.0 per `alpha-engine-config/changelog/vocab.yaml`. Carries
controlled-vocab fields (`severity=high`, `subsystem=infrastructure`,
`root_cause_category=infrastructure_failure`, `auto_emitted=true`)
chosen as sensible defaults — most SNS-mirrored alerts are SF/Lambda
failures. Operator can refine via a follow-up
`changelog-log --event-type investigation` entry whose `git_refs`
reference the original `event_id`.

Legacy dual-write to `changelog/incidents/{YYYY}/{MM}/{DD}T...` retired
2026-05-07 after the 1-week back-compat bake (per CLAUDE.md S3
contract). Historical objects under that prefix remain in S3 for
retroactive queries.

## Why this lives outside CloudFormation

Originally added to the `alpha-engine-orchestration` CF stack as a
sibling of `AlertsTopic` (alpha-engine-data PR #122). That triggered a
perm-cascade on the `github-actions-lambda-deploy` OIDC role — the
role lacked `iam:TagRole`, `iam:GetRole`, etc. needed by CF to
introspect the new resources during `update-stack`. Two recovery
cycles into the cascade, the team decided to orphan the Lambda
rather than keep expanding the OIDC role to manage one 50-line
inline-handler Lambda.

Resources kept alive across the orphaning via `DeletionPolicy:
Retain` (CF removed them from tracking but didn't call any
service-side delete API). They are now hand-managed via this
directory.

The decision to orphan + reconsideration triggers are documented in
`alpha-engine-config/private-docs/ROADMAP.md` under the Observability
section.

## What's here

| File | Purpose |
|---|---|
| `index.py` | Lambda handler source (Python 3.12, arm64, 256 MB, 30s) |
| `iam-policy.json` | Inline policy attached to the function's role |
| `deploy.sh` | One-shot redeploy script (updates function code only) |
| `README.md` | this file |

The Lambda's function name, role name, SNS subscription, and Lambda
permission are all live in AWS but **not** versioned in any IaC.
Their config:

| Resource | Identifier | Notes |
|---|---|---|
| Function | `alpha-engine-changelog-incident-mirror` | Python 3.12, arm64, 256 MB, 30s, env vars: `CHANGELOG_BUCKET=alpha-engine-research`, `CHANGELOG_PREFIX=changelog/incidents`, `CHANGELOG_STRUCTURED_PREFIX=changelog/entries` |
| Execution role | `alpha-engine-changelog-incident-mirror` | Trust: `lambda.amazonaws.com`. Managed: `AWSLambdaBasicExecutionRole`. Inline: `changelog-incident-mirror-s3` (see `iam-policy.json`) |
| SNS subscription | `alpha-engine-alerts` topic, lambda protocol | Endpoint = function ARN |
| Lambda permission | `lambda:InvokeFunction` from `sns.amazonaws.com` | SourceArn = AlertsTopic ARN |

## Operations

### Update the handler code

```bash
bash infrastructure/lambdas/changelog-incident-mirror/deploy.sh
# or with smoke test:
bash infrastructure/lambdas/changelog-incident-mirror/deploy.sh --smoke
```

The script packages `index.py` into a zip, calls
`aws lambda update-function-code`, and waits for the update to
settle. Auth uses your local AWS CLI creds — the personal IAM user
(`cipher813`) has enough perms; the `github-actions-lambda-deploy`
OIDC role intentionally does not.

### Recreate from scratch (disaster recovery / new account)

If the resources need to be rebuilt — e.g. in a new AWS account, or
after an accidental deletion — run these commands in order. The
Lambda is small enough that recreation is straightforward.

```bash
# 1. Create execution role
aws iam create-role \
  --role-name alpha-engine-changelog-incident-mirror \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy \
  --role-name alpha-engine-changelog-incident-mirror \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam put-role-policy \
  --role-name alpha-engine-changelog-incident-mirror \
  --policy-name changelog-incident-mirror-s3 \
  --policy-document file://infrastructure/lambdas/changelog-incident-mirror/iam-policy.json

# 2. Package and create function (wait a few seconds after role creation for IAM propagation)
zip -j /tmp/fn.zip infrastructure/lambdas/changelog-incident-mirror/index.py

aws lambda create-function \
  --function-name alpha-engine-changelog-incident-mirror \
  --runtime python3.12 \
  --architectures arm64 \
  --handler index.handler \
  --role "arn:aws:iam::711398986525:role/alpha-engine-changelog-incident-mirror" \
  --timeout 30 --memory-size 256 \
  --environment "Variables={CHANGELOG_BUCKET=alpha-engine-research,CHANGELOG_PREFIX=changelog/incidents,CHANGELOG_STRUCTURED_PREFIX=changelog/entries}" \
  --zip-file fileb:///tmp/fn.zip

# 3. Subscribe to SNS topic
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts \
  --protocol lambda \
  --notification-endpoint arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-changelog-incident-mirror

# 4. Allow SNS to invoke the function
aws lambda add-permission \
  --function-name alpha-engine-changelog-incident-mirror \
  --statement-id sns-alerts-invoke \
  --action lambda:InvokeFunction \
  --principal sns.amazonaws.com \
  --source-arn arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts
```

### Verify

```bash
# Publish a test message
aws sns publish \
  --topic-arn arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts \
  --subject "Smoke test" --message "Verify mirror works"

# Check the entry landed
aws s3 ls s3://alpha-engine-research/changelog/entries/$(date -u +%Y-%m-%d)/ --recursive | tail
```

### Retire (end-of-life)

```bash
# Find subscription ARN
SUB_ARN=$(aws sns list-subscriptions-by-topic \
  --topic-arn arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts \
  --query "Subscriptions[?Endpoint=='arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-changelog-incident-mirror'].SubscriptionArn | [0]" \
  --output text)

aws sns unsubscribe --subscription-arn "$SUB_ARN"
aws lambda delete-function --function-name alpha-engine-changelog-incident-mirror
aws iam delete-role-policy --role-name alpha-engine-changelog-incident-mirror --policy-name changelog-incident-mirror-s3
aws iam detach-role-policy --role-name alpha-engine-changelog-incident-mirror --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name alpha-engine-changelog-incident-mirror
```
