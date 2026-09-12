#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-overseer-dispatcher Lambda.
#
# WHY (alpha-engine-config-I2823, epic I2821): registry-driven router in
# front of the fleet's failure-response executor Lambdas. One dispatch entry,
# one playbook registry (infrastructure/overseer/playbooks.yaml — BUNDLED
# into the zip here), one owner of verdict-based P1 filing + loud paging
# (previously duplicated in sf-watch.yml GHA yaml). Executors unchanged.
#
# IAM (iam-policy.json): lambda:InvokeFunction on the two routed executors,
# ssm:GetParameter on the fleet PAT + Telegram secrets, s3:PutObject on the
# dispatch-ledger + intake-fallback prefixes, sns:Publish on
# alpha-engine-alerts, events:PutEvents on the nousergon-alerts bus. No EC2
# permissions — launching is the EXECUTORS' job.
#
# Managed OUTSIDE CloudFormation like its sibling dispatchers. Flagless run
# is code-only (the GHA auto-deploy path); --bootstrap creates role + policy
# + function (operator-run only).
#
# Usage:
#   bash .../overseer-dispatcher/deploy.sh             # update code only
#   bash .../overseer-dispatcher/deploy.sh --bootstrap # operator-only: create role + Lambda
#   bash .../overseer-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../overseer-dispatcher/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-overseer-dispatcher"
ROLE_NAME="alpha-engine-overseer-dispatcher-role"
POLICY_NAME="alpha-engine-overseer-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
# Bootstrap default (first-time only) — the update path preserves live flags.
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,OVERSEER_DISPATCH_ENABLED=true}'

# Shared operator-flag-preserve helper (config#1818/#2236/#2264 bug class).
source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Scratch dir + validate handler syntax -----------------------------
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
KREPIS_REQ=$(requirement_pin "${SCRIPT_DIR}/requirements.txt" krepis)
run_handler_tests "${SCRIPT_DIR}" "${KREPIS_REQ}"

# ----- 1. Package: pip install deps + zip handler + bundle registry ---------
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
# The playbook registry is the router's routing table — bundled from the
# repo SSoT so a registry edit deploys through the normal code path (pinned
# by tests/test_overseer_playbook_registry.py::test_router_bundles_this_registry).
cp "${SCRIPT_DIR}/../../overseer/playbooks.yaml" "${PKG}/playbooks.yaml"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------
# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no alarms."
  exit 0
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for IAM role propagation..."
    sleep 10
  fi

  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    echo "  Creating Lambda function: ${FUNCTION_NAME}"
    # Timeout 300s: the executor invoke is SYNCHRONOUS; the slowest executor
    # (alert-drain, 300s Lambda timeout) bounds it, with the invoke client's
    # own 290s read-timeout + zero retries doing the fine-grained bounding.
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --architectures x86_64 \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
      --timeout 300 \
      --memory-size 256 \
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda function exists: ${FUNCTION_NAME}"
  fi
fi

# ----- 3. Update code (always) ----------------------------------------------
echo "Updating ${FUNCTION_NAME} code..."
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
  verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"
  # Preserve operator-owned runtime flags across redeploys (config#1818 class).
  CURRENT_ENABLED=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" OVERSEER_DISPATCH_ENABLED true)
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "Variables={LOG_LEVEL=INFO,OVERSEER_DISPATCH_ENABLED=${CURRENT_ENABLED}}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

# ----- 4. Zero-retry inbound async-invoke config (always, idempotent) -------
# config#2902: the router's INBOUND async-invoke surface (any future
# InvocationType=Event caller, e.g. the I2830 M2 path) must not silently
# double-dispatch on a platform retry — AWS's async-invoke default is 2
# retries. Router failures are handled by the clean-JSON-never-raise
# contract + the watch-plane Errors alarm backstop, not by AWS-level retry.
echo "Setting zero-retry async-invoke config on ${FUNCTION_NAME}..."
run aws lambda put-function-event-invoke-config \
  --function-name "${FUNCTION_NAME}" \
  --maximum-retry-attempts 0 \
  --region "${REGION}" \
  --query 'MaximumRetryAttempts' --output text

# ----- 5. Check IAM policy against live (READ-ONLY — I9045) ----------------
# This path deploys CODE ONLY and mutates no IAM, which is what every
# deploy-*.yml header has always claimed. It was not true until 2026-08-29: the
# old call here issued `aws iam put-role-policy` on every merge and classified
# the inevitable AccessDenied as expected, so each merge left a CloudTrail
# AccessDenied on iam:PutRolePolicy from an identity that must never hold it
# (single-writer rule; identity-access-policy.md §4 — the answer to a denied
# write is not to grant it, and here it was not to make the call).
#
# What runs instead compares live IAM to iam-policy.json and, on drift, prints
# the exact operator command. IAM writes live behind --bootstrap and
# --apply-iam, where an operator states the intent with a flag.
#
# No `||` here on purpose (alpha-engine-config-I7338): a broken checker — this
# helper not sourced at all, an unreadable iam-policy.json — aborts the deploy
# under `set -e` rather than printing a reassurance.
check_iam_policy_on_deploy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json"

# ----- 6. Publish alert_classes projection to S3 (alpha-engine-config#5200) -----
# The alert-drain box clones only alpha-engine-config, so the registry at
# infrastructure/overseer/playbooks.yaml is unreachable from there. Publish a
# lightweight projection (alert_classes only, not the full playbooks.yaml) to S3
# so the drain can mechanically verify every observed source against the
# declared set. The drain charter points at this S3 URI.
#
# TWO DEFECTS FIXED HERE 2026-08-29 (alpha-engine-config-I9045). The step was
# the fatal one in run 33229043798 — exit 255, the whole workflow red — and
# both halves are the same shape: a producer that swallowed, feeding a consumer
# that exploded.
#
#   1. `import yaml` raised ModuleNotFoundError on the CI runner. PyYAML is in
#      this lambda's requirements.txt but nothing put it on the DEPLOY host's
#      interpreter, and actions/setup-python ships a bare 3.12. The extraction
#      caught it, printed `WARN ... (non-fatal)`, and exited 0 WITHOUT writing
#      alert_classes.json — then the `aws s3 cp` below died on a file that was
#      never created. The producer's own failure was reported as benign and the
#      consumer's cascade is what actually paged.
#
#      Fixed by pointing PYTHONPATH at ${PKG}: step 1 already installed this
#      lambda's requirements.txt (pyyaml>=6.0 included) there via Docker. Zero
#      new dependency, nothing to keep in sync — the deploy reads the same
#      pyyaml it is about to ship. The linux/amd64 wheel is fine to import on an
#      operator's macOS too: yaml.safe_load is pure Python and PyYAML guards its
#      optional libyaml C extension itself.
#
#      And the swallow is gone. This repo is a PRODUCER (AGENTS.md): a writer
#      that returns partial output is a silent corruption of every consumer, so
#      an extraction failure is now fatal.
#
#   2. The `|| echo "...non-fatal..."` on the `aws s3 cp` was UNREACHABLE.
#      `run()` calls `exit`, not `return` (alpha-engine-config-I8033), and `||`
#      guards a RETURN. That is exactly the class I8125 closed with
#      run_tolerating(), and this was the last `run ... ||` site left in any
#      deploy.sh in this repo. Using run_tolerating() also narrows the
#      tolerance from "any failure" to the one named cause.
#
# THE TOLERATED CAUSE, and why it is still tolerated: `github-actions-lambda-
# deploy` holds s3:PutObject on alpha-engine-research/infrastructure/*,
# /changelog/* and /ops/registry/* and NOT on /overseer/* (measured 2026-08-29
# against the live inline policy). Widening the deploy identity is an operator
# act under the single-writer rule, not something this deploy may do for
# itself. Consequence, stated rather than hidden: the object does not exist in
# S3 today and CI cannot create it. Tracked as alpha-engine-config-I9238; an
# operator running this deploy.sh publishes it in the meantime.
echo "Publishing alert_classes to S3..."
PYTHONPATH="${PKG}" python3 -c "
import json, sys
import yaml

path = '${SCRIPT_DIR}/../../overseer/playbooks.yaml'
with open(path) as f:
    data = yaml.safe_load(f)
classes = data.get('alert_classes', [])
if not classes:
    print('ERROR: playbooks.yaml declares no alert_classes — refusing to '
          'publish an empty projection over the declared set.', file=sys.stderr)
    raise SystemExit(1)
payload = json.dumps({'alert_classes': classes, 'schema_version': 1}, indent=2)
with open('${PKG}/alert_classes.json', 'w') as f:
    f.write(payload)
print(f'Extracted {len(classes)} alert classes from playbooks.yaml')
"
echo "NOTE: the CI auto-deploy identity holds no s3:PutObject on"
echo "NOTE: alpha-engine-research/overseer/ — from CI this publish is EXPECTED to be"
echo "NOTE: tolerated, and s3://alpha-engine-research/overseer/alert_classes.json"
echo "NOTE: therefore does not exist yet (alpha-engine-config-I9238). Running this"
echo "NOTE: deploy.sh as an operator publishes it."
run_tolerating "AccessDenied" \
  aws s3 cp "${PKG}/alert_classes.json" \
  "s3://alpha-engine-research/overseer/alert_classes.json" \
  --content-type application/json

echo "Done."
