#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-data-spot-dispatcher Lambda.
#
# This Lambda is the SF-invokable launcher for the data-heavy weekday/EOD enrich
# workloads on an ephemeral EC2 spot box (config#1767 Phase 2). It is invoked
# DIRECTLY by the two existing orchestration Step Functions —
# `ne-preopen-trading-pipeline` (MorningEnrich + MorningArcticAppend) and
# `ne-postclose-trading-pipeline` (PostMarketData + PostMarketArcticAppend) — so,
# UNLIKE scheduled-groom-dispatcher, there is NO wrapping Step Function and NO
# EventBridge Scheduler to wire here. This script manages only the Lambda and its
# execution role. The SF EXECUTION-role delta (sf-execution-iam-policy.json —
# `lambda:InvokeFunction` on this function) lives in
# `infrastructure/iam/alpha-engine-step-functions-role.json` and is applied
# SEPARATELY by `infrastructure/iam/apply.sh` (the human-gated IAM path), NOT here.
#
# Managed OUTSIDE CloudFormation — same rationale as the sibling dispatchers
# (scheduled-groom-dispatcher, spot-orphan-reaper): keep the
# github-actions-lambda-deploy OIDC role's blast radius narrow. That OIDC role
# deliberately LACKS iam:CreateRole / iam:PutRolePolicy (a fleet-wide policy
# after repeated IAM-clobber incidents — see infrastructure/iam/README.md), so
# the FIRST-TIME `--bootstrap` (which mints the execution role + creates the
# function) MUST be run by an operator with IAM rights. The flagless run is
# code-only (`update-function-code` + env converge) and is the CI auto-deploy
# path once a deploy-data-spot-dispatcher.yml workflow exists.
#
# WHY THIS SCRIPT EXISTS (2026-07-08 EOD incident): config#1767 Phase 2 (#643)
# shipped this Lambda's source + IAM policy + SF wiring + the SF-role invoke
# grant, but NO deploy.sh — so step 1 of the README rollout ("create the Lambda +
# execution role") had no runnable tooling and was skipped. The SF re-deploy
# (auto, on merge) and the SF-role IAM re-apply (manual) both landed, but the
# function itself was never created: LaunchPostMarketDataSpot got a 403 (grant
# not yet applied) and then, once the grant WAS applied, a 404
# ResourceNotFoundException (function absent). Fail-open let the pipeline
# continue, but the post-market SPY close was never fetched and EODReconcile
# correctly failed loud. This script is the missing, repeatable step-1 tooling.
#
# Usage:
#   bash infrastructure/lambdas/data-spot-dispatcher/deploy.sh             # update code + env only (CI auto-deploy path)
#   bash infrastructure/lambdas/data-spot-dispatcher/deploy.sh --bootstrap # operator-only: create/update the execution role + create the Lambda
#   bash infrastructure/lambdas/data-spot-dispatcher/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-data-spot-dispatcher"
ROLE_NAME="alpha-engine-data-spot-dispatcher-role"
POLICY_NAME="alpha-engine-data-spot-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Canonical function env — defined ONCE so the create / update paths can never
# drift out of lockstep. DATA_SPOT_DISPATCH_ENABLED is the kill-switch the
# handler reads (index.py: DISPATCH_ENABLED); every other DATA_SPOT_* knob uses
# the handler's in-code default, so they are intentionally NOT set here.
PROD_ENV='Variables={LOG_LEVEL=INFO,DATA_SPOT_DISPATCH_ENABLED=true}'

# Timeout must cover the handler's worst case: launch a spot + wait for its SSM
# agent to come Online (DATA_SPOT_SSM_ONLINE_BUDGET_SEC default 300s) before the
# async detached SSM send-command + immediate return. 300s matches that budget;
# memory is small (the box does the heavy lifting, not the Lambda).
FN_TIMEOUT=300
FN_MEMORY=256

DRY_RUN=false
BOOTSTRAP=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

run() {
  if $DRY_RUN; then
    echo "DRY: $*"
  else
    "$@"
  fi
}

# ----- 0. Scratch dir + validate handler syntax ------------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 1. Package: pip install deps (Lambda-safe) + zip handler --------------
# index.py imports nousergon_lib.ec2_spot (the config#1767 spot-launch
# chokepoint) + boto3 + krepis, all pinned in requirements.txt — so the deps are
# bundled via the shared Lambda-safe installer (same as scheduled-groom-
# dispatcher, whose requirements.txt carries the SAME nousergon-lib pin).

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only, operator-run) --------------------------

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline policy ---
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --description "Execution role for ${FUNCTION_NAME} — launch the data-enrich spot box + fire async SSM (config#1767)" \
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

  # --- 2b. Lambda function ---
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout "${FN_TIMEOUT}" \
      --memory-size "${FN_MEMORY}" \
      --environment "${PROD_ENV}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 3. Update function code (always, idempotent) --------------------------
# On a not-yet-bootstrapped function this update-function-code FAILS LOUD with a
# 404 (set -e aborts) — deliberately: a flagless run cannot silently "succeed"
# when the function was never created. Run --bootstrap first.

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

echo "✓ Code deployed."

# ----- 4. Converge function env (always) -------------------------------------
# update-function-code does not touch env, so converge the canonical env on
# every deploy (this is how DATA_SPOT_DISPATCH_ENABLED lands on an already-
# created function and how any interim override is reset to the canonical value).

echo "Converging Lambda environment..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${PROD_ENV}" \
  --timeout "${FN_TIMEOUT}" \
  --memory-size "${FN_MEMORY}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi
echo "✓ Env converged: ${PROD_ENV}"

echo ""
echo "Done. Next (operator): re-run the affected pipeline once to validate end to"
echo "end — e.g. a plain EOD rerun (idempotent, no skip flags) confirms"
echo "LaunchPostMarketDataSpot now invokes the function and the SPY close lands."
