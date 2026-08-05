#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-weekly-preflight Lambda.
#
# This Lambda implements the pre-spend gate for ne-weekly-freshness-pipeline
# (I4494). It runs sf_preflight checks (IAM reachability, tool contracts,
# definition input coherence, Lambda memory headroom) BEFORE the pipeline
# launches any spot or acquires the mutex, stopping the run in seconds
# when a Saturday-fatal condition exists.
#
# Managed outside CloudFormation — same rationale as all sibling Lambdas
# in this directory (narrow OIDC role blast radius).
#
# Usage:
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh             # update code only (CI path)
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh --bootstrap # first-time create
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh --apply-iam # re-apply iam-policy.json
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-weekly-preflight"
ROLE_NAME="alpha-engine-weekly-preflight-role"
POLICY_NAME="alpha-engine-weekly-preflight-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

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

run() {
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"
python3 -c "import ast; ast.parse(open('${REPO_ROOT}/sf_preflight.py').read()); print('sf_preflight.py syntax OK')"

# ----- Handler unit tests (shared gate) -------------------------------------
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

# ----- 1. Package: copy sf_preflight.py + handler + deps --------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

# Copy sf_preflight.py from the repo root — this is the core check logic.
echo "Copying sf_preflight.py to package..."
cp "${REPO_ROOT}/sf_preflight.py" "${PKG}/sf_preflight.py"

echo "Installing deps into ${PKG}..."
python3 -m pip install --quiet --target "${PKG}" --upgrade -r "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied."
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 --role "${ROLE_ARN}" --handler index.handler \
      --zip-file "fileb://${ZIP}" --timeout 120 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,SF_DEFINITION_BUCKET=alpha-engine-research}' \
      --region "${REGION}" --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 3. Grant SF execution role permission to invoke this Lambda -----------
# The step functions execution role needs lambda:InvokeFunction on this
# new preflight Lambda. The SF execution role policy now lives in the
# nous-ergon-ops repo (nous-ergon-ops/infrastructure/iam/alpha-engine-
# step-functions-role.json) and is applied by that repo's apply.sh — IAM is
# no longer owned by this repo (infrastructure-ownership-policy.md §35). For
# reference, the grant to add there:
#
#   {
#     "Sid": "InvokeWeeklyPreflight",
#     "Effect": "Allow",
#     "Action": "lambda:InvokeFunction",
#     "Resource": "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-weekly-preflight:*"
#   }
echo ""
echo "NOTE: The SF execution role grant (lambda:InvokeFunction on ${FUNCTION_NAME})"
echo "must be added to the SF execution role policy in the nous-ergon-ops repo"
echo "and applied via that repo's apply.sh before the step function can"
echo "invoke this Lambda."
echo ""

# ----- 4. Update function code (always, idempotent) --------------------------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "✓ ${FUNCTION_NAME} deployed."
