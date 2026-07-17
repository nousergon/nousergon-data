#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-substrate-health-gate Lambda.
#
# config#2249: fast pre-dispatch substrate health gate for the weekly SF.
# Runs as a NEW Task state immediately before MorningEnrich — one shot,
# self-contained (issues its own tiny SSM `df` probe command and polls it to
# terminal status inside this single invocation, bounded well under the
# Lambda's own timeout). See index.py for the full rationale and verdict
# shapes (HEALTHY / SUBSTRATE_UNHEALTHY with reason in
# {disk_full, ssm_unresponsive, ssm_command_never_registered}).
#
# No EventBridge trigger — invoked ONLY by ne-weekly-pipeline
# (lambda:InvokeFunction must be granted to alpha-engine-step-functions-role
# in the codified IAM: infrastructure/iam/ + apply.sh — NOT done by this
# script, mirrors ssm-liveness-poller's bootstrap note below).
#
# Managed outside CloudFormation — same rationale as ssm-liveness-poller /
# pipeline-watchdog / eod-backstop (operator-deployed only, narrow OIDC
# blast radius).
#
# Usage:
#   bash infrastructure/lambdas/substrate-health-gate/deploy.sh             # update code only
#   bash infrastructure/lambdas/substrate-health-gate/deploy.sh --bootstrap # first-time create
#   bash infrastructure/lambdas/substrate-health-gate/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/substrate-health-gate/deploy.sh --smoke     # one read-only-ish invoke (WARNING: issues a real tiny SSM df command against the given instance)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-substrate-health-gate"
ROLE_NAME="alpha-engine-substrate-health-gate-role"
POLICY_NAME="alpha-engine-substrate-health-gate-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

DRY_RUN=false
BOOTSTRAP=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --smoke) SMOKE=true ;;
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

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

# ----- 1. Package: pip install deps + zip handler ---------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (pip install -t)..."
python3 -m pip install \
  --quiet \
  --target "${PKG}" \
  --upgrade \
  -r "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

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

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 90 \
      --memory-size 128 \
      --environment 'Variables={LOG_LEVEL=INFO}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  echo "  NOTE: grant lambda:InvokeFunction on this function to"
  echo "        alpha-engine-step-functions-role via the codified IAM"
  echo "        (alpha-engine/infrastructure/iam/ + apply.sh) before"
  echo "        deploying the SF definition that calls it."
fi

# ----- 3. Update function code (always after bootstrap, idempotent) ---------

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

# ----- 4. Smoke (issues a REAL tiny `df` SSM command against the given
#          instance — not read-only like ssm-liveness-poller's smoke, since
#          this Lambda's whole job is to issue one) -------------------------

if $SMOKE; then
  echo ""
  echo "Smoke: live invoke against i-018eb3307a21329bf (issues a real df probe command)"
  RESP=$(mktemp)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{"instance_id":"i-018eb3307a21329bf"}' \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
