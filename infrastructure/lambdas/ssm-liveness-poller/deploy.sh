#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-ssm-liveness-poller Lambda.
#
# config#1811: liveness-aware SSM poll iteration for the weekday pipeline's
# SSM command loops (MorningEnrich / MorningArcticAppend / ChronicGapSelfHeal /
# RunMorningPlanner / CodeFreshnessGate). One invocation = one poll: command
# status + independent SSM-agent PingStatus + bounded attempt/ping-miss
# accounting. See index.py for the full rationale (2026-07-06 incident: the
# in-box executionTimeout cannot fire when the box itself is wedged).
#
# No EventBridge trigger — invoked ONLY by ne-preopen-trading-pipeline
# (lambda:InvokeFunction is granted to alpha-engine-step-functions-role in
# the codified IAM: alpha-engine/infrastructure/iam/).
#
# Managed outside CloudFormation — same rationale as pipeline-watchdog /
# eod-backstop (operator-deployed only, narrow OIDC blast radius).
#
# Usage:
#   bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh             # update code only
#   bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --bootstrap # first-time create
#   bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --smoke     # one read-only invoke against a dummy command id

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-ssm-liveness-poller"
ROLE_NAME="alpha-engine-ssm-liveness-poller-role"
POLICY_NAME="alpha-engine-ssm-liveness-poller-policy"
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

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Running handler unit tests..."
  python3 -m pytest "${SCRIPT_DIR}/test_handler.py" -q
fi

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
      --timeout 30 \
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

# ----- 4. Smoke (read-only — a nonexistent command id exercises the
#          registration-window path and both SSM read calls) ----------------

if $SMOKE; then
  echo ""
  echo "Smoke: read-only invoke (nonexistent command id → IN_PROGRESS/Registering)"
  RESP=$(mktemp)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{"instance_id":"i-018eb3307a21329bf","command_id":"00000000-0000-0000-0000-000000000000","attempts":0,"ping_misses":0,"max_attempts":3,"max_ping_misses":3,"step":"smoke"}' \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
