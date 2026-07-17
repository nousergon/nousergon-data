#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-ssm-liveness-poller Lambda.
#
# config#1811: liveness-aware SSM poll iteration for the weekday pipeline's
# SSM command loops (RunMorningPlanner / CodeFreshnessGate — ChronicGapSelfHeal
# REMOVED per alpha-engine-config-I2717 2026-07-16, moved to the standalone
# --daily-heal job off the trading box entirely; MorningEnrich/
# MorningArcticAppend moved off-box even earlier, config#1767 Phase 2, onto
# their own bare-ssm-poll ephemeral spots). One invocation = one poll: command
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
#   bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --smoke     # one read-only invoke against a dummy command id

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-ssm-liveness-poller"
ROLE_NAME="alpha-engine-ssm-liveness-poller-role"
POLICY_NAME="alpha-engine-ssm-liveness-poller-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops
# instead of silently running the real deploy path (alpha-engine-config-
# I2752 incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked
# here, matching other tools' convention, and triggered a real deploy).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
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
# Delegates to the one _shared/run_handler_tests.sh so this gate can never
# re-drift into the naive no-install `python3 -m pytest` form (config#2295).
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

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
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
