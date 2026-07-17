#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-eod-backstop Lambda + its
# EventBridge cron rule.
#
# Phase 2 of the trading-day-gap arc (config#1229). Same-day backstop for the
# EOD Step Function, whose ONLY normal trigger is the daemon shutdown hook. If
# the daemon dies before that hook, the EOD SF never fires and the day's
# eod_pnl row goes missing → the next reconcile's headline spans multiple
# sessions (the 2026-06-24 → RGEN +14.92% class; config#1228/#1229).
#
# Fires ~22:30 UTC MON-FRI (well after the daemon's ~20:15 UTC EOD). Starts the
# EOD SF IFF the trading box is still running AND no EOD started today. See
# index.py for the full guard rationale.
#
# Managed outside CloudFormation — same rationale as pipeline-watchdog /
# sf-telegram-notifier / eod-success-friday-shell-trigger (operator-deployed
# only, narrow OIDC blast radius).
#
# SAFE ROLLOUT: --bootstrap creates the EventBridge rule DISABLED. Soak the
# Lambda via --smoke on a non-trading-day / box-down state (guaranteed no-op),
# review the first dry firings in logs, THEN enable:
#   aws events enable-rule --name alpha-engine-eod-backstop-daily --region us-east-1
#
# Usage:
#   bash infrastructure/lambdas/eod-backstop/deploy.sh             # update code only
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --bootstrap # first-time create (rule DISABLED)
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --smoke     # invoke once (no-op unless box up + no EOD today)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-eod-backstop"
ROLE_NAME="alpha-engine-eod-backstop-role"
POLICY_NAME="alpha-engine-eod-backstop-policy"
RULE_NAME="alpha-engine-eod-backstop-daily"
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
run_handler_tests "${SCRIPT_DIR}" boto3 -r "${SCRIPT_DIR}/requirements.txt"

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
      --timeout 60 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # EventBridge cron: 22:30 UTC MON-FRI — comfortably after the daemon's
  # nominal ~20:15 UTC EOD in both DST regimes. Created DISABLED for safe
  # rollout (this Lambda can START the trading EOD pipeline); enable
  # deliberately after a soak via `aws events enable-rule`.
  echo "  Creating EventBridge rule: ${RULE_NAME} (DISABLED)"
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --schedule-expression 'cron(30 22 ? * MON-FRI *)' \
    --state DISABLED \
    --description "EOD-pipeline backstop fire at 22:30 UTC MON-FRI (Lambda gates on trading-day + box-running + no-EOD-today). DISABLED until soak-reviewed." \
    --region "${REGION}" \
    --query 'RuleArn' --output text

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  run aws events put-targets \
    --rule "${RULE_NAME}" \
    --targets "Id=1,Arn=${FN_ARN}" \
    --region "${REGION}"

  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${RULE_ARN}" \
    --region "${REGION}" 2>/dev/null || true

  echo "  NOTE: rule is DISABLED. After soak: aws events enable-rule --name ${RULE_NAME} --region ${REGION}"
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

# ----- 4. Smoke (synthetic empty event — exercises the full handler) --------

if $SMOKE; then
  echo ""
  echo "WARNING: --smoke runs the REAL handler. If today is a trading day AND the"
  echo "         trading box is running AND no EOD started today, it WILL start the"
  echo "         EOD Step Function. Run it only when you expect a no-op (non-trading"
  echo "         day, or box already stopped), or when you genuinely want to recover"
  echo "         today's missing EOD."
  RESP=$(mktemp)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
