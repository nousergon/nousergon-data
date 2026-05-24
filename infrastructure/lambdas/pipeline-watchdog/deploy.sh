#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-pipeline-watchdog Lambda + its
# EventBridge cron rule + the alpha-engine-watchdog-alerts SNS topic.
#
# Phase 4 of the pipeline-reporting-revamp arc (ROADMAP L3050, plan doc
# ~/Development/alpha-engine-docs/private/pipeline-reporting-revamp-260524.md
# §3.5). Trading-day-aware per the Phase 0 Q2 SOTA-lock.
#
# Per-day cron at 14:00 UTC (07:00 PT) checks each of the 3 SFs:
#   - Weekday SF: 24h window, watch-day = today is a NYSE trading day
#   - EOD SF:     24h window, watch-day = today is a NYSE trading day
#   - Saturday SF: 7d window, watch-day = today is Sunday
# Alerts fire via alpha_engine_lib.alerts.publish to a DISTINCT SNS topic
# (alpha-engine-watchdog-alerts) + Telegram in parallel — channel
# independence preserved per plan doc §3.5.
#
# Managed outside CloudFormation — same rationale as
# sf-telegram-notifier / eod-success-friday-shell-trigger /
# spot-orphan-reaper / changelog-cloudwatch-mirror (operator-deployed
# only, narrow OIDC blast radius).
#
# Usage:
#   bash infrastructure/lambdas/pipeline-watchdog/deploy.sh             # update code only
#   bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --bootstrap # first-time create + wire SNS + EventBridge
#   bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --smoke     # invoke once with no event payload

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-pipeline-watchdog"
ROLE_NAME="alpha-engine-pipeline-watchdog-role"
POLICY_NAME="alpha-engine-pipeline-watchdog-policy"
RULE_NAME="alpha-engine-pipeline-watchdog-daily"
SNS_TOPIC_NAME="alpha-engine-watchdog-alerts"
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

  # 2a. SNS topic for watchdog audit trail (NOT alpha-engine-alerts — channel
  # independence per plan doc §3.5)
  echo "  Ensuring SNS topic: ${SNS_TOPIC_NAME}"
  run aws sns create-topic \
    --name "${SNS_TOPIC_NAME}" \
    --region "${REGION}" \
    --query 'TopicArn' --output text

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

  # EventBridge cron: every day at 14:00 UTC (07:00 PT). Lambda
  # itself gates per-SF watch-day decisions via trading_calendar, so
  # the cron is dumb every-day (no need for separate weekday/weekend
  # rules at the EventBridge layer).
  echo "  Creating EventBridge rule: ${RULE_NAME}"
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --schedule-expression 'cron(0 14 * * ? *)' \
    --description "Daily pipeline-watchdog fire at 14:00 UTC (Lambda gates per-SF trading-day eligibility)" \
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
  echo "Smoke-testing via direct invoke (empty payload — exercises the full check chain)..."
  RESP=$(mktemp)
  PAYLOAD='{}'
  echo "WARNING: --smoke will publish a real alert if any SF is missing executions in its window."
  echo "         Confirm before proceeding or omit --smoke and rely on unit tests + first cron firing."
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload "${PAYLOAD}" \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
