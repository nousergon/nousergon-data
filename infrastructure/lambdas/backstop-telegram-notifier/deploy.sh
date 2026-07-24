#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-backstop-telegram-notifier Lambda
# and wire its SNS subscription to the backstop alarm topic.
#
# This Lambda subscribes DIRECTLY to alpha-engine-alarm-backstop SNS topic (not
# via EventBridge) and forwards every CloudWatch alarm to Telegram via raw
# urllib — NO krepis/nousergon_lib/flow-doctor/eventbridge dependencies.
# Designed per alpha-engine-config-I2899 invariant 3: the backstop must never
# involve an agent, a queue, or anything that can fail non-obviously.
#
# Zero pip dependencies — the handler uses only the Python standard library +
# boto3 (Lambda runtime built-in). No requirements.txt needed.
#
# Managed outside CloudFormation — same rationale as sf-telegram-notifier +
# other operator-deployed Lambdas.
#
# Usage:
#   bash infrastructure/lambdas/backstop-telegram-notifier/deploy.sh             # update code only
#   bash infrastructure/lambdas/backstop-telegram-notifier/deploy.sh --bootstrap # first-time create
#   bash infrastructure/lambdas/backstop-telegram-notifier/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/backstop-telegram-notifier/deploy.sh --smoke     # invoke once with a synthetic ALARM

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-backstop-telegram-notifier"
ROLE_NAME="alpha-engine-backstop-telegram-notifier-role"
POLICY_NAME="alpha-engine-backstop-telegram-notifier-policy"
BACKSTOP_TOPIC_NAME="alpha-engine-alarm-backstop"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run flag.
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
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

# ----- 0. Validate handler syntax -------------------------------------------

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ----- 1. Package: zip handler (no pip deps — stdlib only) -------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

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
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 2b. Reconcile SNS subscription (ALWAYS — not bootstrap-gated) ---------
# Subscribes the Lambda to the backstop SNS topic directly (no EventBridge).
# Idempotent: list-subscriptions-by-topic dedup, add-permission ignores
# duplicates.

BACKSTOP_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${BACKSTOP_TOPIC_NAME}"
FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

echo "Reconciling SNS subscription: ${BACKSTOP_TOPIC_ARN} -> ${FUNCTION_NAME}"

# Give SNS permission to invoke the Lambda (idempotent — 2>/dev/null||true on
# existing statements)
run aws lambda add-permission \
  --function-name "${FUNCTION_NAME}" \
  --statement-id "sns-${BACKSTOP_TOPIC_NAME}" \
  --action lambda:InvokeFunction \
  --principal sns.amazonaws.com \
  --source-arn "${BACKSTOP_TOPIC_ARN}" \
  --region "${REGION}" 2>/dev/null || true

# Check if an SNS->Lambda subscription already exists
EXISTING_SUB=$(aws sns list-subscriptions-by-topic \
  --topic-arn "${BACKSTOP_TOPIC_ARN}" \
  --query "Subscriptions[?Protocol=='lambda' && Endpoint=='${FN_ARN}'].SubscriptionArn" \
  --output text --region "${REGION}" 2>/dev/null || echo "")

if [[ -z "$EXISTING_SUB" || "$EXISTING_SUB" == "None" ]]; then
  echo "  Subscribing ${FUNCTION_NAME} to ${BACKSTOP_TOPIC_NAME}..."
  run aws sns subscribe \
    --region "${REGION}" \
    --topic-arn "${BACKSTOP_TOPIC_ARN}" \
    --protocol lambda \
    --notification-endpoint "${FN_ARN}" \
    --query 'SubscriptionArn' --output text
else
  echo "  Subscription already exists: ${EXISTING_SUB}"
fi

# ----- 3. Update function code (always after bootstrap, idempotent) ----------

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

# ----- 4. Smoke (synthetic ALARM event) -------------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic ALARM event)..."
  RESP=$(mktemp)
  SMOKE_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${BACKSTOP_TOPIC_NAME}"
  PAYLOAD=$(cat <<EOF
{
  "Records": [
    {
      "Sns": {
        "MessageId": "smoke-test-$(date +%s)",
        "TopicArn": "${SMOKE_TOPIC_ARN}",
        "Message": "{\"AlarmName\":\"alpha-engine-watch-plane-smoke-test\",\"AlarmDescription\":\"Synthetic smoke test alarm for backstop Telegram forwarder (alpha-engine-config-I2899). Generated by deploy.sh --smoke.\",\"AWSAccountId\":\"${ACCOUNT_ID}\",\"NewStateValue\":\"ALARM\",\"NewStateReason\":\"Smoke test: verifying backstop Telegram forwarder delivery (alpha-engine-config-I2899).\",\"StateChangeTime\":\"$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)\",\"Region\":\"${REGION}\",\"OldStateValue\":\"OK\",\"Trigger\":{\"MetricName\":\"SmokeTest\",\"Namespace\":\"AWS/BackstopTelegram\",\"StatisticType\":\"Sum\",\"Statistic\":\"SUM\",\"Period\":300,\"EvaluationPeriods\":1,\"ComparisonOperator\":\"GreaterThanOrEqualToThreshold\",\"Threshold\":1,\"Dimensions\":[]}}"
      }
    }
  ]
}
EOF
)
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
