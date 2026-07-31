#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-overseer-backstop-responder Lambda
# and wire its SNS subscription to the backstop alarm topic.
#
# This Lambda subscribes DIRECTLY to alpha-engine-alarm-backstop SNS topic (not
# via EventBridge) and:
#   1. Gathers fleet state (kill-switches, queue depth/age, last ledger, probe state)
#   2. Attempts ONE bounded recovery per alarm per cooldown window:
#      - Re-invoke the liveness probe (read-only, always safe)
#      - Re-dispatch alert-drain via the router (for intake-age alarm)
#   3. Escalates loudly on second occurrence within the window
#   4. Forwards an enhanced page to Telegram
#
# Designed per alpha-engine-config-I4480 (G9 of the 2026-07-27 conformance audit).
# The backstop must stay dumb: no agent, no queue, no bus dependency.
#
# Zero pip dependencies — the handler uses only the Python standard library +
# boto3 (Lambda runtime built-in). No requirements.txt needed.
# The playbooks.yaml registry is bundled from the repo SSoT.
#
# Managed outside CloudFormation — same rationale as backstop-telegram-notifier +
# other operator-deployed Lambdas.
#
# Usage:
#   bash deploy.sh                                   # update code only
#   bash deploy.sh --bootstrap                       # first-time create
#   bash deploy.sh --dry-run                         # show actions, do not apply
#   bash deploy.sh --smoke                           # invoke once with a synthetic ALARM

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-overseer-backstop-responder"
ROLE_NAME="alpha-engine-overseer-backstop-responder-role"
POLICY_NAME="alpha-engine-overseer-backstop-responder-policy"
BACKSTOP_TOPIC_NAME="alpha-engine-alarm-backstop"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

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
REPO_ROOT="$(cd "${LAMBDAS_DIR}/../.." && pwd)"
REGISTRY_SRC="${REPO_ROOT}/infrastructure/overseer/playbooks.yaml"

# ----- 0b. Preflight handler unit tests (shared gate — config#2381) ----------
# Delegates to the one _shared/run_handler_tests.sh so this gate can never
# re-drift into the naive no-install `python3 -m pytest` form (config#2295).
# Handler imports boto3 (Lambda runtime built-in) at module scope, and tests
# stub it via unittest.mock — boto3 must be installed for the import to work.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

# ----- 1. Package: zip handler + playbooks registry -------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"

# Bundle the playbook registry (same SSoT as overseer-dispatcher + liveness-probe).
if [[ -f "${REGISTRY_SRC}" ]]; then
  cp "${REGISTRY_SRC}" "${PKG}/playbooks.yaml"
  echo "Bundled playbooks.yaml from ${REGISTRY_SRC}"
else
  echo "WARNING: playbooks.yaml not found at ${REGISTRY_SRC} — Lambda will fail to load registry"
  echo "Continuing; deploy.sh --smoke will catch this."
fi

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
      --timeout 60 \
      --memory-size 256 \
      --region "${REGION}" \
      --environment "Variables={COOLDOWN_MINUTES=60,LOG_LEVEL=INFO}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 2b. Reconcile SNS subscription (ALWAYS — not bootstrap-gated) ---------

BACKSTOP_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${BACKSTOP_TOPIC_NAME}"
FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

echo "Reconciling SNS subscription: ${BACKSTOP_TOPIC_ARN} -> ${FUNCTION_NAME}"

# Give SNS permission to invoke the Lambda
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

echo "Deploy complete."

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
        "Message": "{\"AlarmName\":\"alpha-engine-watch-plane-overseer-intake-age\",\"AlarmDescription\":\"Overseer intake queue message age exceeds threshold — synthetic smoke test for backstop responder (alpha-engine-config-I4480).\",\"AWSAccountId\":\"${ACCOUNT_ID}\",\"NewStateValue\":\"ALARM\",\"NewStateReason\":\"Smoke test: verifying backstop responder state gathering + recovery attempt + Telegram delivery (alpha-engine-config-I4480).\",\"StateChangeTime\":\"$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)\",\"Region\":\"${REGION}\",\"OldStateValue\":\"OK\",\"Trigger\":{\"MetricName\":\"ApproximateAgeOfOldestMessage\",\"Namespace\":\"AWS/SQS\",\"StatisticType\":\"Maximum\",\"Statistic\":\"MAX\",\"Period\":300,\"EvaluationPeriods\":1,\"ComparisonOperator\":\"GreaterThanOrEqualToThreshold\",\"Threshold\":3600,\"Dimensions\":[{\"name\":\"QueueName\",\"value\":\"nousergon-overseer-intake\"}]}}"
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
