#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-saturday-integrity-sentinel
# Lambda and wire its Monday-pre-open EventBridge cron.
#
# Saturday-SF Watch arc, M4 — the independent Sat→Monday swallow safeguard.
# Reads the freshness-monitor's saturday_sf cycle verdict and pages a GO/NO-GO
# Monday ~12:30 UTC (15 min before the weekday SF at 12:45 UTC). Non-blocking.
# Spec: nousergon/alpha-engine-config#1227.
#
# Managed outside CloudFormation (operator-deployed; keeps the GHA OIDC role
# narrow). Merging the PR has ZERO live effect until --bootstrap.
#
# Usage:
#   bash …/deploy.sh             # update code only
#   bash …/deploy.sh --bootstrap # first-time create + wire EventBridge cron
#   bash …/deploy.sh --dry-run   # show actions, do not apply
#   bash …/deploy.sh --smoke     # invoke once (reads the live verdict, sends Telegram)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-saturday-integrity-sentinel"
ROLE_NAME="alpha-engine-saturday-integrity-sentinel-role"
POLICY_NAME="alpha-engine-saturday-integrity-sentinel-policy"
RULE_NAME="alpha-engine-saturday-integrity-monday"
SCHEDULE="cron(30 12 ? * MON *)"   # Monday 12:30 UTC, 15 min before weekday SF
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

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# ----- 0. Validate handler + run unit tests ---------------------------------
python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"
if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Running handler unit tests..."
  python3 -m pytest "${SCRIPT_DIR}/test_handler.py" -q
fi

# ----- 1. Package -----------------------------------------------------------
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
echo "Installing deps into ${PKG} (pip install -t)..."
python3 -m pip install --quiet --target "${PKG}" --upgrade -r "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/../flow_doctor_telegram.py" "${PKG}/flow_doctor_telegram.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap ---------------------------------------------------------
if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role --role-name "${ROLE_NAME}" --assume-role-policy-document "${TRUST_POLICY}" --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" --policy-name "${POLICY_NAME}" --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" --runtime python3.12 --role "${ROLE_ARN}" \
      --handler index.handler --zip-file "fileb://${ZIP}" --timeout 60 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' --region "${REGION}" --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  echo "  Creating EventBridge cron rule: ${RULE_NAME} (${SCHEDULE})"
  run aws events put-rule --name "${RULE_NAME}" --schedule-expression "${SCHEDULE}" \
    --description "Monday pre-open Saturday-integrity GO/NO-GO sentinel" --region "${REGION}" --query 'RuleArn' --output text

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  run aws events put-targets --rule "${RULE_NAME}" --targets "Id=1,Arn=${FN_ARN}" --region "${REGION}"

  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run aws lambda add-permission --function-name "${FUNCTION_NAME}" --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction --principal events.amazonaws.com --source-arn "${RULE_ARN}" --region "${REGION}" 2>/dev/null || true
fi

# ----- 3. Update function code ----------------------------------------------
echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"; fi
echo "✓ Code deployed."

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"; fi

# ----- 4. Smoke -------------------------------------------------------------
if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (reads the live verdict, sends Telegram)..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out --payload '{}' --region "${REGION}" "${RESP}" >/dev/null
  cat "${RESP}"; echo ""; rm -f "${RESP}"
fi
