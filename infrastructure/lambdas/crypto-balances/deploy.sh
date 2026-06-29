#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-crypto-balances Lambda and wire its
# EventBridge Scheduler rate(15 min) rule (metron-ops#111).
#
# The Lambda runs collectors/crypto_balances.collect() around the clock: reads Metron's
# published wallet addresses, fetches BTC/ETH balances + prices, writes crypto/holdings.json.
# A Lambda (not a systemd timer on the trading box) because crypto is 24/7 and the trading
# box stops after EOD. EventBridge-Scheduler conventions mirror the sibling
# scheduled-groom-dispatcher (scheduler.amazonaws.com role, single-target blast radius).
#
# Managed OUTSIDE CloudFormation — operator-deployed. Merging the PR has ZERO live effect
# until an operator runs this with --bootstrap.
#
# Usage:
#   bash .../crypto-balances/deploy.sh             # update code only
#   bash .../crypto-balances/deploy.sh --bootstrap # first-time create + wire EventBridge Scheduler
#   bash .../crypto-balances/deploy.sh --dry-run   # show actions, do not apply
#   bash .../crypto-balances/deploy.sh --smoke     # invoke once (⚠ writes a REAL crypto/holdings.json)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
FUNCTION_NAME="alpha-engine-crypto-balances"
ROLE_NAME="alpha-engine-crypto-balances-role"
POLICY_NAME="alpha-engine-crypto-balances-policy"
SCHED_ROLE_NAME="alpha-engine-crypto-balances-scheduler-role"
SCHED_POLICY_NAME="invoke-crypto-balances"
SCHED_NAME="alpha-engine-crypto-balances-15min"
SCHED_CRON="rate(15 minutes)"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

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

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Running handler unit tests..."
  python3 -m pytest "${SCRIPT_DIR}/test_handler.py" -q
fi

# ----- 1. Package: pip install deps + vendor the collector + zip -------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (pip install -t)..."
python3 -m pip install --quiet --target "${PKG}" --upgrade -r "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
# Vendor the tested collector flat next to the handler (it has no intra-repo imports).
cp "${REPO_ROOT}/collectors/crypto_balances.py" "${PKG}/crypto_balances.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline least-privilege policy ---
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  # --- 2b. Lambda function ---
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 --role "${ROLE_ARN}" --handler index.handler \
      --zip-file "fileb://${ZIP}" --timeout 60 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,CRYPTO_BALANCES_ENABLED=true,MARKET_DATA_BUCKET=alpha-engine-research}' \
      --region "${REGION}" --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # --- 2c. EventBridge Scheduler execution role (invoke this Lambda only) ---
  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} every 15 min" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"${FN_ARN}\"}]}"
  echo "  Applying Scheduler invoke policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}" --policy-document "${SCHED_INVOKE_POLICY}"

  if ! $DRY_RUN; then echo "  Waiting 10s for Scheduler role propagation..."; sleep 10; fi

  # --- 2d. The EventBridge Scheduler rule (rate 15 min, 24/7) ---
  TARGET="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${SCHED_ROLE_ARN}\",\"Input\":\"{}\"}"
  if aws scheduler get-schedule --name "${SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
    echo "  Updating Scheduler rule: ${SCHED_NAME} → ${SCHED_CRON}"
    run aws scheduler update-schedule --name "${SCHED_NAME}" \
      --schedule-expression "${SCHED_CRON}" --flexible-time-window '{"Mode":"OFF"}' \
      --target "${TARGET}" --region "${REGION}" --query 'ScheduleArn' --output text
  else
    echo "  Creating Scheduler rule: ${SCHED_NAME} → ${SCHED_CRON}"
    run aws scheduler create-schedule --name "${SCHED_NAME}" \
      --schedule-expression "${SCHED_CRON}" --flexible-time-window '{"Mode":"OFF"}' \
      --target "${TARGET}" --region "${REGION}" --query 'ScheduleArn' --output text
  fi
  if ! $DRY_RUN; then
    aws scheduler get-schedule --name "${SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null \
      || { echo "ERROR: Scheduler rule ${SCHED_NAME} not found after create/update" >&2; exit 1; }
  fi
fi

# ----- 3. Update function code (idempotent) ---------------------------------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi
echo "✓ Code deployed."

# ----- 4. Smoke (real invoke — writes crypto/holdings.json if addresses exist) ---

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (⚠ a real run — writes crypto/holdings.json)..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out --payload '{}' \
    --region "${REGION}" "${RESP}" >/dev/null
  cat "${RESP}"; echo ""
  rm -f "${RESP}"
fi
