#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-eod-success-friday-shell-trigger
# Lambda and wire its EventBridge EOD-SUCCEEDED trigger.
#
# This Lambda subscribes to `aws.states` / "Step Functions Execution Status
# Change" events for `ne-postclose-trading-pipeline` SUCCEEDED transitions only.
# On every EOD success it derives trading_day via the canonical
# `nousergon_lib.trading_calendar.last_closed_trading_day` (handles UTC
# rollover) and, if trading_day.weekday()==4 (Friday), invokes the Saturday
# Step Function with `shell_run: true`.
#
# Replaced the prior fixed-time cron rule `alpha-engine-friday-shell-run`
# (cron(45 20 ? * FRI *) = 13:45 PT Friday), which ran DISABLED from the
# 2026-05-21 cutover and was RETIRED from CloudFormation 2026-05-29 (ROADMAP
# L4055) after the event-driven path confirmed across two Fridays (5/22 + 5/29).
#
# Managed outside CloudFormation — same rationale as sf-telegram-notifier +
# spot-orphan-reaper + changelog-cloudwatch-mirror (keeps the
# github-actions-lambda-deploy OIDC role's blast radius narrow;
# operator-deployed only).
#
# Usage:
#   bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh             # update code only
#   bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh --bootstrap # first-time create + wire EventBridge
#   bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh --smoke     # invoke once with a synthetic Fri-SUCCEEDED event

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-eod-success-friday-shell-trigger"
ROLE_NAME="alpha-engine-eod-success-friday-shell-trigger-role"
POLICY_NAME="alpha-engine-eod-success-friday-shell-trigger-policy"
RULE_NAME="alpha-engine-eod-success-friday-shell-trigger"
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
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # EventBridge rule: Step Functions Execution Status Change for EOD SF SUCCEEDED only
  echo "  Creating EventBridge rule: ${RULE_NAME}"
  EVENT_PATTERN=$(cat <<EOF
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "stateMachineArn": [
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline"
    ],
    "status": ["SUCCEEDED"]
  }
}
EOF
)
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --event-pattern "${EVENT_PATTERN}" \
    --description "Trigger Saturday SF shell-run on Friday EOD SUCCEEDED (Lambda day-of-week-guards)" \
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

# ----- 4. Smoke (synthetic Friday-SUCCEEDED event) --------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic Friday-EOD SUCCEEDED event)..."
  RESP=$(mktemp)
  # 2026-05-22 (Friday) 20:25 UTC = 13:25 PT — normal Friday EOD success window
  PAYLOAD=$(cat <<'EOF'
{
  "source": "aws.states",
  "detail-type": "Step Functions Execution Status Change",
  "detail": {
    "status": "SUCCEEDED",
    "stateMachineArn": "arn:aws:states:us-east-1:711398986525:stateMachine:ne-postclose-trading-pipeline",
    "executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-postclose-trading-pipeline:smoke-test",
    "name": "smoke-test",
    "startDate": 1779827700000,
    "stopDate": 1779828300000
  }
}
EOF
)
  echo "WARNING: --smoke will START a real saturday-pipeline shell run if the embedded date is a Friday."
  echo "         Confirm before proceeding or omit --smoke and rely on unit tests + first live event."
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
