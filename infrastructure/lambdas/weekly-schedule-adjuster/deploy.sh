#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-weekly-schedule-adjuster Lambda
# and its weekly EventBridge tick.
#
# The Lambda runs mid-week (Wed 06:00 UTC) and reconciles the LIVE weekly-run
# schedule to the NYSE calendar for the current week's weekend: normal weeks
# leave the CFN-owned alpha-engine-saturday cron ENABLED; trailing-holiday
# weeks (Good Friday; Friday-observed July-4/Christmas) DISABLE it and stand up
# a one-shot rule on the earlier run day (the day after the week's last trading
# session). Fail-safe: a broken adjuster degrades to the normal Saturday run,
# never a missed run (see index.py header).
#
# Managed OUTSIDE CloudFormation — same rationale as the sibling event Lambdas
# (eod-success-friday-shell-trigger, sf-telegram-notifier, spot-orphan-reaper):
# keeps the github-actions-lambda-deploy OIDC role's blast radius narrow.
#
# Usage:
#   bash infrastructure/lambdas/weekly-schedule-adjuster/deploy.sh             # update code only
#   bash infrastructure/lambdas/weekly-schedule-adjuster/deploy.sh --bootstrap # first-time create + wire the weekly tick
#   bash infrastructure/lambdas/weekly-schedule-adjuster/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-weekly-schedule-adjuster"
ROLE_NAME="alpha-engine-weekly-schedule-adjuster-role"
POLICY_NAME="alpha-engine-weekly-schedule-adjuster-policy"
RULE_NAME="alpha-engine-weekly-schedule-adjuster"
# Wednesday 06:00 UTC — mid-week, well before the Fri/Sat weekend run so a
# holiday-shift is in place with days to spare.
SCHEDULE_EXPR="cron(0 6 ? * WED *)"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

DRY_RUN=false
BOOTSTRAP=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# ----- 0. Validate handler + run unit tests ---------------------------------
python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"
# ----- Preflight handler unit tests (shared gate — config#2381) -------------
# Delegates to the one _shared/run_handler_tests.sh so this gate can never
# re-drift into the naive no-install `python3 -m pytest` form (config#2295).
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

# ----- 1. Package: pip install deps + zip handler ---------------------------
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
echo "Installing deps into ${PKG} (pip install -t)..."
python3 -m pip install --quiet --target "${PKG}" --upgrade -r "${SCRIPT_DIR}/requirements.txt"
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
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi
  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"
  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 --role "${ROLE_ARN}" --handler index.handler \
      --zip-file "fileb://${ZIP}" --timeout 60 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO}' --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  echo "  Creating weekly EventBridge tick: ${RULE_NAME} (${SCHEDULE_EXPR})"
  run aws events put-rule --name "${RULE_NAME}" --schedule-expression "${SCHEDULE_EXPR}" \
    --state ENABLED --description "Weekly reconcile of the holiday-shifted weekly-SF run day" \
    --region "${REGION}" --query 'RuleArn' --output text
  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  run aws events put-targets --rule "${RULE_NAME}" --targets "Id=1,Arn=${FN_ARN}" --region "${REGION}"
  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run aws lambda add-permission --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${RULE_NAME}" --action lambda:InvokeFunction \
    --principal events.amazonaws.com --source-arn "${RULE_ARN}" --region "${REGION}" 2>/dev/null || true
fi

# ----- 3. Update function code (always, idempotent) -------------------------
echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"; fi
echo "✓ Code deployed."
