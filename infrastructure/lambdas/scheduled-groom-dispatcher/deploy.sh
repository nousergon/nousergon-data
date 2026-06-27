#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-scheduled-groom-dispatcher Lambda
# and wire its three EventBridge Scheduler rules (the reliable replacement for
# the backlog-groom GHA `schedule:` crons — config#1322).
#
# Each Scheduler rule fires THIS Lambda on cadence; the Lambda repository_
# dispatches the backlog groom (nousergon/alpha-engine-config :: backlog-groom.yml,
# type `scheduled-groom`) carrying the run-mode in client_payload. Mirrors the
# sibling `saturday-sf-success-groom-dispatcher`; EventBridge Scheduler conventions
# (scheduler.amazonaws.com role, cron() expression, flexible-time-window) mirror
# `infrastructure/run_weekly_offcycle.sh`.
#
# Cadence (UTC, mirrors the GHA crons exactly):
#   07:00 Sun-Fri   cron(0 7 ? * SUN-FRI *)   FULL   # 12am PT, skips Sat
#   15:00 Sun-Fri   cron(0 15 ? * SUN-FRI *)  FULL   # 8am PT, skips Sat
#   23:00 daily     cron(0 23 * * ? *)        FULL   # 4pm PT, every day incl. Sat
#
# Managed OUTSIDE CloudFormation — same rationale as the sibling dispatchers
# (keeps the github-actions-lambda-deploy OIDC role's blast radius narrow;
# operator-deployed only). Merging the PR has ZERO live effect until an operator
# runs this with --bootstrap, AND the GHA `schedule:` crons stay live as a
# backstop until a multi-day on-time-firing soak confirms this path.
#
# Usage:
#   bash .../scheduled-groom-dispatcher/deploy.sh             # update code only
#   bash .../scheduled-groom-dispatcher/deploy.sh --bootstrap # first-time create + wire EventBridge Scheduler
#   bash .../scheduled-groom-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../scheduled-groom-dispatcher/deploy.sh --smoke     # invoke once with a synthetic schedule event (⚠ fires a REAL groom)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-scheduled-groom-dispatcher"
ROLE_NAME="alpha-engine-scheduled-groom-dispatcher-role"
POLICY_NAME="alpha-engine-scheduled-groom-dispatcher-policy"
# EventBridge Scheduler execution role (assumed by scheduler.amazonaws.com to
# invoke the Lambda). Single-target blast radius: lambda:InvokeFunction on this
# function only.
SCHED_ROLE_NAME="alpha-engine-scheduled-groom-dispatcher-scheduler-role"
SCHED_POLICY_NAME="invoke-scheduled-groom-dispatcher"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

# Schedule definitions: name | cron expression (UTC) | JSON input (run_mode + label).
# Mirrors backlog-groom.yml's three `schedule:` crons one-for-one.
SCHED_NAMES=(
  "alpha-engine-scheduled-groom-0700-sunfri"
  "alpha-engine-scheduled-groom-1500-sunfri"
  "alpha-engine-scheduled-groom-2300-daily"
)
SCHED_CRONS=(
  "cron(0 7 ? * SUN-FRI *)"
  "cron(0 15 ? * SUN-FRI *)"
  "cron(0 23 * * ? *)"
)
SCHED_INPUTS=(
  '{"run_mode":"full","schedule":"0 7 * * 0-5"}'
  '{"run_mode":"full","schedule":"0 15 * * 0-5"}'
  '{"run_mode":"full","schedule":"0 23 * * *"}'
)

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

  # --- 2a. Lambda execution role + inline policy ---
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

  # --- 2b. Lambda function ---
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
      --environment 'Variables={LOG_LEVEL=INFO,GROOM_DISPATCH_ENABLED=true}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # --- 2c. EventBridge Scheduler execution role (invoke this Lambda only) ---
  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role \
      --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} on the groom cadence" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["lambda:InvokeFunction"],"Resource":"${FN_ARN}"}]}
EOF
)
  echo "  Applying Scheduler invoke policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "${SCHED_INVOKE_POLICY}"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for Scheduler role propagation..."
    sleep 10
  fi

  # --- 2d. The three EventBridge Scheduler rules ---
  for i in "${!SCHED_NAMES[@]}"; do
    name="${SCHED_NAMES[$i]}"
    cron="${SCHED_CRONS[$i]}"
    input="${SCHED_INPUTS[$i]}"
    target=$(cat <<EOF
{"Arn":"${FN_ARN}","RoleArn":"${SCHED_ROLE_ARN}","Input":"$(printf '%s' "$input" | sed 's/"/\\"/g')"}
EOF
)
    if aws scheduler get-schedule --name "${name}" --region "${REGION}" \
        --query 'Name' --output text >/dev/null 2>&1; then
      echo "  Updating Scheduler rule: ${name} → ${cron}"
      run aws scheduler update-schedule \
        --name "${name}" \
        --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" \
        --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" \
        --region "${REGION}" \
        --query 'ScheduleArn' --output text
    else
      echo "  Creating Scheduler rule: ${name} → ${cron}"
      run aws scheduler create-schedule \
        --name "${name}" \
        --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" \
        --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" \
        --region "${REGION}" \
        --query 'ScheduleArn' --output text
    fi
    # Fail-loud: verify it landed.
    if ! $DRY_RUN; then
      aws scheduler get-schedule --name "${name}" --region "${REGION}" \
        --query 'Name' --output text >/dev/null \
        || { echo "ERROR: Scheduler rule ${name} not found after create/update" >&2; exit 1; }
    fi
  done
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

# ----- 4. Smoke (synthetic schedule event) ----------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic schedule event, run_mode=full)..."
  RESP=$(mktemp)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{"run_mode":"full","schedule":"smoke-test"}' \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
