#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-scheduled-groom-dispatcher Lambda
# and wire its EventBridge Scheduler rules (config#1322, #1432).
#
# Each Scheduler rule fires THIS Lambda on cadence; the Lambda LAUNCHES A
# DEDICATED EC2 SPOT BOX (nousergon_lib.ec2_spot, on-demand fallback) and fires an
# async SSM command that clones nousergon/alpha-engine-config and runs the SAME
# scripts/groom_run.sh entrypoint the GHA workflow uses, then self-terminates.
# This moves the heavy ~hours-long groom OFF the org's 2,000 included PRIVATE-repo
# GHA Actions minutes (config#1432; was: a repository_dispatch into backlog-groom.yml).
# EventBridge Scheduler conventions (scheduler.amazonaws.com role, cron()
# expression, flexible-time-window) mirror `infrastructure/run_weekly_offcycle.sh`.
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole (the
# executor role) + ssm:SendCommand. The BOX reads all secrets itself from SSM via
# its instance profile (alpha-engine-executor-role → ssm:GetParameter on
# /alpha-engine/*), so the Lambda needs NO secret access.
#
# Cadence (UTC, mirrors the GHA crons exactly). Reduced 3->2/day on 2026-06-29
# (the 15:00 UTC / 8am-PT run was dropped per usage pacing):
#   07:00 Sun-Fri   cron(0 7 ? * SUN-FRI *)   FULL   # 12am PT, skips Sat
#   23:00 daily     cron(0 23 * * ? *)        FULL   # 4pm PT, every day incl. Sat
#
# SCHED_NAMES is the source of truth: any live scheduler rule under the
# alpha-engine-scheduled-groom- prefix that is NOT in SCHED_NAMES is PRUNED
# (deleted) on deploy, so removing a cadence here removes it live too.
#
# Managed OUTSIDE CloudFormation — same rationale as the sibling dispatchers
# (keeps the github-actions-lambda-deploy OIDC role's blast radius narrow;
# operator-deployed only). Merging the PR has ZERO live effect until an operator
# runs this with --bootstrap. CUTOVER (config#1432): after a manual --smoke spot
# run validates end-to-end, deploy this AND disable the GHA `schedule:` crons in
# backlog-groom.yml together (so there is no double-groom and no gap). NOTE:
# --smoke fires a REAL groom on a REAL spot box.
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
# Mirrors backlog-groom.yml's `schedule:` crons one-for-one.
SCHED_NAMES=(
  "alpha-engine-scheduled-groom-0700-sunfri"
  "alpha-engine-scheduled-groom-2300-daily"
)
SCHED_CRONS=(
  "cron(0 7 ? * SUN-FRI *)"
  "cron(0 23 * * ? *)"
)
SCHED_INPUTS=(
  '{"run_mode":"full","schedule":"0 7 * * 0-5"}'
  '{"run_mode":"full","schedule":"0 23 * * *"}'
)
# Prefix used to discover live rules for prune reconciliation (see step 2d).
SCHED_PREFIX="alpha-engine-scheduled-groom-"

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
      --timeout 300 \
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

  # --- 2d. The EventBridge Scheduler rules ---
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

  # --- 2e. Prune reconciliation: delete any live rule under SCHED_PREFIX that is
  # no longer in SCHED_NAMES (so dropping a cadence above removes it live too,
  # rather than silently orphaning a still-firing schedule). Added 2026-06-29
  # alongside the 3->2/day reduction (the 1500-sunfri rule is the first prunee).
  echo "  Pruning orphaned Scheduler rules under prefix ${SCHED_PREFIX}..."
  LIVE_RULES=$(aws scheduler list-schedules --name-prefix "${SCHED_PREFIX}" \
    --region "${REGION}" --query 'Schedules[].Name' --output text 2>/dev/null || echo "")
  for live in ${LIVE_RULES}; do
    keep=false
    for want in "${SCHED_NAMES[@]}"; do
      [ "${live}" = "${want}" ] && { keep=true; break; }
    done
    if ! $keep; then
      echo "    Deleting orphaned Scheduler rule: ${live}"
      run aws scheduler delete-schedule --name "${live}" --region "${REGION}"
      if ! $DRY_RUN; then
        aws scheduler get-schedule --name "${live}" --region "${REGION}" \
          --query 'Name' --output text >/dev/null 2>&1 \
          && { echo "ERROR: Scheduler rule ${live} still present after delete" >&2; exit 1; }
      fi
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
