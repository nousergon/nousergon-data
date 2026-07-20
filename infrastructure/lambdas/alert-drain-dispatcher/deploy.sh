#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-alert-drain-dispatcher Lambda
# + the twice-daily router-targeting schedules (alpha-engine-config-I2824).
#
# Launch leg of the Overseer alert-drain (epic I2821 phase 3). The POLICY
# lives in alpha-engine-config's .github/alert-drain-prompt.md charter; this
# Lambda only launches the spot box that runs it. Dispatch path: EventBridge
# Scheduler -> alpha-engine-overseer-dispatcher router (playbook alert-drain)
# -> THIS Lambda -> spot box. The schedules deliberately target the ROUTER,
# not this executor — kill switches, verdict escalation, and the dispatch
# ledger all live there (phase-2 coherence).
#
# --bootstrap creates: (1) this Lambda's execution role + inline policy,
# (2) the Lambda, (3) the shared alpha-engine-overseer-scheduler-role
# (invoke-router-only — reusable by any future router-targeting schedule,
# e.g. the I2832 drill re-point), (4) TWO daily EventBridge Scheduler
# schedules (10:00 + 22:00 UTC — both off US market hours year-round).
#
# Managed OUTSIDE CloudFormation like the sibling dispatchers. Flagless run
# is code-only (GHA auto-deploy path); --bootstrap is operator-only.
#
# Usage:
#   bash .../alert-drain-dispatcher/deploy.sh             # update code only
#   bash .../alert-drain-dispatcher/deploy.sh --bootstrap # operator-only: role + Lambda + schedules
#   bash .../alert-drain-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../alert-drain-dispatcher/deploy.sh --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-alert-drain-dispatcher"
ROLE_NAME="alpha-engine-alert-drain-dispatcher-role"
POLICY_NAME="alpha-engine-alert-drain-dispatcher-policy"
ROUTER_FUNCTION="alpha-engine-overseer-dispatcher"
SCHED_ROLE_NAME="alpha-engine-overseer-scheduler-role"
SCHED_POLICY_NAME="invoke-overseer-dispatcher"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
ROUTER_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${ROUTER_FUNCTION}"
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,ALERT_DRAIN_DISPATCH_ENABLED=true}'

source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# ----- 0. Validate handler syntax + preflight unit tests ---------------------
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
ast.parse(open('${SCRIPT_DIR}/index.py').read())
print('index.py syntax OK')
"

source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
NOUSERGON_LIB_REQ=$(grep -E '^nousergon-lib' "${SCRIPT_DIR}/requirements.txt" | head -1)
KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" "${KREPIS_REQ}" "${NOUSERGON_LIB_REQ}"

# ----- 1. Package ------------------------------------------------------------
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap ----------------------------------------------------------
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
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"
  if ! $DRY_RUN; then echo "  Waiting 10s for IAM propagation..."; sleep 10; fi

  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    # Timeout 300s: the launch leg waits for spot capacity + SSM online (the
    # sibling dispatchers use the same headroom class).
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --architectures x86_64 \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
      --timeout 300 \
      --memory-size 256 \
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda function exists: ${FUNCTION_NAME}"
  fi

  # --- Scheduler role (router-invoke-only; shared by future router schedules) ---
  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler role exists: ${SCHED_ROLE_NAME}"
  fi
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"lambda:InvokeFunction\",\"Resource\":\"${ROUTER_ARN}\"}]}"
  if ! $DRY_RUN; then sleep 10; fi

  # --- Twice-daily drain schedules -> ROUTER (playbook alert-drain) ----------
  # 10:00 UTC (pre-market, after overnight alert accrual) + 22:00 UTC
  # (post-close, after the daily pipelines) — both outside US market hours
  # (13:30-20:00 UTC standard AND daylight time).
  for slot in 1000 2200; do
    HH="${slot:0:2}"
    SCHED_NAME="alpha-engine-alert-drain-${slot}utc"
    INPUT="{\"playbook\":\"alert-drain\",\"payload\":{\"trigger\":\"scheduled-${slot}utc\"}}"
    if aws scheduler get-schedule --name "${SCHED_NAME}" --region "${REGION}" >/dev/null 2>&1; then
      echo "  Schedule exists: ${SCHED_NAME} (updating)"
      VERB=update-schedule
    else
      VERB=create-schedule
    fi
    # Input must be a JSON-ESCAPED string inside the target JSON.
    INPUT_ESCAPED=$(printf '%s' "$INPUT" | python3 -c "import json,sys; print(json.dumps(sys.stdin.read()))")
    # config#2902: zero-retry on the router-targeting schedule — AWS Scheduler
    # defaults to MaximumRetryAttempts=185, which would re-dispatch this
    # payload for up to a day on any transient router error. The router's
    # clean-JSON-never-raise contract + watch-plane Errors alarm are the
    # intended failure surface, not scheduler-level retry.
    run aws scheduler "${VERB}" \
      --name "${SCHED_NAME}" \
      --schedule-expression "cron(0 ${HH} * * ? *)" \
      --flexible-time-window '{"Mode":"OFF"}' \
      --description "Overseer alert-drain ${slot} UTC daily via the overseer-dispatcher router (alpha-engine-config-I2824)" \
      --target "{\"Arn\":\"${ROUTER_ARN}\",\"RoleArn\":\"arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}\",\"Input\":${INPUT_ESCAPED},\"RetryPolicy\":{\"MaximumRetryAttempts\":0,\"MaximumEventAgeInSeconds\":60}}" \
      --region "${REGION}" > /dev/null || echo "  WARN: ${VERB} ${SCHED_NAME} failed"
  done
fi

# ----- 3. Update code (always) -----------------------------------------------
echo "Updating ${FUNCTION_NAME} code..."
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
  CURRENT_ENABLED=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" ALERT_DRAIN_DISPATCH_ENABLED true)
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "Variables={LOG_LEVEL=INFO,ALERT_DRAIN_DISPATCH_ENABLED=${CURRENT_ENABLED}}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "Done."
