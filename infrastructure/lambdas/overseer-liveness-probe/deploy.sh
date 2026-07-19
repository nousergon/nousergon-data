#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-overseer-liveness-probe Lambda
# and wire its EventBridge Scheduler rules.
#
# WHY (alpha-engine-config-I2831, epic I2821): ONE registry-driven watch-plane
# liveness probe. It iterates infrastructure/overseer/playbooks.yaml (BUNDLED
# into the zip here, same as overseer-dispatcher) and runs each playbook's
# declared `liveness` checks + the top-level `watch_plane_liveness` checks —
# config-drift wiring (EventBridge rules / Step Functions / dispatcher Lambdas /
# launch config) AND groom-style run-window accounting — replacing the two
# per-probe enumerations (sf-watch-liveness-probe's wiring checks, migrated here;
# groom-liveness-probe, fully migrated + deleted). Read-only + silent-unless-
# broken. The sf-watch reclaim-checker (config#2270) + disabled-window sweep
# (config#2257) ACTION paths STAY in the slimmed sf-watch-liveness-probe.
#
# IAM (iam-policy.json): logs + ssm:GetParameter (Telegram creds) + read-only
# events:DescribeRule/ListTargetsByRule + states:DescribeStateMachine +
# states:ListExecutions/DescribeExecution (sf_watch_invocation_success,
# config#2901) + lambda:GetFunctionConfiguration +
# ec2:Describe{Images,SecurityGroups,Subnets} + sqs:GetQueueUrl + s3 Get/List
# on the run-window + sf-watch-invocation-log prefixes + s3 Get/Put on the
# dedup state key + dynamodb on the flow-doctor store. NO InvokeFunction / EC2
# mutate — this probe never acts.
#
# Cadence (UTC): 4x daily (config#2901 bump from 2x — every check here is a
# cheap read-only API call, and halving worst-case detection latency to ~6h
# is worth it for a live-incident watch plane), offset :50 past the hour to
# avoid colliding with the slimmed sf-watch probe's own sweep cadence
# (06:45/14:45) — this is a config-drift + run-window/invocation-success
# check, not tied to any pipeline's own schedule:
#   02:50 daily   cron(50 2 * * ? *)
#   08:50 daily   cron(50 8 * * ? *)
#   14:50 daily   cron(50 14 * * ? *)
#   20:50 daily   cron(50 20 * * ? *)
#
# Managed OUTSIDE CloudFormation — mirrors the sibling dispatchers/probes
# (narrow OIDC blast radius, operator-deployed only). Merging the PR has ZERO
# live effect until an operator runs this with --bootstrap.
#
# Usage:
#   bash .../overseer-liveness-probe/deploy.sh             # update code only
#   bash .../overseer-liveness-probe/deploy.sh --bootstrap # first-time create + wire schedules
#   bash .../overseer-liveness-probe/deploy.sh --dry-run   # show actions, do not apply
#   bash .../overseer-liveness-probe/deploy.sh --smoke     # invoke once (read-only; pings only on a real problem)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-overseer-liveness-probe"
ROLE_NAME="alpha-engine-overseer-liveness-probe-role"
POLICY_NAME="alpha-engine-overseer-liveness-probe-policy"
SCHED_ROLE_NAME="alpha-engine-overseer-liveness-probe-scheduler-role"
SCHED_POLICY_NAME="invoke-overseer-liveness-probe"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

SCHED_NAMES=(
  "alpha-engine-overseer-liveness-0250-daily"
  "alpha-engine-overseer-liveness-0850-daily"
  "alpha-engine-overseer-liveness-1450-daily"
  "alpha-engine-overseer-liveness-2050-daily"
)
SCHED_CRONS=(
  "cron(50 2 * * ? *)"
  "cron(50 8 * * ? *)"
  "cron(50 14 * * ? *)"
  "cron(50 20 * * ? *)"
)
SCHED_PREFIX="alpha-engine-overseer-liveness-"

# DRY_RUN honors an ambient env var (true/1/yes) as well as --dry-run, so
# DRY_RUN=1/true from a caller's shell no-ops instead of silently deploying
# (alpha-engine-config-I2752 incident class, 2026-07-16).
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
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3 pyyaml -r "${SCRIPT_DIR}/requirements.txt"

# ----- 1. Package: pip install deps + zip handler + bundle registry ---------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/../flow_doctor_telegram.py" "${PKG}/flow_doctor_telegram.py"
# The playbook registry is this probe's check table — bundled from the repo
# SSoT so a registry edit deploys through the normal code path (mirrors
# overseer-dispatcher/deploy.sh; pinned by
# tests/test_overseer_playbook_registry.py::test_liveness_probe_bundles_registry).
cp "${SCRIPT_DIR}/../../overseer/playbooks.yaml" "${PKG}/playbooks.yaml"
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
      --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1,ACCOUNT_ID='"${ACCOUNT_ID}"'}' --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} on the liveness cadence" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"${FN_ARN}\"}]}"
  echo "  Applying Scheduler invoke policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "${SCHED_INVOKE_POLICY}"

  if ! $DRY_RUN; then echo "  Waiting 10s for Scheduler role propagation..."; sleep 10; fi

  for i in "${!SCHED_NAMES[@]}"; do
    name="${SCHED_NAMES[$i]}"
    cron="${SCHED_CRONS[$i]}"
    target="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${SCHED_ROLE_ARN}\",\"Input\":\"{}\"}"
    if aws scheduler get-schedule --name "${name}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
      echo "  Updating Scheduler rule: ${name} → ${cron}"
      run aws scheduler update-schedule --name "${name}" --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" --region "${REGION}" --query 'ScheduleArn' --output text
    else
      echo "  Creating Scheduler rule: ${name} → ${cron}"
      run aws scheduler create-schedule --name "${name}" --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" --region "${REGION}" --query 'ScheduleArn' --output text
    fi
    if ! $DRY_RUN; then
      aws scheduler get-schedule --name "${name}" --region "${REGION}" --query 'Name' --output text >/dev/null \
        || { echo "ERROR: Scheduler rule ${name} not found after create/update" >&2; exit 1; }
    fi
  done

  # Prune reconciliation: delete any live rule under SCHED_PREFIX not in SCHED_NAMES.
  echo "  Pruning orphaned Scheduler rules under prefix ${SCHED_PREFIX}..."
  LIVE_RULES=$(aws scheduler list-schedules --name-prefix "${SCHED_PREFIX}" --region "${REGION}" --query 'Schedules[].Name' --output text 2>/dev/null || echo "")
  for live in ${LIVE_RULES}; do
    keep=false
    for want in "${SCHED_NAMES[@]}"; do [ "${live}" = "${want}" ] && { keep=true; break; }; done
    if ! $keep; then
      echo "    Deleting orphaned Scheduler rule: ${live}"
      run aws scheduler delete-schedule --name "${live}" --region "${REGION}"
    fi
  done
fi

# ----- 3. Update function code (always, idempotent) -------------------------

# New-Lambda safety: the auto-deploy-on-merge workflow runs this flagless (no
# --bootstrap), but the OIDC deploy role cannot create the IAM role / Lambda /
# scheduler rules — an operator must run --bootstrap first. Until then the
# function does not exist, and a bare update-function-code would FAIL the deploy
# workflow (notify-main-failure). Skip gracefully instead, so merging this stays
# GREEN and activation is cleanly deferred to the operator bootstrap.
if ! $BOOTSTRAP && ! aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  echo "NOTE: ${FUNCTION_NAME} is not bootstrapped yet (function does not exist)."
  echo "      Skipping code deploy — an operator must run 'deploy.sh --bootstrap'"
  echo "      first (creates the role + Lambda + 2 scheduler rules)."
  exit 0
fi

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "✓ Code deployed."

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1,ACCOUNT_ID='"${ACCOUNT_ID}"'}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

# ----- 4. Smoke (synthetic invoke; read-only — only pings on a REAL problem) -

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (read-only wiring check)..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out \
    --payload '{}' --region "${REGION}" "${RESP}" >/dev/null
  cat "${RESP}"; echo ""
  rm -f "${RESP}"
fi
