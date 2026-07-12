#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-ci-watch-dispatcher Lambda.
#
# WHY (config#1432-style migration, see index.py's module docstring): Fleet CI
# Watch (`sf-watch`, alpha-engine-config) diagnoses+fixes fleet CI failures on
# GitHub-hosted Actions runners, burning the org's metered Actions-minutes
# budget — currently gated to Saturday-only as a stopgap. This Lambda moves it
# to EC2 spot, mirroring the PROVEN scheduled-groom-dispatcher pattern in THIS
# repo, but MUCH SIMPLER: no Step Function — CI-watch is invoked directly via
# a SYNCHRONOUS `lambda invoke` from a GHA job (built by a sibling agent in
# alpha-engine-config's sf-watch.yml) once per real CI failure event, not on a
# cron cadence. --bootstrap creates: (1) this Lambda's OWN execution role +
# inline policy, (2) the Lambda function itself, (3) the weekly canary drill
# Scheduler rule + its dedicated invoke role (config#2223, step 2c) — the one
# standing schedule, which fires a synthetic `is_drill` payload through the
# same pipe.
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole
# (scoped to alpha-engine-ci-watch-executor-role — a NEW, dedicated role a
# sibling agent is creating in alpha-engine-config, deliberately NOT the
# shared trading alpha-engine-executor-role) + ssm:SendCommand. The BOX reads
# its own run secrets (PAT) via ITS instance profile, so this Lambda needs no
# secret access of its own.
#
# Managed OUTSIDE CloudFormation (same rationale as the sibling dispatchers):
# keeps the github-actions-lambda-deploy OIDC role's blast radius narrow — it
# deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet-wide policy after
# 4 IAM-clobber incidents in 2 months; see infrastructure/iam/README.md if
# present). This script's FLAGLESS run is already code-only (this is what the
# GHA auto-deploy workflow calls); --bootstrap is what ADDS IAM-role-creation +
# Lambda-function-creation on top, operator-run only, never in CI.
#
# Usage:
#   bash .../ci-watch-dispatcher/deploy.sh             # update code only (also the CI auto-deploy path)
#   bash .../ci-watch-dispatcher/deploy.sh --bootstrap # operator-only: create/update the IAM roles + Lambda function + weekly canary drill schedule (config#2223)
#   bash .../ci-watch-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../ci-watch-dispatcher/deploy.sh --smoke     # invoke once with a synthetic event (fires a REAL spot box)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-ci-watch-dispatcher"
ROLE_NAME="alpha-engine-ci-watch-dispatcher-role"
POLICY_NAME="alpha-engine-ci-watch-dispatcher-policy"
# Role EventBridge Scheduler assumes to fire the weekly canary drill
# (config#2223). Created in --bootstrap only; may ONLY invoke THIS function.
# (sf-watch-spot-dispatcher reuses its existing defer-scheduler role for the
# same purpose; ci-watch has no prior scheduler role, hence a dedicated one.)
CANARY_SCHED_ROLE_NAME="alpha-engine-ci-watch-canary-scheduler-role"
CANARY_SCHED_POLICY_NAME="invoke-ci-watch-dispatcher"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
CANARY_SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${CANARY_SCHED_ROLE_NAME}"
# Bootstrap default (first-time deployment only) — sets CI_WATCH_DISPATCH_ENABLED=true
# as the safe default. The update path (step 3) will read the live value and preserve it.
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,CI_WATCH_DISPATCH_ENABLED=true}'

# Shared operator-flag-preserve helper (config#1818/#2236/#2264 bug class).
source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

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

# ----- 0. Scratch dirs + validate handler syntax -----------------------------
# PKG and TEST_DEPS are both created up front (mirrors scheduled-groom-
# dispatcher/deploy.sh) so ONE trap covers both — a pytest-install failure
# below still cleans up.

PKG=$(mktemp -d)
TEST_DEPS=$(mktemp -d)
trap "rm -rf '$PKG' '$TEST_DEPS'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------
# Hermetic for AWS: boto3 + nousergon_lib.ec2_spot are stubbed in sys.modules
# before `import index` (see test_handler.py). The pinned nousergon-lib +
# krepis are installed for real into a scratch TEST_DEPS dir — NOT the
# caller's global site-packages, not bundled into the Lambda zip.
if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  NOUSERGON_LIB_REQ=$(grep -E '^nousergon-lib' "${SCRIPT_DIR}/requirements.txt" | head -1)
  KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
  echo "Installing pytest + krepis + pinned nousergon-lib into ${TEST_DEPS}..."
  python3 -m pip install --quiet --target "${TEST_DEPS}" pytest "${KREPIS_REQ}" "${NOUSERGON_LIB_REQ}"
  echo "Running handler unit tests..."
  PYTHONPATH="${TEST_DEPS}" python3 -m pytest "${SCRIPT_DIR}/test_handler.py" -q
fi

# ----- 1. Package: pip install deps + zip handler ---------------------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

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
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # --- 2c. Weekly canary drill schedule (config#2223) ---
  # Exercises the WHOLE dispatch pipe (Lambda IAM -> scoped RunInstances ->
  # SSM -> bootstrap start) without a real CI failure. The box short-circuits
  # before the agent and writes the _canary heartbeat the Fleet Status page
  # escalates on when missed. 30 min after the sf-watch drill (15:00 UTC) so
  # the two drills never contend. Idempotent create-or-update, mirrors
  # sf-watch-liveness-probe/deploy.sh's scheduler style.
  CANARY_SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${CANARY_SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${CANARY_SCHED_ROLE_NAME}"
    run aws iam create-role --role-name "${CANARY_SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${CANARY_SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} for the weekly canary drill (config#2223)" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${CANARY_SCHED_ROLE_NAME}"
  fi
  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  CANARY_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"lambda:InvokeFunction\",\"Resource\":[\"${FN_ARN}\",\"${FN_ARN}:*\"]}]}"
  echo "  Applying Scheduler invoke policy: ${CANARY_SCHED_POLICY_NAME}"
  run aws iam put-role-policy --role-name "${CANARY_SCHED_ROLE_NAME}" \
    --policy-name "${CANARY_SCHED_POLICY_NAME}" \
    --policy-document "${CANARY_INVOKE_POLICY}"
  if ! $DRY_RUN; then echo "  Waiting 10s for Scheduler role propagation..."; sleep 10; fi

  CANARY_SCHED_NAME="alpha-engine-ci-watch-canary-drill-weekly"
  CANARY_CRON="cron(30 15 ? * WED *)"
  # Static Input: the drill's ENTIRE identity (repo/sha/run_id/...) is
  # synthesized in index.py so no payload can carry a real (repo, sha) into
  # a drill (see index.py DRILL_REPO isolation invariant).
  CANARY_TARGET="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${CANARY_SCHED_ROLE_ARN}\",\"Input\":\"{\\\"is_drill\\\":\\\"true\\\"}\"}"
  if aws scheduler get-schedule --name "${CANARY_SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
    echo "  Updating Scheduler rule: ${CANARY_SCHED_NAME} → ${CANARY_CRON}"
    run aws scheduler update-schedule --name "${CANARY_SCHED_NAME}" \
      --schedule-expression "${CANARY_CRON}" --schedule-expression-timezone "UTC" \
      --flexible-time-window '{"Mode":"OFF"}' --target "${CANARY_TARGET}" \
      --region "${REGION}" --query 'ScheduleArn' --output text
  else
    echo "  Creating Scheduler rule: ${CANARY_SCHED_NAME} → ${CANARY_CRON}"
    run aws scheduler create-schedule --name "${CANARY_SCHED_NAME}" \
      --schedule-expression "${CANARY_CRON}" --schedule-expression-timezone "UTC" \
      --flexible-time-window '{"Mode":"OFF"}' --target "${CANARY_TARGET}" \
      --region "${REGION}" --query 'ScheduleArn' --output text
  fi
  if ! $DRY_RUN; then
    aws scheduler get-schedule --name "${CANARY_SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null \
      || { echo "ERROR: Scheduler rule ${CANARY_SCHED_NAME} not found after create/update" >&2; exit 1; }
  fi
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

echo "Updating Lambda environment (preserving operator-owned CI_WATCH_DISPATCH_ENABLED)..."
# CI_WATCH_DISPATCH_ENABLED is an OPERATOR-OWNED runtime kill-switch — the
# update path must PRESERVE its live value, never reset it to the bootstrap
# default. Mirrors the saturday/spot dispatcher fixes (config#1818/#2236): a
# routine redeploy must not silently re-arm the operator's containment flag.
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" CI_WATCH_DISPATCH_ENABLED true)
LAMBDA_ENV="Variables={LOG_LEVEL=INFO,CI_WATCH_DISPATCH_ENABLED=${CURRENT_DISPATCH}}"
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${LAMBDA_ENV}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

# ----- 4. Smoke (synthetic event, direct invoke) -----------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic CI-failure event)..."
  echo "⚠ this fires a REAL spot box + REAL ci_watch_spot_bootstrap.sh run."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"repo":"nousergon/alpha-engine-config","sha":"0000000000000000000000000000000000000000","run_id":"999999999","run_url":"https://github.com/nousergon/alpha-engine-config/actions/runs/999999999","workflow":"smoke-test","branch":"main"}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
fi
