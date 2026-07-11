#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-sf-watch-spot-dispatcher Lambda.
#
# WHY: finishes config#2001 (the saturday-sf-failure half; the ci-main-failure
# half shipped as ci-watch-dispatcher/deploy.sh under the same issue). Fleet-SF
# Watch's diagnose-fix-rerun agent still ran on a GHA-hosted `ubuntu-latest`
# runner for saturday-sf-failure, burning the org's metered Actions-minutes
# budget — exactly the exposure config#2001 was filed to eliminate. This
# Lambda moves it to EC2 spot, mirroring ci-watch-dispatcher's PROVEN pattern
# byte-for-byte in shape: no Step Function, no EventBridge Scheduler rules —
# invoked directly via a SYNCHRONOUS `lambda invoke` from a GHA job (built in
# alpha-engine-config's sf-watch.yml, `sf-watch-dispatch` job) once per real
# saturday-sf-failure event, not on a cron cadence.
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole
# (scoped to alpha-engine-sf-watch-executor-role — a NEW, dedicated role
# created in alpha-engine-config, deliberately NOT the shared trading
# alpha-engine-executor-role NOR the OIDC-only saturday-sf-watch-role) +
# ssm:SendCommand. The BOX reads its own run secrets (PAT) via ITS instance
# profile, so this Lambda needs no secret access of its own.
#
# DEFER-NOT-DROP (config#2226): the Lambda additionally needs
# scheduler:CreateSchedule/GetSchedule (scoped to schedule/default/
# sf-watch-defer-*), iam:PassRole on alpha-engine-sf-watch-defer-scheduler-
# role (the role EventBridge Scheduler assumes to re-invoke this Lambda —
# created below in --bootstrap), and states:ListExecutions on the three ne-*
# pipeline state machines for the deferred-invocation re-evaluation.
#
# Managed OUTSIDE CloudFormation (same rationale as the sibling dispatchers):
# keeps the github-actions-lambda-deploy OIDC role's blast radius narrow — it
# deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet-wide policy after
# 4 IAM-clobber incidents). This script's FLAGLESS run is already code-only
# (this is what the GHA auto-deploy workflow calls); --bootstrap is what ADDS
# IAM-role-creation + Lambda-function-creation on top, operator-run only,
# never in CI.
#
# Usage:
#   bash .../sf-watch-spot-dispatcher/deploy.sh             # update code only (also the CI auto-deploy path)
#   bash .../sf-watch-spot-dispatcher/deploy.sh --bootstrap # operator-only: create/update the IAM role + Lambda function
#   bash .../sf-watch-spot-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../sf-watch-spot-dispatcher/deploy.sh --smoke     # invoke once with a synthetic event (fires a REAL spot box)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-sf-watch-spot-dispatcher"
ROLE_NAME="alpha-engine-sf-watch-spot-dispatcher-role"
POLICY_NAME="alpha-engine-sf-watch-spot-dispatcher-policy"
# Role EventBridge Scheduler assumes to fire the one-shot defer-not-drop
# re-invokes of this Lambda (config#2226). Created in --bootstrap only.
DEFER_ROLE_NAME="alpha-engine-sf-watch-defer-scheduler-role"
DEFER_POLICY_NAME="alpha-engine-sf-watch-defer-scheduler-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
DEFER_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${DEFER_ROLE_NAME}"
# Bootstrap default (first-time deployment only) — sets SF_WATCH_DISPATCH_ENABLED=true
# as the safe default. The update path (step 3) will read the live value and preserve it.
LAMBDA_ENV_BOOTSTRAP="Variables={LOG_LEVEL=INFO,SF_WATCH_DISPATCH_ENABLED=true,SF_WATCH_DEFER_ROLE_ARN=${DEFER_ROLE_ARN}}"

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
# PKG and TEST_DEPS are both created up front (mirrors ci-watch-dispatcher/
# scheduled-groom-dispatcher/deploy.sh) so ONE trap covers both — a
# pytest-install failure below still cleans up.

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

  # --- 2a2. EventBridge Scheduler invoke role (defer-not-drop, config#2226) ---
  # Assumed by scheduler.amazonaws.com to fire the one-shot deferred
  # re-invokes; may ONLY invoke THIS function.
  DEFER_TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${DEFER_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${DEFER_ROLE_NAME}"
    run aws iam create-role \
      --role-name "${DEFER_ROLE_NAME}" \
      --assume-role-policy-document "${DEFER_TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${DEFER_ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${DEFER_POLICY_NAME}"
  # Both the unqualified function ARN and :* (version/alias-qualified) — the
  # schedule targets the unqualified ARN, but keep qualified invokes working
  # if an alias is ever introduced.
  DEFER_INVOKE_POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":["arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}","arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}:*"]}]}
EOF
)
  run aws iam put-role-policy \
    --role-name "${DEFER_ROLE_NAME}" \
    --policy-name "${DEFER_POLICY_NAME}" \
    --policy-document "${DEFER_INVOKE_POLICY}"

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

echo "Updating Lambda environment (preserving operator-owned SF_WATCH_DISPATCH_ENABLED)..."
# SF_WATCH_DISPATCH_ENABLED is an OPERATOR-OWNED runtime flag (defer-not-drop gate) —
# the update path must PRESERVE its live value, never reset it to bootstrap defaults.
# This mirrors the saturday-sf-watch-dispatcher fix (config#1818): a routine redeploy
# should not silently re-arm/disarm the operator's incident-response flag.
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" SF_WATCH_DISPATCH_ENABLED true)
LAMBDA_ENV="Variables={LOG_LEVEL=INFO,SF_WATCH_DISPATCH_ENABLED=${CURRENT_DISPATCH},SF_WATCH_DEFER_ROLE_ARN=${DEFER_ROLE_ARN}}"
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
  echo "Smoke-testing via direct invoke (synthetic saturday-sf-failure event)..."
  echo "⚠ this fires a REAL spot box + REAL sf_watch_spot_bootstrap.sh run."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"pipeline_name":"ne-weekly-freshness-pipeline","cadence_slug":"saturday","state_machine_arn":"arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline","execution_arn":"arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:smoke-test-0000000000000000","run_date":"1970-01-01","failed_state":"SmokeTest","cause":"synthetic smoke-test invocation, not a real failure","watch_log_key":"consolidated/saturday_sf_watch/1970-01-01.json","is_preflight":"false"}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
fi
