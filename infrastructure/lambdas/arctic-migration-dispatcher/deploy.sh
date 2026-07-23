#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-arctic-migration-dispatcher
# Lambda (alpha-engine-config-I3242, runner half of the config-I3236
# structural fix).
#
# WHY: pairs with the already-merged ArcticDB schema-migration FRAMEWORK
# (nousergon-data-PR988, config-I3241/I3238) — that PR made migrations
# discoverable, tested code; this Lambda is the merge-triggered EXECUTION leg
# a GHA workflow (`.github/workflows/run-arctic-migrations.yml`, path-filtered
# on `migrations/**`) invokes SYNCHRONOUSLY via `lambda invoke
# --invocation-type RequestResponse`, mirroring sf-watch-spot-dispatcher's
# proven shape via the shared `nousergon_lib.spot_dispatch` primitives
# (config#2106) — no bespoke copy of the concurrency-lock/launch-with-
# fallback/terminate-on-failure logic.
#
# IAM (iam-policy.json): ec2:RunInstances + iam:PassRole (scoped to the
# EXISTING alpha-engine-executor-role — reused, not a new profile; see
# index.py's IAM PROFILE docstring section) + ssm:SendCommand +
# sns:Publish/ssm:GetParameter/s3 (the spot-quota-exceeded alert path
# `nousergon_lib.spot_dispatch.launch_with_fallback` calls internally on
# SpotQuotaExceededError — same grants data-spot-dispatcher's policy carries
# for the identical reason).
#
# Managed OUTSIDE CloudFormation (same as every sibling dispatcher): keeps the
# github-actions-lambda-deploy OIDC role's blast radius narrow — it
# deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet-wide policy after
# repeated IAM-clobber incidents). This script's FLAGLESS run is code-only
# (what the GHA auto-deploy workflow calls, once one exists for this dir);
# --bootstrap is what ADDS IAM-role-creation + Lambda-function-creation on
# top, operator-run only, never in CI.
#
# Usage:
#   bash infrastructure/lambdas/arctic-migration-dispatcher/deploy.sh             # update code + env only
#   bash infrastructure/lambdas/arctic-migration-dispatcher/deploy.sh --bootstrap # operator-only: create/update the execution role + create the Lambda
#   bash infrastructure/lambdas/arctic-migration-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/arctic-migration-dispatcher/deploy.sh --smoke     # invoke once with a synthetic event (fires a REAL spot box)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-arctic-migration-dispatcher"
ROLE_NAME="alpha-engine-arctic-migration-dispatcher-role"
POLICY_NAME="alpha-engine-arctic-migration-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
FN_TIMEOUT=600
FN_MEMORY=256

# Defer-not-drop scheduler role (config#2226 / config-I3254): assumed by
# EventBridge Scheduler to re-invoke this Lambda for the auto-retry cycle.
DEFER_ROLE_NAME="alpha-engine-arctic-migration-defer-scheduler-role"
DEFER_POLICY_NAME="alpha-engine-arctic-migration-defer-scheduler-policy"

# Role ARN for the defer-not-drop scheduler: derived once after ACCOUNT_ID
# is resolved; populated by --bootstrap, overridable after deployment via
# env update, and preserved by preserve_env_flags below.
DEFER_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${DEFER_ROLE_NAME}"

lambda_env_json() {
  # $1 = ARCTIC_MIGRATION_DISPATCH_ENABLED value (true|false)
  printf '{"Variables":{"LOG_LEVEL":"INFO","ARCTIC_MIGRATION_DISPATCH_ENABLED":"%s","ARCTIC_MIGRATION_DEFER_ROLE_ARN":"%s"}}' "$1" "${DEFER_ROLE_ARN}"
}
LAMBDA_ENV_BOOTSTRAP="$(lambda_env_json true)"

# Shared operator-flag-preserve helper (config#1818/#2236/#2264 bug class): a
# routine code-only redeploy must never silently reset the operator's
# kill-switch back to the bootstrap default.
source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

# DRY_RUN honors an ambient env var too (config-alpha-engine-config-I2752
# convention — see sibling dispatchers).
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

# ----- 0. Scratch dir + validate handler syntax ------------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------
# Hermetic for AWS: boto3 + nousergon_lib.ec2_spot are stubbed in sys.modules
# before `import index` (see test_handler.py).

source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
NOUSERGON_LIB_REQ=$(grep -E '^nousergon-lib' "${SCRIPT_DIR}/requirements.txt" | head -1)
KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" "${KREPIS_REQ}" "${NOUSERGON_LIB_REQ}"

# ----- 1. Package: pip install deps (Lambda-safe) + zip handler --------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only, operator-run) --------------------------

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline policy ---
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --description "Execution role for ${FUNCTION_NAME} — launch the in-region ArcticDB migration spot box (alpha-engine-config-I3242)" \
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

  # --- 2a2. EventBridge Scheduler invoke role (defer-not-drop, config#2226) ---
  # Assumed by scheduler.amazonaws.com to fire the one-shot deferred
  # re-invokes of the migration dispatcher; may ONLY invoke this function.
  DEFER_TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${DEFER_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${DEFER_ROLE_NAME}"
    run aws iam create-role \
      --role-name "${DEFER_ROLE_NAME}" \
      --assume-role-policy-document "${DEFER_TRUST_POLICY}" \
      --description "EventBridge Scheduler role: fire one-shot arctic-migration defer-not-drop re-invokes (config-I3254)" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${DEFER_ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${DEFER_POLICY_NAME}"
  DEFER_INVOKE_POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":["arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}","arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}:*"]}]}
EOF
)
  run aws iam put-role-policy \
    --role-name "${DEFER_ROLE_NAME}" \
    --policy-name "${DEFER_POLICY_NAME}" \
    --policy-document "${DEFER_INVOKE_POLICY}"

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
      --timeout "${FN_TIMEOUT}" \
      --memory-size "${FN_MEMORY}" \
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 3. Update function code (always, idempotent) --------------------------
# On a not-yet-bootstrapped function this FAILS LOUD with a 404 (set -e
# aborts) — deliberately: run --bootstrap first.

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

echo "Updating Lambda environment (preserving operator-owned ARCTIC_MIGRATION_DISPATCH_ENABLED)..."
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" ARCTIC_MIGRATION_DISPATCH_ENABLED true)
LAMBDA_ENV="$(lambda_env_json "${CURRENT_DISPATCH}")"
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${LAMBDA_ENV}" \
  --timeout "${FN_TIMEOUT}" \
  --memory-size "${FN_MEMORY}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi
echo "✓ Env converged."

# ----- 4. Smoke (synthetic event, direct invoke) -----------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic merged_sha/head_migration_number)..."
  echo "⚠ this fires a REAL spot box + REAL scripts/run_arctic_migrations.py run."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"merged_sha":"0000000000000000000000000000000000000000","head_migration_number":0}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
fi

echo ""
echo "Done. Next (operator): confirm .github/workflows/run-arctic-migrations.yml"
echo "on nousergon-data main now succeeds end to end on the next migrations/**"
echo "push — see alpha-engine-config-I3242's PR body for the full deploy plan."
