#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-spot-orphan-reaper Lambda
# and wire its hourly EventBridge trigger.
#
# Backstop for the spot-side watchdog installed by the four spot launcher
# scripts. Hourly scan terminates any alpha-engine-* tagged spot instance
# whose age exceeds its per-tag-prefix budget + 30-min grace.
#
# ci-watch-dispatcher migration (new dependency): index.py now imports
# nousergon_lib.telegram (re-exports krepis.telegram.send_message) for the
# CI-watch incomplete-reap alert. nousergon-lib pulls in pydantic (pydantic-
# core ships a compiled, platform-specific wheel — verified: a bare macOS
# `pip install --target` produces a darwin/arm64 .so that is NOT Lambda-safe),
# so packaging now goes through lambda_pip_install.sh (Docker linux/amd64),
# mirroring scheduled-groom-dispatcher/deploy.sh exactly, instead of the old
# single-file `zip index.py`.
#
# Managed outside CloudFormation — same rationale as the
# changelog-cloudwatch-mirror Lambda (keeps the github-actions-lambda-deploy
# OIDC role's blast radius narrow; this Lambda has destructive ec2:Terminate
# permission and should be operator-deployed only).
#
# Usage:
#   bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh             # update code only
#   bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --bootstrap # first-time create + wire EventBridge
#   bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/spot-orphan-reaper/deploy.sh --smoke     # invoke once with DRY_RUN=true and print scan output

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-spot-orphan-reaper"
ROLE_NAME="alpha-engine-spot-orphan-reaper-role"
POLICY_NAME="alpha-engine-spot-orphan-reaper-policy"
RULE_NAME="alpha-engine-spot-orphan-reaper-hourly"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Canonical function env — defined ONCE so the create / update / smoke paths can
# never drift out of lockstep. MAX_SPOT_BUDGET_SECONDS is the single global reap
# cap (longest fleet watchdog; see index.py docstring); GRACE covers scan cadence.
PROD_ENV='Variables={MAX_SPOT_BUDGET_SECONDS=21600,GRACE_SECONDS=1800,DRY_RUN=false}'
SMOKE_ENV='Variables={MAX_SPOT_BUDGET_SECONDS=21600,GRACE_SECONDS=1800,DRY_RUN=true}'

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

# ----- 0. Scratch dir + validate handler syntax -----------------------------
# PKG (the Lambda-zip staging dir) is created up front; the shared handler-
# test gate (0b) provisions its OWN scratch dir for pytest + deps (config#2381).

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast, sys
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------
# Hermetic for AWS: boto3 + nousergon_lib.telegram are stubbed in sys.modules
# before `import index` (see test_handler.py). The pinned nousergon-lib is
# installed for real by the shared gate into its own scratch dir (config#1746 hermetic-
# import-guard pattern) — NOT the caller's global site-packages, not bundled
# into the Lambda zip.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
NOUSERGON_LIB_REQ=$(grep -E '^nousergon-lib' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" "${NOUSERGON_LIB_REQ}"

# ----- 1. Bootstrap (first-time only) ---------------------------------------

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

  LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
  echo "  Installing deps into ${PKG} (Lambda-safe Docker pip)..."
  bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
  cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
  ZIP="${PKG}/function.zip"
  (cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
  echo "  Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 60 \
      --memory-size 128 \
      --environment "${PROD_ENV}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  fi

  # EventBridge hourly trigger
  echo "  Creating EventBridge rule: ${RULE_NAME}"
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --schedule-expression "cron(15 * * * ? *)" \
    --description "Hourly scan for orphan alpha-engine spot instances" \
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

# ----- 2. Update function code (always) -------------------------------------

if ! $BOOTSTRAP; then
  LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
  echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
  bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
  cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
  ZIP="${PKG}/function.zip"
  (cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
  echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

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
fi

echo "✓ Code deployed."

# Converge function env on EVERY deploy. `update-function-code` does not touch
# env, so an existing function would otherwise keep whatever env it had — this is
# how MAX_SPOT_BUDGET_SECONDS lands on an already-created reaper (and how any
# interim env override gets reset to the canonical value).
if ! $DRY_RUN; then
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "${PROD_ENV}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text > /dev/null
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
  echo "✓ Env converged: ${PROD_ENV}"
fi

# ----- 3. Smoke (dry-run scan) ----------------------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (DRY_RUN override)..."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  # Override DRY_RUN at the env layer for the smoke invoke
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "${SMOKE_ENV}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text > /dev/null
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"

  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""

  # Restore production env (DRY_RUN=false)
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "${PROD_ENV}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text > /dev/null
fi
