#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-canary-replay-dispatcher Lambda
# (alpha-engine-config#2246, Saturday-replay canary).
#
# --bootstrap creates: (1) this Lambda's OWN execution role + inline policy,
# (2) the Lambda function itself, (3) the Thursday EventBridge cron rule
# (classic `events put-rule`, NOT EventBridge Scheduler — this Lambda takes no
# per-invocation identity the way ci-watch-dispatcher's weekly drill needs,
# a plain rule->Lambda target is the simpler, correct fit, mirroring
# eod-backstop/deploy.sh's shape).
#
# SAFE ROLLOUT (mirrors eod-backstop): the EventBridge rule is created
# DISABLED. Enable it deliberately AFTER canary-replay-liveness-probe is
# deployed and verified live (it is the ONLY thing watching the scheduled
# path — nothing else notices a silent Thursday failure):
#   aws events enable-rule --name alpha-engine-canary-replay-thursday --region us-east-1
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole
# (scoped to alpha-engine-canary-replay-executor-role — a NEW, dedicated role
# a sibling agent creates in alpha-engine-config, deliberately NOT the shared
# trading/dashboard executor role) + ssm:SendCommand. The BOX reads its own
# run secrets via ITS instance profile, so this Lambda needs no secret
# access of its own.
#
# Managed OUTSIDE CloudFormation (same as every sibling dispatcher): keeps the
# github-actions-lambda-deploy OIDC role's blast radius narrow. This script's
# FLAGLESS run is code-only (the CI auto-deploy path); --bootstrap is
# operator-only, never in CI.
#
# Usage:
#   bash .../canary-replay-dispatcher/deploy.sh             # update code only (also the CI auto-deploy path)
#   bash .../canary-replay-dispatcher/deploy.sh --bootstrap # operator-only: create/update the IAM role + Lambda function + Thursday rule (DISABLED)
#   bash .../canary-replay-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../canary-replay-dispatcher/deploy.sh --smoke     # invoke once with a synthetic scheduled event (fires a REAL spot box)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-canary-replay-dispatcher"
ROLE_NAME="alpha-engine-canary-replay-dispatcher-role"
POLICY_NAME="alpha-engine-canary-replay-dispatcher-policy"
RULE_NAME="alpha-engine-canary-replay-thursday"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
# Bootstrap default (first-time deployment only) — safe default. The update
# path (step 3) reads the live value and preserves it (config#1818/#2236 bug
# class: a routine redeploy must not silently re-arm an operator kill-switch).
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,CANARY_REPLAY_DISPATCH_ENABLED=true}'

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

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Installing pytest into ${TEST_DEPS}..."
  python3 -m pip install --quiet --target "${TEST_DEPS}" pytest
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

  # --- 2c. Thursday EventBridge cron rule (DISABLED at first deploy — see
  # header. 09:00 UTC Thursday, ahead of Saturday's weekly run with a full
  # business day of slack to investigate a failure before Saturday.) ---
  echo "  Creating EventBridge rule: ${RULE_NAME} (DISABLED)"
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --schedule-expression 'cron(0 9 ? * THU *)' \
    --state DISABLED \
    --description "Saturday-replay canary (config#2246) weekly trigger, 09:00 UTC Thursday. DISABLED until canary-replay-liveness-probe is deployed and verified live." \
    --region "${REGION}" \
    --query 'RuleArn' --output text

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  # JSON array form, not shorthand — shorthand's `Input={...}` cannot embed
  # nested JSON (the CLI's shorthand parser chokes on the unescaped `"`,
  # live-caught on first bootstrap: config#2246).
  run aws events put-targets \
    --rule "${RULE_NAME}" \
    --targets "[{\"Id\":\"1\",\"Arn\":\"${FN_ARN}\",\"Input\":\"{\\\"mode\\\":\\\"scheduled\\\"}\"}]" \
    --region "${REGION}"

  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${RULE_ARN}" \
    --region "${REGION}" 2>/dev/null || true

  echo "  NOTE: rule is DISABLED. Enable AFTER canary-replay-liveness-probe is verified live:"
  echo "        aws events enable-rule --name ${RULE_NAME} --region ${REGION}"
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

echo "Updating Lambda environment (preserving operator-owned CANARY_REPLAY_DISPATCH_ENABLED)..."
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" CANARY_REPLAY_DISPATCH_ENABLED true)
LAMBDA_ENV="Variables={LOG_LEVEL=INFO,CANARY_REPLAY_DISPATCH_ENABLED=${CURRENT_DISPATCH}}"
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

# ----- 4. Smoke (synthetic scheduled event, direct invoke) -------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic scheduled event)..."
  echo "⚠ this fires a REAL spot box + REAL canary_replay_spot_bootstrap.sh run"
  echo "  (live LLM calls against the real held-ticker archive, ~\$1 in Anthropic cost)."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"mode":"scheduled"}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
fi
