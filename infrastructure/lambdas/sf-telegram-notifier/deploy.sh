#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-sf-telegram-notifier Lambda
# and wire its EventBridge SF status-change trigger.
#
# This Lambda subscribes to `aws.states` / "Step Functions Execution Status
# Change" events for the three Alpha Engine SFs (saturday / weekday / eod)
# and forwards human-readable summaries to Telegram via
# `nousergon_lib.telegram.send_message`. Existing SNS → email path is
# unaffected.
#
# Managed outside CloudFormation — same rationale as spot-orphan-reaper +
# changelog-cloudwatch-mirror (keeps the github-actions-lambda-deploy
# OIDC role's blast radius narrow; operator-deployed only).
#
# Usage:
#   bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh             # update code only
#   bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh --bootstrap # first-time create + wire EventBridge
#   bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/sf-telegram-notifier/deploy.sh --smoke     # invoke once with a synthetic SUCCEEDED event

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-sf-telegram-notifier"
ROLE_NAME="alpha-engine-sf-telegram-notifier-role"
POLICY_NAME="alpha-engine-sf-telegram-notifier-policy"
RULE_NAME="alpha-engine-sf-status-change"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops
# instead of silently running the real deploy path (alpha-engine-config-
# I2752 incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked
# here, matching other tools' convention, and triggered a real deploy).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
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

# ----- 0. Validate handler syntax -------------------------------------------

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ----- 1. Package: pip install deps + zip handler ---------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
# Both test files stub nousergon_lib in sys.modules, so the shared gate only
# provisions pytest; LAMBDAS_DIR is on PYTHONPATH for `import flow_doctor_
# telegram`. This replaces a gate that relied on AMBIENT pytest (PYTHONPATH=$PKG
# carried no pytest) — the same no-install drift class as config#2295. Runs on
# Darwin too now: host pip fetches pure-python pytest, and the lib being stubbed
# means no linux/amd64 wheel is imported, so the old Darwin skip is obsolete.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
HANDLER_TEST_PYTHONPATH="${LAMBDAS_DIR}" \
HANDLER_TEST_TARGETS="${SCRIPT_DIR}/test_execution_digest.py" \
  run_handler_tests "${SCRIPT_DIR}"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/execution_digest.py" "${PKG}/execution_digest.py"
cp "${SCRIPT_DIR}/../flow_doctor_telegram.py" "${PKG}/flow_doctor_telegram.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

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
      --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 2b. Reconcile EventBridge rule (ALWAYS — not bootstrap-gated) --------
#
# config#1453: a rename that changes an SF ARN (or the rule's status/source
# filter) must be picked up by a PLAIN `deploy.sh` run, not just the
# first-time `--bootstrap`. put-rule/put-targets/add-permission are all
# idempotent create-or-update calls, so running this block on every deploy
# converges the live rule to source with no bootstrap dependency. Requires
# the Lambda to already exist (i.e. `--bootstrap` has run at least once).

echo "Reconciling EventBridge rule: ${RULE_NAME}"
EVENT_PATTERN=$(cat <<EOF
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "stateMachineArn": [
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline",
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-preopen-trading-pipeline",
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline"
    ],
    "status": ["RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
  }
}
EOF
)
run aws events put-rule \
  --name "${RULE_NAME}" \
  --event-pattern "${EVENT_PATTERN}" \
  --description "Fan SF status changes to alpha-engine-sf-telegram-notifier" \
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

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

# ----- 4. Smoke (synthetic SUCCEEDED event) ---------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic SUCCEEDED event)..."
  RESP=$(mktemp)
  PAYLOAD=$(cat <<'EOF'
{
  "source": "aws.states",
  "detail-type": "Step Functions Execution Status Change",
  "detail": {
    "status": "SUCCEEDED",
    "stateMachineArn": "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
    "executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:smoke-test",
    "name": "smoke-test",
    "startDate": 0,
    "stopDate": 60000
  }
}
EOF
)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload "${PAYLOAD}" \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
