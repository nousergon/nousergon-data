#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-spot-interruption-recorder
# Lambda (alpha-engine-config-I5197).
#
# --bootstrap creates: (1) this Lambda's OWN execution role + inline policy,
# (2) the Lambda function, (3) a 5-minute reconciler rule (ENABLED — the
# Lambda is read-only apart from its own S3 prefix, so running it early is
# harmless: it simply records nothing on a tick with no evictions), and
# (4) the spot-interruption-warning rule.
#
# WHY THERE IS NO BROAD `EC2 Instance State-change Notification` RULE.
# EventBridge cannot filter on tags, so such a rule fires for EVERY instance
# state change in the account and the Lambda would discard most of them. The
# 5-minute reconciler already meets I5197's "recorded within 5 minutes"
# criterion from CloudTrail, which — unlike the warning — sees reclaims that
# happen during bootstrap with no notice emitted. The handler supports the
# state-change path if a scoped rule is ever wanted; nothing here creates one.
#
# Usage:
#   bash .../spot-interruption-recorder/deploy.sh              # update code only (CI auto-deploy path)
#   bash .../spot-interruption-recorder/deploy.sh --bootstrap  # operator-only: role + function + rules
#   bash .../spot-interruption-recorder/deploy.sh --apply-iam  # re-apply iam-policy.json only
#   bash .../spot-interruption-recorder/deploy.sh --dry-run    # show actions, do not apply
#   bash .../spot-interruption-recorder/deploy.sh --backfill N # invoke once with mode=backfill, N days (max 90)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-spot-interruption-recorder"
ROLE_NAME="alpha-engine-spot-interruption-recorder-role"
POLICY_NAME="alpha-engine-spot-interruption-recorder-policy"
TICK_RULE="alpha-engine-spot-interruption-recorder-tick"
WARN_RULE="alpha-engine-spot-interruption-warning-record"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# DRY_RUN honors an ambient env var as well as the flag (alpha-engine-config-I2752).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
BACKFILL_DAYS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    --backfill) BACKFILL_DAYS="${2:-7}"; shift ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
  shift
done

run() {
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

# ----- 0. Scratch dirs + validate handler syntax -----------------------------

PKG=$(mktemp -d)
TEST_DEPS=$(mktemp -d)
trap "rm -rf '$PKG' '$TEST_DEPS'" EXIT

python3 -c "
import ast
ast.parse(open('${SCRIPT_DIR}/index.py').read())
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Installing test deps into ${TEST_DEPS}..."
  # boto3 (and its botocore dependency) are needed because the handler
  # imports botocore.config.Config at module level (fa7979c sibling fix).
  python3 -m pip install --quiet --target "${TEST_DEPS}" pytest boto3
  echo "Running handler unit tests..."
  PYTHONPATH="${TEST_DEPS}" python3 -m pytest "${SCRIPT_DIR}/test_handler.py" -q
fi

# ----- 1. Package ------------------------------------------------------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- Apply IAM only --------------------------------------------------------

if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied."
fi

# ----- 2. Bootstrap ----------------------------------------------------------

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for IAM role propagation..."
    sleep 10
  fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    # 300s: a backfill sweep paginates CloudTrail across up to 90 days.
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 300 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

  echo "  Creating EventBridge rule: ${TICK_RULE} (ENABLED, 5 min)"
  run aws events put-rule --name "${TICK_RULE}" \
    --schedule-expression 'rate(5 minutes)' --state ENABLED \
    --description "Spot-interruption reconciler tick (alpha-engine-config-I5197) - sweeps CloudTrail BidEvictedEvent and records any eviction not already in overseer/interruptions/." \
    --region "${REGION}" --query 'RuleArn' --output text
  run aws events put-targets --rule "${TICK_RULE}" \
    --targets "Id=1,Arn=${FN_ARN}" --region "${REGION}"
  run aws lambda add-permission --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${TICK_RULE}" --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${TICK_RULE}" \
    --region "${REGION}" 2>/dev/null || true

  echo "  Creating EventBridge rule: ${WARN_RULE} (ENABLED)"
  run aws events put-rule --name "${WARN_RULE}" \
    --event-pattern '{"source":["aws.ec2"],"detail-type":["EC2 Spot Instance Interruption Warning"]}' \
    --state ENABLED \
    --description "Low-latency leg of the spot-interruption recorder (alpha-engine-config-I5197). NOT sufficient alone - AWS emits no warning for a box reclaimed during bootstrap, which is what the reconciler tick covers." \
    --region "${REGION}" --query 'RuleArn' --output text
  run aws events put-targets --rule "${WARN_RULE}" \
    --targets "Id=1,Arn=${FN_ARN}" --region "${REGION}"
  run aws lambda add-permission --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${WARN_RULE}" --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${WARN_RULE}" \
    --region "${REGION}" 2>/dev/null || true
fi

# ----- 3. Update function code ----------------------------------------------

# A not-yet-bootstrapped function must NOT fail the CI deploy job: the
# github-actions-lambda-deploy OIDC role deliberately cannot create Lambdas or
# IAM roles, so between this file merging and an operator running --bootstrap
# the function legitimately does not exist. Hard-failing there turns main red
# on first merge for a reason nobody can fix from CI (alpha-engine-config-I2831
# hit exactly this on the overseer-liveness-probe's first merge).
if ! aws lambda get-function --function-name "${FUNCTION_NAME}" \
     --region "${REGION}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
  echo "⚠ ${FUNCTION_NAME} does not exist yet — skipping code update."
  echo "  An operator must run: bash ${BASH_SOURCE[0]} --bootstrap"
  exit 0
fi

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi
echo "✓ Code deployed."

# ----- 4. Optional backfill --------------------------------------------------

if [[ -n "${BACKFILL_DAYS}" ]]; then
  echo "Backfilling ${BACKFILL_DAYS} day(s) of eviction history from CloudTrail..."
  run aws lambda invoke --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload "{\"mode\": \"backfill\", \"days\": ${BACKFILL_DAYS}}" \
    --region "${REGION}" /dev/stdout
fi
