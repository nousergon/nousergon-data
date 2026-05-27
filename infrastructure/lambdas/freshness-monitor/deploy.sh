#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-freshness-monitor Lambda,
# wire its EventBridge cron, and upload the artifact registry from the
# local alpha-engine-config clone to S3.
#
# Phase 3 of the artifact-freshness-monitor arc (plan doc at
# ~/Development/alpha-engine-docs/private/artifact-freshness-monitor-260527.md).
# Loads `private-docs/ARTIFACT_REGISTRY.yaml` from the operator's local
# clone of cipher813/alpha-engine-config and uploads it to
# s3://alpha-engine-research/_freshness_monitor/ARTIFACT_REGISTRY.yaml.
# Validates the registry locally before upload — a malformed registry
# never reaches S3.
#
# Managed outside CloudFormation — same rationale as sf-telegram-notifier /
# spot-orphan-reaper / changelog-cloudwatch-mirror (keeps the
# github-actions-lambda-deploy OIDC role's blast radius narrow;
# operator-deployed only). Phase 6 cutover flips
# MNEMON_FRESHNESS_MONITOR_ENABLED via
# `aws lambda update-function-configuration` without redeploying.
#
# Usage:
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh             # update code + registry
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --bootstrap # first-time create + wire EventBridge
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --smoke     # invoke once after deploy

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-freshness-monitor"
ROLE_NAME="alpha-engine-freshness-monitor-role"
POLICY_NAME="alpha-engine-freshness-monitor-policy"
RULE_NAME="alpha-engine-freshness-monitor-cron"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Registry SoT. The validator lives next to the YAML in alpha-engine-config;
# we sanity-check the file parses + matches the lib's expected schema
# before uploading.
CONFIG_REPO="${CONFIG_REPO:-${HOME}/Development/alpha-engine-config}"
REGISTRY_LOCAL="${CONFIG_REPO}/private-docs/ARTIFACT_REGISTRY.yaml"
REGISTRY_VALIDATOR="${CONFIG_REPO}/scripts/validate_artifact_registry.py"
REGISTRY_BUCKET="alpha-engine-research"
REGISTRY_S3_KEY="_freshness_monitor/ARTIFACT_REGISTRY.yaml"

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

# ----- 0. Validate handler syntax + run unit tests --------------------------

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

# ----- 0b. Validate registry locally before upload --------------------------

if [[ ! -f "${REGISTRY_LOCAL}" ]]; then
  echo "❌ Registry not found at ${REGISTRY_LOCAL}"
  echo "   Clone cipher813/alpha-engine-config into ~/Development/ or set CONFIG_REPO"
  exit 1
fi

if [[ ! -f "${REGISTRY_VALIDATOR}" ]]; then
  echo "❌ Validator not found at ${REGISTRY_VALIDATOR}"
  echo "   alpha-engine-config must be at the post-PR-#344 commit (artifact-registry-bootstrap merged)"
  exit 1
fi

echo "Validating registry locally before upload..."
python3 "${REGISTRY_VALIDATOR}" --registry "${REGISTRY_LOCAL}"

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
    # OBSERVE-mode default: MNEMON_FRESHNESS_MONITOR_ENABLED=false.
    # Phase 6 cutover flips via update-function-configuration without
    # redeploying. ≥2 weekly soak cycles before flip.
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 120 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,MNEMON_FRESHNESS_MONITOR_ENABLED=false}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # EventBridge cron: every 15 minutes. Sub-15min granularity isn't
  # load-bearing (alerts dedup by cadence window) — this is the floor
  # for "operator-perceived" probe cadence.
  echo "  Creating EventBridge cron: ${RULE_NAME}"
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --schedule-expression "cron(*/15 * * * ? *)" \
    --description "Every 15min probe of the artifact freshness registry" \
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

# ----- 4. Upload registry to S3 ---------------------------------------------

echo "Uploading registry: ${REGISTRY_LOCAL} → s3://${REGISTRY_BUCKET}/${REGISTRY_S3_KEY}"
run aws s3 cp \
  "${REGISTRY_LOCAL}" \
  "s3://${REGISTRY_BUCKET}/${REGISTRY_S3_KEY}" \
  --region "${REGION}"

echo "✓ Registry uploaded."

# ----- 5. Smoke (direct invoke) ---------------------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke..."
  RESP=$(mktemp)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  echo "Lambda response:"
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
