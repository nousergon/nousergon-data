#!/usr/bin/env bash
# deploy.sh — Update the alpha-engine-alerts-forwarder Lambda's code.
#
# Mirrors the sibling changelog-incident-mirror/deploy.sh pattern.
#
# This Lambda is managed OUTSIDE CloudFormation (same rationale as
# changelog-incident-mirror). By default this script only updates the
# function CODE — IAM, SNS subscription, and Lambda permission are
# provisioned by infrastructure/setup_overseer_intake.sh.
#
# IAM-apply parity (mirroring changelog-incident-mirror's --apply-iam):
# pass --apply-iam to (re)apply the inline execution-role policy from
# iam-policy.json. The operation is idempotent (create-role-if-missing +
# put-role-policy overwrite-in-place).
#
# Usage:
#   bash infrastructure/lambdas/alpha-engine-alerts-forwarder/deploy.sh
#   bash infrastructure/lambdas/alpha-engine-alerts-forwarder/deploy.sh --dry-run
#   bash infrastructure/lambdas/alpha-engine-alerts-forwarder/deploy.sh --apply-iam
#
# Auth: uses active AWS CLI creds. Personal IAM user (cipher813) has
# enough perms; the github-actions-lambda-deploy OIDC role does NOT.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-alerts-forwarder"
ROLE_NAME="alpha-engine-alerts-forwarder"
POLICY_NAME="alpha-engine-alerts-forwarder-events"
REGION="${AWS_REGION:-us-east-1}"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run flag.
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
APPLY_IAM=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --apply-iam) APPLY_IAM=true ;;
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

# Validate index.py syntax locally before shipping.
python3 -c "
import ast, sys
src = open('${SCRIPT_DIR}/index.py').read()
try:
    ast.parse(src)
except SyntaxError as e:
    print(f'index.py syntax error: {e}', file=sys.stderr)
    sys.exit(1)
print('index.py syntax OK')
"

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Running handler smoke tests..."
  python3 "${SCRIPT_DIR}/test_handler.py" >/dev/null
  echo "  ✓ Smoke tests pass"
fi

# ----- IAM apply (opt-in) --------------------------------------------------
# Mirror changelog-incident-mirror's --apply-iam semantics. Idempotent.
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
    run aws iam attach-role-policy \
      --role-name "${ROLE_NAME}" \
      --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    if ! $DRY_RUN; then
      echo "  Waiting 10s for IAM role propagation..."
      sleep 10
    fi
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"
  echo "  ✓ IAM applied."
fi

# Package the handler into a zip in /tmp.
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -q "function.zip" index.py)
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

if $DRY_RUN; then
  echo "(--dry-run) would update Lambda code: ${FUNCTION_NAME}"
  exit 0
fi

# Update function code.
echo "Updating Lambda function code: ${FUNCTION_NAME}"
aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

# Wait for update to settle.
echo "Waiting for update to complete..."
aws lambda wait function-updated \
  --function-name "${FUNCTION_NAME}" \
  --region "${REGION}"

echo "✓ Deployed."
