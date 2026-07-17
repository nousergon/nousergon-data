#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-overseer-dispatcher Lambda.
#
# WHY (alpha-engine-config-I2823, epic I2821): registry-driven router in
# front of the fleet's failure-response executor Lambdas. One dispatch entry,
# one playbook registry (infrastructure/overseer/playbooks.yaml — BUNDLED
# into the zip here), one owner of verdict-based P1 filing + loud paging
# (previously duplicated in sf-watch.yml GHA yaml). Executors unchanged.
#
# IAM (iam-policy.json): lambda:InvokeFunction on the two routed executors,
# ssm:GetParameter on the fleet PAT + Telegram secrets, s3:PutObject on the
# dispatch-ledger + intake-fallback prefixes, sns:Publish on
# alpha-engine-alerts, events:PutEvents on the nousergon-alerts bus. No EC2
# permissions — launching is the EXECUTORS' job.
#
# Managed OUTSIDE CloudFormation like its sibling dispatchers. Flagless run
# is code-only (the GHA auto-deploy path); --bootstrap creates role + policy
# + function (operator-run only).
#
# Usage:
#   bash .../overseer-dispatcher/deploy.sh             # update code only
#   bash .../overseer-dispatcher/deploy.sh --bootstrap # operator-only: create role + Lambda
#   bash .../overseer-dispatcher/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-overseer-dispatcher"
ROLE_NAME="alpha-engine-overseer-dispatcher-role"
POLICY_NAME="alpha-engine-overseer-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
# Bootstrap default (first-time only) — the update path preserves live flags.
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,OVERSEER_DISPATCH_ENABLED=true}'

# Shared operator-flag-preserve helper (config#1818/#2236/#2264 bug class).
source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
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
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" "${KREPIS_REQ}"

# ----- 1. Package: pip install deps + zip handler + bundle registry ---------
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
# The playbook registry is the router's routing table — bundled from the
# repo SSoT so a registry edit deploys through the normal code path (pinned
# by tests/test_overseer_playbook_registry.py::test_router_bundles_this_registry).
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

  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    echo "  Creating Lambda function: ${FUNCTION_NAME}"
    # Timeout 120s: the executor invoke is SYNCHRONOUS and the slowest
    # executor leg (spot launch + SSM online wait) can take ~90s.
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --architectures x86_64 \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
      --timeout 120 \
      --memory-size 256 \
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda function exists: ${FUNCTION_NAME}"
  fi
fi

# ----- 3. Update code (always) ----------------------------------------------
echo "Updating ${FUNCTION_NAME} code..."
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
  # Preserve operator-owned runtime flags across redeploys (config#1818 class).
  CURRENT_ENABLED=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" OVERSEER_DISPATCH_ENABLED true)
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "Variables={LOG_LEVEL=INFO,OVERSEER_DISPATCH_ENABLED=${CURRENT_ENABLED}}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "Done."
