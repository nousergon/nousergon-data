#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-collection-readiness-probe Lambda.
#
# alpha-engine-config-I11264 deliverable 1: the v1 consumers' readiness question
# over the standalone collector's run manifests. Invoked DIRECTLY by the three v1
# Step Functions' `WaitForCollectionManifests` states (step_function.json,
# step_function_daily.json, step_function_eod.json) — no EventBridge Scheduler,
# no wrapping SF. This script manages only the Lambda and its execution role.
# The SF EXECUTION-role grant (`lambda:InvokeFunction` on this function) lives in
# nous-ergon-ops/infrastructure/iam/alpha-engine-step-functions-role/ and is
# applied on merge THERE.
#
# Managed OUTSIDE CloudFormation — same rationale as the sibling probes: the
# github-actions-lambda-deploy OIDC role deliberately LACKS iam:CreateRole /
# iam:PutRolePolicy, so the FIRST-TIME `--bootstrap` (execution role + function
# create) MUST be run by an operator with IAM rights, BEFORE the v1 definitions
# that invoke it deploy. The flagless run is code-only and is the CI auto-deploy
# path (.github/workflows/deploy-collection-readiness-probe.yml, path-filtered on
# this directory AND the shared predicate + descriptors it packages).
#
# Usage:
#   bash infrastructure/lambdas/collection-readiness-probe/deploy.sh             # update code only (CI auto-deploy path)
#   bash infrastructure/lambdas/collection-readiness-probe/deploy.sh --bootstrap # operator-only: create role + policy + function
#   bash infrastructure/lambdas/collection-readiness-probe/deploy.sh --apply-iam # re-apply iam-policy.json only
#   bash infrastructure/lambdas/collection-readiness-probe/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${LAMBDAS_DIR}/_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-collection-readiness-probe"
ROLE_NAME="alpha-engine-collection-readiness-probe-role"
POLICY_NAME="alpha-engine-collection-readiness-probe-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${LAMBDAS_DIR}/_shared/deploy_run.sh"

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "
import ast
ast.parse(open('${SCRIPT_DIR}/index.py').read())
print('index.py syntax OK')
"

# shellcheck source=infrastructure/lambdas/_shared/run_handler_tests.sh
source "${LAMBDAS_DIR}/_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" -r "${SCRIPT_DIR}/requirements.txt"

# ----- 1. Package: deps + handler + the SHARED predicate and descriptors ------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (Lambda-safe pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"

# The predicate and the descriptors it grades ride at their repo-relative paths,
# exactly as in the data-spot dispatcher's zip: descriptors.py computes UNITS_DIR
# from its own location, so `data_gate/` + `registry.d/units/` at the zip root
# resolve under /var/task. ONE implementation of the predicate, packaged twice —
# never a copy that drifts (alpha-engine-config-I11264: "Do not reimplement the
# predicate; share it").
mkdir -p "${PKG}/data_gate" "${PKG}/registry.d/units"
cp "${REPO_ROOT_DIR}/data_gate/__init__.py" "${REPO_ROOT_DIR}/data_gate/descriptors.py" \
  "${REPO_ROOT_DIR}/data_gate/run_manifest_predicate.py" "${PKG}/data_gate/"
cp "${REPO_ROOT_DIR}"/registry.d/units/*.yaml "${PKG}/registry.d/units/"
echo "Packaged $(ls "${PKG}/registry.d/units" | wc -l | tr -d ' ') unit descriptors + the shared predicate"

ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. IAM only -----------------------------------------------------------

if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  IAM applied. Nothing else was touched."
  exit 0
fi

# ----- 3. Bootstrap (first-time only, operator) ------------------------------

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."
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
    # 90 s: a poll lists at most 18 unit prefixes and reads one manifest each.
    # The v1 states that call it carry TimeoutSeconds 60, BELOW this, so the
    # state is what binds (tests/test_sf_lambda_timeout_ordering.py;
    # infrastructure/sf_definitions.py::CODIFIED_FUNCTION_TIMEOUTS_SEC).
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 90 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 4"
  fi
fi

# ----- 4. Update function code (always, idempotent) --------------------------

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

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

echo "Code deployed."
