#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-ci-watch-dispatcher Lambda.
#
# WHY (config#1432-style migration, see index.py's module docstring): Fleet CI
# Watch (`sf-watch`, alpha-engine-config) diagnoses+fixes fleet CI failures on
# GitHub-hosted Actions runners, burning the org's metered Actions-minutes
# budget — currently gated to Saturday-only as a stopgap. This Lambda moves it
# to EC2 spot, mirroring the PROVEN scheduled-groom-dispatcher pattern in THIS
# repo, but MUCH SIMPLER: no Step Function, no EventBridge Scheduler rules —
# CI-watch is invoked directly via a SYNCHRONOUS `lambda invoke` from a GHA job
# (built by a sibling agent in alpha-engine-config's sf-watch.yml) once per
# real CI failure event, not on a cron cadence. So --bootstrap here only needs
# to create: (1) this Lambda's OWN execution role + inline policy, (2) the
# Lambda function itself. No SF, no Scheduler execution role, no SCHED_NAMES.
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole
# (scoped to alpha-engine-ci-watch-executor-role — a NEW, dedicated role a
# sibling agent is creating in alpha-engine-config, deliberately NOT the
# shared trading alpha-engine-executor-role) + ssm:SendCommand. The BOX reads
# its own run secrets (PAT) via ITS instance profile, so this Lambda needs no
# secret access of its own.
#
# Managed OUTSIDE CloudFormation (same rationale as the sibling dispatchers):
# keeps the github-actions-lambda-deploy OIDC role's blast radius narrow — it
# deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet-wide policy after
# 4 IAM-clobber incidents in 2 months; see infrastructure/iam/README.md if
# present). This script's FLAGLESS run is already code-only (this is what the
# GHA auto-deploy workflow calls); --bootstrap is what ADDS IAM-role-creation +
# Lambda-function-creation on top, operator-run only, never in CI.
#
# Usage:
#   bash .../ci-watch-dispatcher/deploy.sh             # update code only (also the CI auto-deploy path)
#   bash .../ci-watch-dispatcher/deploy.sh --bootstrap # operator-only: create/update the IAM role + Lambda function
#   bash .../ci-watch-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../ci-watch-dispatcher/deploy.sh --smoke     # invoke once with a synthetic event (fires a REAL spot box)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-ci-watch-dispatcher"
ROLE_NAME="alpha-engine-ci-watch-dispatcher-role"
POLICY_NAME="alpha-engine-ci-watch-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

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
# PKG and TEST_DEPS are both created up front (mirrors scheduled-groom-
# dispatcher/deploy.sh) so ONE trap covers both — a pytest-install failure
# below still cleans up.

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
      --environment 'Variables={LOG_LEVEL=INFO,CI_WATCH_DISPATCH_ENABLED=true}' \
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

echo "Updating Lambda environment..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,CI_WATCH_DISPATCH_ENABLED=true}' \
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
  echo "Smoke-testing via direct invoke (synthetic CI-failure event)..."
  echo "⚠ this fires a REAL spot box + REAL ci_watch_spot_bootstrap.sh run."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"repo":"nousergon/alpha-engine-config","sha":"0000000000000000000000000000000000000000","run_id":"999999999","run_url":"https://github.com/nousergon/alpha-engine-config/actions/runs/999999999","workflow":"smoke-test","branch":"main"}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
fi
