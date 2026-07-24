#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-sf-watch-market-hours-toggler
# Lambda and wire its rate(5 minutes) EventBridge schedule.
#
# config#2932 — structural market-hours enforcement for
# `alpha-engine-sf-watch-executor-role`'s trading-pipeline `StartExecution`
# grant (Brian's 2026-07-20 ruling, Option E: schedule the existing codified
# writer instead of adding an independent second one). See
# alpha-engine-config/infrastructure/iam/README.md's `sf-watch-executor-role`
# section for the full mechanism writeup.
#
# Managed outside CloudFormation (operator-deployed; keeps the GHA OIDC role
# narrow — same rationale as saturday-integrity-sentinel / pipeline-watchdog /
# eod-backstop). Merging the PR has ZERO live effect until --bootstrap.
#
# Usage:
#   bash …/deploy.sh --iam-repo <path>              # update code only, re-sync policy JSON
#   bash …/deploy.sh --iam-repo <path> --bootstrap   # first-time create + wire EventBridge rule
#   bash …/deploy.sh --iam-repo <path> --dry-run     # show actions, do not apply
#   bash …/deploy.sh --iam-repo <path> --smoke       # invoke once (real GetRolePolicy read;
#                                                     #   only writes if the live policy is stale)
#
# --iam-repo <path> is REQUIRED on every real (non---dry-run) invocation: it
# points at a local `alpha-engine-config` checkout, and deploy.sh copies
# `infrastructure/iam/sf-watch-executor-role-policy{,-market-hours}.json`
# from THERE into this Lambda's deployment package, overwriting the
# committed snapshots in this directory (kept only so `test_handler.py` /
# CI have fixtures to load without a cross-repo checkout). This is the
# sync step that keeps the toggler's policy CONTENT identical to
# `apply.sh`'s — deploy.sh refuses to package stale/local-only copies.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-sf-watch-market-hours-toggler"
ROLE_NAME="alpha-engine-sf-watch-market-hours-toggler-role"
POLICY_NAME="alpha-engine-sf-watch-market-hours-toggler-policy"
RULE_NAME="alpha-engine-sf-watch-market-hours-toggle"
SCHEDULE="rate(5 minutes)"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
SMOKE=false
IAM_REPO=""
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --smoke) SMOKE=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done
# --iam-repo takes a value, so parse it separately from the flag scan above.
prev=""
for arg in "$@"; do
  if [[ "$prev" == "--iam-repo" ]]; then IAM_REPO="$arg"; fi
  prev="$arg"
done

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# ----- 0. Validate handler + run unit tests ---------------------------------
python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

# ----- 1. Sync policy JSON from the alpha-engine-config checkout ------------
if [[ -z "$IAM_REPO" ]]; then
  echo "ERROR: --iam-repo <path-to-alpha-engine-config-checkout> is required" >&2
  echo "       (this Lambda's policy documents are sourced from there — see" >&2
  echo "       the header comment in this script)." >&2
  exit 1
fi
SRC_PERMISSIVE="${IAM_REPO}/infrastructure/iam/sf-watch-executor-role-policy.json"
SRC_MARKET_HOURS="${IAM_REPO}/infrastructure/iam/sf-watch-executor-role-policy-market-hours.json"
for f in "$SRC_PERMISSIVE" "$SRC_MARKET_HOURS"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: $f not found — is --iam-repo pointing at a valid alpha-engine-config checkout?" >&2
    exit 1
  fi
done
echo "Syncing policy JSON from ${IAM_REPO}..."
cp "$SRC_PERMISSIVE" "${SCRIPT_DIR}/sf-watch-executor-role-policy.json"
cp "$SRC_MARKET_HOURS" "${SCRIPT_DIR}/sf-watch-executor-role-policy-market-hours.json"

# ----- 2. Package -------------------------------------------------------------
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${SCRIPT_DIR}/../lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/sf-watch-executor-role-policy.json" "${PKG}/sf-watch-executor-role-policy.json"
cp "${SCRIPT_DIR}/sf-watch-executor-role-policy-market-hours.json" "${PKG}/sf-watch-executor-role-policy-market-hours.json"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 3. Bootstrap -----------------------------------------------------------
if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role --role-name "${ROLE_NAME}" --assume-role-policy-document "${TRUST_POLICY}" --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" --policy-name "${POLICY_NAME}" --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" --runtime python3.12 --role "${ROLE_ARN}" \
      --handler index.handler --zip-file "fileb://${ZIP}" --timeout 30 --memory-size 128 \
      --environment 'Variables={LOG_LEVEL=INFO}' --region "${REGION}" --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 4"
  fi

  echo "  Creating EventBridge rule: ${RULE_NAME} (${SCHEDULE})"
  run aws events put-rule --name "${RULE_NAME}" --schedule-expression "${SCHEDULE}" \
    --description "Poll NYSE market-hours state, toggle sf-watch-executor-role's trading-pipeline StartExecution grant (config#2932)" \
    --region "${REGION}" --query 'RuleArn' --output text

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  run aws events put-targets --rule "${RULE_NAME}" --targets "Id=1,Arn=${FN_ARN}" --region "${REGION}"

  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run aws lambda add-permission --function-name "${FUNCTION_NAME}" --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction --principal events.amazonaws.com --source-arn "${RULE_ARN}" --region "${REGION}" 2>/dev/null || true

  echo "  NOTE: run once with --smoke right after bootstrap to confirm the"
  echo "        toggler correctly no-ops or applies against the LIVE role"
  echo "        before trusting the 5-minute schedule unattended."
fi

# ----- 4. Update function code ------------------------------------------------
echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"; fi
echo "✓ Code deployed."

# ----- 5. Smoke ----------------------------------------------------------------
if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (reads + may write the LIVE sf-watch-executor-role policy)..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out --payload '{}' --region "${REGION}" "${RESP}" >/dev/null
  cat "${RESP}"; echo ""; rm -f "${RESP}"
fi
