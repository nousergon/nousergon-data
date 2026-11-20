#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-preflight-sweep-dispatcher
# Lambda (alpha-engine-config-I7249, index.py's module docstring has the full
# rationale: sixteen consecutive weekly-SF failures since 2026-08-10, each a
# different root cause, all on the spot box after boot).
#
# CRITICAL DIFFERENCE FROM EVERY SIBLING DISPATCHER: the EventBridge schedule
# for this Lambda is NOT created here. It is declared as
# PreflightSweepDailyTrigger in infrastructure/cloudformation/
# alpha-engine-orchestration.yaml (cron_utc sourced from the single manifest,
# infrastructure/preflight_sweep_cadence.json) and deployed on EVERY merge to
# main by .github/workflows/deploy-infrastructure.yml — that workflow has no
# path filter and restamps the CF stack on every commit (see its header). So:
#
#   * --bootstrap here creates ONLY this Lambda's IAM role + inline policy +
#     the Lambda function itself, plus the one `lambda add-permission`
#     statement that lets events.amazonaws.com invoke it FROM the CFN-owned
#     rule ARN (arn:aws:events:us-east-1:711398986525:rule/alpha-engine-
#     preflight-sweep-daily). It does NOT create, enable, or touch any
#     EventBridge rule — there is nothing to reconcile against
#     automation_pause.json here (`_shared/pause.sh` is deliberately NOT
#     sourced; the rule's pause posture is CFN's concern, not this script's).
#   * The function itself is operator-bootstrapped ahead of any merge that
#     depends on it existing (same ordering discipline as every other
#     dispatcher's README: the invoked-by-schedule side must exist before the
#     invoking side goes live).
#   * The MERGE to main is what actually activates dispatch — not because the
#     rule flips on then (it may already be enabled from a prior merge), but
#     because the handler's pre-spend guard (`sweep_code_is_deployed`) checks
#     GitHub raw for infrastructure/preflight_sweep.py and
#     infrastructure/preflight_sweep.sh on main and returns a declared skip,
#     spending nothing, until they are actually there. A merge that ships the
#     sweep code is what turns the very next scheduled fire from a no-spend
#     skip into a real dispatch. No post-merge command exists to forget
#     (pull-request-policy §4.2 form 1: automated ON merge, from code already
#     in the repo).
#
# Managed OUTSIDE CloudFormation for the Lambda + IAM half (same rationale as
# every sibling dispatcher): keeps the github-actions-lambda-deploy OIDC
# role's blast radius narrow — that role deliberately lacks iam:CreateRole /
# iam:PutRolePolicy. This script's FLAGLESS run is code-only (the CI
# auto-deploy path); --bootstrap and --apply-iam are operator-only, never in
# CI.
#
# Usage:
#   bash .../preflight-sweep-dispatcher/deploy.sh             # update code + env only (also the CI auto-deploy path)
#   bash .../preflight-sweep-dispatcher/deploy.sh --bootstrap # operator-only: create/update the IAM role + Lambda function + the events.amazonaws.com invoke permission
#   bash .../preflight-sweep-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, mirrors config#2825)
#   bash .../preflight-sweep-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../preflight-sweep-dispatcher/deploy.sh --smoke     # invoke once with a synthetic scheduled event (fires a REAL spot launcher box + a real sweep, ~1 spot-hour / ~$0.20)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-preflight-sweep-dispatcher"
ROLE_NAME="alpha-engine-preflight-sweep-dispatcher-role"
POLICY_NAME="alpha-engine-preflight-sweep-dispatcher-policy"
RULE_NAME="alpha-engine-preflight-sweep-daily"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
# Bootstrap default (first-time deployment only) — safe default. The update
# path (step 3) reads the live value and preserves it (config#1818/#2236 bug
# class: a routine redeploy must not silently re-arm an operator kill-switch).
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,PREFLIGHT_SWEEP_DISPATCH_ENABLED=true}'

source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops instead
# of silently running the real deploy path (alpha-engine-config-I2752
# incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked here,
# matching other tools' convention, and triggered a real deploy).
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

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Scratch dirs + validate handler syntax -----------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------

# The shared gate (config#2381), not a hand-rolled copy. The copy this
# replaced installed only `pytest`, ran ONLY test_handler.py, and put
# nothing but the scratch dir on PYTHONPATH. Measured 2026-08-28,
# preflight-sweep-dispatcher had failed 6 of 6 runs since the workflow
# shipped on 2026-08-13 — `ModuleNotFoundError: No module named boto3`,
# because its index.py imports boto3 at module scope and the hand-rolled
# list did not. spot-interruption-recorder had already hit the identical
# wall and patched its own copy in place rather than the shared helper,
# which is what left the other three exposed.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

# ----- 1. Package: pip install deps + zip handler ---------------------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

# ----- Apply IAM only (mirrors config#2825, no bootstrap side effects) -----
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no alarms."
  exit 0
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline policy ---
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --description "Execution role for ${FUNCTION_NAME} - reuse the weekly launcher box, SSM the all-stage preflight sweep, write the console unmeasured row on failure (config-I7249)" \
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

  # --- 2c. Permit the CFN-owned EventBridge rule to invoke this function ---
  # The rule ITSELF (PreflightSweepDailyTrigger, cron_utc sourced from
  # infrastructure/preflight_sweep_cadence.json) is created and reconciled by
  # infrastructure/cloudformation/alpha-engine-orchestration.yaml on every
  # merge to main — this script never calls `aws events put-rule` /
  # `put-targets` for it. Only the resource-based invoke permission is
  # granted here, against the rule's STATIC arn (the name is fixed;
  # add-permission does not require the rule to exist yet, matching every
  # sibling dispatcher's identical bootstrap-before-CFN ordering).
  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  echo "  Granting events.amazonaws.com invoke permission from ${RULE_ARN}"
  run_tolerating "ResourceConflictException" \
    aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${RULE_ARN}" \
    --region "${REGION}"   # swallowed: idempotent re-bootstrap hits ResourceConflictException when the statement already exists; add-permission has no --if-not-exists, and a stale statement here is harmless (same-shaped statement every time)
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

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

echo "✓ Code deployed."

echo "Updating Lambda environment (preserving operator-owned PREFLIGHT_SWEEP_DISPATCH_ENABLED)..."
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" PREFLIGHT_SWEEP_DISPATCH_ENABLED true)
LAMBDA_ENV="Variables={LOG_LEVEL=INFO,PREFLIGHT_SWEEP_DISPATCH_ENABLED=${CURRENT_DISPATCH}}"
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

# shellcheck source=infrastructure/lambdas/_shared/smoke.sh
source "${SCRIPT_DIR}/../_shared/smoke.sh"
if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic scheduled event)..."
  echo "⚠ this launches a REAL spot launcher box (via alpha-engine-weekly-freshness-spot-dispatcher) and runs a REAL all-stage preflight sweep on it — roughly one spot-hour, ~\$0.20."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  INVOKE_STDOUT=$(aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}")
  cat "${RESP}"
  echo ""
  assert_no_function_error "${INVOKE_STDOUT}" "${RESP}"
  assert_json_response_field "${RESP}" "dispatched" "true"
fi
