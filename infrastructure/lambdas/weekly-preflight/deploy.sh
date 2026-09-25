#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-weekly-preflight Lambda.
#
# This Lambda implements the pre-spend gate for ne-weekly-freshness-pipeline
# (I4494). It runs sf_preflight checks (IAM reachability, tool contracts,
# definition input coherence, Lambda memory headroom) BEFORE the pipeline
# launches any spot or acquires the mutex, stopping the run in seconds
# when a Saturday-fatal condition exists.
#
# Managed outside CloudFormation — same rationale as all sibling Lambdas
# in this directory (narrow OIDC role blast radius).
#
# iam-policy.json (2026-08-12, alpha-engine-config-I7051): this Lambda's role
# granted no `logs:` action at all — 18 invocations over 3 days, zero log
# records, no /aws/lambda/alpha-engine-weekly-preflight log group. It gates
# the weekly SF, so a failure here was undiagnosable. Added a CloudWatchLogs
# statement scoped to this function's own log group. The github-actions-lambda-deploy
# OIDC identity deliberately lacks iam:PutRolePolicy (see _shared/apply_iam_policy.sh
# and infrastructure/iam/README.md "Single-writer rule"), so this grant does
# NOT go live on merge — an operator must run --apply-iam once, with their own
# admin credentials, per iam-policy-change-guard.yml's own preferred sequence.
#
# Usage:
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh             # update code only (CI path)
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh --bootstrap # first-time create
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh --apply-iam # re-apply iam-policy.json
#   bash infrastructure/lambdas/weekly-preflight/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-weekly-preflight"
ROLE_NAME="alpha-engine-weekly-preflight-role"
POLICY_NAME="alpha-engine-weekly-preflight-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

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
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"
python3 -c "import ast; ast.parse(open('${REPO_ROOT}/sf_preflight.py').read()); print('sf_preflight.py syntax OK')"

# ----- Handler unit tests (shared gate) -------------------------------------
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3
# NOTE: run_handler_tests returns 0 when the lambda has no test_handler.py.
# This lambda had none until 2026-08-10, so this gate reported green on a
# function that could not execute a line of its own handler. Keep
# test_handler.py present; deleting it silently disables this gate.

# ----- 1. Package: copy sf_preflight.py + handler + deps --------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

# Copy sf_preflight.py from the repo root — this is the core check logic.
echo "Copying sf_preflight.py to package..."
cp "${REPO_ROOT}/sf_preflight.py" "${PKG}/sf_preflight.py"

echo "Installing deps into ${PKG}..."
python3 -m pip install --quiet --target "${PKG}" --upgrade -r "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no alarms."
  exit 0
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 --role "${ROLE_ARN}" --handler index.handler \
      --zip-file "fileb://${ZIP}" --timeout 120 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,SF_DEFINITION_BUCKET=alpha-engine-research}' \
      --region "${REGION}" --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 4"
  fi
fi

# ----- 3. Grant SF execution role permission to invoke this Lambda -----------
# The step functions execution role needs lambda:InvokeFunction on this
# new preflight Lambda. The SF execution role policy now lives in the
# nous-ergon-ops repo (nous-ergon-ops/infrastructure/iam/alpha-engine-
# step-functions-role.json) and is applied by that repo's apply.sh — IAM is
# no longer owned by this repo (infrastructure-ownership-policy.md §35). For
# reference, the grant to add there:
#
#   {
#     "Sid": "InvokeWeeklyPreflight",
#     "Effect": "Allow",
#     "Action": "lambda:InvokeFunction",
#     "Resource": "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-weekly-preflight:*"
#   }
echo ""
echo "NOTE: The SF execution role grant (lambda:InvokeFunction on ${FUNCTION_NAME})"
echo "must be added to the SF execution role policy in the nous-ergon-ops repo"
echo "and applied via that repo's apply.sh before the step function can"
echo "invoke this Lambda."
echo ""

# ----- 4. Update function code (always, idempotent) --------------------------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

# ----- 5. Publish a version + point the 'live' alias (the SF invokes :live) --
# The Saturday SF invokes this Lambda as 'alpha-engine-weekly-preflight:live'
# (the fleet's stable-version convention for SF-invoked lambdas — the same
# ':live' shape predictor-inference/evaluator/evaluator-director use). A
# created or code-updated function has NO alias until publish-version +
# create-alias run: without this step the state machine would 404 on the
# ':live' alias the moment WeeklyPreflight executes, even though the bare
# function exists. Publish after every code update and point the alias at the
# new version, so a deploy is immediately live for the next run.

if ! $DRY_RUN; then
  NEW_VERSION="$(aws lambda publish-version --function-name "${FUNCTION_NAME}" \
    --region "${REGION}" --query 'Version' --output text)"
  echo "  Published version ${NEW_VERSION}."
  if aws lambda get-alias --function-name "${FUNCTION_NAME}" --name live \
      --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
    echo "  Updating 'live' alias -> ${NEW_VERSION}"
    aws lambda update-alias --function-name "${FUNCTION_NAME}" --name live \
      --function-version "${NEW_VERSION}" --region "${REGION}" \
      --query 'AliasArn' --output text
  else
    echo "  Creating 'live' alias -> ${NEW_VERSION}"
    aws lambda create-alias --function-name "${FUNCTION_NAME}" --name live \
      --function-version "${NEW_VERSION}" --region "${REGION}" \
      --query 'AliasArn' --output text
  fi
else
  echo "DRY: publish-version + ensure 'live' alias for ${FUNCTION_NAME}"
fi

# ----- 6. Post-deploy smoke invoke (BLOCKING) --------------------------------
# The gate's whole job is to run BEFORE the pipeline spends money, once a week.
# Nothing else executes this code, so a packaging defect stays invisible from
# the merge until the following Saturday — which is exactly what happened on
# 2026-08-10 (`No module named 'nousergon_lib'`: sf_preflight.py was copied
# into the zip but nousergon-lib was never in requirements.txt). Unit tests
# cannot see it: they stub sf_preflight and run against the repo venv, where
# every import resolves.
#
# So invoke the alias we just published, against the REAL runtime, and fail
# the deploy on status=ERROR (handler could not execute — packaging, IAM, or
# import defect). status=FAIL is a genuine preflight violation about system
# state, not about this code: report it loudly and let the deploy succeed,
# since blocking the deploy would block the fix for the very violation.
# status=DEGRADED (alpha-engine-config-I11112) means the handler RAN and a
# REQUIRED check could not — and from a Lambda invoke it always will: the
# Lambda runtime provides no arctic/repo_modules/polygon/checkout capability,
# so those checks skip here by design and run on the weekly box instead.
# It is a successful execution with a disclosed gap, so report the gap and
# let the deploy succeed (alpha-engine-config-I11408: treating it as
# "could not execute" turned every push red from 2026-09-21 on).
# The handler is strictly read-only (Describe/Get/Simulate), so invoking it
# on every deploy has no side effects and costs a sub-second Lambda run.
if ! $DRY_RUN; then
  echo "Smoke-invoking ${FUNCTION_NAME}:live..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}:live" \
    --cli-binary-format raw-in-base64-out --payload '{}' \
    --region "${REGION}" "${RESP}" >/dev/null
  SMOKE_STATUS=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('status','MALFORMED'))" "${RESP}")
  echo "  smoke status: ${SMOKE_STATUS}"
  case "${SMOKE_STATUS}" in
    OK)
      python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(f\"  ran {d.get('ran_count')} check(s), {d.get('skip_count')} skipped, {d.get('warn_count')} warning(s)\")" "${RESP}"
      ;;
    DEGRADED)
      python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(f\"  ran {d.get('ran_count')} check(s); {d.get('required_skip_count')} required check(s) unreachable from Lambda (expected here): {d.get('required_skip_names')}\")" "${RESP}"
      ;;
    FAIL)
      echo "  ⚠ preflight reports a genuine violation (system state, not this deploy):"
      python3 -c "import json,sys; print('   ', json.load(open(sys.argv[1])).get('failures'))" "${RESP}"
      ;;
    *)
      echo "  ✗ handler could not execute — the deployed gate is broken:"
      cat "${RESP}"; echo ""
      rm -f "${RESP}"
      exit 1
      ;;
  esac
  rm -f "${RESP}"
else
  echo "DRY: smoke-invoke ${FUNCTION_NAME}:live and fail on status=ERROR"
fi

echo "✓ ${FUNCTION_NAME} deployed."
