#!/usr/bin/env bash
# deploy.sh — Create or update the vires-runner-dispatcher Lambda.
#
# WHY: mirrors nousergon/alpha-engine-config's self-hosted-runner dispatcher
# (alpha-engine-config-I2572) verbatim in mechanism, re-namespaced for vires.
# A live billing-usage audit (2026-07-17) found vires among the largest
# non-alpha-engine-config private-repo GHA-minute consumers (~1,152 min in
# July alone) after the org crossed its 3,000-min included-Actions-minute
# line for the month. This Lambda receives GitHub's `workflow_job` webhook directly
# (a Function URL, not a `lambda invoke` from a GHA job — there is no
# GHA-hosted leg left to invoke it once the target workflows' runs-on points
# at a self-hosted runner) and launches an EC2-spot box that registers as an
# EPHEMERAL self-hosted runner for exactly one queued job, then
# self-terminates. --bootstrap creates: (1) this Lambda's own execution role
# + inline policy, (2) the Lambda function itself, (3) its public Function
# URL + the resource policy allowing GitHub to invoke it.
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole
# (scoped to vires-runner-executor-role — a NEW, dedicated role
# a sibling agent is creating in vires) + ssm:SendCommand +
# ssm:GetParameter (its OWN webhook secret — unlike ci-watch-dispatcher, this
# Lambda verifies GitHub's HMAC signature itself) + lambda:InvokeFunction on
# itself (the two-phase webhook-receiver/worker self-invoke — see index.py's
# module docstring for why the launch work cannot run inline in the
# webhook-receiving invocation).
#
# Managed OUTSIDE CloudFormation (same rationale as the sibling dispatchers):
# keeps the github-actions-lambda-deploy OIDC role's blast radius narrow.
# This script's FLAGLESS run is code-only; --bootstrap is operator-only.
#
# POST-BOOTSTRAP MANUAL STEPS (cannot be automated via this script — see
# alpha-engine-config-I2572/I2653, the source design, for the full
# sequencing rationale):
#   1. Create the SSM params this Lambda/box read:
#        /vires/runner/webhook_secret  (random string, shared
#          with the GitHub webhook config below)
#        /vires/runner/github_pat  (fine-grained PAT, owner=
#          nousergon, repo=vires, Administration: Read and
#          write + Contents: read — Administration:write is REQUIRED to mint
#          runner registration tokens; the Actions permission does NOT cover
#          this. Fine-grained PAT creation/scoping is a GitHub web-UI-only,
#          human action — cannot be done via this script or the API.)
#        /vires/runner/github_read_pat  (SEPARATE fine-grained
#          PAT, owner=nousergon, repo=vires, Actions: Read
#          ONLY — deliberately NOT the same PAT as above. This one is read
#          by the Lambda's own execution role (behind a public Function URL)
#          for the reconcile phase's read-only "list queued jobs" calls;
#          keeping it separate and minimally-scoped means a Lambda-side
#          issue can never expose Administration:write.)
#   2. Register the GitHub webhook on nousergon/vires:
#        event: workflow_job, content type: json, secret: (the same value as
#        the SSM param above), URL: this script's printed Function URL.
#      Can be done via `gh api repos/nousergon/vires/hooks`.
#
# Usage:
#   bash .../vires-runner-dispatcher/deploy.sh             # update code only (also the CI auto-deploy path)
#   bash .../vires-runner-dispatcher/deploy.sh --bootstrap # operator-only: create/update the IAM role + Lambda + Function URL + reconcile schedule
#   bash .../vires-runner-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../vires-runner-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../vires-runner-dispatcher/deploy.sh --smoke     # invoke the WORKER phase once with a synthetic job id (fires a REAL spot box)
#   bash .../vires-runner-dispatcher/deploy.sh --reconcile-test # invoke the RECONCILE phase once directly (read-only unless it finds a genuinely stale job)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="vires-runner-dispatcher"
ROLE_NAME="vires-runner-dispatcher-role"
POLICY_NAME="vires-runner-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
# Bootstrap default (first-time deployment only) — safe default ON, mirrors
# every other fleet dispatcher's kill-switch convention.
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,VIRES_RUNNER_DISPATCH_ENABLED=true}'

source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops
# instead of silently running the real deploy path (vires-
# I2752 incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked
# here, matching other tools' convention, and triggered a real deploy).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
SMOKE=false
RECONCILE_TEST=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    --smoke) SMOKE=true ;;
    --reconcile-test) RECONCILE_TEST=true ;;
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
      --timeout 60 \
      --memory-size 256 \
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
    if ! $DRY_RUN; then
      aws lambda wait function-active --function-name "${FUNCTION_NAME}" --region "${REGION}"
    fi
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # --- 2b. Function URL (GitHub webhooks can't sign SigV4 — auth is the
  # handler's own HMAC verification against the shared webhook secret) ---
  if ! aws lambda get-function-url-config --function-name "${FUNCTION_NAME}" --query 'FunctionUrl' --output text >/dev/null 2>&1; then
    echo "  Creating Function URL (AuthType=NONE; handler-level HMAC verification is the real auth boundary)"
    run aws lambda create-function-url-config \
      --function-name "${FUNCTION_NAME}" \
      --auth-type NONE \
      --region "${REGION}" \
      --query 'FunctionUrl' --output text
    echo "  Granting public InvokeFunctionUrl permission"
    run aws lambda add-permission \
      --function-name "${FUNCTION_NAME}" \
      --statement-id "AllowPublicFunctionUrlInvoke" \
      --action lambda:InvokeFunctionUrl \
      --principal "*" \
      --function-url-auth-type NONE \
      --region "${REGION}" >/dev/null
  else
    echo "  Function URL already configured"
  fi

  if ! $DRY_RUN; then
    FN_URL=$(aws lambda get-function-url-config --function-name "${FUNCTION_NAME}" --region "${REGION}" --query 'FunctionUrl' --output text)
    echo ""
    echo "  Function URL: ${FN_URL}"
    echo "  -> register as the GitHub webhook target for nousergon/vires"
    echo "     (event: workflow_job, content type: json, secret: matches"
    echo "     /vires/runner/webhook_secret) — see this script's"
    echo "     header for the full remaining manual steps."
    echo ""
  fi

  # --- 2c. Reconcile backstop schedule (alpha-engine-config-I2653) ---
  # A self-hosted runner registered via a plain registration token binds to
  # the repo's WHOLE label pool, not the specific job that triggered its
  # launch (GitHub has no per-job reservation API — verified against
  # GitHub's own docs; an earlier JIT-runner-config fix attempt for this
  # issue was wrong, JIT mints the same kind of "any matching job"
  # registration, just more securely). Under concurrent load a dispatched
  # runner can grab an unrelated queued job, permanently stranding the job
  # that triggered its launch — GitHub sends `queued` exactly once per job.
  # This EventBridge Scheduler rule invokes the Lambda's RECONCILE phase
  # every 60s as a self-healing backstop; see index.py's module docstring
  # phase 3 for the full mechanism.
  RECONCILE_SCHED_ROLE_NAME="vires-runner-reconcile-scheduler-role"
  RECONCILE_SCHED_POLICY_NAME="invoke-vires-runner-dispatcher"
  RECONCILE_SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${RECONCILE_SCHED_ROLE_NAME}"
  RECONCILE_SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${RECONCILE_SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${RECONCILE_SCHED_ROLE_NAME}"
    run aws iam create-role --role-name "${RECONCILE_SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${RECONCILE_SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME}'s reconcile phase every 60s (alpha-engine-config-I2653)" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${RECONCILE_SCHED_ROLE_NAME}"
  fi
  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  RECONCILE_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"lambda:InvokeFunction\",\"Resource\":[\"${FN_ARN}\",\"${FN_ARN}:*\"]}]}"
  echo "  Applying Scheduler invoke policy: ${RECONCILE_SCHED_POLICY_NAME}"
  run aws iam put-role-policy --role-name "${RECONCILE_SCHED_ROLE_NAME}" \
    --policy-name "${RECONCILE_SCHED_POLICY_NAME}" \
    --policy-document "${RECONCILE_INVOKE_POLICY}"
  if ! $DRY_RUN; then echo "  Waiting 10s for Scheduler role propagation..."; sleep 10; fi

  RECONCILE_SCHED_NAME="vires-runner-reconcile"
  RECONCILE_TARGET="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${RECONCILE_SCHED_ROLE_ARN}\",\"Input\":\"{\\\"reconcile\\\":true}\"}"
  if aws scheduler get-schedule --name "${RECONCILE_SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
    echo "  Updating Scheduler rule: ${RECONCILE_SCHED_NAME} -> every 60s"
    run aws scheduler update-schedule --name "${RECONCILE_SCHED_NAME}" \
      --schedule-expression "rate(1 minute)" \
      --flexible-time-window '{"Mode":"OFF"}' --target "${RECONCILE_TARGET}" \
      --region "${REGION}" --query 'ScheduleArn' --output text
  else
    echo "  Creating Scheduler rule: ${RECONCILE_SCHED_NAME} -> every 60s"
    run aws scheduler create-schedule --name "${RECONCILE_SCHED_NAME}" \
      --schedule-expression "rate(1 minute)" \
      --flexible-time-window '{"Mode":"OFF"}' --target "${RECONCILE_TARGET}" \
      --region "${REGION}" --query 'ScheduleArn' --output text
  fi
  if ! $DRY_RUN; then
    aws scheduler get-schedule --name "${RECONCILE_SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null \
      || { echo "ERROR: Scheduler rule ${RECONCILE_SCHED_NAME} not found after create/update" >&2; exit 1; }
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

echo "Updating Lambda environment (preserving operator-owned VIRES_RUNNER_DISPATCH_ENABLED)..."
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" VIRES_RUNNER_DISPATCH_ENABLED true)
LAMBDA_ENV="Variables={LOG_LEVEL=INFO,VIRES_RUNNER_DISPATCH_ENABLED=${CURRENT_DISPATCH}}"
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

# ----- 4. Smoke (synthetic worker-phase event, direct invoke) ---------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing the WORKER phase via direct invoke (synthetic job id)..."
  echo "⚠ this fires a REAL spot box + REAL vires_runner_spot_bootstrap.sh run."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"vires_runner_job_id":"smoke-test-0"}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
fi

# ----- 5. Reconcile test (synthetic reconcile-phase event, direct invoke) ---

if $RECONCILE_TEST; then
  echo ""
  echo "Testing the RECONCILE phase via direct invoke..."
  echo "Read-only unless it finds a genuinely stale (>90s) queued job with no in-flight box — in which case it dispatches a REAL spot box for it (which is the correct/intended behavior, not a side effect to worry about)."
  RESP2=$(mktemp)
  trap "rm -f '${RESP2}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"reconcile":true}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP2}" >/dev/null
  cat "${RESP2}"
  echo ""
fi
