#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-alert-drain-liveness-probe
# Lambda and wire its EC2 reclaim EventBridge rules (config#3173).
#
# WHY: alert-drain runs on a twice-daily schedule, so a dead run isn't
# permanently silent (the next scheduled run still fires) — but the self-heal
# is slow (up to ~24h) and, worse, SILENT: nothing tells anyone a drain box
# died mid-run rather than finishing cleanly. This Lambda is the mid-run
# spot-reclaim checker for the alert-drain family, mirroring
# sf-watch-reclaim-sweep-handler's config#2270 mechanism: on an EC2 spot
# interruption or terminated state-change, check the box's completion
# marker; if absent, relaunch ONCE (ceiling-bounded, recorded in a relaunch
# ledger) by invoking alpha-engine-alert-drain-dispatcher directly; a SECOND
# death for the same run_id escalates LOUD instead of relaunching again.
#
# No scheduled sweep here — alert-drain's own twice-daily schedule already
# re-fires independent of any prior run's outcome; there's no "disabled
# window drops a one-shot trigger" risk the way sf-watch's config#2257 sweep
# guards against. This Lambda has exactly one trigger surface: the two EC2
# reclaim EventBridge rules below.
#
# IAM (iam-policy.json): logs + ssm:GetParameter (Telegram creds) +
# ec2:DescribeTags (Describe* — not resource-scopable) + s3:GetObject on the
# overseer/_control/completed/ prefix + s3 Get/Put on the
# overseer/_control/relaunch/ ledger + lambda:InvokeFunction scoped to
# alpha-engine-alert-drain-dispatcher.
#
# Managed OUTSIDE CloudFormation — mirrors every sibling watch-plane
# dispatcher/probe (narrow OIDC blast radius, operator-deployed only).
# Merging the PR has ZERO live effect until an operator runs this with
# --bootstrap.
#
# Usage:
#   bash .../alert-drain-liveness-probe/deploy.sh             # update code only
#   bash .../alert-drain-liveness-probe/deploy.sh --bootstrap # first-time create + wire the EC2 reclaim rules
#   bash .../alert-drain-liveness-probe/deploy.sh --dry-run   # show actions, do not apply
#   bash .../alert-drain-liveness-probe/deploy.sh --smoke     # invoke once with a no-op payload (read-only)

set -euo pipefail

# alpha-engine-config-I6619: --state must come from the automation-pause
# manifest, not from the API default (ENABLED). See infrastructure/lambdas/_shared/pause.sh.
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# alpha-engine-config-I7338: this source was MISSING while the script called
# apply_iam_policy at the #4472 auto-apply site below, so that call failed
# with `command not found` on every run — privileged or not — and the old
# `|| echo "WARN: ... expected in CI"` reported it as a permission boundary.
# shellcheck source=infrastructure/lambdas/_shared/apply_iam_policy.sh
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"

FUNCTION_NAME="alpha-engine-alert-drain-liveness-probe"
ROLE_NAME="alpha-engine-alert-drain-liveness-probe-role"
POLICY_NAME="alpha-engine-alert-drain-liveness-probe-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

# config#3173: dedicated rules (not a second target on sf-watch/ci-watch's
# rules) — keeps each Lambda's deploy.sh self-contained/idempotent without a
# cross-Lambda coupling on rule ownership. Both rules match FLEET-WIDE EC2
# events (neither event type is tag-scopable) — the handler filters by
# Name=alpha-engine-alert-drain-spot.
RECLAIM_RULE_NAMES=(
  "alpha-engine-alert-drain-spot-interruption"
  "alpha-engine-alert-drain-instance-terminated"
)
RECLAIM_RULE_PATTERNS=(
  '{"source":["aws.ec2"],"detail-type":["EC2 Spot Instance Interruption Warning"]}'
  '{"source":["aws.ec2"],"detail-type":["EC2 Instance State-change Notification"],"detail":{"state":["terminated"]}}'
)
RECLAIM_RULE_DESCRIPTIONS=(
  "EC2 spot interruption warning -> alert-drain mid-run reclaim checker (config#3173)"
  "EC2 instance terminated -> alert-drain mid-run reclaim checker (config#3173)"
)

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
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

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3 -r "${SCRIPT_DIR}/requirements.txt"

# ----- 1. Package: pip install deps + zip handler ---------------------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/../flow_doctor_telegram.py" "${PKG}/flow_doctor_telegram.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

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
  run aws iam put-role-policy --role-name "${ROLE_NAME}" --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 --role "${ROLE_ARN}" --handler index.handler \
      --zip-file "fileb://${ZIP}" --timeout 30 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1,ACCOUNT_ID='"${ACCOUNT_ID}"'}' --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # EventBridge rules for the mid-run spot-reclaim checker (config#3173).
  for i in "${!RECLAIM_RULE_NAMES[@]}"; do
    rule="${RECLAIM_RULE_NAMES[$i]}"
    echo "  Creating/updating EventBridge rule: ${rule}"
    run aws events put-rule \
      --name "${rule}" --state "$(pause_state "${rule}")" \
      --event-pattern "${RECLAIM_RULE_PATTERNS[$i]}" \
      --description "${RECLAIM_RULE_DESCRIPTIONS[$i]}" \
      --region "${REGION}" \
      --query 'RuleArn' --output text
    run aws events put-targets \
      --rule "${rule}" \
      --targets "Id=1,Arn=${FN_ARN}" \
      --region "${REGION}"
    run_tolerating "ResourceConflictException" \
      aws lambda add-permission \
      --function-name "${FUNCTION_NAME}" \
      --statement-id "eventbridge-${rule}" \
      --action lambda:InvokeFunction \
      --principal events.amazonaws.com \
      --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${rule}" \
      --region "${REGION}"
  done
fi

# ----- 3. Update function code (always, idempotent) -------------------------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

echo "✓ Code deployed."

# ----- 4. Check IAM policy against live (READ-ONLY — I9045) ----------------
# This path deploys CODE ONLY and mutates no IAM, which is what every
# deploy-*.yml header has always claimed. It was not true until 2026-08-29: the
# old call here issued `aws iam put-role-policy` on every merge and classified
# the inevitable AccessDenied as expected, so each merge left a CloudTrail
# AccessDenied on iam:PutRolePolicy from an identity that must never hold it
# (single-writer rule; identity-access-policy.md §4 — the answer to a denied
# write is not to grant it, and here it was not to make the call).
#
# What runs instead compares live IAM to iam-policy.json and, on drift, prints
# the exact operator command. IAM writes live behind --bootstrap and
# --apply-iam, where an operator states the intent with a flag.
#
# No `||` here on purpose (alpha-engine-config-I7338): a broken checker — this
# helper not sourced at all, an unreadable iam-policy.json — aborts the deploy
# under `set -e` rather than printing a reassurance.
check_iam_policy_on_deploy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json"

# ----- 4b. Reconcile the reclaim rules from the repo (always, idempotent) ---
# config-I7400. The rules' definition and ENABLED/DISABLED bit used to be
# written ONLY under --bootstrap, so a code-only deploy could never correct
# either. On 2026-08-15 `alpha-engine-alert-drain-instance-terminated` was
# hand-disabled to contain a relaunch loop, and nothing in a merge would ever
# have put it back -- re-arming was an operator step, which
# pull-request-policy.md 4.2 does not permit as a post-merge action.
#
# Re-asserting here makes this repo the SOLE writer of each reclaim rule's
# pattern and armed state -- the repo-is-the-only-writer property
# (sf-pipeline-policy.md 2.4), which a bootstrap-gated setting does not have.
# A hand-disable becomes drift the next merge corrects, visibly, in the deploy
# log, instead of surviving unrecorded until it is forgotten.
#
# `put-rule` rather than `enable-rule`/`disable-rule`: the deploy identity
# (github-actions-lambda-deploy) holds `events:PutRule` but NOT
# `events:EnableRule`, and widening a CI identity's authority to avoid one
# extra argument is the wrong trade. put-rule does not touch targets, which
# stay bootstrap-owned above.
#
# Ordered AFTER the code update on purpose: a rule must never be armed onto a
# stale handler.
for i in "${!RECLAIM_RULE_NAMES[@]}"; do
  rule="${RECLAIM_RULE_NAMES[$i]}"
  echo "  Reclaim rule ${rule}: asserting state from automation_pause.json"
  # pause_state is inlined into the put-rule statement, not hoisted into a
  # variable: tests/test_automation_pause.py scans each EventBridge write
  # STATEMENT for it, and a hoisted value reads to that guard exactly like a
  # hardcoded literal would.
  run aws events put-rule \
    --name "${rule}" --state "$(pause_state "${rule}")" \
    --event-pattern "${RECLAIM_RULE_PATTERNS[$i]}" \
    --description "${RECLAIM_RULE_DESCRIPTIONS[$i]}" \
    --region "${REGION}" \
    --query 'RuleArn' --output text
done

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1,ACCOUNT_ID='"${ACCOUNT_ID}"'}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

# ----- 4. Smoke (no-op payload; the reclaim path only fires off real EC2 events) -

# shellcheck source=infrastructure/lambdas/_shared/smoke.sh
source "${SCRIPT_DIR}/../_shared/smoke.sh"
if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (no-op payload — the reclaim path only "
  echo "fires from the EC2 EventBridge rules above)..."
  RESP=$(mktemp)
  INVOKE_STDOUT=$(aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out \
    --payload '{}' --region "${REGION}" "${RESP}")
  cat "${RESP}"; echo ""
  assert_no_function_error "${INVOKE_STDOUT}" "${RESP}"
  # SMOKE_SEMANTIC_EXEMPTION: reclaim requires a real EC2 termination event and cannot be forced safely.
  rm -f "${RESP}"
fi
