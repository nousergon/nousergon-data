#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-eod-backstop Lambda + its
# EventBridge cron rule.
#
# Phase 2 of the trading-day-gap arc (config#1229). Same-day backstop for the
# EOD Step Function, whose ONLY normal trigger is the daemon shutdown hook. If
# the daemon dies before that hook, the EOD SF never fires and the day's
# eod_pnl row goes missing → the next reconcile's headline spans multiple
# sessions (the 2026-06-24 → RGEN +14.92% class; config#1228/#1229).
#
# Fires ~22:30 UTC MON-FRI (well after the daemon's ~20:15 UTC EOD). Starts the
# EOD SF IFF it is a trading day AND no EOD started today — regardless of
# trading-box state (config-I6690, 2026-08-09: widened from the original
# box-must-be-running gate, which made a box-never-started day a silent
# no-op). See index.py for the full guard rationale.
#
# SPLIT 2026-09-30 (alpha-engine-config-I11269 follow-up): this Lambda also
# starts ne-postclose-reconcile-pipeline — on ne-data-collection-eod's terminal
# event, and from a 02:15 UTC reconcile backstop. Both rules are reconciled on
# every deploy (step 2b below), so they ship with the merge. The 22:30 UTC
# post-close rule stays bootstrap-created, keyed since the split on the
# CaptureSnapshot artifact rather than the eod_pnl row.
#
# Code-only auto-deploy on merge to main: .github/workflows/deploy-eod-
# backstop.yml (config-I6690). Infra (IAM/EventBridge rule) stays operator-run
# via --bootstrap/--apply-iam below — same narrow-OIDC-blast-radius rationale
# pipeline-watchdog / sf-telegram-notifier / eod-success-friday-shell-trigger
# use for their own bootstrap paths.
#
# SAFE ROLLOUT: --bootstrap creates the EventBridge rule DISABLED. Soak the
# Lambda via --smoke on a non-trading-day / box-down state (guaranteed no-op),
# review the first dry firings in logs, THEN enable:
#   aws events enable-rule --name alpha-engine-eod-backstop-daily --region us-east-1
#
# Usage:
#   bash infrastructure/lambdas/eod-backstop/deploy.sh             # update code only
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --bootstrap # first-time create (rule DISABLED)
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/eod-backstop/deploy.sh --smoke     # invoke once (no-op unless box up + no EOD today)

set -euo pipefail

# alpha-engine-config-I6619: --state must come from the automation-pause
# manifest, not from the API default (ENABLED). See infrastructure/lambdas/_shared/pause.sh.
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-eod-backstop"
ROLE_NAME="alpha-engine-eod-backstop-role"
POLICY_NAME="alpha-engine-eod-backstop-policy"
RULE_NAME="alpha-engine-eod-backstop-daily"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops
# instead of silently running the real deploy path (alpha-engine-config-
# I2752 incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked
# here, matching other tools' convention, and triggered a real deploy).
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

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
# Delegates to the one _shared/run_handler_tests.sh so this gate can never
# re-drift into the naive no-install `python3 -m pytest` form (config#2295).
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3 -r "${SCRIPT_DIR}/requirements.txt"

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
# alpha-engine-config-I7582: the backstop's dispatch predicate is "did the EOD
# produce its artifacts", read from the SAME module sf-telegram-notifier uses to
# decide whether a terminal message may read clean. One definition, two
# consumers — a backstop that stands down on a day the notifier would call
# incomplete is the 2026-08-17 gap.
cp "${LAMBDAS_DIR}/eod_artifact_verification.py" "${PKG}/eod_artifact_verification.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
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
      --environment 'Variables={LOG_LEVEL=INFO}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # EventBridge cron: 22:30 UTC MON-FRI — comfortably after the daemon's
  # nominal ~20:15 UTC EOD in both DST regimes. Created DISABLED for safe
  # rollout (this Lambda can START the trading EOD pipeline); enable
  # deliberately after a soak via `aws events enable-rule`.
  echo "  Creating EventBridge rule: ${RULE_NAME} (DISABLED)"
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --schedule-expression 'cron(30 22 ? * MON-FRI *)' \
    --state "$(pause_state "${RULE_NAME}")" \
    --description "EOD-pipeline backstop fire at 22:30 UTC MON-FRI (Lambda gates on trading-day + no-EOD-today; config-I6690 dropped the box-running requirement). State is derived from infrastructure/automation_pause.json, not pinned here (I6619); the soak this text referred to completed and the rule has been ENABLED since." \
    --region "${REGION}" \
    --query 'RuleArn' --output text

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  run aws events put-targets \
    --rule "${RULE_NAME}" \
    --targets "Id=1,Arn=${FN_ARN}" \
    --region "${REGION}"

  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run_tolerating "ResourceConflictException" \
    aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${RULE_ARN}" \
    --region "${REGION}"

  echo "  NOTE: rule is DISABLED. After soak: aws events enable-rule --name ${RULE_NAME} --region ${REGION}"
fi

# ----- 2b. Reconcile the post-close RECONCILE triggers (ALWAYS — not bootstrap-gated)
#
# alpha-engine-config-I11269 follow-up (2026-09-30): the collector-dependent
# half of the post-close pipeline is its own state machine,
# ne-postclose-reconcile-pipeline, and this Lambda is what starts it. Two
# rules, reconciled on EVERY deploy (the sf-telegram-notifier §2b pattern,
# config#1453) so the merge that ships the split also ships its trigger:
# put-rule / put-targets / add-permission are idempotent create-or-update calls
# the CI deploy identity (github-actions-lambda-deploy) already holds —
# events:PutRule/PutTargets on *, lambda:AddPermission on
# function:alpha-engine-*. Requires the Lambda to exist (it does since
# config#1229's --bootstrap). State comes from automation_pause.json (I6619).
#
#   * alpha-engine-eod-reconcile-trigger — ne-data-collection-eod's terminal
#     status change. SUCCEEDED / FAILED / TIMED_OUT (ABORTED is an operator's
#     deliberate stop). The reconcile's OWN heal loop starts that collection as
#     v1-eod-heal-*, and those terminals are excluded HERE and again in
#     index.py, so a heal can never start a reconcile under the loop that
#     launched it.
#   * alpha-engine-eod-reconcile-backstop-daily — 02:15 UTC TUE-SAT (22:15 EDT
#     / 21:15 EST on the MON-FRI trading evening), after the collection's
#     18:15 ET cron plus the declared caps of every workload it runs
#     (tests/test_v1_collection_readiness_wait.py derives the bound). Starts
#     the reconcile iff the eod_pnl row is missing and nothing is running.
#
# The heredoc variable names below deliberately differ from EVENT_PATTERN /
# RULE_NAME: infrastructure/eventbridge/check-drift.py discovers the FIRST of
# each per deploy.sh, and that is still the 22:30 UTC post-close rule above.
RECONCILE_TRIGGER_RULE="alpha-engine-eod-reconcile-trigger"
RECONCILE_BACKSTOP_RULE="alpha-engine-eod-reconcile-backstop-daily"
RECONCILE_FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

echo "Reconciling EventBridge rule: ${RECONCILE_TRIGGER_RULE}"
COLLECTION_TERMINAL_PATTERN=$(cat <<EOF
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "stateMachineArn": [
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-data-collection-eod"
    ],
    "status": ["SUCCEEDED", "FAILED", "TIMED_OUT"],
    "name": [{"anything-but": {"prefix": "v1-eod-heal-"}}]
  }
}
EOF
)
run aws events put-rule \
  --name "${RECONCILE_TRIGGER_RULE}" --state "$(pause_state "${RECONCILE_TRIGGER_RULE}")" \
  --event-pattern "${COLLECTION_TERMINAL_PATTERN}" \
  --description "Start ne-postclose-reconcile-pipeline when ne-data-collection-eod reaches a terminal state (alpha-engine-eod-backstop event mode; heal executions excluded)" \
  --region "${REGION}" \
  --query 'RuleArn' --output text

run aws events put-targets \
  --rule "${RECONCILE_TRIGGER_RULE}" \
  --targets "Id=1,Arn=${RECONCILE_FN_ARN}" \
  --region "${REGION}"

run_tolerating "ResourceConflictException" \
  aws lambda add-permission \
  --function-name "${FUNCTION_NAME}" \
  --statement-id "eventbridge-${RECONCILE_TRIGGER_RULE}" \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RECONCILE_TRIGGER_RULE}" \
  --region "${REGION}"

echo "Reconciling EventBridge rule: ${RECONCILE_BACKSTOP_RULE}"
run aws events put-rule \
  --name "${RECONCILE_BACKSTOP_RULE}" \
  --schedule-expression 'cron(15 2 ? * TUE-SAT *)' \
  --state "$(pause_state "${RECONCILE_BACKSTOP_RULE}")" \
  --description "Post-close RECONCILE backstop at 02:15 UTC TUE-SAT: start ne-postclose-reconcile-pipeline iff the evening's eod_pnl row is missing and no reconcile or EOD collection is running (alpha-engine-eod-backstop, mode=reconcile-backstop)." \
  --region "${REGION}" \
  --query 'RuleArn' --output text

# JSON array form, not shorthand — shorthand's `Input={...}` cannot embed
# nested JSON (the canary-replay-dispatcher lesson, config#2246).
run aws events put-targets \
  --rule "${RECONCILE_BACKSTOP_RULE}" \
  --targets "[{\"Id\":\"1\",\"Arn\":\"${RECONCILE_FN_ARN}\",\"Input\":\"{\\\"mode\\\":\\\"reconcile-backstop\\\"}\"}]" \
  --region "${REGION}"

run_tolerating "ResourceConflictException" \
  aws lambda add-permission \
  --function-name "${FUNCTION_NAME}" \
  --statement-id "eventbridge-${RECONCILE_BACKSTOP_RULE}" \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RECONCILE_BACKSTOP_RULE}" \
  --region "${REGION}"

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

# ----- 4. Smoke (synthetic empty event — exercises the full handler) --------

# shellcheck source=infrastructure/lambdas/_shared/smoke.sh
source "${SCRIPT_DIR}/../_shared/smoke.sh"
if $SMOKE; then
  echo ""
  echo "WARNING: --smoke runs the REAL handler. If today is a trading day AND no EOD"
  echo "         started today, it WILL start the EOD Step Function — REGARDLESS of"
  echo "         trading-box state (config-I6690 dropped the box-running gate). Run"
  echo "         it only when you expect a no-op (non-trading day, or an EOD already"
  echo "         ran today), or when you genuinely want to recover today's missing EOD."
  RESP=$(mktemp)
  INVOKE_STDOUT=$(aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}")
  cat "${RESP}"
  echo ""
  assert_no_function_error "${INVOKE_STDOUT}" "${RESP}"
  rm -f "${RESP}"
fi
