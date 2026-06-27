#!/usr/bin/env bash
# deploy.sh — Create or update the changelog-cloudwatch-mirror Lambda
# and wire per-Lambda CloudWatch Logs subscription filters.
#
# This Lambda is the second half of the changelog event-mining coverage
# matrix (sibling SNS-mirror Lambda is the first half). It receives
# CloudWatch Logs subscription-filter events for every alpha-engine
# Lambda's error patterns and writes one structured incident entry
# per matched log event.
#
# Managed outside CloudFormation, same rationale as the SNS-mirror
# Lambda — keeps the github-actions-lambda-deploy OIDC role's blast
# radius narrow.
#
# Usage:
#   bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh           # update code only
#   bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --bootstrap   # first-time create + wire all subscriptions
#   bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --wire-subs   # (re)apply subscription filters only
#   bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --dry-run     # show actions, do not apply
#   bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --smoke       # publish a synthetic ERROR log + verify entry
#   bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --audit-targets  # diff live alpha-engine-* Lambdas vs TARGET_FUNCTIONS, exit 1 on drift
#
# --audit-targets (config#862): TARGET_FUNCTIONS is hand-maintained, so a
# newly-deployed alpha-engine-* Lambda won't auto-subscribe to the error
# mirror. This mode lists live functions (aws lambda list-functions) whose
# name starts with "alpha-engine-", subtracts the two changelog mirrors
# (the recursion guard exclusions, which must NOT be subscribed), and
# diffs against TARGET_FUNCTIONS. Exits 1 if any live Lambda is missing
# from the array (or any array entry no longer exists), so it can run as a
# Saturday-SF substrate-health row or a periodic CI step. Read-only.
#
# Auth: uses active AWS CLI creds. Personal IAM user (cipher813) has
# enough perms; this script is intentionally NOT wired into CI.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-changelog-cloudwatch-mirror"
ROLE_NAME="alpha-engine-changelog-cloudwatch-mirror-role"
POLICY_NAME="alpha-engine-changelog-cloudwatch-mirror-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
SUBSCRIPTION_FILTER_NAME="alpha-engine-error-mirror"
# Subscription filter pattern — matches lines containing any of:
#   ERROR, CRITICAL, "Task timed out". Quoted strings are required for
#   patterns containing spaces per AWS subscription-filter syntax.
FILTER_PATTERN='?ERROR ?CRITICAL ?"Task timed out"'

# Target Lambdas — every alpha-engine-* function EXCEPT the two
# changelog-mirror Lambdas (recursion guard: if the mirror Lambda
# itself errors, its log lines that contain "ERROR" must not feed
# back into itself).
TARGET_FUNCTIONS=(
  "alpha-engine-data-collector"
  "alpha-engine-ec2-lifecycle"
  "alpha-engine-predictor-health-check"
  "alpha-engine-predictor-inference"
  "alpha-engine-replay-concordance"
  "alpha-engine-replay-counterfactual"
  "alpha-engine-research-alerts"
  "alpha-engine-research-eval-judge"
  "alpha-engine-research-eval-rolling-mean"
  "alpha-engine-research-rationale-clustering"
  "alpha-engine-research-runner"
  # Operational/infra Lambdas — capture completeness (config#1273 Phase B).
  # Their ERROR/CRITICAL/timeout logs now mirror into the changelog event-lake
  # (subsystem inferred as "infrastructure" by default). The two changelog
  # mirrors are deliberately EXCLUDED per the recursion guard above and get a
  # CloudWatch Errors alarm instead (watch-the-watchers — config#1273 follow-up).
  "alpha-engine-eod-backstop"
  "alpha-engine-eod-success-friday-shell-trigger"
  "alpha-engine-freshness-monitor"
  "alpha-engine-friday-shell-run-report"
  "alpha-engine-pipeline-watchdog"
  "alpha-engine-saturday-integrity-sentinel"
  "alpha-engine-saturday-sf-success-groom-dispatcher"
  "alpha-engine-saturday-sf-watch-dispatcher"
  "alpha-engine-sf-telegram-notifier"
  "alpha-engine-spot-orphan-reaper"
)

DRY_RUN=false
BOOTSTRAP=false
WIRE_SUBS=false
SMOKE=false
AUDIT_TARGETS=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --wire-subs) WIRE_SUBS=true ;;
    --smoke) SMOKE=true ;;
    --audit-targets) AUDIT_TARGETS=true ;;
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

# ----- TARGET_FUNCTIONS drift audit (config#862) ----------------------------
# Read-only. List live alpha-engine-* Lambdas, subtract the recursion-guard
# exclusions (the two changelog mirrors must never subscribe to the error
# mirror), and diff against the hand-maintained TARGET_FUNCTIONS array.
# Exit 1 on any drift so this can gate a CI step or Saturday-SF row.
if $AUDIT_TARGETS; then
  echo "Auditing TARGET_FUNCTIONS drift (region=${REGION})..."

  # Recursion-guard exclusions: the two changelog mirrors. A live
  # alpha-engine-* Lambda matching one of these is EXPECTED to be absent
  # from TARGET_FUNCTIONS and must not be flagged.
  EXCLUDED=(
    "${FUNCTION_NAME}"                          # this cloudwatch mirror
    "alpha-engine-changelog-incident-mirror"    # the SNS mirror sibling
  )

  # Live alpha-engine-* function names, one per line, sorted.
  LIVE=$(aws lambda list-functions \
    --query "Functions[?starts_with(FunctionName, 'alpha-engine-')].FunctionName" \
    --output text --region "${REGION}" | tr '\t' '\n' | sort -u)

  # Expected = live minus the excluded mirrors.
  EXPECTED=$(comm -23 \
    <(printf '%s\n' "${LIVE}") \
    <(printf '%s\n' "${EXCLUDED[@]}" | sort -u))

  CONFIGURED=$(printf '%s\n' "${TARGET_FUNCTIONS[@]}" | sort -u)

  # Live Lambdas not yet subscribed (in EXPECTED, not in CONFIGURED).
  MISSING=$(comm -23 <(printf '%s\n' "${EXPECTED}") <(printf '%s\n' "${CONFIGURED}"))
  # Configured targets that no longer exist (in CONFIGURED, not in LIVE).
  STALE=$(comm -23 <(printf '%s\n' "${CONFIGURED}") <(printf '%s\n' "${LIVE}"))

  DRIFT=0
  if [[ -n "${MISSING}" ]]; then
    DRIFT=1
    echo "  ✗ Live alpha-engine-* Lambdas NOT in TARGET_FUNCTIONS (won't mirror errors):"
    printf '      %s\n' ${MISSING}
  fi
  if [[ -n "${STALE}" ]]; then
    DRIFT=1
    echo "  ✗ TARGET_FUNCTIONS entries that no longer exist as live Lambdas:"
    printf '      %s\n' ${STALE}
  fi

  if [[ "${DRIFT}" -eq 0 ]]; then
    echo "  ✓ No drift — TARGET_FUNCTIONS matches live alpha-engine-* Lambdas."
    exit 0
  fi
  echo "  → Reconcile by editing TARGET_FUNCTIONS, then re-run with --wire-subs."
  exit 1
fi

# ----- 0. Validate handler ---------------------------------------------------

python3 -c "
import ast, sys
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Running handler smoke tests..."
  python3 "${SCRIPT_DIR}/test_handler.py" >/dev/null 2>&1 && echo "  ✓ Smoke tests pass"
fi

# ----- 1. Bootstrap (first-time only) ---------------------------------------

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # 1a. IAM role with Lambda trust policy
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

  # 1b. Inline policy (S3 PutObject + Lambda log writing)
  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  # 1c. Wait for IAM role to propagate (eventually-consistent)
  if ! $DRY_RUN; then
    echo "  Waiting 10s for IAM role propagation..."
    sleep 10
  fi

  # 1d. Package handler + vendored vocab + create function
  PKG=$(mktemp -d)
  trap "rm -rf '$PKG'" EXIT
  cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
  cp "${SCRIPT_DIR}/../_shared/vocab.py" "${PKG}/vocab.py"
  ZIP="${PKG}/function.zip"
  (cd "${PKG}" && zip -q "function.zip" index.py vocab.py)
  echo "  Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 30 \
      --memory-size 128 \
      --environment 'Variables={CHANGELOG_BUCKET=alpha-engine-research,CHANGELOG_STRUCTURED_PREFIX=changelog/entries,CHANGELOG_QUARANTINE_PREFIX=changelog/quarantine}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  fi
fi

# ----- 2. Update function code (always) -------------------------------------

if ! $BOOTSTRAP; then
  PKG=$(mktemp -d)
  trap "rm -rf '$PKG'" EXIT
  cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
  cp "${SCRIPT_DIR}/../_shared/vocab.py" "${PKG}/vocab.py"
  ZIP="${PKG}/function.zip"
  (cd "${PKG}" && zip -q "function.zip" index.py vocab.py)
  echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

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

  echo "Ensuring env config..."
  run aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment 'Variables={CHANGELOG_BUCKET=alpha-engine-research,CHANGELOG_STRUCTURED_PREFIX=changelog/entries,CHANGELOG_QUARANTINE_PREFIX=changelog/quarantine}' \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text

  if ! $DRY_RUN; then
    aws lambda wait function-updated \
      --function-name "${FUNCTION_NAME}" \
      --region "${REGION}"
  fi
fi

echo "✓ Code deployed."

# ----- 3. Wire per-Lambda subscription filters ------------------------------

if $BOOTSTRAP || $WIRE_SUBS; then
  RELAY_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  echo ""
  echo "Wiring CloudWatch Logs subscription filters..."
  for fn in "${TARGET_FUNCTIONS[@]}"; do
    LOG_GROUP="/aws/lambda/${fn}"

    # Ensure log group exists (Lambda creates it on first invocation; if
    # the function has never run, the group won't exist yet).
    if ! aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP}" --region "${REGION}" \
        --query "logGroups[?logGroupName=='${LOG_GROUP}']" --output text | grep -q "${LOG_GROUP}"; then
      echo "  ⊘ ${fn} — log group does not exist yet (Lambda hasn't run). Skipping."
      continue
    fi

    # Per-source-Lambda permission allowing CloudWatch Logs to invoke the relay.
    # StatementId per source — idempotent only if removed-then-added; ignore
    # ResourceConflictException on re-runs.
    PERM_SID="cwlogs-invoke-${fn}"
    PERM_OUT=$(aws lambda add-permission \
      --function-name "${FUNCTION_NAME}" \
      --statement-id "${PERM_SID}" \
      --action lambda:InvokeFunction \
      --principal "logs.${REGION}.amazonaws.com" \
      --source-arn "arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${LOG_GROUP}:*" \
      --region "${REGION}" 2>&1) || true

    # IAM/Lambda permission propagation is eventually consistent —
    # put-subscription-filter validates the resource policy and fails
    # fast if it hasn't replicated yet. Sleep only when we just added a
    # NEW permission (not on idempotent re-runs).
    if echo "${PERM_OUT}" | grep -q '"Statement"'; then
      if ! $DRY_RUN; then
        sleep 5
      fi
    fi

    # Subscription filter — idempotent, AWS overwrites by name. Retry once
    # on InvalidParameterException to ride out residual IAM propagation lag.
    echo "  → ${fn}"
    if ! run aws logs put-subscription-filter \
      --log-group-name "${LOG_GROUP}" \
      --filter-name "${SUBSCRIPTION_FILTER_NAME}" \
      --filter-pattern "${FILTER_PATTERN}" \
      --destination-arn "${RELAY_ARN}" \
      --region "${REGION}" 2>&1; then
      echo "    (retrying after 10s — IAM propagation)"
      if ! $DRY_RUN; then sleep 10; fi
      run aws logs put-subscription-filter \
        --log-group-name "${LOG_GROUP}" \
        --filter-name "${SUBSCRIPTION_FILTER_NAME}" \
        --filter-pattern "${FILTER_PATTERN}" \
        --destination-arn "${RELAY_ARN}" \
        --region "${REGION}"
    fi
  done
  echo "✓ Subscription filters applied."
fi

# ----- 4. Smoke test --------------------------------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct Lambda invoke (synthetic payload)..."
  TS=$(date -u +%s)
  # Build a synthetic CloudWatch Logs subscription filter event payload.
  # Keep this in sync with index.py's expected shape.
  PAYLOAD_DECODED='{"messageType":"DATA_MESSAGE","owner":"'${ACCOUNT_ID}'","logGroup":"/aws/lambda/alpha-engine-predictor-inference","logStream":"smoke-test","subscriptionFilters":["alpha-engine-error-mirror"],"logEvents":[{"id":"smoke-'${TS}'","timestamp":'${TS}'000,"message":"[ERROR] deploy.sh smoke test '${TS}'"}]}'
  PAYLOAD_BLOB=$(printf '%s' "${PAYLOAD_DECODED}" | gzip | base64)
  EVENT='{"awslogs":{"data":"'${PAYLOAD_BLOB}'"}}'

  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload "${EVENT}" \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  echo "  → Check entry at: aws s3 ls s3://alpha-engine-research/changelog/entries/$(date -u +%Y-%m-%d)/ --recursive | grep '${TS}'"
fi
