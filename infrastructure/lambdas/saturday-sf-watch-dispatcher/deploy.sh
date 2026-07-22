#!/usr/bin/env bash
# deploy.sh — Create or update the sf-watch-dispatcher Lambda and wire its
# EventBridge trigger (terminal failure of ANY of the three fleet SFs:
# saturday / weekday / eod).
#
# Fleet-SF Watch (generalized from Saturday-only; spec: #1227, fan-out: #1375).
# The Lambda writes a per-pipeline watch-log artifact to
# s3://alpha-engine-research/consolidated/{saturday|weekday|eod}_sf_watch/{run_date}.json
# and sends a SILENT Telegram receipt; with AGENT_DISPATCH_ENABLED=true it also
# fires the per-pipeline repository_dispatch to the resilience agent.
#
# NOTE: FUNCTION_NAME / RULE_NAME retain the "saturday" string for now so the
# routine code-update path keeps targeting the LIVE function — renaming is a
# tracked fast-follow (config#1375) coupled to a re-bootstrap. Re-running with
# --bootstrap updates the EXISTING rule's event pattern in place to the 3 ARNs.
#
# Managed outside CloudFormation — same rationale as sf-telegram-notifier +
# spot-orphan-reaper (keeps the github-actions-lambda-deploy OIDC role's blast
# radius narrow; operator-deployed only). Merging the PR has ZERO live effect
# until an operator runs this with --bootstrap.
#
# Usage:
#   bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh             # update code only
#   bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh --bootstrap # first-time create + wire EventBridge
#   bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh --smoke     # invoke once with a synthetic FAILED event

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-saturday-sf-watch-dispatcher"
ROLE_NAME="alpha-engine-saturday-sf-watch-dispatcher-role"
POLICY_NAME="alpha-engine-saturday-sf-watch-dispatcher-policy"
RULE_NAME="alpha-engine-saturday-sf-watch-failed"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Shared operator-flag-preserve helper (config#1818/#2236/#2264 bug class).
source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

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

# ----- 0. Scratch dir + validate handler syntax -----------------------------
# PKG (the Lambda-zip staging dir) is created up front; the shared handler-
# test gate (0b) provisions its OWN scratch dir for pytest + deps (config#2381).

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------
# The gate now runs on bare CI runners (deploy-saturday-sf-watch-dispatcher.yml,
# config#2295), not just operator laptops where pytest/boto3 were ambient — so
# it must self-provision its own test deps or it dies "No module named pytest"
# (2026-07-12 incident; same class the fleet already hit 2026-07-02/-04 and
# fixed the same way in ci-watch-dispatcher, spot-orphan-reaper, groom-liveness-
# probe, et al). test_handler.py does a REAL `import index` (no sys.modules
# stubs), which pulls boto3, the local flow_doctor_telegram (found via the
# test's sys.path parent insert), and nousergon_lib.flow_doctor_fleet. pytest +
# boto3 + the pinned nousergon-lib + krepis are installed by the shared gate into its own scratch
# dir — NOT the caller's global site-packages, not bundled into the
# Lambda zip.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
NOUSERGON_LIB_REQ=$(grep -E '^nousergon-lib' "${SCRIPT_DIR}/requirements.txt" | head -1)
KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" boto3 "${KREPIS_REQ}" "${NOUSERGON_LIB_REQ}"

# ----- 1. Package: pip install deps + zip handler ---------------------------

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
      --environment 'Variables={LOG_LEVEL=INFO,AGENT_DISPATCH_ENABLED=false,M2_DISPATCH_TARGET=repository_dispatch,FAST_PATH_ENABLED=false,SF_WATCH_DISPATCH_AFTER_ESCALATION=true,EOD_SF_WATCH_DISPATCH_AFTER_ESCALATION=true,SF_WATCH_MAX_DISPATCHES_SATURDAY=8,SF_WATCH_MAX_DISPATCHES_WEEKDAY=2,SF_WATCH_MAX_DISPATCHES_EOD=2,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # EventBridge rule: terminal-failure statuses of ANY registered fleet SF.
  # One rule, one target. The ARN list is in ENFORCED lockstep with
  # index.PIPELINES — tests/test_sf_watch_rule_pattern_lockstep.py statically
  # extracts this heredoc's pipeline names and fails CI on drift. (Added
  # 2026-07-21, alpha-engine-config-I3187: the I2890 re-inline retired
  # ne-weekly-advisory-pipeline / ne-modelzoo-sunday-pipeline from
  # index.PIPELINES, but this heredoc kept both ARNs because the old
  # "keep in lockstep" comment was prose, not a contract — the live rule
  # then re-acquired the stale ARNs on bootstrap.)
  echo "  Creating EventBridge rule: ${RULE_NAME}"
  EVENT_PATTERN=$(cat <<EOF
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "stateMachineArn": [
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline",
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-preopen-trading-pipeline",
      "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline"
    ],
    "status": ["FAILED", "TIMED_OUT", "ABORTED"]
  }
}
EOF
)
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --event-pattern "${EVENT_PATTERN}" \
    --description "Fleet SF (weekly/preopen/postclose) terminal failure → sf-watch-dispatcher" \
    --region "${REGION}" \
    --query 'RuleArn' --output text

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  run aws events put-targets \
    --rule "${RULE_NAME}" \
    --targets "Id=1,Arn=${FN_ARN}" \
    --region "${REGION}"

  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${RULE_ARN}" \
    --region "${REGION}" 2>/dev/null || true
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

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
# AGENT_DISPATCH_ENABLED is an OPERATOR-OWNED runtime flag (the M2 autonomous-
# dispatch gate) — the update path must PRESERVE its live value, never reset
# it to the bootstrap default. 2026-07-05 incident (config#1818): this line
# hardcoded false; the routine groom-removal redeploy silently reverted the
# operator-enabled flag, and the resilience agent dispatched NOTHING for the
# 2026-07-06 preopen SF failures (dispatched=False on both) while the market
# was open. Bootstrap (create-function above) still defaults false — safe
# rollout posture for a NEW deployment only.
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" AGENT_DISPATCH_ENABLED false)
# M2_DISPATCH_TARGET (alpha-engine-config-I2823) — operator-owned routing flag:
# repository_dispatch (legacy GitHub round-trip) vs overseer (direct router).
CURRENT_M2_TARGET=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" M2_DISPATCH_TARGET repository_dispatch)
# FAST_PATH_ENABLED (config#1900) is operator-owned exactly like
# AGENT_DISPATCH_ENABLED — preserve the live value across redeploys.
CURRENT_FAST_PATH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" FAST_PATH_ENABLED false)
# SF_WATCH_DISPATCH_AFTER_ESCALATION (config#2003, renamed from
# EOD_SF_WATCH_DISPATCH_AFTER_ESCALATION by config#2953 — the old name
# implied EOD-only scope on a fleet-wide dispatcher) is operator-owned
# exactly like the two flags above — preserved via the shared helper
# (config#1818/#2264 class: the update call REPLACES the whole Variables
# map, so any operator-set flag missing here silently resets on redeploy).
# Default flipped false->true (config#2953, shepherd ruling: the overseer
# owns the incident arc by default; the config#2269 ceiling is the bound).
# Read the OLD name's live value first as the fallback default so an
# operator override made under the old name before this rename survives one
# more redeploy; both names are then written in lockstep for one release so
# either name reflects the live posture (drop EOD_* in a follow-up PR).
CURRENT_DISPATCH_AFTER_ESCALATION_LEGACY=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" EOD_SF_WATCH_DISPATCH_AFTER_ESCALATION true)
CURRENT_DISPATCH_AFTER_ESCALATION=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" SF_WATCH_DISPATCH_AFTER_ESCALATION "${CURRENT_DISPATCH_AFTER_ESCALATION_LEGACY}")
# SF_WATCH_MAX_DISPATCHES_* (config#2269) are CONFIG DEFAULTS, not operator
# kill-switches — deliberately NOT run through preserve_env_flag: the
# canonical way to change a per-cadence dispatch ceiling is a PR editing
# index.py's defaults + these pins together (a live env tweak SHOULD be reset
# to the reviewed value on the next redeploy). Values mirror the charter's
# Brian-ruled per-cadence budgets (saturday 8 / weekday 2 / eod 2).
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "Variables={LOG_LEVEL=INFO,AGENT_DISPATCH_ENABLED=${CURRENT_DISPATCH},M2_DISPATCH_TARGET=${CURRENT_M2_TARGET},FAST_PATH_ENABLED=${CURRENT_FAST_PATH},SF_WATCH_DISPATCH_AFTER_ESCALATION=${CURRENT_DISPATCH_AFTER_ESCALATION},EOD_SF_WATCH_DISPATCH_AFTER_ESCALATION=${CURRENT_DISPATCH_AFTER_ESCALATION},SF_WATCH_MAX_DISPATCHES_SATURDAY=8,SF_WATCH_MAX_DISPATCHES_WEEKDAY=2,SF_WATCH_MAX_DISPATCHES_EOD=2,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

# ----- 4. Smoke (synthetic FAILED event) ------------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic FAILED event)..."
  RESP=$(mktemp)
  PAYLOAD=$(cat <<'EOF'
{
  "source": "aws.states",
  "detail-type": "Step Functions Execution Status Change",
  "detail": {
    "status": "FAILED",
    "stateMachineArn": "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
    "executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:smoke-test",
    "name": "smoke-test",
    "startDate": 0,
    "stopDate": 60000
  }
}
EOF
)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload "${PAYLOAD}" \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
