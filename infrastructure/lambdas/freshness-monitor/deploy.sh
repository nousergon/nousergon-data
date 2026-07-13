#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-freshness-monitor Lambda,
# wire its EventBridge cron, and upload the artifact registry from the
# local alpha-engine-config clone to S3.
#
# Phase 3 of the artifact-freshness-monitor arc (plan doc at
# ~/Development/alpha-engine-docs/private/artifact-freshness-monitor-260527.md).
# Loads `private-docs/ARTIFACT_REGISTRY.yaml` from the operator's local
# clone of nousergon/alpha-engine-config and uploads it to
# s3://alpha-engine-research/_freshness_monitor/ARTIFACT_REGISTRY.yaml.
# Validates the registry locally before upload — a malformed registry
# never reaches S3.
#
# Managed outside CloudFormation — same packaging rationale as
# sf-telegram-notifier / spot-orphan-reaper / changelog-cloudwatch-mirror.
# CODE auto-deploys on merge to main via
# `.github/workflows/deploy-freshness-monitor.yml` (path-filtered to
# `infrastructure/lambdas/freshness-monitor/**`), which runs this script
# with `--code-only` under the github-actions-lambda-deploy OIDC role
# (granted `lambda:UpdateFunctionCode` on `alpha-engine-*`). The artifact
# REGISTRY is owned by alpha-engine-config and uploaded to S3 by its own
# `sync-artifact-registry.yml` on registry merges — so `--code-only` skips
# the registry validation + upload here (no ae-config clone needed in CI).
# The full (non-`--code-only`) path remains the operator command for a
# from-a-laptop deploy that also re-pushes the registry. Phase 6 cutover
# flips FRESHNESS_MONITOR_ENABLED via
# `aws lambda update-function-configuration` without redeploying.
#
# Usage:
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh             # update code + registry (operator; needs ae-config clone)
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --code-only # update code ONLY (CI path; no registry, no ae-config clone)
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --bootstrap # first-time create + wire EventBridge
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --smoke     # invoke once after deploy

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-freshness-monitor"
ROLE_NAME="alpha-engine-freshness-monitor-role"
POLICY_NAME="alpha-engine-freshness-monitor-policy"
RULE_NAME="alpha-engine-freshness-monitor-cron"
HISTORICAL_RULE_NAME="alpha-engine-freshness-monitor-historical-cron"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Registry SoT. The validator lives next to the YAML in alpha-engine-config;
# we sanity-check the file parses + matches the lib's expected schema
# before uploading.
CONFIG_REPO="${CONFIG_REPO:-${HOME}/Development/alpha-engine-config}"
REGISTRY_LOCAL="${CONFIG_REPO}/private-docs/ARTIFACT_REGISTRY.yaml"
REGISTRY_VALIDATOR="${CONFIG_REPO}/scripts/validate_artifact_registry.py"
REGISTRY_BUCKET="alpha-engine-research"
REGISTRY_S3_KEY="_freshness_monitor/ARTIFACT_REGISTRY.yaml"

DRY_RUN=false
BOOTSTRAP=false
SMOKE=false
CODE_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --smoke) SMOKE=true ;;
    --code-only) CODE_ONLY=true ;;
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

# ----- 0a. Syntax-check handler (no imports — works on bare python) --------

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Verify ae-config clone present (registry validation runs later) -
# Skipped under --code-only: the registry is owned + S3-synced by
# alpha-engine-config (sync-artifact-registry.yml), so a code-only CI
# deploy needs no ae-config clone.

if ! $CODE_ONLY; then
  if [[ ! -f "${REGISTRY_LOCAL}" ]]; then
    echo "❌ Registry not found at ${REGISTRY_LOCAL}"
    echo "   Clone nousergon/alpha-engine-config into ~/Development/ or set CONFIG_REPO"
    echo "   (or pass --code-only to deploy code without re-pushing the registry)"
    exit 1
  fi

  if [[ ! -f "${REGISTRY_VALIDATOR}" ]]; then
    echo "❌ Validator not found at ${REGISTRY_VALIDATOR}"
    echo "   alpha-engine-config must be at the post-PR-#344 commit (artifact-registry-bootstrap merged)"
    exit 1
  fi
fi

# ----- 1. Package: pip install runtime deps into $PKG ----------------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing runtime deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

# ----- 1a. Validate registry locally before upload --------------------------
# Runs AFTER step 1's pip install — the validator imports yaml which isn't
# guaranteed in the caller's bare python. PYTHONPATH=$PKG resolves it.
# Skipped under --code-only (alpha-engine-config CI validates + uploads it).

if ! $CODE_ONLY; then
  echo "Validating registry locally before upload..."
  PYTHONPATH="${PKG}" python3 "${REGISTRY_VALIDATOR}" --registry "${REGISTRY_LOCAL}"
fi

# ----- 1b. Preflight handler unit tests with runtime deps available --------
# The test does a REAL `import index` (yaml + boto3 + nousergon_lib.{alerts,
# artifact_freshness}), so the shared gate provisions the lambda's own
# requirements.txt alongside pytest into its own scratch dir (config#2381) —
# host wheels, NOT bundled into the Lambda zip.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" -r "${SCRIPT_DIR}/requirements.txt"

# ----- 1c. Copy handler + zip Lambda package -------------------------------

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
    # ENFORCE mode: FRESHNESS_MONITOR_ENABLED=true.
    # Phase 6 cutover EXECUTED 2026-06-25 after a ~1mo observe soak (the
    # monitor correctly detected the missed 6/20 Saturday cycle the whole
    # time but stayed muted). Code is now the source of truth for the flag —
    # a fresh bootstrap comes up enforcing. For an already-deployed function,
    # flip live via `aws lambda update-function-configuration` (no redeploy).
    #
    # config#1240 auto-remediation: FRESHNESS_MONITOR_RECOVERY_ENABLED defaults
    # OFF (OBSERVE) — a fresh bootstrap LOGS the would-dispatch but calls no
    # SF/Lambda and writes no marker. The dispatch path is flipped live ONLY
    # after the end-to-end drill validates it (delete a recent load-bearing
    # artifact, confirm the monitor auto-dispatches the correct backfill):
    #   aws lambda update-function-configuration \
    #     --function-name alpha-engine-freshness-monitor \
    #     --environment 'Variables={LOG_LEVEL=INFO,FRESHNESS_MONITOR_ENABLED=true,FRESHNESS_MONITOR_RECOVERY_ENABLED=true}'
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 120 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,FRESHNESS_MONITOR_ENABLED=true,FRESHNESS_MONITOR_RECOVERY_ENABLED=false}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # EventBridge cron: every 15 minutes. Sub-15min granularity isn't
  # load-bearing (alerts dedup by cadence window) — this is the floor
  # for "operator-perceived" probe cadence.
  echo "  Creating EventBridge cron: ${RULE_NAME}"
  run aws events put-rule \
    --name "${RULE_NAME}" \
    --schedule-expression "cron(*/15 * * * ? *)" \
    --description "Every 15min probe of the artifact freshness registry" \
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

  # Historical-mode cron: daily at 04:00 UTC, off-peak. Fires the same
  # Lambda with event={"mode": "historical"} so it probes the last N
  # cycles of each artifact and writes _freshness_monitor/history.json
  # (page 26 reads this for per-row history expanders + gap counts).
  # Lookback defaults: 12 saturday + 30 weekday/eod cycles.
  echo "  Creating EventBridge historical cron: ${HISTORICAL_RULE_NAME}"
  run aws events put-rule \
    --name "${HISTORICAL_RULE_NAME}" \
    --schedule-expression "cron(0 4 * * ? *)" \
    --description "Daily 04:00 UTC historical-cycle probe (mode=historical)" \
    --region "${REGION}" \
    --query 'RuleArn' --output text

  # JSON Input (`{"mode":"historical"}`) doesn't fit the put-targets
  # shorthand form (Id=,Arn=,Input= chokes on the embedded quotes +
  # comma). Write a temp JSON file + pass via file:// to dodge the
  # shell-quoting trap. Caught live 2026-05-28 when --bootstrap re-run
  # tripped argparse on the shorthand.
  HIST_TARGET_JSON=$(mktemp)
  cat > "${HIST_TARGET_JSON}" <<EOF
[
  {
    "Id": "1",
    "Arn": "${FN_ARN}",
    "Input": "{\"mode\":\"historical\"}"
  }
]
EOF
  run aws events put-targets \
    --rule "${HISTORICAL_RULE_NAME}" \
    --targets "file://${HIST_TARGET_JSON}" \
    --region "${REGION}"
  rm -f "${HIST_TARGET_JSON}"

  HIST_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${HISTORICAL_RULE_NAME}"
  run aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${HISTORICAL_RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${HIST_RULE_ARN}" \
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

echo "Updating Lambda environment (flow-doctor SSM hydration; preserve alert flags)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,FRESHNESS_MONITOR_ENABLED=true,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

# ----- 4. Upload registry to S3 ---------------------------------------------
# Skipped under --code-only: alpha-engine-config's sync-artifact-registry.yml
# owns the registry → S3 upload on registry merges. Keeping it here too would
# double-write (harmless) but requires the ae-config clone the CI path lacks.

if ! $CODE_ONLY; then
  echo "Uploading registry: ${REGISTRY_LOCAL} → s3://${REGISTRY_BUCKET}/${REGISTRY_S3_KEY}"
  run aws s3 cp \
    "${REGISTRY_LOCAL}" \
    "s3://${REGISTRY_BUCKET}/${REGISTRY_S3_KEY}" \
    --region "${REGION}"
  echo "✓ Registry uploaded."
else
  echo "↪ --code-only: skipping registry upload (owned by alpha-engine-config sync workflow)."
fi

# ----- 5. Smoke (direct invoke) ---------------------------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke..."
  RESP=$(mktemp)
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  echo "Lambda response:"
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
