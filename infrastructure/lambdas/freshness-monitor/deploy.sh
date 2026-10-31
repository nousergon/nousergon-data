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
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --reconcile-triggers # upsert the three cron rules ONLY (the CI path; no packaging)
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --apply-iam # re-apply iam-policy.json and EXIT — no code, no env, no registry (config#2825, config-I6661)
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/freshness-monitor/deploy.sh --smoke     # invoke once after deploy

set -euo pipefail

# alpha-engine-config-I6619: --state must come from the automation-pause
# manifest, not from the API default (ENABLED). See infrastructure/lambdas/_shared/pause.sh.
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-freshness-monitor"
ROLE_NAME="alpha-engine-freshness-monitor-role"
POLICY_NAME="alpha-engine-freshness-monitor-policy"
RULE_NAME="alpha-engine-freshness-monitor-cron"
HISTORICAL_RULE_NAME="alpha-engine-freshness-monitor-historical-cron"
INTRADAY_RULE_NAME="alpha-engine-freshness-monitor-intraday-cron"

# alpha-engine-config-I9045. These three rules wake the freshness monitor, and
# the daily one ALSO wakes the Overseer alert-drain playbook: on a freshness
# CRITICAL with no declared operator/recovery lane the handler invokes
# alpha-engine-overseer-dispatcher directly with {"playbook":"alert-drain"}
# (config-I3282, gated by FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED, verified
# live true 2026-08-28). Measured the same day: that leg was running the drain
# EVERY DAY while all four of alert-drain's own Scheduler schedules sat
# DISABLED — and no tag, description or field on any AWS resource said so.
# Listing the rules here opts them into the reconcile below and into
# infrastructure/overseer/trigger_surface_drift.py's grading. Each cron and
# state is written on every run: `events put-rule` is an upsert that DEFAULTS
# TO ENABLED when --state is omitted (I6619) and drops the description when
# --description is omitted.
RECONCILE_DESCRIPTION_TRIGGERS=(
  "events:alpha-engine-freshness-monitor-cron"
  "events:alpha-engine-freshness-monitor-historical-cron"
  "events:alpha-engine-freshness-monitor-intraday-cron"
)
RECONCILE_CRONS=(
  "cron(0 12 * * ? *)"
  "cron(0 4 * * ? *)"
  "cron(0/30 * * * ? *)"
)
RECONCILE_PROSE=(
  "Daily 12:00 UTC probe of the artifact freshness registry"
  "Daily 04:00 UTC historical-cycle probe (mode=historical)"
  "30-min every-day probe of the intraday-tier rows (mode=intraday)"
)
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
CODE_ONLY=false
RECONCILE_TRIGGERS=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --reconcile-triggers) RECONCILE_TRIGGERS=true ;;
    --apply-iam) APPLY_IAM=true ;;
    --smoke) SMOKE=true ;;
    --code-only) CODE_ONLY=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- Reconcile the three cron rules' state + description ------------------
# The description is prose + a marker derived from playbooks.yaml, so the
# alert-drain dispatch leg is legible from `aws events describe-rule` alone.
# Targets are NOT touched: `put-rule` with --schedule-expression leaves them in
# place, and re-declaring them here would be a second copy of the bootstrap's
# mode= Input JSON to keep in sync.
reconcile_freshness_rules() {
  local i key rule_name desc
  for i in "${!RECONCILE_DESCRIPTION_TRIGGERS[@]}"; do
    key="${RECONCILE_DESCRIPTION_TRIGGERS[$i]}"
    rule_name="${key#*:}"
    desc=$(python3 "${SCRIPT_DIR}/../../overseer/trigger_descriptions.py" \
      --trigger "${key}" --prose "${RECONCILE_PROSE[$i]}")
    echo "  put-rule ${rule_name} (state=$(pause_state "${rule_name}"))"
    run aws events put-rule \
      --name "${rule_name}" --state "$(pause_state "${rule_name}")" \
      --schedule-expression "${RECONCILE_CRONS[$i]}" \
      --description "${desc}" \
      --region "${REGION}" \
      --query 'RuleArn' --output text
  done
}

# ----- Reconcile-triggers only, then EXIT ------------------------------------
# Placed FIRST, alongside --apply-iam and for the same nous-ergon-ops-I520
# reason: nothing below installs, tests or zips anything this mode needs, and a
# mode that fell through would become a second, undeclared deploy path.
if $RECONCILE_TRIGGERS; then
  echo "Reconciling freshness-monitor EventBridge rules (state + description)..."
  reconcile_freshness_rules
  echo "  ✓ rules reconciled. Nothing else was touched — no code, no env, no registry."
  exit 0
fi

# ----- Apply IAM only, then EXIT (config#2825, config-I6661) ---------------
# Placed FIRST, before packaging, deliberately. "only" has to mean the whole
# run, not just the effects at the end: everything below installs pip deps,
# runs the handler suite and builds a ~29MB zip, none of which an IAM re-apply
# needs.
#
# The exit is the load-bearing line (nous-ergon-ops-I520). Until 2026-08-08
# this block had none, so --apply-iam applied the policy and then fell through
# into the code deploy, the environment merge, and the registry upload — and
# that upload republished ARTIFACT_REGISTRY.yaml from a sibling checkout four
# commits behind origin/main, silently reverting two rows merged an hour
# earlier. Every statement below was correct in isolation; the defect was an
# absent line.
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no registry."
  exit 0
fi

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
cp "${SCRIPT_DIR}/unregistered_envelopes.py" "${PKG}/unregistered_envelopes.py"
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
    # SF/Lambda and writes no marker. Same for config-I3282's
    # FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED (critical-page → overseer
    # alert-drain dispatch). Each dispatch path is flipped live ONLY after its
    # end-to-end drill validates it — and the routine-deploy env update below
    # MERGES with the live env, so a flipped flag survives redeploys:
    #   aws lambda update-function-configuration \
    #     --function-name alpha-engine-freshness-monitor \
    #     --environment 'Variables={...existing...,FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED=true}'
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

  # EventBridge cron: daily (config#1297, Brian-directed 2026-06-27 — the
  # prior 15-min sweep was unnecessary noise once the saturday_sf/run_calendar
  # staleness models were fixed to be jitter-tolerant). 12:00 UTC gives an
  # operator-visible check before US market open (13:30 UTC) while landing
  # comfortably after every prior day's overnight/Saturday-SF producer runs.
  # The genuinely-intraday artifacts (open_orders_latest,
  # freshness_monitor_heartbeat) are NOT blinded by this — they're also
  # covered by the separate 30-min mini-rule below.
  # All three rules are created by the SAME function the CI reconcile calls
  # (alpha-engine-config-I9045). Bootstrap used to write its own put-rule per
  # rule, with its own description literal — two writers of one field is how
  # the marker would have gone missing on the next bootstrap.
  echo "  Creating/updating the three EventBridge crons"
  reconcile_freshness_rules

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

  # Historical-mode cron: daily at 04:00 UTC, off-peak. Fires the same
  # Lambda with event={"mode": "historical"} so it probes the last N
  # cycles of each artifact and writes _freshness_monitor/history.json
  # (page 26 reads this for per-row history expanders + gap counts).
  # Lookback defaults: 12 saturday + 30 weekday/eod cycles.
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
  run_tolerating "ResourceConflictException" \
    aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${HISTORICAL_RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${HIST_RULE_ARN}" \
    --region "${REGION}"

  # Intraday mini-rule (config#1297): every 30 min, every day since
  # alpha-engine-config-I11823 (it was weekdays 14-21 UTC, which left the Claude
  # plan meter's 20-minute SLA unprobed for 14.5 h every night). Fires the same
  # Lambda with event={"mode": "intraday"} so it probes ONLY the
  # INTRADAY_ARTIFACT_IDS rows (open_orders_latest,
  # freshness_monitor_heartbeat, claude_plan_usage_latest) without touching the shared
  # check_results/heartbeat/cycle_verdict surfaces the daily sweep owns.
  # Same file:// dodge as the historical target above (embedded-quote JSON
  # doesn't survive the put-targets shorthand form).
  INTRADAY_TARGET_JSON=$(mktemp)
  cat > "${INTRADAY_TARGET_JSON}" <<EOF
[
  {
    "Id": "1",
    "Arn": "${FN_ARN}",
    "Input": "{\"mode\":\"intraday\"}"
  }
]
EOF
  run aws events put-targets \
    --rule "${INTRADAY_RULE_NAME}" \
    --targets "file://${INTRADAY_TARGET_JSON}" \
    --region "${REGION}"
  rm -f "${INTRADAY_TARGET_JSON}"

  INTRADAY_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${INTRADAY_RULE_NAME}"
  run_tolerating "ResourceConflictException" \
    aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${INTRADAY_RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${INTRADAY_RULE_ARN}" \
    --region "${REGION}"
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

# MERGE, don't overwrite (config-I3282; same silent-flag-wipe class as the
# M2_DISPATCH_TARGET lesson): operator-flipped flags on the live function
# (FRESHNESS_MONITOR_RECOVERY_ENABLED, FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED,
# cooldown overrides) must SURVIVE a routine redeploy. The previous hardcoded
# Variables={...} map silently reverted every non-listed flag to its code
# default on each deploy. Deploy-owned keys below still win on collision.
echo "Updating Lambda environment (merge — preserve operator-set flags)..."
LIVE_ENV=$(aws lambda get-function-configuration \
  --function-name "${FUNCTION_NAME}" --region "${REGION}" \
  --query 'Environment.Variables' --output json 2>/dev/null || echo '{}')
MERGED_ENV=$(python3 - "$LIVE_ENV" <<'PYEOF'
import json, sys
live = json.loads(sys.argv[1] or "null") or {}
deploy_owned = {
    "LOG_LEVEL": "INFO",
    "FRESHNESS_MONITOR_ENABLED": "true",
    "FLOW_DOCTOR_ENABLED": "1",
    "ALPHA_ENGINE_DEPLOYED": "1",
}
live.update(deploy_owned)
print(json.dumps({"Variables": live}))
PYEOF
)
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${MERGED_ENV}" \
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
  # A publish step sourced from a mutable local checkout is a supply chain with
  # no version in it (nous-ergon-ops-I520). ${CONFIG_REPO} is a SIBLING repo the
  # operator has no reason to have refreshed, and concurrent sessions routinely
  # leave it behind. Refuse rather than silently republish an older registry
  # over a newer one — this upload is a duplicate of alpha-engine-config's own
  # sync-artifact-registry.yml, so failing closed costs nothing.
  if git -C "${CONFIG_REPO}" rev-parse --git-dir >/dev/null 2>&1; then
    git -C "${CONFIG_REPO}" fetch --quiet origin main 2>/dev/null || true
    BEHIND=$(git -C "${CONFIG_REPO}" rev-list --count HEAD..origin/main 2>/dev/null || echo 0)
    DIRTY=$(git -C "${CONFIG_REPO}" status --porcelain -- private-docs/ARTIFACT_REGISTRY.yaml 2>/dev/null)
    if [ "${BEHIND}" -gt 0 ] || [ -n "${DIRTY}" ]; then
      echo "REFUSING to upload the registry: ${CONFIG_REPO} is ${BEHIND} commit(s) behind origin/main${DIRTY:+ and has uncommitted registry changes}." >&2
      echo "  Publishing from it would revert the live registry to an older state." >&2
      echo "  Fix: git -C ${CONFIG_REPO} pull --ff-only   (or re-run with --code-only to skip this step)" >&2
      exit 1
    fi
  fi
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

# shellcheck source=infrastructure/lambdas/_shared/smoke.sh
source "${SCRIPT_DIR}/../_shared/smoke.sh"
if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke..."
  RESP=$(mktemp)
  INVOKE_STDOUT=$(aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}")
  echo "Lambda response:"
  cat "${RESP}"
  echo ""
  assert_no_function_error "${INVOKE_STDOUT}" "${RESP}"
  rm -f "${RESP}"
fi
