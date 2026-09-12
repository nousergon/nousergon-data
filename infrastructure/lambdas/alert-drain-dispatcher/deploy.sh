#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-alert-drain-dispatcher Lambda
# + the twice-daily router-targeting schedules (alpha-engine-config-I2824).
#
# Launch leg of the Overseer alert-drain (epic I2821 phase 3). The POLICY
# lives in alpha-engine-config's .github/alert-drain-prompt.md charter; this
# Lambda only launches the spot box that runs it. Dispatch path: EventBridge
# Scheduler -> alpha-engine-overseer-dispatcher router (playbook alert-drain)
# -> THIS Lambda -> spot box. The schedules deliberately target the ROUTER,
# not this executor — kill switches, verdict escalation, and the dispatch
# ledger all live there (phase-2 coherence).
#
# --bootstrap creates: (1) this Lambda's execution role + inline policy,
# (2) the Lambda, (3) the shared alpha-engine-overseer-scheduler-role
# (invoke-router-only — reusable by any future router-targeting schedule,
# e.g. the I2832 drill re-point), (4) TWO daily EventBridge Scheduler
# schedules (10:00 + 22:00 UTC — both off US market hours year-round).
#
# Managed OUTSIDE CloudFormation like the sibling dispatchers. Flagless run
# is code-only (GHA auto-deploy path); --bootstrap is operator-only.
#
# Usage:
#   bash .../alert-drain-dispatcher/deploy.sh                     # update code only
#   bash .../alert-drain-dispatcher/deploy.sh --bootstrap         # operator-only: role + Lambda + schedules
#   bash .../alert-drain-dispatcher/deploy.sh --reconcile-triggers # upsert the four schedules ONLY (the CI path; no packaging)
#   bash .../alert-drain-dispatcher/deploy.sh --apply-iam         # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../alert-drain-dispatcher/deploy.sh --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-alert-drain-dispatcher"
ROLE_NAME="alpha-engine-alert-drain-dispatcher-role"
POLICY_NAME="alpha-engine-alert-drain-dispatcher-policy"
ROUTER_FUNCTION="alpha-engine-overseer-dispatcher"
SCHED_ROLE_NAME="alpha-engine-overseer-scheduler-role"
SCHED_POLICY_NAME="invoke-overseer-dispatcher"

# alpha-engine-config-I6619: --state must come from the automation-pause
# manifest, not from the API default. `scheduler update-schedule` is a FULL
# REPLACE with no "leave the state alone" option, so every reconcile of a
# paused schedule silently un-paused it until this was sourced here.
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"

# The FOUR live drain schedules, in the shape trigger_surface_drift.py scans
# for (alpha-engine-config-I9045). Until this array existed, deploy.sh's
# bootstrap loop wrote only the 1000/2200 slots while 0400 and 1600 also
# existed live — created out of band, codified nowhere, covered by no drift
# check. Measured 2026-08-28, the cost of that: config#2902's zero-retry fix
# (MaximumRetryAttempts 185 -> 0, so a transient router error cannot
# re-dispatch a drain for a day) reached 1000/2200 and NEVER reached
# 0400/1600, which still carried the AWS default of 185 attempts / 86400s.
RECONCILE_DESCRIPTION_TRIGGERS=(
  "scheduler:alpha-engine-alert-drain-0400utc"
  "scheduler:alpha-engine-alert-drain-1000utc"
  "scheduler:alpha-engine-alert-drain-1600utc"
  "scheduler:alpha-engine-alert-drain-2200utc"
)
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
ROUTER_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${ROUTER_FUNCTION}"
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO,ALERT_DRAIN_DISPATCH_ENABLED=true}'

source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
RECONCILE_TRIGGERS=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --reconcile-triggers) RECONCILE_TRIGGERS=true ;;
    --apply-iam) APPLY_IAM=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- Reconcile the drain schedules (shared by --bootstrap and the CI path) --
#
# alpha-engine-config-I9045. Every field below is written on EVERY run, because
# `scheduler update-schedule` is a full replace: whatever this function does not
# say, AWS resets. Three of those fields exist for a reason worth stating:
#
#   --state        from the pause manifest (I6619). Omitting it defaults to
#                  ENABLED, which would silently lift Brian's 2026-08-07 pause
#                  on the next merge that touched this Lambda.
#   RetryPolicy    zero-retry (config#2902). AWS defaults to 185 attempts over
#                  24h, which would re-dispatch a drain all day on a transient
#                  router error.
#   --description  prose + the machine-readable marker derived from
#                  playbooks.yaml. This is the deliverable: the marker names the
#                  event-time freshness leg that also wakes alert-drain, so
#                  `aws scheduler get-schedule` alone answers "what runs this",
#                  instead of a DISABLED state implying nothing does.
reconcile_drain_schedules() {
  local key surface sched_name slot hh input input_escaped desc verb
  for key in "${RECONCILE_DESCRIPTION_TRIGGERS[@]}"; do
    surface="${key%%:*}"
    sched_name="${key#*:}"
    slot="${sched_name#alpha-engine-alert-drain-}"
    slot="${slot%utc}"
    hh="${slot:0:2}"
    input="{\"playbook\":\"alert-drain\",\"payload\":{\"trigger\":\"scheduled-${slot}utc\"}}"
    # Input must be a JSON-ESCAPED string inside the target JSON.
    input_escaped=$(printf '%s' "$input" | python3 -c "import json,sys; print(json.dumps(sys.stdin.read()))")
    # Fails the whole run if playbooks.yaml does not declare this schedule —
    # a schedule created with no declaration behind it is the other half of
    # the same defect, and it must not be creatable by this script.
    desc=$(python3 "${SCRIPT_DIR}/../../overseer/trigger_descriptions.py" \
      --trigger "${surface}:${sched_name}" \
      --prose "Overseer alert-drain ${slot} UTC daily via the overseer-dispatcher router (alpha-engine-config-I2824)")
    if aws scheduler get-schedule --name "${sched_name}" --region "${REGION}" >/dev/null 2>&1; then
      verb=update-schedule
    else
      verb=create-schedule
    fi
    echo "  ${verb} ${sched_name} (state=$(pause_state "${sched_name}"))"
    run aws scheduler "${verb}" \
      --name "${sched_name}" \
      --state "$(pause_state "${sched_name}")" \
      --schedule-expression "cron(0 ${hh} * * ? *)" \
      --flexible-time-window '{"Mode":"OFF"}' \
      --description "${desc}" \
      --target "{\"Arn\":\"${ROUTER_ARN}\",\"RoleArn\":\"arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}\",\"Input\":${input_escaped},\"RetryPolicy\":{\"MaximumRetryAttempts\":0,\"MaximumEventAgeInSeconds\":60}}" \
      --region "${REGION}" > /dev/null
  done
}

# ----- Schedules-only run (the CI path) --------------------------------------
# Placed BEFORE packaging and EXITING, for the reason nous-ergon-ops-I520 made
# expensive: "only" has to mean the whole run. Nothing below this point is
# needed to write a description, and a schedules-only mode that fell through
# into a code deploy would be a second deploy path nobody asked for.
if $RECONCILE_TRIGGERS; then
  echo "Reconciling alert-drain schedules (descriptions + state + retry policy)..."
  reconcile_drain_schedules
  echo "  ✓ schedules reconciled. Nothing else was touched — no code, no env, no IAM."
  exit 0
fi

# ----- 0. Validate handler syntax + preflight unit tests ---------------------
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
ast.parse(open('${SCRIPT_DIR}/index.py').read())
print('index.py syntax OK')
"

source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
NOUSERGON_LIB_REQ=$(requirement_pin "${SCRIPT_DIR}/requirements.txt" nousergon-lib)
KREPIS_REQ=$(requirement_pin "${SCRIPT_DIR}/requirements.txt" krepis)
run_handler_tests "${SCRIPT_DIR}" "${KREPIS_REQ}" "${NOUSERGON_LIB_REQ}"

# ----- 1. Package ------------------------------------------------------------
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap ----------------------------------------------------------
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
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"
  if ! $DRY_RUN; then echo "  Waiting 10s for IAM propagation..."; sleep 10; fi

  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    # Timeout 300s: the launch leg waits for spot capacity + SSM online (the
    # sibling dispatchers use the same headroom class).
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --architectures x86_64 \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
      --timeout 300 \
      --memory-size 256 \
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda function exists: ${FUNCTION_NAME}"
  fi

  # --- Scheduler role (router-invoke-only; shared by future router schedules) ---
  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler role exists: ${SCHED_ROLE_NAME}"
  fi
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"lambda:InvokeFunction\",\"Resource\":\"${ROUTER_ARN}\"}]}"
  if ! $DRY_RUN; then sleep 10; fi

  # --- The daily drain schedules -> ROUTER (playbook alert-drain) -----------
  # One function, shared with --reconcile-triggers, so bootstrap and the CI
  # path can never write different schedules (they did: this block used to
  # loop over `1000 2200` while 0400 and 1600 existed live).
  reconcile_drain_schedules
fi

# ----- 3. Update code (always) -----------------------------------------------
echo "Updating ${FUNCTION_NAME} code..."
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
  verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"
  CURRENT_ENABLED=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" ALERT_DRAIN_DISPATCH_ENABLED true)
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "Variables={LOG_LEVEL=INFO,ALERT_DRAIN_DISPATCH_ENABLED=${CURRENT_ENABLED}}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

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

echo "Done."
