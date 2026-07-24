#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-sf-watch-liveness-probe Lambda
# and wire its EventBridge Scheduler rules.
#
# WHY: Fleet-SF Watch (saturday-sf-watch-dispatcher) is event-driven — it only
# fires when a registered pipeline's SF reaches a terminal FAILED/TIMED_OUT/
# ABORTED status via its EventBridge rule. Nothing notices if the WATCHER's own
# wiring silently breaks — exactly what happened 2026-06-29: the rule pointed
# at a deleted SF ARN for an unknown period, and the Lambda's own Errors metric
# stayed at zero the whole time (it simply never got invoked). This probe is
# the external watchdog FOR the watchdog: read-only, schedule-aware, asserts
# the rule/registry/target-Lambda wiring is intact, and LOUD-pings Telegram
# only when something's actually broken (silent-unless-broken, mirroring the
# groom-liveness-probe's philosophy one layer up).
#
# IAM (iam-policy.json): logs + ssm:GetParameter (Telegram creds) +
# events:DescribeRule/ListTargetsByRule + states:DescribeStateMachine (scoped
# to the registered pipeline ARNs) + lambda:GetFunctionConfiguration (scoped
# to the dispatchers) + s3 Get/Put on the dedup state key. The probe path is
# read-only; the mid-run reclaim-checker branch (config#2270) additionally
# needs ec2:DescribeTags (Describe* — not resource-scopable), s3:GetObject on
# the sf_watch/_control/completed/ markers, s3 Get/Put on the watch-log
# prefixes (the reclaim_relaunch record), and lambda:InvokeFunction scoped to
# alpha-engine-sf-watch-spot-dispatcher.
#
# Cadence (UTC): the SWEEP runs twice daily (the reclaim checker is event-driven,
# not scheduled). Offset from the overseer-liveness-probe's cadence (06:50/14:50)
# purely to avoid simultaneous invocation — the sweep isn't tied to any
# pipeline's own schedule:
#   06:45 daily   cron(45 6 * * ? *)
#   14:45 daily   cron(45 14 * * ? *)
#
# Managed OUTSIDE CloudFormation — mirrors the sibling dispatchers/probes
# (narrow OIDC blast radius, operator-deployed only). Merging the PR has ZERO
# live effect until an operator runs this with --bootstrap.
#
# Usage:
#   bash .../sf-watch-liveness-probe/deploy.sh             # update code only
#   bash .../sf-watch-liveness-probe/deploy.sh --bootstrap # first-time create + wire schedules + EC2 reclaim rules (config#2270)
#   bash .../sf-watch-liveness-probe/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../sf-watch-liveness-probe/deploy.sh --dry-run   # show actions, do not apply
#   bash .../sf-watch-liveness-probe/deploy.sh --smoke     # invoke once (read-only check; pings only on a real problem)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-sf-watch-liveness-probe"
ROLE_NAME="alpha-engine-sf-watch-liveness-probe-role"
POLICY_NAME="alpha-engine-sf-watch-liveness-probe-policy"
SCHED_ROLE_NAME="alpha-engine-sf-watch-liveness-probe-scheduler-role"
SCHED_POLICY_NAME="invoke-sf-watch-liveness-probe"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

SCHED_NAMES=(
  "alpha-engine-sf-watch-liveness-0645-daily"
  "alpha-engine-sf-watch-liveness-1445-daily"
)
SCHED_CRONS=(
  "cron(45 6 * * ? *)"
  "cron(45 14 * * ? *)"
)
SCHED_PREFIX="alpha-engine-sf-watch-liveness-"

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

run() {
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
# Delegates to the one _shared/run_handler_tests.sh so this gate can never
# re-drift into the naive no-install `python3 -m pytest` form (config#2295).
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

  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} on the liveness cadence" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"${FN_ARN}\"}]}"
  echo "  Applying Scheduler invoke policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "${SCHED_INVOKE_POLICY}"

  if ! $DRY_RUN; then echo "  Waiting 10s for Scheduler role propagation..."; sleep 10; fi

  for i in "${!SCHED_NAMES[@]}"; do
    name="${SCHED_NAMES[$i]}"
    cron="${SCHED_CRONS[$i]}"
    target="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${SCHED_ROLE_ARN}\",\"Input\":\"{}\"}"
    if aws scheduler get-schedule --name "${name}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
      echo "  Updating Scheduler rule: ${name} → ${cron}"
      run aws scheduler update-schedule --name "${name}" --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" --region "${REGION}" --query 'ScheduleArn' --output text
    else
      echo "  Creating Scheduler rule: ${name} → ${cron}"
      run aws scheduler create-schedule --name "${name}" --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" --region "${REGION}" --query 'ScheduleArn' --output text
    fi
    if ! $DRY_RUN; then
      aws scheduler get-schedule --name "${name}" --region "${REGION}" --query 'Name' --output text >/dev/null \
        || { echo "ERROR: Scheduler rule ${name} not found after create/update" >&2; exit 1; }
    fi
  done

  # Prune reconciliation: delete any live rule under SCHED_PREFIX not in SCHED_NAMES.
  echo "  Pruning orphaned Scheduler rules under prefix ${SCHED_PREFIX}..."
  LIVE_RULES=$(aws scheduler list-schedules --name-prefix "${SCHED_PREFIX}" --region "${REGION}" --query 'Schedules[].Name' --output text 2>/dev/null || echo "")
  for live in ${LIVE_RULES}; do
    keep=false
    for want in "${SCHED_NAMES[@]}"; do [ "${live}" = "${want}" ] && { keep=true; break; }; done
    if ! $keep; then
      echo "    Deleting orphaned Scheduler rule: ${live}"
      run aws scheduler delete-schedule --name "${live}" --region "${REGION}"
    fi
  done

  # EventBridge rules for the mid-run spot-reclaim checker (config#2270).
  # NOTE: neither EC2 event type can be TAG-scoped in the rule pattern (the
  # events carry only instance-id) — the handler filters by the box's
  # Name=alpha-engine-sf-watch-spot tag and exits quietly for everything else.
  # put-rule/put-targets are idempotent upserts (mirrors the sibling
  # saturday-sf-watch-dispatcher bootstrap style); add-permission tolerates
  # the already-exists rerun.
  RECLAIM_RULE_NAMES=(
    "alpha-engine-sf-watch-spot-interruption"
    "alpha-engine-sf-watch-instance-terminated"
  )
  RECLAIM_RULE_PATTERNS=(
    '{"source":["aws.ec2"],"detail-type":["EC2 Spot Instance Interruption Warning"]}'
    '{"source":["aws.ec2"],"detail-type":["EC2 Instance State-change Notification"],"detail":{"state":["terminated"]}}'
  )
  RECLAIM_RULE_DESCRIPTIONS=(
    "EC2 spot interruption warning -> sf-watch mid-run reclaim checker (config#2270)"
    "EC2 instance terminated -> sf-watch mid-run reclaim checker (config#2270)"
  )
  for i in "${!RECLAIM_RULE_NAMES[@]}"; do
    rule="${RECLAIM_RULE_NAMES[$i]}"
    echo "  Creating/updating EventBridge rule: ${rule}"
    run aws events put-rule \
      --name "${rule}" \
      --event-pattern "${RECLAIM_RULE_PATTERNS[$i]}" \
      --description "${RECLAIM_RULE_DESCRIPTIONS[$i]}" \
      --region "${REGION}" \
      --query 'RuleArn' --output text
    run aws events put-targets \
      --rule "${rule}" \
      --targets "Id=1,Arn=${FN_ARN}" \
      --region "${REGION}"
    run aws lambda add-permission \
      --function-name "${FUNCTION_NAME}" \
      --statement-id "eventbridge-${rule}" \
      --action lambda:InvokeFunction \
      --principal events.amazonaws.com \
      --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${rule}" \
      --region "${REGION}" 2>/dev/null || true
  done
fi

# ----- 3. Update function code (always, idempotent) -------------------------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "✓ Code deployed."

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1,ACCOUNT_ID='"${ACCOUNT_ID}"'}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

# ----- 4. Smoke (synthetic invoke; read-only — only pings on a REAL problem) -

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (read-only wiring check)..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out \
    --payload '{}' --region "${REGION}" "${RESP}" >/dev/null
  cat "${RESP}"; echo ""
  rm -f "${RESP}"
fi
