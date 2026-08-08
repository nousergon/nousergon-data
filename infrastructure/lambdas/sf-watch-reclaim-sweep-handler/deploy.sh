
# alpha-engine-config-I6619: --state must come from the automation-pause
# manifest, not from the API default (ENABLED). See infrastructure/lambdas/_shared/pause.sh.
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"
#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-sf-watch-reclaim-sweep-handler Lambda
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
# (narrow OIDC blast radius). CODE, and (since alpha-engine-config-I5815)
# SCHEDULES, auto-deploy on merge to main via
# `.github/workflows/deploy-sf-watch-reclaim-sweep-handler.yml`, which runs
# this script twice: flagless (Lambda code) and then `--reconcile-schedules`
# (EventBridge Scheduler upsert + prune), both under the
# github-actions-lambda-deploy OIDC role.
#
# alpha-engine-config-I5815 is why the schedule half is here at all. It used to
# sit inside --bootstrap next to the IAM-role creation, so the OIDC role —
# which deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet single-writer
# rule after 4 IAM-clobber incidents in 2 months) — could not run it, and both
# daily schedules were never created. Splitting on the IAM boundary rather than
# on "first time vs not" is what makes the schedule half CI-runnable.
#
# What still needs an operator: --bootstrap-iam, i.e. creating the IAM roles
# and their inline policies, plus first-ever creation of the Lambda and the
# EventBridge reclaim rules. That is genuinely first-time-only.
#
# Usage:
#   bash .../sf-watch-reclaim-sweep-handler/deploy.sh             # code only (the CI path, step 1)
#   bash .../sf-watch-reclaim-sweep-handler/deploy.sh --reconcile-schedules  # + upsert/prune Scheduler rules (the CI path, step 2)
#   bash .../sf-watch-reclaim-sweep-handler/deploy.sh --bootstrap-iam        # operator-only: create IAM roles, Lambda, reclaim rules
#   bash .../sf-watch-reclaim-sweep-handler/deploy.sh --bootstrap            # both of the above (unchanged meaning)
#   bash .../sf-watch-reclaim-sweep-handler/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../sf-watch-reclaim-sweep-handler/deploy.sh --dry-run   # show actions, do not apply
#   bash .../sf-watch-reclaim-sweep-handler/deploy.sh --smoke     # invoke once (read-only check; pings only on a real problem)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-sf-watch-reclaim-sweep-handler"
ROLE_NAME="alpha-engine-sf-watch-reclaim-sweep-handler-role"
POLICY_NAME="alpha-engine-sf-watch-reclaim-sweep-handler-policy"
SCHED_ROLE_NAME="alpha-engine-sf-watch-reclaim-sweep-handler-scheduler-role"
SCHED_POLICY_NAME="invoke-sf-watch-reclaim-sweep-handler"

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

SCHED_NAMES=(
  "alpha-engine-sf-watch-reclaim-sweep-0645-daily"
  "alpha-engine-sf-watch-reclaim-sweep-1445-daily"
)
SCHED_CRONS=(
  "cron(45 6 * * ? *)"
  "cron(45 14 * * ? *)"
)
SCHED_PREFIX="alpha-engine-sf-watch-reclaim-sweep-"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops
# instead of silently running the real deploy path (alpha-engine-config-
# I2752 incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked
# here, matching other tools' convention, and triggered a real deploy).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
# alpha-engine-config-I5815 splits the old monolithic --bootstrap in two, along
# the line that actually matters: which grants the caller needs.
#
#   BOOTSTRAP_IAM       creates IAM roles and puts inline policies. Needs
#                       iam:CreateRole + iam:PutRolePolicy, which the GHA OIDC
#                       role deliberately does NOT have (fleet single-writer
#                       rule after 4 IAM-clobber incidents). Operator-only, and
#                       first-time-only in practice.
#   RECONCILE_SCHEDULES upserts + prunes the EventBridge Scheduler rules. Needs
#                       ONLY scheduler:{Get,List,Create,Update,Delete}Schedule
#                       and a scoped iam:PassRole — all of which
#                       github-actions-lambda-deploy already holds. Safe to run
#                       on every merge, and now does.
#
# --bootstrap keeps its old meaning (both) so every runbook, every comment and
# every operator habit that predates the split still does what it says.
BOOTSTRAP_IAM=false
RECONCILE_SCHEDULES=false
APPLY_IAM=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP_IAM=true; RECONCILE_SCHEDULES=true ;;
    --bootstrap-iam) BOOTSTRAP_IAM=true ;;
    --reconcile-schedules) RECONCILE_SCHEDULES=true ;;
    --apply-iam) APPLY_IAM=true ;;
    --smoke) SMOKE=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# `--reconcile-schedules` ALONE is a schedules-only run: it skips packaging
# (docker pip + handler tests) and the code push. The CI deploy therefore
# invokes this script twice without paying for the ~2min package build twice,
# and an operator repairing live schedule drift does not have to redeploy code
# to do it. Combined with --bootstrap or run flagless, everything happens as
# before.
CODE_DEPLOY=true
if $RECONCILE_SCHEDULES && ! $BOOTSTRAP_IAM && ! $APPLY_IAM && ! $SMOKE; then
  CODE_DEPLOY=false
fi

run() {
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

# ----- 0. Validate handler + run unit tests ----------------------------------

if $CODE_DEPLOY; then
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
fi

# ----- 2. Bootstrap IAM (operator-only — first-time only) -------------------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied."
fi

if $BOOTSTRAP_IAM; then
  echo "Bootstrapping IAM for ${FUNCTION_NAME}..."

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
      --name "${rule}" --state "$(pause_state "${rule}")" \
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

# ----- 2bis. Reconcile EventBridge Scheduler rules (CI-safe, every merge) ---

if $RECONCILE_SCHEDULES; then
  echo "Reconciling EventBridge Scheduler rules for ${FUNCTION_NAME}..."

  for i in "${!SCHED_NAMES[@]}"; do
    name="${SCHED_NAMES[$i]}"
    cron="${SCHED_CRONS[$i]}"
    target="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${SCHED_ROLE_ARN}\",\"Input\":\"{}\"}"
    if aws scheduler get-schedule --name "${name}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
      echo "  Updating Scheduler rule: ${name} → ${cron}"
      run aws scheduler update-schedule --name "${name}" --state "$(pause_state "${name}")" --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" --region "${REGION}" --query 'ScheduleArn' --output text
    else
      echo "  Creating Scheduler rule: ${name} → ${cron}"
      run aws scheduler create-schedule --name "${name}" --state "$(pause_state "${name}")" --schedule-expression "${cron}" \
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
fi

# ----- 3. Update function code (always, idempotent) -------------------------

if $CODE_DEPLOY; then
BOOTSTRAPPED=true
echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text \
  || { echo "WARNING: Function ${FUNCTION_NAME} not found — not bootstrapped yet (run --bootstrap-iam). Skipping code update."; BOOTSTRAPPED=false; }

if $BOOTSTRAPPED; then
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
else
  # Function hasn't been bootstrapped yet (config-I3111 first-merge pattern:
  # the OIDC role can't CREATE the Lambda — only an operator running
  # --bootstrap-iam from their own AWS creds can). Gracefully skip everything
  # after the update step so the workflow job reports success and the report
  # step shows "(function not bootstrapped yet)". CI-watch then sees a green
  # main instead of filing extra deploy-failure issues every push.
  echo "✓ (not bootstrapped — skipping environment update and smoke test)"
fi
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
