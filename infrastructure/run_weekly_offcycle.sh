#!/usr/bin/env bash
# run_weekly_offcycle.sh — fire the weekly Saturday pipeline OFF-SCHEDULE.
#
# Low-friction operator wrapper for the two weekly triggers when you need to
# run them on a non-standard day (holiday weeks, schedule shifts, ad-hoc
# rehearsals). It reproduces — byte-for-byte — the two production input
# contracts so an off-cycle run is indistinguishable from the scheduled one:
#
#   * `shell` — the Friday rehearsal. Mirrors the input built by the
#     `alpha-engine-eod-success-friday-shell-trigger` Lambda
#     (`shell_run=true`, `pipeline_role="shell-run"`). PURELY ADDITIVE — the
#     shell run boots spots + validates bootstrap/import/clone/lib-pin/wiring
#     and short-circuits the workloads; it never conflicts with the Saturday
#     cron, so it is safe to fire any time. On a holiday Friday there is no
#     EOD success, so the Lambda never fires and this is the ONLY way to get
#     the rehearsal.
#
#   * `full` — the canonical weekly run. Mirrors the `alpha-engine-saturday`
#     EventBridge cron target input (`pipeline_role="weekly"`, no
#     `shell_run`). A `full` run is NOT additive: if a scheduled Saturday
#     cron fire follows it, the weekly pipeline would run TWICE (wasted spot
#     $, double model-zoo rotation, artifact clobber). So `full` SUPPRESSES
#     the next Saturday cron fire and registers a one-time EventBridge
#     Scheduler job that AUTO RE-ENABLES the rule right after that skipped
#     fire — zero manual follow-up, next week's cadence untouched.
#
# Fail-loud ordering for `full`: the auto re-enable is scheduled and verified
# BEFORE the cron rule is disabled, and the cron is disabled + verified BEFORE
# the execution starts. Any failure aborts in a SAFE state (rule left ENABLED;
# re-enabling an already-enabled rule is an idempotent no-op that self-deletes).
#
# Verbs:
#   shell             start a shell-run (rehearsal) now
#   full              start a full weekly run now + suppress next Saturday cron
#   restore           re-enable the Saturday cron now + drop any pending
#                     re-enable schedule (manual escape hatch)
#   status            show cron-rule state, pending re-enable schedule, recent runs
#
# Usage:
#   bash infrastructure/run_weekly_offcycle.sh shell
#   bash infrastructure/run_weekly_offcycle.sh full
#   bash infrastructure/run_weekly_offcycle.sh full --dry-run
#   bash infrastructure/run_weekly_offcycle.sh restore
#   bash infrastructure/run_weekly_offcycle.sh status
#
# Auth: uses active AWS CLI creds (personal IAM user has enough perms).
# Deliberately operator-run, not wired into CI — same convention as
# infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh.

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

SF_NAME="ne-weekly-freshness-pipeline"
SF_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SF_NAME}"
SATURDAY_RULE="alpha-engine-saturday"
SATURDAY_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${SATURDAY_RULE}"

# These two MUST match the live cron target + the friday-shell Lambda inputs.
# Pinned by tests/test_run_weekly_offcycle.py against the CFN / Lambda source.
EC2_INSTANCE_ID="i-09b539c844515d549"
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"

# Auto re-enable infra (one-time EventBridge Scheduler job + its execution role)
REENABLE_ROLE_NAME="alpha-engine-offcycle-cron-role"
REENABLE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${REENABLE_ROLE_NAME}"
REENABLE_SCHEDULE_PREFIX="reenable-saturday-"
REENABLE_UNIVERSAL_TARGET="arn:aws:scheduler:::aws-sdk:eventbridge:enableRule"

DRY_RUN=false
VERB=""
for arg in "$@"; do
  case "$arg" in
    shell|full|restore|status) VERB="$arg" ;;
    --dry-run) DRY_RUN=true ;;
    -h|--help) sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "ERROR: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if [[ -z "$VERB" ]]; then
  echo "ERROR: a verb is required (shell | full | restore | status)" >&2
  echo "       run with --help for usage" >&2
  exit 2
fi

run() {
  if $DRY_RUN; then
    echo "DRY: $*"
  else
    "$@"
  fi
}

utc_stamp() { date -u +%Y%m%d-%H%M%S; }
utc_date()  { date -u +%Y-%m-%d; }

# Next Saturday on/after today (UTC). Used to skip exactly the upcoming cron fire.
next_saturday_utc() {
  # date(1) on macOS (BSD) vs Linux (GNU) differ; probe and branch.
  if date -u -v +1d >/dev/null 2>&1; then
    # BSD date (macOS)
    local d=0 dow
    while [[ $d -lt 7 ]]; do
      dow=$(date -u -v +"${d}"d +%u)   # %u: 1=Mon..7=Sun; Saturday=6
      if [[ "$dow" == "6" ]]; then
        date -u -v +"${d}"d +%Y-%m-%d
        return 0
      fi
      d=$((d + 1))
    done
  else
    # GNU date (Linux / CI)
    local d=0 dow
    while [[ $d -lt 7 ]]; do
      dow=$(date -u -d "+${d} day" +%u)
      if [[ "$dow" == "6" ]]; then
        date -u -d "+${d} day" +%Y-%m-%d
        return 0
      fi
      d=$((d + 1))
    done
  fi
  echo "ERROR: could not compute next Saturday" >&2
  return 1
}

# ── input builders (the two production contracts) ───────────────────────────

shell_input() {
  cat <<EOF
{"ec2_instance_id": ["${EC2_INSTANCE_ID}"], "sns_topic_arn": "${SNS_TOPIC_ARN}", "shell_run": true, "pipeline_role": "shell-run"}
EOF
}

full_input() {
  cat <<EOF
{"ec2_instance_id": ["${EC2_INSTANCE_ID}"], "sns_topic_arn": "${SNS_TOPIC_ARN}", "pipeline_role": "weekly"}
EOF
}

start_execution() {
  local name="$1" input="$2"
  echo "Starting ${SF_NAME} execution: ${name}"
  echo "  input: ${input}"
  if $DRY_RUN; then
    echo "DRY: aws stepfunctions start-execution --name ${name} ..."
    return 0
  fi
  local arn
  arn=$(aws stepfunctions start-execution \
    --state-machine-arn "${SF_ARN}" \
    --name "${name}" \
    --input "${input}" \
    --region "${REGION}" \
    --query 'executionArn' --output text)
  echo "  ✓ started: ${arn}"
  echo "  console: https://${REGION}.console.aws.amazon.com/states/home?region=${REGION}#/v2/executions/details/${arn}"
}

# ── cron-rule helpers ───────────────────────────────────────────────────────

rule_state() {
  aws events describe-rule --name "${SATURDAY_RULE}" --region "${REGION}" \
    --query 'State' --output text
}

# Idempotently ensure the EventBridge Scheduler execution role exists with
# events:EnableRule on the Saturday rule only (single-target blast radius).
ensure_reenable_role() {
  if aws iam get-role --role-name "${REENABLE_ROLE_NAME}" \
      --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  re-enable role exists: ${REENABLE_ROLE_NAME}"
    return 0
  fi
  echo "  creating re-enable role: ${REENABLE_ROLE_NAME}"
  local trust policy
  trust='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  policy=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["events:EnableRule"],"Resource":"${SATURDAY_RULE_ARN}"}]}
EOF
)
  run aws iam create-role \
    --role-name "${REENABLE_ROLE_NAME}" \
    --assume-role-policy-document "${trust}" \
    --description "EventBridge Scheduler role: re-enable ${SATURDAY_RULE} after an off-cycle full run" \
    --query 'Role.RoleName' --output text
  run aws iam put-role-policy \
    --role-name "${REENABLE_ROLE_NAME}" \
    --policy-name "enable-saturday-rule" \
    --policy-document "${policy}"
  if ! $DRY_RUN; then
    echo "  waiting 10s for IAM role propagation..."
    sleep 10
  fi
}

# Create (or replace) the one-time re-enable schedule for the given UTC date.
# Self-deletes after firing (ActionAfterCompletion=DELETE).
schedule_reenable() {
  local sat_date="$1"
  local sched_name="${REENABLE_SCHEDULE_PREFIX}${sat_date}"
  # Re-enable at 09:30 UTC — 30 min AFTER the 09:00 cron fire window, so the
  # rule stays DISABLED through the skipped fire, then comes back for next week.
  local at_expr="at(${sat_date}T09:30:00)"
  local target
  target=$(cat <<EOF
{"Arn":"${REENABLE_UNIVERSAL_TARGET}","RoleArn":"${REENABLE_ROLE_ARN}","Input":"{\"Name\":\"${SATURDAY_RULE}\"}"}
EOF
)
  # Replace any stale schedule of the same name (create fails on conflict).
  if aws scheduler get-schedule --name "${sched_name}" --region "${REGION}" \
      --query 'Name' --output text >/dev/null 2>&1; then
    echo "  replacing existing re-enable schedule: ${sched_name}"
    run aws scheduler delete-schedule --name "${sched_name}" --region "${REGION}"
  fi
  echo "  scheduling auto re-enable: ${sched_name} → ${at_expr} UTC"
  run aws scheduler create-schedule \
    --name "${sched_name}" \
    --schedule-expression "${at_expr}" \
    --schedule-expression-timezone "UTC" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --action-after-completion DELETE \
    --target "${target}" \
    --region "${REGION}" \
    --query 'ScheduleArn' --output text
  # Verify it landed before we touch the cron rule (fail-loud).
  if ! $DRY_RUN; then
    aws scheduler get-schedule --name "${sched_name}" --region "${REGION}" \
      --query 'Name' --output text >/dev/null \
      || { echo "ERROR: re-enable schedule not found after create — aborting BEFORE disabling cron" >&2; exit 1; }
  fi
}

# ── verbs ───────────────────────────────────────────────────────────────────

do_shell() {
  echo "== OFF-CYCLE SHELL RUN (rehearsal) =="
  start_execution "offcycle-shell-$(utc_stamp)" "$(shell_input)"
  echo "Shell run is additive — Saturday cron untouched."
}

do_full() {
  echo "== OFF-CYCLE FULL WEEKLY RUN =="
  local sat_date
  sat_date=$(next_saturday_utc)
  echo "Upcoming Saturday cron fire to SUPPRESS: ${sat_date} 09:00 UTC"
  echo "Auto re-enable scheduled for:           ${sat_date} 09:30 UTC"
  echo ""
  echo "[1/3] ensure re-enable infra + schedule (BEFORE disabling cron)"
  ensure_reenable_role
  schedule_reenable "${sat_date}"
  echo ""
  echo "[2/3] disable Saturday cron"
  run aws events disable-rule --name "${SATURDAY_RULE}" --region "${REGION}"
  if ! $DRY_RUN; then
    local st
    st=$(rule_state)
    [[ "$st" == "DISABLED" ]] \
      || { echo "ERROR: ${SATURDAY_RULE} state is '${st}', expected DISABLED — aborting BEFORE starting run" >&2; exit 1; }
    echo "  ✓ ${SATURDAY_RULE} is DISABLED"
  fi
  echo ""
  echo "[3/3] start full weekly execution"
  start_execution "offcycle-full-$(utc_stamp)" "$(full_input)"
  echo ""
  echo "Done. Saturday cron will auto re-enable at ${sat_date} 09:30 UTC."
  echo "To restore manually at any time: bash $0 restore"
}

do_restore() {
  echo "== RESTORE SATURDAY CRON =="
  run aws events enable-rule --name "${SATURDAY_RULE}" --region "${REGION}"
  if ! $DRY_RUN; then
    local st
    st=$(rule_state)
    [[ "$st" == "ENABLED" ]] \
      || { echo "ERROR: ${SATURDAY_RULE} state is '${st}', expected ENABLED" >&2; exit 1; }
    echo "  ✓ ${SATURDAY_RULE} is ENABLED"
  fi
  # Drop any pending re-enable schedules (now redundant).
  local names
  names=$(aws scheduler list-schedules --name-prefix "${REENABLE_SCHEDULE_PREFIX}" \
    --region "${REGION}" --query 'Schedules[].Name' --output text 2>/dev/null || true)
  if [[ -n "${names}" ]]; then
    for n in ${names}; do
      echo "  deleting pending re-enable schedule: ${n}"
      run aws scheduler delete-schedule --name "${n}" --region "${REGION}"
    done
  else
    echo "  no pending re-enable schedules"
  fi
}

do_status() {
  echo "== OFF-CYCLE STATUS =="
  echo "Saturday cron (${SATURDAY_RULE}): $(rule_state)"
  echo ""
  echo "Pending re-enable schedules:"
  aws scheduler list-schedules --name-prefix "${REENABLE_SCHEDULE_PREFIX}" \
    --region "${REGION}" --query 'Schedules[].Name' --output text 2>/dev/null \
    | tr '\t' '\n' | sed 's/^/  /' | grep . || echo "  (none)"
  echo ""
  echo "Recent ${SF_NAME} executions:"
  aws stepfunctions list-executions --state-machine-arn "${SF_ARN}" \
    --max-results 5 --region "${REGION}" \
    --query 'executions[].{name:name,status:status,start:startDate}' --output table
}

case "$VERB" in
  shell)   do_shell ;;
  full)    do_full ;;
  restore) do_restore ;;
  status)  do_status ;;
esac
