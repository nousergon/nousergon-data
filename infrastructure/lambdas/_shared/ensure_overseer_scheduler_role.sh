#!/usr/bin/env bash
# ensure_overseer_scheduler_role.sh — idempotently ensure the ONE shared
# EventBridge Scheduler role that invokes the Overseer router
# (alpha-engine-config-I2832). Every router-targeting schedule uses this
# single role instead of a per-schedule bespoke one:
#   - the twice-daily alert-drain schedules (created by
#     alert-drain-dispatcher/deploy.sh --bootstrap)
#   - the Wednesday sf-watch + ci-watch canary drills (re-pointed at the
#     router by their dispatchers' deploy.sh --bootstrap)
# Jointly owned: whichever --bootstrap runs first creates it; the rest find
# it present. put-role-policy is unconditional so the invoke grant self-heals.
#
# Usage:  ROLE_ARN=$(ensure_overseer_scheduler_role "<region>" "<account_id>" run)
#   Pass the caller's dry-run-aware `run` function name as $3 so create/put
#   honor --dry-run (the get-role probe is always a real read). Echoes the
#   role ARN on stdout regardless.
ensure_overseer_scheduler_role() {
  local region="$1" account_id="$2" runner="${3:-}"
  local role_name="alpha-engine-overseer-scheduler-role"
  local policy_name="invoke-overseer-dispatcher"
  local router_arn="arn:aws:lambda:${region}:${account_id}:function:alpha-engine-overseer-dispatcher"
  local role_arn="arn:aws:iam::${account_id}:role/${role_name}"
  local trust='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  local invoke_policy="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"lambda:InvokeFunction\",\"Resource\":\"${router_arn}\"}]}"
  _eosr_run() { if [ -n "$runner" ]; then "$runner" "$@"; else "$@"; fi; }
  if ! aws iam get-role --role-name "$role_name" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating shared Overseer scheduler role: ${role_name}" >&2
    _eosr_run aws iam create-role --role-name "$role_name" \
      --assume-role-policy-document "$trust" \
      --description "Shared EventBridge Scheduler role: invoke the Overseer router (alpha-engine-config-I2832)" \
      --query 'Role.RoleName' --output text >/dev/null
  else
    echo "  Shared Overseer scheduler role exists: ${role_name}" >&2
  fi
  _eosr_run aws iam put-role-policy --role-name "$role_name" \
    --policy-name "$policy_name" --policy-document "$invoke_policy" >&2
  echo "$role_arn"
}
