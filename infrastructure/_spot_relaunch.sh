#!/usr/bin/env bash
# infrastructure/_spot_relaunch.sh — the ONE definition of what a spot
# launcher does DIFFERENTLY on the attempt after a spot interruption
# (alpha-engine-config-I11565).
#
# Sourced by BOTH `_spot_common.sh` (which the per-stage launchers source) and
# `spot_data_weekly.sh` (the monolith, which deliberately does NOT source
# `_spot_common.sh`). Same shape and same reason as `_stage_window.sh`: two
# adoptions is the `policy-shared-code` lift trigger, and a copy pasted into
# the monolith is the fork it forbids.
#
# WHY THIS EXISTS. Measured on `rehearsal-2026-09-24-1` (DataPhase1), four
# consecutive spot boxes, all c6i.large, all ended `Server.SpotInstanceTermination`:
#
#   i-0d696d54ea5edc7cd  22:06-22:10  us-east-1a   script attempt 1/2
#   i-012e3d53bdade7804  22:11-22:35  us-east-1b   script attempt 2/2
#   i-0c6145a4239a1a506  22:36-22:39  us-east-1b   SF reissue, attempt 1/2
#   i-0a82259f5c30717af  22:40-22:50  us-east-1a   SF reissue, attempt 2/2
#
# `krepis.ec2_spot launch` walks types x subnets IN ORDER and c6i.large leads
# the rotation (alpha-engine-config-I11412), so every relaunch - the script's
# own `exec` AND the SF's DataPhase1Reissue, which re-runs the script from
# scratch on the same launcher box - went straight back into the pool that was
# being reclaimed, and nothing ever escalated to on-demand. The run failed at
# DataPhase1.
#
# TWO RULES, both applied at launch time by `spot_launch_plan`:
#
#  1. DEMOTE. Every CONFIRMED reclaim (krepis `relaunch-decision` classified it
#     `reclaim`, whatever its relaunch verdict) records the reclaimed box's
#     instance type and subnet (= AZ). Every later launch in the same run moves
#     those to the BACK of INSTANCE_TYPES / SUBNETS - never removes them, so a
#     single-type `--instance-type` override still has something to launch.
#     The record is a file on the LAUNCHER host keyed by RUN_TOKEN (the SF
#     execution name, which `krepis.ssm_log_capture` exports into the launcher's
#     environment), so it survives the SF reissue: that reissue is a fresh SSM
#     command on the same `$.ec2_instance_id`, and the rehearsal's two launcher
#     logs are both from ip-172-31-42-164. Without a RUN_TOKEN (an ad-hoc run)
#     the key is a chain id carried through the relaunch `exec` only, so one
#     manual run never inherits another's demotions.
#
#  2. ESCALATE. The FINAL attempt of a relaunch chain (SPOT_ATTEMPT > 1 and
#     SPOT_ATTEMPT >= MAX_SPOT_ATTEMPTS) that follows a spot interruption
#     launches ON-DEMAND. This is `nousergon_lib.spot_dispatch.
#     launch_with_fallback`'s on-demand rung, reached through the same
#     primitive it calls - `krepis.ec2_spot.launch(..., spot=False)`, which is
#     `launch --no-spot` on the CLI - with the same `LaunchMarket` /
#     `LaunchReason` provenance tags on the same RunInstances call, so the
#     escalation is countable by spot-interruption-recorder's fallback sweep
#     rather than only visible on the bill. `LaunchReason` is
#     `force_on_demand` after a reclaim (the bounded-relaunch escalation that
#     reason was defined for, config#1645), `capacity_exhausted` after an
#     all-pools launch refusal (ec2_spot exit 64), and `quota_exceeded` after
#     a spot-quota refusal (exit 65) — which escalates on the NEXT attempt,
#     not only the final one, because the quota is account-wide
#     (alpha-engine-config-I11574). An on-demand box cannot be reclaimed, so the
#     last attempt stops depending on the market that failed the first ones.
#     `SPOT_FINAL_ATTEMPT_ON_DEMAND=0` turns rule 2 off for one invocation.
#
# What this deliberately does NOT change: whether to relaunch at all, and how
# many times. That verdict stays with `krepis.ec2_spot relaunch-decision`
# (classify + MAX_SPOT_ATTEMPTS budget + SF-timeout coupling), and a
# non-reclaim failure is still never retried. This file only decides what the
# next launch looks like.
#
# The sourcing script must define INSTANCE_TYPES, SUBNETS, SPOT_ATTEMPT,
# MAX_SPOT_ATTEMPTS and AWS_REGION before any function here is CALLED (not
# before it is sourced). The only thing evaluated at source time is the
# relaunch-chain id below.

# Carried through the relaunch `exec` (exported), minted once per chain.
SPOT_RECLAIM_CHAIN_ID="${SPOT_RECLAIM_CHAIN_ID:-adhoc-$$-$(date -u +%Y%m%dT%H%M%SZ)}"
export SPOT_RECLAIM_CHAIN_ID
# Set by on_exit on the relaunch `exec` to the class of interruption that
# ended the previous attempt: `reclaim` | `launch-capacity-exhausted`. Empty
# on a first attempt, which is how rule 2 tells a relaunch from a fresh run.
SPOT_RELAUNCH_CAUSE="${SPOT_RELAUNCH_CAUSE:-}"
SPOT_FINAL_ATTEMPT_ON_DEMAND="${SPOT_FINAL_ATTEMPT_ON_DEMAND:-1}"
# 12h = the weekly SF's TimeoutSeconds ceiling: nothing in one execution can
# be older, so an older record belongs to some other run that reused the key.
SPOT_RECLAIM_MEMORY_SECONDS="${SPOT_RECLAIM_MEMORY_SECONDS:-43200}"

# Outputs of spot_launch_plan (consumed by the launcher's krepis call).
_SPOT_PLAN_TYPES=""
_SPOT_PLAN_SUBNETS=""
_SPOT_PLAN_MARKET=""
_SPOT_PLAN_REASON=""
_SPOT_PLAN_MARKET_ARGS=()
# Set by spot_capture_launched_pool right after a launch.
_SPOT_LAUNCHED_TYPE=""
_SPOT_LAUNCHED_SUBNET=""

# spot_demote_csv LIST DEMOTED -> LIST with every member of DEMOTED that it
# contains moved to the back. The moved members keep DEMOTED's order with the
# LAST occurrence winning, so the most recently reclaimed pool is tried last.
# Nothing is ever dropped and nothing absent from LIST is ever added.
spot_demote_csv() {
  local _list="${1:-}" _demoted="${2:-}" _head="" _tail="" _x
  local IFS=','
  for _x in $_demoted; do
    [ -n "$_x" ] || continue
    case ",${_list}," in *",${_x},"*) ;; *) continue ;; esac
    _tail=",${_tail},"
    _tail="${_tail//,${_x},/,}"
    _tail="${_tail#,}"
    _tail="${_tail%,}"
    _tail="${_tail:+${_tail},}${_x}"
  done
  for _x in $_list; do
    [ -n "$_x" ] || continue
    case ",${_tail}," in *",${_x},"*) continue ;; esac
    _head="${_head:+${_head},}${_x}"
  done
  echo "${_head}${_head:+${_tail:+,}}${_tail}"
}

# The per-run record file. RUN_TOKEN when the SF supplied one (so the SF
# reissue sees it), else this relaunch chain's own id.
spot_reclaim_state_file() {
  local _key="${RUN_TOKEN:-$SPOT_RECLAIM_CHAIN_ID}"
  _key="$(printf '%s' "$_key" | tr -c 'A-Za-z0-9._-' '_')"
  echo "${SPOT_RECLAIM_STATE_DIR:-${TMPDIR:-/tmp}/ae-spot-reclaims}/${_key}.tsv"
}

# spot_reclaimed_csv COLUMN -> comma list of that column (2 = instance type,
# 3 = subnet) over the fresh records, in reclaim order.
spot_reclaimed_csv() {
  local _col="$1" _f _min
  _f="$(spot_reclaim_state_file)"
  [ -r "$_f" ] || { echo ""; return 0; }
  _min=$(( $(date +%s) - SPOT_RECLAIM_MEMORY_SECONDS ))
  awk -F'\t' -v min="$_min" -v col="$_col" \
    '$1 >= min && $col != "" { out = out (out == "" ? "" : ",") $col } END { print out }' \
    "$_f" 2>/dev/null || echo ""
}

# Called right after a successful launch, while the box is pending/running:
# a terminated instance no longer reports its SubnetId, so the pool has to be
# read now rather than at reclaim time. Best-effort by design - a miss here
# only costs the subnet half of a later demotion (the type is re-read at
# record time, and survives termination).
spot_capture_launched_pool() {
  local _iid="$1" _out="" _i
  _SPOT_LAUNCHED_TYPE=""
  _SPOT_LAUNCHED_SUBNET=""
  [ -n "$_iid" ] || return 0
  for _i in 1 2 3; do
    _out="$(aws ec2 describe-instances --instance-ids "$_iid" --region "$AWS_REGION" \
      --query 'Reservations[0].Instances[0].[InstanceType,SubnetId]' \
      --output text 2>/dev/null)" || _out=""
    if [ -n "$_out" ] && [ "$_out" != "None" ]; then
      break
    fi
    # RunInstances -> DescribeInstances is eventually consistent.
    sleep "${SPOT_DESCRIBE_RETRY_SECONDS:-2}"
  done
  read -r _SPOT_LAUNCHED_TYPE _SPOT_LAUNCHED_SUBNET <<<"${_out:-}" || true
  [ "$_SPOT_LAUNCHED_TYPE" = "None" ] && _SPOT_LAUNCHED_TYPE=""
  [ "$_SPOT_LAUNCHED_SUBNET" = "None" ] && _SPOT_LAUNCHED_SUBNET=""
  return 0
}

# spot_record_reclaim INSTANCE_ID STAGE — append one confirmed reclaim to the
# run's record. Writes only to stderr and the record file, and always
# returns 0: it runs inside the EXIT trap's classification and must never
# change the launcher's exit status or the classifier's stdout contract.
spot_record_reclaim() {
  local _iid="${1:-}" _stage="${2:-}" _type="$_SPOT_LAUNCHED_TYPE" _subnet="$_SPOT_LAUNCHED_SUBNET" _f
  if [ -z "$_type" ] && [ -n "$_iid" ]; then
    _type="$(aws ec2 describe-instances --instance-ids "$_iid" --region "$AWS_REGION" \
      --query 'Reservations[0].Instances[0].InstanceType' --output text 2>/dev/null)" || _type=""
    [ "$_type" = "None" ] && _type=""
  fi
  if [ -z "$_type" ] && [ -z "$_subnet" ]; then
    echo "  spot reclaim: WARNING could not read the reclaimed pool of ${_iid:-<none>} — nothing to demote" >&2
    return 0
  fi
  _f="$(spot_reclaim_state_file)"
  if mkdir -p "$(dirname "$_f")" 2>/dev/null \
      && printf '%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "$_type" "$_subnet" "$_stage" "$_iid" >>"$_f" 2>/dev/null; then
    echo "  spot reclaim recorded: ${_type:-?}@${_subnet:-?} (${_iid}) — demoted to the back of the rotation for the rest of this run (${_f})" >&2
  else
    echo "  spot reclaim: WARNING could not write ${_f} — ${_type:-?}@${_subnet:-?} will NOT be demoted on the next launch" >&2
  fi
  return 0
}

# spot_launch_plan — set _SPOT_PLAN_{TYPES,SUBNETS,MARKET,REASON} and
# _SPOT_PLAN_MARKET_ARGS (the krepis `launch` arguments that pick the market
# and tag its provenance) from INSTANCE_TYPES / SUBNETS, the run's reclaim
# record, and where this attempt sits in the relaunch chain.
spot_launch_plan() {
  local _rtypes _rsubnets
  _rtypes="$(spot_reclaimed_csv 2)"
  _rsubnets="$(spot_reclaimed_csv 3)"
  _SPOT_PLAN_TYPES="$(spot_demote_csv "$INSTANCE_TYPES" "$_rtypes")"
  _SPOT_PLAN_SUBNETS="$(spot_demote_csv "$SUBNETS" "$_rsubnets")"
  _SPOT_PLAN_MARKET="spot"
  _SPOT_PLAN_REASON="spot_ok"
  if [ "$SPOT_FINAL_ATTEMPT_ON_DEMAND" = "1" ] \
      && [ -n "$SPOT_RELAUNCH_CAUSE" ] \
      && [ "$SPOT_ATTEMPT" -gt 1 ] \
      && [ "$SPOT_ATTEMPT" -ge "$MAX_SPOT_ATTEMPTS" ]; then
    _SPOT_PLAN_MARKET="on-demand"
    case "$SPOT_RELAUNCH_CAUSE" in
      launch-capacity-exhausted) _SPOT_PLAN_REASON="capacity_exhausted" ;;
      launch-quota-exceeded) _SPOT_PLAN_REASON="quota_exceeded" ;;
      *) _SPOT_PLAN_REASON="force_on_demand" ;;
    esac
  fi
  # A spot QUOTA refusal is account-wide: no spot attempt in this chain can
  # succeed, so the very next one is on-demand whatever budget is left
  # (alpha-engine-config-I11574; lib spot_dispatch REASON_QUOTA).
  if [ "$SPOT_FINAL_ATTEMPT_ON_DEMAND" = "1" ] \
      && [ "$SPOT_RELAUNCH_CAUSE" = "launch-quota-exceeded" ] \
      && [ "$SPOT_ATTEMPT" -gt 1 ]; then
    _SPOT_PLAN_MARKET="on-demand"
    _SPOT_PLAN_REASON="quota_exceeded"
  fi
  _SPOT_PLAN_MARKET_ARGS=(--extra-tag "LaunchMarket=${_SPOT_PLAN_MARKET}" --extra-tag "LaunchReason=${_SPOT_PLAN_REASON}")
  if [ "$_SPOT_PLAN_MARKET" = "on-demand" ]; then
    _SPOT_PLAN_MARKET_ARGS=(--no-spot "${_SPOT_PLAN_MARKET_ARGS[@]}")
  fi
  if [ -n "$_rtypes$_rsubnets" ]; then
    echo "  spot reclaims this run: types=[${_rtypes}] subnets=[${_rsubnets}] — demoted to the back of the rotation" >&2
  fi
  if [ "$_SPOT_PLAN_MARKET" = "on-demand" ]; then
    echo "  FINAL attempt $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS after ${SPOT_RELAUNCH_CAUSE} — launching ON-DEMAND (LaunchReason=${_SPOT_PLAN_REASON})" >&2
  fi
  return 0
}

# spot_relaunch_cause REASON -> the SPOT_RELAUNCH_CAUSE value for a relaunch
# whose classifier returned REASON (see `_spot_failure_reason`).
spot_relaunch_cause() {
  case "${1:-}" in
    launch-capacity-exhausted*) echo "launch-capacity-exhausted" ;;
    launch-quota-exceeded*) echo "launch-quota-exceeded" ;;
    *) echo "reclaim" ;;
  esac
}
