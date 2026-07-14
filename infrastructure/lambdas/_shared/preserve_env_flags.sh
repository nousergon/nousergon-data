#!/usr/bin/env bash
# preserve_env_flags.sh — shared "preserve operator-owned env flags" helper,
# sourced by dispatcher deploy.sh scripts.
#
# WHY (3rd instance of the operator-flag-clobber class — config#1818 saturday,
# config#2236 spot, config#2264 ci-watch): dispatcher Lambdas carry OPERATOR-
# OWNED runtime kill-switches (AGENT_DISPATCH_ENABLED, FAST_PATH_ENABLED,
# SF_WATCH_DISPATCH_ENABLED, CI_WATCH_DISPATCH_ENABLED). A deploy script's
# code-update path must READ the live value and carry it forward — never reset
# it to the bootstrap default. 2026-07-05 incident: the saturday dispatcher's
# update path hardcoded false; a routine redeploy silently disarmed autonomous
# dispatch, and both 2026-07-06 preopen SF failures went un-dispatched with
# the market open. Bootstrap (create-function) still applies each dispatcher's
# hardcoded default — safe posture for a brand-NEW deployment only.
#
# Usage (from a deploy.sh that defines SCRIPT_DIR):
#   source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"
#   CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" CI_WATCH_DISPATCH_ENABLED true)
#
# The preserved value (true|false) is printed on stdout for command
# substitution; the human-readable "preserving ..." receipt goes to stderr so
# it still lands in deploy logs without polluting the captured value.

# preserve_env_flag FUNCTION_NAME REGION VAR_NAME DEFAULT — echoes true|false.
preserve_env_flag() {
  local fn="$1" region="$2" var="$3" default="$4"
  local val
  # 2>/dev/null suppresses only the CLI's stderr chatter; a nonzero aws exit
  # (e.g. function not found) still aborts the caller under set -e — same
  # fail-loud posture as the pre-extraction inline blocks. A missing VAR on an
  # existing function returns "None" with exit 0 and falls through to the
  # per-dispatcher default below; the stderr receipt records the value applied.
  val=$(aws lambda get-function-configuration \
    --function-name "${fn}" \
    --region "${region}" \
    --query "Environment.Variables.${var}" --output text 2>/dev/null)
  case "${val}" in true|false) ;; *) val="${default}" ;; esac
  echo "  preserving ${var}=${val} (operator-owned)" >&2
  echo "${val}"
}
