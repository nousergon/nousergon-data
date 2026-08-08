# shellcheck shell=bash
#
# pause.sh — resolve a trigger's intended state from the automation-pause manifest.
#
# Source this from any deploy.sh that creates or updates an EventBridge rule or
# an EventBridge Scheduler schedule, and pass the result as --state:
#
#   source "${SCRIPT_DIR}/../_shared/pause.sh"
#   aws events put-rule --name "${RULE}" --state "$(pause_state "${RULE}")" ...
#
# WHY (alpha-engine-config-I6619). Brian's 2026-08-07 ruling paused 40 scheduled
# triggers; infrastructure/automation_pause.json records which. Neither
# `aws events put-rule` nor `aws scheduler create-schedule|update-schedule`
# accepts a "leave the state alone" option — both DEFAULT TO ENABLED when
# --state is omitted, and update-schedule is a full replace. So every deploy.sh
# that reconciles its own triggers silently un-paused them on the next redeploy.
# The pause survived only by detection: automation_pause.py --check goes red the
# next morning. This closes it at the write site instead.
#
# Fail-OPEN on purpose. If the manifest is missing or unreadable this returns
# ENABLED — the pre-pause behaviour — rather than disabling a trigger nobody
# asked to disable. The asymmetry is deliberate: a pause that silently SPREADS
# would stop the weekly SF with no signal that a config file was the cause,
# while a pause that silently LIFTS is caught by automation_pause.py --check the
# next morning. One failure mode has a detector; the other does not.

# Absolute path to the manifest, resolved from THIS file's location so a
# deploy.sh may be invoked from any cwd (they are, by CI and by operators).
_PAUSE_MANIFEST="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/automation_pause.json"

# pause_state <trigger-name> -> "DISABLED" if the manifest pauses it, else "ENABLED".
#
# Matches on the trigger NAME across both surfaces. EventBridge rules and
# Scheduler schedules are different APIs but share one namespace here, and
# tests/test_automation_pause.py asserts no name appears on both, so a single
# lookup is unambiguous.
pause_state() {
  local name="${1:?pause_state: trigger name required}"

  if [ ! -r "${_PAUSE_MANIFEST}" ]; then
    echo "ENABLED"
    return 0
  fi

  # python3 rather than jq: every deploy.sh in this repo already depends on
  # python3, none depends on jq, and adding a dependency to 26 scripts to read
  # one JSON file is not worth it.
  python3 - "${_PAUSE_MANIFEST}" "${name}" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        manifest = json.load(fh)
    paused = manifest["paused"]
    names = set(paused["events_rules"]) | set(paused["scheduler_schedules"])
    # `pending` = paused, but not created live yet. Same write-time behaviour;
    # it exists as a separate block only because automation_pause.py --check
    # requires every `paused` entry to exist live. Keys prefixed `_` are notes.
    names |= {k for k in manifest.get("pending", {}) if not k.startswith("_")}
except Exception:
    # Unreadable or malformed manifest -> pre-pause behaviour. See the
    # fail-open note at the top of this file.
    print("ENABLED")
else:
    print("DISABLED" if sys.argv[2] in names else "ENABLED")
PY
}
