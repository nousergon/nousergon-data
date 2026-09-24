#!/usr/bin/env bash
# infrastructure/_stage_window.sh — the ONE definition of the stage-coverage
# assertion window's cycle-tracking rule (alpha-engine-config-I10194 §3).
#
# Sourced by BOTH `_spot_common.sh` (which the per-stage launchers source) and
# `spot_data_weekly.sh` (the monolith, which deliberately does NOT source
# `_spot_common.sh` — it carries its own run_ssm/launch pair). Those are the
# two adoptions `policy-shared-code` names as the lift trigger; a second copy
# pasted into the monolith is the fork it forbids, and this repo has already
# paid for that class once (`alpha-engine-config-I6922`).
#
# The sourcing script must define `_STAGE_WINDOW_START`, `S3_BUCKET`,
# `AWS_REGION` and `LIB_PYTHON` before the function is CALLED (not before it is
# sourced — nothing here is evaluated at source time).
# Per-stage DECLARATION (alpha-engine-config-I10194 §3): does this stage's
# workload AUTO-SKIP work an EARLIER ATTEMPT OF THE SAME CYCLE already did?
#
# Declared EMPTY here, and set with a BARE assignment (`_STAGE_WINDOW_TRACKS_
# CYCLE=1`) by the launchers it holds for — never `${VAR:-1}`. The `:-` form
# is the swallow this file already carries a 30-line comment about above: a
# non-empty value assigned here would make every launcher's own assignment a
# silent no-op (alpha-engine-config-I6922).
#
# WHY THIS EXISTS. `_STAGE_WINDOW_START` above is "this execution's start",
# and for a stage with no auto-skip that is exactly right — an artifact older
# than it IS a leftover from a previous cycle, which is the whole detector.
# But `weekly_collector.py`'s PhaseRegistry auto-skips any phase whose output
# is already on S3 for the cycle's date (`PHASE_SKIP name=<x>
# reason=auto_skip_marker_ok`), which is CORRECT idempotency: on a rerun of a
# failed attempt, the phases that already succeeded do not re-fetch. So on a
# RERUN the two facts collide — the artifacts are this cycle's own valid
# output, and they predate this execution's window.
#
# Measured 2026-09-08 (`alpha-engine-config-I10194` §3, `-I10173`): the
# 2026-09-04 cycle's `DataPhase1` verdict carried
# `window_start: 2026-09-05T15:06:42Z`, while `macro.json`,
# `short_interest.json`, `macro_history.parquet`,
# `macro_release_calendar.parquet`, `archive/fundamentals/2026-09-04.json`,
# `universe_classification/latest.json` and `valuation_medians/latest.json`
# were all written 2026-09-05T09:48-10:30Z by the SAME cycle's earlier
# attempt, and the SSM log
# `_ssm_logs/data-weekly/2026-09-05/...-155232Z-watch-rerun-2026-09-04-1.log`
# shows every one of those collectors logging `PHASE_SKIP ...
# reason=auto_skip_marker_ok`. The stage read STALE on its own valid output.
#
# WHY IT IS NOT A BLANKET CHANGE. Making every stage's window track the
# cycle would delete the leftover-from-a-previous-cycle detector for the
# stages that have no auto-skip — for those, "artifacts exist but predate
# this attempt" is precisely the finding, and reusing an earlier attempt's
# window would turn a stage that STOPPED WRITING on a rerun into a false
# COVERED. The narrowing is therefore a per-stage claim about the workload,
# and it is a CHECKED claim, not a hand-kept list: `tests/
# test_stage_window_tracks_the_cycle.py` DERIVES auto-skip capability from
# `weekly_collector.py`'s own source (every `_phase_collect(...)` call in the
# mode's dispatch function that does not pass `supports_auto_skip=False`) and
# asserts the biconditional against this flag, per launcher. Flip a phase's
# auto-skip in the collector and that test names the launcher that must
# change with it.
_STAGE_WINDOW_TRACKS_CYCLE="${_STAGE_WINDOW_TRACKS_CYCLE:-}"

# Resolve the window this stage should assert against.
#
# For a stage that has NOT declared `_STAGE_WINDOW_TRACKS_CYCLE=1` this is
# `$_STAGE_WINDOW_START` verbatim — the semantics are unchanged fleet-wide.
#
# For a declared stage, an EXISTING `_stage_coverage/<run_date>/<stage>.json`
# verdict means an earlier attempt of THIS cycle already asserted, and its
# `window_start` is that first attempt's start. Reusing it makes the window
# track the CYCLE rather than this particular execution. It never admits a
# PREVIOUS cycle's leftovers: the first attempt for run_date X necessarily
# started after cycle X began, so a genuine leftover still predates it and
# still reads STALE. Successive attempts re-write the same value, so the
# window is monotone and converges on the cycle's first attempt.
#
# Every failure path DEGRADES TOWARD THE ALARMING SIDE — back to this
# execution's start — and says so on stderr. That direction is deliberate:
# a false STALE is a finding a human reads and can dismiss; a false COVERED
# is silence, and silence is what this whole mechanism exists to remove
# (`principles.md` §2.7). Nothing here can fail the stage: the caller is in
# observe mode and this function always returns 0.
#
# Residual, named rather than swallowed: if the cycle's FIRST attempt died
# before it asserted, no verdict exists to reuse and the second attempt
# captures its own window — the original false STALE is still possible for
# that shape. It is a strictly smaller window of exposure than today's
# (which mis-reads EVERY rerun), and an attempt that never reached its
# assertion is also an attempt whose phases mostly did not complete.
resolve_stage_window_start() {
  local stage="$1" run_date="${2:-}"

  if [ "${_STAGE_WINDOW_TRACKS_CYCLE:-}" != "1" ]; then
    printf '%s' "$_STAGE_WINDOW_START"
    return 0
  fi

  if [ -z "$run_date" ]; then
    echo "WARNING: stage-window ${stage}: no run_date — cannot look up this cycle's first-attempt window; using this execution's start $_STAGE_WINDOW_START (alpha-engine-config-I10194)" >&2
    printf '%s' "$_STAGE_WINDOW_START"
    return 0
  fi

  local key="_stage_coverage/${run_date}/${stage}.json"
  local err_file body rc=0
  err_file="$(mktemp)"
  body="$(aws s3 cp "s3://${S3_BUCKET}/${key}" - --region "$AWS_REGION" 2>"$err_file")" || rc=$?

  if [ "$rc" -ne 0 ]; then
    if grep -qiE '404|Not Found|NoSuchKey|does not exist' "$err_file"; then
      echo "  stage-window ${stage}: no prior verdict at s3://${S3_BUCKET}/${key} — this is the cycle's first attempt; window = ${_STAGE_WINDOW_START}" >&2
    else
      echo "WARNING: stage-window ${stage}: could not read s3://${S3_BUCKET}/${key} (rc=${rc}): $(tr '\n' ' ' < "$err_file")" >&2
      echo "         Degrading to THIS execution's window start ${_STAGE_WINDOW_START} — the ALARMING side. A rerun may now report STALE on its own auto-skipped output (alpha-engine-config-I10194 §3); that is a visible finding, not silence." >&2
    fi
    rm -f "$err_file"
    printf '%s' "$_STAGE_WINDOW_START"
    return 0
  fi
  rm -f "$err_file"

  local prior=""
  prior="$(printf '%s' "$body" | "$LIB_PYTHON" -c 'import json,sys
try:
    value = json.load(sys.stdin).get("window_start")
except Exception:
    value = None
print(value if isinstance(value, str) and value.strip() else "")' 2>/dev/null)" || prior=""

  if [ -z "$prior" ]; then
    echo "WARNING: stage-window ${stage}: the prior verdict for ${run_date} carries no usable window_start — degrading to this execution's start ${_STAGE_WINDOW_START} (the alarming side)" >&2
    printf '%s' "$_STAGE_WINDOW_START"
    return 0
  fi

  echo "  stage-window ${stage}: reusing this CYCLE's first-attempt window ${prior} from the existing ${run_date} verdict, not this execution's ${_STAGE_WINDOW_START} (alpha-engine-config-I10194 §3)" >&2
  printf '%s' "$prior"
  return 0
}

# The date a stage LABELS itself with (alpha-engine-config-I11475).
#
# Every launcher used to print `$(date +%Y-%m-%d)` in its banner — the box's
# UTC calendar day. A run that crosses 00:00 UTC then labels itself a day
# ahead of its own cycle: measured on the 2026-09-23 weekly rehearsal, whose
# run_date was 2026-09-23 and whose DataPhase2 banner (launched ~00:44 UTC)
# read 2026-09-24. The stage-coverage assertion a few lines further down the
# same launchers already files its verdict under `$EXECUTION_RUN_DATE`, so the
# banner and the verdict named two different days for one stage.
#
# Resolution order, one definition for every launcher that sources this file:
#   1. `$EXECUTION_RUN_DATE` — exported by step_function.json from $.run_date
#      (the cycle's trading day after NormalizeRunDates); the SF's own answer.
#   2. The exchange's calendar day (America/New_York) — for a manual launch
#      with no SF around it. Never the UTC day: between 00:00 UTC and midnight
#      ET the UTC day is already tomorrow on the exchange's calendar, the same
#      class `collectors/universe_returns.py::_market_today` fixed in
#      alpha-engine-config-I11445.
stage_run_date() {
  if [ -n "${EXECUTION_RUN_DATE:-}" ]; then
    printf '%s' "$EXECUTION_RUN_DATE"
    return 0
  fi
  TZ=America/New_York date +%Y-%m-%d
}
