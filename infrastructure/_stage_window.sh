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

# Where a declared stage records its FIRST ENTRY for a cycle.
#
# NOT under `_stage_coverage/<run_date>/`: the coverage sweep
# (`nousergon_lib.pipeline_status.coverage._load_verdicts_one`) reads every
# `.json` directly under that prefix as a stage VERDICT, so a record there
# would be counted as a stage. `_stage_coverage/_entry/` sits beside the
# sweep's own `_stage_coverage/_sweep/`, where no date partition is listed.
_STAGE_ENTRY_PREFIX="_stage_coverage/_entry"

# Record the instant this stage was FIRST entered for `run_date`, before its
# workload writes anything (alpha-engine-config-I10173 / -I10194 §3).
#
# WHY A RECORD, not only the prior verdict. `resolve_stage_window_start` below
# used to recover the cycle's first attempt from the `window_start` of an
# EXISTING verdict. A verdict is written at the END of an attempt, so it can
# only carry what that attempt believed its window was — and twice that was
# already late:
#
#   - Every cycle from 2026-09-11 to 2026-09-25: DataPhase1's spot was
#     reclaimed, the launcher re-exec'd itself (`exec bash "$0"` in
#     `_spot_common.sh::on_exit`), and the unexported start was recomputed as
#     "now". Measured on rehearsal-2026-09-25-1: the command entered 21:50:25Z,
#     attempt 1 wrote macro/short_interest/macro_history/release_calendar/
#     universe_classification at 21:54-22:12Z, attempt 2 captured 22:19:59Z,
#     auto-skipped them at 22:22Z, and recorded STALE. `nousergon-data#1972`
#     exported the start so a relaunch keeps it — but the late window was
#     already PERSISTED in that verdict, and the scheduled run the next morning
#     reused it verbatim (verdict version recorded 2026-09-26T10:24:06Z,
#     `window_start: 2026-09-25T22:19:59+00:00`, five artifacts STALE again).
#     A window that is only as good as the last verdict's is poisoned for the
#     rest of the cycle by one bad verdict.
#   - An SF reissue (`DataPhase1Reissue` in step_function.json) is a NEW SSM
#     command, so no exported value survives into it; and a first command that
#     died mid-workload never reached its assertion, so there is no verdict to
#     reuse either. The reissue then captured its own start, after the first
#     command's writes — the same false STALE one level up.
#
# A record written at FIRST ENTRY closes both: it exists before any workload
# write, it is written once per (run_date, stage) and never moved, and the
# resolver takes the earliest start it can prove for this cycle.
#
# Write-once: an existing record is left alone (the first entry wins). An
# unreadable record is NOT overwritten — a rewrite would move the first entry
# LATER, which is the defect. Never fails the stage: always returns 0, and
# every non-write is loud on stderr.
#
# Call it only on the path that runs the real workload — never from a
# `--preflight-only` / `--smoke-only` run, which writes nothing and would
# otherwise open the window before the cycle's first real write.
record_stage_entry() {
  local stage="$1" run_date="${2:-}"

  if [ "${_STAGE_WINDOW_TRACKS_CYCLE:-}" != "1" ]; then
    return 0
  fi

  if [ -z "$run_date" ]; then
    echo "WARNING: stage-entry ${stage}: no run_date — not recording a first entry; a later attempt of this cycle cannot find it (alpha-engine-config-I10194)" >&2
    return 0
  fi

  local key="${_STAGE_ENTRY_PREFIX}/${run_date}/${stage}.json"
  local err_file rc=0
  err_file="$(mktemp)"
  aws s3api head-object --bucket "$S3_BUCKET" --key "$key" --region "$AWS_REGION" >/dev/null 2>"$err_file" || rc=$?

  if [ "$rc" -eq 0 ]; then
    echo "  stage-entry ${stage}: ${run_date} was already entered (s3://${S3_BUCKET}/${key}) — keeping the FIRST entry, not this attempt's ${_STAGE_WINDOW_START}" >&2
    rm -f "$err_file"
    return 0
  fi

  if ! grep -qiE '404|Not Found|NoSuchKey|does not exist' "$err_file"; then
    echo "WARNING: stage-entry ${stage}: could not read s3://${S3_BUCKET}/${key} (rc=${rc}): $(tr '\n' ' ' < "$err_file")" >&2
    echo "         NOT writing a first-entry record — overwriting one that exists would move this cycle's window LATER. A later attempt falls back to the prior verdict's window." >&2
    rm -f "$err_file"
    return 0
  fi

  local body
  body="$(printf '{"stage": "%s", "run_date": "%s", "window_start": "%s", "recorded_at": "%s", "spot_attempt": "%s"}' \
    "$stage" "$run_date" "$_STAGE_WINDOW_START" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${SPOT_ATTEMPT:-}")"
  rc=0
  printf '%s' "$body" | aws s3 cp - "s3://${S3_BUCKET}/${key}" --region "$AWS_REGION" --content-type application/json >/dev/null 2>"$err_file" || rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "  stage-entry ${stage}: recorded this cycle's first entry ${_STAGE_WINDOW_START} for ${run_date} at s3://${S3_BUCKET}/${key}" >&2
  else
    echo "WARNING: stage-entry ${stage}: could not write s3://${S3_BUCKET}/${key} (rc=${rc}): $(tr '\n' ' ' < "$err_file") — a later attempt of this cycle falls back to the prior verdict's window" >&2
  fi
  rm -f "$err_file"
  return 0
}

# Resolve the window this stage should assert against.
#
# For a stage that has NOT declared `_STAGE_WINDOW_TRACKS_CYCLE=1` this is
# `$_STAGE_WINDOW_START` verbatim — the semantics are unchanged fleet-wide.
#
# For a declared stage it is the EARLIEST start this cycle can prove for the
# stage, read from two places under `run_date`:
#
#   1. the first-entry record `record_stage_entry` wrote before the cycle's
#      first workload write (see above) — the authoritative source;
#   2. the `window_start` of an EXISTING `_stage_coverage/<run_date>/<stage>.json`
#      verdict — an earlier attempt's window, kept for cycles whose first
#      attempt ran before the record existed or could not write it.
#
# Both are starts of attempts of THIS cycle, so the earliest is the stage's
# first entry and it never admits a PREVIOUS cycle's leftovers: those were
# written before any attempt for run_date X began. That is also CHECKED, not
# assumed — a candidate earlier than `run_date`'s own 00:00Z cannot be an entry
# for that cycle and is rejected loudly.
#
# Every failure path DEGRADES TOWARD THE ALARMING SIDE — a candidate that
# cannot be read or parsed is dropped, and with none left the window is this
# execution's own start — and says so on stderr. That direction is deliberate:
# a false STALE is a finding a human reads and can dismiss; a false COVERED is
# silence, and silence is what this whole mechanism exists to remove
# (`principles.md` §2.7). Nothing here can fail the stage: the caller is in
# observe mode and this function always returns 0.
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

  local candidates=() source key err_file body rc value
  for source in first-entry-record prior-verdict; do
    if [ "$source" = "first-entry-record" ]; then
      key="${_STAGE_ENTRY_PREFIX}/${run_date}/${stage}.json"
    else
      key="_stage_coverage/${run_date}/${stage}.json"
    fi
    rc=0
    err_file="$(mktemp)"
    body="$(aws s3 cp "s3://${S3_BUCKET}/${key}" - --region "$AWS_REGION" 2>"$err_file")" || rc=$?

    if [ "$rc" -ne 0 ]; then
      if grep -qiE '404|Not Found|NoSuchKey|does not exist' "$err_file"; then
        if [ "$source" = "prior-verdict" ]; then
          echo "  stage-window ${stage}: no prior verdict at s3://${S3_BUCKET}/${key}" >&2
        else
          echo "  stage-window ${stage}: no first-entry record at s3://${S3_BUCKET}/${key}" >&2
        fi
      else
        echo "WARNING: stage-window ${stage}: could not read s3://${S3_BUCKET}/${key} (rc=${rc}): $(tr '\n' ' ' < "$err_file")" >&2
        echo "         Dropping the ${source} — the ALARMING side. A rerun may now report STALE on its own auto-skipped output (alpha-engine-config-I10194 §3); that is a visible finding, not silence." >&2
      fi
      rm -f "$err_file"
      continue
    fi
    rm -f "$err_file"

    value="$(printf '%s' "$body" | "$LIB_PYTHON" -c 'import json,sys
try:
    value = json.load(sys.stdin).get("window_start")
except Exception:
    value = None
print(value if isinstance(value, str) and value.strip() else "")' 2>/dev/null)" || value=""

    if [ -z "$value" ]; then
      echo "WARNING: stage-window ${stage}: the ${source} for ${run_date} carries no usable window_start — dropping it (the alarming side)" >&2
      continue
    fi
    candidates+=("${source}=${value}")
  done

  local prior=""
  if [ "${#candidates[@]}" -gt 0 ]; then
    prior="$(printf '%s\n' "${candidates[@]}" | "$LIB_PYTHON" -c 'import sys
from datetime import datetime, timezone
run_date = sys.argv[1]
floor = datetime.fromisoformat(run_date).replace(tzinfo=timezone.utc)
best = None
for line in sys.stdin.read().splitlines():
    source, _, raw = line.partition("=")
    try:
        at = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        print(f"WARNING: stage-window: the {source} window_start {raw!r} does not parse - dropping it (the alarming side)", file=sys.stderr)
        continue
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    if at < floor:
        print(f"WARNING: stage-window: the {source} window_start {raw} predates run_date {run_date} itself - it cannot be an entry for this cycle; dropping it (the alarming side)", file=sys.stderr)
        continue
    if best is None or at < best[0]:
        best = (at, raw.strip())
print(best[1] if best else "")' "$run_date")" || prior=""
  fi

  if [ -z "$prior" ]; then
    echo "  stage-window ${stage}: no usable first-entry record or prior verdict for ${run_date} — this is the cycle's first attempt; window = ${_STAGE_WINDOW_START}" >&2
    printf '%s' "$_STAGE_WINDOW_START"
    return 0
  fi

  echo "  stage-window ${stage}: reusing this CYCLE's first-attempt window ${prior} (earliest of: ${candidates[*]}), not this execution's ${_STAGE_WINDOW_START} (alpha-engine-config-I10194 §3)" >&2
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
