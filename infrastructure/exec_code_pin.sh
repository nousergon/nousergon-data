#!/usr/bin/env bash
# infrastructure/exec_code_pin.sh — one code SHA per repo per SF execution.
#
# Usage (as ec2-user, under the fleet git-sync lock):
#   flock -w 150 /home/ec2-user/.ae-git-sync.lock \
#     bash /home/ec2-user/alpha-engine-data/infrastructure/exec_code_pin.sh \
#       <execution-name> <checkout-dir>
#
# WHY (alpha-engine-config-I11570). Every weekly-SF box stage used to open with
# `git -C <checkout> pull --ff-only origin main`. On rehearsal-2026-09-24-1 the
# DataPhase1 re-issue ran that pull again and advanced the box's
# alpha-engine-data checkout dafacc5 -> 6d2460d (PR1927) mid-execution, so one
# execution ran two code SHAs and its results could not be tied to either.
#
# CONTRACT
#   * The FIRST call for (execution, checkout) pulls origin main exactly as
#     before and records the SHA it resolved in
#     $AE_EXEC_PIN_ROOT/<execution-name>/<checkout basename>. A fresh execution
#     therefore still runs "latest main at start".
#   * Every LATER call in the same execution — the next stage, a re-issue of
#     the same stage, a parallel sibling — checks out exactly that SHA and
#     never pulls. A re-issue therefore re-runs the code that failed.
#   * A new execution (scripts/weekly_sf_rerun.py reuses the box under a new
#     execution name) has no pin yet, so it resolves main afresh.
#   * The spot workers that spot_{morning_enrich,data_phase1,rag_ingestion}.sh
#     launch read the same pin via $RUN_TOKEN (see _spot_common.sh
#     `_exec_code_pin_sha`), so the worker runs the launcher's SHA rather than
#     re-cloning main.
#
# The caller must hold /home/ec2-user/.ae-git-sync.lock: check-then-pull is not
# atomic on its own, and ParityParallel runs three of these concurrently.
#
# All output goes to stderr so a stage whose StandardOutputContent is parsed
# (ResolveZooSpecs) is unaffected.
#
# The body is one function called on the last line: bash parses the whole
# function before running it, so the pull below rewriting this very file (it
# lives in the alpha-engine-data checkout) cannot change what executes.

_exec_code_pin_main() {
  set -euo pipefail
  exec 1>&2

  local exec_name="${1:-}" checkout="${2:-}"
  if [ -z "$exec_name" ] || [ -z "$checkout" ] || [ "$#" -ne 2 ]; then
    echo "exec-code-pin: usage: exec_code_pin.sh <execution-name> <checkout-dir>" >&2
    return 2
  fi
  # SF execution names cannot contain '/', but the name becomes a path
  # component here, so refuse anything that could escape the pin root.
  if ! [[ "$exec_name" =~ ^[A-Za-z0-9._-]+$ ]] || [ "$exec_name" = "." ] || [ "$exec_name" = ".." ]; then
    echo "exec-code-pin: refusing execution name '${exec_name}' (not [A-Za-z0-9._-]+)" >&2
    return 2
  fi
  if ! git -C "$checkout" rev-parse --git-dir >/dev/null 2>&1; then
    echo "exec-code-pin: ${checkout} is not a git checkout" >&2
    return 1
  fi

  local pin_root="${AE_EXEC_PIN_ROOT:-/home/ec2-user/.ae-exec-pin}"
  local pin_dir="${pin_root}/${exec_name}"
  local pin_file
  pin_file="${pin_dir}/$(basename "$checkout")"

  local sha head
  if [ -s "$pin_file" ]; then
    sha="$(tr -d '[:space:]' < "$pin_file")"
    if ! [[ "$sha" =~ ^[0-9a-f]{40}$ ]]; then
      echo "exec-code-pin: pin file ${pin_file} holds '${sha}', not a 40-hex SHA" >&2
      return 1
    fi
    head="$(git -C "$checkout" rev-parse HEAD)"
    if [ "$head" != "$sha" ]; then
      if ! git -C "$checkout" cat-file -e "${sha}^{commit}" 2>/dev/null; then
        git -C "$checkout" fetch -q origin "$sha"
      fi
      git -C "$checkout" -c advice.detachedHead=false checkout -q --detach "$sha"
      echo "exec-code-pin: ${checkout} was at ${head}; checked out execution pin ${sha} (execution ${exec_name})"
    else
      echo "exec-code-pin: ${checkout} at execution pin ${sha} (execution ${exec_name})"
    fi
    return 0
  fi

  git -C "$checkout" pull --ff-only origin main
  sha="$(git -C "$checkout" rev-parse HEAD)"
  mkdir -p "$pin_dir"
  local tmp
  tmp="$(mktemp "${pin_dir}/.pin.XXXXXX")"
  printf '%s\n' "$sha" > "$tmp"
  mv -f "$tmp" "$pin_file"
  echo "exec-code-pin: ${checkout} resolved origin/main -> ${sha}; pinned for execution ${exec_name}"
}

_exec_code_pin_main "$@"
