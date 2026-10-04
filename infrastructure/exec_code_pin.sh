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
#   * The pin OUTLIVES the box. The first call writes it to S3 at
#     s3://alpha-engine-research/health/exec_code_pin/<execution>/<checkout
#     basename> BEFORE it records it locally, and a call that finds no local
#     pin reads that key before it considers main. A replacement box after
#     substrate loss (RelaunchWeeklyFreshnessSpot -> ResumeAfterSubstrateRelaunch)
#     is a fresh clone of main with an empty pin root. Until this mirror it
#     resolved main again, so one execution ran two SHAs across the relaunch,
#     which is the same defect by a second route (the residue PR1966 named).
#     Restoring the local file also restores the spot-worker pin above,
#     because that reads the local file.
#   * S3 failures fail LOUD. A key that does not exist (NoSuchKey) means "first
#     call of this execution". Any other read error, or a write that still
#     fails after bounded retries, fails the call. It never falls back to main,
#     because a silent fallback is how one execution comes to run two SHAs.
#     `--region` is always explicit: the sync runs under `sudo -u ec2-user`,
#     which drops the stage's exported AWS_REGION, and the weekly box's SSM
#     shell exports none (alpha-engine-config-I11567).
#
# The caller must hold /home/ec2-user/.ae-git-sync.lock: check-then-pull is not
# atomic on its own, and ParityParallel runs three of these concurrently.
#
# All output goes to stderr so a stage whose StandardOutputContent is parsed
# (ResolveZooSpecs) is unaffected.
#
# The body is one function (plus the helpers above) called on the last line: bash parses the whole
# function before running it, so the pull below rewriting this very file (it
# lives in the alpha-engine-data checkout) cannot change what executes.

# Advance <checkout> to the tip of origin/main WITHOUT `git pull`.
#
# WHY (alpha-engine-config-I11780). `git pull --ff-only origin main` runs a fetch and
# then reads <checkout>/.git/FETCH_HEAD, which is shared per-checkout state: fetch
# truncates it at start and appends at the end, so two fetches overlapping in one
# checkout leave TWO for-merge lines and the pull dies with "fatal: Cannot fast-forward
# to multiple branches." (rc=128). The flock this helper runs under serialises pin
# calls only — any git fetch in that checkout that does not hold the lock defeats it.
# Measured 2026-10-01 on preflight-sweep-20261001T080010Z (ModelZooSelect); the
# mechanism reproduces locally (a pull racing a bare fetch fails 98 of 100 times).
#
# So nothing here reads FETCH_HEAD: the tip comes from `git ls-remote` (no local
# state), the object is fetched by exact SHA with --no-write-fetch-head, and the move
# is `git merge --ff-only <sha>`. The contract is unchanged — latest main at the
# start of the execution, fast-forward only, HEAD left where a pull would leave it.
# Bounded retry, because the remaining failure modes (network blip, a ref/index lock
# held by that same unlocked writer) are transient; a non-fast-forward is not, and
# still fails after the last attempt.
_exec_code_pin_advance_to_main() {
  local checkout="$1" tip attempt=1 max=3
  while :; do
    if tip="$(git -C "$checkout" ls-remote origin refs/heads/main | awk 'NR==1{print $1}')" \
       && [[ "$tip" =~ ^[0-9a-f]{40}$ ]] \
       && { git -C "$checkout" cat-file -e "${tip}^{commit}" 2>/dev/null \
            || git -C "$checkout" fetch -q --no-write-fetch-head origin "$tip"; } \
       && git -C "$checkout" merge -q --ff-only "$tip"; then
      return 0
    fi
    if [ "$attempt" -ge "$max" ]; then
      echo "exec-code-pin: could not fast-forward ${checkout} to origin/main after ${max} attempts" >&2
      return 1
    fi
    echo "exec-code-pin: advancing ${checkout} to origin/main failed (attempt ${attempt}/${max}); retrying" >&2
    sleep $((attempt * 2))
    attempt=$((attempt + 1))
  done
}

# ── Durable pin mirror (alpha-engine-config-I11570, substrate-loss residue) ──
# `health/*` is the prefix alpha-engine-executor-role's SCOPED S3 grant already
# reads and writes, beside health/weekly_preflight_on_spot/<run_date>/<execution>.json.
# A future narrowing of the bucket-wide wildcard (alpha-engine-config-I11063) can
# therefore not turn this mirror into a production AccessDenied.
_EXEC_CODE_PIN_S3_BUCKET="${AE_EXEC_PIN_S3_BUCKET:-alpha-engine-research}"
_EXEC_CODE_PIN_S3_PREFIX="${AE_EXEC_PIN_S3_PREFIX:-health/exec_code_pin}"
_EXEC_CODE_PIN_S3_REGION="${AE_EXEC_PIN_S3_REGION:-us-east-1}"

# Print the mirrored SHA for <key>, or nothing when the key does not exist.
# Returns 1 on any other error, after bounded retries.
_exec_code_pin_s3_get() {
  local key="$1" attempt=1 max=3 body err
  body="$(mktemp)"
  err="$(mktemp)"
  while :; do
    if aws s3api get-object --region "$_EXEC_CODE_PIN_S3_REGION" \
        --bucket "$_EXEC_CODE_PIN_S3_BUCKET" --key "$key" "$body" >/dev/null 2>"$err"; then
      tr -d '[:space:]' < "$body"
      rm -f "$body" "$err"
      return 0
    fi
    if grep -q "NoSuchKey" "$err"; then
      rm -f "$body" "$err"
      return 0
    fi
    if [ "$attempt" -ge "$max" ]; then
      echo "exec-code-pin: could not read s3://${_EXEC_CODE_PIN_S3_BUCKET}/${key} after ${max} attempts: $(tr '\n' ' ' < "$err")" >&2
      rm -f "$body" "$err"
      return 1
    fi
    sleep $((attempt * 2))
    attempt=$((attempt + 1))
  done
}

_exec_code_pin_s3_put() {
  local key="$1" sha="$2" attempt=1 max=3 body
  body="$(mktemp)"
  printf '%s\n' "$sha" > "$body"
  while :; do
    if aws s3api put-object --region "$_EXEC_CODE_PIN_S3_REGION" \
        --bucket "$_EXEC_CODE_PIN_S3_BUCKET" --key "$key" \
        --content-type text/plain --body "$body" >/dev/null; then
      rm -f "$body"
      return 0
    fi
    if [ "$attempt" -ge "$max" ]; then
      echo "exec-code-pin: could not write s3://${_EXEC_CODE_PIN_S3_BUCKET}/${key} after ${max} attempts" >&2
      rm -f "$body"
      return 1
    fi
    sleep $((attempt * 2))
    attempt=$((attempt + 1))
  done
}

# Record <sha> in <pin_file> atomically.
_exec_code_pin_record_local() {
  local pin_file="$1" sha="$2" tmp
  mkdir -p "$(dirname "$pin_file")"
  tmp="$(mktemp "$(dirname "$pin_file")/.pin.XXXXXX")"
  printf '%s\n' "$sha" > "$tmp"
  mv -f "$tmp" "$pin_file"
}

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

  local s3_key
  s3_key="${_EXEC_CODE_PIN_S3_PREFIX}/${exec_name}/$(basename "$checkout")"

  local sha head
  if [ ! -s "$pin_file" ]; then
    # No local pin. Either this is the execution's first call for this
    # checkout, or this box replaced one lost mid-execution. The S3 mirror
    # is the only thing that can tell those apart.
    sha="$(_exec_code_pin_s3_get "$s3_key")" || return 1
    if [ -n "$sha" ]; then
      if ! [[ "$sha" =~ ^[0-9a-f]{40}$ ]]; then
        echo "exec-code-pin: s3://${_EXEC_CODE_PIN_S3_BUCKET}/${s3_key} holds '${sha}', not a 40-hex SHA" >&2
        return 1
      fi
      _exec_code_pin_record_local "$pin_file" "$sha"
      echo "exec-code-pin: restored execution pin ${sha} for ${checkout} from s3://${_EXEC_CODE_PIN_S3_BUCKET}/${s3_key} (this box did not record it, e.g. a replacement after substrate loss)"
    fi
  fi
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

  _exec_code_pin_advance_to_main "$checkout"
  sha="$(git -C "$checkout" rev-parse HEAD)"
  # Durable copy FIRST. If it cannot be written the call fails with no
  # local pin either, so the re-issue resolves and mirrors again. Writing the
  # local pin first would let every later stage proceed on a pin that a
  # replacement box can never recover.
  _exec_code_pin_s3_put "$s3_key" "$sha" || return 1
  _exec_code_pin_record_local "$pin_file" "$sha"
  echo "exec-code-pin: ${checkout} resolved origin/main -> ${sha}; pinned for execution ${exec_name} (mirrored to s3://${_EXEC_CODE_PIN_S3_BUCKET}/${s3_key})"
}

_exec_code_pin_main "$@"
