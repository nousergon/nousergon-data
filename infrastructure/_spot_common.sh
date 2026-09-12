#!/usr/bin/env bash
# infrastructure/_spot_common.sh — Shared spot-instance infrastructure for
# nousergon-data per-stage launcher scripts.
#
# Source this file from per-stage scripts:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/_spot_common.sh"
#
# Provides shared defaults, spot launch, SSM dispatch, cleanup with
# spot-interruption retry, bootstrap, and dependency install.
#
# Each per-stage script MUST set the following BEFORE sourcing this file:
#   _SPOT_NAME    — spot instance Name tag suffix (e.g. "morning-enrich")
#   _SSM_SLUG     — log-capture slug for krepis.ssm_log_capture
#   _PROCESS_NAME — CloudWatch dimension Process name
#   MAX_RUNTIME_SECONDS — SSM command timeout for the workload
#   ORIG_ARGS     — array copy of "$@" captured before flag parsing

set -euo pipefail

# ── Global defaults ──────────────────────────────────────────────────────────

AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-alpha-engine-research}"
BRANCH="${BRANCH:-main}"

INSTANCE_TYPES="${INSTANCE_TYPES:-c5.large,m5.large,c6i.large,c5a.large}"
INSTANCE_TYPE=""
AMI_ID="ami-0c421724a94bba6d6"  # Amazon Linux 2023 x86_64
KEY_NAME="alpha-engine-key"
SECURITY_GROUP="sg-03cd3c4bd91e610b0"
SUBNETS="${SUBNETS:-subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,subnet-c670118d,subnet-7cff7c43,subnet-e07166ec}"
IAM_PROFILE="alpha-engine-executor-profile"
# Lib CLI path. The ops-owned guard /opt/nousergon/bin/lib-python
# (nous-ergon-ops: alpha-engine-dashboard/live/infrastructure/bin/lib-python)
# execs the box's DECLARED krepis venv and aborts with EX_CONFIG (78), naming
# the version it found, rather than silently falling back to a co-tenant
# checkout — the defect alpha-engine-config-I6931/I7343 removes.
#
# It is NOT the default here, because THIS SCRIPT DOES NOT RUN ON THAT BOX
# (alpha-engine-config-I7386). The guard is installed by
# nous-ergon-ops/alpha-engine-dashboard/live/infrastructure/bin/install-box-config.sh,
# whose whole tree provisions the DASHBOARD BOX. This file is sourced by
# spot_morning_enrich.sh / spot_data_phase1.sh / spot_rag_ingestion.sh, which
# the weekly SF delivers as ssm:sendCommand payloads to $.ec2_instance_id —
# the ephemeral weekly-freshness spot. That spot is bootstrapped by
# infrastructure/lambdas/weekly-freshness-spot-dispatcher/index.py
# (_bootstrap_command): it clones four repos and builds
# /home/ec2-user/alpha-engine-dashboard/.venv, and never creates
# /opt/nousergon. Measured on execution
# friday-shell-2026-08-14-validate-i7382, MorningEnrich:
# "_spot_common.sh: line 128: /opt/nousergon/bin/lib-python: No such file or
# directory", exit 127.
#
# So the default names the interpreter that host actually has. The
# ${LIB_PYTHON:-...} override is preserved, so a caller ON a box that does
# have the guard still names it explicitly and gets the declared floor.
# Do NOT add a guard block here: the contract lives ONCE, in the repo that
# owns the box's provisioning (nine copies across five repos is I6922). The
# SOTA close — install the guard on the spot too, then restore this default —
# is alpha-engine-config-I7383.
LIB_PYTHON="${LIB_PYTHON:-/home/ec2-user/alpha-engine-dashboard/.venv/bin/python}"

# Spot-reclaim relaunch (#883)
MAX_SPOT_ATTEMPTS="${MAX_SPOT_ATTEMPTS:-2}"
SPOT_ATTEMPT="${SPOT_ATTEMPT:-1}"
SF_EXECUTION_TIMEOUT="${SF_EXECUTION_TIMEOUT:-}"
SPOT_RETRY_BACKOFF_SECONDS="${SPOT_RETRY_BACKOFF_SECONDS:-20}"

# Per-stage identity — DECLARED HERE, NEVER DEFAULTED HERE.
#
# This block used to carry `data-weekly` / `spot-data` defaults, and all three
# per-stage scripts then set their own with the SAME `${VAR:-...}` form AFTER
# sourcing this file — where the parameter is already non-empty, so the
# expansion is a NO-OP and the stage identity silently stayed whatever this
# file said. spot_morning_enrich.sh asks for `morning-enrich`, spot_data_
# phase1.sh for `data-phase1`, spot_rag_ingestion.sh for `rag-ingestion`, and
# every one of them ran as `data-weekly`: the instance Name tag, the
# `Process` dimension on the CloudWatch `Heartbeat` metric, the `Process`
# dimension on `SpotInterruptionRetry`, and the `run_ssm` command description
# all carried the wrong stage. Three stages, one identity — so a heartbeat
# gap could not be attributed and a retry could not be counted per stage.
#
# crucible-predictor hit and fixed the identical defect in its twin of this
# file (measured on `watch-rerun-2026-08-10-9`, 2026-08-11: the heartbeat
# emitted under slug `spot-spot-training` although the launcher asked for
# `spot-full-training`). This is the mirror, per alpha-engine-config-I6922 —
# the same no-op class, unported until now.
#
# Declared EMPTY so `set -u` is satisfied at source time and the per-stage
# `${VAR:-<stage default>}` lines are load-bearing again (an explicit env
# override still wins, because it is set before this file runs). `spot_launch`
# asserts all four are non-empty before any instance exists, so a stage that
# forgets one fails loud and free rather than inheriting a sibling's identity.
_SPOT_NAME="${_SPOT_NAME:-}"
_SSM_SLUG="${_SSM_SLUG:-}"
_PROCESS_NAME="${_PROCESS_NAME:-}"
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-}"

# Stage-coverage window (alpha-engine-config-I7214): the instant this launcher
# started. An artifact whose LastModified predates it is a leftover from a
# previous cycle, not this run's output — an existence-only probe cannot tell
# those apart, which is how a stage that STOPPED writing keeps reading green.
# Captured here rather than at assertion time because the workload runs in
# between: a window taken after the write would be trivially satisfied by it.
_STAGE_WINDOW_START="${_STAGE_WINDOW_START:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

# The window rule lives in ONE file, sourced by both this file and the
# spot_data_weekly.sh monolith (alpha-engine-config-I10194 §3).
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_stage_window.sh"

# Derived at launch time
_INSTANCE_ID=""
_S3_STAGING_PREFIX=""
_S3_STAGING=""

# krepis RUN_TOKEN forwarding
if [ -n "${RUN_TOKEN:-}" ]; then
  _RUN_TOKEN_EXPORT="export RUN_TOKEN=${RUN_TOKEN}"$'\n'
else
  _RUN_TOKEN_EXPORT="export RUN_TOKEN=spot-data-weekly-$(date -u +%Y%m%d)"$'\n'
fi

# ── Spot launch (capacity-resilient) ─────────────────────────────────────────

spot_launch() {
  # Stage identity must be real BEFORE anything is billable. See the
  # "Per-stage identity" block above for why these can no longer be defaulted
  # in this file: a default here silently swallows the per-stage assignment.
  local _unset=""
  [ -n "$_SPOT_NAME" ] || _unset="${_unset} _SPOT_NAME"
  [ -n "$_SSM_SLUG" ] || _unset="${_unset} _SSM_SLUG"
  [ -n "$_PROCESS_NAME" ] || _unset="${_unset} _PROCESS_NAME"
  [ -n "$MAX_RUNTIME_SECONDS" ] || _unset="${_unset} MAX_RUNTIME_SECONDS"
  if [ -n "$_unset" ]; then
    echo "ERROR: per-stage variable(s) unset before spot_launch:${_unset}" >&2
    echo "       Set them AFTER sourcing _spot_common.sh — see its header." >&2
    exit 2
  fi

  echo "==> Requesting spot instance (lib CLI rotation: types=[$INSTANCE_TYPES], subnets=[$SUBNETS])..."

  _INSTANCE_ID=$("$LIB_PYTHON" -m krepis.ec2_spot launch \
    --types "$INSTANCE_TYPES" \
    --subnets "$SUBNETS" \
    --image-id "$AMI_ID" \
    --key-name "$KEY_NAME" \
    --security-group "$SECURITY_GROUP" \
    --iam-profile "$IAM_PROFILE" \
    --name "alpha-engine-data-${_SPOT_NAME}-$(date +%Y%m%d)" \
    --region "$AWS_REGION")
  local ec2_spot_rc=$?

  if [ "$ec2_spot_rc" -ne 0 ] || [ -z "$_INSTANCE_ID" ]; then
    if [ "$ec2_spot_rc" -eq 64 ]; then
      echo "ERROR: capacity exhausted across all instance_type x subnet combinations" >&2
    fi
    if [ "$ec2_spot_rc" -eq 0 ]; then
      echo "ERROR: ec2_spot launch exited 0 without an instance id — failing loud (config#1646)" >&2
      ec2_spot_rc=1
    fi
    exit "$ec2_spot_rc"
  fi

  echo "  Instance ID: $_INSTANCE_ID"

  local _RUN_ID
  _RUN_ID="$(date +%Y%m%dT%H%M%SZ)-${_INSTANCE_ID}"
  _S3_STAGING_PREFIX="tmp/spot_data_weekly/${_RUN_ID}"
  _S3_STAGING="s3://${S3_BUCKET}/${_S3_STAGING_PREFIX}"

  echo "  S3 staging: ${_S3_STAGING}/"
}

# ── Staging teardown (config-I7442, superseding config-I7396) ───────────────
# The staging prefix holds the ONLY full copy of a stage's stdout/stderr:
# run_ssm() below points SSM's --output-key-prefix at
# "${_S3_STAGING_PREFIX}/ssm-output", and GetCommandInvocation returns just
# the first 24 KB — on any long-running stage the failing tail exists
# nowhere else. Measured 2026-08-15 (config-I7396 → I7442): the
# PredictorBacktest stage died after ~4h, printed an error naming that exact
# S3 prefix as "the full remote log", and the SAME exit path then ran
# `aws s3 rm "$S3_STAGING" --recursive` four lines later — the prefix was
# empty by the time anyone read the message. That defect was fixed locally
# in crucible-backtester (#675) with a retain-on-failure branch, but
# mirroring 55 lines of bash into every sibling `_spot_common.sh`/monolith
# is exactly what policy-shared-code forbids at second adoption. This now
# goes through `krepis.spot_evidence teardown`, the one chokepoint every
# launcher in the fleet calls: it copies the staging prefix to
# `_spot_evidence/<slug>/<date>/<run-id>/` FIRST and deletes staging only if
# that copy succeeded (a clean run deletes with nothing retained, so no
# green run accrues storage), and sweeps stale `tmp/spot_<slug>/` and
# `_spot_evidence/<slug>/` prefixes on every teardown.
teardown_staging() {
  local _exit_code="$1"
  if [ -z "${_S3_STAGING:-}" ]; then
    return 0
  fi
  if "$LIB_PYTHON" -m krepis.spot_evidence teardown \
      --staging "$_S3_STAGING" \
      --slug "$_SPOT_NAME" \
      --exit-code "$_exit_code"; then
    return 0
  fi
  # The chokepoint is unreachable on this box — a krepis pin older than the
  # release carrying `spot_evidence`, or a broken interpreter. RETAIN rather
  # than fall back to `aws s3 rm`: an unswept prefix is a cost bounded by the
  # next teardown's sweep, whereas re-deleting here would repeat I7442. This
  # is also what makes the merge order safe — this change is correct on a
  # box whose krepis pin has not yet been bumped. `return 0` always: a
  # janitor that changes the trap's own exit status would mask the
  # workload's real failure.
  echo "  spot_evidence: chokepoint unavailable via $LIB_PYTHON — S3 staging RETAINED at $_S3_STAGING/ (not deleted)" >&2
  echo "    Full stage stdout/stderr: $_S3_STAGING/ssm-output/" >&2
  return 0
}

# ── Cleanup (instance + S3 staging) ──────────────────────────────────────────

cleanup() {
  local _exit_code="${1:-0}"
  local _keep="${KEEP_INSTANCE:-0}"
  if [ "$_keep" = "1" ]; then
    # launch-only handoff: no workload has run on this box — the caller's
    # own downstream stage is what will succeed or fail later. Evidence
    # teardown here must not read a workload exit status that was never
    # evaluated (config-I7442).
    teardown_staging 0
    echo "  launch-only: instance $_INSTANCE_ID left running (SF-owned); staging teardown reported above."
    return 0
  fi
  if [ -n "$_INSTANCE_ID" ]; then
    echo ""
    echo "==> Terminating spot instance $_INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids "$_INSTANCE_ID" --region "$AWS_REGION" --output text > /dev/null 2>&1 || true
  fi
  teardown_staging "$_exit_code"
  [ -n "$_INSTANCE_ID" ] && echo "  Instance terminated."
  return 0
}

# ── Spot failure classification ──────────────────────────────────────────────

_spot_failure_reason() {
  local rc="$1"
  if [ "$rc" -eq 64 ]; then echo "launch-capacity-exhausted"; return 0; fi
  [ -z "$_INSTANCE_ID" ] && return 1
  # See alpha-engine-config-I7009 — migrated off the exit-code contract to --json.
  local _decide_json="" _decide_rc=0
  _decide_json="$("$LIB_PYTHON" -m krepis.ec2_spot relaunch-decision \
    --instance-id "$_INSTANCE_ID" \
    --region "$AWS_REGION" \
    --attempt "$SPOT_ATTEMPT" \
    --max-attempts "$MAX_SPOT_ATTEMPTS" \
    ${SF_EXECUTION_TIMEOUT:+--sf-execution-timeout "$SF_EXECUTION_TIMEOUT" --per-attempt-seconds "$MAX_RUNTIME_SECONDS"} \
    --json \
    2>/dev/null)" || _decide_rc=$?
  [ "$_decide_rc" -eq 0 ] || return 1   # CLI failed to answer -> treat as hold (do not relaunch)
  local _relaunch=""
  _relaunch="$(printf '%s' "$_decide_json" | "$LIB_PYTHON" -c 'import json,sys; print("1" if json.load(sys.stdin).get("relaunch") else "0")')"
  echo "  spot relaunch-decision (attempt $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS): $_decide_json" >&2
  [ "$_relaunch" = "1" ] || return 1
  echo "confirmed-reclaim${_decide_json:+ ($_decide_json)}"
}

# ── EXIT handler (classification + cleanup + optional relaunch) ──────────────

on_exit() {
  local rc=$?
  local reason=""
  if [ "$rc" -ne 0 ]; then
    reason="$(_spot_failure_reason "$rc")" || reason=""
  fi
  cleanup "$rc"
  if [ "$rc" -ne 0 ] && [ -n "$reason" ] && [ "$SPOT_ATTEMPT" -lt "$MAX_SPOT_ATTEMPTS" ]; then
    aws cloudwatch put-metric-data \
      --namespace "AlphaEngine" \
      --metric-name "SpotInterruptionRetry" \
      --dimensions "Process=${_PROCESS_NAME}" \
      --value 1 --unit "Count" \
      --region "$AWS_REGION" 2>/dev/null || true
    echo "" >&2
    echo "==> Spot interruption (reason=$reason) on attempt $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS — relaunching in ${SPOT_RETRY_BACKOFF_SECONDS}s..." >&2
    sleep "$SPOT_RETRY_BACKOFF_SECONDS"
    trap - EXIT
    SPOT_ATTEMPT=$((SPOT_ATTEMPT + 1)) exec bash "$0" ${ORIG_ARGS[@]+"${ORIG_ARGS[@]}"}
  fi
  if [ "$rc" -ne 0 ] && [ -n "$reason" ]; then
    echo "ERROR: spot interruption (reason=$reason) persisted across all $MAX_SPOT_ATTEMPTS attempt(s) — giving up." >&2
  fi
  exit "$rc"
}

# ── SSM agent wait ───────────────────────────────────────────────────────────

wait_ssm_agent() {
  echo "==> Waiting for SSM agent to come Online..."
  for i in $(seq 1 36); do
    local ping
    ping=$(aws ssm describe-instance-information \
      --filters "Key=InstanceIds,Values=$_INSTANCE_ID" \
      --query 'InstanceInformationList[0].PingStatus' \
      --output text --region "$AWS_REGION" 2>/dev/null || true)
    if [ "$ping" = "Online" ]; then
      echo "  SSM agent Online."
      return 0
    fi
    if [ "$i" -eq 36 ]; then
      echo "ERROR: SSM agent not Online after 180s (instance $_INSTANCE_ID)" >&2
      exit 1
    fi
    sleep 5
  done
}

# ── SSM dispatch ─────────────────────────────────────────────────────────────

run_ssm() {
  local description="$1" script="$2" timeout_s="${3:-3600}"
  printf '%s' "$script" | "$LIB_PYTHON" -m krepis.ssm_dispatcher run \
    --instance-id "$_INSTANCE_ID" \
    --description "${_PROCESS_NAME}: $description" \
    --timeout "$timeout_s" \
    --output-bucket "$S3_BUCKET" \
    --output-key-prefix "${_S3_STAGING_PREFIX}/ssm-output" \
    --region "$AWS_REGION" \
    --diagnostics-bucket "$S3_BUCKET" \
    --diagnostics-prefix "_spot_diagnostics/ae-data" \
    --script-stdin
}

# ── Config staging ───────────────────────────────────────────────────────────

stage_config() {
  local src="$1" dest_key="${2:-config.yaml}"
  echo "==> Staging ${src} → ${_S3_STAGING}/${dest_key}"
  aws s3 cp "$src" "${_S3_STAGING}/${dest_key}" --region "$AWS_REGION" --quiet
}

# ── Bootstrap (watchdog + python + clone + config) ───────────────────────────

bootstrap_spot() {
  echo "==> Bootstrapping spot (watchdog, python, clone, config)..."
  # Rendered by krepis.spot_bootstrap (alpha-engine-config-I6922) rather than
  # built as an inline heredoc. That module is the fleet's single canonical
  # source for the watchdog unit, the interpreter install and the clone/config
  # shape this heredoc used to hand-carry (nousergon-data#1294, #1296;
  # crucible-predictor#461, #462, #463 were the same defects reaching the
  # sibling copy hours to days later). Region and branch are passed as
  # LAUNCHER-SIDE LITERALS rather than left for the remote shell to expand —
  # the predictor's REPO_URL/BRANCH class of bug (crucible-predictor#463: a
  # value interpolated into the heredoc but never actually exported, so the
  # remote expansion silently resolved to an empty string) cannot happen when
  # the value is baked into the rendered script instead of read from the
  # remote environment. `--region us-east-1` mirrors this heredoc's prior
  # hardcode exactly (it never read the outer $AWS_REGION); tests/
  # test_spot_bootstrap_invariants.py and its siblings assert the rendered
  # output stays byte-for-byte equivalent on the parts that must not drift.
  #
  # `--max-runtime-seconds` is a GAINED guarantee, not a restored one
  # (alpha-engine-config-I7372): neither this function nor the heredoc it
  # replaced ever armed a hard-timeout timer — the pre-cutover copy installed
  # the `ec2-spot-watchdog` unit only, and MAX_RUNTIME_SECONDS was purely the
  # SSM command budget plus the `--per-attempt-seconds` input to
  # relaunch-decision. The unit answers "the SSM agent died"; the timer
  # answers "the workload itself hung", and only the retired
  # spot_data_weekly.sh monolith carried the second one. krepis-PR152 was
  # unmerged when #1388 was written, so the argument did not exist then.
  # spot_launch() hard-exits when MAX_RUNTIME_SECONDS is empty and always runs
  # before this function, so the value here is guaranteed non-empty.
  local _script
  _script="$("$LIB_PYTHON" -m krepis.spot_bootstrap render \
    --repo-url https://github.com/nousergon/nousergon-data.git \
    --checkout /home/ec2-user/data \
    --branch "${BRANCH:-main}" \
    --region us-east-1 \
    --max-runtime-seconds "$MAX_RUNTIME_SECONDS" \
    --export "S3_STAGING=${_S3_STAGING}" \
    --config-copy config.yaml:/home/ec2-user/alpha-engine-config/data/config.yaml:/home/ec2-user/alpha-engine-config)"
  run_ssm "bootstrap" "$_script" 300
  echo "  Bootstrap complete."
}

# ── Dependency installation ──────────────────────────────────────────────────

install_deps() {
  # Rendered by krepis.spot_bootstrap.render_install_deps (alpha-engine-config-
  # I7372) rather than carried as a heredoc. #1388 collapsed the BOOTSTRAP onto
  # the shared renderer and left this heredoc standing, and this is the step
  # where the interpreter actually matters: it carried
  #
  #     command -v python3.12 >/dev/null && PY=python3.12 || PY=python3
  #
  # so a box that failed to install 3.12 resolved requirements.txt against the
  # AMI's python3 — different wheels, silently. The renderer's copy has no
  # fallback at all (render_bootstrap has already asserted the interpreter),
  # and is otherwise the same script this heredoc had converged on: keep the
  # pip log, fail on a dropped extra, report `pip check` non-fatally.
  # config-I6949 / config-I6963 are the failures those three lines encode.
  echo "==> Installing python deps..."
  local _script
  _script="$("$LIB_PYTHON" -m krepis.spot_bootstrap render-deps \
    --repo-url https://github.com/nousergon/nousergon-data.git \
    --checkout /home/ec2-user/data \
    --region us-east-1)"
  run_ssm "deps" "$_script" 600
  echo "  Deps installed."
}

# ── Gitleaks binary + DLP preflight gate ─────────────────────────────────────

install_gitleaks_dlp() {
  # krepis.session_dlp (LLMClient's DLP hook) shells out to the gitleaks
  # BINARY on every LLM call and fails CLOSED when it is absent — a Go binary
  # a wheel cannot carry, so `pip install -r requirements.txt` in install_deps()
  # above never provides it. Every spot this repo boots makes LLM calls
  # (flow-doctor diagnosis), so every one of them fails closed on first call
  # without this (alpha-engine-config-I10370).
  #
  # Pin mirrors the fleet standard exactly — same version, same sha256, same
  # release asset — so it moves in lockstep rather than being re-derived per
  # substrate: nous-ergon-ops/alpha-engine-dashboard/infrastructure/
  # provisioning/bootstrap.sh, nous-ergon-ops/infrastructure/cloudformation/
  # crucible-v2.yaml (GitleaksVersion/GitleaksSha256X64), and
  # alpha-engine-config/infrastructure/groom_spot_bootstrap.sh. Bump all four
  # together, never independently.
  #
  # `python -m krepis.session_dlp preflight` (krepis-PR211) is run as a BOOT
  # GATE, not deferred to the first LLM call — the same shape
  # crucible-v2.yaml's inline preflight already uses. A gate that cannot pass
  # aborts the boot (fail-closed stays; this makes the binary present so
  # fail-closed no longer fires on every boot). KREPIS_DLP_DISABLED=1 is
  # explicitly not read anywhere here — see the non-inferable gotcha in
  # alpha-engine-config-I10370: it would silence the fleet's only DLP control.
  echo "==> Installing gitleaks + running DLP preflight gate..."
  local _script
  read -r -d '' _script <<'GITLEAKS_DLP_SCRIPT' || true
set -euo pipefail
GITLEAKS_VERSION=8.30.1
GITLEAKS_SHA256=551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb
if ! command -v gitleaks >/dev/null 2>&1; then
  echo "installing gitleaks ${GITLEAKS_VERSION}..."
  curl -fsSL -o /tmp/gitleaks.tar.gz \
    "https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz"
  echo "${GITLEAKS_SHA256}  /tmp/gitleaks.tar.gz" | sha256sum -c -
  sudo tar -xzf /tmp/gitleaks.tar.gz -C /usr/local/bin gitleaks
  sudo chmod +x /usr/local/bin/gitleaks
  rm -f /tmp/gitleaks.tar.gz
fi
command -v gitleaks >/dev/null 2>&1 || { echo "FATAL: gitleaks binary unavailable after install (fail-closed)" >&2; exit 1; }
command -v python3.12 >/dev/null 2>&1 || { echo "FATAL: python3.12 not found — cannot run the DLP preflight gate" >&2; exit 1; }
PYTHON_BIN=python3.12
if ! "$PYTHON_BIN" -m krepis.session_dlp preflight; then
  echo "FATAL: DLP preflight failed (gitleaks binary/config not ready) — refusing to proceed, same fail-closed posture as the proxy's own missing-config guard" >&2
  exit 1
fi
echo "DLP preflight OK."
GITLEAKS_DLP_SCRIPT
  run_ssm "gitleaks-dlp" "$_script" 300
  echo "  Gitleaks installed, DLP preflight OK."
}

# ── Utilities ────────────────────────────────────────────────────────────────

print_banner() {
  local title="$1"
  echo ""
  echo "═══════════════════════════════════════════════════════════════"
  echo "  ${title}"
  echo "═══════════════════════════════════════════════════════════════"
}

emit_heartbeat() {
  aws cloudwatch put-metric-data \
    --namespace "AlphaEngine" \
    --metric-name "Heartbeat" \
    --dimensions "Process=${_PROCESS_NAME}" \
    --value 1 --unit "Count" \
    --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
    && echo "Heartbeat emitted: ${_PROCESS_NAME}" \
    || echo "WARNING: Failed to emit heartbeat (non-fatal)"
}
