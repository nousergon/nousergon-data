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
LIB_PYTHON="${LIB_PYTHON:-/home/ec2-user/alpha-engine-dashboard/.venv/bin/python}"

# Spot-reclaim relaunch (#883)
MAX_SPOT_ATTEMPTS="${MAX_SPOT_ATTEMPTS:-2}"
SPOT_ATTEMPT="${SPOT_ATTEMPT:-1}"
SF_EXECUTION_TIMEOUT="${SF_EXECUTION_TIMEOUT:-}"
SPOT_RETRY_BACKOFF_SECONDS="${SPOT_RETRY_BACKOFF_SECONDS:-20}"

# Per-stage overrides
_SPOT_NAME="${_SPOT_NAME:-data-weekly}"
_SSM_SLUG="${_SSM_SLUG:-spot-data}"
_PROCESS_NAME="${_PROCESS_NAME:-data-weekly}"
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-5400}"

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

# ── Cleanup (instance + S3 staging) ──────────────────────────────────────────

cleanup() {
  local _keep="${KEEP_INSTANCE:-0}"
  if [ "$_keep" = "1" ]; then
    [ -n "$_S3_STAGING" ] && aws s3 rm "$_S3_STAGING" --recursive --quiet 2>/dev/null || true
    echo "  launch-only: instance $_INSTANCE_ID left running (SF-owned); staging cleaned."
    return 0
  fi
  if [ -n "$_INSTANCE_ID" ]; then
    echo ""
    echo "==> Terminating spot instance $_INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids "$_INSTANCE_ID" --region "$AWS_REGION" --output text > /dev/null 2>&1 || true
  fi
  [ -n "$_S3_STAGING" ] && aws s3 rm "$_S3_STAGING" --recursive --quiet 2>/dev/null || true
  [ -n "$_INSTANCE_ID" ] && echo "  Instance terminated; S3 staging cleaned."
  return 0
}

# ── Spot failure classification ──────────────────────────────────────────────

_spot_failure_reason() {
  local rc="$1"
  if [ "$rc" -eq 64 ]; then echo "launch-capacity-exhausted"; return 0; fi
  [ -z "$_INSTANCE_ID" ] && return 1
  local _decide_out _decide_rc
  _decide_out="$("$LIB_PYTHON" -m krepis.ec2_spot relaunch-decision \
    --instance-id "$_INSTANCE_ID" \
    --region "$AWS_REGION" \
    --attempt "$SPOT_ATTEMPT" \
    --max-attempts "$MAX_SPOT_ATTEMPTS" \
    ${SF_EXECUTION_TIMEOUT:+--sf-execution-timeout "$SF_EXECUTION_TIMEOUT" --per-attempt-seconds "$MAX_RUNTIME_SECONDS"} \
    2>/dev/null)"
  _decide_rc=$?
  echo "  spot relaunch-decision (attempt $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS): rc=$_decide_rc ${_decide_out:+[$_decide_out]}" >&2
  [ "$_decide_rc" -eq 0 ] || return 1
  echo "confirmed-reclaim${_decide_out:+ ($_decide_out)}"
}

# ── EXIT handler (classification + cleanup + optional relaunch) ──────────────

on_exit() {
  local rc=$?
  local reason=""
  if [ "$rc" -ne 0 ]; then
    reason="$(_spot_failure_reason "$rc")" || reason=""
  fi
  cleanup
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
  local _spot_env_export
  _spot_env_export="export S3_STAGING=${_S3_STAGING} BRANCH=${BRANCH}"$'\n'
  run_ssm "bootstrap" "${_spot_env_export}$(cat <<'BOOTSTRAP'
set -eo pipefail
export HOME=/home/ec2-user XDG_CACHE_HOME=/tmp AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1

# systemd watchdog (config#2693)
if ! systemctl is-enabled ec2-spot-watchdog 2>/dev/null; then
  cat > /tmp/ec2-spot-watchdog.service <<'UNIT'
[Unit]
Description=EC2 Spot Watchdog
After=amazon-ssm-agent.service
Requires=amazon-ssm-agent.service
[Service]
Type=oneshot
ExecStart=/usr/local/bin/ec2-spot-watchdog.sh
RemainAfterExit=yes
[Install]
WantedBy=multi-user.target
UNIT
  cat > /usr/local/bin/ec2-spot-watchdog.sh <<'WDSH'
#!/usr/bin/env bash
set -euo pipefail
while true; do
  if ! systemctl is-active amazon-ssm-agent >/dev/null 2>&1; then
    sleep 60
    if ! systemctl is-active amazon-ssm-agent >/dev/null 2>&1; then
      shutdown -h now
    fi
  fi
  sleep 60
done
WDSH
  chmod +x /usr/local/bin/ec2-spot-watchdog.sh
  cp /tmp/ec2-spot-watchdog.service /etc/systemd/system/
  systemctl enable ec2-spot-watchdog
  systemctl start ec2-spot-watchdog
fi

command -v python3.12 >/dev/null || { echo "ERROR: python3.12 not found" >&2; exit 1; }

if [ ! -d /home/ec2-user/data/.git ]; then
  rm -rf /home/ec2-user/data
  git clone --depth 1 --branch "${BRANCH:-main}" https://github.com/nousergon/nousergon-data.git /home/ec2-user/data
fi

mkdir -p /home/ec2-user/data/config
aws s3 cp "${S3_STAGING}/config.yaml" "/home/ec2-user/data/config/config.yaml" --region "${AWS_REGION:-us-east-1}" --quiet
BOOTSTRAP
)" 300
  echo "  Bootstrap complete."
}

# ── Dependency installation ──────────────────────────────────────────────────

install_deps() {
  echo "==> Installing python deps..."
  run_ssm "deps" "$(cat <<'DEPS'
set -eo pipefail
export HOME=/home/ec2-user XDG_CACHE_HOME=/tmp AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
cd /home/ec2-user/data
command -v python3.12 >/dev/null && PY=python3.12 || PY=python3
$PY -m pip install --quiet --no-warn-script-location -r requirements.txt 2>&1 | tail -1
DEPS
)" 600
  echo "  Deps installed."
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
