#!/usr/bin/env bash
# infrastructure/spot_morning_enrich.sh — MorningEnrich SF state runner.
# Polygon T+1 close fill on a dedicated spot EC2.
#
# Sources infrastructure/_spot_common.sh for shared spot infrastructure.
#
# Supports:
#   --preflight-only  — boot + preflight, exit 0 (NO fetch/write)
#   --smoke-only      — boot + dry-run, exit 0
#   --instance-type   — override instance type
#
# Usage:
#   ./infrastructure/spot_morning_enrich.sh                         # full run
#   ./infrastructure/spot_morning_enrich.sh --preflight-only
#   ./infrastructure/spot_morning_enrich.sh --smoke-only
#   ./infrastructure/spot_morning_enrich.sh --instance-type c5.xlarge

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_spot_common.sh"

# ── Stage-specific defaults ──────────────────────────────────────────────────
_SPOT_NAME="${_SPOT_NAME:-morning-enrich}"
_SSM_SLUG="${_SSM_SLUG:-spot-morning-enrich}"
_PROCESS_NAME="${_PROCESS_NAME:-morning-enrich}"
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-5400}"

# ── Parse flags ──────────────────────────────────────────────────────────────
MODE="run"
PREFLIGHT_ONLY=0
ORIG_ARGS=("$@")

while [[ $# -gt 0 ]]; do
  case "$1" in
    --preflight-only) PREFLIGHT_ONLY=1; shift ;;
    --smoke-only) MODE="smoke-only"; shift ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    *) echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
  esac
done

[ -n "$INSTANCE_TYPE" ] && INSTANCE_TYPES="$INSTANCE_TYPE"

# Config source (alpha-engine-config/data/config.yaml)
_CONFIG_SRC="/home/ec2-user/alpha-engine-config/data/config.yaml"
if [ ! -f "$_CONFIG_SRC" ]; then
  _CONFIG_SRC="$HOME/Development/alpha-engine-config/data/config.yaml"
fi
if [ ! -f "$_CONFIG_SRC" ]; then
  _CONFIG_SRC="$(cd "$SCRIPT_DIR/../.." && pwd)/config/config.yaml"
fi

# ENV_SOURCE block (interpolated into SSM heredocs)
read -r -d '' _ENV_SOURCE <<'ENV_EOF' || true
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/tmp
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3
export PYTHON_BIN
ENV_EOF

echo "═══════════════════════════════════════════════════════════════"
echo "  MorningEnrich — $(date +%Y-%m-%d)"
echo "═══════════════════════════════════════════════════════════════"
echo "  Instance types: $INSTANCE_TYPES | Subnets: $SUBNETS | Branch: $BRANCH"
echo "  Preflight-only: $PREFLIGHT_ONLY | Attempt: $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS"
echo ""

# ── Launch + wait + config + SSM ─────────────────────────────────────────────
spot_launch
trap on_exit EXIT

aws ec2 wait instance-running --instance-ids "$_INSTANCE_ID" --region "$AWS_REGION"
stage_config "$_CONFIG_SRC" "config.yaml"
wait_ssm_agent
bootstrap_spot
install_deps

# ── Smoke-only ───────────────────────────────────────────────────────────────
if [ "$MODE" = "smoke-only" ]; then
  print_banner "SMOKE TEST"
  run_ssm "smoke" "$(cat <<SMOKE
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data
echo "==> Smoke: import weekly_collector"
\$PYTHON_BIN -c "import weekly_collector; print('import OK')"
echo "==> Smoke: weekly_collector.py --morning-enrich --dry-run"
\$PYTHON_BIN weekly_collector.py --morning-enrich --dry-run 2>&1
SMOKE
)" 1800
  echo "==> Smoke complete."
  exit 0
fi

# ── Preflight-only (Friday shell-run dry path) ───────────────────────────────
if [ "$PREFLIGHT_ONLY" = "1" ]; then
  print_banner "PREFLIGHT-ONLY (NO fetch/write)"
  run_ssm "preflight" "$(cat <<PREFLIGHT
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data
echo "==> weekly_collector --morning-enrich --preflight-only"
if ! \$PYTHON_BIN weekly_collector.py --morning-enrich --preflight-only 2>&1; then
    echo "ERROR: morning-enrich preflight failed" >&2
    exit 1
fi
echo "OK at \$(date) — NO fetch, NO write."
PREFLIGHT
)" 900
  echo "==> Preflight complete."
  exit 0
fi

# ── Morning-enrich run ───────────────────────────────────────────────────────
print_banner "MORNING ENRICH (polygon T+1 fill)"
run_ssm "morning-enrich" "$(cat <<WORKLOAD
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data
echo "==> Starting weekly_collector.py --morning-enrich at \$(date)"
if ! \$PYTHON_BIN weekly_collector.py --morning-enrich 2>&1; then
    echo "ERROR: morning-enrich failed" >&2
    exit 1
fi
echo "MorningEnrich complete at \$(date)"
WORKLOAD
)" "${MAX_RUNTIME_SECONDS}"

emit_heartbeat
echo "==> Morning-enrich complete."
