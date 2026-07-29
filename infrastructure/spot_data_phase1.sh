#!/usr/bin/env bash
# infrastructure/spot_data_phase1.sh — DataPhase1 SF state runner.
# Full price-cache refresh (weekly_collector.py --phase 1 + prune) on a
# dedicated spot EC2.
#
# Sources infrastructure/_spot_common.sh for shared spot infrastructure.
#
# Supports:
#   --preflight-only  — boot + preflight, exit 0 (NO fetch/write)
#   --smoke-only      — boot + dry-run, exit 0
#   --instance-type   — override instance type
#
# Usage:
#   ./infrastructure/spot_data_phase1.sh                         # full run
#   ./infrastructure/spot_data_phase1.sh --preflight-only
#   ./infrastructure/spot_data_phase1.sh --smoke-only
#   ./infrastructure/spot_data_phase1.sh --instance-type c5.xlarge

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_spot_common.sh"

# ── Stage-specific defaults ──────────────────────────────────────────────────
_SPOT_NAME="${_SPOT_NAME:-data-phase1}"
_SSM_SLUG="${_SSM_SLUG:-spot-data-phase1}"
_PROCESS_NAME="${_PROCESS_NAME:-data-phase1}"
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

_CONFIG_SRC="/home/ec2-user/alpha-engine-config/data/config.yaml"
[ ! -f "$_CONFIG_SRC" ] && _CONFIG_SRC="$HOME/Development/alpha-engine-config/data/config.yaml"
[ ! -f "$_CONFIG_SRC" ] && _CONFIG_SRC="$(cd "$SCRIPT_DIR/../.." && pwd)/config/config.yaml"

read -r -d '' _ENV_SOURCE <<'ENV_EOF' || true
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/tmp
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3
export PYTHON_BIN
ENV_EOF

echo "═══════════════════════════════════════════════════════════════"
echo "  DataPhase1 — $(date +%Y-%m-%d)"
echo "═══════════════════════════════════════════════════════════════"
echo "  Instance types: $INSTANCE_TYPES | Branch: $BRANCH"
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
echo "==> Smoke: import builders.prune_delisted_tickers"
\$PYTHON_BIN -c "from builders import prune_delisted_tickers; print('import OK')"
echo "==> Smoke: weekly_collector.py --phase 1 --dry-run"
\$PYTHON_BIN weekly_collector.py --phase 1 --dry-run 2>&1
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
echo "==> weekly_collector --phase 1 --preflight-only"
if ! \$PYTHON_BIN weekly_collector.py --phase 1 --preflight-only 2>&1; then
    echo "ERROR: phase1 preflight failed" >&2
    exit 1
fi
echo "Phase1 preflight OK at \$(date) — NO fetch, NO write."
PREFLIGHT
)" 900
  echo "==> Preflight complete."
  exit 0
fi

# ── DataPhase1 run (phase1 + prune) ──────────────────────────────────────────
print_banner "DATAPHASE1 (price refresh + prune)"
run_ssm "phase1" "$(cat <<WORKLOAD
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data

echo "==> Starting weekly_collector.py --phase 1 at \$(date)"
if ! \$PYTHON_BIN weekly_collector.py --phase 1 2>&1; then
    echo "ERROR: DataPhase1 failed" >&2
    exit 1
fi
echo "DataPhase1 complete at \$(date)"

echo "==> Starting builders.prune_delisted_tickers at \$(date)"
if ! \$PYTHON_BIN -m builders.prune_delisted_tickers --apply 2>&1; then
    echo "ERROR: prune_delisted_tickers failed" >&2
    exit 1
fi
echo "UniversePrune complete at \$(date)"
WORKLOAD
)" "${MAX_RUNTIME_SECONDS}"

emit_heartbeat
echo "==> DataPhase1 complete."
