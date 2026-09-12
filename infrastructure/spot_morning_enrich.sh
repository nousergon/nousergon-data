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

# Default-init: SF-driven invocations pass zero flags, so the loop body
# above never runs and these would otherwise be unbound under `set -u`
# (config#2949, PR936/PR937 bug class). _spot_common.sh already sets safe
# defaults before this loop runs, but the guard in
# tests/test_shell_arg_parse_default_init.py is branch-path-sensitive per
# file and does not see across the `source` — restate the defaults here,
# matching the ID_ARTIFACT_KEY pattern in spot_data_weekly.sh.
BRANCH="${BRANCH:-main}"
INSTANCE_TYPE="${INSTANCE_TYPE:-}"

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
# Router addressing (alpha-engine-config-I7409) - undeclared before this, krepis defaulted to
# exec_context=laptop and RouterUnresolvable'd every flow-doctor diagnosis call on this box.
export KREPIS_EXEC_CONTEXT=ec2
export KREPIS_LITELLM_PROXY_URL=https://router.nousergon.ai:8443
export KREPIS_ROUTER_CREDENTIAL_SECRET=ROUTER_CONSUMER_DATA
export KREPIS_APPCONFIG_APPLICATION=alpha-engine
export KREPIS_APPCONFIG_CONFIG_PROFILE=llm-model-registry
export KREPIS_APPCONFIG_ENVIRONMENT=production
if ! command -v python3.12 >/dev/null 2>&1; then
    echo "ERROR: python3.12 not found on this spot — bootstrap_spot() installs and asserts it. Refusing to fall back to the AMI python3: requirements.txt is resolved against 3.12 and the wheels differ (alpha-engine-config-I7372)." >&2
    exit 1
fi
PYTHON_BIN=python3.12
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
install_gitleaks_dlp

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

# ── Per-stage output assertion (config-I7214) ────────────────────────────────
# sf-pipeline-policy.md §2.1: assert THIS stage wrote what it declared, at the
# boundary where the fact becomes knowable — not three hours later at the
# pipeline's convergence point, where a miss can no longer be attributed to a
# stage still in flight. Placed AFTER the workload and after both early-exit
# paths above, so the Friday preflight-only and smoke-only runs — which write
# nothing by design — never reach it and never report 43 false misses.
#
# OBSERVE MODE: the CLI exits 0 for every verdict. `|| echo ... >&2` rather
# than `|| true` deliberately — a bare `|| true` would make an unreachable
# assertion indistinguishable from a covered stage, which is the exact silence
# this mechanism exists to remove. Promotion to enforcing is one flag
# (`--enforce`), guarded by tests/test_spot_stage_coverage_assertions.py.
#
# --run-date is explicit ($EXECUTION_RUN_DATE, not $RUN_DATE): this launcher
# never receives RUN_DATE at all, and even where RUN_DATE does exist elsewhere
# in the fleet it is reassigned to the trading day by crucible-backtester's
# infrastructure/_spot_common.sh — a carrier other code rewrites is exactly the
# defect alpha-engine-config-I8155 fixes. EXECUTION_RUN_DATE is exported by
# step_function.json from $.run_date and is never normalized by anything.
"$LIB_PYTHON" -m krepis.stage_coverage assert --stage MorningEnrich --window-start "$_STAGE_WINDOW_START" --run-date "$EXECUTION_RUN_DATE" || echo "WARNING: stage-coverage assertion did not run for MorningEnrich (rc=$?) — observe mode, stage NOT failed (config-I7214)" >&2

echo "==> Morning-enrich complete."
