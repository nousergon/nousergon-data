#!/usr/bin/env bash
# infrastructure/spot_drift_detection.sh — Feature + prediction drift check on spot EC2.
#
# Launches a c5.large spot, clones alpha-engine-data AND alpha-engine-predictor
# (drift_detector reads predictor weights + data slim cache), runs
# `python -m monitoring.drift_detector --alert`, emits a heartbeat on success,
# and self-terminates.
#
# Origin: moved off ae-dashboard (t3.micro) as part of the 2026-04-16
# spot-migration push. DriftDetection is lightweight (~5 min workload), so
# the ~7 min spot bootstrap is disproportionate cost-wise. Accepting that
# in exchange for removing the heavy alpha-engine-data `.venv` from the
# micro entirely. Roadmap P2: consider bundling onto the PredictorTraining
# spot since drift depends on predictor weights produced by that step.
#
# Non-blocking: drift failures should not halt the Saturday pipeline — the
# SF's DriftDetection step has a Catch → Backtester so an error here only
# fires an alert. This launcher still exits non-zero on failure so the
# SF receives a signal; the SF's non-blocking catch handles the rest.
#
# Usage:
#   ./infrastructure/spot_drift_detection.sh
#   ./infrastructure/spot_drift_detection.sh --smoke-only
#   ./infrastructure/spot_drift_detection.sh --instance-type c5.xlarge
#   ./infrastructure/spot_drift_detection.sh --branch my-branch

set -euo pipefail

export HOME="${HOME:-/home/ec2-user}"

# Secrets resolve from SSM at Python startup via
# alpha_engine_lib.secrets.get_secret(); the spot's IAM profile
# (alpha-engine-executor-profile) grants ssm:GetParameter on /alpha-engine/*.
# No .env is sourced anywhere in this script post the 2026-05-14 .env-deprecation arc.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Spot configuration ──────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-us-east-1}"
BRANCH="${BRANCH:-main}"
INSTANCE_TYPE="c5.large"
AMI_ID="ami-0c421724a94bba6d6"
KEY_NAME="alpha-engine-key"
KEY_FILE="$HOME/.ssh/alpha-engine-key.pem"
SECURITY_GROUP="sg-03cd3c4bd91e610b0"
SUBNET_ID="subnet-e07166ec"
IAM_PROFILE="alpha-engine-executor-profile"
# Spot-side watchdog budget: DriftDetection workload is ~5 min; 30 min
# of headroom covers pip install + preflight + retries. If the workload
# legitimately needs longer, bump this — don't silently rely on the
# orphan reaper.
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-1800}"

RUN_MODE="full"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke-only) RUN_MODE="smoke-only"; shift ;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
        --branch) BRANCH="$2"; shift 2 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════"
echo "  DriftDetection Spot Run — $(date +%Y-%m-%d)"
echo "═══════════════════════════════════════════════════════════════"
echo "  Instance type : $INSTANCE_TYPE"
echo "  Run mode      : $RUN_MODE"
echo ""

# ── Preflight ───────────────────────────────────────────────────────────────
if [ ! -f "$KEY_FILE" ]; then
    echo "ERROR: SSH key not found at $KEY_FILE"
    exit 1
fi
# Note: alpha-engine-lib was flipped public 2026-05-03; spot installs it
# directly from git+https with no auth required.

# ── Launch spot ──────────────────────────────────────────────────────────────
echo "==> Requesting spot instance ($INSTANCE_TYPE)..."
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP" \
    --subnet-id "$SUBNET_ID" \
    --iam-instance-profile Name="$IAM_PROFILE" \
    --instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}' \
    --instance-initiated-shutdown-behavior terminate \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=alpha-engine-drift-$(date +%Y%m%d)}]" \
    --region "$AWS_REGION" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo "  Instance ID: $INSTANCE_ID"

cleanup() {
    echo ""
    echo "==> Terminating spot instance $INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" --output text > /dev/null 2>&1 || true
    echo "  Instance terminated."
}
trap cleanup EXIT

echo "==> Waiting for instance to enter running state..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"

PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text \
    --region "$AWS_REGION")

if [ "$PUBLIC_IP" = "None" ] || [ -z "$PUBLIC_IP" ]; then
    echo "ERROR: Instance has no public IP. Check subnet/VPC configuration."
    exit 1
fi

echo "  Public IP: $PUBLIC_IP"

# ── Wait for SSH ─────────────────────────────────────────────────────────────
echo "==> Waiting for SSH to become available..."
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"
for i in $(seq 1 30); do
    if ssh $SSH_OPTS -i "$KEY_FILE" ec2-user@"$PUBLIC_IP" "echo ok" 2>/dev/null; then
        echo "  SSH ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: SSH not available after 150s"
        exit 1
    fi
    sleep 5
done

run_remote() {
    ssh $SSH_OPTS -i "$KEY_FILE" ec2-user@"$PUBLIC_IP" "$@"
}

# ── Spot-side watchdog ──────────────────────────────────────────────────────
# Dispatcher-side `trap cleanup EXIT` only fires when THIS bash script
# exits cleanly. If the dispatcher SSM command is cancelled, the
# dispatcher EC2 is stopped mid-run, or the shell gets SIGKILLed, the
# trap never runs and the spot orphans until manually terminated.
# Installs a transient systemd timer on the spot that fires
# shutdown -h now after MAX_RUNTIME_SECONDS regardless of dispatcher
# state. AL2023's InstanceInitiatedShutdownBehavior=terminate makes
# the shutdown a termination (matches run-instances flag above).
echo "==> Installing spot-side watchdog (${MAX_RUNTIME_SECONDS}s = $((MAX_RUNTIME_SECONDS / 60)) min)..."
run_remote "sudo systemd-run --on-active=${MAX_RUNTIME_SECONDS} --unit=alpha-engine-watchdog --description='alpha-engine spot hard-timeout' /sbin/shutdown -h now"

# ── Bootstrap python + git ───────────────────────────────────────────────────
echo "==> Bootstrapping spot environment..."
run_remote bash -s <<'BOOTSTRAP'
set -euo pipefail
sudo dnf install -y -q python3.12 python3.12-pip python3.12-devel git gcc 2>/dev/null || \
    sudo dnf install -y -q python3 python3-pip python3-devel git gcc
mkdir -p ~/.ssh
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
BOOTSTRAP

# ── Clone alpha-engine-data + alpha-engine-predictor ─────────────────────────
# drift_detector lives in alpha-engine-data/monitoring/ but imports from
# alpha-engine-predictor via PYTHONPATH. Both must be present.
echo "==> Cloning alpha-engine-data + alpha-engine-predictor (branch: $BRANCH)..."
for REPO in alpha-engine-data alpha-engine-predictor; do
    run_remote "git clone --depth 1 --branch $BRANCH https://github.com/cipher813/$REPO.git /home/ec2-user/$REPO"
done

# ── Install dependencies ─────────────────────────────────────────────────────
# alpha-engine-lib is public; pip installs it from git+https with no auth.
echo "==> Installing Python dependencies..."
run_remote bash -s <<'DEPS'
set -euo pipefail
cd /home/ec2-user/alpha-engine-data

if command -v python3.12 &>/dev/null; then
    PIP="python3.12 -m pip"
else
    PIP="python3 -m pip"
fi

$PIP install --upgrade pip -q
$PIP install -q -r requirements.txt
$PIP install -q 'numpy<2'

echo "Dependencies installed."
DEPS

REMOTE_PYTHON=$(run_remote "command -v python3.12 || command -v python3")
# AWS_REGION/AWS_DEFAULT_REGION re-export: same #241 regression as
# spot_data_weekly.sh — the spot shell no longer sources a .env, so the
# region env vars boto3 + lib preflight depend on must be set explicitly
# from the dispatcher-side $AWS_REGION (set above with us-east-1 fallback).
ENV_SOURCE="export XDG_CACHE_HOME=/tmp; export PYTHONPATH=/home/ec2-user/alpha-engine-predictor; export AWS_REGION=$AWS_REGION; export AWS_DEFAULT_REGION=$AWS_REGION;"

# ── Smoke-only: imports + --help ─────────────────────────────────────────────
if [ "$RUN_MODE" = "smoke-only" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  SMOKE TEST"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<SMOKE
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "==> Smoke: python -m monitoring.drift_detector --help"
$REMOTE_PYTHON -m monitoring.drift_detector --help 2>&1 | head -20
SMOKE

    echo "==> Smoke complete — instance will be terminated."
    exit 0
fi

# ── Full drift detection ────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  DRIFT DETECTION"
echo "═══════════════════════════════════════════════════════════════"

run_remote bash -s <<DRIFT
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "Starting drift_detector at \$(date)"
if ! $REMOTE_PYTHON -m monitoring.drift_detector --alert 2>&1; then
    echo "ERROR: drift_detector failed." >&2
    exit 1
fi
echo "DriftDetection complete at \$(date)"
DRIFT

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  DriftDetection complete. Instance will be terminated."
echo "═══════════════════════════════════════════════════════════════"

aws cloudwatch put-metric-data \
  --namespace "AlphaEngine" \
  --metric-name "Heartbeat" \
  --dimensions "Process=drift-detection" \
  --value 1 --unit "Count" \
  --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
  && echo "Heartbeat emitted: drift-detection" \
  || echo "WARNING: Failed to emit heartbeat (non-fatal)"
