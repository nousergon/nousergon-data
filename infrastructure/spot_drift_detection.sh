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
# Transport: dispatcher→spot communication is via `aws ssm send-command`
# (routed through the lib chokepoint `python -m nousergon_lib.ssm_dispatcher`,
# lib v0.35.0+) — NO ssh / scp / ssh-keyscan and NO port-22 inbound
# dependency. This is the SSH/SCP→SSM migration of config#893's drift
# sibling, mirroring spot_data_weekly.sh #330 / spot_backtest.sh #405 /
# spot_train.sh #168 1:1. The spot pulls everything over HTTPS git-clone
# (lib is public) + its IAM role's S3 grant; the drift workload reads the
# alpha-engine-research bucket directly, so unlike the data/backtest paths
# there is NO private config.yaml to S3-stage — the only dispatcher-side
# S3 use is the SSM stdout-overflow / diagnostics prefix.
#
# Non-blocking: drift failures should not halt the Saturday pipeline — the
# SF's DriftDetection step has a Catch → Backtester so an error here only
# fires an alert. This launcher still exits non-zero on failure so the
# SF receives a signal; the SF's non-blocking catch handles the rest.
#
# Usage:
#   ./infrastructure/spot_drift_detection.sh
#   ./infrastructure/spot_drift_detection.sh --smoke-only
#   ./infrastructure/spot_drift_detection.sh --preflight-only  # boot + read-only preflight, exit 0 (NO scan/fetch/write)
#   ./infrastructure/spot_drift_detection.sh --instance-type c5.xlarge
#   ./infrastructure/spot_drift_detection.sh --branch my-branch
#
# --preflight-only (Friday shell-run dry path, ROADMAP "Friday shell-run —
# per-module dry-path activation" — closes the DriftDetection skip-exception):
# boots the spot for real, clones both repos, installs deps, then runs ONLY a
# read-only preflight and `exit 0` BEFORE `monitoring.drift_detector` is ever
# invoked. Catches bootstrap-class breakage (lib-pin drift, sys.path / sibling-
# clone collision, missing dep, SSM/region env gap) ~12h before the real
# Saturday run, while doing ZERO drift scan, ZERO external API data fetch, and
# ZERO S3/CloudWatch/SNS/config writes.
#
# Substrate: the drift workload binary (`monitoring.drift_detector`) lives in
# alpha-engine-predictor, not this repo, and has no --preflight-only flag of
# its own; this repo's `preflight.py` DataPreflight modes are data-collection
# scoped (daily / morning_enrich / phase1 / phase2) — none maps to drift. So
# per the canonical-lib fallback the preflight here composes the canonical
# `alpha_engine_lib.preflight.BasePreflight` directly (env-vars + S3-bucket
# HEAD — both strictly read-only) plus an import-only smoke of the drift
# module under the same PYTHONPATH the real run uses. No bespoke preflight
# scaffolding is duplicated. PREFLIGHT_ONLY is a MODIFIER, orthogonal to
# RUN_MODE — it only swaps "preflight + drift scan" for "preflight + exit 0".

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
# S3 bucket for SSM stdout-overflow + diagnostics staging (the drift
# workload itself reads/writes the alpha-engine-research bucket directly
# via the lib's DEFAULT_BUCKET — this is only the dispatcher's transport
# scratch namespace). Matches the sibling spot scripts' S3_BUCKET default.
S3_BUCKET="${S3_BUCKET:-alpha-engine-research}"
BRANCH="${BRANCH:-main}"
INSTANCE_TYPE="c5.large"
# Lib CLI rotates across these on capacity error (ec2_spot exit 64 when
# every type × subnet combination is exhausted). c5.large-first list keeps
# the existing cost/perf profile.
INSTANCE_TYPES="${INSTANCE_TYPES:-c5.large,m5.large,c6i.large,c5a.large}"
AMI_ID="ami-0c421724a94bba6d6"      # Amazon Linux 2023 x86_64
# Key-pair name kept ONLY for compatibility with
# nousergon_lib.ec2_spot's --key-name flag — the spot still launches
# with this key associated, but NOTHING in this script SSH's into the
# instance. Communication is via SSM; the key remains as a manual
# break-glass option (operator can `ssh -i ~/.ssh/...pem` only if the
# security group's port-22 inbound rule is temporarily re-opened, which
# it should NOT be in steady state — see ROADMAP L342 PR 5).
KEY_NAME="alpha-engine-key"
SECURITY_GROUP="sg-03cd3c4bd91e610b0"
# All 6 default-VPC subnets across us-east-1{a,b,c,d,e,f}; the lib CLI
# rotates across this list on capacity error. Mirrors spot_data_weekly.sh.
SUBNETS="${SUBNETS:-subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,subnet-c670118d,subnet-7cff7c43,subnet-e07166ec}"
IAM_PROFILE="alpha-engine-executor-profile"
# Lib CLI path: ae-dashboard is the SSM target instance for the Saturday-SF
# spot states; the dispatcher's .venv has nousergon-lib installed (see
# deploy-on-merge.sh in the dashboard repo). Bare `python3` resolves to
# system python which does NOT carry the lib — use the full venv path.
LIB_PYTHON="${LIB_PYTHON:-/home/ec2-user/alpha-engine-dashboard/.venv/bin/python}"
# Spot-side watchdog budget: DriftDetection workload is ~5 min; 30 min
# of headroom covers pip install + preflight + retries. If the workload
# legitimately needs longer, bump this — don't silently rely on the
# orphan reaper.
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-1800}"

RUN_MODE="full"
# PREFLIGHT_ONLY is a MODIFIER, orthogonal to RUN_MODE (mirrors the
# spot_data_weekly.sh #259 / predictor #175 / backtester #224 pattern).
# When set, the drift scan + heartbeat are replaced by a read-only
# preflight + early `exit 0`; no monitoring.drift_detector code path
# (which is the SOLE function doing any S3 read/put_object, SNS publish,
# or CloudWatch emit) is reachable. Initialised before the parse loop
# for `set -u` safety.
PREFLIGHT_ONLY=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke-only) RUN_MODE="smoke-only"; shift ;;
        --preflight-only) PREFLIGHT_ONLY=1; shift ;;
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
echo "  Preflight-only: $PREFLIGHT_ONLY  (1 = boot + read-only preflight + exit 0, NO scan/fetch/write)"
echo ""

# ── Launch spot ──────────────────────────────────────────────────────────────
# INSTANCE_ID / S3_STAGING are read at trap-FIRE time, so they pick up the
# values assigned after a successful launch below; both default empty so
# cleanup is a no-op if we never got that far.
INSTANCE_ID=""
S3_STAGING=""

cleanup() {
    if [ -n "$INSTANCE_ID" ]; then
        echo ""
        echo "==> Terminating spot instance $INSTANCE_ID..."
        aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" --output text > /dev/null 2>&1 || true
        echo "  Instance terminated."
    fi
    [ -n "$S3_STAGING" ] && aws s3 rm "$S3_STAGING" --recursive --quiet 2>/dev/null || true
    return 0
}
trap cleanup EXIT

# Note: alpha-engine-lib was flipped public 2026-05-03; the spot installs it
# directly from git+https with no auth required.

echo "==> Requesting spot instance (lib CLI rotation: types=[$INSTANCE_TYPES], subnets=[$SUBNETS])..."
INSTANCE_ID=$("$LIB_PYTHON" -m nousergon_lib.ec2_spot launch \
    --types "$INSTANCE_TYPES" \
    --subnets "$SUBNETS" \
    --image-id "$AMI_ID" \
    --key-name "$KEY_NAME" \
    --security-group "$SECURITY_GROUP" \
    --iam-profile "$IAM_PROFILE" \
    --name "alpha-engine-drift-$(date +%Y%m%d)" \
    --region "$AWS_REGION")
ec2_spot_rc=$?
if [ "$ec2_spot_rc" -ne 0 ] || [ -z "$INSTANCE_ID" ]; then
    if [ "$ec2_spot_rc" -eq 64 ]; then
        echo "ERROR: capacity exhausted across all instance_type × subnet combinations. Wait + retry, or expand the lists." >&2
    fi
    exit "${ec2_spot_rc:-1}"
fi

echo "  Instance ID: $INSTANCE_ID"

RUN_ID="$(date +%Y%m%dT%H%M%SZ)-${INSTANCE_ID}"
S3_STAGING_PREFIX="tmp/spot_drift_detection/${RUN_ID}"
# S3_STAGING is consumed by cleanup() (declared with the trap above);
# assigning it here arms staging-prefix removal now that the launch
# succeeded. (S3 lifecycle on tmp/ is the belt-and-suspenders if the trap
# never fires.)
S3_STAGING="s3://${S3_BUCKET}/${S3_STAGING_PREFIX}"

echo "==> Waiting for instance to enter running state..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"

# ── Wait for the SSM agent to register ────────────────────────────────────────
# Replaces the old SSH-readiness poll. AL2023 ships the SSM agent; with the
# instance profile's AmazonSSMManagedInstanceCore (in alpha-engine-executor-profile)
# it registers within ~1 min.
echo "==> Waiting for SSM agent to come Online..."
for i in $(seq 1 36); do  # 36 × 5s = 180s budget
    ping=$(aws ssm describe-instance-information \
        --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
        --query 'InstanceInformationList[0].PingStatus' \
        --output text --region "$AWS_REGION" 2>/dev/null || true)
    if [ "$ping" = "Online" ]; then
        echo "  SSM agent Online."
        break
    fi
    if [ "$i" -eq 36 ]; then
        echo "ERROR: SSM agent not Online after 180s (instance $INSTANCE_ID)"
        exit 1
    fi
    sleep 5
done

# ── SSM dispatch primitive (lib chokepoint) ──────────────────────────────────
# run_ssm "<description>" [timeout_seconds] <<HEREDOC ... HEREDOC
#
# Thin wrapper around `python -m nousergon_lib.ssm_dispatcher run` (lib
# v0.35.0+). The lib base64-wraps the script body (read from stdin via the
# `--script-stdin` flag) for AWS-RunShellScript transport, polls
# get-command-invocation, streams StandardOutputContent delta to this
# process's stdout, and propagates the inner script's exit status (0 on
# Success; 1 on Failed/TimedOut/Cancelled). Full stdout/stderr beyond
# SSM's 24KB inline cap is written to the --output-bucket /
# --output-key-prefix for post-mortem.
#
# Stdin-fed by design (mirrors spot_data_weekly.sh #330): the spot script
# bodies contain shell metachars which break the `"$(cat <<HEREDOC ...)"`
# command-substitution pattern the predictor pre-lift run_remote used.
# Reading from stdin keeps the body verbatim.
#
# L394 cascade: --diagnostics-bucket + --diagnostics-prefix activate the
# lib v0.39.0 chokepoint that writes a JSON failure record (status +
# command_id + 4KB stdout/stderr tails + instance_id) to
# s3://${S3_BUCKET}/_spot_diagnostics/ae-data/{YYYY-MM-DD}.json on
# terminal non-Success. Best-effort write inside the lib; the inner SSM
# exit code is preserved. No-op on Success (substrate is failure-only).
run_ssm() {
    local description="$1" timeout_s="${2:-3600}"
    "$LIB_PYTHON" -m nousergon_lib.ssm_dispatcher run \
        --instance-id "$INSTANCE_ID" \
        --description "drift-detection: $description" \
        --timeout "$timeout_s" \
        --output-bucket "$S3_BUCKET" \
        --output-key-prefix "${S3_STAGING_PREFIX}/ssm-output" \
        --region "$AWS_REGION" \
        --diagnostics-bucket "$S3_BUCKET" \
        --diagnostics-prefix "_spot_diagnostics/ae-data" \
        --script-stdin
}

# Each run_ssm step is a fresh SSM shell with a minimal env. The
# .env-deprecation arc deleted the sourced .env, so AWS_REGION /
# AWS_DEFAULT_REGION (which boto3 + alpha_engine_lib.preflight.check_env_vars
# require) are no longer set unless each step's export line sets them.
# Same #247 regression as sibling spot scripts. System is single-region
# us-east-1 (matches this file's own ${AWS_REGION:-us-east-1} defaults).
#
# PYTHON_BIN is set per-block via `command -v python3.12 || command -v
# python3`; PYTHONPATH points at the sibling alpha-engine-predictor clone
# (drift_detector lives in alpha-engine-data/monitoring/ but imports from
# alpha-engine-predictor). AL2023 spots install python3.12 but have no bare
# `python` symlink.
read -r -d '' ENV_SOURCE <<'ENV_EOF' || true
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/tmp
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export PYTHONPATH=/home/ec2-user/alpha-engine-predictor
command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3
export PYTHON_BIN
ENV_EOF

# ── Bootstrap spot: watchdog + python + git + clone both repos ──────────────
# Single SSM call covering: spot-side hard-timeout watchdog,
# python3.12/git install, and clone of BOTH alpha-engine-data and
# alpha-engine-predictor (drift_detector reads predictor weights + data
# slim cache via PYTHONPATH). Watchdog rationale: dispatcher-side
# `trap cleanup EXIT` only fires when THIS script exits cleanly. If the
# dispatcher SSM command is cancelled, the dispatcher EC2 is stopped
# mid-run, or the shell gets SIGKILLed, the trap never runs and the spot
# orphans until manually terminated. systemd-run shuts the box down after
# MAX_RUNTIME_SECONDS regardless of dispatcher state. AL2023's
# InstanceInitiatedShutdownBehavior for spots defaults to terminate.
#
# Repos renamed + moved to the nousergon org 2026-06-15; local checkout
# dirs stay alpha-engine-* (dir-name ≠ repo-name split). Clone the new
# slugs explicitly rather than depending on GitHub's rename/transfer 301
# redirect from the old cipher813 paths.
echo "==> Bootstrapping spot (watchdog, python, clone both repos)..."
run_ssm "bootstrap" 600 <<BOOTSTRAP
set -eo pipefail
${ENV_SOURCE}

# Spot-side hard-timeout watchdog (see bootstrap-step rationale above).
systemd-run --on-active=${MAX_RUNTIME_SECONDS} --unit=alpha-engine-watchdog \
    --description='alpha-engine spot hard-timeout' /sbin/shutdown -h now

dnf install -y -q python3.12 python3.12-pip python3.12-devel git gcc 2>/dev/null || \
    dnf install -y -q python3 python3-pip python3-devel git gcc
echo "Using: \$(\$PYTHON_BIN --version)"

git clone --depth 1 --branch ${BRANCH} https://github.com/nousergon/nousergon-data.git /home/ec2-user/alpha-engine-data
git clone --depth 1 --branch ${BRANCH} https://github.com/nousergon/crucible-predictor.git /home/ec2-user/alpha-engine-predictor
echo "Bootstrap complete: both repos cloned (data + predictor sibling for PYTHONPATH)."
BOOTSTRAP

# ── Install dependencies ─────────────────────────────────────────────────────
# alpha-engine-lib is public; pip installs it from git+https with no auth.
echo "==> Installing Python dependencies..."
run_ssm "deps" 900 <<DEPS
set -eo pipefail
${ENV_SOURCE}
cd /home/ec2-user/alpha-engine-data

\$PYTHON_BIN -m pip install --upgrade pip -q
\$PYTHON_BIN -m pip install -q -r requirements.txt
\$PYTHON_BIN -m pip install -q 'numpy<2'
echo "Dependencies installed."
DEPS

# ── Smoke-only: imports + --help ─────────────────────────────────────────────
if [ "$RUN_MODE" = "smoke-only" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  SMOKE TEST"
    echo "═══════════════════════════════════════════════════════════════"
    run_ssm "smoke" 600 <<SMOKE
set -eo pipefail
${ENV_SOURCE}
cd /home/ec2-user/alpha-engine-data

echo "==> Smoke: python -m monitoring.drift_detector --help"
\$PYTHON_BIN -m monitoring.drift_detector --help 2>&1 | head -20
SMOKE

    echo "==> Smoke complete — instance will be terminated."
    exit 0
fi

# ── Preflight-only (Friday shell-run dry path) ──────────────────────────────
# Closes the DriftDetection skip-exception in ROADMAP "Friday shell-run —
# per-module dry-path activation". Runs ONLY a read-only preflight then
# `exit 0` strictly BEFORE the `run_ssm "drift"` block below —
# `monitoring.drift_detector` (the SOLE code that does ANY S3 get_object/
# put_object of the drift report, SNS publish on alert, and which this
# launcher's CloudWatch put-metric-data heartbeat trails) is therefore
# statically unreachable here. No scan, no external API data fetch, no
# S3/CW/SNS/config mutation — a passed preflight is a healthy outcome, so
# the early exit is 0 (SSM/SF report Success).
#
# The preflight composes the canonical lib substrate directly — NO bespoke
# scaffolding (Brian standing canonical-lib rule):
#   * alpha_engine_lib.preflight.BasePreflight.check_env_vars("AWS_REGION")
#     — the same fail-fast gate the data path uses; AWS_REGION/.._DEFAULT_REGION
#     are exported via ${ENV_SOURCE} below (the #241 .env-deprecation re-export).
#   * BasePreflight.check_s3_bucket() — a HEAD-bucket probe ONLY (read-only;
#     proves the spot's IAM profile + region reach the drift bucket the real
#     run reads predictor weights / slim cache / metrics from).
#   * an import-only smoke of `monitoring.drift_detector` under the exact
#     PYTHONPATH (sibling alpha-engine-predictor clone) the real run uses —
#     this is what actually catches the bootstrap-class breakage a Friday
#     dry path exists for (lib-pin drift, sys.path / sibling-clone collision,
#     a missing/renamed dep). Importing the module runs no scan: the boto3
#     client + drift checks live behind `def main()` / `check_drift()`, gated
#     by `if __name__ == "__main__"`, none of which import triggers.
# DEFAULT_BUCKET in monitoring.drift_detector is "alpha-engine-research"; the
# preflight HEADs that same bucket so a bucket/region/IAM regression fails
# here ~12h early instead of mid-Saturday.
if [ "$PREFLIGHT_ONLY" = "1" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  PREFLIGHT-ONLY: DriftDetection"
    echo "  (boot + read-only preflight + exit 0 — NO scan, NO fetch, NO write)"
    echo "═══════════════════════════════════════════════════════════════"
    run_ssm "preflight-only" 600 <<'PREFLIGHT_ONLY_BLOCK'
set -eo pipefail
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/tmp
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export PYTHONPATH=/home/ec2-user/alpha-engine-predictor
command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3
cd /home/ec2-user/alpha-engine-data

echo "Starting read-only preflight at $(date)"
if ! $PYTHON_BIN - <<'PYEOF'
import sys

from alpha_engine_lib.preflight import BasePreflight

# Read-only canonical preflight: env-vars fail-fast + S3 bucket HEAD.
# "alpha-engine-research" mirrors monitoring.drift_detector.DEFAULT_BUCKET.
pf = BasePreflight("alpha-engine-research")
pf.check_env_vars("AWS_REGION")
pf.check_s3_bucket()
print("preflight: BasePreflight env-vars + S3 HEAD OK (read-only)")

# Import-only smoke of the drift workload under the real PYTHONPATH. This
# imports the module (catching lib-pin / sys.path / missing-dep breakage)
# WITHOUT invoking it: boto3 clients + scan live behind def main() /
# check_drift(), gated by __main__, which an import does not trigger.
import importlib

mod = importlib.import_module("monitoring.drift_detector")
assert hasattr(mod, "main") and hasattr(mod, "check_drift"), (
    "monitoring.drift_detector missing expected entrypoints — "
    "stale clone or API drift"
)
print("preflight: monitoring.drift_detector import OK (no scan invoked)")
sys.exit(0)
PYEOF
then
    echo "ERROR: DriftDetection preflight failed (bootstrap-class breakage caught ~12h before Saturday)." >&2
    exit 1
fi
echo "DriftDetection preflight-only OK at $(date) — NO scan, NO fetch, NO write."
PREFLIGHT_ONLY_BLOCK

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Preflight-only complete (NO scan/fetch/write). Instance will be terminated."
    echo "═══════════════════════════════════════════════════════════════"
    exit 0
fi

# ── Full drift detection ────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  DRIFT DETECTION"
echo "═══════════════════════════════════════════════════════════════"

run_ssm "drift" "$MAX_RUNTIME_SECONDS" <<DRIFT
set -eo pipefail
${ENV_SOURCE}
cd /home/ec2-user/alpha-engine-data

echo "Starting drift_detector at \$(date)"
if ! \$PYTHON_BIN -m monitoring.drift_detector --alert 2>&1; then
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
