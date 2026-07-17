"""alpha-engine-data-spot-dispatcher — launch the data-heavy weekday/EOD enrich
workloads on a dedicated ephemeral EC2 spot box (config#1767, Phase 2).

WHY SPOT, NOT ON THE ALWAYS-ON TRADING BOX (config#1767): today the weekday
pre-open pipeline (`step_function_daily.json`: MorningEnrich + MorningArcticAppend)
and the EOD post-close pipeline (`step_function_eod.json`: PostMarketData +
PostMarketArcticAppend) SSM-invoke onto ae-trading (i-018eb3307a21329bf, t3.small
8GB). That ~30-50 min of daily_closes fetch + ArcticDB append fills /tmp, competes
with IB Gateway + the executor daemon, and on 2026-07-05 pushed /tmp to 100% and
failed a manual risk_model run. This Lambda moves the data phase onto a fresh spot
box with a large ephemeral disk; ae-trading stays data-free and reserved for IB
Gateway + the daemon.

Mechanism (mirrors the fleet gold-standard `scheduled-groom-dispatcher/index.py`,
which itself mirrors the Saturday `spot_data_weekly.sh` — SAME two fleet
chokepoints, no lib change):
  1. `nousergon_lib.ec2_spot.launch()` rotates instance_type x subnet on capacity
     error; on SpotCapacityExhausted across all pools we relaunch ON-DEMAND
     (spot=False) so a capacity dip never starves the pre-open enrich the
     predictor reads next.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an ASYNC, detached `ssm send-command` (AWS-RunShellScript) that clones
     alpha-engine-data, builds a venv, and runs the SAME `weekly_collector.py`
     entrypoint the on-trading states ran (e.g. `--morning-enrich`). The box
     self-terminates (InstanceInitiatedShutdownBehavior=terminate + a watchdog).
     The Lambda returns immediately with the command_id — the Step Function polls
     ssm:GetCommandInvocation to a terminal status, exactly like the groom SF.

The box does its Arctic write / S3 read+write via its OWN instance profile
(alpha-engine-executor-profile -> alpha-engine-executor-role), the SAME profile
`spot_data_weekly.sh` uses for the Saturday data spot — so the ArcticDB/S3
credentials already exist on the box; this Lambda passes NONE of them.

FAILURE ISOLATION (config#1767 deliverable #4, LOAD-BEARING): this Lambda is only
the launcher. The fail-OPEN decision lives in the Step Function: a data-spot
launch/run failure must NOT block daemon start (weekday) or reconcile+instance-stop
(EOD). The SF routes a spot failure to the continue path, mirroring the Saturday
`ResearchPredictorParallel` branch-error pattern (record failure as data, do not
hard-fail the pipeline). This Lambda still RAISES on launch failure so the SF's
Catch can convert it to that fail-open branch (a raise is observable; a silent
launched:false is not — same posture as the groom dispatcher).

Managed OUTSIDE CloudFormation (same as scheduled-groom-dispatcher): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect until
the new code + IAM are deployed AND the daily/EOD SFs are re-deployed with the
new states.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid

import boto3
from nousergon_lib import ec2_spot
from nousergon_lib.ec2_spot import SpotCapacityExhausted

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: DATA_SPOT_DISPATCH_ENABLED=false disables the launch without
# deleting the SF states — the SF's CheckDataSpotLaunched -> *Skipped branch
# (same shape as the groom SF's CheckLaunched -> GroomSkipped) handles it as an
# intentional no-op, NOT a failure. Default ON.
DISPATCH_ENABLED = (
    os.environ.get("DATA_SPOT_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config (env-overridable; defaults mirror spot_data_weekly.sh) ──
# c5/c5a/m5 .large for the fetch+append compute; the lib CLI rotates on capacity
# error. Cheap-first order biases pool selection toward price. spot_data_weekly.sh
# uses c5.large for the Saturday data spot — same family here.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "DATA_SPOT_INSTANCE_TYPES", "c5.large,c5a.large,m5.large"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "DATA_SPOT_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("DATA_SPOT_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("DATA_SPOT_KEY_NAME", "alpha-engine-key")
# NO IB port exposure (config#1767 deliverable #3): reuse the standard fleet SG,
# which does not open the IB Gateway port. The data spot only needs egress + SSM.
SECURITY_GROUP = os.environ.get("DATA_SPOT_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# The box's Arctic-write + S3 read/write come from this profile — the SAME one
# spot_data_weekly.sh grants the Saturday data spot (executor role already has
# ArcticDB s3 read/write for enrich paths). Mirrors the Saturday spot role rather
# than minting a new one.
IAM_PROFILE = os.environ.get("DATA_SPOT_IAM_PROFILE", "alpha-engine-executor-profile")
# Large ephemeral disk so daily_closes fetch + ArcticDB append never hit the
# /tmp-100% failure mode that motivated this move (config#1767 gotcha).
VOLUME_SIZE_GB = int(os.environ.get("DATA_SPOT_VOLUME_SIZE_GB", "60"))

DATA_REPO = os.environ.get("DATA_SPOT_REPO", "nousergon/nousergon-data")
DATA_BRANCH = os.environ.get("DATA_SPOT_BRANCH", "main")
# Private config package weekly_collector.py resolves via resolve_experiment_config
# (experiments/reference/data/config.yaml). The spot box mirrors groom-dispatcher:
# read the fleet PAT from SSM (executor role grants alpha-engine/* GetParameter)
# and shallow-clone alpha-engine-config. spot_data_weekly.sh stages config via S3
# from ae-dashboard instead — no dispatcher host with a local clone exists here.
CONFIG_REPO = os.environ.get("DATA_SPOT_CONFIG_REPO", "nousergon/alpha-engine-config")
CONFIG_BRANCH = os.environ.get("DATA_SPOT_CONFIG_BRANCH", "main")
GH_PAT_SSM = os.environ.get(
    "DATA_SPOT_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
# Hard ceiling for the on-box SSM command (matches the bootstrap watchdog). Sized
# above the observed ~50 min enrich + ~38 min append tail with headroom.
MAX_RUNTIME_SECONDS = int(os.environ.get("DATA_SPOT_MAX_RUNTIME_SECONDS", "7200"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("DATA_SPOT_SSM_ONLINE_BUDGET_SEC", "300"))
CW_LOG_GROUP = os.environ.get("DATA_SPOT_CW_LOG_GROUP", "/alpha-engine/data-spot")

# The five data-phase workloads this dispatcher can run, mapped to the EXACT
# weekly_collector.py invocation the on-trading SF states ran (unchanged args =
# unchanged M0 data contract: same paths/schemas). Any other value is rejected.
_WORKLOADS: dict[str, str] = {
    # weekday pre-open (was step_function_daily.json MorningEnrich)
    "morning-enrich": (
        "python weekly_collector.py --morning-enrich "
        "--skip-chronic-heal --skip-arctic-append"
    ),
    # weekday pre-open (was step_function_daily.json MorningArcticAppend)
    "morning-arctic-append": "python weekly_collector.py --morning-arctic-append",
    # EOD post-close (was step_function_eod.json PostMarketData)
    "post-market-data": (
        "python weekly_collector.py --daily --skip-arctic-append"
    ),
    # EOD post-close (was step_function_eod.json PostMarketArcticAppend)
    "post-market-arctic-append": (
        "python weekly_collector.py --daily-arctic-append"
    ),
    # alpha-engine-config-I2717: standalone daily data-heal, EventBridge-triggered
    # ~09:00 UTC weekdays (alpha-engine-daily-heal rule) — was inline in preopen's
    # MorningArcticAppend (universe-gap self-heal head) + the weekday SF's own
    # on-trading ChronicGapSelfHeal state, both REMOVED from
    # step_function_daily.json entirely. Runs off the preopen critical path with
    # a much bigger heal timeout budget (see weekly_collector._run_daily_heal).
    "daily-heal": "python weekly_collector.py --daily-heal",
}
# Defense-in-depth: the workload key is SF-config-controlled, not raw user input,
# but the value is embedded verbatim into the SSM shell command, so pin it to a
# strict allowlist regex too (rules out shell-metacharacter injection outright).
_WORKLOAD_RE = re.compile(r"^[a-z][a-z-]{0,63}$")


def _resolve_workload(event: dict) -> tuple[str, str]:
    """Pull the workload key from the SF input; unknown/malformed RAISES (a
    mis-wired SF state must fail loud, not silently run the wrong collector)."""
    w = str(event.get("workload") or "").strip()
    if not _WORKLOAD_RE.match(w) or w not in _WORKLOADS:
        raise ValueError(
            f"unknown data-spot workload {w!r} — expected one of {sorted(_WORKLOADS)}"
        )
    return w, _WORKLOADS[w]


def _bootstrap_command(workload: str, collector_cmd: str, run_token: str) -> str:
    """The async SSM RunShellScript body: install runtime, clone alpha-engine-data
    + alpha-engine-config (private config.yaml), build the venv, run the collector,
    self-terminate.

    Runs as root on the box. Mirrors spot_data_weekly.sh's bootstrap: a spot-side
    hard-timeout watchdog (so a dispatcher-side failure can never orphan the box),
    python3.12 + git, shallow clones, a requirements.txt venv with the numpy<2
    pin the fleet's pyarrow is compiled against, then the collector. The box
    self-terminates on completion (InstanceInitiatedShutdownBehavior=terminate).
    """
    log = f"/var/log/data-spot-{workload}.log"
    s3_log = (
        f"s3://alpha-engine-research/_ssm_logs/data-spot/{workload}/"
        f"$(date -u +%Y-%m-%d)/$(hostname)-$(date -u +%H%M%S)-{run_token}.log"
    )
    return f"""set -uo pipefail
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/home/ec2-user/.cache
export AWS_REGION={REGION}
export AWS_DEFAULT_REGION={REGION}
export FLOW_DOCTOR_ENABLED=1
export ALPHA_ENGINE_DEPLOYED=1
export ALPHA_ENGINE_EXPERIMENT_ID=reference
fail() {{ echo "[data-spot-prelude] FATAL: $1"; aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true; shutdown -h now; exit 1; }}
# Spot-side hard-timeout watchdog: shuts the box down after MAX_RUNTIME_SECONDS
# regardless of dispatcher state (mirrors spot_data_weekly.sh's watchdog). A
# dispatcher-side failure after send-command can never leave this box orphaned.
systemd-run --on-active={MAX_RUNTIME_SECONDS} --unit=alpha-engine-data-watchdog \\
  --description='alpha-engine data-spot hard-timeout' /sbin/shutdown -h now || true
dnf install -y -q python3.12 python3.12-pip python3.12-devel git gcc >/dev/null 2>&1 \\
  || dnf install -y -q python3 python3-pip python3-devel git gcc >/dev/null 2>&1 \\
  || fail "runtime install failed"
command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-data
git clone --depth 1 --branch {DATA_BRANCH} \\
  https://github.com/{DATA_REPO}.git /home/ec2-user/alpha-engine-data || fail "clone failed"
PAT=$(aws ssm get-parameter --name {GH_PAT_SSM} --with-decryption \\
  --query Parameter.Value --output text --region {REGION}) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {CONFIG_BRANCH} \\
  "https://x-access-token:${{PAT}}@github.com/{CONFIG_REPO}.git" \\
  /home/ec2-user/alpha-engine-config || fail "config clone failed"
cd /home/ec2-user/alpha-engine-data
"$PYTHON_BIN" -m venv .venv || fail "venv create failed"
source .venv/bin/activate
pip install --upgrade pip -q || fail "pip upgrade failed"
pip install -q -r requirements.txt || fail "deps install failed"
# numpy<2 pin to match other spot workloads (pyarrow compiled against 1.x).
pip install -q 'numpy<2' || fail "numpy pin failed"
mkdir -p "$(dirname {log})"
set +e
{collector_cmd} 2>&1 | tee -a {log}
rc=${{PIPESTATUS[0]}}
set -e
aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true
[ "$rc" -eq 0 ] || fail "workload {workload} exited $rc"
echo "[data-spot] workload {workload} complete"
"""


def _launch_instance(force_on_demand: bool = False) -> tuple[str, str]:
    """Launch the data spot box; spot first, on-demand fallback on capacity
    exhaustion (a pre-open enrich the predictor reads next must not be starved by
    a spot-capacity dip). Mirrors _launch_instance in scheduled-groom-dispatcher.

    force_on_demand=True skips the spot attempt entirely. Set by the EOD SF's
    bounded retry-on-relaunch (2026-07-14 incident: a data-spot box was
    reclaimed by AWS — Server.SpotInstanceTermination — ~22min into a
    post-market-data run, which the SF now retries once) and, identically
    (config#2542), by the weekday SF's morning-enrich/morning-arctic-append
    retry-on-relaunch. A workload that already lost one box to a spot
    interruption should not gamble on spot again for its one retry attempt —
    the cost delta is a few cents for a sub-hour c5.large-class box,
    negligible against the reconcile-reliability this buys."""
    common = dict(
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        shutdown_behavior="terminate",
        tag_name="alpha-engine-data-spot",
        region=REGION,
    )
    if force_on_demand:
        logger.info(
            "force_on_demand=True (spot-interruption retry) — launching ON-DEMAND directly, skipping spot"
        )
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=False, **common)
        return iid, "on-demand"
    try:
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=True, **common)
        return iid, "spot"
    except SpotCapacityExhausted:
        logger.warning(
            "spot capacity exhausted across all type x subnet pools — relaunching ON-DEMAND"
        )
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=False, **common)
        return iid, "on-demand"


def _wait_ssm_online(instance_id: str) -> None:
    """Block until the instance is running AND its SSM agent registers Online."""
    import time

    ec2 = boto3.client("ec2", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    ec2.get_waiter("instance_running").wait(
        InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60}
    )
    deadline = time.time() + SSM_ONLINE_BUDGET_SEC
    while time.time() < deadline:
        info = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        ).get("InstanceInformationList", [])
        if info and info[0].get("PingStatus") == "Online":
            logger.info("SSM agent Online for %s", instance_id)
            return
        time.sleep(5)
    raise RuntimeError(
        f"SSM agent not Online after {SSM_ONLINE_BUDGET_SEC}s for {instance_id}"
    )


def _send_bootstrap(instance_id: str, workload: str, collector_cmd: str, run_token: str) -> str:
    """Fire the async, detached SSM command that runs the collector + self-terminates."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment=f"data-spot {workload} — config#1767",
        Parameters={
            "commands": [_bootstrap_command(workload, collector_cmd, run_token)],
            # Execution timeout (NOT the start timeout) — without this SSM kills
            # the command at the 3600s default, guillotining the append tail.
            "executionTimeout": [str(MAX_RUNTIME_SECONDS)],
        },
        TimeoutSeconds=600,  # time to START delivering before giving up
        CloudWatchOutputConfig={
            "CloudWatchLogGroupName": CW_LOG_GROUP,
            "CloudWatchOutputEnabled": True,
        },
    )
    return resp["Command"]["CommandId"]


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a just-launched box whose post-launch steps failed.
    Without this the box orphans: it received no bootstrap, so neither the in-script
    watchdog nor the EXIT trap is running to tear it down. Never masks the original
    error (logged, not raised)."""
    try:
        boto3.client("ec2", region_name=REGION).terminate_instances(InstanceIds=[instance_id])
        logger.warning("terminated data-spot box %s after post-launch failure (no orphan)", instance_id)
    except Exception as exc:  # noqa: BLE001 — cleanup; original error re-raises below
        logger.error(
            "FAILED to terminate %s after a post-launch error (%s) — MANUAL cleanup needed",
            instance_id, exc,
        )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Step Function handler — launch the data spot box for one workload.

    `event` carries {"workload": "morning-enrich" | "morning-arctic-append" |
    "post-market-data" | "post-market-arctic-append" | "daily-heal",
    "force_on_demand": bool}. `force_on_demand` (default False) is set by the
    EOD SF's post-interruption retry (2026-07-14 incident) and the weekday
    SF's identical retry (config#2542) so the one retry attempt never gambles
    on spot a second time; the "daily-heal" workload (alpha-engine-config-
    I2717) is invoked directly by its own EventBridge rule (NOT from either
    SF) and omits `force_on_demand`, so it defaults to False (spot-first, no
    retry-budget coupling to either pipeline). Returns, wrapped under a
    `data_spot` key (mirrors the groom dispatcher's `groom` wrap so the SF's
    JSONPath is $.<result>.Payload.data_spot.*):

      {"launched": true, "instance_id", "command_id", "workload", "run_token"}
      or {"launched": false, "reason": "disabled"} under the kill-switch.

    Fail-loud on launch: a launch/SSM error RAISES so the SF's Catch converts it
    to the fail-open continue branch (config#1767 deliverable #4). Any box brought
    up before the error is torn down first so nothing orphans.
    """
    event = event or {}
    workload, collector_cmd = _resolve_workload(event)
    force_on_demand = bool(event.get("force_on_demand", False))

    if not DISPATCH_ENABLED:
        logger.warning("DATA_SPOT_DISPATCH_ENABLED=false — data spot NOT launched")
        return {"data_spot": {"launched": False, "reason": "disabled", "workload": workload}}

    run_token = uuid.uuid4().hex
    instance_id, market = _launch_instance(force_on_demand=force_on_demand)
    logger.info("launched data-spot box %s (%s) for %s", instance_id, market, workload)
    # Once the box is up, ANY failure before the bootstrap command is delivered
    # would orphan it (no watchdog/trap yet). Terminate-on-error so a slow
    # SSM-online or an SSM SendCommand error tears the box down.
    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(instance_id, workload, collector_cmd, run_token)
    except Exception:
        _terminate_instance(instance_id)
        raise
    logger.info(
        "data-spot dispatched: instance=%s market=%s command=%s workload=%s run_token=%s",
        instance_id, market, command_id, workload, run_token,
    )
    return {
        "data_spot": {
            "launched": True,
            "instance_id": instance_id,
            "market": market,
            "command_id": command_id,
            "workload": workload,
            "run_token": run_token,
        }
    }
