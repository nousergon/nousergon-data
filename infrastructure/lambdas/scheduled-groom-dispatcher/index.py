"""alpha-engine-scheduled-groom-dispatcher — launch the backlog groom on a
dedicated EC2 spot box on an EventBridge-Scheduler-driven cadence.

WHY SPOT, NOT GITHUB ACTIONS (config#1432): the 2-3×/day FULL grooms run a
~hours-long Claude Code agent. Running them on GitHub-hosted runners in the
PRIVATE `alpha-engine-config` repo burned the org's 2,000 included Actions
minutes (100% used 2026-06-29; public repos are free/unlimited). This Lambda now
launches a capacity-resilient EC2 spot box (~$2/mo) that runs the SAME
`scripts/groom_run.sh` entrypoint the GHA workflow uses, then self-terminates.

Mechanism (mirrors the fleet gold-standard `spot_data_weekly.sh`, reusing both
fleet chokepoints — no lib change):
  1. `nousergon_lib.ec2_spot.launch()` rotates instance_type × subnet on capacity
     error; on SpotCapacityExhausted across all pools we relaunch ON-DEMAND
     (spot=False) so a capacity dip never starves a groom.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an ASYNC, detached `ssm send-command` (AWS-RunShellScript) carrying a
     small prelude: fetch the PAT from SSM, clone alpha-engine-config, then
     `exec infrastructure/groom_spot_bootstrap.sh`. The box self-terminates
     (InstanceInitiatedShutdownBehavior=terminate + a watchdog). The Lambda
     returns immediately — it does NOT babysit the multi-hour run.

The box reads ALL secrets itself from SSM via its instance profile
(alpha-engine-executor-profile → alpha-engine-executor-role, which already has
ssm:GetParameter on /alpha-engine/*), so this Lambda needs NO secret access — it
only needs ec2:RunInstances, iam:PassRole (the executor role), and ssm:SendCommand.

Fail-loud (a scheduled groom IS the deliverable): a launch/SSM failure RAISES so
EventBridge retries + the Lambda error metric + a CloudWatch alarm surface the
miss, rather than silently dropping a pass.

Managed OUTSIDE CloudFormation (same as before): operator-deployed via
`deploy.sh --bootstrap`. Merging the PR has ZERO live effect until the new code +
IAM are deployed AND the GHA `schedule:` crons are disabled (the gated cutover).
"""

from __future__ import annotations

import logging
import os
import re
import time

import boto3
from nousergon_lib import ec2_spot
from nousergon_lib.ec2_spot import SpotCapacityExhausted

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
# Kill-switch: GROOM_DISPATCH_ENABLED=false disables the trigger without deleting
# the EventBridge Scheduler rules. Default ON.
DISPATCH_ENABLED = os.environ.get("GROOM_DISPATCH_ENABLED", "true").lower() == "true"

# ── Spot launch config (env-overridable; defaults mirror spot_data_weekly.sh) ──
# t3/t3a/t2 .medium (4 GB) across all 6 default-VPC subnets; the lib CLI rotates
# on capacity error. Cheap-first type order biases pool selection toward price.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get("GROOM_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium").split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "GROOM_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("GROOM_AMI_ID", "ami-0c421724a94bba6d6")  # Amazon Linux 2023 x86_64
KEY_NAME = os.environ.get("GROOM_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("GROOM_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
IAM_PROFILE = os.environ.get("GROOM_IAM_PROFILE", "alpha-engine-executor-profile")
VOLUME_SIZE_GB = int(os.environ.get("GROOM_VOLUME_SIZE_GB", "40"))  # node + claude-code + repo clones

GROOM_REPO = os.environ.get("GROOM_REPO", "nousergon/alpha-engine-config")
GROOM_BRANCH = os.environ.get("GROOM_BRANCH", "main")
# The BOX reads the PAT via its instance profile (this Lambda does not).
GROOM_GH_PAT_SSM = os.environ.get("GROOM_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat")
# Hard ceiling for the on-box SSM command (matches the bootstrap watchdog). The
# in-run soft budget (~340 min) is the binding stop; this is the backstop.
MAX_RUNTIME_SECONDS = int(os.environ.get("GROOM_MAX_RUNTIME_SECONDS", "21600"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("GROOM_SSM_ONLINE_BUDGET_SEC", "180"))
CW_LOG_GROUP = os.environ.get("GROOM_CW_LOG_GROUP", "/alpha-engine/groom-spot")

_VALID_RUN_MODES = {"full", "sweep"}
_DEFAULT_RUN_MODE = "full"
_VALID_ISSUE_FILTERS = {"default", "high-only"}
_DEFAULT_ISSUE_FILTER = "default"
_DEFAULT_MODEL = "claude-sonnet-5"
# Defense-in-depth allowlist for the model id (embedded verbatim into the SSM
# shell command below) — model ids are Lambda-config-controlled, not raw user
# input, but this is cheap and rules out shell-metacharacter injection outright.
_MODEL_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def _resolve_run_mode(event: dict) -> str:
    """Pull run-mode from the EventBridge Scheduler input; unknown/missing → full."""
    rm = str(event.get("run_mode") or _DEFAULT_RUN_MODE).strip().lower()
    if rm not in _VALID_RUN_MODES:
        logger.warning("unknown run_mode %r — defaulting to %s", rm, _DEFAULT_RUN_MODE)
        rm = _DEFAULT_RUN_MODE
    return rm


def _resolve_issue_filter(event: dict) -> str:
    """Pull issue_filter from the schedule input; unknown/missing → default (Sonnet queue)."""
    f = str(event.get("issue_filter") or _DEFAULT_ISSUE_FILTER).strip().lower()
    if f not in _VALID_ISSUE_FILTERS:
        logger.warning("unknown issue_filter %r — defaulting to %s", f, _DEFAULT_ISSUE_FILTER)
        f = _DEFAULT_ISSUE_FILTER
    return f


def _resolve_model(event: dict) -> str:
    """Pull model from the schedule input; missing/malformed → default (Sonnet 5)."""
    m = str(event.get("model") or _DEFAULT_MODEL).strip()
    if not _MODEL_RE.match(m):
        logger.warning("malformed model %r — defaulting to %s", m, _DEFAULT_MODEL)
        m = _DEFAULT_MODEL
    return m


def _bootstrap_command(run_mode: str, run_url: str, model: str, issue_filter: str) -> str:
    """The async SSM RunShellScript body: fetch PAT, clone config, exec bootstrap.

    Runs as root on the box. The heavy, version-controlled logic lives in the
    repo's infrastructure/groom_spot_bootstrap.sh; this prelude is only the
    minimal clone glue (it needs the PAT before it can clone the private repo).
    Any prelude failure shuts the box down so a botched launch never idles.
    """
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
# SSM RunShellScript runs as root with NO $HOME set; git config/clone need it.
export HOME=/root
fail() {{ echo "[groom-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
# Stock AL2023 ships neither git nor python3.12. Install BEFORE the clone (git is
# needed now; python3.12 gives the groom the same interpreter as the GHA runner —
# mirrors spot_data_weekly.sh's bootstrap, which installs the same set).
dnf install -y -q git python3.12 python3.12-pip >/dev/null 2>&1 \
  || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {GROOM_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {GROOM_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{GROOM_REPO}.git" \
  /home/ec2-user/alpha-engine-config || fail "clone failed"
cd /home/ec2-user/alpha-engine-config
export GROOM_MODEL={model}
export GROOM_ISSUE_FILTER={issue_filter}
exec bash infrastructure/groom_spot_bootstrap.sh --mode {run_mode} --run-url "{run_url}"
"""


def _launch_instance() -> tuple[str, str]:
    """Launch the groom box; spot first, on-demand fallback on capacity exhaustion."""
    common = dict(
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        shutdown_behavior="terminate",
        tag_name="alpha-engine-groom-spot",
        region=REGION,
    )
    try:
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=True, **common)
        return iid, "spot"
    except SpotCapacityExhausted:
        logger.warning(
            "spot capacity exhausted across all type×subnet pools — relaunching ON-DEMAND"
        )
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=False, **common)
        return iid, "on-demand"


def _wait_ssm_online(instance_id: str) -> None:
    """Block until the instance is running AND its SSM agent registers Online."""
    ec2 = boto3.client("ec2", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    ec2.get_waiter("instance_running").wait(
        InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 40}
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


def _send_bootstrap(instance_id: str, run_mode: str, run_url: str, model: str, issue_filter: str) -> str:
    """Fire the async, detached SSM command that runs the groom + self-terminates."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment=f"backlog groom ({run_mode}, {model}, {issue_filter}) — config#1432/#1495",
        Parameters={
            "commands": [_bootstrap_command(run_mode, run_url, model, issue_filter)],
            # Execution timeout (NOT the start timeout) — without this SSM kills the
            # command at the 3600s default, guillotining a multi-hour groom.
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
    """Best-effort terminate of a just-launched box whose post-launch steps
    failed. Without this the box orphans: it received no bootstrap, so neither
    the in-script watchdog nor the EXIT trap (both armed BY the bootstrap) is
    running to tear it down — it idles until manually killed. Never masks the
    original error (logged, not raised)."""
    try:
        boto3.client("ec2", region_name=REGION).terminate_instances(InstanceIds=[instance_id])
        logger.warning("terminated groom box %s after post-launch failure (no orphan)", instance_id)
    except Exception as exc:  # noqa: BLE001 — cleanup; the original error re-raises below
        logger.error(
            "FAILED to terminate %s after a post-launch error (%s) — MANUAL cleanup needed",
            instance_id, exc,
        )


def _launch_groom_spot(run_mode: str, schedule_label: str, model: str, issue_filter: str) -> dict:
    """Launch + bootstrap the groom box. Fail-loud — any error RAISES."""
    if not DISPATCH_ENABLED:
        logger.warning("GROOM_DISPATCH_ENABLED=false — groom spot NOT launched")
        return {"launched": False, "reason": "disabled"}

    instance_id, market = _launch_instance()
    logger.info("launched groom box %s (%s)", instance_id, market)
    # Once the box is up, ANY failure before the bootstrap command is delivered
    # would orphan it (no watchdog/trap yet). Terminate-on-error so a slow
    # SSM-online, an SSM SendCommand error, etc. tears the box down instead of
    # leaving it idling. (A hard Lambda-timeout KILL can't run this — that's why
    # the function timeout is also sized well above the launch+online budget.)
    try:
        _wait_ssm_online(instance_id)
        # IMPORTANT: this string is embedded in the prelude's bash double-quoted
        # `--run-url "..."`, so it MUST NOT contain `$` — the AWS console's normal
        # `$252F` log-group encoding would expand as positional params ($2, $5...)
        # under `set -u` and abort the prelude. Keep it `$`-free.
        run_url = (
            f"https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}"
            f"#logsV2:log-groups (log group {CW_LOG_GROUP}, instance {instance_id})"
        )
        command_id = _send_bootstrap(instance_id, run_mode, run_url, model, issue_filter)
    except Exception:
        _terminate_instance(instance_id)
        raise
    logger.info(
        "groom dispatched: instance=%s market=%s command=%s run_mode=%s model=%s issue_filter=%s schedule=%s",
        instance_id,
        market,
        command_id,
        run_mode,
        model,
        issue_filter,
        schedule_label,
    )
    return {
        "launched": True,
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "run_mode": run_mode,
        "model": model,
        "issue_filter": issue_filter,
    }


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge Scheduler handler — launches the groom spot box on cadence.

    `event` is the schedule's JSON input, e.g. {"run_mode": "full", "model":
    "claude-opus-4-8", "issue_filter": "high-only", "schedule": "0 15 * * *"}.
    `model`/`issue_filter` default to the Sonnet mid-tier queue when absent
    (the two pre-existing Sonnet schedules don't set them).
    """
    event = event or {}
    run_mode = _resolve_run_mode(event)
    model = _resolve_model(event)
    issue_filter = _resolve_issue_filter(event)
    schedule_label = str(event.get("schedule") or "unknown")
    logger.info("scheduled groom trigger: run_mode=%s model=%s issue_filter=%s schedule=%s",
                run_mode, model, issue_filter, schedule_label)
    result = _launch_groom_spot(run_mode, schedule_label, model, issue_filter)
    return {"groom": result}
