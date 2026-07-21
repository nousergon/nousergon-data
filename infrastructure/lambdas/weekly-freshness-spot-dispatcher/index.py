"""alpha-engine-weekly-freshness-spot-dispatcher — launch the Saturday weekly
pipeline's LAUNCHER box on a fresh, ephemeral EC2 spot instead of the
always-on dashboard box (nousergon/alpha-engine-config#2248).

WHY THIS EXISTS (config#2248): all 14 `ne-weekly-freshness-pipeline`
sendCommand/Lambda states that touch an EC2 instance key off
`$.ec2_instance_id`, and that field was a HARDCODED literal
(i-09b539c844515d549 — the always-on dashboard box, which also runs 12 live
services) baked into the SaturdayTrigger EventBridge Input and the Friday
shell-run trigger Lambda. A full disk on that box killed the entire weekly
pipeline — it is a structural single point of failure for the whole SF, not
because it does the heavy lifting (MorningEnrich/DataPhase1/Backtester/etc.
each launch their OWN nested spot via `spot_data_weekly.sh`/`spot_backtest.sh`
and only use the dashboard box as a LAUNCHER), but because the SF has no
launcher of its own — it borrows a persistent, stateful, shared box for that
role every week.

THIS Lambda breaks that coupling: dispatched ONCE per SF execution (from a
new leading Choice/Task pair in step_function.json, after AcquireMutex and
before any of the 14 consumer states), it launches a FRESH ephemeral spot,
clones all four repos the consumer states' `git -C ... pull --ff-only`
commands expect at their dashboard-box paths
(`/home/ec2-user/alpha-engine-{data,config,backtester,dashboard}`), builds
`/home/ec2-user/alpha-engine-dashboard/.venv` (the interpreter
`MorningEnrich`/`DataPhase1`/etc. invoke via
`/home/ec2-user/alpha-engine-dashboard/.venv/bin/python -m
krepis.ssm_log_capture`), and returns the new instance id for the SF to
thread into `$.ec2_instance_id` — so all 14 downstream states work UNCHANGED
(same paths, same venv, same `InstanceIds.$: "$.ec2_instance_id"` reference).

Mechanism (mirrors the fleet `nousergon_lib.spot_dispatch` chokepoint —
`launch_with_fallback` + `wait_ssm_online` + `send_async_command`, same as
`alert-drain-dispatcher`/`ci-watch-dispatcher`/`scheduled-groom-dispatcher`):
  1. `spot_dispatch.launch_with_fallback()` rotates instance_type x subnet on
     capacity error; on SpotCapacityExhausted/SpotQuotaExceededError across
     all pools we relaunch ON-DEMAND — a capacity dip must never sink the
     whole weekly run.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an ASYNC, detached `ssm send-command` that clones the 4 repos +
     builds the dashboard venv, THEN runs the two direct on-box workloads
     this box itself executes (SaturdayHealthCheck/WeeklySubstrateHealthCheck
     are the only 2 of the 14 consumers that run ON this box rather than on a
     nested spot — but those are separate SF states with their own commands;
     this Lambda's bootstrap is ONLY the clone+venv setup those and every
     other consumer state's `git pull` depends on already having succeeded).
     The Lambda returns immediately with instance_id + command_id — it does
     NOT block on the multi-minute clone+venv-build. The SF's own
     WaitForWeeklyFreshnessSpotBootstrap/Check.../Wait polling loop (mirrors
     the existing WaitForMorningEnrich-style idiom used by every other
     sendCommand state in this SF) polls `ssm:getCommandInvocation` to a
     terminal status BEFORE the SF proceeds into CheckShellRun /
     CheckSkipMorningEnrich — so no downstream state can race an incomplete
     bootstrap.

NOT SELF-TERMINATING AFTER ONE WORKLOAD (unlike data-spot-dispatcher's nested
spots): this box IS the launcher for the WHOLE weekly pipeline — it must stay
up for the SF's entire run (TimeoutSeconds 43200 = 12h at the top level). Its
bootstrap arms a `systemd-run --on-active=<seconds>` shutdown watchdog sized
to comfortably exceed that 12h ceiling (WATCHDOG_SECONDS default 46800 = 13h)
as an orphan-prevention backstop only — nothing on the happy path relies on
it firing. `InstanceInitiatedShutdownBehavior=terminate` (set by
spot_dispatch.launch_with_fallback's `shutdown_behavior="terminate"`) so the
watchdog's `shutdown -h now` actually TERMINATES the box, not just stops it.

IAM: reuses `alpha-engine-executor-profile` (-> `alpha-engine-executor-role`,
home repo `alpha-engine`) — the SAME profile `spot_data_weekly.sh` /
`spot_backtest.sh` grant their Saturday spots, and the SAME profile
`data-spot-dispatcher`'s launched box uses. That role already carries
`ec2:RunInstances`/`ec2:CreateTags`/`ec2:DescribeInstances` etc. (see
`scheduled-groom-dispatcher/index.py`'s header — "alpha-engine-executor-role,
which already has..."), which is exactly what THIS box needs to itself launch
the nested spots `spot_data_weekly.sh`/`spot_backtest.sh` create — no new
role, no IAM change outside this Lambda's own execution role
(iam-policy.json).

FAIL-LOUD (mirrors data-spot-dispatcher, NOT alert-drain-dispatcher's clean-
JSON contract): this Lambda is invoked by the Step Function via
`arn:aws:states:::lambda:invoke`, so a launch/SSM failure RAISES — the SF's
own Catch (-> ExtractWeeklyFreshnessSpotDispatchError -> NormalizeFailureContext
-> HandleFailure) converts it into the SAME loud SNS-paged failure path every
other Task state in this SF uses. There is no fail-open branch here (unlike
data-spot-dispatcher's weekday/EOD fail-open posture): the weekly pipeline
cannot run AT ALL without a launcher box, so a dispatch failure must halt the
run loudly, not silently skip 13 downstream states.

ESCAPE HATCH: the SF's new CheckSpotDispatchNeeded Choice (step_function.json,
inserted right after AcquireMutex) skips this Lambda entirely when
`$.ec2_instance_id` is ALREADY present/non-empty on the execution input — the
operator manual-override / partial-redrive-against-an-existing-box path.
`scripts/weekly_sf_rerun.py`'s `rerun_input()` passthrough (unchanged by this
PR) is exactly that path: a watch-rerun's emitted input carries the ORIGINAL
failed execution's `ec2_instance_id` (which THIS Lambda populated on the
original run) verbatim, so a recovery rerun reuses the same still-live spot
rather than paying for a second launch.

Managed OUTSIDE CloudFormation (same as every sibling dispatcher): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect
until the Lambda + IAM are deployed AND the weekly SF is re-deployed with the
new states — see this dispatcher's README for the exact rollout order.
"""

from __future__ import annotations

import logging
import os
import uuid

from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotLaunchError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: disables the launch without deleting the SF states. There is
# deliberately NO fail-open/skip branch downstream of this flag on the SF
# side — flipping it off is an explicit "I will pass ec2_instance_id myself"
# operator action (the CheckSpotDispatchNeeded escape hatch), not a silent
# no-op. Default ON.
DISPATCH_ENABLED = (
    os.environ.get("WEEKLY_SPOT_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config (env-overridable; defaults mirror spot_data_weekly.sh /
# spot_backtest.sh — same AMI/instance-type family/subnets the nested spots
# THIS box itself launches already use, so a c5.large-class launcher is
# consistent with the rest of the fleet's Saturday spend). ───────────────────
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "WEEKLY_SPOT_INSTANCE_TYPES", "c5.large,m5.large,c6i.large,c5a.large"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "WEEKLY_SPOT_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("WEEKLY_SPOT_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("WEEKLY_SPOT_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("WEEKLY_SPOT_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# Same profile the nested spots THIS box launches already run under, and the
# same profile data-spot-dispatcher's box uses — grants ec2:RunInstances/
# CreateTags/DescribeInstances (this box launching its OWN nested spots),
# ssm:GetParameter on /alpha-engine/* (PAT + other secrets), and the Arctic/S3
# read-write the two on-box health checks need.
IAM_PROFILE = os.environ.get("WEEKLY_SPOT_IAM_PROFILE", "alpha-engine-executor-profile")
# Modest disk: this box does not itself hold price data (its nested spots do
# their own large-disk launches) — it only holds 4 shallow repo clones + one
# venv. Headroom above the groom box's 40GB since the dashboard venv pulls in
# the full nousergon_lib/krepis/pandas/numpy/pyarrow stack.
VOLUME_SIZE_GB = int(os.environ.get("WEEKLY_SPOT_VOLUME_SIZE_GB", "40"))

DATA_REPO = os.environ.get("WEEKLY_SPOT_DATA_REPO", "nousergon/nousergon-data")
DATA_BRANCH = os.environ.get("WEEKLY_SPOT_DATA_BRANCH", "main")
CONFIG_REPO = os.environ.get("WEEKLY_SPOT_CONFIG_REPO", "nousergon/alpha-engine-config")
CONFIG_BRANCH = os.environ.get("WEEKLY_SPOT_CONFIG_BRANCH", "main")
BACKTESTER_REPO = os.environ.get("WEEKLY_SPOT_BACKTESTER_REPO", "nousergon/crucible-backtester")
BACKTESTER_BRANCH = os.environ.get("WEEKLY_SPOT_BACKTESTER_BRANCH", "main")
DASHBOARD_REPO = os.environ.get("WEEKLY_SPOT_DASHBOARD_REPO", "nousergon/crucible-dashboard")
DASHBOARD_BRANCH = os.environ.get("WEEKLY_SPOT_DASHBOARD_BRANCH", "main")

# alpha-engine-config is private; the box reads the fleet PAT from SSM via its
# instance profile — same pattern data-spot-dispatcher/scheduled-groom-
# dispatcher/alert-drain-dispatcher all already use.
GH_PAT_SSM = os.environ.get(
    "WEEKLY_SPOT_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)

# Bootstrap (clone x4 + venv build) execution timeout — the SSM command's own
# ceiling, independent of the SF's poll loop. Generous: 4 shallow clones +
# one full nousergon_lib/krepis/pandas/numpy/pyarrow venv build realistically
# takes low-single-digit minutes, but a cold pip index / dnf mirror can be
# slow; bounding at 20 min leaves large headroom without risking a false
# guillotine on a slow-but-healthy build.
BOOTSTRAP_TIMEOUT_SECONDS = int(
    os.environ.get("WEEKLY_SPOT_BOOTSTRAP_TIMEOUT_SECONDS", "1200")
)
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("WEEKLY_SPOT_SSM_ONLINE_BUDGET_SEC", "300"))
CW_LOG_GROUP = os.environ.get("WEEKLY_SPOT_CW_LOG_GROUP", "/alpha-engine/weekly-freshness-spot")

# Orphan-prevention backstop ONLY — sized to comfortably exceed the weekly
# SF's own top-level TimeoutSeconds (43200s = 12h, step_function.json) so it
# never fires on a healthy run. 46800s = 13h: 1h of headroom past the SF's
# own hang-detection ceiling, so a genuinely hung SF still gets caught by
# TIMED_OUT (routing into sf-watch, per test_sf_global_timeout.py) before
# this watchdog would ever pull the box out from under it.
WATCHDOG_SECONDS = int(os.environ.get("WEEKLY_SPOT_WATCHDOG_SECONDS", "46800"))


def _bootstrap_command(run_token: str) -> str:
    """The async SSM RunShellScript body: install runtime, clone all four
    repos the 14 downstream SF states' `git -C ... pull --ff-only` commands
    expect at their dashboard-box paths, build the dashboard venv, arm the
    long-lived watchdog. Runs as root; the repos land under /home/ec2-user so
    the downstream states' `sudo -u ec2-user git -C ... pull` succeeds
    unchanged (they pull, not clone — this bootstrap does the initial clone).

    Deliberately does NOT run any workload itself (unlike data-spot-
    dispatcher's/scheduled-groom-dispatcher's bootstrap, which execs straight
    into the actual job) — this box's job IS the clone+venv setup; the 14
    consumer states drive the actual work via their own separate sendCommand
    calls once the SF's poll loop observes this command reach Success.
    """
    log = f"/var/log/weekly-freshness-spot-bootstrap-{run_token}.log"
    s3_log = (
        f"s3://alpha-engine-research/_ssm_logs/weekly-freshness-spot/bootstrap/"
        f"$(date -u +%Y-%m-%d)/$(hostname)-$(date -u +%H%M%S)-{run_token}.log"
    )
    return f"""set -uo pipefail
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/home/ec2-user/.cache
export AWS_REGION={REGION}
export AWS_DEFAULT_REGION={REGION}
fail() {{ echo "[weekly-freshness-spot-bootstrap] FATAL: $1"; aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true; shutdown -h now; exit 1; }}
mkdir -p "$(dirname {log})"
exec > >(tee -a {log}) 2>&1
# Long-lived orphan-prevention watchdog (NOT the happy-path stop mechanism —
# this box stays up for the whole weekly SF run, unlike the nested spots it
# launches, which self-terminate per-workload). Sized to comfortably exceed
# the SF's own 43200s (12h) top-level TimeoutSeconds.
systemd-run --on-active={WATCHDOG_SECONDS} --unit=alpha-engine-weekly-freshness-watchdog \\
  --description='alpha-engine weekly-freshness-spot orphan-prevention watchdog' /sbin/shutdown -h now || true
dnf install -y -q python3.12 python3.12-pip python3.12-devel git gcc >/dev/null 2>&1 \\
  || dnf install -y -q python3 python3-pip python3-devel git gcc >/dev/null 2>&1 \\
  || fail "runtime install failed"
command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3
git config --global --add safe.directory '*' || true
PAT=$(aws ssm get-parameter --name {GH_PAT_SSM} --with-decryption \\
  --query Parameter.Value --output text --region {REGION}) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
echo "[weekly-freshness-spot-bootstrap] cloning alpha-engine-data..."
rm -rf /home/ec2-user/alpha-engine-data
git clone --depth 1 --branch {DATA_BRANCH} \\
  https://github.com/{DATA_REPO}.git /home/ec2-user/alpha-engine-data || fail "alpha-engine-data clone failed"
echo "[weekly-freshness-spot-bootstrap] cloning alpha-engine-config..."
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {CONFIG_BRANCH} \\
  "https://x-access-token:${{PAT}}@github.com/{CONFIG_REPO}.git" \\
  /home/ec2-user/alpha-engine-config || fail "alpha-engine-config clone failed"
echo "[weekly-freshness-spot-bootstrap] cloning alpha-engine-backtester..."
rm -rf /home/ec2-user/alpha-engine-backtester
git clone --depth 1 --branch {BACKTESTER_BRANCH} \\
  "https://x-access-token:${{PAT}}@github.com/{BACKTESTER_REPO}.git" \\
  /home/ec2-user/alpha-engine-backtester || fail "alpha-engine-backtester clone failed"
echo "[weekly-freshness-spot-bootstrap] cloning alpha-engine-dashboard..."
rm -rf /home/ec2-user/alpha-engine-dashboard
git clone --depth 1 --branch {DASHBOARD_BRANCH} \\
  "https://x-access-token:${{PAT}}@github.com/{DASHBOARD_REPO}.git" \\
  /home/ec2-user/alpha-engine-dashboard || fail "alpha-engine-dashboard clone failed"
chown -R ec2-user:ec2-user /home/ec2-user/alpha-engine-data /home/ec2-user/alpha-engine-config \\
  /home/ec2-user/alpha-engine-backtester /home/ec2-user/alpha-engine-dashboard || fail "chown failed"
echo "[weekly-freshness-spot-bootstrap] building alpha-engine-dashboard venv..."
cd /home/ec2-user/alpha-engine-dashboard
"$PYTHON_BIN" -m venv .venv || fail "venv create failed"
source .venv/bin/activate
pip install --upgrade pip -q || fail "pip upgrade failed"
if [ -f requirements.txt ]; then
  pip install -q -r requirements.txt || fail "dashboard requirements install failed"
fi
# numpy<2 pin to match every other spot workload (pyarrow compiled against 1.x).
pip install -q 'numpy<2' || fail "numpy pin failed"
chown -R ec2-user:ec2-user /home/ec2-user/alpha-engine-dashboard/.venv || fail "venv chown failed"
aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true
echo "[weekly-freshness-spot-bootstrap] complete — launcher box ready"
"""


def _launch_instance(force_on_demand: bool = False) -> tuple[str, str]:
    """Launch the launcher box; spot first, on-demand fallback on capacity/
    quota exhaustion via the shared spot_dispatch chokepoint (same posture as
    every other fleet dispatcher — the weekly run must not be starved by a
    capacity dip)."""
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name="alpha-engine-weekly-freshness-spot",
        region=REGION,
        force_on_demand=force_on_demand,
    )


def _wait_ssm_online(instance_id: str) -> None:
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(instance_id: str, run_token: str) -> str:
    """Fire the async, detached SSM command that clones the 4 repos + builds
    the dashboard venv. Returns the command id for the SF's poll loop."""
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(run_token),
        comment=f"weekly-freshness-spot bootstrap ({run_token}) — config#2248",
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=BOOTSTRAP_TIMEOUT_SECONDS,
    )


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a just-launched box whose post-launch steps
    failed — without this the box orphans (no watchdog/trap armed yet, that
    only happens inside the bootstrap this box never received). Never masks
    the original error (logged, not raised)."""
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="weekly-freshness")


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Step Function handler — launch the weekly pipeline's launcher spot box.

    `event` carries `{"force_on_demand": bool}` (reserved for a future bounded
    retry-on-relaunch, mirroring the daily/EOD data-spot pattern; no current
    caller sets it — defaults False). Returns:

      {"instance_id": "i-...", "command_id": "...", "market": "spot"|"on-demand",
       "run_token": "..."}

    Fail-loud: a launch/SSM error RAISES (no kill-switch skip, no fail-open
    branch) — the SF's own Catch converts it into the same loud
    HandleFailure/SNS path every other Task state in this pipeline uses. The
    weekly run cannot proceed at all without a launcher box, so degrading
    silently here would be strictly worse than halting loudly.
    """
    event = event or {}
    force_on_demand = bool(event.get("force_on_demand", False))

    if not DISPATCH_ENABLED:
        # No fail-open skip on the SF side for this flag — flipping it off is
        # an explicit "I will pass ec2_instance_id myself" operator action.
        # Raising here (rather than data-spot-dispatcher's silent-skip
        # {"launched": false}) keeps that contract honest: a dispatch that
        # was supposed to happen and didn't must not be indistinguishable
        # from "the operator already supplied an instance id".
        raise RuntimeError(
            "WEEKLY_SPOT_DISPATCH_ENABLED=false but no $.ec2_instance_id was "
            "supplied on the execution input — either re-enable the "
            "dispatcher or pass ec2_instance_id explicitly (see "
            "scripts/weekly_sf_rerun.py / run_weekly_offcycle.sh for the "
            "manual-override shape)"
        )

    run_token = uuid.uuid4().hex
    try:
        instance_id, market = _launch_instance(force_on_demand=force_on_demand)
    except SpotLaunchError:
        logger.error("weekly-freshness-spot launch failed (spot + on-demand exhausted)")
        raise
    logger.info("launched weekly-freshness-spot launcher box %s (%s)", instance_id, market)
    # Once the box is up, ANY failure before the bootstrap command is fired
    # would orphan it (no watchdog/trap yet — that's armed BY the bootstrap
    # this box hasn't received). Terminate-on-error so a slow SSM-online or
    # an SSM SendCommand error tears the box down instead of leaving it idle
    # for the rest of the week.
    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(instance_id, run_token)
    except Exception:
        _terminate_instance(instance_id)
        raise
    logger.info(
        "weekly-freshness-spot dispatched: instance=%s market=%s command=%s run_token=%s",
        instance_id, market, command_id, run_token,
    )
    return {
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "run_token": run_token,
    }
