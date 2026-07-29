"""alpha-engine-thinktank-spot-dispatcher — run the daily Think Tank on EC2 spot.

WHY (alpha-engine-config-I5208, nous-ergon-ops-I162, ARCHITECTURE §47): the
daily Think Tank ran as a Lambda and hit the 900s hard ceiling every day from
2026-07-17, dying mid-loop before any of its terminal writes. §47 has required
since 2026-06-30 that a long-running agent/batch job runs on owned compute
behind a dispatcher, never on a metered/ceilinged runtime. This is that
dispatcher. The workload is a multi-tier LLM agent loop over a growing coverage
universe — exactly the §47 shape.

Measured, so the sizing is not a guess (see RUN_BUDGET_SECONDS): the
2026-07-16 run did 8 theses AND a 70-name events sweep in 443s; the 2026-07-29
run did 5 theses and NO sweep in 801s, truncated. crucible-research PR#464's
pillar/moat call roughly tripled per-thesis wall-clock (~55s -> ~160s), which
is what crossed the ceiling. Steady state is ~25 min — a shade over 2x the
Lambda maximum, not hours.

Mechanism (mirrors scheduled-groom-dispatcher / data-spot-dispatcher via the
shared `nousergon_lib.spot_dispatch` chokepoint — no lib change):
  1. `spot_dispatch.launch_with_fallback()` rotates instance_type x subnet on
     capacity error, then falls back to on-demand so a capacity dip never
     costs a day's Think Tank coverage.
  2. Wait for the instance to run + its SSM agent to register Online.
  3. Fire an ASYNC, detached `ssm send-command` carrying a minimal prelude:
     install runtime, clone the (public) research repo, exec
     `infrastructure/thinktank_spot_bootstrap.sh`. The box self-terminates.
     This Lambda returns immediately — it does not babysit the run.

THE BUDGET/TIMEOUT COUPLING IS LOAD-BEARING. `RUN_BUDGET_SECONDS` must stay
below `RUN_TIMEOUT_SECONDS` by at least the run module's terminal-write
reserve (`thinktank.run._TERMINAL_WRITE_RESERVE_S`, 120s). The box derives its
deadline from the budget; SSM kills the command at the timeout. If the budget
ever meets or exceeds the timeout, SSM guillotines the run mid-loop and every
terminal write is lost again — i.e. the exact failure this dispatcher exists
to fix, reintroduced through a config drift. `handler` refuses to launch in
that state and `test_handler.py` asserts the inequality; do not "fix" a
truncating run by raising the budget without raising the timeout first.

Fail-loud (the daily Think Tank IS the deliverable, and it is one of three
count-matched champion/challenger arms per config-I4983): a launch/SSM failure
RAISES so EventBridge's async retries, the Lambda Errors metric, and the alarm
watching it all surface the miss, rather than silently dropping a day.

Managed OUTSIDE CloudFormation (same as every sibling dispatcher): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect until
the Lambda + IAM are deployed AND the `alpha-research-thinktank-daily`
EventBridge rule is repointed off the Lambda alias — see README.md for the
staged-cutover order (§47 sub-rule (b): keep the old path live until the new
one is validated by a REAL run).
"""

from __future__ import annotations

import logging
import os
import uuid

from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotLaunchError, SpotProbeError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch. Unlike the weekly launcher there is no operator-supplied
# alternative path here, so disabling this genuinely means "no Think Tank
# today" — it RAISES rather than returning a quiet {"launched": false}, so a
# disabled dispatcher is never indistinguishable from a healthy no-op.
DISPATCH_ENABLED = (
    os.environ.get("THINKTANK_SPOT_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config ──────────────────────────────────────────────────────
# Instance family mirrors the sibling launchers. The workload is LLM/network-
# bound, not CPU-bound (the Lambda peaked at 394MB of 1024MB), so the smallest
# standard tier is right; the multi-type list exists for capacity resilience,
# not performance.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "THINKTANK_SPOT_INSTANCE_TYPES", "c5.large,m5.large,c6i.large,c5a.large"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "THINKTANK_SPOT_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("THINKTANK_SPOT_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("THINKTANK_SPOT_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("THINKTANK_SPOT_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# Same profile every sibling spot box uses: ssm:GetParameter on /alpha-engine/*
# (the config PAT + the provider key the run needs) and read/write on
# s3://alpha-engine-research. This Lambda passes none of those itself.
IAM_PROFILE = os.environ.get("THINKTANK_SPOT_IAM_PROFILE", "alpha-engine-executor-profile")
# One shallow public clone + one private config clone + a venv carrying the
# research stack. 40GB matches the weekly launcher's sizing for the same
# reason (the venv, not the data).
VOLUME_SIZE_GB = int(os.environ.get("THINKTANK_SPOT_VOLUME_SIZE_GB", "40"))

RESEARCH_REPO = os.environ.get("THINKTANK_SPOT_RESEARCH_REPO", "nousergon/crucible-research")
RESEARCH_BRANCH = os.environ.get("THINKTANK_SPOT_RESEARCH_BRANCH", "main")

# ── Timing (see the module docstring: this coupling is load-bearing) ────────
# Budget the box runs its deadline against. Derived from measured manifests,
# with headroom for coverage growth to rank_ceiling=150 and the
# stale_after_days=30 refresh wave starting early August.
RUN_BUDGET_SECONDS = int(os.environ.get("THINKTANK_RUN_BUDGET_SECONDS", "5400"))  # 90 min
# SSM's own ceiling on the command. Must exceed RUN_BUDGET_SECONDS by more
# than the run module's 120s terminal-write reserve, PLUS bootstrap time
# (clone + venv build, low single-digit minutes on a warm mirror).
RUN_TIMEOUT_SECONDS = int(os.environ.get("THINKTANK_SPOT_RUN_TIMEOUT_SECONDS", "7200"))  # 2h
# Orphan-prevention backstop only — never fires on a healthy run. Sized above
# the SSM timeout so SSM's own kill (which the bootstrap trap converts into a
# clean self-terminate) always wins first. spot-orphan-reaper is a 6.5h AGE
# CAP, not a health check, so it is not a substitute for this.
WATCHDOG_SECONDS = int(os.environ.get("THINKTANK_SPOT_WATCHDOG_SECONDS", "9000"))  # 2.5h
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("THINKTANK_SPOT_SSM_ONLINE_BUDGET_SEC", "300"))
CW_LOG_GROUP = os.environ.get("THINKTANK_SPOT_CW_LOG_GROUP", "/alpha-engine/thinktank-spot")

INSTANCE_TAG_NAME = "alpha-engine-thinktank-spot"


def _bootstrap_command(run_token: str) -> str:
    """The async SSM RunShellScript body: install runtime, clone the research
    repo, exec the repo's bootstrap.

    Deliberately minimal. The heavy, version-controlled logic lives in
    crucible-research's ``infrastructure/thinktank_spot_bootstrap.sh`` so a
    change to the run shape is a PR in that repo rather than a Lambda
    redeploy (§47 sub-rule (a): one shared entrypoint, not two drifting
    copies). This prelude is only the clone glue.

    Runs as root under SSM with no $HOME — both are set explicitly below.
    Stock AL2023 ships neither git nor python3.12, so both are installed
    before the clone. Any prelude failure shuts the box down so a botched
    launch never idles (§47 sub-rule (b): these are the exact classes that
    only a real launch surfaces).
    """
    log = f"/var/log/thinktank-spot-bootstrap-{run_token}.log"
    s3_log = (
        f"s3://alpha-engine-research/_ssm_logs/thinktank-spot/"
        f"$(date -u +%Y-%m-%d)/$(hostname)-$(date -u +%H%M%S)-{run_token}.log"
    )
    return f"""set -uo pipefail
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/home/ec2-user/.cache
export AWS_REGION={REGION}
export AWS_DEFAULT_REGION={REGION}
fail() {{ echo "[thinktank-spot-prelude] FATAL: $1"; aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true; shutdown -h now; exit 1; }}
mkdir -p "$(dirname {log})"
exec > >(tee -a {log}) 2>&1
systemd-run --on-active={WATCHDOG_SECONDS} --unit=alpha-engine-thinktank-spot-watchdog \\
  --description='alpha-engine thinktank-spot orphan-prevention watchdog' /sbin/shutdown -h now || true
dnf install -y -q git python3.12 python3.12-pip python3.12-devel gcc >/dev/null 2>&1 \\
  || fail "runtime install (git/python3.12) failed"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/crucible-research
git clone --depth 1 --branch {RESEARCH_BRANCH} \\
  https://github.com/{RESEARCH_REPO}.git /home/ec2-user/crucible-research \\
  || fail "crucible-research clone failed"
cd /home/ec2-user/crucible-research
export THINKTANK_RUN_BUDGET_SECONDS={RUN_BUDGET_SECONDS}
export THINKTANK_SPOT_RUN_TOKEN={run_token}
exec bash infrastructure/thinktank_spot_bootstrap.sh
"""


def _launch_instance(force_on_demand: bool = False) -> tuple[str, str]:
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES,
        SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=INSTANCE_TAG_NAME,
        region=REGION,
        force_on_demand=force_on_demand,
    )


def _already_running() -> list[str]:
    """Duplicate-launch guard.

    A degraded EC2 API must never read as "no duplicate running" — that is the
    config#2267 fail-open class. ``running_instance_ids`` raises SpotProbeError
    rather than returning a clean empty list, and the caller below chooses
    coverage over dedupe explicitly and records the choice.
    """
    # discriminator_tags is REQUIRED positional. Empty is correct here: this
    # dispatcher has exactly one lane (one daily run), so the Name tag alone
    # identifies a duplicate — same shape as alert-drain-dispatcher. Lanes
    # that DO partition (groom tiers, arctic migrations) pass a discriminator.
    return spot_dispatch.running_instance_ids(INSTANCE_TAG_NAME, {}, region=REGION)


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — launch the daily Think Tank box.

    ``event`` may carry ``{"force_on_demand": bool}``. Returns
    ``{"launched", "instance_id", "command_id", "market", "run_token",
    "dedupe_degraded"}``.

    Fail-loud: a launch/SSM error RAISES so EventBridge's two async retries
    engage and the Lambda Errors metric drives the alarm.
    """
    event = event or {}
    force_on_demand = bool(event.get("force_on_demand", False))

    if not DISPATCH_ENABLED:
        raise RuntimeError(
            "THINKTANK_SPOT_DISPATCH_ENABLED=false — the daily Think Tank will "
            "not run today. There is no alternative path; re-enable the "
            "dispatcher or invoke the run manually."
        )

    if RUN_BUDGET_SECONDS >= RUN_TIMEOUT_SECONDS:
        # Refuse to launch rather than run a box whose deadline SSM will
        # preempt — that silently reintroduces the lost-terminal-writes bug.
        raise ValueError(
            f"THINKTANK_RUN_BUDGET_SECONDS={RUN_BUDGET_SECONDS} must be strictly "
            f"below THINKTANK_SPOT_RUN_TIMEOUT_SECONDS={RUN_TIMEOUT_SECONDS}; "
            "otherwise SSM guillotines the run before its terminal writes "
            "(alpha-engine-config-I5208)"
        )

    dedupe_degraded = False
    try:
        running = _already_running()
    except SpotProbeError:
        # Coverage beats dedupe for a once-daily arm: a duplicate box costs
        # cents and both runs converge on the same checkpointed ledger, while
        # a skipped day costs a day of challenger evidence. Recorded, never
        # silent.
        logger.warning(
            "thinktank-spot dedupe probe DEGRADED (DescribeInstances failed) — "
            "launching anyway; a duplicate box is cheaper than a missed day"
        )
        dedupe_degraded = True
        running = []
    if running:
        logger.warning(
            "thinktank-spot box already running (%s) — skipping this launch", running
        )
        return {"launched": False, "reason": "already_running", "instance_ids": running}

    run_token = uuid.uuid4().hex
    try:
        instance_id, market = _launch_instance(force_on_demand=force_on_demand)
    except SpotLaunchError:
        logger.error("thinktank-spot launch failed (spot + on-demand exhausted)")
        raise
    logger.info("launched thinktank-spot box %s (%s)", instance_id, market)

    # Between launch and the bootstrap command landing there is no watchdog or
    # trap on the box yet — anything failing in here would orphan it.
    try:
        spot_dispatch.wait_ssm_online(
            instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
        )
        command_id = spot_dispatch.send_async_command(
            instance_id,
            _bootstrap_command(run_token),
            comment=f"daily Think Tank run (config-I5208 §47) — {run_token}",
            region=REGION,
            cw_log_group=CW_LOG_GROUP,
            execution_timeout_seconds=RUN_TIMEOUT_SECONDS,
        )
    except Exception:
        spot_dispatch.terminate_on_failure(
            instance_id, region=REGION, label="thinktank-spot"
        )
        raise

    return {
        "launched": True,
        "instance_id": instance_id,
        "command_id": command_id,
        "market": market,
        "run_token": run_token,
        "dedupe_degraded": dedupe_degraded,
    }
