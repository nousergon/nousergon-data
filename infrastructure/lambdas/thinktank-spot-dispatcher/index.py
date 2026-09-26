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

Mechanism — a SELF-STARTING box (alpha-engine-config-I11597):
  ONE call, `krepis.ec2_spot.launch_self_starting()`. It rotates
  instance_type x subnet on capacity error and falls back to on-demand, so a
  capacity dip never costs a day's Think Tank coverage. The box's user-data
  (`krepis.spot_bootstrap.render_self_starting_user_data`) installs the job
  as a oneshot systemd unit and starts it at boot. The job is a minimal
  prelude: install runtime, clone the (public) research repo, exec
  `infrastructure/thinktank_spot_bootstrap.sh`. The box self-terminates.
  This Lambda returns in seconds. It never waits for SSM and never sends a
  command.

  Why not launch -> wait for SSM Online -> send-command, as this used to:
  those are two non-atomic steps inside a timeout-bounded invocation. On
  2026-09-23 (alpha-engine-config-I11532) SSM registered slowly, Lambda killed
  the handler between the launch and the send, and the async retry found a
  running box that had never been given its job. With the job in user-data
  there is no second step to lose.

  Replay-safe: every RunInstances attempt carries a ClientToken derived from
  this invocation's request id. EventBridge's async retries of one event carry
  that event's request id, so a retry gets back the box its predecessor
  launched and launches nothing new. The run token is derived from the same
  id, so the replay's tags and user-data match the original's exactly.

THE BUDGET/TIMEOUT COUPLING IS LOAD-BEARING. `RUN_BUDGET_SECONDS` must stay
below `RUN_TIMEOUT_SECONDS` by at least the run module's terminal-write
reserve (`thinktank.run._TERMINAL_WRITE_RESERVE_S`, 120s). The box derives its
deadline from the budget; systemd kills the job unit at the timeout
(`TimeoutStartSec`). If the budget ever meets or exceeds the timeout, the run
is killed mid-loop and every terminal write is lost again — i.e. the exact
failure this dispatcher exists to fix, reintroduced through a config drift.
`handler` refuses to launch in that state and `test_handler.py` asserts the
inequality; do not "fix" a truncating run by raising the budget without
raising the timeout first.

Fail-loud (the daily Think Tank IS the deliverable, and it is one of three
count-matched champion/challenger arms per config-I4983): a launch failure
RAISES so EventBridge's async retries, the Lambda Errors metric, and the alarm
watching it all surface the miss, rather than silently dropping a day. Once the
box exists, its liveness is watched where it always was: the completion
marker, spot-orphan-reaper's Think-Tank WatchKind and the
`thinktank_challenger_selection` freshness row.

Managed OUTSIDE CloudFormation (same as every sibling dispatcher): operator-
bootstrapped via `deploy.sh --bootstrap`; code deploys on merge. See README.md.
"""

from __future__ import annotations

import datetime
import hashlib
import logging
import os
import uuid

from krepis import ec2_spot
from krepis.spot_bootstrap import render_self_starting_user_data
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
#
# Widened 4 -> 9 types across 3 families (alpha-engine-config-I11340 item 5),
# same set and same rationale as `data-spot-dispatcher/index.py`'s widening —
# CloudTrail (2026-09-21) shows this role as one of the two c5.large launchers
# behind August's on-demand/spot c5.large concurrency (946 combined hours >
# the 744 in the month). Every addition is x86_64, 2 vCPU, 4 GiB class,
# drawn from the same already-evidenced set this repo's own
# `spot_data_weekly.sh`/`_spot_common.sh` ALLOWED_INSTANCE_TYPES treats as
# offered in this account's subnets. NEEDS AN IAM CHANGE (see iam-policy.json)
# in the same change set — operator-gated `deploy.sh --apply-iam`.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "THINKTANK_SPOT_INSTANCE_TYPES",
        "c5.large,c5a.large,c6i.large,m5.large,m5a.large,m6i.large,"
        "r5.large,r5a.large,r6i.large",
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
# The job unit's hard cap (systemd TimeoutStartSec — the role SSM's
# executionTimeout played before alpha-engine-config-I11597). Must exceed
# RUN_BUDGET_SECONDS by more than the run module's 120s terminal-write reserve,
# PLUS bootstrap time (runtime install + clone + venv build, low single-digit
# minutes).
RUN_TIMEOUT_SECONDS = int(os.environ.get("THINKTANK_SPOT_RUN_TIMEOUT_SECONDS", "7200"))  # 2h
# Orphan-prevention backstop only — never fires on a healthy run. Sized above
# the unit timeout so systemd's own kill (which the bootstrap's EXIT trap and
# the unit's ExecStopPost turn into a clean self-terminate) always wins first.
# spot-orphan-reaper is a 6.5h AGE CAP, not a health check, so it is not a
# substitute for this.
WATCHDOG_SECONDS = int(os.environ.get("THINKTANK_SPOT_WATCHDOG_SECONDS", "9000"))  # 2.5h
# After the cap's SIGTERM, how long the bootstrap's EXIT trap gets to ship its
# log and publish its failure alert before SIGKILL.
STOP_GRACE_SECONDS = 120

#: The systemd unit the box runs the job as.
JOB_UNIT = "alpha-engine-thinktank-run"

# ── Launch + completion records (alpha-engine-config-I5752 / I11597) ────────
# With no SSM command there is no command_id to reconcile a run against. The
# box writes a LAUNCH record at boot, before the job starts, and the SUCCESS
# path of thinktank_spot_bootstrap.sh writes the COMPLETION marker. Both are
# keyed `{trading_day}-{run_token}.json` in the same `_control/` family, so a
# reconciler pairs them by key: launched and not completed by `deadline_at`
# means the run failed or wedged. The box writes the launch record because the
# box role already writes this bucket; this Lambda's role holds no S3 grant.
RECORD_BUCKET = "alpha-engine-research"
LAUNCH_RECORD_PREFIX = "thinktank/_control/launched/"
COMPLETION_PREFIX = "thinktank/_control/completed/"

# ── Router addressing (alpha-engine-config-I6367 / I6373) ──────────────────
# Brian's ruling 2026-08-03: no agent may be directly linked to OpenRouter.
# The Think Tank's tiers address model GROUPS, resolved through the
# authenticated router edge. Three facts the box cannot derive for itself:
#
#   where it runs        — a stock-AMI spot box, so the dashboard box's local
#                          egress proxy at 127.0.0.1:8990 does NOT answer here
#                          and the edge is the only path;
#   which URL             — model-router-policy §3.4a R27a: the router is
#                          addressed by (url, credential) and reaching it may
#                          not depend on host, VPC, subnet, SG or private IP;
#   which credential     — the edge identifies a consumer BY its credential
#                          VALUE, and krepis.secrets resolves SSM BEFORE
#                          os.environ, so sharing the secret NAME
#                          `LITELLM_MASTER_KEY` would collapse this box into
#                          the director's identity however the environment is
#                          set. `thinktank` is its own consumer in
#                          nous-ergon-ops bin/render-router-secrets.sh.
#
# The registry itself comes from AppConfig: crucible-research is PUBLIC, so
# private-docs/LLM_MODEL_REGISTRY.yaml is correctly absent from the clone.
KREPIS_EXEC_CONTEXT = os.environ.get("THINKTANK_SPOT_EXEC_CONTEXT", "ec2")
KREPIS_LITELLM_PROXY_URL = os.environ.get(
    "THINKTANK_SPOT_ROUTER_URL", "https://router.nousergon.ai:8443"
)
KREPIS_ROUTER_CREDENTIAL_SECRET = os.environ.get(
    "THINKTANK_SPOT_ROUTER_CREDENTIAL_SECRET", "ROUTER_CONSUMER_THINKTANK"
)
KREPIS_APPCONFIG_APPLICATION = os.environ.get(
    "THINKTANK_SPOT_APPCONFIG_APPLICATION", "alpha-engine"
)
KREPIS_APPCONFIG_CONFIG_PROFILE = os.environ.get(
    "THINKTANK_SPOT_APPCONFIG_CONFIG_PROFILE", "llm-model-registry"
)
KREPIS_APPCONFIG_ENVIRONMENT = os.environ.get(
    "THINKTANK_SPOT_APPCONFIG_ENVIRONMENT", "production"
)

INSTANCE_TAG_NAME = "alpha-engine-thinktank-spot"


def _job_script(run_token: str) -> str:
    """The job the box runs at boot: install runtime, clone the research repo,
    exec the repo's bootstrap.

    Deliberately minimal. The heavy, version-controlled logic lives in
    crucible-research's ``infrastructure/thinktank_spot_bootstrap.sh`` so a
    change to the run shape is a PR in that repo rather than a Lambda
    redeploy (§47 sub-rule (a): one shared entrypoint, not two drifting
    copies). This prelude is only the clone glue.

    It ships inside the box's user-data (alpha-engine-config-I11597), which
    anyone with ``ec2:DescribeInstanceAttribute`` can read — so it carries
    REFERENCES only: a run token, SSM parameter NAMES, public URLs. Every
    secret is resolved on the box from SSM. ``test_handler.py`` asserts both
    that and the 16 KB user-data ceiling.

    Runs as root under systemd with no $HOME — both are set explicitly below.
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
export KREPIS_EXEC_CONTEXT={KREPIS_EXEC_CONTEXT}
export KREPIS_LITELLM_PROXY_URL={KREPIS_LITELLM_PROXY_URL}
export KREPIS_ROUTER_CREDENTIAL_SECRET={KREPIS_ROUTER_CREDENTIAL_SECRET}
export KREPIS_APPCONFIG_APPLICATION={KREPIS_APPCONFIG_APPLICATION}
export KREPIS_APPCONFIG_CONFIG_PROFILE={KREPIS_APPCONFIG_CONFIG_PROFILE}
export KREPIS_APPCONFIG_ENVIRONMENT={KREPIS_APPCONFIG_ENVIRONMENT}
exec bash infrastructure/thinktank_spot_bootstrap.sh
"""


def _record_key(prefix: str, run_token: str) -> str:
    """``{prefix}{trading_day}-{run_token}.json`` — the completion marker's key
    shape, which spot-orphan-reaper's Think-Tank WatchKind also derives."""
    return f"{prefix}{_trading_day()}-{run_token}.json"


def _launch_record_uri(run_token: str) -> str:
    return f"s3://{RECORD_BUCKET}/{_record_key(LAUNCH_RECORD_PREFIX, run_token)}"


def _user_data(run_token: str) -> str:
    """The box's whole dispatch: the job, installed and started at boot as a
    capped oneshot unit that powers the box off however it ends, preceded by
    the launch record a completion reconciler reads (I5752).

    A pure function of the run token and the UTC date — nothing
    clock-dependent beyond the day — so a replay of the same event renders it
    byte for byte, which EC2 requires before it hands back the ClientToken's
    instance."""
    return render_self_starting_user_data(
        _job_script(run_token),
        unit=JOB_UNIT,
        description=f"daily Think Tank run (config-I5208 §47) {run_token}",
        timeout_seconds=RUN_TIMEOUT_SECONDS,
        stop_grace_seconds=STOP_GRACE_SECONDS,
        launch_record_uri=_launch_record_uri(run_token),
        launch_record={
            "workload": "thinktank",
            "run_token": run_token,
            "trading_day": _trading_day(),
            "budget_seconds": RUN_BUDGET_SECONDS,
            "completion_marker": (
                f"s3://{RECORD_BUCKET}/{_record_key(COMPLETION_PREFIX, run_token)}"
            ),
        },
        region=REGION,
    )


def _run_token(idempotency_key: str) -> str:
    """32 hex chars (the shape ``uuid4().hex`` had), derived from the
    invocation's idempotency key.

    Derived, not random, because the token rides the launch twice — in the
    discriminator tags and in the user-data — and EC2 hands a ClientToken's
    instance back only when the replayed RunInstances parameters MATCH. A
    retry minting a fresh token would present different tags and user-data,
    which EC2 refuses as a parameter mismatch. It also means the replay reports
    the token the box is actually running with, which is the one its
    completion marker carries.
    """
    return hashlib.sha256(f"thinktank-spot:{idempotency_key}".encode()).hexdigest()[:32]


def _trading_day() -> str:
    """The UTC date the box will also compute at exit.

    The box derives its completion-marker key from ``date -u +%Y-%m-%d`` in
    ``thinktank_spot_bootstrap.sh``'s ``on_exit``; the reaper derives the key it
    looks up from these tags. Both must agree, and they agree because a Think
    Tank box cannot span UTC midnight: dispatch is 14:30 UTC and
    ``RUN_TIMEOUT_SECONDS`` is 2h, leaving ~7h of margin.

    That is an invariant, not a coincidence, so
    ``test_handler.py::test_the_box_cannot_span_utc_midnight`` fails if anyone
    moves the schedule or raises the timeout into the boundary rather than
    discovering the mismatch as a silently-missing marker.
    """
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")


def _discriminator_tags(run_token: str) -> dict[str, str]:
    """Tags spot-orphan-reaper reconstructs the completion-marker key from.

    Joined with '-' and suffixed '.json' onto the WatchKind's
    ``completion_prefix``, these must reproduce exactly the key the box writes:
    ``thinktank/_control/completed/{trading_day}-{run_token}.json``. Key order
    here is the tuple order in ``WATCH_KINDS`` — the reaper joins by that
    tuple, not by dict order.
    """
    return {
        "thinktank-trading-day": _trading_day(),
        "thinktank-run-token": run_token,
    }




def _launch_instance(
    run_token: str,
    idempotency_key: str,
    force_on_demand: bool = False,
    extra_tags: dict | None = None,
) -> ec2_spot.SelfStartingLaunch:
    return ec2_spot.launch_self_starting(
        INSTANCE_TYPES,
        SUBNETS,
        idempotency_key=idempotency_key,
        user_data=_user_data(run_token),
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=INSTANCE_TAG_NAME,
        region=REGION,
        force_on_demand=force_on_demand,
        # Atomic with RunInstances, never a post-launch create_tags: a box
        # reaped inside the tagging window would be unlookupable either way
        # (config#2292, the root fix for config#2267 site 2). The reaper's
        # discriminator tags and the config#5504 identity tags ride the same
        # call; identity tags never collide with the discriminator keys.
        extra_tags={**_discriminator_tags(run_token), **(extra_tags or {})},
    )


def _already_running() -> list[str]:
    """Duplicate-launch guard.

    A degraded EC2 API must never read as "no duplicate running" — that is the
    config#2267 fail-open class. ``running_instance_ids`` raises SpotProbeError
    rather than returning a clean empty list, and the caller below chooses
    coverage over dedupe explicitly and records the choice.

    A running box is always a box that is running its job: the job rides the
    launch in user-data, so there is no longer a "launched but never
    dispatched" state for a retry to find (alpha-engine-config-I11597; see
    nousergon-data#1957 for the adoption rule this replaces).
    """
    # discriminator_tags is REQUIRED positional. Empty is correct here: this
    # dispatcher has exactly one lane (one daily run), so the Name tag alone
    # identifies a duplicate — same shape as alert-drain-dispatcher. Lanes
    # that DO partition (groom tiers, arctic migrations) pass a discriminator.
    return spot_dispatch.running_instance_ids(INSTANCE_TAG_NAME, {}, region=REGION)


def handler(event: dict, context) -> dict:
    """EventBridge handler — launch the daily Think Tank box.

    ``event`` may carry ``{"force_on_demand": bool}``. Returns
    ``{"launched": True, "replayed", "instance_id", "market", "run_token",
    "launch_record", "dedupe_degraded", "idempotency_probe_degraded"}``, or
    ``{"launched": False, "reason": "already_running", "instance_ids"}``.

    Fail-loud: a launch error RAISES so EventBridge's two async retries engage
    and the Lambda Errors metric drives the alarm. A retry is safe wherever the
    previous invocation died: the launch is keyed on this event's request id,
    so a box that already exists for it is returned (``replayed``), never
    duplicated.
    """
    event = event or {}
    force_on_demand = bool(event.get("force_on_demand", False))

    # Per-run identity tags (config#5504): attribute the Think Tank box for EC2
    # cost measurement. This dispatcher is EventBridge-triggered (not SF), so
    # execution_id may be absent; when present from a chained upstream SF it
    # rides the RunInstances call atomically, otherwise the box has only its
    # Name tag.
    extra_tags = {}
    for key, tag_name in (
        ("execution_id", "execution-id"),
        ("run_date", "run-date"),
        ("pipeline_role", "pipeline-role"),
    ):
        val = str(event.get(key, "")).strip()
        if val:
            extra_tags[tag_name] = val

    if not DISPATCH_ENABLED:
        raise RuntimeError(
            "THINKTANK_SPOT_DISPATCH_ENABLED=false — the daily Think Tank will "
            "not run today. There is no alternative path; re-enable the "
            "dispatcher or invoke the run manually."
        )

    if RUN_BUDGET_SECONDS >= RUN_TIMEOUT_SECONDS:
        # Refuse to launch rather than run a box whose deadline the unit's cap
        # will preempt — that silently reintroduces the lost-terminal-writes bug.
        raise ValueError(
            f"THINKTANK_RUN_BUDGET_SECONDS={RUN_BUDGET_SECONDS} must be strictly "
            f"below THINKTANK_SPOT_RUN_TIMEOUT_SECONDS={RUN_TIMEOUT_SECONDS}; "
            "otherwise the job unit is killed before its terminal writes "
            "(alpha-engine-config-I5208)"
        )

    # The idempotency key. Lambda's async retries of one event reuse its
    # request id; a direct call without a context (a local test harness) gets
    # a fresh key, i.e. no replay protection, which is all such a caller can
    # have.
    idempotency_key = getattr(context, "aws_request_id", None) or uuid.uuid4().hex
    run_token = _run_token(idempotency_key)

    dedupe_degraded = False
    try:
        running = _already_running()
    except SpotProbeError:
        # Coverage beats dedupe for a once-daily arm: a duplicate box costs
        # cents and both runs converge on the same checkpointed ledger, while
        # a skipped day costs a day of challenger evidence. Recorded, never
        # silent. A retry of THIS event is still deduplicated by the launch's
        # ClientToken below.
        logger.warning(
            "thinktank-spot dedupe probe DEGRADED (DescribeInstances failed) — "
            "launching anyway; a duplicate box is cheaper than a missed day"
        )
        dedupe_degraded = True
        running = []
    if running:
        # Including the box an earlier attempt of THIS event launched before it
        # died: that box carries its job and is running it.
        logger.warning(
            "thinktank-spot box already running (%s) — skipping this launch", running
        )
        return {"launched": False, "reason": "already_running", "instance_ids": running}

    try:
        result = _launch_instance(
            run_token,
            idempotency_key,
            force_on_demand=force_on_demand,
            extra_tags=extra_tags or None,
        )
    except SpotLaunchError:
        logger.error("thinktank-spot launch failed (spot + on-demand exhausted)")
        raise
    logger.info(
        "%s thinktank-spot box %s (%s), run_token=%s, request=%s",
        "replayed" if result.replayed else "launched",
        result.instance_id,
        result.market,
        run_token,
        idempotency_key,
    )

    return {
        "launched": True,
        "replayed": result.replayed,
        "instance_id": result.instance_id,
        "market": result.market,
        "run_token": run_token,
        "launch_record": _launch_record_uri(run_token),
        "dedupe_degraded": dedupe_degraded,
        "idempotency_probe_degraded": result.probe_degraded,
    }
