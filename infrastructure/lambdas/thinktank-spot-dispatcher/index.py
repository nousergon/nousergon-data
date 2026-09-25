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

import datetime
import logging
import math
import os
import uuid

import boto3
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

# ── Lambda-timeout headroom (alpha-engine-config-I11532) ───────────────────
# On 2026-09-23 the function's timeout was 300s and SSM_ONLINE_BUDGET_SEC was
# ALSO 300s, so `wait_ssm_online` was permitted to consume the whole invocation.
# SSM registered slowly, Lambda killed the handler before `send_async_command`,
# and the day's Think Tank run was lost. The two numbers lived in different
# files (here and deploy.sh), which is why nobody saw the equality.
#
# The function's timeout is DECLARED in deploy.sh as FN_TIMEOUT and converged
# onto the live function by every deploy; this constant mirrors it and
# test_handler.py fails if the two ever differ. What has to fit inside it:
#
#   launch (spot rotation + on-demand fallback, seconds in practice)
#   + INSTANCE_RUNNING_WAIT_MAX_SEC  (the lib's instance_running waiter, which
#                                     runs BEFORE the SSM budget starts)
#   + SSM_ONLINE_BUDGET_SEC
#   + DISPATCH_RESERVE_SEC            (send_command + tag + return)
#
# test_handler.py asserts that sum stays strictly below the timeout, and
# `_ssm_online_budget` clamps the wait at RUNTIME to what the invocation has
# left, so even a drifted configuration makes the wait RAISE (which terminates
# the box and lets the async retry launch a fresh one) instead of being killed
# silently mid-wait.
LAMBDA_TIMEOUT_SECONDS = 900
# `nousergon_lib.spot_dispatch.wait_ssm_online` waits for `instance_running`
# with WaiterConfig Delay=5 x MaxAttempts=40 before its SSM loop begins.
# test_handler.py reads the real lib source and fails if that grows past this.
INSTANCE_RUNNING_WAIT_MAX_SEC = 200
DISPATCH_RESERVE_SEC = 60

# ── Interrupted-dispatch recovery (alpha-engine-config-I11532) ─────────────
# A box is marked as DISPATCHED by this tag, written the moment
# `send_async_command` returns. A running Think Tank box WITHOUT it is a box
# whose dispatching invocation died between launch and send — the 2026-09-23
# orphan. The async retry used to read that orphan as a healthy concurrent run
# and skip, which is what turned a slow SSM registration into a lost day.
COMMAND_ID_TAG = "thinktank-command-id"
# The invocation that launched the box, stamped atomically with RunInstances.
# Lambda's async retries of one event carry that event's request id, so a
# retry finding its own id on an undispatched box knows that box's dispatcher
# was its own earlier attempt — dead. Should that ever not hold, the age rule
# in `_adoptable_orphan` still adopts once no invocation can be alive.
DISPATCH_REQUEST_ID_TAG = "thinktank-dispatch-request-id"
# How old an undispatched box may be and still be adopted. It must cover the
# whole async-retry schedule (initial invoke + ~1 min + retry + ~2 min + retry,
# each up to LAMBDA_TIMEOUT_SECONDS) — test_handler.py asserts that — and no
# more: an older undispatched box is not one this day's retries created.
ADOPT_WINDOW_SEC = int(os.environ.get("THINKTANK_SPOT_ADOPT_WINDOW_SEC", "2700"))  # 45 min
CW_LOG_GROUP = os.environ.get("THINKTANK_SPOT_CW_LOG_GROUP", "/alpha-engine/thinktank-spot")

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
export KREPIS_EXEC_CONTEXT={KREPIS_EXEC_CONTEXT}
export KREPIS_LITELLM_PROXY_URL={KREPIS_LITELLM_PROXY_URL}
export KREPIS_ROUTER_CREDENTIAL_SECRET={KREPIS_ROUTER_CREDENTIAL_SECRET}
export KREPIS_APPCONFIG_APPLICATION={KREPIS_APPCONFIG_APPLICATION}
export KREPIS_APPCONFIG_CONFIG_PROFILE={KREPIS_APPCONFIG_CONFIG_PROFILE}
export KREPIS_APPCONFIG_ENVIRONMENT={KREPIS_APPCONFIG_ENVIRONMENT}
exec bash infrastructure/thinktank_spot_bootstrap.sh
"""


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
    run_token: str, force_on_demand: bool = False, extra_tags: dict | None = None
) -> tuple[str, str]:
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
        # Atomic with RunInstances, never a post-launch create_tags: a box
        # reaped inside the tagging window would be unlookupable either way
        # (config#2292, the root fix for config#2267 site 2). The reaper's
        # discriminator tags and the config#5504 identity tags ride the same
        # call; identity tags never collide with the discriminator keys.
        extra_tags={**_discriminator_tags(run_token), **(extra_tags or {})},
    )


def _running_boxes() -> list[dict]:
    """Duplicate-launch guard: every live Think Tank box, with its tags.

    Returns ``[{"instance_id", "launch_time", "tags"}]``. A degraded EC2 API
    must never read as "no duplicate running" — that is the config#2267
    fail-open class. ``running_instance_ids`` raises SpotProbeError rather than
    returning a clean empty list, the tag read below does the same, and the
    caller chooses coverage over dedupe explicitly and records the choice.
    """
    # discriminator_tags is REQUIRED positional. Empty is correct here: this
    # dispatcher has exactly one lane (one daily run), so the Name tag alone
    # identifies a duplicate — same shape as alert-drain-dispatcher. Lanes
    # that DO partition (groom tiers, arctic migrations) pass a discriminator.
    ids = spot_dispatch.running_instance_ids(INSTANCE_TAG_NAME, {}, region=REGION)
    if not ids:
        return []
    # The lib returns ids only; whether a box was DISPATCHED is in its tags.
    try:
        resp = boto3.client("ec2", region_name=REGION).describe_instances(
            InstanceIds=list(ids)
        )
    except Exception as exc:  # noqa: BLE001 - re-raised as SpotProbeError; never swallowed
        raise SpotProbeError(
            f"tag read for running thinktank boxes {ids} failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    return [
        {
            "instance_id": inst["InstanceId"],
            "launch_time": inst.get("LaunchTime"),
            "tags": {t["Key"]: t["Value"] for t in inst.get("Tags", [])},
        }
        for r in resp.get("Reservations", [])
        for inst in r.get("Instances", [])
    ]


def _adoptable_orphan(
    boxes: list[dict], *, request_id: str | None, timeout_bound_sec: float
) -> dict | None:
    """The running box whose dispatch provably died before the send, or None.

    None means "skip, as before": either a dispatched run is live, or nothing
    here is provably ours to finish. A box qualifies only if ALL of:

    * no box carries COMMAND_ID_TAG — a dispatched run is in flight, so a
      second one would be a duplicate, not a recovery;
    * it carries this dispatcher's run-token, TODAY's trading-day tag and a
      DISPATCH_REQUEST_ID_TAG (so a box launched before this recovery path
      existed is never adopted), and no termination-reason tag
      (terminate_on_failure is already tearing it down);
    * its dispatcher is provably dead — either this invocation is the async
      retry of the one that launched it (same request id), or the box is older
      than any invocation can live (``timeout_bound_sec``), so no dispatcher
      can still be mid-wait on it. A younger box launched by a DIFFERENT
      request id may belong to a live invocation (a duplicate EventBridge
      delivery, a manual invoke), and sending to it would double-run the box;
    * it is no older than ADOPT_WINDOW_SEC.

    Of several, the most recently launched wins.
    """
    if any(COMMAND_ID_TAG in b["tags"] for b in boxes):
        return None
    now = datetime.datetime.now(datetime.timezone.utc)
    today = _trading_day()
    candidates = []
    for b in boxes:
        tags = b["tags"]
        if not tags.get("thinktank-run-token") or not tags.get(DISPATCH_REQUEST_ID_TAG):
            continue
        if tags.get("thinktank-trading-day") != today:
            continue
        if spot_dispatch.TERMINATION_REASON_TAG in tags:
            continue
        launched = b.get("launch_time")
        if launched is None:
            continue
        age = (now - launched).total_seconds()
        if age > ADOPT_WINDOW_SEC:
            continue
        own_retry = bool(request_id) and tags.get(DISPATCH_REQUEST_ID_TAG) == request_id
        if own_retry or age > timeout_bound_sec:
            candidates.append(b)
    if not candidates:
        return None
    return max(candidates, key=lambda b: b["launch_time"])


def _remaining_sec(context) -> float | None:
    getter = getattr(context, "get_remaining_time_in_millis", None)
    if getter is None:
        return None
    return getter() / 1000.0


def _ssm_online_budget(context) -> int:
    """SSM_ONLINE_BUDGET_SEC, clamped to what this invocation can still afford.

    The clamp is what makes the I11532 class impossible rather than merely
    unlikely: whatever the configured timeout, the wait gives up with enough
    left to terminate the box and RAISE — so the async retry launches a fresh
    one — instead of Lambda killing the handler mid-wait with the box orphaned.
    """
    remaining = _remaining_sec(context)
    if remaining is None:
        return SSM_ONLINE_BUDGET_SEC
    affordable = int(remaining - INSTANCE_RUNNING_WAIT_MAX_SEC - DISPATCH_RESERVE_SEC)
    if affordable < SSM_ONLINE_BUDGET_SEC:
        logger.warning(
            "thinktank-spot: only %.0fs left in this invocation — clamping the SSM "
            "Online wait from %ss to %ss so it fails loud before Lambda's timeout "
            "(alpha-engine-config-I11532)",
            remaining, SSM_ONLINE_BUDGET_SEC, max(affordable, 0),
        )
    return max(min(SSM_ONLINE_BUDGET_SEC, affordable), 0)


def _record_dispatch(instance_id: str, command_id: str) -> bool:
    """Mark the box DISPATCHED (COMMAND_ID_TAG). Never raises.

    The command is already running when this is called, so a tagging failure
    must not RAISE: that would trigger an async retry which, finding the box
    untagged, would send the command a second time. Retried once, then logged.
    Needs no new IAM: the role's `CreateDiscriminatorTagsOnOwnBoxesOnly`
    statement already grants ec2:CreateTags on instances Name-tagged
    alpha-engine-thinktank-spot.
    """
    ec2 = boto3.client("ec2", region_name=REGION)
    for attempt in (1, 2):
        try:
            ec2.create_tags(
                Resources=[instance_id],
                Tags=[{"Key": COMMAND_ID_TAG, "Value": command_id}],
            )
            return True
        except Exception as exc:  # noqa: BLE001 - see docstring
            logger.error(
                "thinktank-spot: could not tag %s with %s=%s (attempt %d): %s: %s",
                instance_id, COMMAND_ID_TAG, command_id, attempt,
                type(exc).__name__, exc,
            )
    return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — launch the daily Think Tank box.

    ``event`` may carry ``{"force_on_demand": bool}``. Returns
    ``{"launched", "adopted", "instance_id", "command_id", "command_tagged",
    "market", "run_token", "dedupe_degraded"}``, or
    ``{"launched": False, "reason": "already_running", "instance_ids"}``.

    Fail-loud: a launch/SSM error RAISES so EventBridge's two async retries
    engage and the Lambda Errors metric drives the alarm.

    Retry-safe (alpha-engine-config-I11532): EventBridge's async retry re-enters
    here after an invocation that may have died ANYWHERE — including after the
    launch and before the send. A running box with no COMMAND_ID_TAG whose
    dispatcher provably died is therefore finished (``adopted``), not skipped;
    see ``_adoptable_orphan`` for exactly when.
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
        # Refuse to launch rather than run a box whose deadline SSM will
        # preempt — that silently reintroduces the lost-terminal-writes bug.
        raise ValueError(
            f"THINKTANK_RUN_BUDGET_SECONDS={RUN_BUDGET_SECONDS} must be strictly "
            f"below THINKTANK_SPOT_RUN_TIMEOUT_SECONDS={RUN_TIMEOUT_SECONDS}; "
            "otherwise SSM guillotines the run before its terminal writes "
            "(alpha-engine-config-I5208)"
        )

    request_id = getattr(context, "aws_request_id", None)
    # No invocation outlives the function's timeout, so a box older than this
    # cannot still have a live dispatcher waiting on it. The live timeout is
    # read from the context as well as the constant, so a hand-raised timeout
    # can only make adoption MORE conservative, never less.
    remaining_at_start = _remaining_sec(context)
    timeout_bound_sec = max(
        LAMBDA_TIMEOUT_SECONDS,
        math.ceil(remaining_at_start) if remaining_at_start is not None else 0,
    )

    dedupe_degraded = False
    try:
        running = _running_boxes()
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

    adopted = False
    if running:
        orphan = _adoptable_orphan(
            running, request_id=request_id, timeout_bound_sec=timeout_bound_sec
        )
        if orphan is None:
            ids = [b["instance_id"] for b in running]
            logger.warning(
                "thinktank-spot box already running (%s) — skipping this launch", ids
            )
            return {"launched": False, "reason": "already_running", "instance_ids": ids}
        # alpha-engine-config-I11532: the box exists but its dispatcher died
        # before sending the command (2026-09-23: Lambda timeout mid-SSM-wait).
        # Finish THAT dispatch — same box, same run token, so the completion
        # marker the reaper looks for is the one the box will write.
        instance_id = orphan["instance_id"]
        run_token = orphan["tags"]["thinktank-run-token"]
        market = "adopted"
        adopted = True
        logger.warning(
            "thinktank-spot box %s is running but was never dispatched (no %s tag; "
            "launched %s by request %s) — its dispatcher died before the send. "
            "Sending the command to it instead of skipping (alpha-engine-config-I11532)",
            instance_id, COMMAND_ID_TAG, orphan["launch_time"],
            orphan["tags"].get(DISPATCH_REQUEST_ID_TAG, "<untagged>"),
        )
    else:
        run_token = uuid.uuid4().hex
        if request_id:
            extra_tags[DISPATCH_REQUEST_ID_TAG] = request_id
        try:
            instance_id, market = _launch_instance(run_token, force_on_demand=force_on_demand, extra_tags=extra_tags or None)
        except SpotLaunchError:
            logger.error("thinktank-spot launch failed (spot + on-demand exhausted)")
            raise
        logger.info("launched thinktank-spot box %s (%s)", instance_id, market)

    # Between launch and the bootstrap command landing there is no watchdog or
    # trap on the box yet — anything failing in here would orphan it.
    try:
        spot_dispatch.wait_ssm_online(
            instance_id, region=REGION, ssm_online_budget_sec=_ssm_online_budget(context)
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

    # AFTER the send and outside the try: the command is running, so nothing
    # from here on may terminate the box or raise.
    command_tagged = _record_dispatch(instance_id, command_id)

    return {
        "launched": True,
        "adopted": adopted,
        "instance_id": instance_id,
        "command_id": command_id,
        "command_tagged": command_tagged,
        "market": market,
        "run_token": run_token,
        "dedupe_degraded": dedupe_degraded,
    }
