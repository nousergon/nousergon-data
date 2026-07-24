"""alpha-engine-canary-replay-dispatcher — launch the Saturday-replay canary
(alpha-engine-config#2246) on a dedicated EC2 spot box.

WHY: the Friday dry-preflight is structurally blind to every bug class that
fails Saturday's live run — `--preflight-only`/`--dry-run` never execute
`rag.pipelines.filing_change_detection` (data) or the real held-thesis-
update / qual-analyst / structured-output-retry paths (research) against
live data. This canary actually replays those 3 paths, on a schedule AND
per-PR, so a regression is caught before Saturday instead of during it.

TWO CALLERS, ONE DISPATCH PATH:
  1. EventBridge (Thursday cron) invokes this Lambda directly with
     `{"mode": "scheduled"}` — `research_ref`/`data_ref` default to `main`.
  2. A thin GHA shim on `crucible-research`/`nousergon-data` (gated by the
     `canary:replay` label) invokes this Lambda SYNCHRONOUSLY with
     `{"mode": "pr", "research_ref": ..., "data_ref": ..., "pr_number": ...}`
     — whichever repo's PR triggered the run supplies its own branch ref;
     the OTHER repo defaults to `main`. Brian's binding ruling on #2246: the
     canary ALWAYS runs all 3 probes together regardless of which repo
     changed — a per-repo PR still exercises the full weekly-critical path.

Mechanism (mirrors ci-watch-dispatcher/index.py's PROVEN async spot-dispatch
shape via the shared `nousergon_lib.spot_dispatch` primitives — every
dispatcher in this fleet is deliberately ASYNC: none bets a gating operation
on Lambda's 900s hard ceiling under spot-capacity/LLM-latency variance):
  1. `spot_dispatch.launch_with_fallback()` — spot first, on-demand fallback
     on capacity exhaustion across all pools.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an async, detached `ssm send-command` (AWS-RunShellScript) that
     fetches the fleet PAT from SSM, clones alpha-engine-config, then
     `exec`s `infrastructure/canary_replay_spot_bootstrap.sh` (built by a
     sibling agent in that repo) with the resolved refs/run-token/mode. The
     box self-terminates (InstanceInitiatedShutdownBehavior=terminate + its
     own on-box watchdog — config#1472 shape).

DETERMINISTIC run_token (the key divergence from ci-watch-dispatcher's
random uuid4): the SCHEDULED path derives `sched-{isoyear}w{isoweek:02d}`
from the current UTC date, and the PR path derives
`pr-{repo-with-slash-flattened}-{pr_number}-{sha[:12]}` from the event —
both WITHOUT any IPC. This lets `canary-replay-liveness-probe` (the
Thursday-path watchdog) independently compute the SAME S3 completion-marker
key it needs to poll, with no coupling to this Lambda beyond the shared
derivation function. It also gives natural at-most-one-live-marker
idempotency: a retried Thursday dispatch in the same ISO week, or a
re-pushed commit on the same PR/sha, overwrites the same key rather than
littering orphan markers.

STALE-MARKER GUARD (config-I2753, 2026-07-17): the same "overwrites the
same key" property above has a sharp edge — "overwrites" is only true once
the NEW box finishes and writes its own marker, which is minutes after
this Lambda returns. In between, a poller's first check can still find the
OLD marker from a prior attempt against the same token. `_launch_canary_
replay_spot()` therefore synchronously deletes any pre-existing marker at
`marker_key` (`_clear_stale_marker()`) before launching, so a poller can
never observe a leftover verdict from a previous dispatch; the launched
verdict also carries `dispatched_at` for a caller that wants an extra,
independent freshness check on top.

SYNCHRONOUS CONTRACT (mirrors ci-watch-dispatcher): every anticipated
failure mode — concurrency skip, spot+on-demand launch exhaustion, a
malformed event, a stale-marker clear failure, a post-launch SSM
failure — returns a clean, well-formed
`{"launched": false, "reason": ...}` rather than raising, so the per-PR
GHA job's synchronous `aws lambda invoke` gets an unambiguous JSON verdict
to branch on. Only a genuinely unexpected internal bug propagates as an
exception (which EventBridge's Thursday caller then retries, the correct
behavior for a scheduled invocation).

IAM PROFILE — deliberately NOT the shared trading-executor or dashboard
profile. Uses `alpha-engine-canary-replay-executor-profile` (a sibling
agent in alpha-engine-config), because the per-PR trigger path means this
box is reachable from PR content a contributor (not just a scheduled
trigger) can influence — blast-radius isolation matters more here than for
ci-watch/sf-watch.

Managed OUTSIDE CloudFormation (same as every sibling dispatcher):
operator-deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live
effect until the new code + IAM are deployed AND the GHA workflows/
EventBridge rule are wired.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone

import boto3
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import (
    SpotLaunchError,
    SpotProbeError,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: mirrors every other fleet dispatcher's safety valve. Default ON.
DISPATCH_ENABLED = os.environ.get("CANARY_REPLAY_DISPATCH_ENABLED", "true").lower() == "true"

# ── Spot launch config (env-overridable; IAM LOCKSTEP — see iam-policy.json's
# scoped ec2:RunInstances condition, which enumerates these same values;
# changing a default here WITHOUT updating iam-policy.json breaks RunInstances
# with UnauthorizedOperation at the next dispatch) ───────────────────────────
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "CANARY_REPLAY_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "CANARY_REPLAY_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("CANARY_REPLAY_AMI_ID", "ami-0c421724a94bba6d6")  # Amazon Linux 2023 x86_64
KEY_NAME = os.environ.get("CANARY_REPLAY_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("CANARY_REPLAY_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
IAM_PROFILE = os.environ.get(
    "CANARY_REPLAY_IAM_PROFILE", "alpha-engine-canary-replay-executor-profile"
)
VOLUME_SIZE_GB = int(os.environ.get("CANARY_REPLAY_VOLUME_SIZE_GB", "40"))

CANARY_REPLAY_TAG_NAME = "alpha-engine-canary-replay-spot"
CANARY_REPLAY_RUN_TOKEN_TAG_KEY = "canary-replay-run-token"
MARKER_BUCKET = os.environ.get("CANARY_REPLAY_MARKER_BUCKET", "alpha-engine-research")

CANARY_REPLAY_GH_PAT_SSM = os.environ.get(
    "CANARY_REPLAY_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
CANARY_REPLAY_CONFIG_REPO = os.environ.get(
    "CANARY_REPLAY_CONFIG_REPO", "nousergon/alpha-engine-config"
)
CANARY_REPLAY_CONFIG_BRANCH = os.environ.get("CANARY_REPLAY_CONFIG_BRANCH", "main")
# Nominal probe runtime ~5-10 min; generous headroom without the fleet's
# usual 2h dispatcher-level ceiling — this box's own work is small and
# bounded, unlike groom/ci-watch/sf-watch. Matches
# canary_replay_spot_bootstrap.sh's own MAX_RUNTIME_SECONDS default.
MAX_RUNTIME_SECONDS = int(os.environ.get("CANARY_REPLAY_MAX_RUNTIME_SECONDS", "2400"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("CANARY_REPLAY_SSM_ONLINE_BUDGET_SEC", "180"))
CW_LOG_GROUP = os.environ.get("CANARY_REPLAY_CW_LOG_GROUP", "/alpha-engine/canary-replay-spot")

# Defense-in-depth allowlists for event fields embedded verbatim into the SSM
# shell command below (mirrors ci-watch-dispatcher's _REPO_RE/_SHA_RE). These
# come from a GHA job (not raw external user input), but the same cheap
# regex check rules out shell-metacharacter injection outright.
_REF_RE = re.compile(r"^[A-Za-z0-9_./-]{1,200}$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
_PR_NUMBER_RE = re.compile(r"^[0-9]+$")
_MODE_RE = re.compile(r"^(scheduled|pr)$")


class _InvalidEvent(ValueError):
    """A required event field is missing or fails its allowlist."""


def _require(event: dict, key: str, pattern: "re.Pattern[str]") -> str:
    val = str(event.get(key) or "").strip()
    if not pattern.match(val):
        raise _InvalidEvent(f"missing/malformed {key!r} in event: {val!r}")
    return val


def _optional(event: dict, key: str, pattern: "re.Pattern[str]", default: str) -> str:
    val = str(event.get(key) or "").strip()
    if not val:
        return default
    if not pattern.match(val):
        raise _InvalidEvent(f"malformed {key!r} in event: {val!r}")
    return val


def _scheduled_run_token(now: datetime) -> str:
    """Deterministic per-ISO-week token — see module docstring. A retried
    Thursday dispatch in the same week overwrites the same marker key
    rather than orphaning a new one."""
    iso_year, iso_week, _ = now.isocalendar()
    return f"sched-{iso_year}w{iso_week:02d}"


def _pr_run_token(repo: str, pr_number: str, sha: str) -> str:
    """Deterministic per-(repo, PR, sha) token — see module docstring. A
    re-pushed commit gets a fresh token (new sha); re-running CI on the SAME
    commit (e.g. a manual re-run) overwrites the same marker key."""
    return f"pr-{repo.replace('/', '-')}-{pr_number}-{sha[:12]}"


def _resolve_event_fields(event: dict) -> tuple[str, str, str, str]:
    """Validate the caller's payload; raises _InvalidEvent on any
    missing/malformed field (caught once, at the handler, and converted to a
    clean launched:false — see module docstring's synchronous contract).

    Returns (mode, research_ref, data_ref, run_token).
    """
    mode = _require(event, "mode", _MODE_RE)
    research_ref = _optional(event, "research_ref", _REF_RE, default="main")
    data_ref = _optional(event, "data_ref", _REF_RE, default="main")

    if mode == "scheduled":
        run_token = _scheduled_run_token(datetime.now(timezone.utc))
        return mode, research_ref, data_ref, run_token

    # mode == "pr"
    repo = _require(event, "repo", _REPO_RE)
    pr_number = _require(event, "pr_number", _PR_NUMBER_RE)
    sha = _require(event, "sha", _SHA_RE)
    run_token = _pr_run_token(repo, pr_number, sha)
    return mode, research_ref, data_ref, run_token


def _bootstrap_command(research_ref: str, data_ref: str, run_token: str, mode: str) -> str:
    """The async SSM RunShellScript body: fetch PAT, clone config, exec
    canary_replay_spot_bootstrap.sh (built by a sibling agent in
    alpha-engine-config). Any prelude failure shuts the box down so a
    botched launch never idles (mirrors ci-watch-dispatcher's prelude
    fail() trap exactly)."""
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
export HOME=/root
fail() {{ echo "[canary-replay-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
dnf install -y -q git python3.12 python3.12-pip >/dev/null 2>&1 \
  || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {CANARY_REPLAY_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {CANARY_REPLAY_CONFIG_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{CANARY_REPLAY_CONFIG_REPO}.git" \
  /home/ec2-user/alpha-engine-config || fail "clone failed"
cd /home/ec2-user/alpha-engine-config
exec bash infrastructure/canary_replay_spot_bootstrap.sh \
  --research-ref "{research_ref}" --data-ref "{data_ref}" \
  --run-token "{run_token}" --mode "{mode}"
"""


def _launch_instance(run_token: str) -> tuple[str, str]:
    """Launch the canary-replay box; spot first, on-demand fallback on
    capacity exhaustion. Raises SpotLaunchError if both are exhausted —
    caught once by the caller and converted to a clean launched:false.

    The run_token discriminator tag is applied AS PART of the RunInstances
    call (``extra_tags``) rather than as a separate post-launch
    ``create_tags`` call — this closes the TOCTOU race between launch and
    tag write where a second, near-simultaneous dispatch for the same
    run_token could see no tagged instance yet and launch a duplicate box.
    """
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=CANARY_REPLAY_TAG_NAME,
        region=REGION,
        extra_tags={CANARY_REPLAY_RUN_TOKEN_TAG_KEY: run_token},
    )


def _wait_ssm_online(instance_id: str) -> None:
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(instance_id: str, research_ref: str, data_ref: str,
                    run_token: str, mode: str) -> str:
    """Fire the async, detached SSM command that runs the canary + self-terminates."""
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(research_ref, data_ref, run_token, mode),
        comment=f"canary-replay ({mode}, token {run_token[:24]})",
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=MAX_RUNTIME_SECONDS,
    )


def _running_canary_instance_ids(run_token: str) -> list[str]:
    """Instance ids for a LIVE (pending/running) canary box already working
    THIS exact run_token — prevents a double-dispatch for the same
    ISO-week/PR-sha. Raises SpotProbeError when the probe itself fails; the
    caller degrades to launch-with-dedupe_degraded, never a silent
    fail-open []."""
    return spot_dispatch.running_instance_ids(
        CANARY_REPLAY_TAG_NAME,
        {CANARY_REPLAY_RUN_TOKEN_TAG_KEY: run_token},
        region=REGION,
    )


def _terminate_instance(instance_id: str) -> None:
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="canary-replay")


def _clear_stale_marker(run_token: str) -> None:
    """Delete any pre-existing completion marker at this run_token's S3 key
    BEFORE launching a new box (config-I2753 fix).

    run_token is deterministic per (repo, PR, sha) / per ISO-week (module
    docstring's "DETERMINISTIC run_token" section) — a re-dispatch against
    an unchanged commit/week (e.g. a retriggered PR whose head sha hasn't
    moved, or a manual retry within the same ISO week) reuses the EXACT
    SAME marker_key a prior attempt used. Without clearing it first, the
    per-PR GHA poller's very first `head-object` check can find a STALE
    marker left over from a PRIOR (possibly hours/days-old, possibly
    since-fixed-cause) failed attempt and report THAT as this dispatch's
    verdict — while the box just launched hasn't even finished booting.
    Root-caused live 2026-07-17 on crucible-research#444: the GHA "Poll for
    completion marker" step found and reported a marker with
    finished_at ~23h in the past (from the 2026-07-16 Neon-quota-outage
    era) within 2 seconds of dispatch, while the freshly-launched box went
    on to genuinely PASS ~5.5 minutes later — too late, the job had already
    failed the check.

    S3 delete-then-read is strongly read-after-write consistent (no
    eventual-consistency window since Dec 2020), so once this call
    returns, any marker a poller subsequently finds at this key is
    guaranteed to have been written by THIS dispatch, not a leftover. A
    missing key is not an error (idempotent no-op — DeleteObject on an
    absent key is a normal 204, boto3 does not raise). Any OTHER S3
    failure propagates (fail-loud): a silently-kept stale marker produces
    a WRONG verdict, which is worse than a failed dispatch that produces no
    verdict at all — the caller converts this to a clean launched:false
    rather than proceeding into launching a box whose result nobody can
    trust to be freshly reported.
    """
    boto3.client("s3", region_name=REGION).delete_object(
        Bucket=MARKER_BUCKET, Key=f"tmp/canary/{run_token}.json"
    )


def _launch_canary_replay_spot(mode: str, research_ref: str, data_ref: str,
                               run_token: str) -> dict:
    """Launch + bootstrap the canary box. SYNCHRONOUS contract: every
    anticipated failure mode returns a clean, well-formed launched:false
    rather than raising — see module docstring."""
    if not DISPATCH_ENABLED:
        logger.warning("CANARY_REPLAY_DISPATCH_ENABLED=false — canary spot NOT launched")
        return {"launched": False, "reason": "disabled", "run_token": run_token}

    dedupe_degraded = False
    dedupe_probe_error = ""
    try:
        existing = _running_canary_instance_ids(run_token)
    except SpotProbeError as exc:
        dedupe_degraded = True
        dedupe_probe_error = f"{type(exc).__name__}: {exc}"
        existing = []
        logger.error(
            "canary-replay concurrency probe FAILED for token=%s — proceeding to "
            "launch with dedupe_degraded=true: %s", run_token, dedupe_probe_error,
        )
    if existing:
        logger.warning(
            "canary-replay box already live for token=%s (%s) — skipping launch",
            run_token, existing,
        )
        return {"launched": False, "reason": "concurrent_skip",
                "existing_instance_ids": existing, "run_token": run_token}

    try:
        _clear_stale_marker(run_token)
    except Exception as exc:  # noqa: BLE001 — converted to a clean launched:false
        logger.error(
            "canary-replay marker-clear FAILED for token=%s — aborting dispatch "
            "rather than risk a poller reading a stale marker: %s: %s",
            run_token, type(exc).__name__, exc,
        )
        return {"launched": False, "reason": "marker_clear_failed",
                "error": f"{type(exc).__name__}: {exc}", "run_token": run_token,
                "dedupe_degraded": dedupe_degraded}

    dispatched_at = time.time()
    try:
        instance_id, market = _launch_instance(run_token)
    except SpotLaunchError as exc:
        logger.error("canary-replay spot launch failed: %s: %s", type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc),
                "run_token": run_token}

    logger.info("launched canary-replay box %s (%s) for token=%s%s", instance_id, market,
                run_token, " dedupe_degraded=true" if dedupe_degraded else "")

    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(instance_id, research_ref, data_ref, run_token, mode)
    except Exception as exc:  # noqa: BLE001 — converted to a clean launched:false
        _terminate_instance(instance_id)
        logger.error("canary-replay post-launch step failed for %s: %s: %s",
                     instance_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "post_launch_failed",
                "instance_id": instance_id, "error": str(exc),
                "run_token": run_token, "dedupe_degraded": dedupe_degraded}

    logger.info(
        "canary-replay dispatched: instance=%s market=%s command=%s mode=%s "
        "research_ref=%s data_ref=%s run_token=%s dedupe_degraded=%s",
        instance_id, market, command_id, mode, research_ref, data_ref, run_token,
        dedupe_degraded,
    )
    verdict = {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "mode": mode,
        "research_ref": research_ref,
        "data_ref": data_ref,
        "run_token": run_token,
        "marker_key": f"tmp/canary/{run_token}.json",
        # config-I2753 defense-in-depth: the primary fix is
        # _clear_stale_marker() above, but a caller that independently
        # wants to double-check freshness (belt-and-suspenders against any
        # future variant of the same staleness class) can reject any
        # marker whose finished_at predates this dispatch.
        "dispatched_at": dispatched_at,
        "dedupe_degraded": dedupe_degraded,
    }
    if dedupe_degraded:
        verdict["dedupe_probe_error"] = dedupe_probe_error
    return verdict


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Entrypoint for both callers — see module docstring. `event` carries
    `{"mode": "scheduled"}` (EventBridge Thursday cron) or
    `{"mode": "pr", "repo", "pr_number", "sha", "research_ref"?, "data_ref"?}`
    (GHA per-PR shim, synchronous invoke).

    Returns {"launched": bool, "reason": str, "run_token": str, ...} — the
    per-PR GHA job reads this DIRECTLY as its success signal and then polls
    `marker_key` for the probe verdict. Every anticipated failure (malformed
    event, concurrency skip, spot+on-demand launch exhaustion, post-launch
    SSM failure) is a clean return, never an exception — see module
    docstring's synchronous contract.
    """
    event = event or {}
    try:
        mode, research_ref, data_ref, run_token = _resolve_event_fields(event)
    except _InvalidEvent as exc:
        logger.error("invalid canary-replay event: %s", exc)
        return {"launched": False, "reason": "invalid_event", "error": str(exc)}

    logger.info(
        "canary-replay trigger: mode=%s research_ref=%s data_ref=%s run_token=%s",
        mode, research_ref, data_ref, run_token,
    )
    return _launch_canary_replay_spot(mode, research_ref, data_ref, run_token)
