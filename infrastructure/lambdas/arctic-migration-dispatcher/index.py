"""alpha-engine-arctic-migration-dispatcher — launch the in-region ArcticDB
schema-migration runner on a dedicated EC2-spot box, once per push to
``nousergon-data`` main that touches ``migrations/**`` (alpha-engine-config-
I3242, runner half of the config-I3236 structural fix — pairs with the
already-merged framework, nousergon-data-PR988).

Mechanism: mirrors the fleet's proven `nousergon_lib.spot_dispatch` shape
(config#2106) that sf-watch-spot-dispatcher / data-spot-dispatcher already
use — no bespoke sixth copy of the concurrency-lock/launch-with-fallback/
terminate-on-failure primitives:
  1. `spot_dispatch.launch_with_fallback()` rotates instance_type x subnet on
     capacity error; on SpotCapacityExhausted/SpotQuotaExceededError across
     all pools we relaunch ON-DEMAND — a merge-triggered migration must not
     be starved by a capacity dip (the whole point is "merge triggers the
     migration with zero operator action").
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an async, detached `ssm send-command` (AWS-RunShellScript): fetch
     the PAT, clone `nousergon-data` at the EXACT merged SHA (not just a
     branch — the runner must migrate precisely the code that was merged),
     build a venv from requirements.txt, run
     `scripts/run_arctic_migrations.py`. The box self-terminates
     (InstanceInitiatedShutdownBehavior=terminate + an in-script watchdog,
     mirroring every sibling spot bootstrap).

SYNCHRONOUS CONTRACT (identical to sf-watch-spot-dispatcher's /
ci-watch-dispatcher's): the GHA workflow invokes this Lambda with
RequestResponse (not async) and branches directly on the returned JSON.
Every anticipated failure mode — concurrency skip, a degraded concurrency
probe, spot+on-demand launch exhaustion, a malformed event, a post-launch SSM
failure — returns a clean, well-formed `{"launched": false, "reason": ...}`
rather than raising. Only a genuinely unexpected internal bug should still
propagate as a Python exception.

CONCURRENCY LOCK — DELIBERATELY FAIL-CLOSED (posture differs from sf-watch's
"coverage beats dedupe"): keyed on `Name=alpha-engine-arctic-migration-spot`
+ `arctic-migration-head=<NNNN>` (the head migration number at the merged
SHA — the full identifying key: two different heads racing must each get
their own box, but the SAME head must never get two). Unlike sf-watch's
site-1 policy (a broken duplicate-box probe still launches, because an
unwatched SF failure is the worse outcome), a migration full-`write_batch`-
rewrites EVERY `universe` symbol — TWO boxes racing the same head is a real
correctness risk (interleaved partial rewrites), not just an efficiency
concern. So a probe failure here returns a clean `probe_failed` verdict
(no launch) rather than proceeding — refusing to guess, mirroring the
identical fail-closed posture the on-box runner itself takes toward the
live-trading-pipeline mutex (see scripts/run_arctic_migrations.py's
module docstring). The GHA caller escalates `probe_failed` to a P1 like
every other non-benign reason.

NO TELEGRAM FROM THIS LAMBDA: mirrors sf-watch-spot-dispatcher's own
posture — the synchronous GHA caller reads the return value directly for its
own P1-filing fallback, and the on-box runner (scripts/run_arctic_migrations.py)
sends the richer, outcome-specific Telegram receipt once the actual migration
work concludes. This Lambda's job is launch only.

IAM PROFILE — reuses `alpha-engine-executor-profile` (the SAME profile
data-spot-dispatcher's box already runs under), NOT a new profile: the issue's
own gotcha says to reuse an existing data-plane profile when scopes match
rather than minting a new IAM surface, and this profile already carries the
ArcticDB S3 read/write the migration rewrite needs. If a future migration
needs a broader grant (e.g. the flow-doctor DynamoDB dedup store — see the
runner's Telegram notify path, which degrades gracefully without it today),
that is a SEPARATE operator IAM step, called out in the PR body, not bundled
here.

Managed OUTSIDE CloudFormation (same as every sibling dispatcher): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR that ships this file has
ZERO live effect until the Lambda + IAM are deployed AND
`run-arctic-migrations.yml` is live (which itself is inert with a 404 until
this function exists) — see the PR body's deploy plan.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import (
    SpotCapacityExhausted,
    SpotLaunchError,
    SpotProbeError,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: ARCTIC_MIGRATION_DISPATCH_ENABLED=false disables the launch
# without touching the GHA invoke wiring — mirrors every other fleet
# dispatcher's safety valve. Default ON.
DISPATCH_ENABLED = (
    os.environ.get("ARCTIC_MIGRATION_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config (env-overridable; defaults mirror data-spot-dispatcher —
# same c5-family/subnet/AMI/SG conventions; a full universe rewrite is a
# CPU+disk-bound batch job like the data-phase workloads, not latency-bound). ──
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "ARCTIC_MIGRATION_INSTANCE_TYPES", "c5.large,c5a.large,m5.large"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "ARCTIC_MIGRATION_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("ARCTIC_MIGRATION_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("ARCTIC_MIGRATION_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("ARCTIC_MIGRATION_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# Reuse the executor profile (ArcticDB S3 read/write already granted) — see
# module docstring's IAM PROFILE section.
IAM_PROFILE = os.environ.get("ARCTIC_MIGRATION_IAM_PROFILE", "alpha-engine-executor-profile")
VOLUME_SIZE_GB = int(os.environ.get("ARCTIC_MIGRATION_VOLUME_SIZE_GB", "60"))

MIGRATION_REPO = os.environ.get("ARCTIC_MIGRATION_REPO", "nousergon/nousergon-data")
CONFIG_REPO = os.environ.get("ARCTIC_MIGRATION_CONFIG_REPO", "nousergon/alpha-engine-config")
CONFIG_BRANCH = os.environ.get("ARCTIC_MIGRATION_CONFIG_BRANCH", "main")
GH_PAT_SSM = os.environ.get(
    "ARCTIC_MIGRATION_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
# A full ~900-symbol write_batch rewrite is heavier than a single-workload
# data-spot run; sized with headroom above the data-spot-dispatcher's 7200s.
MAX_RUNTIME_SECONDS = int(os.environ.get("ARCTIC_MIGRATION_MAX_RUNTIME_SECONDS", "9000"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("ARCTIC_MIGRATION_SSM_ONLINE_BUDGET_SEC", "300"))
CW_LOG_GROUP = os.environ.get("ARCTIC_MIGRATION_CW_LOG_GROUP", "/alpha-engine/arctic-migration-spot")

# ── Defer-not-drop config (config#2226, mirroring sf-watch-spot-dispatcher) ──
# When a launched box writes `refused_mutex_active`, the defer cycle re-checks
# the marker periodically. On each deferred re-check that still finds mutex
# held, we launch a FRESH box (which re-evaluates the mutex against live state)
# AND create a one-shot safety-net schedule for the next check. On exhaustion
# (gen >= DEFER_MAX_GENERATION) the migration escalates to a P1.
DEFER_DELAY_SECONDS = int(os.environ.get("ARCTIC_MIGRATION_DEFER_DELAY_SECONDS", "900"))  # 15 min
DEFER_MAX_GENERATION = int(os.environ.get("ARCTIC_MIGRATION_DEFER_MAX_GENERATION", "3"))
DEFER_ROLE_ARN = os.environ.get("ARCTIC_MIGRATION_DEFER_ROLE_ARN", "")
DEFER_ROLE_NAME = "alpha-engine-arctic-migration-defer-scheduler-role"
DEFER_SCHEDULE_GROUP = os.environ.get("ARCTIC_MIGRATION_DEFER_SCHEDULE_GROUP", "default")
COMPLETION_BUCKET = os.environ.get("ARCTIC_MIGRATION_COMPLETION_BUCKET", "alpha-engine-research")

TAG_NAME = "alpha-engine-arctic-migration-spot"
HEAD_TAG_KEY = "arctic-migration-head"

# Defense-in-depth allowlists for event fields embedded verbatim into the
# constructed SSM shell command (mirrors sf-watch-spot-dispatcher's
# _PIPELINE_RE/_SHA_RE-style guards). These come from a GHA job, not raw
# external input, but the same cheap regex rules out shell-metacharacter
# injection outright.
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_HEAD_RE = re.compile(r"^\d{1,6}$")


class _InvalidEvent(ValueError):
    """A required event field is missing or fails its allowlist."""


def _resolve_event_fields(event: dict) -> dict:
    merged_sha = str(event.get("merged_sha") or "").strip().lower()
    if not _SHA_RE.match(merged_sha):
        raise _InvalidEvent(f"missing/malformed 'merged_sha' in event: {merged_sha!r}")
    head_raw = str(event.get("head_migration_number") if event.get("head_migration_number") is not None else "").strip()
    if not _HEAD_RE.match(head_raw):
        raise _InvalidEvent(
            f"missing/malformed 'head_migration_number' in event: {head_raw!r}"
        )
    return {"merged_sha": merged_sha, "head_migration_number": int(head_raw)}


# ── Defer-not-drop helpers (mirror sf-watch-spot-dispatcher's config#2226 pattern) ──


def _completion_marker_key(head_migration_number: int) -> str:
    """S3 key for the migration run's terminal-state marker (mirrors
    scripts/run_arctic_migrations.py's ``completion_marker_key``)."""
    return (
        f"overseer/_control/completed/"
        f"arctic-migration-{head_migration_number:04d}.json"
    )


def _defer_schedule_name(head_migration_number: int, generation: int) -> str:
    """Deterministic one-shot schedule name for (head, generation) — the
    determinism IS the idempotency lock: a duplicate defer attempt for the same
    key+generation hits ConflictException and is treated as already-deferred.
    EventBridge Scheduler caps Name at 64 chars; the readable form overflows
    for 6-digit head numbers (51 chars — fine), but the digest fallback is
    here for completeness, mirroring sf-watch-spot-dispatcher's same pattern."""
    name = f"arctic-migration-defer-{head_migration_number:04d}-g{generation}"
    if len(name) <= 64:
        return name
    digest = hashlib.sha256(
        f"arctic-migration-{head_migration_number}".encode("utf-8")
    ).hexdigest()[:16]
    return f"arctic-migration-defer-{digest}-g{generation}"


def _is_scheduler_conflict(exc: Exception) -> bool:
    """True when the Scheduler API says the schedule already exists — matched
    by exception class name AND botocore error code (covers both the real
    boto3 ConflictException class and a generic ClientError carrying it)."""
    if type(exc).__name__ == "ConflictException":
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return response.get("Error", {}).get("Code") == "ConflictException"
    return False


def _read_completion_marker(
    head_migration_number: int,
    bucket: str = COMPLETION_BUCKET,
    region: str = REGION,
) -> dict | None:
    """Read the migration-run completion marker from S3. Returns None if
    the marker doesn't exist yet (box still running or never wrote one) or
    if the read fails (logged, non-fatal)."""
    import boto3  # defer import: not needed on every invocation

    key = _completion_marker_key(head_migration_number)
    try:
        resp = boto3.client("s3", region_name=region).get_object(
            Bucket=bucket, Key=key
        )
        return json.loads(resp["Body"].read().decode("utf-8"))
    except boto3.client("s3").exceptions.NoSuchKey:
        return None
    except Exception as exc:  # noqa: BLE001 — non-fatal; caller treats None as "in flight"
        logger.warning(
            "completion marker read FAILED for s3://%s/%s (non-fatal, treating "
            "as in-flight): %s: %s",
            bucket, key, type(exc).__name__, exc,
        )
        return None


def _schedule_defer_check(
    fields: dict, generation: int, context,
) -> tuple[str | None, str]:
    """Create a ONE-SHOT EventBridge Scheduler schedule that re-invokes this
    same Lambda with ``defer_generation=generation`` after
    DEFER_DELAY_SECONDS, so the deferred invocation can re-check the
    completion marker and decide whether to launch a fresh box or escalate.
    Returns (schedule_name, fire_at_iso) on success, or (None, error_message)
    on failure — the caller returns `defer_schedule_failed` for the latter.

    ``_is_scheduler_conflict`` idempotency: a duplicate call for the same
    (head, generation) returns the existing schedule name with
    ``already_scheduled=True`` as an info-logged no-op (the earlier schedule
    already covers this re-check)."""
    schedule_name = _defer_schedule_name(
        fields["head_migration_number"], generation,
    )
    function_arn = str(getattr(context, "invoked_function_arn", "") or "")
    if not function_arn:
        logger.error(
            "defer schedule FAILED for head %d gen %d: "
            "no invoked_function_arn on the Lambda context",
            fields["head_migration_number"], generation,
        )
        return None, "no invoked_function_arn on context"

    # Target the UNQUALIFIED function ARN so the deferred invoke always runs
    # the live code (mirrors sf-watch-spot-dispatcher).
    target_arn = ":".join(function_arn.split(":")[:7])
    role_arn = DEFER_ROLE_ARN or (
        f"arn:aws:iam::{function_arn.split(':')[4]}:role/{DEFER_ROLE_NAME}"
    )

    payload = {
        "merged_sha": fields["merged_sha"],
        "head_migration_number": fields["head_migration_number"],
    }
    payload["defer_generation"] = generation
    fire_at = datetime.now(timezone.utc) + timedelta(seconds=DEFER_DELAY_SECONDS)

    import boto3  # noqa: F811 — defer import (not needed every invocation)

    try:
        boto3.client("scheduler", region_name=REGION).create_schedule(
            Name=schedule_name,
            GroupName=DEFER_SCHEDULE_GROUP,
            # at() with no ScheduleExpressionTimezone is UTC — matches fire_at.
            ScheduleExpression=f"at({fire_at.strftime('%Y-%m-%dT%H:%M:%S')})",
            FlexibleTimeWindow={"Mode": "OFF"},
            ActionAfterCompletion="DELETE",  # one-shot: self-deletes after firing
            Description=(
                f"arctic-migration defer-not-drop re-check (config#2226): head "
                f"{fields['head_migration_number']:04d} generation {generation}"
            ),
            Target={
                "Arn": target_arn,
                "RoleArn": role_arn,
                "Input": json.dumps(payload),
                "RetryPolicy": {
                    "MaximumRetryAttempts": 3,
                    "MaximumEventAgeInSeconds": 3600,
                },
            },
        )
    except Exception as exc:  # noqa: BLE001 — synchronous contract: clean JSON verdict, never raise
        if _is_scheduler_conflict(exc):
            logger.info(
                "defer schedule %s already exists — treating as already-scheduled "
                "(carries forward the prior re-check window)",
                schedule_name,
            )
            # Return the schedule_name as a signal so the caller can surface
            # it in the verdict; the concurrent creation is benign.
            return schedule_name, "already_exists"
        logger.error(
            "defer schedule creation FAILED for %s (%s: %s) — this defer "
            "cycle will NOT auto-retry; escalation applies",
            schedule_name, type(exc).__name__, exc,
        )
        return None, f"{type(exc).__name__}: {exc}"

    logger.warning(
        "defer schedule %s created: re-invokes at %sZ (head %04d, gen %d)",
        schedule_name, fire_at.strftime("%Y-%m-%dT%H:%M:%S"),
        fields["head_migration_number"], generation,
    )
    return schedule_name, fire_at.isoformat()


# ---- Deferred-invocation handler (called by the one-shot schedule) ---------


def _handle_deferred(fields: dict, defer_generation: int, context) -> dict:
    """Handle a deferred re-invoke: read the completion marker and decide
    whether to launch a fresh box (mutex likely cleared), defer again, or
    escalate to P1 on exhaustion.

    Returns a terminal verdict dict — the caller (``handler``) returns it
    directly without entering the normal launch flow."""
    marker = _read_completion_marker(fields["head_migration_number"])

    if marker is None:
        # No marker yet — the original (or a prior deferred) box is still
        # running (or never wrote one). Don't interfere; return recovered.
        logger.info(
            "deferred check (head %04d, gen %d): no completion marker yet — "
            "migration in flight, treating as recovered",
            fields["head_migration_number"], defer_generation,
        )
        return {
            "launched": False, "reason": "recovered",
            "marker_state": None, "defer_generation": defer_generation,
        }

    marker_state = str(marker.get("state") or "unknown")
    if marker_state != "refused_mutex_active":
        # The migration reached a different terminal state (success/failure/
        # noop_up_to_date/refused_mutex_probe_failed) — no further action
        # needed from the defer cycle.
        logger.info(
            "deferred check (head %04d, gen %d): marker state=%s — recovered",
            fields["head_migration_number"], defer_generation, marker_state,
        )
        return {
            "launched": False, "reason": "recovered",
            "marker_state": marker_state, "defer_generation": defer_generation,
        }

    # Marker still says refused_mutex_active.
    if defer_generation >= DEFER_MAX_GENERATION:
        logger.error(
            "arctic-migration defer EXHAUSTED at generation %d for head %04d "
            "(merged_sha=%s) — mutex still held after %d attempts; manual "
            "triage and re-trigger needed",
            defer_generation, fields["head_migration_number"],
            fields["merged_sha"], defer_generation,
        )
        return {
            "launched": False, "reason": "defer_exhausted",
            "defer_generation": defer_generation,
            "head_migration_number": fields["head_migration_number"],
            "merged_sha": fields["merged_sha"],
        }

    # Create the NEXT safety-net schedule BEFORE launching, so a launch-
    # time failure doesn't drop the retry chain.
    sched_name, sched_info = _schedule_defer_check(
        fields, defer_generation + 1, context,
    )
    if sched_name is None:
        # Schedule creation failed — the defer chain is broken. Log loud
        # and return defer_schedule_failed so the caller can escalate.
        return {
            "launched": False, "reason": "defer_schedule_failed",
            "error": sched_info, "defer_generation": defer_generation,
            "head_migration_number": fields["head_migration_number"],
        }

    # Safety-net schedule is in place. Now try to launch a fresh box that
    # will re-evaluate the mutex against LIVE pipeline state.
    logger.warning(
        "deferred (head %04d, gen %d): marker still refuses — launching "
        "fresh box; safety-net schedule %s re-checks at gen %d",
        fields["head_migration_number"], defer_generation,
        sched_name, defer_generation + 1,
    )
    # Fall through to the normal launch path below.
    return None  # signal to handler: proceed with normal launch

def _bootstrap_command(fields: dict) -> str:
    """The async SSM RunShellScript body: fetch PAT, clone nousergon-data at
    the EXACT merged SHA (never a branch tip — the runner must migrate
    precisely the code that was merged, and main may have moved on by the
    time the box boots), build a venv, run the migration runner script,
    self-terminate on any prelude failure so a botched launch never idles."""
    merged_sha = fields["merged_sha"]
    head = fields["head_migration_number"]
    log = "/var/log/arctic-migration.log"
    s3_log = (
        f"s3://alpha-engine-research/_ssm_logs/arctic-migration/"
        f"$(date -u +%Y-%m-%d)/$(hostname)-$(date -u +%H%M%S)-{merged_sha[:12]}.log"
    )
    return f"""set -uo pipefail
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/home/ec2-user/.cache
export AWS_REGION={REGION}
export AWS_DEFAULT_REGION={REGION}
fail() {{ echo "[arctic-migration-prelude] FATAL: $1"; aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true; shutdown -h now; exit 1; }}
systemd-run --on-active={MAX_RUNTIME_SECONDS} --unit=alpha-engine-arctic-migration-watchdog \\
  --description='alpha-engine arctic-migration spot hard-timeout' /sbin/shutdown -h now || true
dnf install -y -q python3.12 python3.12-pip python3.12-devel git gcc >/dev/null 2>&1 \\
  || dnf install -y -q python3 python3-pip python3-devel git gcc >/dev/null 2>&1 \\
  || fail "runtime install failed"
command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3
command -v gh >/dev/null 2>&1 || {{ dnf install -y 'dnf-command(config-manager)' >/dev/null 2>&1 || true; dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo >/dev/null 2>&1 || true; dnf install -y -q gh >/dev/null 2>&1 || echo "[arctic-migration-prelude] WARN: gh install failed (P1-on-crash filing will no-op)"; }}
git config --global --add safe.directory '*' || true
PAT=$(aws ssm get-parameter --name {GH_PAT_SSM} --with-decryption \\
  --query Parameter.Value --output text --region {REGION}) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
rm -rf /home/ec2-user/nousergon-data
git clone --quiet "https://x-access-token:${{PAT}}@github.com/{MIGRATION_REPO}.git" \\
  /home/ec2-user/nousergon-data || fail "clone failed"
cd /home/ec2-user/nousergon-data
git fetch --quiet --depth 1 origin {merged_sha} || fail "fetch of merged_sha failed"
git checkout --quiet {merged_sha} || fail "checkout of merged_sha failed"
"$PYTHON_BIN" -m venv .venv || fail "venv create failed"
source .venv/bin/activate
pip install --upgrade pip -q || fail "pip upgrade failed"
pip install -q -r requirements.txt || fail "deps install failed"
pip install -q 'numpy<2' || fail "numpy pin failed"
export GH_TOKEN="$PAT"
mkdir -p "$(dirname {log})"
set +e
python scripts/run_arctic_migrations.py --merged-sha {merged_sha} \\
  --head-migration-number {head} 2>&1 | tee -a {log}
rc=${{PIPESTATUS[0]}}
set -e
aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true
[ "$rc" -eq 0 ] || fail "migration runner exited $rc"
echo "[arctic-migration] head {head} complete"
"""


def _launch_instance() -> tuple[str, str]:
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=TAG_NAME,
        region=REGION,
    )


def _wait_ssm_online(instance_id: str) -> None:
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(fields: dict, instance_id: str) -> str:
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(fields),
        comment=(
            f"arctic-migration head={fields['head_migration_number']} "
            f"sha={fields['merged_sha'][:12]}"
        ),
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=MAX_RUNTIME_SECONDS,
    )


def _running_instance_ids(head_migration_number: int) -> list[str]:
    return spot_dispatch.running_instance_ids(
        TAG_NAME, {HEAD_TAG_KEY: str(head_migration_number)}, region=REGION,
    )


def _terminate_instance(instance_id: str) -> None:
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="arctic-migration")


def handler(event: dict, context) -> dict:
    """Synchronous handler invoked once per push to nousergon-data main that
    touches migrations/** (`.github/workflows/run-arctic-migrations.yml`'s
    `lambda invoke --invocation-type RequestResponse`). ``event`` carries
    ``{"merged_sha": <40-hex sha>, "head_migration_number": <int>}``.
    Returns ``{"launched": bool, "reason": str, ...}`` — read DIRECTLY by the
    GHA job as its branch signal. Every anticipated failure mode is a clean
    return, never an exception — see module docstring's synchronous contract.

    DEFERRED INVOCATIONS (config#2226, this issue's auto-retry): a
    one-shot EventBridge Scheduler schedule fires with the same payload plus
    ``defer_generation: <int>``. On deferred re-invoke, ``_handle_deferred``
    reads the S3 completion marker and decides whether to launch a fresh box
    (mutex likely cleared), defer again (mutex still held, generation < max),
    or escalate (generation exhausted)."""
    event = event or {}

    # Parse defer_generation (0 or absent = normal invocation)
    try:
        defer_generation = int(str(event.get("defer_generation") or 0))
        if defer_generation < 0:
            raise ValueError(defer_generation)
    except ValueError:
        logger.error("invalid arctic-migration event: malformed defer_generation %r",
                      event.get("defer_generation"))
        return {"launched": False, "reason": "invalid_event",
                "error": f"malformed defer_generation: {event.get('defer_generation')!r}"}

    if not DISPATCH_ENABLED:
        logger.warning("ARCTIC_MIGRATION_DISPATCH_ENABLED=false — migration spot NOT launched")
        return {"launched": False, "reason": "disabled"}

    try:
        fields = _resolve_event_fields(event)
    except _InvalidEvent as exc:
        logger.error("invalid arctic-migration event: %s", exc)
        return {"launched": False, "reason": "invalid_event", "error": str(exc)}

    # Deferred re-invoke: read completion marker, decide next action.
    if defer_generation >= 1:
        verdict = _handle_deferred(fields, defer_generation, context)
        if verdict is not None:
            # One of: recovered, defer_exhausted, or defer_schedule_failed
            return verdict
        # verdict is None -> fall through to normal launch (the safety-net
        # schedule was already created by _handle_deferred above).

    head = fields["head_migration_number"]

    # FAIL-CLOSED concurrency probe (see module docstring: posture differs
    # deliberately from sf-watch's coverage-beats-dedupe — two boxes racing
    # the SAME head is a correctness risk, not just an efficiency concern).
    try:
        existing = _running_instance_ids(head)
    except SpotProbeError as exc:
        logger.error(
            "concurrency probe FAILED for head=%d — refusing to launch (fail-closed, "
            "unlike sf-watch's coverage-beats-dedupe posture): %s", head, exc,
        )
        return {"launched": False, "reason": "probe_failed", "error": str(exc)}

    if existing:
        logger.info(
            "arctic-migration box already live for head=%d (%s) — concurrent_skip "
            "(duplicate dispatch of identical already-merged work, safe to skip)",
            head, existing,
        )
        return {
            "launched": False, "reason": "concurrent_skip",
            "existing_instance_ids": existing, "head_migration_number": head,
        }

    try:
        instance_id, market = _launch_instance()
    except SpotLaunchError as exc:
        logger.error("arctic-migration spot launch failed: %s: %s", type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched arctic-migration box %s (%s) for head=%d", instance_id, market, head)

    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(fields, instance_id)
    except Exception as exc:  # noqa: BLE001 — converted to a clean launched:false
        _terminate_instance(instance_id)
        logger.error(
            "arctic-migration post-launch step failed for %s: %s: %s",
            instance_id, type(exc).__name__, exc,
        )
        return {
            "launched": False, "reason": "post_launch_failed",
            "instance_id": instance_id, "error": str(exc),
        }

    logger.info(
        "arctic-migration dispatched: instance=%s market=%s command=%s head=%d sha=%s",
        instance_id, market, command_id, head, fields["merged_sha"],
    )

    # Arm the defer-not-drop safety-net (config#2226): create a one-shot
    # schedule that re-checks the completion marker in DEFER_DELAY_SECONDS.
    # For the FIRST launch (defer_generation == 0) we create the initial
    # schedule; for deferred re-launches the safety-net was already created
    # by _handle_deferred above. Non-fatal: a schedule-creation failure is
    # logged but does NOT flip launched to false — the primary deliverable
    # (the migration box being in flight) is already achieved.
    if defer_generation == 0:
        _schedule_defer_check(fields, 1, context)

    return {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "head_migration_number": head,
        "merged_sha": fields["merged_sha"],
    }
