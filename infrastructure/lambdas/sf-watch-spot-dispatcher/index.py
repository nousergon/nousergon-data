"""alpha-engine-sf-watch-spot-dispatcher — launch the Fleet-SF Watch
diagnose+fix+rerun agent on a dedicated EC2 spot box, once per real
saturday-sf-failure event.

Finishes config#2001 (the SF-failure half; the ci-main-failure half shipped
as ci-watch-dispatcher/index.py under the same issue). Mirrors that Lambda's
proven shape via the shared `nousergon_lib.spot_dispatch` primitives
(config#2106) — no bespoke third copy of the concurrency-lock/launch-with-
fallback/terminate-on-failure logic.

Mechanism:
  1. `spot_dispatch.launch_with_fallback()` rotates instance_type x subnet on
     capacity error; on SpotCapacityExhausted across all pools we relaunch
     ON-DEMAND (spot=False) so a capacity dip never silently drops an SF
     repair.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an async, detached `ssm send-command` (AWS-RunShellScript) carrying
     a small prelude: fetch the PAT from SSM, clone alpha-engine-config, then
     `exec infrastructure/sf_watch_spot_bootstrap.sh` (a sibling script in
     alpha-engine-config). The box self-terminates
     (InstanceInitiatedShutdownBehavior=terminate + its own on-box watchdog).

SYNCHRONOUS CONTRACT (identical to ci-watch-dispatcher's): a GHA job invokes
this Lambda with RequestResponse (not async) and branches directly on the
returned JSON. Every anticipated failure mode — concurrency skip,
spot+on-demand launch exhaustion, a malformed event, a post-launch SSM
failure — returns a clean, well-formed `{"launched": false, "reason": ...}`
rather than raising. Only a genuinely unexpected internal bug should still
propagate as a Python exception.

CONCURRENCY LOCK: keyed on `Name=alpha-engine-sf-watch-spot` +
`sf-watch-cadence=<cadence_slug>` + `sf-watch-pipeline=<pipeline_name>` +
`sf-watch-run-date=<run_date>` — the FULL identifying key (mirrors ci-watch's
own use of its full (repo, sha) key, not a partial one): two different
run_dates failing independently for the same pipeline+cadence must each get
their own box. A FAILED probe (SpotProbeError, config#2267 site 1) does NOT
fail-open silently: the dispatch proceeds — coverage beats dedupe; a probe
failure must never leave a real SF failure uncovered — but the degradation
is recorded loudly (`dedupe_degraded: true` in the returned verdict + an
ERROR log naming the probe error).

DEFER, NOT DROP (config#2226): when the lock finds a live box for the SAME
key, the second failure is NOT dropped — the live box has no obligation to
notice it (2026-07-11: a concurrent_skip left ne-weekly-freshness-pipeline
FAILED with zero watch coverage). Instead the dispatcher creates a ONE-SHOT
EventBridge Scheduler schedule (`ActionAfterCompletion=DELETE`) that
re-invokes this same Lambda ~10 minutes later with the original payload plus
`defer_generation` (capped at 3 — exhaustion returns `defer_exhausted`,
which the GHA caller treats as launched!=true and files a P1). A deferred
invocation (`defer_generation` >= 1) first RE-EVALUATES via
`states:ListExecutions`: if the newest execution is RUNNING/SUCCEEDED the
live box (or an operator) recovered the pipeline → `recovered`, no launch;
if it is FAILED/TIMED_OUT/ABORTED the dispatch proceeds against that NEWEST
failed execution's ARN (the original one may be stale by then).

CAUSE FIELD IS BASE64: `cause` (the SF failure detail) is arbitrary AWS-
supplied text, unlike every other event field here — it is NOT regex-
validated and is base64-encoded before being embedded in the constructed SSM
shell command (command-injection guard; see `_bootstrap_command` and
alpha-engine-config's `sf_watch_spot_bootstrap.sh --cause-b64`).

IAM PROFILE — deliberately NOT `alpha-engine-executor-profile` (shared with
the live trading executor) NOR the OIDC-only `saturday-sf-watch-role` (which
cannot back an EC2 instance profile at all). Uses
`alpha-engine-sf-watch-executor-profile`, a dedicated instance profile
created in `alpha-engine-config`'s IAM json files.

Managed OUTSIDE CloudFormation (same as ci-watch-dispatcher/scheduled-groom-
dispatcher): operator-deployed via `deploy.sh --bootstrap`. Merging the PR
has ZERO live effect until the new code + IAM are deployed AND
`sf-watch.yml`'s `sf-watch-dispatch` job is wired to invoke this Lambda.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import (  # SpotProbeError: nousergon-lib >= 0.106.0
    SpotCapacityExhausted,
    SpotLaunchError,
    SpotProbeError,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: SF_WATCH_DISPATCH_ENABLED=false disables the launch without
# touching the GHA invoke wiring — mirrors every other fleet dispatcher's
# safety valve. Default ON.
DISPATCH_ENABLED = os.environ.get("SF_WATCH_DISPATCH_ENABLED", "true").lower() == "true"

# ── Spot launch config (env-overridable; defaults mirror ci-watch-dispatcher/
# scheduled-groom-dispatcher — same default-VPC/AMI/security-group, only the
# IAM profile differs for blast-radius isolation). ────────────────────────────
# IAM LOCKSTEP (config#2271): the INSTANCE_TYPES default below, AMI_ID,
# SUBNETS, and SECURITY_GROUP are ENUMERATED in this Lambda's scoped
# ec2:RunInstances policy (sibling iam-policy.json — JSON carries no
# comments, so this is the canonical cross-reference). Changing any of these
# defaults (or their env overrides at deploy time) WITHOUT updating
# iam-policy.json + re-applying it will make RunInstances fail with
# UnauthorizedOperation at the next dispatch. Keep them in sync.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "SF_WATCH_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "SF_WATCH_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("SF_WATCH_AMI_ID", "ami-0c421724a94bba6d6")  # Amazon Linux 2023 x86_64
KEY_NAME = os.environ.get("SF_WATCH_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("SF_WATCH_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# NEW, dedicated profile — deliberately NOT alpha-engine-executor-profile (the
# live trading executor's profile) NOR saturday-sf-watch-role (OIDC-only,
# cannot back an EC2 instance profile). See module docstring.
IAM_PROFILE = os.environ.get("SF_WATCH_IAM_PROFILE", "alpha-engine-sf-watch-executor-profile")
VOLUME_SIZE_GB = int(os.environ.get("SF_WATCH_VOLUME_SIZE_GB", "40"))

SF_WATCH_TAG_NAME = "alpha-engine-sf-watch-spot"
SF_WATCH_CADENCE_TAG_KEY = "sf-watch-cadence"
SF_WATCH_PIPELINE_TAG_KEY = "sf-watch-pipeline"
SF_WATCH_RUN_DATE_TAG_KEY = "sf-watch-run-date"

# Discriminator tag write (config#2267 site 2): the tags are LOAD-BEARING —
# without them the next failure's dedupe guard is blind (duplicate box) and
# spot-orphan-reaper cannot derive the completion-marker key (guaranteed
# false "incomplete reap" page for a healthy run). Bounded retry, then
# terminate-the-box-and-fail-the-dispatch on final failure. Retry delay is
# env-overridable so tests run at 0.
TAG_WRITE_ATTEMPTS = int(os.environ.get("SF_WATCH_TAG_WRITE_ATTEMPTS", "3"))
TAG_WRITE_RETRY_DELAY_SEC = float(os.environ.get("SF_WATCH_TAG_WRITE_RETRY_DELAY_SEC", "2"))

# The box reads its own run secrets (PAT) via its instance profile in the
# common case, but the PRELUDE below (run before the profile-backed bootstrap
# script takes over) still needs the PAT to clone the private config repo —
# same shape as the other spot dispatchers' preludes. Reuses the SAME shared
# SSM param the other spot dispatchers already read.
SF_WATCH_GH_PAT_SSM = os.environ.get(
    "SF_WATCH_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
SF_WATCH_CONFIG_REPO = os.environ.get("SF_WATCH_CONFIG_REPO", "nousergon/alpha-engine-config")
SF_WATCH_CONFIG_BRANCH = os.environ.get("SF_WATCH_CONFIG_BRANCH", "main")
# Hard ceiling for the on-box SSM command (matches the bootstrap watchdog).
# Mirrors the retired inline GHA job's 300-min timeout + headroom, same as
# ci-watch-dispatcher's.
MAX_RUNTIME_SECONDS = int(os.environ.get("SF_WATCH_MAX_RUNTIME_SECONDS", "19200"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("SF_WATCH_SSM_ONLINE_BUDGET_SEC", "180"))
CW_LOG_GROUP = os.environ.get("SF_WATCH_CW_LOG_GROUP", "/alpha-engine/sf-watch-spot")

# ── Defer-not-drop config (config#2226) ──────────────────────────────────────
# How far out the one-shot re-invoke schedule fires, and the generation cap
# beyond which we stop deferring and fail LOUD (`defer_exhausted` — the GHA
# caller files a P1 on any unexpected launched!=true reason).
DEFER_DELAY_SECONDS = int(os.environ.get("SF_WATCH_DEFER_DELAY_SECONDS", "600"))
DEFER_MAX_GENERATION = int(os.environ.get("SF_WATCH_DEFER_MAX_GENERATION", "3"))
# Role EventBridge Scheduler assumes to invoke this Lambda. Set by deploy.sh
# --bootstrap; when unset, constructed at call time from the account id parsed
# out of context.invoked_function_arn (see _defer_relaunch).
DEFER_ROLE_ARN = os.environ.get("SF_WATCH_DEFER_ROLE_ARN", "")
DEFER_ROLE_NAME = "alpha-engine-sf-watch-defer-scheduler-role"
DEFER_SCHEDULE_GROUP = os.environ.get("SF_WATCH_DEFER_SCHEDULE_GROUP", "default")

# Operator-refire fallback (config#2226): the canonical watch_log_key is
# minted ONLY by saturday-sf-watch-dispatcher's `_artifact_key(watch_prefix,
# run_date)`. When an operator re-fires this Lambda by hand with an EMPTY
# watch_log_key, synthesize `{prefix}/{run_date}.json` from this mirror of
# that dispatcher's PIPELINES watch_prefix column. LOCKSTEP-GUARDED:
# tests/test_sf_watch_defer_prefix_lockstep.py fails CI if this dict drifts
# from saturday-sf-watch-dispatcher/index.py's PIPELINES.
_WATCH_PREFIXES = {
    "ne-weekly-freshness-pipeline": "consolidated/saturday_sf_watch",
    "ne-preopen-trading-pipeline": "consolidated/weekday_sf_watch",
    "ne-postclose-trading-pipeline": "consolidated/eod_sf_watch",
    # TRANSITIONAL old EOD SF name — mirrors the same entry in
    # saturday-sf-watch-dispatcher's PIPELINES (remove together at the
    # config#1408 SF-rename cutover; the lockstep test enforces "together").
    "alpha-engine-eod-pipeline": "consolidated/eod_sf_watch",
}

# Defense-in-depth allowlists for event fields embedded verbatim into the SSM
# shell command below (mirrors ci-watch-dispatcher's _REPO_RE/_SHA_RE/etc).
# These come from a GHA job (not raw external user input), but the same cheap
# regex check rules out shell-metacharacter injection outright. `cause` is
# deliberately NOT here — see module docstring's "CAUSE FIELD IS BASE64".
_PIPELINE_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_CADENCE_RE = re.compile(r"^[a-z0-9_-]{1,32}$")
_ARN_RE = re.compile(r"^arn:aws:states:[a-z0-9-]+:\d{12}:(stateMachine|execution):[A-Za-z0-9_.:/-]+$")
_RUN_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FAILED_STATE_RE = re.compile(r"^[A-Za-z0-9 _.:()/-]{0,200}$")
_WATCH_LOG_KEY_RE = re.compile(r"^[A-Za-z0-9_./-]{0,300}$")
_BOOL_RE = re.compile(r"^(true|false)$")


class _InvalidEvent(ValueError):
    """A required event field is missing or fails its allowlist."""


def _require(event: dict, key: str, pattern: "re.Pattern[str]") -> str:
    val = str(event.get(key) or "").strip()
    if not pattern.match(val):
        raise _InvalidEvent(f"missing/malformed {key!r} in event: {val!r}")
    return val


def _optional(event: dict, key: str, pattern: "re.Pattern[str]", default: str = "") -> str:
    val = str(event.get(key) or "").strip()
    if not val:
        return default
    if not pattern.match(val):
        raise _InvalidEvent(f"malformed {key!r} in event: {val!r}")
    return val


def _resolve_event_fields(event: dict) -> dict:
    """Validate the GHA payload's SF fields; raises _InvalidEvent on any
    missing/malformed field (caught once, at the handler, and converted to a
    clean launched:false — see module docstring's synchronous contract)."""
    pipeline_name = _require(event, "pipeline_name", _PIPELINE_RE)
    cadence_slug = _require(event, "cadence_slug", _CADENCE_RE)
    run_date = _require(event, "run_date", _RUN_DATE_RE)
    execution_arn = _require(event, "execution_arn", _ARN_RE)
    state_machine_arn = _optional(event, "state_machine_arn", _ARN_RE)
    failed_state = _optional(event, "failed_state", _FAILED_STATE_RE)
    watch_log_key = _optional(event, "watch_log_key", _WATCH_LOG_KEY_RE)
    is_preflight = _optional(event, "is_preflight", _BOOL_RE, default="false")
    # cause: deliberately unvalidated — arbitrary AWS text, base64-encoded
    # before it ever reaches a shell command (see _bootstrap_command).
    cause = str(event.get("cause") or "")
    return {
        "pipeline_name": pipeline_name,
        "cadence_slug": cadence_slug,
        "run_date": run_date,
        "execution_arn": execution_arn,
        "state_machine_arn": state_machine_arn,
        "failed_state": failed_state,
        "watch_log_key": watch_log_key,
        "is_preflight": is_preflight,
        "cause": cause,
    }


def _bootstrap_command(fields: dict, run_token: str) -> str:
    """The async SSM RunShellScript body: fetch PAT, clone config, exec the
    sf_watch_spot_bootstrap.sh entrypoint in alpha-engine-config. Any prelude
    failure shuts the box down so a botched launch never idles (mirrors
    ci-watch-dispatcher's prelude fail() trap exactly).

    ``sf_watch_spot_bootstrap.sh`` takes its SF fields as CLI FLAGS
    (``--pipeline``/``--cadence-slug``/...), not environment variables —
    invoke it that way, not via `export`. ``run_token`` is deliberately NOT
    threaded into the box: the bootstrap/run-script side keys its S3
    completion marker directly on (cadence_slug, pipeline_name, run_date) —
    it stays a Lambda-side-only correlation id (see the SSM Comment field in
    ``_send_bootstrap``, and the handler's returned JSON)."""
    cause_b64 = base64.b64encode(fields["cause"].encode("utf-8")).decode("ascii")
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
# SSM RunShellScript runs as root with NO $HOME set; git config/clone need it.
export HOME=/root
fail() {{ echo "[sf-watch-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
dnf install -y -q git python3.12 python3.12-pip >/dev/null 2>&1 \
  || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {SF_WATCH_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {SF_WATCH_CONFIG_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{SF_WATCH_CONFIG_REPO}.git" \
  /home/ec2-user/alpha-engine-config || fail "clone failed"
cd /home/ec2-user/alpha-engine-config
exec bash infrastructure/sf_watch_spot_bootstrap.sh \
  --pipeline "{fields['pipeline_name']}" --cadence-slug "{fields['cadence_slug']}" \
  --state-machine-arn "{fields['state_machine_arn']}" --execution-arn "{fields['execution_arn']}" \
  --run-date "{fields['run_date']}" --failed-state "{fields['failed_state']}" \
  --cause-b64 "{cause_b64}" --watch-log-key "{fields['watch_log_key']}" \
  --is-preflight "{fields['is_preflight']}"
"""


def _launch_instance() -> tuple[str, str]:
    """Launch the SF-watch box; spot first, on-demand fallback on capacity
    exhaustion. Raises SpotLaunchError (or the SpotCapacityExhausted
    subclass) if BOTH the spot attempt and the on-demand fallback are
    exhausted/fail — caught once by the caller and converted to a clean
    launched:false."""
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=SF_WATCH_TAG_NAME,
        region=REGION,
    )


def _wait_ssm_online(instance_id: str) -> None:
    """Block until the instance is running AND its SSM agent registers Online."""
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(fields: dict, instance_id: str, run_token: str) -> str:
    """Fire the async, detached SSM command that runs SF-watch + self-terminates."""
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(fields, run_token),
        comment=(
            f"sf-watch ({fields['cadence_slug']}/{fields['pipeline_name']}, "
            f"run_date {fields['run_date']}, token {run_token[:12]})"
        ),
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=MAX_RUNTIME_SECONDS,
    )


def _running_sf_watch_instance_ids(cadence_slug: str, pipeline_name: str, run_date: str) -> list[str]:
    """Instance ids for a LIVE (pending/running) sf-watch box already working
    THIS exact (cadence_slug, pipeline_name, run_date) — the full identifying
    key (mirrors ci-watch-dispatcher's own use of its full (repo, sha) key):
    two different run_dates failing independently for the same
    pipeline+cadence must each get their own box. Raises SpotProbeError
    (nousergon-lib >= 0.106.0, config#2267 site 1) when the probe itself
    fails — the caller degrades to launch-with-dedupe_degraded, never a
    silent fail-open []."""
    return spot_dispatch.running_instance_ids(
        SF_WATCH_TAG_NAME,
        {
            SF_WATCH_CADENCE_TAG_KEY: cadence_slug,
            SF_WATCH_PIPELINE_TAG_KEY: pipeline_name,
            SF_WATCH_RUN_DATE_TAG_KEY: run_date,
        },
        region=REGION,
    )


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a just-launched box whose post-launch steps
    failed. Without this the box orphans: it received no bootstrap, so
    neither the on-box watchdog nor the EXIT trap (both armed BY the
    bootstrap) is running to tear it down. Never masks the original error
    (logged, not raised) — mirrors ci-watch-dispatcher's `_terminate_instance`
    exactly."""
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="sf-watch")


def _create_discriminator_tags(
    instance_id: str, cadence_slug: str, pipeline_name: str, run_date: str
) -> str | None:
    """Write the load-bearing discriminator tags (config#2267 site 2) with a
    bounded retry. Returns None on success, or the final ``"ExcName: msg"``
    string after TAG_WRITE_ATTEMPTS failures — the caller terminates the box
    and fails the dispatch (the tags are what make the box visible to the
    dedupe guard and the spot-orphan-reaper; an untagged box must not run)."""
    tags = [
        {"Key": SF_WATCH_CADENCE_TAG_KEY, "Value": cadence_slug},
        {"Key": SF_WATCH_PIPELINE_TAG_KEY, "Value": pipeline_name},
        {"Key": SF_WATCH_RUN_DATE_TAG_KEY, "Value": run_date},
    ]
    last_error = ""
    for attempt in range(1, TAG_WRITE_ATTEMPTS + 1):
        try:
            boto3.client("ec2", region_name=REGION).create_tags(
                Resources=[instance_id], Tags=tags
            )
            return None
        except Exception as exc:  # noqa: BLE001 — bounded retry; final failure is FATAL to the dispatch (caller terminates + fails)
            last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "sf-watch discriminator tag write attempt %d/%d failed for %s: %s",
                attempt, TAG_WRITE_ATTEMPTS, instance_id, last_error,
            )
            if attempt < TAG_WRITE_ATTEMPTS:
                time.sleep(TAG_WRITE_RETRY_DELAY_SEC)
    return last_error


def _defer_schedule_name(
    cadence_slug: str, pipeline_name: str, run_date: str, generation: int
) -> str:
    """Deterministic one-shot schedule name for (key, generation) — the
    determinism IS the idempotency lock: a duplicate defer attempt for the
    same key+generation hits ConflictException and is treated as
    already-deferred. EventBridge Scheduler caps Name at 64 chars; the
    readable form overflows for ne-weekly-freshness-pipeline (66 chars), so
    anything over the cap degrades to an equally-deterministic sha256 digest
    of the key. BOTH forms keep the `sf-watch-defer-` prefix the IAM policy
    scopes on (`schedule/default/sf-watch-defer-*`)."""
    name = f"sf-watch-defer-{cadence_slug}-{pipeline_name}-{run_date}-g{generation}"
    if len(name) <= 64:
        return name
    digest = hashlib.sha256(
        f"{cadence_slug}|{pipeline_name}|{run_date}".encode("utf-8")
    ).hexdigest()[:16]
    return f"sf-watch-defer-{digest}-g{generation}"


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


def _defer_relaunch(fields: dict, generation: int, context, existing: list[str]) -> dict:
    """A live box holds the (cadence, pipeline, run_date) lock — DEFER this
    dispatch instead of dropping it (config#2226): schedule a one-shot
    EventBridge Scheduler re-invoke of this same Lambda in
    DEFER_DELAY_SECONDS carrying the original payload + defer_generation.
    Every failure mode returns a clean launched:false (synchronous contract),
    but exhaustion and scheduling failures log ERROR so the drop is LOUD."""
    if generation >= DEFER_MAX_GENERATION:
        logger.error(
            "sf-watch defer EXHAUSTED at generation %d for %s/%s@%s — a live box "
            "(%s) still holds the lock and this failure will NOT be retried; the "
            "synchronous caller must escalate (P1)",
            generation, fields["cadence_slug"], fields["pipeline_name"],
            fields["run_date"], existing,
        )
        return {"launched": False, "reason": "defer_exhausted",
                "defer_generation": generation, "existing_instance_ids": existing}

    next_generation = generation + 1
    schedule_name = _defer_schedule_name(
        fields["cadence_slug"], fields["pipeline_name"], fields["run_date"], next_generation
    )

    function_arn = str(getattr(context, "invoked_function_arn", "") or "")
    if not function_arn:
        logger.error(
            "sf-watch defer FAILED for %s/%s@%s: no invoked_function_arn on the "
            "Lambda context — cannot self-target the deferred re-invoke",
            fields["cadence_slug"], fields["pipeline_name"], fields["run_date"],
        )
        return {"launched": False, "reason": "defer_schedule_failed",
                "error": "no invoked_function_arn on context"}
    # arn:aws:lambda:region:acct:function:name[:qualifier] — target the
    # UNQUALIFIED function so the deferred invoke always runs the live code.
    target_arn = ":".join(function_arn.split(":")[:7])
    role_arn = DEFER_ROLE_ARN or (
        f"arn:aws:iam::{function_arn.split(':')[4]}:role/{DEFER_ROLE_NAME}"
    )

    payload = {k: fields[k] for k in (
        "pipeline_name", "cadence_slug", "run_date", "execution_arn",
        "state_machine_arn", "failed_state", "watch_log_key", "is_preflight",
        "cause",
    )}
    payload["defer_generation"] = next_generation
    fire_at = datetime.now(timezone.utc) + timedelta(seconds=DEFER_DELAY_SECONDS)

    try:
        boto3.client("scheduler", region_name=REGION).create_schedule(
            Name=schedule_name,
            GroupName=DEFER_SCHEDULE_GROUP,
            # at() with no ScheduleExpressionTimezone is UTC — matches fire_at.
            ScheduleExpression=f"at({fire_at.strftime('%Y-%m-%dT%H:%M:%S')})",
            FlexibleTimeWindow={"Mode": "OFF"},
            ActionAfterCompletion="DELETE",  # one-shot: self-deletes after firing
            Description=(
                f"sf-watch defer-not-drop re-invoke (config#2226): "
                f"{fields['cadence_slug']}/{fields['pipeline_name']}@"
                f"{fields['run_date']} generation {next_generation}"
            ),
            Target={
                "Arn": target_arn,
                "RoleArn": role_arn,
                "Input": json.dumps(payload),
                # Bounded retry: the re-invoke is a re-CHECK, not the repair
                # itself — a next-generation defer covers a missed window.
                "RetryPolicy": {
                    "MaximumRetryAttempts": 3,
                    "MaximumEventAgeInSeconds": 3600,
                },
            },
        )
    except Exception as exc:  # noqa: BLE001 — synchronous contract: clean JSON verdict, never raise
        if _is_scheduler_conflict(exc):
            # Already-deferred swallow: a duplicate defer attempt for the same
            # (key, generation) — the earlier schedule already covers this
            # failure; recorded via this INFO log + the returned verdict.
            logger.info(
                "sf-watch defer schedule %s already exists — treating as "
                "already-deferred", schedule_name,
            )
            return {"launched": False, "reason": "deferred",
                    "defer_generation": next_generation,
                    "schedule_name": schedule_name, "already_scheduled": True}
        logger.error(
            "sf-watch defer schedule creation FAILED for %s (%s: %s) — this "
            "failure will NOT be retried; the synchronous caller must escalate",
            schedule_name, type(exc).__name__, exc,
        )
        return {"launched": False, "reason": "defer_schedule_failed",
                "error": f"{type(exc).__name__}: {exc}"}

    logger.warning(
        "sf-watch box already live for %s/%s@%s (%s) — DEFERRED (not dropped): "
        "schedule %s re-invokes at %sZ (generation %d)",
        fields["cadence_slug"], fields["pipeline_name"], fields["run_date"],
        existing, schedule_name, fire_at.strftime("%Y-%m-%dT%H:%M:%S"), next_generation,
    )
    return {"launched": False, "reason": "deferred",
            "defer_generation": next_generation, "schedule_name": schedule_name,
            "existing_instance_ids": existing}


def _state_machine_arn_from_execution(execution_arn: str) -> str:
    """Derive the stateMachine ARN from an execution ARN:
    arn:aws:states:R:A:execution:SM:NAME -> arn:aws:states:R:A:stateMachine:SM
    (drop the trailing :NAME segment, swap the resource type)."""
    without_name = execution_arn.rpartition(":")[0]
    return without_name.replace(":execution:", ":stateMachine:", 1)


def _reevaluate_after_defer(fields: dict) -> dict | None:
    """On a DEFERRED invocation (defer_generation >= 1), re-check the state
    machine before dispatching: the live box that forced the defer may have
    recovered the pipeline meanwhile. Returns a terminal verdict dict
    (`recovered`) to short-circuit the dispatch, or None to proceed — in the
    proceed case, `fields["execution_arn"]` is retargeted at the NEWEST
    failed execution (the originally-reported one may be stale by now).
    Fail-safe toward LAUNCHING on any States API error (mirrors the
    concurrency check's posture: a broken check never blocks a repair)."""
    state_machine_arn = fields["state_machine_arn"] or _state_machine_arn_from_execution(
        fields["execution_arn"]
    )
    try:
        response = boto3.client("stepfunctions", region_name=REGION).list_executions(
            stateMachineArn=state_machine_arn, maxResults=5
        )
        executions = response.get("executions") or []
    except Exception as exc:  # noqa: BLE001 — fail-safe: a broken re-check must not block the repair dispatch (logged here)
        logger.warning(
            "deferred re-evaluation ListExecutions failed for %s (non-fatal, "
            "dispatching anyway): %s: %s", state_machine_arn, type(exc).__name__, exc,
        )
        return None
    if not executions:
        # Zero executions on a machine we KNOW failed is a States-API anomaly,
        # not a recovery signal — fail-safe toward launching (logged here).
        logger.warning(
            "deferred re-evaluation found NO executions for %s — dispatching "
            "against the original execution_arn", state_machine_arn,
        )
        return None

    latest = executions[0]  # ListExecutions returns newest-first
    status = str(latest.get("status") or "")
    if status in ("RUNNING", "SUCCEEDED", "PENDING_REDRIVE"):
        # RUNNING/SUCCEEDED: the live box (or an operator) already recovered
        # the pipeline. PENDING_REDRIVE: a redrive is in flight — launching a
        # repair box now would duplicate it.
        logger.info(
            "deferred re-evaluation: latest execution %s is %s — recovered, "
            "no dispatch needed", latest.get("executionArn"), status,
        )
        return {"launched": False, "reason": "recovered",
                "latest_execution_arn": latest.get("executionArn"),
                "latest_status": status}

    # FAILED / TIMED_OUT / ABORTED — still broken; retarget the dispatch at
    # the NEWEST failed execution so the repair agent diagnoses the current
    # failure, not a stale one. The ARN comes from AWS but is embedded into a
    # shell command downstream — hold it to the same allowlist as the event's.
    newest_arn = str(latest.get("executionArn") or "")
    if _ARN_RE.match(newest_arn):
        if newest_arn != fields["execution_arn"]:
            logger.info(
                "deferred re-evaluation: retargeting dispatch from %s to newest "
                "%s execution %s", fields["execution_arn"], status, newest_arn,
            )
            fields["execution_arn"] = newest_arn
    else:
        # Malformed-ARN swallow: keep the original validated execution_arn
        # rather than embedding an unvalidated string into the SSM shell
        # command; recorded via this WARNING log.
        logger.warning(
            "deferred re-evaluation: newest execution ARN %r fails the ARN "
            "allowlist — keeping the original execution_arn", newest_arn,
        )
    return None


def _launch_sf_watch_spot(fields: dict, context=None, defer_generation: int = 0) -> dict:
    """Launch + bootstrap the SF-watch box. SYNCHRONOUS contract: every
    anticipated failure mode returns a clean, well-formed launched:false
    rather than raising — see module docstring."""
    if not DISPATCH_ENABLED:
        logger.warning("SF_WATCH_DISPATCH_ENABLED=false — sf-watch spot NOT launched")
        return {"launched": False, "reason": "disabled"}

    cadence_slug, pipeline_name, run_date = (
        fields["cadence_slug"], fields["pipeline_name"], fields["run_date"],
    )
    dedupe_degraded = False
    dedupe_probe_error = ""
    try:
        existing = _running_sf_watch_instance_ids(cadence_slug, pipeline_name, run_date)
    except SpotProbeError as exc:
        # Degraded-probe swallow (config#2267 site 1 POLICY): failure mode
        # swallowed = a possible duplicate box (the probe could not rule one
        # out); the primary deliverable — watch coverage of a REAL SF failure
        # — survives, and coverage beats dedupe: a probe failure must never
        # leave a real SF failure uncovered. Recording surfaces: this ERROR
        # log + `dedupe_degraded: true` in the returned verdict the GHA
        # caller archives.
        dedupe_degraded = True
        dedupe_probe_error = f"{type(exc).__name__}: {exc}"
        existing = []
        logger.error(
            "sf-watch concurrency probe FAILED for %s/%s@%s — proceeding to "
            "launch with dedupe_degraded=true (coverage beats dedupe; a "
            "duplicate box is possible): %s",
            cadence_slug, pipeline_name, run_date, dedupe_probe_error,
        )
    if existing:
        # DEFER, NOT DROP (config#2226): the live box has no obligation to
        # notice THIS failure — schedule a one-shot re-invoke instead of
        # silently dropping it (see module docstring).
        return _defer_relaunch(fields, defer_generation, context, existing)

    run_token = uuid.uuid4().hex
    try:
        instance_id, market = _launch_instance()
    except SpotLaunchError as exc:
        logger.error("sf-watch spot launch failed: %s: %s", type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched sf-watch box %s (%s) for %s/%s@%s%s",
               instance_id, market, cadence_slug, pipeline_name, run_date,
               " dedupe_degraded=true" if dedupe_degraded else "")
    # config#1979-style tags so the NEXT trigger's guard check (above) — and
    # the fleet spot-orphan-reaper's completion-marker lookup — can find the
    # box. LOAD-BEARING, not cosmetic (config#2267 site 2): bounded retry,
    # then TERMINATE the box and fail the dispatch on final failure — a box
    # invisible to the dedupe guard and the reaper must not run. (The root
    # fix — discriminator tags atomic with launch via RunInstances
    # TagSpecifications — is blocked on krepis.ec2_spot.launch, the fleet's
    # RunInstances chokepoint, growing an extra-tags parameter; until then
    # this retry+terminate closes the hole loudly.)
    tag_error = _create_discriminator_tags(instance_id, cadence_slug, pipeline_name, run_date)
    if tag_error is not None:
        _terminate_instance(instance_id)
        logger.error(
            "sf-watch discriminator tag write FAILED after %d attempts for %s "
            "(%s/%s@%s) — box terminated, dispatch failed: %s",
            TAG_WRITE_ATTEMPTS, instance_id, cadence_slug, pipeline_name,
            run_date, tag_error,
        )
        return {"launched": False, "reason": "tag_write_failed",
                "instance_id": instance_id, "error": tag_error,
                "dedupe_degraded": dedupe_degraded}

    # Once the box is up, ANY failure before the bootstrap command is
    # delivered would orphan it (no watchdog/trap yet). Terminate-on-error —
    # return a clean result rather than re-raising (this Lambda's synchronous
    # caller needs a JSON verdict, not an invocation error to unwrap).
    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(fields, instance_id, run_token)
    except Exception as exc:  # noqa: BLE001 — converted to a clean launched:false
        _terminate_instance(instance_id)
        logger.error("sf-watch post-launch step failed for %s: %s: %s",
                     instance_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "post_launch_failed",
                "instance_id": instance_id, "error": str(exc),
                "dedupe_degraded": dedupe_degraded}

    logger.info(
        "sf-watch dispatched: instance=%s market=%s command=%s cadence=%s pipeline=%s "
        "run_date=%s run_token=%s dedupe_degraded=%s", instance_id, market, command_id,
        cadence_slug, pipeline_name, run_date, run_token, dedupe_degraded,
    )
    verdict = {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "cadence_slug": cadence_slug,
        "pipeline_name": pipeline_name,
        "run_date": run_date,
        "run_token": run_token,
        "dedupe_degraded": dedupe_degraded,
    }
    if dedupe_degraded:
        verdict["dedupe_probe_error"] = dedupe_probe_error
    return verdict


def handler(event: dict, context) -> dict:
    """Synchronous handler invoked once per real saturday-sf-failure event —
    NOT on a cron schedule. `event` carries {"pipeline_name", "cadence_slug",
    "state_machine_arn", "execution_arn", "run_date", "failed_state", "cause",
    "watch_log_key", "is_preflight"} from the GHA job's `lambda invoke`
    payload (RequestResponse). A DEFERRED re-invoke (config#2226 — fired by
    the one-shot EventBridge Scheduler schedule this handler created on a
    concurrency skip) carries the same payload plus `defer_generation` >= 1
    and first re-evaluates the state machine before dispatching.

    Returns {"launched": bool, "reason": str, "instance_id": ..., ...} — read
    DIRECTLY by the GHA job as its success signal. Every anticipated failure
    (malformed event, defer-scheduling failure, defer exhaustion,
    spot+on-demand launch exhaustion, post-launch SSM failure) is a clean
    return, never an exception — see module docstring's synchronous contract.
    """
    event = event or {}
    try:
        fields = _resolve_event_fields(event)
    except _InvalidEvent as exc:
        logger.error("invalid sf-watch event: %s", exc)
        return {"launched": False, "reason": "invalid_event", "error": str(exc)}

    try:
        defer_generation = int(str(event.get("defer_generation") or 0))
        if defer_generation < 0:
            raise ValueError(defer_generation)
    except ValueError:
        logger.error("invalid sf-watch event: malformed defer_generation %r",
                     event.get("defer_generation"))
        return {"launched": False, "reason": "invalid_event",
                "error": f"malformed defer_generation: {event.get('defer_generation')!r}"}

    if not fields["watch_log_key"]:
        # Operator-refire fallback (config#2226): synthesize the canonical
        # per-pipeline key exactly as saturday-sf-watch-dispatcher's
        # _artifact_key mints it (lockstep-guarded — see _WATCH_PREFIXES).
        prefix = _WATCH_PREFIXES.get(fields["pipeline_name"])
        if prefix:
            fields["watch_log_key"] = f"{prefix}/{fields['run_date']}.json"
            logger.info("empty watch_log_key — synthesized %s", fields["watch_log_key"])
        else:
            # Unknown-pipeline swallow: watch_log_key is OPTIONAL in the box's
            # contract (the bootstrap tolerates an empty flag), so an
            # unregistered pipeline proceeds without one; recorded via this
            # WARNING log.
            logger.warning(
                "empty watch_log_key and pipeline %r has no registered watch "
                "prefix — dispatching without one", fields["pipeline_name"],
            )

    logger.info(
        "sf-watch trigger: pipeline=%s cadence=%s run_date=%s execution_arn=%s "
        "defer_generation=%d",
        fields["pipeline_name"], fields["cadence_slug"], fields["run_date"],
        fields["execution_arn"], defer_generation,
    )
    if defer_generation >= 1:
        verdict = _reevaluate_after_defer(fields)
        if verdict is not None:
            return verdict
    return _launch_sf_watch_spot(fields, context, defer_generation)
