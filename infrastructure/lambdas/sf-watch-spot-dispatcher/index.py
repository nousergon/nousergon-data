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
their own box. Fail-safe OPEN on any API error.

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
import logging
import os
import re
import time
import uuid

import boto3
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotCapacityExhausted, SpotLaunchError

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
    pipeline+cadence must each get their own box. Fail-safe: any API error
    returns [] (never blocks a launch on a broken check)."""
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


def _launch_sf_watch_spot(fields: dict) -> dict:
    """Launch + bootstrap the SF-watch box. SYNCHRONOUS contract: every
    anticipated failure mode returns a clean, well-formed launched:false
    rather than raising — see module docstring."""
    if not DISPATCH_ENABLED:
        logger.warning("SF_WATCH_DISPATCH_ENABLED=false — sf-watch spot NOT launched")
        return {"launched": False, "reason": "disabled"}

    cadence_slug, pipeline_name, run_date = (
        fields["cadence_slug"], fields["pipeline_name"], fields["run_date"],
    )
    existing = _running_sf_watch_instance_ids(cadence_slug, pipeline_name, run_date)
    if existing:
        logger.warning(
            "sf-watch box already live for %s/%s@%s (%s) — skipping launch to "
            "avoid a concurrent duplicate run", cadence_slug, pipeline_name, run_date, existing)
        return {"launched": False, "reason": "concurrent_skip",
                "existing_instance_ids": existing}

    run_token = uuid.uuid4().hex
    try:
        instance_id, market = _launch_instance()
    except SpotLaunchError as exc:
        logger.error("sf-watch spot launch failed: %s: %s", type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched sf-watch box %s (%s) for %s/%s@%s",
               instance_id, market, cadence_slug, pipeline_name, run_date)
    # config#1979-style tags so the NEXT trigger's guard check (above) — and
    # the fleet spot-orphan-reaper's incomplete-reap alert — can find them.
    # Best-effort — a tag-write failure must not abort an already-launched
    # box (mirrors ci-watch-dispatcher's fail-safe posture on its own tags).
    try:
        boto3.client("ec2", region_name=REGION).create_tags(
            Resources=[instance_id],
            Tags=[
                {"Key": SF_WATCH_CADENCE_TAG_KEY, "Value": cadence_slug},
                {"Key": SF_WATCH_PIPELINE_TAG_KEY, "Value": pipeline_name},
                {"Key": SF_WATCH_RUN_DATE_TAG_KEY, "Value": run_date},
            ],
        )
    except Exception as exc:  # noqa: BLE001 — non-fatal, mirrors ci-watch's tag write
        logger.warning("sf-watch discriminator tag write failed (non-fatal): %s: %s",
                       type(exc).__name__, exc)

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
                "instance_id": instance_id, "error": str(exc)}

    logger.info(
        "sf-watch dispatched: instance=%s market=%s command=%s cadence=%s pipeline=%s "
        "run_date=%s run_token=%s", instance_id, market, command_id, cadence_slug,
        pipeline_name, run_date, run_token,
    )
    return {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "cadence_slug": cadence_slug,
        "pipeline_name": pipeline_name,
        "run_date": run_date,
        "run_token": run_token,
    }


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Synchronous handler invoked once per real saturday-sf-failure event —
    NOT on a schedule. `event` carries {"pipeline_name", "cadence_slug",
    "state_machine_arn", "execution_arn", "run_date", "failed_state", "cause",
    "watch_log_key", "is_preflight"} from the GHA job's `lambda invoke`
    payload (RequestResponse).

    Returns {"launched": bool, "reason": str, "instance_id": ..., ...} — read
    DIRECTLY by the GHA job as its success signal. Every anticipated failure
    (malformed event, concurrency skip, spot+on-demand launch exhaustion,
    post-launch SSM failure) is a clean return, never an exception — see
    module docstring's synchronous contract.
    """
    event = event or {}
    try:
        fields = _resolve_event_fields(event)
    except _InvalidEvent as exc:
        logger.error("invalid sf-watch event: %s", exc)
        return {"launched": False, "reason": "invalid_event", "error": str(exc)}

    logger.info(
        "sf-watch trigger: pipeline=%s cadence=%s run_date=%s execution_arn=%s",
        fields["pipeline_name"], fields["cadence_slug"], fields["run_date"],
        fields["execution_arn"],
    )
    return _launch_sf_watch_spot(fields)
