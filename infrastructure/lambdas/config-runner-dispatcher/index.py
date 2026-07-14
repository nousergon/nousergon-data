"""alpha-engine-config-runner-dispatcher — launch an EPHEMERAL GitHub Actions
self-hosted runner on a dedicated EC2 spot box, once per queued CI/validation
job in nousergon/alpha-engine-config. alpha-engine-config-I2572.

WHY: a hard GHA audit (2026-07-14) found alpha-engine-config's own CI/
validation workflows (scripts-tests.yml, changelog.yml, the validate-*.yaml
set, etc.) — NOT the heavy agentic workloads ci-watch/sf-watch/groom already
moved to spot — are now the dominant steady-state draw on the org's metered
private-repo GHA-minutes quota (~1.6k min/30d projected, ~93% of all
private-repo GHA spend fleet-wide). Those are frequent (dozens/day) and
short-lived (<1 min avg) — a poor fit for the ci-watch/sf-watch pattern (a
thin GHA-hosted dispatch leg synchronously invoking a Lambda that runs a
bespoke script over SSM, outside the Actions protocol entirely): replicating
that shape per-workflow would mean hand-rolling GitHub Checks-API status
reporting ~13 times over. Registering a REAL ephemeral self-hosted runner is
simpler AND more correct here — the existing workflow YAML (checkout,
setup-python, pytest, etc.) is untouched, only `runs-on:` changes, and
GitHub's own Actions service handles job dispatch + Check Run status
natively, exactly as it does for hosted runners.

MECHANISM (two-phase single Lambda, self-invoked async — see module-level
`handler` for the branch):
  1. WEBHOOK RECEIVER phase: GitHub calls this Lambda's Function URL directly
     on every `workflow_job` event for alpha-engine-config (repo-level
     webhook, event type `workflow_job`). Verifies the HMAC signature, filters
     to `action=queued` + our `alpha-engine-config-spot` label, then
     self-invokes ASYNC (`InvocationType=Event`) with a minimal worker
     payload and returns 200 immediately. GitHub's webhook delivery timeout
     is short (single-digit seconds) — the actual spot launch below can take
     30-90s+, so it MUST NOT block the HTTP response, or every delivery would
     show as a spurious timeout in GitHub's UI even though the box still
     launches (Lambda execution isn't cancelled by a disconnected client, but
     a clean signal matters for operability).
  2. WORKER phase (the self-invoked async call): does the actual dispatch —
     `spot_dispatch.launch_with_fallback()` (spot-first, on-demand fallback
     on capacity exhaustion), wait for SSM-online, then an async detached SSM
     command that clones alpha-engine-config and `exec`s
     `infrastructure/config_runner_spot_bootstrap.sh` (built by a sibling
     agent in alpha-engine-config) — which installs+registers the actual
     `actions-runner` binary in `--ephemeral` mode, runs exactly one job, and
     self-terminates (InstanceInitiatedShutdownBehavior=terminate + its own
     on-box watchdog). Mirrors `ci-watch-dispatcher/index.py`'s
     launch/wait/dispatch shape via the shared `nousergon_lib.spot_dispatch`
     primitives (config#2106) — NOT reinvented here.

CONCURRENCY LOCK: keyed on `Name=alpha-engine-config-runner-spot` +
`config-runner-job-id=<workflow_job.id>` — one job, one box, 1:1 (unlike
ci-watch's repo+sha lock, a GitHub Actions job id is already the unique
discriminator; no broader key needed). A duplicate `queued` delivery for the
same job (GitHub webhooks are at-least-once) is a clean no-op skip.

IAM PROFILE: `alpha-engine-config-runner-executor-profile`, a NEW dedicated
instance profile (infrastructure/iam/config-runner-executor-role-*.json in
alpha-engine-config) scoped to read exactly one SSM param (the runner-
registration PAT) — deliberately not `alpha-engine-ci-watch-executor-profile`
or any other existing profile, so this new/less-proven workload's blast
radius stays contained. Every AWS credential the actual CI STEPS need (S3
sync, OIDC role assumption, etc.) continues to flow through GitHub's own
per-workflow OIDC mechanism, unrelated to this instance's own role.

FAIL-SOFT ON THE WEBHOOK RECEIVER PHASE, FAIL-LOUD ON THE WORKER PHASE: an
unrecognized/malformed webhook delivery is a clean 200 no-op (GitHub sends
many event types/actions we don't care about — treating them as errors would
just generate webhook-delivery noise for no reason). The worker phase mirrors
ci-watch's SYNCHRONOUS-style clean-failure contract even though its own
caller is async (nothing reads the return value) — CloudWatch Logs is the
observability surface for worker-phase failures; a genuinely unexpected
internal bug still propagates as a Python exception (visible as a Lambda
error metric).

Managed OUTSIDE CloudFormation (same as every other fleet dispatcher):
operator-deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live
effect until the new code + IAM + Function URL + GitHub webhook registration
are applied AND the (separately-tracked, human-only) runner-registration PAT
with Administration:write on alpha-engine-config exists in SSM — see
alpha-engine-config-I2572 for the full rollout sequencing.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os

import boto3
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotLaunchError, SpotProbeError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: CONFIG_RUNNER_DISPATCH_ENABLED=false disables the launch
# without touching the GitHub webhook wiring — mirrors every other fleet
# dispatcher's safety valve. Default ON.
DISPATCH_ENABLED = os.environ.get("CONFIG_RUNNER_DISPATCH_ENABLED", "true").lower() == "true"

TARGET_REPO_FULL_NAME = "nousergon/alpha-engine-config"
TARGET_LABEL = "alpha-engine-config-spot"

# ── Spot launch config (env-overridable; same default-VPC/AMI/security-group
# as the fleet's other spot dispatchers — only the IAM profile + tag differ).
# IAM LOCKSTEP (mirrors ci-watch-dispatcher): these defaults are ENUMERATED
# in this Lambda's scoped ec2:RunInstances policy (sibling iam-policy.json).
# Changing them without re-applying that policy makes RunInstances fail with
# UnauthorizedOperation at the next dispatch. Keep them in sync.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "CONFIG_RUNNER_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "CONFIG_RUNNER_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("CONFIG_RUNNER_AMI_ID", "ami-0c421724a94bba6d6")  # Amazon Linux 2023 x86_64
KEY_NAME = os.environ.get("CONFIG_RUNNER_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("CONFIG_RUNNER_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
IAM_PROFILE = os.environ.get(
    "CONFIG_RUNNER_IAM_PROFILE", "alpha-engine-config-runner-executor-profile"
)
VOLUME_SIZE_GB = int(os.environ.get("CONFIG_RUNNER_VOLUME_SIZE_GB", "30"))

CONFIG_RUNNER_TAG_NAME = "alpha-engine-config-runner-spot"
CONFIG_RUNNER_JOB_ID_TAG_KEY = "config-runner-job-id"

# This Lambda's OWN secret (unlike ci-watch-dispatcher, which needs none) —
# the webhook HMAC secret, read once per cold start and cached at module
# scope (same pattern as every fleet Lambda that reads a param once, not
# per-invocation, to keep p50 latency low on the hot webhook-verification path).
WEBHOOK_SECRET_SSM = os.environ.get(
    "CONFIG_RUNNER_WEBHOOK_SECRET_SSM", "/alpha-engine/config_runner/webhook_secret"
)
_webhook_secret_cache: str | None = None


def _webhook_secret() -> str:
    global _webhook_secret_cache
    if _webhook_secret_cache is None:
        _webhook_secret_cache = boto3.client("ssm", region_name=REGION).get_parameter(
            Name=WEBHOOK_SECRET_SSM, WithDecryption=True  # gitleaks:allow — SSM param path, not a secret value
        )["Parameter"]["Value"]
    return _webhook_secret_cache


CONFIG_RUNNER_GH_PAT_SSM = os.environ.get(
    "CONFIG_RUNNER_GH_PAT_SSM", "/alpha-engine/config_runner/github_pat"
)
CONFIG_RUNNER_CONFIG_BRANCH = os.environ.get("CONFIG_RUNNER_CONFIG_BRANCH", "main")
MAX_RUNTIME_SECONDS = int(os.environ.get("CONFIG_RUNNER_MAX_RUNTIME_SECONDS", "1800"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("CONFIG_RUNNER_SSM_ONLINE_BUDGET_SEC", "180"))
CW_LOG_GROUP = os.environ.get("CONFIG_RUNNER_CW_LOG_GROUP", "/alpha-engine/config-runner-spot")


def _verify_signature(raw_body: bytes, signature_header: str | None) -> bool:
    """Constant-time HMAC-SHA256 verification of GitHub's
    `X-Hub-Signature-256` header against the shared webhook secret."""
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        _webhook_secret().encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


def _decode_body(event: dict) -> bytes:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        import base64

        return base64.b64decode(body)
    return body.encode("utf-8")


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _handle_webhook(event: dict) -> dict:
    """Phase 1: verify + filter a raw GitHub `workflow_job` webhook delivery.
    Always returns fast (no spot-launch work happens in this phase) — a
    matching event triggers an async self-invoke of the worker phase."""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    raw_body = _decode_body(event)

    if not _verify_signature(raw_body, headers.get("x-hub-signature-256")):
        logger.warning("webhook signature verification failed — rejecting delivery")
        return _response(401, {"error": "invalid signature"})

    if headers.get("x-github-event") != "workflow_job":
        # `ping` (sent on webhook creation) and anything else we didn't
        # subscribe to — clean no-op, not an error.
        return _response(200, {"ignored": "not a workflow_job event"})

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        logger.error("malformed webhook JSON body: %s", exc)
        return _response(400, {"error": "malformed JSON"})

    action = payload.get("action")
    repo_full_name = (payload.get("repository") or {}).get("full_name")
    job = payload.get("workflow_job") or {}
    labels = job.get("labels") or []
    job_id = job.get("id")

    if (
        action != "queued"
        or repo_full_name != TARGET_REPO_FULL_NAME
        or TARGET_LABEL not in labels
        or job_id is None
    ):
        return _response(200, {"ignored": True, "action": action, "repo": repo_full_name})

    if not DISPATCH_ENABLED:
        logger.warning("CONFIG_RUNNER_DISPATCH_ENABLED=false — job %s NOT dispatched", job_id)
        return _response(200, {"launched": False, "reason": "disabled", "job_id": job_id})

    logger.info("queued job %s matches — self-invoking worker phase async", job_id)
    boto3.client("lambda", region_name=REGION).invoke(
        FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"],
        InvocationType="Event",
        Payload=json.dumps({"config_runner_job_id": str(job_id)}).encode("utf-8"),
    )
    return _response(200, {"accepted": True, "job_id": job_id})


def _running_config_runner_instance_ids(job_id: str) -> list[str]:
    return spot_dispatch.running_instance_ids(
        CONFIG_RUNNER_TAG_NAME,
        {CONFIG_RUNNER_JOB_ID_TAG_KEY: job_id},
        region=REGION,
    )


def _bootstrap_command(job_id: str) -> str:
    """The async SSM RunShellScript prelude: minimal-install, fetch the PAT,
    clone alpha-engine-config, exec its config_runner_spot_bootstrap.sh
    entrypoint. Mirrors ci-watch-dispatcher's prelude exactly (same fail()
    trap shape) — the difference is entirely in what runs AFTER the clone."""
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
export HOME=/root
fail() {{ echo "[config-runner-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
dnf install -y -q git python3.12 >/dev/null 2>&1 || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {CONFIG_RUNNER_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {CONFIG_RUNNER_CONFIG_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{TARGET_REPO_FULL_NAME}.git" \
  /home/ec2-user/alpha-engine-config || fail "clone failed"
cd /home/ec2-user/alpha-engine-config
exec bash infrastructure/config_runner_spot_bootstrap.sh --job-id "{job_id}"
"""


def _launch_config_runner_spot(job_id: str) -> dict:
    dedupe_degraded = False
    try:
        existing = _running_config_runner_instance_ids(job_id)
    except SpotProbeError as exc:
        # Coverage beats dedupe (same policy as ci-watch-dispatcher, config#2267
        # site 1): a failed probe must never leave a real queued job uncovered.
        dedupe_degraded = True
        existing = []
        logger.error(
            "config-runner concurrency probe FAILED for job %s — proceeding "
            "with dedupe_degraded=true: %s: %s", job_id, type(exc).__name__, exc,
        )
    if existing:
        logger.warning("config-runner box already live for job %s (%s) — skipping",
                       job_id, existing)
        return {"launched": False, "reason": "concurrent_skip", "existing_instance_ids": existing}

    try:
        instance_id, market = spot_dispatch.launch_with_fallback(
            INSTANCE_TYPES, SUBNETS,
            image_id=AMI_ID,
            key_name=KEY_NAME,
            security_group_ids=[SECURITY_GROUP],
            iam_instance_profile=IAM_PROFILE,
            volume_size_gb=VOLUME_SIZE_GB,
            tag_name=CONFIG_RUNNER_TAG_NAME,
            region=REGION,
        )
    except SpotLaunchError as exc:
        logger.error("config-runner spot launch failed for job %s: %s: %s",
                     job_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched config-runner box %s (%s) for job %s%s", instance_id, market, job_id,
                " dedupe_degraded=true" if dedupe_degraded else "")

    try:
        boto3.client("ec2", region_name=REGION).create_tags(
            Resources=[instance_id],
            Tags=[{"Key": CONFIG_RUNNER_JOB_ID_TAG_KEY, "Value": job_id}],
        )
    except Exception as exc:  # noqa: BLE001 — load-bearing tag write; terminate + fail loud on failure
        spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="config-runner")
        logger.error("config-runner discriminator tag write FAILED for %s (job %s) — "
                     "box terminated, dispatch failed: %s: %s",
                     instance_id, job_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "tag_write_failed", "instance_id": instance_id,
                "error": str(exc), "dedupe_degraded": dedupe_degraded}

    try:
        spot_dispatch.wait_ssm_online(
            instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
        )
        command_id = spot_dispatch.send_async_command(
            instance_id,
            _bootstrap_command(job_id),
            comment=f"config-runner (job {job_id})",
            region=REGION,
            cw_log_group=CW_LOG_GROUP,
            execution_timeout_seconds=MAX_RUNTIME_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001 — post-launch failure; terminate the orphan
        spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="config-runner")
        logger.error("config-runner post-launch step failed for %s (job %s): %s: %s",
                     instance_id, job_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "post_launch_failed", "instance_id": instance_id,
                "error": str(exc), "dedupe_degraded": dedupe_degraded}

    logger.info("config-runner dispatched: instance=%s market=%s command=%s job_id=%s",
               instance_id, market, command_id, job_id)
    return {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "job_id": job_id,
        "dedupe_degraded": dedupe_degraded,
    }


def handler(event: dict, context) -> dict:
    """Two-phase entrypoint — see module docstring. A Function URL delivery
    carries `requestContext` (the API Gateway v2-shaped proxy event); the
    async self-invoked worker payload does not."""
    event = event or {}
    if "requestContext" in event:
        return _handle_webhook(event)

    job_id = event.get("config_runner_job_id")
    if not job_id:
        logger.error("worker-phase invocation missing config_runner_job_id: %r", event)
        return {"launched": False, "reason": "invalid_event"}
    return _launch_config_runner_spot(str(job_id))
