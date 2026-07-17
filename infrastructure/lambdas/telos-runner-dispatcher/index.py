"""telos-runner-dispatcher — launch an EPHEMERAL GitHub Actions
self-hosted runner on a dedicated EC2 spot box, once per queued CI/validation
job in nousergon/telos. Mirrors nousergon/alpha-engine-config's
self-hosted-runner dispatcher (alpha-engine-config-I2572) verbatim in
mechanism, re-namespaced for telos.

WHY: a hard GHA audit (2026-07-14) found telos's own CI/
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

MECHANISM (three-phase single Lambda, self-invoked async — see module-level
`handler` for the branch):
  1. WEBHOOK RECEIVER phase: GitHub calls this Lambda's Function URL directly
     on every `workflow_job` event for telos (repo-level
     webhook, event type `workflow_job`). Verifies the HMAC signature, filters
     to `action=queued` + our `telos-spot` label, then
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
     command that clones telos and `exec`s
     `infrastructure/telos_runner_spot_bootstrap.sh` (built by a sibling
     agent in telos) — which installs+registers the actual
     `actions-runner` binary in `--ephemeral` mode, runs exactly one job, and
     self-terminates (InstanceInitiatedShutdownBehavior=terminate + its own
     on-box watchdog). Mirrors `ci-watch-dispatcher/index.py`'s
     launch/wait/dispatch shape via the shared `nousergon_lib.spot_dispatch`
     primitives (config#2106) — NOT reinvented here.
  3. RECONCILE phase (EventBridge Scheduler, ~every 60s — config-I2653): a
     self-hosted runner registered via a plain registration token binds to
     the repo's WHOLE label pool, not the specific job that triggered its
     launch — GitHub has no API to reserve a job for a not-yet-connected
     runner (confirmed against GitHub's own docs before implementing this;
     an earlier JIT-runner-config diagnosis for this issue was WRONG — JIT
     is a more secure way to mint the same "any matching job" registration,
     it does not bind to one job either). Under concurrent load a dispatched
     runner can grab an unrelated queued job, leaving the job that triggered
     its launch permanently stuck — GitHub sends the `queued` webhook exactly
     ONCE per job, so there is no other path back to it without this backstop.
     `_reconcile()` lists queued jobs matching our label that have sat queued
     longer than `TELOS_RUNNER_RECONCILE_STALE_SECONDS` with no in-flight
     box (the existing job-id-tag dedup check), and dispatches a fresh runner
     for each. Same "coverage beats dedupe" posture as the SpotProbeError
     handling below — doesn't prevent the occasional mismatch, guarantees no
     job stays stranded indefinitely.

CIRCUIT BREAKER + FLEET CAP (2026-07-15 spot-quota-starvation incident,
config#2697): the reconcile backstop above is intentionally unbounded in
WHEN it fires (every stale queued job, every ~60s pass, forever) — that is
its whole point (I2653). What it lacked was a bound on repeated launches
for a job whose boxes keep failing identically, and a global ceiling on
total fleet size. Both live in `_launch_telos_runner_spot()` (the single
chokepoint the webhook-worker path AND the reconcile path both funnel
through, including the config#2267 dedupe_degraded "proceed anyway" path):
a job at/over `TELOS_RUNNER_MAX_ATTEMPTS_PER_JOB` launches (default 3,
counting live+recently-terminated boxes) is abandoned (paged once, no
further dispatch until a human/code-fix intervenes); the fleet overall is
capped at `TELOS_RUNNER_MAX_FLEET` running+pending boxes (default 6 = 12
vCPUs, leaving >=20 of the account's 32-vCPU standard-spot quota for
production) regardless of per-job attempt counts. A `AlphaEngine/Infra
telos_runner_launches` custom metric + CloudWatch alarm
(infrastructure/setup_telos_runner_launch_rate_alarm.sh) is the
independent early-warning signal on launch RATE itself, since the incident
ran ~3h with only a single quota-exhaustion page at the very end.

CONCURRENCY LOCK: keyed on `Name=telos-runner-spot` +
`telos-runner-job-id=<workflow_job.id>` — one job, one box, 1:1 (unlike
ci-watch's repo+sha lock, a GitHub Actions job id is already the unique
discriminator; no broader key needed). A duplicate `queued` delivery for the
same job (GitHub webhooks are at-least-once) is a clean no-op skip.

IAM PROFILE: `telos-runner-executor-profile`, a NEW dedicated
instance profile (infrastructure/iam/telos-runner-executor-role-*.json in
telos) scoped to read exactly one SSM param (the runner-
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
with Administration:write on telos exists in SSM — see
alpha-engine-config-I2572 (the original design this dispatcher mirrors)
for the full rollout sequencing rationale.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotLaunchError, SpotProbeError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: TELOS_RUNNER_DISPATCH_ENABLED=false disables the launch
# without touching the GitHub webhook wiring — mirrors every other fleet
# dispatcher's safety valve. Default ON.
DISPATCH_ENABLED = os.environ.get("TELOS_RUNNER_DISPATCH_ENABLED", "true").lower() == "true"

TARGET_REPO_FULL_NAME = "nousergon/telos"
TARGET_LABEL = "telos-spot"

# ── Spot launch config (env-overridable; same default-VPC/AMI/security-group
# as the fleet's other spot dispatchers — only the IAM profile + tag differ).
# IAM LOCKSTEP (mirrors ci-watch-dispatcher): these defaults are ENUMERATED
# in this Lambda's scoped ec2:RunInstances policy (sibling iam-policy.json).
# Changing them without re-applying that policy makes RunInstances fail with
# UnauthorizedOperation at the next dispatch. Keep them in sync.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "TELOS_RUNNER_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "TELOS_RUNNER_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("TELOS_RUNNER_AMI_ID", "ami-0c421724a94bba6d6")  # Amazon Linux 2023 x86_64
KEY_NAME = os.environ.get("TELOS_RUNNER_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("TELOS_RUNNER_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
IAM_PROFILE = os.environ.get(
    "TELOS_RUNNER_IAM_PROFILE", "telos-runner-executor-profile"
)
VOLUME_SIZE_GB = int(os.environ.get("TELOS_RUNNER_VOLUME_SIZE_GB", "30"))

TELOS_RUNNER_TAG_NAME = "telos-runner-spot"
TELOS_RUNNER_JOB_ID_TAG_KEY = "telos-runner-job-id"

# ── Circuit breaker + fleet cap (2026-07-15 spot-quota-starvation incident,
# alpha-engine-config#2697 — the incident this circuit breaker design was
# originally built for; mirrored here unchanged) ──────────────────────────
# With every runner box failing identically (a deprecated runner version —
# tracked separately), _reconcile() relaunched a fresh box per stuck queued
# job on every ~60s pass, FOREVER: ~150 t3.medium spot launches in 45 min,
# ~16 concurrent boxes = 32 vCPUs = 100% of the account's standard-spot quota
# (L-34B43A08, value 32). The post-close trading SF's data-spot launch then
# failed MaxSpotInstanceCountExceeded at 20:00 UTC — CI must never be able to
# starve production of spot quota. Two independent bounds, both enforced
# inside _launch_telos_runner_spot (the single chokepoint both the reactive
# webhook-worker path AND the reconcile path funnel through — including the
# config#2267 dedupe_degraded "proceed anyway" path, which is exactly where
# an unbounded loop would otherwise re-open):
#
#   1. PER-JOB ATTEMPT LIMIT: once a job has had >= MAX_ATTEMPTS_PER_JOB boxes
#      launched for it (live OR recently-terminated — a job whose boxes keep
#      dying identically is exactly the runaway pattern), stop dispatching
#      for THAT job and page once. Does not touch other jobs' dispatch, and
#      does not disable the reconcile backstop itself (config-I2653 still
#      recovers jobs whose one-shot `queued` webhook was consumed by a runner
#      that grabbed a different job — this only bounds the RETRY count for a
#      job that keeps failing).
#   2. GLOBAL FLEET CAP: refuse to launch (for ANY job) once
#      running+pending telos-runner boxes >= MAX_FLEET, regardless of
#      per-job attempt counts — the last-resort backstop against any launch
#      pattern (not just the identical-failure one above) that could
#      otherwise still saturate the account's spot quota.
TELOS_RUNNER_MAX_ATTEMPTS_PER_JOB = int(os.environ.get("TELOS_RUNNER_MAX_ATTEMPTS_PER_JOB", "3"))
# ~1h: describe-instances reliably still returns a terminated instance for
# about this long after termination (AWS does not document an exact
# retention SLA; this matches the window the issue's incident review
# confirmed empirically and is env-tunable if that changes).
TELOS_RUNNER_ATTEMPT_LOOKBACK_SECONDS = int(
    os.environ.get("TELOS_RUNNER_ATTEMPT_LOOKBACK_SECONDS", "3600")
)
# 6 boxes = 12 vCPUs (t3/t3a/t2.medium = 2 vCPUs each), leaving >= 20 vCPUs of
# the 32-vCPU account standard-spot quota (L-34B43A08) for production.
TELOS_RUNNER_MAX_FLEET = int(os.environ.get("TELOS_RUNNER_MAX_FLEET", "6"))

# Jobs that tripped the per-job attempt limit this cold start — logged/paged
# once per job per cold start rather than once per reconcile pass (a stuck
# job is still stale on every subsequent ~60s pass; repaging it every minute
# forever would be exactly the "single quota page at the very end" alerting
# failure mode this issue is also about, just for a different signal).
_abandoned_job_ids_paged: set[str] = set()

# ── Two-phase bootstrap state machine (2026-07-15 zombie-leak incident,
# alpha-engine-config-I2692 — source incident for this two-phase bootstrap
# design; mirrored here unchanged) ─────────────────────────────────────────
# The launch path previously did wait_ssm_online (60-200s) + send_command
# INSIDE this Lambda's 60s timeout — the Lambda died mid-wait, the box never
# received its bootstrap, never registered a runner, and never self-
# terminated (the terminate-on-failure except died with the Lambda). Under
# the reconcile loop that manufactured a zombie box per minute until the
# spot quota exhausted and ALL fleet CI queued (2026-07-15 17:33-18:40 UTC).
# Now: launch TAGS the box and returns in seconds; every ~60s reconcile pass
# delivers the bootstrap to any SSM-online box that lacks it (single
# describe, zero waiting), reaps boxes whose bootstrap can't be delivered by
# BOOTSTRAP_DEADLINE, and reaps any box alive past RUNNER_MAX_LIFETIME
# regardless of state (the janitor: no leak class can ever eat the quota
# silently again).
TELOS_RUNNER_BOOTSTRAP_SENT_TAG_KEY = "telos-runner-bootstrap-sent"
BOOTSTRAP_DEADLINE_SECONDS = int(os.environ.get("TELOS_RUNNER_BOOTSTRAP_DEADLINE_SECONDS", "300"))
RUNNER_MAX_LIFETIME_SECONDS = int(os.environ.get("TELOS_RUNNER_MAX_LIFETIME_SECONDS", "5400"))

# Loud page on quota exhaustion (the incident's silent failure mode: an hour
# of CI paralysis visible only as ERROR log lines). Fleet-standard params.
TELEGRAM_BOT_TOKEN_SSM = os.environ.get("TELOS_RUNNER_TELEGRAM_BOT_TOKEN_SSM",
                                        "/alpha-engine/TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID_SSM = os.environ.get("TELOS_RUNNER_TELEGRAM_CHAT_ID_SSM",
                                      "/alpha-engine/TELEGRAM_CHAT_ID")
_page_sent_this_invocation = False


def _page(message: str) -> None:
    """Best-effort LOUD Telegram page (disable_notification=False — the
    config-I2461 lesson: silent notifications get missed). Degrades to
    log-only on any failure; at most one page per invocation to avoid a
    reconcile loop spamming one page per queued job."""
    global _page_sent_this_invocation
    if _page_sent_this_invocation:
        return
    _page_sent_this_invocation = True
    try:
        ssm = boto3.client("ssm", region_name=REGION)
        token = ssm.get_parameter(Name=TELEGRAM_BOT_TOKEN_SSM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TELEGRAM_CHAT_ID_SSM, WithDecryption=True)["Parameter"]["Value"]
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=json.dumps({"chat_id": chat_id, "text": message,
                             "disable_notification": False}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as exc:  # noqa: BLE001 — paging is best-effort; the log line below is the fallback surface
        logger.error("telegram page FAILED (%s): %s — message was: %s",
                     type(exc).__name__, exc, message)

# This Lambda's OWN secret (unlike ci-watch-dispatcher, which needs none) —
# the webhook HMAC secret, read once per cold start and cached at module
# scope (same pattern as every fleet Lambda that reads a param once, not
# per-invocation, to keep p50 latency low on the hot webhook-verification path).
WEBHOOK_SECRET_SSM = os.environ.get(
    "TELOS_RUNNER_WEBHOOK_SECRET_SSM", "/telos/runner/webhook_secret"
)
_webhook_secret_cache: str | None = None


def _webhook_secret() -> str:
    global _webhook_secret_cache
    if _webhook_secret_cache is None:
        _webhook_secret_cache = boto3.client("ssm", region_name=REGION).get_parameter(
            Name=WEBHOOK_SECRET_SSM, WithDecryption=True  # gitleaks:allow — SSM param path, not a secret value
        )["Parameter"]["Value"]
    return _webhook_secret_cache


TELOS_RUNNER_GH_PAT_SSM = os.environ.get(
    "TELOS_RUNNER_GH_PAT_SSM", "/telos/runner/github_pat"
)
TELOS_RUNNER_CONFIG_BRANCH = os.environ.get("TELOS_RUNNER_CONFIG_BRANCH", "main")
MAX_RUNTIME_SECONDS = int(os.environ.get("TELOS_RUNNER_MAX_RUNTIME_SECONDS", "1800"))
# (SSM_ONLINE_BUDGET_SEC removed with the in-Lambda wait, I2692 — the
# reconcile-driven bootstrap never waits; BOOTSTRAP_DEADLINE_SECONDS above
# is its replacement bound.)
CW_LOG_GROUP = os.environ.get("TELOS_RUNNER_CW_LOG_GROUP", "/telos/runner-spot")

# Reconcile backstop (config-I2653): a queued job older than this with no
# in-flight box (per the same job-id-tag dedup check the reactive path uses)
# gets a fresh dispatch. ~2-3x the typical observed dispatch+SSM-online
# latency (15-40s) — comfortably past normal in-flight dispatches, without
# waiting so long that a genuinely-orphaned job sits stuck for minutes.
RECONCILE_STALE_SECONDS = int(os.environ.get("TELOS_RUNNER_RECONCILE_STALE_SECONDS", "90"))
RECONCILE_MAX_QUEUED_RUNS = int(os.environ.get("TELOS_RUNNER_RECONCILE_MAX_QUEUED_RUNS", "50"))

# Runner-fleet concurrency cap (config-I2692 item 4, 2026-07-15): a CI burst
# must never be able to consume the WHOLE account spot quota by itself and
# starve sibling workloads (groom/data/watch boxes share the same quota).
# Default 20 concurrent telos-runner boxes (t3.medium-class, 2 vCPU each =
# 40 vCPU) leaves >half of the current 96-vCPU quota for everything else;
# env-tunable since the right number is a fleet-wide capacity tradeoff, not
# a fact this Lambda can derive on its own. A job that can't get a box this
# pass because the cap is full simply stays queued — the NEXT reconcile pass
# (~60s) retries once older boxes finish and free a slot.
MAX_CONCURRENT_RUNNERS = int(os.environ.get("TELOS_RUNNER_MAX_CONCURRENT", "20"))

TELOS_RUNNER_READ_PAT_SSM = os.environ.get(
    "TELOS_RUNNER_READ_PAT_SSM", "/telos/runner/github_read_pat"
)
_gh_read_pat_cache: str | None = None


def _gh_read_pat() -> str:
    """A SEPARATE, dedicated, least-privilege PAT (Actions: Read ONLY —
    cannot register runners, cannot touch repo settings) — deliberately NOT
    the Administration:write PAT the box uses to register runners. This
    Lambda sits behind a public, unauthenticated Function URL (HMAC
    verification protects the WEBHOOK path, not a credential the Lambda's
    own execution role can read); granting it read access to the powerful
    registration PAT would widen blast radius for no reason the reconcile
    logic actually needs (it only ever lists queued runs/jobs)."""
    global _gh_read_pat_cache
    if _gh_read_pat_cache is None:
        _gh_read_pat_cache = boto3.client("ssm", region_name=REGION).get_parameter(
            Name=TELOS_RUNNER_READ_PAT_SSM, WithDecryption=True  # gitleaks:allow — SSM param path, not a secret value
        )["Parameter"]["Value"]
    return _gh_read_pat_cache


def _gh_api_get(path: str) -> dict:
    req = urllib.request.Request(
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"Bearer {_gh_read_pat()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 - fixed https://api.github.com host
        return json.loads(resp.read())


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
        logger.warning("TELOS_RUNNER_DISPATCH_ENABLED=false — job %s NOT dispatched", job_id)
        return _response(200, {"launched": False, "reason": "disabled", "job_id": job_id})

    logger.info("queued job %s matches — self-invoking worker phase async", job_id)
    boto3.client("lambda", region_name=REGION).invoke(
        FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"],
        InvocationType="Event",
        Payload=json.dumps({"telos_runner_job_id": str(job_id)}).encode("utf-8"),
    )
    return _response(200, {"accepted": True, "job_id": job_id})


def _running_telos_runner_instance_ids(job_id: str) -> list[str]:
    return spot_dispatch.running_instance_ids(
        TELOS_RUNNER_TAG_NAME,
        {TELOS_RUNNER_JOB_ID_TAG_KEY: job_id},
        region=REGION,
    )


# Any state that counts as "we already tried this" — including terminated
# ones (a box that died is still a launch attempt against the job; that's
# the entire point of the circuit breaker: N identical failures must stop,
# not just N concurrently-alive boxes). shutting-down/stopping are
# transitional states a box passes through on its way to terminated.
_ANY_ATTEMPT_STATES = [
    "pending", "running", "shutting-down", "stopping", "stopped", "terminated",
]


def _job_attempt_count(job_id: str) -> int:
    """How many telos-runner boxes have been launched for ``job_id`` within
    the last ``TELOS_RUNNER_ATTEMPT_LOOKBACK_SECONDS`` (live or recently-
    terminated — describe-instances keeps a terminated instance queryable for
    ~1h, comfortably longer than this Lambda's own dispatch cadence). A
    DescribeInstances failure here must NOT degrade to "0 attempts, dispatch
    anyway" — that would silently defeat the entire circuit breaker on the
    exact kind of degraded-EC2-API pass that a runaway is most likely to
    coincide with, so it re-raises (the caller decides the fail-safe
    posture; see _launch_telos_runner_spot)."""
    ec2 = boto3.client("ec2", region_name=REGION)
    resp = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TELOS_RUNNER_TAG_NAME]},
        {"Name": f"tag:{TELOS_RUNNER_JOB_ID_TAG_KEY}", "Values": [job_id]},
        {"Name": "instance-state-name", "Values": _ANY_ATTEMPT_STATES},
    ])
    now = datetime.now(timezone.utc)
    count = 0
    for r in resp.get("Reservations", []):
        for i in r.get("Instances", []):
            launch_time = i.get("LaunchTime")
            if launch_time is not None:
                age = (now - launch_time).total_seconds()
                if age > TELOS_RUNNER_ATTEMPT_LOOKBACK_SECONDS:
                    continue
            count += 1
    return count


def _current_fleet_size() -> int:
    """Running+pending telos-runner boxes, fleet-wide (across ALL jobs) —
    the global cap backstop. Deliberately a SEPARATE query from
    _job_attempt_count (different filter shape, different failure posture):
    a failure here also does not degrade to "0, launch anyway" (see
    _launch_telos_runner_spot)."""
    ec2 = boto3.client("ec2", region_name=REGION)
    resp = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TELOS_RUNNER_TAG_NAME]},
        {"Name": "instance-state-name", "Values": ["pending", "running"]},
    ])
    return sum(len(r.get("Instances", [])) for r in resp.get("Reservations", []))


TELOS_RUNNER_LAUNCH_METRIC_NAMESPACE = "AlphaEngine/Infra"
TELOS_RUNNER_LAUNCH_METRIC_NAME = "telos_runner_launches"


def _emit_launch_metric() -> None:
    """One CloudWatch custom-metric datapoint per successful launch (mirrors
    spot-orphan-reaper's ``_emit_metric`` pattern — a namespace/metric-name
    pair, best-effort, never raises). This is the launch-rate alarm's data
    source (config#2697): the 2026-07-15 runaway ran ~3h at ~150 launches/45min
    with only a single quota page at the very end — a launch-COUNT alarm
    (independent of whether launches are currently succeeding or already
    failing quota) catches the runaway itself, not just its terminal
    symptom. Alarm provisioned by
    infrastructure/setup_telos_runner_launch_rate_alarm.sh (this repo has no
    log-metric-filter precedent; every existing alarm — see
    setup_watch_plane_alarms.sh, spot-orphan-reaper's own
    spot_orphans_terminated metric — alarms on a Lambda-emitted custom/AWS
    metric instead, which this follows)."""
    try:
        boto3.client("cloudwatch", region_name=REGION).put_metric_data(
            Namespace=TELOS_RUNNER_LAUNCH_METRIC_NAMESPACE,
            MetricData=[{
                "MetricName": TELOS_RUNNER_LAUNCH_METRIC_NAME,
                "Value": 1.0,
                "Unit": "Count",
            }],
        )
    except Exception as exc:  # noqa: BLE001 — observability only; must never block/fail a dispatch
        logger.warning("CloudWatch put_metric_data (%s) failed (non-fatal): %s",
                        TELOS_RUNNER_LAUNCH_METRIC_NAME, exc)


def _bootstrap_command(job_id: str) -> str:
    """The async SSM RunShellScript prelude: minimal-install, fetch the PAT,
    clone telos, exec its telos_runner_spot_bootstrap.sh
    entrypoint. Mirrors ci-watch-dispatcher's prelude exactly (same fail()
    trap shape) — the difference is entirely in what runs AFTER the clone."""
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
export HOME=/root
fail() {{ echo "[telos-runner-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
dnf install -y -q git python3.12 >/dev/null 2>&1 || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {TELOS_RUNNER_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/telos
git clone --depth 1 --branch {TELOS_RUNNER_CONFIG_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{TARGET_REPO_FULL_NAME}.git" \
  /home/ec2-user/telos || fail "clone failed"
cd /home/ec2-user/telos
exec bash infrastructure/telos_runner_spot_bootstrap.sh --job-id "{job_id}"
"""


def _launch_telos_runner_spot(job_id: str) -> dict:
    # ── Circuit breaker + fleet cap (config#2697) — checked FIRST, ahead of
    # the dedup probe, so both bind unconditionally: in particular, the
    # per-job attempt limit MUST still apply on the config#2267
    # dedupe_degraded "proceed anyway" path below, or a runaway job just
    # re-opens the loop there instead. Both counts fail LOUD (re-raise) on a
    # DescribeInstances error rather than degrade to "0, dispatch anyway" —
    # unlike the dedup probe, coverage does NOT beat safety here: the whole
    # point of a circuit breaker is that it must hold even when the AWS API
    # is degraded, which is exactly when a runaway is likely to be underway.
    try:
        attempt_count = _job_attempt_count(job_id)
    except Exception as exc:  # noqa: BLE001 — fail-safe: an unknown attempt count blocks the launch
        logger.error(
            "telos-runner attempt-count probe FAILED for job %s — refusing to "
            "dispatch (fail-safe, unlike the dedupe probe): %s: %s",
            job_id, type(exc).__name__, exc,
        )
        return {"launched": False, "reason": "attempt_probe_failed", "job_id": job_id,
                "error": str(exc)}

    if attempt_count >= TELOS_RUNNER_MAX_ATTEMPTS_PER_JOB:
        if job_id not in _abandoned_job_ids_paged:
            _abandoned_job_ids_paged.add(job_id)
            _page(
                f"🔴 telos-runner: job {job_id} ABANDONED after "
                f"{attempt_count} launch attempts (limit "
                f"{TELOS_RUNNER_MAX_ATTEMPTS_PER_JOB}) — every box likely "
                "failing identically (e.g. a bad runner version/bootstrap "
                "script). No further boxes will be dispatched for this job "
                "until a human intervenes or ships a fix. "
                "alpha-engine-config#2697 (source incident for this circuit breaker)."
            )
        logger.error(
            "telos-runner job %s ABANDONED — %d attempts >= limit %d, "
            "not dispatching", job_id, attempt_count, TELOS_RUNNER_MAX_ATTEMPTS_PER_JOB,
        )
        return {"launched": False, "reason": "attempt_limit_exceeded", "job_id": job_id,
                "attempt_count": attempt_count}

    try:
        fleet_size = _current_fleet_size()
    except Exception as exc:  # noqa: BLE001 — fail-safe: an unknown fleet size blocks the launch
        logger.error(
            "telos-runner fleet-size probe FAILED for job %s — refusing to "
            "dispatch (fail-safe): %s: %s", job_id, type(exc).__name__, exc,
        )
        return {"launched": False, "reason": "fleet_probe_failed", "job_id": job_id,
                "error": str(exc)}

    if fleet_size >= TELOS_RUNNER_MAX_FLEET:
        _page(
            f"🔴 telos-runner: fleet cap reached ({fleet_size} >= "
            f"{TELOS_RUNNER_MAX_FLEET}) — job {job_id} NOT dispatched this "
            "pass. Refusing further launches until the fleet shrinks (global "
            "backstop against spot-quota starvation; design mirrors alpha-engine-config#2697)."
        )
        logger.error(
            "telos-runner FLEET CAP reached (%d >= %d) — job %s not "
            "dispatched", fleet_size, TELOS_RUNNER_MAX_FLEET, job_id,
        )
        return {"launched": False, "reason": "fleet_cap_reached", "job_id": job_id,
                "fleet_size": fleet_size}

    dedupe_degraded = False
    try:
        existing = _running_telos_runner_instance_ids(job_id)
    except SpotProbeError as exc:
        # Coverage beats dedupe (same policy as ci-watch-dispatcher, config#2267
        # site 1): a failed probe must never leave a real queued job uncovered.
        dedupe_degraded = True
        existing = []
        logger.error(
            "telos-runner concurrency probe FAILED for job %s — proceeding "
            "with dedupe_degraded=true: %s: %s", job_id, type(exc).__name__, exc,
        )
    if existing:
        logger.warning("telos-runner box already live for job %s (%s) — skipping",
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
            tag_name=TELOS_RUNNER_TAG_NAME,
            region=REGION,
        )
    except SpotLaunchError as exc:
        logger.error("telos-runner spot launch failed for job %s: %s: %s",
                     job_id, type(exc).__name__, exc)
        if "MaxSpotInstanceCountExceeded" in str(exc):
            _page("🔴 telos-runner: spot QUOTA EXHAUSTED "
                  "(MaxSpotInstanceCountExceeded) — CI dispatch for "
                  f"{TARGET_REPO_FULL_NAME} is stalling. Check for leaked "
                  "runner boxes (aws ec2 describe-instances "
                  f"Name={TELOS_RUNNER_TAG_NAME}) — the reconcile janitor "
                  "should be reaping them; if this page repeats, it isn't. "
                  "design mirrors alpha-engine-config-I2692.")
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched telos-runner box %s (%s) for job %s%s", instance_id, market, job_id,
                " dedupe_degraded=true" if dedupe_degraded else "")
    _emit_launch_metric()

    try:
        boto3.client("ec2", region_name=REGION).create_tags(
            Resources=[instance_id],
            Tags=[{"Key": TELOS_RUNNER_JOB_ID_TAG_KEY, "Value": job_id}],
        )
    except Exception as exc:  # noqa: BLE001 — load-bearing tag write; terminate + fail loud on failure
        spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="telos-runner")
        logger.error("telos-runner discriminator tag write FAILED for %s (job %s) — "
                     "box terminated, dispatch failed: %s: %s",
                     instance_id, job_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "tag_write_failed", "instance_id": instance_id,
                "error": str(exc), "dedupe_degraded": dedupe_degraded}

    # Two-phase contract (I2692): launch + tag ONLY — this function must
    # return in seconds. NO wait_ssm_online, NO send_command here: the old
    # in-line wait blew this Lambda's 60s timeout, killing the bootstrap AND
    # the terminate-on-failure handler with it (the 2026-07-15 zombie-leak
    # incident). _bootstrap_and_reap() (every reconcile pass, ~60s) delivers
    # the bootstrap once the box's SSM agent is online — which takes 40-90s
    # after launch anyway, so this adds ~zero real latency.
    logger.info("telos-runner launched (bootstrap deferred to reconcile): "
                "instance=%s market=%s job_id=%s", instance_id, market, job_id)
    return {
        "launched": True,
        "reason": "launched_bootstrap_pending",
        "instance_id": instance_id,
        "market": market,
        "job_id": job_id,
        "dedupe_degraded": dedupe_degraded,
    }


def _bootstrap_and_reap() -> dict:
    """The reconcile-driven bootstrap deliverer + janitor (I2692). For every
    running telos-runner box, exactly one of:

      - no bootstrap-sent tag + SSM Online   -> send bootstrap, tag it
      - no bootstrap-sent tag + SSM not up   -> reap once older than
        BOOTSTRAP_DEADLINE_SECONDS (SSM never came up / undeliverable)
      - any box older than RUNNER_MAX_LIFETIME_SECONDS -> reap (leak
        backstop: an ephemeral runner that hasn't self-terminated by then is
        a zombie regardless of how it got wedged)

    Every step is a single fast API call — no waiting anywhere, so any
    number of boxes fits inside the Lambda budget. Never raises: one box's
    failure must not strand the rest."""
    ec2 = boto3.client("ec2", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    stats = {"bootstrapped": 0, "reaped_no_ssm": 0, "reaped_lifetime": 0,
             "waiting_ssm": 0, "healthy": 0, "errors": 0, "running_after_reap": 0}
    now = datetime.now(timezone.utc)

    try:
        resp = ec2.describe_instances(Filters=[
            {"Name": "tag:Name", "Values": [TELOS_RUNNER_TAG_NAME]},
            {"Name": "instance-state-name", "Values": ["running", "pending"]},
        ])
        boxes = [i for r in resp.get("Reservations", []) for i in r.get("Instances", [])]
    except Exception as exc:  # noqa: BLE001 — enumeration failure: nothing to do this pass, retry next
        logger.error("bootstrap_and_reap: describe_instances failed: %s: %s",
                     type(exc).__name__, exc)
        stats["errors"] += 1
        return stats

    online_ids: set[str] = set()
    if boxes:
        try:
            info = ssm.describe_instance_information(Filters=[
                {"Key": "InstanceIds", "Values": [b["InstanceId"] for b in boxes]},
            ]).get("InstanceInformationList", [])
            online_ids = {i["InstanceId"] for i in info if i.get("PingStatus") == "Online"}
        except Exception as exc:  # noqa: BLE001 — degrade to "none online"; young boxes wait, old ones still reap
            logger.error("bootstrap_and_reap: SSM describe failed: %s: %s",
                         type(exc).__name__, exc)

    for box in boxes:
        iid = box["InstanceId"]
        tags = {t["Key"]: t["Value"] for t in box.get("Tags", [])}
        age = (now - box["LaunchTime"]).total_seconds()

        if age > RUNNER_MAX_LIFETIME_SECONDS:
            try:
                ec2.terminate_instances(InstanceIds=[iid])
                logger.warning("bootstrap_and_reap: REAPED %s (lifetime %ds > %ds — leaked box)",
                               iid, int(age), RUNNER_MAX_LIFETIME_SECONDS)
                stats["reaped_lifetime"] += 1
            except Exception as exc:  # noqa: BLE001 — retried next pass
                logger.error("bootstrap_and_reap: reap %s failed: %s", iid, exc)
                stats["errors"] += 1
            continue

        if TELOS_RUNNER_BOOTSTRAP_SENT_TAG_KEY in tags:
            stats["healthy"] += 1
            continue

        job_id = tags.get(TELOS_RUNNER_JOB_ID_TAG_KEY, "")
        if iid in online_ids and job_id:
            try:
                spot_dispatch.send_async_command(
                    iid,
                    _bootstrap_command(job_id),
                    comment=f"telos-runner (job {job_id})",
                    region=REGION,
                    cw_log_group=CW_LOG_GROUP,
                    execution_timeout_seconds=MAX_RUNTIME_SECONDS,
                )
                ec2.create_tags(Resources=[iid], Tags=[{
                    "Key": TELOS_RUNNER_BOOTSTRAP_SENT_TAG_KEY,
                    "Value": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }])
                logger.info("bootstrap_and_reap: bootstrapped %s (job %s, %ds after launch)",
                            iid, job_id, int(age))
                stats["bootstrapped"] += 1
            except Exception as exc:  # noqa: BLE001 — send/tag failure: untagged, retried next pass
                logger.error("bootstrap_and_reap: bootstrap %s failed (retry next pass): %s: %s",
                             iid, type(exc).__name__, exc)
                stats["errors"] += 1
        elif age > BOOTSTRAP_DEADLINE_SECONDS:
            try:
                ec2.terminate_instances(InstanceIds=[iid])
                logger.warning("bootstrap_and_reap: REAPED %s (no SSM/job-id after %ds — "
                               "bootstrap undeliverable)", iid, int(age))
                stats["reaped_no_ssm"] += 1
            except Exception as exc:  # noqa: BLE001 — retried next pass
                logger.error("bootstrap_and_reap: reap %s failed: %s", iid, exc)
                stats["errors"] += 1
        else:
            stats["waiting_ssm"] += 1

    stats["running_after_reap"] = len(boxes) - stats["reaped_no_ssm"] - stats["reaped_lifetime"]
    logger.info("bootstrap_and_reap: %s", stats)
    return stats


def _reconcile() -> dict:
    """Scheduled backstop (~every 60s, config-I2653): dispatch a fresh runner
    for any queued job matching our label that's had no in-flight box for
    RECONCILE_STALE_SECONDS. See module docstring phase 3 for the full
    rationale — this is NOT redundant with the reactive webhook path; it is
    the only mechanism that can recover a job whose one-shot `queued`
    webhook already fired but whose dispatched runner grabbed a different
    job instead."""
    if not DISPATCH_ENABLED:
        logger.info("reconcile: TELOS_RUNNER_DISPATCH_ENABLED=false — skipping")
        return {"reconciled": 0, "reason": "disabled"}

    # Bootstrap-deliverer + janitor FIRST (I2692): serve boxes already up
    # before launching new ones, and reap zombies so the queued-job scan
    # below never launches into an exhausted quota that leaked boxes caused.
    br_stats = _bootstrap_and_reap()

    now = datetime.now(timezone.utc)
    try:
        runs_resp = _gh_api_get(
            f"/repos/{TARGET_REPO_FULL_NAME}/actions/runs"
            f"?status=queued&per_page={RECONCILE_MAX_QUEUED_RUNS}"
        )
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        # Best-effort: a failed reconcile pass just means "no backstop this
        # minute" — the NEXT scheduled invocation tries again. Never raise
        # (this is EventBridge-invoked; an unhandled exception would just
        # generate a Lambda-error-metric page for a transient GitHub API
        # blip with no actionable fix).
        logger.error("reconcile: failed to list queued runs: %s: %s", type(exc).__name__, exc)
        return {"reconciled": 0, "reason": "list_runs_failed", "error": str(exc)}

    # Collect ALL stale queued jobs first, then dispatch OLDEST-FIRST.
    # GitHub lists runs newest-first; dispatching in listing order let a
    # constant churn of fresh runs (e.g. the 2026-07-15 Dependabot-drain
    # rebase wave) win every quota-constrained launch race while the oldest
    # job starved indefinitely (PR2690's pytest sat queued 95+ min while
    # newer jobs got every available spot slot). FIFO makes quota pressure
    # degrade to bounded latency for everyone instead of unbounded latency
    # for the unluckiest.
    stale_jobs: list[tuple[float, str]] = []
    for run in runs_resp.get("workflow_runs", []):
        try:
            jobs_resp = _gh_api_get(
                f"/repos/{TARGET_REPO_FULL_NAME}/actions/runs/{run['id']}/jobs"
            )
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            logger.error("reconcile: failed to list jobs for run %s: %s: %s",
                         run.get("id"), type(exc).__name__, exc)
            continue
        for job in jobs_resp.get("jobs", []):
            if job.get("status") != "queued" or TARGET_LABEL not in (job.get("labels") or []):
                continue
            created_at = datetime.fromisoformat(job["created_at"].replace("Z", "+00:00"))
            age_seconds = (now - created_at).total_seconds()
            if age_seconds < RECONCILE_STALE_SECONDS:
                continue  # still within the reactive path's normal dispatch latency
            stale_jobs.append((age_seconds, str(job["id"])))

    # Concurrency cap (I2692 item 4): count this pass's OWN dispatches against
    # the fleet already standing after bootstrap_and_reap's janitor pass, so a
    # single burst of stale jobs can't blow past the cap in one reconcile.
    available_slots = max(0, MAX_CONCURRENT_RUNNERS - br_stats.get("running_after_reap", 0))
    capped_at_slot_zero = False

    dispatched = []
    skipped = []
    for age_seconds, job_id in sorted(stale_jobs, reverse=True):  # oldest first
        try:
            existing = _running_telos_runner_instance_ids(job_id)
        except SpotProbeError:
            existing = []  # degrade to "dispatch anyway" — the launch's own dedup/tag logic is authoritative
        if existing:
            skipped.append(job_id)
            continue
        if available_slots <= 0:
            logger.warning("reconcile: job %s queued %.0fs but MAX_CONCURRENT_RUNNERS=%d "
                            "already reached — leaving queued for next pass",
                            job_id, age_seconds, MAX_CONCURRENT_RUNNERS)
            skipped.append(job_id)
            capped_at_slot_zero = True
            continue
        logger.info("reconcile: job %s queued %.0fs with no in-flight box — dispatching",
                   job_id, age_seconds)
        result = _launch_telos_runner_spot(job_id)
        if result.get("launched"):
            available_slots -= 1
        dispatched.append({"job_id": job_id, "age_seconds": age_seconds, "result": result})

    if capped_at_slot_zero:
        _page(f"⚠️ telos-runner: MAX_CONCURRENT_RUNNERS={MAX_CONCURRENT_RUNNERS} reached — "
              "one or more stale jobs left queued this reconcile pass pending a free slot. "
              "design mirrors alpha-engine-config-I2692.")

    logger.info("reconcile: dispatched=%d skipped=%d bootstrap_and_reap=%s",
                len(dispatched), len(skipped), br_stats)
    return {"reconciled": len(dispatched), "dispatched": dispatched,
            "skipped": skipped, "bootstrap_and_reap": br_stats}


def handler(event: dict, context) -> dict:
    """Three-phase entrypoint — see module docstring. A Function URL
    delivery carries `requestContext` (the API Gateway v2-shaped proxy
    event); the EventBridge-scheduled reconcile trigger carries
    `{"reconcile": true}`; the async self-invoked worker payload carries
    `telos_runner_job_id`."""
    event = event or {}
    if event.get("reconcile"):
        return _reconcile()
    if "requestContext" in event:
        return _handle_webhook(event)

    job_id = event.get("telos_runner_job_id")
    if not job_id:
        logger.error("worker-phase invocation missing telos_runner_job_id: %r", event)
        return {"launched": False, "reason": "invalid_event"}
    return _launch_telos_runner_spot(str(job_id))
