"""alpha-engine-ci-watch-dispatcher — launch the Fleet CI Watch diagnose+fix
agent on a dedicated EC2 spot box, once per real CI failure event.

WHY SPOT, NOT GITHUB ACTIONS: `sf-watch` (Fleet CI Watch, lives in
`nousergon/alpha-engine-config`) diagnoses+fixes fleet CI failures and merge
conflicts. Running that agent on GitHub-hosted Actions runners burned the
org's metered Actions-minutes budget — the same defect class that motivated
`scheduled-groom-dispatcher` (config#1432) — so CI-watch is currently gated to
Saturday-only. This Lambda moves it to EC2 spot, mirroring that PROVEN
dispatcher pattern, but SIMPLER: CI-watch fires once per real CI failure event
via a SYNCHRONOUS `lambda invoke` from a GHA job (not a schedule), so none of
groom's tier/model/demand-gate/pace-gate complexity applies here.

Mechanism (mirrors `scheduled-groom-dispatcher/index.py` via the shared
`nousergon_lib.spot_dispatch` primitives, config#2106 — concurrency lock,
launch-with-fallback, and terminate-on-failure are no longer duplicated
per-dispatcher):
  1. `spot_dispatch.launch_with_fallback()` rotates instance_type x subnet on
     capacity error; on SpotCapacityExhausted across all pools we relaunch
     ON-DEMAND (spot=False) so a capacity dip never silently drops a CI fix.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an async, detached `ssm send-command` (AWS-RunShellScript) carrying
     a small prelude: fetch the PAT from SSM, clone alpha-engine-config, then
     `exec infrastructure/ci_watch_spot_bootstrap.sh` (built by a sibling
     agent in alpha-engine-config). The box self-terminates
     (InstanceInitiatedShutdownBehavior=terminate + its own on-box watchdog).

SYNCHRONOUS CONTRACT (the key divergence from groom's fail-loud posture): a
GHA job invokes this Lambda with RequestResponse (not async) and branches
directly on the returned JSON. Every anticipated failure mode — concurrency
skip, spot+on-demand launch exhaustion, a malformed event, a post-launch SSM
failure — returns a clean, well-formed `{"launched": false, "reason": ...}`
rather than raising. Groom's Lambda deliberately RAISES on these same failure
classes because the caller there is EventBridge (retry-on-error is the
correct behavior for a scheduled job); CI-watch's caller is a synchronous GHA
step that needs an unambiguous JSON verdict to branch on, not a Lambda
invocation error to unwrap. Only a genuinely unexpected internal bug should
still propagate as a Python exception.

CONCURRENCY LOCK — narrower than groom's per-tier lock (config#1979):
keyed on `Name=alpha-engine-ci-watch-spot` + `ci-watch-repo=<repo>` +
`ci-watch-sha=<sha>` (NOT bare repo). Two different commits on the same repo
failing CI independently must each get their own box — a repo-only lock
would starve the second commit's fix. Fail-safe OPEN on any API error (same
posture as groom's `_running_tier_instance_ids`) — this guard is an
optimization against duplicate spend, never a correctness gate.

IAM PROFILE — deliberately NOT `alpha-engine-executor-profile` (shared with
the live trading executor). Uses `alpha-engine-ci-watch-executor-profile`, a
dedicated instance profile a sibling agent is creating in
`alpha-engine-config`'s IAM json files, so a CI-watch box's blast radius
never touches trading credentials.

Managed OUTSIDE CloudFormation (same as scheduled-groom-dispatcher): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect
until the new code + IAM are deployed AND a sibling agent's GHA workflow
(`sf-watch.yml` in alpha-engine-config) is wired to invoke this Lambda.
"""

from __future__ import annotations

import logging
import os
import re
import uuid

import boto3
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotCapacityExhausted, SpotLaunchError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: CI_WATCH_DISPATCH_ENABLED=false disables the launch without
# touching the GHA invoke wiring — mirrors every other fleet dispatcher's
# safety valve. Default ON.
DISPATCH_ENABLED = os.environ.get("CI_WATCH_DISPATCH_ENABLED", "true").lower() == "true"

# ── Spot launch config (env-overridable; defaults mirror scheduled-groom-
# dispatcher/data-spot-dispatcher — same default-VPC/AMI/security-group, only
# the IAM profile differs for blast-radius isolation). ────────────────────────
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "CI_WATCH_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "CI_WATCH_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("CI_WATCH_AMI_ID", "ami-0c421724a94bba6d6")  # Amazon Linux 2023 x86_64
KEY_NAME = os.environ.get("CI_WATCH_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("CI_WATCH_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# NEW, dedicated profile — deliberately NOT alpha-engine-executor-profile (the
# live trading executor's profile). See module docstring.
IAM_PROFILE = os.environ.get("CI_WATCH_IAM_PROFILE", "alpha-engine-ci-watch-executor-profile")
VOLUME_SIZE_GB = int(os.environ.get("CI_WATCH_VOLUME_SIZE_GB", "40"))

CI_WATCH_TAG_NAME = "alpha-engine-ci-watch-spot"
CI_WATCH_REPO_TAG_KEY = "ci-watch-repo"
CI_WATCH_SHA_TAG_KEY = "ci-watch-sha"

# The box reads its own run secrets (PAT) via its instance profile in the
# common case, but the PRELUDE below (run before the profile-backed bootstrap
# script takes over) still needs the PAT to clone the private config repo —
# same shape as groom's prelude. Reuses the SAME shared SSM param the other
# spot dispatchers already read (data-spot-dispatcher, scheduled-groom-
# dispatcher) rather than assuming a new dedicated param exists.
CI_WATCH_GH_PAT_SSM = os.environ.get(
    "CI_WATCH_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
CI_WATCH_CONFIG_REPO = os.environ.get("CI_WATCH_CONFIG_REPO", "nousergon/alpha-engine-config")
CI_WATCH_CONFIG_BRANCH = os.environ.get("CI_WATCH_CONFIG_BRANCH", "main")
# Hard ceiling for the on-box SSM command (matches the bootstrap watchdog). CI
# fixes are a much shorter-lived workload than a full groom sweep; 2h default,
# env-overridable if a sibling agent's bootstrap script needs more headroom.
MAX_RUNTIME_SECONDS = int(os.environ.get("CI_WATCH_MAX_RUNTIME_SECONDS", "7200"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("CI_WATCH_SSM_ONLINE_BUDGET_SEC", "180"))
CW_LOG_GROUP = os.environ.get("CI_WATCH_CW_LOG_GROUP", "/alpha-engine/ci-watch-spot")

# Defense-in-depth allowlists for event fields embedded verbatim into the SSM
# shell command below (mirrors groom's _MODEL_RE / data-spot's _WORKLOAD_RE).
# These come from a GHA job (not raw external user input), but the same cheap
# regex check rules out shell-metacharacter injection outright.
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
_RUN_ID_RE = re.compile(r"^[0-9]+$")
_BRANCH_RE = re.compile(r"^[A-Za-z0-9_./-]{1,200}$")
_WORKFLOW_RE = re.compile(r"^[A-Za-z0-9 _./:()-]{1,200}$")


class _InvalidEvent(ValueError):
    """A required event field is missing or fails its allowlist."""


def _require(event: dict, key: str, pattern: "re.Pattern[str]") -> str:
    val = str(event.get(key) or "").strip()
    if not pattern.match(val):
        raise _InvalidEvent(f"missing/malformed {key!r} in event: {val!r}")
    return val


def _resolve_event_fields(event: dict) -> tuple[str, str, str, str, str, str]:
    """Validate the GHA payload's CI fields; raises _InvalidEvent on any
    missing/malformed field (caught once, at the handler, and converted to a
    clean launched:false — see module docstring's synchronous contract)."""
    repo = _require(event, "repo", _REPO_RE)
    sha = _require(event, "sha", _SHA_RE)
    run_id = _require(event, "run_id", _RUN_ID_RE)
    workflow = _require(event, "workflow", _WORKFLOW_RE)
    branch = _require(event, "branch", _BRANCH_RE)
    run_url = str(event.get("run_url") or "").strip()
    if not run_url.startswith("https://") or "$" in run_url:
        # `$` would be embedded into the double-quoted bootstrap export below;
        # under `set -u` it could expand as a positional param (same gotcha
        # groom's run_url note documents) and abort the prelude.
        raise _InvalidEvent(f"missing/malformed 'run_url' in event: {run_url!r}")
    return repo, sha, run_id, run_url, workflow, branch


def _bootstrap_command(repo: str, sha: str, run_id: str, run_url: str,
                       workflow: str, branch: str, run_token: str) -> str:
    """The async SSM RunShellScript body: fetch PAT, clone config, exec the
    ci_watch_spot_bootstrap.sh entrypoint (built by a sibling agent in
    alpha-engine-config). Any prelude failure shuts the box down so a botched
    launch never idles (mirrors groom's prelude fail() trap exactly).

    ``ci_watch_spot_bootstrap.sh`` takes its CI fields as CLI FLAGS
    (``--ci-repo``/``--ci-sha``/...), not environment variables — invoke it
    that way, not via `export`. ``run_token`` is deliberately NOT threaded
    into the box: the bootstrap/run-script side keys its S3 completion
    marker directly on repo+sha (no per-attempt dispatch token, unlike
    groom's ``GROOM_RUN_TOKEN``), so there is no in-box consumer for it — it
    stays a Lambda-side-only correlation id (see the SSM Comment field in
    ``_send_bootstrap``, and the handler's returned JSON)."""
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
# SSM RunShellScript runs as root with NO $HOME set; git config/clone need it.
export HOME=/root
fail() {{ echo "[ci-watch-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
dnf install -y -q git python3.12 python3.12-pip >/dev/null 2>&1 \
  || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {CI_WATCH_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {CI_WATCH_CONFIG_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{CI_WATCH_CONFIG_REPO}.git" \
  /home/ec2-user/alpha-engine-config || fail "clone failed"
cd /home/ec2-user/alpha-engine-config
exec bash infrastructure/ci_watch_spot_bootstrap.sh \
  --ci-repo "{repo}" --ci-sha "{sha}" --ci-run-id "{run_id}" \
  --ci-run-url "{run_url}" --ci-workflow "{workflow}" --ci-branch "{branch}"
"""


def _launch_instance() -> tuple[str, str]:
    """Launch the CI-watch box; spot first, on-demand fallback on capacity
    exhaustion. Raises SpotLaunchError (or the SpotCapacityExhausted subclass)
    if BOTH the spot attempt and the on-demand fallback are exhausted/fail —
    caught once by the caller and converted to a clean launched:false."""
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=CI_WATCH_TAG_NAME,
        region=REGION,
    )


def _wait_ssm_online(instance_id: str) -> None:
    """Block until the instance is running AND its SSM agent registers Online."""
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(instance_id: str, repo: str, sha: str, run_id: str, run_url: str,
                    workflow: str, branch: str, run_token: str) -> str:
    """Fire the async, detached SSM command that runs CI-watch + self-terminates."""
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(repo, sha, run_id, run_url, workflow, branch, run_token),
        comment=f"ci-watch ({repo}@{sha[:12]}, run {run_id}, token {run_token[:12]})",
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=MAX_RUNTIME_SECONDS,
    )


def _running_ci_watch_instance_ids(repo: str, sha: str) -> list[str]:
    """Instance ids for a LIVE (pending/running) ci-watch box already working
    THIS exact (repo, sha) — deliberately NARROWER than groom's per-tier lock
    (config#1979): two different commits on the same repo failing CI
    independently must each get their own box. Fail-safe: any API error
    returns [] (never blocks a launch on a broken check — an optimization,
    not a correctness gate, mirroring every other pre-launch guard in the
    fleet)."""
    return spot_dispatch.running_instance_ids(
        CI_WATCH_TAG_NAME,
        {CI_WATCH_REPO_TAG_KEY: repo, CI_WATCH_SHA_TAG_KEY: sha},
        region=REGION,
    )


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a just-launched box whose post-launch steps
    failed. Without this the box orphans: it received no bootstrap, so
    neither the on-box watchdog nor the EXIT trap (both armed BY the
    bootstrap) is running to tear it down. Never masks the original error
    (logged, not raised) — mirrors groom's `_terminate_instance` exactly."""
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="ci-watch")


def _launch_ci_watch_spot(repo: str, sha: str, run_id: str, run_url: str,
                          workflow: str, branch: str) -> dict:
    """Launch + bootstrap the CI-watch box. SYNCHRONOUS contract: every
    anticipated failure mode returns a clean, well-formed launched:false
    rather than raising — see module docstring."""
    if not DISPATCH_ENABLED:
        logger.warning("CI_WATCH_DISPATCH_ENABLED=false — ci-watch spot NOT launched")
        return {"launched": False, "reason": "disabled"}

    existing = _running_ci_watch_instance_ids(repo, sha)
    if existing:
        logger.warning(
            "ci-watch box already live for %s@%s (%s) — skipping launch to avoid a "
            "concurrent duplicate run", repo, sha, existing)
        return {"launched": False, "reason": "concurrent_skip",
                "existing_instance_ids": existing}

    run_token = uuid.uuid4().hex
    try:
        instance_id, market = _launch_instance()
    except SpotLaunchError as exc:
        logger.error("ci-watch spot launch failed: %s: %s", type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched ci-watch box %s (%s) for %s@%s", instance_id, market, repo, sha)
    # config#1979-style tag so the NEXT trigger's guard check (above) can find
    # it. Best-effort — a tag-write failure must not abort an already-launched
    # box (mirrors groom's fail-safe posture on its own tier tag).
    try:
        boto3.client("ec2", region_name=REGION).create_tags(
            Resources=[instance_id],
            Tags=[
                {"Key": CI_WATCH_REPO_TAG_KEY, "Value": repo},
                {"Key": CI_WATCH_SHA_TAG_KEY, "Value": sha},
            ],
        )
    except Exception as exc:  # noqa: BLE001 — non-fatal, mirrors groom's tier-tag write
        logger.warning("ci-watch repo/sha tag write failed (non-fatal): %s: %s",
                       type(exc).__name__, exc)

    # Once the box is up, ANY failure before the bootstrap command is
    # delivered would orphan it (no watchdog/trap yet). Terminate-on-error —
    # but, unlike groom, return a clean result rather than re-raising (this
    # Lambda's synchronous caller needs a JSON verdict, not an invocation
    # error to unwrap).
    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(instance_id, repo, sha, run_id, run_url, workflow, branch, run_token)
    except Exception as exc:  # noqa: BLE001 — converted to a clean launched:false
        _terminate_instance(instance_id)
        logger.error("ci-watch post-launch step failed for %s: %s: %s",
                     instance_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "post_launch_failed",
                "instance_id": instance_id, "error": str(exc)}

    logger.info(
        "ci-watch dispatched: instance=%s market=%s command=%s repo=%s sha=%s run_id=%s "
        "run_token=%s", instance_id, market, command_id, repo, sha, run_id, run_token,
    )
    return {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "repo": repo,
        "sha": sha,
        "run_id": run_id,
        "run_token": run_token,
    }


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Synchronous handler invoked once per real CI failure event — NOT on a
    schedule. `event` carries {"repo", "sha", "run_id", "run_url", "workflow",
    "branch"} from the GHA job's `lambda invoke` payload (RequestResponse).

    Returns {"launched": bool, "reason": str, "instance_id": ..., ...} —
    read DIRECTLY by the GHA job as its success signal. Every anticipated
    failure (malformed event, concurrency skip, spot+on-demand launch
    exhaustion, post-launch SSM failure) is a clean return, never an
    exception — see module docstring's synchronous contract.
    """
    event = event or {}
    try:
        repo, sha, run_id, run_url, workflow, branch = _resolve_event_fields(event)
    except _InvalidEvent as exc:
        logger.error("invalid ci-watch event: %s", exc)
        return {"launched": False, "reason": "invalid_event", "error": str(exc)}

    logger.info(
        "ci-watch trigger: repo=%s sha=%s run_id=%s workflow=%s branch=%s",
        repo, sha, run_id, workflow, branch,
    )
    return _launch_ci_watch_spot(repo, sha, run_id, run_url, workflow, branch)
