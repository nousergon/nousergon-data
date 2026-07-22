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
would starve the second commit's fix. A FAILED probe (SpotProbeError,
config#2267 site 1) does NOT fail-open silently: the dispatch proceeds —
coverage beats dedupe; a probe failure must never leave a real CI failure
uncovered — but the degradation is recorded loudly (`dedupe_degraded: true`
in the returned verdict + an ERROR log naming the probe error). This guard
is an optimization against duplicate spend, never a correctness gate.

SIGNATURE-REPEAT LAUNCH DEDUP (config#2862): the (repo, sha) lock above is
blind to a SHA-INDEPENDENT latent infra defect — one that fails EVERY new
commit's CI regardless of content, so per-SHA dedup can never collapse it
(2026-07-17: one missing IAM trust relationship, 11 red deploys, 11 spot
launches, even though every box independently diagnosed the SAME root cause
and no-op'd as a `REPEAT`). Before the (repo, sha) probe above, this
dispatcher additionally checks whether ANY signature marker under
`ci_watch/_control/signatures/{repo}/{today}/` (the S3 control-plane the
on-box agents themselves populate via
`alpha-engine-config/scripts/ci_watch_claim_attempt_signature.sh`, STEP 0.6
of `.github/ci-watch-prompt.md`) already carries a non-null `fix_pr` — see
`_known_fixed_signature_exists()` for the full rationale (including why the
"compute the signature before dispatch" layer config#2862 also named is
infeasible: notify-ci-failure.yml has zero per-repo diagnostic knowledge to
derive a matching signature with). A hit returns
`{"launched": false, "reason": "signature_repeat_skip"}`. Same
coverage-beats-dedup posture as the probe above: no markers, no fix_pr yet,
or ANY list/read failure all fail OPEN (launch proceeds).

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

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone

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

# Kill-switch: CI_WATCH_DISPATCH_ENABLED=false disables the launch without
# touching the GHA invoke wiring — mirrors every other fleet dispatcher's
# safety valve. Default ON.
DISPATCH_ENABLED = os.environ.get("CI_WATCH_DISPATCH_ENABLED", "true").lower() == "true"

# ── Spot launch config (env-overridable; defaults mirror scheduled-groom-
# dispatcher/data-spot-dispatcher — same default-VPC/AMI/security-group, only
# the IAM profile differs for blast-radius isolation). ────────────────────────
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
# Canary drill discriminator (config#2223): set on drill boxes ONLY. The tag
# KEY is deliberately the same one sf-watch-spot-dispatcher uses
# (sf-watch-drill) — one fleet-wide drill marker every consumer can filter
# on, not a per-dispatcher variant.
CI_WATCH_DRILL_TAG_KEY = "sf-watch-drill"

# ── Signature-repeat launch dedup (config#2862) ──────────────────────────────
# WHY: the (repo, sha) concurrency lock above is deliberately narrow — two
# DIFFERENT commits failing CI independently must each get their own box
# (see CONCURRENCY LOCK note). But a SHA-INDEPENDENT latent infra defect
# (e.g. a missing IAM trust relationship that fails a CFN deploy rollback on
# every single post-merge commit regardless of content) defeats that lock
# entirely: each new commit's SHA is novel, so N consecutive broken commits
# launch N boxes even though every one of them will independently diagnose
# the SAME root cause, compute the SAME signature via
# alpha-engine-config/scripts/ci_watch_signature_hash.sh (STEP 0.6 of
# .github/ci-watch-prompt.md), and no-op as a `REPEAT` (2026-07-17: one
# missing `scheduler.amazonaws.com` trust, 11 red deploys, 11 spot launches).
#
# WHY THIS IS THE FALLBACK, NOT THE "PREFERRED" LAYER: config#2862 named a
# preferred layer where nousergon-lib's notify-ci-failure.yml computes
# `signature_hash` BEFORE dispatch and passes it in the client_payload. That
# is infeasible as scoped: notify-ci-failure.yml is a generic reusable
# workflow shared by every fleet repo with zero domain knowledge of any
# repo's canaries/CFN stacks/failure taxonomy — its client_payload is just
# {repo, sha, run_id, run_url, workflow, branch}. Computing a signature that
# matches what the on-box agent would independently derive requires the
# agent's own diagnosis (gh run view --log-failed, canary/CFN/step
# classification, the canonical-mode registry) — work that only happens
# AFTER a box is already up. So this dispatcher instead consults the
# EXISTING signature control-plane THOSE agents already populate
# (ci_watch_claim_attempt_signature.sh, config#2395) for ANY already-fixed
# signature recorded for (repo, today) — no new pre-dispatch signal needed
# from nousergon-lib at all.
#
# MECHANISM: before launching, list
# s3://{CI_WATCH_SIGNATURES_BUCKET}/ci_watch/_control/signatures/{repo_flat}/{utc_date}/
# (written by every ci-watch box that reaches STEP 0.6 for this repo today)
# and read each marker. If ANY marker already carries a non-null `fix_pr`,
# skip the launch — a fix is already known for at least one of today's
# distinct root causes on this repo, so a fresh box is very likely just
# going to independently re-derive a REPEAT and no-op (a box that hits a
# genuinely NEW, still-unfixed signature will still find nothing with
# fix_pr set for ITS signature — but see the guardrail below).
#
# COVERAGE-BEATS-DEDUP GUARDRAIL (binding, config#2862): this is an
# optimization against duplicate spend, never a correctness gate — mirrors
# the existing `dedupe_degraded` posture (config#2267 site 1). A brand-new
# repo-wide signature landscape (no markers yet, or markers present but none
# carry a fix_pr) MUST launch. ANY probe/list/read failure (throttling,
# malformed marker JSON, access error) MUST fail OPEN (launch) — never
# silently treated as "known repeat." This dedup layer is coarser than a
# true per-signature match (it skips on ANY known-fixed signature for the
# repo today, not specifically the launching box's own eventual signature) —
# a deliberate, documented tradeoff: the alternative (compute the real
# signature pre-dispatch) is the infeasible "preferred" layer above, and a
# slightly coarser skip that still fails open on anything uncertain is far
# better than the N-boxes-for-one-defect status quo this issue tracks.
CI_WATCH_SIGNATURES_BUCKET = os.environ.get("CI_WATCH_SIGNATURES_BUCKET", "alpha-engine-research")
CI_WATCH_SIGNATURES_PREFIX = os.environ.get(
    "CI_WATCH_SIGNATURES_PREFIX", "ci_watch/_control/signatures"
)


def _known_fixed_signature_exists(repo: str) -> bool:
    """True iff at least one signature marker for (repo, today) already
    records a non-null `fix_pr` — i.e. a prior box today already diagnosed
    and fixed a recurring root cause on this repo. Any list/read failure (or
    simply no markers / no fix_pr yet) returns False — the caller launches;
    coverage beats dedup (see module-level guardrail note above)."""
    now = datetime.now(timezone.utc)
    repo_flat = repo.replace("/", "-")
    prefix = f"{CI_WATCH_SIGNATURES_PREFIX}/{repo_flat}/{now:%Y-%m-%d}/"
    try:
        s3 = boto3.client("s3", region_name=REGION)
        resp = s3.list_objects_v2(Bucket=CI_WATCH_SIGNATURES_BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []) or []:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            marker = json.loads(
                s3.get_object(Bucket=CI_WATCH_SIGNATURES_BUCKET, Key=key)["Body"].read()
            )
            if marker.get("fix_pr"):
                logger.info(
                    "ci-watch signature-repeat-skip candidate: %s already has "
                    "fix_pr=%s for repo=%s", key, marker.get("fix_pr"), repo,
                )
                return True
        return False
    except Exception as exc:  # noqa: BLE001 — fail OPEN, never block a launch
        logger.error(
            "ci-watch signature probe FAILED for repo=%s prefix=%s — failing "
            "OPEN (launching as normal; coverage beats dedup): %s: %s",
            repo, prefix, type(exc).__name__, exc,
        )
        return False


# ── Canary drill identity (config#2223) ──────────────────────────────────────
# DRILL-vs-REAL ISOLATION INVARIANT: a drill's (repo, sha) lock key is ALWAYS
# synthesized in code — never taken from the payload. DRILL_REPO is not a
# real fleet repository: the only real caller of this Lambda is sf-watch.yml's
# `ci-watch-dispatch` job relaying nousergon-lib's notify-ci-failure.yml
# repository_dispatch, which always carries the actual `owner/repo` of a live
# fleet repo — so the (repo, sha) concurrency lock and the completion-marker
# key `ci_watch/_control/completed/{repo-with-slash-flattened}-{sha}.json`
# (which therefore contains "drill-") can never collide with, dedupe-block,
# or reclaim-confuse a real dispatch. The sha is a deterministic per-UTC-day
# digest so a duplicate drill on the same day dedupes against itself only.
# Pinned by test_drill_identity_can_never_collide_with_a_real_dispatch.
DRILL_REPO = "nousergon/ci-watch-drill"
DRILL_RUN_URL = "https://drill.invalid/ci-watch-canary"
DRILL_WORKFLOW = "canary-drill"


def _drill_sha(now: datetime) -> str:
    """Deterministic per-UTC-day 40-hex drill sha (matches _SHA_RE)."""
    return hashlib.sha256(
        f"ci-watch-drill-{now:%Y-%m-%d}".encode("utf-8")
    ).hexdigest()[:40]

# Discriminator tags (config#2267 site 2, config#2292 root fix): the (repo,
# sha) tags are LOAD-BEARING — without them the next failure's dedupe guard
# is blind (duplicate box) and spot-orphan-reaper cannot derive the
# completion-marker key. They now ride the RunInstances TagSpecifications
# call ATOMICALLY (see _launch_instance's extra_tags) instead of a separate
# post-launch create_tags call — the box is never observably untagged, so
# the PR758 bounded-retry-then-terminate path this replaced is gone entirely
# (one mechanism, not two).

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


def _resolve_event_fields(event: dict) -> tuple[str, str, str, str, str, str, str]:
    """Validate the GHA payload's CI fields; raises _InvalidEvent on any
    missing/malformed field (caught once, at the handler, and converted to a
    clean launched:false — see module docstring's synchronous contract).

    is_drill (config#2223): "true" on the weekly synthetic canary drill the
    EventBridge Scheduler rule fires (see deploy.sh --bootstrap). A drill's
    ENTIRE identity is synthesized in code — repo/sha/run_id/run_url/
    workflow/branch from the payload are IGNORED — so no payload can carry a
    real (repo, sha) into a drill's lock/marker keys. See the DRILL_REPO
    isolation invariant above."""
    is_drill = _optional(event, "is_drill", _BOOL_RE, default="false")
    if is_drill == "true":
        return (DRILL_REPO, _drill_sha(datetime.now(timezone.utc)), "0",
                DRILL_RUN_URL, DRILL_WORKFLOW, "main", is_drill)
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
    return repo, sha, run_id, run_url, workflow, branch, is_drill


def _bootstrap_command(repo: str, sha: str, run_id: str, run_url: str,
                       workflow: str, branch: str, run_token: str,
                       is_drill: str = "false") -> str:
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
  --ci-run-url "{run_url}" --ci-workflow "{workflow}" --ci-branch "{branch}" \
  --is-drill "{is_drill}"
"""


def _launch_instance(repo: str, sha: str, is_drill: bool = False) -> tuple[str, str]:
    """Launch the CI-watch box; spot first, on-demand fallback on capacity
    exhaustion. Raises SpotLaunchError (or the SpotCapacityExhausted subclass)
    if BOTH the spot attempt and the on-demand fallback are exhausted/fail —
    caught once by the caller and converted to a clean launched:false.

    The load-bearing (repo, sha) discriminator tags (config#2267 site 2) ride
    the SAME RunInstances call as the launch itself via ``extra_tags``
    (config#2292 root fix, nousergon-lib >= 0.108.0 / krepis >= 0.12.0) — the
    box is never observably untagged, so there is no post-launch create_tags
    step to retry or fail."""
    extra_tags = {CI_WATCH_REPO_TAG_KEY: repo, CI_WATCH_SHA_TAG_KEY: sha}
    if is_drill:
        extra_tags[CI_WATCH_DRILL_TAG_KEY] = "true"
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=CI_WATCH_TAG_NAME,
        extra_tags=extra_tags,
        region=REGION,
    )


def _wait_ssm_online(instance_id: str) -> None:
    """Block until the instance is running AND its SSM agent registers Online."""
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(instance_id: str, repo: str, sha: str, run_id: str, run_url: str,
                    workflow: str, branch: str, run_token: str,
                    is_drill: str = "false") -> str:
    """Fire the async, detached SSM command that runs CI-watch + self-terminates."""
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(repo, sha, run_id, run_url, workflow, branch, run_token,
                           is_drill=is_drill),
        comment=f"ci-watch ({repo}@{sha[:12]}, run {run_id}, token {run_token[:12]})",
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=MAX_RUNTIME_SECONDS,
    )


def _running_ci_watch_instance_ids(repo: str, sha: str) -> list[str]:
    """Instance ids for a LIVE (pending/running) ci-watch box already working
    THIS exact (repo, sha) — deliberately NARROWER than groom's per-tier lock
    (config#1979): two different commits on the same repo failing CI
    independently must each get their own box. Raises SpotProbeError
    (nousergon-lib >= 0.106.0, config#2267 site 1) when the probe itself
    fails — the caller degrades to launch-with-dedupe_degraded, never a
    silent fail-open []."""
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
                          workflow: str, branch: str,
                          is_drill: str = "false") -> dict:
    """Launch + bootstrap the CI-watch box. SYNCHRONOUS contract: every
    anticipated failure mode returns a clean, well-formed launched:false
    rather than raising — see module docstring."""
    if not DISPATCH_ENABLED:
        logger.warning("CI_WATCH_DISPATCH_ENABLED=false — ci-watch spot NOT launched")
        return {"launched": False, "reason": "disabled"}

    # Signature-repeat launch dedup (config#2862). Skipped entirely for
    # drills — DRILL_REPO is a synthetic repo with no real signature
    # markers (so this would always no-op anyway), and the weekly canary's
    # whole purpose is to exercise the REAL launch pipe unconditionally.
    if is_drill != "true" and _known_fixed_signature_exists(repo):
        logger.warning(
            "ci-watch signature-repeat-skip for %s — a signature already "
            "recorded a fix_pr for a failure on this repo today; skipping "
            "launch (coverage beats dedup — see module docstring guardrail)",
            repo,
        )
        return {"launched": False, "reason": "signature_repeat_skip"}

    dedupe_degraded = False
    dedupe_probe_error = ""
    try:
        existing = _running_ci_watch_instance_ids(repo, sha)
    except SpotProbeError as exc:
        # Degraded-probe swallow (config#2267 site 1 POLICY): failure mode
        # swallowed = a possible duplicate box (the probe could not rule one
        # out); the primary deliverable — watch coverage of a REAL CI failure
        # — survives, and coverage beats dedupe: a probe failure must never
        # leave a real CI failure uncovered. Recording surfaces: this ERROR
        # log + `dedupe_degraded: true` in the returned verdict the GHA
        # caller archives.
        dedupe_degraded = True
        dedupe_probe_error = f"{type(exc).__name__}: {exc}"
        existing = []
        logger.error(
            "ci-watch concurrency probe FAILED for %s@%s — proceeding to "
            "launch with dedupe_degraded=true (coverage beats dedupe; a "
            "duplicate box is possible): %s",
            repo, sha, dedupe_probe_error,
        )
    if existing:
        logger.warning(
            "ci-watch box already live for %s@%s (%s) — skipping launch to avoid a "
            "concurrent duplicate run", repo, sha, existing)
        return {"launched": False, "reason": "concurrent_skip",
                "existing_instance_ids": existing}

    run_token = uuid.uuid4().hex
    try:
        instance_id, market = _launch_instance(repo, sha, is_drill=is_drill == "true")
    except SpotLaunchError as exc:
        logger.error("ci-watch spot launch failed: %s: %s", type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched ci-watch box %s (%s) for %s@%s%s", instance_id, market, repo, sha,
                " dedupe_degraded=true" if dedupe_degraded else "")
    # config#1979-style tags so the NEXT trigger's guard check (above) — and
    # the fleet spot-orphan-reaper's completion-marker lookup — can find the
    # box. LOAD-BEARING, not cosmetic (config#2267 site 2) — and, as of
    # config#2292, ATOMIC with launch: _launch_instance already passed them
    # as extra_tags into the RunInstances TagSpecifications, so the box is
    # never observably untagged. No post-launch create_tags step remains to
    # retry or fail here.

    # Once the box is up, ANY failure before the bootstrap command is
    # delivered would orphan it (no watchdog/trap yet). Terminate-on-error —
    # but, unlike groom, return a clean result rather than re-raising (this
    # Lambda's synchronous caller needs a JSON verdict, not an invocation
    # error to unwrap).
    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(instance_id, repo, sha, run_id, run_url,
                                     workflow, branch, run_token, is_drill=is_drill)
    except Exception as exc:  # noqa: BLE001 — converted to a clean launched:false
        _terminate_instance(instance_id)
        logger.error("ci-watch post-launch step failed for %s: %s: %s",
                     instance_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "post_launch_failed",
                "instance_id": instance_id, "error": str(exc),
                "dedupe_degraded": dedupe_degraded}

    logger.info(
        "ci-watch dispatched: instance=%s market=%s command=%s repo=%s sha=%s run_id=%s "
        "run_token=%s dedupe_degraded=%s", instance_id, market, command_id, repo, sha,
        run_id, run_token, dedupe_degraded,
    )
    verdict = {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "repo": repo,
        "sha": sha,
        "run_id": run_id,
        "run_token": run_token,
        "dedupe_degraded": dedupe_degraded,
        "is_drill": is_drill == "true",
    }
    if dedupe_degraded:
        verdict["dedupe_probe_error"] = dedupe_probe_error
    return verdict


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Synchronous handler invoked once per real CI failure event. `event`
    carries {"repo", "sha", "run_id", "run_url", "workflow", "branch"} from
    the GHA job's `lambda invoke` payload (RequestResponse).

    CANARY DRILL (config#2223): the ONE scheduled caller is the weekly
    EventBridge Scheduler rule (`alpha-engine-ci-watch-canary-drill-weekly`,
    created by deploy.sh --bootstrap) invoking with `{"is_drill": "true"}`.
    The dispatch pipe runs FOR REAL (spot launch, SSM, bootstrap start) but
    the box short-circuits before the agent (alpha-engine-config's
    ci_watch_run.sh drill guard), writes the
    `consolidated/ci_watch/_canary/{date}.json` heartbeat, and
    self-terminates. Drill isolation: see DRILL_REPO.

    Returns {"launched": bool, "reason": str, "instance_id": ..., ...} —
    read DIRECTLY by the GHA job as its success signal. Every anticipated
    failure (malformed event, concurrency skip, spot+on-demand launch
    exhaustion, post-launch SSM failure) is a clean return, never an
    exception — see module docstring's synchronous contract.
    """
    event = event or {}
    try:
        repo, sha, run_id, run_url, workflow, branch, is_drill = _resolve_event_fields(event)
    except _InvalidEvent as exc:
        logger.error("invalid ci-watch event: %s", exc)
        return {"launched": False, "reason": "invalid_event", "error": str(exc)}

    logger.info(
        "ci-watch trigger: repo=%s sha=%s run_id=%s workflow=%s branch=%s is_drill=%s",
        repo, sha, run_id, workflow, branch, is_drill,
    )
    return _launch_ci_watch_spot(repo, sha, run_id, run_url, workflow, branch,
                                 is_drill=is_drill)
