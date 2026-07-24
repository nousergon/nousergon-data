"""alpha-engine-alert-drain-dispatcher — launch the Overseer alert-drain agent
on a dedicated EC2-spot box, twice daily (alpha-engine-config-I2824, epic
I2821 phase 3).

The drain gives every fleet alert an owner: it consumes the phase-1 intake
(SQS ``nousergon-overseer-intake`` + S3 fallback), classifies incidents into
the epic's T0-T3 authority tiers, acts within charter authority, and writes a
disposition ledger. THIS Lambda is only the launch leg — the policy lives in
alpha-engine-config's ``.github/alert-drain-prompt.md`` charter, executed by
``scripts/alert_drain_run.sh`` on the box.

DISPATCH PATH (phase-2 coherence): EventBridge Scheduler (2x daily,
off-market) -> alpha-engine-overseer-dispatcher router (playbook
``alert-drain``, registry-routed, kill-switched, ledgered, escalation-owned)
-> THIS executor (sync) -> spot box. The router's verdict contract applies:
every anticipated failure returns a clean ``{"launched": false, "reason":
...}``; ``concurrent_skip``/``disabled`` are the registry's benign declines.

NO PACE GATE — deliberate (deviation from I2824's original text): usage
pacing was DISMANTLED fleet-wide by Brian's 2026-07-14 ruling (see
scheduled-groom-dispatcher's header); the drain follows the standing ruling,
not the stale issue text. Cost control = the charter's own bounded caps
(500-message ingest, 150 agent turns, 3h watchdog) + the twice-daily cadence.

CONCURRENCY LOCK: single lane — tag ``Name=alpha-engine-alert-drain-spot``,
any live box skips the launch (two overlapping drains would double-process
queue messages; at-least-once tolerates it but a clean skip is free). A drill
shares the lane: drill-vs-real overlap resolves as a benign concurrent_skip.

IAM PROFILE: ``alpha-engine-alert-drain-executor-profile`` — DEDICATED
(per-playbook scoped IAM, epic invariant 5): intake-queue consume, overseer
S3 prefixes, read-only diagnosis, states:StartExecution on ONLY the three
pipelines. Never the trading executor's profile.

Managed OUTSIDE CloudFormation like its sibling dispatchers
(deploy.sh --bootstrap, operator-run).
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone

from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotLaunchError, SpotProbeError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: mirrors every fleet dispatcher's safety valve. Default ON.
DISPATCH_ENABLED = (
    os.environ.get("ALERT_DRAIN_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config (env-overridable; defaults mirror ci-watch-dispatcher —
# same default-VPC/AMI/security-group, only the IAM profile differs).
# IAM LOCKSTEP (config#2271): INSTANCE_TYPES/AMI_ID/SUBNETS/SECURITY_GROUP are
# ENUMERATED in this Lambda's scoped ec2:RunInstances policy (sibling
# iam-policy.json). Change them together or RunInstances 403s. ─────────────────
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "ALERT_DRAIN_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium"
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "ALERT_DRAIN_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("ALERT_DRAIN_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("ALERT_DRAIN_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("ALERT_DRAIN_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
IAM_PROFILE = os.environ.get(
    "ALERT_DRAIN_IAM_PROFILE", "alpha-engine-alert-drain-executor-profile"
)
VOLUME_SIZE_GB = int(os.environ.get("ALERT_DRAIN_VOLUME_SIZE_GB", "40"))

DRAIN_TAG_NAME = "alpha-engine-alert-drain-spot"
DRAIN_RUN_ID_TAG_KEY = "alert-drain-run-id"
# Fleet-wide drill marker — SAME tag key as sf/ci watch (config#2223): one
# discriminator every consumer can filter on.
DRAIN_DRILL_TAG_KEY = "sf-watch-drill"

DRAIN_GH_PAT_SSM = os.environ.get(
    "ALERT_DRAIN_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
DRAIN_CONFIG_REPO = os.environ.get("ALERT_DRAIN_CONFIG_REPO", "nousergon/alpha-engine-config")
DRAIN_CONFIG_BRANCH = os.environ.get("ALERT_DRAIN_CONFIG_BRANCH", "main")
# Matches the bootstrap's 3h watchdog (bounded triage sweep, not a build session).
MAX_RUNTIME_SECONDS = int(os.environ.get("ALERT_DRAIN_MAX_RUNTIME_SECONDS", "10800"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("ALERT_DRAIN_SSM_ONLINE_BUDGET_SEC", "180"))
CW_LOG_GROUP = os.environ.get("ALERT_DRAIN_CW_LOG_GROUP", "/alpha-engine/alert-drain-spot")

_BOOL_RE = re.compile(r"^(true|false)$")
_TRIGGER_RE = re.compile(r"^[a-z0-9_-]{0,64}$")
# config-I3293 — optional registry-declared model, injected by the overseer
# router from playbooks.yaml. Empty = absent = run-script inline default.
_MODEL_RE = re.compile(r"^(claude-[a-z0-9.-]{1,60})?$")


class _InvalidEvent(ValueError):
    """A required event field is missing or fails its allowlist."""


def _resolve_event_fields(event: dict) -> tuple[str, str, str, str]:
    """Validate + synthesize the run identity. A drill's run id ALWAYS carries
    the ``drill-`` prefix (drill-vs-real isolation on the completion-marker
    and ledger keys, config#2223 pattern) and a real run's never does."""
    is_drill = str(event.get("is_drill") or "false").strip()
    if not _BOOL_RE.match(is_drill):
        raise _InvalidEvent(f"malformed 'is_drill' in event: {is_drill!r}")
    trigger = str(event.get("trigger") or "").strip()
    if not _TRIGGER_RE.match(trigger):
        raise _InvalidEvent(f"malformed 'trigger' in event: {trigger!r}")
    model = str(event.get("model") or "").strip()
    if not _MODEL_RE.match(model):
        raise _InvalidEvent(f"malformed 'model' in event: {model!r}")
    now = datetime.now(timezone.utc)
    prefix = "drill-" if is_drill == "true" else "drain-"
    run_id = f"{prefix}{now:%Y-%m-%dT%H%MZ}"
    return run_id, is_drill, trigger, model


def _bootstrap_command(run_id: str, is_drill: str, model: str = "") -> str:
    """The async SSM RunShellScript body: fetch PAT, clone config, exec the
    alert_drain_spot_bootstrap.sh entrypoint. Prelude failure shuts the box
    down (mirrors the sibling dispatchers' prelude fail() trap exactly).

    ``model`` (config-I3293) is the registry-declared agent model injected by
    the overseer router; exported as DRAIN_MODEL for the bootstrap's runuser
    env passthrough (empty → alert_drain_run.sh's inline default applies —
    its ``:-`` expansion treats empty as unset). Regex-validated upstream
    (_MODEL_RE) so the f-string interpolation cannot inject shell."""
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
export DRAIN_MODEL="{model}"
# SSM RunShellScript runs as root with NO $HOME set; git config/clone need it.
export HOME=/root
fail() {{ echo "[alert-drain-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
dnf install -y -q git python3.12 python3.12-pip >/dev/null 2>&1 \
  || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {DRAIN_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {DRAIN_CONFIG_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{DRAIN_CONFIG_REPO}.git" \
  /home/ec2-user/alpha-engine-config || fail "clone failed"
cd /home/ec2-user/alpha-engine-config
exec bash infrastructure/alert_drain_spot_bootstrap.sh \
  --run-id "{run_id}" --is-drill "{is_drill}"
"""


def _running_drain_instance_ids() -> list[str]:
    """Any LIVE (pending/running) drain box — single-lane lock. Raises
    SpotProbeError on probe failure; the caller degrades to
    launch-with-dedupe_degraded (coverage beats dedupe, config#2267 site 1)."""
    return spot_dispatch.running_instance_ids(DRAIN_TAG_NAME, {}, region=REGION)


def _launch_instance(run_id: str, is_drill: str) -> tuple[str, str]:
    """Launch the drain box; spot first, on-demand fallback. Discriminator
    tags ride the RunInstances call atomically via extra_tags (config#2292)."""
    extra_tags = {DRAIN_RUN_ID_TAG_KEY: run_id}
    if is_drill == "true":
        extra_tags[DRAIN_DRILL_TAG_KEY] = "true"
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=DRAIN_TAG_NAME,
        region=REGION,
        extra_tags=extra_tags,
    )


def _wait_ssm_online(instance_id: str) -> None:
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(instance_id: str, run_id: str, is_drill: str,
                    model: str = "") -> str:
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(run_id, is_drill, model),
        comment=f"alert-drain ({run_id})",
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=MAX_RUNTIME_SECONDS,
    )


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a box whose post-launch steps failed (no
    watchdog armed yet — an orphan otherwise). Logged, never raised."""
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="alert-drain")


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Synchronous handler invoked by the overseer-dispatcher router (playbook
    ``alert-drain``) when the twice-daily Scheduler fires — or with
    ``{"is_drill": "true"}`` on a canary drill. Clean-JSON contract identical
    to the sibling executors: every anticipated failure returns
    ``{"launched": false, "reason": ...}``, never raises."""
    event = event or {}
    try:
        run_id, is_drill, trigger, model = _resolve_event_fields(event)
    except _InvalidEvent as exc:
        logger.error("invalid alert-drain event: %s", exc)
        return {"launched": False, "reason": "invalid_event", "error": str(exc)}

    if not DISPATCH_ENABLED:
        logger.warning("ALERT_DRAIN_DISPATCH_ENABLED=false — drain NOT launched")
        return {"launched": False, "reason": "disabled"}

    dedupe_degraded = False
    dedupe_probe_error = ""
    try:
        existing = _running_drain_instance_ids()
    except SpotProbeError as exc:
        # Degraded-probe swallow (config#2267 site 1 policy): failure mode
        # swallowed = a possible duplicate drain box; the primary deliverable
        # (alerts get drained) survives, and at-least-once queue discipline
        # absorbs double-processing. Recording surfaces: this ERROR log +
        # dedupe_degraded in the verdict the router ledgers.
        dedupe_degraded = True
        dedupe_probe_error = f"{type(exc).__name__}: {exc}"
        existing = []
        logger.error(
            "alert-drain concurrency probe FAILED — proceeding with "
            "dedupe_degraded=true: %s", dedupe_probe_error,
        )
    if existing:
        logger.warning(
            "alert-drain box already live (%s) — skipping launch (single-lane lock)",
            existing,
        )
        return {"launched": False, "reason": "concurrent_skip",
                "existing_instance_ids": existing}

    try:
        instance_id, market = _launch_instance(run_id, is_drill)
    except SpotLaunchError as exc:
        logger.error("alert-drain spot launch failed: %s: %s", type(exc).__name__, exc)
        return {"launched": False, "reason": "launch_failed", "error": str(exc)}

    logger.info("launched alert-drain box %s (%s) run_id=%s%s", instance_id, market,
                run_id, " dedupe_degraded=true" if dedupe_degraded else "")

    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(instance_id, run_id, is_drill, model)
    except Exception as exc:  # noqa: BLE001 — converted to a clean launched:false (router escalates)
        _terminate_instance(instance_id)
        logger.error("alert-drain post-launch step failed for %s: %s: %s",
                     instance_id, type(exc).__name__, exc)
        return {"launched": False, "reason": "post_launch_failed",
                "instance_id": instance_id, "error": str(exc),
                "dedupe_degraded": dedupe_degraded}

    verdict = {
        "launched": True,
        "reason": "launched",
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "run_id": run_id,
        "trigger": trigger,
        "is_drill": is_drill == "true",
        "dedupe_degraded": dedupe_degraded,
    }
    if dedupe_degraded:
        verdict["dedupe_probe_error"] = dedupe_probe_error
    logger.info("alert-drain dispatched: %s", verdict)
    return verdict
