"""alpha-engine-scheduled-groom-dispatcher — launch the backlog groom on a
dedicated EC2 spot box on an EventBridge-Scheduler-driven cadence.

WHY SPOT, NOT GITHUB ACTIONS (config#1432): the 2-3×/day FULL grooms run a
~hours-long Claude Code agent. Running them on GitHub-hosted runners in the
PRIVATE `alpha-engine-config` repo burned the org's 2,000 included Actions
minutes (100% used 2026-06-29; public repos are free/unlimited). This Lambda now
launches a capacity-resilient EC2 spot box (~$2/mo) that runs the SAME
`scripts/groom_run.sh` entrypoint the GHA workflow uses, then self-terminates.

Mechanism (mirrors the fleet gold-standard `spot_data_weekly.sh`, reusing both
fleet chokepoints — no lib change):
  1. `nousergon_lib.ec2_spot.launch()` rotates instance_type × subnet on capacity
     error; on SpotCapacityExhausted across all pools we relaunch ON-DEMAND
     (spot=False) so a capacity dip never starves a groom.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an ASYNC, detached `ssm send-command` (AWS-RunShellScript) carrying a
     small prelude: fetch the PAT from SSM, clone alpha-engine-config, then
     `exec infrastructure/groom_spot_bootstrap.sh`. The box self-terminates
     (InstanceInitiatedShutdownBehavior=terminate + a watchdog). The Lambda
     returns immediately — it does NOT babysit the multi-hour run.

The box reads its OWN run secrets (PAT, etc.) from SSM via its instance profile
(alpha-engine-executor-profile → alpha-engine-executor-role, which already has
ssm:GetParameter on /alpha-engine/*) — this Lambda needs none of those. It DOES
(2026-07-04) read the two Telegram secrets itself, scoped narrowly (see the
pre-boot pace gate note below), for the one notification only IT can send (a
pre-boot skip never boots a box, so there's no on-box groom_run.sh to ping).

Fail-loud (a scheduled groom IS the deliverable): a launch/SSM failure RAISES so
EventBridge retries + the Lambda error metric + a CloudWatch alarm surface the
miss, rather than silently dropping a pass.

**Pre-boot pace gate (Brian-ratified 2026-07-04).** Before launching the spot
box at all, this handler compares the reset-aligned weekly WET (interactive +
groom, same S3 source `alpha-engine-config/scripts/groom_budget.py` reads) to
how much of the current weekly reset window has already elapsed. If usage is
running ahead of a straight-line pace through the window (the same
``krepis.usage_pacing.pace_check`` gate `groom_budget.py` runs on-box), the
launch is skipped entirely — no spot cost at all, not even the ~2-5 min boot.
This REPLACES the box-side static 85%/95% floor/taper (config#1348) as the
short-circuit's first line; `groom_budget.py`'s on-box gate (same pace check,
different vantage point — sees this run's own live consumption too) remains
as the second line for the GHA path and as a belt for this one. Fail-safe:
ANY error reading S3/computing the gate → launch proceeds, never blocked by a
pace-gate infra hiccup (mirrors `groom_budget.py`'s own fail-safe posture).
A pace-gate skip returns ``launched: False`` — the dispatch Step Function's
existing `CheckLaunched` → `GroomSkipped` branch (already used for the
`GROOM_DISPATCH_ENABLED=false` kill-switch) handles it with no SF changes. A
skip also sends its own Telegram ping — best-effort via `krepis.telegram
.send_message` (never raises), scoped IAM to read just the two Telegram SSM
params (see iam-policy.json). Distinct from the on-box budget gate's own
Telegram ping (`groom_budget.py` skip, or `groom_driver.py`'s mid-run
recheck via `groom_run.sh`'s "WOUND DOWN" message) — a pre-boot skip never
reaches the box, so this Lambda is the ONLY place that can notify for it.

Managed OUTSIDE CloudFormation (same as before): operator-deployed via
`deploy.sh --bootstrap`. Merging the PR has ZERO live effect until the new code +
IAM are deployed AND the GHA `schedule:` crons are disabled (the gated cutover).
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from flow_doctor_telegram import notify_via_flow_doctor
from krepis.usage_pacing import pace_check, reset_window
import urllib.error
import urllib.request

from nousergon_lib import groom_eligibility as ge
from nousergon_lib import spot_dispatch
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
_FLOW_NAME = "scheduled-groom-dispatcher"
_DB_BASENAME = "flow_doctor_scheduled_groom_dispatcher"
_GROOM_LIFECYCLE_TOPICS = (FleetTelegramTopic.GROOM,)
# Kill-switch: GROOM_DISPATCH_ENABLED=false disables the trigger without deleting
# the EventBridge Scheduler rules. Default ON.
DISPATCH_ENABLED = os.environ.get("GROOM_DISPATCH_ENABLED", "true").lower() == "true"

# config#1933 demand-driven dispatch: enumerate actionable issues per tier
# BEFORE any spot spend and launch only when the slot's queue (own tier +
# starving lower tiers) clears the floor or an escape valve fires. Kill-switch
# independent of GROOM_DISPATCH_ENABLED — flipping this off restores the
# legacy unconditional slot launches without touching schedules.
DEMAND_GATE_ENABLED = os.environ.get("GROOM_DEMAND_GATE_ENABLED", "true").lower() == "true"
BACKLOG_REPOS = (
    "nousergon/alpha-engine-config", "nousergon/metron-ops",
    "nousergon/vires-ops", "nousergon/telos-ops",
)
_RESEARCH_BUCKET = "alpha-engine-research"

# ── Pre-boot pace gate (mirrors alpha-engine-config/scripts/groom_budget.py) ───
# Kill-switch independent of GROOM_DISPATCH_ENABLED — lets ops disable JUST the
# pace gate (e.g. during a known burst) without touching the dispatch trigger.
PACE_GATE_ENABLED = os.environ.get("GROOM_PACE_GATE_ENABLED", "true").lower() == "true"
# Calibrated 2026-07-08 — MUST track alpha-engine-config/scripts/groom_budget.py's
# WEEKLY_WET_CEILING; re-calibrate both together against /usage every few days.
WEEKLY_WET_CEILING = int(os.environ.get("GROOM_WEEKLY_WET_CEILING", "850000000"))
_PT = ZoneInfo("America/Los_Angeles")
# MUST match groom_budget.py's WEEKLY_RESET_ANCHOR/WEEKLY_PERIOD exactly — both
# derive the SAME reset-aligned window from one observed reset instant.
WEEKLY_RESET_ANCHOR = datetime(2026, 7, 12, 21, 0)   # PT, naive — Sunday 9pm PT
WEEKLY_PERIOD = timedelta(days=7)
CCUSAGE_BUCKET = os.environ.get("CCUSAGE_BUCKET", "alpha-engine-research")
CCUSAGE_PREFIX = "claude_code_usage/"
# Self-expiring operator override — MUST track groom_budget.py::OVERRIDE_UNTIL_PARAM.
OVERRIDE_UNTIL_PARAM = "/alpha-engine/groom/dynamic_budget_override_until"

# ── Spot launch config (env-overridable; defaults mirror spot_data_weekly.sh) ──
# t3/t3a/t2 .medium (4 GB) across all 6 default-VPC subnets; the lib CLI rotates
# on capacity error. Cheap-first type order biases pool selection toward price.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get("GROOM_INSTANCE_TYPES", "t3.medium,t3a.medium,t2.medium").split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "GROOM_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("GROOM_AMI_ID", "ami-0c421724a94bba6d6")  # Amazon Linux 2023 x86_64
KEY_NAME = os.environ.get("GROOM_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("GROOM_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
IAM_PROFILE = os.environ.get("GROOM_IAM_PROFILE", "alpha-engine-executor-profile")
VOLUME_SIZE_GB = int(os.environ.get("GROOM_VOLUME_SIZE_GB", "40"))  # node + claude-code + repo clones

GROOM_REPO = os.environ.get("GROOM_REPO", "nousergon/alpha-engine-config")
GROOM_BRANCH = os.environ.get("GROOM_BRANCH", "main")
# The BOX reads the PAT via its instance profile (this Lambda does not).
GROOM_GH_PAT_SSM = os.environ.get("GROOM_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat")
# Hard ceiling for the on-box SSM command (matches the bootstrap watchdog). The
# in-run soft budget (~340 min) is the binding stop; this is the backstop.
MAX_RUNTIME_SECONDS = int(os.environ.get("GROOM_MAX_RUNTIME_SECONDS", "21600"))
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("GROOM_SSM_ONLINE_BUDGET_SEC", "180"))
CW_LOG_GROUP = os.environ.get("GROOM_CW_LOG_GROUP", "/alpha-engine/groom-spot")

_VALID_RUN_MODES = {"full", "sweep"}
_DEFAULT_RUN_MODE = "full"
# config#1891: "gated-reverify" is the weekly Sunday stale-gate lane — missing
# from this set until 2026-07-07, so the Sunday schedule would have silently
# degraded to mid-only (caught by a manual dispatch before the first fire).
# config#1933: the filter set now comes from nousergon_lib.groom_eligibility —
# ONE source shared with the config groom driver (contract-tested there), so
# the PR683 silent-drift class (this set diverging from the driver's) is
# structurally closed.
_VALID_ISSUE_FILTERS = set(ge.VALID_ISSUE_FILTERS)
_DEFAULT_ISSUE_FILTER = "mid-only"
_DEFAULT_MODEL = "claude-sonnet-5"
# Defense-in-depth allowlist for the model id (embedded verbatim into the SSM
# shell command below) — model ids are Lambda-config-controlled, not raw user
# input, but this is cheap and rules out shell-metacharacter injection outright.
_MODEL_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def _parse_ccusage_key(key: str) -> "str | None":
    """Date string from either layout: {source}/{date}.json or {source}/{date}/{run}.json.

    Boto3 mirror of alpha-engine-config/scripts/groom_budget.py::_parse_key —
    duplicated (not imported) because that script is a different repo and this
    Lambda needs a boto3, not subprocess-`aws`-CLI, S3 client."""
    p = key[len(CCUSAGE_PREFIX):].split("/")
    if len(p) == 2 and p[1].endswith(".json"):
        return p[1][:-5]
    if len(p) == 3 and p[2].endswith(".json"):
        return p[1]
    return None


def _read_weekly_wet(window_start: datetime) -> float:
    """Sum WET (all sources) at/after the PT datetime ``window_start``, hour-precise.

    Boto3 mirror of groom_budget.py::read_weekly_wet (same S3 layout, same
    hour-boundary trim on the window's start day)."""
    s3 = boto3.client("s3", region_name=REGION)
    total = 0.0
    start_date = window_start.date().isoformat()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=CCUSAGE_BUCKET, Prefix=CCUSAGE_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            d = _parse_ccusage_key(key)
            if not d or d < start_date:
                continue
            body = s3.get_object(Bucket=CCUSAGE_BUCKET, Key=key)["Body"].read()
            doc = json.loads(body or b"{}")
            for hr, models in (doc.get("by_hour") or {}).items():
                if d == start_date and int(hr) < window_start.hour:
                    continue
                total += sum(r.get("wet", 0) for r in models.values())
    return total


def _parse_override_until(raw: str) -> "datetime | None":
    """PT-naive datetime from the SSM value; None if blank/unparseable."""
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(_PT).replace(tzinfo=None)
    return dt


def _read_override_until() -> "datetime | None":
    """Active override expiry from SSM; None when absent/unreadable."""
    try:
        ssm = boto3.client("ssm", region_name=REGION)
        raw = ssm.get_parameter(Name=OVERRIDE_UNTIL_PARAM)["Parameter"]["Value"]
    except Exception:  # noqa: BLE001 — absent override => normal policy
        return None
    return _parse_override_until(raw)


def _pace_gate_status() -> dict:
    """Compute the pre-boot pace-gate decision. Fail-safe: ANY error (S3 read,
    parse, credentials) returns ``exceeded: False`` with the error recorded —
    a pace-gate infra hiccup must never block a scheduled groom, mirroring
    groom_budget.py's own fail-safe posture."""
    try:
        now_pt = datetime.now(_PT).replace(tzinfo=None)
        override_until = _read_override_until()
        if override_until is not None and now_pt < override_until:
            return {
                "exceeded": False,
                "override_until": override_until.isoformat(),
                "reason": "operator override active — pace gate suspended",
            }
        window_start, _next_reset = reset_window(now_pt, WEEKLY_RESET_ANCHOR, WEEKLY_PERIOD)
        wet = _read_weekly_wet(window_start)
        used_frac = wet / WEEKLY_WET_CEILING if WEEKLY_WET_CEILING else 0.0
        status = pace_check(used_frac, now_pt, WEEKLY_RESET_ANCHOR, WEEKLY_PERIOD)
        return {
            "exceeded": status.exceeded,
            "used_frac": round(status.used_frac, 4),
            "elapsed_frac": round(status.elapsed_frac, 4),
            "overrun": round(status.overrun, 4),
            "wet": round(wet, 1),
        }
    except Exception as e:  # noqa: BLE001 — fail-safe: never block a scheduled groom
        logger.warning("pace gate check failed (non-fatal, launching anyway): %s: %s",
                       type(e).__name__, e)
        return {"exceeded": False, "error": f"{type(e).__name__}: {e}"}


def _resolve_run_mode(event: dict) -> str:
    """Pull run-mode from the EventBridge Scheduler input; unknown/missing → full."""
    rm = str(event.get("run_mode") or _DEFAULT_RUN_MODE).strip().lower()
    if rm not in _VALID_RUN_MODES:
        logger.warning("unknown run_mode %r — defaulting to %s", rm, _DEFAULT_RUN_MODE)
        rm = _DEFAULT_RUN_MODE
    return rm


def _resolve_issue_filter(event: dict) -> str:
    """Pull issue_filter from the schedule input; unknown/missing → mid-only (Sonnet queue)."""
    f = str(event.get("issue_filter") or _DEFAULT_ISSUE_FILTER).strip().lower()
    if f == "default":
        f = "mid-only"
    if f not in _VALID_ISSUE_FILTERS:
        logger.warning("unknown issue_filter %r — defaulting to %s", f, _DEFAULT_ISSUE_FILTER)
        f = _DEFAULT_ISSUE_FILTER
    return f


def _resolve_model(event: dict) -> str:
    """Pull model from the schedule input; missing/malformed → default (Sonnet 5)."""
    m = str(event.get("model") or _DEFAULT_MODEL).strip()
    if not _MODEL_RE.match(m):
        logger.warning("malformed model %r — defaulting to %s", m, _DEFAULT_MODEL)
        m = _DEFAULT_MODEL
    return m


def _resolve_force_on_demand(event: dict) -> bool:
    """config#1645: set by the dispatch Step Function's relaunch loop on the
    final bounded retry after repeated spot-interruption (mid-run box death,
    not a launch-time capacity error — see _launch_instance) — forces this
    attempt onto on-demand so a bad spot-capacity window still guarantees
    completion rather than retrying on the same flaky market indefinitely.
    Not set by any of the 3 live schedules' own EventBridge Scheduler input."""
    return bool(event.get("force_on_demand", False))


def _resolve_soft_limit_min(event: dict) -> int | None:
    """Optional bounded-test override — NOT set by any of the 3 live schedules
    (their SCHED_INPUTS carry no such key), only by a manual `aws lambda invoke`
    validating a change before/without waiting for the cron. Missing/invalid →
    None (groom_run.sh's own static/dynamic-budget default applies)."""
    raw = event.get("soft_limit_min")
    if raw is None:
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        logger.warning("malformed soft_limit_min %r — ignoring (default budget applies)", raw)
        return None
    if n <= 0:
        logger.warning("non-positive soft_limit_min %r — ignoring (default budget applies)", raw)
        return None
    return n


def _resolve_pr_budget(event: dict) -> int | None:
    """Optional per-schedule PR budget override (config#1769).

    Only the Opus high-only schedule sets this today; missing/invalid → None
    (groom_spot_bootstrap.sh's GROOM_PR_BUDGET default of 50 applies).
    """
    raw = event.get("pr_budget")
    if raw is None:
        raw = event.get("GROOM_PR_BUDGET")
    if raw is None:
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        logger.warning("malformed pr_budget %r — ignoring (default 50 applies)", raw)
        return None
    if n <= 0:
        logger.warning("non-positive pr_budget %r — ignoring (default 50 applies)", raw)
        return None
    return n


def _bootstrap_command(run_mode: str, run_url: str, model: str, issue_filter: str,
                       run_token: str, soft_limit_min: int | None = None,
                       pr_budget: int | None = None,
                       queue_manifest_key: str = "") -> str:
    """The async SSM RunShellScript body: fetch PAT, clone config, exec bootstrap.

    Runs as root on the box. The heavy, version-controlled logic lives in the
    repo's infrastructure/groom_spot_bootstrap.sh; this prelude is only the
    minimal clone glue (it needs the PAT before it can clone the private repo).
    Any prelude failure shuts the box down so a botched launch never idles.

    config#2201: the config#2129 GROOM_SWEEP_PARTITION_INDEX/COUNT exports are
    retired — groom boxes are pure issue-coverage workers now; PR merge-
    readiness sweeping moved to the single end-of-SF run_mode=sweep box the
    dispatch SF launches after every Map wind-down.
    """
    soft_limit_flag = f" --soft-limit-min {soft_limit_min}" if soft_limit_min else ""
    pr_budget_export = f"export GROOM_PR_BUDGET={pr_budget}\n" if pr_budget else ""
    # config#2152/#2147: manifest-consumption opt-in (drain runs / post-parity
    # cutover) — the driver builds its queue from this S3 key instead of
    # enumerating GitHub. Validated in handler() before it reaches this
    # root-shell command line.
    manifest_export = (f"export GROOM_QUEUE_MANIFEST_KEY={queue_manifest_key}\n"
                       if queue_manifest_key else "")
    return f"""set -uo pipefail
export AWS_DEFAULT_REGION={REGION}
# SSM RunShellScript runs as root with NO $HOME set; git config/clone need it.
export HOME=/root
fail() {{ echo "[groom-prelude] FATAL: $1"; shutdown -h now; exit 1; }}
# Stock AL2023 ships neither git nor python3.12. Install BEFORE the clone (git is
# needed now; python3.12 gives the groom the same interpreter as the GHA runner —
# mirrors spot_data_weekly.sh's bootstrap, which installs the same set).
dnf install -y -q git python3.12 python3.12-pip >/dev/null 2>&1 \
  || fail "runtime install (git/python3.12) failed"
PAT=$(aws ssm get-parameter --name {GROOM_GH_PAT_SSM} --with-decryption \
  --query Parameter.Value --output text --region {REGION} 2>/dev/null) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
git config --global --add safe.directory '*' || true
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {GROOM_BRANCH} \
  "https://x-access-token:${{PAT}}@github.com/{GROOM_REPO}.git" \
  /home/ec2-user/alpha-engine-config || fail "clone failed"
cd /home/ec2-user/alpha-engine-config
export GROOM_MODEL={model}
export GROOM_ISSUE_FILTER={issue_filter}
export GROOM_RUN_TOKEN={run_token}
{pr_budget_export}{manifest_export}exec bash infrastructure/groom_spot_bootstrap.sh --mode {run_mode} --run-url "{run_url}"{soft_limit_flag}
"""


def _launch_instance(force_on_demand: bool = False) -> tuple[str, str]:
    """Launch the groom box; spot first, on-demand fallback on capacity exhaustion
    OR when force_on_demand (config#1645: the dispatch SF's last bounded relaunch
    attempt after repeated mid-run spot interruption — skip straight to on-demand
    rather than trying the same flaky spot market a third time)."""
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name="alpha-engine-groom-spot",
        region=REGION,
        force_on_demand=force_on_demand,
    )


def _wait_ssm_online(instance_id: str) -> None:
    """Block until the instance is running AND its SSM agent registers Online."""
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(instance_id: str, run_mode: str, run_url: str, model: str, issue_filter: str,
                    run_token: str, soft_limit_min: int | None = None,
                    pr_budget: int | None = None, queue_manifest_key: str = "") -> str:
    """Fire the async, detached SSM command that runs the groom + self-terminates."""
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(
            run_mode, run_url, model, issue_filter, run_token, soft_limit_min, pr_budget,
            queue_manifest_key,
        ),
        comment=f"backlog groom ({run_mode}, {model}, {issue_filter}) — config#1432/#1495/#1645",
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        # Execution timeout (NOT the start timeout) — without this SSM kills the
        # command at the 3600s default, guillotining a multi-hour groom.
        execution_timeout_seconds=MAX_RUNTIME_SECONDS,
    )


GROOM_TIER_TAG_KEY = "groom-issue-filter"
# config#2201: the value stamped into GROOM_TIER_TAG_KEY for run_mode=sweep
# boxes. Sweep boxes are guarded per-KIND, not per-issue_filter — the launch
# event still carries a lib-valid issue_filter (inert for sweep mode, but the
# launch path validates it), and tagging that filter verbatim would make a
# live mid-only GROOM box block the end-of-SF sweep launch (and a live sweep
# box block the next mid-only groom) via the config#1979 concurrent guard.
# "sweep" is deliberately NOT a member of VALID_ISSUE_FILTERS — it exists in
# the EC2 tag namespace only, never in the filter-validation path.
_SWEEP_TIER_TAG = "sweep"


def _tier_tag(run_mode: str, issue_filter: str) -> str:
    """The concurrent-guard tag value for a launch (config#1979/#2201):
    groom boxes guard per issue_filter tier; sweep boxes guard as one
    distinct 'sweep' lane regardless of the (inert) issue_filter they carry."""
    return _SWEEP_TIER_TAG if run_mode == "sweep" else issue_filter


def _running_tier_instance_ids(tier_tag: str) -> list[str]:
    """Instance ids for a LIVE (pending/running) groom-spot box already working
    THIS ``tier_tag`` lane (config#1979; an ``issue_filter`` for groom boxes,
    the distinct ``sweep`` lane for run_mode=sweep boxes — config#2201). Two
    concurrent boxes on the same lane would race the identical GitHub queue
    (issue queue for grooms, open-PR set for sweeps) and double-spend WET on
    duplicate work — config#1969's adaptive re-queue now makes a full-coverage
    run legitimately take longer (a run can now genuinely work its ENTIRE
    queue rather than stopping early), raising the odds a prior trigger's box
    for a lane is still running when the next trigger re-evaluates the same
    lane. Fail-safe: any API error returns ``[]`` (never blocks a launch on a
    broken check — this guard is an optimization, not a correctness gate,
    mirroring every other pre-launch gate in this file)."""
    return spot_dispatch.running_instance_ids(
        "alpha-engine-groom-spot",
        {GROOM_TIER_TAG_KEY: tier_tag},
        region=REGION,
    )


def _notify_concurrent_skip(tier_tag: str, existing_ids: list[str], schedule_label: str) -> None:
    """Best-effort loud ping for a concurrent-same-lane skip — never raises."""
    text = (
        "⚪ Backlog groom slot SKIPPED — a box for this lane is already running "
        f"(config#1979). lane={tier_tag}, existing instance(s): "
        f"{', '.join(existing_ids)}. schedule={schedule_label}. Zero spot/WET "
        "spend; this slot's queue rides the next trigger once the running box "
        "finishes."
    )
    try:
        notify_via_flow_doctor(
            text, silent=True, severity="info",
            dedup_key=f"{_FLOW_NAME}:concurrent_tier_skip:{tier_tag}",
            flow_name=_FLOW_NAME, topics=_GROOM_LIFECYCLE_TOPICS,
            db_basename=_DB_BASENAME,
            context={"schedule": schedule_label, "tier_tag": tier_tag,
                     "existing_instance_ids": existing_ids},
            silent_topic=FleetTelegramTopic.GROOM,
        )
    except Exception as exc:  # noqa: BLE001 — secondary observability
        logger.warning("concurrent-lane skip Telegram failed (non-fatal): %s", exc)


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a just-launched box whose post-launch steps
    failed. Without this the box orphans: it received no bootstrap, so neither
    the in-script watchdog nor the EXIT trap (both armed BY the bootstrap) is
    running to tear it down — it idles until manually killed. Never masks the
    original error (logged, not raised)."""
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="groom")


def _launch_groom_spot(run_mode: str, schedule_label: str, model: str, issue_filter: str,
                       soft_limit_min: int | None = None, pr_budget: int | None = None,
                       force_on_demand: bool = False,
                       queue_manifest_key: str = "") -> dict:
    """Launch + bootstrap the groom box. Fail-loud — any error RAISES."""
    if not DISPATCH_ENABLED:
        logger.warning("GROOM_DISPATCH_ENABLED=false — groom spot NOT launched")
        return {"launched": False, "reason": "disabled"}

    # config#1979: skip if a box for THIS SAME lane is already live — a prior
    # trigger's run that's still working its queue (now more likely to run
    # long thanks to config#1969's adaptive re-queue) must not get a second,
    # concurrent box racing the identical GitHub queue. config#2201: sweep
    # boxes guard on the distinct 'sweep' lane (see _tier_tag) so the
    # end-of-SF sweep never collides with a live mid-only groom box.
    tier_tag = _tier_tag(run_mode, issue_filter)
    existing = _running_tier_instance_ids(tier_tag)
    if existing:
        logger.warning(
            "lane %s already has a live groom box (%s) — skipping launch to avoid "
            "a concurrent same-lane run", tier_tag, existing)
        _notify_concurrent_skip(tier_tag, existing, schedule_label)
        return {"launched": False, "reason": "concurrent_tier_skip",
                "issue_filter": issue_filter, "tier_tag": tier_tag,
                "existing_instance_ids": existing}

    # config#1645: a fresh token per launch ATTEMPT (not per schedule) — the
    # dispatch Step Function's relaunch loop calls this Lambda again with a new
    # execution, so each attempt gets its own S3 completion-marker key. A dead
    # box's stale marker (if any) can never be mistaken for THIS attempt's.
    run_token = uuid.uuid4().hex
    instance_id, market = _launch_instance(force_on_demand=force_on_demand)
    logger.info("launched groom box %s (%s)", instance_id, market)
    # config#1979: tag the box with its lane (tier, or 'sweep' — config#2201)
    # so the NEXT trigger's guard check (above) can find it. Best-effort — a
    # tag-write failure must not abort an already-launched box (mirrors the
    # fail-safe posture of the check itself).
    try:
        boto3.client("ec2", region_name=REGION).create_tags(
            Resources=[instance_id], Tags=[{"Key": GROOM_TIER_TAG_KEY, "Value": tier_tag}])
    except Exception as exc:  # noqa: BLE001 — non-fatal, mirrors _running_tier_instance_ids
        logger.warning("groom-issue-filter tag write failed (non-fatal): %s: %s",
                       type(exc).__name__, exc)
    # Once the box is up, ANY failure before the bootstrap command is delivered
    # would orphan it (no watchdog/trap yet). Terminate-on-error so a slow
    # SSM-online, an SSM SendCommand error, etc. tears the box down instead of
    # leaving it idling. (A hard Lambda-timeout KILL can't run this — that's why
    # the function timeout is also sized well above the launch+online budget.)
    try:
        _wait_ssm_online(instance_id)
        # IMPORTANT: this string is embedded in the prelude's bash double-quoted
        # `--run-url "..."`, so it MUST NOT contain `$` — the AWS console's normal
        # `$252F` log-group encoding would expand as positional params ($2, $5...)
        # under `set -u` and abort the prelude. Keep it `$`-free.
        run_url = (
            f"https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}"
            f"#logsV2:log-groups (log group {CW_LOG_GROUP}, instance {instance_id})"
        )
        command_id = _send_bootstrap(
            instance_id, run_mode, run_url, model, issue_filter, run_token, soft_limit_min, pr_budget,
            queue_manifest_key,
        )
    except Exception:
        _terminate_instance(instance_id)
        raise
    logger.info(
        "groom dispatched: instance=%s market=%s command=%s run_mode=%s model=%s issue_filter=%s "
        "schedule=%s run_token=%s",
        instance_id,
        market,
        command_id,
        run_mode,
        model,
        issue_filter,
        schedule_label,
        run_token,
    )
    return {
        "launched": True,
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "run_mode": run_mode,
        "model": model,
        "issue_filter": issue_filter,
        "tier_tag": tier_tag,
        "run_token": run_token,
        **({"pr_budget": pr_budget} if pr_budget is not None else {}),
    }


def _notify_pace_skip(pace: dict, schedule_label: str, run_mode: str) -> None:
    """Best-effort loud ping for a pre-boot pace skip — never raises."""
    text = (
        "🟡 Backlog groom SKIPPED — soft budget threshold passed before boot "
        f"(schedule={schedule_label}, run_mode={run_mode}). Weekly usage "
        f"{pace['used_frac']:.0%} > {pace['elapsed_frac']:.0%} elapsed "
        f"(overrun {pace['overrun']:+.0%}). Spot box was never launched — no "
        "cost incurred. Resumes automatically on the next scheduled run "
        "(or after the weekly reset)."
    )
    try:
        notify_via_flow_doctor(
            text,
            silent=True,
            severity="info",
            dedup_key=f"{_FLOW_NAME}:pace_skip:{schedule_label}",
            flow_name=_FLOW_NAME,
            topics=_GROOM_LIFECYCLE_TOPICS,
            db_basename=_DB_BASENAME,
            context={"schedule": schedule_label, "run_mode": run_mode, **pace},
            silent_topic=FleetTelegramTopic.GROOM,
        )
    except Exception as exc:  # noqa: BLE001 — secondary observability
        logger.warning("pace-gate skip Telegram failed (non-fatal): %s", exc)


def _github_token() -> str:
    """PAT for pre-boot enumeration, read from SSM (same param the box uses)."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=GROOM_GH_PAT_SSM, WithDecryption=True)
    return resp["Parameter"]["Value"].strip()


def _enumerate_tier_stats(token: str) -> tuple[dict, dict, bool]:
    """(counts per tier, oldest wait-hours per tier, has actionable P0).

    Wait = hours since the issue's last activity (updatedAt) — the operative
    "sitting untouched" signal for the ARCH §66 escape valve. Freshness-skip
    is NOT applied here (the on-box driver enumeration stays authoritative);
    the slight overcount only ever biases toward launching.
    """
    counts: dict[str, int] = {t: 0 for t in ge.TIERS}
    oldest: dict[str, float] = {t: 0.0 for t in ge.TIERS}
    has_p0 = False
    now = datetime.now(ZoneInfo("UTC"))
    for repo in BACKLOG_REPOS:
        page = 1
        while True:
            req = urllib.request.Request(
                f"https://api.github.com/repos/{repo}/issues"
                f"?state=open&per_page=100&page={page}",
                headers={"Authorization": f"token {token}",
                         "Accept": "application/vnd.github+json",
                         "User-Agent": "scheduled-groom-dispatcher"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                batch = json.loads(resp.read().decode())
            for it in batch:
                if "pull_request" in it:
                    continue
                labels = [lbl["name"] for lbl in it.get("labels", [])]
                tier = ge.is_actionable(labels)
                if tier is None:
                    continue
                counts[tier] += 1
                updated = datetime.fromisoformat(
                    str(it.get("updated_at", "")).replace("Z", "+00:00"))
                waited = max(0.0, (now - updated).total_seconds() / 3600.0)
                oldest[tier] = max(oldest[tier], waited)
                if "P0" in labels:
                    has_p0 = True
            if len(batch) < 100:
                break
            page += 1
    return counts, oldest, has_p0


def _write_decision_record(slot_tier: str, decision, counts: dict,
                           schedule_label: str) -> None:
    """groom/decisions/{date}/{slot}.json — a skipped slot must be
    distinguishable from a broken scheduler (no-silent-caps). Best-effort."""
    date = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")
    key = f"groom/decisions/{date}/{slot_tier}.json"
    body = json.dumps({
        "schema_version": 1, "slot_tier": slot_tier, "schedule": schedule_label,
        "counts": counts, **decision.as_record(),
        "decided_at": datetime.now(ZoneInfo("UTC")).isoformat(),
    })
    try:
        boto3.client("s3", region_name=REGION).put_object(
            Bucket=_RESEARCH_BUCKET, Key=key, Body=body.encode(),
            ContentType="application/json")
    except Exception as exc:  # noqa: BLE001 — observability, never blocks dispatch
        logger.warning("decision record write failed (non-fatal): %s", exc)


def _notify_demand_trigger_failed(exc: Exception, schedule_label: str) -> None:
    """Loud page for a failed trigger evaluation — never raises (config#2142).

    A demand-all trigger that cannot enumerate (GitHub or the S3 engagement
    scan down) is skipped fail-closed — meaning NO groom boxes launch for
    that slot. That must page ops-health, not sit in CloudWatch: the
    predecessor failure mode (engagement scan AccessDenied logged at WARNING
    only) ran undetected on 8 consecutive triggers, 2026-07-08 → 07-10.
    """
    text = (
        "🔴 Backlog groom trigger FAILED — demand-all enumeration errored, "
        f"trigger SKIPPED fail-closed (schedule={schedule_label}). NO groom "
        f"boxes launched for this slot. Error: {exc}. If this repeats on the "
        "next trigger, grooms are fully stalled — investigate the dispatcher "
        "Lambda's GitHub/S3 access (cf. config#2142)."
    )
    try:
        notify_via_flow_doctor(
            text, silent=False, severity="warning",
            dedup_key=f"{_FLOW_NAME}:demand_trigger_failed:{schedule_label}",
            flow_name=_FLOW_NAME, topics=_GROOM_LIFECYCLE_TOPICS,
            db_basename=_DB_BASENAME,
            context={"schedule": schedule_label, "error": str(exc)},
        )
    except Exception as notify_exc:  # noqa: BLE001 — secondary observability
        logger.warning("trigger-failed Telegram failed (non-fatal): %s", notify_exc)


def _notify_demand_skip(decision, counts: dict, schedule_label: str) -> None:
    """Best-effort ping for a demand-gate skip — never raises."""
    text = (
        "⚪ Backlog groom slot SKIPPED — light queue (config#1933). "
        f"schedule={schedule_label}: {decision.reason}. Counts {counts}. "
        "Zero spot/WET spend; issues defer upward or ride the next slot."
    )
    try:
        notify_via_flow_doctor(
            text, silent=True, severity="info",
            dedup_key=f"{_FLOW_NAME}:demand_skip:{schedule_label}",
            flow_name=_FLOW_NAME, topics=_GROOM_LIFECYCLE_TOPICS,
            db_basename=_DB_BASENAME,
            context={"schedule": schedule_label, **decision.as_record()},
            silent_topic=FleetTelegramTopic.GROOM,
        )
    except Exception as exc:  # noqa: BLE001 — secondary observability
        logger.warning("demand-skip Telegram failed (non-fatal): %s", exc)


def _demand_decision(issue_filter: str, schedule_label: str):
    """config#1933 enumerate-then-decide. Returns a SlotDecision, or None to
    proceed with the legacy unconditional launch (gate off / non-slot run /
    enumeration failure — the gate is an optimization, NEVER a correctness
    gate, so any error here fail-safes to launching). Counts are passed to
    the record writer via the decision closure below."""
    if not DEMAND_GATE_ENABLED:
        return None
    try:
        slot_tiers = ge.filter_tiers(issue_filter)
    except ValueError:
        return None
    if len(slot_tiers) != 1:
        return None  # gated-reverify / already-bundled manual invokes bypass
    try:
        counts, oldest, has_p0 = _enumerate_tier_stats(_github_token())
        decision = ge.decide_slot(slot_tiers[0], counts, oldest, has_p0)
        _write_decision_record(slot_tiers[0], decision, counts, schedule_label)
        return decision, counts
    except Exception as exc:  # noqa: BLE001 — fail-safe to legacy launch
        logger.warning("demand gate unavailable (%s) — legacy unconditional "
                       "launch", exc)
        return None


def _load_recent_engagements() -> dict:
    """(repo, number) -> engagement horizon epoch from the last
    ``ge.ENGAGEMENT_LOOKBACK_DAYS`` days' S3 run artifacts (same schema the
    driver's config#1893 fresh-skip reads).

    RAISES on any read failure (config#2142). This was previously fail-safe
    ``{}`` ("skip nothing — counting a few extra issues"), which masked a
    total AccessDenied on the ``groom/{date}/`` engagement scan for every
    trigger from ship (2026-07-08) to 2026-07-10: fresh-skip enumeration ran
    with an empty map on 8 consecutive triggers with zero pipeline signal,
    inflating every advertised per-tier count. The trigger handler's own
    catch is the recording surface: it skips the trigger (fail-closed, no
    launch on over-counted queues) AND pages ops-health.

    config#2038: the lookback (was a hardcoded ``range(3)``) and the engaged-
    disposition set (was a hardcoded tuple literal) had silently drifted from
    groom_driver.py's own constants (4-day lookback, same 4 dispositions) —
    this module already imports ``nousergon_lib.groom_eligibility`` as the
    declared single source of truth for exactly this class of drift; the two
    values just weren't pulled from it yet. Read them from ``ge`` so they
    can't re-drift.
    """
    out: dict = {}
    s3 = boto3.client("s3", region_name=REGION)
    now = datetime.now(ZoneInfo("UTC"))
    for d in range(ge.ENGAGEMENT_LOOKBACK_DAYS):
        date = (now - timedelta(days=d)).strftime("%Y-%m-%d")
        resp = s3.list_objects_v2(Bucket=_RESEARCH_BUCKET, Prefix=f"groom/{date}/")
        for obj in resp.get("Contents", []) or []:
            if not obj["Key"].endswith(".json"):
                continue
            art = json.loads(s3.get_object(Bucket=_RESEARCH_BUCKET, Key=obj["Key"])["Body"].read())
            run_start = art.get("run_start", "")
            if not run_start:
                continue
            horizon = (datetime.fromisoformat(run_start.replace("Z", "+00:00")).timestamp()
                       + int(art.get("elapsed_min", 0)) * 60 + ge.FRESH_SKIP_SLACK_SEC)
            for rec in art.get("issues", []):
                if rec.get("disposition") in ge.ENGAGED_DISPOSITIONS:
                    k = (rec.get("repo", ""), rec.get("number"))
                    out[k] = max(out.get(k, 0.0), horizon)
    return out


def _enumerate_tier_stats_fresh(token: str) -> tuple[dict, dict, list, dict]:
    """Tier stats with config#1893 fresh-skip applied (P0 exempt).

    Returns (counts, oldest_wait_hours, p0_tiers, tier_issues) —
    the first three for ge.decide_trigger(); ``tier_issues`` maps tier →
    the actual issue dicts behind each count (repo/number/title/labels/
    updated_at), consumed by ``_write_queue_manifests`` (config#2152: the
    enumerate-once queue manifest — counts and queue derive from the SAME
    walk by construction, so they can never diverge)."""
    engagements = _load_recent_engagements()
    counts: dict[str, int] = {t: 0 for t in ge.TIERS}
    oldest: dict[str, float] = {t: 0.0 for t in ge.TIERS}
    tier_issues: dict[str, list[dict]] = {t: [] for t in ge.TIERS}
    p0_tiers: set = set()
    now = datetime.now(ZoneInfo("UTC"))
    now_epoch = now.timestamp()
    for repo in BACKLOG_REPOS:
        page = 1
        while True:
            req = urllib.request.Request(
                f"https://api.github.com/repos/{repo}/issues"
                f"?state=open&per_page=100&page={page}",
                headers={"Authorization": f"token {token}",
                         "Accept": "application/vnd.github+json",
                         "User-Agent": "scheduled-groom-dispatcher"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                batch = json.loads(resp.read().decode())
            for it in batch:
                if "pull_request" in it:
                    continue
                labels = [lbl["name"] for lbl in it.get("labels", [])]
                tier = ge.is_actionable(labels)
                if tier is None:
                    continue
                is_p0 = "P0" in labels
                updated_epoch = datetime.fromisoformat(
                    str(it.get("updated_at", "")).replace("Z", "+00:00")).timestamp()
                engaged = engagements.get((repo, it["number"]))
                if engaged and not is_p0 and ge.fresh_skip_active(engaged, updated_epoch, now_epoch):
                    continue
                counts[tier] += 1
                tier_issues[tier].append({
                    "repo": repo, "number": it["number"],
                    "title": it.get("title", ""), "labels": labels,
                    "updated_at": str(it.get("updated_at", "")),
                })
                oldest[tier] = max(oldest[tier], (now_epoch - updated_epoch) / 3600.0)
                if is_p0:
                    p0_tiers.add(tier)
            if len(batch) < 100:
                break
            page += 1
    return counts, oldest, sorted(p0_tiers), tier_issues


def _write_queue_manifests(schedule_label: str, launches: list,
                           tier_issues: dict) -> dict:
    """config#2152 (queue manifest, OBSERVER phase): one manifest per launched
    box at a deterministic key — ``groom/queues/{date}/trigger-{HHMM}-{issue_
    filter}.json`` — carrying the exact issue list behind the launch decision.
    The on-box driver discovers its manifest by (date, issue_filter, freshness)
    and records a parity comparison in its run artifact; it does NOT consume
    the manifest yet. Cutover (driver enumeration replaced by manifest +
    revalidation) is gated on ≥3 days of clean parity — see I2152/I2154.

    Best-effort during the observer phase ONLY: a write failure here must not
    block the launches (the grooms are the primary deliverable), and the
    failure IS recorded — the driver's parity check reports the missing
    manifest in its run artifact (``manifest_parity``), which the I2152
    cutover review reads. At cutover this becomes fail-loud like the trigger
    record itself.

    Returns {issue_filter: s3_key} for the trigger record / response payload.
    """
    now = datetime.now(ZoneInfo("UTC"))
    keys: dict = {}
    for d in launches:
        if not d.launch:
            continue
        issues = [it for t in d.tiers for it in tier_issues.get(t, [])]
        key = f"groom/queues/{now:%Y-%m-%d}/trigger-{now:%H%M}-{d.issue_filter}.json"
        body = json.dumps({
            "schema_version": 1, "schedule": schedule_label,
            "issue_filter": d.issue_filter, "model": d.model,
            "tiers": list(d.tiers), "decided_at": now.isoformat(),
            "issue_count": len(issues), "issues": issues,
        })
        try:
            boto3.client("s3", region_name=REGION).put_object(
                Bucket=_RESEARCH_BUCKET, Key=key, Body=body.encode(),
                ContentType="application/json")
            keys[d.issue_filter] = key
        except Exception as exc:  # noqa: BLE001 — observer phase; recording surface = driver manifest_parity (see docstring)
            logger.warning("queue manifest write failed for %s (observer phase — "
                           "driver parity will report it): %s", d.issue_filter, exc)
    return keys


def _write_trigger_record(schedule_label: str, launches: list, counts: dict) -> None:
    """One record per trigger evaluation — groom/decisions/{date}/{HHMM}.json."""
    now = datetime.now(ZoneInfo("UTC"))
    key = f"groom/decisions/{now:%Y-%m-%d}/trigger-{now:%H%M}.json"
    body = json.dumps({
        "schema_version": 2, "trigger": "demand-all", "schedule": schedule_label,
        "counts": counts, "decisions": [d.as_record() for d in launches],
        "decided_at": now.isoformat(),
    })
    try:
        boto3.client("s3", region_name=REGION).put_object(
            Bucket=_RESEARCH_BUCKET, Key=key, Body=body.encode(),
            ContentType="application/json")
    except Exception as exc:  # noqa: BLE001
        logger.warning("trigger record write failed (non-fatal): %s", exc)


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge Scheduler handler — launches the groom spot box on cadence.

    `event` is the schedule's JSON input, e.g. {"run_mode": "full", "model":
    "claude-opus-4-8", "issue_filter": "high-only", "schedule": "0 1 * * *"}.
    `model`/`issue_filter` default to the Sonnet mid-tier queue when absent
    (the two pre-existing Sonnet schedules don't set them). `soft_limit_min` is
    a manual-invoke-ONLY bounded-test override — no live schedule sets it.
    `pr_budget` is set only on the Opus high-only schedule (config#1769).
    `force_on_demand` (config#1645) is set only by the dispatch Step Function's
    own relaunch loop on its final bounded retry — no live schedule sets it.
    `queue_manifest_key` (config#2152/#2175) marks an explicit operator queue
    (drain runs): it bypasses the demand-count gates — which count GitHub
    enumeration, meaningless for a manifest — but launches SPOT-FIRST and
    still honors the pre-boot pace gate (weekly WET protection covers drains).

    Pre-boot pace gate (2026-07-04): if weekly Claude usage is running ahead
    of a linear pace through the current reset window, the launch is skipped
    entirely — before any spot cost is incurred — rather than deferring the
    decision to the on-box `groom_budget.py` gate. See module docstring. A
    skip here sends its own Telegram ping (best-effort — `krepis.telegram
    .send_message` never raises) since this is the ONLY place a pre-boot skip
    is ever visible; a run that never boots has no on-box `groom_run.sh` to
    fire its own notification the way the on-box budget-gate skip does.

    config#2129 two-phase SF flow (`decide_only` / `launch_decided`): the
    dispatch Step Function no longer invokes this Lambda once per trigger and
    tries to poll a response shape that varies 1-vs-N launches (the OLD
    single-invocation demand-all path returned ``{"launches": [...]}`` with NO
    top-level ``launched``/``instance_id``/``command_id`` — the SF's
    CheckLaunched/PollGroomCommand states expected the singular shape and
    failed with States.Runtime on ~83% of real triggers, see I2129). Instead:
    ``event.get("decide_only")`` computes 0..N launch decisions WITHOUT
    launching anything (used by the SF's DecideLaunches state); the SF then
    fans out one ``launch_decided`` invocation per decision via a Map state,
    each an independently pollable/relaunchable branch reusing the existing
    per-box states unchanged. Legacy direct invokes (no `decide_only` /
    `launch_decided` key) behave EXACTLY as before — decide AND launch in one
    call, same response shapes — for any caller not yet on the new SF flow.
    """
    event = event or {}
    run_mode = _resolve_run_mode(event)
    model = _resolve_model(event)
    issue_filter = _resolve_issue_filter(event)
    soft_limit_min = _resolve_soft_limit_min(event)
    pr_budget = _resolve_pr_budget(event)
    force_on_demand = _resolve_force_on_demand(event)
    schedule_label = str(event.get("schedule") or "unknown")
    logger.info(
        "scheduled groom trigger: run_mode=%s model=%s issue_filter=%s soft_limit_min=%s "
        "pr_budget=%s force_on_demand=%s schedule=%s decide_only=%s launch_decided=%s",
        run_mode, model, issue_filter, soft_limit_min, pr_budget, force_on_demand, schedule_label,
        bool(event.get("decide_only")), bool(event.get("launch_decided")),
    )

    # config#2152/#2147: manifest-consumption opt-in (drain runs / post-parity
    # cutover). FAIL LOUD on a malformed key — this string is embedded in the
    # box's root-shell bootstrap command line, so the character set is strict.
    #
    # config#2175 (gate/market split): a manifest run BYPASSES the demand-count
    # gates below — the demand gate counts GitHub enumeration, meaningless for
    # an explicit operator-built queue — but launches SPOT-FIRST like every
    # other run (the lib's on-demand capacity fallback still applies).
    # Previously drain runs had to set force_on_demand:true just to skip the
    # gate, paying for an on-demand box as a side effect of `force_on_demand`
    # conflating "skip demand gate" with "force on-demand market";
    # force_on_demand keeps BOTH meanings for its one remaining caller (the
    # dispatch SF's final bounded relaunch retry). The pre-boot PACE gate
    # deliberately still applies to manifest runs — weekly WET protection
    # covers drains too.
    queue_manifest_key = str(event.get("queue_manifest_key") or "")
    if queue_manifest_key and not re.fullmatch(r"[A-Za-z0-9._/-]{1,512}", queue_manifest_key):
        raise ValueError(f"invalid queue_manifest_key: {queue_manifest_key!r}")

    if event.get("launch_decided"):
        # config#2129: a decide_only call (or the SF's bounded-relaunch loop
        # re-invoking for the SAME already-decided box) already made this
        # decision — launch EXACTLY what's given, no pace gate (a per-TRIGGER
        # call, not per-box), no re-decision. Returns the SAME singular
        # {"groom": {...}} shape the SF's existing per-box states expect.
        # config#2201: the SF's end-of-SF DispatchEndOfSfSweep state also
        # lands here (run_mode=sweep + launch_decided) — the sweep box is
        # unconditional by design, so bypassing the pace/demand gates is
        # exactly right; the config#1979 concurrent guard (on the distinct
        # 'sweep' lane tag) is the only pre-launch check that applies.
        result = _launch_groom_spot(
            run_mode, schedule_label, model, issue_filter, soft_limit_min, pr_budget,
            force_on_demand,
            queue_manifest_key=queue_manifest_key,
        )
        return {"groom": result}

    if PACE_GATE_ENABLED:
        pace = _pace_gate_status()
        if pace.get("exceeded"):
            logger.warning(
                "pace gate SKIP — used_frac=%.4f > elapsed_frac=%.4f (overrun=%+.4f, "
                "wet=%.0f) — groom spot NOT launched, resumes next schedule/reset",
                pace["used_frac"], pace["elapsed_frac"], pace["overrun"], pace["wet"],
            )
            _notify_pace_skip(pace, schedule_label, run_mode)
            skip = {"launched": False, "reason": "pace_gate_skip", **pace}
            if event.get("decide_only"):
                return {"decide": {"launches": [], **skip}}
            return {"groom": skip}

    if (run_mode == "full" and not force_on_demand and not queue_manifest_key
            and str(event.get("trigger", "")) == "demand-all"):
        # config#1933 SYMMETRIC triggers (Brian's ratified correction): every
        # scheduled trigger evaluates the FULL backlog and launches 0..3 boxes
        # — one per tier clearing the floor, thin tiers attached upward, the
        # leftover-thin pool valve-gated. config#2201: PR sweeping is no
        # longer a per-box concern at all — the dispatch SF launches ONE
        # end-of-SF run_mode=sweep box after all these groom boxes wind down.
        try:
            counts, oldest, p0_tiers, tier_issues = _enumerate_tier_stats_fresh(_github_token())
            launches = ge.decide_trigger(counts, oldest, p0_tiers)
        except Exception as exc:  # noqa: BLE001 — fail-closed: skip this trigger rather than launching with stale (no-fresh-skip) legacy counts; recorded via ops-health page (config#2142)
            logger.warning("demand trigger unavailable (%s) — skipping trigger (legacy fallthrough retired, fresh-skip-less enumeration over-counts the queue)", exc)
            _notify_demand_trigger_failed(exc, schedule_label)
            err = {"launched": False, "reason": "demand_all_failed", "error": str(exc)}
            if event.get("decide_only"):
                return {"decide": {"launches": [], **err}}
            return {"groom": err}
        if launches is not None:
            manifest_keys = _write_queue_manifests(schedule_label, launches, tier_issues)
            _write_trigger_record(schedule_label, launches, counts)
            entries = []
            for d in launches:
                if not d.launch:
                    logger.info("demand trigger: %s", d.reason)
                    _notify_demand_skip(d, counts, schedule_label)
                    continue
                logger.info("demand trigger LAUNCH — %s (filter=%s model=%s)",
                            d.reason, d.issue_filter, d.model)
                # config#2201/#2205: the end-of-SF sweep box replaced per-box
                # partitioned sweeps, so this dispatcher consumes only
                # d.model/d.issue_filter. The now-unused SlotDecision
                # partition_index/partition_count fields were dropped in
                # nousergon-lib v0.103.0 (pinned above).
                entry = {"model": d.model, "issue_filter": d.issue_filter}
                if pr_budget is not None and "high" in d.tiers:
                    entry["pr_budget"] = pr_budget
                entries.append(entry)
            decisions_record = [d.as_record() for d in launches]
            if event.get("decide_only"):
                # config#2152: manifests are written at DECIDE time (above) —
                # the enumerate-once product of the same walk as the counts —
                # so the two-phase SF path records them here too.
                return {"decide": {"trigger": "demand-all", "counts": counts,
                                   "decisions": decisions_record,
                                   "queue_manifests": manifest_keys,
                                   "launches": entries}}
            results = [
                _launch_groom_spot(
                    run_mode, schedule_label, e["model"], e["issue_filter"], soft_limit_min,
                    e.get("pr_budget"), force_on_demand,
                )
                for e in entries
            ]
            return {"groom": {"trigger": "demand-all", "counts": counts,
                              "decisions": decisions_record,
                              "queue_manifests": manifest_keys,
                              "launches": results}}

    if run_mode == "full" and not force_on_demand and not queue_manifest_key:
        decided = _demand_decision(issue_filter, schedule_label)
        if decided is not None:
            decision, counts = decided
            if not decision.launch:
                logger.info("demand gate SKIP — %s", decision.reason)
                _notify_demand_skip(decision, counts, schedule_label)
                skip = {"launched": False, "reason": "demand_gate_skip",
                        "decision": decision.as_record(), "counts": counts}
                if event.get("decide_only"):
                    return {"decide": {"launches": [], **skip}}
                return {"groom": skip}
            # Launch with the DECIDED bundle + cheapest adequate model — the
            # schedule's model is only the slot default; high never runs
            # below Opus, and a bundle without high never pays for it.
            issue_filter = decision.issue_filter
            model = decision.model
            logger.info("demand gate LAUNCH — %s (filter=%s model=%s)",
                        decision.reason, issue_filter, model)

    if event.get("decide_only"):
        entry = {"model": model, "issue_filter": issue_filter}
        if pr_budget is not None:
            entry["pr_budget"] = pr_budget
        return {"decide": {"launches": [entry]}}

    result = _launch_groom_spot(
        run_mode, schedule_label, model, issue_filter, soft_limit_min, pr_budget, force_on_demand,
        queue_manifest_key=queue_manifest_key,
    )
    return {"groom": result}
