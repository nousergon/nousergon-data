"""alpha-engine-spot-orphan-reaper — terminate orphan alpha-engine spot instances.

Backstop for the spot-side watchdog every spot launcher installs (`systemd-run
... shutdown -h now` after that workload's MAX_RUNTIME_SECONDS, combined with
`InstanceInitiatedShutdownBehavior=terminate`). This Lambda catches the residual
case where the watchdog itself never installed — dispatcher SSM cancelled before
reaching the `systemd-run` step, package manager interrupted bootstrap, etc.

DESIGN — one number, zero per-workload config (2026-07-01, config#1492).
Every alpha-engine spot box self-terminates via its own on-box watchdog; this
reaper is ONLY a backstop for the box whose watchdog failed to arm. A backstop
does not need per-workload precision — it needs a single invariant:

    no alpha-engine spot box should ever outlive the LONGEST watchdog in the
    fleet (plus a grace window).

So there is deliberately NO per-tag budget table. The previous table
(`TAG_BUDGETS`) had to be kept in lockstep with each launcher's MAX_RUNTIME_SECONDS
in a DIFFERENT repo; on 2026-07-01 the groom-on-spot migration (config#1432) added
`alpha-engine-groom-spot` (6h watchdog) without a table row, so the reaper's 2h
default killed a live groom mid-run at 2.5h (config#1492). A single global cap
above the longest watchdog cannot drift out of lockstep with anything:

  - Adding a new spot workload touches ONLY its own launcher. The reaper needs no
    change as long as the workload's watchdog <= MAX_SPOT_BUDGET_SECONDS.
  - The only time this constant moves is when a workload legitimately needs a
    LONGER watchdog than any today — a rare, deliberate act, and the failure mode
    if forgotten is LOUD (the box is reaped at the cap and logged), never a silent
    mis-kill at a wrong per-workload guess.

Cap sizing: MAX_SPOT_BUDGET_SECONDS defaults to 21600 (6h) — the longest fleet
watchdog (backlog groom, groom_spot_bootstrap.sh). GRACE_SECONDS (default 1800)
covers the gap between that watchdog firing and the reaper's hourly cadence, so
the effective reap threshold is 6.5h. Both are env-overridable.

Hourly EventBridge cron scans running `alpha-engine-*` spot instances and
terminates any older than the threshold. Emits CloudWatch custom metric
`AlphaEngine/Infra/spot_orphans_terminated` (sum) with a `name` dimension (the
box's Name tag) purely for observability — it does NOT feed the reap decision.

WATCH-KIND INCOMPLETE-REAP ALERT (additive, generalized config#2106): for a
small, explicit set of "watch" workloads (Fleet CI Watch, Fleet-SF Watch), a box
reaped by the fleet-wide age cap — rather than its own on-box completion path —
can mean the diagnose+fix agent never finished, leaving something unrepaired
with nobody told. `WATCH_KINDS` below is a table of these workloads (tag name,
S3 completion-marker prefix, and the discriminator tag keys the marker key is
built from); one shared check/notify path serves all of them instead of a
parallel `_ci_watch_*`/`_sf_watch_*` function pair per kind — the second entry
(`sf-watch`, finishing config#2001) was the trigger to generalize rather than
duplicate the first (`ci-watch`, config#2001/#2004) a second time. Before
terminating a `WATCH_KINDS`-tagged box, check for its sibling run script's S3
completion marker; if absent, fire one best-effort Telegram ping via
`krepis.telegram.send_message` (through the `nousergon_lib.telegram` re-export —
the same base primitive `flow_doctor_telegram.py` itself falls back to, reused
directly here since this Lambda sends exactly one alert shape per kind and
doesn't need the full forum-topic/dedup machinery). This check is purely
additive to every OTHER (non-`WATCH_KINDS`) spot workload's reap path, which is
untouched.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3
from nousergon_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")


@dataclass(frozen=True)
class WatchKind:
    """One "watch"-class spot workload whose completion is externally
    verifiable via an S3 marker before this reaper's age-cap terminates it."""

    tag_name: str
    completion_prefix: str
    discriminator_tag_keys: tuple[str, ...]
    label: str
    # The legacy per-kind key this Lambda's return dict uses for that kind's
    # incomplete-reap instance-id list — additive-only surface (see handler()).
    result_key: str


WATCH_KINDS: tuple[WatchKind, ...] = (
    WatchKind(
        tag_name="alpha-engine-ci-watch-spot",
        completion_prefix="ci_watch/_control/completed/",
        discriminator_tag_keys=("ci-watch-repo", "ci-watch-sha"),
        label="CI-watch",
        result_key="ci_watch_incomplete_reaps",
    ),
    WatchKind(
        tag_name="alpha-engine-sf-watch-spot",
        completion_prefix="sf_watch/_control/completed/",
        discriminator_tag_keys=("sf-watch-cadence", "sf-watch-pipeline", "sf-watch-run-date"),
        label="SF-watch",
        result_key="sf_watch_incomplete_reaps",
    ),
)
_WATCH_KIND_BY_TAG_NAME: dict[str, WatchKind] = {wk.tag_name: wk for wk in WATCH_KINDS}
_ALL_DISCRIMINATOR_TAG_KEYS: tuple[str, ...] = tuple(
    sorted({key for wk in WATCH_KINDS for key in wk.discriminator_tag_keys})
)

WATCH_COMPLETION_BUCKET = os.environ.get(
    # Renamed from the ci-watch-only CI_WATCH_COMPLETION_BUCKET now that this
    # bucket is shared across every WATCH_KINDS entry; the old env var name is
    # still honored as a fallback so an un-updated deploy.sh env doesn't
    # silently stop overriding the bucket.
    "WATCH_COMPLETION_BUCKET",
    os.environ.get("CI_WATCH_COMPLETION_BUCKET", "alpha-engine-research"),
)
# The longest on-box watchdog in the fleet. Keep >= the largest launcher
# MAX_RUNTIME_SECONDS (today: backlog groom = 21600s / 6h). This is the ONLY
# number that ties the reaper to the workloads, and it is a single ceiling, not a
# per-workload table — see the module docstring.
MAX_SPOT_BUDGET_SECONDS = int(os.environ.get("MAX_SPOT_BUDGET_SECONDS", "21600"))
# Grace between a watchdog firing and the reaper's hourly scan noticing.
GRACE_SECONDS = int(os.environ.get("GRACE_SECONDS", "1800"))
REAP_AFTER_SECONDS = MAX_SPOT_BUDGET_SECONDS + GRACE_SECONDS
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"


def _scan_spot_instances(ec2) -> list[dict]:
    """List running alpha-engine-tagged spot instances."""
    paginator = ec2.get_paginator("describe_instances")
    out: list[dict] = []
    for page in paginator.paginate(
        Filters=[
            {"Name": "instance-state-name", "Values": ["running"]},
            {"Name": "instance-lifecycle", "Values": ["spot"]},
            {"Name": "tag:Name", "Values": ["alpha-engine-*"]},
        ],
    ):
        for reservation in page.get("Reservations", []):
            for inst in reservation.get("Instances", []):
                tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
                out.append({
                    "instance_id": inst["InstanceId"],
                    "name": tags.get("Name", ""),
                    "launch_time": inst["LaunchTime"],
                    "instance_type": inst.get("InstanceType", ""),
                    # Only meaningful for a WATCH_KINDS-tagged box; empty
                    # (harmless no-op) for every other workload's instances.
                    "watch_tags": {k: tags.get(k, "") for k in _ALL_DISCRIMINATOR_TAG_KEYS},
                })
    return out


def _completion_key(kind: WatchKind, watch_tags: dict[str, str]) -> str:
    """Build the S3 completion-marker key for this kind from its
    discriminator tag values. Any '/' in a value is flattened to '-' (needed
    for ci-watch's repo value, e.g. "owner/name"; a harmless no-op for every
    other kind's values, which never contain '/') so the key never creates an
    unintended nested "directory"."""
    parts = [watch_tags.get(key, "").replace("/", "-") for key in kind.discriminator_tag_keys]
    return f"{kind.completion_prefix}{'-'.join(parts)}.json"


def _completion_marker_exists(s3, kind: WatchKind, watch_tags: dict[str, str]) -> bool:
    """True iff the sibling run script wrote its S3 completion marker for
    this instance's discriminator tags before the reaper's fleet-wide age cap
    fired.

    Fail-safe direction (deliberately the OPPOSITE of every other guard in
    this file): any inability to confirm completion — a genuine 404 (marker
    truly absent) OR an unrelated S3 error (throttle, auth hiccup) — is
    treated as "not confirmed complete", so the alert still fires. An
    occasional false-positive ping from a rare S3 hiccup is the safer
    failure direction than silently swallowing a genuine incomplete run
    (recording surface: the logger.warning below)."""
    if not all(watch_tags.get(key) for key in kind.discriminator_tag_keys):
        # Box was reaped before its discriminator tags ever landed — cannot
        # look up a marker either way. INVARIANT (config#2267 site 2): the
        # sf-watch/ci-watch dispatchers now TERMINATE a box whose
        # discriminator tag write fails after bounded retries, so a
        # long-lived tagless WATCH_KINDS box should no longer exist — hitting
        # this branch means either the narrow launch→tag race window (the
        # tags are still post-launch create_tags, not atomic RunInstances
        # TagSpecifications — that root fix is blocked on krepis.ec2_spot)
        # or a genuine anomaly worth the alert this False triggers.
        return False
    key = _completion_key(kind, watch_tags)
    try:
        s3.head_object(Bucket=WATCH_COMPLETION_BUCKET, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001 — see fail-safe-direction note above
        logger.warning(
            "%s completion marker not confirmed (key=%s): %s", kind.label, key, exc,
        )
        return False


def _notify_incomplete_reap(kind: WatchKind, instance_id: str, watch_tags: dict[str, str]) -> None:
    """Best-effort Telegram ping — a WATCH_KINDS box reaped without its
    completion marker can mean its agent never finished. Reuses
    ``krepis.telegram.send_message`` directly (via the ``nousergon_lib.
    telegram`` re-export) rather than the full ``flow_doctor_telegram`` forum-
    topic wrapper other Lambdas use — this Lambda sends exactly one alert
    shape per kind, so the dedup/topic-routing machinery is unneeded weight.
    ``send_message`` itself never raises (see its own docstring), but this is
    still wrapped defensively: the reap already completed by the time this
    runs, so nothing here may ever mask or retry that outcome (recording
    surface: the logger.warning below)."""
    key = _completion_key(kind, watch_tags)
    context = ", ".join(
        f"{tag_key}={watch_tags.get(tag_key) or 'unknown'}" for tag_key in kind.discriminator_tag_keys
    )
    text = (
        f"🟠 {kind.label} box reaped WITHOUT completing ({context}, instance={instance_id}) "
        f"— may still be unrepaired. No completion marker at "
        f"s3://{WATCH_COMPLETION_BUCKET}/{key} before the orphan-reaper's "
        "fleet-wide age cap fired."
    )
    try:
        send_message(text, disable_notification=False)
    except Exception as exc:  # noqa: BLE001 — secondary observability only
        logger.warning("%s incomplete-reap Telegram send failed (non-fatal): %s", kind.label, exc)


def _emit_metric(cw, name: str, count: int) -> None:
    """Emit one CloudWatch metric data point per terminated instance group."""
    if count == 0:
        return
    try:
        cw.put_metric_data(
            Namespace="AlphaEngine/Infra",
            MetricData=[{
                "MetricName": "spot_orphans_terminated",
                "Dimensions": [{"Name": "name", "Value": name}],
                "Value": float(count),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.warning("CloudWatch put_metric_data failed for %s: %s", name, exc)


def handler(event: dict, context) -> dict:
    """Hourly orphan scan + termination.

    Returns a summary dict for CloudWatch Logs grep + observability.
    """
    ec2 = boto3.client("ec2", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    now = datetime.now(timezone.utc)
    threshold = timedelta(seconds=REAP_AFTER_SECONDS)

    instances = _scan_spot_instances(ec2)
    logger.info(
        "Scanned %d running alpha-engine spot instances (reap threshold=%ds)",
        len(instances), REAP_AFTER_SECONDS,
    )

    orphans: list[dict] = []
    terminated: list[str] = []
    per_name_terminated: dict[str, int] = {}
    incomplete_reaps: dict[str, list[str]] = {wk.result_key: [] for wk in WATCH_KINDS}

    for inst in instances:
        age = now - inst["launch_time"]
        if age <= threshold:
            continue
        orphans.append({
            "instance_id": inst["instance_id"],
            "name": inst["name"],
            "age_seconds": int(age.total_seconds()),
            "reap_after_seconds": REAP_AFTER_SECONDS,
            "instance_type": inst["instance_type"],
        })
        if DRY_RUN:
            logger.warning(
                "DRY_RUN orphan %s (%s, age=%ds, reap_after=%ds): would terminate",
                inst["instance_id"], inst["name"], int(age.total_seconds()),
                REAP_AFTER_SECONDS,
            )
            continue
        try:
            ec2.terminate_instances(InstanceIds=[inst["instance_id"]])
            terminated.append(inst["instance_id"])
            per_name_terminated[inst["name"]] = per_name_terminated.get(inst["name"], 0) + 1
            logger.warning(
                "Terminated orphan %s (%s, age=%ds, reap_after=%ds, type=%s)",
                inst["instance_id"], inst["name"], int(age.total_seconds()),
                REAP_AFTER_SECONDS, inst["instance_type"],
            )
            # WATCH_KINDS migration (additive — every other tag's reap path
            # above is unchanged): a WATCH_KINDS box reaped by the fleet-wide
            # age cap, rather than its own on-box completion path, can mean
            # its agent never finished.
            kind = _WATCH_KIND_BY_TAG_NAME.get(inst["name"])
            if kind is not None:
                if not _completion_marker_exists(s3, kind, inst["watch_tags"]):
                    _notify_incomplete_reap(kind, inst["instance_id"], inst["watch_tags"])
                    incomplete_reaps[kind.result_key].append(inst["instance_id"])
        except Exception as exc:
            logger.error(
                "terminate_instances failed for %s: %s", inst["instance_id"], exc,
            )

    for name, count in per_name_terminated.items():
        _emit_metric(cw, name, count)

    return {
        "scanned": len(instances),
        "orphans_detected": len(orphans),
        "terminated": terminated,
        "dry_run": DRY_RUN,
        "reap_after_seconds": REAP_AFTER_SECONDS,
        "orphan_detail": orphans,
        **incomplete_reaps,
    }
