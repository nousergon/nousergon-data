"""alpha-engine-spot-orphan-reaper — terminate orphan alpha-engine spot instances.

Backstop for the spot-side watchdog every spot launcher installs (`systemd-run
... shutdown -h now` after that workload's MAX_RUNTIME_SECONDS, combined with
`InstanceInitiatedShutdownBehavior=terminate`). This Lambda catches the residual
case where the watchdog itself never installed — dispatcher SSM cancelled before
reaching the `systemd-run` step, package manager interrupted bootstrap, etc.

DESIGN — per-box watchdog-deadline tag, global cap fallback (2026-07-30,
config#5695).

Every launcher that knows its own watchdog budget stamps a ``watchdog-deadline``
tag (ISO8601 UTC) on the box at launch time, atomically with RunInstances via
the shared ``spot_dispatch.launch_with_fallback(extra_tags=...)`` chokepoint.
The reaper reads this tag on every running instance:

  - **Tag present + valid**: reap if ``now > watchdog-deadline + GRACE_SECONDS``
    (the per-box deadline, sized to the launcher's own known budget).
  - **Tag absent or malformed**: reap if ``age > MAX_SPOT_BUDGET_SECONDS +
    GRACE_SECONDS`` (the global cap fallback — unchanged behaviour for legacy
    workloads that predate this feature, or boxes whose watchdog never armed).

This replaces the previous single-global-cap design (config#1492, 2026-07-01),
which had no enforcement mechanism: the invariant "every workload's watchdog <=
MAX_SPOT_BUDGET_SECONDS" was documented-only and silently violated when the
weekly SF launcher's 13h watchdog exceeded the 6.5h reaper cap (config#5695).
The per-box tag is self-enforcing — each launcher declares its own deadline,
and the reaper honours it.

Cap sizing (fallback): MAX_SPOT_BUDGET_SECONDS defaults to 21600 (6h) — the
longest legacy fleet watchdog (backlog groom). GRACE_SECONDS (default 1800)
covers the gap between that watchdog firing and the reaper's hourly cadence.
Every new workload should emit ``watchdog-deadline`` rather than fitting under
the fallback cap.

Hourly EventBridge cron scans running ``alpha-engine-*`` spot instances and
terminates any older than its effective threshold. Emits CloudWatch custom
metric ``AlphaEngine/Infra/spot_orphans_terminated`` (sum) with a ``name``
dimension (the box's Name tag) — purely for observability, NOT feeding the reap
decision.

WATCH-KIND INCOMPLETE-REAP ALERT (additive, generalized config#2106): for a
small, explicit set of "watch" workloads (Fleet CI Watch, Fleet-SF Watch,
Alert-Drain, Think-Tank), a box reaped by the fleet-wide age cap — rather than
its own on-box completion path — can mean the diagnose+fix agent never
finished, leaving something unrepaired with nobody told. ``WATCH_KINDS`` below
is a table of these workloads (tag name, S3 completion-marker prefix, and the
discriminator tag keys the marker key is built from); one shared check/notify
path serves all of them instead of a parallel ``_ci_watch_*``/``_sf_watch_*``
function pair per kind. Before terminating a ``WATCH_KINDS``-tagged box, check
for its sibling run script's S3 completion marker; if absent, fire one best-
effort Telegram ping via ``krepis.telegram.send_message``. This check is purely
additive to every OTHER (non-``WATCH_KINDS``) spot workload's reap path, which
is untouched.
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
    # config#3173: alert-drain had ZERO incomplete-reap coverage — a drain box
    # whose on-box watchdog failed to arm silently ate the fleet-wide age cap
    # with nobody told (the same class of miss ci-watch/sf-watch already
    # close here). completion_prefix + the single alert-drain-run-id
    # discriminator tag match scripts/alert_drain_run.sh's completion_key()
    # exactly (`overseer/_control/completed/alert-drain-{run_id}.json`).
    WatchKind(
        tag_name="alpha-engine-alert-drain-spot",
        completion_prefix="overseer/_control/completed/alert-drain-",
        discriminator_tag_keys=("alert-drain-run-id",),
        label="Alert-drain",
        result_key="alert_drain_incomplete_reaps",
    ),
    # alpha-engine-config-I5752: Think Tank was the fourth box class and the
    # only one still missing a row here — the same miss ci-watch, sf-watch and
    # alert-drain each closed before it. Its box runs the daily challenger arm
    # (config-I5208 §47 cutover), and a box that overruns its 2.5h watchdog and
    # reaches this reaper's 6.5h age cap was terminated with nobody told.
    #
    # This row covers the HANG end specifically. The fast-fail end is covered
    # on the box itself: crucible-research#558 made
    # thinktank_spot_bootstrap.sh's on_exit publish to alpha-engine-alerts on a
    # non-zero rc, which is the window flow-doctor cannot see (a failure before
    # the Python run configures it). Neither is a substitute for the other.
    #
    # completion_prefix + the two discriminator tags match that script's
    # COMPLETION_KEY exactly:
    # thinktank/_control/completed/{trading_day}-{run_token}.json — and the
    # marker is written on the SUCCESS PATH ONLY (principles.md §2.7), so a
    # failed run leaves no marker and this row fires rather than reading green.
    WatchKind(
        tag_name="alpha-engine-thinktank-spot",
        completion_prefix="thinktank/_control/completed/",
        discriminator_tag_keys=("thinktank-trading-day", "thinktank-run-token"),
        label="Think-Tank",
        result_key="thinktank_incomplete_reaps",
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
                    # Optional per-box watchdog deadline (ISO8601 UTC, set
                    # atomically with RunInstances by the launcher via
                    # spot_dispatch.launch_with_fallback(extra_tags=...)).
                    # When present and parseable, the reap decision uses this
                    # deadline + GRACE_SECONDS instead of the global cap.
                    "watchdog_deadline": tags.get("watchdog-deadline", ""),
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
        # look up a marker either way. INVARIANT (config#2267 site 2, root
        # fix landed config#2292): the sf-watch/ci-watch dispatchers now pass
        # discriminator tags as ``extra_tags`` into the SAME RunInstances
        # TagSpecifications call that launches the box (krepis.ec2_spot.launch
        # >= 0.12.0, via nousergon-lib's spot_dispatch.launch_with_fallback)
        # — tagging is atomic with launch, not a separate post-launch
        # create_tags call, so there is no launch→tag race window left at
        # all. A box that reaches the reaper with any discriminator tag
        # missing is therefore a genuine anomaly (e.g. an operator-run
        # instance manually stripped of tags), not the old race — worth the
        # alert this False triggers.
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
        # ── Effective per-instance reap threshold ─────────────────────────────
        # If the launcher stamped a watchdog-deadline tag (ISO8601 UTC), use that
        # as the per-box deadline + GRACE. Otherwise fall back to the global cap
        # (MAX_SPOT_BUDGET_SECONDS + GRACE_SECONDS). A malformed tag value drops
        # to the global cap fallback (logged, not silently accepted or rejected).
        watchdog_deadline_str = inst.get("watchdog_deadline", "")
        if watchdog_deadline_str:
            try:
                deadline = datetime.fromisoformat(watchdog_deadline_str.replace("Z", "+00:00"))
                effective_threshold = timedelta(seconds=max(
                    0, (deadline - inst["launch_time"]).total_seconds() + GRACE_SECONDS
                ))
            except (ValueError, TypeError):
                logger.warning(
                    "Malformed watchdog-deadline tag on %s (%s): %r — falling back to global cap",
                    inst["instance_id"], inst["name"], watchdog_deadline_str,
                )
                effective_threshold = threshold
        else:
            effective_threshold = threshold

        if age <= effective_threshold:
            continue
        orphans.append({
            "instance_id": inst["instance_id"],
            "name": inst["name"],
            "age_seconds": int(age.total_seconds()),
            "reap_after_seconds": int(effective_threshold.total_seconds()),
            "instance_type": inst["instance_type"],
            "watchdog_deadline": watchdog_deadline_str or None,
        })
        if DRY_RUN:
            logger.warning(
                "DRY_RUN orphan %s (%s, age=%ds, reap_after=%ds): would terminate",
                inst["instance_id"], inst["name"], int(age.total_seconds()),
                int(effective_threshold.total_seconds()),
            )
            continue
        try:
            ec2.terminate_instances(InstanceIds=[inst["instance_id"]])
            terminated.append(inst["instance_id"])
            per_name_terminated[inst["name"]] = per_name_terminated.get(inst["name"], 0) + 1
            logger.warning(
                "Terminated orphan %s (%s, age=%ds, reap_after=%ds, type=%s)",
                inst["instance_id"], inst["name"], int(age.total_seconds()),
                int(effective_threshold.total_seconds()), inst["instance_type"],
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
