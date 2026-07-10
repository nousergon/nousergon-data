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

CI-WATCH INCOMPLETE-REAP ALERT (additive, ci-watch-dispatcher migration): when
the terminated box is specifically tagged `Name=alpha-engine-ci-watch-spot`,
this reaper is no longer just a generic backstop — a CI-watch box reaped by
the fleet-wide age cap (rather than its own on-box completion path) can mean
the diagnose+fix agent never finished, leaving `main` red with nobody told.
Before terminating such a box, check for the sibling `ci_watch_run.sh`'s S3
completion marker (`s3://alpha-engine-research/ci_watch/_control/completed/
<repo>-<sha>.json`, written on every one of its exit paths); if absent, fire
one best-effort Telegram ping via `krepis.telegram.send_message` (through the
`nousergon_lib.telegram` re-export — the same base primitive
`flow_doctor_telegram.py` itself falls back to, reused directly here since
this Lambda sends exactly one alert shape and doesn't need the full forum-
topic/dedup machinery). This check is purely additive to every OTHER tagged
spot workload's reap path, which is untouched.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from nousergon_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
# ci-watch-dispatcher migration: the ONE tag value that gets the extra
# incomplete-reap check below. No other workload's reap path is affected.
CI_WATCH_TAG_NAME = "alpha-engine-ci-watch-spot"
CI_WATCH_COMPLETION_BUCKET = os.environ.get("CI_WATCH_COMPLETION_BUCKET", "alpha-engine-research")
CI_WATCH_COMPLETION_PREFIX = "ci_watch/_control/completed/"
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
                    # Only meaningful for CI_WATCH_TAG_NAME boxes; empty for
                    # every other workload's instances (harmless no-op there).
                    "ci_watch_repo": tags.get("ci-watch-repo", ""),
                    "ci_watch_sha": tags.get("ci-watch-sha", ""),
                })
    return out


def _ci_watch_completion_marker_exists(s3, repo: str, sha: str) -> bool:
    """True iff the sibling ``ci_watch_run.sh`` wrote its S3 completion marker
    for this (repo, sha) before the reaper's fleet-wide age cap fired.

    Fail-safe direction (deliberately the OPPOSITE of every other guard in
    this file): any inability to confirm completion — a genuine 404 (marker
    truly absent) OR an unrelated S3 error (throttle, auth hiccup) — is
    treated as "not confirmed complete", so the alert still fires. An
    occasional false-positive ping from a rare S3 hiccup is the safer
    failure direction than silently swallowing a genuine incomplete CI-watch
    run (recording surface: the logger.warning below)."""
    if not repo or not sha:
        # Box was reaped before its repo/sha tags ever landed (e.g. tag-write
        # failed right after launch) — cannot look up a marker either way.
        return False
    # repo is "owner/name" (a literal "/") — ci_watch_run.sh flattens that to
    # "-" before writing its marker key (so the S3 key has no unintended
    # nested "directory" per repo); mirror the SAME escaping here or every
    # lookup 404s against a key that was never written.
    key = f"{CI_WATCH_COMPLETION_PREFIX}{repo.replace('/', '-')}-{sha}.json"
    try:
        s3.head_object(Bucket=CI_WATCH_COMPLETION_BUCKET, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001 — see fail-safe-direction note above
        logger.warning(
            "ci-watch completion marker not confirmed for %s@%s (key=%s): %s",
            repo, sha, key, exc,
        )
        return False


def _notify_ci_watch_incomplete_reap(instance_id: str, repo: str, sha: str) -> None:
    """Best-effort Telegram ping — a CI-watch box reaped without its
    completion marker can mean the diagnose+fix agent never finished, leaving
    `main` red with nobody told. Reuses ``krepis.telegram.send_message``
    directly (via the ``nousergon_lib.telegram`` re-export) rather than the
    full ``flow_doctor_telegram`` forum-topic wrapper other Lambdas use — this
    Lambda sends exactly one alert shape, so the dedup/topic-routing
    machinery is unneeded weight. ``send_message`` itself never raises (see
    its own docstring), but this is still wrapped defensively: the reap
    already completed by the time this runs, so nothing here may ever mask
    or retry that outcome (recording surface: the logger.warning below)."""
    key = f"{CI_WATCH_COMPLETION_PREFIX}{repo}-{sha}.json"
    text = (
        "🟠 CI-watch box reaped WITHOUT completing "
        f"(repo={repo or 'unknown'}, sha={sha or 'unknown'}, instance={instance_id}) "
        f"— main may still be red. No completion marker at "
        f"s3://{CI_WATCH_COMPLETION_BUCKET}/{key} before the orphan-reaper's "
        "fleet-wide age cap fired."
    )
    try:
        send_message(text, disable_notification=False)
    except Exception as exc:  # noqa: BLE001 — secondary observability only
        logger.warning("ci-watch incomplete-reap Telegram send failed (non-fatal): %s", exc)


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
    ci_watch_incomplete_reaps: list[str] = []

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
            # ci-watch-dispatcher migration (additive — every other tag's reap
            # path above is unchanged): a ci-watch box reaped by the fleet-wide
            # age cap, rather than its own on-box completion path, can mean the
            # diagnose+fix agent never finished.
            if inst["name"] == CI_WATCH_TAG_NAME:
                repo, sha = inst["ci_watch_repo"], inst["ci_watch_sha"]
                if not _ci_watch_completion_marker_exists(s3, repo, sha):
                    _notify_ci_watch_incomplete_reap(inst["instance_id"], repo, sha)
                    ci_watch_incomplete_reaps.append(inst["instance_id"])
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
        "ci_watch_incomplete_reaps": ci_watch_incomplete_reaps,
    }
