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
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
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
                })
    return out


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
    }
