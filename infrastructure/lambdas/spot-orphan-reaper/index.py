"""alpha-engine-spot-orphan-reaper — terminate orphan alpha-engine spot instances.

Backstop for the spot-side watchdog installed by the four spot launcher
scripts (data_weekly / drift_detection / train / backtest). The watchdog
fires `shutdown -h now` after MAX_RUNTIME_SECONDS, which combined with
`InstanceInitiatedShutdownBehavior=terminate` (also set by the launchers)
terminates the spot. This Lambda catches the residual case where the
watchdog itself never installed — dispatcher SSM cancelled before reaching
the `systemd-run` step, package manager interrupted bootstrap, etc.

Hourly EventBridge cron scans `alpha-engine-*` tagged spot instances. Any
instance whose `LaunchTime` exceeds the per-tag-prefix budget plus a 30
minute grace window is terminated. Emits CloudWatch custom metric
`AlphaEngine/Infra/spot_orphans_terminated` (sum) with `tag_prefix`
dimension for trend observation.

Budgets mirror MAX_RUNTIME_SECONDS in each launcher; grace covers the
gap between launcher budget and reaper firing cadence.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
GRACE_SECONDS = int(os.environ.get("GRACE_SECONDS", "1800"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

# Tag-prefix → MAX_RUNTIME_SECONDS. Must stay in lockstep with the launcher
# scripts' MAX_RUNTIME_SECONDS defaults; if a launcher's budget changes
# without updating this table the orphan reaper will terminate live workloads.
TAG_BUDGETS: dict[str, int] = {
    "alpha-engine-data-weekly-": 5400,    # spot_data_weekly.sh
    "alpha-engine-drift-": 1800,          # spot_drift_detection.sh
    "alpha-engine-gbm-train-": 5400,      # spot_train.sh
    "alpha-engine-backtest-": 7200,       # spot_backtest.sh
}
DEFAULT_BUDGET_SECONDS = 7200  # for unrecognised alpha-engine-* tags


def _budget_for_name(name: str) -> int:
    """Return the runtime budget for a spot tagged with this Name value."""
    for prefix, budget in TAG_BUDGETS.items():
        if name.startswith(prefix):
            return budget
    return DEFAULT_BUDGET_SECONDS


def _matched_prefix(name: str) -> str:
    """Return the matched tag prefix, or 'alpha-engine-other-' for the default branch."""
    for prefix in TAG_BUDGETS:
        if name.startswith(prefix):
            return prefix
    return "alpha-engine-other-"


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


def _emit_metric(cw, tag_prefix: str, count: int) -> None:
    """Emit one CloudWatch metric data point per terminated instance group."""
    if count == 0:
        return
    try:
        cw.put_metric_data(
            Namespace="AlphaEngine/Infra",
            MetricData=[{
                "MetricName": "spot_orphans_terminated",
                "Dimensions": [{"Name": "tag_prefix", "Value": tag_prefix}],
                "Value": float(count),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.warning("CloudWatch put_metric_data failed for %s: %s", tag_prefix, exc)


def handler(event: dict, context) -> dict:
    """Hourly orphan scan + termination.

    Returns a summary dict for CloudWatch Logs grep + observability.
    """
    ec2 = boto3.client("ec2", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)
    now = datetime.now(timezone.utc)

    instances = _scan_spot_instances(ec2)
    logger.info("Scanned %d running alpha-engine spot instances", len(instances))

    orphans: list[dict] = []
    terminated: list[str] = []
    per_prefix_terminated: dict[str, int] = {}

    for inst in instances:
        budget = _budget_for_name(inst["name"])
        threshold = timedelta(seconds=budget + GRACE_SECONDS)
        age = now - inst["launch_time"]
        if age <= threshold:
            continue
        orphans.append({
            "instance_id": inst["instance_id"],
            "name": inst["name"],
            "age_seconds": int(age.total_seconds()),
            "budget_seconds": budget,
            "grace_seconds": GRACE_SECONDS,
            "instance_type": inst["instance_type"],
        })
        prefix = _matched_prefix(inst["name"])
        if DRY_RUN:
            logger.warning(
                "DRY_RUN orphan %s (%s, age=%ds, budget=%ds): would terminate",
                inst["instance_id"], inst["name"], int(age.total_seconds()), budget,
            )
            continue
        try:
            ec2.terminate_instances(InstanceIds=[inst["instance_id"]])
            terminated.append(inst["instance_id"])
            per_prefix_terminated[prefix] = per_prefix_terminated.get(prefix, 0) + 1
            logger.warning(
                "Terminated orphan %s (%s, age=%ds, budget=%ds, type=%s)",
                inst["instance_id"], inst["name"], int(age.total_seconds()),
                budget, inst["instance_type"],
            )
        except Exception as exc:
            logger.error(
                "terminate_instances failed for %s: %s", inst["instance_id"], exc,
            )

    for prefix, count in per_prefix_terminated.items():
        _emit_metric(cw, prefix, count)

    return {
        "scanned": len(instances),
        "orphans_detected": len(orphans),
        "terminated": terminated,
        "dry_run": DRY_RUN,
        "orphan_detail": orphans,
    }
