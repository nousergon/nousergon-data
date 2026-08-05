"""alpha-engine-ssm-reachability-probe — is the SSM control plane reachable?

alpha-engine-config-I6198.

WHY THIS EXISTS. SSM is the single transport by which every unattended workload
in this fleet receives its work: the backlog groom and sweep, sf-watch,
ci-watch, alert-drain, think-tank, data-spot and the weekly Step Functions
pipeline all dispatch by launching a spot box and sending it an SSM command. It
had no health check of any kind.

On 2026-08-03T01:50:11Z an interface VPC endpoint for
``com.amazonaws.us-east-1.ssm`` was created in vpc-566f002e with private DNS
enabled, attached to a security group allowing inbound 443 only from the
Cloudflare prefix lists. Private DNS overrides ``ssm.us-east-1.amazonaws.com``
for EVERY instance in the VPC, so the whole fleet began resolving the SSM
control plane to an ENI that dropped its packets. Measured consequences:

  - The always-on dashboard box sat at ``PingStatus=ConnectionLost`` for 2h31m.
    Nothing alarmed, nothing paged, no surface showed it.
  - Eleven spot boxes failed to register and were terminated by their
    dispatchers. The only signal that reached a human was three lane-DEATH
    pages naming the wrong cause (alpha-engine-config-I6199), and the outage
    was diagnosed only because someone asked why they kept arriving.

Per principles.md §2.7: a component emitting nothing is not healthy, it is
unobserved — and *no data* is never rendered as green. This probe is what makes
the fleet's most load-bearing dependency emit.

DESIGN — start from DescribeInstances, not DescribeInstanceInformation.

``ssm:DescribeInstanceInformation`` lists only instances that have EVER
registered with SSM. A box that never registers — precisely the 2026-08-03
failure mode — is invisible to it. The comparison therefore starts from
``ec2:DescribeInstances`` (ground truth for what SHOULD be reachable) and
subtracts what SSM reports as ``Online``.

GRACE. A freshly launched box legitimately takes tens of seconds to register;
the groom dispatcher's own budget is 180s. Instances younger than
``GRACE_SECONDS`` (default 300) are excluded so normal boot is not reported as
an outage.

EMITTING ZERO IS THE POINT. ``ssm_unreachable_instances`` is published as an
explicit ``0`` when everything is reachable. Without that datapoint, "healthy"
and "the probe is dead" are the same shape on the metric — the failure this
exists to prevent. ``ssm_probe_heartbeat`` is published on every invocation for
the same reason one layer up: it makes the detector itself observable, so a
dead probe alarms instead of reading as a quiet fleet.

FAIL LOUD. Every AWS call here is allowed to raise. A probe that swallows its
own errors and returns a clean result is worse than no probe: it would publish
"0 unreachable" from a failed scan. The Lambda erroring is a visible, alarmable
state; a false green is not.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

#: Seconds an instance may be running before its SSM registration is expected.
#: The groom dispatcher waits 180s for `Online` before giving up, so anything at
#: or below that would report normal boot as an outage.
GRACE_SECONDS = int(os.environ.get("GRACE_SECONDS", "300"))

#: Only instances whose Name tag carries this prefix are in scope — the fleet's
#: own boxes. An unrelated instance in the account must not page this fleet.
NAME_PREFIX = os.environ.get("NAME_PREFIX", "alpha-engine-")

METRIC_NAMESPACE = "AlphaEngine/Infra"
UNREACHABLE_METRIC = "ssm_unreachable_instances"
HEARTBEAT_METRIC = "ssm_probe_heartbeat"

#: PutMetricData rejects a call carrying more than 1000 MetricDatum.
_PUT_METRIC_DATA_MAX = 1000


def _name_tag(instance: dict) -> str:
    for tag in instance.get("Tags", []):
        if tag.get("Key") == "Name":
            return tag.get("Value", "")
    return ""


def _expected_instances(ec2, now: datetime) -> dict[str, str]:
    """Running fleet instances old enough to have registered: id -> Name tag."""
    cutoff = now - timedelta(seconds=GRACE_SECONDS)
    expected: dict[str, str] = {}
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(
        Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
    ):
        for reservation in page.get("Reservations", []):
            for inst in reservation.get("Instances", []):
                name = _name_tag(inst)
                if not name.startswith(NAME_PREFIX):
                    continue
                if inst["LaunchTime"] > cutoff:
                    # Still inside its legitimate registration window.
                    continue
                expected[inst["InstanceId"]] = name
    return expected


def _online_instance_ids(ssm) -> set[str]:
    online: set[str] = set()
    paginator = ssm.get_paginator("describe_instance_information")
    for page in paginator.paginate():
        for info in page.get("InstanceInformationList", []):
            if info.get("PingStatus") == "Online":
                online.add(info["InstanceId"])
    return online


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    now = datetime.now(timezone.utc)
    ec2 = boto3.client("ec2", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    expected = _expected_instances(ec2, now)
    online = _online_instance_ids(ssm)
    unreachable = {iid: name for iid, name in expected.items() if iid not in online}

    # The aggregate datapoint (what the alarm evaluates) is published FIRST and
    # unconditionally — including as 0. An absent datapoint must mean the probe
    # is dead, never that the fleet is healthy. The per-box datapoints that
    # follow tell an operator WHICH box without opening the logs, and are
    # truncated rather than allowed to displace the aggregate.
    metric_data: list[dict] = [
        {
            "MetricName": UNREACHABLE_METRIC,
            "Value": float(len(unreachable)),
            "Unit": "Count",
            "Timestamp": now,
        },
        {
            "MetricName": HEARTBEAT_METRIC,
            "Value": 1.0,
            "Unit": "Count",
            "Timestamp": now,
        },
    ]
    for iid, name in sorted(unreachable.items()):
        metric_data.append({
            "MetricName": UNREACHABLE_METRIC,
            "Dimensions": [{"Name": "name", "Value": name}],
            "Value": 1.0,
            "Unit": "Count",
            "Timestamp": now,
        })
        logger.error(
            "SSM UNREACHABLE: %s (%s) is running but not Online in SSM after %ss",
            iid, name, GRACE_SECONDS,
        )

    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE, MetricData=metric_data[:_PUT_METRIC_DATA_MAX]
    )

    if unreachable:
        logger.error(
            "%d of %d fleet instances unreachable via SSM", len(unreachable), len(expected)
        )
    else:
        logger.info("all %d fleet instances reachable via SSM", len(expected))

    return {
        "checked_at": now.isoformat(),
        "expected": len(expected),
        "online": len(expected) - len(unreachable),
        "unreachable": len(unreachable),
        "unreachable_instances": sorted(unreachable),
        "grace_seconds": GRACE_SECONDS,
    }
