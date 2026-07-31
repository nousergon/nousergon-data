"""Tests for the spot-interruption recorder (alpha-engine-config-I5197).

The fixtures are shaped from REAL CloudTrail events captured on 2026-07-28
rather than invented, because the field this whole Lambda depends on —
`serviceEventDetails.instanceIdSet` — is not where you would guess it is
(`requestParameters` and `responseElements` are both null on a `BidEvictedEvent`).
A hand-written fixture would have encoded the guess.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))

# boto3 is import-time in the module; stub before import so tests need no creds.
if "boto3" not in sys.modules:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *a, **k: MagicMock())

import index  # noqa: E402

NOW = datetime(2026, 7, 28, 23, 0, 0, tzinfo=timezone.utc)

# Verbatim shape of a real BidEvictedEvent (2026-07-28T22:52:16Z).
REAL_EVICTION = {
    "eventVersion": "1.11",
    "eventTime": "2026-07-28T22:52:16Z",
    "eventSource": "ec2.amazonaws.com",
    "eventName": "BidEvictedEvent",
    "awsRegion": "us-east-1",
    "requestParameters": None,
    "responseElements": None,
    "eventID": "4ca413e4-0bfc-4cba-b65c-75617bbf08d5",
    "eventType": "AwsServiceEvent",
    "serviceEventDetails": {"instanceIdSet": ["i-07d70f8f1ca48020c"]},
}

INSTANCE = {
    "InstanceId": "i-07d70f8f1ca48020c",
    "InstanceType": "t4g.medium",
    "Placement": {"AvailabilityZone": "us-east-1c"},
    "InstanceLifecycle": "spot",
    "LaunchTime": datetime(2026, 7, 28, 22, 29, 39, tzinfo=timezone.utc),
    "Tags": [
        {"Key": "Name", "Value": "alpha-engine-groom-spot"},
        {"Key": "groom-issue-filter", "Value": "high-only"},
    ],
}


@pytest.fixture
def aws(monkeypatch):
    """Wire all three clients; S3 defaults to 'nothing recorded yet'."""
    s3, ec2, ct = MagicMock(), MagicMock(), MagicMock()
    s3.head_object.side_effect = Exception("404")
    ec2.describe_instances.return_value = {"Reservations": [{"Instances": [INSTANCE]}]}
    ct.lookup_events.return_value = {
        "Events": [{"CloudTrailEvent": json.dumps(REAL_EVICTION)}]
    }
    monkeypatch.setattr(index, "_s3", lambda: s3)
    monkeypatch.setattr(index, "_ec2", lambda: ec2)
    monkeypatch.setattr(index, "_cloudtrail", lambda: ct)
    monkeypatch.setattr(index, "_now", lambda: NOW)
    return types.SimpleNamespace(s3=s3, ec2=ec2, ct=ct)


def _puts(aws, prefix):
    """Every object written under a prefix, as (key, body) pairs.

    A reconcile tick now writes to TWO series — the interruption records and
    the I5727 fallback rollup — so reading `call_args` (the LAST write) picks
    whichever ran last rather than the one under test.
    """
    return [(c.kwargs["Key"], json.loads(c.kwargs["Body"].decode()))
            for c in aws.s3.put_object.call_args_list
            if c.kwargs["Key"].startswith(prefix)]


FALLBACK_PREFIX = "overseer/interruptions/_fallbacks/"


def _interruptions(aws):
    """Reclaim records only. The I5727 fallback series is a SUB-prefix of this
    one (it rides the same S3 grant), so a plain prefix match catches both."""
    return [(k, b) for k, b in _puts(aws, "overseer/interruptions/")
            if not k.startswith(FALLBACK_PREFIX)]


def _written(aws):
    got = _interruptions(aws)
    assert got, "no interruption record was written"
    return got[-1][1]


def _fallback_daily(aws):
    got = _puts(aws, "overseer/interruptions/_fallbacks/_daily/")
    assert got, "no fallback rollup was written"
    return got[-1][1]


def _fallback_records(aws):
    return [b for k, b in _puts(aws, FALLBACK_PREFIX) if "/_daily/" not in k]


# ── reconcile ────────────────────────────────────────────────────────────────


def test_reconcile_records_a_real_eviction_with_full_attribution(aws):
    out = index.handler({"mode": "reconcile"}, None)
    assert out["evictions_seen"] == 1 and out["records_written"] == 1
    rec = _written(aws)
    assert rec["instance_id"] == "i-07d70f8f1ca48020c"
    assert rec["instance_type"] == "t4g.medium"
    assert rec["availability_zone"] == "us-east-1c"
    assert rec["pool"] == "t4g.medium/us-east-1c"
    assert rec["workload"] == "high-only"          # from groom-issue-filter
    assert rec["attribution_gap"] is False
    assert rec["occurred_at"] == "2026-07-28T22:52:16Z"
    assert rec["lived_seconds"] == 1357            # 22:29:39 -> 22:52:16


def test_record_key_is_partitioned_by_the_event_date_not_today(aws):
    """A backfill run must not file historical events under the run date."""
    index.handler({"mode": "reconcile"}, None)
    key = _interruptions(aws)[-1][0]
    assert key.startswith("overseer/interruptions/2026-07-28/")
    assert REAL_EVICTION["eventID"] in key


def test_already_recorded_is_skipped_not_rewritten(aws):
    aws.s3.head_object.side_effect = None          # object exists
    aws.s3.head_object.return_value = {}
    out = index.handler({"mode": "reconcile"}, None)
    assert out["records_written"] == 0 and out["already_recorded"] == 1
    # The fallback rollup still writes every tick by design (its absence is
    # what a consumer renders as stale), so assert on the SERIES under test.
    assert not _interruptions(aws)


def test_multi_instance_eviction_yields_distinct_records(aws):
    ev = dict(REAL_EVICTION)
    ev["serviceEventDetails"] = {"instanceIdSet": ["i-aaa", "i-bbb"]}
    aws.ct.lookup_events.return_value = {"Events": [{"CloudTrailEvent": json.dumps(ev)}]}
    out = index.handler({"mode": "reconcile"}, None)
    assert out["records_written"] == 2
    keys = [k for k, _ in _interruptions(aws)]
    assert len(set(keys)) == 2, f"event_ids collided: {keys}"


# ── attribution gaps are findings, not nulls ─────────────────────────────────


def test_untagged_instance_is_flagged_as_an_attribution_gap(aws):
    aws.ec2.describe_instances.return_value = {
        "Reservations": [{"Instances": [{**INSTANCE, "Tags": []}]}]
    }
    aws.ct.lookup_events.side_effect = [
        {"Events": [{"CloudTrailEvent": json.dumps(REAL_EVICTION)}]},  # evictions
        {"Events": []},                                               # RunInstances fallback
    ]
    out = index.handler({"mode": "reconcile"}, None)
    assert out["attribution_gaps"] == 1
    assert _written(aws)["attribution_gap"] is True


def test_eviction_naming_no_instance_is_still_recorded(aws):
    ev = dict(REAL_EVICTION)
    ev["serviceEventDetails"] = {}
    aws.ct.lookup_events.return_value = {"Events": [{"CloudTrailEvent": json.dumps(ev)}]}
    out = index.handler({"mode": "reconcile"}, None)
    assert out["records_written"] == 1 and out["attribution_gaps"] == 1
    rec = _written(aws)
    assert rec["instance_id"] is None and rec["attribution_gap"] is True


def test_falls_back_to_runinstances_when_ec2_has_forgotten_the_box(aws):
    """The case that makes backfill work at all — EC2 keeps a terminated
    instance for about an hour; CloudTrail keeps the launch for 90 days."""
    aws.ec2.describe_instances.side_effect = Exception("InvalidInstanceID.NotFound")
    run_ev = {
        "eventTime": "2026-07-28T22:29:39Z",
        "requestParameters": {
            "instanceType": "t4g.medium",
            "instanceMarketOptions": {"marketType": "spot"},
            "tagSpecificationSet": {"items": [
                {"tags": [{"key": "groom-issue-filter", "value": "low-only"}]}
            ]},
        },
        "responseElements": {"instancesSet": {"items": [
            {"instanceId": "i-07d70f8f1ca48020c", "instanceType": "t4g.medium",
             "placement": {"availabilityZone": "us-east-1c"}}
        ]}},
    }
    aws.ct.lookup_events.side_effect = [
        {"Events": [{"CloudTrailEvent": json.dumps(REAL_EVICTION)}]},
        {"Events": [{"CloudTrailEvent": json.dumps(run_ev)}]},
    ]
    index.handler({"mode": "reconcile"}, None)
    rec = _written(aws)
    assert rec["workload"] == "low-only"
    assert rec["attribution_source"] == "cloudtrail_runinstances"
    assert rec["availability_zone"] == "us-east-1c"
    assert rec["attribution_gap"] is False


# ── fail-loud ────────────────────────────────────────────────────────────────


def test_cloudtrail_failure_raises_rather_than_reporting_an_empty_sweep(aws):
    """A recorder that returns 'zero evictions' on a broken PRIMARY input is
    indistinguishable from a quiet day — the exact class this Lambda ends."""
    aws.ct.lookup_events.side_effect = Exception("AccessDenied")
    with pytest.raises(index._RecorderError):
        index.handler({"mode": "reconcile"}, None)


def test_backfill_beyond_cloudtrail_retention_raises(aws):
    with pytest.raises(index._RecorderError, match="90-day"):
        index.handler({"mode": "backfill", "days": 120}, None)


def test_unknown_mode_raises(aws):
    with pytest.raises(index._RecorderError, match="unknown mode"):
        index.handler({"mode": "nope"}, None)


def test_backfill_widens_the_window(aws):
    index.handler({"mode": "backfill", "days": 7}, None)
    start = aws.ct.lookup_events.call_args.kwargs["StartTime"]
    assert NOW - start >= timedelta(days=7) - timedelta(minutes=1)


# ── event path ───────────────────────────────────────────────────────────────


def test_state_change_event_records_a_spot_termination(aws):
    out = index.handler({
        "source": "aws.ec2", "detail-type": "EC2 Instance State-change Notification",
        "detail": {"instance-id": "i-07d70f8f1ca48020c", "state": "terminated"},
    }, None)
    assert out["mode"] == "event" and out["written"]
    assert _written(aws)["detected_via"] == "state_change"


def test_on_demand_termination_is_not_a_reclaim(aws):
    aws.ec2.describe_instances.return_value = {
        "Reservations": [{"Instances": [{**INSTANCE, "InstanceLifecycle": None}]}]
    }
    out = index.handler({
        "source": "aws.ec2", "detail-type": "EC2 Instance State-change Notification",
        "detail": {"instance-id": "i-07d70f8f1ca48020c", "state": "terminated"},
    }, None)
    assert "skipped" in out
    # The fallback rollup still writes every tick by design (its absence is
    # what a consumer renders as stale), so assert on the SERIES under test.
    assert not _interruptions(aws)


def test_running_state_change_is_ignored(aws):
    out = index.handler({
        "source": "aws.ec2", "detail-type": "EC2 Instance State-change Notification",
        "detail": {"instance-id": "i-x", "state": "running"},
    }, None)
    assert out["skipped"] == "state=running"
    # The fallback rollup still writes every tick by design (its absence is
    # what a consumer renders as stale), so assert on the SERIES under test.
    assert not _interruptions(aws)


def test_event_without_instance_id_raises(aws):
    with pytest.raises(index._RecorderError, match="no instance-id"):
        index.handler({"source": "aws.ec2", "detail-type": "EC2 Instance State-change Notification",
                       "detail": {}}, None)


def test_interruption_warning_is_recorded_with_its_own_provenance(aws):
    out = index.handler({
        "source": "aws.ec2", "detail-type": "EC2 Spot Instance Interruption Warning",
        "detail": {"instance-id": "i-07d70f8f1ca48020c", "instance-action": "terminate"},
    }, None)
    assert out["mode"] == "event"
    assert _written(aws)["detected_via"] == "spot_interruption_warning"


# ── the RunInstances index is per-invocation, never carried between them ─────


def test_run_instances_index_is_rebuilt_each_invocation(aws):
    """A Lambda container is reused. An index cached across invocations would
    serve a 10:00 build to a 10:55 tick and silently fail to attribute every
    box launched in between — a stale-cache bug that looks exactly like an
    attribution gap. Caught by this test when the cache was keyed on a coarse
    time bucket instead of being reset at handler entry."""
    aws.ec2.describe_instances.side_effect = Exception("InvalidInstanceID.NotFound")

    def _run_ev(instance_id, workload):
        return {
            "eventTime": "2026-07-28T22:29:39Z",
            "requestParameters": {
                "instanceMarketOptions": {"marketType": "spot"},
                "tagSpecificationSet": {"items": [
                    {"tags": [{"key": "groom-issue-filter", "value": workload}]}
                ]},
            },
            "responseElements": {"instancesSet": {"items": [
                {"instanceId": instance_id, "instanceType": "t4g.medium",
                 "placement": {"availabilityZone": "us-east-1c"}}
            ]}},
        }

    first = dict(REAL_EVICTION, eventID="ev-1")
    first["serviceEventDetails"] = {"instanceIdSet": ["i-first"]}
    second = dict(REAL_EVICTION, eventID="ev-2")
    second["serviceEventDetails"] = {"instanceIdSet": ["i-second"]}

    # Invocation 1 sees only i-first's launch.
    aws.ct.lookup_events.side_effect = [
        {"Events": [{"CloudTrailEvent": json.dumps(first)}]},
        {"Events": [{"CloudTrailEvent": json.dumps(_run_ev("i-first", "high-only"))}]},
    ]
    index.handler({"mode": "reconcile"}, None)
    assert _written(aws)["workload"] == "high-only"

    # Invocation 2: a DIFFERENT box, launched after the first index was built.
    # With a carried-over index this attributes to nothing.
    aws.s3.put_object.reset_mock()
    aws.ct.lookup_events.side_effect = [
        {"Events": [{"CloudTrailEvent": json.dumps(second)}]},
        {"Events": [{"CloudTrailEvent": json.dumps(_run_ev("i-second", "mid-only"))}]},
    ]
    index.handler({"mode": "reconcile"}, None)
    rec = _written(aws)
    assert rec["workload"] == "mid-only", "index was carried across invocations"
    assert rec["attribution_gap"] is False


def test_index_is_built_once_within_a_single_run(aws):
    """The whole point of the index: N evictions must not cost N scans. The
    first implementation did, and CloudTrail throttled the 7-day backfill into
    a 300s timeout after recording only two days."""
    aws.ec2.describe_instances.side_effect = Exception("InvalidInstanceID.NotFound")
    ev = dict(REAL_EVICTION)
    ev["serviceEventDetails"] = {"instanceIdSet": [f"i-{n}" for n in range(10)]}
    aws.ct.lookup_events.side_effect = [
        {"Events": [{"CloudTrailEvent": json.dumps(ev)}]},   # evictions
        {"Events": []},                                      # ONE RunInstances scan
    ]
    index.handler({"mode": "reconcile"}, None)
    # 1 eviction lookup + exactly 1 RunInstances lookup, for 10 instances.
    assert aws.ct.lookup_events.call_count == 2, (
        f"expected 2 CloudTrail lookups, got {aws.ct.lookup_events.call_count} "
        f"— the index is being rebuilt per instance"
    )


# ── fallback series (alpha-engine-config-I5727) ──────────────────────────────
#
# The reclaim paths above cannot see a spot->on-demand ESCALATION: nothing was
# interrupted, so there is no BidEvictedEvent. The sweep reads the provenance
# tags nousergon-lib v0.124.23 writes onto every launch, out of the SAME
# RunInstances index the reclaim reconciler already builds.

def _run_instances_event(*, iid, tags, at="2026-07-28T22:29:39Z", itype="t4g.medium"):
    return {
        "eventTime": at,
        "eventName": "RunInstances",
        "eventID": f"run-{iid}",
        "requestParameters": {
            "instanceType": itype,
            "tagSpecificationSet": {
                "items": [{"resourceType": "instance",
                           "tags": [{"key": k, "value": v} for k, v in tags.items()]}]
            },
        },
        "responseElements": {"instancesSet": {"items": [
            {"instanceId": iid, "instanceType": itype,
             "placement": {"availabilityZone": "us-east-1c"}}
        ]}},
    }


def _route_lookups(aws, launches):
    """CloudTrail returns evictions for one EventName and launches for the other."""
    def _lookup(**kwargs):
        name = kwargs["LookupAttributes"][0]["AttributeValue"]
        events = launches if name == "RunInstances" else [REAL_EVICTION]
        return {"Events": [{"CloudTrailEvent": json.dumps(e)} for e in events]}
    aws.ct.lookup_events.side_effect = _lookup


def test_capacity_fallback_is_recorded_with_its_discriminator(aws):
    _route_lookups(aws, [_run_instances_event(
        iid="i-fell", tags={"Name": "alpha-engine-groom-spot",
                            "LaunchMarket": "on-demand",
                            "LaunchReason": "capacity_exhausted"})])
    index.handler({"mode": "reconcile"}, None)

    recs = _fallback_records(aws)
    assert len(recs) == 1
    assert recs[0]["reason"] == "capacity_exhausted"
    assert recs[0]["market"] == "on-demand"
    assert recs[0]["workload"] == "alpha-engine-groom-spot"


def test_a_successful_spot_launch_is_counted_but_not_recorded(aws):
    """The denominator. A fallback COUNT with no launch count is not a rate, and
    a rate is what the spot-vs-on-demand ruling's revisit condition needs."""
    _route_lookups(aws, [_run_instances_event(
        iid="i-ok", tags={"Name": "alpha-engine-groom-spot",
                          "LaunchMarket": "spot", "LaunchReason": "spot_ok"})])
    index.handler({"mode": "reconcile"}, None)

    assert _fallback_records(aws) == []
    daily = _fallback_daily(aws)
    assert daily["launches"] == 1 and daily["fallbacks"] == 0
    assert daily["lanes"]["alpha-engine-groom-spot"]["spot_ok"] == 1


def test_a_launch_without_provenance_is_not_counted_as_a_spot_success(aws):
    """A box predating v0.124.23, or launched by a path that bypasses
    launch_with_fallback, is UNCLASSIFIED. Folding it into spot_ok would be the
    absence-reads-as-health defect this series exists to close."""
    _route_lookups(aws, [_run_instances_event(
        iid="i-old", tags={"Name": "alpha-engine-groom-spot"})])
    index.handler({"mode": "reconcile"}, None)

    daily = _fallback_daily(aws)
    assert daily["provenance_missing"] == 1
    assert daily["lanes"]["alpha-engine-groom-spot"].get("spot_ok", 0) == 0
    assert _fallback_records(aws) == []


def test_forced_and_capacity_fallbacks_stay_distinct_in_the_rollup(aws):
    """Both cost the on-demand premium; they have different fixes."""
    _route_lookups(aws, [
        _run_instances_event(iid="i-a", tags={"Name": "lane-a",
                                              "LaunchMarket": "on-demand",
                                              "LaunchReason": "capacity_exhausted"}),
        _run_instances_event(iid="i-b", tags={"Name": "lane-a",
                                              "LaunchMarket": "on-demand",
                                              "LaunchReason": "force_on_demand"}),
    ])
    index.handler({"mode": "reconcile"}, None)

    lane = _fallback_daily(aws)["lanes"]["lane-a"]
    assert lane["capacity_exhausted"] == 1
    assert lane["force_on_demand"] == 1
    assert lane["launches"] == 2


def test_an_unknown_reason_is_reported_verbatim_not_bucketed(aws):
    """A value added to the library and not mirrored here must surface as
    itself, never silently join spot_ok."""
    _route_lookups(aws, [_run_instances_event(
        iid="i-new", tags={"Name": "lane-a", "LaunchReason": "some_future_reason"})])
    index.handler({"mode": "reconcile"}, None)

    assert _fallback_daily(aws)["lanes"]["lane-a"]["some_future_reason"] == 1


def test_the_rollup_is_written_even_when_the_window_held_no_launches(aws):
    """'Swept, saw nothing' must be distinguishable from 'did not sweep'. The
    consumer renders an ABSENT rollup as stale; an empty one is a real zero."""
    _route_lookups(aws, [])
    index.handler({"mode": "reconcile"}, None)

    daily = _fallback_daily(aws)
    assert daily["launches"] == 0 and daily["lanes"] == {}


def test_a_failing_fallback_sweep_does_not_lose_the_reclaim_result(aws, monkeypatch):
    """The reclaim series is the higher-value artifact and already succeeded.
    The error is reported in the return value AND logged — never swallowed."""
    monkeypatch.setattr(index, "_sweep_fallbacks",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    out = index.handler({"mode": "reconcile"}, None)

    assert out["records_written"] == 1
    assert "boom" in out["fallbacks"]["error"]


def test_fallback_reason_vocabulary_matches_lib():
    """This Lambda duplicates the four-string vocabulary rather than importing
    nousergon_lib (its requirements.txt does not pull the library). The
    duplication is pinned here so a library rename cannot drift silently."""
    assert set(index.FALLBACK_REASONS) == {
        "capacity_exhausted", "quota_exceeded", "force_on_demand"}
    assert index.REASON_SPOT_OK == "spot_ok"
    assert index.LAUNCH_MARKET_TAG == "LaunchMarket"
    assert index.LAUNCH_REASON_TAG == "LaunchReason"
