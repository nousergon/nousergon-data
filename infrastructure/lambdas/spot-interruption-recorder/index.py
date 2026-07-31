"""alpha-engine-spot-interruption-recorder — a durable series of every spot
reclaim, with the unit of work it was carrying (alpha-engine-config-I5197).

WHY THIS EXISTS. The fleet runs its agent plane on interruptible capacity by
deliberate choice (`cost-management-policy.md`) and, before this Lambda,
recorded **nothing** when that capacity was taken away. The consequence was not
abstract: the 2026-07-28 spot-vs-on-demand ruling had to be argued from
anecdote, and its own revisit condition — "revisit if reclaim rate degrades
cycle completion rate" — was unobservable, because there was no reclaim rate.

TWO DETECTION PATHS, AND WHY THE SECOND IS THE LOAD-BEARING ONE.

  1. **Event path** — EventBridge delivers `EC2 Spot Instance Interruption
     Warning` (the 2-minute notice) and `EC2 Instance State-change
     Notification`. Low latency, and insufficient alone: AWS emits the warning
     only when it has two minutes to give. A box reclaimed during bootstrap
     gets no warning at all, and every one of the six reclaims measured on
     2026-07-28 was of exactly that kind. The pre-existing rule
     (`alpha-engine-sf-watch-spot-interruption`) fired for none of them.

  2. **Reconciler path** — a scheduled sweep of CloudTrail `BidEvictedEvent`.
     Failure-mode-agnostic in the sense `groom-sweep-policy.md` §2.7 means: it
     does not care *how* the box died or whether any notice was emitted, only
     that AWS recorded an eviction. It is the path that actually closes the
     gap; the event path is a latency optimisation on top of it.

WHY CLOUDTRAIL AND NOT `describe-spot-instance-requests`. I5197 originally
specified the latter. It does not work, and this was measured rather than
assumed: on 2026-07-28 the reclaims at 12:00Z and 15:27Z were **already absent**
from `describe-spot-instance-requests` by 23:00Z the same day, while the 22:18Z
one was still present. AWS drops closed spot requests within hours, so any
sweep slower than that silently loses events and no backfill is possible.
CloudTrail retains 90 days and carries the instance set in
`serviceEventDetails.instanceIdSet`. It is the only durable source, which also
makes historical backfill possible — see `mode: backfill`.

ATTRIBUTION. A reclaim that cannot be joined to a unit of work is a finding,
not a row to drop (I5197 deliverable 4). Instance tags are the join; terminated
instances remain describable for roughly an hour, so the reconciler falls back
to the instance's `RunInstances` CloudTrail record (90 days) to recover type,
AZ and tags when the instance itself is gone. Only when BOTH fail is the record
written with `attribution_gap: true`, and that is surfaced, never silently
normalised into a null.

NOT IN SCOPE — REMEDIATION. This Lambda observes. Recovery belongs to the
interrupted work's own path (the groom's expectation ledger + reconciler, and
sf-watch's relaunch for pipeline boxes). A second remediation path here would
duplicate authority and is out of charter per I5197.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
PREFIX = os.environ.get("INTERRUPTION_PREFIX", "overseer/interruptions")
SCHEMA_VERSION = 1

# How far back a routine reconciler tick looks. Generous relative to the 5-min
# cadence: overlap is free (writes are idempotent on event_id) and a missed tick
# must not leave a hole.
DEFAULT_LOOKBACK_MIN = int(os.environ.get("RECONCILE_LOOKBACK_MIN", "90"))

# ── Fallback series (alpha-engine-config-I5727) ──────────────────────────────
# A spot->on-demand ESCALATION is a different event from a reclaim and none of
# this Lambda's eviction paths can see one: nothing was interrupted, no instance
# state changed, and there is no BidEvictedEvent, because there was no eviction.
# It is a decision taken at launch because spot capacity was refused.
#
# nousergon-lib v0.124.23 records that decision ON the box, as LaunchMarket /
# LaunchReason tags riding the same RunInstances call. Those tags are in the
# CloudTrail RunInstances record this Lambda ALREADY fetches and indexes for
# reclaim attribution — so the sweep below costs no additional API calls, only
# a second read of an index that is already in memory.
# Deliberately a SUB-prefix of the interruption series, not a sibling
# `overseer/fallbacks`. The sibling reads better and costs an operator step: this
# Lambda's role is granted s3:PutObject on `overseer/interruptions/*` only, and
# widening it would make the PR undeployable by the merge button alone
# (pull-request-policy.md §4.2 — and iam-policy-change-guard enforces exactly
# that, correctly). An S3 prefix name is not a security boundary, so paying a
# standing human step for a naming preference is the wrong trade. `_fallbacks`
# cannot collide with the date-partitioned record keys beside it.
FALLBACK_PREFIX = os.environ.get("FALLBACK_PREFIX", "overseer/interruptions/_fallbacks")
FALLBACK_SCHEMA_VERSION = 1

LAUNCH_MARKET_TAG = "LaunchMarket"
LAUNCH_REASON_TAG = "LaunchReason"
REASON_SPOT_OK = "spot_ok"
#: Reasons that mean an escalation off spot happened. Mirrors
#: nousergon_lib.spot_dispatch.FALLBACK_REASONS. Kept as a literal rather than
#: imported because this Lambda's requirements.txt does not pull the library,
#: and a lockstep on a four-string vocabulary is worse than a duplicated
#: constant that a test pins (see test_fallback_reason_vocabulary_matches_lib).
FALLBACK_REASONS = ("capacity_exhausted", "quota_exceeded", "force_on_demand")
#: A launch whose provenance tags are absent. NOT folded into spot_ok: the box
#: may predate v0.124.23, or have been launched by a path that bypasses
#: launch_with_fallback. Either way it is un-classified, and reporting it as a
#: successful spot launch would be the absence-reads-as-health defect this
#: series exists to close.
REASON_MISSING = "provenance_missing"

# Tag keys that name the unit of work, most specific first. These are keys the
# fleet's launchers already set; a new launcher adds its key here rather than
# inventing a parallel tagging scheme.
WORKLOAD_TAG_KEYS = ("groom-issue-filter", "alpha-engine-workload", "Name")


class _RecorderError(RuntimeError):
    """A PRIMARY input failed. Raised so the failure surfaces on the Lambda
    Errors metric rather than being written as an empty sweep — a recorder that
    silently records nothing is indistinguishable from a quiet day, which is
    the exact failure class this Lambda exists to end."""


def _s3():
    return boto3.client("s3", region_name=REGION)


def _ec2():
    return boto3.client("ec2", region_name=REGION)


def _cloudtrail():
    # LookupEvents is quota-limited (~2 TPS). Adaptive mode adds client-side
    # rate limiting on top of retries, so a burst degrades into slower calls
    # rather than the ThrottlingException that killed the first backfill.
    return boto3.client(
        "cloudtrail", region_name=REGION,
        config=BotoConfig(retries={"max_attempts": 8, "mode": "adaptive"}),
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# ── CloudTrail ───────────────────────────────────────────────────────────────


def _lookup(event_name: str, start: datetime, end: datetime | None = None) -> list[dict]:
    """Every CloudTrail event of a name in a window. PRIMARY — raises."""
    ct = _cloudtrail()
    kwargs = {
        "LookupAttributes": [{"AttributeKey": "EventName", "AttributeValue": event_name}],
        "StartTime": start,
        "MaxResults": 50,
    }
    if end:
        kwargs["EndTime"] = end
    out, token = [], None
    try:
        while True:
            if token:
                kwargs["NextToken"] = token
            resp = ct.lookup_events(**kwargs)
            for row in resp.get("Events", []):
                try:
                    out.append(json.loads(row["CloudTrailEvent"]))
                except (KeyError, ValueError) as exc:
                    raise _RecorderError(f"unparseable CloudTrail event: {exc}") from exc
            token = resp.get("NextToken")
            if not token:
                return out
    except _RecorderError:
        raise
    except Exception as exc:  # noqa: BLE001 — converted to a loud failure
        raise _RecorderError(f"CloudTrail lookup({event_name}) failed: {exc}") from exc


def _evicted_instances(event: dict) -> list[str]:
    return list((event.get("serviceEventDetails") or {}).get("instanceIdSet") or [])


# ── Attribution ──────────────────────────────────────────────────────────────


def _describe_instance(instance_id: str) -> dict | None:
    """Instance facts while EC2 still has them (~1h after termination)."""
    try:
        resp = _ec2().describe_instances(InstanceIds=[instance_id])
    except Exception as exc:  # noqa: BLE001 — a vanished instance is expected, not fatal
        logger.info("describe_instances(%s) unavailable: %s", instance_id, exc)
        return None
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst
    return None


# RunInstances index, built ONCE per invocation and keyed by lookback start.
#
# The first implementation did a full paginated CloudTrail scan PER eviction.
# Measured live on the first 7-day backfill: ~21 pages × 63 evictions ≈ 1,300
# LookupEvents calls against a ~2 TPS quota — it threw ThrottlingException
# ("reached max retries: 4") and then timed out at 300 s having recorded only
# the two most recent days. One scan, indexed by instance id, is the same data
# for ~1/60th of the calls.
# Scoped to ONE invocation and cleared at handler entry — never carried across
# invocations. A Lambda container is reused, so a module-level cache keyed on a
# coarse time bucket would happily serve an index built at 10:00 to a tick at
# 10:55 and silently fail to attribute every box launched in between. The cache
# exists to collapse N scans within a run into one, not to persist anything.
_RUN_INDEX: dict[str, dict] = {}
_RUN_INDEX_BUILT: bool = False


def _reset_run_index() -> None:
    global _RUN_INDEX, _RUN_INDEX_BUILT
    _RUN_INDEX, _RUN_INDEX_BUILT = {}, False


def _run_instances_index(since: datetime) -> dict[str, dict]:
    """instance_id -> {event, item} for every RunInstances in the window."""
    global _RUN_INDEX, _RUN_INDEX_BUILT
    if _RUN_INDEX_BUILT:
        return _RUN_INDEX
    index: dict[str, dict] = {}
    for ev in _lookup("RunInstances", since):
        items = ((ev.get("responseElements") or {}).get("instancesSet") or {}).get("items", [])
        for item in items:
            iid = item.get("instanceId")
            if iid and iid not in index:
                index[iid] = {"event": ev, "item": item}
    _RUN_INDEX, _RUN_INDEX_BUILT = index, True
    logger.info("RunInstances index built: %d instance(s) since %s", len(index), _iso(since))
    return index


def _run_instances_record(instance_id: str, since: datetime) -> dict | None:
    """Fall back to the launch's own CloudTrail record (90 days) once EC2 has
    forgotten the instance. This is what keeps a reclaim attributable long after
    the box is gone, and it is why backfill works at all."""
    return _run_instances_index(since).get(instance_id)


def _tags_to_dict(tags) -> dict:
    return {t.get("Key") or t.get("key"): t.get("Value") or t.get("value")
            for t in (tags or []) if (t.get("Key") or t.get("key"))}


def _workload_from_tags(tags: dict) -> str | None:
    for key in WORKLOAD_TAG_KEYS:
        if tags.get(key):
            return tags[key]
    return None


def _attribute(instance_id: str, occurred_at: datetime,
               launch_since: datetime | None = None) -> dict:
    """Everything known about the box: from EC2 if it still exists, and from the
    launch's CloudTrail record if it does not."""
    facts = {
        "instance_type": None, "availability_zone": None, "market": None,
        "launched_at": None, "tags": {}, "attribution_source": None,
    }

    inst = _describe_instance(instance_id)
    if inst:
        facts.update({
            "instance_type": inst.get("InstanceType"),
            "availability_zone": (inst.get("Placement") or {}).get("AvailabilityZone"),
            "market": inst.get("InstanceLifecycle") or "on-demand",
            "launched_at": _iso(inst["LaunchTime"]) if inst.get("LaunchTime") else None,
            "tags": _tags_to_dict(inst.get("Tags")),
            "attribution_source": "ec2",
        })
        if facts["tags"]:
            return facts

    # EC2 has forgotten it (or it carried no tags) — recover from the launch.
    if launch_since is None:
        launch_since = occurred_at - timedelta(days=_launch_lookback_days())
    rec = _run_instances_record(instance_id, launch_since)
    if rec:
        item, ev = rec["item"], rec["event"]
        rp = ev.get("requestParameters") or {}
        tags = {}
        for spec in (rp.get("tagSpecificationSet") or {}).get("items", []):
            tags.update(_tags_to_dict(spec.get("tags")))
        facts.update({
            "instance_type": facts["instance_type"] or item.get("instanceType") or rp.get("instanceType"),
            "availability_zone": facts["availability_zone"] or (item.get("placement") or {}).get("availabilityZone"),
            "market": facts["market"] or ("spot" if "spot" in json.dumps(rp).lower() else None),
            "launched_at": facts["launched_at"] or ev.get("eventTime"),
            "tags": facts["tags"] or tags,
            "attribution_source": facts["attribution_source"] or "cloudtrail_runinstances",
        })
    return facts


# ── Records ──────────────────────────────────────────────────────────────────


def _key(occurred_at: datetime, event_id: str) -> str:
    return f"{PREFIX}/{occurred_at.strftime('%Y-%m-%d')}/{event_id}.json"


def _exists(key: str) -> bool:
    try:
        _s3().head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:  # noqa: BLE001 — 404 is the common case
        return False


def _launch_lookback_days() -> int:
    return int(os.getenv("LAUNCH_LOOKBACK_DAYS", "3"))


def _build_record(*, event_id: str, instance_id: str, occurred_at: datetime,
                  detected_via: str, cause_code: str | None,
                  launch_since: datetime | None = None) -> dict:
    facts = _attribute(instance_id, occurred_at, launch_since)
    workload = _workload_from_tags(facts["tags"])
    lived = None
    if facts["launched_at"]:
        try:
            lived = int((occurred_at - _parse_iso(facts["launched_at"])).total_seconds())
        except ValueError:
            lived = None
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "detected_via": detected_via,
        "occurred_at": _iso(occurred_at),
        "recorded_at": _iso(_now()),
        "instance_id": instance_id,
        "instance_type": facts["instance_type"],
        "availability_zone": facts["availability_zone"],
        "pool": f"{facts['instance_type']}/{facts['availability_zone']}",
        "market": facts["market"],
        "cause_code": cause_code,
        "workload": workload,
        "tags": facts["tags"],
        "launched_at": facts["launched_at"],
        "lived_seconds": lived,
        "attribution_source": facts["attribution_source"],
        # I5197 deliverable 4: surfaced, never silently normalised to null.
        "attribution_gap": workload is None,
    }


def _write(record: dict) -> str:
    occurred = _parse_iso(record["occurred_at"])
    key = _key(occurred, record["event_id"])
    if _exists(key):
        return ""
    _s3().put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(record, indent=2).encode(),
        ContentType="application/json",
    )
    return key


def _unattributable_record(event_id: str, occurred: datetime) -> dict:
    """An eviction naming no instance is itself unattributable — recorded
    rather than dropped, so the gap is countable."""
    return {
        "schema_version": SCHEMA_VERSION, "event_id": event_id,
        "detected_via": "cloudtrail_bid_evicted",
        "occurred_at": _iso(occurred), "recorded_at": _iso(_now()),
        "instance_id": None, "instance_type": None, "availability_zone": None,
        "pool": None, "market": "spot", "cause_code": "bid-evicted",
        "workload": None, "tags": {}, "launched_at": None, "lived_seconds": None,
        "attribution_source": None, "attribution_gap": True,
    }


# ── Modes ────────────────────────────────────────────────────────────────────


_LAST_WINDOW: dict = {}


def _reconcile(lookback_min: int, until: datetime | None = None) -> dict:
    """Sweep CloudTrail evictions and record any not already recorded."""
    end = until or _now()
    start = end - timedelta(minutes=lookback_min)
    events = _lookup("BidEvictedEvent", start, end)

    # ONE launch-lookback window for the whole run, so the RunInstances index is
    # built exactly once however many evictions the window holds. Per-eviction
    # windows would each miss the cache and re-scan.
    launch_since = start - timedelta(days=_launch_lookback_days())

    # Published so the fallback sweep runs over the identical window and the
    # identical RunInstances index rather than recomputing either.
    _LAST_WINDOW.update({"start": start, "end": end, "launch_since": launch_since})

    written, skipped, gaps = [], 0, 0
    for ev in events:
        occurred = _parse_iso(ev["eventTime"])
        instances = _evicted_instances(ev) or [None]
        for idx, iid in enumerate(instances):
            event_id = ev.get("eventID", "") + (f"-{idx}" if len(instances) > 1 else "")
            rec = (_unattributable_record(event_id, occurred) if iid is None
                   else _build_record(event_id=event_id, instance_id=iid,
                                      occurred_at=occurred,
                                      detected_via="cloudtrail_bid_evicted",
                                      cause_code="bid-evicted",
                                      launch_since=launch_since))
            key = _write(rec)
            if key:
                written.append(key)
                gaps += 1 if rec["attribution_gap"] else 0
            else:
                skipped += 1

    result = {
        "mode": "reconcile", "window_start": _iso(start), "window_end": _iso(end),
        "evictions_seen": len(events), "records_written": len(written),
        "already_recorded": skipped, "attribution_gaps": gaps,
    }
    logger.info("reconcile: %s", json.dumps(result))
    return result


def _from_event(event: dict) -> dict:
    """Low-latency path: an EventBridge EC2 notification."""
    detail = event.get("detail") or {}
    iid = detail.get("instance-id")
    if not iid:
        raise _RecorderError(f"EC2 event carries no instance-id: {json.dumps(event)[:300]}")
    if "Interruption Warning" in event.get("detail-type", ""):
        cause, via = "interruption-warning", "spot_interruption_warning"
    else:
        state = detail.get("state", "")
        if state not in ("shutting-down", "terminated"):
            return {"mode": "event", "skipped": f"state={state}"}
        cause, via = f"state-{state}", "state_change"

    occurred = _now()
    rec = _build_record(event_id=f"{iid}-{int(occurred.timestamp())}", instance_id=iid,
                        occurred_at=occurred, detected_via=via, cause_code=cause)
    # Only spot boxes belong in a spot-interruption series; an on-demand
    # termination is a normal shutdown, not a reclaim.
    if rec.get("market") not in ("spot", None):
        return {"mode": "event", "skipped": f"market={rec.get('market')}"}
    key = _write(rec)
    return {"mode": "event", "instance_id": iid, "written": key or "already-recorded"}


def _fallback_key(launched_at: datetime, event_id: str) -> str:
    return f"{FALLBACK_PREFIX}/{launched_at.strftime('%Y-%m-%d')}/{event_id}.json"


def _daily_key(day: str) -> str:
    return f"{FALLBACK_PREFIX}/_daily/{day}.json"


def _classify(tags: dict) -> str:
    """Total classifier. An unrecognised LaunchReason is returned verbatim
    rather than bucketed, so a value added to the library and not here shows up
    as itself instead of silently joining spot_ok."""
    reason = tags.get(LAUNCH_REASON_TAG)
    return reason if reason else REASON_MISSING


def _build_fallback_record(*, event_id: str, instance_id: str, item: dict,
                           ev: dict, tags: dict, reason: str) -> dict:
    rp = ev.get("requestParameters") or {}
    return {
        "schema_version": FALLBACK_SCHEMA_VERSION,
        "event_id": event_id,
        "instance_id": instance_id,
        "launched_at": ev.get("eventTime"),
        "recorded_at": _iso(_now()),
        "reason": reason,
        "market": tags.get(LAUNCH_MARKET_TAG),
        "instance_type": item.get("instanceType") or rp.get("instanceType"),
        "availability_zone": (item.get("placement") or {}).get("availabilityZone"),
        "workload": _workload_from_tags(tags),
        "tags": tags,
        # Mirrors the interruption series: a launch nothing can attribute to a
        # lane is recorded WITH the gap flagged, never dropped.
        "attribution_gap": _workload_from_tags(tags) is None,
    }


def _sweep_fallbacks(start: datetime, end: datetime, launch_since: datetime) -> dict:
    """Classify every launch in [start, end) by its provenance tags.

    Reads the RunInstances index built by the reclaim reconciler in the same
    invocation — see the module note on FALLBACK_PREFIX. ``launch_since`` is the
    index's own window and is passed explicitly rather than recomputed, so a
    future change to one cannot silently desynchronise the other.

    Returns per-lane counts INCLUDING the denominator. A fallback count with no
    launch count is not a rate, and a rate is the thing the spot-vs-on-demand
    ruling's revisit condition is written against.
    """
    lanes: dict[str, dict[str, int]] = {}
    written: list[str] = []
    for iid, rec in _run_instances_index(launch_since).items():
        ev, item = rec["event"], rec["item"]
        launched_raw = ev.get("eventTime")
        if not launched_raw:
            continue
        try:
            launched = _parse_iso(launched_raw)
        except ValueError:
            continue
        if not (start <= launched < end):
            continue

        tags = {}
        for spec in ((ev.get("requestParameters") or {}).get("tagSpecificationSet")
                     or {}).get("items", []):
            tags.update(_tags_to_dict(spec.get("tags")))
        reason = _classify(tags)
        lane = _workload_from_tags(tags) or "_unattributed"
        counts = lanes.setdefault(lane, {"launches": 0})
        counts["launches"] += 1
        counts[reason] = counts.get(reason, 0) + 1

        if reason in FALLBACK_REASONS:
            event_id = ev.get("eventID", "") or f"{iid}-{launched_raw}"
            key = _fallback_key(launched, event_id)
            if not _exists(key):
                _s3().put_object(
                    Bucket=BUCKET, Key=key,
                    Body=json.dumps(_build_fallback_record(
                        event_id=event_id, instance_id=iid, item=item, ev=ev,
                        tags=tags, reason=reason), indent=2).encode(),
                    ContentType="application/json",
                )
                written.append(key)

    summary = {
        "schema_version": FALLBACK_SCHEMA_VERSION,
        "window_start": _iso(start),
        "window_end": _iso(end),
        "recorded_at": _iso(_now()),
        "lanes": lanes,
        "launches": sum(c["launches"] for c in lanes.values()),
        "fallbacks": sum(c.get(r, 0) for c in lanes.values() for r in FALLBACK_REASONS),
        "provenance_missing": sum(c.get(REASON_MISSING, 0) for c in lanes.values()),
        "written": written,
    }
    # The rollup is overwritten every sweep for the day it ends in — idempotent, and
    # the freshest window wins. Its absence is what a consumer must render as
    # stale; an empty `lanes` means "swept, saw nothing", which is different.
    _s3().put_object(
        Bucket=BUCKET, Key=_daily_key(end.strftime("%Y-%m-%d")),
        Body=json.dumps(summary, indent=2).encode(),
        ContentType="application/json",
    )
    logger.info("fallback sweep: %d launch(es), %d fallback(s), %d without provenance",
                summary["launches"], summary["fallbacks"], summary["provenance_missing"])
    return summary


def handler(event, context):  # noqa: ANN001, ARG001
    """Modes:
      EC2 EventBridge notification      low-latency single-instance record
      {"mode": "reconcile", ...}        sweep the last RECONCILE_LOOKBACK_MIN (default)
      {"mode": "backfill", "days": N}   sweep up to CloudTrail's 90-day retention
    """
    event = event or {}
    # Per-invocation scope for the RunInstances index (see _reset_run_index).
    _reset_run_index()
    if event.get("source") == "aws.ec2" and "detail" in event:
        return _from_event(event)

    mode = event.get("mode", "reconcile")
    if mode == "backfill":
        days = int(event.get("days", 7))
        if days > 90:
            raise _RecorderError(f"backfill days={days} exceeds CloudTrail's 90-day retention")
        return _reconcile(lookback_min=days * 24 * 60)
    if mode == "reconcile":
        out = _reconcile(lookback_min=int(event.get("lookback_min", DEFAULT_LOOKBACK_MIN)))
        # Same invocation, same window, same already-built RunInstances index —
        # the fallback sweep adds no CloudTrail calls (I5727). Failing it must
        # not lose the reclaim result that already succeeded, so it is reported
        # rather than raised: the reclaim series is the higher-value artifact
        # and the error lands in the return value AND the log, never silently.
        try:
            out["fallbacks"] = _sweep_fallbacks(
                _LAST_WINDOW["start"], _LAST_WINDOW["end"], _LAST_WINDOW["launch_since"])
        except Exception as exc:  # noqa: BLE001
            logger.exception("fallback sweep failed")
            out["fallbacks"] = {"error": repr(exc)}
        return out
    raise _RecorderError(f"unknown mode {mode!r}")
