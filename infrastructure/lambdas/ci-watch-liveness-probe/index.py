"""alpha-engine-ci-watch-liveness-probe — mid-run spot-reclaim checker for
Fleet CI Watch (config#3173, generalizing sf-watch-liveness-probe's
config#2270 mechanism to the ci-watch dispatch family).

WHY: ci-watch is purely EVENT-driven — a fresh box is only ever dispatched
when a NEW commit's CI run fails (nousergon-lib notify-ci-failure.yml ->
repository_dispatch -> sf-watch.yml's ci-watch-dispatch job -> the Overseer
router -> alpha-engine-ci-watch-dispatcher). Unlike Fleet-SF Watch or groom,
NOTHING re-fires on a cadence or on the SF's own retry: if the dispatched box
is reclaimed by AWS (spot interruption) or otherwise dies before writing its
completion marker, main stays red and NOTHING notices until a human happens
to look or a fresh commit lands — a genuine silent stall, the exact failure
mode alpha-engine-config#3173 (child of the #3137 stall-watchdog charter)
asks every dispatch family to close.

MECHANISM (mirrors sf-watch-liveness-probe's reclaim checker almost exactly):
this Lambda is the EventBridge target for `EC2 Spot Instance Interruption
Warning` and `EC2 Instance State-change Notification` (state=terminated)
events fleet-wide (neither event type can be tag-scoped in the rule pattern —
this handler filters by the box's own Name tag, exiting quietly for every
non-ci-watch instance). For a `alpha-engine-ci-watch-spot` box:
  1. DescribeTags for `ci-watch-repo` / `ci-watch-sha` (config#2267 site 2 —
     these ride the RunInstances call atomically with launch, so a box that
     reaches this checker with either tag missing is a genuine anomaly, not a
     launch/tag race).
  2. HEAD the completion marker
     (`ci_watch/_control/completed/{repo}-{sha}.json`, the same key
     scripts/ci_watch_run.sh writes and spot-orphan-reaper's WATCH_KINDS
     entry already reads). Present = clean finish, nothing to do.
  3. Absent = the box died mid-run. Read the relaunch ledger
     (`ci_watch/_control/relaunch/{repo}-{sha}.json`): a record naming THIS
     dead instance is a duplicate notification (both EC2 event types fire for
     one death); a record naming a DIFFERENT instance is a second death for
     the same (repo, sha) — the exactly-one relaunch bound is spent, escalate
     LOUD instead of relaunching again (second death = human, mirroring
     config#2270's own posture).
  4. First death: read the dispatch record
     (`ci_watch/_control/dispatched/{repo}-{sha}.json`, written by
     ci-watch-dispatcher right after launch — config#3173) to recover
     run_id/run_url/workflow/branch. Missing/unreadable = escalate LOUD
     ("unreconstructable dispatch fields") rather than guessing. Present:
     record the relaunch decision FIRST (exactly-one bound), THEN invoke
     alpha-engine-ci-watch-dispatcher DIRECTLY (bypassing the Overseer router
     and GHA entirely, same as sf-watch-liveness-probe invokes the spot
     dispatcher directly) with the reconstructed fields — a fresh box picks
     up the same (repo, sha) with no dedup conflict (the dead box no longer
     holds the concurrency lock).

Deliberately NOT included (kept narrow, matching what #3173 actually asks
for): a "dropped-window" scheduled sweep analogous to sf-watch's config#2257.
ci-watch has no enable/disable window that can eat a real-time trigger the
way sf-watch's EventBridge rule can (ci-watch's caller chain already files a
P1 when the Overseer router itself is unreachable/malformed — see
sf-watch.yml's ci-watch-dispatch job) — inventing a speculative sweep for a
failure mode with no documented incident would be scope creep, not a fix.

Fail-loud (CLAUDE.md no-silent-fails): DescribeTags and the ledger/marker
reads RAISE on any error OTHER than genuine absence — a misread here would
either duplicate a relaunch or silently drop a real stall. The Telegram send
and the dispatcher re-invoke are the only best-effort/secondary surfaces
(exactly like sf-watch-liveness-probe).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
_FLOW_NAME = "ci-watch-liveness-probe"
_DB_BASENAME = "flow_doctor_ci_watch_liveness_probe"
_OPS_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)

# The dispatcher this checker relaunches through directly — bypassing the
# Overseer router and GHA entirely, mirroring sf-watch-liveness-probe's
# direct invoke of the spot dispatcher.
CI_WATCH_DISPATCHER_FUNCTION = os.environ.get(
    "CI_WATCH_DISPATCHER_FUNCTION", "alpha-engine-ci-watch-dispatcher"
)

WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")

CI_WATCH_SPOT_TAG_NAME = "alpha-engine-ci-watch-spot"
CI_WATCH_REPO_TAG_KEY = "ci-watch-repo"
CI_WATCH_SHA_TAG_KEY = "ci-watch-sha"
COMPLETION_MARKER_PREFIX = "ci_watch/_control/completed/"
DISPATCHED_RECORD_PREFIX = "ci_watch/_control/dispatched/"
RELAUNCH_LEDGER_PREFIX = "ci_watch/_control/relaunch/"

# ci-watch-dispatcher/index.py's DRILL_REPO — a drill's ENTIRE identity is
# synthesized in code there, never taken from a payload, so this constant can
# never collide with a real fleet repo. A drill box reclaimed mid-run is NOT a
# repair to retry: ci-watch-dispatcher deliberately skips writing a dispatch
# record for drills (see its _write_dispatch_record call site), so this
# checker would otherwise escalate LOUD on every drill death purely because
# nothing exists to reconstruct — noise, not signal. The correct alerting
# surface for a drill's mid-run death is its own missed
# consolidated/ci_watch/_canary/{date}.json heartbeat (config#2223), exactly
# mirroring sf-watch-liveness-probe's drill-isolation posture.
DRILL_REPO = "nousergon/ci-watch-drill"

RECLAIM_INTERRUPTION_DETAIL_TYPE = "EC2 Spot Instance Interruption Warning"
RECLAIM_STATE_CHANGE_DETAIL_TYPE = "EC2 Instance State-change Notification"
RECLAIM_DETAIL_TYPES = frozenset(
    {RECLAIM_INTERRUPTION_DETAIL_TYPE, RECLAIM_STATE_CHANGE_DETAIL_TYPE}
)


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _ec2_client():
    return boto3.client("ec2", region_name=REGION)


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _lambda_client():
    return boto3.client("lambda", region_name=REGION)


def _is_reclaim_event(event: dict) -> bool:
    return (
        isinstance(event, dict)
        and event.get("source") == "aws.ec2"
        and event.get("detail-type") in RECLAIM_DETAIL_TYPES
    )


def _instance_tags(instance_id: str) -> dict[str, str]:
    """Tags for ``instance_id`` via DescribeTags — still queryable for a while
    after termination. Raises on any API error (fail-loud: the tags are the
    PRIMARY input; an unreadable tag set must surface via the Lambda Errors
    metric, never silently classify a dead watch box as 'not ours')."""
    resp = _ec2_client().describe_tags(
        Filters=[{"Name": "resource-id", "Values": [instance_id]}]
    )
    return {t.get("Key", ""): t.get("Value", "") for t in resp.get("Tags", [])}


def _flat(repo: str, sha: str) -> str:
    return f"{repo.replace('/', '-')}-{sha}"


def _completion_key(repo: str, sha: str) -> str:
    return f"{COMPLETION_MARKER_PREFIX}{_flat(repo, sha)}.json"


def _dispatched_key(repo: str, sha: str) -> str:
    return f"{DISPATCHED_RECORD_PREFIX}{_flat(repo, sha)}.json"


def _relaunch_key(repo: str, sha: str) -> str:
    return f"{RELAUNCH_LEDGER_PREFIX}{_flat(repo, sha)}.json"


def _completion_marker_exists(s3, repo: str, sha: str) -> bool:
    """Only a true absence (404/NoSuchKey/NotFound) means 'no marker'; any
    OTHER error RAISES — misreading an S3 hiccup as absent would fire a
    duplicate relaunch, misreading it as present would silently drop a real
    stall (the config#2267 site-4 lesson, applied here as in
    sf-watch-liveness-probe)."""
    try:
        s3.head_object(Bucket=WATCH_BUCKET, Key=_completion_key(repo, sha))
        return True
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _read_json(s3, key: str) -> dict | None:
    """The object at ``key`` parsed as JSON, or None on a true absence
    (404/NoSuchKey). Any other read error RAISES. A present-but-unparseable
    object also returns None — the caller's None-handling escalates loud
    rather than guessing at a corrupted record (mirrors
    sf-watch-liveness-probe's `_load_watch_log`)."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    try:
        return json.loads(obj["Body"].read())
    except (ValueError, TypeError) as exc:
        logger.warning("unparseable object at %s: %s", key, exc)
        return None


def _record_relaunch(s3, repo: str, sha: str, dead_instance_id: str) -> None:
    """PRIMARY deliverable of the relaunch path — RAISES on failure, and runs
    BEFORE the dispatcher invoke: if this write fails, NO relaunch fires (the
    Lambda error pages via the watch-plane alarms) — the safe failure
    direction, since a relaunch without its ledger record would break the
    exactly-one bound and permit unbounded relaunches."""
    s3.put_object(
        Bucket=WATCH_BUCKET,
        Key=_relaunch_key(repo, sha),
        Body=json.dumps({
            "dead_instance_id": dead_instance_id,
            "relaunched_at": datetime.now(timezone.utc).isoformat(),
        }).encode("utf-8"),
        ContentType="application/json",
    )


def _escalate(text: str, dedup_key: str, context_info: dict) -> bool:
    """LOUD escalation (second death / untagged / unreconstructable = human
    needed). Best-effort delivery — a Telegram outage logs WARNING, never
    masks the finding (already in the CloudWatch log + returned verdict)."""
    try:
        return notify_via_flow_doctor(
            text,
            silent=False,
            severity="error",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context=context_info,
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still logged + returned
        logger.warning("ci-watch reclaim escalation Telegram send failed (non-fatal): %s", exc)
        return False


def _note(text: str, dedup_key: str, context_info: dict) -> bool:
    """Silent Telegram note for a successful bounded relaunch. Best-effort."""
    try:
        return notify_via_flow_doctor(
            text,
            silent=True,
            severity="info",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context=context_info,
            silent_topic=FleetTelegramTopic.OPS_HEALTH,
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; relaunch already fired + recorded
        logger.warning("ci-watch reclaim relaunch Telegram note failed (non-fatal): %s", exc)
        return False


def _handle_reclaim_event(event: dict) -> dict:
    detail = event.get("detail") or {}
    detail_type = str(event.get("detail-type") or "")
    instance_id = str(detail.get("instance-id") or "")
    if not instance_id:
        # Rule contract violation — the EventBridge patterns always carry an
        # instance-id. Fail loud rather than silently ignoring a malformed
        # event that might be a real watch-box death.
        raise ValueError(f"EC2 reclaim event without instance-id (detail-type={detail_type!r})")
    base = {"reclaim_event": True, "detail_type": detail_type, "instance_id": instance_id}

    if detail_type == RECLAIM_STATE_CHANGE_DETAIL_TYPE and str(detail.get("state") or "") != "terminated":
        logger.info("reclaim check: ignoring non-terminated state-change for %s", instance_id)
        return {**base, "handled": False, "reason": "not_terminated"}

    tags = _instance_tags(instance_id)
    if tags.get("Name") != CI_WATCH_SPOT_TAG_NAME:
        logger.info("reclaim check: %s is not a ci-watch box (Name=%r) — ignoring",
                    instance_id, tags.get("Name"))
        return {**base, "watch_box": False}

    repo = tags.get(CI_WATCH_REPO_TAG_KEY, "")
    sha = tags.get(CI_WATCH_SHA_TAG_KEY, "")
    if not (repo and sha):
        alerted = _escalate(
            "\U0001f6a8 *CI-Watch reclaim checker — UNTAGGED watch box died*\n"
            f"Watch box `{instance_id}` terminated without its repo/sha "
            "discriminator tags — cannot verify completion or relaunch. "
            "Check the ci-watch-dispatcher tag-write path (config#2267 site 2).",
            dedup_key=f"{_FLOW_NAME}:untagged:{instance_id}",
            context_info={"instance_id": instance_id, "tags": tags},
        )
        return {**base, "watch_box": True, "handled": False,
                "reason": "missing_discriminator_tags", "escalated": alerted}

    key_ctx = {"instance_id": instance_id, "repo": repo, "sha": sha}
    s3 = _s3_client()

    if repo == DRILL_REPO:
        completed = _completion_marker_exists(s3, repo, sha)
        logger.info(
            "reclaim check: drill box %s (%s@%s) %s — no relaunch/escalation "
            "for drills; the missed _canary heartbeat is the alerting surface "
            "(config#2223)", instance_id, repo, sha,
            "finished cleanly" if completed else "died WITHOUT a completion marker",
        )
        return {**base, "watch_box": True, "drill": True, "completed": completed,
                "relaunched": False}

    if _completion_marker_exists(s3, repo, sha):
        logger.info("reclaim check: %s finished cleanly (%s@%s)", instance_id, repo, sha)
        return {**base, "watch_box": True, "completed": True}

    # No completion marker: the box died mid-run.
    relaunch_record = _read_json(s3, _relaunch_key(repo, sha))
    if relaunch_record is not None:
        if relaunch_record.get("dead_instance_id") == instance_id:
            # The interruption WARNING and the terminated state-change both
            # fire for one reclaim — the second notification of the SAME
            # death is a duplicate, not a second death.
            logger.info("reclaim check: death of %s already handled — duplicate notification",
                        instance_id)
            return {**base, "watch_box": True, "completed": False, "duplicate_notification": True}
        # A DIFFERENT box already died and was relaunched for this
        # (repo, sha) — exactly-one bound spent, second death = human.
        alerted = _escalate(
            "\U0001f6a8 *CI-Watch reclaim checker — SECOND watch-box death*\n"
            f"{repo}@{sha[:12]}: relaunched box `{instance_id}` ALSO died "
            "without a completion marker (prior relaunch: "
            f"`{relaunch_record.get('dead_instance_id', '?')}`). The bounded "
            "relaunch budget is spent — human needed (config#3173).",
            dedup_key=f"{_FLOW_NAME}:second_death:{repo}:{sha}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": False,
                "reason": "second_death", "escalated": alerted}

    # First mid-run death for this (repo, sha): reconstruct the dispatch
    # fields from the record ci-watch-dispatcher wrote at launch time
    # (config#3173).
    dispatch_record = _read_json(s3, _dispatched_key(repo, sha))
    if dispatch_record is None:
        alerted = _escalate(
            "\U0001f6a8 *CI-Watch reclaim checker — watch box died mid-run, "
            "dispatch fields UNRECONSTRUCTABLE*\n"
            f"{repo}@{sha[:12]}: box `{instance_id}` died without a "
            f"completion marker, and no dispatch record was found at "
            f"`{_dispatched_key(repo, sha)}` to rebuild the relaunch from — "
            "relaunch manually (config#3173).",
            dedup_key=f"{_FLOW_NAME}:no_dispatch_record:{repo}:{sha}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": False,
                "reason": "no_dispatch_record", "escalated": alerted}

    # Record FIRST (exactly-one bound), then invoke — both fail-loud (the
    # invoke itself is the one best-effort step, matching sf-watch's posture:
    # the ledger write is what makes the bound durable even if the invoke
    # below never lands).
    _record_relaunch(s3, repo, sha, instance_id)
    payload = {
        "repo": dispatch_record.get("repo", repo),
        "sha": dispatch_record.get("sha", sha),
        "run_id": dispatch_record.get("run_id", ""),
        "run_url": dispatch_record.get("run_url", ""),
        "workflow": dispatch_record.get("workflow", ""),
        "branch": dispatch_record.get("branch", ""),
        "is_drill": "false",
    }
    try:
        _lambda_client().invoke(
            FunctionName=CI_WATCH_DISPATCHER_FUNCTION,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        invoked = True
    except Exception as exc:  # noqa: BLE001 — relaunch decision already recorded; invoke is best-effort
        invoked = False
        logger.error(
            "ci-watch relaunch invoke failed for %s@%s (relaunch already "
            "recorded — this will NOT retry itself; treat as an escalation): "
            "%s: %s", repo, sha, type(exc).__name__, exc,
        )
    logger.warning(
        "reclaim check: ci-watch box %s (%s@%s) died mid-run — relaunch %s",
        instance_id, repo, sha, "dispatched" if invoked else "invoke FAILED",
    )
    if invoked:
        noted = _note(
            "\U0001f6f0️ *CI-Watch reclaim checker — bounded relaunch*\n"
            f"Watch box `{instance_id}` ({repo}@{sha[:12]}) was reclaimed "
            "mid-repair — a fresh box was relaunched (attempt 1/1; a second "
            "death escalates loud, config#3173).",
            dedup_key=f"{_FLOW_NAME}:relaunch:{repo}:{sha}:{instance_id}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": True,
                "telegram_sent": noted}

    alerted = _escalate(
        "\U0001f6a8 *CI-Watch reclaim checker — relaunch invoke FAILED*\n"
        f"{repo}@{sha[:12]}: box `{instance_id}` died mid-run; the relaunch "
        "decision was recorded but invoking "
        f"`{CI_WATCH_DISPATCHER_FUNCTION}` failed — no fresh box was "
        "actually launched. Relaunch manually (config#3173).",
        dedup_key=f"{_FLOW_NAME}:invoke_failed:{repo}:{sha}",
        context_info=key_ctx,
    )
    return {**base, "watch_box": True, "completed": False, "relaunched": False,
            "reason": "invoke_failed", "escalated": alerted}


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Entrypoint. The only path this Lambda serves is the EC2 reclaim/
    termination event (source: aws.ec2) — see module docstring for why no
    scheduled sweep path exists here. Any other invocation (e.g. a manual
    smoke-test with payload {}) is a documented no-op."""
    if _is_reclaim_event(event or {}):
        return _handle_reclaim_event(event)
    logger.info("ci-watch-liveness-probe: non-reclaim invocation — no-op (event=%r)", event)
    return {"reclaim_event": False, "noop": True}
