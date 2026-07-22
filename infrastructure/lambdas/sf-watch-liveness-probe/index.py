"""alpha-engine-sf-watch-liveness-probe — the ACTION half of the Fleet-SF Watch
watchdog: the mid-run spot-reclaim checker + the disabled-window dropped-failure
sweep.

SLIMMED (alpha-engine-config-I2831): the read-only WIRING-INTEGRITY checks that
used to live here (EventBridge rule / registered-SF-ARN / dead-state-machine /
dispatcher-Lambda-health / spot launch-config drift) moved to the registry-driven
alpha-engine-overseer-liveness-probe, which iterates
infrastructure/overseer/playbooks.yaml so the watch-plane surface is enumerated
in ONE place instead of per-probe constants. This Lambda retains ONLY the two
ACTION paths below — they have their own EC2-event trigger topology + 45 pinned
behavioral tests, so they were deliberately NOT migrated in that pass (a
follow-up tracks their eventual move). The Lambda name / EC2 reclaim rules /
scheduler cron are unchanged (renaming would re-point live EventBridge targets
for zero gain).

**Mid-run spot-reclaim checker (config#2270).** This Lambda is ALSO the
EventBridge target for `EC2 Spot Instance Interruption Warning` and
`EC2 Instance State-change Notification` (state=terminated) events — the
handler branches on event shape (the scheduled probe payload is `{}`; the EC2
events carry `source: aws.ec2`). A spot reclaim MID-REPAIR used to kill the
watch box with no completion marker, no failure marker, and no relaunch —
detection was the >=6.5h spot-orphan-reaper ping. The checker lives HERE
(rather than a new Lambda) per the issue's candidate-home evaluation: this is
already the external observer of the watch plane. Flow: the EC2 events carry
only an instance-id (no tags — the rules cannot be tag-scoped), so the
checker DescribeTags the instance (tags stay queryable post-termination for a
while) → non-watch boxes (`Name` != alpha-engine-sf-watch-spot) exit quietly
→ for a watch box it heads the S3 completion marker
`sf_watch/_control/completed/{cadence}-{pipeline}-{run_date}.json` (the same
key the spot-orphan-reaper derives) → marker present = clean run, exit;
marker ABSENT = the box died mid-run: re-invoke
alpha-engine-sf-watch-spot-dispatcher ONCE with the original dispatch fields
(reconstructed from the discriminator tags + the newest watch-log event) and
`force_on_demand: true` — the groom-SF bounded-relaunch pattern
(config#1645 / nousergon-data#658). Exactly-one bound: the relaunch decision
is recorded as an `action: reclaim_relaunch` watch-log event (which the
saturday dispatcher's config#2269 mechanical attempt ceiling counts) BEFORE
the invoke; a second death for the same (cadence, pipeline, run_date)
escalates LOUD instead of relaunching again (second death = human).

**Disabled-window dropped-failure sweep (config#2257).** Demonstrated live
2026-07-11 ~16:47 UTC: `watch-rerun-4` failed minutes before dispatch was
re-enabled — the trigger was declined ("disabled") and dropped FOREVER; the
operator had to notice and manually re-fire. A disabled window of ANY length
can eat exactly one critical failure event this way. Every scheduled probe
pass therefore sweeps the registered pipelines' LATEST execution: a
terminal-failed (FAILED/TIMED_OUT/ABORTED) execution with NO covering
watch-log event for its run_date — and dispatch currently ENABLED — gets a
synthesized failure event re-driven through
``alpha-engine-saturday-sf-watch-dispatcher`` (async invoke of
EXPECTED_TARGET_FUNCTION with the same EventBridge event shape the real rule
delivers). Design decisions, deliberate:
  * The sweep re-enters the SATURDAY dispatcher, NOT the spot dispatcher
    directly: the saturday dispatcher owns the watch-log write (which is what
    marks the execution COVERED, making the sweep exactly-once), the
    config#2269 mechanical attempt ceiling, and the config#2003/#1827
    suppression carve-outs (operator aborts, recovery reruns, post-escalation
    repeats). A direct spot-dispatcher invoke would bypass all three — an
    unbounded, suppression-blind re-dispatch loop.
  * Coverage = "any event in the run_date's watch-log carries this
    execution_arn" (membership, not last-event recency): robust to multiple
    failures per day. Known limitation, deliberate: a failure whose watch-log
    event WAS written but whose downstream spot launch was declined is
    "covered" here — that half of config#2257 (decline-aware re-fire) belongs
    to the dispatcher home, not this probe.
  * A freshness floor (_SWEEP_MIN_AGE_SECONDS) skips executions that
    terminated moments ago: the real-time EventBridge→dispatcher path may
    still be in flight, and racing it would double-dispatch one failure.
  * Gated on the spot dispatcher's live SF_WATCH_DISPATCH_ENABLED value the
    probe already reads: sweeping while dispatch is DISABLED would burn the
    one-shot coverage marker on a dispatch the spot leg then declines. The
    enable transition needs no hook — the first probe pass after re-enable
    sweeps the window's drops.

**Fail-loud (CLAUDE.md no-silent-fails).** Every AWS describe/list call here is
the PRIMARY input: an UNEXPECTED API error (anything other than the specific
"this resource doesn't exist" codes we're explicitly checking for) RAISES, so a
broken probe surfaces via the Lambda Errors metric — alarmed by the watch-plane
CloudWatch alarms provisioned in infrastructure/setup_watch_plane_alarms.sh
(the dead-probe backstop) — rather than silently skipping the one check that
verifies nothing else is silently broken.
The Telegram alert itself is a secondary delivery surface: its own failure is
logged + returned, not raised.
"""

from __future__ import annotations

import hashlib
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
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")
_FLOW_NAME = "sf-watch-liveness-probe"
_DB_BASENAME = "flow_doctor_sf_watch_liveness_probe"
_OPS_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)

# The saturday dispatcher the config#2257 sweep re-drives dropped failures
# through (its watch-log write is what marks an execution covered).
EXPECTED_TARGET_FUNCTION = os.environ.get(
    "SF_WATCH_FUNCTION_NAME", "alpha-engine-saturday-sf-watch-dispatcher"
)

WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")

# The spot dispatcher the reclaim checker relaunches through + whose live
# SF_WATCH_DISPATCH_ENABLED kill-switch the sweep gates on.
SPOT_DISPATCHER_FUNCTION = os.environ.get(
    "SF_WATCH_SPOT_DISPATCHER_FUNCTION", "alpha-engine-sf-watch-spot-dispatcher"
)

# ── Mid-run spot-reclaim checker (config#2270) ───────────────────────────────
# Tag names/keys mirror sf-watch-spot-dispatcher/index.py (SF_WATCH_TAG_NAME +
# the three SF_WATCH_*_TAG_KEY discriminators) and spot-orphan-reaper's
# WATCH_KINDS sf-watch entry — the same triple every watch-plane consumer keys
# on. The completion-marker key shape below is the reaper's `_completion_key`
# shape for the sf-watch kind.
SF_WATCH_SPOT_TAG_NAME = "alpha-engine-sf-watch-spot"
SF_WATCH_CADENCE_TAG_KEY = "sf-watch-cadence"
SF_WATCH_PIPELINE_TAG_KEY = "sf-watch-pipeline"
SF_WATCH_RUN_DATE_TAG_KEY = "sf-watch-run-date"
COMPLETION_MARKER_PREFIX = "sf_watch/_control/completed/"
RECLAIM_INTERRUPTION_DETAIL_TYPE = "EC2 Spot Instance Interruption Warning"
RECLAIM_STATE_CHANGE_DETAIL_TYPE = "EC2 Instance State-change Notification"
RECLAIM_DETAIL_TYPES = frozenset(
    {RECLAIM_INTERRUPTION_DETAIL_TYPE, RECLAIM_STATE_CHANGE_DETAIL_TYPE}
)

# ── Disabled-window dropped-failure sweep (config#2257) ──────────────────────
# {pipeline_name: watch_prefix} — MUST stay in lockstep with
# saturday-sf-watch-dispatcher's PIPELINES watch prefixes AND
# sf-watch-spot-dispatcher's _WATCH_PREFIXES (tests/
# test_sf_watch_defer_prefix_lockstep.py pins all three copies equal). The
# sweep reads the canonical watch-log at f"{prefix}/{run_date}.json" — the
# exact key saturday-sf-watch-dispatcher's _artifact_key mints — to decide
# whether a terminal execution is already covered.
_WATCH_PREFIXES: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "consolidated/saturday_sf_watch",
    "ne-preopen-trading-pipeline": "consolidated/weekday_sf_watch",
    "ne-postclose-trading-pipeline": "consolidated/eod_sf_watch",
    # alpha-engine-config-I2890 (2026-07-17): ne-weekly-advisory-pipeline and
    # ne-modelzoo-sunday-pipeline (added in lockstep 2026-07-14 per
    # I2544/I2545) were retired live (config#2890 re-inlined both back into
    # this Saturday SF) — removed here in the same lockstep-required move
    # together with saturday-sf-watch-dispatcher's PIPELINES and
    # sf-watch-spot-dispatcher's own _WATCH_PREFIXES copy (config#2937).
}
# The statuses the watch's EventBridge rule matches (deploy.sh EVENT_PATTERN);
# the saturday dispatcher itself applies the ABORTED operator-abort carve-out.
_SWEEP_TERMINAL_STATUSES = frozenset({"FAILED", "TIMED_OUT", "ABORTED"})
# Don't sweep an execution that terminated moments ago: the real-time
# EventBridge→dispatcher path (seconds, worst-case a few minutes including the
# dispatcher's DescribeExecution/GetExecutionHistory enrichment) may not have
# written its watch-log event yet, and racing it would dispatch TWICE for one
# failure. 15 min is far beyond real-time delivery while still well inside a
# single probe cadence.
_SWEEP_MIN_AGE_SECONDS = 900


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _sfn_client():
    return boto3.client("stepfunctions", region_name=REGION)


def _lambda_client():
    return boto3.client("lambda", region_name=REGION)


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _ec2_client():
    return boto3.client("ec2", region_name=REGION)


# ── Mid-run spot-reclaim checker (config#2270) ───────────────────────────────


def _is_reclaim_event(event: dict) -> bool:
    """True iff this invocation is an EC2 reclaim/termination EventBridge
    event rather than the scheduled probe (whose payload is ``{}``)."""
    return (
        isinstance(event, dict)
        and event.get("source") == "aws.ec2"
        and event.get("detail-type") in RECLAIM_DETAIL_TYPES
    )


def _instance_tags(instance_id: str) -> dict[str, str]:
    """Tags for ``instance_id`` via DescribeTags — still queryable for a
    while after termination, unlike the instance record itself. Raises on any
    API error (fail-loud: the tags are the PRIMARY input of the reclaim
    check; an unreadable tag set must surface via the Lambda Errors metric,
    never silently classify a dead watch box as 'not ours')."""
    resp = _ec2_client().describe_tags(
        Filters=[{"Name": "resource-id", "Values": [instance_id]}]
    )
    return {t.get("Key", ""): t.get("Value", "") for t in resp.get("Tags", [])}


def _completion_marker_exists(marker_key: str) -> bool:
    """HeadObject on the run's completion marker. Only a true absence
    (404/NoSuchKey/NotFound) means "no marker"; any OTHER error RAISES —
    misreading an S3 hiccup as 'absent' would fire a duplicate relaunch, and
    misreading it as 'present' would silently drop coverage (the config#2267
    site-4 lesson, applied to this read)."""
    try:
        _s3_client().head_object(Bucket=WATCH_BUCKET, Key=marker_key)
        return True
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _load_watch_log(s3, watch_log_key: str) -> dict | None:
    """The day's watch-log doc, or None when it truly doesn't exist (404).
    Any other read error RAISES (fail-loud — the exactly-one relaunch bound
    depends on this read). A present-but-unparseable doc also returns None:
    without a readable event history the checker can neither reconstruct the
    dispatch fields nor verify the exactly-one bound, and the None path below
    escalates LOUDLY instead of relaunching — that escalation is the
    recording surface for this swallow (failure mode swallowed: a corrupted
    watch-log blob; the reclaim finding itself still pages)."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=watch_log_key)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey"}:
            return None
        raise
    try:
        doc = json.loads(obj["Body"].read())
    except (ValueError, TypeError) as exc:
        logger.warning("watch-log %s unparseable during reclaim check: %s", watch_log_key, exc)
        return None
    if isinstance(doc, dict) and isinstance(doc.get("events"), list):
        return doc
    logger.warning("watch-log %s has an unexpected shape during reclaim check", watch_log_key)
    return None


def _record_reclaim_relaunch(s3, watch_log_key: str, doc: dict, record: dict) -> None:
    """Append the relaunch decision to the day's watch-log and write it back.
    PRIMARY deliverable of the relaunch path — RAISES on failure, and runs
    BEFORE the dispatcher invoke: if this write fails, NO relaunch fires (the
    Lambda error pages via the watch-plane alarms) — the safe failure
    direction, since a relaunch without its record would break the
    exactly-one bound and permit unbounded relaunches."""
    doc["updated_at"] = record["detected_at"]
    doc["events"].append(record)
    s3.put_object(
        Bucket=WATCH_BUCKET,
        Key=watch_log_key,
        Body=json.dumps(doc, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def _reclaim_escalate(text: str, dedup_key: str, context_info: dict) -> bool:
    """LOUD reclaim-path escalation (second death / unreconstructable dispatch
    fields = human needed). Best-effort delivery surface — a Telegram outage
    logs WARNING and is returned, never masks the finding (which is already
    in the CloudWatch log + returned verdict)."""
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
        logger.warning("reclaim escalation Telegram send failed (non-fatal): %s", exc)
        return False


def _reclaim_note(text: str, dedup_key: str, context_info: dict) -> bool:
    """Silent Telegram note for a successful bounded relaunch (mirrors the
    dispatcher's silent 'watch is acting' receipt). Best-effort."""
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
        logger.warning("reclaim relaunch Telegram note failed (non-fatal): %s", exc)
        return False


def _handle_reclaim_event(event: dict) -> dict:
    """Mid-run spot-reclaim checker (config#2270) — see module docstring."""
    detail = event.get("detail") or {}
    detail_type = str(event.get("detail-type") or "")
    instance_id = str(detail.get("instance-id") or "")
    if not instance_id:
        # Rule contract violation — the EventBridge patterns always carry an
        # instance-id. Fail loud rather than silently ignoring a malformed
        # event that might be a real watch-box death.
        raise ValueError(f"EC2 reclaim event without instance-id (detail-type={detail_type!r})")
    base = {"reclaim_event": True, "detail_type": detail_type, "instance_id": instance_id}

    # Defense in depth: the terminated rule's pattern filters state=terminated;
    # a stopping/running state-change reaching us anyway is not a death.
    if detail_type == RECLAIM_STATE_CHANGE_DETAIL_TYPE and str(detail.get("state") or "") != "terminated":
        logger.info("reclaim check: ignoring non-terminated state-change for %s", instance_id)
        return {**base, "handled": False, "reason": "not_terminated"}

    tags = _instance_tags(instance_id)
    if tags.get("Name") != SF_WATCH_SPOT_TAG_NAME:
        # Every instance in the account hits these rules (they cannot be
        # tag-scoped) — non-watch boxes exit quietly, log only.
        logger.info("reclaim check: %s is not an sf-watch box (Name=%r) — ignoring",
                    instance_id, tags.get("Name"))
        return {**base, "watch_box": False}

    cadence = tags.get(SF_WATCH_CADENCE_TAG_KEY, "")
    pipeline = tags.get(SF_WATCH_PIPELINE_TAG_KEY, "")
    run_date = tags.get(SF_WATCH_RUN_DATE_TAG_KEY, "")
    if not (cadence and pipeline and run_date):
        # A watch box died inside the narrow launch→tag window (or a tag write
        # regressed): no discriminators means no marker key and no dispatch
        # fields — a human must look. LOUD, never a quiet drop.
        alerted = _reclaim_escalate(
            "\U0001f6a8 *SF-Watch reclaim checker — UNTAGGED watch box died*\n"
            f"Watch box `{instance_id}` terminated without its cadence/pipeline/"
            "run-date discriminator tags — cannot verify completion or relaunch. "
            "Check the sf-watch-spot-dispatcher tag-write path (config#2267 site 2).",
            dedup_key=f"{_FLOW_NAME}:reclaim_untagged:{instance_id}",
            context_info={"instance_id": instance_id, "tags": tags},
        )
        return {**base, "watch_box": True, "handled": False,
                "reason": "missing_discriminator_tags", "escalated": alerted}

    key_ctx = {"instance_id": instance_id, "cadence": cadence,
               "pipeline": pipeline, "run_date": run_date}
    marker_key = f"{COMPLETION_MARKER_PREFIX}{cadence}-{pipeline}-{run_date}.json"

    # Canary-drill isolation (config#2223): a drill box's run_date tag is
    # ALWAYS "drill-YYYY-MM-DD" (synthesized by sf-watch-spot-dispatcher —
    # a real run_date is bare YYYY-MM-DD, so the two can never collide). A
    # drill is not a repair: its death — clean OR mid-run — must NEVER
    # consume the reclaim-relaunch budget, spend an on-demand relaunch, or
    # page "unreconstructable dispatch fields" (a drill writes no watch-log
    # at all, so that escalation would fire on every reclaimed drill). A
    # drill that died before writing its completion marker surfaces through
    # the DESIGNED canary channel instead: the missing
    # consolidated/*/_canary/{date}.json heartbeat escalates the Fleet
    # Status dot to YELLOW/RED (crucible-dashboard fleet_status.py).
    if run_date.startswith("drill-"):
        completed = _completion_marker_exists(marker_key)
        if completed:
            logger.info("reclaim check: drill box %s (%s/%s@%s) finished cleanly",
                        instance_id, cadence, pipeline, run_date)
        else:
            logger.warning(
                "reclaim check: drill box %s (%s/%s@%s) died WITHOUT a "
                "completion marker — no relaunch/escalation for drills; the "
                "missed _canary heartbeat is the alerting surface (config#2223)",
                instance_id, cadence, pipeline, run_date,
            )
        return {**base, "watch_box": True, "drill": True,
                "completed": completed, "relaunched": False}

    if _completion_marker_exists(marker_key):
        logger.info("reclaim check: %s finished cleanly (marker %s present)",
                    instance_id, marker_key)
        return {**base, "watch_box": True, "completed": True}

    # No completion marker: the box died MID-RUN.
    watch_log_key = f"consolidated/{cadence}_sf_watch/{run_date}.json"
    s3 = _s3_client()
    doc = _load_watch_log(s3, watch_log_key)
    events = (doc or {}).get("events", [])
    relaunches = [ev for ev in events if ev.get("action") == "reclaim_relaunch"]

    if any(ev.get("dead_instance_id") == instance_id for ev in relaunches):
        # The interruption WARNING and the terminated state-change both fire
        # for one reclaim — the second notification of the SAME death is a
        # duplicate, not a second death.
        logger.info("reclaim check: death of %s already handled — duplicate notification",
                    instance_id)
        return {**base, "watch_box": True, "completed": False,
                "duplicate_notification": True}

    if relaunches:
        # Exactly-one bound: a DIFFERENT box already died and was relaunched
        # for this (cadence, pipeline, run_date) — second death = human.
        alerted = _reclaim_escalate(
            "\U0001f6a8 *SF-Watch reclaim checker — SECOND watch-box death*\n"
            f"{cadence}/{pipeline}@{run_date}: relaunched box `{instance_id}` "
            "ALSO died without a completion marker (prior relaunch: "
            f"`{relaunches[-1].get('dead_instance_id', '?')}` → on-demand). "
            "The bounded relaunch budget is spent — human needed (config#2270).",
            dedup_key=f"{_FLOW_NAME}:reclaim_second_death:{pipeline}:{run_date}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": False,
                "reason": "second_death", "escalated": alerted}

    # First mid-run death for this key: reconstruct the dispatch fields. The
    # tags carry (cadence, pipeline, run_date); the execution context comes
    # from the NEWEST watch-log event carrying an execution_arn — the failure
    # this box was dispatched for (the dispatcher writes the log BEFORE
    # dispatching, so a dispatched box always has one... unless the log is
    # missing/corrupted, in which case escalate LOUD instead of guessing).
    source_ev = next((ev for ev in reversed(events) if ev.get("execution_arn")), None)
    if source_ev is None:
        alerted = _reclaim_escalate(
            "\U0001f6a8 *SF-Watch reclaim checker — watch box died mid-run, "
            "dispatch fields UNRECONSTRUCTABLE*\n"
            f"{cadence}/{pipeline}@{run_date}: box `{instance_id}` died without "
            f"a completion marker, and the watch-log `{watch_log_key}` has no "
            "usable event to rebuild the dispatch from — relaunch manually "
            "(config#2270).",
            dedup_key=f"{_FLOW_NAME}:reclaim_no_source:{pipeline}:{run_date}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": False,
                "reason": "no_source_event", "escalated": alerted}

    payload = {
        "pipeline_name": pipeline,
        "cadence_slug": cadence,
        "run_date": run_date,
        "execution_arn": source_ev.get("execution_arn", ""),
        "state_machine_arn": f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{pipeline}",
        "failed_state": source_ev.get("failed_state") or "",
        "cause": source_ev.get("cause") or "",
        "watch_log_key": watch_log_key,
        "is_preflight": "true" if source_ev.get("is_preflight") else "false",
        # The whole point: a spot reclaim already proved spot unreliable for
        # this run — relaunch straight to on-demand (lib >= 0.106.0
        # launch_with_fallback(force_on_demand=True), config#1645 pattern).
        "force_on_demand": "true",
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    record = {
        "detected_at": now_iso,
        # Counted by the saturday dispatcher's config#2269 mechanical attempt
        # ceiling (_BUDGET_CONSUMING_ACTIONS) — the relaunch consumes the same
        # shared budget as agent dispatches and fast-path reruns.
        "action": "reclaim_relaunch",
        "source": _FLOW_NAME,
        "dead_instance_id": instance_id,
        "reclaim_detail_type": detail_type,
        "cadence_slug": cadence,
        "pipeline": pipeline,
        "run_date": run_date,
        "execution_arn": source_ev.get("execution_arn", ""),
        "force_on_demand": True,
    }
    # Record FIRST (exactly-one bound), then invoke — both fail-loud.
    _record_reclaim_relaunch(s3, watch_log_key, doc, record)
    _lambda_client().invoke(
        FunctionName=SPOT_DISPATCHER_FUNCTION,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    logger.warning(
        "reclaim check: watch box %s (%s/%s@%s) died mid-repair — relaunch "
        "dispatched with force_on_demand", instance_id, cadence, pipeline, run_date,
    )
    noted = _reclaim_note(
        "\U0001f6f0️ *SF-Watch reclaim checker — bounded relaunch*\n"
        f"Watch box `{instance_id}` ({cadence}/{pipeline}@{run_date}) was "
        "reclaimed mid-repair — relaunched ON-DEMAND (attempt 1/1; a second "
        "death escalates loud, config#2270).",
        dedup_key=f"{_FLOW_NAME}:reclaim_relaunch:{pipeline}:{run_date}:{instance_id}",
        context_info=key_ctx,
    )
    return {**base, "watch_box": True, "completed": False, "relaunched": True,
            "telegram_sent": noted, "watch_log_key": watch_log_key}


# ── Disabled-window dropped-failure sweep (config#2257) — see module docstring


def _read_sweep_kill_switch() -> dict[str, str]:
    """Read ONLY the spot dispatcher's live SF_WATCH_DISPATCH_ENABLED value (the
    single input the config#2257 sweep gates on). This was previously a
    by-product of the wiring-check spot-leg inspection, which moved to
    overseer-liveness-probe (I2831). Fail-loud on an unexpected API error; a
    missing function reads as UNREADABLE — the sweep then treats it as disabled
    (there is nothing to dispatch through), matching the prior semantics."""
    try:
        cfg = _lambda_client().get_function_configuration(FunctionName=SPOT_DISPATCHER_FUNCTION)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) == "ResourceNotFoundException":
            return {"SF_WATCH_DISPATCH_ENABLED": "UNREADABLE(function missing)"}
        raise
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    return {"SF_WATCH_DISPATCH_ENABLED": env.get("SF_WATCH_DISPATCH_ENABLED", "unset(default:true)")}


def _sweep_dispatch_enabled(kill_switches: dict[str, str]) -> bool:
    """True iff the spot dispatcher's LIVE kill-switch permits dispatch —
    computed from the value _check_spot_dispatch_leg already read, applying
    the dispatcher's own semantics (unset defaults to true). An UNREADABLE
    value (function missing) reads as disabled: there is nothing to dispatch
    through, and the missing function is already a loud probe finding."""
    raw = str(kill_switches.get("SF_WATCH_DISPATCH_ENABLED") or "")
    return raw == "unset(default:true)" or raw.lower() == "true"


def _sweep_run_date(sfn, execution: dict) -> str:
    """Mirror saturday-sf-watch-dispatcher's `_run_date` derivation (execution
    input's run_date → execution startDate → today UTC) so the coverage check
    reads the SAME watch-log key the dispatcher would write for this event.
    DescribeExecution failure is a best-effort swallow (failure mode: the
    fallback date could differ from the dispatcher's input-derived one,
    risking ONE duplicate dispatch — bounded by the dispatcher's config#2269
    attempt ceiling; recording surface: the WARNING below), mirroring the
    dispatcher's own best-effort `_describe_execution` posture."""
    execution_arn = str(execution.get("executionArn") or "")
    resp = None
    try:
        resp = _sfn_client().describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001 — see docstring: bounded, recorded here
        logger.warning(
            "sweep: describe_execution failed for %s — deriving run_date from "
            "startDate instead: %s", execution_arn, exc,
        )
    if resp is not None:
        try:
            rd = json.loads(resp.get("input") or "{}").get("run_date")
        except (ValueError, TypeError):
            rd = None
        if isinstance(rd, str) and rd:
            return rd
    start = execution.get("startDate")
    if isinstance(start, datetime):
        return start.astimezone(timezone.utc).date().isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def _sweep_execution_covered(s3, watch_log_key: str, execution_arn: str) -> bool:
    """True iff ANY event in the run_date's watch-log carries this
    execution_arn (membership, not last-event recency — robust to multiple
    failures per day). A missing watch-log means nothing covered it. Reuses
    `_load_watch_log`: unexpected S3 errors RAISE (a misread here would either
    re-dispatch a covered failure or silently drop an uncovered one)."""
    doc = _load_watch_log(s3, watch_log_key)
    if doc is None:
        return False
    return any(str(ev.get("execution_arn") or "") == execution_arn for ev in doc.get("events", []))


def _sweep_dropped_failures(kill_switches: dict[str, str]) -> dict:
    """Sweep the registered pipelines' latest executions for terminal failures
    with no covering watch-log event (config#2257) — see module docstring for
    the design decisions. Per-pipeline errors are collected so one broken
    pipeline never blocks sweeping the others, then RAISED together at the
    end (fail-loud: a silently failing sweep is exactly the silent drop this
    sweep exists to close; already-fired dispatches are async and unaffected)."""
    if not _sweep_dispatch_enabled(kill_switches):
        logger.info(
            "sweep: SF_WATCH_DISPATCH_ENABLED=%r — sweep skipped (the first "
            "probe pass after re-enable sweeps the window's drops)",
            kill_switches.get("SF_WATCH_DISPATCH_ENABLED"),
        )
        return {"enabled": False, "swept": [], "skipped_recent": []}

    sfn = _sfn_client()
    s3 = _s3_client()
    lam = _lambda_client()
    now = datetime.now(timezone.utc)
    swept: list[dict] = []
    skipped_recent: list[str] = []
    errors: list[str] = []
    for pipeline, prefix in _WATCH_PREFIXES.items():
        sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{pipeline}"
        try:
            executions = sfn.list_executions(
                stateMachineArn=sm_arn, maxResults=1
            ).get("executions") or []
            if not executions:
                continue
            latest = executions[0]
            status = str(latest.get("status") or "")
            if status not in _SWEEP_TERMINAL_STATUSES:
                continue
            stop = latest.get("stopDate")
            if isinstance(stop, datetime) and (now - stop).total_seconds() < _SWEEP_MIN_AGE_SECONDS:
                # Real-time EventBridge→dispatcher delivery may still be in
                # flight — racing it would double-dispatch one failure. The
                # next probe pass re-checks (by then it is covered or truly
                # dropped).
                skipped_recent.append(pipeline)
                continue
            execution_arn = str(latest.get("executionArn") or "")
            run_date = _sweep_run_date(sfn, latest)
            watch_log_key = f"{prefix}/{run_date}.json"
            if _sweep_execution_covered(s3, watch_log_key, execution_arn):
                continue

            # Dropped failure: re-drive it through the saturday dispatcher
            # with the same event shape the real EventBridge rule delivers —
            # its watch-log write is what marks this execution covered
            # (exactly-once), and its suppression carve-outs + attempt
            # ceiling all apply (see module docstring).
            detail: dict[str, object] = {
                "executionArn": execution_arn,
                "stateMachineArn": sm_arn,
                "name": str(latest.get("name") or ""),
                "status": status,
            }
            start = latest.get("startDate")
            if isinstance(start, datetime):
                detail["startDate"] = int(start.timestamp() * 1000)
            if isinstance(stop, datetime):
                detail["stopDate"] = int(stop.timestamp() * 1000)
            lam.invoke(
                FunctionName=EXPECTED_TARGET_FUNCTION,
                InvocationType="Event",
                Payload=json.dumps({
                    "source": "aws.states",
                    "detail-type": "Step Functions Execution Status Change",
                    # Provenance marker — ignored by the dispatcher's handler,
                    # visible in its invocation log for forensics.
                    "sf_watch_sweep": {"source": _FLOW_NAME, "swept_at": now.isoformat()},
                    "detail": detail,
                }).encode("utf-8"),
            )
            logger.warning(
                "sweep: dropped %s execution %s (%s, run_date=%s) had NO "
                "covering event in %s — re-driven through %s (config#2257)",
                status, execution_arn, pipeline, run_date, watch_log_key,
                EXPECTED_TARGET_FUNCTION,
            )
            swept.append({
                "pipeline": pipeline,
                "execution_arn": execution_arn,
                "status": status,
                "run_date": run_date,
                "watch_log_key": watch_log_key,
            })
        except Exception as exc:  # noqa: BLE001 — collected, then RAISED below
            logger.error("sweep: %s failed: %s: %s", pipeline, type(exc).__name__, exc)
            errors.append(f"{pipeline}: {type(exc).__name__}: {exc}")
    if errors:
        raise RuntimeError(
            f"config#2257 dropped-failure sweep hit unexpected errors on "
            f"{len(errors)} pipeline(s) (swept {len(swept)} before/around "
            f"them): {'; '.join(errors)}"
        )
    return {"enabled": True, "swept": swept, "skipped_recent": skipped_recent}


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Entrypoint for the two ACTION paths this (slimmed) Lambda retains:

      * the mid-run spot-reclaim checker (config#2270) — when invoked by the EC2
        reclaim/termination EventBridge rules (event carries ``source: aws.ec2``);
      * the disabled-window dropped-failure sweep (config#2257) — on the
        scheduled invocation (payload ``{}``).

    The read-only WIRING checks that used to run here moved to the registry-driven
    alpha-engine-overseer-liveness-probe (alpha-engine-config-I2831). Raises on an
    unexpected AWS API failure so neither action path can silently no-op."""
    if _is_reclaim_event(event or {}):
        return _handle_reclaim_event(event)

    # Scheduled path: the config#2257 sweep only. It gates on the spot
    # dispatcher's live SF_WATCH_DISPATCH_ENABLED kill-switch — read it directly
    # (previously a by-product of the wiring-check spot-leg inspection that moved
    # to overseer-liveness-probe).
    kill_switches = _read_sweep_kill_switch()
    logger.info("sf-watch sweep: dispatch kill-switches: %s", kill_switches)
    sweep = _sweep_dropped_failures(kill_switches)

    return {"kill_switches": kill_switches, "sweep": sweep}
