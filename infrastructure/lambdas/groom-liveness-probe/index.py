"""alpha-engine-groom-liveness-probe — external heartbeat for the EC2-spot groom.

The backlog groom (config#1432) self-reports its terminal state: every run —
clean, floor-breach, or timeout — writes an S3 run artifact
(``groom/{date}/{run_id}.json``, config#1808) and pings Telegram. That covers
the **loud** failure modes. It does NOT cover the **silent** ones — a spot
reclaim mid-run, the box OOMing or panicking before ``groom_run.sh`` installs
its reporting trap, an SSM command that never lands, the dispatcher Lambda
erroring, or the EventBridge schedule being broken/disabled (the 2026-06-29
dead-trigger class). In every silent mode NO terminal artifact is written for a
scheduled run — and nothing notices.

This probe is the independent watchdog. It is schedule-aware: it knows when a
groom was *supposed* to run, and for each scheduled trigger that has had time to
finish, it asserts an S3 run artifact was written inside that run's window.
A trigger with no terminal report → the box died silently or never launched →
LOUD Telegram alert (the one surface the groom's own self-report could not
reach). Per-trigger accounting (not just "latest artifact age") so a single
silent death masked by the next successful groom is still caught.

Mirrors the Fleet-SF Watch philosophy (nousergon/alpha-engine-config#1227) — an
external observer of a producer that cannot be trusted to report its own death —
applied to the groom, which (unlike the three fleet SFs) is not a Step Function
and so gets no EventBridge terminal-failure event. (SF-wrap follow-up tracked
separately; this is the "probe now" half.)

**Fail-loud (CLAUDE.md no-silent-fails).** Listing/reading the S3 run artifacts
is the PRIMARY input → an S3 error RAISES so the check's absence surfaces via
the Lambda error metric + CW alarm (a silently-skipped liveness check is itself
the silent failure this guards against). The Telegram alert is the delivery
surface; its failure is logged + returned but does not raise (the missed-run
finding is still in the structured return + logs).

config#2414: this probe originally asserted a GitHub ``groom-digest``-labeled
issue was filed per trigger window. config#1808 retired that GitHub mechanism
in favor of the S3 run artifact as groom's PRIMARY record, which this probe
never picked up — every run false-alarmed as a silent failure regardless of
actual health. Switched to reading the S3 artifact directly (same source
``groom_driver.py``'s ``write_run_artifact`` already writes).

config#2667: ``_expected_triggers`` (the fixed-cron ``_DEFAULT_SCHEDULE``
enumeration below) only ever covered ``run_mode=full`` — the 3 daily
01:00/07:00/19:00 UTC slots. ``run_mode=sweep`` (the end-of-SF PR-sweep box,
config#2201) fires on a variable, event-driven trigger — "after a trading
pipeline finishes" — with NO fixed cron at all, so this probe had ZERO
awareness of it: a sweep box that never wrote a run artifact was invisible to
the only liveness check that exists (4 of 14 sweep cycles, 2026-07-11→15,
confirmed silently missing with no page). Rather than guess at a sweep
schedule (which would only re-create the same cron-vs-reality drift risk the
``_DEFAULT_SCHEDULE`` docstring already flags), this probe now ALSO reads the
dispatcher's own real dispatch-decision log
(``groom/decisions/{date}/*.json`` — schema_version 2, written by
scheduled-groom-dispatcher's ``_write_trigger_record``/``_write_skip_record``/
``_write_sweep_decision_record`` for EVERY trigger evaluation, full or sweep)
and treats every record with at least one ``launch=true`` decision as an
expected trigger, using the exact same maturity + per-trigger-window miss
logic already used for the fixed-cron full-mode schedule. The fixed-cron
check is kept running alongside it (belt-and-braces / redundant, since every
full-mode dispatch now ALSO appears in the decision log) rather than removed,
so a decision-log read failure degrades to the pre-existing cron-only
coverage rather than below it.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
# groom/decisions/{date}/*.json — the dispatcher's own dispatch-decision log
# (config#1432/#2201/#2667; scheduled-groom-dispatcher's _write_trigger_record
# / _write_skip_record / _write_sweep_decision_record). Same bucket as the run
# artifacts below.
DECISION_RECORD_PREFIX = os.environ.get("GROOM_DECISION_RECORD_PREFIX", "groom/decisions/")
_FLOW_NAME = "groom-liveness-probe"
_DB_BASENAME = "flow_doctor_groom_liveness_probe"
_OPS_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)
# De-dup state: ISO timestamps of trigger windows already alerted, so a standing
# miss isn't re-pinged on every probe run. S3 (not in-Lambda) because the probe is
# stateless across invocations. Generous lookback + this state = tolerant to
# schedule/ceiling changes (no fragile probe-time tuning needed for once-per-miss).
WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
STATE_KEY = os.environ.get("GROOM_LIVENESS_STATE_KEY", "consolidated/groom_liveness/alerted.json")
# S3 prefix groom_driver.py's write_run_artifact() writes to (config#1808):
# {RUN_ARTIFACT_PREFIX}{date}/{run_id}.json, date = run_start[:10] (UTC,
# YYYY-MM-DD). Same bucket as the dedup state above.
RUN_ARTIFACT_PREFIX = os.environ.get("GROOM_RUN_ARTIFACT_PREFIX", "groom/")
# A run's worst-case wall clock = the spot box hard-timeout watchdog
# (groom_spot_bootstrap.sh MAX_RUNTIME_SECONDS, 360 min) + slack for box boot +
# report latency. A trigger is only checked once now >= T + CEILING + MARGIN, so
# a still-legitimately-running groom never false-alarms.
CEILING_MIN = int(os.environ.get("GROOM_CEILING_MIN", "360"))
MARGIN_MIN = int(os.environ.get("GROOM_MARGIN_MIN", "45"))
# How far back to enumerate triggers. Must exceed the longest inter-groom gap +
# CEILING + MARGIN so a single silent death (otherwise masked by the next
# successful run's artifact) is still attributed to its own trigger window.
LOOKBACK_HOURS = int(os.environ.get("GROOM_LOOKBACK_HOURS", "30"))

# Groom schedule (UTC), MIRRORS the dispatcher's EventBridge Scheduler crons
# (scheduled-groom-dispatcher/deploy.sh SCHED_CRONS). `dows` are Python weekday
# ordinals: Mon=0 … Sun=6. Override via the GROOM_SCHEDULE env (JSON list of
# {hour, minute, dows}) if the dispatcher cadence changes — keep the two in sync.
# Uniform 3x/day, all 7 days, since 2026-07-02 (the 07:00 Sat-skip was dropped —
# no real contention with the weekly SF; see scheduled-groom-dispatcher/README.md).
# The 15:00 entry (config#1571) was ADDED 2026-07-02 — the dispatcher's Opus/
# complexity:high-only schedule existed since 2026-07-01 (config#1495 follow-up)
# but this probe never tracked it, a blind spot for that schedule's silent
# failures. No special-casing needed for its "empty complexity:high queue"
# clean-stop case: groom_driver.py writes an S3 run artifact even on a
# total==0 clean shutdown, so _missed()'s presence-in-window check (it never
# inspects artifact CONTENT) already treats that correctly as "not missed."
#   cron(0 1 * * ? *)  → 01:00 daily (Sonnet, complexity:high only — config#2409, moved off Opus 2026-07-13)
#   cron(0 7 * * ? *)  → 07:00 daily (Sonnet, complexity:mid only)
#   cron(0 19 * * ? *) → 19:00 daily (Haiku, complexity:low only)
# At CEILING_MIN=360/MARGIN_MIN=45 (6h45m) the windows can overlap slightly
# (01:00→07:45 vs 07:00 Sonnet; 19:00→01:45 vs next-day 01:00 high-only) — _missed
# attributes by trigger timestamp, not by exclusive window.
_DEFAULT_SCHEDULE = [
    {"hour": 1, "minute": 0, "dows": [0, 1, 2, 3, 4, 5, 6], "label": "01:00 daily (Sonnet high-only)"},
    {"hour": 7, "minute": 0, "dows": [0, 1, 2, 3, 4, 5, 6], "label": "07:00 daily (Sonnet mid-only)"},
    {"hour": 19, "minute": 0, "dows": [0, 1, 2, 3, 4, 5, 6], "label": "19:00 daily (Haiku low-only)"},
]


def _schedule() -> list[dict]:
    raw = os.environ.get("GROOM_SCHEDULE")
    if not raw:
        return _DEFAULT_SCHEDULE
    try:
        sched = json.loads(raw)
        assert isinstance(sched, list) and sched
        return sched
    except (ValueError, AssertionError) as exc:
        logger.warning("bad GROOM_SCHEDULE env (%s); using default", exc)
        return _DEFAULT_SCHEDULE


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _expected_triggers(now: datetime) -> list[dict]:
    """Enumerate every FIXED-CRON ``run_mode=full`` groom trigger
    (``_DEFAULT_SCHEDULE``/``GROOM_SCHEDULE``) in the lookback window that is
    now MATURE (had CEILING+MARGIN minutes to finish). Each → {at, label}.

    config#2667: this only ever covers the fixed-cron full-mode schedule — it
    has NO awareness of ``run_mode=sweep`` (event-driven, no cron) at all. Kept
    running as a belt-and-braces cross-check alongside
    ``_expected_triggers_from_decisions`` (see ``_all_expected_triggers``)
    rather than removed: a decision-log read failure there degrades to this
    function's pre-existing (in-window) coverage, never below it.
    """
    horizon = now - timedelta(hours=LOOKBACK_HOURS)
    mature_before = now - timedelta(minutes=CEILING_MIN + MARGIN_MIN)
    out: list[dict] = []
    # Walk each calendar date in [horizon-1d, now] so a trigger near the window
    # edge isn't dropped, then filter to [horizon, mature_before].
    day = (horizon - timedelta(days=1)).date()
    last = now.date()
    while day <= last:
        for entry in _schedule():
            if day.weekday() not in set(entry["dows"]):
                continue
            t = datetime(
                day.year, day.month, day.day,
                int(entry["hour"]), int(entry["minute"]),
                tzinfo=timezone.utc,
            )
            if horizon <= t <= mature_before:
                out.append({"at": t, "label": entry.get("label", f"{entry['hour']:02d}:{entry['minute']:02d}")})
        day += timedelta(days=1)
    out.sort(key=lambda d: d["at"])
    return out


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _decision_record_dates(now: datetime) -> list[str]:
    """UTC calendar dates spanning the lookback window — same shape as
    ``_lookback_dates`` (which covers RUN artifacts), applied here to the
    dispatcher's DECISION records (``groom/decisions/{date}/*.json``)."""
    horizon = now - timedelta(hours=LOOKBACK_HOURS)
    dates: list[str] = []
    d = horizon.date()
    last = now.date()
    while d <= last:
        dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


def _decision_launched(record: dict) -> bool:
    """True iff this decision record shows AT LEAST ONE launch=true decision.

    Handles both the top-level ``launched`` bool some records carry directly
    (a bare ``launch_decided`` sweep record, or a legacy single-decision
    shape) and the ``decisions: [...]`` list schema_version-2 records use
    (``_write_trigger_record``/``_write_skip_record``/
    ``_write_sweep_decision_record``), where any entry's ``launch`` (or
    ``launched``) key being true counts. A skip-only record (``decisions: []``
    or every entry ``launch: false``) is correctly NOT expected to have a run
    artifact — it's ignored, not flagged."""
    if record.get("launched") is True or record.get("launch") is True:
        return True
    decisions = record.get("decisions")
    if isinstance(decisions, list):
        for d in decisions:
            if not isinstance(d, dict):
                continue
            if d.get("launch") is True or d.get("launched") is True:
                return True
    return False


def _expected_triggers_from_decisions(s3, now: datetime) -> list[dict]:
    """config#2667: enumerate every MATURE expected trigger from the
    dispatcher's OWN dispatch-decision log
    (``groom/decisions/{date}/*.json``) rather than an assumed schedule — this
    is how sweep-mode (event-driven, no fixed cron) dispatches become visible
    to this probe at all, and it also naturally covers full-mode (the
    demand-all path writes the same records), so it is not sweep-only.

    Each record with at least one ``launch=true`` decision (see
    ``_decision_launched``) contributes one expected trigger at its
    ``decided_at`` timestamp — the same ``{at, label}`` shape
    ``_expected_triggers`` returns, so ``_missed`` treats both sources
    identically. A record with NO launch=true decision (a demand-gate skip, a
    concurrent-lane skip, an enumeration failure fail-closed skip) is
    correctly excluded — it was never expected to produce a run artifact.

    Best-effort on the READ: an individual malformed/unreadable record is
    skipped (logged) rather than raising — the fixed-cron
    ``_expected_triggers`` cross-check (see ``_all_expected_triggers``) is the
    fallback if the decision log itself is entirely unavailable. This mirrors
    ``_fetch_run_artifact_timestamps``'s malformed-artifact handling but is
    deliberately non-fatal here (unlike that PRIMARY input) because the fixed
    -cron schedule is a redundant, independent source for full-mode misses."""
    horizon = now - timedelta(hours=LOOKBACK_HOURS)
    mature_before = now - timedelta(minutes=CEILING_MIN + MARGIN_MIN)
    out: list[dict] = []
    for date in _decision_record_dates(now):
        prefix = f"{DECISION_RECORD_PREFIX}{date}/"
        token = None
        while True:
            kwargs = {"Bucket": WATCH_BUCKET, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            try:
                resp = s3.list_objects_v2(**kwargs)
            except Exception as exc:  # noqa: BLE001 — redundant source; fixed-cron cross-check remains
                logger.warning("decision-record list failed for prefix %s (%s) — "
                               "sweep-mode coverage degraded to fixed-cron this run", prefix, exc)
                break
            for obj in resp.get("Contents", []) or []:
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                try:
                    body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
                    record = json.loads(body)
                except Exception as exc:  # noqa: BLE001 — one bad record must not hide the rest
                    logger.warning("decision record %s unreadable (%s) — skipped", key, exc)
                    continue
                if not _decision_launched(record):
                    continue
                decided_at = record.get("decided_at")
                if not decided_at:
                    continue
                try:
                    t = datetime.fromisoformat(str(decided_at).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue
                if not (horizon <= t <= mature_before):
                    continue
                label = f"decision-log:{record.get('trigger', record.get('run_mode', 'unknown'))}"
                out.append({"at": t, "label": label})
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    out.sort(key=lambda d: d["at"])
    return out


def _all_expected_triggers(s3, now: datetime) -> list[dict]:
    """Union of the fixed-cron schedule (``_expected_triggers``) and the real
    dispatch-decision log (``_expected_triggers_from_decisions``) — the actual
    input ``handler()`` checks against. De-duplicated by ``at`` timestamp
    (a full-mode trigger legitimately appears in BOTH sources once
    scheduled-groom-dispatcher's demand-all path writes its decision record at
    the same instant the fixed cron fired) so it is never double-counted/
    double-alerted for the same trigger instant."""
    merged: dict[datetime, dict] = {}
    for trig in _expected_triggers(now):
        merged[trig["at"]] = trig
    for trig in _expected_triggers_from_decisions(s3, now):
        merged.setdefault(trig["at"], trig)
    return sorted(merged.values(), key=lambda d: d["at"])


def _load_alerted(s3, now: datetime) -> set[str]:
    """ISO trigger-timestamps already alerted (pruned to the lookback window).
    A missing object is the expected first-run case, NOT an error → empty set."""
    horizon = now - timedelta(hours=LOOKBACK_HOURS)
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=STATE_KEY)
        data = json.loads(obj["Body"].read())
        items = data.get("alerted", []) if isinstance(data, dict) else []
    except Exception as exc:  # noqa: BLE001 — absence expected; bad blob recoverable
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", "403"}:
            logger.warning("could not read liveness state %s: %s", STATE_KEY, exc)
        items = []
    out: set[str] = set()
    for iso in items:
        try:
            if datetime.fromisoformat(iso) >= horizon:
                out.add(iso)
        except (ValueError, TypeError):
            continue
    return out


def _save_alerted(s3, alerted: set[str]) -> None:
    """Persist the pruned alerted-set. Best-effort: a write failure only risks a
    duplicate alert next run (logged), never a missed finding — so it does NOT
    raise (the finding already surfaced via Telegram + the structured return)."""
    try:
        s3.put_object(
            Bucket=WATCH_BUCKET,
            Key=STATE_KEY,
            Body=json.dumps({"alerted": sorted(alerted)}, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — dedup state; failure only risks a dup ping
        logger.warning("could not persist liveness state %s: %s", STATE_KEY, exc)


def _lookback_dates(now: datetime) -> list[str]:
    """UTC calendar dates (YYYY-MM-DD) spanning [now - LOOKBACK_HOURS, now],
    inclusive — the set of S3 date-partitions that could hold a run artifact
    for a trigger still in the lookback window."""
    horizon = now - timedelta(hours=LOOKBACK_HOURS)
    dates: list[str] = []
    d = horizon.date()
    last = now.date()
    while d <= last:
        dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


def _fetch_run_artifact_timestamps(s3, now: datetime) -> list[datetime]:
    """``run_start`` timestamps of recent S3 groom run artifacts
    (``{RUN_ARTIFACT_PREFIX}{date}/{run_id}.json`` — same schema
    ``groom_driver.py``'s ``write_run_artifact`` writes, config#1808). PRIMARY
    input — RAISES on error (fail-loud); a malformed individual artifact is
    treated the same way (skipping it silently would let a genuinely-missed
    trigger hide behind a corrupt one).

    config#2414: replaces the retired GitHub ``groom-digest`` issue fetch —
    the driver stopped filing that issue and started writing this S3 artifact
    as the PRIMARY run record, but this probe kept checking the old signal."""
    stamps: list[datetime] = []
    for date in _lookback_dates(now):
        prefix = f"{RUN_ARTIFACT_PREFIX}{date}/"
        token = None
        while True:
            kwargs = {"Bucket": WATCH_BUCKET, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
                art = json.loads(body)
                run_start = art.get("run_start")
                if not run_start:
                    continue
                stamps.append(datetime.fromisoformat(run_start.replace("Z", "+00:00")))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    return stamps


def _missed(triggers: list[dict], stamps: list[datetime]) -> list[dict]:
    """A trigger is a MISS iff no run artifact's run_start fell inside its run
    window [T, T + CEILING + MARGIN]. Windows for the default schedule don't
    overlap, so attribution is 1:1."""
    window = timedelta(minutes=CEILING_MIN + MARGIN_MIN)
    misses: list[dict] = []
    for trig in triggers:
        t = trig["at"]
        if not any(t <= s <= t + window for s in stamps):
            misses.append(trig)
    return misses


def _alert(misses: list[dict]) -> bool:
    """LOUD Telegram alert — the surface the groom's own self-report can't reach.
    Best-effort: failure is logged + returned, does not raise (the finding is in
    the structured return)."""
    lines = [
        "\U0001f6f0️ *Groom Liveness Probe — SILENT FAILURE*",
        f"{len(misses)} scheduled groom run(s) filed NO terminal report "
        f"(no S3 run artifact under `{RUN_ARTIFACT_PREFIX}` in-window):",
    ]
    for m in misses:
        lines.append(f"• {m['label']} @ {m['at'].strftime('%Y-%m-%d %H:%M')}Z")
    lines.append(
        "_Box likely died silently (spot reclaim / OOM / pre-trap crash) or was "
        "never dispatched (schedule/dispatcher broken). Check the "
        "scheduled-groom-dispatcher logs + SSM command history._"
    )
    text = "\n".join(lines)
    dedup_key = f"{_FLOW_NAME}:miss:" + "|".join(m["at"].isoformat() for m in misses)
    try:
        return notify_via_flow_doctor(
            text,
            silent=False,
            severity="error",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context={"misses": len(misses)},
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still returned
        logger.warning("liveness alert Telegram send failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled (EventBridge) entrypoint. Returns a structured result; raises on
    a PRIMARY-input (S3) failure so the check can never silently no-op."""
    now = _now()
    s3 = _s3_client()
    # config#2667: union of the fixed-cron full-mode schedule AND the
    # dispatcher's own dispatch-decision log (covers sweep-mode, which has no
    # fixed cron at all — see _all_expected_triggers).
    triggers = _all_expected_triggers(s3, now)
    logger.info("groom liveness probe: %d mature trigger(s) in last %dh", len(triggers), LOOKBACK_HOURS)
    if not triggers:
        return {"checked": 0, "missed": 0, "alerted": False, "reason": "no mature triggers in window"}

    stamps = _fetch_run_artifact_timestamps(s3, now)  # PRIMARY — fail-loud
    misses = _missed(triggers, stamps)

    already = _load_alerted(s3, now)
    new_misses = [m for m in misses if m["at"].isoformat() not in already]

    alerted = False
    if new_misses:
        logger.warning(
            "groom liveness: %d NEW scheduled run(s) with NO terminal report (of %d mature): %s",
            len(new_misses), len(triggers), [m["at"].isoformat() for m in new_misses],
        )
        alerted = _alert(new_misses)
        # Record only on a successful alert so a delivery outage retries next run.
        if alerted:
            already |= {m["at"].isoformat() for m in new_misses}
            _save_alerted(s3, already)
    elif misses:
        logger.info("groom liveness: %d miss(es) already alerted — suppressed", len(misses))
    else:
        logger.info("groom liveness: all %d scheduled run(s) reported a terminal artifact", len(triggers))

    return {
        "checked": len(triggers),
        "missed": len(misses),
        "new_missed": len(new_misses),
        "missed_triggers": [m["at"].isoformat() for m in new_misses],
        "artifacts_seen": len(stamps),
        "alerted": alerted,
    }
