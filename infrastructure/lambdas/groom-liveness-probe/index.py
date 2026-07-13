"""alpha-engine-groom-liveness-probe — external heartbeat for the EC2-spot groom.

The backlog groom (config#1432) self-reports its terminal state as an S3 run
artifact (``groom/{date}/{run_id}.json`` — config#1808 made this the PRIMARY
run record, written by every completed run: success, floor-breach,
crash-cascade, turn-budget-exceeded) plus a Telegram ping. That covers the
**loud** failure modes. It does NOT cover the **silent** ones — a spot reclaim
mid-run, the box OOMing or panicking before ``groom_run.sh`` installs its
reporting trap, an SSM command that never lands, the dispatcher Lambda
erroring, or the EventBridge schedule being broken/disabled (the 2026-06-29
dead-trigger class). In every silent mode NO terminal artifact is filed for a
scheduled run — and nothing notices.

This probe is the independent watchdog. It is schedule-aware: it knows when a
groom was *supposed* to run, and for each scheduled trigger that has had time to
finish, it asserts a run artifact was written inside that run's window.
A trigger with no terminal report → the box died silently or never launched →
LOUD Telegram alert (the one surface the groom's own self-report could not
reach). Per-trigger accounting (not just "latest artifact age") so a single silent
death masked by the next successful groom is still caught.

Mirrors the Fleet-SF Watch philosophy (nousergon/alpha-engine-config#1227) — an
external observer of a producer that cannot be trusted to report its own death —
applied to the groom, which (unlike the three fleet SFs) is not a Step Function
and so gets no EventBridge terminal-failure event. (SF-wrap follow-up tracked
separately; this is the "probe now" half.)

config#2037: this probe originally read GitHub issues labeled ``groom-digest``
instead of the S3 artifact directly. config#1808 (merged 2026-07-07) retired
the routine per-run ``groom-digest`` issue entirely, so every clean scheduled
trigger since then produced zero matching issues and the probe was on track to
false-alarm "SILENT FAILURE" on nearly every mature trigger; config#2033
retires the alarm-issue class too, making the GitHub signal permanently empty.
Switched the PRIMARY input to the S3 run-artifact prefix itself — the signal
this probe should have used from the start (no third-party API round-trip, and
it predates config#1808 too).

**Fail-loud (CLAUDE.md no-silent-fails).** Reading the run artifacts is the
PRIMARY input → an S3 list/read error RAISES so the check's absence surfaces via
the Lambda error metric + CW alarm (a silently-skipped liveness check is itself
the silent failure this guards against). A malformed individual artifact body is
logged and skipped (a content anomaly, not an infra failure the probe itself
should die on). The Telegram alert is the delivery surface; its failure is
logged + returned but does not raise (the missed-run finding is still in the
structured return + logs).
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
# S3 prefix the groom driver writes its per-run PRIMARY artifact under
# (groom_driver.py::write_run_artifact, config#1808): groom/{date}/{run_id}.json.
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
# clean-stop case: groom_driver.py writes a run artifact even on a total==0
# clean shutdown, so _missed()'s presence-in-window check (it never inspects
# artifact CONTENT) already treats that correctly as "not missed."
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
    """Enumerate every scheduled groom trigger in the lookback window that is now
    MATURE (had CEILING+MARGIN minutes to finish). Each → {at, label}."""
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


def _fetch_run_artifact_timestamps(s3, now: datetime) -> list[datetime]:
    """``run_start`` timestamps of recent groom S3 run artifacts
    (``groom/{date}/*.json`` — config#1808's PRIMARY run record, written by
    every completed run: success, floor-breach, crash-cascade,
    turn-budget-exceeded). PRIMARY input — a list/read (boto3 ClientError)
    failure RAISES uncaught (fail-loud); a malformed individual artifact body
    is logged and skipped (content anomaly, not an infra failure)."""
    horizon = now - timedelta(hours=LOOKBACK_HOURS)
    stamps: list[datetime] = []
    paginator = s3.get_paginator("list_objects_v2")
    day = horizon.date()
    last = now.date()
    while day <= last:
        prefix = f"{RUN_ARTIFACT_PREFIX}{day.isoformat()}/"
        for page in paginator.paginate(Bucket=WATCH_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                try:
                    body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
                    art = json.loads(body)
                except json.JSONDecodeError as exc:
                    logger.warning("could not parse run artifact %s (malformed JSON): %s", key, exc)
                    continue
                run_start = art.get("run_start", "")
                if not run_start:
                    continue
                try:
                    stamps.append(datetime.fromisoformat(run_start.replace("Z", "+00:00")))
                except ValueError:
                    logger.warning("run artifact %s has unparsable run_start=%r", key, run_start)
        day += timedelta(days=1)
    return stamps


def _missed(triggers: list[dict], stamps: list[datetime]) -> list[dict]:
    """A trigger is a MISS iff no run artifact's run_start falls inside its run
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
    a PRIMARY-input (S3 list/read) failure so the check can never silently no-op."""
    now = _now()
    triggers = _expected_triggers(now)
    logger.info("groom liveness probe: %d mature trigger(s) in last %dh", len(triggers), LOOKBACK_HOURS)
    if not triggers:
        return {"checked": 0, "missed": 0, "alerted": False, "reason": "no mature triggers in window"}

    s3 = _s3_client()
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
