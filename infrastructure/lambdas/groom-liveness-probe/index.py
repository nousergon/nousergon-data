"""alpha-engine-groom-liveness-probe — external heartbeat for the EC2-spot groom.

The backlog groom (config#1432) self-reports its terminal state: a clean run
files a ``groom-digest`` issue, a loud failure/timeout files a ``groom-digest``
failure issue, and both ping Telegram. That covers the **loud** failure modes.
It does NOT cover the **silent** ones — a spot reclaim mid-run, the box OOMing
or panicking before ``groom_run.sh`` installs its reporting trap, an SSM command
that never lands, the dispatcher Lambda erroring, or the EventBridge schedule
being broken/disabled (the 2026-06-29 dead-trigger class). In every silent mode
NO terminal artifact is filed for a scheduled run — and nothing notices.

This probe is the independent watchdog. It is schedule-aware: it knows when a
groom was *supposed* to run, and for each scheduled trigger that has had time to
finish, it asserts a ``groom-digest`` issue was filed inside that run's window.
A trigger with no terminal report → the box died silently or never launched →
LOUD Telegram alert (the one surface the groom's own self-report could not
reach). Per-trigger accounting (not just "latest digest age") so a single silent
death masked by the next successful groom is still caught.

Mirrors the Fleet-SF Watch philosophy (nousergon/alpha-engine-config#1227) — an
external observer of a producer that cannot be trusted to report its own death —
applied to the groom, which (unlike the three fleet SFs) is not a Step Function
and so gets no EventBridge terminal-failure event. (SF-wrap follow-up tracked
separately; this is the "probe now" half.)

**Fail-loud (CLAUDE.md no-silent-fails).** Reading the digest issues is the
PRIMARY input → a GitHub/SSM error RAISES so the check's absence surfaces via the
Lambda error metric + CW alarm (a silently-skipped liveness check is itself the
silent failure this guards against). The Telegram alert is the delivery surface;
its failure is logged + returned but does not raise (the missed-run finding is
still in the structured return + logs).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

from alpha_engine_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
# De-dup state: ISO timestamps of trigger windows already alerted, so a standing
# miss isn't re-pinged on every probe run. S3 (not in-Lambda) because the probe is
# stateless across invocations. Generous lookback + this state = tolerant to
# schedule/ceiling changes (no fragile probe-time tuning needed for once-per-miss).
WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
STATE_KEY = os.environ.get("GROOM_LIVENESS_STATE_KEY", "consolidated/groom_liveness/alerted.json")
# Repo the groom files its digest / failure issues into.
DIGEST_REPO = os.environ.get("GROOM_DIGEST_REPO", "nousergon/alpha-engine-config")
DIGEST_LABEL = os.environ.get("GROOM_DIGEST_LABEL", "groom-digest")
# Shared fine-grained PAT (SecureString) — same param the Fleet-SF Watch uses;
# needs `issues:read` on DIGEST_REPO. Read at probe time only, never logged.
GITHUB_PAT_SSM_PARAM = os.environ.get(
    "GITHUB_PAT_SSM_PARAM", "/alpha-engine/saturday_sf_watch/github_pat"
)
# A run's worst-case wall clock = the spot box hard-timeout watchdog
# (groom_spot_bootstrap.sh MAX_RUNTIME_SECONDS, 360 min) + slack for box boot +
# report latency. A trigger is only checked once now >= T + CEILING + MARGIN, so
# a still-legitimately-running groom never false-alarms.
CEILING_MIN = int(os.environ.get("GROOM_CEILING_MIN", "360"))
MARGIN_MIN = int(os.environ.get("GROOM_MARGIN_MIN", "45"))
# How far back to enumerate triggers. Must exceed the longest inter-groom gap +
# CEILING + MARGIN so a single silent death (otherwise masked by the next
# successful run's digest) is still attributed to its own trigger window.
LOOKBACK_HOURS = int(os.environ.get("GROOM_LOOKBACK_HOURS", "30"))
_PAT_TIMEOUT_SEC = 15

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
# clean-stop case: groom_driver.py files a groom-digest issue even on a
# total==0 clean shutdown, so _missed()'s presence-in-window check (it never
# inspects digest CONTENT) already treats that correctly as "not missed."
#   cron(0 7 * * ? *)  → 07:00 daily
#   cron(0 15 * * ? *) → 15:00 daily (Opus, complexity:high only)
#   cron(0 23 * * ? *) → 23:00 daily
# The three windows [T, T+CEILING+MARGIN] never overlap at the default
# CEILING_MIN=360/MARGIN_MIN=45 (6h45m): 07:00+6:45=13:45 (< 15:00),
# 15:00+6:45=21:45 (< 23:00), 23:00+6:45=05:45 next day (< 07:00) — so
# per-trigger attribution stays 1:1 (see _missed's docstring).
_DEFAULT_SCHEDULE = [
    {"hour": 7, "minute": 0, "dows": [0, 1, 2, 3, 4, 5, 6], "label": "07:00 daily"},
    {"hour": 15, "minute": 0, "dows": [0, 1, 2, 3, 4, 5, 6], "label": "15:00 daily (Opus high-only)"},
    {"hour": 23, "minute": 0, "dows": [0, 1, 2, 3, 4, 5, 6], "label": "23:00 daily"},
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


def _get_github_pat() -> str:
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=GITHUB_PAT_SSM_PARAM, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _fetch_digest_timestamps(pat: str) -> list[datetime]:
    """Created-at timestamps of recent ``groom-digest``-labeled issues (success
    digests AND loud-failure issues both carry the label). PRIMARY input — RAISES
    on error (fail-loud)."""
    url = (
        f"https://api.github.com/repos/{DIGEST_REPO}/issues"
        f"?labels={DIGEST_LABEL}&state=all&sort=created&direction=desc&per_page=50"
    )
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {pat}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "groom-liveness-probe",
        },
    )
    with urllib.request.urlopen(req, timeout=_PAT_TIMEOUT_SEC) as resp:
        issues = json.loads(resp.read())
    stamps: list[datetime] = []
    for it in issues:
        created = it.get("created_at")
        if not created:
            continue
        stamps.append(datetime.fromisoformat(created.replace("Z", "+00:00")))
    return stamps


def _missed(triggers: list[dict], stamps: list[datetime]) -> list[dict]:
    """A trigger is a MISS iff no digest issue was created inside its run window
    [T, T + CEILING + MARGIN]. Windows for the default schedule don't overlap, so
    attribution is 1:1."""
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
        f"(no `{DIGEST_LABEL}` issue in-window):",
    ]
    for m in misses:
        lines.append(f"• {m['label']} @ {m['at'].strftime('%Y-%m-%d %H:%M')}Z")
    lines.append(
        "_Box likely died silently (spot reclaim / OOM / pre-trap crash) or was "
        "never dispatched (schedule/dispatcher broken). Check the "
        "scheduled-groom-dispatcher logs + SSM command history._"
    )
    try:
        return bool(send_message("\n".join(lines), disable_notification=False))
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still returned
        logger.warning("liveness alert Telegram send failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled (EventBridge) entrypoint. Returns a structured result; raises on
    a PRIMARY-input (GitHub/SSM) failure so the check can never silently no-op."""
    now = _now()
    triggers = _expected_triggers(now)
    logger.info("groom liveness probe: %d mature trigger(s) in last %dh", len(triggers), LOOKBACK_HOURS)
    if not triggers:
        return {"checked": 0, "missed": 0, "alerted": False, "reason": "no mature triggers in window"}

    pat = _get_github_pat()
    stamps = _fetch_digest_timestamps(pat)  # PRIMARY — fail-loud
    misses = _missed(triggers, stamps)

    s3 = _s3_client()
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
        logger.info("groom liveness: all %d scheduled run(s) reported a terminal digest", len(triggers))

    return {
        "checked": len(triggers),
        "missed": len(misses),
        "new_missed": len(new_misses),
        "missed_triggers": [m["at"].isoformat() for m in new_misses],
        "digests_seen": len(stamps),
        "alerted": alerted,
    }
