"""alpha-engine-usage-pace-alert — Telegram early-warning for Brian's Claude Max
weekly usage pace (alpha-engine-config#2043).

Compares live Claude Code / Max-20x usage against a straight-line pace through
the weekly reset window (resets Sunday 9pm PT) and alerts Telegram at two
independent tiers:

  - WARN: used_pct >= elapsed_pct - 2  (2 percentage points, absolute margin)
  - OVER: used_pct >= elapsed_pct      (same condition as the backlog groom's
    existing pace gate, alpha-engine-config's scripts/groom_budget.py,
    config#1348 — the difference here is this is a NOTIFICATION for Brian's
    awareness, not a control action; the groom gate independently still
    throttles the groom itself)

Both tiers are RISING-EDGE conditions: each alerts once per breach episode per
weekly window, then goes quiet, and re-arms if usage drops back under its own
line and later re-crosses it. State (`consolidated/usage_pace_alert/state.json`)
tracks only the last-observed breached/not-breached flag per tier — the window
resets both flags automatically once `window_start` advances to the next week.

**Fail-loud (CLAUDE.md no-silent-fails).** Reading the pacing config and the
weekly usage totals are the PRIMARY inputs -> a read/parse failure RAISES, so
the check's absence surfaces via the Lambda error metric + CW alarm (a
silently-skipped pacing check is itself the failure mode this guards against).
This is a deliberate departure from groom_budget.py's fail-safe-to-full-run
posture: for THIS consumer, silently falling back to a stale ceiling/anchor
would defeat the point of the SSoT and Brian would never know his alert
threshold had drifted. The Telegram send is the delivery surface only; its
failure is logged + returned but does not raise.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from krepis.usage_pacing import pace_check, reset_window

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
_FLOW_NAME = "usage-pace-alert"
_DB_BASENAME = "flow_doctor_usage_pace_alert"
_WARN_TOPICS = (FleetTelegramTopic.OPS_HEALTH,)
_OVER_TOPICS = (FleetTelegramTopic.CRITICAL, FleetTelegramTopic.OPS_HEALTH)

WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
# SSoT (config#2043) — written by alpha-engine-config's set_usage_pacing_config.py,
# also read by that repo's groom_budget.py and alpha-engine-dashboard view 36.
PACING_CONFIG_KEY = os.environ.get("PACING_CONFIG_KEY", "config/usage_pacing.json")
USAGE_PREFIX = "claude_code_usage/"
# Rising-edge state: {"window_start": iso, "warn_breached": bool, "over_breached": bool}.
# A single object (not a growing list) — only two ongoing conditions to track,
# not N independent per-run triggers like groom-liveness-probe's dedup set.
STATE_KEY = os.environ.get("USAGE_PACE_STATE_KEY", "consolidated/usage_pace_alert/state.json")

_PT = ZoneInfo("America/Los_Angeles")
WEEKLY_PERIOD = timedelta(days=7)

# 2 percentage points, absolute margin (NOT a ratio) — Brian-specified
# (Tuesday 9pm PT example: elapsed 48/168=28.6%, WARN fires at used >= 26.6%).
PACE_ALERT_MARGIN = 0.02


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _now_pt() -> datetime:
    return datetime.now(_PT).replace(tzinfo=None)


def _load_pacing_config(s3) -> tuple[float, datetime]:
    """(weekly_wet_ceiling, weekly_reset_anchor_pt) from the SSoT. PRIMARY input
    — RAISES on any read/parse failure (missing object, malformed JSON, missing
    keys). No fallback here: a silent fallback would defeat the SSoT."""
    obj = s3.get_object(Bucket=WATCH_BUCKET, Key=PACING_CONFIG_KEY)
    doc = json.loads(obj["Body"].read())
    ceiling = float(doc["weekly_wet_ceiling"])
    anchor = datetime.fromisoformat(doc["weekly_reset_anchor_pt"])
    return ceiling, anchor


def _parse_key(key: str) -> str | None:
    """Date string from either layout: {source}/{date}.json or {source}/{date}/{run}.json."""
    p = key[len(USAGE_PREFIX):].split("/")
    if len(p) == 2 and p[1].endswith(".json"):
        return p[1][:-5]
    if len(p) == 3 and p[2].endswith(".json"):
        return p[1]
    return None


def _read_weekly_wet(s3, window_start: datetime) -> float:
    """Sum WET (all sources) at/after the PT datetime ``window_start``,
    hour-precise. boto3 reimplementation of alpha-engine-config's
    groom_budget.py::read_weekly_wet (that one shells out to the aws CLI for
    its GHA-runner context; this one uses boto3 directly, matching how
    groom-liveness-probe doesn't share code with groom_budget.py either).
    PRIMARY input — RAISES on any S3/JSON failure."""
    paginator = s3.get_paginator("list_objects_v2")
    total = 0.0
    start_date = window_start.date().isoformat()
    for page in paginator.paginate(Bucket=WATCH_BUCKET, Prefix=USAGE_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            d = _parse_key(key)
            if not d or d < start_date:
                continue
            body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
            doc = json.loads(body) if body else {}
            for hr, models in (doc.get("by_hour") or {}).items():
                # exclude hours before the window's start hour on the boundary day
                if d == start_date and int(hr) < window_start.hour:
                    continue
                total += sum(r.get("wet", 0) for r in models.values())
    return total


def _load_state(s3, window_start: datetime) -> dict:
    """Prior tier flags for the CURRENT window; ``{}`` (both tiers unarmed) if
    absent, unreadable, or from a prior (now-elapsed) window — so both tiers
    naturally re-arm at each weekly reset. Best-effort: a read failure is
    treated as "no prior state" (worst case: one duplicate alert), never
    raises (this is not the primary breach signal, just dedup bookkeeping)."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=STATE_KEY)
        data = json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001 — absence expected; bad blob recoverable
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", "403"}:
            logger.warning("could not read pacing state %s: %s", STATE_KEY, exc)
        return {}
    if not isinstance(data, dict) or data.get("window_start") != window_start.isoformat():
        return {}
    return data


def _save_state(s3, window_start: datetime, warn_breached: bool, over_breached: bool) -> None:
    """Persist this run's observed flags. Best-effort: a write failure only
    risks a duplicate alert next run (logged), never a missed breach (the
    breach itself already surfaced via Telegram + the structured return)."""
    try:
        s3.put_object(
            Bucket=WATCH_BUCKET,
            Key=STATE_KEY,
            Body=json.dumps({
                "window_start": window_start.isoformat(),
                "warn_breached": warn_breached,
                "over_breached": over_breached,
            }, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — dedup state; failure only risks a dup ping
        logger.warning("could not persist pacing state %s: %s", STATE_KEY, exc)


def _fmt_common(window_start: datetime, elapsed_frac: float, used_frac: float,
                 wet: float, ceiling: float) -> str:
    return (
        f"Window since {window_start:%a %Y-%m-%d %H:%M} PT — "
        f"elapsed {elapsed_frac:.1%}, used {used_frac:.1%} "
        f"(WET {wet/1e6:,.0f}M / {ceiling/1e6:,.0f}M ceiling).\n"
        "_See the LLM Usage page (view 36) in the console for detail._"
    )


def _alert_warn(window_start: datetime, elapsed_frac: float, used_frac: float,
                 warn_threshold: float, wet: float, ceiling: float) -> bool:
    text = (
        "⚠️ *Claude Max usage — approaching weekly pace*\n"
        f"Used {used_frac:.1%} is within {PACE_ALERT_MARGIN:.0%} of the on-pace "
        f"line (threshold {max(warn_threshold, 0.0):.1%}).\n"
        + _fmt_common(window_start, elapsed_frac, used_frac, wet, ceiling)
    )
    dedup_key = f"{_FLOW_NAME}:warn:{window_start.isoformat()}"
    try:
        return notify_via_flow_doctor(
            text, silent=False, severity="warning", dedup_key=dedup_key,
            flow_name=_FLOW_NAME, topics=_WARN_TOPICS, db_basename=_DB_BASENAME,
            context={"tier": "warn", "used_frac": used_frac, "elapsed_frac": elapsed_frac},
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still returned
        logger.warning("WARN alert Telegram send failed (non-fatal): %s", exc)
        return False


def _alert_over(window_start: datetime, elapsed_frac: float, used_frac: float,
                 wet: float, ceiling: float) -> bool:
    text = (
        "\U0001f6a8 *Claude Max usage — OVER weekly pace*\n"
        f"used {used_frac:.1%} > elapsed {elapsed_frac:.1%} of the weekly reset "
        "window (same condition as the backlog groom's own pace gate).\n"
        + _fmt_common(window_start, elapsed_frac, used_frac, wet, ceiling)
    )
    dedup_key = f"{_FLOW_NAME}:over:{window_start.isoformat()}"
    try:
        return notify_via_flow_doctor(
            text, silent=False, severity="error", dedup_key=dedup_key,
            flow_name=_FLOW_NAME, topics=_OVER_TOPICS, db_basename=_DB_BASENAME,
            context={"tier": "over", "used_frac": used_frac, "elapsed_frac": elapsed_frac},
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still returned
        logger.warning("OVER alert Telegram send failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled (EventBridge) entrypoint. Returns a structured result; raises
    on a PRIMARY-input (pacing config / usage read) failure so the check can
    never silently no-op."""
    now_pt = _now_pt()
    s3 = _s3_client()

    ceiling, anchor = _load_pacing_config(s3)  # PRIMARY — fail-loud
    win_start, _next_reset = reset_window(now_pt, anchor, WEEKLY_PERIOD)
    wet = _read_weekly_wet(s3, win_start)  # PRIMARY — fail-loud
    used_frac = wet / ceiling if ceiling else 0.0

    status = pace_check(used_frac, now_pt, anchor, WEEKLY_PERIOD)
    elapsed_frac = status.elapsed_frac
    over_breached = status.exceeded  # used_frac >= elapsed_frac (config#1348's own condition)
    warn_threshold = elapsed_frac - PACE_ALERT_MARGIN
    warn_breached = elapsed_frac >= PACE_ALERT_MARGIN and used_frac >= warn_threshold

    prev = _load_state(s3, win_start)
    new_warn = warn_breached and not prev.get("warn_breached", False)
    new_over = over_breached and not prev.get("over_breached", False)

    alerted_warn = alerted_over = False
    if new_warn:
        logger.warning("usage-pace WARN: used %.1f%% vs threshold %.1f%%", used_frac * 100,
                        max(warn_threshold, 0.0) * 100)
        alerted_warn = _alert_warn(win_start, elapsed_frac, used_frac, warn_threshold, wet, ceiling)
    if new_over:
        logger.warning("usage-pace OVER: used %.1f%% vs elapsed %.1f%%", used_frac * 100,
                        elapsed_frac * 100)
        alerted_over = _alert_over(win_start, elapsed_frac, used_frac, wet, ceiling)

    _save_state(s3, win_start, warn_breached, over_breached)

    return {
        "window_start": win_start.isoformat(),
        "elapsed_frac": elapsed_frac,
        "used_frac": used_frac,
        "warn_threshold": max(warn_threshold, 0.0),
        "warn_breached": warn_breached,
        "over_breached": over_breached,
        "new_warn": new_warn,
        "new_over": new_over,
        "alerted_warn": alerted_warn,
        "alerted_over": alerted_over,
    }
