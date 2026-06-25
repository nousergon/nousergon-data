"""alpha-engine-saturday-integrity-sentinel — Saturday-SF Watch, M4.

The **independent** Sat→Monday swallow safeguard for the autonomous resilience
agent (config#1227). The agent (M2c) reports what it fixed; this sentinel does
NOT trust that report. It reads the freshness-monitor's pre-computed
``saturday_sf`` cycle-completion verdict — which is derived by probing S3
directly, independent of the agent — and answers ONE question Monday pre-open:

    Are last Saturday's critical artifacts (signals, predictor weights manifest,
    constituents, training summary, …) actually present + fresh, so it is safe to
    trade on them today?

If NO (a swallow that left an artifact silently stale/missing makes the run look
green but the verdict incomplete), it pages LOUD. It is **non-blocking** — it
does not touch the weekday SF; it pages Brian + writes a dashboard marker so the
miss is caught before/at Monday open, within the Sat→Mon buffer.

**Why independent matters.** A swallow can fool the agent's own report but cannot
fool the freshness-monitor (which HEADs the artifacts in S3). Reading its verdict
is what makes this a real safeguard rather than a second self-report. It also
fires even when freshness-monitor *alerting* is in OBSERVE mode (the verdict is
computed regardless of the alert gate), so the Monday GO/NO-GO is never silenced.

**Fail-loud on uncertainty.** For a safety check, ambiguity = NO-GO: a missing or
stale ``cycle_verdict.json`` (monitor down) pages loud rather than assuming GO.
The marker write is the primary deliverable (RAISES on failure); Telegram is
best-effort.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3

from alpha_engine_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
CYCLE_VERDICT_KEY = os.environ.get(
    "CYCLE_VERDICT_KEY", "_freshness_monitor/cycle_verdict.json"
)
MARKER_PREFIX = os.environ.get(
    "MARKER_PREFIX", "consolidated/saturday_integrity"
)
TARGET_CADENCE = "saturday_sf"
# If the freshness verdict itself is older than this, the monitor may be down →
# treat as uncertain (NO-GO), don't trust a stale GO.
VERDICT_STALE_HOURS = float(os.environ.get("VERDICT_STALE_HOURS", "3"))


def _s3():
    return boto3.client("s3", region_name=REGION)


def _read_cycle_verdict(s3) -> dict | None:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=CYCLE_VERDICT_KEY)
        return json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001 — absence IS a finding (NO-GO), handled by caller
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", "403"}:
            logger.warning("could not read %s: %s", CYCLE_VERDICT_KEY, exc)
        return None


def _verdict_age_hours(doc: dict, now: datetime) -> float | None:
    run_at = doc.get("run_at")
    if not run_at:
        return None
    try:
        ts = datetime.fromisoformat(run_at)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (now - ts).total_seconds() / 3600.0
    except (ValueError, TypeError):
        return None


def _evaluate(doc: dict | None, now: datetime) -> dict:
    """Return a GO/NO-GO verdict dict. NO-GO on incomplete OR on any uncertainty
    (missing verdict / stale monitor / absent saturday_sf row)."""
    if doc is None:
        return {
            "go": False, "uncertain": True,
            "reason": "freshness cycle_verdict.json unavailable — cannot confirm Saturday integrity",
            "missing": [], "stale": [],
        }
    age = _verdict_age_hours(doc, now)
    if age is not None and age > VERDICT_STALE_HOURS:
        return {
            "go": False, "uncertain": True,
            "reason": f"freshness verdict is stale ({age:.1f}h old > {VERDICT_STALE_HOURS}h) — monitor may be down",
            "missing": [], "stale": [],
        }
    sat = next(
        (v for v in doc.get("verdicts", []) if v.get("cadence") == TARGET_CADENCE),
        None,
    )
    if sat is None:
        return {
            "go": False, "uncertain": True,
            "reason": f"no {TARGET_CADENCE} verdict in cycle_verdict.json",
            "missing": [], "stale": [],
        }
    if sat.get("complete"):
        return {
            "go": True, "uncertain": False,
            "reason": f"all {sat.get('n_required')} critical {TARGET_CADENCE} artifacts present",
            "cycle_label": sat.get("cycle_label"),
            "missing": [], "stale": [],
        }
    return {
        "go": False, "uncertain": False,
        "reason": (
            f"{TARGET_CADENCE} cycle INCOMPLETE "
            f"({sat.get('n_satisfied')}/{sat.get('n_required')} critical artifacts)"
        ),
        "cycle_label": sat.get("cycle_label"),
        "missing": sat.get("missing", []),
        "stale": sat.get("stale", []),
    }


def _write_marker(s3, run_date: str, verdict: dict) -> str:
    """Write the GO/NO-GO marker (dashboard surface). PRIMARY — RAISES on failure."""
    key = f"{MARKER_PREFIX}/{run_date}.json"
    body = json.dumps(
        {"run_date": run_date, "checked_at": verdict["checked_at"], **verdict},
        indent=2, default=str,
    ).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    return key


def _notify(verdict: dict, key: str) -> bool:
    """LOUD on NO-GO; silent heartbeat on GO. Best-effort — never raises."""
    go = verdict["go"]
    if go:
        text = (
            "🟢 *Saturday Integrity — GO*\n"
            f"{verdict['reason']}\n"
            "_safe to trade on Saturday's outputs_"
        )
        silent = True
    else:
        flag = "⚠️ UNCERTAIN" if verdict.get("uncertain") else "🔴 NO-GO"
        lines = [f"{flag} *Saturday Integrity — NO-GO*", verdict["reason"]]
        if verdict.get("missing"):
            lines.append(f"Missing: {', '.join(verdict['missing'])}")
        if verdict.get("stale"):
            lines.append(f"Stale: {', '.join(verdict['stale'])}")
        lines.append(f"Marker: s3://{BUCKET}/{key}")
        lines.append("_independent of the watch agent — a swallow can't hide here_")
        text = "\n".join(lines)
        silent = False
    try:
        return bool(send_message(text, disable_notification=silent))
    except Exception as exc:  # noqa: BLE001 — secondary observability
        logger.warning("integrity Telegram failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — runs Monday pre-open. Reads the freshness-monitor's
    saturday_sf completion verdict and pages a GO/NO-GO. Non-blocking."""
    now = datetime.now(timezone.utc)
    run_date = now.date().isoformat()
    s3 = _s3()

    doc = _read_cycle_verdict(s3)
    verdict = _evaluate(doc, now)
    verdict["checked_at"] = now.isoformat()

    key = _write_marker(s3, run_date, verdict)  # PRIMARY — fail-loud
    telegram_sent = _notify(verdict, key)       # secondary — best-effort

    logger.info(
        "Saturday integrity %s: %s (marker=%s telegram=%s)",
        "GO" if verdict["go"] else "NO-GO", verdict["reason"], key, telegram_sent,
    )
    return {
        "run_date": run_date,
        "go": verdict["go"],
        "uncertain": verdict["uncertain"],
        "reason": verdict["reason"],
        "marker_key": key,
        "telegram_sent": telegram_sent,
    }
