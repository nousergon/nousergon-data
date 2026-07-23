"""alpha-engine-saturday-integrity-sentinel — Saturday-SF Watch, M4.

The **independent** Sat→Monday swallow safeguard for the autonomous resilience
agent (config#1227). The agent (M2c) reports what it fixed; this sentinel does
NOT trust that report. It reads the freshness-monitor's per-spec
``check_results.json`` — derived by probing S3 directly, independent of the
agent — computes the ``saturday_sf``-critical completeness itself, and answers
ONE question Monday pre-open:

    Are last Saturday's critical artifacts (signals, predictor weights manifest,
    constituents, training summary, …) actually present + fresh, so it is safe to
    trade on them today?

If NO (a swallow that left an artifact silently stale/missing makes the run look
green but the verdict incomplete), it pages LOUD. It is **non-blocking** — it
does not touch the weekday SF; it pages Brian + writes a dashboard marker so the
miss is caught before/at Monday open, within the Sat→Mon buffer.

**Why independent matters.** A swallow can fool the agent's own report but cannot
fool the freshness-monitor (which HEADs the artifacts in S3). Reading its results
is what makes this a real safeguard rather than a second self-report. It also
fires even when freshness-monitor *alerting* is in OBSERVE mode (check_results is
written regardless of the alert gate), so the Monday GO/NO-GO is never silenced.

**Fail-loud on uncertainty.** For a safety check, ambiguity = NO-GO: missing or
stale ``check_results.json`` (monitor down), or no saturday_sf-critical rows,
pages loud rather than assuming GO. The marker write is the primary deliverable
(RAISES on failure); Telegram is best-effort.
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
BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
# The per-spec freshness results (reliably produced every 15 min). We compute the
# saturday_sf-critical completeness HERE rather than depending on the derived
# `cycle_verdict.json`, which the deployed freshness-monitor does not emit
# (2026-06-25: only check_results/heartbeat/history exist in S3). Depending on a
# maybe-absent derived artifact would make this safety net false-alarm every
# Monday and train the operator to ignore it.
CHECK_RESULTS_KEY = os.environ.get(
    "CHECK_RESULTS_KEY", "_freshness_monitor/check_results.json"
)
MARKER_PREFIX = os.environ.get(
    "MARKER_PREFIX", "consolidated/saturday_integrity"
)
TARGET_CADENCE = "saturday_sf"
TARGET_SEVERITY = "critical"
_FLOW_NAME = "saturday-integrity-sentinel"
_DB_BASENAME = "flow_doctor_saturday_integrity_sentinel"
_OPS_HEALTH_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)
# Per-spec states that count as "present + safe to trade on" (mirrors the
# freshness substrate's cycle_completion, which filters to severity=critical).
_OK_STATES = frozenset({"fresh", "grace_period"})
# If check_results itself is older than this, the monitor may be down → treat as
# uncertain (NO-GO); don't trust a stale GO.
VERDICT_STALE_HOURS = float(os.environ.get("VERDICT_STALE_HOURS", "3"))


def _s3():
    return boto3.client("s3", region_name=REGION)


def _read_check_results(s3) -> dict | None:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=CHECK_RESULTS_KEY)
        return json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001 — absence IS a finding (NO-GO), handled by caller
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", "403"}:
            logger.warning("could not read %s: %s", CHECK_RESULTS_KEY, exc)
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
    """Return a GO/NO-GO verdict by computing saturday_sf-critical completeness
    from the freshness check_results rows. NO-GO on any incomplete artifact OR on
    uncertainty (results missing / monitor stale / no saturday_sf-critical rows)."""
    if doc is None:
        return {
            "go": False, "uncertain": True,
            "reason": "freshness check_results.json unavailable — cannot confirm Saturday integrity",
            "missing": [], "stale": [],
        }
    age = _verdict_age_hours(doc, now)
    if age is not None and age > VERDICT_STALE_HOURS:
        return {
            "go": False, "uncertain": True,
            "reason": f"freshness check_results is stale ({age:.1f}h old > {VERDICT_STALE_HOURS}h) — monitor may be down",
            "missing": [], "stale": [],
        }
    critical = [
        r for r in doc.get("results", [])
        if r.get("cadence") == TARGET_CADENCE and r.get("severity") == TARGET_SEVERITY
    ]
    if not critical:
        return {
            "go": False, "uncertain": True,
            "reason": f"no {TARGET_CADENCE}/{TARGET_SEVERITY} rows in check_results — registry coverage gap?",
            "missing": [], "stale": [],
        }
    bad = [r for r in critical if r.get("state") not in _OK_STATES]
    if not bad:
        return {
            "go": True, "uncertain": False,
            "reason": f"all {len(critical)} critical {TARGET_CADENCE} artifacts present",
            "missing": [], "stale": [],
        }
    missing = [r.get("artifact_id") for r in bad if r.get("state") == "missing"]
    stale = [r.get("artifact_id") for r in bad if r.get("state") != "missing"]
    return {
        "go": False, "uncertain": False,
        "reason": (
            f"{TARGET_CADENCE} INCOMPLETE "
            f"({len(critical) - len(bad)}/{len(critical)} critical artifacts present)"
        ),
        "missing": missing,
        "stale": stale,
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


def _notify(verdict: dict, key: str, run_date: str) -> bool:
    """LOUD on NO-GO; silent heartbeat on GO. Best-effort — never raises."""
    go = verdict["go"]
    uncertain = verdict.get("uncertain", False)
    if go:
        text = (
            "🟢 *Saturday Integrity — GO*\n"
            f"{verdict['reason']}\n"
            "_safe to trade on Saturday's outputs_"
        )
        silent = True
        severity = "info"
    else:
        flag = "⚠️ UNCERTAIN" if uncertain else "🔴 NO-GO"
        lines = [f"{flag} *Saturday Integrity — NO-GO*", verdict["reason"]]
        if verdict.get("missing"):
            lines.append(f"Missing: {', '.join(verdict['missing'])}")
        if verdict.get("stale"):
            lines.append(f"Stale: {', '.join(verdict['stale'])}")
        lines.append(f"Marker: s3://{BUCKET}/{key}")
        lines.append("_independent of the watch agent — a swallow can't hide here_")
        text = "\n".join(lines)
        silent = False
        severity = "warning" if uncertain else "error"
    try:
        return notify_via_flow_doctor(
            text,
            silent=silent,
            severity=severity,
            dedup_key=f"{_FLOW_NAME}:{run_date}:go={go}:uncertain={uncertain}",
            flow_name=_FLOW_NAME,
            topics=_OPS_HEALTH_TOPICS,
            db_basename=_DB_BASENAME,
            context={"run_date": run_date, "go": go, "uncertain": uncertain},
            silent_topic=FleetTelegramTopic.OPS_HEALTH,
            # No playbooks.yaml alert_classes row exists yet for this Lambda's
            # own identity (config-I3513 audit finding) — using _FLOW_NAME is
            # still strictly correct (matches this Lambda's own naming
            # convention, consistent with every other caller in this repo)
            # even though the event will currently classify as unregistered
            # in Overseer until a row is added. Follow-up filed to add one.
            source=_FLOW_NAME,
        )
    except Exception as exc:  # noqa: BLE001 — secondary observability
        logger.warning("integrity Telegram failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — runs Monday pre-open. Reads the freshness-monitor's
    saturday_sf completion verdict and pages a GO/NO-GO. Non-blocking."""
    now = datetime.now(timezone.utc)
    run_date = now.date().isoformat()
    s3 = _s3()

    doc = _read_check_results(s3)
    verdict = _evaluate(doc, now)
    verdict["checked_at"] = now.isoformat()

    key = _write_marker(s3, run_date, verdict)  # PRIMARY — fail-loud
    telegram_sent = _notify(verdict, key, run_date)       # secondary — best-effort

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
