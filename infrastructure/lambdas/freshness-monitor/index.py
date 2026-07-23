"""alpha-engine-freshness-monitor — absence-driven S3 artifact monitor.

Phase 3 of the artifact-freshness-monitor arc (plan doc at
``~/Development/alpha-engine-docs/private/artifact-freshness-monitor-260527.md``).
Closes the silent absence-of-artifact bug class — the 2026-05-17→27
``pit_parity.json`` incident (load-bearing artifact silently absent
for 11 days), the 2026-05-18 factor-profiles orphan, and the
2026-05-23 missing-``signals.json`` incident are the sibling triggers.
SF Catch / flow-doctor / substrate-health-check are all *event-driven*
(failure → alert); this Lambda is the *absence-driven* complement
(silence → alert).

**Architecture.** EventBridge fires this Lambda every 15min. Per
invocation:

  1. Load the registry from S3
     (``s3://{REGISTRY_BUCKET}/{REGISTRY_KEY}``, YAML).
  2. Walk every spec. For each, call
     :func:`nousergon_lib.artifact_freshness.check_freshness`
     against the current ``now`` (UTC).
  3. Aggregate results into a single ``check_results.json`` artifact
     under ``_freshness_monitor/`` (the dashboard surface reads this).
  4. Emit a self-heartbeat at ``_freshness_monitor/heartbeat.json``
     — the monitor monitors itself; substrate-health-check daily
     watches the heartbeat.
  5. For misses past SLA (``state ∈ {missing, stale, probe_failed}``),
     route SNS via :func:`krepis.alerts.publish` (``telegram=False``) and
     Telegram via flow-doctor forum topics (config#1742 T2 /
     config#1747) with ``dedup_key=resolve_dedup_key(spec, now)`` — dedup
     collapses 4×/hour retries to one alert per cycle per artifact.
     **``severity=warning`` registry rows are console-only** (written to
     ``check_results.json``; no SNS/Telegram — see ARTIFACT_REGISTRY
     "dashboard-only" convention) — with two config-I3086 exceptions:
     a row listing the live champion arm in ``critical_while_champion_arm``
     is coerced to critical at probe time, and a warning row
     confirmed-missing for ``WARNING_ESCALATION_RUNS`` consecutive sweeps
     escalates to the critical page path.
  6. **OBSERVE-mode gate**: when env
     ``FRESHNESS_MONITOR_ENABLED`` is anything other than
     ``"true"`` (case-insensitive), alerts are suppressed but the
     check results and heartbeat are still emitted. Phase 6 cutover
     flips the env var via ``aws lambda update-function-configuration``
     without redeploying — mirrors the mnemon 0.7.0rc4 pattern.

**Never raises.** Lambda failures cannot be silent (this monitor IS
the silent-failure trap). The handler catches per-spec exceptions
and records them in ``check_results.json`` so a bad registry entry
doesn't take down the whole probe pass. The handler's own outer
exception path is a CW Logs-level surface.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from collections import defaultdict
from dataclasses import replace as dc_replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import yaml

from krepis.alerts import publish
from nousergon_lib.artifact_freshness import (
    ArtifactSpec,
    CheckResult,
    check_freshness,
    cycle_completion,
    resolve_current_cycle,
    resolve_dedup_key,
)
from nousergon_lib.trading_calendar import previous_trading_day
from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_FLOW_NAME = "freshness-monitor"
_DB_BASENAME = "flow_doctor_freshness_monitor"
_FRESHNESS_TELEGRAM_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)

# ── Configuration (env-driven so Phase 6 cutover is a single CLI flip) ──────

REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET", "alpha-engine-research")
REGISTRY_KEY = os.environ.get(
    "REGISTRY_KEY", "_freshness_monitor/ARTIFACT_REGISTRY.yaml"
)
HEARTBEAT_KEY = "_freshness_monitor/heartbeat.json"
CHECK_RESULTS_KEY = "_freshness_monitor/check_results.json"
HISTORY_KEY = "_freshness_monitor/history.json"
CYCLE_VERDICT_KEY = "_freshness_monitor/cycle_verdict.json"

# config#1297 — the general sweep moved from a 15-min cron to daily (Brian's
# 2026-06-27 directive: the 15-min sweep was unnecessary noise once the
# saturday_sf/run_calendar staleness models were fixed). These two artifacts
# stay on a 30-min weekday-market-hours mini-rule (separate EB cron, event={
# "mode": "intraday"}) so genuinely intraday monitoring isn't blinded by the
# daily cadence: `open_orders_latest` (market-hours order-book freshness) and
# `freshness_monitor_heartbeat` (the monitor's OWN dead-man's-switch artifact
# — its whole purpose is fast detection of a monitor outage, which a daily
# cadence would defeat).
INTRADAY_ARTIFACT_IDS = frozenset({"open_orders_latest", "freshness_monitor_heartbeat"})

# config#1240 — auto-remediation dispatch. The confirmed-miss path reads the
# per-artifact `recovery:` spec from the registry and DISPATCHES the named
# backfill primitive (SF start_execution / Lambda invoke) instead of (or, per
# the spec's mode, in addition to) only paging. The monitor is a pure
# reconciler — it never hardcodes per-artifact logic; it reads the declarative
# spec and drives to desired state.
#
# DEDUP. A still-missing artifact would otherwise re-dispatch on every 15-min
# poll until the heal lands and the next probe sees it fresh. We write an
# in-progress marker to S3 keyed by the SAME per-cycle dedup label the alert
# path uses (resolve_dedup_key). The marker's presence within a cooldown window
# suppresses re-dispatch for that (artifact, cycle-window). The marker lives
# under the already-granted `_freshness_monitor/` prefix (iam-policy.json
# S3WriteMonitorArtifacts) so no new write grant is required.
RECOVERY_MARKER_PREFIX = "_freshness_monitor/_recovery/"

# Cooldown (minutes) after a dispatch during which the same (artifact,
# cycle-window) is NOT re-dispatched. Sized longer than the 15-min poll so at
# least one full backfill attempt completes before a retry is considered, but
# short enough that a genuinely-failed heal is retried within the cycle. Env-
# tunable to mirror the OBSERVE-mode cutover-by-CLI-flip pattern.
RECOVERY_COOLDOWN_MINUTES = int(
    os.environ.get("RECOVERY_COOLDOWN_MINUTES", "120")
)

# Master gate for the dispatch side effect, independent of ALERTS_ENABLED so
# the cutover can stage: alerts can go live while dispatch stays in OBSERVE
# (log the would-dispatch, write no marker, call no AWS) until validated. Phase
# flips this via `aws lambda update-function-configuration` with no redeploy.
RECOVERY_DISPATCH_ENABLED = (
    os.environ.get("FRESHNESS_MONITOR_RECOVERY_ENABLED", "false").lower()
    == "true"
)

# config-I3282 — freshness-critical → overseer drain dispatch (phase 1;
# Brian directive 2026-07-22). Every CRITICAL page from a row whose declared
# response lane is NOT `remediation: operator` (and that has no `recovery:`
# heal of its own in flight) triggers ONE event-time alert-drain run through
# the overseer router, instead of the critical sitting in the intake queue
# until the next scheduled drain (10:00/22:00 UTC — today's four criticals
# waited ~10h). The drain agent consumes the whole queue, so a sweep that
# pages several criticals dispatches ONCE, and the router's alert-drain
# playbook + the executor's EC2 tag lock (concurrent_skip benign) guard
# overlap. Async Event invoke — the router owns escalation on executor
# failure (mirrors saturday-sf-watch-dispatcher's M2 posture).
#
# Rows with a `recovery:` block are EXCLUDED: their declared lane is the
# auto-backfill heal, and a drain on top of an in-flight heal is redundant.
# Rows with no declaration at all (only reachable via warning-escalation or
# probe_failed coercion — the PR-time completeness check requires a lane on
# every statically-critical row) DEFAULT to dispatch: a critical page nobody
# declared a response for is exactly the case that must not sit unactioned.
DRAIN_DISPATCH_ENABLED = (
    os.environ.get("FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED", "false").lower()
    == "true"
)
DRAIN_DISPATCH_MARKER_KEY = (
    "_freshness_monitor/_dispatch/last_drain_dispatch.json"
)
# One drain covers every critical in the queue at launch; the cooldown only
# bounds how often a PERSISTENTLY-critical sweep can relaunch one. Sized to
# the drain's own runtime ceiling (3h watchdog) would starve genuinely new
# incidents; 120min matches the recovery cooldown's retry philosophy.
DRAIN_DISPATCH_COOLDOWN_MINUTES = int(
    os.environ.get("DRAIN_DISPATCH_COOLDOWN_MINUTES", "120")
)
OVERSEER_DISPATCHER_FUNCTION = os.environ.get(
    "OVERSEER_DISPATCHER_FUNCTION", "alpha-engine-overseer-dispatcher"
)
DRAIN_PLAYBOOK = os.environ.get("FRESHNESS_DRAIN_PLAYBOOK", "alert-drain")

# CloudWatch namespace for the per-cycle completion rollup metric. Shares
# the substrate-health namespace so cycle-completion + substrate-row health
# graph together. Dimensioned by Cadence only (low-cardinality, alarm-able).
CW_NAMESPACE = "AlphaEngine/Substrate"

# Historical-mode lookback depth per cadence. Tunable via event payload
# (event["lookback"] = {"saturday_sf": 12, ...}). Defaults sized for ~3
# months of history at ~negligible cost: 51 artifacts × 50 cycles ≈
# 2,500 S3 HEAD requests per daily historical run ≈ $0.001/day.
_DEFAULT_LOOKBACK = {
    "saturday_sf": 12,
    "weekday_sf": 30,
    "eod_sf": 30,
    "continuous": 0,  # current-state probe covers continuous artifacts
}

# OBSERVE-mode gate. Plan §3 invariant 10 + §4 Phase 6 default. Anything
# other than literal "true" (case-insensitive) suppresses alerts. Check
# results + heartbeat are emitted regardless.
ALERTS_ENABLED = (
    os.environ.get("FRESHNESS_MONITOR_ENABLED", "false").lower() == "true"
)

# config-I3086 — dynamic severity + warning escalation. Two post-detection
# gaps surfaced by the 2026-07-20 stale-champion-feed incident (config-I3053):
# a row's declared severity is static while "hard-blocks downstream" is a
# dynamic property of the promoted champion arm, and a severity=warning miss
# is console-only forever no matter how long it persists.
#
# 1. Rows may declare `critical_while_champion_arm: [<arm>, ...]` — effective
#    severity is coerced to critical at probe time while the live champion
#    pointer (config/producer_champion.json, schema_version-1 field
#    `champion` — the same key crucible-executor's champion.py
#    load_champion_pointer reads) names a listed arm. A
#    pointer read failure coerces listed rows to critical too: fail toward
#    paging, never toward silence.
# 2. A severity=warning row confirmed-missing for WARNING_ESCALATION_RUNS
#    consecutive evaluated sweeps escalates to the critical page path. The
#    counter is carried in check_results.json (`consecutive_miss_runs`), so
#    no new state surface is introduced.
CHAMPION_POINTER_KEY = os.environ.get(
    "CHAMPION_POINTER_KEY", "config/producer_champion.json"
)
WARNING_ESCALATION_RUNS = int(os.environ.get("WARNING_ESCALATION_RUNS", "3"))

# config#2055 Gap 2 — key-deliverable extended-staleness escalation into the
# Decision Queue (Brian's 2026-07-21 Option-A ruling: the Lambda files the
# issue directly, mirroring overseer-dispatcher's `_file_p1`). A row opts in
# via the registry's `escalate_to_issue: true` flag (parsed as a parallel
# map, same pattern as `critical_while_champion_arm` — not a schema field on
# the frozen lib `ArtifactSpec`). WARNING_ESCALATION_RUNS (above) already
# promotes a persistent warning to the critical SNS/Telegram page after 3
# daily sweeps (~3 days) — this is a SEPARATE, much longer threshold for
# "nobody has acted on the page either": ~2 weeks of consecutive confirmed
# misses before a P1 lands on the Decision Queue. For an `event_driven` row
# (whose own freshness check ALWAYS short-circuits to fresh — see
# `check_freshness`'s event-driven short-circuit — so its own
# `consecutive_miss_runs` is always 0), the threshold is evaluated against
# its `liveness_via` ANCHOR's miss-streak instead; see
# `_escalate_stale_key_deliverables`.
ISSUE_ESCALATION_RUNS = int(os.environ.get("ISSUE_ESCALATION_RUNS", "14"))
ISSUES_REPO = os.environ.get("ISSUES_REPO", "nousergon/alpha-engine-config")
# Same SSM param overseer-dispatcher already reads (IAM-reuse convention) —
# no new secret, just a new grant on the existing parameter.
GH_PAT_SSM = os.environ.get(
    "GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
_ISSUE_TIMEOUT_SEC = int(os.environ.get("ISSUE_ESCALATION_TIMEOUT_SEC", "10"))

# ArtifactSpec field set — used to strip extra YAML keys (e.g., the
# top-level `defaults` shape carries `s3_bucket` which we want, but a
# future schema extension would otherwise pollute the constructor).
_SPEC_FIELDS = frozenset(
    {
        "artifact_id",
        "s3_bucket",
        "s3_key_template",
        "cadence",
        "sla_minutes_after_cron",
        "severity",
        "owner_repo",
        "created_at",
        "grace_period_cycles",
        "recovery_key_template",
        "calendar_aware",
        "interval_minutes",
        # Continuous run-calendar (nousergon-lib >= v0.73.0) — the single
        # source of truth for a continuous artifact's calendar-awareness
        # (trading_days / all_days / market_hours). Drives both the idle
        # short-circuit and a trading-day-aware freshness floor.
        "run_calendar",
        # Continuous active-window bound: active_hours_utc is the
        # market_hours session window (nousergon-lib >= v0.63.0). The
        # deprecated active_trading_days_only boolean (subsumed by
        # run_calendar) was removed in nousergon-lib v0.102.0 / config#1334;
        # unknown keys in the registry are stripped by the loader below, so
        # this is forward-safe regardless of the pinned lib version.
        "active_hours_utc",
        "produces",
        "depends_on",
        "liveness_via",
    }
)


# ── Registry loader ─────────────────────────────────────────────────────────


def _coerce_date(value: Any) -> date:
    """YAML ``safe_load`` already returns ``datetime.date`` for ISO date
    scalars; this is a defensive coercion for string inputs (e.g.,
    when the registry is hand-built in a test fixture)."""
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"created_at must be date or ISO-string, got {type(value).__name__}")


def load_registry(s3_client: Any, bucket: str, key: str) -> list[ArtifactSpec]:
    """Fetch the registry from S3 and parse into :class:`ArtifactSpec`
    instances. The YAML ``defaults`` block is merged into every entry
    (per-entry keys override defaults).

    Raises on YAML parse error or schema violation — the Lambda's outer
    handler logs + re-raises so the failure surfaces in CW Logs +
    Lambda error metrics (which Brian's existing CW alarms cover).
    The registry's PR-time validator (alpha-engine-config
    ``scripts/validate_artifact_registry.py``) is the prevent-it-at-PR
    chokepoint; this is the runtime defense.
    """
    return load_registry_with_recovery(s3_client, bucket, key)[0]


def load_registry_with_recovery(
    s3_client: Any, bucket: str, key: str
) -> tuple[list[ArtifactSpec], dict[str, dict], dict[str, list[str]],
           dict[str, bool], dict[str, str]]:
    """Like :func:`load_registry`, but also returns the per-artifact
    ``recovery:`` spec map (config#1240), the
    ``critical_while_champion_arm`` map (config-I3086), the
    ``escalate_to_issue`` map (config#2055 Gap 2), and the
    ``remediation:`` declared-response-lane map (config-I3282), all keyed
    by ``artifact_id``.

    ``ArtifactSpec`` is a frozen lib dataclass without a ``recovery``
    field (the monitor's dispatch concern is not the substrate's
    freshness concern), so the recovery block is parsed into a parallel
    map rather than threaded onto the spec; the champion-arm and
    escalate-to-issue blocks follow the same pattern. Artifacts without a
    block are simply absent from the respective map — the dispatch path
    treats a missing key as "no auto-remediation, page only"; the
    severity path treats it as "static severity only"; the escalation
    path treats it as "console/page-only, never files an issue".
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    data = yaml.safe_load(body)
    if not isinstance(data, dict) or not data.get("artifacts"):
        raise ValueError(f"registry at s3://{bucket}/{key} missing 'artifacts'")

    defaults = data.get("defaults", {}) or {}
    specs: list[ArtifactSpec] = []
    recovery_by_id: dict[str, dict] = {}
    critical_arms_by_id: dict[str, list[str]] = {}
    escalate_to_issue_by_id: dict[str, bool] = {}
    remediation_by_id: dict[str, str] = {}
    for entry in data["artifacts"]:
        merged = {**defaults, **entry}
        merged["created_at"] = _coerce_date(merged["created_at"])
        # Strip any extension fields (forward-compat with future schema).
        spec_kwargs = {k: v for k, v in merged.items() if k in _SPEC_FIELDS}
        spec = ArtifactSpec(**spec_kwargs)
        specs.append(spec)
        recovery = merged.get("recovery")
        if isinstance(recovery, dict):
            recovery_by_id[spec.artifact_id] = recovery
        arms = merged.get("critical_while_champion_arm")
        if isinstance(arms, list) and arms:
            critical_arms_by_id[spec.artifact_id] = [str(a) for a in arms]
        if merged.get("escalate_to_issue") is True:
            escalate_to_issue_by_id[spec.artifact_id] = True
        remediation = merged.get("remediation")
        if isinstance(remediation, str) and remediation:
            remediation_by_id[spec.artifact_id] = remediation
    return (specs, recovery_by_id, critical_arms_by_id,
            escalate_to_issue_by_id, remediation_by_id)


# ── Dynamic severity (config-I3086) ─────────────────────────────────────────


def _load_champion_arm(s3_client: Any) -> tuple[str | None, bool]:
    """Read the live champion pointer. Returns ``(arm, read_failed)``.

    ``read_failed=True`` on any read/parse problem — the caller coerces
    listed rows to critical in that case (fail toward paging, never
    toward silence).
    """
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=CHAMPION_POINTER_KEY)
        pointer = json.loads(obj["Body"].read())
        # schema_version-1 pointer key is `champion` (verified against the
        # live object AND crucible-executor champion.py's own read —
        # pointer["champion"]). The original I3086 patch read a
        # `champion_arm` key that never existed in the pointer schema.
        arm = pointer.get("champion")
        if isinstance(arm, str) and arm:
            return arm, False
        logger.warning(
            "champion pointer at %s has no usable `champion` field: %r",
            CHAMPION_POINTER_KEY, pointer,
        )
        return None, True
    except Exception as exc:  # noqa: BLE001 — read failure must not sink the pass
        logger.warning("champion pointer read failed (config-I3086): %s", exc)
        return None, True


def apply_dynamic_severity(
    s3_client: Any,
    specs: list[ArtifactSpec],
    critical_arms_by_id: dict[str, list[str]],
) -> tuple[list[ArtifactSpec], set[str]]:
    """Coerce effective severity to ``critical`` for rows whose
    ``critical_while_champion_arm`` names the live champion arm
    (config-I3086). Returns ``(specs, coerced_ids)``.

    Root incident: ``research_free_backfill`` was correctly
    ``severity=warning`` at registration (observational backfill); the
    2026-07-13 champion promotion silently made it a hard-block live
    trade feed and nothing re-derived severity — its confirmed miss
    stayed console-only until the weekday order pipeline hard-failed
    (config-I3053).
    """
    if not critical_arms_by_id:
        return specs, set()
    arm, read_failed = _load_champion_arm(s3_client)
    out: list[ArtifactSpec] = []
    coerced: set[str] = set()
    for spec in specs:
        arms = critical_arms_by_id.get(spec.artifact_id)
        if arms and spec.severity != "critical" and (read_failed or arm in arms):
            logger.info(
                "dynamic severity (config-I3086): %s %s→critical "
                "(champion_arm=%s%s)",
                spec.artifact_id, spec.severity, arm,
                "; pointer unreadable — fail-loud coercion" if read_failed else "",
            )
            out.append(dc_replace(spec, severity="critical"))
            coerced.add(spec.artifact_id)
        else:
            out.append(spec)
    return out, coerced


def _load_prev_miss_counts(s3_client: Any) -> dict[str, int]:
    """Previous sweep's per-artifact ``consecutive_miss_runs`` counters,
    read back from ``check_results.json`` (config-I3086 warning
    escalation). Missing/malformed prior results reset all counters —
    surfaced as a ::warning, never fatal."""
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=CHECK_RESULTS_KEY)
        data = json.loads(obj["Body"].read())
        return {
            row["artifact_id"]: int(row.get("consecutive_miss_runs", 0))
            for row in data.get("results", [])
            if isinstance(row, dict) and row.get("artifact_id")
        }
    except Exception as exc:  # noqa: BLE001 — counter loss degrades to reset, not failure
        logger.warning(
            "previous check_results read failed — escalation counters reset "
            "(config-I3086): %s", exc,
        )
        return {}


def _load_prev_issue_filed(s3_client: Any) -> dict[str, str]:
    """Previous sweep's per-artifact filed-issue URLs (config#2055 Gap 2),
    read back from ``check_results.json``. A present entry means an
    escalation P1 was already filed for this artifact's CURRENT incident —
    dedup source of truth, so a still-stale row doesn't re-file every day.
    Missing/malformed prior results degrade to "nothing filed yet" (an
    extra issue on next threshold-cross is a much smaller cost than a
    counter read failure silently suppressing a real escalation forever)."""
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=CHECK_RESULTS_KEY)
        data = json.loads(obj["Body"].read())
        return {
            row["artifact_id"]: row["issue_filed_url"]
            for row in data.get("results", [])
            if isinstance(row, dict) and row.get("artifact_id") and row.get("issue_filed_url")
        }
    except Exception as exc:  # noqa: BLE001 — read failure degrades to "nothing filed yet"
        logger.warning(
            "previous check_results read failed — issue-filed markers reset "
            "(config#2055): %s", exc,
        )
        return {}


def _is_confirmed_miss(result: CheckResult) -> bool:
    """The same confirmed-miss shape the alert path fires on: an
    alerting state past its SLA grace (probe_failed has no grace)."""
    if result.state not in _ALERTING_STATES:
        return False
    return result.state == "probe_failed" or result.sla_violated_by_minutes > 0


# ── Per-spec probe (catches per-spec errors so one bad row doesn't sink the pass) ─


def _check_one(
    s3_client: Any, spec: ArtifactSpec, now: datetime
) -> tuple[CheckResult, Exception | None]:
    """Wrap :func:`check_freshness` with a per-spec exception trap.

    Returns ``(result, None)`` on success or
    ``(synthesized_probe_failed_result, exc)`` on a per-spec error
    (e.g., a malformed key template that fails ``str.format``,
    a transient network blip the substrate didn't classify).
    """
    try:
        return check_freshness(s3_client, spec, now), None
    except Exception as exc:  # noqa: BLE001 — per-spec resilience
        result = CheckResult(
            state="probe_failed",
            reason=f"per-spec exception: {type(exc).__name__}: {exc}",
            canonical_key=spec.s3_key_template,
        )
        return result, exc


# ── Aggregation + S3 emission ───────────────────────────────────────────────


def _serialize_check_results(
    pairs: list[tuple[ArtifactSpec, CheckResult]], now: datetime,
    miss_counts: dict[str, int] | None = None,
    coerced_ids: set[str] | None = None,
    issue_filed_by_id: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    """Build the ``check_results.json`` payload — one row per spec for
    the dashboard surface (Phase 5). ``miss_counts``/``coerced_ids``
    (config-I3086) persist the warning-escalation counters and mark rows
    whose severity was dynamically coerced, so the dashboard can explain
    a row paging as critical while the registry declares warning.
    ``issue_filed_by_id`` (config#2055 Gap 2) persists the extended-
    staleness escalation's dedup marker — the URL of the P1 filed for
    this artifact's current incident, or ``None``/absent if none is
    in flight."""
    miss_counts = miss_counts or {}
    coerced_ids = coerced_ids or set()
    issue_filed_by_id = issue_filed_by_id or {}
    rows = []
    for spec, result in pairs:
        rows.append(
            {
                "artifact_id": spec.artifact_id,
                "owner_repo": spec.owner_repo,
                "severity": spec.severity,
                "severity_dynamic": spec.artifact_id in coerced_ids,
                "consecutive_miss_runs": miss_counts.get(spec.artifact_id, 0),
                "cadence": spec.cadence,
                "canonical_key": result.canonical_key,
                "state": result.state,
                "reason": result.reason,
                "last_modified": (
                    result.last_modified.isoformat()
                    if result.last_modified is not None
                    else None
                ),
                "sla_violated_by_minutes": result.sla_violated_by_minutes,
                "recovery_substituted": result.recovery_substituted,
                "issue_filed_url": issue_filed_by_id.get(spec.artifact_id),
            }
        )
    return {
        "run_at": now.isoformat(),
        "alerts_enabled": ALERTS_ENABLED,
        "n_entries": len(rows),
        "results": rows,
    }


def _serialize_heartbeat(
    pairs: list[tuple[ArtifactSpec, CheckResult]],
    now: datetime,
    started_at_epoch: float,
) -> dict[str, Any]:
    """Build the ``heartbeat.json`` payload. Plan §3 invariant 9: the
    monitor monitors itself; substrate-health-check daily SSM watches
    this artifact's freshness. Self-registered in
    ``ARTIFACT_REGISTRY.yaml`` as ``freshness_monitor_heartbeat``."""
    counts: dict[str, int] = {
        "fresh": 0,
        "stale": 0,
        "missing": 0,
        "probe_failed": 0,
        "grace_period": 0,
    }
    for _spec, result in pairs:
        counts[result.state] = counts.get(result.state, 0) + 1

    return {
        "last_run": now.isoformat(),
        "alerts_enabled": ALERTS_ENABLED,
        "duration_seconds": round(time.time() - started_at_epoch, 3),
        "n_entries_checked": len(pairs),
        "counts": counts,
    }


def _put_json(s3_client: Any, bucket: str, key: str, payload: dict) -> None:
    body = json.dumps(payload, indent=2, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


# ── Per-cycle completion rollup (L249 consumer) ─────────────────────────────


def _serialize_cycle_verdicts(
    pairs: list[tuple[ArtifactSpec, CheckResult]], now: datetime
) -> dict[str, Any]:
    """Roll the per-artifact probe results up into one completion verdict
    per execution cycle, via :func:`cycle_completion`.

    The registry walk covers EVERY cadence in a single 15-min pass, so the
    grouping by ``(cadence, cycle_label)`` is mandatory — a single rollup
    over the mixed-cadence ``pairs`` would conflate the Saturday, weekday,
    EOD and continuous cycles into one meaningless verdict. ``weekday_sf``
    and ``eod_sf`` share a date-shaped label, so the cadence is part of the
    group key to keep them distinct.

    ``cycle_completion`` itself filters to ``severity="critical"`` rows; a
    group whose cadence has no critical artifacts rolls up vacuously
    complete (``n_required=0``).
    """
    groups: dict[tuple[str, str], list[tuple[ArtifactSpec, CheckResult]]] = (
        defaultdict(list)
    )
    for spec, result in pairs:
        _, label = resolve_current_cycle(spec, now)
        groups[(spec.cadence, label)].append((spec, result))

    verdicts = []
    for (cadence, label), grp in sorted(groups.items()):
        v = cycle_completion(grp, cycle_label=label)
        verdicts.append(
            {
                "cadence": cadence,
                "cycle_label": label,
                "state": v.state,
                "complete": v.complete,
                "n_required": v.n_required,
                "n_satisfied": v.n_satisfied,
                "missing": v.missing,
                "stale": v.stale,
                "probe_failed": v.probe_failed,
                "grace_period": v.grace_period,
                "reason": v.reason,
            }
        )
    return {"run_at": now.isoformat(), "verdicts": verdicts}


def _emit_cycle_metrics(cw_client: Any, verdict_payload: dict[str, Any]) -> None:
    """Emit one ``ArtifactFreshnessCycleComplete`` datapoint per cadence
    (1.0 complete / 0.0 not), in :data:`CW_NAMESPACE`.

    Dimensioned by ``Cadence`` ONLY — a stable, low-cardinality set
    (``{saturday_sf, weekday_sf, eod_sf, continuous}``) that a CW alarm can
    bind to. The per-cycle ``cycle_label`` is recorded in the S3 artifact,
    NOT a metric dimension: a label is high-cardinality (a new value every
    week/day) and would make the metric both unalarmable and costly.
    """
    metric_data = [
        {
            "MetricName": "ArtifactFreshnessCycleComplete",
            "Dimensions": [{"Name": "Cadence", "Value": v["cadence"]}],
            "Value": 1.0 if v["complete"] else 0.0,
            "Unit": "Count",
        }
        for v in verdict_payload["verdicts"]
    ]
    if metric_data:
        # Cadence set is ≤4 → one call, well under CW's 1000-metric cap.
        cw_client.put_metric_data(Namespace=CW_NAMESPACE, MetricData=metric_data)


def _emit_cycle_verdict_error(stage: str) -> None:
    """Emit one ``ArtifactFreshnessCycleVerdictError`` datapoint (Value=1.0) so a
    swallowed cycle-verdict rollup failure has an alarmable recording surface
    (config#1236) — not only the absence/staleness of ``cycle_verdict.json``.

    Dimensioned by ``Stage`` (``serialize_or_s3_write`` / ``cw_metric_emit``) to
    locate the failing step. Best-effort: the emit itself is trapped so this
    error-signal path can never sink the monitor (and a missing PutMetricData
    grant — the very thing it might be reporting — won't raise here).
    """
    try:
        boto3.client("cloudwatch").put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "ArtifactFreshnessCycleVerdictError",
                    "Dimensions": [{"Name": "Stage", "Value": stage}],
                    "Value": 1.0,
                    "Unit": "Count",
                }
            ],
        )
    except Exception as exc:  # noqa: BLE001 — error-signal emit must never raise
        logger.warning(
            "failed to emit ArtifactFreshnessCycleVerdictError[%s] (non-fatal): %s",
            stage, exc,
        )


# ── Auto-remediation dispatch (config#1240) ─────────────────────────────────
#
# Promote the monitor from alert-only to alert+heal. On a confirmed miss past
# grace, an artifact carrying a `recovery:` spec gets its backfill primitive
# DISPATCHED. The monitor reads the spec; it is NEVER hardcoded per artifact.


# (resolve_current_cycle is imported at module top alongside the other
# substrate entry points.)


def _resolve_recovery_params(
    params: dict[str, Any] | None, spec: ArtifactSpec, now: datetime
) -> dict[str, Any]:
    """Resolve ``{date}``/``{trading_day}``/``{cycle_label}`` placeholders in
    a recovery spec's ``params`` against the CURRENT MISS's cycle.

    The miss is for *this* cycle, so the backfill must target this cycle's
    trading day — NOT "today" (a Saturday-cron miss probed Monday must still
    backfill the Saturday cycle). We reuse the substrate's cycle resolution so
    the date the backfill targets is exactly the date the probe checked.
    Non-string param values pass through untouched.
    """
    if not params:
        return {}
    cycle_tick, cycle_label = resolve_current_cycle(spec, now)
    iso = cycle_tick.date().isoformat()
    resolved: dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(v, str):
            resolved[k] = v.format(date=iso, trading_day=iso, cycle_label=cycle_label)
        else:
            resolved[k] = v
    return resolved


def _recovery_marker_key(spec: ArtifactSpec, now: datetime) -> str:
    """In-progress dedup marker key for the current (artifact, cycle-window).

    Keyed by the SAME per-cycle label the alert dedup uses, so a backfill is
    dispatched at most once per cycle per artifact regardless of how many
    15-min polls observe the still-missing artifact before the heal lands.
    """
    _, label = resolve_current_cycle(spec, now)
    return f"{RECOVERY_MARKER_PREFIX}{spec.artifact_id}/{label}.json"


def _recovery_already_dispatched(
    s3_client: Any, spec: ArtifactSpec, now: datetime
) -> bool:
    """True if a dispatch marker for this (artifact, cycle) exists AND is
    within the cooldown window — i.e. a recovery is already in-flight and the
    artifact simply hasn't reappeared yet, so we must NOT re-dispatch.

    A marker older than the cooldown is treated as stale (the prior heal
    evidently failed) and dispatch is allowed again. A HEAD failure other than
    404 is treated as "assume dispatched" (fail-closed) so a transient S3 blip
    can't trigger a dispatch storm.
    """
    key = _recovery_marker_key(spec, now)
    try:
        resp = s3_client.head_object(Bucket=REGISTRY_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001 — classify by error code
        code = str(
            getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        )
        status = getattr(exc, "response", {}).get("ResponseMetadata", {}).get(
            "HTTPStatusCode", 0
        )
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False  # no marker → first dispatch for this cycle
        # Any other error (403/500/network) → fail-closed: assume in-flight.
        logger.warning(
            "recovery marker HEAD for %s failed (%s) — assuming dispatched "
            "to avoid a re-dispatch storm",
            spec.artifact_id, exc,
        )
        return True
    lm = resp.get("LastModified")
    if lm is None:
        return True
    age_min = (now - lm).total_seconds() / 60.0
    return age_min < RECOVERY_COOLDOWN_MINUTES


def _write_recovery_marker(
    s3_client: Any, spec: ArtifactSpec, now: datetime, payload: dict[str, Any]
) -> None:
    """Persist the in-progress marker so subsequent polls dedup against it."""
    key = _recovery_marker_key(spec, now)
    _put_json(s3_client, REGISTRY_BUCKET, key, payload)


def _dispatch_recovery(
    aws_clients: dict[str, Any],
    spec: ArtifactSpec,
    recovery: dict[str, Any],
    now: datetime,
) -> None:
    """Dispatch the recovery primitive named by the spec.

    ``type: step_function`` → ``stepfunctions.start_execution`` with the
    resolved params JSON as input. ``type: lambda`` → ``lambda.invoke``
    (Event/async) with the resolved params as the payload. Lazily-created
    clients are cached in ``aws_clients`` so a pass dispatching several
    recoveries shares one client per service.
    """
    rtype = recovery.get("type")
    target = recovery.get("target")
    resolved_params = _resolve_recovery_params(recovery.get("params"), spec, now)

    if rtype == "step_function":
        sf = aws_clients.get("stepfunctions")
        if sf is None:
            sf = boto3.client("stepfunctions")
            aws_clients["stepfunctions"] = sf
        sf.start_execution(
            stateMachineArn=target,
            input=json.dumps(resolved_params, default=str),
        )
    elif rtype == "lambda":
        lam = aws_clients.get("lambda")
        if lam is None:
            lam = boto3.client("lambda")
            aws_clients["lambda"] = lam
        lam.invoke(
            FunctionName=target,
            InvocationType="Event",  # async fire-and-forget; the next probe verifies
            Payload=json.dumps(resolved_params, default=str).encode("utf-8"),
        )
    else:
        raise ValueError(f"unknown recovery.type={rtype!r} for {spec.artifact_id}")


def _maybe_dispatch_recovery(
    s3_client: Any,
    aws_clients: dict[str, Any],
    spec: ArtifactSpec,
    recovery: dict[str, Any] | None,
    result: CheckResult,
    now: datetime,
) -> bool:
    """Auto-remediation entry point — mirror of :func:`_maybe_alert`.

    Returns ``True`` iff a dispatch was actually performed this pass. Fires
    only when ALL of:
      - a ``recovery:`` spec exists for the artifact;
      - the same confirmed-miss gate the alert path uses holds
        (``state ∈ {missing, stale}`` past SLA — ``probe_failed`` is NOT
        auto-healed: a broken probe means the monitor is blind, not that the
        artifact is absent, so blind-dispatching a backfill is unsafe);
      - no in-flight dispatch marker within the cooldown (dedup);
      - :data:`RECOVERY_DISPATCH_ENABLED` (OBSERVE-mode gate — logs the
        would-dispatch and writes NO marker / calls NO AWS when off).

    On dispatch, an in-progress marker is written so the next 15-min poll
    against the still-missing artifact dedups instead of re-dispatching.
    """
    if recovery is None:
        return False
    if result.state not in ("missing", "stale"):
        return False
    if result.sla_violated_by_minutes == 0:
        return False  # still within SLA grace — same gate as _maybe_alert

    if not RECOVERY_DISPATCH_ENABLED:
        logger.info(
            "OBSERVE-mode (recovery): would dispatch %s recovery for %s "
            "(state=%s) target=%s",
            recovery.get("type"), spec.artifact_id, result.state,
            recovery.get("target"),
        )
        return False

    # Dedup: a recovery already in-flight for this (artifact, cycle) → skip.
    if _recovery_already_dispatched(s3_client, spec, now):
        logger.info(
            "recovery for %s already dispatched this cycle (deduped)",
            spec.artifact_id,
        )
        return False

    # Write the marker BEFORE dispatching so a dispatch that succeeds but
    # whose marker-write would have failed can't loop; and so a crash between
    # dispatch and marker-write errs toward not-re-dispatching. The marker is
    # the dedup source of truth.
    marker = {
        "artifact_id": spec.artifact_id,
        "dispatched_at": now.isoformat(),
        "state": result.state,
        "recovery_type": recovery.get("type"),
        "target": recovery.get("target"),
    }
    _write_recovery_marker(s3_client, spec, now, marker)

    _dispatch_recovery(aws_clients, spec, recovery, now)
    logger.info(
        "DISPATCHED %s recovery for %s (state=%s) target=%s",
        recovery.get("type"), spec.artifact_id, result.state,
        recovery.get("target"),
    )
    return True


# ── Freshness-critical → overseer drain dispatch (config-I3282 phase 1) ─────


def _drain_dispatch_in_cooldown(s3_client: Any, now: datetime) -> bool:
    """True if a drain-dispatch marker exists within the cooldown window.

    Global (not per-artifact): one drain consumes the WHOLE intake queue, so
    every critical paged before (or shortly after) the launch is covered by
    the same run. Fail-closed on non-404 HEAD errors, mirroring
    :func:`_recovery_already_dispatched` — a transient S3 blip must not
    trigger a dispatch storm.
    """
    try:
        resp = s3_client.head_object(
            Bucket=REGISTRY_BUCKET, Key=DRAIN_DISPATCH_MARKER_KEY
        )
    except Exception as exc:  # noqa: BLE001 — classify by error code
        code = str(
            getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        )
        status = getattr(exc, "response", {}).get("ResponseMetadata", {}).get(
            "HTTPStatusCode", 0
        )
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False  # no marker → first dispatch
        logger.warning(
            "drain-dispatch marker HEAD failed (%s) — assuming in-flight to "
            "avoid a dispatch storm", exc,
        )
        return True
    lm = resp.get("LastModified")
    if lm is None:
        return True
    age_min = (now - lm).total_seconds() / 60.0
    return age_min < DRAIN_DISPATCH_COOLDOWN_MINUTES


def _maybe_dispatch_drain(
    s3_client: Any,
    aws_clients: dict[str, Any],
    candidate_ids: list[str],
    now: datetime,
) -> bool:
    """Event-time overseer drain dispatch for this sweep's critical pages
    (config-I3282 phase 1). Called ONCE per pass with every artifact whose
    critical page fired and whose declared lane admits dispatch.

    Returns ``True`` iff a dispatch was performed. Fires only when ALL of:
      - ``candidate_ids`` is non-empty;
      - :data:`DRAIN_DISPATCH_ENABLED` (OBSERVE-mode gate — logs the
        would-dispatch and writes NO marker / calls NO AWS when off);
      - no dispatch marker within the cooldown window (global dedup — one
        drain covers the whole queue).

    The router invoke is async (``Event``): the overseer-dispatcher owns
    verdict handling and escalation (P1 + loud page) end-to-end, exactly as
    it does for saturday-sf-watch-dispatcher's M2 dispatches. The marker is
    written BEFORE the invoke (same crash-ordering argument as the recovery
    marker: err toward not-re-dispatching).
    """
    if not candidate_ids:
        return False

    if not DRAIN_DISPATCH_ENABLED:
        logger.info(
            "OBSERVE-mode (drain-dispatch): would dispatch playbook=%s via %s "
            "for %d critical page(s): %s",
            DRAIN_PLAYBOOK, OVERSEER_DISPATCHER_FUNCTION,
            len(candidate_ids), sorted(candidate_ids),
        )
        return False

    if _drain_dispatch_in_cooldown(s3_client, now):
        logger.info(
            "drain dispatch in cooldown — %d critical page(s) (%s) covered by "
            "the in-flight/recent drain",
            len(candidate_ids), sorted(candidate_ids),
        )
        return False

    _put_json(
        s3_client, REGISTRY_BUCKET, DRAIN_DISPATCH_MARKER_KEY,
        {
            "dispatched_at": now.isoformat(),
            "artifact_ids": sorted(candidate_ids),
            "playbook": DRAIN_PLAYBOOK,
        },
    )

    lam = aws_clients.get("lambda")
    if lam is None:
        lam = boto3.client("lambda")
        aws_clients["lambda"] = lam
    lam.invoke(
        FunctionName=OVERSEER_DISPATCHER_FUNCTION,
        InvocationType="Event",  # async; the router owns escalation
        Payload=json.dumps({
            "playbook": DRAIN_PLAYBOOK,
            "payload": {"trigger": "freshness-critical", "is_drill": "false"},
        }).encode("utf-8"),
    )
    logger.info(
        "DISPATCHED overseer playbook=%s via %s for %d critical page(s): %s",
        DRAIN_PLAYBOOK, OVERSEER_DISPATCHER_FUNCTION,
        len(candidate_ids), sorted(candidate_ids),
    )
    return True


# ── Alerting (gated on ALERTS_ENABLED) ──────────────────────────────────────


_ALERTING_STATES = frozenset({"missing", "stale", "probe_failed"})


def _maybe_alert(spec: ArtifactSpec, result: CheckResult, now: datetime,
                 consecutive_miss_runs: int = 0) -> bool:
    """Route an alert for a non-fresh probe result. Returns True if
    publish was attempted (OBSERVE-mode short-circuit returns False).

    Only fires when:
      - ``result.state ∈ {missing, stale, probe_failed}``
      - ``result.sla_violated_by_minutes > 0`` for missing/stale (give
        the SLA grace window), OR ``probe_failed`` (no grace for
        broken probes — operator needs to know immediately)
      - :data:`ALERTS_ENABLED` is True
      - resolved severity is ``critical`` (``severity=warning`` rows are
        console-only via ``check_results.json`` — no SNS/Telegram)

    Dedup-key resolution via
    :func:`nousergon_lib.artifact_freshness.resolve_dedup_key` ⇒
    at most one alert per (artifact, cadence-window) regardless of
    how many 15min probes have already fired in this window.
    """
    if result.state not in _ALERTING_STATES:
        return False

    # Substrate already filters fresh/grace; the SLA-grace filter
    # mirrors the substrate's clip-at-zero arithmetic for
    # missing/stale. probe_failed has no SLA — fire immediately.
    if result.state in ("missing", "stale") and result.sla_violated_by_minutes == 0:
        return False

    if not ALERTS_ENABLED:
        logger.info(
            "OBSERVE-mode: would alert on %s state=%s reason=%r",
            spec.artifact_id, result.state, result.reason,
        )
        return False

    # Probe failures route to critical (the monitor itself is broken);
    # missing/stale respect the spec's severity. Plan §3 invariant 6.
    severity = "critical" if result.state == "probe_failed" else spec.severity

    # config-I3086 warning escalation: a warning row confirmed-missing for
    # WARNING_ESCALATION_RUNS consecutive evaluated sweeps stops being a
    # console-only fact and pages via the critical path. One cycle of
    # console-only is the designed noise floor; a PERSISTENT warning is an
    # incident nobody is looking at (the I3053 champion-feed staleness sat
    # on dashboard page 26 for days).
    escalated = (
        severity == "warning"
        and WARNING_ESCALATION_RUNS > 0
        and consecutive_miss_runs >= WARNING_ESCALATION_RUNS
    )
    if escalated:
        severity = "critical"
        logger.info(
            "warning-escalation (config-I3086): %s confirmed-missing for %d "
            "consecutive sweeps — paging via critical path",
            spec.artifact_id, consecutive_miss_runs,
        )

    # Registry convention: severity=warning means dashboard/console-only —
    # the operator surface is check_results.json + this page, not ops-health
    # Telegram. Critical (and probe_failed, coerced above) pages via SNS +
    # flow-doctor. Aligns with ARTIFACT_REGISTRY comments ("dashboard-only")
    # and the fleet notification consolidation arc (config#1740 / #1724).
    if severity == "warning":
        logger.info(
            "console-only (severity=warning): %s state=%s — surfaced in "
            "check_results.json, no SNS/Telegram",
            spec.artifact_id, result.state,
        )
        return False

    # Compose the alert body.
    body = (
        f"artifact_id={spec.artifact_id} "
        f"owner_repo={spec.owner_repo} "
        f"state={result.state} "
        f"key={result.canonical_key} "
        f"sla_violated_by_minutes={result.sla_violated_by_minutes} "
        f"reason={result.reason}"
    )
    if escalated:
        body += (
            f" escalated_from=warning after_consecutive_miss_runs="
            f"{consecutive_miss_runs}"
        )
    dedup_key = resolve_dedup_key(spec, now)

    publish(
        body,
        severity=severity,
        source="freshness-monitor",
        dedup_key=dedup_key,
        dedup_window_min=None,  # one alert per cadence window — substrate handles cycle bucketing
        telegram=False,
    )
    notify_via_flow_doctor(
        body,
        silent=False,
        severity=severity,
        dedup_key=dedup_key,
        flow_name=_FLOW_NAME,
        topics=_FRESHNESS_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
        context={
            "artifact_id": spec.artifact_id,
            "state": result.state,
            "owner_repo": spec.owner_repo,
        },
        # Must match the SNS/bus path's source= above exactly — both paths
        # alert on the same event, and the registered `freshness_monitor_staleness`
        # class in playbooks.yaml keys on this string (config-I3513).
        source="freshness-monitor",
    )
    return True


# ── Key-deliverable extended-staleness escalation (config#2055 Gap 2) ───────
#
# Even a confirmed critical page (above) is console/Telegram-only — nothing
# lands where Brian triages open work. A `severity=warning` row is worse:
# it's dashboard-only forever, no matter how long it persists (the exact
# "sat on dashboard page 26 for days" shape config-I3086 already fixed once
# for the *critical-page* threshold). This closes the same gap one rung
# higher: an artifact flagged `escalate_to_issue: true` that's been
# confirmed-missing for `ISSUE_ESCALATION_RUNS` consecutive daily sweeps
# gets a `[P1] gate:operator` issue filed directly on the Decision Queue
# (Brian's 2026-07-21 Option-A ruling on config#2055) — mirrors
# `overseer-dispatcher/index.py::_file_p1` byte-for-byte (same SSM-sourced
# PAT, same urllib POST, same repo target) rather than inventing a second
# GitHub-issue-filing implementation.


def _file_escalation_issue(
    artifact_id: str, owner_repo: str, miss_runs: int, anchor_id: str,
) -> dict:
    """File the extended-staleness P1 on ISSUES_REPO. Best-effort — the
    WARNING log + the returned dict (persisted into check_results.json's
    ``issue_filed_url`` for dedup) are the other recording surfaces."""
    try:
        pat = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "us-east-1")).get_parameter(
            Name=GH_PAT_SSM, WithDecryption=True
        )["Parameter"]["Value"]
        body = "\n".join([
            f"`{artifact_id}` (owner: `{owner_repo}`) has been confirmed-missing/"
            f"stale for {miss_runs} consecutive daily freshness sweeps via its "
            f"liveness proxy `{anchor_id}` — well past the point a `severity="
            "warning` console-only page is enough; nobody has acted on it.",
            "",
            "**Summary:** Freshness monitor's extended-staleness escalation "
            f"(config#2055 Gap 2) fired for `{artifact_id}` — flagged "
            "key-deliverable, confirmed stale via its liveness proxy for "
            f"{miss_runs}+ consecutive daily sweeps with no operator action.",
            "**Ask:** Investigate why `{}` (or its liveness proxy `{}`) has "
            "stopped updating, and either fix the producer or acknowledge the "
            "staleness is expected right now.".format(artifact_id, anchor_id),
            "**Options:** A) Investigate the producer pipeline for "
            f"`{artifact_id}` / `{anchor_id}` now (recommended) B) Acknowledge "
            "as expected (e.g. a genuinely quiet promotion period) and push "
            "out the re-exam date",
            "**SOTA:** Every key-deliverable artifact's staleness is caught "
            "and triaged within its cadence window — no silent multi-week gaps "
            "(the config#2054 incident this escalation path exists to prevent).",
            "**Delta:** IS SOTA — no delta; this issue IS the triage step.",
            "**Consequence of no action:** This artifact (or the promotion "
            "pipeline behind it) may stay silently stalled indefinitely — the "
            "exact config#2054 failure shape, just past the point a console "
            "page alone was working.",
            "",
            f"- **Anchor (liveness proxy):** `{anchor_id}`",
            f"- **Consecutive confirmed-miss daily sweeps:** {miss_runs} "
            f"(threshold: {ISSUE_ESCALATION_RUNS})",
            "- **Filed via:** alpha-engine-freshness-monitor (config#2055 Gap 2)",
            "",
            "Closes-when: the underlying staleness is resolved (producer fixed "
            "and a fresh write confirmed) or explicitly acknowledged as "
            "expected for this period.",
        ])
        req = urllib.request.Request(
            f"https://api.github.com/repos/{ISSUES_REPO}/issues",
            data=json.dumps({
                "title": f"[P1] Freshness monitor: {artifact_id} stale for "
                         f"{miss_runs}+ consecutive sweeps — extended staleness",
                "body": body,
                "labels": ["P1", "gate:operator", "area:infrastructure"],
            }).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {pat}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "freshness-monitor",
            },
        )
        with urllib.request.urlopen(req, timeout=_ISSUE_TIMEOUT_SEC) as resp:
            issue = json.loads(resp.read())
        logger.info(
            "config#2055 extended-staleness P1 filed for %s: %s",
            artifact_id, issue.get("html_url"),
        )
        return {"filed": True, "url": issue.get("html_url")}
    except Exception as exc:  # noqa: BLE001 — best-effort leg; recording surfaces: this WARNING, the returned dict
        logger.warning(
            "config#2055 extended-staleness P1 filing FAILED for %s: %s: %s",
            artifact_id, type(exc).__name__, exc,
        )
        return {"filed": False, "error": f"{type(exc).__name__}: {exc}"}


def _escalate_stale_key_deliverables(
    pairs: list[tuple[ArtifactSpec, CheckResult]],
    miss_counts: dict[str, int],
    escalate_to_issue_by_id: dict[str, bool],
    prev_issue_filed: dict[str, str],
    now: datetime,
) -> dict[str, str | None]:
    """For every ``escalate_to_issue``-flagged spec, file a Decision-Queue
    P1 once its confirmed-miss streak crosses :data:`ISSUE_ESCALATION_RUNS`.

    An ``event_driven`` row's OWN ``check_freshness`` result always
    short-circuits to ``fresh`` (see the event-driven short-circuit in
    ``nousergon_lib.artifact_freshness``), so its own ``miss_counts`` entry
    is always 0 — the miss-streak that actually matters is its
    ``liveness_via`` ANCHOR's, which this same sweep already computed (both
    rows are walked in the same registry pass, so the anchor's entry is
    always present in ``miss_counts`` by the time this runs). Non-
    ``event_driven`` flagged rows (should any exist in future) use their
    own miss streak directly.

    Returns the artifact_id -> issue URL map to persist into
    check_results.json (config#2055's dedup source of truth): sticky while
    the miss-streak persists, reset to ``None`` the moment it recovers so a
    FUTURE incident can file a fresh issue.
    """
    if not escalate_to_issue_by_id:
        return {}
    results_by_id = {spec.artifact_id: (spec, result) for spec, result in pairs}
    issue_filed_by_id: dict[str, str | None] = {}
    for artifact_id in escalate_to_issue_by_id:
        pair = results_by_id.get(artifact_id)
        if pair is None:
            continue
        spec, _result = pair
        anchor_id = (
            spec.liveness_via if spec.cadence == "event_driven" else spec.artifact_id
        )
        anchor_miss = miss_counts.get(anchor_id, 0)

        if anchor_miss == 0:
            # Recovered (or never missing) — clear any sticky marker so a
            # future incident can file a fresh issue.
            issue_filed_by_id[artifact_id] = None
            continue

        already_filed = prev_issue_filed.get(artifact_id)
        if already_filed:
            # Still stale, already escalated for THIS incident — carry
            # forward, don't re-file.
            issue_filed_by_id[artifact_id] = already_filed
            continue

        if not ALERTS_ENABLED:
            logger.info(
                "OBSERVE-mode: would escalate %s to Decision Queue P1 "
                "(anchor=%s miss_runs=%d)", artifact_id, anchor_id, anchor_miss,
            )
            issue_filed_by_id[artifact_id] = None
            continue

        if anchor_miss < ISSUE_ESCALATION_RUNS:
            issue_filed_by_id[artifact_id] = None
            continue

        filed = _file_escalation_issue(artifact_id, spec.owner_repo, anchor_miss, anchor_id)
        issue_filed_by_id[artifact_id] = filed.get("url") if filed.get("filed") else None

    return issue_filed_by_id


# ── Handler ─────────────────────────────────────────────────────────────────


# ── Historical-mode probe ───────────────────────────────────────────────────
#
# Closes the gap surfaced 2026-05-28: the current-state probe answers
# "is the artifact present *now*?" but operators also need "did it
# land last weekend? the weekend before? are there gaps in the
# producer's history?" Filed per the same feedback memory
# [[feedback_observe_mode_unconditional_gates_govern_cutover]] —
# absence-of-artifact is the failure mode, and a single-cycle absence
# could be a false-positive (instance failure) where a multi-cycle gap
# is a real producer regression.
#
# Fires on a separate EventBridge cron (daily ~04:00 UTC, off-peak)
# via event={"mode": "historical"}. Writes
# s3://alpha-engine-research/_freshness_monitor/history.json which
# page 26 reads to surface per-artifact gap counts + per-row history
# expanders.
#
# Date resolution is intentionally simple (calendar-naive):
#   - saturday_sf: last N calendar Saturdays
#   - weekday_sf / eod_sf: last N Mon-Fri days
# NYSE holidays show up as false-positive "absent" days. Operators
# interpret them in context (or filter via the page 26 surface). When
# the holiday-aware backfill becomes worth the dependency lift, we
# can route via nousergon_lib.dates.


def _iter_sf_firing_dates(cadence: str, now: datetime, count: int) -> list[date]:
    """Return the N most recent SF firing dates (calendar) for the given
    cadence, newest-first. The SF cron's actual firing dates — Saturdays
    for saturday_sf, Mon-Fri for weekday_sf / eod_sf. Calendar-naive
    (NYSE holidays NOT skipped at this layer — observable false-positives
    for holiday-skipped firings surface as ❌ absent cells, which the
    operator interprets in context).
    """
    if count <= 0:
        return []
    today = now.date()
    dates: list[date] = []
    if cadence == "saturday_sf":
        d = today - timedelta(days=1)
        while len(dates) < count:
            if d.weekday() == 5:  # Saturday
                dates.append(d)
            d -= timedelta(days=1)
    elif cadence in {"weekday_sf", "eod_sf"}:
        d = today - timedelta(days=1)
        while len(dates) < count:
            if d.weekday() < 5:  # Mon-Fri
                dates.append(d)
            d -= timedelta(days=1)
    return dates


def _resolve_axis_dates(
    firing_dates: list[date], template: str, cadence: str,
) -> list[date]:
    """Translate SF firing dates to the date axis the s3_key_template
    actually uses. Two axes are supported:

      - ``{date}`` — calendar date (the SF firing date itself). Used by
        artifacts whose key reflects the SF firing identity, e.g.
        ``_weekly/{date}/manifest.json`` (the data manifest IS the
        Saturday firing receipt).
      - ``{trading_day}`` — NYSE trading day. Used by artifacts whose
        key reflects the trading-day the data refers to, NOT the SF
        firing date. Cadence-specific resolution:
          * saturday_sf: previous_trading_day(saturday) → typically Fri
            (the trading day whose close drove this Saturday's research).
          * weekday_sf: previous_trading_day(weekday) → the prior trading
            day's close (the AM SF fires before market open).
          * eod_sf: weekday itself → today's close (the EOD SF fires
            after market close, so today IS the trading_day).

    Per the system-wide ``now_dual()`` convention
    (``trading_day = last_closed_trading_day(now)``); see
    alpha-engine-docs/private/DATE_CONVENTIONS.md.

    Calendar-naive at the SF-firing layer above, but trading_day
    resolution uses ``nousergon_lib.trading_calendar.previous_trading_day``
    which IS NYSE-holiday-aware. So holiday-skipped firings still
    surface as cleanly-absent cells, but their resolved trading_day
    skips the holiday correctly.
    """
    if "{trading_day}" in template:
        if cadence == "eod_sf":
            return list(firing_dates)
        return [previous_trading_day(d) for d in firing_dates]
    return list(firing_dates)


def _iter_historical_cycle_dates(
    cadence: str, now: datetime, count: int, template: str = "",
) -> list[date]:
    """Return the N most recent cycle dates resolved to the axis the
    template uses. See ``_iter_sf_firing_dates`` +
    ``_resolve_axis_dates`` for the two-stage derivation.

    Backward compat: callers that omit ``template`` get calendar-axis
    resolution (the pre-2026-05-28 behavior). The historical-mode
    handler always passes the template.
    """
    firing_dates = _iter_sf_firing_dates(cadence, now, count)
    return _resolve_axis_dates(firing_dates, template, cadence)


def _format_historical_key(template: str, target_date: date) -> str:
    """Substitute date placeholders. Supports the same placeholders the
    substrate's _format_key handles: ``{date}``, ``{trading_day}``.
    ``{cycle_label}`` (fortnightly/quarterly buckets) is not historical-
    probable from a single date, so artifacts using it are skipped.
    """
    iso = target_date.isoformat()
    return template.format(date=iso, trading_day=iso)


def _probe_historical(
    s3_client: Any,
    spec: ArtifactSpec,
    cycle_dates: list[date],
) -> tuple[list[dict], bool]:
    """Probe the last N cycles' keys for one artifact. Returns
    ``(cycles, is_latest_pointer)``. Each ``cycles`` entry is a dict
    with ``date``, ``present``, ``size``, ``last_modified``.

    For artifacts whose ``s3_key_template`` is a latest-pointer (no
    ``{date}``/``{trading_day}`` placeholder), returns a single-entry
    list with the pointer's current state — historical sequence isn't
    observable from the pointer alone, so the page must render this
    distinction.
    """
    template = spec.s3_key_template
    has_date_placeholder = "{date}" in template or "{trading_day}" in template
    has_unsupported_placeholder = "{cycle_label}" in template

    if has_unsupported_placeholder:
        return [], False

    bucket = spec.s3_bucket or REGISTRY_BUCKET

    if not has_date_placeholder:
        # Latest-pointer: HEAD once, report current state only.
        try:
            resp = s3_client.head_object(Bucket=bucket, Key=template)
            return [{
                "date": "(latest)",
                "present": True,
                "size": resp["ContentLength"],
                "last_modified": resp["LastModified"].isoformat(),
            }], True
        except Exception as exc:  # noqa: BLE001 — record per-spec failures inline
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", "unknown"))
            entry: dict = {"date": "(latest)", "present": False}
            if code not in {"404", "403", "NoSuchKey"}:
                entry["error_code"] = code
            return [entry], True

    # Date-templated: probe each historical date.
    cycles = []
    for d in cycle_dates:
        try:
            key = _format_historical_key(template, d)
        except (KeyError, IndexError) as exc:
            cycles.append({
                "date": d.isoformat(),
                "present": False,
                "error_code": f"template_render_failed:{type(exc).__name__}",
            })
            continue
        try:
            resp = s3_client.head_object(Bucket=bucket, Key=key)
            cycles.append({
                "date": d.isoformat(),
                "present": True,
                "size": resp["ContentLength"],
                "last_modified": resp["LastModified"].isoformat(),
            })
        except Exception as exc:  # noqa: BLE001
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", "unknown"))
            # 404 (object missing) AND 403 (object missing, no ListBucket) both
            # mean "not present" in S3 semantics — when the Lambda lacks
            # s3:ListBucket on the bucket, S3 returns 403 instead of 404 for
            # missing keys. Surface both as cleanly-absent (no error_code) so
            # the page 26 display doesn't show spurious "403 errors" on
            # legitimately-absent historical cycles. Other codes (500, etc.)
            # keep error_code for operator visibility.
            if code in {"404", "403", "NoSuchKey"}:
                cycles.append({"date": d.isoformat(), "present": False})
            else:
                cycles.append({
                    "date": d.isoformat(),
                    "present": False,
                    "error_code": code,
                })
    return cycles, False


def _handle_historical(
    s3_client: Any,
    now: datetime,
    started_at: float,
    lookback_overrides: dict | None,
) -> dict:
    """Walk the registry, probe each artifact's last N cycles, write
    ``history.json``. Same outer error handling as the current-state
    handler — load_registry raises on YAML parse / schema, per-spec
    failures are caught inline."""
    logger.info(
        "freshness-monitor invoked in HISTORICAL mode at %s",
        now.isoformat(),
    )
    lookback = dict(_DEFAULT_LOOKBACK)
    if lookback_overrides:
        lookback.update(lookback_overrides)

    specs = load_registry(s3_client, REGISTRY_BUCKET, REGISTRY_KEY)
    logger.info("loaded %d specs from registry", len(specs))

    artifacts_history: dict[str, dict] = {}
    skipped_unsupported = 0
    total_cycles_probed = 0
    for spec in specs:
        count = lookback.get(spec.cadence, 0)
        cycle_dates = _iter_historical_cycle_dates(
            spec.cadence, now, count, template=spec.s3_key_template,
        )
        cycles, is_latest_pointer = _probe_historical(s3_client, spec, cycle_dates)
        if not cycles and "{cycle_label}" in spec.s3_key_template:
            skipped_unsupported += 1
            continue
        total_cycles_probed += len(cycles)
        # Gap count: present=False entries in date-templated history.
        # Latest-pointers don't have a meaningful gap count (single point).
        if is_latest_pointer:
            gap_count = None
            continuous = (
                len(cycles) == 1 and cycles[0].get("present") is True
            )
        else:
            gap_count = sum(1 for c in cycles if not c.get("present"))
            continuous = (gap_count == 0 and len(cycles) > 0)
        artifacts_history[spec.artifact_id] = {
            "cadence": spec.cadence,
            "severity": spec.severity,
            "owner_repo": spec.owner_repo,
            "s3_key_template": spec.s3_key_template,
            "is_latest_pointer": is_latest_pointer,
            "lookback_cycles": count if not is_latest_pointer else 1,
            "gap_count": gap_count,
            "continuous": continuous,
            "history": cycles,
        }

    payload = {
        "generated_at": now.isoformat(),
        "lookback": lookback,
        "duration_seconds": round(time.time() - started_at, 2),
        "n_artifacts": len(artifacts_history),
        "n_cycles_probed": total_cycles_probed,
        "skipped_unsupported": skipped_unsupported,
        "artifacts": artifacts_history,
    }
    _put_json(s3_client, REGISTRY_BUCKET, HISTORY_KEY, payload)

    logger.info(
        "freshness-monitor HISTORICAL complete: %d artifacts, %d cycles probed, %d skipped, duration=%.2fs",
        len(artifacts_history),
        total_cycles_probed,
        skipped_unsupported,
        payload["duration_seconds"],
    )

    return {
        "mode": "historical",
        "n_artifacts": len(artifacts_history),
        "n_cycles_probed": total_cycles_probed,
        "skipped_unsupported": skipped_unsupported,
        "duration_seconds": payload["duration_seconds"],
    }


# ── Intraday-mode probe (config#1297) ───────────────────────────────────────


def _handle_intraday(s3_client: Any, now: datetime, started_at: float) -> dict:
    """30-min weekday-market-hours mini-rule, scoped to
    :data:`INTRADAY_ARTIFACT_IDS` only.

    Alerts/dispatches exactly like the daily full sweep (same
    :func:`_run_probe_pass`) but writes NO check_results/heartbeat/
    cycle_verdict — those full-registry dashboard surfaces are owned solely
    by the daily sweep (which itself checks these same two artifacts, just
    once a day), so a partial pass can never overwrite them with a
    2-artifact-only view.
    """
    logger.info(
        "freshness-monitor invoked in INTRADAY mode at %s (alerts_enabled=%s)",
        now.isoformat(), ALERTS_ENABLED,
    )

    (specs, recovery_by_id, critical_arms_by_id, _escalate_to_issue_by_id,
     remediation_by_id) = (
        load_registry_with_recovery(s3_client, REGISTRY_BUCKET, REGISTRY_KEY)
    )
    specs, _coerced = apply_dynamic_severity(s3_client, specs, critical_arms_by_id)
    intraday_specs = [s for s in specs if s.artifact_id in INTRADAY_ARTIFACT_IDS]
    missing_ids = INTRADAY_ARTIFACT_IDS - {s.artifact_id for s in intraday_specs}
    if missing_ids:
        logger.warning(
            "intraday mode: registry is missing expected artifact_id(s) %s",
            sorted(missing_ids),
        )

    pairs, alerted, dispatched, per_spec_exceptions, _miss_counts = _run_probe_pass(
        s3_client, intraday_specs, recovery_by_id, now,
        remediation_by_id=remediation_by_id,
    )

    duration_seconds = round(time.time() - started_at, 2)
    logger.info(
        "freshness-monitor INTRADAY complete: %s checked, %s alerted, %s dispatched, "
        "%s per-spec exceptions, duration=%.2fs",
        len(pairs), alerted, dispatched, per_spec_exceptions, duration_seconds,
    )

    return {
        "mode": "intraday",
        "n_entries_checked": len(pairs),
        "alerts_enabled": ALERTS_ENABLED,
        "alerted": alerted,
        "dispatched": dispatched,
        "per_spec_exceptions": per_spec_exceptions,
        "duration_seconds": duration_seconds,
    }


# ── Probe pass (shared by the daily full sweep + the intraday mini-rule) ────


def _run_probe_pass(
    s3_client: Any,
    specs: list[ArtifactSpec],
    recovery_by_id: dict[str, dict],
    now: datetime,
    prev_miss_counts: dict[str, int] | None = None,
    remediation_by_id: dict[str, str] | None = None,
) -> tuple[list[tuple[ArtifactSpec, CheckResult]], int, int, int, dict[str, int]]:
    """Walk ``specs``, probe each, dispatch confirmed-miss recoveries, and
    alert. Returns ``(pairs, alerted, dispatched, per_spec_exceptions)``.

    config-I3282: after the walk, one aggregated event-time drain dispatch
    fires for the pass's critical pages (see :func:`_maybe_dispatch_drain`
    for the eligibility + dedup semantics) — independently trapped like the
    per-artifact recovery dispatches, so it can never sink the pass.

    Shared verbatim by the daily full-registry sweep and the intraday
    mini-rule (config#1297) — the only difference between the two callers
    is which `specs` they pass in and what they do with the returned
    `pairs` (the full sweep serializes them to the shared dashboard
    surfaces; the intraday mini-rule only alerts, per `handler`'s docstring).

    ``prev_miss_counts`` (config-I3086) carries the previous sweep's
    per-artifact consecutive confirmed-miss counters; the returned
    ``miss_counts`` is this sweep's updated map (persisted into
    check_results.json by the daily caller — the intraday mini-rule
    passes None and gets all-zero counters, so it never escalates).
    """
    pairs: list[tuple[ArtifactSpec, CheckResult]] = []
    alerted = 0
    dispatched = 0
    per_spec_exceptions = 0
    prev_miss_counts = prev_miss_counts or {}
    remediation_by_id = remediation_by_id or {}
    miss_counts: dict[str, int] = {}
    drain_candidates: list[str] = []
    # Per-pass cache of lazily-created SF/Lambda clients (shared across the
    # walk so a pass dispatching several recoveries reuses one client each).
    aws_clients: dict[str, Any] = {}
    for spec in specs:
        result, exc = _check_one(s3_client, spec, now)
        if exc is not None:
            per_spec_exceptions += 1
            logger.warning(
                "per-spec exception for %s: %s", spec.artifact_id, exc,
            )
        pairs.append((spec, result))

        # config#1240 — auto-remediation. Attempt a dispatch on a confirmed
        # miss (independently trapped so a dispatch failure can NEVER sink the
        # monitor's primary alert/heartbeat deliverables). `mode: dispatch`
        # suppresses the page once a heal is dispatched this cycle; the default
        # `dispatch_and_page` pages AND heals (belt-and-braces).
        recovery = recovery_by_id.get(spec.artifact_id)
        did_dispatch = False
        try:
            did_dispatch = _maybe_dispatch_recovery(
                s3_client, aws_clients, spec, recovery, result, now,
            )
        except Exception as disp_exc:  # noqa: BLE001 — dispatch must not sink the pass
            logger.warning(
                "recovery dispatch for %s failed (non-fatal): %s",
                spec.artifact_id, disp_exc, exc_info=True,
            )
        if did_dispatch:
            dispatched += 1

        # config-I3086: consecutive confirmed-miss counter (0 on any
        # non-miss, including grace/fresh — a recovered artifact resets).
        miss_runs = (
            prev_miss_counts.get(spec.artifact_id, 0) + 1
            if _is_confirmed_miss(result) else 0
        )
        miss_counts[spec.artifact_id] = miss_runs

        suppress_page = (
            did_dispatch
            and isinstance(recovery, dict)
            and recovery.get("mode", "dispatch_and_page") == "dispatch"
        )
        paged = False
        if not suppress_page and _maybe_alert(
                spec, result, now, consecutive_miss_runs=miss_runs):
            alerted += 1
            paged = True

        # config-I3282 — collect this pass's dispatch-eligible critical
        # pages. `_maybe_alert` returns True ONLY on an actual critical
        # publish (warnings and OBSERVE mode return False), so `paged` is
        # already the effective-severity gate. Excluded: rows with a
        # `recovery:` heal of their own (their declared lane), and rows
        # declared `remediation: operator` (page-only by declaration).
        if (
            paged
            and spec.artifact_id not in recovery_by_id
            and remediation_by_id.get(spec.artifact_id) != "operator"
        ):
            drain_candidates.append(spec.artifact_id)

    # One aggregated event-time drain per pass (config-I3282), trapped so a
    # dispatch failure can never sink the pass's primary deliverables. The
    # critical page(s) above already fired, so the operator surface exists
    # regardless of this leg's outcome.
    try:
        _maybe_dispatch_drain(s3_client, aws_clients, drain_candidates, now)
    except Exception as drain_exc:  # noqa: BLE001 — side effect; pages already fired, this ERROR + the un-drained queue are the recording surfaces
        logger.error(
            "FRESHNESS_DRAIN_DISPATCH_FAILED for %s (non-fatal): %s",
            sorted(drain_candidates), drain_exc, exc_info=True,
        )

    return pairs, alerted, dispatched, per_spec_exceptions, miss_counts


# ── Main handler ────────────────────────────────────────────────────────────


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge cron handler — daily walk of the full registry, emit
    heartbeat + check_results, alert on misses past SLA.

    ``event["mode"] == "historical"`` dispatches to the daily
    historical-probe path instead (separate EB cron at ~04:00 UTC).

    ``event["mode"] == "intraday"`` (config#1297) dispatches to a lighter
    pass scoped to :data:`INTRADAY_ARTIFACT_IDS` only, on a separate 30-min
    weekday-market-hours EB cron. It alerts/dispatches exactly like the full
    sweep but does NOT write check_results/heartbeat/cycle_verdict — those
    are the full-registry dashboard surfaces and only the daily sweep (which
    covers every artifact, including these two) owns them, so a partial
    intraday pass can never clobber them with a 2-artifact-only view.
    """
    started_at = time.time()
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3")

    if event and event.get("mode") == "historical":
        return _handle_historical(
            s3, now, started_at, event.get("lookback"),
        )

    if event and event.get("mode") == "intraday":
        return _handle_intraday(s3, now, started_at)

    logger.info(
        "freshness-monitor invoked at %s (alerts_enabled=%s)",
        now.isoformat(), ALERTS_ENABLED,
    )

    # Load registry. If THIS fails, we want the Lambda to error out
    # so the CW alarm fires — a broken registry must not be silent.
    (specs, recovery_by_id, critical_arms_by_id, escalate_to_issue_by_id,
     remediation_by_id) = (
        load_registry_with_recovery(s3, REGISTRY_BUCKET, REGISTRY_KEY)
    )
    logger.info(
        "loaded %d specs from registry (%d with recovery specs, %d with "
        "champion-arm dynamic severity, %d flagged for issue escalation, "
        "%d with declared remediation lanes)",
        len(specs), len(recovery_by_id), len(critical_arms_by_id),
        len(escalate_to_issue_by_id), len(remediation_by_id),
    )

    # config-I3086: dynamic severity + warning-escalation counters.
    specs, coerced_ids = apply_dynamic_severity(s3, specs, critical_arms_by_id)
    prev_miss_counts = _load_prev_miss_counts(s3)

    pairs, alerted, dispatched, per_spec_exceptions, miss_counts = _run_probe_pass(
        s3, specs, recovery_by_id, now, prev_miss_counts,
        remediation_by_id=remediation_by_id,
    )

    # config#2055 Gap 2: extended-staleness -> Decision Queue P1. Runs after
    # the full pass so flagged `event_driven` rows can look up their
    # `liveness_via` ANCHOR's miss-streak from this same sweep (their own
    # `consecutive_miss_runs` is always 0 — event_driven never self-pages).
    prev_issue_filed = _load_prev_issue_filed(s3)
    issue_filed_by_id = _escalate_stale_key_deliverables(
        pairs, miss_counts, escalate_to_issue_by_id, prev_issue_filed, now,
    )

    # Emit dashboard surface + self-heartbeat.
    check_results = _serialize_check_results(
        pairs, now, miss_counts=miss_counts, coerced_ids=coerced_ids,
        issue_filed_by_id=issue_filed_by_id,
    )
    heartbeat = _serialize_heartbeat(pairs, now, started_at)

    _put_json(s3, REGISTRY_BUCKET, CHECK_RESULTS_KEY, check_results)
    _put_json(s3, REGISTRY_BUCKET, HEARTBEAT_KEY, heartbeat)

    # ── Per-cycle completion rollup (L249 consumer) ─────────────────────
    # Secondary observability hung off the primary probe pass. The artifact
    # The S3 verdict write comes first (S3 PutObject is already granted); the
    # CW emit is independent (it needs the cloudwatch:PutMetricData grant in
    # iam-policy.json, scoped to the AlphaEngine/Substrate namespace). Both are
    # wrapped so a failure here can NEVER take down the monitor's primary
    # deliverables (check_results + heartbeat + alerts), already persisted above.
    #
    # The two side effects are split into INDEPENDENT traps so a CW-emit failure
    # (e.g. a PutMetricData grant regression) cannot suppress the cycle_verdict.json
    # write — config#1236 found the deployed Lambda not emitting cycle_verdict.json
    # and the single combined trap masked which step failed. Each trap now:
    #   (a) records the swallowed failure with exc_info (full CW Logs traceback), and
    #   (b) emits an ArtifactFreshnessCycleVerdictError CW datapoint dimensioned by
    #       the failing Stage, so a silent rollup failure is alarmable rather than
    #       only visible by the absence/staleness of cycle_verdict.json.
    # Per CLAUDE.md no-silent-fails secondary-observability carve-out.
    cycle_verdicts: dict[str, str] = {}
    verdict_payload: dict[str, Any] | None = None
    try:
        verdict_payload = _serialize_cycle_verdicts(pairs, now)
        _put_json(s3, REGISTRY_BUCKET, CYCLE_VERDICT_KEY, verdict_payload)
        cycle_verdicts = {
            v["cadence"]: v["state"] for v in verdict_payload["verdicts"]
        }
        logger.info("cycle verdicts: %s", cycle_verdicts)
    except Exception as exc:  # noqa: BLE001 — secondary observability, must not sink the monitor
        logger.warning(
            "cycle-verdict serialize/S3-write failed (non-fatal): %s", exc, exc_info=True
        )
        _emit_cycle_verdict_error("serialize_or_s3_write")

    # CW metric emit is best-effort and independent of the S3 write above: even
    # if it fails (grant regression), cycle_verdict.json is already persisted.
    if verdict_payload is not None:
        try:
            _emit_cycle_metrics(boto3.client("cloudwatch"), verdict_payload)
        except Exception as exc:  # noqa: BLE001 — observability emit, must not sink the monitor
            logger.warning(
                "cycle-completion CW metric emit failed (non-fatal): %s", exc, exc_info=True
            )
            _emit_cycle_verdict_error("cw_metric_emit")

    issues_filed_this_run = sum(
        1 for aid, url in issue_filed_by_id.items()
        if url and prev_issue_filed.get(aid) is None  # newly filed, not carried forward
    )
    logger.info(
        "freshness-monitor complete: %s checked, %s alerted, %s dispatched, "
        "%s issues filed, %s per-spec exceptions, duration=%.2fs",
        heartbeat["n_entries_checked"], alerted, dispatched,
        issues_filed_this_run, per_spec_exceptions, heartbeat["duration_seconds"],
    )

    return {
        "n_entries_checked": heartbeat["n_entries_checked"],
        "counts": heartbeat["counts"],
        "alerts_enabled": ALERTS_ENABLED,
        "recovery_dispatch_enabled": RECOVERY_DISPATCH_ENABLED,
        "drain_dispatch_enabled": DRAIN_DISPATCH_ENABLED,
        "alerted": alerted,
        "dispatched": dispatched,
        "issues_filed": issues_filed_this_run,
        "per_spec_exceptions": per_spec_exceptions,
        "duration_seconds": heartbeat["duration_seconds"],
        "cycle_verdicts": cycle_verdicts,
    }
