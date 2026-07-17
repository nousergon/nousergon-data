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
     "dashboard-only" convention).
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
from collections import defaultdict
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
) -> tuple[list[ArtifactSpec], dict[str, dict]]:
    """Like :func:`load_registry`, but also returns the per-artifact
    ``recovery:`` spec map (config#1240) keyed by ``artifact_id``.

    ``ArtifactSpec`` is a frozen lib dataclass without a ``recovery``
    field (the monitor's dispatch concern is not the substrate's
    freshness concern), so the recovery block is parsed into a parallel
    map rather than threaded onto the spec. Artifacts without a
    ``recovery:`` block are simply absent from the map — the dispatch
    path treats a missing key as "no auto-remediation, page only".
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    data = yaml.safe_load(body)
    if not isinstance(data, dict) or not data.get("artifacts"):
        raise ValueError(f"registry at s3://{bucket}/{key} missing 'artifacts'")

    defaults = data.get("defaults", {}) or {}
    specs: list[ArtifactSpec] = []
    recovery_by_id: dict[str, dict] = {}
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
    return specs, recovery_by_id


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
    pairs: list[tuple[ArtifactSpec, CheckResult]], now: datetime
) -> dict[str, Any]:
    """Build the ``check_results.json`` payload — one row per spec for
    the dashboard surface (Phase 5)."""
    rows = []
    for spec, result in pairs:
        rows.append(
            {
                "artifact_id": spec.artifact_id,
                "owner_repo": spec.owner_repo,
                "severity": spec.severity,
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


# ── Alerting (gated on ALERTS_ENABLED) ──────────────────────────────────────


_ALERTING_STATES = frozenset({"missing", "stale", "probe_failed"})


def _maybe_alert(spec: ArtifactSpec, result: CheckResult, now: datetime) -> bool:
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
    )
    return True


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

    specs, recovery_by_id = load_registry_with_recovery(
        s3_client, REGISTRY_BUCKET, REGISTRY_KEY
    )
    intraday_specs = [s for s in specs if s.artifact_id in INTRADAY_ARTIFACT_IDS]
    missing_ids = INTRADAY_ARTIFACT_IDS - {s.artifact_id for s in intraday_specs}
    if missing_ids:
        logger.warning(
            "intraday mode: registry is missing expected artifact_id(s) %s",
            sorted(missing_ids),
        )

    pairs, alerted, dispatched, per_spec_exceptions = _run_probe_pass(
        s3_client, intraday_specs, recovery_by_id, now,
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
) -> tuple[list[tuple[ArtifactSpec, CheckResult]], int, int, int]:
    """Walk ``specs``, probe each, dispatch confirmed-miss recoveries, and
    alert. Returns ``(pairs, alerted, dispatched, per_spec_exceptions)``.

    Shared verbatim by the daily full-registry sweep and the intraday
    mini-rule (config#1297) — the only difference between the two callers
    is which `specs` they pass in and what they do with the returned
    `pairs` (the full sweep serializes them to the shared dashboard
    surfaces; the intraday mini-rule only alerts, per `handler`'s docstring).
    """
    pairs: list[tuple[ArtifactSpec, CheckResult]] = []
    alerted = 0
    dispatched = 0
    per_spec_exceptions = 0
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

        suppress_page = (
            did_dispatch
            and isinstance(recovery, dict)
            and recovery.get("mode", "dispatch_and_page") == "dispatch"
        )
        if not suppress_page and _maybe_alert(spec, result, now):
            alerted += 1

    return pairs, alerted, dispatched, per_spec_exceptions


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
    specs, recovery_by_id = load_registry_with_recovery(
        s3, REGISTRY_BUCKET, REGISTRY_KEY
    )
    logger.info(
        "loaded %d specs from registry (%d with recovery specs)",
        len(specs), len(recovery_by_id),
    )

    pairs, alerted, dispatched, per_spec_exceptions = _run_probe_pass(
        s3, specs, recovery_by_id, now,
    )

    # Emit dashboard surface + self-heartbeat.
    check_results = _serialize_check_results(pairs, now)
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

    logger.info(
        "freshness-monitor complete: %s checked, %s alerted, %s dispatched, "
        "%s per-spec exceptions, duration=%.2fs",
        heartbeat["n_entries_checked"], alerted, dispatched, per_spec_exceptions,
        heartbeat["duration_seconds"],
    )

    return {
        "n_entries_checked": heartbeat["n_entries_checked"],
        "counts": heartbeat["counts"],
        "alerts_enabled": ALERTS_ENABLED,
        "recovery_dispatch_enabled": RECOVERY_DISPATCH_ENABLED,
        "alerted": alerted,
        "dispatched": dispatched,
        "per_spec_exceptions": per_spec_exceptions,
        "duration_seconds": heartbeat["duration_seconds"],
        "cycle_verdicts": cycle_verdicts,
    }
