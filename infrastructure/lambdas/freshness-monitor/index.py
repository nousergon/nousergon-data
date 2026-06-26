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
     :func:`alpha_engine_lib.artifact_freshness.check_freshness`
     against the current ``now`` (UTC).
  3. Aggregate results into a single ``check_results.json`` artifact
     under ``_freshness_monitor/`` (the dashboard surface reads this).
  4. Emit a self-heartbeat at ``_freshness_monitor/heartbeat.json``
     — the monitor monitors itself; substrate-health-check daily
     watches the heartbeat.
  5. For misses past SLA (``state ∈ {missing, stale, probe_failed}``),
     route to :func:`alpha_engine_lib.alerts.publish` with
     ``dedup_key=resolve_dedup_key(spec, now)`` — dedup collapses
     4×/hour retries to one alert per cycle per artifact.
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

from alpha_engine_lib.alerts import publish
from alpha_engine_lib.artifact_freshness import (
    ArtifactSpec,
    CheckResult,
    check_freshness,
    cycle_completion,
    resolve_current_cycle,
    resolve_dedup_key,
)
from alpha_engine_lib.trading_calendar import previous_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# ── Configuration (env-driven so Phase 6 cutover is a single CLI flip) ──────

REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET", "alpha-engine-research")
REGISTRY_KEY = os.environ.get(
    "REGISTRY_KEY", "_freshness_monitor/ARTIFACT_REGISTRY.yaml"
)
HEARTBEAT_KEY = "_freshness_monitor/heartbeat.json"
CHECK_RESULTS_KEY = "_freshness_monitor/check_results.json"
HISTORY_KEY = "_freshness_monitor/history.json"
CYCLE_VERDICT_KEY = "_freshness_monitor/cycle_verdict.json"

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
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    data = yaml.safe_load(body)
    if not isinstance(data, dict) or not data.get("artifacts"):
        raise ValueError(f"registry at s3://{bucket}/{key} missing 'artifacts'")

    defaults = data.get("defaults", {}) or {}
    specs: list[ArtifactSpec] = []
    for entry in data["artifacts"]:
        merged = {**defaults, **entry}
        merged["created_at"] = _coerce_date(merged["created_at"])
        # Strip any extension fields (forward-compat with future schema).
        spec_kwargs = {k: v for k, v in merged.items() if k in _SPEC_FIELDS}
        specs.append(ArtifactSpec(**spec_kwargs))
    return specs


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

    Dedup-key resolution via
    :func:`alpha_engine_lib.artifact_freshness.resolve_dedup_key` ⇒
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

    # Probe failures route to critical (the monitor itself is broken);
    # missing/stale respect the spec's severity. Plan §3 invariant 6.
    severity = "critical" if result.state == "probe_failed" else spec.severity

    publish(
        body,
        severity=severity,
        source="freshness-monitor",
        dedup_key=dedup_key,
        dedup_window_min=None,  # one alert per cadence window — substrate handles cycle bucketing
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
# can route via alpha_engine_lib.dates.


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
    resolution uses ``alpha_engine_lib.trading_calendar.previous_trading_day``
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


# ── Main handler ────────────────────────────────────────────────────────────


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge cron handler — every 15min walk the registry,
    emit heartbeat + check_results, alert on misses past SLA.

    ``event["mode"] == "historical"`` dispatches to the daily
    historical-probe path instead (separate EB cron at ~04:00 UTC).
    """
    started_at = time.time()
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3")

    if event and event.get("mode") == "historical":
        return _handle_historical(
            s3, now, started_at, event.get("lookback"),
        )

    logger.info(
        "freshness-monitor invoked at %s (alerts_enabled=%s)",
        now.isoformat(), ALERTS_ENABLED,
    )

    # Load registry. If THIS fails, we want the Lambda to error out
    # so the CW alarm fires — a broken registry must not be silent.
    specs = load_registry(s3, REGISTRY_BUCKET, REGISTRY_KEY)
    logger.info("loaded %d specs from registry", len(specs))

    # Walk and probe.
    pairs: list[tuple[ArtifactSpec, CheckResult]] = []
    alerted = 0
    per_spec_exceptions = 0
    for spec in specs:
        result, exc = _check_one(s3, spec, now)
        if exc is not None:
            per_spec_exceptions += 1
            logger.warning(
                "per-spec exception for %s: %s", spec.artifact_id, exc,
            )
        pairs.append((spec, result))
        if _maybe_alert(spec, result, now):
            alerted += 1

    # Emit dashboard surface + self-heartbeat.
    check_results = _serialize_check_results(pairs, now)
    heartbeat = _serialize_heartbeat(pairs, now, started_at)

    _put_json(s3, REGISTRY_BUCKET, CHECK_RESULTS_KEY, check_results)
    _put_json(s3, REGISTRY_BUCKET, HEARTBEAT_KEY, heartbeat)

    # ── Per-cycle completion rollup (L249 consumer) ─────────────────────
    # Secondary observability hung off the primary probe pass. The artifact
    # write comes first (S3 PutObject is already granted); the CW emit is
    # last (it needs the cloudwatch:PutMetricData grant added in iam-policy.json,
    # which only takes effect on the next deploy). The whole block is wrapped
    # so a failure here can NEVER take down the monitor's primary deliverables
    # (check_results + heartbeat + alerts), already persisted above.
    #   (a) swallowed failure: cycle-verdict compute / S3 write / CW put error.
    #   (c) recording surface: the WARN log below (CW Logs) + the staleness of
    #       cycle_verdict.json (itself a monitorable artifact).
    # Per CLAUDE.md no-silent-fails secondary-observability carve-out.
    cycle_verdicts: dict[str, str] = {}
    try:
        verdict_payload = _serialize_cycle_verdicts(pairs, now)
        _put_json(s3, REGISTRY_BUCKET, CYCLE_VERDICT_KEY, verdict_payload)
        _emit_cycle_metrics(boto3.client("cloudwatch"), verdict_payload)
        cycle_verdicts = {
            v["cadence"]: v["state"] for v in verdict_payload["verdicts"]
        }
        logger.info("cycle verdicts: %s", cycle_verdicts)
    except Exception as exc:  # noqa: BLE001 — secondary observability, must not sink the monitor
        logger.warning("cycle-completion rollup failed (non-fatal): %s", exc)

    logger.info(
        "freshness-monitor complete: %s checked, %s alerted, %s per-spec exceptions, "
        "duration=%.2fs",
        heartbeat["n_entries_checked"], alerted, per_spec_exceptions,
        heartbeat["duration_seconds"],
    )

    return {
        "n_entries_checked": heartbeat["n_entries_checked"],
        "counts": heartbeat["counts"],
        "alerts_enabled": ALERTS_ENABLED,
        "alerted": alerted,
        "per_spec_exceptions": per_spec_exceptions,
        "duration_seconds": heartbeat["duration_seconds"],
        "cycle_verdicts": cycle_verdicts,
    }
