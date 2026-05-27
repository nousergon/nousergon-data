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
     ``MNEMON_FRESHNESS_MONITOR_ENABLED`` is anything other than
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
from datetime import date, datetime, timezone
from typing import Any

import boto3
import yaml

from alpha_engine_lib.alerts import publish
from alpha_engine_lib.artifact_freshness import (
    ArtifactSpec,
    CheckResult,
    check_freshness,
    resolve_dedup_key,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# ── Configuration (env-driven so Phase 6 cutover is a single CLI flip) ──────

REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET", "alpha-engine-research")
REGISTRY_KEY = os.environ.get(
    "REGISTRY_KEY", "_freshness_monitor/ARTIFACT_REGISTRY.yaml"
)
HEARTBEAT_KEY = "_freshness_monitor/heartbeat.json"
CHECK_RESULTS_KEY = "_freshness_monitor/check_results.json"

# OBSERVE-mode gate. Plan §3 invariant 10 + §4 Phase 6 default. Anything
# other than literal "true" (case-insensitive) suppresses alerts. Check
# results + heartbeat are emitted regardless.
ALERTS_ENABLED = (
    os.environ.get("MNEMON_FRESHNESS_MONITOR_ENABLED", "false").lower() == "true"
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


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge cron handler — every 15min walk the registry,
    emit heartbeat + check_results, alert on misses past SLA."""
    started_at = time.time()
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3")

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
    }
