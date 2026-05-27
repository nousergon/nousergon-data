"""
Unit tests for the freshness-monitor Lambda (``index.py``).

Phase 3 of the artifact-freshness-monitor arc. Pins the Lambda-level
contract: registry loading, per-spec exception isolation, heartbeat
+ check_results emission, OBSERVE-mode alert suppression, dedup-key
threading, severity routing for probe_failed.

Tests mock both ``boto3.client`` AND ``alpha_engine_lib.alerts.publish``
so no live AWS or Telegram calls fire. The lib substrate
(``check_freshness`` itself) is exercised through real code — only
the S3 client is mocked, mirroring the substrate's own test pattern.

See also: ``alpha-engine-lib/tests/test_artifact_freshness.py`` (the
substrate's exhaustive 37-test suite) — this file does not duplicate
those branches; it covers the Lambda-orchestration layer on top.
"""

from __future__ import annotations

import io
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

# Make the Lambda handler importable.
sys.path.insert(0, str(Path(__file__).parent))


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def yaml_registry_body() -> bytes:
    """A small but representative registry — three rows covering the
    canonical-fresh path, the missing path, and a continuous-cadence
    heartbeat. created_at=2025-01-01 puts every row well past the
    grace period."""
    return b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 2
  calendar_aware: true
  severity: warning
artifacts:
  - artifact_id: probe_fresh
    s3_key_template: "path/{date}/fresh.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: alpha-engine-test
    created_at: 2025-01-01
  - artifact_id: probe_missing
    s3_key_template: "path/{date}/missing.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: critical
    owner_repo: alpha-engine-test
    created_at: 2025-01-01
  - artifact_id: probe_heartbeat
    s3_key_template: "_freshness_monitor/heartbeat.json"
    cadence: continuous
    interval_minutes: 15
    sla_minutes_after_cron: 15
    severity: critical
    owner_repo: alpha-engine-data
    calendar_aware: false
    created_at: 2025-01-01
"""


@pytest.fixture
def fake_s3():
    """Fake boto3 S3 client tracking put_object payloads and routing
    head_object via a per-key dispatch table."""
    client = mock.Mock()
    client._put_calls: list[tuple[str, str, bytes]] = []
    client._head_returns: dict[str, dict] = {}

    def _head(*, Bucket, Key):
        if Key in client._head_returns:
            return client._head_returns[Key]
        err = _ClientError404()
        raise err

    def _put(*, Bucket, Key, Body, **kwargs):
        client._put_calls.append((Bucket, Key, Body))
        return {"ETag": '"deadbeef"'}

    def _get(*, Bucket, Key):
        return {"Body": io.BytesIO(client._registry_body)}

    client.head_object.side_effect = _head
    client.put_object.side_effect = _put
    client.get_object.side_effect = _get
    return client


class _ClientError404(Exception):
    def __init__(self):
        super().__init__("Not Found")
        self.response = {
            "Error": {"Code": "404"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }


@pytest.fixture
def fixed_now():
    """Pin ``datetime.now`` to a Saturday 18:00 UTC inside W22 so the
    saturday_sf cycle is 2026-05-30 and all SLA arithmetic is
    deterministic."""
    return datetime(2026, 5, 30, 18, 0, tzinfo=timezone.utc)


# ── load_registry ───────────────────────────────────────────────────────────


def test_load_registry_parses_and_merges_defaults(yaml_registry_body, fake_s3):
    """Defaults block must merge into each entry; per-entry keys override."""
    fake_s3._registry_body = yaml_registry_body
    import index
    specs = index.load_registry(fake_s3, "buck", "key")
    assert len(specs) == 3
    by_id = {s.artifact_id: s for s in specs}
    assert by_id["probe_fresh"].s3_bucket == "alpha-engine-research"  # from defaults
    assert by_id["probe_fresh"].grace_period_cycles == 2              # from defaults
    assert by_id["probe_missing"].severity == "critical"              # per-entry override
    assert by_id["probe_heartbeat"].calendar_aware is False           # per-entry override


def test_load_registry_raises_on_missing_artifacts_key(fake_s3):
    fake_s3._registry_body = b"schema_version: 1\nartifacts: null\n"
    import index
    with pytest.raises(ValueError, match="missing 'artifacts'"):
        index.load_registry(fake_s3, "buck", "key")


def test_load_registry_coerces_iso_date_string(fake_s3):
    """YAML safe_load returns date for ISO scalars; defensive coercion
    handles fixtures that quote the date as a string."""
    fake_s3._registry_body = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
artifacts:
  - artifact_id: probe_x
    s3_key_template: "path/{date}/x.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: ae-test
    created_at: "2025-01-01"
"""
    import index
    specs = index.load_registry(fake_s3, "buck", "key")
    assert specs[0].created_at == date(2025, 1, 1)


# ── handler — alerts disabled (OBSERVE mode) ────────────────────────────────


def _patch_now(monkeypatch, fixed):
    import index
    real_dt = index.datetime

    class _FixedDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return fixed if tz is None else fixed.astimezone(tz)

    monkeypatch.setattr(index, "datetime", _FixedDT)


def test_handler_observe_mode_does_not_alert(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """OBSERVE mode (MNEMON_FRESHNESS_MONITOR_ENABLED unset) writes
    heartbeat + check_results but suppresses alerts.publish."""
    monkeypatch.delenv("MNEMON_FRESHNESS_MONITOR_ENABLED", raising=False)
    fake_s3._registry_body = yaml_registry_body

    # Mark probe_fresh as actually fresh (HEAD returns within cycle).
    cycle_tick = datetime(2026, 5, 30, 9, 0, tzinfo=timezone.utc)
    fake_s3._head_returns["path/2026-05-30/fresh.json"] = {
        "LastModified": cycle_tick.replace(hour=12),
    }
    # probe_missing 404s by default.
    # probe_heartbeat 404s by default (will be classified missing).

    import importlib
    import index
    importlib.reload(index)  # pick up env state
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))

    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)

    result = index.handler({}, None)

    assert result["alerts_enabled"] is False
    assert result["n_entries_checked"] == 3
    assert publish_mock.call_count == 0  # OBSERVE mode

    # heartbeat + check_results both emitted regardless of OBSERVE mode.
    put_keys = [k for (_, k, _) in fake_s3._put_calls]
    assert "_freshness_monitor/heartbeat.json" in put_keys
    assert "_freshness_monitor/check_results.json" in put_keys

    # heartbeat counts reflect the three states.
    heartbeat_body = next(
        body for (_, k, body) in fake_s3._put_calls
        if k == "_freshness_monitor/heartbeat.json"
    )
    heartbeat = json.loads(heartbeat_body)
    assert heartbeat["counts"]["fresh"] == 1
    assert heartbeat["counts"]["missing"] == 2  # probe_missing + probe_heartbeat
    assert heartbeat["alerts_enabled"] is False


def test_handler_alerts_enabled_fires_with_dedup_key(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """Production mode (MNEMON_FRESHNESS_MONITOR_ENABLED=true) routes
    misses past SLA to alerts.publish with the resolved dedup key."""
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = yaml_registry_body

    cycle_tick = datetime(2026, 5, 30, 9, 0, tzinfo=timezone.utc)
    fake_s3._head_returns["path/2026-05-30/fresh.json"] = {
        "LastModified": cycle_tick.replace(hour=12),
    }
    # probe_missing 404s (past SLA — Sat 18:00 - (09:00 + 60min) = 8hr breach)
    # probe_heartbeat 404s

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))

    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)

    result = index.handler({}, None)

    assert result["alerts_enabled"] is True
    assert publish_mock.called

    # Inspect the publish calls — dedup keys should be unique per-artifact
    # and reflect the cycle window.
    dedup_keys = [c.kwargs["dedup_key"] for c in publish_mock.call_args_list]
    # probe_missing is saturday_sf in W22 → "freshness_probe_missing_2026-W22"
    assert "freshness_probe_missing_2026-W22" in dedup_keys


def test_handler_probe_failed_routes_to_critical(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """probe_failed (e.g., 403) routes to critical regardless of the
    spec's severity — the monitor itself is broken; operator must know.
    Plan §3 invariant 6."""
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = yaml_registry_body

    class _ClientError403(Exception):
        def __init__(self):
            super().__init__("Access Denied")
            self.response = {
                "Error": {"Code": "AccessDenied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            }

    def _head(*, Bucket, Key):
        if Key == "path/2026-05-30/fresh.json":
            raise _ClientError403()
        raise _ClientError404()
    fake_s3.head_object.side_effect = _head

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))

    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)

    index.handler({}, None)

    # Find the probe_fresh call (which now probe_failed) — severity should be critical
    # NOT the spec's warning.
    fresh_calls = [
        c for c in publish_mock.call_args_list
        if "probe_fresh" in c.args[0]
    ]
    assert len(fresh_calls) == 1
    assert fresh_calls[0].kwargs["severity"] == "critical"


def test_handler_per_spec_exception_does_not_sink_pass(
    monkeypatch, fake_s3, fixed_now
):
    """A malformed spec (e.g., key template requiring an unsupported
    placeholder) should result in probe_failed for that spec, not a
    handler-level raise. The other specs in the registry still get
    probed."""
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    # `{ticker}` is NOT a supported placeholder in the substrate's
    # _format_key — str.format will raise KeyError.
    fake_s3._registry_body = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
artifacts:
  - artifact_id: probe_bad_template
    s3_key_template: "path/{ticker}/x.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: ae-test
    created_at: 2025-01-01
  - artifact_id: probe_ok
    s3_key_template: "path/{date}/x.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: ae-test
    created_at: 2025-01-01
"""

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())

    result = index.handler({}, None)

    assert result["n_entries_checked"] == 2
    assert result["per_spec_exceptions"] == 1
    # Both specs landed in the heartbeat counts.
    assert sum(result["counts"].values()) == 2


def test_handler_observe_to_production_cutover_via_env_flip(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """Mirrors the mnemon 0.7.0rc4 pattern from 2026-05-24 — flipping
    the env var should change alert behavior without code redeploy.
    Tested via two reloads under different env state."""
    fake_s3._registry_body = yaml_registry_body

    # Pass 1: OBSERVE mode.
    monkeypatch.delenv("MNEMON_FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)
    r1 = index.handler({}, None)
    assert r1["alerts_enabled"] is False
    assert publish_mock.call_count == 0

    # Pass 2: env flipped to true, reload, re-invoke.
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._put_calls.clear()
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    publish_mock2 = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock2)
    r2 = index.handler({}, None)
    assert r2["alerts_enabled"] is True
    assert publish_mock2.call_count >= 1


# ── _maybe_alert direct unit coverage ───────────────────────────────────────


def test_maybe_alert_skips_fresh_state(monkeypatch, fixed_now):
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from alpha_engine_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="x", s3_bucket="b", s3_key_template="k/{date}",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="warning", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="fresh")
    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)
    assert index._maybe_alert(spec, result, fixed_now) is False
    assert publish_mock.call_count == 0


def test_maybe_alert_skips_missing_within_sla_grace(monkeypatch, fixed_now):
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from alpha_engine_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="x", s3_bucket="b", s3_key_template="k/{date}",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="warning", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    # missing but sla_violated_by_minutes=0 ⇒ still within grace; no alert.
    result = CheckResult(state="missing", sla_violated_by_minutes=0)
    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)
    assert index._maybe_alert(spec, result, fixed_now) is False
    assert publish_mock.call_count == 0


def test_maybe_alert_fires_missing_past_sla(monkeypatch, fixed_now):
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from alpha_engine_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="x", s3_bucket="b", s3_key_template="k/{date}",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="warning", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    result = CheckResult(
        state="missing", sla_violated_by_minutes=120,
        canonical_key="k/2026-05-30", reason="absent",
    )
    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)
    assert index._maybe_alert(spec, result, fixed_now) is True
    publish_mock.assert_called_once()
    call = publish_mock.call_args
    assert "artifact_id=x" in call.args[0]
    assert call.kwargs["severity"] == "warning"  # spec severity, not bumped
    assert call.kwargs["dedup_key"] == "freshness_x_2026-W22"


def test_maybe_alert_probe_failed_uses_critical_severity(monkeypatch, fixed_now):
    """probe_failed always escalates to critical regardless of spec."""
    monkeypatch.setenv("MNEMON_FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from alpha_engine_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="x", s3_bucket="b", s3_key_template="k/{date}",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="warning", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="probe_failed", reason="403")
    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)
    assert index._maybe_alert(spec, result, fixed_now) is True
    publish_mock.assert_called_once()
    assert publish_mock.call_args.kwargs["severity"] == "critical"
