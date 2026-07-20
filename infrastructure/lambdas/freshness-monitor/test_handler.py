"""
Unit tests for the freshness-monitor Lambda (``index.py``).

Phase 3 of the artifact-freshness-monitor arc. Pins the Lambda-level
contract: registry loading, per-spec exception isolation, heartbeat
+ check_results emission, OBSERVE-mode alert suppression, dedup-key
threading, severity routing for probe_failed.

Tests mock ``boto3.client``, ``krepis.alerts.publish``, and
``notify_via_flow_doctor`` so no live AWS or Telegram calls fire. The lib substrate
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
import types
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

# Make the Lambda handler importable.
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Hermetic guard (config#2208) ────────────────────────────────────────────
# 2026-07-11 incident: this suite's per-test `monkeypatch.setattr(index,
# "notify_via_flow_doctor", ...)` convention had one gap —
# test_handler_per_spec_exception_does_not_sink_pass enables
# FRESHNESS_MONITOR_ENABLED=true but never re-stubbed the notifier after its
# `importlib.reload(index)`, so the REAL flow_doctor_telegram.notify_via_
# flow_doctor ran whenever this file was exercised with live AWS/Telegram
# credentials ambient (laptop, EC2 box) — paging the live ops-health
# Telegram channel with fixture data (probe_bad_template / probe_missing).
#
# Fix, mirroring scheduled-groom-dispatcher's `_install_stubs` fleet
# pattern: replace `flow_doctor_telegram` in sys.modules with a safe no-op
# BEFORE any test's `import index` (or `importlib.reload(index)`) runs. Since
# index.py re-executes `from flow_doctor_telegram import
# notify_via_flow_doctor` on every reload, every reload re-binds to this
# no-op — a test can no longer reach the real notifier just by forgetting a
# monkeypatch. Individual tests still layer their own tracked Mock on top via
# `monkeypatch.setattr(index, "notify_via_flow_doctor", ...)` to assert on
# call args (dedup_key, severity, ...); that is a deliberate override for
# assertions, not a gap this stub needs to anticipate.
#
# `_real_flow_doctor_telegram` keeps a handle on the REAL module (captured
# before it's replaced below) so the deterministic owner_repo backstop added
# alongside this fix can be tested directly, in isolation from this file's
# stub.
import flow_doctor_telegram as _real_flow_doctor_telegram  # noqa: E402

_fdt_stub = types.ModuleType("flow_doctor_telegram")
_fdt_stub.notify_via_flow_doctor = lambda *a, **k: True  # type: ignore[attr-defined]
sys.modules["flow_doctor_telegram"] = _fdt_stub


# ── Hermetic guard regression coverage (config#2208) ────────────────────────


def test_notify_via_flow_doctor_is_hermetically_stubbed_by_default():
    """Regression guard for the 2026-07-11 incident: even a fresh
    ``importlib.reload(index)`` — with no per-test monkeypatch applied at
    all — must bind ``index.notify_via_flow_doctor`` to this file's no-op
    stub, never to the real ``flow_doctor_telegram.notify_via_flow_doctor``
    (which reaches live Telegram). This is what makes every test in this
    file safe by construction, not by every author remembering to stub."""
    import importlib
    import index

    importlib.reload(index)
    assert index.notify_via_flow_doctor is _fdt_stub.notify_via_flow_doctor
    assert index.notify_via_flow_doctor is not _real_flow_doctor_telegram.notify_via_flow_doctor


def test_real_notify_via_flow_doctor_refuses_test_namespace_owner_repo(monkeypatch):
    """Deterministic belt (config#2208 optional backstop): the REAL
    ``notify_via_flow_doctor`` — exercised directly here via the reference
    saved before this file's module stub replaced ``sys.modules
    ["flow_doctor_telegram"]`` — refuses to dispatch when ``context
    ["owner_repo"]`` is a test-fixture namespace, before it ever reaches
    flow-doctor init or the ``send_message`` fallback. Covers both fixture
    owner_repo values seen in the 2026-07-11 incident."""
    get_fd_mock = mock.Mock(side_effect=AssertionError("must not init flow-doctor"))
    send_message_mock = mock.Mock(side_effect=AssertionError("must not fall back to send_message"))
    monkeypatch.setattr(_real_flow_doctor_telegram, "get_flow_doctor", get_fd_mock)
    monkeypatch.setattr(_real_flow_doctor_telegram, "send_message", send_message_mock)

    for owner_repo in ("ae-test", "alpha-engine-test"):
        result = _real_flow_doctor_telegram.notify_via_flow_doctor(
            "artifact_id=probe_bad_template owner_repo=%s state=probe_failed" % owner_repo,
            silent=False,
            severity="critical",
            dedup_key="freshness_probe_bad_template_2026-W28",
            flow_name="freshness-monitor",
            topics=(),
            db_basename="flow_doctor_freshness_monitor",
            context={"artifact_id": "probe_bad_template", "owner_repo": owner_repo},
        )
        assert result is False

    get_fd_mock.assert_not_called()
    send_message_mock.assert_not_called()


def test_real_notify_via_flow_doctor_does_not_refuse_real_owner_repo(monkeypatch):
    """The backstop is scoped to the known test namespaces — a real
    owner_repo must still reach flow-doctor init, not be silently
    swallowed."""
    get_fd_mock = mock.Mock(return_value=None)
    send_message_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(_real_flow_doctor_telegram, "get_flow_doctor", get_fd_mock)
    monkeypatch.setattr(_real_flow_doctor_telegram, "send_message", send_message_mock)

    result = _real_flow_doctor_telegram.notify_via_flow_doctor(
        "artifact_id=pit_parity owner_repo=alpha-engine-data state=missing",
        silent=False,
        severity="critical",
        dedup_key="freshness_pit_parity_2026-W28",
        flow_name="freshness-monitor",
        topics=(),
        db_basename="flow_doctor_freshness_monitor",
        context={"artifact_id": "pit_parity", "owner_repo": "alpha-engine-data"},
    )
    assert result is True
    get_fd_mock.assert_called_once()


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

    def _paginate(*, Bucket, Prefix):
        # Recency model (nousergon-lib >=0.62.0): date-templated probes LIST
        # the prefix and take the newest matching object. Derive the listing
        # from the same _head_returns table so a single per-key fixture entry
        # feeds both the fixed-key HEAD path and the date-templated LIST path.
        contents = [
            {"Key": k, "LastModified": v["LastModified"]}
            for k, v in client._head_returns.items()
            if k.startswith(Prefix) and isinstance(v, dict) and "LastModified" in v
        ]
        return iter([{"Contents": contents}])

    paginator = mock.Mock()
    paginator.paginate.side_effect = _paginate

    client.head_object.side_effect = _head
    client.put_object.side_effect = _put
    client.get_object.side_effect = _get
    client.get_paginator.return_value = paginator
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


def test_load_registry_threads_active_window_fields(fake_s3):
    """The continuous active-window bound (nousergon-lib >=0.63.0) must survive
    the _SPEC_FIELDS strip and thread through to ArtifactSpec, with
    active_hours_utc coerced from a YAML list to a tuple. A deprecated
    active_trading_days_only key (removed in lib v0.102.0 / config#1334) is a
    now-unknown field and must be silently stripped, not error."""
    fake_s3._registry_body = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
artifacts:
  - artifact_id: open_orders_latest
    s3_key_template: "trades/open_orders/latest.json"
    cadence: continuous
    interval_minutes: 30
    sla_minutes_after_cron: 15
    severity: warning
    owner_repo: alpha-engine
    created_at: "2025-01-01"
    active_trading_days_only: true
    active_hours_utc: [14, 21]
"""
    import index
    spec = index.load_registry(fake_s3, "buck", "key")[0]
    assert spec.active_hours_utc == (14, 21)


def test_load_registry_threads_run_calendar(fake_s3):
    """The continuous run_calendar enum (nousergon-lib >=0.73.0) must survive
    the _SPEC_FIELDS strip and thread through to ArtifactSpec — the field that
    ties a daily trading-day producer's freshness floor to the trading
    calendar (config#1297 continuous-cadence fold-in)."""
    fake_s3._registry_body = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
artifacts:
  - artifact_id: health_alpha_engine_data
    s3_key_template: "health/daily_data.json"
    cadence: continuous
    interval_minutes: 1440
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: alpha-engine-data
    created_at: "2025-01-01"
    run_calendar: trading_days
"""
    import index
    spec = index.load_registry(fake_s3, "buck", "key")[0]
    assert spec.run_calendar == "trading_days"


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
    """OBSERVE mode (FRESHNESS_MONITOR_ENABLED unset) writes
    heartbeat + check_results but suppresses alerts.publish."""
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
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
    """Production mode (FRESHNESS_MONITOR_ENABLED=true) routes
    misses past SLA to alerts.publish with the resolved dedup key."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
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
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

    result = index.handler({}, None)

    assert result["alerts_enabled"] is True
    assert publish_mock.called
    assert notify_mock.called

    # Inspect the publish calls — dedup keys should be unique per-artifact
    # and reflect the cycle window.
    dedup_keys = [c.kwargs["dedup_key"] for c in publish_mock.call_args_list]
    # probe_missing is saturday_sf in W22 → "freshness_probe_missing_2026-W22"
    assert "freshness_probe_missing_2026-W22" in dedup_keys


def test_handler_warning_severity_console_only_no_alert(
    monkeypatch, fake_s3, fixed_now
):
    """severity=warning misses surface in check_results but do NOT page
    (no SNS / flow-doctor) — console-only per ARTIFACT_REGISTRY convention."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 0
  calendar_aware: true
artifacts:
  - artifact_id: probe_stale_warning
    s3_key_template: "path/{date}/stale.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: alpha-engine-test
    created_at: 2025-01-01
"""
    # Newest instance is from a prior cycle that has aged out of the
    # saturday_sf recency window → stale. fixed_now = Sat 2026-05-30 18:00
    # UTC, so the freshness floor is now−10d = 2026-05-20 18:00 (config#1297).
    # A 2026-05-16 instance is >10 calendar days old → state="stale",
    # sla_violated_by_minutes = (floor − last_modified) > 0.
    fake_s3._head_returns["path/2026-05-16/stale.json"] = {
        "LastModified": datetime(2026, 5, 16, 9, 30, tzinfo=timezone.utc),
    }

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))

    publish_mock = mock.Mock()
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

    result = index.handler({}, None)

    assert result["alerts_enabled"] is True
    assert result["counts"].get("stale", 0) >= 1
    publish_mock.assert_not_called()
    notify_mock.assert_not_called()

    check_body = next(
        body for (_, k, body) in fake_s3._put_calls
        if k == "_freshness_monitor/check_results.json"
    )
    check = json.loads(check_body)
    row = next(r for r in check["results"] if r["artifact_id"] == "probe_stale_warning")
    assert row["state"] == "stale"


def test_handler_probe_failed_routes_to_critical(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """probe_failed (e.g., 403) routes to critical regardless of the
    spec's severity — the monitor itself is broken; operator must know.
    Plan §3 invariant 6."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
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

    # Recency model (lib >=0.62.0) LISTs the prefix for date-templated keys —
    # make the LIST 403 for probe_fresh's prefix so the canonical probe is
    # authoritative-failed (the monitor itself is blind → probe_failed).
    def _paginate_403(*, Bucket, Prefix):
        if Prefix.startswith("path/"):
            raise _ClientError403()
        return iter([{"Contents": []}])
    fake_s3.get_paginator.return_value.paginate.side_effect = _paginate_403

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))

    publish_mock = mock.Mock()
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

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
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
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
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
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
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
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
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

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
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

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
    """A missing-past-SLA artifact with severity=critical pages via SNS +
    flow-doctor, carrying the spec's severity (not bumped). Warning-severity
    is console-only (no page) — pinned separately by
    test_maybe_alert_warning_missing_console_only; that routing split landed
    in #630 (config#1724)."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="x", s3_bucket="b", s3_key_template="k/{date}",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="critical", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    result = CheckResult(
        state="missing", sla_violated_by_minutes=120,
        canonical_key="k/2026-05-30", reason="absent",
    )
    publish_mock = mock.Mock()
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert index._maybe_alert(spec, result, fixed_now) is True
    publish_mock.assert_called_once()
    call = publish_mock.call_args
    assert "artifact_id=x" in call.args[0]
    assert call.kwargs["severity"] == "critical"  # spec severity, not bumped
    assert call.kwargs["telegram"] is False
    assert call.kwargs["dedup_key"] == "freshness_x_2026-W22"
    notify_mock.assert_called_once()
    assert notify_mock.call_args.kwargs["dedup_key"] == "freshness_x_2026-W22"


def test_maybe_alert_warning_missing_console_only(monkeypatch, fixed_now):
    """severity=warning missing-past-SLA is console-only: _maybe_alert
    returns False and NEITHER SNS publish NOR flow-doctor fires — the miss
    surfaces only in check_results.json. Pins the routing contract from #630
    (config#1724) at the unit level (handler-level surface:
    test_handler_warning_severity_console_only_no_alert)."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

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
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert index._maybe_alert(spec, result, fixed_now) is False
    publish_mock.assert_not_called()
    notify_mock.assert_not_called()


def test_maybe_alert_probe_failed_uses_critical_severity(monkeypatch, fixed_now):
    """probe_failed always escalates to critical regardless of spec."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="x", s3_bucket="b", s3_key_template="k/{date}",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="warning", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="probe_failed", reason="403")
    publish_mock = mock.Mock()
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert index._maybe_alert(spec, result, fixed_now) is True
    publish_mock.assert_called_once()
    assert publish_mock.call_args.kwargs["severity"] == "critical"
    assert publish_mock.call_args.kwargs["telegram"] is False
    notify_mock.assert_called_once()
    assert notify_mock.call_args.kwargs["severity"] == "critical"


# ── Historical-mode tests ────────────────────────────────────────────────────


def test_iter_historical_cycle_dates_saturday_returns_previous_saturdays(fixed_now):
    """Saturday cadence walks back day-by-day collecting Saturdays only.
    Verified anchor: 2026-05-28 is a Thursday; previous Saturdays are
    2026-05-23, 2026-05-16, 2026-05-09, etc."""
    import index
    dates = index._iter_historical_cycle_dates("saturday_sf", fixed_now, 3)
    assert [d.isoformat() for d in dates] == ["2026-05-23", "2026-05-16", "2026-05-09"]


def test_iter_historical_cycle_dates_weekday_returns_previous_mon_fri(fixed_now):
    """weekday_sf walks back collecting Mon-Fri only. fixed_now is Sat
    2026-05-30; previous Mon-Fri sequence is Fri 5/29, Thu 5/28, Wed
    5/27, Tue 5/26, Mon 5/25."""
    import index
    dates = index._iter_historical_cycle_dates("weekday_sf", fixed_now, 5)
    assert [d.isoformat() for d in dates] == [
        "2026-05-29", "2026-05-28", "2026-05-27", "2026-05-26", "2026-05-25",
    ]


def test_iter_historical_cycle_dates_eod_matches_weekday(fixed_now):
    """eod_sf shares the weekday cadence — confirmed by callers in
    ARTIFACT_REGISTRY.yaml (regime_state_dated, predictor_drift_detection)."""
    import index
    sat_dates = index._iter_historical_cycle_dates("weekday_sf", fixed_now, 4)
    eod_dates = index._iter_historical_cycle_dates("eod_sf", fixed_now, 4)
    assert sat_dates == eod_dates


def test_iter_historical_cycle_dates_continuous_returns_empty(fixed_now):
    """continuous cadence is intentionally skipped — current-state probe
    covers it at 15min granularity."""
    import index
    assert index._iter_historical_cycle_dates("continuous", fixed_now, 100) == []


def test_iter_historical_cycle_dates_zero_count_returns_empty(fixed_now):
    """count=0 short-circuits — early return prevents infinite loop on a
    cadence string whose weekday filter never matches."""
    import index
    assert index._iter_historical_cycle_dates("saturday_sf", fixed_now, 0) == []


def test_format_historical_key_substitutes_date_placeholder():
    import index
    assert index._format_historical_key(
        "candidates/{date}/candidates.json", date(2026, 5, 23),
    ) == "candidates/2026-05-23/candidates.json"


def test_format_historical_key_substitutes_trading_day_placeholder():
    """{trading_day} renders the same ISO date as {date} — the lib's
    placeholder set treats them as synonyms for historical-probe purposes."""
    import index
    assert index._format_historical_key(
        "predictor/predictions/{trading_day}.json", date(2026, 5, 27),
    ) == "predictor/predictions/2026-05-27.json"


def test_format_historical_key_passes_through_latest_pointer():
    """Latest-pointer templates have no placeholder — format is a no-op."""
    import index
    assert index._format_historical_key(
        "factors/profiles/latest.json", date(2026, 5, 24),
    ) == "factors/profiles/latest.json"


def test_handler_dispatches_to_historical_on_mode_flag(monkeypatch, fixed_now):
    """event={'mode': 'historical'} routes to _handle_historical without
    touching the current-state path."""
    import importlib
    import index
    importlib.reload(index)
    monkeypatch.setattr(
        index, "_handle_historical",
        mock.Mock(return_value={"mode": "historical", "n_artifacts": 0,
                                "n_cycles_probed": 0, "skipped_unsupported": 0,
                                "duration_seconds": 0.0}),
    )
    monkeypatch.setattr(index, "load_registry", mock.Mock())  # would fail otherwise
    monkeypatch.setattr(
        index, "datetime", mock.Mock(
            now=mock.Mock(return_value=fixed_now),
        ),
    )
    result = index.handler({"mode": "historical"}, None)
    assert result["mode"] == "historical"
    index._handle_historical.assert_called_once()
    index.load_registry.assert_not_called()  # current-state path NOT taken


# ── Intraday-mode probe (config#1297) ───────────────────────────────────────


def test_handler_dispatches_to_intraday_on_mode_flag(monkeypatch, fixed_now):
    """event={'mode': 'intraday'} routes to _handle_intraday without
    touching the current-state (daily full-sweep) path."""
    import importlib
    import index
    importlib.reload(index)
    monkeypatch.setattr(
        index, "_handle_intraday",
        mock.Mock(return_value={"mode": "intraday", "n_entries_checked": 0,
                                 "alerts_enabled": False, "alerted": 0,
                                 "dispatched": 0, "per_spec_exceptions": 0,
                                 "duration_seconds": 0.0}),
    )
    monkeypatch.setattr(index, "load_registry_with_recovery", mock.Mock())  # would fail otherwise
    monkeypatch.setattr(
        index, "datetime", mock.Mock(
            now=mock.Mock(return_value=fixed_now),
        ),
    )
    result = index.handler({"mode": "intraday"}, None)
    assert result["mode"] == "intraday"
    index._handle_intraday.assert_called_once()
    index.load_registry_with_recovery.assert_not_called()  # daily full-sweep path NOT taken


@pytest.fixture
def intraday_registry_body() -> bytes:
    """A registry with the two intraday artifacts plus one unrelated
    daily artifact — the intraday pass must check only the former two."""
    return b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 2
  severity: warning
artifacts:
  - artifact_id: open_orders_latest
    s3_key_template: "trades/open_orders/latest.json"
    cadence: continuous
    interval_minutes: 30
    sla_minutes_after_cron: 15
    severity: warning
    owner_repo: alpha-engine
    created_at: "2025-01-01"
    run_calendar: market_hours
    active_hours_utc: [14, 21]
  - artifact_id: freshness_monitor_heartbeat
    s3_key_template: "_freshness_monitor/heartbeat.json"
    cadence: continuous
    interval_minutes: 1440
    sla_minutes_after_cron: 15
    severity: critical
    owner_repo: alpha-engine-data
    created_at: "2025-01-01"
    run_calendar: all_days
  - artifact_id: probe_missing
    s3_key_template: "path/{date}/missing.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: critical
    owner_repo: alpha-engine-test
    created_at: "2025-01-01"
"""


def test_handle_intraday_scopes_to_intraday_artifact_ids_only(
    monkeypatch, intraday_registry_body, fake_s3, fixed_now
):
    """The intraday pass must check exactly INTRADAY_ARTIFACT_IDS, never the
    rest of the registry (probe_missing here) — that's the daily sweep's job."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = intraday_registry_body

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())

    result = index.handler({"mode": "intraday"}, None)

    assert result["mode"] == "intraday"
    assert result["n_entries_checked"] == 2  # open_orders_latest + heartbeat only


def test_handle_intraday_does_not_write_shared_dashboard_surfaces(
    monkeypatch, intraday_registry_body, fake_s3, fixed_now
):
    """The intraday pass alerts but must NOT write check_results/heartbeat/
    cycle_verdict — those full-registry surfaces are owned solely by the
    daily sweep; a partial write would clobber them with a 2-artifact view."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = intraday_registry_body

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())

    index.handler({"mode": "intraday"}, None)

    put_keys = [k for (_, k, _) in fake_s3._put_calls]
    assert index.CHECK_RESULTS_KEY not in put_keys
    assert index.HEARTBEAT_KEY not in put_keys
    assert index.CYCLE_VERDICT_KEY not in put_keys


def test_handle_intraday_warns_on_missing_expected_artifact_id(
    monkeypatch, fake_s3, fixed_now
):
    """A registry missing one of the two hardcoded intraday ids logs a
    warning rather than silently checking zero/one artifact."""
    fake_s3._registry_body = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
artifacts:
  - artifact_id: open_orders_latest
    s3_key_template: "trades/open_orders/latest.json"
    cadence: continuous
    interval_minutes: 30
    sla_minutes_after_cron: 15
    severity: warning
    owner_repo: alpha-engine
    created_at: "2025-01-01"
    run_calendar: market_hours
    active_hours_utc: [14, 21]
"""
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())

    result = index.handler({"mode": "intraday"}, None)

    assert result["n_entries_checked"] == 1  # only open_orders_latest present


# ── Trading-day-axis historical-probe tests ─────────────────────────────────


def test_iter_historical_resolves_trading_day_axis_for_saturday_sf(fixed_now):
    """When template uses {trading_day}, saturday_sf cycle dates resolve
    to the previous NYSE trading day before each Saturday. fixed_now is
    Sat 2026-05-30; prev Saturdays are 5/23, 5/16, 5/9; their
    previous_trading_day values are Fri 5/22, Fri 5/15, Fri 5/8."""
    import index
    dates = index._iter_historical_cycle_dates(
        "saturday_sf", fixed_now, 3,
        template="signals/{trading_day}/signals.json",
    )
    assert [d.isoformat() for d in dates] == [
        "2026-05-22", "2026-05-15", "2026-05-08",
    ]


def test_iter_historical_resolves_trading_day_axis_for_weekday_sf(fixed_now):
    """weekday_sf with {trading_day}: previous_trading_day of each
    weekday firing date — the AM SF fires before market open so the
    'available' trading day is the previous one. From Fri 5/29 (the
    first weekday before fixed_now Sat 5/30): prev trading day = Thu
    5/28; from Thu 5/28 → Wed 5/27; etc."""
    import index
    dates = index._iter_historical_cycle_dates(
        "weekday_sf", fixed_now, 4,
        template="predictor/predictions/{trading_day}.json",
    )
    assert [d.isoformat() for d in dates] == [
        "2026-05-28", "2026-05-27", "2026-05-26", "2026-05-22",
    ]


def test_iter_historical_resolves_eod_keeps_firing_date_for_trading_day(fixed_now):
    """eod_sf with {trading_day}: EOD writes today's data after market
    close, so trading_day == the SF firing weekday itself (no offset).
    fixed_now Sat 5/30; previous weekday firings 5/29, 5/28, 5/27."""
    import index
    dates = index._iter_historical_cycle_dates(
        "eod_sf", fixed_now, 3,
        template="regime/{trading_day}.json",
    )
    assert [d.isoformat() for d in dates] == [
        "2026-05-29", "2026-05-28", "2026-05-27",
    ]


def test_iter_historical_calendar_axis_unchanged_for_date_placeholder(fixed_now):
    """{date} placeholder keeps calendar-axis resolution (no
    previous_trading_day translation). Used by _weekly/{date}/manifest.json
    where the {date} IS the Saturday firing date."""
    import index
    dates = index._iter_historical_cycle_dates(
        "saturday_sf", fixed_now, 3,
        template="_weekly/{date}/manifest.json",
    )
    assert [d.isoformat() for d in dates] == [
        "2026-05-23", "2026-05-16", "2026-05-09",
    ]


def test_iter_historical_backward_compat_no_template_arg(fixed_now):
    """Pre-PR callers that omit template still get calendar-axis
    resolution. Required so the prior 21 tests don't regress."""
    import index
    dates = index._iter_historical_cycle_dates("saturday_sf", fixed_now, 3)
    assert [d.isoformat() for d in dates] == [
        "2026-05-23", "2026-05-16", "2026-05-09",
    ]


def test_resolve_axis_dates_holiday_skips_via_lib():
    """previous_trading_day is NYSE-holiday-aware. Memorial Day 2026-05-25
    (Mon) is a NYSE holiday; previous_trading_day(2026-05-25) returns
    Fri 5/22 (skipping the Mon holiday)."""
    from datetime import date as _date
    import index
    dates = index._resolve_axis_dates(
        [_date(2026, 5, 26)],  # Tue after Memorial Day
        template="x/{trading_day}.json",
        cadence="weekday_sf",
    )
    # Tue 5/26's prior trading day skips Mon 5/25 (Memorial Day) →
    # lands on Fri 5/22 if 5/25 is holiday-marked in the lib's calendar.
    # Don't pin a specific value here — just assert it's NOT Mon 5/25.
    assert dates[0] != _date(2026, 5, 25)
    assert dates[0] < _date(2026, 5, 26)


# ── Per-cycle completion rollup (L249 consumer) ─────────────────────────────


def _run_handler(monkeypatch, fake_s3, fixed_now, *, registry_body):
    """Invoke the handler in OBSERVE mode with boto3 routed to fake_s3
    (which also serves boto3.client('cloudwatch') — put_metric_data lands
    on the same recording mock)."""
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    fake_s3._registry_body = registry_body
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())
    return index.handler({}, None)


def _cycle_verdict_payload(fake_s3) -> dict:
    body = next(
        b for (_, k, b) in fake_s3._put_calls
        if k == "_freshness_monitor/cycle_verdict.json"
    )
    return json.loads(body)


def test_handler_emits_cycle_verdict_per_cadence(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """The registry walk is mixed-cadence; the rollup must produce ONE
    verdict per (cadence, label), never a single conflated verdict. With
    the fixture: saturday_sf critical (probe_missing) 404s → incomplete;
    continuous critical (probe_heartbeat) 404s → incomplete. probe_fresh is
    WARNING → excluded from the required set."""
    # probe_fresh fresh; the two criticals 404 by default.
    fake_s3._head_returns["path/2026-05-30/fresh.json"] = {
        "LastModified": datetime(2026, 5, 30, 12, 0, tzinfo=timezone.utc),
    }
    result = _run_handler(monkeypatch, fake_s3, fixed_now, registry_body=yaml_registry_body)

    payload = _cycle_verdict_payload(fake_s3)
    by_cadence = {v["cadence"]: v for v in payload["verdicts"]}
    assert set(by_cadence) == {"saturday_sf", "continuous"}

    sat = by_cadence["saturday_sf"]
    assert sat["state"] == "incomplete"
    assert sat["n_required"] == 1          # only probe_missing (critical); probe_fresh excluded
    assert sat["missing"] == ["probe_missing"]

    cont = by_cadence["continuous"]
    assert cont["state"] == "incomplete"
    assert cont["missing"] == ["probe_heartbeat"]

    assert result["cycle_verdicts"] == {
        "saturday_sf": "incomplete",
        "continuous": "incomplete",
    }


def test_handler_cycle_complete_when_criticals_fresh(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """All critical artifacts present+valid → every cadence complete."""
    # Saturday cycle tick is 09:00 UTC, so 12:00 is fresh.
    sat_lm = {"LastModified": datetime(2026, 5, 30, 12, 0, tzinfo=timezone.utc)}
    fake_s3._head_returns["path/2026-05-30/fresh.json"] = sat_lm
    fake_s3._head_returns["path/2026-05-30/missing.json"] = sat_lm
    # Continuous cycle tick is the current 15-min bucket (== now, 18:00), so the
    # heartbeat must be modified at/after now to count fresh.
    fake_s3._head_returns["_freshness_monitor/heartbeat.json"] = {
        "LastModified": datetime(2026, 5, 30, 18, 0, tzinfo=timezone.utc),
    }

    result = _run_handler(monkeypatch, fake_s3, fixed_now, registry_body=yaml_registry_body)
    assert result["cycle_verdicts"] == {
        "saturday_sf": "complete",
        "continuous": "complete",
    }


def test_handler_emits_cycle_completion_cw_metric(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """One ArtifactFreshnessCycleComplete datapoint per cadence, dimensioned
    by Cadence only, in the AlphaEngine/Substrate namespace."""
    fake_s3._head_returns["path/2026-05-30/fresh.json"] = {
        "LastModified": datetime(2026, 5, 30, 12, 0, tzinfo=timezone.utc),
    }
    _run_handler(monkeypatch, fake_s3, fixed_now, registry_body=yaml_registry_body)

    assert fake_s3.put_metric_data.called
    _, kwargs = fake_s3.put_metric_data.call_args
    assert kwargs["Namespace"] == "AlphaEngine/Substrate"
    md = kwargs["MetricData"]
    assert {m["MetricName"] for m in md} == {"ArtifactFreshnessCycleComplete"}
    dims = {m["Dimensions"][0]["Value"] for m in md}
    assert dims == {"saturday_sf", "continuous"}
    # Both cadences incomplete here → all values 0.0.
    assert all(m["Value"] == 0.0 for m in md)


def test_handler_cycle_rollup_failure_is_non_fatal(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """A failure in the cycle-rollup block must NOT sink the monitor — the
    primary check_results + heartbeat are still written and the handler
    returns with cycle_verdicts={}."""
    fake_s3._registry_body = yaml_registry_body
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())
    # Force the rollup to blow up.
    def _boom(*a, **kw):
        raise RuntimeError("rollup exploded")
    monkeypatch.setattr(index, "_serialize_cycle_verdicts", _boom)

    result = index.handler({}, None)

    assert result["cycle_verdicts"] == {}
    put_keys = [k for (_, k, _) in fake_s3._put_calls]
    assert "_freshness_monitor/heartbeat.json" in put_keys
    assert "_freshness_monitor/check_results.json" in put_keys
    assert "_freshness_monitor/cycle_verdict.json" not in put_keys


def test_handler_cw_emit_failure_does_not_suppress_cycle_verdict_write(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """config#1236: a CloudWatch put_metric_data failure (e.g. a grant
    regression) must NOT prevent the cycle_verdict.json S3 write — the two side
    effects are independently trapped, so the verdict artifact still lands even
    when the metric emit blows up."""
    fake_s3._registry_body = yaml_registry_body
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())
    # The metric emit (only) explodes — S3 write already happened before it.
    def _boom(*a, **kw):
        raise RuntimeError("PutMetricData AccessDenied")
    monkeypatch.setattr(index, "_emit_cycle_metrics", _boom)

    result = index.handler({}, None)

    # cycle_verdict.json was still written despite the CW failure.
    put_keys = [k for (_, k, _) in fake_s3._put_calls]
    assert "_freshness_monitor/cycle_verdict.json" in put_keys
    # The verdict map is populated (it is built before the CW emit), so a
    # downstream consumer of the return is unaffected by the metric failure.
    assert result["cycle_verdicts"] != {}


def test_handler_cycle_verdict_error_metric_on_swallowed_failure(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """config#1236: a swallowed cycle-verdict failure emits an alarmable
    ArtifactFreshnessCycleVerdictError datapoint (dimensioned by the failing
    Stage) so the silent block has a real recording surface — not only the
    absence of cycle_verdict.json."""
    fake_s3._registry_body = yaml_registry_body
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock())
    def _boom(*a, **kw):
        raise RuntimeError("serialize exploded")
    monkeypatch.setattr(index, "_serialize_cycle_verdicts", _boom)

    result = index.handler({}, None)

    assert result["cycle_verdicts"] == {}
    # An error-signal datapoint was emitted, dimensioned by the failing stage.
    error_calls = [
        kw for (_, kw) in fake_s3.put_metric_data.call_args_list
        if any(
            m["MetricName"] == "ArtifactFreshnessCycleVerdictError"
            for m in kw.get("MetricData", [])
        )
    ]
    assert error_calls, "expected an ArtifactFreshnessCycleVerdictError datapoint"
    stages = {
        m["Dimensions"][0]["Value"]
        for kw in error_calls
        for m in kw["MetricData"]
        if m["MetricName"] == "ArtifactFreshnessCycleVerdictError"
    }
    assert "serialize_or_s3_write" in stages


# ── Auto-remediation dispatch (config#1240) ─────────────────────────────────
#
# The freshness-monitor was alert-ONLY: a confirmed miss paged but never
# healed. config#1240 wires the declarative `recovery:` spec to an actual
# dispatch (SF start_execution / Lambda invoke) with per-(artifact, cycle)
# dedup so a still-missing artifact is not re-dispatched every 15-min poll.
#
# These tests mock boto3 via a per-service client factory (S3 + stepfunctions
# + lambda land on distinct recording mocks) and assert: (a) a recovery spec
# triggers exactly one dispatch on a confirmed miss, (b) no dispatch when the
# spec is absent, (c) the S3 marker dedups a re-poll.


_RECOVERY_REGISTRY = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 2
  calendar_aware: true
  severity: warning
artifacts:
  - artifact_id: closes_recoverable
    s3_key_template: "staging/daily_closes/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    severity: critical
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
    recovery:
      type: step_function
      target: "arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline"
      params:
        trigger: freshness_monitor_backfill
        trading_day: "{trading_day}"
  - artifact_id: missing_no_recovery
    s3_key_template: "staging/other/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    severity: critical
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
"""


def _make_clients(fake_s3, sf_mock=None, lambda_mock=None):
    """A boto3.client(service) factory routing each service to a distinct
    recording mock; defaults to fresh mocks for sf/lambda."""
    sf = sf_mock if sf_mock is not None else mock.Mock()
    lam = lambda_mock if lambda_mock is not None else mock.Mock()

    def _client(service, *a, **kw):
        if service == "s3":
            return fake_s3
        if service == "stepfunctions":
            return sf
        if service == "lambda":
            return lam
        # cloudwatch and anything else land on the recording fake_s3 (it has
        # put_metric_data) — mirrors the existing _run_handler convention.
        return fake_s3

    return _client, sf, lam


def _run_recovery_handler(monkeypatch, fake_s3, fixed_now, *, recovery_enabled):
    fake_s3._registry_body = _RECOVERY_REGISTRY
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)  # OBSERVE alerts
    if recovery_enabled:
        monkeypatch.setenv("FRESHNESS_MONITOR_RECOVERY_ENABLED", "true")
    else:
        monkeypatch.delenv("FRESHNESS_MONITOR_RECOVERY_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    factory, sf, lam = _make_clients(fake_s3)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=factory))
    monkeypatch.setattr(index, "publish", mock.Mock())
    result = index.handler({}, None)
    return result, sf, lam, index


def test_recovery_dispatches_once_on_confirmed_miss(monkeypatch, fake_s3, fixed_now):
    """(a) An artifact with a `recovery:` spec triggers EXACTLY one SF
    dispatch on a confirmed miss, with the resolved trading_day threaded into
    the SF input."""
    # Both artifacts 404 (missing); fixed_now Sat 18:00 is well past the
    # weekday SLA, so the miss is confirmed.
    result, sf, lam, index = _run_recovery_handler(
        monkeypatch, fake_s3, fixed_now, recovery_enabled=True
    )

    assert result["dispatched"] == 1
    sf.start_execution.assert_called_once()
    kwargs = sf.start_execution.call_args.kwargs
    assert kwargs["stateMachineArn"].endswith("ne-preopen-trading-pipeline")
    payload = json.loads(kwargs["input"])
    assert payload["trigger"] == "freshness_monitor_backfill"
    # The placeholder resolved to a concrete ISO date (not the literal token).
    assert payload["trading_day"] != "{trading_day}"
    assert payload["trading_day"].startswith("2026-05")
    lam.invoke.assert_not_called()


def test_recovery_no_dispatch_when_spec_absent(monkeypatch, fake_s3, fixed_now):
    """(b) The artifact WITHOUT a recovery spec (missing_no_recovery) is
    missing too, but no dispatch fires for it — only the one recoverable
    artifact dispatches."""
    result, sf, lam, index = _run_recovery_handler(
        monkeypatch, fake_s3, fixed_now, recovery_enabled=True
    )
    # Exactly one dispatch total → the no-recovery artifact contributed none.
    assert result["dispatched"] == 1
    assert sf.start_execution.call_count == 1


def test_recovery_writes_dedup_marker(monkeypatch, fake_s3, fixed_now):
    """A dispatch persists an in-progress marker under
    _freshness_monitor/_recovery/ so a re-poll can dedup against it."""
    _run_recovery_handler(monkeypatch, fake_s3, fixed_now, recovery_enabled=True)
    marker_puts = [
        k for (_, k, _) in fake_s3._put_calls
        if k.startswith("_freshness_monitor/_recovery/closes_recoverable/")
    ]
    assert len(marker_puts) == 1


def test_recovery_dedup_prevents_redispatch(monkeypatch, fake_s3, fixed_now):
    """(c) DEDUP — a second poll while the artifact is STILL missing must NOT
    re-dispatch: the in-progress marker (within cooldown) short-circuits."""
    # Seed the marker as already present (fresh — modified at `now`), as if a
    # prior poll dispatched. The recovery marker key embeds the cycle label.
    fake_s3._registry_body = _RECOVERY_REGISTRY
    monkeypatch.setenv("FRESHNESS_MONITOR_RECOVERY_ENABLED", "true")
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)

    from nousergon_lib.artifact_freshness import ArtifactSpec
    spec = ArtifactSpec(
        artifact_id="closes_recoverable", s3_bucket="alpha-engine-research",
        s3_key_template="staging/daily_closes/{trading_day}.parquet",
        cadence="weekday_sf", sla_minutes_after_cron=30, severity="critical",
        owner_repo="alpha-engine-data", created_at=date(2025, 1, 1),
    )
    marker_key = index._recovery_marker_key(spec, fixed_now)
    fake_s3._head_returns[marker_key] = {"LastModified": fixed_now}

    factory, sf, lam = _make_clients(fake_s3)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=factory))
    monkeypatch.setattr(index, "publish", mock.Mock())

    result = index.handler({}, None)

    assert result["dispatched"] == 0       # deduped
    sf.start_execution.assert_not_called()


def test_recovery_stale_marker_allows_redispatch(monkeypatch, fake_s3, fixed_now):
    """A marker OLDER than the cooldown window is treated as a failed prior
    heal — dispatch is allowed again (so a genuinely-stuck miss isn't stranded
    forever behind a stale marker)."""
    fake_s3._registry_body = _RECOVERY_REGISTRY
    monkeypatch.setenv("FRESHNESS_MONITOR_RECOVERY_ENABLED", "true")
    monkeypatch.setenv("RECOVERY_COOLDOWN_MINUTES", "120")
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)

    from datetime import timedelta
    from nousergon_lib.artifact_freshness import ArtifactSpec
    spec = ArtifactSpec(
        artifact_id="closes_recoverable", s3_bucket="alpha-engine-research",
        s3_key_template="staging/daily_closes/{trading_day}.parquet",
        cadence="weekday_sf", sla_minutes_after_cron=30, severity="critical",
        owner_repo="alpha-engine-data", created_at=date(2025, 1, 1),
    )
    marker_key = index._recovery_marker_key(spec, fixed_now)
    # 3h old > 120min cooldown → stale.
    fake_s3._head_returns[marker_key] = {
        "LastModified": fixed_now - timedelta(hours=3),
    }

    factory, sf, lam = _make_clients(fake_s3)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=factory))
    monkeypatch.setattr(index, "publish", mock.Mock())

    result = index.handler({}, None)
    assert result["dispatched"] == 1
    sf.start_execution.assert_called_once()


def test_recovery_observe_mode_logs_no_dispatch(monkeypatch, fake_s3, fixed_now):
    """OBSERVE gate: with FRESHNESS_MONITOR_RECOVERY_ENABLED unset, a
    recoverable miss logs the would-dispatch but calls NO AWS and writes NO
    marker — mirrors the alert OBSERVE-mode cutover discipline."""
    result, sf, lam, index = _run_recovery_handler(
        monkeypatch, fake_s3, fixed_now, recovery_enabled=False
    )
    assert result["dispatched"] == 0
    assert result["recovery_dispatch_enabled"] is False
    sf.start_execution.assert_not_called()
    marker_puts = [
        k for (_, k, _) in fake_s3._put_calls
        if k.startswith("_freshness_monitor/_recovery/")
    ]
    assert marker_puts == []


def test_recovery_dispatch_failure_does_not_sink_pass(monkeypatch, fake_s3, fixed_now):
    """A dispatch exception (e.g. SF AccessDenied) must NOT sink the monitor:
    the primary heartbeat + check_results are still written and the handler
    returns normally."""
    fake_s3._registry_body = _RECOVERY_REGISTRY
    monkeypatch.setenv("FRESHNESS_MONITOR_RECOVERY_ENABLED", "true")
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)

    sf = mock.Mock()
    sf.start_execution.side_effect = RuntimeError("States.AccessDenied")
    factory, _, lam = _make_clients(fake_s3, sf_mock=sf)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=factory))
    monkeypatch.setattr(index, "publish", mock.Mock())

    result = index.handler({}, None)

    assert result["dispatched"] == 0  # the dispatch raised → not counted
    put_keys = [k for (_, k, _) in fake_s3._put_calls]
    assert "_freshness_monitor/heartbeat.json" in put_keys
    assert "_freshness_monitor/check_results.json" in put_keys


def test_recovery_mode_dispatch_suppresses_page(monkeypatch, fake_s3, fixed_now):
    """mode: dispatch suppresses the page once a heal is dispatched this cycle
    (vs the default dispatch_and_page which does both)."""
    registry = _RECOVERY_REGISTRY.replace(
        b'        trading_day: "{trading_day}"\n',
        b'        trading_day: "{trading_day}"\n      mode: dispatch\n',
    )
    fake_s3._registry_body = registry
    monkeypatch.setenv("FRESHNESS_MONITOR_RECOVERY_ENABLED", "true")
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")  # alerts ON
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    factory, sf, lam = _make_clients(fake_s3)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=factory))
    publish_mock = mock.Mock()
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

    result = index.handler({}, None)

    assert result["dispatched"] == 1
    # closes_recoverable is healed → NOT paged. missing_no_recovery has no
    # recovery → still paged. So exactly one publish, for the no-recovery one.
    paged_ids = [c.args[0] for c in publish_mock.call_args_list]
    assert any("missing_no_recovery" in b for b in paged_ids)
    assert not any("closes_recoverable" in b for b in paged_ids)


def test_load_registry_with_recovery_parses_block(monkeypatch, fake_s3):
    """load_registry_with_recovery returns the recovery map keyed by
    artifact_id; artifacts without a block are absent from the map."""
    fake_s3._registry_body = _RECOVERY_REGISTRY
    import index
    specs, recovery, critical_arms = index.load_registry_with_recovery(
        fake_s3, "b", "k")
    assert len(specs) == 2
    assert set(recovery) == {"closes_recoverable"}
    assert recovery["closes_recoverable"]["type"] == "step_function"
    assert critical_arms == {}
    # Back-compat: load_registry still returns just the list.
    assert len(index.load_registry(fake_s3, "b", "k")) == 2


# ── config-I3086: dynamic severity + warning escalation ─────────────────────


_CHAMPION_ARM_REGISTRY = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 2
  calendar_aware: true
artifacts:
  - artifact_id: champion_feed
    s3_key_template: "predictor/research_free_backfill/feed.parquet"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: alpha-engine-test
    created_at: 2025-01-01
    critical_while_champion_arm:
      - scanner_predictor_direct
  - artifact_id: plain_warning
    s3_key_template: "path/{date}/plain.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 60
    severity: warning
    owner_repo: alpha-engine-test
    created_at: 2025-01-01
"""


def _keyed_get_object(fake_s3, extra: dict[str, bytes]) -> None:
    """Route get_object by key: registry body by default, `extra` overrides.
    A key mapped to None raises (simulates a read failure)."""
    def _get(*, Bucket, Key):
        if Key in extra:
            body = extra[Key]
            if body is None:
                raise RuntimeError(f"injected read failure for {Key}")
            return {"Body": io.BytesIO(body)}
        return {"Body": io.BytesIO(fake_s3._registry_body)}
    fake_s3.get_object.side_effect = _get


def test_load_registry_parses_critical_while_champion_arm(fake_s3):
    fake_s3._registry_body = _CHAMPION_ARM_REGISTRY
    import index
    _specs, _recovery, critical_arms = index.load_registry_with_recovery(
        fake_s3, "b", "k")
    assert critical_arms == {"champion_feed": ["scanner_predictor_direct"]}


def test_dynamic_severity_coerces_when_champion_arm_matches(fake_s3):
    fake_s3._registry_body = _CHAMPION_ARM_REGISTRY
    import index
    specs, _r, arms = index.load_registry_with_recovery(fake_s3, "b", "k")
    _keyed_get_object(fake_s3, {
        index.CHAMPION_POINTER_KEY:
            b'{"schema_version": 1, "champion": "scanner_predictor_direct"}',
    })
    coerced_specs, coerced_ids = index.apply_dynamic_severity(
        fake_s3, specs, arms)
    by_id = {s.artifact_id: s for s in coerced_specs}
    assert by_id["champion_feed"].severity == "critical"
    assert by_id["plain_warning"].severity == "warning"
    assert coerced_ids == {"champion_feed"}


def test_dynamic_severity_not_coerced_for_other_arm(fake_s3):
    fake_s3._registry_body = _CHAMPION_ARM_REGISTRY
    import index
    specs, _r, arms = index.load_registry_with_recovery(fake_s3, "b", "k")
    _keyed_get_object(fake_s3, {
        index.CHAMPION_POINTER_KEY: b'{"schema_version": 1, "champion": "think_tank"}',
    })
    coerced_specs, coerced_ids = index.apply_dynamic_severity(
        fake_s3, specs, arms)
    assert coerced_ids == set()
    assert all(s.severity == "warning" for s in coerced_specs)


def test_dynamic_severity_pointer_read_failure_fails_toward_critical(fake_s3):
    """An unreadable champion pointer must coerce LISTED rows to critical —
    fail toward paging, never toward silence."""
    fake_s3._registry_body = _CHAMPION_ARM_REGISTRY
    import index
    specs, _r, arms = index.load_registry_with_recovery(fake_s3, "b", "k")
    _keyed_get_object(fake_s3, {index.CHAMPION_POINTER_KEY: None})
    coerced_specs, coerced_ids = index.apply_dynamic_severity(
        fake_s3, specs, arms)
    by_id = {s.artifact_id: s for s in coerced_specs}
    assert coerced_ids == {"champion_feed"}
    assert by_id["champion_feed"].severity == "critical"
    assert by_id["plain_warning"].severity == "warning"


def test_dynamic_severity_no_listed_rows_skips_pointer_read(fake_s3):
    """No registry row lists a champion arm → the pointer is never read."""
    import index
    calls = []
    fake_s3.get_object.side_effect = lambda **kw: calls.append(kw)
    specs_out, coerced = index.apply_dynamic_severity(fake_s3, [], {})
    assert specs_out == [] and coerced == set()
    assert calls == []


def _warning_spec_and_missing_result(index):
    from nousergon_lib.artifact_freshness import CheckResult
    spec = index.ArtifactSpec(
        artifact_id="champion_feed",
        s3_bucket="alpha-engine-research",
        s3_key_template="predictor/research_free_backfill/feed.parquet",
        cadence="saturday_sf",
        sla_minutes_after_cron=60,
        severity="warning",
        owner_repo="alpha-engine-test",
        created_at=date(2025, 1, 1),
    )
    result = CheckResult(
        state="missing",
        reason="not found",
        canonical_key=spec.s3_key_template,
        sla_violated_by_minutes=120,
    )
    return spec, result


def test_maybe_alert_warning_escalates_after_threshold(monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock()
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    spec, result = _warning_spec_and_missing_result(index)
    assert index._maybe_alert(
        spec, result, fixed_now,
        consecutive_miss_runs=index.WARNING_ESCALATION_RUNS) is True
    body = publish_mock.call_args.args[0]
    assert "escalated_from=warning" in body
    assert publish_mock.call_args.kwargs["severity"] == "critical"


def test_maybe_alert_warning_below_threshold_stays_console_only(
        monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock()
    monkeypatch.setattr(index, "publish", publish_mock)
    spec, result = _warning_spec_and_missing_result(index)
    assert index._maybe_alert(
        spec, result, fixed_now,
        consecutive_miss_runs=index.WARNING_ESCALATION_RUNS - 1) is False
    publish_mock.assert_not_called()


def test_probe_pass_miss_counter_increments_and_resets(fake_s3, monkeypatch,
                                                       fixed_now):
    """The counter carries prev+1 on a confirmed miss and resets to 0 on a
    fresh probe — verified through _run_probe_pass with a stubbed probe."""
    import index
    from nousergon_lib.artifact_freshness import CheckResult
    spec, missing_result = _warning_spec_and_missing_result(index)
    monkeypatch.setattr(index, "_check_one",
                        lambda s3c, sp, now: (missing_result, None))
    _pairs, _a, _d, _e, counts = index._run_probe_pass(
        fake_s3, [spec], {}, fixed_now, {"champion_feed": 2})
    assert counts == {"champion_feed": 3}

    fresh_result = CheckResult(
        state="fresh", reason="ok", canonical_key=spec.s3_key_template)
    monkeypatch.setattr(index, "_check_one",
                        lambda s3c, sp, now: (fresh_result, None))
    _pairs, _a, _d, _e, counts = index._run_probe_pass(
        fake_s3, [spec], {}, fixed_now, {"champion_feed": 7})
    assert counts == {"champion_feed": 0}


def test_prev_miss_counts_roundtrip_via_check_results(fake_s3, fixed_now):
    """_serialize_check_results persists consecutive_miss_runs and
    _load_prev_miss_counts reads them back — the counter needs no new
    state surface."""
    import index
    spec, result = _warning_spec_and_missing_result(index)
    payload = index._serialize_check_results(
        [(spec, result)], fixed_now,
        miss_counts={"champion_feed": 2}, coerced_ids={"champion_feed"})
    row = payload["results"][0]
    assert row["consecutive_miss_runs"] == 2
    assert row["severity_dynamic"] is True
    _keyed_get_object(fake_s3, {
        index.CHECK_RESULTS_KEY: json.dumps(payload).encode(),
    })
    assert index._load_prev_miss_counts(fake_s3) == {"champion_feed": 2}


def test_prev_miss_counts_missing_file_resets(fake_s3):
    import index
    _keyed_get_object(fake_s3, {index.CHECK_RESULTS_KEY: None})
    assert index._load_prev_miss_counts(fake_s3) == {}
