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
import time
import types
from datetime import date, datetime, timedelta, timezone
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


# ── Hermetic guard #2: no live SSM read (I7326 / §7.4a) ─────────────────────
# The owning-item resolver reads the GitHub PAT from SSM on every confirmed
# miss, so a test exercising the probe pass would otherwise reach real SSM on
# any machine with ambient AWS credentials — and, with a real PAT in hand,
# real GitHub search. Same failure shape as the 2026-07-11 flow-doctor
# incident, so it gets the same by-construction treatment rather than relying
# on every future test author remembering to stub.
#
# Patching `boto3.client` on the boto3 MODULE (not on `index`) is what makes
# this survive the `importlib.reload(index)` many tests perform: reload
# re-binds `index.boto3` to this same module object. Tests that deliberately
# replace `index.boto3` with a Mock are unaffected. The AssertionError lands
# inside the resolver's own trap, so the guard also exercises the degraded
# path — a page still fires, carrying `owning_item=unknown`.
import boto3 as _boto3  # noqa: E402

_real_boto3_client = _boto3.client


def _no_live_ssm(service, *args, **kwargs):
    if service == "ssm":
        raise AssertionError(
            "hermetic guard: a unit test attempted a live SSM read "
            "(owning-item PAT / escalation PAT). Stub it in the test."
        )
    return _real_boto3_client(service, *args, **kwargs)


_boto3.client = _no_live_ssm


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


# ── source= propagation (config-I3513) ──────────────────────────────────────
#
# ``notify_via_flow_doctor`` has no way to pass ``source`` down to
# ``krepis.telegram.send_message`` (which has no such parameter — it always
# calls ``krepis.fleet_events.emit_alert_event`` with no explicit source, so
# attribution resolves via ``_resolve_source``: explicit arg (never supplied
# on this path) > ``KREPIS_EVENT_SOURCE`` env > Lambda runtime identity). The
# fix sets ``KREPIS_EVENT_SOURCE`` for the duration of the call — these tests
# exercise the REAL module directly (mirroring the two tests above), never
# the file's hermetic stub.


def test_notify_via_flow_doctor_sets_krepis_event_source_env_during_call(monkeypatch):
    """An explicit ``source=`` must be visible as ``KREPIS_EVENT_SOURCE`` in
    the environment at the moment the Telegram send actually fires — this is
    the only lever available to attribute the resulting bus event correctly,
    since ``krepis.telegram.send_message`` has no ``source`` parameter."""
    monkeypatch.delenv("KREPIS_EVENT_SOURCE", raising=False)
    seen_source = {}

    def _fake_send_message(text, *, disable_notification=False):
        seen_source["value"] = os.environ.get("KREPIS_EVENT_SOURCE")
        return True

    monkeypatch.setattr(_real_flow_doctor_telegram, "get_flow_doctor", mock.Mock(return_value=None))
    monkeypatch.setattr(_real_flow_doctor_telegram, "send_message", _fake_send_message)

    result = _real_flow_doctor_telegram.notify_via_flow_doctor(
        "text", silent=False, severity="error", dedup_key="k",
        flow_name="freshness-monitor", topics=(), db_basename="db",
        source="freshness-monitor",
    )
    assert result is True
    assert seen_source["value"] == "freshness-monitor"
    # Restored to unset after the call — a warm Lambda container must never
    # leak one invocation's source into the next.
    assert os.environ.get("KREPIS_EVENT_SOURCE") is None


def test_notify_via_flow_doctor_no_source_leaves_krepis_event_source_untouched(monkeypatch):
    """Omitting ``source`` (the pre-fix default) must not touch
    ``KREPIS_EVENT_SOURCE`` at all — preserves whatever env-level
    attribution (or lack thereof) was already in effect, matching the
    documented backward-compat contract."""
    monkeypatch.delenv("KREPIS_EVENT_SOURCE", raising=False)
    seen_source = {}

    def _fake_send_message(text, *, disable_notification=False):
        seen_source["value"] = os.environ.get("KREPIS_EVENT_SOURCE")
        return True

    monkeypatch.setattr(_real_flow_doctor_telegram, "get_flow_doctor", mock.Mock(return_value=None))
    monkeypatch.setattr(_real_flow_doctor_telegram, "send_message", _fake_send_message)

    result = _real_flow_doctor_telegram.notify_via_flow_doctor(
        "text", silent=False, severity="error", dedup_key="k",
        flow_name="freshness-monitor", topics=(), db_basename="db",
    )
    assert result is True
    assert seen_source["value"] is None
    assert "KREPIS_EVENT_SOURCE" not in os.environ


def test_notify_via_flow_doctor_restores_prior_krepis_event_source(monkeypatch):
    """A caller invoked inside a warm container that already has
    ``KREPIS_EVENT_SOURCE`` set (e.g. by an outer chokepoint) must get that
    prior value back afterward, not have it wiped or left as this call's
    override."""
    monkeypatch.setenv("KREPIS_EVENT_SOURCE", "some-outer-source")

    monkeypatch.setattr(_real_flow_doctor_telegram, "get_flow_doctor", mock.Mock(return_value=None))
    monkeypatch.setattr(_real_flow_doctor_telegram, "send_message", mock.Mock(return_value=True))

    _real_flow_doctor_telegram.notify_via_flow_doctor(
        "text", silent=False, severity="error", dedup_key="k",
        flow_name="freshness-monitor", topics=(), db_basename="db",
        source="inner-source",
    )
    assert os.environ.get("KREPIS_EVENT_SOURCE") == "some-outer-source"


def test_notify_via_flow_doctor_sets_source_across_primary_flow_doctor_path(monkeypatch):
    """The override must also cover the primary ``fd.notify_event`` path
    (not just the ``fd is None`` fallback to ``send_message``) — that path
    is what a real, configured flow-doctor instance takes in production."""
    monkeypatch.delenv("KREPIS_EVENT_SOURCE", raising=False)
    seen_source = {}

    fake_fd = mock.Mock()

    def _fake_notify_event(subject, *, body, severity, context, dedup_key):
        seen_source["value"] = os.environ.get("KREPIS_EVENT_SOURCE")
        return "report-id-123"

    fake_fd.notify_event.side_effect = _fake_notify_event
    monkeypatch.setattr(_real_flow_doctor_telegram, "get_flow_doctor", mock.Mock(return_value=fake_fd))

    result = _real_flow_doctor_telegram.notify_via_flow_doctor(
        "text", silent=False, severity="error", dedup_key="k",
        flow_name="freshness-monitor", topics=(), db_basename="db",
        source="freshness-monitor",
    )
    assert result is True
    assert seen_source["value"] == "freshness-monitor"
    assert os.environ.get("KREPIS_EVENT_SOURCE") is None


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

    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
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

    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

    result = index.handler({}, None)

    assert result["alerts_enabled"] is True
    assert publish_mock.called
    assert notify_mock.called

    # config-I7713 — a sweep emits exactly ONE page, whose dedup key identifies
    # the condition SET. The per-artifact key this used to assert
    # ("freshness_probe_missing_2026-W22") deduped correctly per artifact and
    # was the reason 17 true statements about one cause arrived as 17 pages on
    # 2026-08-19. The artifact is still named — in the body, not in the key.
    #
    # config-I9336 — the key is no longer date-stamped (that re-baked the
    # SAME run-keyed defect one layer up: an unchanged standing condition
    # re-paged every UTC midnight). It is now a fingerprint of the open
    # EPISODE set, stable across day rollovers — see
    # test_digest_dedup_key_stable_across_day_rollover_same_episode below.
    assert publish_mock.call_count == 1, publish_mock.call_args_list
    dedup_key = publish_mock.call_args.kwargs["dedup_key"]
    assert dedup_key.startswith("freshness_digest_")
    assert "2026-05-30" not in dedup_key
    assert "artifact_id=probe_missing" in publish_mock.call_args.args[0]
    assert "driver=missing_no_instance" in publish_mock.call_args.args[0]


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

    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
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

    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))

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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
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
    publish_mock2 = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock2)
    r2 = index.handler({}, None)
    assert r2["alerts_enabled"] is True
    assert publish_mock2.call_count >= 1


# ── config-I7713: decide-then-deliver ───────────────────────────────────────
#
# A sweep now emits ONE grouped page instead of one message per artifact, so
# `_maybe_alert` decides and `_publish_digest` delivers. `_page` runs both, which
# is what the per-artifact tests below were always really asserting: given this
# spec and result, does a page go out and what does it say? The single-decision
# case is deliberately covered by the same assertions as before — grouping must
# not change severity, and must not drop a field from the body.
def _page(index_mod, spec, result, now, **kwargs) -> bool:
    decision = index_mod._alert_decision(spec, result, now, **kwargs)
    if decision is None:
        return False
    index_mod._publish_digest([decision], now)
    return True


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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    assert _page(index, spec, result, fixed_now) is False
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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    assert _page(index, spec, result, fixed_now) is False
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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert _page(index, spec, result, fixed_now) is True
    publish_mock.assert_called_once()
    call = publish_mock.call_args
    assert "artifact_id=x" in call.args[0]
    assert call.kwargs["severity"] == "critical"  # spec severity, not bumped
    assert call.kwargs["telegram"] is False
    # config-I7713: the key now identifies the CONDITION SET, not one artifact —
    # that is what lets an unchanged situation stay quiet while a new artifact
    # joining it re-pages. A per-artifact key could not express either.
    # config-I9336: no longer date-stamped — see
    # test_digest_dedup_key_stable_across_day_rollover_same_episode.
    assert call.kwargs["dedup_key"].startswith("freshness_digest_")
    assert "2026-05-30" not in call.kwargs["dedup_key"]
    assert "driver=missing_no_instance" in call.args[0]
    notify_mock.assert_called_once()
    assert notify_mock.call_args.kwargs["dedup_key"] == call.kwargs["dedup_key"]


def test_maybe_alert_telegram_suppressed_when_publish_dedup_skipped(monkeypatch, fixed_now):
    """config-I6796: when ``publish()`` reports ``dedup_skipped=True`` (an
    earlier probe already paged this same cadence window), the Telegram path
    must NOT fire a second time — even though the SLA violation is still
    active and would otherwise satisfy every severity/state gate above.
    Before this fix, ``notify_via_flow_doctor`` was called unconditionally
    regardless of the SNS channel's own dedup verdict, so a daily-cadence
    artifact re-probed by the 30-min intraday cron re-paged Telegram on every
    tick because flow-doctor's DynamoDB cooldown (1 minute,
    flow_doctor_telegram.build_flow_doctor_config) is decoupled from the
    artifact's registered cadence. ``_maybe_alert`` still returns True — the
    event was genuinely alerting, just not re-delivered to Telegram."""
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
    publish_mock = mock.Mock(
        return_value=mock.Mock(dedup_skipped=True, dedup_reason="within window")
    )
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert _page(index, spec, result, fixed_now) is True
    publish_mock.assert_called_once()
    notify_mock.assert_not_called()


def test_maybe_alert_telegram_path_carries_freshness_monitor_source(monkeypatch, fixed_now):
    """Regression guard for config-I3513: the Telegram path's
    ``notify_via_flow_doctor`` call must carry ``source="freshness-monitor"``,
    matching the SNS/bus ``publish(source=...)`` call exactly. Before the
    fix, the Telegram path passed no ``source`` at all, so attribution
    silently fell back to the Lambda's runtime ``AWS_LAMBDA_FUNCTION_NAME``
    identity (``alpha-engine-freshness-monitor``) — a string matching NO row
    in ``playbooks.yaml``'s ``alert_classes`` (confirmed live: 7 of 10
    intake events unclassified)."""
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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert _page(index, spec, result, fixed_now) is True

    publish_mock.assert_called_once()
    assert publish_mock.call_args.kwargs["source"] == "freshness-monitor"
    notify_mock.assert_called_once()
    assert notify_mock.call_args.kwargs["source"] == "freshness-monitor"
    # Both paths alert on the SAME event — the two sources must match
    # exactly, or Overseer alert-drain sees them as different classes.
    assert publish_mock.call_args.kwargs["source"] == notify_mock.call_args.kwargs["source"]


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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert _page(index, spec, result, fixed_now) is False
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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    assert _page(index, spec, result, fixed_now) is True
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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))

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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))

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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))

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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))

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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))

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
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))

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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
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
    specs, recovery, critical_arms, _esc, _rem, _pt, _do = index.load_registry_with_recovery(
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
    _specs, _recovery, critical_arms, _esc, _rem, _pt, _do = index.load_registry_with_recovery(
        fake_s3, "b", "k")
    assert critical_arms == {"champion_feed": ["scanner_predictor_direct"]}


def test_dynamic_severity_coerces_when_champion_arm_matches(fake_s3):
    fake_s3._registry_body = _CHAMPION_ARM_REGISTRY
    import index
    specs, _r, arms, _esc, _rem, _pt, _do = index.load_registry_with_recovery(fake_s3, "b", "k")
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
    specs, _r, arms, _esc, _rem, _pt, _do = index.load_registry_with_recovery(fake_s3, "b", "k")
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
    specs, _r, arms, _esc, _rem, _pt, _do = index.load_registry_with_recovery(fake_s3, "b", "k")
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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)
    spec, result = _warning_spec_and_missing_result(index)
    assert _page(index, 
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
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    spec, result = _warning_spec_and_missing_result(index)
    assert _page(index, 
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
                        lambda s3c, sp, now, **_kw: (missing_result, None))
    _pairs, _a, _d, _e, counts, _tel = index._run_probe_pass(
        fake_s3, [spec], {}, fixed_now, {"champion_feed": 2})
    assert counts == {"champion_feed": 3}

    fresh_result = CheckResult(
        state="fresh", reason="ok", canonical_key=spec.s3_key_template)
    monkeypatch.setattr(index, "_check_one",
                        lambda s3c, sp, now, **_kw: (fresh_result, None))
    _pairs, _a, _d, _e, counts, _tel = index._run_probe_pass(
        fake_s3, [spec], {}, fixed_now, {"champion_feed": 7})
    assert counts == {"champion_feed": 0}


def test_event_driven_row_probes_its_own_key_for_never_written(fake_s3, monkeypatch, fixed_now):
    """alpha-engine-config-I8810. An event_driven row's OWN `check_freshness`
    result is unconditionally `fresh` (nousergon_lib's short-circuit — no age
    floor applies), so `result.state == "missing"` can never gate this probe
    for one. `thinktank_inst_ownership` (data/inst_ownership/latest.json,
    liveness_via=thinktank_coverage_ledger) sat undetected this way: its own
    key was never once checked for existence. The probe must run off
    `spec.cadence == "event_driven"` too, independent of `result.state`."""
    import index
    spec, fresh_result = _event_driven_pair(index, "thinktank_inst_ownership",
                                            "thinktank_coverage_ledger")
    monkeypatch.setattr(index, "_check_one",
                        lambda s3c, sp, now, **_kw: (fresh_result, None))
    fake_s3.list_objects_v2 = mock.Mock(return_value={"KeyCount": 0})
    _pairs, _a, _d, _e, _counts, telemetry = index._run_probe_pass(
        fake_s3, [spec], {}, fixed_now)
    assert telemetry["never_written_by_id"] == {"thinktank_inst_ownership": True}
    fake_s3.list_objects_v2.assert_called_once()


def test_event_driven_row_confirmed_written_is_not_flagged(fake_s3, monkeypatch, fixed_now):
    """The other half: an event_driven row whose own key HAS been written at
    least once is not a producer-birth gap, so `never_written` is False, not
    True — the probe distinguishes the two rather than flagging every
    event_driven row on principle."""
    import index
    spec, fresh_result = _event_driven_pair(index, "thinktank_inst_ownership",
                                            "thinktank_coverage_ledger")
    monkeypatch.setattr(index, "_check_one",
                        lambda s3c, sp, now, **_kw: (fresh_result, None))
    fake_s3.list_objects_v2 = mock.Mock(return_value={"KeyCount": 1})
    _pairs, _a, _d, _e, _counts, telemetry = index._run_probe_pass(
        fake_s3, [spec], {}, fixed_now)
    assert telemetry["never_written_by_id"] == {"thinktank_inst_ownership": False}


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


# ── config#2055 Gap 2: extended-staleness -> Decision Queue P1 ──────────────


def test_load_registry_parses_escalate_to_issue_flag(fake_s3):
    fake_s3._registry_body = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 2
artifacts:
  - artifact_id: config_scoring_weights
    s3_key_template: "config/scoring_weights.json"
    cadence: event_driven
    liveness_via: config_apply_audit
    sla_minutes_after_cron: 360
    severity: warning
    escalate_to_issue: true
    owner_repo: alpha-engine-backtester
    created_at: 2025-01-01
  - artifact_id: config_apply_audit
    s3_key_template: "config/apply_audit/latest.json"
    cadence: saturday_sf
    sla_minutes_after_cron: 360
    severity: warning
    owner_repo: alpha-engine-backtester
    created_at: 2025-01-01
"""
    import index
    _specs, _recovery, _arms, escalate, _rem, _pt, _do = index.load_registry_with_recovery(
        fake_s3, "b", "k")
    assert escalate == {"config_scoring_weights": True}


def _event_driven_pair(index, artifact_id, liveness_via):
    from nousergon_lib.artifact_freshness import CheckResult
    spec = index.ArtifactSpec(
        artifact_id=artifact_id,
        s3_bucket="alpha-engine-research",
        s3_key_template=f"config/{artifact_id}.json",
        cadence="event_driven",
        liveness_via=liveness_via,
        sla_minutes_after_cron=360,
        severity="warning",
        owner_repo="alpha-engine-backtester",
        created_at=date(2025, 1, 1),
    )
    # event_driven rows always short-circuit to fresh (see check_freshness) —
    # _escalate_stale_key_deliverables never reads this row's own state.
    result = CheckResult(state="fresh", reason="event_driven",
                         canonical_key=spec.s3_key_template)
    return spec, result


def _anchor_pair(index, artifact_id="config_apply_audit"):
    from nousergon_lib.artifact_freshness import CheckResult
    spec = index.ArtifactSpec(
        artifact_id=artifact_id,
        s3_bucket="alpha-engine-research",
        s3_key_template=f"config/{artifact_id}/latest.json",
        cadence="saturday_sf",
        sla_minutes_after_cron=360,
        severity="warning",
        owner_repo="alpha-engine-backtester",
        created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="stale", reason="past sla",
                         canonical_key=spec.s3_key_template,
                         sla_violated_by_minutes=999)
    return spec, result


def test_escalate_files_issue_when_anchor_miss_crosses_threshold(monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    spec, result = _event_driven_pair(index, "config_scoring_weights", "config_apply_audit")
    anchor_spec, anchor_result = _anchor_pair(index)
    pairs = [(spec, result), (anchor_spec, anchor_result)]
    miss_counts = {"config_apply_audit": index.WARNING_ESCALATION_RUNS}
    file_mock = mock.Mock(return_value={"filed": True, "url": "https://github.com/x/y/issues/1"})
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)
    out = index._escalate_stale_key_deliverables(
        pairs, miss_counts, {"config_scoring_weights": True}, {}, fixed_now,
    )
    assert out == {"config_scoring_weights": "https://github.com/x/y/issues/1"}
    file_mock.assert_called_once_with(
        "config_scoring_weights", "alpha-engine-backtester",
        index.WARNING_ESCALATION_RUNS, "config_apply_audit",
        index.WARNING_ESCALATION_RUNS, None)


def test_escalate_does_not_file_below_threshold(monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    spec, result = _event_driven_pair(index, "config_scoring_weights", "config_apply_audit")
    anchor_spec, anchor_result = _anchor_pair(index)
    pairs = [(spec, result), (anchor_spec, anchor_result)]
    miss_counts = {"config_apply_audit": index.WARNING_ESCALATION_RUNS - 1}
    file_mock = mock.Mock()
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)
    out = index._escalate_stale_key_deliverables(
        pairs, miss_counts, {"config_scoring_weights": True}, {}, fixed_now,
    )
    file_mock.assert_not_called()
    assert out == {"config_scoring_weights": None}


def test_escalate_dedupes_already_filed(monkeypatch, fixed_now):
    """Still stale past threshold, but already escalated for this
    incident — must NOT re-file."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    spec, result = _event_driven_pair(index, "config_scoring_weights", "config_apply_audit")
    anchor_spec, anchor_result = _anchor_pair(index)
    pairs = [(spec, result), (anchor_spec, anchor_result)]
    miss_counts = {"config_apply_audit": index.WARNING_ESCALATION_RUNS + 5}
    file_mock = mock.Mock()
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)
    prev = {"config_scoring_weights": "https://github.com/x/y/issues/1"}
    out = index._escalate_stale_key_deliverables(
        pairs, miss_counts, {"config_scoring_weights": True}, prev, fixed_now,
    )
    file_mock.assert_not_called()
    assert out == {"config_scoring_weights": "https://github.com/x/y/issues/1"}


def test_escalate_resets_marker_on_recovery(monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    spec, result = _event_driven_pair(index, "config_scoring_weights", "config_apply_audit")
    anchor_spec, anchor_result = _anchor_pair(index)
    pairs = [(spec, result), (anchor_spec, anchor_result)]
    miss_counts = {"config_apply_audit": 0}
    file_mock = mock.Mock()
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)
    prev = {"config_scoring_weights": "https://github.com/x/y/issues/1"}
    out = index._escalate_stale_key_deliverables(
        pairs, miss_counts, {"config_scoring_weights": True}, prev, fixed_now,
    )
    file_mock.assert_not_called()
    assert out == {"config_scoring_weights": None}


def test_escalate_observe_mode_never_files(monkeypatch, fixed_now):
    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    import importlib
    import index
    importlib.reload(index)
    spec, result = _event_driven_pair(index, "config_scoring_weights", "config_apply_audit")
    anchor_spec, anchor_result = _anchor_pair(index)
    pairs = [(spec, result), (anchor_spec, anchor_result)]
    miss_counts = {"config_apply_audit": index.WARNING_ESCALATION_RUNS + 5}
    file_mock = mock.Mock()
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)
    out = index._escalate_stale_key_deliverables(
        pairs, miss_counts, {"config_scoring_weights": True}, {}, fixed_now,
    )
    file_mock.assert_not_called()
    assert out == {"config_scoring_weights": None}


def test_escalate_uses_own_miss_count_for_non_event_driven(monkeypatch, fixed_now):
    """A (hypothetically) flagged non-event_driven row uses its own
    consecutive_miss_runs directly, not an anchor's."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    anchor_spec, anchor_result = _anchor_pair(index, artifact_id="some_direct_artifact")
    pairs = [(anchor_spec, anchor_result)]
    miss_counts = {"some_direct_artifact": index.WARNING_ESCALATION_RUNS}
    file_mock = mock.Mock(return_value={"filed": True, "url": "https://x/1"})
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)
    out = index._escalate_stale_key_deliverables(
        pairs, miss_counts, {"some_direct_artifact": True}, {}, fixed_now,
    )
    file_mock.assert_called_once_with(
        "some_direct_artifact", "alpha-engine-backtester",
        index.WARNING_ESCALATION_RUNS, "some_direct_artifact",
        index.WARNING_ESCALATION_RUNS, None)
    assert out == {"some_direct_artifact": "https://x/1"}


def test_escalate_no_flagged_artifacts_is_a_noop(fixed_now):
    import index
    out = index._escalate_stale_key_deliverables([], {}, {}, {}, fixed_now)
    assert out == {}


def test_file_escalation_issue_posts_expected_shape(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    ssm_client = mock.Mock()
    ssm_client.get_parameter.return_value = {"Parameter": {"Value": "fake-pat-token"}}
    monkeypatch.setattr(index, "boto3", mock.Mock(client=mock.Mock(return_value=ssm_client)))

    captured = {}

    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps({
                "html_url": "https://github.com/nousergon/alpha-engine-config/issues/9999",
            }).encode()

    def _fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.headers)
        captured["body"] = json.loads(req.data)
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", _fake_urlopen)

    result = index._file_escalation_issue(
        "config_scoring_weights", "alpha-engine-backtester", 20,
        "config_apply_audit", 3)

    assert result == {
        "filed": True,
        "url": "https://github.com/nousergon/alpha-engine-config/issues/9999",
    }
    assert captured["url"] == "https://api.github.com/repos/nousergon/alpha-engine-config/issues"
    assert captured["body"]["labels"] == ["P1", "gate:operator", "area:infrastructure"]
    assert "config_scoring_weights" in captured["body"]["title"]
    body = captured["body"]["body"]
    for marker in ("**Summary:**", "**Ask:**", "**Options:**", "**SOTA:**",
                   "**Delta:**", "**Consequence of no action:**"):
        assert marker in body, f"missing Ask-block field {marker!r}"
    assert any(k.lower() == "authorization" for k in captured["headers"])


def test_file_escalation_issue_failure_is_non_fatal(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    monkeypatch.setattr(index, "boto3", mock.Mock(
        client=mock.Mock(side_effect=RuntimeError("ssm down"))))
    result = index._file_escalation_issue(
        "config_scoring_weights", "alpha-engine-backtester", 20,
        "config_apply_audit", 3)
    assert result["filed"] is False
    assert "ssm down" in result["error"]


def test_issue_filed_url_roundtrip_via_check_results(fake_s3, fixed_now):
    import index
    spec, result = _event_driven_pair(index, "config_scoring_weights", "config_apply_audit")
    payload = index._serialize_check_results(
        [(spec, result)], fixed_now,
        issue_filed_by_id={"config_scoring_weights": "https://github.com/x/y/issues/1"},
    )
    row = payload["results"][0]
    assert row["issue_filed_url"] == "https://github.com/x/y/issues/1"
    _keyed_get_object(fake_s3, {
        index.CHECK_RESULTS_KEY: json.dumps(payload).encode(),
    })
    assert index._load_prev_issue_filed(fake_s3) == {
        "config_scoring_weights": "https://github.com/x/y/issues/1",
    }


def test_prev_issue_filed_missing_file_resets(fake_s3):
    import index
    _keyed_get_object(fake_s3, {index.CHECK_RESULTS_KEY: None})
    assert index._load_prev_issue_filed(fake_s3) == {}


# ── config-I3282: freshness-critical → overseer drain dispatch ──────────────
#
# Eligibility (pinned by these tests): a row joins the pass's ONE aggregated
# drain dispatch iff its critical page actually fired AND it has no
# `recovery:` heal of its own AND it is not declared `remediation: operator`.
# The dispatch is flag-gated (OBSERVE default), globally cooldown-deduped via
# an S3 marker, async (Event) to the overseer router, and independently
# trapped so a failure can never sink the sweep.

_DRAIN_REGISTRY = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 2
  calendar_aware: true
  severity: warning
artifacts:
  - artifact_id: crit_dispatchable
    s3_key_template: "staging/dispatchable/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    severity: critical
    remediation: dispatch-diagnose
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
  - artifact_id: crit_operator
    s3_key_template: "staging/operator/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    severity: critical
    remediation: operator
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
  - artifact_id: crit_healed
    s3_key_template: "staging/healed/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    severity: critical
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
    recovery:
      type: lambda
      target: "some-backfill-fn"
  - artifact_id: warn_quiet
    s3_key_template: "staging/quiet/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    severity: warning
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
"""


def _run_drain_handler(monkeypatch, fake_s3, fixed_now, *, drain_enabled,
                       registry=_DRAIN_REGISTRY):
    """Full-sweep run with alerts ENABLED (pages fire) and recovery dispatch
    in OBSERVE (so the `crit_healed` row's exclusion is purely declarative,
    not an in-flight-heal artifact)."""
    fake_s3._registry_body = registry
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    monkeypatch.delenv("FRESHNESS_MONITOR_RECOVERY_ENABLED", raising=False)
    if drain_enabled:
        monkeypatch.setenv("FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED", "true")
    else:
        monkeypatch.delenv(
            "FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED", raising=False
        )
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    factory, sf, lam = _make_clients(fake_s3)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=factory))
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    result = index.handler({}, None)
    return result, sf, lam, index


def _drain_invokes(lam):
    return [
        c for c in lam.invoke.call_args_list
        if c.kwargs.get("FunctionName") == "alpha-engine-overseer-dispatcher"
    ]


def test_load_registry_parses_remediation_map(monkeypatch, fake_s3):
    """The loader returns the declared-lane map keyed by artifact_id;
    undeclared rows are simply absent."""
    import index
    fake_s3._registry_body = _DRAIN_REGISTRY
    _s, _r, _a, _e, remediation, _pt, _do = index.load_registry_with_recovery(
        fake_s3, "b", "k"
    )
    assert remediation == {
        "crit_dispatchable": "dispatch-diagnose",
        "crit_operator": "operator",
    }


def test_drain_dispatch_observe_mode_no_invoke(monkeypatch, fake_s3, fixed_now):
    """Flag off (default): pages fire, but NO router invoke and NO marker —
    the would-dispatch is log-only."""
    result, _sf, lam, _index = _run_drain_handler(
        monkeypatch, fake_s3, fixed_now, drain_enabled=False
    )
    assert result["alerted"] >= 1
    assert result["drain_dispatch_enabled"] is False
    assert _drain_invokes(lam) == []
    marker_puts = [
        k for (_, k, _) in fake_s3._put_calls
        if k.startswith("_freshness_monitor/_dispatch/")
    ]
    assert marker_puts == []


def test_drain_dispatch_fires_once_for_eligible_criticals(
    monkeypatch, fake_s3, fixed_now
):
    """Flag on: the three critical rows all page, but exactly ONE router
    invoke fires (aggregated), carrying the alert-drain playbook envelope,
    async, and a dedup marker is written."""
    result, _sf, lam, _index = _run_drain_handler(
        monkeypatch, fake_s3, fixed_now, drain_enabled=True
    )
    assert result["alerted"] == 3  # dispatchable + operator + healed all page
    invokes = _drain_invokes(lam)
    assert len(invokes) == 1
    call = invokes[0]
    assert call.kwargs["InvocationType"] == "Event"
    payload = json.loads(call.kwargs["Payload"])
    assert payload["playbook"] == "alert-drain"
    assert payload["payload"]["trigger"] == "freshness-critical"
    assert payload["payload"]["is_drill"] == "false"
    marker_puts = [
        (k, body) for (_, k, body) in fake_s3._put_calls
        if k == "_freshness_monitor/_dispatch/last_drain_dispatch.json"
    ]
    assert len(marker_puts) == 1
    marker = json.loads(marker_puts[0][1])
    # Only the declared-dispatchable row is a candidate — the operator row
    # is page-only by declaration, the recovery row's lane is its heal.
    assert marker["artifact_ids"] == ["crit_dispatchable"]


def test_drain_dispatch_skips_when_no_eligible_candidates(
    monkeypatch, fake_s3, fixed_now
):
    """A sweep whose only critical pages are operator-declared or
    recovery-bearing rows dispatches nothing."""
    registry = b"".join(
        ln for ln in _DRAIN_REGISTRY.splitlines(keepends=True)
    ).replace(b"remediation: dispatch-diagnose", b"remediation: operator")
    result, _sf, lam, _index = _run_drain_handler(
        monkeypatch, fake_s3, fixed_now, drain_enabled=True, registry=registry
    )
    assert result["alerted"] == 3
    assert _drain_invokes(lam) == []


def test_drain_dispatch_cooldown_dedup(monkeypatch, fake_s3, fixed_now):
    """A fresh marker (within cooldown) suppresses the dispatch."""
    fake_s3._head_returns[
        "_freshness_monitor/_dispatch/last_drain_dispatch.json"
    ] = {"LastModified": fixed_now}
    _result, _sf, lam, _index = _run_drain_handler(
        monkeypatch, fake_s3, fixed_now, drain_enabled=True
    )
    assert _drain_invokes(lam) == []


def test_drain_dispatch_stale_marker_redispatches(
    monkeypatch, fake_s3, fixed_now
):
    """A marker OLDER than the cooldown is a spent dispatch — fire again."""
    from datetime import timedelta
    fake_s3._head_returns[
        "_freshness_monitor/_dispatch/last_drain_dispatch.json"
    ] = {"LastModified": fixed_now - timedelta(minutes=999)}
    _result, _sf, lam, _index = _run_drain_handler(
        monkeypatch, fake_s3, fixed_now, drain_enabled=True
    )
    assert len(_drain_invokes(lam)) == 1


def test_drain_dispatch_failure_does_not_sink_pass(
    monkeypatch, fake_s3, fixed_now
):
    """A router-invoke failure is trapped: the sweep's primary deliverables
    (alerts, heartbeat, check_results) land regardless."""
    fake_s3._registry_body = _DRAIN_REGISTRY
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    monkeypatch.setenv("FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    lam = mock.Mock()
    lam.invoke.side_effect = RuntimeError("router down")
    factory, _sf, _lam = _make_clients(fake_s3, lambda_mock=lam)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=factory))
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))

    result = index.handler({}, None)  # must not raise

    assert result["alerted"] == 3
    put_keys = [k for (_, k, _) in fake_s3._put_calls]
    assert "_freshness_monitor/heartbeat.json" in put_keys
    assert "_freshness_monitor/check_results.json" in put_keys


# ── Producer-trigger suppression (alpha-engine-config-I6570) ────────────────
#
# The property under test is narrow and load-bearing: a deliberately-disabled
# producer must remove the PAGE and nothing else. The row's state, its
# severity, and its presence on the console surface are all unchanged, and any
# failure to establish that the producer is off leaves the page intact.

_PRODUCER_REGISTRY = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
  grace_period_cycles: 2
  calendar_aware: true
  severity: warning
artifacts:
  - artifact_id: paused_producer
    s3_key_template: "thinktank/challenger_selection/latest.json"
    cadence: continuous
    interval_minutes: 1440
    sla_minutes_after_cron: 720
    severity: critical
    producer_trigger: "events:alpha-research-thinktank-daily"
    owner_repo: crucible-research
    created_at: 2025-01-01
  - artifact_id: scheduler_producer
    s3_key_template: "overseer/drain/latest.json"
    cadence: continuous
    interval_minutes: 360
    sla_minutes_after_cron: 60
    severity: critical
    producer_trigger: "scheduler:alpha-engine-alert-drain-0400utc"
    owner_repo: nousergon-data
    created_at: 2025-01-01
  - artifact_id: malformed_producer
    s3_key_template: "staging/x/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    producer_trigger: "alpha-research-thinktank-daily"
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
  - artifact_id: no_producer
    s3_key_template: "staging/y/{trading_day}.parquet"
    cadence: weekday_sf
    sla_minutes_after_cron: 30
    owner_repo: alpha-engine-data
    created_at: 2025-01-01
"""


def _events(state):
    c = mock.Mock()
    c.describe_rule.return_value = {"State": state}
    return c


def _scheduler(state):
    c = mock.Mock()
    c.get_schedule.return_value = {"State": state}
    return c


def test_parse_producer_trigger_grammar():
    import index
    assert index.parse_producer_trigger("events:r") == ("events", "r")
    assert index.parse_producer_trigger("scheduler:s") == ("scheduler", "s")
    assert index.parse_producer_trigger(" events : r ") == ("events", "r")
    # Anything that is not the declared grammar must not suppress.
    for bad in (None, "", "r", "lambda:r", "events:", ":r", 42, ["events:r"]):
        assert index.parse_producer_trigger(bad) is None


def test_loader_parses_producer_trigger_and_drops_malformed(fake_s3):
    """A well-formed trigger is carried; a malformed one is dropped rather
    than raising — the field's job is to REMOVE a page, so a typo degrades to
    today's alerting behaviour instead of taking the registry down."""
    import index
    fake_s3._registry_body = _PRODUCER_REGISTRY
    specs, _r, _a, _e, _rem, producer, _do = index.load_registry_with_recovery(
        fake_s3, "b", "k"
    )
    assert {s.artifact_id for s in specs} == {
        "paused_producer", "scheduler_producer", "malformed_producer", "no_producer",
    }
    # Values are TUPLES since config-I7509 — a row may declare several
    # producers and is suppressed only when every one of them is off.
    assert producer == {
        "paused_producer": ("events:alpha-research-thinktank-daily",),
        "scheduler_producer": ("scheduler:alpha-engine-alert-drain-0400utc",),
    }


def test_resolve_disabled_producers_reads_live_state():
    import index
    assert index.resolve_disabled_producers(
        {"events:r"}, events_client=_events("DISABLED")
    ) == {"events:r": "EventBridge rule r is DISABLED"}
    assert index.resolve_disabled_producers(
        {"events:r"}, events_client=_events("ENABLED")
    ) == {}
    assert index.resolve_disabled_producers(
        {"scheduler:s"}, scheduler_client=_scheduler("DISABLED")
    ) == {"scheduler:s": "Scheduler schedule s is DISABLED"}


def test_resolve_disabled_producers_fails_toward_paging():
    """An unresolvable trigger — denied, throttled, renamed, absent — is
    treated as ENABLED. A suppression path that fails open is a monitor that
    an IAM regression can silence."""
    import index
    boom = mock.Mock()
    boom.describe_rule.side_effect = RuntimeError("AccessDeniedException")
    assert index.resolve_disabled_producers({"events:r"}, events_client=boom) == {}


def test_apply_producer_suppression_stamps_first_observation(fake_s3, fixed_now):
    import index
    fake_s3._registry_body = b"not json"  # disabled_since store absent/unreadable
    out = index.apply_producer_suppression(
        fake_s3,
        {"paused_producer": "events:r"},
        fixed_now,
        events_client=_events("DISABLED"),
    )
    assert out["paused_producer"]["suppressed"] is True
    assert out["paused_producer"]["disabled_since"] == fixed_now.date().isoformat()
    assert out["paused_producer"]["days_disabled"] == 0
    # First observation is persisted so the expiry clock survives the invocation.
    assert any(
        key == index.PRODUCER_DISABLED_SINCE_KEY
        for _bucket, key, _body in fake_s3._put_calls
    )


def test_apply_producer_suppression_lapses_past_max_days(fake_s3, fixed_now, monkeypatch):
    """A pause that becomes permanent must not become a permanent blindfold:
    past the ceiling the row is still annotated but pages again."""
    import index
    monkeypatch.setattr(index, "PRODUCER_SUPPRESSION_MAX_DAYS", 14)
    monkeypatch.setattr(
        index, "_load_producer_disabled_since",
        lambda _s3: {"events:r": "2026-05-01"},   # 29 days before fixed_now
    )
    out = index.apply_producer_suppression(
        fake_s3, {"a": "events:r"}, fixed_now, events_client=_events("DISABLED")
    )
    assert out["a"]["days_disabled"] == 29
    assert out["a"]["suppressed"] is False


def test_apply_producer_suppression_clears_a_re_enabled_trigger(fake_s3, fixed_now, monkeypatch):
    import index
    saved: list[dict] = []
    monkeypatch.setattr(
        index, "_load_producer_disabled_since", lambda _s3: {"events:r": "2026-05-01"}
    )
    monkeypatch.setattr(
        index, "_save_producer_disabled_since",
        lambda _s3, mapping: saved.append(mapping),
    )
    out = index.apply_producer_suppression(
        fake_s3, {"a": "events:r"}, fixed_now, events_client=_events("ENABLED")
    )
    assert out == {}            # enabled producer ⇒ no annotation at all
    assert saved == [{}]        # and the clock is reset, not carried forward


def test_maybe_alert_suppressed_when_producer_disabled(monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="paused_producer", s3_bucket="b", s3_key_template="k",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="critical", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="missing", sla_violated_by_minutes=999)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock())

    suppression = {
        "trigger": "events:r", "reason": "EventBridge rule r is DISABLED",
        "disabled_since": "2026-05-29", "days_disabled": 1, "suppressed": True,
    }
    assert _page(index, 
        spec, result, fixed_now, producer_suppression=suppression) is False
    assert publish_mock.call_count == 0

    # Same critical row, same miss — suppression lapsed ⇒ it pages.
    assert _page(index, 
        spec, result, fixed_now,
        producer_suppression={**suppression, "suppressed": False}) is True
    assert publish_mock.call_count == 1


def test_check_results_row_records_suppression_without_hiding_the_state(fixed_now):
    """A suppressed row keeps its TRUE state on the console surface. The
    console must be able to render 'stale — producer disabled'; it must never
    render green and the row must never be omitted (principles.md §2.7)."""
    import index
    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="paused_producer", s3_bucket="b", s3_key_template="k",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="critical", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="stale", sla_violated_by_minutes=4320)
    payload = index._serialize_check_results(
        [(spec, result)], fixed_now,
        suppression_by_id={
            "paused_producer": {
                "trigger": "events:alpha-research-thinktank-daily",
                "reason": "EventBridge rule alpha-research-thinktank-daily is DISABLED",
                "disabled_since": "2026-05-20", "days_disabled": 10,
                "suppressed": True,
            }
        },
    )
    row = payload["results"][0]
    assert row["state"] == "stale"                     # not rewritten
    assert row["severity"] == "critical"               # not downgraded
    assert row["sla_violated_by_minutes"] == 4320      # not zeroed
    assert row["producer_disabled"] is True
    assert row["alert_suppressed"] is True
    assert row["producer_trigger"] == "events:alpha-research-thinktank-daily"
    assert row["producer_disabled_since"] == "2026-05-20"


def test_check_results_row_defaults_are_inert_without_suppression(fixed_now):
    import index
    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

    spec = ArtifactSpec(
        artifact_id="no_producer", s3_bucket="b", s3_key_template="k",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="warning", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    row = index._serialize_check_results(
        [(spec, CheckResult(state="fresh"))], fixed_now
    )["results"][0]
    assert row["producer_disabled"] is False
    assert row["alert_suppressed"] is False
    assert row["producer_trigger"] is None
    assert row["producer_disabled_since"] is None


# ── config-I6817 D4: the account-wide disabled-schedule inventory ───────────
#
# The 2026-08-07 pause disabled 23 EventBridge rules. `alpha-research-thinktank
# -daily` then stayed off for three days after the provider-balance condition
# that justified it was resolved, because I6617 was CLOSED and NO SURFACE
# ANYWHERE listed which rules were off. The suppression path could not have
# caught it: it only walks triggers some ARTIFACT_REGISTRY row names.


class _FakeEventsPaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self):
        return iter(self._pages)


class _FakeEvents:
    def __init__(self, rules, raises=False):
        self._rules = rules
        self._raises = raises

    def get_paginator(self, _name):
        if self._raises:
            raise RuntimeError("AccessDenied enumerating rules")
        return _FakeEventsPaginator([{"Rules": self._rules}])


class _FakeScheduler:
    def __init__(self, schedules=None):
        self._schedules = schedules or []

    def get_paginator(self, _name):
        return _FakeEventsPaginator([{"Schedules": self._schedules}])


def test_inventory_enumerates_disabled_rules_no_registry_row_names():
    """The load-bearing case. A disabled rule that no artifact row references
    is invisible to every other surface in the fleet — both probes paused on
    2026-08-07 are in that class."""
    import importlib
    import index
    importlib.reload(index)
    events = _FakeEvents([
        {"Name": "alpha-research-thinktank-daily", "State": "DISABLED",
         "ScheduleExpression": "cron(30 14 * * ? *)"},
        {"Name": "alpha-engine-ssm-reachability-probe-5min", "State": "DISABLED",
         "ScheduleExpression": "rate(5 minutes)"},
        {"Name": "alpha-engine-saturday", "State": "ENABLED",
         "ScheduleExpression": "cron(0 9 * * 6 *)"},
    ])
    rows = index.enumerate_disabled_schedules(events, _FakeScheduler())
    names = {r["name"] for r in rows}
    assert names == {
        "alpha-research-thinktank-daily",
        "alpha-engine-ssm-reachability-probe-5min",
    }, "ENABLED rules must not appear; both DISABLED ones must"


def test_an_unenumerable_surface_is_an_error_row_never_an_empty_inventory():
    """An empty inventory must mean 'nothing is disabled', never 'the walk
    failed' — that conflation is the defect this whole feature exists to
    close, reproduced one level up."""
    import importlib
    import index
    importlib.reload(index)
    rows = index.enumerate_disabled_schedules(
        _FakeEvents([], raises=True), _FakeScheduler()
    )
    errs = [r for r in rows if r.get("error")]
    assert errs, "a failed enumeration produced no error row — silent empty"
    assert "AccessDenied" in errs[0]["error"]


def test_the_payload_flags_rows_no_artifact_row_references(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    events = _FakeEvents([
        {"Name": "alpha-research-thinktank-daily", "State": "DISABLED",
         "ScheduleExpression": "cron(30 14 * * ? *)"},
        {"Name": "alpha-engine-router-exposure-probe-15min", "State": "DISABLED",
         "ScheduleExpression": "rate(15 minutes)"},
    ])
    monkeypatch.setattr(
        index, "_load_producer_disabled_since",
        lambda _s: {"events:alpha-research-thinktank-daily": "2026-08-07"},
    )

    class _S3:
        def __init__(self):
            self.put = None

        def put_object(self, **kw):
            self.put = kw

    s3 = _S3()
    now = datetime(2026, 8, 10, tzinfo=timezone.utc)
    payload = index.write_disabled_producer_inventory(
        s3, now, {"events:alpha-research-thinktank-daily"}, events, _FakeScheduler()
    )

    assert payload["complete"] is True
    assert payload["disabled_count"] == 2
    assert payload["unreferenced_count"] == 1, (
        "the router-exposure probe is named by no artifact row and must be "
        "counted as unreferenced — that is the class the thinktank rule's "
        "three-day overrun belongs to"
    )
    by_name = {r["name"]: r for r in payload["rows"]}
    tt = by_name["alpha-research-thinktank-daily"]
    assert tt["disabled_since"] == "2026-08-07"
    assert tt["days_disabled"] == 3, "age must come from producer_disabled_since"
    assert tt["referenced_by_registry"] is True
    assert by_name["alpha-engine-router-exposure-probe-15min"][
        "referenced_by_registry"] is False
    assert s3.put is not None, "the inventory was not written"
    assert s3.put["Key"] == index.DISABLED_PRODUCER_INVENTORY_KEY


def test_a_failed_write_does_not_take_down_the_sweep():
    """This is an observability side effect. The sweep's alerting has already
    run by the time it is called; raising here would trade a real page for a
    bookkeeping failure."""
    import importlib
    import index
    importlib.reload(index)
    class _S3:
        def put_object(self, **kw):
            raise RuntimeError("s3 down")

    payload = index.write_disabled_producer_inventory(
        _S3(), datetime(2026, 8, 10, tzinfo=timezone.utc), set(),
        _FakeEvents([]), _FakeScheduler(),
    )
    assert payload["disabled_count"] == 0


# ── §7.4a: a page names the item that owns it (I7326) ───────────────────────
#
# The measured instance these pin: 2026-08-14 08:00 UTC, `[CRITICAL]
# freshness-monitor: artifact_id=director_retro ... escalated_from=warning
# after_consecutive_miss_runs=13` — while alpha-engine-config-I6562 had
# root-caused the exact condition nine days earlier and three further open
# P1s (#6155, #6345, #6747) described it. The page named none of them.


def _owning(number=6562, priority="P1", age_days=9.0, sla_days=3,
            members=(), degraded=False, reason=None, title="migrate "
            "director-retro-judge off direct OpenRouter"):
    return {
        "resolved": True,
        "degraded": degraded,
        "degraded_reason": reason,
        "owning_item": {
            "number": number,
            "url": f"https://github.com/nousergon/alpha-engine-config/issues/{number}",
            "title": title,
            "priority": priority,
            "sla_days": sla_days,
            "age_days": age_days,
            "created_at": "2026-08-05T00:00:00Z",
            "n_artifacts_named": 1,
        },
        "members": list(members),
        "n_candidates": 1 + len(members),
    }


def _critical_spec_and_missing_result(index, artifact_id="director_retro"):
    from nousergon_lib.artifact_freshness import CheckResult
    spec = index.ArtifactSpec(
        artifact_id=artifact_id,
        s3_bucket="alpha-engine-research",
        s3_key_template="director/{date}/retro.json",
        cadence="weekday_sf",
        sla_minutes_after_cron=60,
        severity="critical",
        owner_repo="alpha-engine-test",
        created_at=date(2025, 1, 1),
    )
    result = CheckResult(
        state="stale",
        reason="past sla",
        canonical_key="director/2026-08-13/retro.json",
        sla_violated_by_minutes=31649,
    )
    return spec, result


def test_page_names_the_already_open_owning_item(monkeypatch, fixed_now):
    """Clause (a): the identifier, priority and age of the open item are in
    the delivered body. This is the exact fact the 2026-08-14 page omitted."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _critical_spec_and_missing_result(index)

    assert _page(index, spec, result, fixed_now,
                              owning=_owning()) is True
    body = publish_mock.call_args.args[0]
    assert "owning_item=#6562" in body
    assert "owning_item_priority=P1" in body
    assert "owning_item_age_days=9.0" in body
    assert "issues/6562" in body


def test_page_lists_the_other_items_describing_the_same_condition(
        monkeypatch, fixed_now):
    """Clause (a) + the many-items-one-cause rule: one page, naming the
    owner, listing the rest as members."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _critical_spec_and_missing_result(index)
    members = [{"number": n} for n in (6155, 6345, 6747)]

    assert _page(index, spec, result, fixed_now,
                              owning=_owning(members=members)) is True
    body = publish_mock.call_args.args[0]
    assert "owning_item=#6562" in body
    assert "owning_item_members=#6155,#6345,#6747" in body
    assert "owning_item_members_total=3" in body
    # Exactly ONE page for the group.
    assert publish_mock.call_count == 1


def test_degraded_owning_item_search_still_pages_and_says_so(
        monkeypatch, fixed_now):
    """Fail SOFT, never silent: an unreachable tracker must not decide
    whether the operator is paged, and 'unknown' must not read as 'none'."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _critical_spec_and_missing_result(index)

    assert _page(index, 
        spec, result, fixed_now,
        owning=index._unresolved("api.github.com: URLError: timed out"),
    ) is True
    body = publish_mock.call_args.args[0]
    assert "owning_item=unknown" in body
    assert "owning_item_lookup=degraded" in body
    assert "timed out" in body
    assert "owning_item=none" not in body


def test_no_open_item_says_none_not_unknown(monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _critical_spec_and_missing_result(index)
    empty = {"resolved": True, "degraded": False, "degraded_reason": None,
             "owning_item": None, "members": [], "n_candidates": 0}

    assert _page(index, spec, result, fixed_now, owning=empty) is True
    body = publish_mock.call_args.args[0]
    assert "owning_item=none owning_item_lookup=ok" in body


# ── §7.4a clause (b): escalation tracks the item's age, not the miss count ──


def test_owning_item_age_drives_escalation_not_the_miss_count(
        monkeypatch, fixed_now):
    """A P1 open nine days escalates BECAUSE it is a P1 open nine days —
    at consecutive_miss_runs=0, which the miss-count ladder would never
    have escalated."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _warning_spec_and_missing_result(index)

    assert _page(index, 
        spec, result, fixed_now, consecutive_miss_runs=0,
        owning=_owning(age_days=9.0, sla_days=3),
    ) is True
    assert publish_mock.call_args.kwargs["severity"] == "critical"
    body = publish_mock.call_args.args[0]
    assert "escalation_basis=owning_item_age" in body
    assert "after_consecutive_miss_runs=0" in body


def test_young_owning_item_does_not_escalate_but_is_still_recorded(
        monkeypatch, fixed_now):
    """The converse of the clause: a P1 filed today is being executed, so
    the miss count is not the reason to wake anyone. This is a DELIVERY
    decision only — the row still carries its true state and its full
    owning-item block into check_results.json (asserted below)."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    spec, result = _warning_spec_and_missing_result(index)
    owning = _owning(age_days=0.5, sla_days=3)

    assert _page(index, 
        spec, result, fixed_now, consecutive_miss_runs=13, owning=owning,
    ) is False
    publish_mock.assert_not_called()

    row = index._serialize_check_results(
        [(spec, result)], fixed_now,
        miss_counts={spec.artifact_id: 13},
        owning_by_id={spec.artifact_id: owning},
    )["results"][0]
    assert row["state"] == "missing"
    assert row["consecutive_miss_runs"] == 13
    assert row["owning_item_number"] == 6562
    assert row["owning_item_age_days"] == 0.5


def test_miss_count_ladder_still_governs_an_undiagnosed_condition(
        monkeypatch, fixed_now):
    """No owning item ⇒ the condition is undiagnosed and the detector's own
    clock is the only one available."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _warning_spec_and_missing_result(index)
    empty = {"resolved": True, "degraded": False, "degraded_reason": None,
             "owning_item": None, "members": [], "n_candidates": 0}

    assert _page(index, 
        spec, result, fixed_now,
        consecutive_miss_runs=index.WARNING_ESCALATION_RUNS, owning=empty,
    ) is True
    assert publish_mock.call_args.kwargs["severity"] == "critical"
    assert "escalation_basis=miss_count" in publish_mock.call_args.args[0]


def test_creating_the_owning_item_is_the_escalation_not_a_rung_above_it():
    """The threshold at which the item is CREATED is the threshold at which
    the row first pages critical — not a separate, higher one. `director_
    retro` sat at miss 13 against the old ISSUE_ESCALATION_RUNS=14, so no
    auto-filed item existed when the CRITICAL fired."""
    import index
    warning_spec, _ = _warning_spec_and_missing_result(index)
    critical_spec, _ = _critical_spec_and_missing_result(index)
    assert index._escalation_threshold(warning_spec) == index.WARNING_ESCALATION_RUNS
    assert index._escalation_threshold(critical_spec) == 1
    assert not hasattr(index, "ISSUE_ESCALATION_RUNS")


def test_no_second_item_is_filed_when_one_already_owns_the_condition(
        monkeypatch, fixed_now):
    """The join is bidirectional now: an item a HUMAN filed suppresses the
    monitor's own filing, which the self-referential `issue_filed_url`
    marker could never do."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    anchor_spec, anchor_result = _anchor_pair(index)
    file_mock = mock.Mock()
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)

    out = index._escalate_stale_key_deliverables(
        [(anchor_spec, anchor_result)],
        {"config_apply_audit": index.WARNING_ESCALATION_RUNS + 10},
        {"config_apply_audit": True}, {}, fixed_now,
        owning_by_id={"config_apply_audit": _owning()},
    )
    file_mock.assert_not_called()
    assert out == {"config_apply_audit": None}


def test_degraded_search_still_files_and_records_the_degradation(
        monkeypatch, fixed_now):
    """Failing toward a possible duplicate is cheaper than failing toward
    an untracked condition — but the duplicate must be reconcilable."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    anchor_spec, anchor_result = _anchor_pair(index)
    file_mock = mock.Mock(return_value={"filed": True, "url": "https://x/1"})
    monkeypatch.setattr(index, "_file_escalation_issue", file_mock)

    index._escalate_stale_key_deliverables(
        [(anchor_spec, anchor_result)],
        {"config_apply_audit": index.WARNING_ESCALATION_RUNS},
        {"config_apply_audit": True}, {}, fixed_now,
        owning_by_id={"config_apply_audit": index._unresolved("api down")},
    )
    file_mock.assert_called_once_with(
        "config_apply_audit", "alpha-engine-backtester",
        index.WARNING_ESCALATION_RUNS, "config_apply_audit",
        index.WARNING_ESCALATION_RUNS, "api down")


# ── §7.4a: cause-level grouping across the tracker ─────────────────────────


_MEASURED_ITEMS = [
    {"number": 6155, "html_url": "https://x/6155", "created_at": "2026-08-02T00:00:00Z",
     "title": "4 director artifacts stale since Jul 17/21",
     "body": "director_retro director_retro_trend director_action_plan director_carryover",
     "labels": [{"name": "P1"}, {"name": "triage:session"}]},
    {"number": 6345, "html_url": "https://x/6345", "created_at": "2026-08-03T00:00:00Z",
     "title": "advisory pipeline artifacts stale",
     "body": "director_retro and director_action_plan are stale",
     "labels": [{"name": "P1"}]},
    {"number": 6562, "html_url": "https://x/6562", "created_at": "2026-08-05T00:00:00Z",
     "title": "migrate director-retro-judge off direct OpenRouter",
     "body": "the retro judge sends a registry group handle to OpenRouter; director_retro",
     "labels": [{"name": "P1"}, {"name": "complexity:mid"}]},
    {"number": 6747, "html_url": "https://x/6747", "created_at": "2026-08-10T00:00:00Z",
     "title": "Director stage has not written fresh output since 07-17/07-20",
     "body": "director_retro director_action_plan director_carryover",
     "labels": [{"name": "P1"}]},
]

_KNOWN_IDS = {"director_retro", "director_retro_trend", "director_action_plan",
              "director_carryover"}


def test_grouping_names_the_cause_owner_and_lists_the_rest(monkeypatch):
    """Four open P1s, one condition ⇒ one owner (the narrowest claim, then
    the oldest) and three members. #6562 is the root cause; the three
    broader items are members, not owners."""
    import index
    now = datetime(2026, 8, 14, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(index, "_github_search_open_issues",
                        lambda term, pat: _MEASURED_ITEMS)

    res = index._resolve_owning_item(
        "director_retro", "director/2026-08-13/retro.json", _KNOWN_IDS,
        now, index._new_lookup_state(),
    )
    assert res["resolved"] is True
    assert res["degraded"] is False
    assert res["owning_item"]["number"] == 6562
    assert res["owning_item"]["priority"] == "P1"
    assert res["owning_item"]["age_days"] == 9.33
    assert [m["number"] for m in res["members"]] == [6345, 6747, 6155]
    assert res["n_candidates"] == 4


def test_search_terms_cover_the_human_prose_variant():
    """#6562's title says `director-retro-judge`. An exact `director_retro`
    match misses it, and missing it is the whole measured defect."""
    import index
    terms = index._search_terms("director_retro", "director/2026-08-13/retro.json")
    assert "director_retro" in terms
    assert "director-retro" in terms
    assert "director retro" in terms


def test_search_terms_no_longer_emit_the_bare_path_prefix():
    """config-I8680. The bare first path segment used to be a search term.

    GitHub issue search is full-text, so one common word (`trades`,
    `predictor`, `director`) returned twenty-to-thirty unrelated open
    issues, and `_rank_key` scores no relevance — so ownership fell to
    whichever of them was the oldest P0. It is now a relevance signal only,
    never a query.
    """
    import index
    assert "director" not in index._search_terms(
        "director_retro", "director/2026-08-13/retro.json")
    assert "trades" not in index._search_terms(
        "open_orders_latest", "trades/open_orders/latest.json")
    # And a date-shaped segment was never a stable identifier either.
    assert "2026-08-13" not in index._search_terms(
        "x_y", "2026-08-13/retro.json")


# ── config-I8680: the relevance gate ───────────────────────────────────────
#
# The measured 2026-08-26 misattribution, verbatim from the CRITICAL page:
#
#   artifact_id=open_orders_latest ... escalated_from=warning
#   escalation_basis=owning_item_age after_consecutive_miss_runs=1
#   owning_item=#4500 owning_item_priority=P0 owning_item_age_days=30.02
#   owning_item_sla_days=1 owning_item_members_total=20
#   owning_item_title='Groom/alert-drain boxes run egress proxy v2.0.0
#   without auto-redaction — hard-blocks where the laptop redacts'
#
# #4500's body matches neither `open_orders` nor `trades/` (verified live via
# `gh issue view 4500 --json body`). It won ownership on priority and age
# alone. Because `_alert_decision` swaps the miss-count ladder for the owning
# item's AGE the moment an item resolves, `30.02 >= 1` made a warning row page
# CRITICAL on its FIRST miss — and #4500 carries `gate:dependency`, so its age
# only grows.

_OPEN_ORDERS_KEY = "trades/open_orders/latest.json"

_IRRELEVANT_OLD_P0 = {
    "number": 4500,
    "html_url": "https://x/4500",
    "created_at": "2026-07-27T13:31:07Z",
    "title": ("Groom/alert-drain boxes run egress proxy v2.0.0 without "
              "auto-redaction — hard-blocks where the laptop redacts"),
    "body": "the groom box's proxy build predates auto-redaction",
    "labels": [{"name": "P0"}, {"name": "gate:dependency"}],
}

_RELEVANT_YOUNGER_P2 = {
    "number": 8000,
    "html_url": "https://x/8000",
    "created_at": "2026-08-24T00:00:00Z",
    "title": "executor daemon stopped writing open_orders_latest",
    "body": "no write to trades/open_orders/latest.json since the restart",
    "labels": [{"name": "P2"}],
}


def test_irrelevant_old_p0_can_never_own_a_row(monkeypatch):
    """The live defect: #4500 is the ONLY search result, so a relevance
    RANK would still crown it. The gate must discard it and leave the row
    unowned, which puts the miss-count ladder back in force."""
    import index
    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(index, "_github_search_open_issues",
                        lambda term, pat: [_IRRELEVANT_OLD_P0])
    res = index._resolve_owning_item(
        "open_orders_latest", _OPEN_ORDERS_KEY, {"open_orders_latest"},
        datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc),
        index._new_lookup_state(),
    )
    assert res["resolved"] is True
    assert res["owning_item"] is None
    assert res["members"] == []
    assert res["n_candidates"] == 0
    # Recorded as a number, not as an absence.
    assert res["n_filtered_irrelevant"] >= 1


def test_relevance_outranks_priority_and_age(monkeypatch):
    """A P2 filed two days ago that NAMES the artifact owns the cause over a
    P0 filed a month ago that does not. Priority and age order candidates;
    they do not create them."""
    import index
    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(
        index, "_github_search_open_issues",
        lambda term, pat: [_IRRELEVANT_OLD_P0, _RELEVANT_YOUNGER_P2],
    )
    res = index._resolve_owning_item(
        "open_orders_latest", _OPEN_ORDERS_KEY, {"open_orders_latest"},
        datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc),
        index._new_lookup_state(),
    )
    assert res["owning_item"]["number"] == 8000
    assert [m["number"] for m in res["members"]] == []


def test_relevance_accepts_the_dedated_s3_key(monkeypatch):
    """An item that names the S3 path instead of the artifact_id is still
    the owner — that recall is what the path prefix used to buy, kept here
    without buying twenty irrelevant candidates alongside it."""
    import index
    by_key = {
        "number": 8100,
        "html_url": "https://x/8100",
        "created_at": "2026-08-20T00:00:00Z",
        "title": "predictor/self_test.json has not been rewritten",
        "body": "the weekly training run did not emit it",
        "labels": [{"name": "P1"}],
    }
    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(index, "_github_search_open_issues",
                        lambda term, pat: [by_key])
    res = index._resolve_owning_item(
        "predictor_self_test", "predictor/2026-08-25/self_test.json",
        {"predictor_self_test"},
        datetime(2026, 8, 26, tzinfo=timezone.utc),
        index._new_lookup_state(),
    )
    assert res["owning_item"]["number"] == 8100


def test_relevance_match_is_case_insensitive(monkeypatch):
    import index
    shouty = dict(_RELEVANT_YOUNGER_P2,
                  title="OPEN_ORDERS_LATEST is not being written",
                  body="no detail")
    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(index, "_github_search_open_issues",
                        lambda term, pat: [shouty])
    res = index._resolve_owning_item(
        "open_orders_latest", _OPEN_ORDERS_KEY, {"open_orders_latest"},
        datetime(2026, 8, 26, tzinfo=timezone.utc),
        index._new_lookup_state(),
    )
    assert res["owning_item"]["number"] == 8000


def test_zero_namer_sorts_last_not_at_ninety_nine():
    """The `... if n else 99` inversion, corrected.

    DEFENSIVE, and honestly so: the sentinel only changed an outcome when a
    competing item named 100+ registry artifacts, which no real item does.
    The relevance gate above is what actually fixes the live defect. This is
    here because `99` was a count-shaped magic number sitting in a
    count-valued slot, and the next person to read it would reasonably
    assume a zero-namer ranks between a 98-namer and a 100-namer — which is
    exactly backwards from the intent stated in the docstring.
    """
    import index
    broad = {"priority": "P1", "n_artifacts_named": 100,
             "created_at": "2026-08-20T00:00:00Z", "number": 2}
    names_none = {"priority": "P1", "n_artifacts_named": 0,
                  "created_at": "2026-01-01T00:00:00Z", "number": 1}
    assert sorted([names_none, broad], key=index._rank_key) == [
        broad, names_none,
    ]
    # And the ordinary case is unchanged: narrower still beats broader.
    narrow = dict(broad, n_artifacts_named=1, number=3)
    assert sorted([broad, narrow], key=index._rank_key) == [narrow, broad]


def test_dedated_key_strips_only_date_segments():
    import index
    assert index._dedated_key("predictor/2026-08-25/self_test.json") == (
        "predictor/self_test.json")
    assert index._dedated_key(_OPEN_ORDERS_KEY) == _OPEN_ORDERS_KEY
    assert index._dedated_key("") == ""


def test_pat_read_failure_degrades_the_lookup_and_never_raises(monkeypatch):
    """The hermetic guard makes the SSM read raise; the resolver must
    absorb it into a DEGRADED resolution so the page still fires."""
    import index
    state = index._new_lookup_state()
    res = index._resolve_owning_item(
        "director_retro", "director/x/retro.json", _KNOWN_IDS,
        datetime(2026, 8, 14, tzinfo=timezone.utc), state,
    )
    assert res["owning_item"] is None
    assert res["degraded"] is True
    assert "pat_read_failed" in res["degraded_reason"]


def test_search_error_on_every_term_degrades_not_raises(monkeypatch):
    import index
    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")

    def _boom(term, pat):
        raise TimeoutError("timed out")

    monkeypatch.setattr(index, "_github_search_open_issues", _boom)
    res = index._resolve_owning_item(
        "director_retro", "director/x/retro.json", _KNOWN_IDS,
        datetime(2026, 8, 14, tzinfo=timezone.utc), index._new_lookup_state(),
    )
    assert res["degraded"] is True
    assert res["owning_item"] is None
    assert "TimeoutError" in res["degraded_reason"]


def test_partial_search_failure_resolves_but_flags_incompleteness(monkeypatch):
    import index
    calls = {"n": 0}

    def _flaky(term, pat):
        calls["n"] += 1
        if calls["n"] == 1:
            return [_MEASURED_ITEMS[2]]
        raise TimeoutError("timed out")

    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(index, "_github_search_open_issues", _flaky)
    res = index._resolve_owning_item(
        "director_retro", "director/x/retro.json", _KNOWN_IDS,
        datetime(2026, 8, 14, tzinfo=timezone.utc), index._new_lookup_state(),
    )
    assert res["owning_item"]["number"] == 6562
    assert res["degraded"] is True


def test_query_budget_exhaustion_is_a_recorded_reason(monkeypatch):
    import index
    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(index, "_github_search_open_issues",
                        lambda term, pat: [])
    state = index._new_lookup_state()
    state["queries"] = index.OWNING_ITEM_LOOKUP_MAX_QUERIES
    res = index._resolve_owning_item(
        "director_retro", "director/x/retro.json", _KNOWN_IDS,
        datetime(2026, 8, 14, tzinfo=timezone.utc), state,
    )
    assert res["degraded"] is True
    assert res["degraded_reason"] == "lookup_budget_exhausted"


def test_self_filed_marker_is_unioned_in_when_the_search_found_nothing():
    import index
    empty = {"resolved": True, "degraded": False, "degraded_reason": None,
             "owning_item": None, "members": [], "n_candidates": 0}
    merged = index._merge_self_filed(
        empty, "https://github.com/nousergon/alpha-engine-config/issues/4242")
    assert merged["owning_item"]["number"] == 4242
    assert merged["owning_item"]["source"] == "self_filed_marker"
    # A human-filed item outranks a stale self-filed marker.
    kept = index._merge_self_filed(_owning(), "https://x/issues/4242")
    assert kept["owning_item"]["number"] == 6562


# ── §7.4a clause (c): the number ───────────────────────────────────────────


def test_execution_loop_number_emits_zero_on_the_healthy_path(fixed_now):
    """A metric that only appears when something is wrong is
    indistinguishable from a dead emitter. Zero pages ⇒ still a row, with
    the denominator published alongside the ratio."""
    import index
    spec, _ = _critical_spec_and_missing_result(index)
    payload = index._summarize_execution_loop([(spec, _)], [], fixed_now)
    all_row = payload["classes"]["freshness-monitor.all"]
    assert all_row == {
        "pages": 0,
        "pages_with_open_owning_item": 0,
        "fraction_with_open_owning_item": 0.0,
        "median_owning_item_age_days_at_page": 0.0,
        "pages_with_degraded_lookup": 0,
    }
    assert "freshness-monitor.weekday_sf" in payload["classes"]


def test_execution_loop_number_counts_pages_against_open_items(fixed_now):
    import index
    spec, _ = _critical_spec_and_missing_result(index)
    records = [
        {"artifact_id": "a", "alert_class": "freshness-monitor.weekday_sf",
         "owning_item_number": 6562, "owning_item_age_days": 9.0,
         "lookup_degraded": False},
        {"artifact_id": "b", "alert_class": "freshness-monitor.weekday_sf",
         "owning_item_number": 6155, "owning_item_age_days": 3.0,
         "lookup_degraded": False},
        {"artifact_id": "c", "alert_class": "freshness-monitor.weekday_sf",
         "owning_item_number": None, "owning_item_age_days": None,
         "lookup_degraded": True},
    ]
    payload = index._summarize_execution_loop([(spec, _)], records, fixed_now)
    row = payload["classes"]["freshness-monitor.weekday_sf"]
    assert row["pages"] == 3
    assert row["pages_with_open_owning_item"] == 2
    assert row["fraction_with_open_owning_item"] == round(2 / 3, 4)
    assert row["median_owning_item_age_days_at_page"] == 6.0
    assert row["pages_with_degraded_lookup"] == 1
    assert payload["classes"]["freshness-monitor.all"] == row


def test_execution_loop_metrics_are_emitted_every_run(fixed_now):
    import index
    spec, _ = _critical_spec_and_missing_result(index)
    payload = index._summarize_execution_loop([(spec, _)], [], fixed_now)
    cw = mock.Mock()
    index._emit_execution_loop_metrics(cw, payload)
    names = {
        d["MetricName"]
        for call in cw.put_metric_data.call_args_list
        for d in call.kwargs["MetricData"]
    }
    assert names == {
        "AlertPages", "AlertPagesWithOpenOwningItem",
        "AlertPagesWithOpenOwningItemFraction",
        "OwningItemAgeDaysAtPageMedian", "OwningItemLookupDegraded",
    }
    dims = {
        d["Dimensions"][0]["Value"]
        for call in cw.put_metric_data.call_args_list
        for d in call.kwargs["MetricData"]
    }
    assert "freshness-monitor.all" in dims


def test_check_results_carries_the_execution_loop_block(fixed_now):
    import index
    spec, result = _critical_spec_and_missing_result(index)
    payload = index._serialize_check_results(
        [(spec, result)], fixed_now,
        execution_loop=index._summarize_execution_loop(
            [(spec, result)], [], fixed_now),
    )
    assert payload["execution_loop"]["classes"]["freshness-monitor.all"]["pages"] == 0


# ── §7.4a: the forbidden remedy ────────────────────────────────────────────


def test_cooldown_constants_are_not_a_noise_remedy():
    """§7.4a clause (c): a class that is correctly right too often is a
    backlog-drain defect, and quieting the channel deletes the only
    evidence the execution loop is not closing. Raising any of these to
    reduce page volume must fail a check rather than merely be
    discouraged — so they are pinned here. Changing one requires changing
    this test, which requires stating why it is not the forbidden move."""
    import index
    assert index.WARNING_ESCALATION_RUNS == 3
    assert index.RECOVERY_COOLDOWN_MINUTES == 120
    assert index.DRAIN_DISPATCH_COOLDOWN_MINUTES == 120
    assert index.PRODUCER_SUPPRESSION_MAX_DAYS == 14


def test_owning_item_resolution_never_widens_a_grace_window(monkeypatch, fixed_now):
    """The join must not become suppression: a row inside its SLA grace is
    still not paged, and a row past it is still paged, regardless of what
    the tracker says."""
    import importlib
    import index
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _critical_spec_and_missing_result(index)
    assert _page(index, spec, result, fixed_now, owning=_owning()) is True
    assert _page(index, 
        spec, result.__class__(state="stale", reason="in grace",
                               canonical_key=result.canonical_key,
                               sla_violated_by_minutes=0),
        fixed_now, owning=_owning()) is False


def test_lookup_is_bounded_by_wall_clock_not_only_query_count(monkeypatch):
    """MEASURED 2026-08-14: the deployed function's timeout is 120s. A
    Lambda timeout is a HARD FAIL that halts the sweep, so the join must be
    bounded by wall clock too — and blowing the budget degrades the
    remaining rows (recorded, still paging) rather than killing the pass."""
    import index
    assert (
        index.OWNING_ITEM_LOOKUP_MAX_QUERIES * index._OWNING_ITEM_TIMEOUT_SEC
        > index.OWNING_ITEM_LOOKUP_MAX_SECONDS
    ), "the count cap alone would not bound this phase — that is why the clock cap exists"
    assert index.OWNING_ITEM_LOOKUP_MAX_SECONDS < 120

    monkeypatch.setattr(index, "_cached_github_pat", lambda state: "pat")
    monkeypatch.setattr(index, "_github_search_open_issues",
                        lambda term, pat: [])
    state = index._new_lookup_state()
    state["started_at"] = time.monotonic() - index.OWNING_ITEM_LOOKUP_MAX_SECONDS - 1
    res = index._resolve_owning_item(
        "director_retro", "director/x/retro.json", _KNOWN_IDS,
        datetime(2026, 8, 14, tzinfo=timezone.utc), state,
    )
    assert res["degraded"] is True
    assert res["degraded_reason"] == "lookup_time_budget_exhausted"
    assert state["queries"] == 0


# ── GitHub Actions producer surface (alpha-engine-config-I7509) ─────────────
#
# Regression set for the 2026-08-12 miss: ten GHA workflows were disabled under
# Brian's groomer/Overseer deactivation ruling (I6984) while the freshness
# monitor could only see EventBridge, so `groom_flow_metrics` and
# `pr_resting_state_trend` escalated warning→critical and paged him for days
# about producers that were off on purpose.

def _keyed_s3(objects: dict[str, bytes]):
    """S3 fake that answers per KEY. `fake_s3` returns one body for every key,
    which cannot express 'inventory present, ownership absent'."""
    client = mock.Mock()
    client._put_calls: list[tuple[str, str, bytes]] = []

    def _get(*, Bucket, Key):
        if Key not in objects:
            raise _ClientError404()
        return {"Body": io.BytesIO(objects[Key])}

    def _put(*, Bucket, Key, Body, **kwargs):
        client._put_calls.append((Bucket, Key, Body))
        return {"ETag": '"deadbeef"'}

    client.get_object.side_effect = _get
    client.put_object.side_effect = _put
    return client


def _gha_inventory(now, workflows, *, complete=True, age_hours=1.0):
    import index
    stamped = now - timedelta(hours=age_hours)
    return {index.GHA_WORKFLOW_STATE_KEY: json.dumps({
        "generated_at": stamped.isoformat(),
        "complete": complete,
        "workflows": {k: {"state": v} for k, v in workflows.items()},
    }).encode()}


def test_parse_producer_trigger_accepts_gha_and_rejects_partial_names():
    import index
    assert index.parse_producer_trigger(
        "gha:nousergon/alpha-engine-config/flow-metrics.yml"
    ) == ("gha", "nousergon/alpha-engine-config/flow-metrics.yml")
    # owner/repo/workflow or nothing — a two-segment value is ambiguous, and
    # guessing would suppress against a workflow nobody named.
    for bad in ("gha:alpha-engine-config/flow-metrics.yml", "gha:flow-metrics.yml",
                "gha:nousergon//flow-metrics.yml", "gha:a/b/c/d", "gha:"):
        assert index.parse_producer_trigger(bad) is None


def test_gha_disabled_manually_suppresses_and_inactivity_does_not(fixed_now):
    """`disabled_manually` is our ruling and suppresses. `disabled_inactivity`
    is GitHub switching a producer off — that is the fault I7370 exists for,
    and suppressing it would convert an open finding into a blind spot."""
    import index
    trig = "gha:nousergon/alpha-engine-config/flow-metrics.yml"
    name = "nousergon/alpha-engine-config/flow-metrics.yml"

    s3 = _keyed_s3(_gha_inventory(fixed_now, {name: "disabled_manually"}))
    out = index.apply_producer_suppression(s3, {"groom_flow_metrics": trig}, fixed_now)
    assert out["groom_flow_metrics"]["suppressed"] is True
    assert "disabled_manually" in out["groom_flow_metrics"]["reason"]

    s3 = _keyed_s3(_gha_inventory(fixed_now, {name: "disabled_inactivity"}))
    assert index.apply_producer_suppression(
        s3, {"groom_flow_metrics": trig}, fixed_now) == {}

    s3 = _keyed_s3(_gha_inventory(fixed_now, {name: "active"}))
    assert index.apply_producer_suppression(
        s3, {"groom_flow_metrics": trig}, fixed_now) == {}


@pytest.mark.parametrize("objects_kwargs, why", [
    (dict(age_hours=48.0), "stale inventory — its own producer may be off"),
    (dict(complete=False), "partial listing cannot prove a workflow is enabled"),
])
def test_gha_inventory_fails_toward_paging(fixed_now, objects_kwargs, why):
    import index
    trig = "gha:nousergon/alpha-engine-config/flow-metrics.yml"
    name = "nousergon/alpha-engine-config/flow-metrics.yml"
    s3 = _keyed_s3(_gha_inventory(
        fixed_now, {name: "disabled_manually"}, **objects_kwargs))
    assert index.apply_producer_suppression(s3, {"a": trig}, fixed_now) == {}, why


def test_gha_inventory_absent_or_unstamped_fails_toward_paging(fixed_now):
    """Absent, malformed, and undateable all resolve nothing. This file can
    only ever REMOVE a page, so refusing to read it is always the safe side."""
    import index
    trig = "gha:nousergon/alpha-engine-config/flow-metrics.yml"
    name = "nousergon/alpha-engine-config/flow-metrics.yml"
    assert index.apply_producer_suppression(
        _keyed_s3({}), {"a": trig}, fixed_now) == {}
    assert index.apply_producer_suppression(
        _keyed_s3({index.GHA_WORKFLOW_STATE_KEY: b"not json"}),
        {"a": trig}, fixed_now) == {}
    unstamped = {index.GHA_WORKFLOW_STATE_KEY: json.dumps(
        {"complete": True, "workflows": {name: "disabled_manually"}}).encode()}
    assert index.apply_producer_suppression(
        _keyed_s3(unstamped), {"a": trig}, fixed_now) == {}


def test_open_owning_item_holds_suppression_past_the_expiry_clock(
    fixed_now, monkeypatch,
):
    """I6984 is an open P1 carrying the restore commands and a queued ruling.
    Paging every 30 minutes for eight weeks about a decision already written
    down is noise with a tracking number — ownership extends the clock."""
    import index
    monkeypatch.setattr(index, "PRODUCER_SUPPRESSION_MAX_DAYS", 14)
    monkeypatch.setattr(
        index, "_load_producer_disabled_since",
        lambda _s3: {"events:r": "2026-05-01"},   # 29 days before fixed_now
    )
    objects = {index.PAUSE_OWNERSHIP_KEY: json.dumps({
        "generated_at": (fixed_now - timedelta(hours=2)).isoformat(),
        "owners": {"events:r": {
            "item": "alpha-engine-config#6984",
            "url": "https://github.com/nousergon/alpha-engine-config/issues/6984",
            "state": "open",
        }},
    }).encode()}
    out = index.apply_producer_suppression(
        _keyed_s3(objects), {"a": "events:r"}, fixed_now,
        events_client=_events("DISABLED"),
    )
    assert out["a"]["days_disabled"] == 29
    assert out["a"]["suppressed"] is True
    assert out["a"]["pause_owner"] == "alpha-engine-config#6984"


@pytest.mark.parametrize("owner_state, age_hours, expected", [
    ("closed", 2, False),   # closed item ⇒ the pause is a latch again (I6828)
    ("open", 200, False),   # stale ownership map ⇒ no extension
])
def test_ownership_extension_is_narrow(
    fixed_now, monkeypatch, owner_state, age_hours, expected,
):
    import index
    monkeypatch.setattr(index, "PRODUCER_SUPPRESSION_MAX_DAYS", 14)
    monkeypatch.setattr(
        index, "_load_producer_disabled_since",
        lambda _s3: {"events:r": "2026-05-01"},
    )
    objects = {index.PAUSE_OWNERSHIP_KEY: json.dumps({
        "generated_at": (fixed_now - timedelta(hours=age_hours)).isoformat(),
        "owners": {"events:r": {"item": "x#1", "state": owner_state}},
    }).encode()}
    out = index.apply_producer_suppression(
        _keyed_s3(objects), {"a": "events:r"}, fixed_now,
        events_client=_events("DISABLED"),
    )
    assert out["a"]["suppressed"] is expected


def test_ownership_never_creates_a_suppression_on_an_enabled_producer(
    fixed_now, monkeypatch,
):
    """Ownership only ever EXTENDS an existing live-confirmed pause. It must
    never be able to silence a row whose producer is running and failing."""
    import index
    objects = {index.PAUSE_OWNERSHIP_KEY: json.dumps({
        "generated_at": fixed_now.isoformat(),
        "owners": {"events:r": {"item": "x#1", "state": "open"}},
    }).encode()}
    assert index.apply_producer_suppression(
        _keyed_s3(objects), {"a": "events:r"}, fixed_now,
        events_client=_events("ENABLED"),
    ) == {}


def test_multi_trigger_row_suppresses_only_when_every_producer_is_off(
    fixed_now, monkeypatch,
):
    """`groom_status_store_groom` is written by whichever of four schedules
    fires. Suppressing while three are still live would silence a row the
    fleet genuinely expects to be written."""
    import index
    states = {"scheduler:a": "DISABLED", "scheduler:b": "ENABLED"}

    def _fake_resolve(triggers, events_client=None, scheduler_client=None,
                      gha_states=None):
        return {t: f"{t} is DISABLED" for t in triggers
                if states.get(t) == "DISABLED"}

    monkeypatch.setattr(index, "resolve_disabled_producers", _fake_resolve)
    s3 = _keyed_s3({})
    assert index.apply_producer_suppression(
        s3, {"groom": ("scheduler:a", "scheduler:b")}, fixed_now) == {}

    states["scheduler:b"] = "DISABLED"
    out = index.apply_producer_suppression(
        s3, {"groom": ("scheduler:a", "scheduler:b")}, fixed_now)
    assert out["groom"]["suppressed"] is True
    assert out["groom"]["trigger"] == "scheduler:a, scheduler:b"


def test_multi_trigger_clock_runs_from_the_most_recent_switch_off(
    fixed_now, monkeypatch,
):
    """A row that lost its last live producer yesterday has been quiet for a
    day, not for the month its first producer has been off."""
    import index
    monkeypatch.setattr(
        index, "resolve_disabled_producers",
        lambda triggers, *_a, **_kw: {t: "off" for t in triggers},
    )
    monkeypatch.setattr(
        index, "_load_producer_disabled_since",
        lambda _s3: {"scheduler:a": "2026-05-01", "scheduler:b": "2026-05-29"},
    )
    out = index.apply_producer_suppression(
        _keyed_s3({}), {"groom": ("scheduler:a", "scheduler:b")}, fixed_now)
    assert out["groom"]["disabled_since"] == "2026-05-29"
    assert out["groom"]["days_disabled"] == 1


def test_partially_malformed_trigger_list_is_dropped_whole(fake_s3, caplog):
    """Keeping the half that parsed would suppress the row on fewer producers
    than it declares — quieter than the registry says, which is the one
    direction this field may never fail in."""
    import index
    fake_s3._registry_body = (
        b"artifacts:\n"
        b"  - artifact_id: half_bad\n"
        b"    s3_bucket: alpha-engine-research\n"
        b"    s3_key_template: k\n"
        b"    cadence: continuous\n"
        b"    interval_minutes: 60\n"
        b"    sla_minutes_after_cron: 60\n"
        b"    severity: warning\n"
        b"    owner_repo: r\n"
        b"    created_at: 2026-01-01\n"
        b"    producer_trigger:\n"
        b"      - scheduler:good\n"
        b"      - not-a-trigger\n"
    )
    _s, _r, _a, _e, _rem, producer, _do = index.load_registry_with_recovery(
        fake_s3, "b", "k"
    )
    assert producer == {}


def test_gha_pause_age_comes_from_githubs_own_switch_off_date(fixed_now):
    """The inventory carries GitHub's `updated_at`. Using this Lambda's
    first-observation instead would date every pause from whenever the monitor
    happened to look, and the latch sweep reading the same inventory would
    then disagree with it about how old the pause is."""
    import index
    trig = "gha:nousergon/alpha-engine-config/merge-drain.yml"
    name = "nousergon/alpha-engine-config/merge-drain.yml"
    objects = {index.GHA_WORKFLOW_STATE_KEY: json.dumps({
        "generated_at": (fixed_now - timedelta(hours=1)).isoformat(),
        "complete": True,
        "workflows": {name: {
            "state": "disabled_manually", "disabled_since": "2026-05-01",
        }},
    }).encode()}
    out = index.apply_producer_suppression(
        _keyed_s3(objects), {"pr_resting_state_trend": trig}, fixed_now)
    assert out["pr_resting_state_trend"]["disabled_since"] == "2026-05-01"
    assert out["pr_resting_state_trend"]["days_disabled"] == 29


# ══════════════════════════════════════════════════════════════════════════
# Suppression coverage — the gap that could only be learned by being paged
# (alpha-engine-config-I7606)
# ══════════════════════════════════════════════════════════════════════════
#
# Producer suppression only ever fires for a row that DECLARED its
# producer_trigger, so coverage of that field is the whole distance between
# "a producer was deliberately switched off" and "Brian gets a CRITICAL page
# for his own ruling". Measured 2026-08-18: 13 of 145 rows declared it, six of
# them sat correctly quiet, and two that did not declare it —
# health_alpha_engine_predictor_health_check and predictor_drift_detection —
# had been escalating warning->critical on miss-count for producers disabled
# under the 2026-08-07 pause (config-I6617). The mechanism worked. The field
# was absent. Nothing anywhere reported the absence, so the page was the
# notification channel for its own coverage gap.


class _Spec:
    def __init__(self, artifact_id):
        self.artifact_id = artifact_id


class _Result:
    def __init__(self, state):
        self.state = state


def _coverage(pairs, declared, suppression=None, inventory=None):
    import index
    return index.suppression_coverage(
        pairs, declared, suppression or {}, inventory,
    )


def test_coverage_counts_a_not_fresh_row_that_declared_nothing():
    """The exact 2026-08-18 case: the row is stale, nothing says what produces
    it, so no producer pause could ever explain the page."""
    pairs = [
        (_Spec("predictor_health"), _Result("stale")),
        (_Spec("fresh_one"), _Result("fresh")),
    ]
    out = _coverage(pairs, {})
    assert out["undeclared_not_fresh"] == 1
    assert out["undeclared_not_fresh_ids"] == ["predictor_health"]
    assert out["not_fresh"] == 1


def test_a_declared_row_is_not_counted_even_when_it_is_still_stale():
    """Suppression does not make a stale artifact fresh — the row keeps its
    true state. What the declaration buys is that the staleness is EXPLAINED,
    so it must not show up as a coverage gap."""
    pairs = [(_Spec("daily_heal_summary"), _Result("stale"))]
    out = _coverage(
        pairs,
        {"daily_heal_summary": "events:alpha-engine-daily-heal"},
        {"daily_heal_summary": {"suppressed": True}},
    )
    assert out["undeclared_not_fresh"] == 0
    assert out["suppressed"] == 1


def test_missing_counts_the_same_as_stale():
    """rag_ingestion_progress was `missing`, not `stale`, for 14 sweeps. A
    coverage measure that only looked at staleness would have scored the
    loudest row in the fleet as covered."""
    pairs = [(_Spec("never_written"), _Result("missing"))]
    assert _coverage(pairs, {})["undeclared_not_fresh"] == 1


def test_coverage_never_guesses_that_a_disabled_producer_owns_a_row():
    """A registry row does not say what produces it. Reporting the unreferenced
    disabled producers NEXT TO the undeclared rows lets a human join them;
    inferring the join here would silence pages on a resemblance."""
    pairs = [(_Spec("orphan"), _Result("stale"))]
    inventory = {
        "complete": True,
        "rows": [
            {"trigger": "events:something-off", "referenced_by_registry": False},
            {"trigger": "events:known", "referenced_by_registry": True},
        ],
    }
    out = _coverage(pairs, {}, None, inventory)
    assert out["undeclared_not_fresh_ids"] == ["orphan"]
    assert out["disabled_producers_unreferenced"] == 1
    # No field claims a link between the two.
    assert not any("owner" in k or "likely" in k for k in out)


def test_an_incomplete_inventory_is_reported_as_incomplete():
    """enumerate_disabled_schedules records an ERROR row rather than returning
    a short list. Coverage must carry that forward: an inventory that failed to
    walk the account must never read as "nothing else is disabled"."""
    out = _coverage([], {}, None, {"complete": False, "rows": []})
    assert out["inventory_complete"] is False
    assert _coverage([], {}, None, None)["inventory_complete"] is False


def test_coverage_metrics_emit_zeros_rather_than_nothing():
    """Zeros included, for the reason the execution-loop emitter states: the
    absence of these datapoints means the emitter is dead, never that coverage
    is complete."""
    import index
    calls = []

    class _CW:
        def put_metric_data(self, **kw):
            calls.append(kw)

    index._emit_suppression_coverage_metrics(_CW(), _coverage([], {}))
    assert len(calls) == 1
    names = {m["MetricName"]: m["Value"] for m in calls[0]["MetricData"]}
    assert names["UndeclaredNotFreshRows"] == 0.0
    assert names["RowsDeclaringProducerTrigger"] == 0.0
    assert names["DisabledProducersUnreferenced"] == 0.0


# ── config-I7713: one page per sweep, grouped by cause ──────────────────────
#
# Brian, 2026-08-19: "I should only get one if it points to a singular issue,
# instead i'm getting ~20. The single error should encompass all errors
# currently triggering."
#
# The measured condition these tests reconstruct: the 2026-08-19T12:03:13Z
# sweep emitted 17 pages and all 17 shared one cause (78 registry rows on a
# weekday cadence over a once-weekly producer — alpha-engine-config-I7709).


def _weekly_spec(index_mod, artifact_id, *, severity="critical", pipeline="ne-weekly-freshness-pipeline"):
    from nousergon_lib.artifact_freshness import ArtifactSpec
    spec = ArtifactSpec(
        artifact_id=artifact_id, s3_bucket="b",
        s3_key_template=f"{artifact_id}/{{date}}.json",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity=severity, owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    object.__setattr__(spec, "produced_by", [{"pipeline": pipeline, "stage": "S"}])
    return spec


def _missing(index_mod, key):
    from nousergon_lib.artifact_freshness import CheckResult
    return CheckResult(state="missing", sla_violated_by_minutes=300,
                       canonical_key=key, reason="absent")


def _decide_all(index_mod, specs, now):
    return [index_mod._alert_decision(s, _missing(index_mod, f"{s.artifact_id}/k"), now)
            for s in specs]


def test_seventeen_artifacts_one_cause_produce_exactly_one_page(monkeypatch, fixed_now):
    """The regression itself, at the measured scale."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

    specs = [_weekly_spec(index, f"backtest_{i}") for i in range(17)]
    covered = index._publish_digest(_decide_all(index, specs, fixed_now), fixed_now)

    assert covered == 17
    publish_mock.assert_called_once()
    notify_mock.assert_called_once()
    body = publish_mock.call_args.args[0]
    # Nothing is dropped: every artifact still appears, with its own reason.
    for spec in specs:
        assert f"artifact_id={spec.artifact_id}" in body
    assert "17 artifact(s) past SLA across 1 cause(s)" in body


def test_distinct_causes_are_separate_blocks_in_the_same_page(monkeypatch, fixed_now):
    """Grouping must not merge unrelated conditions into one undifferentiated
    wall — one page, but the causes stay legible and separately counted."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))

    specs = [
        _weekly_spec(index, "weekly_a"),
        _weekly_spec(index, "weekly_b"),
        _weekly_spec(index, "preopen_a", pipeline="ne-preopen-trading-pipeline"),
    ]
    index._publish_digest(_decide_all(index, specs, fixed_now), fixed_now)

    body = publish_mock.call_args.args[0]
    assert "across 2 cause(s)" in body
    assert "[pipeline:ne-weekly-freshness-pipeline] 2 artifact(s)" in body
    assert "[pipeline:ne-preopen-trading-pipeline] 1 artifact(s)" in body


def test_one_critical_row_makes_the_whole_digest_critical(monkeypatch, fixed_now):
    """The invariant that makes the rollup safe: grouping changes how many
    messages are sent, never which conditions are reportable. A digest that
    took its severity from the majority could demote a critical page by
    surrounding it with warnings."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))

    decisions = [
        index._alert_decision(_weekly_spec(index, "crit"), _missing(index, "k"), fixed_now),
        # A warning row only reaches the page via the escalation ladder, which
        # is exactly how a mixed-severity digest arises in production.
        index._alert_decision(
            _weekly_spec(index, "warn", severity="warning"), _missing(index, "k"),
            fixed_now, consecutive_miss_runs=index.WARNING_ESCALATION_RUNS),
    ]
    index._publish_digest([d for d in decisions if d], fixed_now)
    assert publish_mock.call_args.kwargs["severity"] == "critical"


def test_nothing_alerting_sends_nothing(monkeypatch, fixed_now):
    """Silence by absence of condition. A healthy sweep must not emit an empty
    page — check_results.json and the heartbeat are the surfaces that prove the
    monitor ran (observability-policy §9.2a)."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

    assert index._publish_digest([], fixed_now) == 0
    publish_mock.assert_not_called()
    notify_mock.assert_not_called()


def test_dedup_key_tracks_the_condition_set_not_the_clock(monkeypatch, fixed_now):
    """An unchanged set stays quiet; an artifact joining or recovering re-pages.
    A time-based cooldown cannot tell those apart — it would have to drop the
    new one, which is suppressing a fact rather than combining facts."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    two = _decide_all(index, [_weekly_spec(index, "a"), _weekly_spec(index, "b")], fixed_now)
    three = _decide_all(
        index, [_weekly_spec(index, "a"), _weekly_spec(index, "b"), _weekly_spec(index, "c")],
        fixed_now)

    assert index._digest_dedup_key(two) == index._digest_dedup_key(
        list(reversed(two))), "order must not change the key"
    assert index._digest_dedup_key(two) != index._digest_dedup_key(three)


# ── config-I9336: episode-keyed dedup (run-keyed → episode-keyed fix) ───────
#
# Measured live (see PR body): `freshness_digest_{date}_{fingerprint}` baked
# `now.date()` straight into the dedup key, so an UNCHANGED standing
# incident re-paged CRITICAL on every UTC midnight for as long as it
# lasted (`director_retro_trend` paged again 2026-08-14 for the exact
# condition already paged 2026-08-12). These tests pin the fix at both the
# unit level (`_episode_signature`) and the digest level
# (`_digest_dedup_key` / a two-sweep handler round trip) — every one of
# them FAILS against the pre-fix code (the date-stamped key, or a
# per-artifact `resolve_dedup_key(spec, now)` shaped by the current cycle
# label, both mint a new key every day the incident continues).


def _spec(index_mod, artifact_id, **overrides):
    from nousergon_lib.artifact_freshness import ArtifactSpec
    fields = dict(
        artifact_id=artifact_id, s3_bucket="alpha-engine-research",
        s3_key_template=f"{artifact_id}/{{date}}.json",
        cadence="saturday_sf", sla_minutes_after_cron=60,
        severity="critical", owner_repo="ae-test", created_at=date(2025, 1, 1),
    )
    fields.update(overrides)
    return ArtifactSpec(**fields)


def test_episode_signature_missing_stable_while_state_persists(fixed_now):
    """Two consecutive sweeps over the same unbroken `missing` episode must
    produce the SAME episode key — the sentinel opened_at carries forward
    via the (simulated) check_results.json round trip, even a day later."""
    import index
    from nousergon_lib.artifact_freshness import CheckResult

    spec = _spec(index, "director_retro_trend")
    result = CheckResult(state="missing", sla_violated_by_minutes=30209,
                          canonical_key="director_retro_trend/2026-08-12.json",
                          reason="absent")

    sweep1 = index._episode_signature(spec, result, prev_episode=None, now=fixed_now)
    assert sweep1 is not None
    assert sweep1["state"] == "missing"

    # Sweep 2, TWO DAYS LATER (crosses a UTC-midnight rollover), carries
    # forward sweep 1's round-tripped sentinel exactly as
    # `_load_prev_episode_state` would hand it back.
    later = fixed_now + timedelta(days=2)
    sweep2 = index._episode_signature(
        spec, result, prev_episode={"state": sweep1["state"], "opened_at": sweep1["opened_at"]},
        now=later,
    )
    assert sweep2["key"] == sweep1["key"], (
        "an unbroken missing episode must mint the SAME key across a day "
        "rollover — a differing key here is the run-keyed regression"
    )


def test_episode_signature_missing_then_stale_then_missing_is_new_episode(fixed_now):
    """missing → stale → missing must NOT reuse the first missing episode's
    key — the digest publishes with dedup_window_min=None (forever), so
    reusing it would silently suppress the second, genuinely new,
    incident's page forever."""
    import index
    from nousergon_lib.artifact_freshness import CheckResult

    spec = _spec(index, "x")
    first_missing = CheckResult(state="missing", sla_violated_by_minutes=100,
                                 canonical_key="x/2026-08-01.json", reason="absent")
    ep1 = index._episode_signature(spec, first_missing, prev_episode=None, now=fixed_now)

    # Recovers: an instance lands (stale, not fresh, but a real instance).
    recovered = CheckResult(state="stale", last_modified=fixed_now - timedelta(days=1),
                             sla_violated_by_minutes=50, canonical_key="x/2026-08-05.json",
                             reason="old instance")
    ep_stale = index._episode_signature(
        spec, recovered, prev_episode={"state": ep1["state"], "opened_at": ep1["opened_at"]},
        now=fixed_now + timedelta(days=4),
    )
    assert ep_stale["state"] == "stale"
    assert ep_stale["key"] != ep1["key"]

    # Goes missing again (e.g. the object was deleted). prev_episode here is
    # what `_load_prev_episode_state` would hand back from the STALE sweep
    # (missing rows only persist episode_state/episode_opened_at for
    # missing/probe_failed — a stale prior round-trips as "no missing
    # sentinel carried forward", i.e. prev=None from this function's view).
    second_missing = CheckResult(state="missing", sla_violated_by_minutes=10,
                                  canonical_key="x/2026-08-10.json", reason="absent again")
    ep2 = index._episode_signature(
        spec, second_missing, prev_episode=None, now=fixed_now + timedelta(days=9),
    )
    assert ep2["key"] != ep1["key"], (
        "a second missing episode, after a genuine recovery in between, "
        "must NOT collide with the first missing episode's (permanently "
        "suppressing) dedup marker"
    )


def test_episode_signature_stale_derives_from_last_modified_no_persistence_needed(fixed_now):
    """A `stale` episode's key is derived purely from the freshest
    instance's own last_modified — stable while nothing new lands, with NO
    round-trip required, and changes the instant a genuinely newer
    instance appears (even if it is itself still stale)."""
    import index
    from nousergon_lib.artifact_freshness import CheckResult

    spec = _spec(index, "y")
    frozen_lm = fixed_now - timedelta(days=21)
    stale_now = CheckResult(state="stale", last_modified=frozen_lm,
                             sla_violated_by_minutes=30209,
                             canonical_key="y/2026-08-12.json", reason="old")
    ep_day1 = index._episode_signature(spec, stale_now, prev_episode=None, now=fixed_now)
    ep_day3 = index._episode_signature(
        spec, stale_now, prev_episode=None, now=fixed_now + timedelta(days=2))
    assert ep_day1["key"] == ep_day3["key"]

    # A newer (still stale) instance lands — the episode is genuinely over.
    newer_lm = fixed_now - timedelta(days=1)
    stale_next = CheckResult(state="stale", last_modified=newer_lm,
                              sla_violated_by_minutes=100,
                              canonical_key="y/2026-08-14.json", reason="old")
    ep_next = index._episode_signature(
        spec, stale_next, prev_episode=None, now=fixed_now + timedelta(days=3))
    assert ep_next["key"] != ep_day1["key"]


def test_digest_dedup_key_stable_across_day_rollover_same_episode(monkeypatch, fixed_now):
    """The digest-level fingerprint (what actually reaches `publish`) must
    be identical across two sweeps of the same unbroken episode set, even
    when they land on different UTC calendar days — the exact defect the
    pre-fix `freshness_digest_{date}_{fingerprint}` shape had."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    from nousergon_lib.artifact_freshness import CheckResult

    spec = _spec(index, "director_retro_trend")
    result = CheckResult(state="missing", sla_violated_by_minutes=30209,
                          canonical_key="director_retro_trend/2026-08-12.json",
                          reason="absent")
    episode = index._episode_signature(spec, result, prev_episode=None, now=fixed_now)

    decision_day1 = index._alert_decision(spec, result, fixed_now, episode=episode)
    decision_day3 = index._alert_decision(
        spec, result, fixed_now + timedelta(days=2),
        episode={"key": episode["key"], "state": episode["state"], "opened_at": episode["opened_at"]},
    )
    key1 = index._digest_dedup_key([decision_day1])
    key3 = index._digest_dedup_key([decision_day3])
    assert key1 == key3, "an unbroken episode must dedup across a UTC day rollover"


def test_digest_dedup_key_changes_when_a_new_artifact_episode_opens(monkeypatch, fixed_now):
    """A genuinely new episode (different artifact_id, or the same artifact
    with a fresh opened_at) changes the digest fingerprint — dedup must
    never suppress a fact that actually changed."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    from nousergon_lib.artifact_freshness import CheckResult

    spec = _spec(index, "z")
    result = CheckResult(state="missing", sla_violated_by_minutes=100,
                          canonical_key="z/2026-08-01.json", reason="absent")
    ep_open = index._episode_signature(spec, result, prev_episode=None, now=fixed_now)
    decision_open = index._alert_decision(spec, result, fixed_now, episode=ep_open)

    later_new_episode = index._episode_signature(
        spec, result, prev_episode=None, now=fixed_now + timedelta(days=30))
    decision_new = index._alert_decision(
        spec, result, fixed_now + timedelta(days=30), episode=later_new_episode)

    assert index._digest_dedup_key([decision_open]) != index._digest_dedup_key([decision_new])


def test_digest_enumerates_episode_opened_on_an_earlier_sweep_with_age(monkeypatch, fixed_now):
    """The digest body composed on a LATER sweep must still name an episode
    that opened on an earlier one — with its age — so a standing condition
    can never be invisible even while its dedup key keeps the page quiet."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    from nousergon_lib.artifact_freshness import CheckResult

    spec = _spec(index, "director_retro_trend")
    result = CheckResult(state="missing", sla_violated_by_minutes=30209,
                          canonical_key="director_retro_trend/2026-08-12.json",
                          reason="absent")
    opened_at = fixed_now.isoformat()
    episode = {"key": f"freshness_director_retro_trend_missing_{opened_at}",
               "state": "missing", "opened_at": opened_at}

    later = fixed_now + timedelta(days=3)
    decision = index._alert_decision(spec, result, later, episode=episode)
    body = index._compose_digest([decision], later)

    assert "director_retro_trend" in body
    assert f"episode_open_since={opened_at}" in body
    assert "episode_age_minutes=4320" in body  # 3 days
    assert "oldest open episode 4320min" in body


def test_evaluate_driver_closed_set_including_unattributed_branch(fixed_now):
    """The driver classifier is a closed set over CheckResult.state, with an
    explicit terminal `unattributed` branch for anything outside it —
    never a silent fallthrough to a plausible-looking default."""
    import index
    from nousergon_lib.artifact_freshness import CheckResult

    spec = _spec(index, "w")
    missing = index._evaluate_driver(
        spec, CheckResult(state="missing", canonical_key="w/k.json", reason="r"))
    assert missing["driver"] == "missing_no_instance"
    assert "s3://alpha-engine-research/w/k.json" in missing["clears_when"]

    stale = index._evaluate_driver(
        spec, CheckResult(state="stale", last_modified=fixed_now,
                           canonical_key="w/k.json", reason="r"))
    assert stale["driver"] == "stale_instance_past_sla"

    probe_failed = index._evaluate_driver(
        spec, CheckResult(state="probe_failed", canonical_key="w/k.json", reason="403"))
    assert probe_failed["driver"] == "probe_error"

    fresh = index._evaluate_driver(
        spec, CheckResult(state="fresh", canonical_key="w/k.json", reason="ok"))
    assert fresh["driver"] == "not_applicable"

    # A state outside the closed set (defensive — CheckResult.state never
    # actually emits this today) must land on the TERMINAL unattributed
    # branch, not silently coerce to one of the real drivers above.
    unknown = index._evaluate_driver(
        spec, CheckResult(state="quantum_superposition", canonical_key="w/k.json", reason="r"))
    assert unknown["driver"] == "unattributed"
    assert unknown["driver"] not in (
        "missing_no_instance", "stale_instance_past_sla", "probe_error", "not_applicable",
    )


def test_handler_dedup_key_identical_across_two_sweeps_same_incident(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """End-to-end regression pin: two full `handler()` sweeps, three days
    apart (crossing UTC midnight twice), over the SAME unbroken
    `probe_missing` incident, must publish the SAME dedup key both times —
    this is what makes the second sweep's publish() call a no-op dedup
    skip rather than a second CRITICAL page. Fails against the pre-fix
    date-stamped digest key (and would have failed against the even older
    per-artifact resolve_dedup_key(spec, now) shape, whose cycle_window_label
    also advances with the calendar)."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = yaml_registry_body

    import importlib
    import index
    importlib.reload(index)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))

    # ── Sweep 1 ──
    publish1 = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish1)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    _patch_now(monkeypatch, fixed_now)
    index.handler({}, None)
    assert publish1.called
    key1 = publish1.call_args.kwargs["dedup_key"]

    # Carry sweep 1's check_results.json forward as the ONLY prior-state
    # input to sweep 2 (real production shape: the daily sweep reads back
    # its own last write).
    check_results_body = next(
        body for (_, k, body) in fake_s3._put_calls
        if k == "_freshness_monitor/check_results.json"
    )
    _keyed_get_object(fake_s3, {index.CHECK_RESULTS_KEY: check_results_body})

    # ── Sweep 2, three days later (>= two UTC-midnight rollovers) ──
    later = fixed_now + timedelta(days=3)
    publish2 = mock.Mock(return_value=mock.Mock(dedup_skipped=True, dedup_reason="marker present"))
    monkeypatch.setattr(index, "publish", publish2)
    _patch_now(monkeypatch, later)
    index.handler({}, None)
    assert publish2.called
    key2 = publish2.call_args.kwargs["dedup_key"]

    assert key1 == key2, (
        f"dedup key changed across an unbroken episode ({key1!r} -> "
        f"{key2!r}) — the run-keyed-not-episode-keyed regression"
    )


def test_publish_dedup_skip_does_not_double_send_telegram(monkeypatch, fixed_now):
    """config-I6796's invariant, preserved through the rollup: publish()'s dedup
    verdict is the single source of truth for BOTH channels."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    notify_mock = mock.Mock(return_value=True)
    monkeypatch.setattr(index, "publish", mock.Mock(
        return_value=mock.Mock(dedup_skipped=True, dedup_reason="already-sent")))
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify_mock)

    covered = index._publish_digest(
        _decide_all(index, [_weekly_spec(index, "a")], fixed_now), fixed_now)
    assert covered == 1  # still counted as covered — it IS on the operator page
    notify_mock.assert_not_called()


def test_group_key_falls_back_through_trigger_to_owner_repo(monkeypatch):
    """Every alerting row lands in a NAMED group. A nameless remainder bucket
    would recreate the wall this rollup exists to remove."""
    import importlib
    import index
    importlib.reload(index)
    from nousergon_lib.artifact_freshness import ArtifactSpec

    base = dict(s3_bucket="b", s3_key_template="k/{date}", cadence="continuous",
                interval_minutes=15, sla_minutes_after_cron=30, severity="warning",
                owner_repo="nousergon-data", created_at=date(2025, 1, 1))
    bare = ArtifactSpec(artifact_id="bare", **base)
    assert index._cause_group_key(bare) == "owner_repo:nousergon-data"

    triggered = ArtifactSpec(artifact_id="trig", **base)
    object.__setattr__(triggered, "producer_trigger", ("scheduler:x-15min",))
    assert index._cause_group_key(triggered) == "trigger:scheduler:x-15min"

    # produced_by wins over producer_trigger — the pipeline is the closer cause.
    both = ArtifactSpec(artifact_id="both", **base)
    object.__setattr__(both, "producer_trigger", ("scheduler:x-15min",))
    object.__setattr__(both, "produced_by", [{"pipeline": "ne-weekly-freshness-pipeline"}])
    assert index._cause_group_key(both) == "pipeline:ne-weekly-freshness-pipeline"


def test_digest_publish_failure_does_not_sink_the_sweep(monkeypatch, fixed_now):
    """The page is a notification; check_results.json is the record. A delivery
    failure must be loud and must not cost the durable surface."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    monkeypatch.setattr(index, "publish", mock.Mock(side_effect=RuntimeError("sns down")))
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))

    with pytest.raises(RuntimeError):
        index._publish_digest(
            _decide_all(index, [_weekly_spec(index, "a")], fixed_now), fixed_now)


def test_handler_still_writes_check_results_when_the_digest_cannot_be_sent(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """The other half of the above, through the real handler: delivery raising
    must not cost the durable record. `_publish_digest` raises for the whole
    sweep now rather than for one artifact, so the trap around it carries more
    weight than the per-artifact one it replaced — hence this test."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = yaml_registry_body
    cycle_tick = datetime(2026, 5, 30, 9, 0, tzinfo=timezone.utc)
    fake_s3._head_returns["path/2026-05-30/fresh.json"] = {
        "LastModified": cycle_tick.replace(hour=12),
    }

    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock(side_effect=RuntimeError("sns down")))
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))

    result = index.handler({}, None)

    assert result["alerts_enabled"] is True
    written = {k for (_, k, _) in fake_s3._put_calls}
    assert "_freshness_monitor/check_results.json" in written
    assert "_freshness_monitor/heartbeat.json" in written


# ── config-I7622: "never once written" is not an SLA miss ───────────────────
#
# Brian, 2026-08-19: "if these are false alarms i don't want to receive them."
# After the config-I7709 cadence revert and the config-I7713 rollup, the single
# remaining page in the 2026-08-19T15:41:32Z sweep was `rag_corpus_scope_state`
# — key `rag_corpus/scope_state/latest.json`, which no code in nousergon-data,
# crucible-research, nousergon-lib or alpha-engine-config writes, and whose
# `rag_corpus/` prefix does not exist in the bucket at all. It had reached 6
# consecutive misses and escalated to the critical path.


class _ListStub:
    """Minimal S3 double for the never-written probe.

    `key_count` drives the fixed-key path; `pages` drives the suffix-matching
    path (config-I7622 follow-up) as a list of `list_objects_v2` responses.
    """

    def __init__(self, key_count=None, pages=None):
        self.key_count = key_count
        self.pages = list(pages or [])
        self.calls = []

    def list_objects_v2(self, **kw):
        self.calls.append(kw)
        if isinstance(self.key_count, Exception):
            raise self.key_count
        if self.pages:
            return self.pages.pop(0)
        return {"KeyCount": self.key_count}


def _missing_spec(index_mod, artifact_id="never_made", template="rag_corpus/scope_state/latest.json"):
    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult
    spec = ArtifactSpec(
        artifact_id=artifact_id, s3_bucket="alpha-engine-research",
        s3_key_template=template, cadence="continuous", interval_minutes=1440,
        sla_minutes_after_cron=720, severity="critical", owner_repo="nousergon-data",
        created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="missing", sla_violated_by_minutes=221,
                         canonical_key=template, reason="no instance found")
    return spec, result


def test_a_never_written_row_does_not_page(monkeypatch, fixed_now):
    """The regression itself: an artifact nothing has ever produced is a
    registry/producer-birth gap, and its absence is CORRECT."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    spec, result = _missing_spec(index)
    assert index._alert_decision(spec, result, fixed_now, never_written=True) is None
    # ...and the identical row DOES page once an instance has existed before.
    assert index._alert_decision(spec, result, fixed_now, never_written=False) is not None


def test_an_unanswerable_probe_keeps_the_page_path(monkeypatch, fixed_now):
    """`None` is not `True`. The failure this must lean toward is paging about a
    real absence, never silencing one — so 'I could not tell' behaves exactly
    like 'it has been written before'."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    spec, result = _missing_spec(index)
    assert index._alert_decision(spec, result, fixed_now, never_written=None) is not None


def test_probe_reports_none_not_false_when_s3_raises(monkeypatch):
    """A LIST that errors must not be read as evidence of absence."""
    import importlib
    import index
    importlib.reload(index)
    spec, result = _missing_spec(index)
    assert index._prefix_has_ever_been_written(
        _ListStub(RuntimeError("denied")), spec, result) is None
    # A response with no usable KeyCount is equally unanswerable.
    assert index._prefix_has_ever_been_written(_ListStub(None), spec, result) is None


def test_probe_distinguishes_empty_prefix_from_populated(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    spec, result = _missing_spec(index)
    assert index._prefix_has_ever_been_written(_ListStub(0), spec, result) is False
    assert index._prefix_has_ever_been_written(_ListStub(1), spec, result) is True


def test_probe_prefix_is_the_templates_fixed_head(monkeypatch):
    """A date-templated key must be probed at its fixed prefix, or the probe
    only ever asks about one cycle and calls every dated artifact never-written."""
    import importlib
    import index
    importlib.reload(index)
    from nousergon_lib.artifact_freshness import CheckResult
    spec, _ = _missing_spec(index, template="backtest/{trading_day}/contribution_lift.json")
    result = CheckResult(state="missing", sla_violated_by_minutes=300,
                         canonical_key="backtest/2026-08-18/contribution_lift.json",
                         reason="no instance found")
    stub = _ListStub(pages=[{"Contents": [], "IsTruncated": False}])
    index._prefix_has_ever_been_written(stub, spec, result)
    assert stub.calls[0]["Prefix"] == "backtest/"


def test_never_written_rows_are_reported_in_the_digest_not_silenced(monkeypatch, fixed_now):
    """Not paged is not the same as not said. The registry's debt stays on the
    same surface as the real misses — an unproduced row that vanishes from every
    surface is how one sits unnoticed for a year."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))

    covered = index._publish_digest([], fixed_now, {"rag_corpus_scope_state": True})
    publish_mock.assert_called_once()
    body = publish_mock.call_args.args[0]
    assert "1 registry row(s) never written" in body
    assert "artifact_id=rag_corpus_scope_state never_written=true" in body
    # A never-written-only digest is never critical — nothing is failing.
    assert publish_mock.call_args.kwargs["severity"] == "warning"
    assert covered == 0


def test_never_written_only_changes_the_dedup_key_when_the_set_moves(monkeypatch, fixed_now):
    import importlib
    import index
    importlib.reload(index)
    a = index._digest_dedup_key([], ["x"])
    b = index._digest_dedup_key([], ["x"])
    c = index._digest_dedup_key([], ["x", "y"])
    assert a == b and a != c


def test_a_clean_sweep_with_nothing_unproduced_still_sends_nothing(monkeypatch, fixed_now):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    assert index._publish_digest([], fixed_now, {"a": False, "b": None}) == 0
    publish_mock.assert_not_called()


# ── config-I7622: the owning-item budget was measuring the probe loop ────────


def test_owning_item_wall_clock_starts_at_the_first_lookup(monkeypatch):
    """MEASURED 2026-08-19T15:41:32Z: 2 pages, 2 degraded lookups, on a pass with
    11 confirmed misses and a 24-query budget. The 25s wall clock was started
    when the state was built — before a probe loop that took 68.5s over 146 rows
    — so it was already 40s over budget before the first GitHub query could run,
    and every row resolved `lookup_time_budget_exhausted` no matter how few
    misses there were. The budget was bounding the wrong interval."""
    import importlib
    import index
    importlib.reload(index)
    state = index._new_lookup_state()
    assert state["started_at"] is None, (
        "the clock must not start at construction — that is the probe loop's "
        "duration, not the lookup phase's"
    )


# ── config-I7622 follow-up: prefix membership is not the question ────────────
#
# MEASURED 2026-08-19: `research_self_test` (`research/{date}/self_test.json`)
# resolved never_written=False because the `research/` prefix is populated with
# thousands of unrelated objects — while
# `aws s3 ls s3://alpha-engine-research/research/ --recursive | grep self_test.json`
# returned NOTHING. The artifact has never once been written and the probe could
# not see it. `backtest_contribution_lift` sits under `backtest/` with the same
# shape. A shared top-level prefix answers a different question than the one
# being asked.


def _dated_spec(index_mod):
    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult
    spec = ArtifactSpec(
        artifact_id="research_self_test", s3_bucket="alpha-engine-research",
        s3_key_template="research/{date}/self_test.json", cadence="weekday_sf",
        sla_minutes_after_cron=2880, severity="warning",
        owner_repo="crucible-research", created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="missing", sla_violated_by_minutes=100,
                         canonical_key="research/2026-08-19/self_test.json",
                         reason="no instance found")
    return spec, result


def test_a_populated_prefix_without_the_suffix_is_never_written(monkeypatch):
    """The regression: `research/` is full, and not one object is a self_test."""
    import importlib
    import index
    importlib.reload(index)
    spec, result = _dated_spec(index)
    stub = _ListStub(pages=[{
        "Contents": [{"Key": "research/2026-08-19/signals.json"},
                     {"Key": "research/2026-08-19/rationale.json"}],
        "IsTruncated": False,
    }])
    assert index._prefix_has_ever_been_written(stub, spec, result) is False


def test_one_matching_key_anywhere_in_the_prefix_is_enough(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    spec, result = _dated_spec(index)
    stub = _ListStub(pages=[{
        "Contents": [{"Key": "research/2026-01-02/other.json"},
                     {"Key": "research/2026-03-04/self_test.json"}],
        "IsTruncated": False,
    }])
    assert index._prefix_has_ever_been_written(stub, spec, result) is True


def test_the_match_can_be_on_a_later_page(monkeypatch):
    """A match beyond the first page must still count — S3 lists lexically, so
    the newest keys are frequently in the tail."""
    import importlib
    import index
    importlib.reload(index)
    spec, result = _dated_spec(index)
    stub = _ListStub(pages=[
        {"Contents": [{"Key": "research/2026-01-02/other.json"}],
         "IsTruncated": True, "NextContinuationToken": "t1"},
        {"Contents": [{"Key": "research/2026-08-14/self_test.json"}],
         "IsTruncated": False},
    ])
    assert index._prefix_has_ever_been_written(stub, spec, result) is True
    assert stub.calls[1]["ContinuationToken"] == "t1"


def test_giving_up_at_the_page_cap_reports_unknown_not_never_written(monkeypatch):
    """A prefix too large to search is a question left UNANSWERED. Answering it
    'never written' on the strength of having given up is precisely the failure
    this function is shaped to avoid — the row keeps its page path."""
    monkeypatch.setenv("NEVER_WRITTEN_SCAN_PAGES", "2")
    import importlib
    import index
    importlib.reload(index)
    spec, result = _dated_spec(index)
    stub = _ListStub(pages=[
        {"Contents": [{"Key": f"research/x{i}/other.json"}], "IsTruncated": True,
         "NextContinuationToken": f"t{i}"}
        for i in range(4)
    ])
    assert index._prefix_has_ever_been_written(stub, spec, result) is None
    assert len(stub.calls) == 2, "the cap must actually bound the scan"


def test_a_truncated_page_with_no_token_stops_rather_than_looping(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    spec, result = _dated_spec(index)
    stub = _ListStub(pages=[{"Contents": [], "IsTruncated": True}])
    assert index._prefix_has_ever_been_written(stub, spec, result) is False


def test_a_prefix_shaped_template_still_uses_membership(monkeypatch):
    """`groom/{date}/` has no trailing fixed segment, so prefix membership IS
    the question and the cheap MaxKeys=1 path is correct."""
    import importlib
    import index
    importlib.reload(index)
    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult
    spec = ArtifactSpec(
        artifact_id="groom_run_artifacts", s3_bucket="alpha-engine-research",
        s3_key_template="groom/{date}/", cadence="continuous",
        interval_minutes=1440, sla_minutes_after_cron=60, severity="warning",
        owner_repo="alpha-engine-config", created_at=date(2025, 1, 1),
    )
    result = CheckResult(state="missing", sla_violated_by_minutes=10,
                         canonical_key="groom/2026-08-19/", reason="absent")
    stub = _ListStub(0)
    assert index._prefix_has_ever_been_written(stub, spec, result) is False
    assert stub.calls[0]["MaxKeys"] == 1


# ── config-I7730: the owning-item budget must be spent where it pages ────────
#
# MEASURED 2026-08-19T17:02:08Z, on Brian's Telegram page: the one paging row
# carried `owning_item_lookup=degraded owning_item_lookup_reason=
# 'lookup_budget_exhausted'`. The sweep had 12 confirmed misses, of which NINE
# could not page under any owning-item answer — 7 producer-suppressed
# (config-I6570) and 2 never-written (config-I7622) — and the 24-query cap was
# spent on them first. The join that exists to EXPLAIN a page was starved by
# rows that cannot produce one.
#
# Both gates below are exactly `_alert_decision`'s own early returns, and both
# sit above every use of `owning`, so skipping the lookup cannot change a
# verdict — it only stops paying for an answer nothing reads.


def _lookup_calls(index_mod, monkeypatch):
    """Record which artifact_ids the owning-item join is actually spent on."""
    spent: list[str] = []

    def fake(artifact_id, canonical_key, known, now, state):
        spent.append(artifact_id)
        return {"resolved": True, "degraded": False, "degraded_reason": None,
                "owning_item": None, "members": [], "n_candidates": 0}

    monkeypatch.setattr(index_mod, "_resolve_owning_item", fake)
    return spent


def test_a_suppressed_row_does_not_spend_a_github_query(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = yaml_registry_body
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spent = _lookup_calls(index, monkeypatch)

    # Every not-fresh row reports as producer-suppressed.
    monkeypatch.setattr(index, "apply_producer_suppression", lambda *a, **kw: {
        aid: {"suppressed": True, "reason": "producer disabled",
              "disabled_since": "2026-08-07", "days_disabled": 12,
              "trigger": "scheduler:x", "pause_owner": None, "pause_owner_url": None}
        for aid in ("probe_missing", "probe_heartbeat", "probe_fresh")
    })
    index.handler({}, None)
    assert spent == [], f"budget spent on rows that cannot page: {spent}"


def test_a_never_written_row_does_not_spend_a_github_query(monkeypatch, fixed_now):
    """The gate reads the never-written answer, so the probe must run FIRST."""
    import importlib
    import index
    importlib.reload(index)
    # The ordering is the contract: if the probe moved back below the join, the
    # gate would read an unpopulated dict and silently stop working.
    src = __import__("pathlib").Path(index.__file__).read_text()
    probe_at = src.index("never_written_by_id[spec.artifact_id] = never_written")
    join_at = src.index("_cannot_page = (")
    assert probe_at < join_at, (
        "the never-written probe must run BEFORE the owning-item budget gate "
        "that reads its answer (config-I7730)"
    )


def test_an_unsuppressed_paging_row_still_gets_its_lookup(
    monkeypatch, yaml_registry_body, fake_s3, fixed_now
):
    """The point is not fewer lookups — it is that the row on the page gets one."""
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    fake_s3._registry_body = yaml_registry_body
    import importlib
    import index
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False)))
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spent = _lookup_calls(index, monkeypatch)
    index.handler({}, None)
    assert "probe_missing" in spent


# ── Declared-off rows (alpha-engine-config-I8719) ───────────────────────────
#
# `backtest_pit_parity` CRITICAL-escalated every sweep for a stage that is off
# by ruling, with `escalation_basis=owning_item_age` — the escalation fired
# BECAUSE the deliberate disable was old. These pin the fix and, more
# importantly, pin that the mechanism fails toward PAGING in every direction.

_DO_BLOCK = {
    "since": "2026-05-13",
    "reason": "PitParityCompare is bypassed on every automatic weekly-SF "
              "launch path by skip_parity. Brian ruling 2026-05-13.",
    "owning_item": "alpha-engine-config-I7309",
    "clears_when": {"milestone": "crucible_phase_3"},
}


def _resolution(index, now, *, status="pending", age_hours=1.0, artifact="champion_feed"):
    resolved = (now - timedelta(hours=age_hours)).isoformat()
    return {
        "resolved_at": resolved,
        "row_count": 1,
        "rows": {artifact: {
            "milestone": "crucible_phase_3",
            "milestone_status": status,
            "suppresses": status == "pending",
        }},
    }


def _fresh_index(monkeypatch):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)
    return index


# ── parse_declared_off ──────────────────────────────────────────────────────


def test_parse_declared_off_keeps_a_well_formed_block(monkeypatch):
    index = _fresh_index(monkeypatch)
    out = index.parse_declared_off(
        {"artifacts": [{"artifact_id": "a", "declared_off": dict(_DO_BLOCK)}]}
    )
    assert out["a"]["owning_item"] == "alpha-engine-config-I7309"


@pytest.mark.parametrize("mutate", [
    lambda b: b.pop("since"),
    lambda b: b.pop("reason"),
    lambda b: b.pop("owning_item"),
    lambda b: b.pop("clears_when"),
    lambda b: b.__setitem__("clears_when", {"date": "2026-09-30"}),
    lambda b: b.__setitem__("clears_when", "crucible_phase_3"),
])
def test_a_malformed_declared_off_block_is_dropped_and_the_row_keeps_paging(
        monkeypatch, mutate):
    """Degrades to today's alerting, never to a registry that fails to load —
    a suppression field must not be able to take every row's monitoring down
    with it. The loud half is the PR-time validator in alpha-engine-config."""
    index = _fresh_index(monkeypatch)
    block = dict(_DO_BLOCK)
    mutate(block)
    out = index.parse_declared_off(
        {"artifacts": [{"artifact_id": "a", "declared_off": block}]}
    )
    assert out == {}


def test_a_non_mapping_declared_off_is_dropped(monkeypatch):
    index = _fresh_index(monkeypatch)
    assert index.parse_declared_off(
        {"artifacts": [{"artifact_id": "a", "declared_off": "off"}]}
    ) == {}


# ── resolve_declared_off ────────────────────────────────────────────────────


def test_a_pending_milestone_with_a_fresh_resolution_suppresses(
        monkeypatch, fixed_now):
    index = _fresh_index(monkeypatch)
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)},
        _resolution(index, fixed_now),
        fixed_now,
    )
    assert out["champion_feed"]["suppressed"] is True
    assert out["champion_feed"]["days_declared_off"] == 17
    assert out["champion_feed"]["clears_when_milestone"] == "crucible_phase_3"


def test_a_reached_milestone_does_not_suppress(monkeypatch, fixed_now):
    """The designed clearing path: normal freshness resumes the moment the
    milestone flips, with no second manual step and no separate expiry."""
    index = _fresh_index(monkeypatch)
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)},
        _resolution(index, fixed_now, status="reached"),
        fixed_now,
    )
    assert out["champion_feed"]["suppressed"] is False
    assert out["champion_feed"]["milestone_status"] == "reached"


def test_an_absent_resolution_does_not_suppress(monkeypatch, fixed_now):
    index = _fresh_index(monkeypatch)
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)}, None, fixed_now)
    assert out["champion_feed"]["suppressed"] is False


def test_a_resolution_for_a_different_artifact_does_not_suppress_this_one(
        monkeypatch, fixed_now):
    index = _fresh_index(monkeypatch)
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)},
        _resolution(index, fixed_now, artifact="some_other_row"),
        fixed_now,
    )
    assert out["champion_feed"]["suppressed"] is False


def test_a_stale_resolution_does_not_suppress(monkeypatch, fixed_now):
    """The one failure this must not introduce is a resolution nothing
    refreshes becoming a permanent blindfold. Past the ceiling, the row pages
    and the page reads as 'this has been off for N days', which is the
    decision the operator actually owes."""
    index = _fresh_index(monkeypatch)
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)},
        _resolution(index, fixed_now,
                    age_hours=index.DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS + 1),
        fixed_now,
    )
    assert out["champion_feed"]["suppressed"] is False
    assert out["champion_feed"]["resolution_fresh"] is False


def test_a_resolution_exactly_at_the_ceiling_still_suppresses(
        monkeypatch, fixed_now):
    index = _fresh_index(monkeypatch)
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)},
        _resolution(index, fixed_now,
                    age_hours=index.DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS),
        fixed_now,
    )
    assert out["champion_feed"]["suppressed"] is True


def test_an_unparseable_resolved_at_does_not_suppress(monkeypatch, fixed_now):
    index = _fresh_index(monkeypatch)
    res = _resolution(index, fixed_now)
    res["resolved_at"] = "not-a-timestamp"
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)}, res, fixed_now)
    assert out["champion_feed"]["suppressed"] is False


def test_an_unsuppressed_declared_off_row_is_still_returned(
        monkeypatch, fixed_now):
    """Dropping it would make 'declared off, suppression lapsed' and 'never
    declared anything' indistinguishable to every consumer downstream."""
    index = _fresh_index(monkeypatch)
    out = index.resolve_declared_off(
        {"champion_feed": dict(_DO_BLOCK)},
        _resolution(index, fixed_now, status="reached"),
        fixed_now,
    )
    assert "champion_feed" in out


def test_no_declared_off_rows_resolves_to_an_empty_map(monkeypatch, fixed_now):
    index = _fresh_index(monkeypatch)
    assert index.resolve_declared_off({}, None, fixed_now) == {}


# ── the escalation half: owning_item_age must not promote a declared-off row ─


def test_owning_item_age_does_not_promote_a_declared_off_row(
        monkeypatch, fixed_now):
    """The defect in one test. `owning_item_age` escalates a warning row once
    its owning item is old enough — and a declared-off row's owning item is
    old PRECISELY BECAUSE the producer is deliberately off. Same inputs as
    `test_owning_item_age_drives_escalation_not_the_miss_count`, which pages
    CRITICAL; the only difference is the declaration."""
    index = _fresh_index(monkeypatch)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _warning_spec_and_missing_result(index)

    declared = index.resolve_declared_off(
        {spec.artifact_id: dict(_DO_BLOCK)},
        _resolution(index, fixed_now, artifact=spec.artifact_id),
        fixed_now,
    )[spec.artifact_id]

    assert _page(index, spec, result, fixed_now, consecutive_miss_runs=3,
                 owning=_owning(age_days=90.0, sla_days=3),
                 declared_off=declared) is False
    publish_mock.assert_not_called()


def test_the_same_row_pages_when_its_declaration_is_not_suppressing(
        monkeypatch, fixed_now):
    """The control. Nothing about the escalation path was weakened — a row
    whose declared-off state has lapsed escalates exactly as before."""
    index = _fresh_index(monkeypatch)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _warning_spec_and_missing_result(index)

    declared = index.resolve_declared_off(
        {spec.artifact_id: dict(_DO_BLOCK)},
        _resolution(index, fixed_now, status="reached", artifact=spec.artifact_id),
        fixed_now,
    )[spec.artifact_id]

    assert _page(index, spec, result, fixed_now, consecutive_miss_runs=3,
                 owning=_owning(age_days=90.0, sla_days=3),
                 declared_off=declared) is True
    assert publish_mock.call_args.kwargs["severity"] == "critical"


def test_the_miss_count_ladder_also_cannot_escalate_a_declared_off_row(
        monkeypatch, fixed_now):
    index = _fresh_index(monkeypatch)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    spec, result = _warning_spec_and_missing_result(index)
    declared = index.resolve_declared_off(
        {spec.artifact_id: dict(_DO_BLOCK)},
        _resolution(index, fixed_now, artifact=spec.artifact_id),
        fixed_now,
    )[spec.artifact_id]
    assert _page(index, spec, result, fixed_now, consecutive_miss_runs=99,
                 declared_off=declared) is False
    publish_mock.assert_not_called()


def test_a_probe_failure_on_a_declared_off_row_still_does_not_page(
        monkeypatch, fixed_now):
    """Deliberate. A probe failure means the MONITOR is broken, but a
    declared-off row is one whose artifact is not expected to exist, so a
    failed probe over it carries no information about the producer. It still
    reaches check_results.json with state=probe_failed."""
    from nousergon_lib.artifact_freshness import CheckResult
    index = _fresh_index(monkeypatch)
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    spec, _ = _warning_spec_and_missing_result(index)
    result = CheckResult(state="probe_failed", reason="denied",
                         canonical_key=spec.s3_key_template,
                         sla_violated_by_minutes=0)
    declared = index.resolve_declared_off(
        {spec.artifact_id: dict(_DO_BLOCK)},
        _resolution(index, fixed_now, artifact=spec.artifact_id),
        fixed_now,
    )[spec.artifact_id]
    assert _page(index, spec, result, fixed_now, declared_off=declared) is False


# ── the console half: never silent ──────────────────────────────────────────


def test_a_declared_off_row_renders_with_its_true_state_and_its_age(
        monkeypatch, fixed_now):
    """Removes a PAGE, never a FACT. observability-policy.md §8.3: DISABLED is
    declared, and a declared state renders with its reason, its owner and its
    age — never green and never an omission."""
    index = _fresh_index(monkeypatch)
    spec, result = _warning_spec_and_missing_result(index)
    declared = index.resolve_declared_off(
        {spec.artifact_id: dict(_DO_BLOCK)},
        _resolution(index, fixed_now, artifact=spec.artifact_id),
        fixed_now,
    )
    payload = index._serialize_check_results(
        [(spec, result)], fixed_now, declared_off_by_id=declared)
    row = payload["results"][0]
    assert row["state"] == "missing"          # true state, unchanged
    assert row["declared_off"] is True
    assert row["declared_off_suppressed"] is True
    assert row["console_state"] == "DISABLED"
    assert row["days_declared_off"] == 17
    assert row["declared_off_owning_item"] == "alpha-engine-config-I7309"
    assert row["declared_off_clears_when_milestone"] == "crucible_phase_3"
    assert row["declared_off_milestone_status"] == "pending"
    assert row["alert_suppressed"] is True
    # Distinct from producer-trigger suppression: no live probe found a
    # disabled trigger, and conflating the two senses is how a check became
    # unclearable once already.
    assert row["producer_disabled"] is False
    assert row["producer_trigger"] is None


def test_a_lapsed_declared_off_row_is_not_rendered_DISABLED(
        monkeypatch, fixed_now):
    """`console_state` is a claim the monitor is currently acting on. A row
    that is paging must not simultaneously render as deliberately off."""
    index = _fresh_index(monkeypatch)
    spec, result = _warning_spec_and_missing_result(index)
    declared = index.resolve_declared_off(
        {spec.artifact_id: dict(_DO_BLOCK)},
        _resolution(index, fixed_now, status="reached", artifact=spec.artifact_id),
        fixed_now,
    )
    row = index._serialize_check_results(
        [(spec, result)], fixed_now, declared_off_by_id=declared)["results"][0]
    assert row["declared_off"] is True          # the declaration still shows
    assert row["declared_off_suppressed"] is False
    assert row["console_state"] is None
    assert row["alert_suppressed"] is False


def test_a_row_with_no_declaration_carries_the_fields_as_false_and_none(
        monkeypatch, fixed_now):
    """Emit zero rather than nothing — an absent field and a false one are
    not the same claim to a console."""
    index = _fresh_index(monkeypatch)
    spec, result = _warning_spec_and_missing_result(index)
    row = index._serialize_check_results(
        [(spec, result)], fixed_now)["results"][0]
    assert row["declared_off"] is False
    assert row["declared_off_suppressed"] is False
    assert row["days_declared_off"] is None
    assert row["console_state"] is None


# ── Per-run LIST cache + phase timing (alpha-engine-config-I9206) ────────────
#
# The daily 12:00 UTC sweep ran in 71s on 2026-08-22, ~100-112s on
# 08-27..08-31, and hit its 120s timeout every day from 2026-09-01 (3 Errors a
# day: the initial invocation plus two async retries). Measured cause, from the
# laptop against the live registry: `check_freshness` LISTs the whole
# `_listable_prefix` for EVERY templated spec — 106.7s summed per spec against
# 9.7s once per distinct (bucket, prefix). These tests pin the three things
# that make that fix real: the cache exists once per invocation, its absence is
# RECORDED rather than silent, and the next regression is readable from the log.


def _reloaded_index(monkeypatch, fake_s3, yaml_registry_body, fixed_now):
    import importlib
    import index

    monkeypatch.delenv("FRESHNESS_MONITOR_ENABLED", raising=False)
    fake_s3._registry_body = yaml_registry_body
    importlib.reload(index)
    _patch_now(monkeypatch, fixed_now)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **kw: fake_s3))
    monkeypatch.setattr(
        index, "publish", mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    )
    return index


def test_one_list_cache_is_created_per_invocation_and_threaded_to_every_spec(
    fake_s3, monkeypatch, yaml_registry_body, fixed_now
):
    """ONE cache per invocation, the SAME object for every spec in it, and a
    DIFFERENT object on the next invocation.

    The last clause is the one that matters: a module-level cache would be
    faster still and would answer today's freshness question with yesterday's
    listing, which is the inversion this monitor exists to catch."""
    from nousergon_lib.artifact_freshness import CheckResult

    index = _reloaded_index(monkeypatch, fake_s3, yaml_registry_body, fixed_now)
    monkeypatch.setattr(index, "_CHECK_FRESHNESS_ACCEPTS_LIST_CACHE", True)

    seen: list[list[int]] = []

    def fake_check_freshness(s3c, spec, now, *, list_cache=None):
        assert list_cache is not None, "the cache must be threaded, not dropped"
        seen[-1].append(id(list_cache))
        list_cache["probed"] = True
        return CheckResult(
            state="fresh", reason="ok", canonical_key=spec.s3_key_template
        )

    monkeypatch.setattr(index, "check_freshness", fake_check_freshness)

    seen.append([])
    first = index.handler({}, None)
    assert len(set(seen[-1])) == 1, "every spec in a pass shares one cache"
    assert len(seen[-1]) == first["n_entries_checked"] > 1

    seen.append([])
    index.handler({}, None)
    assert len(set(seen[-1])) == 1
    assert set(seen[-2]).isdisjoint(seen[-1]), (
        "a cache surviving into the next invocation would serve a stale listing"
    )
    assert first["list_cache_supported"] is True
    assert first["list_cache_entries"] == 1


def test_the_intraday_pass_owns_its_own_cache_too(
    fake_s3, monkeypatch, yaml_registry_body, fixed_now
):
    """One way check_freshness is called, not two."""
    from nousergon_lib.artifact_freshness import CheckResult

    index = _reloaded_index(monkeypatch, fake_s3, yaml_registry_body, fixed_now)
    monkeypatch.setattr(index, "_CHECK_FRESHNESS_ACCEPTS_LIST_CACHE", True)
    seen: list[int] = []

    def fake_check_freshness(s3c, spec, now, *, list_cache=None):
        assert list_cache is not None
        seen.append(id(list_cache))
        return CheckResult(
            state="fresh", reason="ok", canonical_key=spec.s3_key_template
        )

    monkeypatch.setattr(index, "check_freshness", fake_check_freshness)
    monkeypatch.setattr(index, "INTRADAY_ARTIFACT_IDS", {"probe_fresh"})
    index.handler({"mode": "intraday"}, None)
    assert seen and len(set(seen)) == 1


def test_an_unsupported_substrate_degrades_loudly_exactly_once(
    fake_s3, monkeypatch, yaml_registry_body, fixed_now, caplog
):
    """The fail-loud obligation. A pin that predates `list_cache=` must not
    degrade in silence: the call still happens (every verdict identical), and
    the degradation is a RECORDED fact naming the pin and the follow-up —
    once per pass, not once per spec across 185 specs."""
    index = _reloaded_index(monkeypatch, fake_s3, yaml_registry_body, fixed_now)
    monkeypatch.setattr(index, "_CHECK_FRESHNESS_ACCEPTS_LIST_CACHE", False)

    with caplog.at_level("WARNING"):
        result = index.handler({}, None)

    warnings = [
        r for r in caplog.records
        if index._LIST_CACHE_UNAVAILABLE_WARNING in r.getMessage()
    ]
    assert len(warnings) == 1, [w.getMessage() for w in warnings]
    text = warnings[0].getMessage()
    assert "nousergon-lib" in text and "alpha-engine-config-I9206" in text
    # The pass still ran and still produced verdicts — only the duration
    # degrades, never the answer.
    assert result["n_entries_checked"] > 0
    assert result["list_cache_supported"] is False


def test_the_complete_line_publishes_the_phase_breakdown(
    fake_s3, monkeypatch, yaml_registry_body, fixed_now, caplog
):
    """The 2026-09-01 timeout had to be re-derived from the laptop because the
    log published ONE number for the whole invocation. The phases are published
    so the next regression is readable from the log alone, and they sum to the
    total by construction."""
    index = _reloaded_index(monkeypatch, fake_s3, yaml_registry_body, fixed_now)

    with caplog.at_level("INFO"):
        result = index.handler({}, None)

    phases = result["phase_seconds"]
    assert set(phases) == {
        "registry_load", "producer_inventory", "spec_loop", "post_loop", "total",
    }
    for name, value in phases.items():
        assert isinstance(value, float), name
        assert value >= 0.0, name
    parts = phases["registry_load"] + phases["producer_inventory"] + \
        phases["spec_loop"] + phases["post_loop"]
    assert abs(parts - phases["total"]) < 0.05, phases

    line = next(
        r.getMessage() for r in caplog.records
        if r.getMessage().startswith("freshness-monitor complete:")
    )
    for token in ("registry_load=", "producer_inventory=", "spec_loop=",
                  "post_loop=", "total=", "list_cache="):
        assert token in line, (token, line)


# ── The producer-chosen `*` segment (alpha-engine-config-I10200) ─────────────
#
# A registry row may declare one path segment whose value the PRODUCER chooses
# at write time — `predictor/diagnostics/oos_rows/*/{date}.parquet`, scoped by
# the model FAMILY under alpha-engine-config-I9378 so a parallel zoo spec
# cannot overwrite the champion's diagnostic.
#
# `_prefix_has_ever_been_written` derived its prefix with a bare
# `template.split("{")`, which yields `oos_rows/*/` for the dated row and the
# WHOLE template — literal `*` and all — for `oos_rows/*/latest.parquet`. Both
# LIST against a prefix no object can start with, return nothing, and answer
# "never written" about an artifact that has existed since 2026-09-05. A
# never_written row does not page (see `_alert_decision`), so the wrong answer
# here is the one that silences a real absence.


def _wildcard_spec(index_mod, template="predictor/diagnostics/oos_rows/*/{date}.parquet"):
    from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult
    spec = ArtifactSpec(
        artifact_id="predictor_oos_rows_dated", s3_bucket="alpha-engine-research",
        s3_key_template=template, cadence="saturday_sf",
        sla_minutes_after_cron=240, severity="warning",
        owner_repo="alpha-engine-predictor", created_at=date(2025, 1, 1),
    )
    result = CheckResult(
        state="missing", sla_violated_by_minutes=5771,
        canonical_key="predictor/diagnostics/oos_rows/*/2026-09-04.parquet",
        reason="no instance found",
    )
    return spec, result


def test_wildcard_probe_lists_the_prefix_before_the_star(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    spec, result = _wildcard_spec(index)
    stub = _ListStub(pages=[{"Contents": [], "IsTruncated": False}])
    index._prefix_has_ever_been_written(stub, spec, result)
    assert stub.calls[0]["Prefix"] == "predictor/diagnostics/oos_rows/"


def test_wildcard_probe_lists_the_prefix_for_a_placeholderless_template(monkeypatch):
    """`oos_rows/*/latest.parquet` carries no brace at all, so the old
    `split("{")` head was the entire template including the literal `*`."""
    import importlib
    import index
    importlib.reload(index)
    spec, result = _wildcard_spec(
        index, template="predictor/diagnostics/oos_rows/*/latest.parquet",
    )
    stub = _ListStub(pages=[{"Contents": [], "IsTruncated": False}])
    index._prefix_has_ever_been_written(stub, spec, result)
    assert stub.calls[0]["Prefix"] == "predictor/diagnostics/oos_rows/"


def test_wildcard_probe_finds_the_live_instance(monkeypatch):
    import importlib
    import index
    importlib.reload(index)
    spec, result = _wildcard_spec(index)
    stub = _ListStub(pages=[{
        "Contents": [
            {"Key": "predictor/diagnostics/oos_rows/v3.0-meta/2026-09-04.parquet"},
        ],
        "IsTruncated": False,
    }])
    assert index._prefix_has_ever_been_written(stub, spec, result) is True


def test_wildcard_probe_does_not_count_a_sibling_diagnostic(monkeypatch):
    """A coarse `.parquet` suffix filter would sweep in every sibling under
    `predictor/diagnostics/` — the config-I7622 over-match, one level down."""
    import importlib
    import index
    importlib.reload(index)
    spec, result = _wildcard_spec(index)
    stub = _ListStub(pages=[{
        "Contents": [
            {"Key": "predictor/diagnostics/oos_rows/v3.0-meta/summary.json"},
            {"Key": "predictor/diagnostics/oos_rows/2026-09-04.parquet"},
        ],
        "IsTruncated": False,
    }])
    assert index._prefix_has_ever_been_written(stub, spec, result) is False
