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
    """Continuous active-window fields (nousergon-lib >=0.63.0) must survive
    the _SPEC_FIELDS strip and thread through to ArtifactSpec, with
    active_hours_utc coerced from a YAML list to a tuple."""
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
    assert spec.active_trading_days_only is True
    assert spec.active_hours_utc == (14, 21)


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
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
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
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
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
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
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
      target: "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-weekday-pipeline"
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
    assert kwargs["stateMachineArn"].endswith("alpha-engine-weekday-pipeline")
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

    from alpha_engine_lib.artifact_freshness import ArtifactSpec
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
    from alpha_engine_lib.artifact_freshness import ArtifactSpec
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
    monkeypatch.setattr(index, "publish", publish_mock)

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
    specs, recovery = index.load_registry_with_recovery(fake_s3, "b", "k")
    assert len(specs) == 2
    assert set(recovery) == {"closes_recoverable"}
    assert recovery["closes_recoverable"]["type"] == "step_function"
    # Back-compat: load_registry still returns just the list.
    assert len(index.load_registry(fake_s3, "b", "k")) == 2
