"""Unit tests for canary-replay-liveness-probe (alpha-engine-config#2246).

Hermetic: ``boto3`` and ``nousergon_lib.alerts`` are stubbed in sys.modules
BEFORE importing index. Validates: the self-gating check window (no-op
before/after), the deterministic Thursday-dispatch-time derivation, the
paging-on-missing-marker and paging-on-FAIL-marker paths, the clean-pass
no-page path, fail-loud on an unexpected S3 error, and the
``_scheduled_run_token`` lockstep with canary-replay-dispatcher/index.py.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import types
from datetime import datetime, timezone

import pytest


class _FakeS3:
    def __init__(self, objects: dict[str, bytes] | None = None, raise_error: Exception | None = None):
        self._objects = dict(objects or {})
        self._raise_error = raise_error

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        if self._raise_error is not None:
            raise self._raise_error
        if Key not in self._objects:
            err = type("ClientError", (Exception,), {})()
            err.response = {"Error": {"Code": "NoSuchKey"}}
            raise err
        body = types.SimpleNamespace(read=lambda: self._objects[Key])
        return {"Body": body}


class _FakeEvents:
    def __init__(self, rule_state: str | None = "ENABLED"):
        self._rule_state = rule_state

    def describe_rule(self, Name):  # noqa: N803 — boto3 kwarg names
        if self._rule_state is None:
            err = type("ClientError", (Exception,), {})()
            err.response = {"Error": {"Code": "ResourceNotFoundException"}}
            raise err
        return {"Name": Name, "State": self._rule_state}


def _install_stubs(s3_objects=None, s3_raises=None, dispatch_rule_state="ENABLED"):
    published: list[dict] = []

    clients = {
        "s3": _FakeS3(s3_objects, s3_raises),
        "events": _FakeEvents(dispatch_rule_state),
    }
    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda name, **kw: clients[name]
    sys.modules["boto3"] = boto3_mod

    alerts_mod = types.ModuleType("nousergon_lib.alerts")

    def _publish(message, **kw):
        published.append({"message": message, **kw})

    alerts_mod.publish = _publish

    nousergon_lib_mod = types.ModuleType("nousergon_lib")
    nousergon_lib_mod.alerts = alerts_mod
    sys.modules["nousergon_lib"] = nousergon_lib_mod
    sys.modules["nousergon_lib.alerts"] = alerts_mod

    return published


def _reload_index():
    sys.path.insert(0, os.path.dirname(__file__))
    if "index" in sys.modules:
        del sys.modules["index"]
    import index  # noqa: PLC0415

    return importlib.reload(index)


class TestScheduledRunTokenLockstep:
    def test_matches_dispatcher_derivation(self):
        """Pins this Lambda's duplicated _scheduled_run_token against
        canary-replay-dispatcher's — both must derive the SAME key for the
        same instant, or the probe would poll a marker key the dispatcher
        never writes."""
        _install_stubs()
        index = _reload_index()

        dispatcher_dir = os.path.join(
            os.path.dirname(__file__), "..", "canary-replay-dispatcher"
        )
        sys.path.insert(0, dispatcher_dir)
        if "dispatcher_index" in sys.modules:
            del sys.modules["dispatcher_index"]

        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "dispatcher_index", os.path.join(dispatcher_dir, "index.py")
        )
        # dispatcher's index.py imports nousergon_lib.spot_dispatch at module
        # load — stub it minimally just for this import.
        spot_dispatch_mod = types.ModuleType("nousergon_lib.spot_dispatch")
        spot_dispatch_mod.SpotLaunchError = Exception
        spot_dispatch_mod.SpotProbeError = Exception
        spot_dispatch_mod.launch_with_fallback = lambda *a, **kw: None
        spot_dispatch_mod.running_instance_ids = lambda *a, **kw: []
        spot_dispatch_mod.wait_ssm_online = lambda *a, **kw: None
        spot_dispatch_mod.send_async_command = lambda *a, **kw: None
        spot_dispatch_mod.terminate_on_failure = lambda *a, **kw: None
        sys.modules["nousergon_lib"].spot_dispatch = spot_dispatch_mod
        sys.modules["nousergon_lib.spot_dispatch"] = spot_dispatch_mod

        dispatcher_index = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dispatcher_index)

        for now in [
            datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
        ]:
            assert index._scheduled_run_token(now) == dispatcher_index._scheduled_run_token(now)


class TestCheckWindowGating:
    def test_before_window_start_is_a_noop(self, monkeypatch):
        _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 16, 9, 10, tzinfo=timezone.utc)  # Thursday, 10 min after dispatch
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["checked"] is False
        assert result["reason"] == "outside_check_window"

    def test_after_window_end_is_a_noop(self, monkeypatch):
        _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc)  # Saturday morning
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["checked"] is False

    def test_inside_window_checks(self, monkeypatch):
        published = _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)  # 1h after Thursday dispatch
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["checked"] is True
        assert result["marker_found"] is False
        assert result["paged"] is True
        assert len(published) == 1


def _make_fixed_datetime(fixed_now):
    class _FD(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now
    return _FD


class TestMarkerOutcomes:
    def test_missing_marker_pages(self, monkeypatch):
        published = _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["marker_found"] is False
        assert result["paged"] is True
        assert "NO completion marker" in published[0]["message"]

    def test_fail_marker_pages_with_probe_detail(self, monkeypatch):
        run_token = "sched-2026w29"
        marker = {
            "run_id": run_token,
            "overall_status": "FAIL",
            "probes": [
                {"name": "filing_change_detection", "status": "PASS"},
                {"name": "thesis_update", "status": "FAIL"},
            ],
        }
        published = _install_stubs(
            s3_objects={f"tmp/canary/{run_token}.json": json.dumps(marker).encode()}
        )
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["marker_found"] is True
        assert result["overall_status"] == "FAIL"
        assert result["paged"] is True
        assert "thesis_update" in published[0]["message"]

    def test_pass_marker_does_not_page(self, monkeypatch):
        run_token = "sched-2026w29"
        marker = {"run_id": run_token, "overall_status": "PASS", "probes": []}
        published = _install_stubs(
            s3_objects={f"tmp/canary/{run_token}.json": json.dumps(marker).encode()}
        )
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["marker_found"] is True
        assert result["overall_status"] == "PASS"
        assert result["paged"] is False
        assert published == []

    def test_unexpected_s3_error_raises(self, monkeypatch):
        _install_stubs(s3_raises=RuntimeError("S3 is having a bad day"))
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        with pytest.raises(RuntimeError, match="bad day"):
            index.handler({}, None)


class TestDispatchTimeDerivation:
    def test_thursday_after_dispatch_hour_uses_today(self):
        _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 16, 15, 0, tzinfo=timezone.utc)  # Thursday afternoon
        result = index._most_recent_thursday_dispatch(now)
        assert result == datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)

    def test_wednesday_uses_previous_thursday(self):
        _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)  # Wednesday
        result = index._most_recent_thursday_dispatch(now)
        assert result == datetime(2026, 7, 9, 9, 0, tzinfo=timezone.utc)

    def test_thursday_before_dispatch_hour_uses_previous_week(self):
        _install_stubs()
        index = _reload_index()
        now = datetime(2026, 7, 16, 5, 0, tzinfo=timezone.utc)  # Thursday, before 09:00
        result = index._most_recent_thursday_dispatch(now)
        assert result == datetime(2026, 7, 9, 9, 0, tzinfo=timezone.utc)


class TestDisabledDispatcherCarveOut:
    def test_disabled_rule_is_state_not_incident_no_page(self, monkeypatch):
        published = _install_stubs(dispatch_rule_state="DISABLED")
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["checked"] is False
        assert result["reason"] == "dispatch_rule_disabled"
        assert published == []

    def test_missing_rule_also_treated_as_nothing_to_check(self, monkeypatch):
        published = _install_stubs(dispatch_rule_state=None)
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["checked"] is False
        assert result["reason"] == "dispatch_rule_disabled"
        assert published == []

    def test_enabled_rule_proceeds_to_check_and_pages_on_missing_marker(self, monkeypatch):
        published = _install_stubs(dispatch_rule_state="ENABLED")
        index = _reload_index()
        now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "datetime", _make_fixed_datetime(now))
        result = index.handler({}, None)
        assert result["checked"] is True
        assert result["paged"] is True
        assert len(published) == 1
