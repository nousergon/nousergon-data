"""Unit tests for the EventBridge-Scheduler → scheduled groom dispatcher.

No AWS / GitHub calls — SSM and urllib are monkeypatched. Validates: a schedule
event posts the correct repository_dispatch to alpha-engine-config carrying the
run_mode; run_mode normalisation/fallback; the kill-switch short-circuits; and
(UNLIKE the convenience success-dispatcher) a dispatch failure RAISES so
EventBridge retries + the error metric surface a dropped pass.
"""

from __future__ import annotations

import importlib
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))


def _load(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import index

    importlib.reload(index)
    return index


def _stub_resp(status=204):
    class _Resp:
        def __init__(self):
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    return _Resp()


def test_schedule_event_dispatches_correct_repository_dispatch(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    monkeypatch.setattr(idx, "_get_github_pat", lambda: "tok")
    captured = {}

    def _fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["body"] = json.loads(req.data.decode())
        captured["auth"] = req.headers.get("Authorization")
        return _stub_resp(204)

    monkeypatch.setattr(idx.urllib.request, "urlopen", _fake_urlopen)
    out = idx.handler({"run_mode": "full", "schedule": "0 23 * * *"}, None)
    assert out["groom"]["dispatched"] is True
    assert out["groom"]["status_code"] == 204
    assert out["groom"]["run_mode"] == "full"
    assert captured["url"].endswith("/repos/nousergon/alpha-engine-config/dispatches")
    assert captured["body"]["event_type"] == "scheduled-groom"
    assert captured["body"]["client_payload"]["run_mode"] == "full"
    assert captured["body"]["client_payload"]["phase"] == "full"
    assert captured["body"]["client_payload"]["schedule"] == "0 23 * * *"
    assert captured["auth"] == "Bearer tok"


def test_unknown_run_mode_falls_back_to_full(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    monkeypatch.setattr(idx, "_get_github_pat", lambda: "tok")
    captured = {}

    def _fake_urlopen(req, timeout=0):
        captured["body"] = json.loads(req.data.decode())
        return _stub_resp(204)

    monkeypatch.setattr(idx.urllib.request, "urlopen", _fake_urlopen)
    out = idx.handler({"run_mode": "bogus"}, None)
    assert out["groom"]["run_mode"] == "full"
    assert captured["body"]["client_payload"]["run_mode"] == "full"


def test_sweep_run_mode_is_forwarded(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    monkeypatch.setattr(idx, "_get_github_pat", lambda: "tok")
    captured = {}

    def _fake_urlopen(req, timeout=0):
        captured["body"] = json.loads(req.data.decode())
        return _stub_resp(204)

    monkeypatch.setattr(idx.urllib.request, "urlopen", _fake_urlopen)
    out = idx.handler({"run_mode": "sweep"}, None)
    assert out["groom"]["run_mode"] == "sweep"
    assert captured["body"]["client_payload"]["run_mode"] == "sweep"


def test_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="false")
    out = idx.handler({"run_mode": "full"}, None)
    assert out["groom"]["dispatched"] is False
    assert out["groom"]["reason"] == "disabled"


def test_dispatch_failure_raises(monkeypatch):
    # Fail-loud: a scheduled groom is the deliverable, so a GitHub failure must
    # RAISE (EventBridge retries + the Lambda error metric record the miss).
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    monkeypatch.setattr(idx, "_get_github_pat", lambda: "tok")

    def _boom(req, timeout=0):
        raise RuntimeError("github down")

    monkeypatch.setattr(idx.urllib.request, "urlopen", _boom)
    with pytest.raises(RuntimeError, match="github down"):
        idx.handler({"run_mode": "full"}, None)
