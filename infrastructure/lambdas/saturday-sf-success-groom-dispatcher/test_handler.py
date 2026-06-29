"""Unit tests for the Saturday-SF success → groom dispatcher.

No AWS / GitHub calls — SSM and urllib are monkeypatched. Validates: only
SUCCEEDED dispatches; the kill-switch short-circuits; a SUCCEEDED event posts the
correct repository_dispatch to alpha-engine-config; failures are recorded, not
raised.
"""

from __future__ import annotations

import importlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))


def _load(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import index

    importlib.reload(index)
    return index


def test_non_succeeded_does_not_dispatch(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    calls = {"n": 0}
    monkeypatch.setattr(
        idx, "_dispatch_groom", lambda *a, **k: calls.__setitem__("n", calls["n"] + 1)
    )
    out = idx.handler({"detail": {"status": "FAILED"}}, None)
    assert out["dispatched"] is False
    assert calls["n"] == 0


def test_disabled_flag_short_circuits(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="false")
    out = idx.handler({"detail": {"status": "SUCCEEDED", "name": "x"}}, None)
    assert out["groom"]["dispatched"] is False
    assert out["groom"]["reason"] == "disabled"


def test_succeeded_dispatches_correct_repository_dispatch(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    monkeypatch.setattr(idx, "_get_github_pat", lambda: "tok")
    captured = {}

    class _Resp:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def _fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["body"] = json.loads(req.data.decode())
        captured["auth"] = req.headers.get("Authorization")
        return _Resp()

    monkeypatch.setattr(idx.urllib.request, "urlopen", _fake_urlopen)
    event = {
        "detail": {
            "status": "SUCCEEDED",
            "name": "sat-run-1",
            "stateMachineArn": "arn:aws:states:us-east-1:1:stateMachine:ne-weekly-freshness-pipeline",
        }
    }
    out = idx.handler(event, None)
    assert out["groom"]["dispatched"] is True
    assert out["groom"]["status_code"] == 204
    assert captured["url"].endswith("/repos/nousergon/alpha-engine-config/dispatches")
    assert captured["body"]["event_type"] == "saturday-sf-success-groom"
    assert captured["body"]["client_payload"]["execution_name"] == "sat-run-1"
    assert captured["auth"] == "Bearer tok"


def test_dispatch_failure_is_recorded_not_raised(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    monkeypatch.setattr(idx, "_get_github_pat", lambda: "tok")

    def _boom(req, timeout=0):
        raise RuntimeError("github down")

    monkeypatch.setattr(idx.urllib.request, "urlopen", _boom)
    out = idx.handler({"detail": {"status": "SUCCEEDED", "name": "x"}}, None)
    assert out["groom"]["dispatched"] is False
    assert "github down" in out["groom"]["error"]
