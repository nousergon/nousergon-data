"""Unit tests for the Saturday-SF success → groom dispatcher.

No AWS calls — boto3 is monkeypatched. Validates: only SUCCEEDED dispatches;
the kill-switch short-circuits; a SUCCEEDED event fires an ASYNC boto3 invoke
of alpha-engine-scheduled-groom-dispatcher carrying the demand-all trigger
event (config#2175 — no GitHub repository_dispatch, no PAT read); failures are
recorded, not raised.
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


def test_succeeded_invokes_groom_dispatcher_async(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")
    captured = {}

    class _FakeLambda:
        def invoke(self, **kw):
            captured.update(kw)
            return {"StatusCode": 202}

    monkeypatch.setattr(
        idx.boto3, "client", lambda name, **kw: {"lambda": _FakeLambda()}[name]
    )
    event = {
        "detail": {
            "status": "SUCCEEDED",
            "name": "sat-run-1",
            "stateMachineArn": "arn:aws:states:us-east-1:1:stateMachine:ne-weekly-freshness-pipeline",
        }
    }
    out = idx.handler(event, None)
    assert out["groom"]["dispatched"] is True
    assert out["groom"]["status_code"] == 202
    assert captured["FunctionName"] == "alpha-engine-scheduled-groom-dispatcher"
    # Async — this Lambda must never babysit the multi-hour groom.
    assert captured["InvocationType"] == "Event"
    payload = json.loads(captured["Payload"].decode())
    # The exact config#2175 demand-all trigger event: the dispatcher enumerates
    # the fresh post-SF backlog per tier (its own pace/demand gates apply).
    assert payload == {
        "run_mode": "full",
        "trigger": "demand-all",
        "schedule": "saturday-sf-success",
    }


def test_no_github_dispatch_machinery_remains():
    # config#2175: the GitHub PAT read + urllib repository_dispatch are GONE —
    # a reintroduction would silently resurrect the retired GHA groom path.
    src = open(os.path.join(os.path.dirname(__file__), "index.py")).read()
    assert "import urllib" not in src
    assert "urlopen" not in src
    assert "api.github.com" not in src
    assert "github_pat" not in src.lower()
    assert "ssm" not in src.lower()


def test_dispatch_failure_is_recorded_not_raised(monkeypatch):
    idx = _load(monkeypatch, GROOM_DISPATCH_ENABLED="true")

    class _BoomLambda:
        def invoke(self, **kw):
            raise RuntimeError("lambda service down")

    monkeypatch.setattr(
        idx.boto3, "client", lambda name, **kw: {"lambda": _BoomLambda()}[name]
    )
    out = idx.handler({"detail": {"status": "SUCCEEDED", "name": "x"}}, None)
    assert out["groom"]["dispatched"] is False
    assert "lambda service down" in out["groom"]["error"]
