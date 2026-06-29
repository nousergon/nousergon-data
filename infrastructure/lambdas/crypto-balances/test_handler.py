"""Unit tests for the crypto-balances Lambda handler.

No AWS / network: ``collect()`` is monkeypatched. Validates that the handler wraps a
healthy run (ok/skipped → 200), RAISES on a systemic failure (status="error" → EventBridge
retries surface it), and honors the kill-switch.
"""

from __future__ import annotations

import importlib
import os
import sys

import pytest

_HERE = os.path.dirname(__file__)
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
# Make both the vendored collector (collectors/crypto_balances.py, imported flat in the
# deployed package) and the handler importable.
sys.path.insert(0, os.path.join(_REPO, "collectors"))
sys.path.insert(0, _HERE)


def _load(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import index

    importlib.reload(index)
    return index


def test_ok_run_returns_200(monkeypatch):
    index = _load(monkeypatch, CRYPTO_BALANCES_ENABLED="true")
    monkeypatch.setattr(index.crypto_balances, "collect", lambda **kw: {"status": "ok", "n_balances": 2})
    out = index.handler({}, None)
    assert out["statusCode"] == 200 and out["body"]["status"] == "ok"


def test_skipped_run_returns_200(monkeypatch):
    index = _load(monkeypatch, CRYPTO_BALANCES_ENABLED="true")
    monkeypatch.setattr(index.crypto_balances, "collect", lambda **kw: {"status": "skipped", "reason": "no addresses"})
    assert index.handler({}, None)["statusCode"] == 200


def test_systemic_error_raises(monkeypatch):
    index = _load(monkeypatch, CRYPTO_BALANCES_ENABLED="true")
    monkeypatch.setattr(index.crypto_balances, "collect", lambda **kw: {"status": "error", "n_failed": 3})
    with pytest.raises(RuntimeError):
        index.handler({}, None)


def test_kill_switch_short_circuits(monkeypatch):
    index = _load(monkeypatch, CRYPTO_BALANCES_ENABLED="false")

    def _boom(**kw):
        raise AssertionError("collect() must not run when disabled")

    monkeypatch.setattr(index.crypto_balances, "collect", _boom)
    out = index.handler({}, None)
    assert out["statusCode"] == 200 and out["body"]["status"] == "disabled"
