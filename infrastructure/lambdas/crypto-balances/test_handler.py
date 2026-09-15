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
# Make the vendored collector (collectors/crypto_balances.py), the vendored
# repo-root modules (run_units.py / dates.py — both
# imported flat in the deployed package) and the handler importable.
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "collectors"))
sys.path.insert(0, _HERE)


@pytest.fixture(autouse=True)
def fake_manifest_sink(monkeypatch):
    """No test in this file may write a real run manifest to S3.

    ``index.handler`` runs the collector through
    ``run_units.recorded_entry`` (alpha-engine-config-I10810), which builds
    an ``S3ManifestSink`` on the production bucket. Swapping the sink — rather
    than the wrapper — keeps these tests on the real code path while making the
    one write land in memory. Returns the list of (key, payload) written.
    """
    import run_units

    written: list[tuple[str, bytes]] = []

    class _Sink:
        bucket = "test-bucket"

        def write(self, key: str, payload: bytes):
            written.append((key, payload))
            return None

    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: _Sink())
    return written


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
