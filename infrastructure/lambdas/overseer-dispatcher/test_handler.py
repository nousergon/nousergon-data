"""Handler tests for alpha-engine-overseer-dispatcher (alpha-engine-config-I2823).

Pins the router contract: registry-driven routing to the executor, bool
stringification, benign-decline pass-through, escalation on non-benign
verdicts / invoke failures / wiring errors (P1 + loud page, both best-effort),
kill switches (env + per-playbook), ledger best-effort, and the clean-JSON
never-raise posture.

Hermetic: boto3 and krepis are stubbed in sys.modules before ``import index``
(the deploy.sh preflight gate runs this file the same way the sibling
dispatchers' gates do).
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent


def _load_index(monkeypatch, registry_path: Path):
    """Import a fresh index module against a stub boto3 + given registry."""
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = MagicMock(name="boto3.client")
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setenv("OVERSEER_REGISTRY_PATH", str(registry_path))
    monkeypatch.syspath_prepend(str(SCRIPT_DIR))
    sys.modules.pop("index", None)
    index = importlib.import_module("index")
    index._REGISTRY_CACHE = None
    return index, fake_boto3


REGISTRY = SCRIPT_DIR.parent.parent / "overseer" / "playbooks.yaml"


@pytest.fixture
def index_mod(monkeypatch):
    index, fake_boto3 = _load_index(monkeypatch, REGISTRY)
    # Never let escalation legs hit the network in tests.
    monkeypatch.setattr(index, "_file_p1",
                        MagicMock(return_value={"filed": True, "url": "http://x"}))
    monkeypatch.setattr(index, "_page_loud", MagicMock(return_value=True))
    monkeypatch.setattr(index, "_write_ledger", MagicMock(return_value="ledger/key"))
    return index, fake_boto3


def _lambda_client_returning(fake_boto3, verdict: dict, function_error: str | None = None):
    lam = MagicMock()
    payload = MagicMock()
    payload.read.return_value = json.dumps(verdict).encode()
    resp = {"Payload": payload}
    if function_error:
        resp["FunctionError"] = function_error
    lam.invoke.return_value = resp
    fake_boto3.client.return_value = lam
    return lam


class TestRouting:
    def test_routes_sf_watch_to_executor_with_stringified_bools(self, index_mod):
        index, fake_boto3 = index_mod
        lam = _lambda_client_returning(fake_boto3, {"launched": True, "reason": "launched"})
        out = index.handler({"playbook": "sf-watch",
                             "payload": {"pipeline_name": "ne-weekly-freshness-pipeline",
                                         "is_preflight": False, "run_date": "2026-07-17"}}, None)
        assert out["routed"] is True
        assert out["verdict"]["launched"] is True
        assert out["escalation"] is None
        sent = json.loads(lam.invoke.call_args.kwargs["Payload"])
        assert sent["is_preflight"] == "false"
        assert lam.invoke.call_args.kwargs["FunctionName"] == \
            "alpha-engine-sf-watch-spot-dispatcher"
        assert lam.invoke.call_args.kwargs["InvocationType"] == "RequestResponse"

    def test_ci_watch_routes_to_its_executor(self, index_mod):
        index, fake_boto3 = index_mod
        lam = _lambda_client_returning(fake_boto3, {"launched": True, "reason": "launched"})
        out = index.handler({"playbook": "ci-watch",
                             "payload": {"repo": "nousergon/krepis", "sha": "a" * 40}}, None)
        assert out["routed"] is True
        assert lam.invoke.call_args.kwargs["FunctionName"] == "alpha-engine-ci-watch-dispatcher"

    def test_benign_decline_does_not_escalate(self, index_mod):
        index, fake_boto3 = index_mod
        _lambda_client_returning(fake_boto3, {"launched": False, "reason": "deferred"})
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["routed"] is True and out["benign"] is True
        assert out["escalation"] is None
        index._file_p1.assert_not_called()

    def test_non_benign_decline_escalates_p1_and_page(self, index_mod):
        index, fake_boto3 = index_mod
        _lambda_client_returning(fake_boto3,
                                 {"launched": False, "reason": "defer_exhausted"})
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["benign"] is False
        assert out["escalation"]["p1"]["filed"] is True
        assert out["escalation"]["paged"] is True
        index._file_p1.assert_called_once()
        assert "defer_exhausted" in index._file_p1.call_args.args

    def test_launched_true_never_escalates_even_with_odd_reason(self, index_mod):
        index, fake_boto3 = index_mod
        _lambda_client_returning(fake_boto3, {"launched": True, "reason": "weird"})
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["escalation"] is None


class TestWiringFailures:
    def test_executor_invoke_error_escalates_clean_json(self, index_mod):
        index, fake_boto3 = index_mod
        _lambda_client_returning(fake_boto3, {"x": 1}, function_error="Unhandled")
        out = index.handler({"playbook": "ci-watch", "payload": {}}, None)
        assert out["routed"] is False and out["reason"] == "executor_invoke_failed"
        index._file_p1.assert_called_once()

    def test_unknown_playbook_escalates(self, index_mod):
        index, _ = index_mod
        out = index.handler({"playbook": "nope", "payload": {}}, None)
        assert out["reason"] == "unknown_playbook"
        index._file_p1.assert_called_once()

    def test_inventory_only_playbook_refused_and_escalated(self, index_mod):
        index, _ = index_mod
        out = index.handler({"playbook": "groom", "payload": {}}, None)
        assert out["reason"] == "not_routable"
        index._file_p1.assert_called_once()

    def test_invalid_event_escalates(self, index_mod):
        index, _ = index_mod
        out = index.handler({"payload": "not-a-dict"}, None)
        assert out["reason"] == "invalid_event"
        index._file_p1.assert_called_once()

    def test_escalation_legs_both_failing_still_returns_clean(self, index_mod, caplog):
        index, fake_boto3 = index_mod
        index._file_p1.return_value = {"filed": False, "error": "gh down"}
        index._page_loud.return_value = False
        _lambda_client_returning(fake_boto3,
                                 {"launched": False, "reason": "launch_failed"})
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["routed"] is True
        assert "OVERSEER_ESCALATION_DELIVERY_FAILED" in caplog.text


class TestKillSwitches:
    def test_global_disable(self, index_mod, monkeypatch):
        index, _ = index_mod
        monkeypatch.setattr(index, "DISPATCH_ENABLED", False)
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out == {"routed": False, "reason": "disabled", "playbook": "sf-watch"}
        index._file_p1.assert_not_called()

    def test_registry_disabled_playbook_declines_without_escalation(
        self, index_mod, monkeypatch, tmp_path
    ):
        index, _ = index_mod
        disabled = {
            "schema_version": 1,
            "playbooks": {"sf-watch": {
                "routed": True, "enabled": False,
                "executor_function": "alpha-engine-sf-watch-spot-dispatcher",
            }},
        }
        import yaml as _yaml
        p = tmp_path / "reg.yaml"
        p.write_text(_yaml.safe_dump(disabled))
        monkeypatch.setattr(index, "REGISTRY_PATH", p)
        index._REGISTRY_CACHE = None
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["reason"] == "playbook_disabled"
        index._file_p1.assert_not_called()


class TestLedger:
    def test_ledger_written_on_routed_dispatch(self, index_mod):
        index, fake_boto3 = index_mod
        _lambda_client_returning(fake_boto3, {"launched": True, "reason": "launched"})
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["ledger_key"] == "ledger/key"
        index._write_ledger.assert_called_once()

    def test_ledger_failure_does_not_break_dispatch(self, index_mod):
        index, fake_boto3 = index_mod
        index._write_ledger.return_value = None
        _lambda_client_returning(fake_boto3, {"launched": True, "reason": "launched"})
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["routed"] is True and out["ledger_key"] is None


class TestRegistryBundle:
    def test_bundled_registry_parses_and_has_routed_playbooks(self, index_mod):
        index, _ = index_mod
        reg = index._registry()
        routed = {k for k, v in reg["playbooks"].items() if v.get("routed")}
        assert routed == {"sf-watch", "ci-watch", "alert-drain"}
        for name in routed:
            assert reg["playbooks"][name]["benign_reasons"]

    def test_registry_error_escalates(self, index_mod, monkeypatch, tmp_path):
        index, _ = index_mod
        monkeypatch.setattr(index, "REGISTRY_PATH", tmp_path / "missing.yaml")
        index._REGISTRY_CACHE = None
        out = index.handler({"playbook": "sf-watch", "payload": {}}, None)
        assert out["reason"] == "registry_error"
        index._file_p1.assert_called_once()


class TestInvokeClientConfig:
    """The executor invoke is non-idempotent: the boto client MUST use zero
    retries and a read-timeout exceeding the slowest executor (the first live
    drill double-dispatched via boto3's silent default retry)."""

    def test_zero_retries_and_long_read_timeout(self, index_mod):
        index, fake_boto3 = index_mod
        _lambda_client_returning(fake_boto3, {"launched": True, "reason": "launched"})
        index.handler({"playbook": "sf-watch", "payload": {}}, None)
        cfg = fake_boto3.client.call_args.kwargs.get("config")
        assert cfg is index._EXECUTOR_INVOKE_CONFIG
        assert cfg.retries == {"max_attempts": 0}
        assert cfg.read_timeout == 290


class TestModelInjection:
    """config-I3293: the registry is the SSoT for the agent's model — the
    router injects the playbook's declared `model` into the executor payload,
    and a caller-provided model (drill/operator override) wins."""

    def test_registry_model_injected_into_executor_payload(self, index_mod):
        index, fake_boto3 = index_mod
        lam = _lambda_client_returning(fake_boto3, {"launched": True, "reason": "launched"})
        index.handler({"playbook": "sf-watch",
                       "payload": {"pipeline_name": "ne-weekly-freshness-pipeline",
                                   "run_date": "2026-07-17"}}, None)
        sent = json.loads(lam.invoke.call_args.kwargs["Payload"])
        reg_model = index._registry()["playbooks"]["sf-watch"]["model"]
        assert sent["model"] == reg_model
        assert reg_model.startswith("claude-")

    def test_caller_model_override_wins(self, index_mod):
        index, fake_boto3 = index_mod
        lam = _lambda_client_returning(fake_boto3, {"launched": True, "reason": "launched"})
        index.handler({"playbook": "alert-drain",
                       "payload": {"trigger": "operator", "model": "claude-opus-4-8"}},
                      None)
        sent = json.loads(lam.invoke.call_args.kwargs["Payload"])
        assert sent["model"] == "claude-opus-4-8"
