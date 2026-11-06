"""observability-policy §7.2b (Brian, 2026-11-06): the pipelines notifier sends
ONE chat message a day, on the day's closing terminal, success or failure.
RUNNING, preflight and every other terminal send nothing to the chat."""
from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

LAMBDA = Path(__file__).resolve().parents[1] / "infrastructure" / "lambdas" / "sf-telegram-notifier"
ARN = "arn:aws:states:us-east-1:111111111111:stateMachine:"


@pytest.fixture
def mod(monkeypatch):
    sent: list[dict] = []
    emitted: list[dict] = []
    fdt = types.ModuleType("flow_doctor_telegram")
    fdt.build_flow_doctor_config = lambda *a, **k: {}
    fdt.notify_via_flow_doctor = lambda text, **kw: sent.append({"text": text, **kw}) or True
    fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")
    fleet.FleetTelegramTopic = types.SimpleNamespace(PIPELINE="pipeline")
    fleet.PIPELINE_OBSERVER_TELEGRAM_TOPICS = ()
    nl = types.ModuleType("nousergon_lib")
    fe = types.ModuleType("krepis.fleet_events")
    fe.emit_alert_event = lambda **kw: emitted.append(kw) or True
    kr = types.ModuleType("krepis")
    for name, m in {"flow_doctor_telegram": fdt, "nousergon_lib": nl,
                    "nousergon_lib.flow_doctor_fleet": fleet,
                    "krepis": kr, "krepis.fleet_events": fe}.items():
        monkeypatch.setitem(sys.modules, name, m)
    monkeypatch.syspath_prepend(str(LAMBDA))
    monkeypatch.syspath_prepend(str(LAMBDA.parent))
    spec = importlib.util.spec_from_file_location("sf_notifier_under_test", LAMBDA / "index.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m._sent, m._emitted = sent, emitted
    monkeypatch.setattr(m, "_describe_execution", lambda arn: {"input": "{}"})
    return m


class _SF:
    def __init__(self, by_name):
        self.by_name = by_name

    def list_executions(self, stateMachineArn, maxResults):
        name = stateMachineArn.rsplit(":", 1)[-1]
        if name == "boom":
            raise RuntimeError("x")
        return {"executions": self.by_name.get(name, [])}


def _event(name, status):
    return {"detail": {"stateMachineArn": ARN + name, "status": status,
                       "executionArn": f"arn:x:{name}:1", "name": f"{name}-2026-11-06"}}


@pytest.mark.parametrize("name,status", [
    ("ne-preopen-trading-pipeline", "RUNNING"),
    ("ne-preopen-trading-pipeline", "SUCCEEDED"),
    ("ne-postclose-trading-pipeline", "FAILED"),
    ("ne-postclose-reconcile-pipeline", "RUNNING"),
    ("alpha-engine-groom-dispatch", "FAILED"),
])
def test_only_the_day_closer_reaches_the_chat(mod, name, status):
    out = mod.handler(_event(name, status), None)
    assert out["telegram_sent"] is False and mod._sent == []


def test_a_mid_day_failure_is_recorded_not_sent(mod):
    mod.handler(_event("ne-postclose-trading-pipeline", "FAILED"), None)
    assert mod._sent == [] and len(mod._emitted) == 1
    assert mod._emitted[0]["delivery_tier"] == "tracked-only"


def test_the_closer_sends_one_message_naming_the_day(mod, monkeypatch):
    now = datetime.now(timezone.utc)
    sf = _SF({
        "ne-preopen-trading-pipeline": [{"name": "a", "status": "SUCCEEDED", "startDate": now}],
        "ne-postclose-trading-pipeline": [{"name": "b", "status": "FAILED", "startDate": now}],
        "ne-postclose-reconcile-pipeline": [{"name": "c", "status": "SUCCEEDED", "startDate": now}],
    })
    monkeypatch.setattr(mod.boto3, "client", lambda *a, **k: sf)
    mod.handler(_event("ne-postclose-reconcile-pipeline", "SUCCEEDED"), None)
    assert len(mod._sent) == 1
    text = mod._sent[0]["text"]
    assert "2 of 3 succeeded" in text and "Post-close Trading SF (failed)" in text
    assert text.endswith(mod._PIPELINES_URL)


def test_day_message_all_green_and_unreadable_never_reads_clean(mod):
    assert mod.day_message("2026-11-06", [("A", "SUCCEEDED"), ("B", "SUCCEEDED")]).startswith(
        "✅ Pipelines 2026-11-06: all 2 succeeded.")
    assert "0 of 1 succeeded" in mod.day_message("d", [("A", "UNREADABLE")])


def test_runs_outside_the_window_and_preflights_are_not_counted(mod):
    now = datetime.now(timezone.utc).timestamp()
    old = datetime.fromtimestamp(now - 30 * 3600, tz=timezone.utc)
    sf = _SF({"ne-preopen-trading-pipeline": [{"name": "x", "status": "FAILED", "startDate": old}],
              "ne-postclose-trading-pipeline": [{"name": "preflight-1", "status": "FAILED",
                                                 "startDate": datetime.now(timezone.utc)}]})
    assert mod._day_outcomes(ARN + "ne-postclose-reconcile-pipeline", sf_client=sf, now_s=now) == []
