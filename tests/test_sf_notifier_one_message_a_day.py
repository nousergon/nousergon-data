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
    m._describes = {}
    monkeypatch.setattr(m, "_describe_execution",
                        lambda arn: m._describes.get(arn, {"input": "{}"}))
    m._ddb_fake = _DDB()
    m._sf_fake = _SF({})
    monkeypatch.setattr(m.boto3, "client",
                        lambda name, *a, **k: m._ddb_fake if name == "dynamodb" else m._sf_fake)
    return m


class _ConditionalCheckFailed(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class _DDB:
    """The two DynamoDB calls the notifier makes, with real condition semantics
    for the one ConditionExpression it uses."""

    def __init__(self):
        self.items: dict[str, dict] = {}

    def put_item(self, TableName, Item, ConditionExpression=None, **kw):
        pk = Item["pk"]["S"]
        if ConditionExpression:
            assert ConditionExpression == "attribute_not_exists(pk) OR #st = :failed"
            prior = self.items.get(pk)
            if prior is not None and prior.get("state", {}).get("S") != "failed":
                raise _ConditionalCheckFailed()
        self.items[pk] = Item

    def get_item(self, TableName, Key, **kw):
        item = self.items.get(Key["pk"]["S"])
        return {"Item": item} if item else {}


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
    mod._sf_fake = sf
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


# --- alpha-engine-config-I13334 review findings (2026-11-10) -----------------

_PARTIAL = {"input": '{"skip_collect": true, "skip_reconcile": true}'}
_RECONCILE = "ne-postclose-reconcile-pipeline"


def _run(name, status, start, *, run=None):
    return {"name": run or f"{name}-run", "status": status, "startDate": start,
            "executionArn": f"arn:x:{name}:{run or 'run'}"}


@pytest.mark.parametrize("canonical,partial", [("FAILED", "SUCCEEDED"), ("SUCCEEDED", "FAILED")])
def test_a_later_partial_run_never_stands_in_for_the_cadence_run(mod, canonical, partial):
    """dc97b6c5e22be24e: a later director-verify-* run (skip_* flags, no
    pipeline_role) replaced the canonical run's outcome, in either direction."""
    now = datetime.now(timezone.utc).timestamp()
    early = datetime.fromtimestamp(now - 3600, tz=timezone.utc)
    late = datetime.fromtimestamp(now - 60, tz=timezone.utc)
    name = "ne-postclose-trading-pipeline"
    verify = _run(name, partial, late, run="director-verify-20261106T210000Z")
    mod._describes[verify["executionArn"]] = _PARTIAL
    sf = _SF({name: [verify, _run(name, canonical, early, run="eod-2026-11-06")]})
    assert mod._day_outcomes(ARN + _RECONCILE, sf_client=sf, now_s=now) == [
        ("Post-close Trading SF", canonical)]


def test_a_shell_run_is_not_counted_either(mod):
    now = datetime.now(timezone.utc).timestamp()
    name = "ne-preopen-trading-pipeline"
    shell = _run(name, "FAILED", datetime.fromtimestamp(now - 60, tz=timezone.utc), run="manual-1")
    mod._describes[shell["executionArn"]] = {"input": '{"shell_run": true}'}
    sf = _SF({name: [shell]})
    assert mod._day_outcomes(ARN + _RECONCILE, sf_client=sf, now_s=now) == []


def _reconcile_succeeded_missing_artifacts(mod, monkeypatch):
    monkeypatch.setattr(mod, "verify_eod_artifacts", lambda s3, run_date: types.SimpleNamespace(
        all_present=False, completion_marker_present=True, pnl_row_present=False,
        run_date=run_date))
    monkeypatch.setattr(mod, "format_eod_artifact_lines", lambda st: ["⚠️ *ARTIFACT(S) MISSING*"])
    monkeypatch.setattr(mod, "build_execution_digest", lambda **kw: ([], False, None))
    monkeypatch.setattr(mod, "terminal_resolution_line", lambda *a: None)


def test_a_reconcile_that_succeeded_without_its_artifacts_is_not_clean(mod, monkeypatch):
    """7f7bc35fc4f871f8: the per-run report flags missing EOD artifacts, but the
    day message read only the raw status and said all N succeeded."""
    _reconcile_succeeded_missing_artifacts(mod, monkeypatch)
    now = datetime.now(timezone.utc)
    mod._sf_fake = _SF({
        "ne-preopen-trading-pipeline": [_run("ne-preopen-trading-pipeline", "SUCCEEDED", now)],
        _RECONCILE: [{"name": f"{_RECONCILE}-2026-11-06", "status": "SUCCEEDED",
                      "startDate": now, "executionArn": f"arn:x:{_RECONCILE}:1"}],
    })
    mod.handler(_event(_RECONCILE, "SUCCEEDED"), None)
    assert len(mod._sent) == 1
    text = mod._sent[0]["text"]
    assert "all 2 succeeded" not in text
    assert "1 of 2 succeeded" in text
    assert "Post-close Reconcile SF (succeeded, eod artifacts missing)" in text
    assert mod._sent[0]["severity"] == "warning"


def test_an_earlier_hollow_success_is_not_clean_on_the_day(mod, monkeypatch):
    """Same finding, for a run whose concern was recorded by an earlier
    invocation (a hollow pre-open success): the closer reads the ledger."""
    monkeypatch.setattr(mod, "build_execution_digest", lambda **kw: ([], True, None))
    monkeypatch.setattr(mod, "terminal_resolution_line", lambda *a: None)
    mod.handler(_event("ne-preopen-trading-pipeline", "SUCCEEDED"), None)
    now = datetime.now(timezone.utc).timestamp()
    sf = _SF({"ne-preopen-trading-pipeline": [{
        "name": "x", "status": "SUCCEEDED", "startDate": now,
        "executionArn": "arn:x:ne-preopen-trading-pipeline:1"}]})
    assert mod._day_outcomes(ARN + _RECONCILE, sf_client=sf, now_s=now) == [
        ("Pre-open Trading SF", "SUCCEEDED, hollow-suspect")]


def _all_green(mod):
    now = datetime.now(timezone.utc)
    mod._sf_fake = _SF({_RECONCILE: [{"name": "c", "status": "SUCCEEDED", "startDate": now,
                                      "executionArn": f"arn:x:{_RECONCILE}:1"}]})
    return now


def test_a_closer_rerun_later_the_same_day_sends_nothing(mod, monkeypatch):
    """273e2edd886bfd5b: the per-day dedup_key only lived as long as
    flow-doctor's 1-minute cooldown; a re-run hours later sent a second message."""
    monkeypatch.setattr(mod, "build_execution_digest", lambda **kw: ([], False, None))
    monkeypatch.setattr(mod, "verify_eod_artifacts", lambda s3, run_date: types.SimpleNamespace(
        all_present=True, run_date=run_date))
    monkeypatch.setattr(mod, "format_eod_artifact_lines", lambda st: [])
    _all_green(mod)
    first = mod.handler(_event(_RECONCILE, "SUCCEEDED"), None)
    rerun = _event(_RECONCILE, "FAILED")
    rerun["detail"]["executionArn"] = f"arn:x:{_RECONCILE}:2"
    second = mod.handler(rerun, None)
    assert first["day_message_sent"] is True and second["day_message_sent"] is False
    assert len(mod._sent) == 1


def test_a_failed_send_can_be_retried_by_a_later_closer(mod, monkeypatch):
    monkeypatch.setattr(mod, "build_execution_digest", lambda **kw: ([], False, None))
    monkeypatch.setattr(mod, "verify_eod_artifacts", lambda s3, run_date: types.SimpleNamespace(
        all_present=True, run_date=run_date))
    monkeypatch.setattr(mod, "format_eod_artifact_lines", lambda st: [])
    _all_green(mod)
    attempts: list[str] = []
    monkeypatch.setattr(mod, "notify_via_flow_doctor",
                        lambda text, **kw: attempts.append(text) or len(attempts) > 1)
    assert mod.handler(_event(_RECONCILE, "SUCCEEDED"), None)["day_message_sent"] is False
    assert mod.handler(_event(_RECONCILE, "SUCCEEDED"), None)["day_message_sent"] is True
    assert len(attempts) == 2


def test_an_unreadable_claim_store_still_sends(mod, monkeypatch):
    def boom(**kw):
        raise RuntimeError("throttled")
    monkeypatch.setattr(mod._ddb_fake, "put_item", boom)
    assert mod._claim_day("k", now_s=0.0) is True
