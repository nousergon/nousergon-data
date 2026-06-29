"""Unit tests for sf-watch-dispatcher index.handler (Fleet-SF Watch).

Stubs ``alpha_engine_lib.telegram.send_message`` (no live Telegram) and mocks
boto3 stepfunctions + s3 clients. Asserts: watch-log artifact written (append +
fresh-skeleton), failed-state extraction from history, fail-loud on the S3 write,
best-effort enrichment + Telegram, per-pipeline routing (saturday/weekday prefix
+ dispatch event type), the unregistered-SF guard, and the dispatch seam.
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Stub the lib before importing the handler (CI runners pre-pip-install).
_lib_pkg = types.ModuleType("alpha_engine_lib")
_telegram_mod = types.ModuleType("alpha_engine_lib.telegram")
_telegram_mod.send_message = MagicMock(return_value=True)
_lib_pkg.telegram = _telegram_mod
sys.modules.setdefault("alpha_engine_lib", _lib_pkg)
sys.modules.setdefault("alpha_engine_lib.telegram", _telegram_mod)

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402

SATURDAY_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
WEEKDAY_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline"
EOD_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-postclose-trading-pipeline"
UNREGISTERED_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:some-other-pipeline"


class FakeClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _event(status: str = "FAILED", sm_arn: str = SATURDAY_ARN, **overrides) -> dict:
    detail = {
        "status": status,
        "stateMachineArn": sm_arn,
        "executionArn": f"arn:aws:states:us-east-1:711398986525:execution:{sm_arn.rsplit(':', 1)[-1]}:exec-001",
        "name": "exec-001",
        "startDate": 1_700_000_000_000,  # 2023-11-14 UTC
        "stopDate": 1_700_000_060_000,
    }
    detail.update(overrides)
    return {"detail": detail}


def _history_chrono_to_resp(chrono: list[dict]) -> dict:
    # Handler calls get_execution_history(reverseOrder=True) then reverses to
    # chronological — so the API returns newest-first.
    return {"events": list(reversed(chrono))}


_DEFAULT_HISTORY = [
    {"type": "ExecutionStarted"},
    {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "MorningEnrich"}},
    {"type": "TaskStateExited", "stateExitedEventDetails": {"name": "MorningEnrich"}},
    {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "RAGIngestion"}},
    {"type": "TaskFailed"},
    {"type": "ExecutionFailed"},
]


def _make_clients(*, describe=None, history=None, existing=None, put=None):
    """Return (factory, sf_mock, s3_mock). ``existing`` None → get_object 404."""
    sf = MagicMock()
    sf.describe_execution.return_value = describe if describe is not None else {
        "input": "{}", "error": "States.TaskFailed", "cause": "RAGIngestion failed",
    }
    sf.get_execution_history.return_value = _history_chrono_to_resp(
        history if history is not None else _DEFAULT_HISTORY
    )
    s3 = MagicMock()
    if existing is None:
        s3.get_object.side_effect = FakeClientError("404")
    else:
        body = MagicMock()
        body.read.return_value = json.dumps(existing).encode()
        s3.get_object.return_value = {"Body": body}
    if put is not None:
        s3.put_object.side_effect = put

    def factory(name, region_name=None):
        return sf if name == "stepfunctions" else s3

    return factory, sf, s3


@pytest.fixture(autouse=True)
def reset_telegram():
    _telegram_mod.send_message.reset_mock()
    _telegram_mod.send_message.return_value = True
    yield


def test_failed_writes_watch_log_and_returns_state():
    factory, sf, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)

    assert result["status"] == "FAILED"
    assert result["failed_state"] == "RAGIngestion"
    assert result["action"] == "observe"
    assert result["agent_dispatch_enabled"] is False
    # run_date from startDate (no input run_date) → 2023-11-14
    assert result["run_date"] == "2023-11-14"
    assert result["watch_log_key"] == "consolidated/saturday_sf_watch/2023-11-14.json"

    s3.put_object.assert_called_once()
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert written["schema_version"] == index.SCHEMA_VERSION
    assert written["run_date"] == "2023-11-14"
    assert len(written["events"]) == 1
    ev = written["events"][0]
    assert ev["failed_state"] == "RAGIngestion"
    assert ev["cause"] == "States.TaskFailed: RAGIngestion failed"
    assert ev["action"] == "observe"
    assert ev["agent_dispatch_enabled"] is False


def test_telegram_is_silent_and_records_artifact_location():
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        index.handler(_event("FAILED"), None)
    _telegram_mod.send_message.assert_called_once()
    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert kwargs["disable_notification"] is True  # notifier already buzzed loud
    assert "Fleet-SF Watch — OBSERVE" in text
    assert "Weekly Freshness SF: FAILED" in text  # pipeline-aware label
    assert "Failed state: RAGIngestion" in text
    assert "consolidated/saturday_sf_watch/2023-11-14.json" in text
    assert "observe-only" in text


def test_run_date_prefers_input_run_date():
    factory, _, s3 = _make_clients(describe={
        "input": json.dumps({"run_date": "2026-06-20", "shell_run": False}),
        "error": "E", "cause": "C",
    })
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    assert result["run_date"] == "2026-06-20"
    assert s3.put_object.call_args.kwargs["Key"] == "consolidated/saturday_sf_watch/2026-06-20.json"


def test_existing_watch_log_is_appended_not_overwritten():
    existing = {
        "schema_version": 1, "run_date": "2023-11-14",
        "events": [{"status": "FAILED", "failed_state": "MorningEnrich", "action": "observe"}],
    }
    factory, _, s3 = _make_clients(existing=existing)
    with patch("index.boto3.client", side_effect=factory):
        index.handler(_event("FAILED"), None)
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert len(written["events"]) == 2  # prior + this one
    assert written["events"][0]["failed_state"] == "MorningEnrich"
    assert written["events"][1]["failed_state"] == "RAGIngestion"


def test_s3_put_failure_raises_fail_loud():
    factory, _, _ = _make_clients(put=RuntimeError("S3 down"))
    with patch("index.boto3.client", side_effect=factory):
        with pytest.raises(RuntimeError, match="S3 down"):
            index.handler(_event("FAILED"), None)


def test_history_api_error_still_writes_artifact_with_null_state():
    factory, sf, s3 = _make_clients()
    sf.get_execution_history.side_effect = RuntimeError("throttled")
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    assert result["failed_state"] is None  # enrichment degraded
    s3.put_object.assert_called_once()  # artifact still written (fail-loud only on S3)


def test_describe_error_still_writes_artifact():
    factory, sf, s3 = _make_clients()
    sf.describe_execution.side_effect = RuntimeError("throttled")
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    # cause unavailable, run_date falls back to startDate
    assert result["run_date"] == "2023-11-14"
    s3.put_object.assert_called_once()


def test_telegram_failure_is_non_fatal():
    _telegram_mod.send_message.side_effect = RuntimeError("bot down")
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    assert result["telegram_sent"] is False
    s3.put_object.assert_called_once()  # primary deliverable survived


def test_preflight_shell_run_marks_record():
    factory, _, s3 = _make_clients(describe={
        "input": json.dumps({"shell_run": True}), "error": "E", "cause": "C",
    })
    with patch("index.boto3.client", side_effect=factory):
        index.handler(_event("FAILED"), None)
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert written["events"][0]["is_preflight"] is True
    text = _telegram_mod.send_message.call_args.args[0]
    assert "Weekly Freshness Preflight SF" in text


def test_unregistered_sf_is_ignored():
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=UNREGISTERED_ARN), None)
    assert result["ignored"] is True
    s3.put_object.assert_not_called()


def test_weekday_sf_routes_to_weekday_prefix_and_label():
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=WEEKDAY_ARN), None)
    assert result["state_machine"] == "ne-preopen-trading-pipeline"
    assert result["watch_log_key"] == "consolidated/weekday_sf_watch/2023-11-14.json"
    assert s3.put_object.call_args.kwargs["Key"].startswith("consolidated/weekday_sf_watch/")
    text = _telegram_mod.send_message.call_args.args[0]
    assert "Pre-open Trading SF: FAILED" in text


def test_eod_sf_routes_to_eod_prefix():
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=EOD_ARN), None)
    assert result["watch_log_key"] == "consolidated/eod_sf_watch/2023-11-14.json"
    text = _telegram_mod.send_message.call_args.args[0]
    assert "Post-close Trading SF: FAILED" in text


@pytest.mark.parametrize("status", ["TIMED_OUT", "ABORTED"])
def test_other_terminal_statuses_recorded(status):
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event(status), None)
    assert result["status"] == status
    s3.put_object.assert_called_once()


def test_failed_state_none_when_state_exited_cleanly():
    # A history where the entered state also exited → no dangling culprit.
    clean = [
        {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "Foo"}},
        {"type": "TaskStateExited", "stateExitedEventDetails": {"name": "Foo"}},
        {"type": "ExecutionFailed"},
    ]
    factory, _, _ = _make_clients(history=clean)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    assert result["failed_state"] is None


# ── M2: repository_dispatch to the autonomous agent ─────────────────────────


class _FakeResp:
    status = 204

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_dispatch_disabled_by_default():
    """AGENT_DISPATCH_ENABLED defaults false → no dispatch, action stays observe."""
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    assert result["agent_dispatch"] == {"dispatched": False, "reason": "disabled"}
    assert result["action"] == "observe"


def test_dispatch_enabled_fires_repository_dispatch(monkeypatch):
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    sent = {}

    def fake_urlopen(req, timeout=None):
        sent["url"] = req.full_url
        sent["data"] = json.loads(req.data)
        sent["auth"] = req.headers.get("Authorization")
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", fake_urlopen)
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)

    assert result["agent_dispatch"] == {
        "dispatched": True, "status_code": 204, "event_type": "saturday-sf-failure",
    }
    assert result["action"] == "dispatched"
    assert sent["url"].endswith("/repos/nousergon/alpha-engine-config/dispatches")
    assert sent["data"]["event_type"] == "saturday-sf-failure"
    cp = sent["data"]["client_payload"]
    assert cp["pipeline_name"] == "ne-weekly-freshness-pipeline"
    assert cp["state_machine_arn"] == SATURDAY_ARN
    assert cp["failed_state"] == "RAGIngestion"
    assert cp["run_date"] == "2023-11-14"
    assert cp["watch_log_key"] == "consolidated/saturday_sf_watch/2023-11-14.json"
    # Watch-log is written BEFORE dispatch (agent reads fresh context).
    s3.put_object.assert_called_once()


def test_dispatch_routes_weekday_event_type(monkeypatch):
    """A weekday failure dispatches the weekday-sf-failure event type + payload."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    sent = {}

    def fake_urlopen(req, timeout=None):
        sent["data"] = json.loads(req.data)
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", fake_urlopen)
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=WEEKDAY_ARN), None)

    assert result["agent_dispatch"]["event_type"] == "weekday-sf-failure"
    assert sent["data"]["event_type"] == "weekday-sf-failure"
    cp = sent["data"]["client_payload"]
    assert cp["pipeline_name"] == "ne-preopen-trading-pipeline"
    assert cp["state_machine_arn"] == WEEKDAY_ARN


def test_dispatch_error_recorded_not_raised(monkeypatch):
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)

    def boom():
        raise RuntimeError("ssm denied")

    monkeypatch.setattr(index, "_get_github_pat", boom)
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    assert result["agent_dispatch"]["dispatched"] is False
    assert "ssm denied" in result["agent_dispatch"]["error"]
    s3.put_object.assert_called_once()  # primary deliverable survived


def test_pat_never_appears_in_result(monkeypatch):
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_SECRET_TOKEN")
    monkeypatch.setattr(
        index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp()
    )
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    assert "ghp_SECRET_TOKEN" not in json.dumps(result)


def _deploy_sh_rule_arns() -> set[str]:
    """Parse the SF names out of deploy.sh's EventBridge EVENT_PATTERN
    stateMachineArn list — the literal ARNs the live rule will match."""
    import re

    text = (Path(__file__).parent / "deploy.sh").read_text()
    # The pattern lives in a heredoc; pull every stateMachine:<name> token that
    # appears inside the EVENT_PATTERN block's stateMachineArn array.
    start = text.index('"stateMachineArn"')
    end = text.index("]", start)
    block = text[start:end]
    return set(re.findall(r"stateMachine:([A-Za-z0-9-]+)", block))


def test_registry_and_eventbridge_rule_are_in_lockstep():
    """REGRESSION GUARD (config#1408, 2026-06-29 dead-watch): every SF name in
    index.PIPELINES MUST appear in deploy.sh's EventBridge rule pattern. The
    handler ignores any SF not in the rule's ARN list never even reaches it;
    any SF not in PIPELINES is ignored by the handler. The two must not drift —
    that drift is exactly what silently disabled the watcher (code generalized
    to 3 pipelines, the rule left at a single deleted ARN)."""
    rule_arns = _deploy_sh_rule_arns()
    registry = set(index.PIPELINES)
    missing_from_rule = registry - rule_arns
    assert not missing_from_rule, (
        f"PIPELINES entries not covered by the EventBridge rule in deploy.sh: "
        f"{sorted(missing_from_rule)} — their failures would never invoke the "
        f"dispatcher. Add them to the EVENT_PATTERN stateMachineArn list."
    )


def test_live_old_named_eod_is_registered_during_rename_transition():
    """Until the SF-rename cutover (config#1408 / re-exam 2026-07-03) the EOD SF
    still runs as `alpha-engine-eod-pipeline`. Drop this assertion when the old
    ARN is removed from the registry + rule at cutover."""
    assert "alpha-engine-eod-pipeline" in index.PIPELINES
    assert index.PIPELINES["alpha-engine-eod-pipeline"]["cadence_slug"] == "eod"
