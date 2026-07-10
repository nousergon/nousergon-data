"""Unit tests for sf-watch-dispatcher index.handler (Fleet-SF Watch).

Mocks flow-doctor notify (no live Telegram) and boto3 stepfunctions + s3 clients.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
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


def _make_clients(*, describe=None, history=None, existing=None, put=None, ssm_value=None):
    """Return (factory, sf_mock, s3_mock). ``existing`` None → get_object 404.

    ``ssm_value`` controls the per-pipeline autonomous-merge kill-switch:
    - None (default): ssm.get_parameter RAISES → handler FAILS CLOSED to the
      cadence default (saturday/groom True, weekday/eod False). This is the
      common path the dispatch tests exercise.
    - a string ("true"/"false"/…): ssm.get_parameter returns that Value.
    """
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

    ssm = MagicMock()
    if ssm_value is None:
        ssm.get_parameter.side_effect = FakeClientError("ParameterNotFound")
    else:
        ssm.get_parameter.return_value = {"Parameter": {"Value": ssm_value}}

    def factory(name, region_name=None):
        if name == "stepfunctions":
            return sf
        if name == "ssm":
            return ssm
        return s3

    return factory, sf, s3


@pytest.fixture(autouse=True)
def reset_notify(monkeypatch):
    mock = MagicMock(return_value=True)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock)
    yield mock


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


def test_observe_only_does_not_telegram():
    """AGENT_DISPATCH_ENABLED=false → watch-log written, no Fleet-SF Watch ping."""
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    index.notify_via_flow_doctor.assert_not_called()
    assert result["telegram_sent"] is False


def test_telegram_fires_when_dispatch_enabled(monkeypatch):
    """When the agent is actually dispatched, send the silent watch receipt."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)
    index.notify_via_flow_doctor.assert_called_once()
    text = index.notify_via_flow_doctor.call_args.args[0]
    kwargs = index.notify_via_flow_doctor.call_args.kwargs
    assert result["telegram_sent"] is True
    assert kwargs["silent"] is True  # notifier already buzzed loud
    assert "Fleet-SF Watch — AUTO-FIX" in text
    assert "Weekly Freshness SF: FAILED" in text  # pipeline-aware label
    assert "Failed state: `RAGIngestion`" in text
    assert "consolidated/saturday_sf_watch/2023-11-14.json" in text
    assert "autonomous fix ACTIVE" in text


def test_watch_log_path_is_code_fenced(monkeypatch):
    """config#1584: underscored S3 keys must survive Telegram legacy Markdown
    (which treats bare ``_`` as italic delimiters) — wrap in backticks."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        index.handler(_event("FAILED"), None)
    text = index.notify_via_flow_doctor.call_args.args[0]
    assert "`s3://alpha-engine-research/consolidated/saturday_sf_watch/2023-11-14.json`" in text
    assert "Failed state: `RAGIngestion`" in text
    assert "Cause: `States.TaskFailed: RAGIngestion failed`" in text


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


def test_telegram_failure_is_non_fatal(monkeypatch):
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    index.notify_via_flow_doctor.side_effect = RuntimeError("bot down")
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
    index.notify_via_flow_doctor.assert_not_called()


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
    index.notify_via_flow_doctor.assert_not_called()


def test_eod_sf_routes_to_eod_prefix():
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=EOD_ARN), None)
    assert result["watch_log_key"] == "consolidated/eod_sf_watch/2023-11-14.json"
    index.notify_via_flow_doctor.assert_not_called()


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
        "autonomous_merge": True,  # saturday default (config#1375)
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
    # SAFETY-CRITICAL: weekday PLACES paper orders → PROPOSE-ONLY by default.
    assert cp["autonomous_merge"] is False
    assert result["agent_dispatch"]["autonomous_merge"] is False


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


def test_groom_failure_dispatches_when_enabled(monkeypatch):
    """2026-07-01 (config#1535 follow-up): groom flipped to has_listener=True
    once `groom-sf-failure` was added to sf-watch.yml's `types:` allowlist and
    the charter gained a dedicated groom guardrail — it now dispatches exactly
    like the trading pipelines when the global flag is on."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    sent = {}

    def fake_urlopen(req, timeout=None):
        sent["data"] = json.loads(req.data)
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", fake_urlopen)
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=GROOM_ARN), None)
    assert result["agent_dispatch"] == {
        "dispatched": True, "status_code": 204, "event_type": "groom-sf-failure",
        "autonomous_merge": True,  # groom default (no live capital, config#1375)
    }
    assert result["action"] == "dispatched"
    assert sent["data"]["event_type"] == "groom-sf-failure"
    assert sent["data"]["client_payload"]["cadence_slug"] == "groom"
    assert sent["data"]["client_payload"]["autonomous_merge"] is True


def test_groom_telegram_claims_autonomous_fix_when_dispatched(monkeypatch):
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        index.handler(_event("FAILED", sm_arn=GROOM_ARN), None)
    text = index.notify_via_flow_doctor.call_args.args[0]
    assert "Fleet-SF Watch — AUTO-FIX" in text
    assert "Backlog Groom SF: FAILED" in text
    assert "autonomous fix ACTIVE" in text


def test_groom_watch_log_records_has_listener_true(monkeypatch):
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    factory, _, s3 = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        index.handler(_event("FAILED", sm_arn=GROOM_ARN), None)
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    event = written["events"][-1]
    assert event["has_listener"] is True
    assert event["action"] == "dispatch"
    assert event["agent_dispatch_enabled"] is True
    assert event["autonomous_merge"] is True  # groom default (config#1375)


def test_no_listener_pipeline_never_dispatches_even_when_globally_enabled(monkeypatch):
    """Regression guard for the has_listener MECHANISM itself (config#1535):
    a pipeline registered with has_listener=False must never fire a
    repository_dispatch nor claim "autonomous fix ACTIVE", regardless of the
    global kill-switch."""
    fake_pipelines = dict(index.PIPELINES)
    fake_pipelines["some-other-pipeline"] = {
        "cadence_slug": "other",
        "label": "Some Other Pipeline",
        "watch_prefix": "consolidated/other_sf_watch",
        "dispatch_event_type": "other-sf-failure",
        "has_listener": False,
    }
    monkeypatch.setattr(index, "PIPELINES", fake_pipelines)
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    fired = {"called": False}

    def fake_urlopen(req, timeout=None):
        fired["called"] = True
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", fake_urlopen)
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=UNREGISTERED_ARN), None)
    assert result["agent_dispatch"] == {"dispatched": False, "reason": "no_listener"}
    assert result["action"] == "observe"
    assert fired["called"] is False
    index.notify_via_flow_doctor.assert_not_called()


def test_saturday_still_dispatches_when_enabled(monkeypatch):
    """Non-regression: the has_listener plumbing must not change behavior for
    the 3 trading pipelines, which all default has_listener=True."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    factory, _, _ = _make_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=SATURDAY_ARN), None)
    assert result["agent_dispatch"]["dispatched"] is True
    assert result["action"] == "dispatched"


# ── config#1375: per-pipeline autonomous-merge KILL-SWITCH (SAFETY-CRITICAL) ──
#
# The global AGENT_DISPATCH_ENABLED gate decides *whether an agent is
# dispatched*; the per-cadence SSM boolean decides *whether that agent may
# auto-merge*. weekday PLACES paper orders + EOD RECONCILES NAV, so both MUST
# default to PROPOSE-ONLY (autonomous_merge=False) and never silently escalate.


def _dispatch_and_capture(monkeypatch, sm_arn, *, ssm_value=None):
    """Run the handler with dispatch enabled and return (result, sent_payload)."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    sent = {}

    def fake_urlopen(req, timeout=None):
        sent["data"] = json.loads(req.data)
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", fake_urlopen)
    factory, _, s3 = _make_clients(ssm_value=ssm_value)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=sm_arn), None)
    return result, sent, s3


@pytest.mark.parametrize(
    "sm_arn, expected",
    [
        (SATURDAY_ARN, True),   # already-ratified autonomous → preserved
        (WEEKDAY_ARN, False),   # PLACES paper orders → PROPOSE-ONLY
        (EOD_ARN, False),       # RECONCILES NAV → PROPOSE-ONLY
        (GROOM_ARN, True),      # no live capital → autonomous
    ],
)
def test_autonomous_merge_defaults_per_cadence_when_param_absent(monkeypatch, sm_arn, expected):
    """With the SSM param ABSENT the handler FAILS CLOSED to the cadence default:
    saturday/groom True, weekday/EOD False. This is the ship-state default."""
    result, sent, _ = _dispatch_and_capture(monkeypatch, sm_arn)  # ssm_value=None → raises
    assert sent["data"]["client_payload"]["autonomous_merge"] is expected
    assert result["agent_dispatch"]["autonomous_merge"] is expected
    assert result["autonomous_merge"] is expected


def test_weekday_kill_switch_can_be_flipped_on_via_ssm(monkeypatch):
    """After the soak, flipping the weekday param to true escalates to auto-merge
    (the flip is a deliberate, human-ratified operator action, not the default)."""
    result, sent, _ = _dispatch_and_capture(monkeypatch, WEEKDAY_ARN, ssm_value="true")
    assert sent["data"]["client_payload"]["autonomous_merge"] is True
    assert result["autonomous_merge"] is True


def test_saturday_kill_switch_off_via_ssm_downgrades_to_propose_only(monkeypatch):
    """An operator can also turn Saturday OFF (kill-switch), forcing PROPOSE-ONLY
    even for the ratified cadence — the SSM value wins over the default."""
    result, sent, _ = _dispatch_and_capture(monkeypatch, SATURDAY_ARN, ssm_value="false")
    assert sent["data"]["client_payload"]["autonomous_merge"] is False
    assert result["autonomous_merge"] is False


def test_unparseable_kill_switch_fails_closed_to_default(monkeypatch):
    """A garbled SSM value FAILS CLOSED to the cadence default (weekday → False):
    a trading pipeline must never auto-merge on an unreadable kill-switch."""
    result, sent, _ = _dispatch_and_capture(monkeypatch, WEEKDAY_ARN, ssm_value="maybe")
    assert sent["data"]["client_payload"]["autonomous_merge"] is False
    assert result["autonomous_merge"] is False


def test_weekday_watch_log_records_propose_only_by_default(monkeypatch):
    """The watch-log event carries the resolved autonomous_merge mode so the
    dashboard shows PROPOSE-ONLY vs AUTO-MERGE per cadence."""
    _, _, s3 = _dispatch_and_capture(monkeypatch, WEEKDAY_ARN)
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    ev = written["events"][-1]
    assert ev["autonomous_merge"] is False
    assert ev["action"] == "dispatch"


def test_weekday_telegram_footer_says_propose_only(monkeypatch):
    """A dispatched-but-PROPOSE-ONLY pipeline must NOT claim 'autonomous fix
    ACTIVE' — the receipt says PROPOSE-ONLY so the mode is visible in Telegram."""
    _dispatch_and_capture(monkeypatch, WEEKDAY_ARN)
    text = index.notify_via_flow_doctor.call_args.args[0]
    assert "Fleet-SF Watch — AUTO-FIX" in text  # an agent IS dispatched
    assert "PROPOSE-ONLY" in text
    assert "autonomous fix ACTIVE" not in text  # but it may not auto-merge


def test_observe_path_does_not_read_kill_switch(monkeypatch):
    """When AGENT_DISPATCH_ENABLED is off there is no dispatch, so the handler
    must NOT read the kill-switch SSM param (pure-observe path stays SSM-free)."""
    # Default AGENT_DISPATCH_ENABLED is False; ssm_value would raise if read.
    factory, _, _ = _make_clients(ssm_value="true")
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=WEEKDAY_ARN), None)
    assert result["action"] == "observe"
    assert result["autonomous_merge"] is False


def test_deploy_sh_creates_kill_switch_params_with_safe_defaults():
    """REGRESSION GUARD (config#1375): deploy.sh must create the four per-cadence
    kill-switch params, and weekday/EOD MUST default to false (PROPOSE-ONLY)."""
    text = (Path(__file__).parent / "deploy.sh").read_text()
    assert "autonomous_merge_enabled" in text
    assert "--no-overwrite" in text  # a re-bootstrap must never stomp a flip
    for pair in ("saturday=true", "weekday=false", "eod=false", "groom=true"):
        assert pair in text, f"deploy.sh kill-switch default missing/wrong: {pair}"


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


# ── config#1827: operator-abort dispatch carve-out ──────────────────────────


def test_operator_abort_suppresses_dispatch_but_still_records(monkeypatch):
    """A deliberate operator abort (status ABORTED + error == "OperatorAbort")
    must NOT auto-dispatch a recovery agent even when the flag + listener are on,
    yet MUST still write the watch-log (fail-loud preserved — only the autonomous
    ACTION on a human decision is withheld; no Fleet-SF Watch Telegram ping)."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    fired = {"called": False}

    def fake_urlopen(req, timeout=None):
        fired["called"] = True
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", fake_urlopen)
    describe = {
        "input": json.dumps({"pipeline_role": "verify-1807"}),
        "error": "OperatorAbort",
        "cause": "Verification mistakenly started during market hours; rescheduling post-close",
    }
    factory, _, s3 = _make_clients(describe=describe)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("ABORTED", sm_arn=WEEKDAY_ARN), None)

    # Dispatch suppressed — no repository_dispatch HTTP call fired.
    assert result["agent_dispatch"] == {"dispatched": False, "reason": "operator_abort"}
    assert result["action"] == "observe"
    assert fired["called"] is False
    # Watch-log STILL written (fail-loud) and carries the auditable reason.
    s3.put_object.assert_called_once()
    written = json.loads(s3.put_object.call_args.kwargs["Body"].decode())
    ev = written["events"][-1]
    assert ev["status"] == "ABORTED"
    assert ev["dispatch_suppressed"] == "operator_abort"
    assert ev["action"] == "observe"
    assert result["telegram_sent"] is False
    index.notify_via_flow_doctor.assert_not_called()


def test_genuine_failed_still_dispatches(monkeypatch):
    """Over-suppression guard: a real FAILED (not an operator abort) must still
    dispatch when the flag + listener are on."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    factory, _, s3 = _make_clients()  # default describe error = States.TaskFailed
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=WEEKDAY_ARN), None)
    assert result["agent_dispatch"]["dispatched"] is True
    assert result["action"] == "dispatched"
    written = json.loads(s3.put_object.call_args.kwargs["Body"].decode())
    assert written["events"][-1]["dispatch_suppressed"] is None


def test_programmatic_abort_still_dispatches(monkeypatch):
    """Over-suppression guard (the inverse fail-loud violation): an ABORTED whose
    error is NOT the operator marker (e.g. a programmatic/self-abort) is a real
    defect and must still dispatch — suppression is on the marker, not on bare
    ABORTED."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    describe = {
        "input": "{}",
        "error": "States.Runtime",
        "cause": "self-abort on invariant breach",
    }
    factory, _, s3 = _make_clients(describe=describe)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("ABORTED", sm_arn=WEEKDAY_ARN), None)
    assert result["agent_dispatch"]["dispatched"] is True
    assert result["action"] == "dispatched"
    written = json.loads(s3.put_object.call_args.kwargs["Body"].decode())
    assert written["events"][-1]["dispatch_suppressed"] is None


# ── preflight dispatch carve-out (found 2026-07-10, before ever firing live) ──
# The Friday-PM dry pass of ne-weekly-freshness-pipeline (shell_run=true in the
# execution input) is a deliberate rehearsal, not a production failure. Prior
# to this fix, is_preflight only suppressed the deterministic fast-path rerun
# (_maybe_fast_path) — the agent-dispatch path had no equivalent gate, so a
# FAILED Friday shell-run would fire a genuine saturday-sf-failure
# repository_dispatch, indistinguishable from a real Saturday production
# failure, and summon the full diagnose-fix-merge-rerun agent.


def test_preflight_suppresses_agent_dispatch(monkeypatch):
    """A Friday shell-run (preflight) failure must NOT auto-dispatch a recovery
    agent even when the flag + listener are on — mirrors the operator-abort
    carve-out (config#1827) exactly, but for a rehearsal run rather than a
    human stop. Watch-log still written (fail-loud preserved); no Telegram."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    fired = {"called": False}

    def fake_urlopen(req, timeout=None):
        fired["called"] = True
        return _FakeResp()

    monkeypatch.setattr(index.urllib.request, "urlopen", fake_urlopen)
    describe = {
        "input": json.dumps({"shell_run": True}),
        "error": "States.TaskFailed",
        "cause": "RAGIngestion failed",
    }
    factory, _, s3 = _make_clients(describe=describe)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED"), None)

    # Dispatch suppressed — no repository_dispatch HTTP call fired.
    assert result["agent_dispatch"] == {"dispatched": False, "reason": "preflight"}
    assert result["action"] == "observe"
    assert fired["called"] is False
    # Watch-log STILL written (fail-loud) and carries the auditable reason.
    s3.put_object.assert_called_once()
    written = json.loads(s3.put_object.call_args.kwargs["Body"].decode())
    ev = written["events"][-1]
    assert ev["is_preflight"] is True
    assert ev["dispatch_suppressed"] == "preflight"
    assert ev["action"] == "observe"
    assert result["telegram_sent"] is False
    index.notify_via_flow_doctor.assert_not_called()


def test_preflight_also_still_gated_from_fast_path(monkeypatch):
    """Over-suppression is impossible to get backwards here: a preflight FAILED
    must not fall through to the fast path either (fast path has its own
    is_preflight check, config#1900) — confirms both recovery layers agree a
    preflight failure gets NO automated action, only the watch-log record.
    Uses the weekday pipeline since it's the only one with a `fast_path` scope
    configured — the is_preflight gate itself is cadence-agnostic."""
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "FAST_PATH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "ghp_fake")
    monkeypatch.setattr(index.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp())
    describe = {
        "input": json.dumps({"shell_run": True, "trading_instance_id": ["i-0"]}),
        "error": "States.TaskFailed",
        "cause": "transient",
    }
    factory, _, s3 = _make_clients(describe=describe)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", sm_arn=WEEKDAY_ARN), None)
    assert result["fast_path"] == {"fast_path": False, "reason": "preflight"}
    assert result["agent_dispatch"] == {"dispatched": False, "reason": "preflight"}


# ── config#1900: deterministic zero-token fast path ──────────────────────────
# A weekday failure whose history EXACTLY matches a known-transient signature
# (data-spot host death / SendCommand invalid-instance) is recovered by a plain
# StartExecution from the dispatcher itself — no agent dispatch. Everything
# else falls through to the normal dispatch path with a recorded reason.

WEEKDAY_INPUT = json.dumps({"pipeline_role": "daily", "trading_instance_id": ["i-018eb3307a21329bf"]})


def _poll_output(payload: dict) -> str:
    return json.dumps({"ExecutedVersion": "$LATEST", "Payload": payload})


_HOST_DEATH_PAYLOAD = {
    "attempts": 67,
    "ping_misses": 0,
    "status": "Failed",
    "response_code": -1,
    "status_details": "Undeliverable",
    "ping_status": "NotRegistered",
    "step": "morning-arctic-append",
    "verdict": "COMMAND_FAILED",
}


def _weekday_history(payload=None, *, veto=False, task_failed_error=None, poll=True):
    """Chronological weekday history ending in HandleFailure→FailExecution."""
    h = [
        {"type": "ExecutionStarted"},
        {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "MorningEnrich"}},
        {"type": "TaskStateExited", "stateExitedEventDetails": {"name": "MorningEnrich"}},
    ]
    if veto:
        h += [
            {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "RunDaemon"}},
            {"type": "TaskStateExited", "stateExitedEventDetails": {"name": "RunDaemon"}},
        ]
    if task_failed_error is not None:
        h += [
            {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "MorningEnrich"}},
            {"type": "TaskFailed", "taskFailedEventDetails": {"error": task_failed_error}},
        ]
    if poll:
        h += [
            {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "WaitForMorningArcticAppend"}},
            {"type": "TaskSucceeded", "taskSucceededEventDetails": {
                "output": _poll_output(payload if payload is not None else _HOST_DEATH_PAYLOAD)}},
            {"type": "TaskStateExited", "stateExitedEventDetails": {"name": "WaitForMorningArcticAppend"}},
        ]
    h += [
        {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "HandleFailure"}},
        {"type": "TaskStateExited", "stateExitedEventDetails": {"name": "HandleFailure"}},
        {"type": "FailStateEntered", "stateEnteredEventDetails": {"name": "FailExecution"}},
        {"type": "ExecutionFailed"},
    ]
    return h


def _fast_path_clients(*, history=None, existing=None, describe_input=WEEKDAY_INPUT,
                       running=None, start_error=None):
    factory, sf, s3 = _make_clients(
        describe={"input": describe_input, "error": "DailyPipelineFailure",
                  "cause": "One or more weekday pipeline steps failed."},
        history=history if history is not None else _weekday_history(),
        existing=existing,
    )
    sf.list_executions.return_value = {"executions": running or []}
    if start_error is not None:
        sf.start_execution.side_effect = start_error
    else:
        sf.start_execution.return_value = {
            "executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-preopen-trading-pipeline:fast-path-rerun-x"
        }
    return factory, sf, s3


@pytest.fixture()
def fast_path_on(monkeypatch):
    monkeypatch.setattr(index, "FAST_PATH_ENABLED", True)


def _put_body(s3) -> dict:
    return json.loads(s3.put_object.call_args.kwargs["Body"])


def test_fast_path_reruns_on_host_death_signature(fast_path_on):
    factory, sf, s3 = _fast_path_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_called_once()
    kwargs = sf.start_execution.call_args.kwargs
    assert kwargs["stateMachineArn"] == WEEKDAY_ARN
    assert kwargs["input"] == WEEKDAY_INPUT  # plain rerun: ORIGINAL input, no skips
    assert kwargs["name"].startswith("fast-path-rerun-")
    assert result["fast_path"]["fast_path"] is True
    assert result["fast_path"]["signature"] == "data_spot_host_death"
    assert result["agent_dispatch"] == {"dispatched": False, "reason": "fast_path_rerun"}
    assert result["telegram_sent"] is True
    index.notify_via_flow_doctor.assert_called_once()
    text = index.notify_via_flow_doctor.call_args.args[0]
    assert "Fleet-SF Watch — AUTO-RERUN" in text
    ev = _put_body(s3)["events"][-1]
    assert ev["action"] == "fast_path_rerun"
    assert ev["lane"] == "A"
    assert ev["agent_attempt"] == 1  # consumes the SAME budget the charter counts
    assert ev["fast_path_signature"] == "data_spot_host_death"
    assert ev["rerun_execution_arn"].endswith("fast-path-rerun-x")


def test_fast_path_invalid_instance_signature(fast_path_on):
    history = _weekday_history(poll=False, task_failed_error="Ssm.InvalidInstanceIdException")
    factory, sf, s3 = _fast_path_clients(history=history)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    assert result["fast_path"]["fast_path"] is True
    assert result["fast_path"]["signature"] == "data_spot_invalid_instance"
    sf.start_execution.assert_called_once()


def test_fast_path_vetoed_when_order_state_ran(fast_path_on):
    factory, sf, s3 = _fast_path_clients(history=_weekday_history(veto=True))
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"] == {"fast_path": False, "reason": "order_emitting_state_ran"}


def test_fast_path_skipped_on_prior_attempt(fast_path_on):
    existing = {"schema_version": 1, "events": [{"agent_attempt": 1, "action": "escalated"}]}
    factory, sf, s3 = _fast_path_clients(existing=existing)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"]["reason"] == "prior_attempt_exists"


def test_fast_path_skipped_on_repeat_failure_day(fast_path_on):
    existing = {"schema_version": 1, "events": [{"action": "observe"}, {"action": "observe"}]}
    factory, sf, s3 = _fast_path_clients(existing=existing)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"]["reason"] == "repeat_failure_day"


def test_fast_path_signature_miss_falls_through(fast_path_on):
    benign = dict(_HOST_DEATH_PAYLOAD, status_details="CommandFailed",
                  response_code=1, ping_status="Online")
    factory, sf, s3 = _fast_path_clients(history=_weekday_history(payload=benign))
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"]["reason"] == "no_signature_match"


def test_fast_path_start_execution_error_falls_back_to_dispatch(fast_path_on, monkeypatch):
    monkeypatch.setattr(index, "AGENT_DISPATCH_ENABLED", True)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "fake-pat")
    factory, sf, s3 = _fast_path_clients(start_error=RuntimeError("boom"))

    class _FakeResp:
        status = 204
        def __enter__(self): return self
        def __exit__(self, *a): return False

    with patch("index.boto3.client", side_effect=factory), \
         patch("index.urllib.request.urlopen", return_value=_FakeResp()) as urlopen:
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    assert result["fast_path"]["reason"] == "start_execution_error"
    ev = _put_body(s3)["events"][-1]
    assert "RuntimeError" in ev["fast_path_error"]
    assert result["agent_dispatch"]["dispatched"] is True  # agent takes over
    urlopen.assert_called_once()


def test_fast_path_disabled_by_default():
    factory, sf, s3 = _fast_path_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"]["reason"] == "disabled"
    assert result["fast_path_enabled"] is False


def test_fast_path_blocked_by_running_execution(fast_path_on):
    factory, sf, s3 = _fast_path_clients(running=[{"name": "concurrent"}])
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"]["reason"] == "execution_already_running"


def test_fast_path_not_configured_for_saturday(fast_path_on):
    factory, sf, s3 = _fast_path_clients()
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", SATURDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"]["reason"] == "no_fast_path_config"


def test_fast_path_preflight_excluded(fast_path_on):
    factory, sf, s3 = _fast_path_clients(
        describe_input=json.dumps({"shell_run": True, "pipeline_role": "daily"})
    )
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(_event("FAILED", WEEKDAY_ARN), None)

    sf.start_execution.assert_not_called()
    assert result["fast_path"]["reason"] == "preflight"
