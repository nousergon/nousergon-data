"""Unit tests for sf-telegram-notifier index.handler.

Mocks nousergon_lib.telegram.send_message so tests do not hit the live
Telegram API. Each test asserts the exact (text, disable_notification) tuple
the handler hands to the primitive, plus the return value shape.
"""

from __future__ import annotations

import io
import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Stub `nousergon_lib.telegram` + `nousergon_lib.flow_doctor_fleet` before
# importing the handler so test environments without the lib installed (CI
# runners pre-pip-install) still pass — the handler depends on both import
# paths from the lib (config#1759: the flow_doctor_fleet stub was missing
# here, which this hermetic pattern requires when a real `nousergon_lib` is
# also on the path — `setdefault` only protects against a missing package,
# not a package that lacks this submodule attribute; matches the sibling
# pipeline-watchdog/test_handler.py stub).
_lib_pkg = types.ModuleType("nousergon_lib")
_telegram_mod = types.ModuleType("nousergon_lib.telegram")
_telegram_mod.send_message = MagicMock(return_value=True)
_lib_pkg.telegram = _telegram_mod
_fleet_mod = types.ModuleType("nousergon_lib.flow_doctor_fleet")


class _FleetTelegramTopic:
    CRITICAL = "CRITICAL"
    PIPELINE = "PIPELINE"
    OPS_HEALTH = "OPS_HEALTH"


_fleet_mod.FleetTelegramTopic = _FleetTelegramTopic
_fleet_mod.PIPELINE_OBSERVER_TELEGRAM_TOPICS = (
    _FleetTelegramTopic.CRITICAL,
    _FleetTelegramTopic.PIPELINE,
    _FleetTelegramTopic.OPS_HEALTH,
)
_lib_pkg.flow_doctor_fleet = _fleet_mod
sys.modules["nousergon_lib"] = _lib_pkg
sys.modules["nousergon_lib.telegram"] = _telegram_mod
sys.modules["nousergon_lib.flow_doctor_fleet"] = _fleet_mod

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import index  # noqa: E402
import flow_doctor_telegram  # noqa: E402
from flow_doctor_telegram import reset_flow_doctor_cache  # noqa: E402


SATURDAY_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
WEEKDAY_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline"
EOD_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-postclose-trading-pipeline"


def _event(status: str, sm_arn: str = SATURDAY_ARN, **detail_overrides) -> dict:
    detail = {
        "status": status,
        "stateMachineArn": sm_arn,
        "executionArn": f"arn:aws:states:us-east-1:711398986525:execution:{sm_arn.rsplit(':', 1)[-1]}:exec-001",
        "name": "exec-001",
        "startDate": 1_700_000_000_000,
        "stopDate": 1_700_000_060_000,  # 60s after start
    }
    detail.update(detail_overrides)
    return {"detail": detail}


def _fake_sf_client(describe_return=None):
    client = MagicMock()
    client.describe_execution.return_value = describe_return or {
        "input": "{}",
        "error": "",
        "cause": "",
    }
    client.get_execution_history.return_value = {"events": []}
    return client


class _NotFoundS3Error(Exception):
    def __init__(self):
        self.response = {
            "Error": {"Code": "404"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }


def _eod_client_stubs(
    monkeypatch,
    *,
    describe_return=None,
    marker_present=True,
    csv_body=b"date,nav\n",
):
    """Wire index.boto3.client to a fresh (sf, s3) mock pair, overriding the
    autouse reset_send_message fixture's own client() closure for this test
    only (nousergon-data-i5289: the autouse fixture's `sf` yield has no `s3`
    handle, so EOD-artifact tests need their own client factory rather than
    widening that fixture's signature and touching every existing caller)."""
    sf = _fake_sf_client(describe_return)
    s3 = MagicMock()
    if marker_present:
        s3.head_object.return_value = {}
    else:
        s3.head_object.side_effect = _NotFoundS3Error()
    s3.get_object.return_value = {"Body": io.BytesIO(csv_body)}

    def client(name, region_name=None):
        if name == "stepfunctions":
            return sf
        if name == "s3":
            return s3
        return MagicMock()

    monkeypatch.setattr(index.boto3, "client", client)
    return sf, s3


@pytest.fixture(autouse=True)
def reset_send_message(monkeypatch):
    monkeypatch.setenv("FLOW_DOCTOR_ENABLED", "0")
    _telegram_mod.send_message.reset_mock()
    _telegram_mod.send_message.return_value = True
    monkeypatch.setattr(flow_doctor_telegram, "send_message", _telegram_mod.send_message)
    reset_flow_doctor_cache()

    sf = _fake_sf_client()
    s3 = MagicMock()

    def client(name, region_name=None):
        if name == "stepfunctions":
            return sf
        if name == "s3":
            return s3
        return MagicMock()

    monkeypatch.setattr(index.boto3, "client", client)
    yield sf


def test_running_sends_silent_message_without_duration_or_cause():
    event = _event("RUNNING", stopDate=None)
    result = index.handler(event, None)

    _telegram_mod.send_message.assert_called_once()
    text, kwargs = _telegram_mod.send_message.call_args.args[0], _telegram_mod.send_message.call_args.kwargs
    assert "Weekly Freshness SF — RUNNING" in text
    assert "Execution: exec-001" in text
    assert "Duration:" not in text
    assert "Cause:" not in text
    assert kwargs["disable_notification"] is True
    assert result["status"] == "RUNNING"
    assert result["silent"] is True
    assert result["telegram_sent"] is True


def test_succeeded_sends_loud_message_with_duration(reset_send_message):
    event = _event("SUCCEEDED", sm_arn=WEEKDAY_ARN)
    result = index.handler(event, None)

    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "Pre-open Trading SF — SUCCEEDED" in text
    assert "Duration: 1m" in text
    assert "*States:*" in text
    assert kwargs["disable_notification"] is False
    assert result["silent"] is False


def test_succeeded_long_duration_formats_hours_and_minutes():
    # 4h 12m → start 0, stop = (4*3600 + 12*60) * 1000
    event = _event("SUCCEEDED", startDate=0, stopDate=(4 * 3600 + 12 * 60) * 1000)
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    assert "Duration: 4h 12m" in text


def test_failed_fetches_and_includes_cause(reset_send_message):
    event = _event("FAILED", sm_arn=EOD_ARN)
    reset_send_message.describe_execution.return_value = {
        "error": "States.TaskFailed",
        "cause": "EODReconcile state failed: NoCredentialsError",
    }
    result = index.handler(event, None)

    reset_send_message.describe_execution.assert_called()
    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "Post-close Trading SF — FAILED" in text
    assert "Cause: States.TaskFailed: EODReconcile state failed: NoCredentialsError" in text
    assert kwargs["disable_notification"] is False
    assert result["status"] == "FAILED"


def _real_preopen_failure_events() -> list[dict]:
    """The real, untouched GetExecutionHistory for
    e888a3a4-05a7-4c42-b2a1-904f44e24bd5 (ne-preopen-trading-pipeline, FAILED
    2026-09-01) — alpha-engine-config-I9742. Timestamps stay as the ISO
    strings botocore already parses to datetime; get_execution_history is
    mocked here so the digest sees exactly what boto3 would hand it."""
    from datetime import datetime

    fixture = (
        Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "sf_history_preopen_2026-09-01_e888a3a4.json"
    )
    events = json.loads(fixture.read_text())["events"]
    for event in events:
        event["timestamp"] = datetime.fromisoformat(event["timestamp"])
    return events


def test_failed_preopen_names_the_state_and_the_real_error_end_to_end(reset_send_message):
    """Closes-when: the real 2026-09-01 preopen failure, replayed through the
    full handler. The alert must name WaitForCodeFreshness (deliverables 1
    and 4 — not "no workload states in history", and not HandleFailure, the
    state that merely sent the alert) and must carry the git-push 403
    (deliverable 5 / sf-pipeline-policy §2.3 corollary), not the boilerplate
    "One or more weekday pipeline steps failed."."""
    reset_send_message.get_execution_history.return_value = {
        "events": _real_preopen_failure_events()
    }
    reset_send_message.describe_execution.return_value = {
        "input": '{"run_date": "2026-09-01", "pipeline_role": "daily"}',
        "error": "DailyPipelineFailure",
        "cause": "One or more weekday pipeline steps failed.",
    }
    event = _event(
        "FAILED",
        sm_arn=WEEKDAY_ARN,
        startDate=1_756_724_141_102,
        stopDate=1_756_724_223_779,
    )
    result = index.handler(event, None)

    text = _telegram_mod.send_message.call_args.args[0]
    assert "Pre-open Trading SF — FAILED" in text
    assert "no workload states in history" not in text
    assert "WaitForCodeFreshness" in text
    # HandleFailure legitimately appears as an ordinary completed row (it did
    # run) — what deliverable 4 forbids is HandleFailure being the ENTERED-
    # BUT-NEVER-COMPLETED fallback line, which never fires here because
    # WaitForCodeFreshness already has a row.
    assert "entered, never completed" not in text
    assert "Cause:" in text
    cause_line = [line for line in text.splitlines() if line.startswith("Cause:")][0]
    assert "403" in cause_line
    assert "Write access to repository not granted" in cause_line
    assert "One or more weekday pipeline steps failed." not in cause_line
    assert result["status"] == "FAILED"


def test_failed_with_describe_execution_error_still_sends(reset_send_message):
    """DescribeExecution failures must not block the Telegram send."""
    event = _event("FAILED")
    reset_send_message.describe_execution.side_effect = RuntimeError("API throttled")
    result = index.handler(event, None)

    text = _telegram_mod.send_message.call_args.args[0]
    assert "Weekly Freshness SF — FAILED" in text
    assert "Cause:" not in text  # enrichment silently dropped
    assert result["telegram_sent"] is True


def test_failed_truncates_long_cause(reset_send_message):
    event = _event("FAILED")
    reset_send_message.describe_execution.return_value = {
        "error": "E",
        "cause": "x" * 500,
    }
    index.handler(event, None)

    text = _telegram_mod.send_message.call_args.args[0]
    cause_line = [line for line in text.splitlines() if line.startswith("Cause:")][0]
    # "Cause: " prefix + cap (_CAUSE_MAX_CHARS) + ellipsis (1) = bounded
    assert len(cause_line) <= len("Cause: ") + index._CAUSE_MAX_CHARS


def test_timed_out_sends_loud_message():
    event = _event("TIMED_OUT")
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "Weekly Freshness SF — TIMED_OUT" in text
    assert kwargs["disable_notification"] is False


def test_aborted_sends_loud_message():
    event = _event("ABORTED")
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "Weekly Freshness SF — ABORTED" in text
    assert kwargs["disable_notification"] is False


def test_unknown_sf_arn_falls_back_to_arn_tail():
    unknown_arn = "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-future-pipeline"
    event = _event("SUCCEEDED", sm_arn=unknown_arn)
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    assert "alpha-engine-future-pipeline — SUCCEEDED" in text


def test_send_message_failure_returned_in_result():
    _telegram_mod.send_message.return_value = False
    result = index.handler(_event("SUCCEEDED"), None)
    assert result["telegram_sent"] is False
    assert result["status"] == "SUCCEEDED"


def test_label_lookup_table_covers_all_three_sfs():
    assert index._SF_LABELS["ne-weekly-freshness-pipeline"] == "Weekly Freshness SF"
    assert index._SF_LABELS["ne-preopen-trading-pipeline"] == "Pre-open Trading SF"
    assert index._SF_LABELS["ne-postclose-trading-pipeline"] == "Post-close Trading SF"


class TestPreflightLabel:
    """2026-05-23 rename: the Weekly Freshness SF's Friday-PM dry-pass execution
    (input ``shell_run=true``) surfaces 'Weekly Freshness Preflight SF' in the
    Telegram message instead of 'Weekly Freshness SF', so the operator can tell
    a green/red preflight result apart from a real Saturday result at a
    glance. Same state machine; differentiated via execution input flag.
    """

    def _saturday_preflight_event(self, status: str):
        return _event(status, sm_arn=SATURDAY_ARN, name="friday-shell-260523")

    def test_saturday_with_shell_run_true_surfaces_preflight_label(self, reset_send_message):
        event = self._saturday_preflight_event("SUCCEEDED")
        reset_send_message.describe_execution.return_value = {
            "input": '{"shell_run": true, "ec2_instance_id": ["i-X"]}',
            "error": "",
            "cause": "",
        }
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Weekly Freshness Preflight SF — SUCCEEDED" in text, (
            f"shell_run=true on Weekly Freshness SF must surface "
            f"'Weekly Freshness Preflight SF' label; got: {text!r}"
        )
        # Default label must NOT appear (Weekly Freshness SF != Weekly Freshness Preflight SF)
        assert "Weekly Freshness SF —" not in text

    def test_saturday_without_shell_run_uses_default_label(self, reset_send_message):
        event = _event("SUCCEEDED", sm_arn=SATURDAY_ARN)
        reset_send_message.describe_execution.return_value = {
            "input": '{"ec2_instance_id": ["i-X"]}',
            "error": "",
            "cause": "",
        }
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Weekly Freshness SF — SUCCEEDED" in text
        assert "Preflight" not in text

    def test_saturday_with_shell_run_false_uses_default_label(self, reset_send_message):
        event = _event("SUCCEEDED", sm_arn=SATURDAY_ARN)
        reset_send_message.describe_execution.return_value = {
            "input": '{"shell_run": false, "ec2_instance_id": ["i-X"]}',
            "error": "",
            "cause": "",
        }
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Weekly Freshness SF — SUCCEEDED" in text
        assert "Preflight" not in text

    def test_non_saturday_sf_with_shell_run_true_keeps_default_label(self, reset_send_message):
        event = _event("SUCCEEDED", sm_arn=WEEKDAY_ARN)
        reset_send_message.describe_execution.return_value = {
            "input": '{"shell_run": true}',
            "error": "",
            "cause": "",
        }
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Pre-open Trading SF — SUCCEEDED" in text
        assert "Preflight" not in text

    def test_describe_execution_error_falls_back_to_default_label(self, reset_send_message):
        event = _event("SUCCEEDED", sm_arn=SATURDAY_ARN)
        reset_send_message.describe_execution.side_effect = RuntimeError("API throttled")
        result = index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Weekly Freshness SF — SUCCEEDED" in text
        assert result["telegram_sent"] is True

    def test_malformed_input_json_falls_back_to_default_label(self, reset_send_message):
        event = _event("FAILED", sm_arn=SATURDAY_ARN)
        reset_send_message.describe_execution.return_value = {
            "input": "{not valid json",
            "error": "E",
            "cause": "C",
        }
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Weekly Freshness SF — FAILED" in text
        # The cause enrichment STILL works — parsing input is independent
        # of error/cause extraction.
        assert "Cause: E: C" in text

    def test_failed_preflight_includes_both_label_and_cause(self, reset_send_message):
        event = self._saturday_preflight_event("FAILED")
        reset_send_message.describe_execution.return_value = {
            "input": '{"shell_run": true}',
            "error": "States.TaskFailed",
            "cause": "MorningEnrich state failed",
        }
        index.handler(event, None)
        reset_send_message.describe_execution.assert_called()
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Weekly Freshness Preflight SF — FAILED" in text
        assert "Cause: States.TaskFailed: MorningEnrich state failed" in text

    def test_preflight_label_override_map_pins_saturday_only(self):
        """The override map is intentionally Saturday-only — the
        weekday + Post-close Trading SFs don't have a preflight variant."""
        assert index._PREFLIGHT_LABEL_OVERRIDE == {
            "ne-weekly-freshness-pipeline": "Weekly Freshness Preflight SF",
        }


def test_format_duration_handles_missing_timestamps():
    assert index._format_duration(None, None) == ""
    assert index._format_duration(1000, None) == ""
    assert index._format_duration(None, 2000) == ""
    assert index._format_duration(0, 1000) == "0m"  # sub-minute rounds down


def test_succeeded_hollow_predictor_training_flags_loud(reset_send_message):
    """config#1672: implausibly fast PredictorTraining → HOLLOW-SUSPECT + loud push.

    alpha-engine-config-I10574: floored/rendered on "WaitForPredictorTraining"
    (the poll state) — "PredictorTraining" is the dispatch state, never
    floored, so a fast dispatch alone would not be an anomaly.
    """
    from datetime import datetime, timezone

    base = datetime(2026, 7, 3, 12, 0, 0, tzinfo=timezone.utc)
    reset_send_message.get_execution_history.return_value = {
        "events": [
            {
                "type": "TaskStateEntered",
                "timestamp": base,
                "stateEnteredEventDetails": {"name": "WaitForPredictorTraining"},
            },
            {
                "type": "TaskStateExited",
                "timestamp": base.replace(minute=2),
                "stateExitedEventDetails": {"name": "WaitForPredictorTraining"},
            },
        ],
    }
    result = index.handler(_event("SUCCEEDED"), None)
    text = _telegram_mod.send_message.call_args.args[0]
    assert "HOLLOW-SUSPECT" in text
    assert "WaitForPredictorTraining" in text
    assert "⚠️" in text
    assert result["hollow_suspect"] is True
    assert result["silent"] is False


class TestPartialRunsDoNotPageAsCadenceFailures:
    """A narrowed run is labelled as one and does not buzz.

    `director-verify-20260804T003005Z` on ne-weekly-freshness-pipeline carried
    24 `skip_*: true` flags and no `pipeline_role`. It ran one stage, failed in
    0m, and paged as "🔴 Weekly Freshness SF — FAILED / Duration: 0m / States:
    (no workload states in history)" — a message carrying, in its own body, the
    evidence that it was not a weekly run. overseer-policy invariant 17:
    severity is a property of the invariant breached, not of the check that
    emitted it.
    """

    _WEEKLY_ARN = (
        "arn:aws:states:us-east-1:711398986525:stateMachine:"
        "ne-weekly-freshness-pipeline"
    )

    def _detail(self, name="director-verify-20260804T003005Z"):
        return {
            "stateMachineArn": self._WEEKLY_ARN,
            "executionArn": f"{self._WEEKLY_ARN}:{name}".replace(
                ":stateMachine:", ":execution:"
            ),
            "name": name,
            "status": "FAILED",
            "startDate": 0,
            "stopDate": 0,
        }

    def test_skip_flagged_run_is_labelled_partial_and_silent(self):
        describe = {"input": json.dumps({
            "run_date": "2026-08-03",
            "skip_scanner": True,
            "skip_evaluator": True,
            "skip_post_eval": False,
        })}
        text, silent, _hollow, is_partial = index._build_message(
            self._detail(), describe
        )
        assert is_partial is True
        assert silent is True, "a narrowed run must not buzz"
        assert "partial run — 2 stage(s) skipped" in text, text
        assert "Weekly Freshness SF (partial run" in text, text

    def test_canonical_cadence_role_wins_over_skip_flags(self):
        """A real weekly rerun skipping completed stages is still the weekly run."""
        describe = {"input": json.dumps({
            "pipeline_role": "weekly",
            "run_date": "2026-08-03",
            "skip_data_phase1": True,
        })}
        text, silent, _hollow, is_partial = index._build_message(
            self._detail("weekly-2026-08-03"), describe
        )
        assert is_partial is False
        assert silent is False, "a cadence run failing must still page"
        assert "partial run" not in text, text

    def test_full_run_with_no_skips_is_not_partial(self):
        describe = {"input": json.dumps({"run_date": "2026-08-03"})}
        _text, silent, _hollow, is_partial = index._build_message(
            self._detail("abc-123"), describe
        )
        assert is_partial is False
        assert silent is False

    def test_unparseable_input_is_not_treated_as_partial(self):
        """Absence of evidence is never evidence of a narrowed run."""
        _text, silent, _hollow, is_partial = index._build_message(
            self._detail("abc-123"), {"input": "not json"}
        )
        assert is_partial is False
        assert silent is False, "an unreadable input must fail toward paging"


class TestDegradedRunMapping:
    """alpha-engine-config#5289 scope item 4: the eod SF's deliberate
    `Error: "DegradedRun"` Fail terminal must render as DEGRADED — distinct
    from both a clean SUCCEEDED and a genuine crash FAILED."""

    def test_degraded_renders_distinct_label_and_emoji(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={
                "input": '{"run_date": "2026-08-08"}',
                "error": "DegradedRun",
                "cause": "EOD pipeline skipped EODReconcile (data gap).",
            },
            marker_present=True,
            csv_body=b"date,nav\n2026-08-08,100000\n",
        )
        event = _event("FAILED", sm_arn=EOD_ARN)
        result = index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        kwargs = _telegram_mod.send_message.call_args.kwargs

        assert "Post-close Trading SF — DEGRADED" in text
        assert "Post-close Trading SF — FAILED" not in text
        assert "\U0001f7e0" in text  # 🟠
        assert "Cause: DegradedRun: EOD pipeline skipped EODReconcile" in text
        assert kwargs["disable_notification"] is False
        # The RAW AWS status is unchanged in the return contract — only the
        # rendered text distinguishes DEGRADED.
        assert result["status"] == "FAILED"

    def test_genuine_crash_failed_is_unaffected(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={
                "input": '{"run_date": "2026-08-08"}',
                "error": "States.TaskFailed",
                "cause": "CaptureSnapshot state failed",
            },
        )
        event = _event("FAILED", sm_arn=EOD_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Post-close Trading SF — FAILED" in text
        assert "DEGRADED" not in text
        assert "🟠" not in text

    def test_degraded_with_missing_pnl_row_renders_loud_artifact_block(self, monkeypatch):
        """The realistic combo: DegradedRun means EODReconcile (which writes
        eod_pnl.csv) was skipped — the artifact check must say so."""
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={
                "input": '{"run_date": "2026-08-08"}',
                "error": "DegradedRun",
                "cause": "EOD pipeline skipped EODReconcile (data gap).",
            },
            marker_present=True,
            csv_body=b"date,nav\n2026-08-07,100000\n",  # no 2026-08-08 row
        )
        event = _event("FAILED", sm_arn=EOD_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Post-close Trading SF — DEGRADED" in text
        assert "⚠️ *ARTIFACT(S) MISSING*" in text
        assert "eod_pnl.csv row for 2026-08-08" in text
        assert "_sf_completion marker" not in text  # marker WAS written


class TestRunDateRendering:
    """alpha-engine-config#5289 scope item 4: run_date, from execution input
    or (fallback) execution name."""

    def test_run_date_rendered_from_execution_input(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": '{"run_date": "2026-08-08"}', "error": "", "cause": ""},
            csv_body=b"date,nav\n2026-08-08,100000\n",
        )
        event = _event("SUCCEEDED", sm_arn=EOD_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Run date: 2026-08-08" in text

    def test_run_date_falls_back_to_execution_name(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": "{}", "error": "", "cause": ""},
            csv_body=b"date,nav\n2026-08-08,100000\n",
        )
        event = _event(
            "SUCCEEDED", sm_arn=EOD_ARN, name="eod-2026-08-08-1754678901"
        )
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Run date: 2026-08-08" in text

    def test_run_date_absent_when_unresolvable(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": "{}", "error": "", "cause": ""},
        )
        event = _event("SUCCEEDED", sm_arn=WEEKDAY_ARN)  # exec-001, no date
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Run date:" not in text


class TestEodArtifactVerification:
    """alpha-engine-config#5289 scope item 4: postclose SUCCEEDED/DEGRADED
    terminals get the day's artifacts verified against S3. Only the EOD
    pipeline triggers this — preopen/weekly are unaffected."""

    def test_succeeded_with_both_artifacts_present_stays_one_extra_line(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": '{"run_date": "2026-08-08"}', "error": "", "cause": ""},
            marker_present=True,
            csv_body=b"date,nav\n2026-08-08,100000\n",
        )
        event = _event("SUCCEEDED", sm_arn=EOD_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Artifacts: ✓ completion marker + eod_pnl row (2026-08-08)" in text
        assert "MISSING" not in text

    def test_succeeded_with_missing_pnl_row_renders_loud(self, monkeypatch):
        """The issue's core failure mode: a SUCCEEDED terminal whose eod_pnl
        row was never written must not read clean."""
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": '{"run_date": "2026-08-08"}', "error": "", "cause": ""},
            marker_present=True,
            csv_body=b"date,nav\n2026-08-07,100000\n",  # no row for run_date
        )
        event = _event("SUCCEEDED", sm_arn=EOD_ARN)
        result = index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Post-close Trading SF — SUCCEEDED" in text
        assert "⚠️ *ARTIFACT(S) MISSING*" in text
        assert "eod_pnl.csv row for 2026-08-08" in text
        assert "_sf_completion marker" not in text
        assert result["telegram_sent"] is True

    def test_succeeded_with_missing_completion_marker_renders_loud(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": '{"run_date": "2026-08-08"}', "error": "", "cause": ""},
            marker_present=False,
            csv_body=b"date,nav\n2026-08-08,100000\n",
        )
        event = _event("SUCCEEDED", sm_arn=EOD_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "⚠️ *ARTIFACT(S) MISSING*" in text
        assert "_sf_completion marker for 2026-08-08" in text
        assert "eod_pnl.csv row" not in text

    def test_succeeded_with_both_artifacts_missing_names_both(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": '{"run_date": "2026-08-08"}', "error": "", "cause": ""},
            marker_present=False,
            csv_body=b"date,nav\n2026-08-07,100000\n",
        )
        event = _event("SUCCEEDED", sm_arn=EOD_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "_sf_completion marker for 2026-08-08" in text
        assert "eod_pnl.csv row for 2026-08-08" in text

    def test_succeeded_with_unresolvable_run_date_reports_unverified(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": "{}", "error": "", "cause": ""},
        )
        event = _event("SUCCEEDED", sm_arn=EOD_ARN, name="exec-001")
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "⚠️ *ARTIFACTS UNVERIFIED*" in text

    def test_non_eod_pipeline_succeeded_skips_artifact_check(self, monkeypatch):
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={"input": '{"run_date": "2026-08-08"}', "error": "", "cause": ""},
        )
        event = _event("SUCCEEDED", sm_arn=WEEKDAY_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Artifacts:" not in text
        assert "ARTIFACT(S) MISSING" not in text
        assert "ARTIFACTS UNVERIFIED" not in text

    def test_failed_non_degraded_eod_skips_artifact_check(self, monkeypatch):
        """A genuine crash FAILED (not DegradedRun) does not trigger the
        artifact check — that check exists for SUCCEEDED/DEGRADED only."""
        _sf, _s3 = _eod_client_stubs(
            monkeypatch,
            describe_return={
                "input": '{"run_date": "2026-08-08"}',
                "error": "States.TaskFailed",
                "cause": "CaptureSnapshot state failed",
            },
        )
        event = _event("FAILED", sm_arn=EOD_ARN)
        index.handler(event, None)
        text = _telegram_mod.send_message.call_args.args[0]
        assert "Artifacts:" not in text
        assert "ARTIFACT(S) MISSING" not in text
