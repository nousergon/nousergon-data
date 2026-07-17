"""Unit tests for alpha-engine-sweep-artifact-monitor (alpha-engine-config#2392).

boto3 is real-imported but every client is a MagicMock via
`patch("index.boto3.client", side_effect=...)` (mirrors
infrastructure/lambdas/friday-shell-run-report/test_handler.py's convention —
no AWS access, no nousergon_lib dependency to stub).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402

GROOM_SF_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-groom-dispatch"
OTHER_SF_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
_EXEC_ARN = (
    "arn:aws:states:us-east-1:711398986525:execution:alpha-engine-groom-dispatch:run-1"
)
# 2026-07-13 14:00:00 UTC in epoch ms.
_STOP_MS = int(datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc).timestamp() * 1000)


def _event(status="SUCCEEDED", sm_arn=GROOM_SF_ARN, stop_ms=_STOP_MS, name="run-1"):
    return {
        "detail": {
            "status": status,
            "stateMachineArn": sm_arn,
            "executionArn": _EXEC_ARN,
            "name": name,
            "stopDate": stop_ms,
        }
    }


def _sweep_output(sweep: dict | None) -> str:
    return json.dumps({"sweep": sweep} if sweep is not None else {})


def _fake_clients(*, describe_output: str, list_objects_pages: dict | None = None,
                   get_object_bodies: dict | None = None):
    """Return a boto3.client side_effect dispatching by service name.

    ``list_objects_pages`` maps Prefix -> list of S3 keys (Contents).
    ``get_object_bodies`` maps Key -> the JSON-serializable artifact body.
    """
    list_objects_pages = list_objects_pages or {}
    get_object_bodies = get_object_bodies or {}

    sfn = MagicMock()
    sfn.describe_execution.return_value = {"output": describe_output}

    s3 = MagicMock()

    def _list_objects_v2(Bucket, Prefix):  # noqa: N803 — boto3 kwarg casing
        keys = list_objects_pages.get(Prefix, [])
        return {"Contents": [{"Key": k} for k in keys]}

    def _get_object(Bucket, Key):  # noqa: N803
        body = get_object_bodies[Key]
        stream = MagicMock()
        stream.read.return_value = json.dumps(body).encode()
        return {"Body": stream}

    s3.list_objects_v2.side_effect = _list_objects_v2
    s3.get_object.side_effect = _get_object

    sns = MagicMock()

    clients = {"stepfunctions": sfn, "s3": s3, "sns": sns}
    return (lambda svc, **kw: clients[svc]), sfn, s3, sns


# ── event filtering (acceptance criterion 3: never alert on FAILED) ────────


def test_wrong_state_machine_is_ignored():
    side, sfn, s3, sns = _fake_clients(describe_output="{}")
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(sm_arn=OTHER_SF_ARN), None)
    assert out == {"checked": False, "reason": "wrong_event"}
    sfn.describe_execution.assert_not_called()
    sns.publish.assert_not_called()


def test_failed_execution_does_not_alert():
    side, sfn, s3, sns = _fake_clients(describe_output="{}")
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(status="FAILED"), None)
    assert out == {"checked": False, "reason": "wrong_event"}
    sfn.describe_execution.assert_not_called()
    sns.publish.assert_not_called()


def test_running_status_is_ignored():
    side, sfn, s3, sns = _fake_clients(describe_output="{}")
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(status="RUNNING"), None)
    assert out["checked"] is False
    sns.publish.assert_not_called()


# ── concurrent-guard skip case (issue gotcha: must not false-positive) ─────


def test_concurrent_tier_skip_does_not_alert():
    sweep = {"launched": False, "reason": "concurrent_tier_skip",
              "existing_instance_ids": ["i-prior-sweep"]}
    side, sfn, s3, sns = _fake_clients(describe_output=_sweep_output(sweep))
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(), None)
    assert out["checked"] is True
    assert out["expected"] is False
    assert "concurrent" in out["reason"]
    s3.list_objects_v2.assert_not_called()
    sns.publish.assert_not_called()


def test_sweep_dispatch_failure_already_notified_does_not_double_alert():
    sweep = {"dispatched": False, "error": {"Error": "States.ALL", "Cause": "boom"}}
    side, sfn, s3, sns = _fake_clients(describe_output=_sweep_output(sweep))
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(), None)
    assert out["checked"] is True
    assert out["expected"] is False
    assert "already" in out["reason"]
    s3.list_objects_v2.assert_not_called()
    sns.publish.assert_not_called()


# ── present-artifact case: no alert ─────────────────────────────────────────


def test_present_sweep_artifact_does_not_alert():
    sweep = {"launched": True, "instance_id": "i-sweep-box"}
    run_start = datetime(2026, 7, 13, 13, 50, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    side, sfn, s3, sns = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={
            "groom/2026-07-13/sweep-": ["groom/2026-07-13/sweep-135000.json"],
        },
        get_object_bodies={
            "groom/2026-07-13/sweep-135000.json": {
                "run_kind": "sweep", "run_start": run_start,
            },
        },
    )
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(), None)
    assert out["checked"] is True
    assert out["expected"] is True
    assert out["found"] is True
    assert out["artifact_key"] == "groom/2026-07-13/sweep-135000.json"
    sns.publish.assert_not_called()


def test_artifact_outside_grace_window_is_not_a_match_and_alerts():
    """run_start 40 minutes before stopDate — outside the default 15-min
    grace window, so it must not count as a match."""
    sweep = {"launched": True}
    run_start = datetime(2026, 7, 13, 13, 20, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    side, sfn, s3, sns = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={
            "groom/2026-07-13/sweep-": ["groom/2026-07-13/sweep-132000.json"],
            "groom/2026-07-12/sweep-": [],
        },
        get_object_bodies={
            "groom/2026-07-13/sweep-132000.json": {
                "run_kind": "sweep", "run_start": run_start,
            },
        },
    )
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(), None)
    assert out["found"] is False
    assert out["alerted"] is True
    sns.publish.assert_called_once()


def test_non_sweep_run_kind_in_prefix_is_skipped():
    """A coverage-run artifact that happens to share the sweep- prefix search
    window must never be mistaken for a sweep artifact (defensive: in
    practice coverage runs use a different run_id shape, but run_kind is the
    authoritative discriminator, not the key name)."""
    sweep = {"launched": True}
    run_start = datetime(2026, 7, 13, 13, 55, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    side, sfn, s3, sns = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={
            "groom/2026-07-13/sweep-": ["groom/2026-07-13/sweep-135500.json"],
            "groom/2026-07-12/sweep-": [],
        },
        get_object_bodies={
            "groom/2026-07-13/sweep-135500.json": {
                "run_kind": "coverage", "run_start": run_start,
            },
        },
    )
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(), None)
    assert out["found"] is False
    assert out["alerted"] is True


# ── missing-artifact case: alerts via SNS alpha-engine-alerts ──────────────


def test_missing_sweep_artifact_alerts_via_sns():
    sweep = {"launched": True}
    side, sfn, s3, sns = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={
            "groom/2026-07-13/sweep-": [],
            "groom/2026-07-12/sweep-": [],
        },
    )
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(), None)
    assert out["checked"] is True
    assert out["expected"] is True
    assert out["found"] is False
    assert out["alerted"] is True
    sns.publish.assert_called_once()
    kwargs = sns.publish.call_args.kwargs
    assert kwargs["TopicArn"] == index.SNS_TOPIC_ARN
    assert "alpha-engine-alerts" in index.SNS_TOPIC_ARN
    assert "sweep" in kwargs["Message"]
    assert _EXEC_ARN in kwargs["Message"]


def test_unparseable_execution_output_falls_through_to_artifact_check():
    """Malformed/absent $.sweep on a SUCCEEDED execution must NOT be silently
    skipped — it falls through to the S3 check (expect-artifact case) so a
    genuinely missing sweep still alerts even if the output shape drifts."""
    side, sfn, s3, sns = _fake_clients(
        describe_output="not valid json",
        list_objects_pages={
            "groom/2026-07-13/sweep-": [],
            "groom/2026-07-12/sweep-": [],
        },
    )
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(), None)
    assert out["expected"] is True
    assert out["alerted"] is True


def test_missing_stopdate_falls_back_to_now_and_still_checks():
    sweep = {"launched": True}
    side, sfn, s3, sns = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={},
    )
    event = _event()
    del event["detail"]["stopDate"]
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(event, None)
    assert out["checked"] is True
    assert out["expected"] is True
    # No artifacts anywhere -> alerted.
    assert out["alerted"] is True


# ── prior-UTC-date search (stopDate shortly after midnight) ────────────────


def test_searches_prior_utc_date_for_run_start_before_midnight():
    """stopDate just after UTC midnight; run_start still dated the previous
    day (the sweep box's own run_start predates the SF's stopDate) — must be
    found via the prior-date prefix search."""
    midnight_plus_5 = datetime(2026, 7, 13, 0, 5, tzinfo=timezone.utc)
    stop_ms = int(midnight_plus_5.timestamp() * 1000)
    run_start = datetime(2026, 7, 12, 23, 58, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    sweep = {"launched": True}
    side, sfn, s3, sns = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={
            "groom/2026-07-13/sweep-": [],
            "groom/2026-07-12/sweep-": ["groom/2026-07-12/sweep-235800.json"],
        },
        get_object_bodies={
            "groom/2026-07-12/sweep-235800.json": {
                "run_kind": "sweep", "run_start": run_start,
            },
        },
    )
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event(stop_ms=stop_ms), None)
    assert out["found"] is True
    assert out["artifact_key"] == "groom/2026-07-12/sweep-235800.json"
    sns.publish.assert_not_called()


# ── Sunday double-trigger: independent executions, independent checks ─────


def test_two_independent_sunday_executions_each_checked_independently():
    """Sunday's 0700 daily-mid and 0900 gated-reverify triggers are separate
    SF executions with separate executionArns — each SUCCEEDED event must be
    evaluated on its own; a present artifact for one must not suppress the
    alert for the other's missing artifact."""
    sweep = {"launched": True}

    # First execution (0700): its own sweep artifact IS present.
    run_start_1 = datetime(2026, 7, 12, 7, 10, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    side1, sfn1, s3_1, sns1 = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={
            "groom/2026-07-12/sweep-": ["groom/2026-07-12/sweep-071000.json"],
            "groom/2026-07-11/sweep-": [],
        },
        get_object_bodies={
            "groom/2026-07-12/sweep-071000.json": {
                "run_kind": "sweep", "run_start": run_start_1,
            },
        },
    )
    stop_ms_1 = int(datetime(2026, 7, 12, 7, 15, tzinfo=timezone.utc).timestamp() * 1000)
    with patch("index.boto3.client", side_effect=side1):
        out1 = index.handler(_event(stop_ms=stop_ms_1, name="run-0700"), None)
    assert out1["found"] is True
    sns1.publish.assert_not_called()

    # Second execution (0900 gated-reverify): its OWN artifact is missing —
    # must still alert despite the 0700 run's artifact existing.
    side2, sfn2, s3_2, sns2 = _fake_clients(
        describe_output=_sweep_output(sweep),
        list_objects_pages={
            "groom/2026-07-12/sweep-": ["groom/2026-07-12/sweep-071000.json"],
            "groom/2026-07-11/sweep-": [],
        },
        get_object_bodies={
            "groom/2026-07-12/sweep-071000.json": {
                "run_kind": "sweep", "run_start": run_start_1,
            },
        },
    )
    stop_ms_2 = int(datetime(2026, 7, 12, 9, 15, tzinfo=timezone.utc).timestamp() * 1000)
    with patch("index.boto3.client", side_effect=side2):
        out2 = index.handler(_event(stop_ms=stop_ms_2, name="run-0900-gated-reverify"), None)
    assert out2["found"] is False
    assert out2["alerted"] is True
    sns2.publish.assert_called_once()


# ── fail-loud: an AWS API failure raises, never a silent skip ──────────────


def test_describe_execution_failure_raises():
    side, sfn, s3, sns = _fake_clients(describe_output="{}")
    sfn.describe_execution.side_effect = RuntimeError("throttled")
    with patch("index.boto3.client", side_effect=side):
        try:
            index.handler(_event(), None)
            assert False, "expected RuntimeError to propagate"
        except RuntimeError as exc:
            assert "throttled" in str(exc)


def test_list_objects_failure_raises():
    sweep = {"launched": True}
    side, sfn, s3, sns = _fake_clients(describe_output=_sweep_output(sweep))
    s3.list_objects_v2.side_effect = RuntimeError("access denied")
    with patch("index.boto3.client", side_effect=side):
        try:
            index.handler(_event(), None)
            assert False, "expected RuntimeError to propagate"
        except RuntimeError as exc:
            assert "access denied" in str(exc)
