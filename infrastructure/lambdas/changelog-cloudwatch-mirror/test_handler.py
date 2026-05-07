"""Smoke tests for the CloudWatch-mirror Lambda handler.

Mocks boto3.client('s3').put_object so the handler runs end-to-end without
needing AWS creds. Verifies decode + structured-entry shape + per-logEvent
fan-out + log-group → subsystem inference.

Run from the repo root:

  python3 infrastructure/lambdas/changelog-cloudwatch-mirror/test_handler.py
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

HERE = Path(__file__).resolve().parent


def _import_handler():
    """Import index.py with boto3.client + env vars stubbed."""
    os.environ.setdefault("CHANGELOG_BUCKET", "test-bucket")
    sys.path.insert(0, str(HERE))

    fake_boto3 = MagicMock()
    fake_s3_client = MagicMock()
    fake_boto3.client = MagicMock(return_value=fake_s3_client)

    sys.modules["boto3"] = fake_boto3
    if "index" in sys.modules:
        del sys.modules["index"]
    import index
    return index, fake_s3_client


def _encode_subscription_event(payload: dict) -> dict:
    """Wrap a CloudWatch Logs payload as a subscription-filter Lambda event."""
    raw = json.dumps(payload).encode("utf-8")
    blob = base64.b64encode(gzip.compress(raw)).decode("ascii")
    return {"awslogs": {"data": blob}}


def _sample_payload(
    log_group: str = "/aws/lambda/alpha-engine-predictor-inference",
    log_events: list[dict] | None = None,
    message_type: str = "DATA_MESSAGE",
) -> dict:
    if log_events is None:
        log_events = [
            {
                "id": "37155797159871123456789012345678901234567890123456",
                "timestamp": 1778198400000,  # 2026-05-08T00:00:00Z
                "message": "[ERROR] Cold start exceeded 60s\nTraceback (most recent call last):\n  File ...",
            }
        ]
    return {
        "messageType": message_type,
        "owner": "711398986525",
        "logGroup": log_group,
        "logStream": "2026/05/08/[$LATEST]abcd1234",
        "subscriptionFilters": ["alpha-engine-error-mirror"],
        "logEvents": log_events,
    }


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.index, self.s3 = _import_handler()

    # --- happy path -------------------------------------------------

    def test_writes_one_entry_per_log_event(self):
        result = self.index.handler(
            _encode_subscription_event(_sample_payload()), context=None
        )
        self.assertEqual(result["wrote"], 1)
        self.assertEqual(self.s3.put_object.call_count, 1)
        key = self.s3.put_object.call_args_list[0].kwargs["Key"]
        self.assertTrue(key.startswith("changelog/entries/2026-05-08/"))
        self.assertTrue(key.endswith(".json"))

    def test_multiple_log_events_fan_out(self):
        payload = _sample_payload(log_events=[
            {"id": "id-a", "timestamp": 1778198400000, "message": "[ERROR] one"},
            {"id": "id-b", "timestamp": 1778198401000, "message": "[CRITICAL] two"},
            {"id": "id-c", "timestamp": 1778198402000, "message": "Task timed out after 60s"},
        ])
        result = self.index.handler(_encode_subscription_event(payload), context=None)
        self.assertEqual(result["wrote"], 3)
        self.assertEqual(self.s3.put_object.call_count, 3)

    def test_structured_entry_payload(self):
        self.index.handler(_encode_subscription_event(_sample_payload()), context=None)
        call = self.s3.put_object.call_args_list[0]
        body = json.loads(call.kwargs["Body"].decode())

        self.assertEqual(body["schema_version"], "1.0.0")
        self.assertEqual(body["event_type"], "incident")
        self.assertEqual(body["severity"], "high")
        self.assertEqual(body["subsystem"], "predictor")
        self.assertEqual(body["root_cause_category"], "infrastructure_failure")
        self.assertEqual(body["source"], "cloudwatch-mirror")
        self.assertTrue(body["auto_emitted"])
        self.assertEqual(body["detected_at"], "2026-05-08T00:00:00Z")
        self.assertIsNone(body["resolved_at"])
        self.assertEqual(body["actor"], "alpha-engine-predictor-inference")
        self.assertEqual(body["machine"], "lambda:changelog-cloudwatch-mirror")
        self.assertEqual(
            body["cloudwatch"]["log_group"],
            "/aws/lambda/alpha-engine-predictor-inference",
        )
        # First non-empty line is the summary
        self.assertEqual(body["summary"], "[ERROR] Cold start exceeded 60s")
        # Full multiline message preserved in description
        self.assertIn("Traceback", body["description"])

    # --- subsystem inference ---------------------------------------

    def test_subsystem_inference_predictor(self):
        self.index.handler(_encode_subscription_event(
            _sample_payload(log_group="/aws/lambda/alpha-engine-predictor-health-check")
        ), context=None)
        body = json.loads(self.s3.put_object.call_args_list[0].kwargs["Body"].decode())
        self.assertEqual(body["subsystem"], "predictor")

    def test_subsystem_inference_research(self):
        self.index.handler(_encode_subscription_event(
            _sample_payload(log_group="/aws/lambda/alpha-engine-research-runner")
        ), context=None)
        body = json.loads(self.s3.put_object.call_args_list[0].kwargs["Body"].decode())
        self.assertEqual(body["subsystem"], "research")

    def test_subsystem_inference_data_pipeline(self):
        self.index.handler(_encode_subscription_event(
            _sample_payload(log_group="/aws/lambda/alpha-engine-data-collector")
        ), context=None)
        body = json.loads(self.s3.put_object.call_args_list[0].kwargs["Body"].decode())
        self.assertEqual(body["subsystem"], "data_pipeline")

    def test_subsystem_inference_eval(self):
        self.index.handler(_encode_subscription_event(
            _sample_payload(log_group="/aws/lambda/alpha-engine-replay-concordance")
        ), context=None)
        body = json.loads(self.s3.put_object.call_args_list[0].kwargs["Body"].decode())
        self.assertEqual(body["subsystem"], "eval")

    def test_subsystem_inference_default(self):
        self.index.handler(_encode_subscription_event(
            _sample_payload(log_group="/aws/lambda/alpha-engine-ec2-lifecycle")
        ), context=None)
        body = json.loads(self.s3.put_object.call_args_list[0].kwargs["Body"].decode())
        self.assertEqual(body["subsystem"], "infrastructure")

    # --- event_id format -------------------------------------------

    def test_event_id_format(self):
        self.index.handler(_encode_subscription_event(_sample_payload()), context=None)
        body = json.loads(self.s3.put_object.call_args_list[0].kwargs["Body"].decode())
        # Format: 2026-05-08T00-00-00_alpha-engine-predictor-inference_<7-hex>
        parts = body["event_id"].split("_")
        self.assertEqual(parts[0], "2026-05-08T00-00-00")
        self.assertEqual(parts[1], "alpha-engine-predictor-inference")
        self.assertEqual(len(parts[2]), 7)

    def test_event_id_distinct_for_same_timestamp(self):
        """Two errors at the same epoch ms must produce distinct event_ids
        (hashed on log_event.id, not ts+summary)."""
        payload = _sample_payload(log_events=[
            {"id": "id-a", "timestamp": 1778198400000, "message": "[ERROR] x"},
            {"id": "id-b", "timestamp": 1778198400000, "message": "[ERROR] x"},
        ])
        self.index.handler(_encode_subscription_event(payload), context=None)
        keys = [c.kwargs["Key"] for c in self.s3.put_object.call_args_list]
        self.assertEqual(len(set(keys)), 2, "event_ids collided")

    # --- edge cases ------------------------------------------------

    def test_control_message_no_op(self):
        result = self.index.handler(
            _encode_subscription_event(_sample_payload(message_type="CONTROL_MESSAGE")),
            context=None,
        )
        self.assertEqual(result["wrote"], 0)
        self.assertEqual(self.s3.put_object.call_count, 0)

    def test_empty_log_events_no_op(self):
        result = self.index.handler(
            _encode_subscription_event(_sample_payload(log_events=[])),
            context=None,
        )
        self.assertEqual(result["wrote"], 0)

    def test_skips_empty_message(self):
        payload = _sample_payload(log_events=[
            {"id": "id-a", "timestamp": 1778198400000, "message": ""},
            {"id": "id-b", "timestamp": 1778198400000, "message": "   "},
            {"id": "id-c", "timestamp": 1778198400000, "message": "[ERROR] real"},
        ])
        result = self.index.handler(_encode_subscription_event(payload), context=None)
        self.assertEqual(result["wrote"], 1)

    def test_handles_missing_timestamp(self):
        payload = _sample_payload(log_events=[
            {"id": "id-a", "message": "[ERROR] no ts"},
        ])
        # Should fall back to current UTC, not crash
        result = self.index.handler(_encode_subscription_event(payload), context=None)
        self.assertEqual(result["wrote"], 1)
        body = json.loads(self.s3.put_object.call_args_list[0].kwargs["Body"].decode())
        # ts_utc format still valid
        import re
        self.assertRegex(body["ts_utc"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


if __name__ == "__main__":
    unittest.main(verbosity=2)
