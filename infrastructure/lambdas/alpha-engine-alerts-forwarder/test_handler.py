"""Smoke tests for the SNS-to-EventBridge forwarder Lambda handler.

Mocks boto3.client('events').put_events so the handler runs end-to-end
without needing AWS creds. Verifies the correct PutEvents call shape
and source attribution.

Run from the repo root:

  python3 infrastructure/lambdas/alpha-engine-alerts-forwarder/test_handler.py
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

HERE = Path(__file__).resolve().parent


def _import_handler() -> tuple:
    """Import index.py with boto3 stubbed.

    Mocks boto3.client('events') before import so the handler can be
    imported in a vanilla Python environment.
    """
    os.environ.setdefault("AWS_ACCOUNT_ID", "711398986525")
    os.environ.setdefault("AWS_REGION", "us-east-1")

    fake_events_client = MagicMock()
    fake_events_client.put_events.return_value = {
        "FailedEntryCount": 0,
        "Entries": [{"EventId": "evt-123"}],
    }

    fake_boto3 = MagicMock()
    fake_boto3.client = MagicMock(return_value=fake_events_client)

    sys.modules["boto3"] = fake_boto3
    if "index" in sys.modules:
        del sys.modules["index"]

    import index

    return index.handler, fake_events_client


def _sample_sns_event(
    topic_arn: str = "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
    subject: str = "Step Function failure: DeployDriftCheck",
    message: str = "DeployDriftCheck timed out after 60s",
    message_id: str = "abcd1234-5678-9012-3456-7890abcdef00",
) -> dict:
    return {
        "Records": [
            {
                "Sns": {
                    "Message": message,
                    "Subject": subject,
                    "TopicArn": topic_arn,
                    "MessageId": message_id,
                    "Timestamp": "2026-07-24T12:00:00.000Z",
                }
            }
        ]
    }


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.handler, self.events_client = _import_handler()

    def test_forwards_one_sns_record(self):
        result = self.handler(_sample_sns_event(), context=None)
        self.assertEqual(result["forwarded"], 1)
        self.assertEqual(result["failed"], 0)
        self.events_client.put_events.assert_called_once()

    def test_uses_correct_event_bus(self):
        self.handler(_sample_sns_event(), context=None)
        call_kwargs = self.events_client.put_events.call_args[1]
        entries = call_kwargs["Entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["EventBusName"], "nousergon-alerts")
        self.assertEqual(entries[0]["Source"], "nousergon.sns-forwarder")
        self.assertEqual(entries[0]["DetailType"], "nousergon.alert.v1")

    def test_source_attribution_in_detail(self):
        self.handler(_sample_sns_event(), context=None)
        call_kwargs = self.events_client.put_events.call_args[1]
        detail = json.loads(call_kwargs["Entries"][0]["Detail"])
        self.assertEqual(detail["original_topic_arn"],
                         "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts")
        self.assertEqual(detail["original_subject"],
                         "Step Function failure: DeployDriftCheck")
        self.assertEqual(detail["original_message_id"],
                         "abcd1234-5678-9012-3456-7890abcdef00")
        self.assertEqual(detail["source"], "sns-forwarder")
        self.assertEqual(detail["version"], "1.0")

    def test_dedup_key_from_message_id(self):
        self.handler(_sample_sns_event(), context=None)
        call_kwargs = self.events_client.put_events.call_args[1]
        detail = json.loads(call_kwargs["Entries"][0]["Detail"])
        self.assertEqual(detail["dedup_key"],
                         "sns-abcd1234-5678-9012-3456-7890abcdef00")

    def test_handles_json_message(self):
        msg = json.dumps({"event": "test", "value": 42})
        self.handler(_sample_sns_event(message=msg), context=None)
        call_kwargs = self.events_client.put_events.call_args[1]
        detail = json.loads(call_kwargs["Entries"][0]["Detail"])
        self.assertEqual(detail["message"],
                         {"event": "test", "value": 42})

    def test_handles_plain_text_message(self):
        self.handler(_sample_sns_event(message="plain text alert"), context=None)
        call_kwargs = self.events_client.put_events.call_args[1]
        detail = json.loads(call_kwargs["Entries"][0]["Detail"])
        self.assertEqual(detail["message"], {"text": "plain text alert"})

    def test_skips_backstop_topic(self):
        """Epic I2821 invariant 3: never forward the backstop topic."""
        backstop_event = _sample_sns_event(
            topic_arn="arn:aws:sns:us-east-1:711398986525:alpha-engine-alarm-backstop",
            subject="Backstop alarm",
            message="Disk space critical",
        )
        result = self.handler(backstop_event, context=None)
        self.assertEqual(result["forwarded"], 0)
        self.assertEqual(result["failed"], 0)
        self.events_client.put_events.assert_not_called()

    def test_handles_missing_subject(self):
        ev = _sample_sns_event(subject="")
        result = self.handler(ev, context=None)
        self.assertEqual(result["forwarded"], 1)
        detail = json.loads(
            self.events_client.put_events.call_args[1]["Entries"][0]["Detail"]
        )
        self.assertEqual(detail["original_subject"], "")

    def test_handles_empty_event(self):
        result = self.handler({"Records": []}, context=None)
        self.assertEqual(result["forwarded"], 0)
        self.assertEqual(result["failed"], 0)
        self.events_client.put_events.assert_not_called()

    def test_handles_records_without_sns_key(self):
        ev = {"Records": [{"no_sns": "here"}]}
        result = self.handler(ev, context=None)
        self.assertEqual(result["forwarded"], 0)
        self.assertEqual(result["failed"], 1)

    def test_batches_across_10_entries(self):
        """PutEvents max per call is 10; verify batching works."""
        records = []
        for i in range(12):
            records.append(
                _sample_sns_event(
                    message=f"test {i}",
                    message_id=f"id-{i:04d}",
                )["Records"][0]
            )
        ev = {"Records": records}
        self.handler(ev, context=None)
        # Should have been called twice (10 + 2)
        self.assertEqual(self.events_client.put_events.call_count, 2)

    def test_forwarder_recovers_from_partial_failure(self):
        """Simulate 1 failed entry in a 2-entry batch."""
        self.events_client.put_events.return_value = {
            "FailedEntryCount": 1,
            "Entries": [
                {"EventId": "evt-ok"},
                {"ErrorCode": "InternalFailure", "ErrorMessage": "test err"},
            ],
        }
        ev = _sample_sns_event(message="first")
        ev["Records"].append(
            _sample_sns_event(
                message="second",
                message_id="eeee-ffff-0000-1111",
            )["Records"][0]
        )
        result = self.handler(ev, context=None)
        self.assertEqual(result["forwarded"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(result["errors"]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
