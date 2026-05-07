"""Smoke tests for the SNS-mirror Lambda handler.

Mocks boto3.client('s3').put_object so the handler runs end-to-end
without needing AWS creds. Verifies the structured entry is emitted
with correct key + payload.

Run from the repo root:

  python3 infrastructure/lambdas/changelog-incident-mirror/test_handler.py
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

HERE = Path(__file__).resolve().parent


def _import_handler():
    """Import index.py with boto3.client + env vars stubbed.

    The module imports boto3 at top-level + reads CHANGELOG_BUCKET from env;
    we patch both before import so the import succeeds in a vanilla
    Python environment without boto3 installed.
    """
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


def _sample_event() -> dict:
    return {
        "Records": [
            {
                "Sns": {
                    "Message": "DeployDriftCheck timed out after 60s\nFunction: alpha-engine-deploy-drift",
                    "Subject": "Step Function failure: DeployDriftCheck",
                    "TopicArn": "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
                    "MessageId": "abcd1234-5678-9012-3456-7890abcdef00",
                    "Timestamp": "2026-05-01T13:00:00.000Z",
                }
            }
        ]
    }


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.index, self.s3 = _import_handler()

    def test_writes_one_structured_entry_per_message(self):
        result = self.index.handler(_sample_event(), context=None)
        self.assertEqual(result["wrote"], 1)
        self.assertEqual(self.s3.put_object.call_count, 1)
        # Single put lands under the structured prefix; legacy
        # changelog/incidents/ writes retired 2026-05-07.
        key = self.s3.put_object.call_args_list[0].kwargs["Key"]
        self.assertTrue(key.startswith("changelog/entries/"))

    def test_structured_entry_payload(self):
        self.index.handler(_sample_event(), context=None)
        calls = self.s3.put_object.call_args_list
        structured_call = next(c for c in calls if "entries/" in c.kwargs["Key"])
        key = structured_call.kwargs["Key"]
        body = json.loads(structured_call.kwargs["Body"].decode())

        self.assertTrue(key.startswith("changelog/entries/2026-05-01/"))
        self.assertTrue(key.endswith(".json"))
        self.assertEqual(body["schema_version"], "1.0.0")
        self.assertEqual(body["event_type"], "incident")
        self.assertEqual(body["severity"], "high")
        self.assertEqual(body["subsystem"], "infrastructure")
        self.assertEqual(body["root_cause_category"], "infrastructure_failure")
        self.assertEqual(body["source"], "sns-mirror")
        self.assertTrue(body["auto_emitted"])
        self.assertEqual(body["detected_at"], "2026-05-01T13:00:00Z")
        self.assertIsNone(body["resolved_at"])
        self.assertEqual(body["actor"], "alpha-engine-alerts")
        self.assertEqual(body["machine"], "lambda:changelog-incident-mirror")
        self.assertEqual(body["sns"]["message_id"], "abcd1234-5678-9012-3456-7890abcdef00")

    def test_event_id_format(self):
        self.index.handler(_sample_event(), context=None)
        calls = self.s3.put_object.call_args_list
        structured_call = next(c for c in calls if "entries/" in c.kwargs["Key"])
        body = json.loads(structured_call.kwargs["Body"].decode())
        # Format: 2026-05-01T13-00-00_alpha-engine-alerts_<7-hex>
        parts = body["event_id"].split("_")
        self.assertEqual(parts[0], "2026-05-01T13-00-00")
        self.assertEqual(parts[1], "alpha-engine-alerts")
        self.assertEqual(len(parts[2]), 7)

    def test_handles_missing_subject(self):
        ev = _sample_event()
        ev["Records"][0]["Sns"]["Subject"] = ""
        self.index.handler(ev, context=None)
        calls = self.s3.put_object.call_args_list
        structured_call = next(c for c in calls if "entries/" in c.kwargs["Key"])
        body = json.loads(structured_call.kwargs["Body"].decode())
        # Falls back to first line of message
        self.assertEqual(body["summary"], "DeployDriftCheck timed out after 60s")

    def test_handles_invalid_timestamp(self):
        ev = _sample_event()
        ev["Records"][0]["Sns"]["Timestamp"] = "not-a-date"
        self.index.handler(ev, context=None)
        calls = self.s3.put_object.call_args_list
        structured_call = next(c for c in calls if "entries/" in c.kwargs["Key"])
        body = json.loads(structured_call.kwargs["Body"].decode())
        # Falls back to current UTC; format is still correct
        self.assertRegex(body["ts_utc"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


if __name__ == "__main__":
    unittest.main(verbosity=2)
