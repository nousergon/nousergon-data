"""Tests for the backstop Telegram forwarder (alpha-engine-config-I2899).

Tests run WITHOUT a live SSM or Telegram — they stub boto3 and urllib at the
function level. The hermetic-import guard applies (no nousergon_lib, no krepis,
no flow-doctor). See test_hermetic_import_guard for the wiring test.
"""

from __future__ import annotations

import json
import os
import unittest
from unittest.mock import MagicMock, patch

# Ensure no nousergon_lib/krepis imports at module level
import index  # noqa: E402 — the handler itself must not import these either


class TestEscapeMarkdown(unittest.TestCase):
    """MarkdownV2 escaping is deterministic — just string transforms."""

    def test_escapes_special_chars(self):
        result = index._escape_markdown("hello_world [test] (yep)")
        self.assertEqual(result, r"hello\_world \[test\] \(yep\)")

    def test_noop_on_clean_text(self):
        result = index._escape_markdown("just plain text 123")
        self.assertEqual(result, "just plain text 123")


class TestFormatAlarmMessage(unittest.TestCase):
    """_format_alarm_message builds the Telegram text from an SNS alarm body."""

    def test_alarm_state_includes_emoji_and_state(self):
        msg = {
            "AlarmName": "test-alarm",
            "OldStateValue": "OK",
            "NewStateValue": "ALARM",
            "NewStateReason": "Threshold Crossed",
            "Region": "us-east-1",
            "Trigger": {
                "MetricName": "Errors",
                "Namespace": "AWS/Lambda",
                "Dimensions": [{"value": "my-function"}],
            },
        }
        text = index._format_alarm_message(msg)
        self.assertIn("BACKSTOP", text)
        self.assertIn("test-alarm", text)
        self.assertIn("ALARM", text)
        self.assertIn("OK", text)
        self.assertIn("Errors", text)

    def test_ok_state_shows_resolved(self):
        msg = {
            "AlarmName": "test-alarm",
            "OldStateValue": "ALARM",
            "NewStateValue": "OK",
            "NewStateReason": "OK back to normal",
            "Region": "us-east-1",
            "Trigger": {},
        }
        text = index._format_alarm_message(msg)
        self.assertIn("RESOLVED", text)
        self.assertIn("OK", text)

    def test_long_reason_is_truncated(self):
        msg = {
            "AlarmName": "test-alarm",
            "OldStateValue": "OK",
            "NewStateValue": "ALARM",
            "NewStateReason": "x" * 2000,
            "Region": "us-east-1",
            "Trigger": {},
        }
        text = index._format_alarm_message(msg)
        self.assertLessEqual(len(text), 1200)  # well under Telegram's 4096 limit

    def test_includes_console_link(self):
        msg = {
            "AlarmName": "my-alarm",
            "OldStateValue": "OK",
            "NewStateValue": "ALARM",
            "NewStateReason": "test",
            "Region": "us-east-2",
            "Trigger": {},
        }
        text = index._format_alarm_message(msg)
        self.assertIn("us-east-2", text)
        self.assertIn("AWS Console", text)
        self.assertIn("cloudwatch", text)

    def test_no_trigger_no_metric_no_crash(self):
        msg = {
            "AlarmName": "minimal",
            "OldStateValue": "OK",
            "NewStateValue": "ALARM",
            "Region": "us-east-1",
        }
        text = index._format_alarm_message(msg)  # no crash
        self.assertIn("minimal", text)
        self.assertIn("ALARM", text)


class TestSendTelegram(unittest.TestCase):
    """_send_telegram wraps urllib — test error handling paths."""

    @patch("index.urllib.request.urlopen")
    def test_success_returns_response(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true, "result": {"message_id": 42}}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        result = index._send_telegram("test-token", "123", "hello")
        self.assertIsNotNone(result)
        self.assertTrue(result["ok"])

    @patch("index.urllib.request.urlopen")
    def test_http_error_returns_none(self, mock_urlopen):
        from urllib.error import HTTPError

        # HTTPError needs at minimum a filepointer to avoid AttributeError on .read()
        import io
        fp = io.BytesIO(b'{"ok":false,"description":"Forbidden"}')
        mock_urlopen.side_effect = HTTPError(
            "https://api.telegram.org/badtoken/sendMessage",
            403,
            "Forbidden",
            {},
            fp,
        )

        result = index._send_telegram("bad-token", "123", "hello")
        self.assertIsNone(result)

    @patch("index.urllib.request.urlopen")
    def test_connection_error_returns_none(self, mock_urlopen):
        from urllib.error import URLError

        mock_urlopen.side_effect = URLError("connection refused")

        result = index._send_telegram("token", "123", "hello")
        self.assertIsNone(result)

    @patch("index.urllib.request.urlopen")
    def test_thread_id_included_when_provided(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        index._send_telegram("token", "123", "hello", thread_id="456")
        # urlopen is called as context manager, so we check the Request's data
        call_req = mock_urlopen.call_args[0][0]
        body = call_req.data.decode("utf-8")
        parsed = json.loads(body)
        self.assertEqual(parsed.get("message_thread_id"), 456)

    @patch("index.urllib.request.urlopen")
    def test_no_thread_id_by_default(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        index._send_telegram("token", "123", "hello")
        call_req = mock_urlopen.call_args[0][0]
        body = call_req.data.decode("utf-8")
        parsed = json.loads(body)
        self.assertNotIn("message_thread_id", parsed)


class TestSSMParameter(unittest.TestCase):
    """_ssm_parameter wraps boto3 SSM GetParameter."""

    @patch("index.boto3.client")
    def test_reads_parameter_correctly(self, mock_boto):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.return_value = {
            "Parameter": {"Value": "secret-value"}
        }
        mock_boto.return_value = ssm_mock

        result = index._ssm_parameter("/test/path")
        self.assertEqual(result, "secret-value")
        ssm_mock.get_parameter.assert_called_once_with(
            Name="/test/path", WithDecryption=True
        )

    @patch("index.boto3.client")
    def test_raises_on_error(self, mock_boto):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = Exception("API error")
        mock_boto.return_value = ssm_mock

        with self.assertRaises(Exception):
            index._ssm_parameter("/test/path")


class TestHandler(unittest.TestCase):
    """Full handler invocation tests with stubbed SSM + urllib."""

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_sends_alarm(self, mock_boto, mock_urlopen):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = [
            {"Parameter": {"Value": "fake-bot-token"}},
            {"Parameter": {"Value": "12345"}},
        ]
        mock_boto.return_value = ssm_mock

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true, "result": {"message_id": 1}}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "test-1",
                        "Message": json.dumps({
                            "AlarmName": "test-alarm",
                            "OldStateValue": "OK",
                            "NewStateValue": "ALARM",
                            "NewStateReason": "Threshold crossed: 1 >= 1",
                            "Region": "us-east-1",
                            "Trigger": {"MetricName": "Errors", "Namespace": "AWS/Lambda"},
                        }),
                    }
                }
            ]
        }

        result = index.handler(event, None)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sent"], 1)

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_skips_non_alarm(self, mock_boto, mock_urlopen):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = [
            {"Parameter": {"Value": "fake-bot-token"}},
            {"Parameter": {"Value": "12345"}},
        ]
        mock_boto.return_value = ssm_mock

        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "test-2",
                        "Message": json.dumps({"notification": "subscription-confirmation"}),
                    }
                }
            ]
        }

        result = index.handler(event, None)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sent"], 0)
        self.assertEqual(result["results"][0]["status"], "skipped")

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_empty_records(self, mock_boto, mock_urlopen):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = [
            {"Parameter": {"Value": "fake-bot-token"}},
            {"Parameter": {"Value": "12345"}},
        ]
        mock_boto.return_value = ssm_mock

        # Empty records list — handler should no-op gracefully
        event = {"Records": []}
        result = index.handler(event, None)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sent"], 0)

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_ssm_failure_returns_error(self, mock_boto, mock_urlopen):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = Exception("SSM unavailable")
        mock_boto.return_value = ssm_mock

        event = {"Records": [{"Sns": {"MessageId": "test-3", "Message": "{}"}}]}
        result = index.handler(event, None)
        self.assertEqual(result["status"], "error")
        self.assertIn("SSM", result["reason"])


class TestHermeticImportGuard(unittest.TestCase):
    """The handler must NOT import nousergon_lib, krepis, or flow_doctor_telegram
    — sharing code with the smart path would violate backstop independence
    invariant 3 (alpha-engine-config-I2899)."""

    def test_no_forbidden_imports_in_index(self):
        import ast

        with open(os.path.join(os.path.dirname(__file__), "index.py")) as f:
            tree = ast.parse(f.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.name.split(".")[0]
                    self.assertNotIn(
                        name,
                        ["nousergon_lib", "krepis", "flow_doctor_telegram"],
                        f"index.py must not import {name} (violates backstop independence)",
                    )
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    base = node.module.split(".")[0]
                    self.assertNotIn(
                        base,
                        ["nousergon_lib", "krepis", "flow_doctor_telegram"],
                        f"index.py must not import from {node.module} (violates backstop independence)",
                    )


if __name__ == "__main__":
    unittest.main()
