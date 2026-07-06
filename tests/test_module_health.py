"""Unit tests for weekly_collector._write_module_health.

Validates that the module-scoped health stamp consumed by the executor's
upstream gate delegates to nousergon_lib.health.write_health with the correct
schema, deliverables mapping, and key pattern.
"""
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

from weekly_collector import _write_module_health


def _capture_put() -> MagicMock:
    """Patch boto3.client('s3') and return the mocked put_object call tracker."""
    s3 = MagicMock()
    return s3


class TestWriteModuleHealth:

    @patch("weekly_collector.boto3")
    def test_ok_status_populates_last_success(self, mock_boto3):
        s3 = _capture_put()
        mock_boto3.client.return_value = s3

        _write_module_health(
            "bucket", "daily_data", "2026-04-16", "ok",
            summary={"tickers_captured": 909},
            duration_seconds=42.7,
        )

        s3.put_object.assert_called_once()
        call_kwargs = s3.put_object.call_args.kwargs
        assert call_kwargs["Key"] == "health/daily_data.json"
        assert call_kwargs["Bucket"] == "bucket"
        payload = json.loads(call_kwargs["Body"].decode("utf-8"))
        assert payload["module"] == "daily_data"
        assert payload["status"] == "ok"
        assert payload["run_date"] == "2026-04-16"
        assert payload["duration_seconds"] == 42.7
        assert payload["summary"] == {"tickers_captured": 909}
        assert payload["warnings"] == []
        assert payload["error"] is None
        assert payload["last_success"] is not None
        assert payload["deliverables"] == [
            {
                "name": "daily_data",
                "required": True,
                "produced": True,
                "detail": "",
            }
        ]
        datetime.fromisoformat(payload["last_success"])

    @patch("weekly_collector.boto3")
    def test_failed_status_nulls_last_success(self, mock_boto3):
        s3 = _capture_put()
        mock_boto3.client.return_value = s3

        _write_module_health(
            "bucket", "daily_data", "2026-04-16", "failed",
            error="polygon 429 rate-limited",
        )

        payload = json.loads(s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
        assert payload["status"] == "failed"
        assert payload["last_success"] is None
        assert payload["error"] == "polygon 429 rate-limited"
        assert payload["deliverables"][0]["produced"] is False

    @patch("weekly_collector.boto3")
    def test_degraded_status_populates_last_success(self, mock_boto3):
        """Degraded runs still count as a last_success — partial data is usable."""
        s3 = _capture_put()
        mock_boto3.client.return_value = s3

        _write_module_health(
            "bucket", "daily_data", "2026-04-16", "degraded",
            warnings=["109 tickers missing from polygon"],
            summary={"tickers_captured": 800},
        )

        payload = json.loads(s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
        assert payload["status"] == "degraded"
        assert payload["last_success"] is not None
        assert payload["warnings"] == ["109 tickers missing from polygon"]

    @patch("weekly_collector.boto3")
    def test_key_pattern_matches_executor_contract(self, mock_boto3):
        """Key must be health/{module_name}.json — matches executor's read_health."""
        s3 = _capture_put()
        mock_boto3.client.return_value = s3

        _write_module_health("bucket", "predictor_inference", "2026-04-16", "ok")
        assert s3.put_object.call_args.kwargs["Key"] == "health/predictor_inference.json"

    @patch("weekly_collector.boto3")
    def test_default_summary_and_warnings_are_empty_dicts_lists(self, mock_boto3):
        s3 = _capture_put()
        mock_boto3.client.return_value = s3

        _write_module_health("bucket", "daily_data", "2026-04-16", "ok")
        payload = json.loads(s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
        assert payload["summary"] == {}
        assert payload["warnings"] == []
        assert payload["error"] is None
        assert payload["duration_seconds"] == 0.0
