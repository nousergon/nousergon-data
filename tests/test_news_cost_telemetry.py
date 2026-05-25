"""Tests for rag/pipelines/_cost_telemetry.py — the news-pipeline cost sink.

Lock down:

- The Anthropic client proxy records each ``messages.create()`` response
  into the buffer without changing the response shape returned to the caller.
- Buffer ``flush()`` writes the rows as a single JSONL S3 object at the
  canonical ``decision_artifacts/_cost_raw/{date}/{date}/data-news-event-extraction.jsonl``
  key, or skips when empty.
- Per-call recording failures (e.g. malformed response) are logged but
  do NOT propagate — the event extractor's primary deliverable must
  survive a cost-telemetry hiccup.
- Flush failures (S3 errors) DO raise per ``[[feedback_no_silent_fails]]``.
"""

from __future__ import annotations

import io
import json
from datetime import date
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from rag.pipelines._cost_telemetry import (
    CostBufferFlushError,
    S3CostBuffer,
    build_news_cost_buffer,
    wrap_client_for_cost_telemetry,
)


_BUCKET = "alpha-engine-research"


# ── Fake Anthropic types (mirrors test_cost.py in alpha-engine-lib) ──────


class _FakeServerToolUsage:
    def __init__(self, *, web_search_requests=0, web_fetch_requests=0):
        self.web_search_requests = web_search_requests
        self.web_fetch_requests = web_fetch_requests


class _FakeUsage:
    def __init__(
        self, *, input_tokens, output_tokens,
        cache_read_input_tokens=None, cache_creation_input_tokens=None,
        server_tool_use=None,
    ):
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cache_read_input_tokens = cache_read_input_tokens
        self.cache_creation_input_tokens = cache_creation_input_tokens
        self.server_tool_use = server_tool_use


class _FakeMessage:
    def __init__(self, *, model, usage):
        self.model = model
        self.usage = usage


@pytest.fixture
def mocked_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=_BUCKET)
        yield s3


class TestS3CostBuffer:
    def test_record_returns_cost_and_appends_row(self):
        buf = S3CostBuffer(
            run_id="2026-05-25", agent_id="data:news_event_extraction",
        )
        msg = _FakeMessage(
            model="claude-haiku-4-5",
            usage=_FakeUsage(input_tokens=1000, output_tokens=200),
        )
        cost = buf.record(msg)
        # (1000 * 1.0 + 200 * 5.0) / 1M = 0.002
        assert cost == pytest.approx(0.002, abs=1e-6)
        assert buf.row_count == 1

    def test_record_stamps_run_id_and_agent_id(self):
        buf = S3CostBuffer(
            run_id="2026-05-25", agent_id="data:news_event_extraction",
        )
        msg = _FakeMessage(
            model="claude-haiku-4-5",
            usage=_FakeUsage(input_tokens=10, output_tokens=5),
        )
        buf.record(msg)
        row = buf._rows[0]
        assert row["run_id"] == "2026-05-25"
        assert row["agent_id"] == "data:news_event_extraction"

    def test_flush_empty_buffer_returns_none_and_writes_nothing(self, mocked_s3):
        buf = S3CostBuffer(
            run_id="2026-05-25", agent_id="data:news_event_extraction",
            s3_client=mocked_s3,
        )
        key = buf.flush()
        assert key is None
        listing = mocked_s3.list_objects_v2(Bucket=_BUCKET)
        assert listing.get("KeyCount", 0) == 0

    def test_flush_writes_single_jsonl_at_canonical_key(self, mocked_s3):
        buf = S3CostBuffer(
            run_id="2026-05-25", agent_id="data:news_event_extraction",
            s3_client=mocked_s3,
        )
        for i in range(3):
            buf.record(_FakeMessage(
                model="claude-haiku-4-5",
                usage=_FakeUsage(input_tokens=100 * (i + 1), output_tokens=50),
            ))
        key = buf.flush()
        expected = (
            "decision_artifacts/_cost_raw/2026-05-25/2026-05-25/"
            "data:news_event_extraction.jsonl"
        )
        assert key == expected
        obj = mocked_s3.get_object(Bucket=_BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")
        lines = [ln for ln in body.splitlines() if ln.strip()]
        assert len(lines) == 3
        for ln in lines:
            row = json.loads(ln)
            assert row["run_id"] == "2026-05-25"
            assert row["agent_id"] == "data:news_event_extraction"
            assert "cost_usd" in row

    def test_flush_failure_hard_fails(self):
        """S3 PutObject failure raises CostBufferFlushError, NOT swallowed.

        Per ``[[feedback_no_silent_fails]]`` — losing the rolled-up cost
        record would defeat the Phase 0 visibility goal."""
        stub = MagicMock()
        stub.put_object.side_effect = RuntimeError("AccessDenied")
        buf = S3CostBuffer(
            run_id="2026-05-25", agent_id="data:news_event_extraction",
            s3_client=stub,
        )
        buf.record(_FakeMessage(
            model="claude-haiku-4-5",
            usage=_FakeUsage(input_tokens=10, output_tokens=5),
        ))
        with pytest.raises(CostBufferFlushError, match="AccessDenied"):
            buf.flush()


class TestWrapClientForCostTelemetry:
    def test_proxy_records_each_create_call(self):
        buf = S3CostBuffer(
            run_id="2026-05-25", agent_id="data:news_event_extraction",
        )
        underlying_response = _FakeMessage(
            model="claude-haiku-4-5",
            usage=_FakeUsage(input_tokens=100, output_tokens=50),
        )
        underlying_client = MagicMock()
        underlying_client.messages.create.return_value = underlying_response

        wrapped = wrap_client_for_cost_telemetry(underlying_client, buf)
        result = wrapped.messages.create(
            model="claude-haiku-4-5", max_tokens=1024,
            messages=[{"role": "user", "content": "x"}],
        )

        # Response unchanged.
        assert result is underlying_response
        # Recorded into buffer.
        assert buf.row_count == 1
        # Underlying client was actually invoked with the passed kwargs.
        underlying_client.messages.create.assert_called_once()

    def test_per_call_recording_failure_is_logged_not_raised(self, caplog):
        """If the recorder raises (e.g., malformed response), the
        primary deliverable (event extraction) MUST continue. Flush-
        time S3 failures still raise; per-call recording does not."""
        buf = MagicMock()
        buf.record.side_effect = RuntimeError("bad msg shape")
        underlying_client = MagicMock()
        underlying_response = MagicMock()
        underlying_client.messages.create.return_value = underlying_response

        wrapped = wrap_client_for_cost_telemetry(underlying_client, buf)
        # Should NOT raise.
        result = wrapped.messages.create(model="x", messages=[])
        assert result is underlying_response
        # Warn logged.
        assert any(
            "per-call recording failed" in r.message
            for r in caplog.records
        )

    def test_non_messages_attributes_forward_to_wrapped(self):
        """Sanity: the proxy must not break access to other SDK surfaces."""
        underlying_client = MagicMock()
        underlying_client.beta = "beta-namespace"
        buf = S3CostBuffer(run_id="2026-05-25", agent_id="x")
        wrapped = wrap_client_for_cost_telemetry(underlying_client, buf)
        assert wrapped.beta == "beta-namespace"


class TestBuildNewsCostBuffer:
    def test_canonical_naming(self):
        buf = build_news_cost_buffer(run_date=date(2026, 5, 25))
        assert buf._run_id == "2026-05-25"
        assert buf._agent_id == "data:news_event_extraction"
