"""Cost-telemetry sink for the news-pipeline LLM call site.

Wraps an Anthropic SDK client so every ``messages.create()`` response
is buffered as a priced JSONL row, flushed to S3 in a single
``PutObject`` at end-of-pipeline. Closes the largest previously-untracked
LLM cost slice in the system (~$20–60/mo per the Phase 0 telemetry
audit at ``alpha-engine-docs/private/prompt-caching-investigation-260525.md``
§1.1).

**Sink contract:**

- One JSONL object per run at
  ``s3://alpha-engine-research/decision_artifacts/_cost_raw/{date}/{date}/data-news-event-extraction.jsonl``.
- Path mirrors the research-side cost-raw partition so the existing
  daily aggregator (``alpha-engine-research/scripts/aggregate_costs.py``)
  picks up data's rows alongside research's — single chokepoint, single
  parquet output, dashboard shows everyone in one panel.
- Per-row fields delegated to :func:`alpha_engine_lib.cost.record_anthropic_call`
  (the v0.33.0 chokepoint). Adds ``run_id`` + ``agent_id`` extras for
  the aggregator's drilldown columns.

Buffered + flushed once at pipeline exit rather than per-call to keep
S3 PutObject volume sane: a single RAGIngestion run can fire 100–300
Haiku calls; per-call writes would be 100–300 PutObjects vs 1.

Per ``[[feedback_no_silent_fails]]`` the flush is hard-fail on S3
error — a silent miss on the dominant cost slice would defeat the
whole Phase 0 visibility goal.
"""

from __future__ import annotations

import json
import logging
from datetime import date as date_type
from typing import Any

from alpha_engine_lib.cost import record_anthropic_call

logger = logging.getLogger(__name__)


_COST_BUCKET = "alpha-engine-research"
_COST_PREFIX = "decision_artifacts/_cost_raw"


class CostBufferFlushError(RuntimeError):
    """Raised when the S3 PutObject for the buffered cost rows fails.

    Per ``[[feedback_no_silent_fails]]`` — a silent S3 failure on the
    cost-telemetry sink would defeat the workstream's visibility goal,
    so the pipeline surfaces it loud rather than swallowing.
    """


class S3CostBuffer:
    """In-memory buffer of priced cost records; flushes once to S3.

    The wrapped client (see :func:`wrap_client_for_cost_telemetry`)
    appends one record per ``messages.create()`` response into
    ``self._rows`` via :meth:`record`. The pipeline calls
    :meth:`flush` at end-of-run to write the accumulated rows as a
    single JSONL object.
    """

    def __init__(
        self,
        *,
        run_id: str,
        agent_id: str,
        bucket: str = _COST_BUCKET,
        s3_client: Any | None = None,
    ) -> None:
        self._run_id = run_id
        self._agent_id = agent_id
        self._bucket = bucket
        self._s3 = s3_client
        self._rows: list[dict] = []

    def record(self, msg: Any) -> float:
        """Price ``msg``, append to buffer, return the row's USD cost.

        Pure delegation to :func:`alpha_engine_lib.cost.record_anthropic_call`
        with the buffer's ``run_id`` + ``agent_id`` stamped onto the
        record's extra_fields so the daily aggregator's by-agent_id
        breakdown surfaces this site's spend.
        """
        record = record_anthropic_call(
            msg,
            extra_fields={
                "run_id": self._run_id,
                "agent_id": self._agent_id,
            },
        )
        self._rows.append(record)
        return float(record["cost_usd"])

    @property
    def row_count(self) -> int:
        return len(self._rows)

    def flush(self) -> str | None:
        """Write the buffered rows as a single JSONL object to S3.

        Returns the S3 key written, or ``None`` if the buffer is empty
        (no LLM calls fired this run — no sink object created so the
        partition stays clean of empty files).

        Raises :exc:`CostBufferFlushError` on any S3 error per
        ``[[feedback_no_silent_fails]]``.
        """
        if not self._rows:
            logger.info(
                "[cost_telemetry] no rows to flush for run_id=%s agent_id=%s",
                self._run_id, self._agent_id,
            )
            return None

        key = (
            f"{_COST_PREFIX}/{self._run_id}/{self._run_id}/"
            f"{self._agent_id}.jsonl"
        )
        body = "\n".join(
            json.dumps(row, default=str) for row in self._rows
        ).encode("utf-8")

        try:
            client = self._s3
            if client is None:
                import boto3
                client = boto3.client("s3")
            client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body,
                ContentType="application/x-ndjson",
            )
        except Exception as exc:
            raise CostBufferFlushError(
                f"Failed to flush {len(self._rows)} cost rows to "
                f"s3://{self._bucket}/{key}: {exc}"
            ) from exc

        logger.info(
            "[cost_telemetry] flushed %d rows to s3://%s/%s "
            "(total cost=$%.4f)",
            len(self._rows), self._bucket, key,
            sum(float(r.get("cost_usd", 0)) for r in self._rows),
        )
        return key


class _CostTrackingMessages:
    """Proxy for ``anthropic.Anthropic().messages`` that records every
    ``create()`` response into the wrapped buffer."""

    def __init__(self, wrapped: Any, buffer: S3CostBuffer) -> None:
        self._wrapped = wrapped
        self._buffer = buffer

    def create(self, *args, **kwargs):
        response = self._wrapped.create(*args, **kwargs)
        try:
            self._buffer.record(response)
        except Exception as exc:
            # Cost-telemetry failure must NOT bring down the producer
            # (event extraction is the primary deliverable). Log loud +
            # keep going. The flush step at pipeline exit still raises
            # on S3 error per the no-silent-fails rule for the artifact
            # write itself — per-call recording failures show up at flush
            # time as a partial row count.
            logger.warning(
                "[cost_telemetry] per-call recording failed: %s "
                "(token counts NOT captured for this call; pipeline "
                "continues)", exc,
            )
        return response


class _CostTrackingClient:
    """Proxy around an Anthropic SDK client. Forwards every attribute
    EXCEPT ``messages``, which is replaced by a ``_CostTrackingMessages``
    proxy that records cost telemetry per call.

    Used by :func:`wrap_client_for_cost_telemetry` so callers can pass
    the wrapped client to any consumer (e.g.,
    ``AnthropicEventExtractor(client=wrapped)``) with zero change to
    the consumer.
    """

    def __init__(self, wrapped: Any, buffer: S3CostBuffer) -> None:
        self._wrapped = wrapped
        self._buffer = buffer

    @property
    def messages(self):
        return _CostTrackingMessages(self._wrapped.messages, self._buffer)

    def __getattr__(self, name: str) -> Any:
        # Forward any other SDK surface (e.g., .beta) verbatim.
        return getattr(self._wrapped, name)


def wrap_client_for_cost_telemetry(
    client: Any,
    buffer: S3CostBuffer,
) -> Any:
    """Wrap an Anthropic SDK client so every ``messages.create()``
    response is recorded into ``buffer``.

    Zero-coupling pattern: the wrapped client is API-compatible with
    the raw SDK client, so consumers (e.g.,
    ``AnthropicEventExtractor(client=...)``) need no change. The
    pipeline composes telemetry at the client-construction layer.
    """
    return _CostTrackingClient(client, buffer)


def build_news_cost_buffer(
    *,
    run_date: date_type,
    s3_client: Any | None = None,
) -> S3CostBuffer:
    """Factory for the news-pipeline cost buffer.

    Standardizes the ``run_id`` + ``agent_id`` naming so the dashboard
    cost panel can pivot on a stable per-site identifier. ``run_id``
    matches the news-pipeline's ``aggregate_date`` (the natural date
    partition for the run) so cost rows align with the data artifact
    they accompany.
    """
    return S3CostBuffer(
        run_id=run_date.isoformat(),
        agent_id="data:news_event_extraction",
        s3_client=s3_client,
    )
