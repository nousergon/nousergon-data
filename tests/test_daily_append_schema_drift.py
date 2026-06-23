"""Tests for the schema_drift_incidents instrumentation in daily_append.

config#1150 Batch B. Before this change an ArcticDB
``StreamDescriptorMismatch`` (the 2026-05-21 EOD incident) propagated out of
the universe / macro write paths UNCOUNTED — invisible to the report card
until an operator noticed the failed run by hand. The instrumentation is a
COUNTED wrapper hung off the existing failure path:

  (a) the per-run counter increments on a schema-drift error,
  (b) the error is STILL RAISED after counting (fail-loud — NEVER swallowed),
  (c) the count lands in CloudWatch + ``market_data/weekly/{date}/manifest.json``.

These three properties are exactly what this module locks.
"""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arcticdb.exceptions import (
    NormalizationException,
    SchemaException,
    StreamDescriptorMismatch,
)

from builders.daily_append import (
    _SCHEMA_DRIFT_EXC,
    _count_schema_drift,
    _emit_schema_drift_metric,
    _write_schema_drift_manifest,
)

_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


class _InMemoryS3:
    """Minimal in-memory S3 mock (put_object + get_object). No moto dep —
    mirrors tests/test_news_aggregates.py::_InMemoryS3."""

    class _NoSuchKey(Exception):
        pass

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self._store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise self._NoSuchKey(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": BytesIO(self._store[(Bucket, Key)])}


def _mismatch() -> StreamDescriptorMismatch:
    """Construct a StreamDescriptorMismatch the way ArcticDB raises it."""
    try:
        # The C++-bound ctor signature varies across versions; fall back to a
        # message-only construction if the rich ctor isn't accepted.
        return StreamDescriptorMismatch("SYM", "expected", "actual")  # type: ignore[call-arg]
    except TypeError:
        return StreamDescriptorMismatch("descriptor mismatch on SYM")


# ── (a) + (b): the counter increments AND the error is re-raised ────────────

class TestCountSchemaDriftWrapper:
    def test_schema_drift_increments_then_reraises(self):
        """Counter increments by one AND the original exception re-raises
        (fail-loud — the run still fails)."""
        counter = [0]
        with pytest.raises(StreamDescriptorMismatch):
            with _count_schema_drift(counter):
                raise _mismatch()
        assert counter[0] == 1, "schema-drift incident must be counted"

    def test_reraises_the_exact_exception_not_swallowed(self):
        """The SAME exception object propagates — proves it is re-raised, not
        replaced/swallowed."""
        counter = [0]
        exc = _mismatch()
        with pytest.raises(StreamDescriptorMismatch) as caught:
            with _count_schema_drift(counter):
                raise exc
        assert caught.value is exc

    @pytest.mark.parametrize("exc_cls", [SchemaException, NormalizationException])
    def test_sibling_schema_exceptions_also_counted(self, exc_cls):
        counter = [0]
        with pytest.raises(exc_cls):
            with _count_schema_drift(counter):
                raise exc_cls("schema-shape failure")
        assert counter[0] == 1

    def test_clean_write_does_not_increment(self):
        counter = [0]
        with _count_schema_drift(counter):
            pass  # a write that succeeds
        assert counter[0] == 0

    def test_non_schema_exception_passes_through_uncounted(self):
        """A non-schema error (e.g. a connectivity RuntimeError) must NOT be
        counted as schema drift and must still propagate."""
        counter = [0]
        with pytest.raises(ValueError):
            with _count_schema_drift(counter):
                raise ValueError("unrelated failure")
        assert counter[0] == 0

    def test_on_drift_emit_fires_before_reraise(self):
        """The emit-on-abort callback runs with the incremented count BEFORE
        the exception re-raises — so the incident lands in CloudWatch +
        manifest even on a fail-loud abort."""
        counter = [0]
        emitted: list[int] = []
        with pytest.raises(StreamDescriptorMismatch):
            with _count_schema_drift(counter, on_drift=emitted.append):
                raise _mismatch()
        assert emitted == [1], "emit-on-abort must fire once with the count"

    def test_on_drift_emit_failure_does_not_mask_incident(self):
        """If the emit itself throws, the ORIGINAL schema-drift exception must
        still re-raise — observability must never eat the incident."""
        counter = [0]

        def _boom(_count):
            raise RuntimeError("cloudwatch down")

        with pytest.raises(StreamDescriptorMismatch):
            with _count_schema_drift(counter, on_drift=_boom):
                raise _mismatch()
        assert counter[0] == 1

    def test_exc_tuple_members(self):
        assert StreamDescriptorMismatch in _SCHEMA_DRIFT_EXC
        assert SchemaException in _SCHEMA_DRIFT_EXC
        assert NormalizationException in _SCHEMA_DRIFT_EXC


# ── (c): the count lands in manifest.json ───────────────────────────────────

class TestSchemaDriftManifest:
    def test_count_lands_in_manifest_json(self):
        s3 = _InMemoryS3()
        _write_schema_drift_manifest(s3, "alpha-engine-research", "2026-06-23", 3)
        key = "market_data/weekly/2026-06-23/manifest.json"
        assert ("alpha-engine-research", key) in s3._store
        manifest = json.loads(s3._store[("alpha-engine-research", key)])
        assert manifest["schema_drift_incidents"] == 3
        assert manifest["date"] == "2026-06-23"
        assert "schema_drift_written_at" in manifest

    def test_zero_count_still_written(self):
        """A clean run (count 0) still writes the manifest so the artifact
        reflects the last run's true state."""
        s3 = _InMemoryS3()
        _write_schema_drift_manifest(s3, "b", "2026-06-23", 0)
        manifest = json.loads(s3._store[("b", "market_data/weekly/2026-06-23/manifest.json")])
        assert manifest["schema_drift_incidents"] == 0

    def test_merges_into_existing_manifest(self):
        """A co-located key written by another producer is preserved."""
        s3 = _InMemoryS3()
        key = "market_data/weekly/2026-06-23/manifest.json"
        s3.put_object(
            Bucket="b", Key=key,
            Body=json.dumps({"other_producer_field": "keep-me"}).encode(),
        )
        _write_schema_drift_manifest(s3, "b", "2026-06-23", 2)
        manifest = json.loads(s3._store[("b", key)])
        assert manifest["other_producer_field"] == "keep-me"
        assert manifest["schema_drift_incidents"] == 2

    def test_s3_error_does_not_raise(self):
        """A manifest write error is best-effort — must not fail the run
        (CloudWatch carries the same count, and the incident itself raises
        elsewhere)."""
        s3 = MagicMock()
        s3.get_object.side_effect = Exception("no prior")
        s3.put_object.side_effect = Exception("s3 down")
        # Must not raise.
        _write_schema_drift_manifest(s3, "b", "2026-06-23", 1)


# ── CloudWatch emit ─────────────────────────────────────────────────────────

class TestSchemaDriftMetric:
    def test_emits_named_metric(self):
        cw = MagicMock()
        with patch("builders.daily_append.boto3.client", return_value=cw):
            _emit_schema_drift_metric(4)
        cw.put_metric_data.assert_called_once()
        kwargs = cw.put_metric_data.call_args.kwargs
        assert kwargs["Namespace"] == "AlphaEngine/Data"
        md = kwargs["MetricData"][0]
        assert md["MetricName"] == "daily_append_schema_drift_count"
        assert md["Value"] == 4.0
        assert md["Unit"] == "Count"

    def test_cloudwatch_error_does_not_raise(self):
        with patch("builders.daily_append.boto3.client", side_effect=Exception("cw down")):
            _emit_schema_drift_metric(1)  # best-effort, must not raise


# ── source-level: both write paths are wrapped (no UNCOUNTED path) ──────────

class TestWritePathsWrapped:
    def test_universe_batch_write_is_wrapped(self):
        src = _source()
        assert "with _count_schema_drift(n_schema_drift, on_drift=_emit_schema_drift):" in src
        # The batch writes must live INSIDE a _count_schema_drift block.
        assert "universe_lib.update_batch(update_payloads, upsert=True)" in src

    def test_macro_write_is_wrapped(self):
        src = _source()
        # The macro/sector _write_row_backfill_safe calls must be inside a
        # _count_schema_drift wrapper.
        assert "with _count_schema_drift(n_schema_drift, on_drift=_emit_schema_drift):\n" \
               "                    mode = _write_row_backfill_safe(macro_lib, key, new_row)" in src

    def test_count_in_result_and_emitted_on_clean_path(self):
        src = _source()
        assert '"schema_drift_incidents": n_schema_drift[0],' in src
        assert "_emit_schema_drift(n_schema_drift[0])" in src
