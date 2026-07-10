"""Tests for the ArcticDB feature-store freshness sentinel in
builders/daily_append.py (config#1787, Brian's 2026-07-08 Option-B ruling).

The sentinel is a small, UNCONDITIONAL S3 marker (``feature_store/_freshness.json``)
written on every successful daily_append ArcticDB write, so
``nousergon_lib.artifact_freshness``'s ordinary S3 ArtifactSpec probe (plain
HEAD/LIST + recency — zero new backend code) has something to check for the
ArcticDB feature-store producer. Deliberately separate from
``health/universe_freshness.json`` (the richer per-symbol staleness receipt
that is conditionally written only when the whole scan passes).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from builders.daily_append import (
    FEATURE_STORE_FRESHNESS_SENTINEL_KEY,
    _write_feature_store_freshness_sentinel,
)


class TestFeatureStoreFreshnessSentinel:
    def test_writes_sentinel_with_timestamp_and_library(self):
        """Happy path: sentinel is written to the canonical S3 key with a
        UTC ISO-8601 timestamp and the symbol/library written."""
        s3 = MagicMock()

        before = datetime.now(timezone.utc).replace(microsecond=0)
        sentinel = _write_feature_store_freshness_sentinel(s3, "test-bucket", library="universe")
        after = datetime.now(timezone.utc).replace(microsecond=0)

        s3.put_object.assert_called_once()
        kwargs = s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "test-bucket"
        assert kwargs["Key"] == FEATURE_STORE_FRESHNESS_SENTINEL_KEY
        assert kwargs["ContentType"] == "application/json"

        body = json.loads(kwargs["Body"].decode("utf-8"))
        assert body["symbol_or_library"] == "universe"
        assert body["library"] == "universe"
        assert sentinel == body

        # Timestamp is a well-formed UTC ISO-8601 string within the call window.
        ts = datetime.strptime(body["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        assert before <= ts <= after

    def test_symbol_or_library_override(self):
        """A caller-supplied symbol_or_library takes precedence over library."""
        s3 = MagicMock()
        sentinel = _write_feature_store_freshness_sentinel(
            s3, "test-bucket", library="universe", symbol_or_library="AAPL",
        )
        assert sentinel["symbol_or_library"] == "AAPL"
        assert sentinel["library"] == "universe"

    def test_s3_failure_is_swallowed_not_raised(self):
        """Best-effort: an S3 put_object failure must be logged and swallowed,
        never raised — the sentinel is observability, not load-bearing, and
        must never fail a daily_append run that already completed its real
        ArcticDB writes."""
        s3 = MagicMock()
        s3.put_object.side_effect = RuntimeError("simulated S3 outage")

        # Must not raise.
        sentinel = _write_feature_store_freshness_sentinel(s3, "test-bucket")
        assert sentinel["library"] == "universe"  # still returns the payload

    def test_s3_failure_logs_warning(self, caplog):
        import logging

        s3 = MagicMock()
        s3.put_object.side_effect = RuntimeError("simulated S3 outage")

        with caplog.at_level(logging.WARNING, logger="builders.daily_append"):
            _write_feature_store_freshness_sentinel(s3, "test-bucket")

        warnings = [r for r in caplog.records if "sentinel write FAILED" in r.message]
        assert warnings, f"expected a WARN log; got: {[r.message for r in caplog.records]}"


class TestFeatureStoreFreshnessSentinelWiredIntoDailyAppend:
    """The sentinel must actually be called from the successful-write path
    of _daily_append_impl — not just exist as a standalone helper."""

    def test_daily_append_module_calls_sentinel_after_error_rate_gate(self):
        """Source-level wiring check: the sentinel write call must appear in
        _daily_append_impl, after the error-rate gate and before the
        (raise-capable) freshness-scan receipt call — so a stale-scan raise
        can never suppress the sentinel the way it suppresses the receipt."""
        import inspect

        from builders import daily_append as da

        src = inspect.getsource(da._daily_append_impl)
        sentinel_idx = src.index("_write_feature_store_freshness_sentinel(")
        error_rate_idx = src.index("exceeds 5% threshold")
        scan_idx = src.index("_scan_universe_and_emit_freshness_receipt(")

        assert error_rate_idx < sentinel_idx < scan_idx, (
            "expected order: error-rate gate -> sentinel write -> freshness-scan "
            "receipt (which can raise on stale symbols and must not suppress "
            "the sentinel)"
        )
