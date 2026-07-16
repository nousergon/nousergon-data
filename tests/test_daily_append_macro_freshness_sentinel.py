"""Tests for the ArcticDB macro/SPY freshness sentinel in
builders/daily_append.py (alpha-engine-config-I2702 deliverable #1/#2).

The sentinel (``feature_store/_macro_freshness.json``) is the ARTIFACT the
new EOD precondition probe (infrastructure/lambdas/eod-precondition-probe)
checks in place of the old ``$.data_spot_error`` launch-phase flag test.
Deliberately a separate S3 key from the universe sentinel
(``FEATURE_STORE_FRESHNESS_SENTINEL_KEY``) — see the module docstring next to
``MACRO_FRESHNESS_SENTINEL_KEY`` for why sharing one key would let the two
writers silently mask each other. Carries an explicit ``run_date`` (unlike
the universe sentinel's recency-only ``timestamp``) so the probe can confirm
THIS SPECIFIC trading day's SPY close was verified present.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from builders.daily_append import (
    MACRO_FRESHNESS_SENTINEL_KEY,
    _write_macro_freshness_sentinel,
)


class TestMacroFreshnessSentinel:
    def test_writes_sentinel_with_run_date_and_verified_keys(self):
        s3 = MagicMock()

        before = datetime.now(timezone.utc).replace(microsecond=0)
        sentinel = _write_macro_freshness_sentinel(
            s3, "test-bucket", run_date="2026-07-15", verified_keys=["SPY", "VIX", "TNX"],
        )
        after = datetime.now(timezone.utc).replace(microsecond=0)

        s3.put_object.assert_called_once()
        kwargs = s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "test-bucket"
        assert kwargs["Key"] == MACRO_FRESHNESS_SENTINEL_KEY
        assert kwargs["Key"] != "feature_store/_freshness.json"  # distinct from the universe key
        assert kwargs["ContentType"] == "application/json"

        body = json.loads(kwargs["Body"].decode("utf-8"))
        assert body["run_date"] == "2026-07-15"
        assert body["verified_keys"] == ["SPY", "TNX", "VIX"]  # sorted
        assert sentinel == body

        ts = datetime.strptime(body["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        assert before <= ts <= after

    def test_verified_keys_is_sorted_and_deduplicated_is_not_required(self):
        # sorted() is applied for deterministic output; duplicates are the
        # caller's responsibility (daily_append never passes duplicates).
        s3 = MagicMock()
        sentinel = _write_macro_freshness_sentinel(
            s3, "test-bucket", run_date="2026-07-15", verified_keys=["XLY", "SPY", "GLD"],
        )
        assert sentinel["verified_keys"] == ["GLD", "SPY", "XLY"]

    def test_s3_failure_is_swallowed_not_raised(self):
        """Best-effort: a sentinel write failure must never fail a
        daily_append run whose real ArcticDB writes already succeeded and
        were already readback-verified (feedback_no_silent_fails swallow
        carve-out: the probe correctly reports precondition_met=False and
        the self-heal loop retries — never a silent false-green)."""
        s3 = MagicMock()
        s3.put_object.side_effect = RuntimeError("simulated S3 outage")

        sentinel = _write_macro_freshness_sentinel(
            s3, "test-bucket", run_date="2026-07-15", verified_keys=["SPY"],
        )
        assert sentinel["run_date"] == "2026-07-15"  # still returns the payload

    def test_s3_failure_logs_warning(self, caplog):
        import logging

        s3 = MagicMock()
        s3.put_object.side_effect = RuntimeError("simulated S3 outage")

        with caplog.at_level(logging.WARNING, logger="builders.daily_append"):
            _write_macro_freshness_sentinel(
                s3, "test-bucket", run_date="2026-07-15", verified_keys=["SPY"],
            )

        warnings = [r for r in caplog.records if "sentinel write FAILED" in r.message]
        assert warnings, f"expected a WARN log; got: {[r.message for r in caplog.records]}"


class TestMacroFreshnessSentinelWiredIntoDailyAppend:
    """Source-level wiring check: the sentinel write must appear in
    _daily_append_impl AFTER the macro verification_failures raise-check, so
    it is only ever written once every key it lists has been proven
    readback-present for date_str — never optimistically ahead of that
    verification."""

    def test_sentinel_write_follows_verification_failures_check(self):
        import inspect

        from builders import daily_append as da

        src = inspect.getsource(da._daily_append_impl)
        verify_idx = src.index("Macro update verification failed for")
        sentinel_idx = src.index("_write_macro_freshness_sentinel(")

        assert verify_idx < sentinel_idx, (
            "expected order: macro verification_failures raise-check -> "
            "macro-freshness sentinel write, so the sentinel is only ever "
            "written after every listed key is confirmed present"
        )

    def test_sentinel_receives_run_date_and_verified_keys_kwargs(self):
        import inspect

        from builders import daily_append as da

        src = inspect.getsource(da._daily_append_impl)
        call_start = src.index("_write_macro_freshness_sentinel(")
        call_snippet = src[call_start:call_start + 300]
        assert "run_date=date_str" in call_snippet
        assert "verified_keys=macro_updated + sector_updated" in call_snippet
