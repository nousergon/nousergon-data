"""Tests for the ArcticDB universe-close freshness sentinel in
builders/daily_append.py (config#3237).

Background: `eod-precondition-probe` only checked the macro-SPY sentinel,
which says nothing about the OTHER held positions' closes in the `universe`
library. On 2026-07-21 the universe append failed 100% while the macro
sentinel stayed present/matching, so the probe wrongly reported
precondition_met=True and reconcile hard-crashed instead of routing to the
self-heal loop. This sentinel (`feature_store/_universe_close_freshness.json`)
is the ARTIFACT that closes that gap — a strict run_date-EXACT readback
count, distinct from the looser `health/universe_freshness.json` receipt
(which tolerates up to `UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS` days
stale and therefore cannot answer "did TODAY's row land").
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from builders.daily_append import (
    UNIVERSE_CLOSE_FRESHNESS_SENTINEL_KEY,
    UNIVERSE_FRESHNESS_RECEIPT_KEY,
    _scan_universe_and_emit_freshness_receipt,
    _write_universe_close_freshness_sentinel,
)


def _today_str(offset_days: int = 0) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=offset_days)).isoformat()


def _mock_lib_with_dates(symbol_to_date: dict[str, str]) -> MagicMock:
    lib = MagicMock()
    lib.list_symbols.return_value = list(symbol_to_date.keys())

    def _tail(sym, n=1):
        df = pd.DataFrame({"Close": [100.0]}, index=[pd.Timestamp(symbol_to_date[sym])])
        result = MagicMock()
        result.data = df
        return result

    lib.tail.side_effect = _tail
    return lib


class TestWriteUniverseCloseFreshnessSentinel:
    def test_writes_sentinel_with_run_date_and_counts(self):
        s3 = MagicMock()

        before = datetime.now(timezone.utc).replace(microsecond=0)
        sentinel = _write_universe_close_freshness_sentinel(
            s3, "test-bucket", run_date="2026-07-21",
            verified_ticker_count=498, total_symbols_checked=500,
        )
        after = datetime.now(timezone.utc).replace(microsecond=0)

        s3.put_object.assert_called_once()
        kwargs = s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "test-bucket"
        assert kwargs["Key"] == UNIVERSE_CLOSE_FRESHNESS_SENTINEL_KEY
        assert kwargs["Key"] != "feature_store/_macro_freshness.json"
        assert kwargs["ContentType"] == "application/json"

        body = json.loads(kwargs["Body"].decode("utf-8"))
        assert body["run_date"] == "2026-07-21"
        assert body["verified_ticker_count"] == 498
        assert body["total_symbols_checked"] == 500
        assert sentinel == body

        ts = datetime.strptime(body["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        assert before <= ts <= after

    def test_s3_failure_is_swallowed_not_raised(self):
        s3 = MagicMock()
        s3.put_object.side_effect = RuntimeError("simulated S3 outage")

        sentinel = _write_universe_close_freshness_sentinel(
            s3, "test-bucket", run_date="2026-07-21",
            verified_ticker_count=0, total_symbols_checked=500,
        )
        assert sentinel["run_date"] == "2026-07-21"  # still returns the payload

    def test_s3_failure_logs_warning(self, caplog):
        s3 = MagicMock()
        s3.put_object.side_effect = RuntimeError("simulated S3 outage")

        with caplog.at_level(logging.WARNING, logger="builders.daily_append"):
            _write_universe_close_freshness_sentinel(
                s3, "test-bucket", run_date="2026-07-21",
                verified_ticker_count=0, total_symbols_checked=500,
            )

        warnings = [r for r in caplog.records if "sentinel write FAILED" in r.message]
        assert warnings, f"expected a WARN log; got: {[r.message for r in caplog.records]}"


class TestScanWiresRunDateThroughToUniverseSentinel:
    """The scan is the single point that already reads back every symbol's
    tail row — the universe sentinel reuses that data instead of a second
    ArcticDB pass, so these tests exercise it through the scan entrypoint."""

    def test_run_date_none_skips_universe_sentinel_entirely(self):
        """Backward compat: existing callers that don't pass run_date (e.g.
        any call site outside daily_append's own EOD write path) keep
        writing exactly the one health/universe_freshness.json receipt —
        no new sentinel, no new S3 call."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({"AAPL": _today_str(0)})

        _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        assert s3.put_object.call_count == 1
        assert s3.put_object.call_args.kwargs["Key"] == UNIVERSE_FRESHNESS_RECEIPT_KEY

    def test_run_date_given_writes_both_sentinels(self):
        s3 = MagicMock()
        today = _today_str(0)
        lib = _mock_lib_with_dates({"AAPL": today, "MSFT": today})

        _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib, run_date=today)

        assert s3.put_object.call_count == 2
        keys = {c.kwargs["Key"] for c in s3.put_object.call_args_list}
        assert keys == {UNIVERSE_FRESHNESS_RECEIPT_KEY, UNIVERSE_CLOSE_FRESHNESS_SENTINEL_KEY}

        universe_call = next(
            c for c in s3.put_object.call_args_list
            if c.kwargs["Key"] == UNIVERSE_CLOSE_FRESHNESS_SENTINEL_KEY
        )
        body = json.loads(universe_call.kwargs["Body"].decode("utf-8"))
        assert body["run_date"] == today
        assert body["verified_ticker_count"] == 2
        assert body["total_symbols_checked"] == 2

    def test_verified_ticker_count_excludes_recently_but_not_exactly_fresh(self):
        """The core config#3237 distinction: a symbol 1 trading day stale
        passes the LOOSE all_fresh check (within the 3-trading-day
        tolerance, so no raise) but must NOT count toward
        verified_ticker_count for the strict run_date-exact sentinel —
        that's precisely the gap between the two artifacts this fix closes."""
        s3 = MagicMock()
        today = _today_str(0)
        yesterday = _today_str(1)
        lib = _mock_lib_with_dates({"FRESH": today, "ONE_DAY_STALE": yesterday})

        receipt = _scan_universe_and_emit_freshness_receipt(
            s3, "test-bucket", lib, run_date=today,
        )

        assert receipt["all_fresh"] is True  # loose check still passes
        universe_call = next(
            c for c in s3.put_object.call_args_list
            if c.kwargs["Key"] == UNIVERSE_CLOSE_FRESHNESS_SENTINEL_KEY
        )
        body = json.loads(universe_call.kwargs["Body"].decode("utf-8"))
        assert body["verified_ticker_count"] == 1  # only FRESH matches today exactly
        assert body["total_symbols_checked"] == 2

    def test_stale_raise_never_reaches_universe_sentinel_write(self):
        """A genuine staleness violation (config#3236-class 100% write
        failure) must never leave a green universe-close sentinel behind —
        the raise happens before either S3 write."""
        s3 = MagicMock()
        today = _today_str(0)
        stale = _today_str(20)
        lib = _mock_lib_with_dates({"AAPL": today, "STALE": stale})

        with pytest.raises(RuntimeError, match="STALE"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib, run_date=today)

        s3.put_object.assert_not_called()


class TestUniverseCloseFreshnessSentinelWiredIntoDailyAppend:
    """Source-level wiring check, mirroring
    TestMacroFreshnessSentinelWiredIntoDailyAppend in
    test_daily_append_macro_freshness_sentinel.py: the scan call inside
    _daily_append_impl must actually pass run_date=date_str, or the sentinel
    silently never fires on the real EOD path."""

    def test_scan_call_receives_run_date_kwarg(self):
        import inspect

        from builders import daily_append as da

        src = inspect.getsource(da._daily_append_impl)
        call_start = src.index("_scan_universe_and_emit_freshness_receipt(")
        call_snippet = src[call_start:call_start + 300]
        assert "run_date=date_str" in call_snippet
