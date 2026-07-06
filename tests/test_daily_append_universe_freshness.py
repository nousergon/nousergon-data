"""Tests for the producer-side universe-freshness scan + receipt emit
in builders/daily_append.py.

The scan is the canonical owner for "every universe symbol got
written today" — replaces the per-Lambda-invocation scans that
previously lived in predictor inference, executor, and backtester
preflights. Hard-fails the daily_append run on any stale symbol
(catches the 2026-04-21 partial-write class). On all-fresh, writes
a receipt JSON to S3 that downstream consumers read in O(1).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from builders.daily_append import (
    UNIVERSE_FRESHNESS_RECEIPT_KEY,
    UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS,
    _scan_universe_and_emit_freshness_receipt,
)
# Backwards-compat alias for tests below — the constant rename (calendar → trading)
# changes the semantic of "N days back," but for these tests the only invariant
# that matters is "above threshold = fail, at or below = pass." Using a calendar
# offset large enough to comfortably exceed the trading-day threshold under any
# day-of-week the suite runs (10 calendar days = ≥7 trading days >> threshold=3).
_STALE_OFFSET_DAYS = UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS + 7
_FRESH_OFFSET_DAYS = 0  # today — always 0 trading days stale


def _mock_lib_with_dates(symbol_to_date: dict[str, str]) -> MagicMock:
    """Build a mock ArcticDB library that responds to list_symbols + tail."""
    lib = MagicMock()
    lib.list_symbols.return_value = list(symbol_to_date.keys())

    def _tail(sym, n=1):
        date_str = symbol_to_date[sym]
        if date_str is None:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame({"Close": [100.0]}, index=[pd.Timestamp(date_str)])
        result = MagicMock()
        result.data = df
        return result

    lib.tail.side_effect = _tail
    return lib


def _today_str(offset_days: int = 0) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=offset_days)).isoformat()


class TestUniverseFreshnessReceipt:
    def test_all_fresh_emits_receipt(self):
        """Happy path: every symbol within threshold → receipt is written
        to the canonical S3 key with all_fresh=True and per-symbol metadata.

        Trading-day-aware: under calendar arithmetic, "1 day ago" meant
        exactly 1; under trading-day arithmetic, the same calendar date can
        be 0 or 1 trading days depending on weekday (Sat → 0, Wed → 1).
        Test verifies the structural invariants (all_fresh + receipt write
        + n_symbols_checked) rather than a specific stalest_age value that
        would flap by day-of-week."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "MSFT": _today_str(1),
            "GOOGL": _today_str(2),
        })

        receipt = _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        assert receipt["all_fresh"] is True
        assert receipt["n_symbols_checked"] == 3
        # stalest field is present and ≤ threshold; the specific symbol +
        # exact age depend on weekday-of-test-run.
        assert receipt["stalest_symbol"] in {"AAPL", "MSFT", "GOOGL"}
        assert receipt["stalest_age_trading_days"] <= UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS

        s3.put_object.assert_called_once()
        kwargs = s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "test-bucket"
        assert kwargs["Key"] == UNIVERSE_FRESHNESS_RECEIPT_KEY
        body = json.loads(kwargs["Body"].decode("utf-8"))
        assert body["all_fresh"] is True
        assert body["library"] == "universe"

    def test_stale_symbol_raises_and_does_not_write(self):
        """Hard-fail mode: if any symbol is older than the threshold,
        raise RuntimeError and do NOT write the receipt — a bad scan
        must not leave a stale artifact that consumers would trust."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "STALE": _today_str(_STALE_OFFSET_DAYS),
        })

        with pytest.raises(RuntimeError, match="STALE"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        s3.put_object.assert_not_called()

    def test_empty_library_raises(self):
        """Zero symbols means upstream pipeline never wrote anything;
        consumers must not receive a misleading all_fresh receipt."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({})

        with pytest.raises(RuntimeError, match="library is empty"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        s3.put_object.assert_not_called()

    def test_read_error_raises(self):
        """A single tail() raise means we can't trust our own scan —
        cannot prove all-fresh, so hard-fail rather than emit the
        receipt with an asterisk."""
        s3 = MagicMock()
        lib = MagicMock()
        lib.list_symbols.return_value = ["AAPL", "BROKEN"]

        def _tail(sym, n=1):
            if sym == "BROKEN":
                raise RuntimeError("simulated arctic read failure")
            result = MagicMock()
            result.data = pd.DataFrame(
                {"Close": [100.0]}, index=[pd.Timestamp(_today_str(0))]
            )
            return result

        lib.tail.side_effect = _tail

        with pytest.raises(RuntimeError, match="failed to read"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        s3.put_object.assert_not_called()

    def test_threshold_boundary_inclusive(self):
        """A symbol at-or-under the trading-day threshold counts as fresh
        (≤, not <). Comfortably above (calendar-day offset >> threshold)
        fails. Trading-day arithmetic via nousergon_lib.dates."""
        s3 = MagicMock()
        # 0 calendar days back = 0 trading days stale = passes
        lib = _mock_lib_with_dates({"EDGE": _today_str(0)})
        receipt = _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)
        assert receipt["all_fresh"] is True

        # ≥7 trading days back = comfortably above the threshold of 3
        lib2 = _mock_lib_with_dates({"EDGE": _today_str(_STALE_OFFSET_DAYS)})
        with pytest.raises(RuntimeError, match="EDGE"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib2)


class TestExpectedTickersScoping:
    """The 2026-05-02 incident's second-layer fix.

    The pre-write missing-from-closes check (PR #132) correctly excludes
    S&P churn-out stragglers via expected_tickers. The post-write
    freshness scan must apply the same scoping or it'll re-trip on the
    same stragglers (one was 25 days stale on 2026-05-02 — HOLX). With
    expected_tickers passed through, only the symbols we actually expect
    to be fresh today get audited; stragglers are excluded and logged at
    INFO so operators see drift building up between prune cycles.
    """

    def test_stragglers_excluded_from_scan(self):
        """ArcticDB has fresh + stragglers; expected_tickers omits stragglers
        → scan only checks the fresh ones → all_fresh=True, receipt written."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "MSFT": _today_str(1),
            "ASGN": _today_str(8),    # straggler, 8d stale
            "HOLX": _today_str(25),   # straggler, 25d stale
        })

        receipt = _scan_universe_and_emit_freshness_receipt(
            s3, "test-bucket", lib,
            expected_tickers=["AAPL", "MSFT"],
        )

        assert receipt["all_fresh"] is True
        assert receipt["n_symbols_checked"] == 2  # stragglers excluded from count
        s3.put_object.assert_called_once()

    def test_stale_symbol_in_expected_still_raises(self):
        """expected_tickers must NOT mask a real freshness gap. A stale
        symbol that IS in expected_tickers must still trip the scan."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "MSFT": _today_str(_STALE_OFFSET_DAYS),  # genuinely stale (≥7 trading days)
            "STRAGGLER": _today_str(20),  # ignored
        })

        with pytest.raises(RuntimeError, match="MSFT"):
            _scan_universe_and_emit_freshness_receipt(
                s3, "test-bucket", lib,
                expected_tickers=["AAPL", "MSFT"],
            )
        s3.put_object.assert_not_called()

    def test_scoping_logs_excluded_stragglers(self, caplog):
        """When stragglers are excluded, log them at INFO so operators see
        drift building up between prune cycles. Silent exclusion = silent fail."""
        import logging
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "STRAGGLER1": _today_str(8),
            "STRAGGLER2": _today_str(20),
        })

        with caplog.at_level(logging.INFO, logger="builders.daily_append"):
            _scan_universe_and_emit_freshness_receipt(
                s3, "test-bucket", lib, expected_tickers=["AAPL"],
            )

        excluded_logs = [r for r in caplog.records if "excluding" in r.message]
        assert excluded_logs, (
            f"Expected an INFO log mentioning 'excluding'; got: "
            f"{[r.message for r in caplog.records]}"
        )
        msg = excluded_logs[0].message
        assert "STRAGGLER1" in msg and "STRAGGLER2" in msg

    def test_scoping_strips_caret_prefix(self):
        """Index tickers (^TNX) carry caret in expected_tickers but arctic
        symbols don't — same lstrip the pre-write check uses."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
        })

        receipt = _scan_universe_and_emit_freshness_receipt(
            s3, "test-bucket", lib,
            expected_tickers=["AAPL", "^VIX", "^TNX"],
        )
        assert receipt["all_fresh"] is True

    def test_no_expected_preserves_legacy_full_scan(self):
        """Backward compat: expected_tickers=None → scan the whole library
        (the pre-PR behavior). Lets unrelated callers keep working."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "STRAGGLER": _today_str(20),  # would be excluded if scoping fired
        })

        with pytest.raises(RuntimeError, match="STRAGGLER"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

    def test_empty_intersection_raises(self):
        """If expected_tickers is non-empty but disjoint from arctic, the
        scan would silently pass (zero symbols → no stale found). Hard-fail
        instead so operators catch the misconfiguration loudly."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "ONLY_IN_ARCTIC": _today_str(0),
        })

        with pytest.raises(RuntimeError, match="zero symbols after expected_tickers"):
            _scan_universe_and_emit_freshness_receipt(
                s3, "test-bucket", lib,
                expected_tickers=["ONLY_IN_EXPECTED"],
            )
        s3.put_object.assert_not_called()
