"""Tests for the shared `load_constituents_for_run_date` helper.

Closes ROADMAP L1397 + 5/23-SF P0 sweep follow-on per
[[feedback_lift_invariants_to_chokepoint_after_second_recurrence]]:
the in-repo chokepoint consumed by BOTH `builders/backfill.py` and
`builders/prune_delisted_tickers.py`.

Pins:
  1. With `run_date`, reads from `weekly/{run_date}/constituents.json`
     directly (NO pointer read).
  2. Without `run_date`, falls back to `latest_weekly.json` pointer.
  3. Empty/missing `tickers` field raises RuntimeError (fail-loud).
  4. Both wrappers (`backfill._load_current_constituents` +
     `prune._load_latest_constituents`) delegate through the helper.
"""
from __future__ import annotations

import io
import json
from unittest.mock import MagicMock

import pytest

from builders._constituents_loader import load_constituents_for_run_date


def _mock_s3_with(get_object_side_effect):
    s3 = MagicMock()
    s3.get_object.side_effect = get_object_side_effect
    return s3


def _make_payload(tickers: list[str]) -> dict:
    return {"Body": io.BytesIO(json.dumps({"tickers": tickers}).encode())}


def test_run_date_provided_reads_direct_partition_no_pointer():
    """When run_date is provided, the read goes directly to
    `weekly/{run_date}/constituents.json` — pointer is NEVER touched."""
    calls = []

    def _get(Bucket, Key):
        calls.append(Key)
        assert Key == "market_data/weekly/2026-05-23/constituents.json", (
            f"unexpected key {Key} — pointer should NOT be read when run_date is set"
        )
        return _make_payload(["AAPL", "BNY", "P", "SN"])

    s3 = _mock_s3_with(_get)
    tickers, weekly_date = load_constituents_for_run_date(
        s3, "alpha-engine-research", run_date="2026-05-23",
    )
    assert tickers == {"AAPL", "BNY", "P", "SN"}
    assert weekly_date == "2026-05-23"
    # Exactly ONE call — the direct partition read.
    assert len(calls) == 1
    assert "latest_weekly.json" not in calls[0]


def test_no_run_date_falls_back_to_pointer():
    """When run_date is None, the pointer is read first, then the
    constituents.json under the pointed-to prefix."""
    calls = []

    def _get(Bucket, Key):
        calls.append(Key)
        if Key == "market_data/latest_weekly.json":
            return {"Body": io.BytesIO(json.dumps({
                "date": "2026-05-16",
                "s3_prefix": "market_data/weekly/2026-05-16/",
            }).encode())}
        if Key == "market_data/weekly/2026-05-16/constituents.json":
            return _make_payload(["AAPL", "MSFT"])
        raise AssertionError(f"unexpected key {Key}")

    s3 = _mock_s3_with(_get)
    tickers, weekly_date = load_constituents_for_run_date(s3, "alpha-engine-research")
    assert tickers == {"AAPL", "MSFT"}
    assert weekly_date == "2026-05-16"
    assert calls == [
        "market_data/latest_weekly.json",
        "market_data/weekly/2026-05-16/constituents.json",
    ]


def test_empty_tickers_raises():
    def _get(Bucket, Key):
        if Key == "market_data/weekly/2026-05-23/constituents.json":
            return {"Body": io.BytesIO(json.dumps({"tickers": []}).encode())}
        raise AssertionError(f"unexpected key {Key}")

    s3 = _mock_s3_with(_get)
    with pytest.raises(RuntimeError, match="no `tickers` field"):
        load_constituents_for_run_date(
            s3, "alpha-engine-research", run_date="2026-05-23",
        )


def test_missing_tickers_field_raises():
    def _get(Bucket, Key):
        return {"Body": io.BytesIO(json.dumps({"date": "2026-05-23"}).encode())}

    s3 = _mock_s3_with(_get)
    with pytest.raises(RuntimeError, match="no `tickers` field"):
        load_constituents_for_run_date(
            s3, "alpha-engine-research", run_date="2026-05-23",
        )


def test_backfill_wrapper_delegates_to_shared_helper():
    """`builders.backfill._load_current_constituents` is now a thin
    wrapper — verify it returns just the ticker set (legacy contract)
    and routes through the shared helper."""
    from builders.backfill import _load_current_constituents

    def _get(Bucket, Key):
        assert Key == "market_data/weekly/2026-05-23/constituents.json"
        return _make_payload(["AAPL", "BNY"])

    s3 = _mock_s3_with(_get)
    tickers = _load_current_constituents(s3, "alpha-engine-research", run_date="2026-05-23")
    # Legacy return shape — set only (NOT tuple) so backfill callers
    # don't need to change.
    assert tickers == {"AAPL", "BNY"}


def test_prune_wrapper_delegates_to_shared_helper_with_run_date():
    """`builders.prune_delisted_tickers._load_latest_constituents` now
    accepts run_date and routes through the shared helper."""
    from builders.prune_delisted_tickers import _load_latest_constituents

    def _get(Bucket, Key):
        assert Key == "market_data/weekly/2026-05-23/constituents.json", (
            "prune should use direct partition read when run_date is set, "
            "NOT the latest_weekly.json pointer (TOCTOU defect class)"
        )
        return _make_payload(["AAPL", "BNY", "P", "SN"])

    s3 = _mock_s3_with(_get)
    tickers, weekly_date = _load_latest_constituents(
        s3, "alpha-engine-research", run_date="2026-05-23",
    )
    assert tickers == {"AAPL", "BNY", "P", "SN"}
    assert weekly_date == "2026-05-23"


def test_prune_wrapper_falls_back_to_pointer_without_run_date():
    """Without run_date (ad-hoc CLI invocation), prune falls back to
    the pointer — same as the pre-lift behavior."""
    from builders.prune_delisted_tickers import _load_latest_constituents

    def _get(Bucket, Key):
        if Key == "market_data/latest_weekly.json":
            return {"Body": io.BytesIO(json.dumps({
                "date": "2026-05-16",
                "s3_prefix": "market_data/weekly/2026-05-16/",
            }).encode())}
        if Key == "market_data/weekly/2026-05-16/constituents.json":
            return _make_payload(["AAPL"])
        raise AssertionError(f"unexpected key {Key}")

    s3 = _mock_s3_with(_get)
    tickers, weekly_date = _load_latest_constituents(s3, "alpha-engine-research")
    assert tickers == {"AAPL"}
    assert weekly_date == "2026-05-16"


def test_prune_function_threads_run_date_to_helper():
    """The public `prune_delisted_tickers()` function now accepts a
    `run_date` parameter and forwards it to the constituents read.
    Regression-pin so a future refactor can't silently drop the
    threading (the L1397 fix's load-bearing semantic)."""
    import inspect
    from builders.prune_delisted_tickers import prune_delisted_tickers
    sig = inspect.signature(prune_delisted_tickers)
    assert "run_date" in sig.parameters, (
        "prune_delisted_tickers() must accept `run_date` parameter — "
        "the L1397 threading fix"
    )
    # Also verify the body forwards run_date to _load_latest_constituents.
    src = inspect.getsource(prune_delisted_tickers)
    assert "_load_latest_constituents(" in src
    assert "run_date=run_date" in src
