"""A ticker that has stopped printing must not hard-fail the daily collection.

Measured 2026-10-07: WBD and PSKY last printed 2026-10-05. On the 10-07 EOD
run yfinance answered their 10y requests with 1 and 49 rows against 2,512-row
caches; the short-fetch guard refused both (correctly — the history is
preserved), D03 went ``partial``, the collector exited 1, and the 10-07
ArcticDB append and EODReconcile never ran. Both heal attempts failed the same
way. A refusal for a ticker whose cache already missed a completed session is a
delisting/rename candidate, not a failed refresh; a refusal for a CURRENT cache
is still the I9256 vendor flake and still fails.
"""

from __future__ import annotations

import collectors.prices as prices
from tests.test_prices_shadow_recent_listings_i11547 import (  # noqa: F401 - fixtures
    BUCKET, LIVE_KEY, PREFIX, _env, _listing, _parquet, _vendor_answers, live_s3,
)

DAY = "2026-10-07"  # a Wednesday; the session before it is 2026-10-06


def _collect(tickers):
    return prices.collect(bucket=BUCKET, tickers=tickers, s3_prefix=PREFIX, reference_date=DAY, batch_size=1)


def _stale(monkeypatch, tickers):
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: list(tickers))
    monkeypatch.setattr(prices, "assert_settled_bar", lambda *a, **k: None)


CLOSES_KEY = f"staging/daily_closes/{DAY}.parquet"


def _closes(*tickers):
    import pandas as pd
    return _parquet(pd.DataFrame({"Close": [1.0] * len(tickers)}, index=list(tickers)))


def test_a_ticker_that_stopped_printing_is_skipped_not_failed(monkeypatch, live_s3):
    live_s3.objects[LIVE_KEY.format(t="WBD")] = _parquet(_listing(2512, end="2026-10-05"))
    live_s3.objects[CLOSES_KEY] = _closes("OK")
    _vendor_answers(monkeypatch, {"WBD": _listing(1, end="2026-10-07"), "OK": _listing(2513, end=DAY)})
    _stale(monkeypatch, ["WBD", "OK"])

    result = _collect(["WBD", "OK"])

    assert result["status"] == "ok"
    assert result["failed"] == 0
    assert result[prices.SKIP_STOPPED_PRINTING_RESULT_KEY] == 1
    assert result["stopped_printing_tickers"] == {"WBD": "2026-10-05"}
    assert LIVE_KEY.format(t="WBD") not in live_s3.puts  # history preserved


def test_a_short_answer_for_a_current_cache_still_fails(monkeypatch, live_s3):
    live_s3.objects[LIVE_KEY.format(t="FLAKY")] = _parquet(_listing(2512, end="2026-10-06"))
    _vendor_answers(monkeypatch, {"FLAKY": _listing(1, end=DAY)})
    _stale(monkeypatch, ["FLAKY"])

    result = _collect(["FLAKY"])

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == 1
    assert result[prices.SKIP_STOPPED_PRINTING_RESULT_KEY] == 0
    assert LIVE_KEY.format(t="FLAKY") not in live_s3.puts


def test_a_stale_cache_is_not_excused_when_the_ticker_closed_today(monkeypatch, live_s3):
    """A prior-day miss followed by a vendor flake: the ticker still printed today."""
    live_s3.objects[LIVE_KEY.format(t="MISSED")] = _parquet(_listing(2512, end="2026-10-05"))
    live_s3.objects[CLOSES_KEY] = _closes("MISSED")
    _vendor_answers(monkeypatch, {"MISSED": _listing(1, end=DAY)})
    _stale(monkeypatch, ["MISSED"])

    result = _collect(["MISSED"])

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == 1
    assert result[prices.SKIP_STOPPED_PRINTING_RESULT_KEY] == 0


def test_no_closes_artifact_excuses_nothing(monkeypatch, live_s3):
    live_s3.objects[LIVE_KEY.format(t="WBD")] = _parquet(_listing(2512, end="2026-10-05"))
    _vendor_answers(monkeypatch, {"WBD": _listing(1, end=DAY)})
    _stale(monkeypatch, ["WBD"])

    result = _collect(["WBD"])

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == 1
