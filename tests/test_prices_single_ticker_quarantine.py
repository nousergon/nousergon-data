"""One guard-refused ticker must not fail the whole EOD D03 stage.

Measured 2026-10-08: TCBI is still listed. It closed at 92.39, and its close is
in ``staging/daily_closes/2026-10-08``. Yet yfinance answered its 10y request
with 1 row on all 3 attempts, against a 2,513-row cache ending 2026-10-07. The
cache was current and the ticker printed today, so it is not stopped printing
(PR #2112). The short-fetch guard refused it, correctly. But D03 went
``partial`` and the collector exited 1, on the 22:15Z run and both heals, so
the 10-08 ArcticDB append and EODReconcile never ran over 1 of 934 tickers.

These tests pin both halves of the fix:
- the guard still refuses: nothing is uploaded and the cache is preserved;
- one refusal within the declared bound is quarantined: the stage is ``ok``
  and the refusal is declared.
Past the bound, for a benchmark symbol, beside any other failure, or once the
cache is too old, the run still fails as before.
"""

from __future__ import annotations

import datetime as dt
import logging

import collectors.prices as prices
import run_units
from tests.test_prices_shadow_recent_listings_i11547 import (  # noqa: F401 - fixtures
    BUCKET, LIVE_KEY, PREFIX, _env, _listing, _parquet, _vendor_answers, live_s3,
)

DAY = "2026-10-08"  # a Thursday
CLOSES_KEY = f"staging/daily_closes/{DAY}.parquet"
#: A production-sized universe, so the fraction bound admits the count bound.
UNIVERSE = [f"U{i:03d}" for i in range(900)]


def _collect(tickers):
    return prices.collect(bucket=BUCKET, tickers=tickers, s3_prefix=PREFIX, reference_date=DAY, batch_size=1)


def _stale(monkeypatch, tickers):
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: list(tickers))
    monkeypatch.setattr(prices, "assert_settled_bar", lambda *a, **k: None)


def _closes(*tickers):
    import pandas as pd
    return _parquet(pd.DataFrame({"Close": [1.0] * len(tickers)}, index=list(tickers)))


def _short_today(live_s3, monkeypatch, short: list[str], ok: list[str] = (), cache_end="2026-10-07"):
    """Each ``short`` ticker: a current 2,513-row cache, it closed today, and the
    vendor returns 1 row. That is the TCBI shape."""
    for t in short:
        live_s3.objects[LIVE_KEY.format(t=t)] = _parquet(_listing(2513, end=cache_end))
    live_s3.objects[CLOSES_KEY] = _closes(*short, *ok)
    answers = {t: _listing(1, end=DAY) for t in short}
    answers.update({t: _listing(2514, end=DAY) for t in ok})
    _vendor_answers(monkeypatch, answers)
    _stale(monkeypatch, [*short, *ok])


def test_the_tcbi_shape_is_quarantined_and_the_stage_completes(monkeypatch, live_s3, caplog):
    _short_today(live_s3, monkeypatch, ["TCBI"], ok=["OK"])

    with caplog.at_level(logging.ERROR, logger=prices.logger.name):
        result = _collect(["TCBI", "OK", *UNIVERSE])

    assert result["status"] == "ok"
    assert result["failed"] == 0
    assert result["failed_short_fetch_refused"] == 0
    assert result[prices.QUARANTINED_RESULT_KEY] == 1
    assert result["quarantined_tickers"] == {"TCBI": prices.FAIL_SHORT_FETCH}
    # The guard still refused it: the short answer was never uploaded.
    assert LIVE_KEY.format(t="TCBI") not in live_s3.puts
    assert LIVE_KEY.format(t="OK") in live_s3.puts
    assert result["written"] == {"OK": 2514}
    # The refusal is declared on the manifest, keyed.
    refused = {g["key"]: g["detail"] for g in result["guards"]
               if g["guard"] == prices.REFUSED_KEYS_GUARD and g["key"]}
    assert refused == {
        LIVE_KEY.format(t="TCBI"): f"TCBI: {prices.FAIL_SHORT_FETCH}; {prices.QUARANTINED}",
    }
    # And it alerts: one ERROR naming the quarantine.
    assert any("QUARANTINED 1 ticker(s)" in r.getMessage() and "TCBI" in r.getMessage()
               for r in caplog.records if r.levelno == logging.ERROR)


def test_quarantine_past_the_count_bound_still_fails(monkeypatch, live_s3):
    short = [f"S{i}" for i in range(prices.QUARANTINE_MAX_TICKERS + 1)]
    _short_today(live_s3, monkeypatch, short)

    result = _collect([*short, *UNIVERSE])

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == len(short)
    assert result[prices.QUARANTINED_RESULT_KEY] == 0
    assert "exceed the bound" in result["quarantine_refused"]
    assert not any(LIVE_KEY.format(t=t) in live_s3.puts for t in short)


def test_a_small_universe_admits_no_quarantine(monkeypatch, live_s3):
    """The fraction bound: one refusal in a ~40-ticker run is 2.5%, not one bad ticker."""
    _short_today(live_s3, monkeypatch, ["TCBI"])

    result = _collect(["TCBI"])

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == 1


def test_a_benchmark_symbol_is_never_quarantined(monkeypatch, live_s3):
    """The I9256 flake on an always-download symbol still fails the run."""
    _short_today(live_s3, monkeypatch, ["GLD"])

    result = _collect(UNIVERSE)

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == 1
    assert result[prices.QUARANTINED_RESULT_KEY] == 0


def test_a_refusal_beside_any_other_failure_still_fails(monkeypatch, live_s3):
    _short_today(live_s3, monkeypatch, ["TCBI"])
    answers = {"TCBI": _listing(1, end=DAY)}
    import pandas as pd

    def _download(*_a, tickers=None, **_k):
        symbol = tickers if isinstance(tickers, str) else tickers[0]
        return pd.DataFrame() if symbol == "EMPTY" else answers[symbol].copy()

    monkeypatch.setattr(prices.yf, "download", _download)
    _stale(monkeypatch, ["TCBI", "EMPTY"])

    result = _collect(["TCBI", "EMPTY", *UNIVERSE])

    assert result["status"] == "partial"
    assert result["failed"] == 2
    assert result[prices.QUARANTINED_RESULT_KEY] == 0


def test_a_persistent_gap_stops_qualifying(monkeypatch, live_s3):
    """The cache ends more than QUARANTINE_MAX_SESSIONS_BEHIND sessions back."""
    _short_today(live_s3, monkeypatch, ["TCBI"], cache_end="2026-10-01")

    result = _collect(["TCBI", *UNIVERSE])

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == 1
    assert "persistent gap" in result["quarantine_refused"]


def test_the_oldest_quarantinable_bar_counts_sessions_not_days():
    # Thu 2026-10-08 -> 3 sessions back is Mon 2026-10-05.
    assert prices.oldest_quarantinable_bar(DAY) == dt.date(2026, 10, 5)


def test_the_bound_is_declared_small():
    assert prices.quarantine_bound(934) == prices.QUARANTINE_MAX_TICKERS == 3
    assert prices.quarantine_bound(40) == 0


def test_d03_declares_the_quarantine_as_a_rejection():
    for mode in ("daily", "phase1"):
        declared = dict(run_units.PHASE_UNITS[(mode, "prices")].rejected_keys)
        assert declared[prices.QUARANTINED_RESULT_KEY] == prices.QUARANTINED
