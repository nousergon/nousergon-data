"""Unit tests for the L1 observer-mode annotation (config#1277 Option A).

Locks the observer contract:

  * strictly additive — ``Close``/``source`` on every record are never mutated;
  * single-source-per-mode cells record ``single_source_provisional`` (flagged);
  * a null/absent close records ``no_data``;
  * a bounded cross-check set gets a real 2nd-source ``GateDecision`` (agreed /
    quarantined), and a QUARANTINE is recorded + surfaced but the value is NOT
    withheld (priority-coalesce still owns the number);
  * with no ``cross_check_fetch`` (dry-run / no network) the bounded set falls
    back to single-source classification and touches no network;
  * fail-soft — a per-record error degrades that row to un-annotated and is
    counted, never raised.
"""

from __future__ import annotations

from collectors.cross_source_observer import (
    XSOURCE_AGREEMENT_BPS,
    XSOURCE_FLAGGED,
    XSOURCE_PROVENANCE,
    XSOURCE_STATUS,
    annotate_records,
)
from sources.cross_source_gate import GateStatus, SourceClose, evaluate

DATE = "2026-07-10"


def _rec(ticker, close, source="polygon", **extra):
    r = {"ticker": ticker, "Close": close, "source": source}
    r.update(extra)
    return r


class TestSingleSourceClassification:

    def test_single_source_records_provisional_and_flagged(self):
        recs = [_rec("AAPL", 210.0, "polygon"), _rec("MSFT", 500.0, "yfinance")]
        out, summary = annotate_records(recs, DATE, source_mode="auto")
        for r in out:
            assert r[XSOURCE_STATUS] == GateStatus.SINGLE_SOURCE_PROVISIONAL.value
            assert r[XSOURCE_FLAGGED] is True
            assert r[XSOURCE_AGREEMENT_BPS] is None
            assert "single-source PROVISIONAL" in r[XSOURCE_PROVENANCE]
        assert summary["status_counts"] == {"single_source_provisional": 2}
        assert summary["cross_checked"] == 0
        assert summary["errors"] == 0

    def test_null_close_records_no_data(self):
        recs = [_rec("AAPL", None, "polygon")]
        out, summary = annotate_records(recs, DATE, source_mode="polygon_only")
        assert out[0][XSOURCE_STATUS] == GateStatus.NO_DATA.value
        assert summary["status_counts"] == {"no_data": 1}

    def test_close_and_source_never_mutated(self):
        recs = [_rec("AAPL", 210.0, "polygon", VWAP=209.5, Volume=1000)]
        out, _ = annotate_records(recs, DATE)
        assert out[0]["Close"] == 210.0
        assert out[0]["source"] == "polygon"
        assert out[0]["VWAP"] == 209.5  # other columns untouched too


class TestBoundedCrossCheck:

    def test_cross_check_agreed_unflagged_bps_recorded(self):
        def fetch(ticker, date):
            return evaluate(
                ticker, date,
                [SourceClose("polygon", 734.30), SourceClose("yfinance", 734.32)],
            )

        recs = [_rec("SPY", 734.30, "polygon"), _rec("AAPL", 210.0, "polygon")]
        out, summary = annotate_records(
            recs, DATE, cross_check_tickers=("SPY",), cross_check_fetch=fetch
        )
        spy = next(r for r in out if r["ticker"] == "SPY")
        aapl = next(r for r in out if r["ticker"] == "AAPL")
        assert spy[XSOURCE_STATUS] == GateStatus.AGREED.value
        assert spy[XSOURCE_FLAGGED] is False
        assert spy[XSOURCE_AGREEMENT_BPS] is not None and spy[XSOURCE_AGREEMENT_BPS] > 0
        assert spy["Close"] == 734.30  # value untouched by the observer
        # non-bounded ticker stays single-source
        assert aapl[XSOURCE_STATUS] == GateStatus.SINGLE_SOURCE_PROVISIONAL.value
        assert summary["cross_checked"] == 1
        assert summary["status_counts"]["agreed"] == 1

    def test_cross_check_quarantine_recorded_but_value_not_withheld(self):
        def fetch(ticker, date):
            # 100 bps apart — well beyond the ~7.5 bps default tolerance.
            return evaluate(
                ticker, date,
                [SourceClose("polygon", 734.30), SourceClose("yfinance", 741.70)],
            )

        recs = [_rec("SPY", 734.30, "polygon")]
        out, summary = annotate_records(
            recs, DATE, cross_check_tickers=("SPY",), cross_check_fetch=fetch
        )
        spy = out[0]
        assert spy[XSOURCE_STATUS] == GateStatus.QUARANTINED.value
        assert spy[XSOURCE_FLAGGED] is True
        # Observer-only: the priority-coalesced value is preserved, NOT withheld.
        assert spy["Close"] == 734.30
        # ...but the quarantine is surfaced for L4/paging.
        assert len(summary["quarantined"]) == 1
        assert summary["quarantined"][0]["ticker"] == "SPY"
        assert summary["quarantined"][0]["spread_bps"] > 7.5

    def test_no_fetch_callback_falls_back_to_single_source(self):
        # dry-run / no network: bounded ticker is classified single-source, and
        # the (would-be) fetch is never called.
        called = {"n": 0}

        def fetch(ticker, date):  # pragma: no cover — must NOT be called
            called["n"] += 1
            raise AssertionError("network fetch should not run without a callback")

        recs = [_rec("SPY", 734.30, "polygon")]
        out, summary = annotate_records(
            recs, DATE, cross_check_tickers=("SPY",), cross_check_fetch=None
        )
        assert out[0][XSOURCE_STATUS] == GateStatus.SINGLE_SOURCE_PROVISIONAL.value
        assert summary["cross_checked"] == 0
        assert called["n"] == 0


class TestFailSoft:

    def test_fetch_raising_is_counted_not_raised(self):
        def boom(ticker, date):
            raise RuntimeError("vendor exploded")

        recs = [_rec("SPY", 734.30, "polygon"), _rec("AAPL", 210.0, "polygon")]
        out, summary = annotate_records(
            recs, DATE, cross_check_tickers=("SPY",), cross_check_fetch=boom
        )
        # SPY annotation failed (error counted, row left un-annotated) but AAPL
        # still classified; nothing raised.
        spy = next(r for r in out if r["ticker"] == "SPY")
        aapl = next(r for r in out if r["ticker"] == "AAPL")
        assert XSOURCE_STATUS not in spy  # un-annotated
        assert aapl[XSOURCE_STATUS] == GateStatus.SINGLE_SOURCE_PROVISIONAL.value
        assert summary["errors"] == 1
        assert summary["annotated"] == 1

    def test_record_without_ticker_is_skipped(self):
        recs = [{"Close": 1.0, "source": "polygon"}, _rec("AAPL", 210.0)]
        out, summary = annotate_records(recs, DATE)
        assert summary["annotated"] == 1  # only AAPL
