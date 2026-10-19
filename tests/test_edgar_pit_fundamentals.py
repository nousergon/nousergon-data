"""`collectors/edgar_pit_fundamentals.py` (alpha-engine-config-I10733).

No network. The three `tests/fixtures/edgar/companyfacts_*.json` files are real
SEC `companyfacts` documents recorded 2026-09-14, trimmed to the tag-map
concepts and to facts filed from 2016-01-01. Assertions against them use
figures from the companies' own filings (Apple FY2023 10-K: net sales
$383,285M, gross margin $169,148M, net income $96,995M, diluted EPS $6.13;
FY2020 diluted EPS $3.28 after the 2020-08-31 4-for-1 split).
"""

from __future__ import annotations

import datetime as dt
import io
import json
import math
import re
from pathlib import Path

import jsonschema
import pandas as pd
import pytest

from collectors import edgar_pit_fundamentals as e

_REPO = Path(__file__).resolve().parents[1]
_FIXTURES = Path(__file__).parent / "fixtures" / "edgar"
_CIKS = {"AAPL": 320193, "NVDA": 1045810, "JPM": 19617}
_NAMES = {"AAPL": "aapl", "NVDA": "nvda", "JPM": "jpm"}


def _o(text: str) -> int:
    return dt.date.fromisoformat(text).toordinal()


_SPLITS = {
    "AAPL": [(_o("2020-08-31"), 0.25)],
    "NVDA": [(_o("2021-07-20"), 0.25), (_o("2024-06-10"), 0.1)],
}


def _doc(ticker: str) -> dict:
    return json.loads((_FIXTURES / f"companyfacts_{_NAMES[ticker]}.json").read_text())


def _events(ticker: str, evaluate_from: str = "2019-01-01") -> list[e.FilingEvent]:
    return e.filing_events(
        e.extract_facts(_doc(ticker)), splits=_SPLITS.get(ticker, ()), evaluate_from=_o(evaluate_from)
    )


def _event_on(events: list[e.FilingEvent], filed: str) -> e.FilingEvent:
    return next(ev for ev in events if ev.filed == _o(filed))


def _fact(
    quantity: str,
    start: str | None,
    end: str,
    value: float,
    filed: str,
    *,
    accession: str = "0000000000-00-000001",
    priority: int = 0,
    unit: str = "USD",
) -> e.Fact:
    return e.Fact(
        quantity=quantity,
        priority=priority,
        start=_o(start) if start else None,
        end=_o(end),
        value=float(value),
        filed=_o(filed),
        accession=accession,
        taxonomy="us-gaap",
        concept=quantity,
        unit=unit,
        form="10-Q",
    )


def _state(*facts: e.Fact) -> e.AsOfState:
    state = e.AsOfState()
    for fact in sorted(facts, key=lambda f: (f.filed, f.accession)):
        state.add(fact, fact.value)
    return state


# -- recorded fixtures --------------------------------------------------------


class TestRecordedFilings:
    def test_apple_fy2023_is_read_on_its_10k_filing_date(self) -> None:
        event = _event_on(_events("AAPL"), "2023-11-03")
        assert event.raw["revenue_ttm_raw"] == 383_285_000_000
        assert event.raw["gross_profit_ttm_raw"] == 169_148_000_000
        assert event.raw["net_income_ttm_raw"] == 96_995_000_000
        assert event.raw["gross_margin"] == pytest.approx(169_148 / 383_285)

    def test_the_filing_before_the_10k_reads_four_quarters(self) -> None:
        """Q4 FY22 (FY22 - 9M) + Q1..Q3 FY23 = 90,146 + 117,154 + 94,836 + 81,797 ($M)."""
        event = _event_on(_events("AAPL"), "2023-08-04")
        assert event.raw["revenue_ttm_raw"] == 383_933_000_000

    def test_eps_growth_spans_the_2020_split_on_one_basis(self) -> None:
        event = _event_on(_events("AAPL"), "2023-11-03")
        assert event.raw["eps_growth_3y"] == pytest.approx((6.13 / 3.28) ** (1 / 3) - 1)

    def test_a_bank_has_no_gross_margin_current_ratio_or_long_term_debt_figure(self) -> None:
        event = _events("JPM")[-1]
        assert event.raw["gross_margin"] is None
        assert event.raw["current_ratio"] is None
        assert event.raw["total_debt_raw"] is None, (
            "JPM tags only short-term borrowings; a borrowings-only total would rank it as "
            "nearly debt-free"
        )
        assert event.raw["roe"] is not None
        assert event.shares_current_basis is not None

    def test_only_periodic_report_forms_are_admitted(self) -> None:
        stats = e.ExtractStats()
        facts = e.extract_facts(_doc("NVDA"), stats)
        assert stats.wrong_form > 0
        assert {f.form for f in facts} <= e.ADMITTED_FORMS
        assert [f.filed for f in facts] == sorted(f.filed for f in facts)


# -- knowledge time and restatement ---------------------------------------------


class TestKnowledgeTime:
    def _quarters(self, q4_value: float, *, restated_on: str | None = None) -> list[e.Fact]:
        facts = [
            _fact("net_income", "2023-01-01", "2023-03-31", 10, "2023-05-01"),
            _fact("net_income", "2023-04-01", "2023-06-30", 10, "2023-08-01"),
            _fact("net_income", "2023-07-01", "2023-09-30", 10, "2023-11-01"),
            _fact("net_income", "2023-10-01", "2023-12-31", q4_value, "2024-02-01"),
        ]
        if restated_on:
            facts.append(
                _fact(
                    "net_income", "2023-10-01", "2023-12-31", 1, restated_on,
                    accession="0000000000-24-000009",
                )
            )
        return facts

    def test_a_restatement_never_reaches_back_before_its_filing_date(self) -> None:
        facts = sorted(self._quarters(10, restated_on="2024-04-15"), key=lambda f: f.filed)
        events = e.filing_events(facts, splits=(), evaluate_from=_o("2023-01-01"))
        before = _event_on(events, "2024-02-01")
        after = _event_on(events, "2024-04-15")
        assert before.raw["net_income_ttm_raw"] == 40
        assert after.raw["net_income_ttm_raw"] == 31

    def test_a_session_reads_a_filing_made_on_its_own_label_and_not_the_label_before(self) -> None:
        facts = self._quarters(10)
        events = e.filing_events(sorted(facts, key=lambda f: f.filed), splits=(), evaluate_from=0)
        closes = pd.DataFrame(
            {"T": [10.0, 10.0]}, index=pd.to_datetime(["2024-01-31", "2024-02-01"])
        )
        frames = e.materialize_sessions(
            {"T": (1, events)}, closes, [dt.date(2024, 1, 31), dt.date(2024, 2, 1)]
        )
        assert frames[dt.date(2024, 1, 31)].loc[0, "latest_filed"] == "2023-11-01"
        assert frames[dt.date(2024, 2, 1)].loc[0, "latest_filed"] == "2024-02-01"
        for label, frame in frames.items():
            assert (frame["latest_filed"] <= label.isoformat()).all()
            assert (frame["knowledge_date"] == label.isoformat()).all()

    def test_a_ticker_with_no_filing_by_the_label_has_no_row(self) -> None:
        events = e.filing_events(
            self._quarters(10), splits=(), evaluate_from=0
        )
        closes = pd.DataFrame({"T": [5.0]}, index=pd.to_datetime(["2023-04-28"]))
        frames = e.materialize_sessions({"T": (1, events)}, closes, [dt.date(2023, 4, 28)])
        assert frames[dt.date(2023, 4, 28)].empty


# -- trailing twelve months ------------------------------------------------------


class TestTrailingTwelveMonths:
    def test_q2_to_q4_are_derived_from_year_to_date_cumulatives(self) -> None:
        state = _state(
            _fact("revenue", "2023-01-01", "2023-12-31", 400, "2024-02-01"),
            _fact("revenue", "2023-01-01", "2023-03-31", 90, "2023-05-01"),
            _fact("revenue", "2023-01-01", "2023-06-30", 190, "2023-08-01"),
            _fact("revenue", "2023-01-01", "2023-09-30", 300, "2023-11-01"),
            _fact("revenue", "2024-01-01", "2024-03-31", 120, "2024-05-01"),
        )
        periods = state.resolved("revenue")
        quarters = e.discrete_quarters(periods)
        assert quarters[_o("2023-06-30")] == 100
        assert quarters[_o("2023-09-30")] == 110
        assert quarters[_o("2023-12-31")] == 100
        assert e.trailing_twelve_months(periods, as_of=_o("2024-05-01")) == (
            100 + 110 + 100 + 120,
            _o("2024-03-31"),
        )

    def test_a_fiscal_year_that_is_the_newest_period_is_used_whole(self) -> None:
        state = _state(_fact("revenue", "2023-01-01", "2023-12-31", 400, "2024-02-01"))
        assert e.trailing_twelve_months(state.resolved("revenue"), as_of=_o("2024-02-01")) == (
            400,
            _o("2023-12-31"),
        )

    def test_a_missing_quarter_is_unmeasured_not_annualised(self) -> None:
        state = _state(
            _fact("revenue", "2023-04-01", "2023-06-30", 100, "2023-08-01"),
            _fact("revenue", "2023-10-01", "2023-12-31", 100, "2024-02-01"),
            _fact("revenue", "2024-01-01", "2024-03-31", 100, "2024-05-01"),
        )
        assert e.trailing_twelve_months(state.resolved("revenue"), as_of=_o("2024-05-01")) is None

    def test_a_filer_that_stopped_filing_is_not_frozen_onto_later_dates(self) -> None:
        state = _state(_fact("revenue", "2020-01-01", "2020-12-31", 400, "2021-02-01"))
        assert e.trailing_twelve_months(state.resolved("revenue"), as_of=_o("2022-06-01")) is None


# -- fields and v1 normalisation ------------------------------------------------


#: Shares in the synthetic filings are in millions, so they clear the placeholder
#: floor (`MIN_SHARE_COUNT`); closes are divided by the same factor, keeping every
#: market cap in the units the asserts read.
_M = 1_000_000


def _balance_sheet_event(**overrides: float) -> dict:
    facts = [
        _fact("net_income", "2023-01-01", "2023-12-31", overrides.get("income", 100), "2024-02-01"),
        _fact("revenue", "2023-01-01", "2023-12-31", 1000, "2024-02-01"),
        _fact("gross_profit", "2023-01-01", "2023-12-31", 400, "2024-02-01"),
        _fact("equity", None, "2023-12-31", overrides.get("equity", 500), "2024-02-01"),
        _fact("assets_current", None, "2023-12-31", 300, "2024-02-01"),
        _fact("liabilities_current", None, "2023-12-31", 100, "2024-02-01"),
        _fact("assets_total", None, "2023-12-31", 2000, "2024-02-01"),
        _fact("liabilities_total", None, "2023-12-31", 1500, "2024-02-01"),
        _fact("operating_cash_flow", "2023-01-01", "2023-12-31", 150, "2024-02-01"),
        _fact("capex", "2023-01-01", "2023-12-31", 50, "2024-02-01"),
        _fact("shares_outstanding", None, "2024-01-20", 100 * _M, "2024-02-01", unit="shares"),
    ]
    if "long_term_debt" in overrides:
        facts.append(
            _fact("long_term_debt_total", None, "2023-12-31", overrides["long_term_debt"], "2024-02-01")
        )
    if "short_term" in overrides:
        facts.append(
            _fact("short_term_borrowings", None, "2023-12-31", overrides["short_term"], "2024-02-01")
        )
    splits = [(_o("2024-06-01"), 0.5)] if overrides.get("split") else []
    events = e.filing_events(facts, splits=splits, evaluate_from=0)
    return {"event": events[-1], "splits": splits}


class TestFields:
    def test_v1_normalisations_and_the_price_fields(self) -> None:
        event = _balance_sheet_event(long_term_debt=400, short_term=100)["event"]
        row = e.session_row(ticker="T", cik=1, label=dt.date(2024, 3, 1), event=event, close=30.0 / _M)
        assert row["roe"] == pytest.approx(0.2)
        assert row["debt_to_equity"] == pytest.approx((500 / 500) / 2)
        assert row["gross_margin"] == pytest.approx(0.4)
        assert row["current_ratio"] == pytest.approx(3.0 / 3)
        assert row["market_cap_raw"] == pytest.approx(3000.0)
        assert row["pe_ratio"] == pytest.approx(3000 / 100 / 30)
        assert row["pb_ratio"] == pytest.approx(3000 / 500 / 5)
        assert row["fcf_yield"] == pytest.approx(100 / 3000)
        assert row["payout_ratio"] == 0.0, (
            "operating cash flow was reported and no dividend was tagged: paid nothing"
        )

    def test_total_assets_and_liabilities_ride_the_newest_balance_sheet(self) -> None:
        event = _balance_sheet_event()["event"]
        row = e.session_row(ticker="T", cik=1, label=dt.date(2024, 3, 1), event=event, close=30.0)
        assert row["assets_total_raw"] == pytest.approx(2000.0)
        assert row["liabilities_total_raw"] == pytest.approx(1500.0)

    def test_the_v1_clips_bound_every_field(self) -> None:
        event = _balance_sheet_event(income=100_000, long_term_debt=400)["event"]
        row = e.session_row(ticker="T", cik=1, label=dt.date(2024, 3, 1), event=event, close=0.001 / _M)
        assert row["roe"] == 1.0
        assert -3.0 <= row["pe_ratio"] <= 3.0

    def test_negative_equity_leaves_roe_and_leverage_unmeasured(self) -> None:
        event = _balance_sheet_event(equity=-50, long_term_debt=400)["event"]
        assert event.raw["roe"] is None
        assert event.raw["debt_to_equity"] is None

    def test_short_term_borrowings_alone_are_not_a_total_debt(self) -> None:
        assert _balance_sheet_event(short_term=100)["event"].raw["total_debt_raw"] is None

    def test_market_cap_puts_reported_shares_on_the_close_basis(self) -> None:
        """Shares reported 2024-01-20; a 2-for-1 split executes 2024-06-01; Close is current basis."""
        built = _balance_sheet_event(long_term_debt=400, split=True)
        assert built["event"].shares_current_basis == pytest.approx(200 * _M)
        row = e.session_row(
            ticker="T", cik=1, label=dt.date(2024, 3, 1), event=built["event"], close=15.0 / _M
        )
        assert row["market_cap_raw"] == pytest.approx(3000.0)

    def test_eps_filed_before_a_split_and_never_restated_is_rebased(self) -> None:
        facts = [
            _fact("eps_diluted", "2019-01-01", "2019-12-31", 4.0, "2020-02-01", unit="USD/shares"),
            _fact("eps_diluted", "2022-01-01", "2022-12-31", 2.0, "2023-02-01", unit="USD/shares"),
        ]
        events = e.filing_events(facts, splits=[(_o("2021-06-01"), 0.5)], evaluate_from=0)
        assert events[-1].raw["eps_growth_3y"] == pytest.approx(0.0)

    def test_a_growth_rate_through_a_loss_is_unmeasured(self) -> None:
        state = _state(
            _fact("revenue", "2019-01-01", "2019-12-31", -5, "2020-02-01"),
            _fact("revenue", "2022-01-01", "2022-12-31", 100, "2023-02-01"),
        )
        assert e.annual_cagr(state.resolved("revenue"), 3, as_of=_o("2023-02-01")) is None


# -- gates -----------------------------------------------------------------------


def _frame(tickers: list[str], pe: list[float]) -> pd.DataFrame:
    rows = []
    for ticker, value in zip(tickers, pe, strict=True):
        rows.append({"ticker": ticker, "market_cap_raw": value * 10.0, "net_income_ttm_raw": 10.0})
    return pd.DataFrame(rows)


class TestGates:
    def test_coverage_below_the_consumer_floor_refuses(self) -> None:
        frame = pd.DataFrame({"ticker": [f"T{i}" for i in range(89)]})
        with pytest.raises(e.GateFailed, match="below the 0.90 floor"):
            e.assert_coverage(frame, [f"T{i}" for i in range(100)], label=dt.date(2026, 9, 11))
        frame = pd.DataFrame({"ticker": [f"T{i}" for i in range(90)]})
        assert e.assert_coverage(frame, [f"T{i}" for i in range(100)], label=dt.date(2026, 9, 11))[
            "ratio"
        ] == pytest.approx(0.9)

    def test_the_cross_check_passes_agreement_and_refuses_a_basis_error(self) -> None:
        tickers = [f"T{i}" for i in range(150)]
        pe = [5.0 + 0.5 * i % 60 for i in range(150)]
        v1 = pd.DataFrame({"ticker": tickers, "pe_ratio": [p / 30.0 for p in pe]})
        reading = e.crosscheck_against_v1(_frame(tickers, pe), v1)
        assert reading["spearman"] == pytest.approx(1.0)
        assert reading["median_abs_log_ratio"] == pytest.approx(0.0, abs=1e-12)
        with pytest.raises(e.GateFailed, match="disagrees"):
            e.crosscheck_against_v1(_frame(tickers, [p / 2.0 for p in pe]), v1)

    def test_the_cross_check_needs_enough_names(self) -> None:
        tickers = [f"T{i}" for i in range(20)]
        v1 = pd.DataFrame({"ticker": tickers, "pe_ratio": [0.5] * 20})
        with pytest.raises(e.GateFailed, match="under the 100-name minimum"):
            e.crosscheck_against_v1(_frame(tickers, [15.0] * 20), v1)


# -- share counts (alpha-engine-config-I12426) --------------------------------------
#
# Each failure shape below is the one measured in companyfacts for the named
# ticker, with the real reported values and filing dates.

_DEI = ("dei", "EntityCommonStockSharesOutstanding", 0)
_CSO = ("us-gaap", "CommonStockSharesOutstanding", 1)
_WAD = ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding", 0)


def _share(tag: tuple[str, str, int], end: str, value: float, filed: str, *, start: str | None = None) -> e.Fact:
    taxonomy, concept, priority = tag
    return e.Fact(
        quantity="diluted_weighted_shares" if tag is _WAD else "shares_outstanding",
        priority=priority,
        start=_o(start) if start else None,
        end=_o(end),
        value=float(value),
        filed=_o(filed),
        accession=f"acc-{filed}",
        taxonomy=taxonomy,
        concept=concept,
        unit="shares",
        form="10-Q",
    )


def _shares_by_filing(facts: list[e.Fact], splits=()) -> tuple[dict[str, float | None], list]:
    rejected: list = []
    ordered = sorted(facts, key=lambda f: (f.filed, f.accession))
    events = e.filing_events(ordered, splits=splits, evaluate_from=0, share_rejections=rejected)
    return {dt.date.fromordinal(ev.filed).isoformat(): ev.shares_current_basis for ev in events}, rejected


class TestShareCounts:
    def test_hxl_a_cover_count_a_million_times_too_large_is_refused_twice(self) -> None:
        shares, rejected = _shares_by_filing(
            [
                _share(_DEI, "2024-04-19", 83_114_360, "2024-04-22"),
                _share(_DEI, "2024-07-15", 81_748_036_000_000, "2024-07-18"),
                _share(_DEI, "2024-10-18", 81_002_128_000_000, "2024-10-21"),
                _share(_DEI, "2025-01-31", 81_131_630, "2025-02-05"),
            ]
        )
        assert shares["2024-07-18"] == shares["2024-10-21"] == 83_114_360
        assert shares["2025-02-05"] == 81_131_630
        assert [(r.shift, r.reason) for r in rejected] == [(6, "scale"), (6, "scale")]

    def test_hl_a_cover_count_in_thousands_is_refused_on_consecutive_filings(self) -> None:
        shares, rejected = _shares_by_filing(
            [
                _share(_DEI, "2024-02-09", 625_117_775, "2024-02-15"),
                _share(_DEI, "2024-05-03", 626_290_204_000, "2024-05-09"),
                _share(_DEI, "2024-08-02", 629_715_867_000, "2024-08-07"),
                _share(_DEI, "2024-11-01", 637_015_400, "2024-11-07"),
            ]
        )
        assert shares["2024-05-09"] == shares["2024-08-07"] == 625_117_775
        assert shares["2024-11-07"] == 637_015_400
        assert {r.shift for r in rejected} == {3}

    def test_ntap_a_cover_count_a_tenth_too_small_falls_to_the_balance_sheet(self) -> None:
        shares, rejected = _shares_by_filing(
            [
                _share(_DEI, "2023-11-28", 206_031_047, "2023-11-30"),
                _share(_CSO, "2024-01-26", 206_000_000, "2024-02-29"),
                _share(_DEI, "2024-02-23", 20_637_666, "2024-02-29"),
            ]
        )
        assert shares["2024-02-29"] == 206_000_000
        assert [(r.concept, r.shift) for r in rejected] == [("EntityCommonStockSharesOutstanding", -1)]

    def test_echo_a_weighted_average_in_thousands_is_refused_and_a_merger_is_not(self) -> None:
        """EchoStar tags no undimensioned cover count, so the weighted average is its only
        share tag. The DISH merger more than tripled it, which is a real change; the 10-Qs
        from 2024-05 carry it a thousand times too small, which is not."""
        shares, rejected = _shares_by_filing(
            [
                _share(_WAD, "2023-09-30", 83_850_000, "2023-11-06", start="2023-07-01"),
                _share(_WAD, "2023-12-31", 270_842_000, "2024-02-29", start="2023-01-01"),
                _share(_WAD, "2023-03-31", 307_410, "2024-05-08", start="2023-01-01"),
                _share(_WAD, "2024-03-31", 271_519, "2024-05-08", start="2024-01-01"),
                _share(_WAD, "2024-06-30", 271_592, "2024-08-09", start="2024-04-01"),
                _share(_WAD, "2024-12-31", 274_079_000, "2025-02-27", start="2024-01-01"),
            ]
        )
        assert shares["2024-02-29"] == 270_842_000
        assert shares["2024-05-08"] == shares["2024-08-09"] == 270_842_000
        assert shares["2025-02-27"] == 274_079_000
        assert len(rejected) == 3 and {r.shift for r in rejected} == {-3}

    def test_ahr_an_offering_that_doubles_the_count_is_kept(self) -> None:
        """AHR's February 2024 listing doubled its shares. Not a decimal shift: kept, and
        under the continuity gate's 3x. Its first post-listing cover count (2024-03-22)
        is half the later ones, which no periodic filing at that date can contradict."""
        shares, rejected = _shares_by_filing(
            [
                _share(_WAD, "2023-09-30", 66_048_991, "2023-11-13", start="2023-07-01"),
                _share(_DEI, "2024-03-15", 65_372_222, "2024-03-22"),
                _share(_DEI, "2024-08-07", 131_647_015, "2024-08-09"),
            ]
        )
        assert shares == {"2023-11-13": 66_048_991, "2024-03-22": 65_372_222, "2024-08-09": 131_647_015}
        assert rejected == []

    def test_a_shell_s_founder_shares_are_not_a_share_count(self) -> None:
        shares, rejected = _shares_by_filing(
            [
                _share(_DEI, "2023-05-11", 1000, "2023-05-16"),
                _share(_DEI, "2023-08-03", 56_566_214, "2023-08-08"),
            ]
        )
        assert shares == {"2023-05-16": None, "2023-08-08": 56_566_214}
        assert [r.reason for r in rejected] == ["placeholder"]

    def test_a_level_change_both_tags_report_is_kept(self) -> None:
        """A 10-for-1 the split set does not know: the cover count and the weighted
        average move together, which a mis-scaled tag never does."""
        shares, rejected = _shares_by_filing(
            [
                _share(_DEI, "2024-04-20", 50_000_000, "2024-04-25"),
                _share(_DEI, "2024-07-20", 500_000_000, "2024-07-25"),
                _share(_WAD, "2024-06-30", 498_000_000, "2024-07-25", start="2024-04-01"),
            ]
        )
        assert shares["2024-07-25"] == 500_000_000
        assert rejected == []

    def test_a_restated_weighted_average_is_not_rebased_twice(self) -> None:
        """GOOG: the 10-Q filed after the 2022-07-18 20-for-1 split reports Q2's weighted
        average already restated; rebasing it from the 06-30 period end made it 20x."""
        split = [(_o("2022-07-18"), 1 / 20)]
        shares, _ = _shares_by_filing(
            [
                _share(_WAD, "2022-03-31", 664_000_000, "2022-04-27", start="2022-01-01"),
                _share(_WAD, "2022-06-30", 13_078_000_000, "2022-07-27", start="2022-04-01"),
            ],
            splits=split,
        )
        assert shares["2022-04-27"] == pytest.approx(13_280_000_000)
        assert shares["2022-07-27"] == pytest.approx(13_078_000_000)

    def test_a_cover_count_dated_before_a_split_takes_it(self) -> None:
        split = [(_o("2024-06-10"), 0.1)]
        shares, _ = _shares_by_filing(
            [_share(_DEI, "2024-06-05", 2_460_000_000, "2024-06-12")], splits=split
        )
        assert shares["2024-06-12"] == pytest.approx(24_600_000_000)

    def test_the_scale_shift_reading(self) -> None:
        assert e.scale_shift(1000.0, 1.0) == 3
        assert e.scale_shift(0.1, 1.0) == -1
        assert e.scale_shift(3.23, 1.0) == 0
        assert e.scale_shift(50.0, 1.0) == 0
        assert e.scale_shift(1.0, 0.0) == 0


def _session(rows: list[tuple[str, float | None, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": [t for t, _, _ in rows],
            "shares_outstanding_raw": [s for _, s, _ in rows],
            "market_cap_raw": [None if s is None else s * 10.0 for _, s, _ in rows],
            "latest_filed": [f for _, _, f in rows],
        }
    )


class TestShareCountContinuity:
    def test_a_jump_in_any_historical_session_refuses_the_run(self) -> None:
        sessions = {
            dt.date(2024, 7, 17): _session([("HXL", 83e6, "2024-04-22"), ("A", 1e8, "2024-06-01")]),
            dt.date(2024, 7, 18): _session([("HXL", 8.17e13, "2024-07-18"), ("A", 1e8, "2024-06-01")]),
            dt.date(2026, 10, 16): _session([("HXL", 76e6, "2026-07-29"), ("A", 1e8, "2024-06-01")]),
        }
        with pytest.raises(e.GateFailed, match="share-count jump") as caught:
            e.assert_share_count_continuity(sessions, {})
        assert "'HXL'" in str(caught.value) and "2024-07-18" in str(caught.value)

    def test_a_split_between_the_sessions_exempts_the_pair(self) -> None:
        sessions = {
            dt.date(2024, 6, 7): _session([("NVDA", 2.46e9, "2024-05-29")]),
            dt.date(2024, 6, 10): _session([("NVDA", 24.6e9, "2024-05-29")]),
        }
        reading = e.assert_share_count_continuity(sessions, {"NVDA": [(_o("2024-06-10"), 0.1)]})
        assert reading["split_exempt"] == 1 and reading["violations"] == 0

    def test_a_declared_jump_passes_and_is_reported(self) -> None:
        sessions = {
            dt.date(2024, 2, 28): _session([("ECHO", 83.85e6, "2023-11-06")]),
            dt.date(2024, 2, 29): _session([("ECHO", 270.842e6, "2024-02-29")]),
        }
        reading = e.assert_share_count_continuity(sessions, {})
        assert [j["ticker"] for j in reading["declared"]] == ["ECHO"]

    def test_a_name_with_no_market_cap_is_not_compared(self) -> None:
        sessions = {
            dt.date(2024, 2, 6): _session([("AHR", 66e6, "2023-11-13")]).assign(market_cap_raw=None),
            dt.date(2024, 2, 7): _session([("AHR", 200e6, "2023-11-13")]),
        }
        assert e.assert_share_count_continuity(sessions, {})["adjacent_pairs"] == 0


class TestCrosscheckHistory:
    def test_every_comparable_snapshot_is_checked_and_a_flat_one_is_counted(self) -> None:
        tickers = [f"T{i}" for i in range(150)]
        pe = [5.0 + 0.5 * i % 60 for i in range(150)]
        good = pd.DataFrame({"ticker": tickers, "pe_ratio": [p / 30.0 for p in pe]})
        flat = pd.DataFrame({"ticker": tickers, "pe_ratio": [0.0] * 150})
        sessions = {dt.date(2026, 4, 1): _frame(tickers, pe), dt.date(2026, 6, 1): _frame(tickers, pe)}
        reading = e.crosscheck_history(sessions, {dt.date(2026, 4, 1): flat, dt.date(2026, 6, 1): good})
        assert reading["snapshots_compared"] == 1 and reading["snapshots_not_comparable"] == 1
        sessions[dt.date(2026, 6, 1)] = _frame(tickers, [p / 2.0 for p in pe])
        with pytest.raises(e.GateFailed, match="v1 snapshot 2026-06-01"):
            e.crosscheck_history(sessions, {dt.date(2026, 6, 1): good})


# -- the run ----------------------------------------------------------------------


class _Facts:
    name = "fixture"

    def get(self, cik: int) -> dict | None:
        return next((_doc(t) for t, c in _CIKS.items() if c == cik), None)


class _Store:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None:
        self.objects[key] = body


def _closes() -> pd.DataFrame:
    index = pd.to_datetime(["2026-09-09", "2026-09-10", "2026-09-11"])
    return pd.DataFrame({"AAPL": [230.0] * 3, "NVDA": [170.0] * 3, "JPM": [290.0] * 3}, index=index)


def _run(store: _Store, **kwargs):
    return e.run(
        store=store,
        tickers=list(_CIKS),
        cik_map=_CIKS,
        facts_source=_Facts(),
        closes=_closes(),
        splits=_SPLITS,
        start=dt.date(2026, 9, 9),
        end=dt.date(2026, 9, 11),
        eligible=list(_CIKS),
        run_date=dt.date(2026, 9, 14),
        run_id="fixture",
        **kwargs,
    )


class TestRun:
    def test_writes_every_session_to_the_contract_and_records_the_run(self, tmp_path) -> None:
        store = _Store()
        summary = _run(store, crosscheck_disabled_reason="fixture has three names", work_dir=str(tmp_path))
        schema = json.loads((_REPO / "contracts" / "edgar_pit_fundamentals_session.schema.json").read_text())
        assert summary["sessions_written"] == 3
        assert summary["crosscheck"] == {"disabled": "fixture has three names"}
        for label in ("2026-09-09", "2026-09-10", "2026-09-11"):
            frame = pd.read_parquet(io.BytesIO(store.objects[e.session_key(dt.date.fromisoformat(label))]))
            assert tuple(frame.columns) == e.SESSION_COLUMNS
            assert sorted(frame["ticker"]) == ["AAPL", "JPM", "NVDA"]
            for record in frame.to_dict("records"):
                clean = {
                    k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in record.items()
                }
                clean["cik"], clean["schema_version"] = int(clean["cik"]), int(clean["schema_version"])
                jsonschema.validate(clean, schema)
                assert clean["latest_filed"] <= label
        assert e.facts_key(dt.date(2026, 9, 14), "fixture") in store.objects
        recorded = json.loads(store.objects[e.run_summary_key(dt.date(2026, 9, 14), "fixture")])
        assert recorded["coverage_newest_session"]["ratio"] == 1.0

    def test_writes_a_fixed_key_freshness_sentinel_every_run(self, tmp_path) -> None:
        # alpha-engine-config-I10750: the freshness monitor cannot probe a
        # variable-run_id key, so every successful run also writes a fixed
        # `runs/latest.json` pointer — same shape as `run_summary_key`.
        store = _Store()
        _run(store, crosscheck_disabled_reason="fixture", work_dir=str(tmp_path))
        assert e.RUN_LATEST_KEY in store.objects
        dated = json.loads(store.objects[e.run_summary_key(dt.date(2026, 9, 14), "fixture")])
        latest = json.loads(store.objects[e.RUN_LATEST_KEY])
        assert latest == dated

    def test_a_second_run_writes_only_what_is_missing(self, tmp_path) -> None:
        store = _Store()
        _run(store, crosscheck_disabled_reason="fixture", work_dir=str(tmp_path))
        again = _run(store, crosscheck_disabled_reason="fixture", work_dir=str(tmp_path))
        assert again["sessions_written"] == 0
        assert again["sessions_skipped_existing"] == 3

    def test_a_failed_gate_writes_nothing(self, tmp_path) -> None:
        store = _Store()
        with pytest.raises(e.GateFailed):
            e.run(
                store=store,
                tickers=list(_CIKS),
                cik_map=_CIKS,
                facts_source=_Facts(),
                closes=_closes(),
                splits=_SPLITS,
                start=dt.date(2026, 9, 9),
                end=dt.date(2026, 9, 11),
                eligible=[*_CIKS, *[f"X{i}" for i in range(10)]],
                crosscheck_disabled_reason="fixture",
                work_dir=str(tmp_path),
            )
        assert store.objects == {}

    def test_the_cross_check_is_on_unless_a_reason_is_given(self, tmp_path) -> None:
        with pytest.raises(e.GateFailed, match="no v1 fundamentals snapshot"):
            _run(_Store(), work_dir=str(tmp_path))


# -- SEC access -------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class _Response:
    def __init__(self, status: int, payload: object = None) -> None:
        self.status_code = status
        self._payload = payload
        self.headers: dict[str, str] = {}

    def json(self) -> object:
        return self._payload


class _Session:
    def __init__(self, statuses: list[int]) -> None:
        self.statuses = list(statuses)
        self.headers_seen: list[dict] = []

    def get(self, url, headers, stream, timeout):  # noqa: ARG002 - requests.Session shape
        self.headers_seen.append(headers)
        return _Response(self.statuses.pop(0) if self.statuses else 200, {"ok": True})


class TestSecClient:
    def test_requests_are_paced_under_secs_ceiling_and_declare_the_user_agent(self) -> None:
        clock, session = _Clock(), _Session([])
        client = e.SecClient(user_agent="Example research contact@example.com", session=session, clock=clock)
        for _ in range(20):
            client.get_json("https://data.sec.gov/x")
        assert clock.now >= 19 / e.SEC_TARGET_REQUESTS_PER_SECOND - 1e-9
        assert 20 / clock.now <= e.SEC_MAX_REQUESTS_PER_SECOND
        assert all(h["User-Agent"] == "Example research contact@example.com" for h in session.headers_seen)

    def test_an_empty_user_agent_or_a_rate_over_ten_is_refused(self) -> None:
        with pytest.raises(ValueError, match="User-Agent"):
            e.SecClient(user_agent="  ", session=_Session([]), clock=_Clock())
        with pytest.raises(ValueError, match="fair-access"):
            e.SecClient(user_agent="x", session=_Session([]), clock=_Clock(), requests_per_second=11)

    def test_a_throttled_response_is_retried_then_a_persistent_one_raises(self) -> None:
        clock = _Clock()
        client = e.SecClient(user_agent="x", session=_Session([429, 200]), clock=clock)
        assert client.get_json("https://data.sec.gov/x") == {"ok": True}
        assert client.request_count == 2
        failing = e.SecClient(user_agent="x", session=_Session([503] * 5), clock=_Clock())
        with pytest.raises(e.SecFetchError) as raised:
            failing.get("https://data.sec.gov/x")
        assert raised.value.status == 503

    def test_the_ticker_map_normalises_class_suffixes(self) -> None:
        mapping = e.ticker_cik_map(
            {"0": {"ticker": "BRK.B", "cik_str": 1067983}, "1": {"ticker": "AAPL", "cik_str": 320193}}
        )
        assert mapping == {"BRK-B": 1067983, "AAPL": 320193}


# -- declarations ------------------------------------------------------------------


def test_the_contract_schema_matches_the_written_columns() -> None:
    schema = json.loads((_REPO / "contracts" / "edgar_pit_fundamentals_session.schema.json").read_text())
    assert set(schema["required"]) == set(e.SESSION_COLUMNS)
    assert set(schema["properties"]) == set(e.SESSION_COLUMNS)
    assert schema["properties"]["schema_version"] == {"const": e.SCHEMA_VERSION}


def test_every_tag_map_concept_is_documented_in_schema_md() -> None:
    text = (_REPO / "features" / "SCHEMA.md").read_text()
    section = text[text.index("## 2c.") : text.index("## 3. Field catalog")]
    for quantity in e.TAG_MAP:
        for _, concept in quantity.concepts:
            assert f"`{concept}`" in section, f"{concept} ({quantity.name}) is not in SCHEMA.md §2c"


def test_the_data_spot_dispatcher_exposes_both_workloads() -> None:
    source = (_REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py").read_text()
    pattern = re.compile(r"^[a-z][a-z-]{0,63}$")
    for name, command in (
        ("edgar-pit-fundamentals-backfill", "python -m collectors.edgar_pit_fundamentals backfill"),
        ("edgar-pit-fundamentals-daily", "python -m collectors.edgar_pit_fundamentals incremental"),
    ):
        assert pattern.match(name)
        assert f'"{name}"' in source
        assert command in source


def test_the_session_key_shape_is_literal() -> None:
    assert (
        e.session_key(dt.date(2026, 9, 11)) == "fundamentals_pit/edgar/v1/sessions/2026-09-11.parquet"
    )


# ── split source (alpha-engine-config-I12418) ────────────────────────────────




def _rows(*items: tuple[str, str, float, float]) -> list[dict]:
    return [
        {"ticker": t, "execution_date": d, "split_from": f, "split_to": to} for t, d, f, to in items
    ]


class TestSplitSource:
    def test_vendor_history_fills_the_splits_the_registry_never_saw(self) -> None:
        registry = {"NVDA": [(_o("2026-03-02"), 0.5)]}
        vendor = e.vendor_split_events(
            _rows(("NVDA", "2024-06-10", 1, 10), ("NVDA", "2021-07-20", 1, 4), ("CMG", "2024-06-26", 1, 50))
        )
        merged, stats = e.merge_split_events(registry, vendor)
        assert merged["NVDA"] == [(_o("2021-07-20"), 0.25), (_o("2024-06-10"), 0.1), (_o("2026-03-02"), 0.5)]
        assert merged["CMG"] == [(_o("2024-06-26"), 0.02)]
        assert stats["vendor_added"] == 3
        # The basis error the issue measured: shares reported before CMG's split
        # are now divided by 0.02, i.e. multiplied by 50.
        assert e.split_factor_after(merged["CMG"], _o("2024-03-01")) == pytest.approx(0.02)

    def test_the_registry_copy_of_a_split_wins_and_a_disagreement_is_counted(self) -> None:
        registry = {"AAA": [(_o("2026-02-02"), 0.5)], "BBB": [(_o("2026-02-02"), 0.5)]}
        vendor = e.vendor_split_events(
            _rows(("AAA", "2026-02-03", 1, 2), ("BBB", "2026-02-02", 2, 1))
        )
        merged, stats = e.merge_split_events(registry, vendor)
        assert merged == registry
        assert stats["duplicates"] == 1 and stats["factor_conflicts"] == 1

    def test_a_split_repeated_in_the_vendor_feed_is_applied_once(self) -> None:
        vendor = e.vendor_split_events(_rows(("CMG", "2024-06-26", 1, 50), ("CMG", "2024-06-26", 1, 50)))
        merged, stats = e.merge_split_events({}, vendor)
        assert merged["CMG"] == [(_o("2024-06-26"), 0.02)]
        assert stats["vendor_added"] == 1 and stats["duplicates"] == 1

    def test_distributions_in_the_splits_feed_are_not_share_splits(self) -> None:
        stats: dict[str, int] = {}
        vendor = e.vendor_split_events(
            _rows(
                ("MMM", "2024-04-01", 1000, 1196),  # spin-off style record
                ("VZ", "2022-01-03", 1, 1.0155),  # fractional distribution
                ("CBSH", "2023-12-01", 20, 21),  # 5% stock dividend: a real share change
                ("WRB", "2024-06-25", 2, 3),  # 3-for-2
                ("SIRI", "2024-09-10", 10, 1),  # 1-for-10 reverse
                ("ODFL", "2024-03-28", 1, 2.0),  # 2-for-1 written as a float
                ("APG", "2024-08-01", 1, 1.5),  # 3-for-2 written unreduced
                ("NRWRF", "2026-06-01", 20.625, 21.625),  # fractional distribution
            ),
            stats,
        )
        assert sorted(vendor) == ["APG", "CBSH", "ODFL", "SIRI", "WRB"]
        assert stats["vendor_not_share_split"] == 3
        assert vendor["SIRI"] == [(_o("2024-09-10"), 10.0)]

    def test_tickers_are_normalized_and_bad_ratios_dropped(self) -> None:
        vendor = e.vendor_split_events(_rows(("BRK.B", "2010-01-21", 1, 50), ("ZZZ", "2020-01-02", 0, 1)))
        assert list(vendor) == ["BRK-B"]

    def test_a_missing_canary_split_refuses_the_run(self) -> None:
        with pytest.raises(e.GateFailed, match="NVDA"):
            e.check_split_canaries({"NVDA": [(_o("2026-03-02"), 0.5)]}, ["NVDA"], window_start=dt.date(2015, 1, 1))

    def test_canaries_outside_the_universe_or_window_are_not_checked(self) -> None:
        assert e.check_split_canaries({}, ["JPM"], window_start=dt.date(2015, 1, 1)) == []
        assert e.check_split_canaries({}, ["AAPL"], window_start=dt.date(2021, 1, 1)) == []
        found = {"AAPL": [(_o("2020-08-31"), 0.25)]}
        assert e.check_split_canaries(found, ["AAPL"], window_start=dt.date(2015, 1, 1)) == ["AAPL"]

    def test_resolve_reaches_back_and_lets_a_vendor_error_stop_the_run(self, monkeypatch) -> None:
        monkeypatch.setattr(e, "registry_split_events", lambda s3, bucket: {})
        seen: list[tuple[str, str]] = []

        def fetch(start: str, end: str) -> list[dict]:
            seen.append((start, end))
            return _rows(("NVDA", "2024-06-10", 1, 10))

        merged, summary = e.resolve_split_events(
            None, "b", universe=["NVDA"], start=dt.date(2022, 1, 3), end=dt.date(2026, 10, 16), fetch_vendor=fetch
        )
        assert seen == [("2015-01-01", "2026-10-16")]
        assert merged["NVDA"] == [(_o("2024-06-10"), 0.1)]
        assert summary["vendor_added"] == 1 and summary["canaries_checked"] == 1

        def broken(start: str, end: str) -> list[dict]:
            raise RuntimeError("polygon 403")

        with pytest.raises(RuntimeError, match="403"):
            e.resolve_split_events(
                None, "b", universe=["NVDA"], start=dt.date(2022, 1, 3), end=dt.date(2026, 10, 16), fetch_vendor=broken
            )


def test_the_rebuild_workload_overwrites_from_2022() -> None:
    source = (_REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py").read_text()
    assert '"edgar-pit-fundamentals-rebuild"' in source
    assert "python -m collectors.edgar_pit_fundamentals backfill --start 2022-01-03 --overwrite" in source
