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


def _balance_sheet_event(**overrides: float) -> dict:
    facts = [
        _fact("net_income", "2023-01-01", "2023-12-31", overrides.get("income", 100), "2024-02-01"),
        _fact("revenue", "2023-01-01", "2023-12-31", 1000, "2024-02-01"),
        _fact("gross_profit", "2023-01-01", "2023-12-31", 400, "2024-02-01"),
        _fact("equity", None, "2023-12-31", overrides.get("equity", 500), "2024-02-01"),
        _fact("assets_current", None, "2023-12-31", 300, "2024-02-01"),
        _fact("liabilities_current", None, "2023-12-31", 100, "2024-02-01"),
        _fact("operating_cash_flow", "2023-01-01", "2023-12-31", 150, "2024-02-01"),
        _fact("capex", "2023-01-01", "2023-12-31", 50, "2024-02-01"),
        _fact("shares_outstanding", None, "2024-01-20", 100, "2024-02-01", unit="shares"),
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
        row = e.session_row(ticker="T", cik=1, label=dt.date(2024, 3, 1), event=event, close=30.0)
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

    def test_the_v1_clips_bound_every_field(self) -> None:
        event = _balance_sheet_event(income=100_000, long_term_debt=400)["event"]
        row = e.session_row(ticker="T", cik=1, label=dt.date(2024, 3, 1), event=event, close=0.001)
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
        assert built["event"].shares_current_basis == pytest.approx(200)
        row = e.session_row(
            ticker="T", cik=1, label=dt.date(2024, 3, 1), event=built["event"], close=15.0
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
