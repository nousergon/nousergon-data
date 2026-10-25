"""EDGAR PIT market cap reads a split-only `Close` (alpha-engine-config-I12436, option A).

`universe` `Close` is dividend back-adjusted across its whole history, so a rebuilt
2022 session read market cap low by the cumulative dividend factor. Ruling (Brian,
2026-10-23 15:05Z): read the split-only CRSP-basis `universe_crsp` library instead.

The numbers are the ones the issue measured on 2026-10-23: the stored 2022-01-03
`close_raw` in the rebuilt sessions (AAPL 177.79, MO 33.30) against AAPL's raw close
of 182.01. AAPL's share count is its 10-K cover-page count (16,406,397,000 at
2021-10-15, filed 2021-10-29). MO's raw close was measured only as "about x0.70" of
the stored value, so the fixture derives it from that ratio rather than quoting a
price, and MO's share count is a round fixture. The exact live check runs after the
library is built (the rebuild wrapper's basis gate).
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from collectors import edgar_pit_fundamentals as e

_REPO = Path(__file__).resolve().parents[1]
_SESSION = dt.date(2022, 1, 3)

_AAPL_RAW = 182.01
_AAPL_DIVIDEND_ADJUSTED = 177.79
_AAPL_SHARES = 16_406_397_000
_MO_DIVIDEND_ADJUSTED = 33.30
_MO_SPLIT_ONLY = _MO_DIVIDEND_ADJUSTED / 0.70
_MO_SHARES = 1_830_000_000


def _o(text: str) -> int:
    return dt.date.fromisoformat(text).toordinal()


def _cover_count(value: float, end: str, filed: str, form: str = "10-Q") -> e.Fact:
    return e.Fact(
        quantity="shares_outstanding",
        priority=0,
        start=None,
        end=_o(end),
        value=float(value),
        filed=_o(filed),
        accession=f"acc-{filed}",
        taxonomy="dei",
        concept="EntityCommonStockSharesOutstanding",
        unit="shares",
        form=form,
    )


class _FakeLibrary:
    def __init__(self, closes: dict[str, dict[str, float]]) -> None:
        self._closes = closes

    def has_symbol(self, symbol: str) -> bool:
        return symbol in self._closes

    def list_symbols(self) -> list[str]:
        return list(self._closes)

    def read(self, symbol: str, columns, date_range) -> SimpleNamespace:
        start, end = date_range
        series = pd.Series(self._closes[symbol], dtype=float)
        series.index = pd.DatetimeIndex(series.index)
        series = series[(series.index >= start) & (series.index <= end)]
        return SimpleNamespace(data=pd.DataFrame({"Close": series}))


_LIVE = {
    "AAPL": {"2021-12-31": 175.11, "2022-01-03": _AAPL_DIVIDEND_ADJUSTED},
    "MO": {"2021-12-31": 33.07, "2022-01-03": _MO_DIVIDEND_ADJUSTED},
}
_SPLIT_ONLY = {
    "AAPL": {"2021-12-31": 177.57, "2022-01-03": _AAPL_RAW},
    "MO": {"2021-12-31": 47.39, "2022-01-03": _MO_SPLIT_ONLY},
}


@pytest.fixture
def libraries(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict]:
    from store import arctic_store

    held = {"universe": _LIVE, "universe_crsp": _SPLIT_ONLY}
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda bucket=None: _FakeLibrary(held["universe"]))
    monkeypatch.setattr(
        arctic_store,
        "get_scratch_universe_lib",
        lambda name, bucket=None: _FakeLibrary(held[name]),
    )
    return held


def _caps(closes: pd.DataFrame) -> dict[str, float]:
    splits = {"AAPL": [(_o("2020-08-31"), 0.25)]}
    events = {
        "AAPL": (320193, e.filing_events(
            [_cover_count(_AAPL_SHARES, "2021-10-15", "2021-10-29", "10-K")],
            splits=splits["AAPL"],
            evaluate_from=0,
        )),
        "MO": (764180, e.filing_events(
            [_cover_count(_MO_SHARES, "2021-10-22", "2021-10-28")], splits=(), evaluate_from=0
        )),
    }
    price_factors, _ = e.split_only_price_factors(splits)
    frames = e.materialize_sessions(
        events, closes, [_SESSION], splits=splits, price_factors=price_factors
    )
    frame = frames[_SESSION].set_index("ticker")
    return {t: float(frame.loc[t, "market_cap_raw"]) for t in ("AAPL", "MO")}


def test_a_backfill_reads_the_split_only_library_and_the_daily_run_the_live_one() -> None:
    backfill = e._parser().parse_args(["backfill", "--start", "2022-01-03"])
    daily = e._parser().parse_args(["incremental"])
    assert backfill.close_library == "universe_crsp"
    assert daily.close_library == "universe"


def test_market_cap_is_the_split_only_close_times_shares(libraries) -> None:
    reader = e.ArcticPriceReader("bucket", library="universe_crsp")
    caps = _caps(reader.closes(["AAPL", "MO"], dt.date(2021, 12, 31), _SESSION))
    assert caps["AAPL"] == pytest.approx(_AAPL_RAW * _AAPL_SHARES, rel=0.005)
    assert caps["MO"] == pytest.approx(_MO_SPLIT_ONLY * _MO_SHARES, rel=0.01)


def test_the_live_library_reads_market_cap_low_by_the_dividend_factor(libraries) -> None:
    """The defect the switch removes: `universe`'s back-adjusted 2022 close."""
    caps = _caps(e.ArcticPriceReader("bucket").closes(["AAPL", "MO"], dt.date(2021, 12, 31), _SESSION))
    assert caps["AAPL"] != pytest.approx(_AAPL_RAW * _AAPL_SHARES, rel=0.005)
    assert caps["MO"] != pytest.approx(_MO_SPLIT_ONLY * _MO_SHARES, rel=0.01)


def test_split_only_price_factors_carry_no_spin_off_or_vendor_factor() -> None:
    splits = {"HON": [(_o("2026-06-29"), 2.0)], "AAPL": [(_o("2020-08-31"), 0.25)]}
    live, _ = e.price_factor_events(splits, through=dt.date(2026, 10, 16))
    assert "CTVA" in live and live["HON"] != splits["HON"]
    prices, summary = e.split_only_price_factors(splits)
    assert prices == splits
    assert "CTVA" not in prices
    assert summary == {"price_factor_overrides": [], "declared_spinoffs": [], "close_basis": "split_only"}
    for ticker in ("HON", "AAPL"):
        for day in ("2022-01-03", "2026-06-26", "2026-07-01"):
            assert e.close_basis(splits[ticker], prices[ticker], _o(day)) == 1.0


def _frame(closes: dict[str, dict[str, float]]) -> pd.DataFrame:
    frame = pd.DataFrame({t: pd.Series(v) for t, v in closes.items()})
    frame.index = pd.DatetimeIndex(frame.index)
    return frame


def test_the_currency_gate_passes_a_current_library() -> None:
    reading = e.assert_close_library_current(
        _frame(_SPLIT_ONLY), _frame(_LIVE), library="universe_crsp"
    )
    assert reading["newest_session"] == reading["reference_newest_session"] == "2022-01-03"
    assert reading["names_priced_by_reference"] == 2 and reading["names_absent"] == 0


def test_the_currency_gate_refuses_a_missing_name() -> None:
    with pytest.raises(e.GateFailed, match=r"1 of 2 priced names .*\['MO'\]"):
        e.assert_close_library_current(
            _frame({"AAPL": _SPLIT_ONLY["AAPL"]}), _frame(_LIVE), library="universe_crsp", missing=["MO"]
        )


def test_the_currency_gate_refuses_a_stale_library() -> None:
    stale = {t: {"2021-12-31": v["2021-12-31"]} for t, v in _SPLIT_ONLY.items()}
    with pytest.raises(e.GateFailed, match="newest session is 2021-12-31 against 2022-01-03"):
        e.assert_close_library_current(_frame(stale), _frame(_LIVE), library="universe_crsp")


def test_the_currency_gate_refuses_an_empty_library() -> None:
    with pytest.raises(e.GateFailed, match="newest session is None"):
        e.assert_close_library_current(pd.DataFrame(), _frame(_LIVE), library="universe_crsp")


def test_the_reader_lists_names_the_split_only_library_lacks(libraries) -> None:
    libraries["universe_crsp"] = {"AAPL": _SPLIT_ONLY["AAPL"]}
    reader = e.ArcticPriceReader("bucket", library="universe_crsp")
    closes = reader.closes(["AAPL", "MO"], dt.date(2021, 12, 31), _SESSION)
    assert list(closes.columns) == ["AAPL"] and reader.missing == ["MO"]


def test_the_dispatcher_can_build_the_split_only_library() -> None:
    source = (_REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py").read_text()
    assert '"universe-crsp-basis-build": (' in source
    assert "python -m builders.migrate_universe_crsp_basis --apply --reconcile-start 2020-06-30" in source
    assert '"universe-crsp-basis-build": 43200,' in source
