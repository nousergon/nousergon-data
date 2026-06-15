"""Price-source adapter contract + golden tests.

Pins three things for the provider-agnostic ingestion seam (alpha-engine-config#1082):

  1. **Schema parity** — the ``PriceBar`` dataclass fields match the authoritative
     field catalog in ``sources/SCHEMA.md`` §2 (mirrors ``test_schema_contract.py``).
  2. **Port conformance** — every registered adapter satisfies the
     ``PriceSourceAdapter`` Protocol, declares capabilities, and round-trips a record.
  3. **Golden fidelity** — for a mocked VENDOR response, each adapter emits the same
     canonical ``PriceBar`` records the live pipeline produces (Phase 1a wraps the
     legacy ``_fetch_*`` faithfully, so output is byte-identical).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors import daily_closes as dc  # noqa: E402
import sources  # noqa: E402
from sources import available, get_adapter  # noqa: E402
from sources.contract import (  # noqa: E402
    PRICEBAR_FIELDS,
    RECORD_KEYS,
    PriceBar,
    PriceSourceAdapter,
    SourceCapabilities,
)

_SCHEMA_MD = Path(__file__).resolve().parents[1] / "sources" / "SCHEMA.md"

# Match `field` at the start of a §2 markdown table row: ``| `ticker` | ... |``
_FIELD_ROW_RE = re.compile(r"^\|\s*`([a-zA-Z0-9_]+)`\s*\|")
_SECTION_2 = "## 2. `PriceBar` — canonical field catalog"
_NEXT_SECTION = "## 3."


def _schema_md_fields() -> set[str]:
    """Field names from the SCHEMA.md §2 catalog table."""
    text = _SCHEMA_MD.read_text()
    start = text.index(_SECTION_2)
    end = text.index(_NEXT_SECTION, start)
    out: set[str] = set()
    for line in text[start:end].splitlines():
        m = _FIELD_ROW_RE.match(line)
        if m:
            out.add(m.group(1))
    return out


# ── 1. Schema parity ─────────────────────────────────────────────────────────

def test_pricebar_fields_match_schema_md():
    """PriceBar dataclass fields == SCHEMA.md §2 catalog (drift fails CI)."""
    assert _schema_md_fields() == set(PRICEBAR_FIELDS)


def test_to_record_keys_are_the_legacy_contract():
    """to_record() reproduces exactly the persisted record keys (no currency)."""
    bar = PriceBar(
        ticker="AAPL", date="2026-06-12", open=1.0, high=2.0, low=0.5,
        close=1.5, adj_close=1.5, volume=1000, source="polygon", vwap=1.4,
    )
    assert tuple(bar.to_record().keys()) == RECORD_KEYS
    assert "currency" not in bar.to_record()


def test_record_roundtrip_lossless():
    """from_record(...).to_record() == original legacy record, for each flavor."""
    samples = [
        {"ticker": "AAPL", "date": "2026-06-12", "Open": 1.0, "High": 2.0,
         "Low": 0.5, "Close": 1.5, "Adj_Close": 1.5, "Volume": 1000,
         "VWAP": 1.4, "source": "polygon"},
        {"ticker": "VIX", "date": "2026-06-12", "Open": 17.0, "High": 17.0,
         "Low": 17.0, "Close": 17.0, "Adj_Close": 17.0, "Volume": 0,
         "VWAP": None, "source": "fred"},
        {"ticker": "MSFT", "date": "2026-06-12", "Open": 10.0, "High": 11.0,
         "Low": 9.0, "Close": 10.5, "Adj_Close": 10.4, "Volume": 50,
         "VWAP": None, "source": "yfinance"},
    ]
    for r in samples:
        assert PriceBar.from_record(r).to_record() == r


# ── 2. Port conformance ──────────────────────────────────────────────────────

def test_three_sources_registered():
    assert set(available()) == {"polygon", "fred", "yfinance"}


@pytest.mark.parametrize("name", ["polygon", "fred", "yfinance"])
def test_adapter_satisfies_port(name):
    adapter = get_adapter(name)
    assert isinstance(adapter, PriceSourceAdapter)
    assert adapter.name == name
    assert isinstance(adapter.capabilities, SourceCapabilities)
    # map_symbol is total + string-returning.
    assert isinstance(adapter.map_symbol("AAPL"), str)


def test_get_adapter_unknown_raises():
    with pytest.raises(ValueError):
        get_adapter("databento")  # not registered yet


def test_polygon_symbol_mapping():
    assert get_adapter("polygon").map_symbol("BRK-B") == "BRK.B"
    assert get_adapter("polygon").map_symbol("AAPL") == "AAPL"


# ── 3. Golden fidelity (mock the vendor boundary, run the real legacy path) ───

class _FakeGroupedClient:
    def get_grouped_daily(self, run_date):
        return {
            "AAPL": {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                     "volume": 1000, "vwap": 1.4},
            "BRK.B": {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
                      "volume": 50, "vwap": 10.2},
        }


def test_polygon_adapter_golden(monkeypatch):
    """Mocked polygon grouped-daily → canonical PriceBars (dash store-key kept)."""
    monkeypatch.setattr("polygon_client.polygon_client", lambda: _FakeGroupedClient())
    bars = get_adapter("polygon").fetch_ohlcv(["AAPL", "BRK-B"], "2026-06-12")
    by_ticker = {b.ticker: b for b in bars}
    assert set(by_ticker) == {"AAPL", "BRK-B"}
    brk = by_ticker["BRK-B"]
    assert brk.ticker == "BRK-B"          # stored dash key, looked up via BRK.B
    assert brk.close == 10.5
    assert brk.vwap == 10.2
    assert brk.volume == 50
    assert brk.source == "polygon"
    assert brk.currency == "USD"
    # to_record() reproduces the legacy persisted shape.
    assert by_ticker["AAPL"].to_record() == {
        "ticker": "AAPL", "date": "2026-06-12", "Open": 1.0, "High": 2.0,
        "Low": 0.5, "Close": 1.5, "Adj_Close": 1.5, "Volume": 1000,
        "VWAP": 1.4, "source": "polygon",
    }


def test_yfinance_adapter_golden(monkeypatch):
    """Mocked yf.download → canonical PriceBar (adjusted close, VWAP None)."""
    pytest.importorskip("yfinance")
    df = pd.DataFrame(
        {"Open": [10.0], "High": [11.0], "Low": [9.0], "Close": [10.5],
         "Adj Close": [10.4], "Volume": [50]},
        index=pd.to_datetime(["2026-06-12"]),
    )
    monkeypatch.setattr("yfinance.download", lambda **kw: df)
    bars = get_adapter("yfinance").fetch_ohlcv(["MSFT"], "2026-06-12")
    assert len(bars) == 1
    bar = bars[0]
    assert bar.ticker == "MSFT"
    assert bar.close == 10.5
    assert bar.adj_close == 10.4   # yfinance supplies a distinct adjusted close
    assert bar.vwap is None        # never a proxy
    assert bar.source == "yfinance"


def test_fred_adapter_golden(monkeypatch):
    """FRED HTTP correctness is covered by test_daily_closes_fred_*; here we pin
    the adapter's conversion contract: whatever record the legacy fetch produces
    is losslessly normalized to a PriceBar."""
    def _fake_fetch(tickers, date_str, records, window_cache=None):
        records.append(dc._fred_record("VIX", date_str, 17.25))
        return 1

    monkeypatch.setattr(dc, "_fetch_fred_closes", _fake_fetch)
    bars = get_adapter("fred").fetch_ohlcv(["VIX"], "2026-06-12")
    assert len(bars) == 1
    bar = bars[0]
    assert bar.ticker == "VIX"
    assert bar.close == 17.25
    assert bar.volume == 0
    assert bar.vwap is None
    assert bar.source == "fred"
