"""alpha-engine-config-I11468: every published constituent has a sector.

The 2026-09-23 constituents.json listed 903 tickers but carried only 899
``sector_map`` entries. The four missing were exactly that week's S&P 400 adds
(AGNC, CORT, EAT, HUBS), which Wikipedia's GICS table had not caught up with.
``collect()`` deliberately tolerated up to 10 such members, logged a warning,
and published them with no sector. Signals then carried sector "Unknown" and
ChallengerShadow refused the write.

These tests pin the fix. A member Wikipedia has not classified gets a sector
from yfinance, mapped onto GICS, with the evidence published in
``sector_fallback``. A member that is still unclassified raises before any S3
write.
"""
from __future__ import annotations

import json
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from collectors import constituents

# The 2026-09-23 yfinance classification of the four adds, as published in
# market_data/universe_classification/latest.json.
_YF_0923 = {
    "HUBS": {"sector": "Technology", "industry": "Software - Application"},
    "EAT": {"sector": "Consumer Cyclical", "industry": "Restaurants"},
    "AGNC": {"sector": "Real Estate", "industry": "REIT - Mortgage"},
    "CORT": {"sector": "Healthcare", "industry": "Biotechnology"},
}


def _fake_fetch_0923():
    """The 9/23 shape: SSGA membership has the four adds, Wikipedia does not."""
    tickers = ["AAPL", "JPM", "HUBS", "EAT", "AGNC", "CORT"]
    return (
        tickers,
        {"AAPL": "Information Technology", "JPM": "Financials"},
        {"AAPL": "XLK", "JPM": "XLF"},
        {"AAPL": "Technology Hardware, Storage & Peripherals"},
        2,
        4,
        constituents.SsgaWeights(),
    )


def _run_collect_capturing_writes(yf_rows: dict) -> dict[str, dict]:
    writes: dict[str, dict] = {}

    def fake_put_object(**kwargs):
        writes[kwargs["Key"]] = json.loads(kwargs["Body"])
        return {}

    with patch("collectors.constituents._fetch_constituents", side_effect=_fake_fetch_0923), \
         patch("collectors.constituents._yfinance_classification", return_value=yf_rows), \
         patch("collectors.constituents.boto3.client") as client:
        client.return_value.put_object.side_effect = fake_put_object
        constituents.collect(bucket="any", run_date="2026-09-23")
    return writes


def test_the_0923_adds_are_published_with_a_gics_sector() -> None:
    writes = _run_collect_capturing_writes(_YF_0923)
    payload = writes["market_data/weekly/2026-09-23/constituents.json"]

    # The producer invariant the issue asked for.
    assert len(payload["sector_map"]) == len(payload["tickers"])
    assert set(payload["sector_etf_map"]) == set(payload["tickers"])

    assert payload["sector_map"]["HUBS"] == "Information Technology"
    assert payload["sector_map"]["EAT"] == "Consumer Discretionary"
    # GICS moved mortgage REITs to Financials in 2023. yfinance still says
    # Real Estate, so the industry override decides this one.
    assert payload["sector_map"]["AGNC"] == "Financials"
    assert payload["sector_map"]["CORT"] == "Health Care"
    assert payload["sector_etf_map"]["AGNC"] == "XLF"

    # The feature store's data/sector_map.json covers them too.
    assert set(writes["data/sector_map.json"]) == set(payload["tickers"])
    # Their sub-sector benchmark falls back to the sector ETF.
    assert payload["sub_sector_etf_map"]["HUBS"] == "XLK"


def test_fallback_provenance_is_published() -> None:
    payload = _run_collect_capturing_writes(_YF_0923)["market_data/weekly/2026-09-23/constituents.json"]
    assert set(payload["sector_fallback"]) == {"HUBS", "EAT", "AGNC", "CORT"}
    assert payload["sector_fallback"]["AGNC"] == {
        "source": "yfinance",
        "sector": "Financials",
        "yf_sector": "Real Estate",
        "yf_industry": "REIT - Mortgage",
    }
    # Wikipedia-classified members are not listed as fallbacks.
    assert "AAPL" not in payload["sector_fallback"]


@pytest.mark.parametrize(
    "row",
    [
        {"error": "HTTPError: 429 Too Many Requests"},
        {"sector": "", "industry": ""},
        {"sector": "Conglomerates", "industry": "Holding Company"},
    ],
    ids=["yfinance-error", "yfinance-empty", "non-gics-sector"],
)
def test_an_unclassifiable_member_fails_before_any_write(row: dict) -> None:
    rows = dict(_YF_0923, HUBS=row)
    with patch("collectors.constituents._fetch_constituents", side_effect=_fake_fetch_0923), \
         patch("collectors.constituents._yfinance_classification", return_value=rows), \
         patch("collectors.constituents.boto3.client") as client:
        with pytest.raises(constituents.SectorCoverageIncomplete) as excinfo:
            constituents.collect(bucket="any", run_date="2026-09-23")
    assert "HUBS" in str(excinfo.value)
    client.return_value.put_object.assert_not_called()


def test_a_member_missing_from_the_yfinance_result_fails() -> None:
    rows = {k: v for k, v in _YF_0923.items() if k != "CORT"}
    with patch("collectors.constituents._fetch_constituents", side_effect=_fake_fetch_0923), \
         patch("collectors.constituents._yfinance_classification", return_value=rows):
        with pytest.raises(constituents.SectorCoverageIncomplete, match="CORT"):
            constituents.collect(bucket="any", dry_run=True)


def test_a_wikipedia_sector_with_no_etf_fails() -> None:
    """sector_etf_map is published separately as data/sector_map.json. A
    member missing only from THAT map is the same 'Unknown' downstream."""
    def fake_fetch():
        return (
            ["AAPL"], {"AAPL": "Information Technology"}, {}, {}, 1, 0,
            constituents.SsgaWeights(),
        )

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch), \
         patch("collectors.constituents._yfinance_classification") as yf:
        with pytest.raises(constituents.SectorCoverageIncomplete, match="AAPL"):
            constituents.collect(bucket="any", dry_run=True)
    yf.assert_not_called()


def test_no_yfinance_call_when_wikipedia_covers_everyone() -> None:
    def fake_fetch():
        return (
            ["AAPL"], {"AAPL": "Information Technology"}, {"AAPL": "XLK"}, {}, 1, 0,
            constituents.SsgaWeights(),
        )

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch), \
         patch("collectors.constituents._yfinance_classification") as yf:
        result = constituents.collect(bucket="any", dry_run=True)
    assert result["status"] == "ok_dry_run"
    yf.assert_not_called()


def test_every_gics_target_has_a_sector_etf() -> None:
    """A fallback sector with no ETF would fail the gate for every member it
    classifies. Pin the two tables together."""
    targets = set(constituents._YF_SECTOR_TO_GICS.values()) | set(
        constituents._YF_INDUSTRY_TO_GICS.values()
    )
    assert targets <= set(constituents.GICS_TO_ETF)
    assert set(constituents._YF_SECTOR_TO_GICS.values()) == set(constituents.GICS_TO_ETF)


def test_yfinance_classification_records_errors_and_uses_dash_share_class(monkeypatch) -> None:
    seen: list[str] = []

    def fake_ticker(sym):
        seen.append(sym)
        if sym == "BAD":
            raise RuntimeError("boom")
        t = MagicMock()
        t.info = {"sector": "Financial Services", "industry": "Insurance - Diversified"}
        return t

    fake_yf = types.SimpleNamespace(Ticker=fake_ticker)
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setattr(constituents, "_YF_FALLBACK_DELAY_SECS", 0)

    out = constituents._yfinance_classification(["BRK.B", "BAD"])
    assert seen == ["BRK-B", "BAD"]
    # Results are keyed by the SSGA spelling, not the yfinance one.
    assert out["BRK.B"] == {"sector": "Financial Services", "industry": "Insurance - Diversified"}
    assert out["BAD"] == {"error": "RuntimeError: boom"}


def test_published_payload_validates_against_the_contract() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from pathlib import Path

    schema = json.loads(
        (Path(__file__).resolve().parent.parent / "contracts" / "constituents.schema.json").read_text()
    )
    payload = _run_collect_capturing_writes(_YF_0923)["market_data/weekly/2026-09-23/constituents.json"]
    jsonschema.validate(payload, schema)
