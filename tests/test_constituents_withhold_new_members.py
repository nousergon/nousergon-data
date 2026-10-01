"""alpha-engine-config-I11785: a brand-new member no source can classify is
withheld with an alert, never a failed run.

2026-10-01: S&P added VYLR (Corteva's seed spin-off) on its distribution date.
SPY held it before Wikipedia listed it and before yfinance had a quote, so the
I11468 coverage gate failed morning_enrich and the preopen ran degraded.
data#2004 declared VYLR's sector by hand; these tests pin the generic rule:

* a member still unclassified that the previous snapshot proves is NEW is left
  out of the published roster (and logged at ERROR, which is the alert);
* a member the previous snapshot already classified keeps that sector;
* with no previous snapshot nothing can be proven new, so it still raises.
"""
from __future__ import annotations

import io
import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from collectors import constituents
from sf_preflight import PreflightContext, check_constituents_fetch

# The real loader, taken before conftest's autouse stub replaces it.
_REAL_LOAD_PREVIOUS = constituents._load_previous_snapshot

_PREVIOUS = {
    "date": "2026-09-30",
    "tickers": ["AAPL", "JPM", "CTVA", "HUBS"],
    "sector_map": {
        "AAPL": "Information Technology",
        "JPM": "Financials",
        "CTVA": "Materials",
        "HUBS": "Information Technology",
    },
}


def _fetch_1001():
    """10-01 shape: SPY holds VYLR, which no source classifies; HUBS has
    dropped off Wikipedia's table (transient) but was classified before."""
    tickers = ["AAPL", "JPM", "CTVA", "VYLR", "HUBS"]
    weights = constituents.SsgaWeights(
        weight_map={"AAPL": 0.5, "JPM": 0.3, "CTVA": 0.15, "VYLR": 0.05, "HUBS": 1.0},
        index_of={"AAPL": "S&P 500", "JPM": "S&P 500", "CTVA": "S&P 500",
                  "VYLR": "S&P 500", "HUBS": "S&P 400"},
        raw_sum_by_index={"S&P 500": 100.0, "S&P 400": 100.0},
        method="ssga_holdings_file",
        members_by_index={"S&P 500": ["AAPL", "JPM", "CTVA", "VYLR"], "S&P 400": ["HUBS"]},
    )
    return (
        tickers,
        {"AAPL": "Information Technology", "JPM": "Financials", "CTVA": "Materials"},
        {"AAPL": "XLK", "JPM": "XLF", "CTVA": "XLB"},
        {},
        4,
        1,
        weights,
    )


_YF_BLANK = {
    "VYLR": {"sector": "", "industry": ""},
    "HUBS": {"error": "HTTPError: 429 Too Many Requests"},
}


def _collect(previous, yf_rows=_YF_BLANK, overrides=None):
    writes: dict[str, dict] = {}

    def fake_put_object(**kwargs):
        writes[kwargs["Key"]] = json.loads(kwargs["Body"])
        return {}

    with patch("collectors.constituents._fetch_constituents", side_effect=_fetch_1001), \
         patch("collectors.constituents._yfinance_classification", return_value=yf_rows), \
         patch("collectors.constituents._load_previous_snapshot", return_value=previous), \
         patch.dict("collectors.constituents._SECTOR_OVERRIDES", overrides or {}, clear=True), \
         patch("collectors.constituents.boto3.client") as client:
        client.return_value.put_object.side_effect = fake_put_object
        result = constituents.collect(bucket="any", run_date="2026-10-01")
    return result, writes["market_data/weekly/2026-10-01/constituents.json"]


def test_a_new_unclassifiable_member_is_withheld_not_fatal(caplog) -> None:
    with caplog.at_level(logging.ERROR, logger="collectors.constituents"):
        result, payload = _collect(_PREVIOUS)

    assert result["status"] == "ok"
    assert "VYLR" not in payload["tickers"]
    assert "VYLR" not in result["tickers"]
    assert payload["withheld_members"]["VYLR"]["absent_from_snapshot"] == "2026-09-30"
    assert "VYLR" not in payload["sector_fallback"]
    # Every published member still has a sector: the I11468 invariant holds.
    assert set(payload["sector_map"]) == set(payload["tickers"])
    assert set(payload["sector_etf_map"]) == set(payload["tickers"])
    # It is gone from the rosters and weights, and the counts agree.
    assert payload["sp500_tickers"] == ["AAPL", "JPM", "CTVA"]
    assert "VYLR" not in payload["weight_map"] and "VYLR" not in payload["index_of"]
    assert payload["sp500_count"] == 3
    # The ERROR log is the alert (flow-doctor's ERROR handler).
    assert any("VYLR" in r.getMessage() and r.levelno == logging.ERROR for r in caplog.records)


def test_an_established_member_keeps_its_previous_sector() -> None:
    _, payload = _collect(_PREVIOUS)
    assert payload["sector_map"]["HUBS"] == "Information Technology"
    assert payload["sector_etf_map"]["HUBS"] == "XLK"
    assert payload["sector_fallback"]["HUBS"]["source"] == "previous_snapshot"
    assert payload["sector_fallback"]["HUBS"]["snapshot_date"] == "2026-09-30"
    assert "429" in payload["sector_fallback"]["HUBS"]["yfinance_error"]


def test_a_current_source_beats_the_previous_sector() -> None:
    rows = dict(_YF_BLANK, HUBS={"sector": "Healthcare", "industry": "Biotechnology"})
    _, payload = _collect(_PREVIOUS, yf_rows=rows)
    assert payload["sector_map"]["HUBS"] == "Health Care"
    assert payload["sector_fallback"]["HUBS"]["source"] == "yfinance"


def test_a_declared_override_beats_withholding() -> None:
    overrides = {"VYLR": ("Materials", "2099-12-31", "test")}
    _, payload = _collect(_PREVIOUS, overrides=overrides)
    assert "VYLR" in payload["tickers"]
    assert payload["sector_map"]["VYLR"] == "Materials"
    assert payload["withheld_members"] == {}


def test_without_a_previous_snapshot_it_still_raises() -> None:
    """Fail closed: nothing can be proven new, so nothing is withheld."""
    with pytest.raises(constituents.SectorCoverageIncomplete, match="VYLR"):
        _collect(None)


def test_an_established_member_with_no_sector_anywhere_still_raises() -> None:
    previous = dict(_PREVIOUS, sector_map={k: v for k, v in _PREVIOUS["sector_map"].items() if k != "HUBS"})
    with pytest.raises(constituents.SectorCoverageIncomplete, match="HUBS"):
        _collect(previous)


def test_the_addition_lag_threshold_still_fails_first() -> None:
    n = constituents._UNMAPPED_SECTOR_HARD_FAIL_THRESHOLD + 1
    tickers = [f"N{i}" for i in range(n)]
    with pytest.raises(constituents.SectorCoverageIncomplete, match="parse/layout break"):
        constituents.resolve_sector_coverage(tickers, {}, {}, previous=_PREVIOUS)


# ── the previous-snapshot loader ─────────────────────────────────────────────


def _s3_with(dates: list[str], bodies: dict[str, object]):
    s3 = MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": f"market_data/weekly/{d}/"} for d in dates]},
    ]

    def get_object(Bucket, Key):
        date = Key.split("/")[2]
        body = bodies.get(date)
        if body is None:
            raise RuntimeError("NoSuchKey")
        return {"Body": io.BytesIO(json.dumps(body).encode())}

    s3.get_object.side_effect = get_object
    return s3


def test_loader_picks_the_newest_snapshot_before_the_run_date() -> None:
    s3 = _s3_with(
        ["2026-09-26", "2026-09-30", "2026-10-01", "not-a-date"],
        {"2026-09-26": {"tickers": ["OLD"]}, "2026-09-30": {"tickers": ["AAPL"]},
         "2026-10-01": {"tickers": ["TODAY"]}},
    )
    with patch("collectors.constituents.boto3.client", return_value=s3):
        snap = _REAL_LOAD_PREVIOUS("any", "market_data/", "2026-10-01")
    assert snap["tickers"] == ["AAPL"]
    assert snap["date"] == "2026-09-30"


def test_loader_skips_an_unreadable_snapshot() -> None:
    s3 = _s3_with(["2026-09-26", "2026-09-30"], {"2026-09-26": {"tickers": ["OLD"]}})
    with patch("collectors.constituents.boto3.client", return_value=s3):
        snap = _REAL_LOAD_PREVIOUS("any", "market_data/", "2026-10-01")
    assert snap["tickers"] == ["OLD"]


def test_loader_returns_none_when_s3_fails() -> None:
    with patch("collectors.constituents.boto3.client", side_effect=RuntimeError("no creds")):
        assert _REAL_LOAD_PREVIOUS("any", "market_data/", "2026-10-01") is None


# ── the preflight predicts the same outcome ──────────────────────────────────


def test_preflight_passes_and_excludes_a_member_collect_will_withhold() -> None:
    tickers = [f"T{i}" for i in range(901)] + ["VYLR", "HUBS"]
    sectors = {t: "Industrials" for t in tickers[:901]}
    etfs = {t: "XLI" for t in sectors}
    fetched = (tickers, sectors, etfs, {}, 503, 400, constituents.SsgaWeights())
    previous = {"date": "2026-09-30", "tickers": tickers[:901] + ["HUBS"],
                "sector_map": dict(sectors, HUBS="Information Technology")}
    ctx = PreflightContext(bucket="any", today="2026-10-01", prior_trading_day="2026-09-30")
    with patch("collectors.constituents._fetch_constituents", return_value=fetched), \
         patch("collectors.constituents._yfinance_classification", return_value=_YF_BLANK), \
         patch("collectors.constituents._load_previous_snapshot", return_value=previous), \
         patch.dict("collectors.constituents._SECTOR_OVERRIDES", {}, clear=True):
        result = check_constituents_fetch(ctx)
    assert result.status == "ok", result.message
    assert "VYLR" not in ctx.fresh_constituents
    assert "HUBS" in ctx.fresh_constituents
    assert set(result.details["withheld_members"]) == {"VYLR"}
    assert "withhold" in result.message
