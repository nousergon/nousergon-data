"""
Regression tests for constituents.py — sector_map coverage.

Bug: prior to fix, _fetch_constituents only extracted GICS sectors from the
S&P 500 Wikipedia table, leaving every S&P 400 mid-cap ticker without a
sector mapping. EOD reconcile's sector attribution depended on this map and
silently fell through to "Unknown" for any held mid-cap (e.g. JHG fired
flow-doctor on 2026-04-30).
"""
from __future__ import annotations

from io import StringIO
from unittest.mock import patch

import pandas as pd
import pytest

from collectors import constituents


def _fake_html(tickers: list[str], sectors: list[str]) -> str:
    """Build minimal Wikipedia-shaped HTML with Symbol + GICS Sector columns."""
    df = pd.DataFrame({"Symbol": tickers, "GICS Sector": sectors})
    return df.to_html(index=False)


class _FakeResp:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        pass


def test_sector_map_covers_both_sp500_and_sp400() -> None:
    """sector_map must include every ticker from both index tables."""
    sp500_html = _fake_html(["AAPL", "MSFT"], ["Information Technology", "Information Technology"])
    sp400_html = _fake_html(["JHG", "WSO"], ["Financials", "Industrials"])

    def fake_get(url, **kwargs):
        if "S%26P_500" in url:
            return _FakeResp(sp500_html)
        if "S%26P_400" in url:
            return _FakeResp(sp400_html)
        raise AssertionError(f"unexpected URL: {url}")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sp500_count, sp400_count = (
            constituents._fetch_constituents()
        )

    assert sp500_count == 2
    assert sp400_count == 2
    assert sector_map["AAPL"] == "Information Technology"
    assert sector_map["JHG"] == "Financials"
    assert sector_map["WSO"] == "Industrials"
    assert sector_etf_map["JHG"] == "XLF"
    assert sector_etf_map["WSO"] == "XLI"
    assert set(tickers) == {"AAPL", "MSFT", "JHG", "WSO"}


def test_collect_raises_when_sector_coverage_incomplete(tmp_path) -> None:
    """If a ticker lands in `tickers` without a sector entry, collect() must raise."""
    # Simulate the prior-bug condition: tickers list has 4 entries but
    # sector_map only has 2 (S&P 500 only).
    def fake_fetch():
        return (
            ["AAPL", "MSFT", "JHG", "WSO"],
            {"AAPL": "Information Technology", "MSFT": "Information Technology"},
            {"AAPL": "XLK", "MSFT": "XLK"},
            2,
            2,
        )

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch):
        with pytest.raises(RuntimeError, match="Sector mapping incomplete"):
            constituents.collect(bucket="any", dry_run=True)


def test_fetch_raises_when_sector_column_missing(tmp_path, monkeypatch) -> None:
    """If Wikipedia table column header changes, _fetch_constituents must
    fall through to the cache (or empty result) rather than silently
    selecting a junk table."""
    # Isolate the cache to a tmp path so prior tests' cache pollution
    # doesn't disguise the missing-column signature.
    monkeypatch.setattr(
        constituents, "_CACHE_PATH", tmp_path / "constituents_cache.csv"
    )
    df = pd.DataFrame({"Symbol": ["AAPL"], "Industry": ["Tech"]})  # no GICS column
    sp500_html = df.to_html(index=False)

    def fake_get(url, **kwargs):
        return _FakeResp(sp500_html)

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, _, _, _ = constituents._fetch_constituents()

    # No tables matched the schema → empty result, which collect() then
    # short-circuits with status=error before any S3 write.
    assert tickers == []
    assert sector_map == {}


def test_cache_persists_sector_map_and_etf(tmp_path, monkeypatch) -> None:
    """On a successful Wikipedia fetch the local cache must persist
    ticker + GICS sector + sector ETF, so a future Wikipedia outage's
    fallback returns a fully-populated sector_map (instead of empty,
    which makes collect() raise 'Sector mapping incomplete')."""
    cache_path = tmp_path / "constituents_cache.csv"
    monkeypatch.setattr(constituents, "_CACHE_PATH", cache_path)

    sp500_html = _fake_html(["AAPL", "MSFT"], ["Information Technology", "Information Technology"])
    sp400_html = _fake_html(["JHG", "WSO"], ["Financials", "Industrials"])

    def fake_get(url, **kwargs):
        return _FakeResp(sp500_html if "500" in url else sp400_html)

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        constituents._fetch_constituents()

    assert cache_path.exists()
    cached = pd.read_csv(cache_path)
    assert set(cached.columns) >= {"ticker", "gics_sector", "sector_etf"}
    row_by_ticker = {r["ticker"]: r for _, r in cached.iterrows()}
    assert row_by_ticker["AAPL"]["gics_sector"] == "Information Technology"
    assert row_by_ticker["AAPL"]["sector_etf"] == "XLK"
    assert row_by_ticker["JHG"]["gics_sector"] == "Financials"
    assert row_by_ticker["JHG"]["sector_etf"] == "XLF"


def test_cache_fallback_returns_full_sector_map(tmp_path, monkeypatch) -> None:
    """When Wikipedia is unreachable, the cache fallback must return
    populated sector_map + sector_etf_map so collect()'s coverage check
    passes. Regression for the 2026-05-11 cascade: partial Wikipedia
    outage → empty-cache fallback → collect() raise → SF cascade silent
    MorningEnrich failure."""
    cache_path = tmp_path / "constituents_cache.csv"
    pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "JHG"],
        "gics_sector": ["Information Technology", "Information Technology", "Financials"],
        "sector_etf": ["XLK", "XLK", "XLF"],
    }).to_csv(cache_path, index=False)
    monkeypatch.setattr(constituents, "_CACHE_PATH", cache_path)

    def fake_get(url, **kwargs):
        raise RuntimeError("simulated Wikipedia outage")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sp500_count, sp400_count = (
            constituents._fetch_constituents()
        )

    assert tickers == ["AAPL", "MSFT", "JHG"]
    assert sector_map == {
        "AAPL": "Information Technology",
        "MSFT": "Information Technology",
        "JHG": "Financials",
    }
    assert sector_etf_map == {"AAPL": "XLK", "MSFT": "XLK", "JHG": "XLF"}
    assert sp500_count == 0 and sp400_count == 0


def test_cache_fallback_handles_legacy_ticker_only_schema(tmp_path, monkeypatch) -> None:
    """Pre-existing caches on EC2 have only the `ticker` column. Reader
    must tolerate that schema and return empty sector dicts (failing
    loud in collect() rather than crashing inside _fetch_constituents)."""
    cache_path = tmp_path / "constituents_cache.csv"
    pd.DataFrame({"ticker": ["AAPL", "MSFT"]}).to_csv(cache_path, index=False)
    monkeypatch.setattr(constituents, "_CACHE_PATH", cache_path)

    def fake_get(url, **kwargs):
        raise RuntimeError("simulated Wikipedia outage")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, _, _ = constituents._fetch_constituents()

    assert tickers == ["AAPL", "MSFT"]
    assert sector_map == {}
    assert sector_etf_map == {}


def test_cache_fallback_missing_cache_returns_empty(tmp_path, monkeypatch) -> None:
    """No Wikipedia AND no cache → empty lists/dicts, not a crash. The
    eventual `collect()` short-circuit ('No tickers fetched') handles
    this state."""
    cache_path = tmp_path / "constituents_cache.csv"  # does not exist
    monkeypatch.setattr(constituents, "_CACHE_PATH", cache_path)

    def fake_get(url, **kwargs):
        raise RuntimeError("simulated Wikipedia outage")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, _, _ = constituents._fetch_constituents()

    assert tickers == []
    assert sector_map == {} and sector_etf_map == {}


def test_select_constituents_table_skips_banner_table() -> None:
    """Wikipedia adds banner/disambiguation tables ahead of the constituents
    table without notice. 2026-05-11 incident: the S&P 400 page inserted a
    1-row, 2-column disambiguation-warning table at index 0, making
    `tables[0]` return columns ``[0, 1]`` instead of the constituents
    table at index 1. _select_constituents_table must scan for the right
    one by columns, not position.
    """
    banner_df = pd.DataFrame({0: [float("nan")], 1: ["This article currently links to a large number of disambiguation pages."]})
    constituents_df = pd.DataFrame({
        "Symbol": [f"T{i:03d}" for i in range(100)],
        "Security": [f"Co {i}" for i in range(100)],
        "GICS Sector": ["Industrials"] * 100,
        "GICS Sub-Industry": ["Misc"] * 100,
    })
    sub_industry_only_df = pd.DataFrame({
        "Symbol": ["X"], "GICS Sub-Industry": ["Subindustry-Only"],
    })

    picked = constituents._select_constituents_table(
        [banner_df, constituents_df, sub_industry_only_df], "S&P 400"
    )
    assert list(picked.columns) == [
        "Symbol", "Security", "GICS Sector", "GICS Sub-Industry"
    ]
    assert len(picked) == 100


def test_select_constituents_table_raises_when_no_match() -> None:
    """If no table on the page has the expected ticker + GICS sector shape,
    raise loudly rather than silently picking a junk table."""
    banner_df = pd.DataFrame({0: [1], 1: ["banner"]})
    nav_df = pd.DataFrame({"vteFoo": ["a"], "vteBar": ["b"]})

    with pytest.raises(RuntimeError, match="No constituents table found"):
        constituents._select_constituents_table([banner_df, nav_df], "S&P 400")


def test_select_constituents_table_picks_largest_candidate() -> None:
    """When multiple tables have Symbol + GICS Sector columns (e.g. a small
    example/docs table alongside the live roster), pick the largest. On the
    real S&P 500/400 pages the roster is the only such table; the largest-
    wins rule is a defense-in-depth tiebreaker if Wikipedia ever adds an
    additional schema-matching table."""
    small_df = pd.DataFrame({
        "Symbol": ["X", "Y"],
        "GICS Sector": ["Energy", "Energy"],
    })
    real_df = pd.DataFrame({
        "Symbol": [f"T{i}" for i in range(100)],
        "GICS Sector": ["Industrials"] * 100,
    })
    picked = constituents._select_constituents_table([small_df, real_df], "S&P 500")
    assert len(picked) == 100


def test_select_constituents_table_flattens_multiindex() -> None:
    """Wikipedia occasionally returns multi-level column headers. The
    selector must flatten them before matching."""
    df = pd.DataFrame({
        ("Stock", "Symbol"): [f"T{i}" for i in range(100)],
        ("Classification", "GICS Sector"): ["Industrials"] * 100,
    })
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    picked = constituents._select_constituents_table([df], "S&P 500")
    assert any("symbol" in str(c).lower() for c in picked.columns)
    assert any(
        "gics" in str(c).lower() and "sector" in str(c).lower()
        for c in picked.columns
    )


def test_fetch_constituents_handles_banner_table_at_index_0() -> None:
    """End-to-end: Wikipedia page with a banner table at index 0 followed by
    the constituents table at index 1 must still produce a populated
    sector_map. Regression for the 2026-05-11 silent-MorningEnrich incident."""
    banner_df = pd.DataFrame({0: [float("nan")], 1: ["disambiguation banner"]})
    sp500_df = pd.DataFrame({
        "Symbol": [f"S5{i:02d}" for i in range(60)],
        "GICS Sector": ["Information Technology"] * 60,
    })
    sp400_df = pd.DataFrame({
        "Symbol": [f"S4{i:02d}" for i in range(60)],
        "GICS Sector": ["Industrials"] * 60,
    })

    sp500_html = banner_df.to_html(index=False) + sp500_df.to_html(index=False)
    sp400_html = banner_df.to_html(index=False) + sp400_df.to_html(index=False)

    def fake_get(url, **kwargs):
        return _FakeResp(sp500_html if "500" in url else sp400_html)

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sp500_count, sp400_count = (
            constituents._fetch_constituents()
        )

    assert sp500_count == 60
    assert sp400_count == 60
    assert len(sector_map) == 120
    assert sector_map["S500"] == "Information Technology"
    assert sector_map["S400"] == "Industrials"
    assert sector_etf_map["S500"] == "XLK"
    assert sector_etf_map["S400"] == "XLI"


def test_select_constituents_table_skips_banner_table() -> None:
    """Wikipedia adds banner/disambiguation tables ahead of the constituents
    table without notice. 2026-05-11 incident: the S&P 400 page inserted a
    1-row, 2-column disambiguation-warning table at index 0, making
    `tables[0]` return columns ``[0, 1]`` instead of the constituents
    table at index 1. _select_constituents_table must scan for the right
    one by columns, not position.
    """
    banner_df = pd.DataFrame({0: [float("nan")], 1: ["This article currently links to a large number of disambiguation pages."]})
    constituents_df = pd.DataFrame({
        "Symbol": [f"T{i:03d}" for i in range(100)],
        "Security": [f"Co {i}" for i in range(100)],
        "GICS Sector": ["Industrials"] * 100,
        "GICS Sub-Industry": ["Misc"] * 100,
    })
    sub_industry_only_df = pd.DataFrame({
        "Symbol": ["X"], "GICS Sub-Industry": ["Subindustry-Only"],
    })

    picked = constituents._select_constituents_table(
        [banner_df, constituents_df, sub_industry_only_df], "S&P 400"
    )
    assert list(picked.columns) == [
        "Symbol", "Security", "GICS Sector", "GICS Sub-Industry"
    ]
    assert len(picked) == 100


def test_select_constituents_table_raises_when_no_match() -> None:
    """If no table on the page has the expected ticker + GICS sector shape,
    raise loudly rather than silently picking a junk table."""
    banner_df = pd.DataFrame({0: [1], 1: ["banner"]})
    nav_df = pd.DataFrame({"vteFoo": ["a"], "vteBar": ["b"]})

    with pytest.raises(RuntimeError, match="No constituents table found"):
        constituents._select_constituents_table([banner_df, nav_df], "S&P 400")


def test_select_constituents_table_picks_largest_candidate() -> None:
    """When multiple tables have Symbol + GICS Sector columns (e.g. a small
    example/docs table alongside the live roster), pick the largest. On the
    real S&P 500/400 pages the roster is the only such table; the largest-
    wins rule is a defense-in-depth tiebreaker if Wikipedia ever adds an
    additional schema-matching table."""
    small_df = pd.DataFrame({
        "Symbol": ["X", "Y"],
        "GICS Sector": ["Energy", "Energy"],
    })
    real_df = pd.DataFrame({
        "Symbol": [f"T{i}" for i in range(100)],
        "GICS Sector": ["Industrials"] * 100,
    })
    picked = constituents._select_constituents_table([small_df, real_df], "S&P 500")
    assert len(picked) == 100


def test_select_constituents_table_flattens_multiindex() -> None:
    """Wikipedia occasionally returns multi-level column headers. The
    selector must flatten them before matching."""
    df = pd.DataFrame({
        ("Stock", "Symbol"): [f"T{i}" for i in range(100)],
        ("Classification", "GICS Sector"): ["Industrials"] * 100,
    })
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    picked = constituents._select_constituents_table([df], "S&P 500")
    assert any("symbol" in str(c).lower() for c in picked.columns)
    assert any(
        "gics" in str(c).lower() and "sector" in str(c).lower()
        for c in picked.columns
    )


def test_fetch_constituents_handles_banner_table_at_index_0() -> None:
    """End-to-end: Wikipedia page with a banner table at index 0 followed by
    the constituents table at index 1 must still produce a populated
    sector_map. Regression for the 2026-05-11 silent-MorningEnrich incident."""
    banner_df = pd.DataFrame({0: [float("nan")], 1: ["disambiguation banner"]})
    sp500_df = pd.DataFrame({
        "Symbol": [f"S5{i:02d}" for i in range(60)],
        "GICS Sector": ["Information Technology"] * 60,
    })
    sp400_df = pd.DataFrame({
        "Symbol": [f"S4{i:02d}" for i in range(60)],
        "GICS Sector": ["Industrials"] * 60,
    })

    sp500_html = banner_df.to_html(index=False) + sp500_df.to_html(index=False)
    sp400_html = banner_df.to_html(index=False) + sp400_df.to_html(index=False)

    def fake_get(url, **kwargs):
        return _FakeResp(sp500_html if "500" in url else sp400_html)

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sp500_count, sp400_count = (
            constituents._fetch_constituents()
        )

    assert sp500_count == 60
    assert sp400_count == 60
    assert len(sector_map) == 120
    assert sector_map["S500"] == "Information Technology"
    assert sector_map["S400"] == "Industrials"
    assert sector_etf_map["S500"] == "XLK"
    assert sector_etf_map["S400"] == "XLI"


def test_collect_returns_tickers_in_dict() -> None:
    """``collect()``'s return contract MUST include the ``tickers`` list.

    Pre-MorningEnrich preflight (PR #134, weekly_collector._run_morning_enrich)
    feeds these tickers directly to prune_delisted_tickers' constituents_override
    and to the daily_closes request list. Without ``tickers`` in the return,
    the caller silently gets [] and either prunes nothing or asks polygon for
    nothing. 2026-05-02 SF redrive #5 was this exact regression: collect()
    returned only ``{"status": "ok", "count": 903}``, the preflight logged
    'Pre-MorningEnrich constituents refresh: 0 tickers', and MorningEnrich
    aborted with 'No tickers available for morning enrichment'.

    Locks both happy paths (ok + ok_dry_run).
    """
    sp500_html = _fake_html(["AAPL", "MSFT"], ["Information Technology", "Information Technology"])
    sp400_html = _fake_html(["JHG", "WSO"], ["Financials", "Industrials"])

    def fake_get(url, **kwargs):
        return _FakeResp(sp500_html if "500" in url else sp400_html)

    # Dry-run path
    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        result = constituents.collect(bucket="any", dry_run=True)
    assert result["status"] == "ok_dry_run"
    assert "tickers" in result, (
        "ok_dry_run return MUST include tickers — preflight callers feed "
        "this directly into prune_delisted_tickers + daily_closes"
    )
    assert set(result["tickers"]) == {"AAPL", "MSFT", "JHG", "WSO"}

    # Non-dry-run path (S3 write mocked)
    with patch("collectors.constituents.requests.get", side_effect=fake_get), \
         patch("collectors.constituents.boto3"):
        result = constituents.collect(bucket="any", dry_run=False)
    assert result["status"] == "ok"
    assert "tickers" in result, (
        "ok return MUST include tickers — same contract as ok_dry_run; "
        "the count is only useful as observability, not as a tickers source"
    )
    assert set(result["tickers"]) == {"AAPL", "MSFT", "JHG", "WSO"}


def test_sector_map_writes_to_all_three_paths() -> None:
    """Wave-3 PR3 (ROADMAP L1401): sector_map.json must be written to:

      1. ``data/sector_map.json`` — canonical \"new\" data path.
      2. ``predictor/price_cache/sector_map.json`` — legacy path (retired
         in PR4).
      3. ``reference/price_cache/sector_map.json`` — Wave-3 new home for
         the predictor/price_cache/ migration. PR1 #270 missed this
         write (it scoped only the ticker-parquet writes); without it,
         readers that hit ``reference/`` first see a stale snapshot
         after PR4 deletes legacy.
    """
    from unittest.mock import MagicMock

    sp500_html = _fake_html(
        ["AAPL"], ["Information Technology"],
    )
    sp400_html = _fake_html(["JHG"], ["Financials"])

    def fake_get(url, **kwargs):
        return _FakeResp(sp500_html if "500" in url else sp400_html)

    put_calls: list[dict] = []
    fake_s3 = MagicMock()
    fake_s3.put_object.side_effect = lambda **kw: put_calls.append(kw)

    fake_boto3 = MagicMock()
    fake_boto3.client.return_value = fake_s3

    with patch("collectors.constituents.requests.get", side_effect=fake_get), \
         patch("collectors.constituents.boto3", fake_boto3):
        constituents.collect(bucket="any", dry_run=False)

    sector_map_writes = [
        c for c in put_calls if c["Key"].endswith("sector_map.json")
    ]
    written_keys = {c["Key"] for c in sector_map_writes}
    assert written_keys == {
        "data/sector_map.json",
        "predictor/price_cache/sector_map.json",
        "reference/price_cache/sector_map.json",
    }, written_keys
    # Bodies must be byte-equal — readers can pick any path safely.
    bodies = [c["Body"] for c in sector_map_writes]
    assert len(set(bodies)) == 1, "sector_map.json bodies diverge across paths"
