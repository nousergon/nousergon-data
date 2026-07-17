"""
Regression tests for constituents.py — membership + sector_map coverage.

config#2812: membership is now sourced from SSGA's SPY/MDY daily holdings
(ground truth, not subject to Wikipedia's community-edit lag — verified live
that Wikipedia still listed two 2026-07-01-delisted tickers 17+ days later,
while both SPY's and MDY's holdings had already dropped them). GICS sector +
sub-industry classification remains Wikipedia-sourced, now filtered down to
the SSGA-sourced membership list.

Bug (pre-config#2812, still relevant to the Wikipedia sector-fetch path):
prior to fix, _fetch_constituents only extracted GICS sectors from the S&P
500 Wikipedia table, leaving every S&P 400 mid-cap ticker without a sector
mapping. EOD reconcile's sector attribution depended on this map and
silently fell through to "Unknown" for any held mid-cap (e.g. JHG fired
flow-doctor on 2026-04-30).
"""
from __future__ import annotations

from io import BytesIO, StringIO
from unittest.mock import patch

import pandas as pd
import pytest

from collectors import constituents


def _fake_html(
    tickers: list[str], sectors: list[str], sub_industries: list[str] | None = None
) -> str:
    """Build minimal Wikipedia-shaped HTML with Symbol + GICS Sector columns.

    ``sub_industries``, when given, adds a "GICS Sub-Industry" column
    alongside "GICS Sector" — mirroring the real Wikipedia table shape
    (config#934 narrow slice).
    """
    data = {"Symbol": tickers, "GICS Sector": sectors}
    if sub_industries is not None:
        data["GICS Sub-Industry"] = sub_industries
    df = pd.DataFrame(data)
    return df.to_html(index=False)


def _fake_ssga_bytes(tickers: list[str]) -> bytes:
    """Build a minimal SSGA-holdings-shaped xlsx: a 4-row banner ahead of
    the real header row (mirrors ``skiprows=4`` in
    ``_fetch_ssga_membership``), then a ``Ticker`` column. Only the column
    that function actually reads is required."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame({"Ticker": tickers}).to_excel(writer, index=False, startrow=4)
    return buf.getvalue()


class _FakeResp:
    """Fake ``requests.Response`` supporting both the Wikipedia (``.text``,
    HTML) and SSGA (``.content``, xlsx bytes) fetch paths."""

    def __init__(self, *, text: str | None = None, content: bytes | None = None) -> None:
        self.text = text
        self.content = content

    def raise_for_status(self) -> None:
        pass


def _make_fake_get(sp500_tickers, sp400_tickers, sp500_sectors=None, sp400_sectors=None,
                    sp500_sub_industries=None, sp400_sub_industries=None):
    """Build a ``requests.get`` side_effect routing SSGA holdings URLs to a
    fake xlsx (membership) and Wikipedia URLs to fake HTML (sector
    classification), keyed off the same ticker lists by default so existing
    assertions (pre-config#2812) continue to hold unless a test explicitly
    diverges the two sources (e.g. to exercise the addition/removal-lag
    tolerance)."""
    sp500_sectors = sp500_sectors or ["Information Technology"] * len(sp500_tickers)
    sp400_sectors = sp400_sectors or ["Industrials"] * len(sp400_tickers)
    sp500_html = _fake_html(sp500_tickers, sp500_sectors, sp500_sub_industries)
    sp400_html = _fake_html(sp400_tickers, sp400_sectors, sp400_sub_industries)
    sp500_xlsx = _fake_ssga_bytes(sp500_tickers)
    sp400_xlsx = _fake_ssga_bytes(sp400_tickers)

    def fake_get(url, **kwargs):
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 500"]:
            return _FakeResp(content=sp500_xlsx)
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 400"]:
            return _FakeResp(content=sp400_xlsx)
        if url == constituents._WIKIPEDIA_URLS["S&P 500"]:
            return _FakeResp(text=sp500_html)
        if url == constituents._WIKIPEDIA_URLS["S&P 400"]:
            return _FakeResp(text=sp400_html)
        raise AssertionError(f"unexpected URL: {url}")

    return fake_get


def test_sector_map_covers_both_sp500_and_sp400() -> None:
    """sector_map must include every ticker from both index tables, and
    ``tickers`` (membership) is SSGA-sourced."""
    fake_get = _make_fake_get(
        ["AAPL", "MSFT"], ["JHG", "WSO"],
        sp500_sectors=["Information Technology", "Information Technology"],
        sp400_sectors=["Financials", "Industrials"],
    )

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sub_industry_map, sp500_count, sp400_count = (
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


def test_membership_is_ssga_sourced_not_wikipedia() -> None:
    """config#2812: a ticker present in Wikipedia's table but ABSENT from
    SSGA's holdings (the JHG/BLD failure mode — Wikipedia lagging a real
    delisting) must NOT appear in the final ``tickers`` list, even though
    it still gets a sector_map entry (harmlessly unused, filtered out)."""
    fake_get = _make_fake_get(
        sp500_tickers=["AAPL", "MSFT"],  # SSGA membership: no JHG
        sp400_tickers=["WSO"],
    )
    # Override: Wikipedia's S&P 500 page still lists a delisted ticker.
    stale_wiki_html = _fake_html(
        ["AAPL", "MSFT", "JHG"],
        ["Information Technology", "Information Technology", "Financials"],
    )

    def fake_get_with_stale_wiki(url, **kwargs):
        if url == constituents._WIKIPEDIA_URLS["S&P 500"]:
            return _FakeResp(text=stale_wiki_html)
        return fake_get(url, **kwargs)

    with patch("collectors.constituents.requests.get", side_effect=fake_get_with_stale_wiki):
        tickers, sector_map, _, _, _, _ = constituents._fetch_constituents()

    assert set(tickers) == {"AAPL", "MSFT", "WSO"}
    assert "JHG" not in tickers, (
        "a ticker Wikipedia still lists but SSGA has dropped must not leak "
        "into membership — this is the exact I2703/I2812 failure mode"
    )
    # Wikipedia's stale JHG row is harmless noise in the raw sector fetch,
    # but must be filtered out of the final (membership-scoped) sector_map.
    assert "JHG" not in sector_map


def test_sub_industry_map_captured_alongside_sector_map() -> None:
    """config#934 narrow slice: the collector must additionally capture the
    GICS Sub-Industry column (one level finer than sector, e.g.
    "Semiconductors" vs. the parent "Information Technology" sector) when
    Wikipedia's table carries it — purely additive, sector_map/sector_etf_map
    behavior must be unchanged."""
    fake_get = _make_fake_get(
        ["AAPL", "MSFT"], ["JHG", "WSO"],
        sp500_sectors=["Information Technology", "Information Technology"],
        sp400_sectors=["Financials", "Industrials"],
        sp500_sub_industries=["Technology Hardware, Storage & Peripherals", "Systems Software"],
        sp400_sub_industries=["Asset Management & Custody Banks", "Building Products"],
    )

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sub_industry_map, sp500_count, sp400_count = (
            constituents._fetch_constituents()
        )

    assert sub_industry_map["AAPL"] == "Technology Hardware, Storage & Peripherals"
    assert sub_industry_map["MSFT"] == "Systems Software"
    assert sub_industry_map["JHG"] == "Asset Management & Custody Banks"
    assert sub_industry_map["WSO"] == "Building Products"
    # sector_map/sector_etf_map are unaffected by sub-industry capture.
    assert sector_map["AAPL"] == "Information Technology"
    assert sector_etf_map["JHG"] == "XLF"


def test_sub_industry_map_empty_when_column_absent_does_not_raise() -> None:
    """A Wikipedia table without a GICS Sub-Industry column must NOT block
    the fetch — sub_industry_map degrades to empty/partial rather than
    raising, since nothing downstream depends on it yet (unlike the sector
    column, which is a hard gate)."""
    fake_get = _make_fake_get(["AAPL"], ["JHG"],
                               sp500_sectors=["Information Technology"],
                               sp400_sectors=["Financials"])

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, _, sub_industry_map, _, _ = constituents._fetch_constituents()

    assert set(tickers) == {"AAPL", "JHG"}
    assert sector_map == {"AAPL": "Information Technology", "JHG": "Financials"}
    assert sub_industry_map == {}


def test_cache_persists_sub_industry_map() -> None:
    """The local CSV cache must round-trip gics_sub_industry, so a future
    source outage's fallback still returns sub_industry_map (best-effort,
    consistent with how gics_sector/sector_etf already round-trip)."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        cache_path = Path(tmp) / "constituents_cache.csv"
        with patch("collectors.constituents._CACHE_PATH", cache_path):
            fake_get = _make_fake_get(
                ["AAPL"], ["JHG"],
                sp500_sectors=["Information Technology"],
                sp400_sectors=["Financials"],
                sp500_sub_industries=["Systems Software"],
                sp400_sub_industries=["Asset Management & Custody Banks"],
            )

            with patch("collectors.constituents.requests.get", side_effect=fake_get):
                constituents._fetch_constituents()

            assert cache_path.exists()
            cached = pd.read_csv(cache_path)
            assert "gics_sub_industry" in cached.columns
            row_by_ticker = {r["ticker"]: r for _, r in cached.iterrows()}
            assert row_by_ticker["AAPL"]["gics_sub_industry"] == "Systems Software"
            assert row_by_ticker["JHG"]["gics_sub_industry"] == "Asset Management & Custody Banks"

            # Fallback from cache must also reconstruct sub_industry_map.
            _, _, _, sub_industry_map, _, _ = constituents._load_from_cache()
            assert sub_industry_map == {
                "AAPL": "Systems Software",
                "JHG": "Asset Management & Custody Banks",
            }


def test_collect_raises_when_sector_coverage_gap_exceeds_tolerance(tmp_path) -> None:
    """If more than the addition-lag tolerance of tickers land in `tickers`
    without a sector entry, collect() must raise (systemic parse failure,
    not a couple of recent-addition stragglers)."""
    unmapped = [f"NEW{i}" for i in range(constituents._UNMAPPED_SECTOR_HARD_FAIL_THRESHOLD + 1)]

    def fake_fetch():
        return (
            ["AAPL", "MSFT", *unmapped],
            {"AAPL": "Information Technology", "MSFT": "Information Technology"},
            {"AAPL": "XLK", "MSFT": "XLK"},
            {},
            2,
            len(unmapped),
        )

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch):
        with pytest.raises(RuntimeError, match="Sector mapping incomplete"):
            constituents.collect(bucket="any", dry_run=True)


def test_collect_warns_but_proceeds_within_sector_gap_tolerance(tmp_path) -> None:
    """config#2812: a small number of SSGA-confirmed-current members
    missing a Wikipedia sector classification (addition-lag, verified live
    for TOST/IESC on the first real run of this fix) must NOT block
    collect() — only exceeding the tolerance does."""
    def fake_fetch():
        return (
            ["AAPL", "MSFT", "TOST"],
            {"AAPL": "Information Technology", "MSFT": "Information Technology"},
            {"AAPL": "XLK", "MSFT": "XLK"},
            {},
            2,
            1,
        )

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch):
        result = constituents.collect(bucket="any", dry_run=True)
    assert result["status"] == "ok_dry_run"
    assert "TOST" in result["tickers"]


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
    bad_wiki_html = df.to_html(index=False)
    ssga_xlsx = _fake_ssga_bytes(["AAPL"])

    def fake_get(url, **kwargs):
        if url in constituents._SSGA_HOLDINGS_URLS.values():
            return _FakeResp(content=ssga_xlsx)
        return _FakeResp(text=bad_wiki_html)

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, _, _, _, _ = constituents._fetch_constituents()

    # No Wikipedia tables matched the schema → the whole fetch falls
    # through to the cache (empty here) → empty result, which collect()
    # then short-circuits with status=error before any S3 write.
    assert tickers == []
    assert sector_map == {}


def test_cache_persists_sector_map_and_etf(tmp_path, monkeypatch) -> None:
    """On a successful fetch the local cache must persist ticker + GICS
    sector + sector ETF, so a future outage's fallback returns a
    fully-populated sector_map (instead of empty, which makes collect()
    raise 'Sector mapping incomplete')."""
    cache_path = tmp_path / "constituents_cache.csv"
    monkeypatch.setattr(constituents, "_CACHE_PATH", cache_path)

    fake_get = _make_fake_get(
        ["AAPL", "MSFT"], ["JHG", "WSO"],
        sp500_sectors=["Information Technology", "Information Technology"],
        sp400_sectors=["Financials", "Industrials"],
    )

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
    """When both sources are unreachable, the cache fallback must return
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
        raise RuntimeError("simulated outage")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sub_industry_map, sp500_count, sp400_count = (
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
        raise RuntimeError("simulated outage")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, _, _, _ = constituents._fetch_constituents()

    assert tickers == ["AAPL", "MSFT"]
    assert sector_map == {}
    assert sector_etf_map == {}


def test_cache_fallback_missing_cache_returns_empty(tmp_path, monkeypatch) -> None:
    """No source AND no cache → empty lists/dicts, not a crash. The
    eventual `collect()` short-circuit ('No tickers fetched') handles
    this state."""
    cache_path = tmp_path / "constituents_cache.csv"  # does not exist
    monkeypatch.setattr(constituents, "_CACHE_PATH", cache_path)

    def fake_get(url, **kwargs):
        raise RuntimeError("simulated outage")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, _, _, _ = constituents._fetch_constituents()

    assert tickers == []
    assert sector_map == {} and sector_etf_map == {}


def test_ssga_membership_filters_non_equity_rows() -> None:
    """SSGA holdings files carry non-equity noise rows (cash positions,
    tiny settlement placeholders, trailing legal-disclaimer text) that must
    never leak into membership. Verified live 2026-07-17 against the real
    SPY/MDY files: '-'/CASH_USD cash rows, a CUSIP-shaped placeholder
    ticker, and multi-paragraph disclaimer rows with NaN ticker."""
    buf = BytesIO()
    noisy = pd.DataFrame({
        "Ticker": ["AAPL", "-", "CASH_USD", "2602335D", None, "MSFT"],
        "Name": ["APPLE INC", "US DOLLAR", "U.S. Dollar", "CONTRA HOLOGIC",
                 "Legal disclaimer text...", "MICROSOFT CORP"],
    })
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        noisy.to_excel(writer, index=False, startrow=4)

    def fake_get(url, **kwargs):
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 500"]:
            return _FakeResp(content=buf.getvalue())
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 400"]:
            return _FakeResp(content=_fake_ssga_bytes([]))
        raise AssertionError(f"unexpected URL in membership-only test: {url}")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        with pytest.raises(RuntimeError, match="zero valid tickers"):
            # S&P 400 leg yields zero tickers post-filter → raises, per
            # _fetch_ssga_membership's own empty-batch guard.
            constituents._fetch_ssga_membership()


def test_ssga_membership_filters_non_equity_rows_both_legs_populated() -> None:
    """Same noise-filtering check, with a non-empty S&P 400 leg so the
    function returns normally and the filtered ticker set can be asserted."""
    buf = BytesIO()
    noisy = pd.DataFrame({
        "Ticker": ["AAPL", "-", "CASH_USD", "2602335D", None, "MSFT"],
    })
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        noisy.to_excel(writer, index=False, startrow=4)

    def fake_get(url, **kwargs):
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 500"]:
            return _FakeResp(content=buf.getvalue())
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 400"]:
            return _FakeResp(content=_fake_ssga_bytes(["JHG", "WSO"]))
        raise AssertionError(f"unexpected URL: {url}")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sp500_count, sp400_count = constituents._fetch_ssga_membership()

    assert set(tickers) == {"AAPL", "MSFT", "JHG", "WSO"}
    assert sp500_count == 2
    assert sp400_count == 2


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


def test_fetch_wikipedia_sectors_handles_banner_table_at_index_0() -> None:
    """End-to-end: Wikipedia page with a banner table at index 0 followed by
    the constituents table at index 1 must still produce a populated
    sector_map. Regression for the 2026-05-11 silent-MorningEnrich incident.
    (Retargeted to ``_fetch_wikipedia_sectors`` directly — config#2812 split
    membership out of this function, so a pure Wikipedia-parsing regression
    test no longer needs an SSGA mock.)"""
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
        return _FakeResp(text=sp500_html if "500" in url else sp400_html)

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        sector_map, sector_etf_map, sub_industry_map = constituents._fetch_wikipedia_sectors()

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
    fake_get = _make_fake_get(
        ["AAPL", "MSFT"], ["JHG", "WSO"],
        sp500_sectors=["Information Technology", "Information Technology"],
        sp400_sectors=["Financials", "Industrials"],
    )

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


def test_sector_map_writes_to_canonical_paths_only() -> None:
    """PR4 cutover (config#780): sector_map.json must be written to:

      1. ``data/sector_map.json`` — canonical \"new\" data path.
      2. ``reference/price_cache/sector_map.json`` — Wave-3 home for
         the predictor/price_cache/ migration.

    The legacy ``predictor/price_cache/sector_map.json`` write is gone —
    it was the one straggler still recreating the deleted legacy prefix
    on every weekly run after the ticker-parquet side (via
    ``_price_cache_write_prefixes()``) had already cut over to
    ``reference/`` only.
    """
    from unittest.mock import MagicMock

    fake_get = _make_fake_get(
        ["AAPL"], ["JHG"],
        sp500_sectors=["Information Technology"],
        sp400_sectors=["Financials"],
    )

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
        "reference/price_cache/sector_map.json",
    }, written_keys
    # Bodies must be byte-equal — readers can pick any path safely.
    bodies = [c["Body"] for c in sector_map_writes]
    assert len(set(bodies)) == 1, "sector_map.json bodies diverge across paths"
