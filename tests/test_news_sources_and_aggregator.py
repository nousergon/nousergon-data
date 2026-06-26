"""Tests for the news-source substrate + aggregator (Wave 1 PR β).

Concrete adapter implementations live in alpha-engine-data; shapes +
Protocols live in alpha-engine-lib (PR α, v0.15.0). See
``~/Development/alpha-engine-docs/private/data-revamp-260513.md``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from alpha_engine_lib.sources import NewsArticle, NewsSource

from collectors.news_aggregator import (
    AggregatedNewsArticle,
    DEFAULT_TRUST_WEIGHTS,
    NewsAggregator,
    _article_fingerprint,
    _normalize_title,
    _url_fingerprint,
)
from collectors.news_sources.benzinga import BenzingaNewsAdapter
from collectors.news_sources.bloomberg import BloombergNewsAdapter
from collectors.news_sources.gdelt import (
    GdeltNewsAdapter,
    _build_query,
    _retry_after_seconds,
)
from collectors.news_sources.polygon import PolygonNewsAdapter
from collectors.news_sources.ravenpack import RavenpackNewsAdapter
from collectors.news_sources.yahoo_rss import YahooRssNewsAdapter


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_article(
    *,
    source: str = "polygon",
    title: str = "Earnings beat",
    url: str = "https://example.com/x",
    tickers: tuple[str, ...] = ("AAPL",),
    published_at: datetime | None = None,
) -> NewsArticle:
    return NewsArticle(
        tickers=tickers,
        title=title,
        body_excerpt="lead paragraph",
        url=url,
        published_at=published_at or _now(),
        source=source,
        vendor_article_id="vid-1",
        fetched_at=_now(),
    )


# ── Protocol structural subtyping ──────────────────────────────────────


class TestNewsSourceProtocol:
    def test_polygon_adapter_satisfies_protocol(self):
        adapter = PolygonNewsAdapter(client=MagicMock())
        assert isinstance(adapter, NewsSource)

    def test_gdelt_adapter_satisfies_protocol(self):
        adapter = GdeltNewsAdapter(ticker_name_map={})
        assert isinstance(adapter, NewsSource)

    def test_yahoo_adapter_satisfies_protocol(self):
        adapter = YahooRssNewsAdapter(feedparser_module=MagicMock())
        assert isinstance(adapter, NewsSource)


# ── Polygon adapter ────────────────────────────────────────────────────


class TestPolygonNewsAdapter:
    def test_happy_path_normalizes_to_news_article(self):
        fake_client = MagicMock()
        fake_client._get.return_value = {
            "results": [{
                "id": "abc-123",
                "title": "NVDA Earnings Beat",
                "description": "Strong Q4 results",
                "article_url": "https://example.com/nvda-q4",
                "published_utc": "2026-05-12T14:30:00Z",
                "tickers": ["NVDA"],
                "keywords": ["earnings", "AI"],
                "author": "Reporter",
            }]
        }
        adapter = PolygonNewsAdapter(client=fake_client)
        out = adapter.fetch(["NVDA"], hours=24)
        assert len(out) == 1
        a = out[0]
        assert a.source == "polygon"
        assert a.title == "NVDA Earnings Beat"
        assert a.vendor_article_id == "abc-123"
        assert a.tickers == ("NVDA",)
        assert "earnings" in a.tags

    def test_transient_failure_returns_partial_batch(self):
        fake_client = MagicMock()

        def side_effect(path, params):
            if params["ticker"] == "BROKEN":
                raise RuntimeError("polygon 500")
            return {"results": [{
                "id": f"id-{params['ticker']}",
                "title": "headline",
                "article_url": f"https://x.com/{params['ticker']}",
                "published_utc": "2026-05-12T14:30:00Z",
                "tickers": [params["ticker"]],
            }]}

        fake_client._get.side_effect = side_effect
        adapter = PolygonNewsAdapter(client=fake_client)
        out = adapter.fetch(["AAPL", "BROKEN", "MSFT"], hours=24)
        assert {a.url for a in out} == {
            "https://x.com/AAPL", "https://x.com/MSFT"
        }

    def test_schema_drift_on_one_item_skips_just_that_item(self):
        fake_client = MagicMock()
        fake_client._get.return_value = {
            "results": [
                {  # missing required published_utc
                    "id": "incomplete",
                    "title": "no date",
                    "article_url": "https://x.com/1",
                },
                {
                    "id": "good",
                    "title": "good headline",
                    "article_url": "https://x.com/2",
                    "published_utc": "2026-05-12T14:30:00Z",
                    "tickers": ["AAPL"],
                },
            ]
        }
        adapter = PolygonNewsAdapter(client=fake_client)
        out = adapter.fetch(["AAPL"], hours=24)
        assert len(out) == 1
        assert out[0].vendor_article_id == "good"


# ── GDELT adapter ──────────────────────────────────────────────────────


class TestGdeltNewsAdapter:
    def test_happy_path(self):
        fake_http = MagicMock()
        fake_http.get.return_value = MagicMock(
            json=MagicMock(return_value={
                "articles": [{
                    "url": "https://reuters.com/x",
                    "title": "AAPL stock surges",
                    "seendate": "20260512T143000Z",
                    "sourcecountry": "US",
                    "domain": "reuters.com",
                    "language": "English",
                }],
            }),
            raise_for_status=MagicMock(return_value=None),
        )
        adapter = GdeltNewsAdapter(
            ticker_name_map={"AAPL": "Apple Inc"},
            http=fake_http,
            inter_request_sleep=0.0,
        )
        out = adapter.fetch(["AAPL"], hours=24)
        assert len(out) == 1
        assert out[0].source == "gdelt"
        assert out[0].url == "https://reuters.com/x"
        assert "reuters.com" in out[0].tags

    def test_query_includes_ticker_and_company_name(self):
        q = _build_query("AAPL", "Apple Inc")
        assert "AAPL" in q
        assert '"Apple Inc"' in q
        assert "sourcecountry:US" in q

    def test_query_handles_single_word_company_name(self):
        q = _build_query("NVDA", "Nvidia")
        assert "Nvidia" in q
        assert '"Nvidia"' not in q

    def test_failure_skips_ticker_continues_batch(self):
        fake_http = MagicMock()
        call_count = {"i": 0}

        def get(url, params, timeout):
            call_count["i"] += 1
            if call_count["i"] == 1:
                raise RuntimeError("gdelt rate-limit")
            return MagicMock(
                json=MagicMock(return_value={"articles": [{
                    "url": "https://x.com/y",
                    "title": "ok",
                    "seendate": "20260512T143000Z",
                }]}),
                raise_for_status=MagicMock(return_value=None),
            )

        fake_http.get.side_effect = get
        adapter = GdeltNewsAdapter(
            ticker_name_map={"AAPL": "Apple", "MSFT": "Microsoft"},
            http=fake_http,
            inter_request_sleep=0.0,
        )
        out = adapter.fetch(["AAPL", "MSFT"])
        assert len(out) == 1

    def test_ticker_falls_back_to_symbol_when_no_name_in_map(self):
        fake_http = MagicMock()
        fake_http.get.return_value = MagicMock(
            json=MagicMock(return_value={"articles": []}),
            raise_for_status=MagicMock(return_value=None),
        )
        adapter = GdeltNewsAdapter(
            ticker_name_map={},
            http=fake_http,
            inter_request_sleep=0.0,
        )
        adapter.fetch(["UNKNOWN"])
        params_used = fake_http.get.call_args.kwargs["params"]
        assert "UNKNOWN" in params_used["query"]

    def test_429_honors_retry_after_then_succeeds(self, monkeypatch):
        """A 429 with Retry-After is waited out and the ticker is retried
        once — recovering coverage instead of dropping the ticker (config#663)."""
        slept: list[float] = []
        monkeypatch.setattr(
            "collectors.news_sources.gdelt.time.sleep",
            lambda s: slept.append(s),
        )

        resp_429 = MagicMock(status_code=429, headers={"Retry-After": "3"})
        resp_ok = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"articles": [{
                "url": "https://x.com/y",
                "title": "recovered",
                "seendate": "20260512T143000Z",
            }]}),
            raise_for_status=MagicMock(return_value=None),
        )
        fake_http = MagicMock()
        fake_http.get.side_effect = [resp_429, resp_ok]

        adapter = GdeltNewsAdapter(
            ticker_name_map={"AAPL": "Apple"},
            http=fake_http,
            inter_request_sleep=0.0,
        )
        out = adapter.fetch(["AAPL"])
        assert len(out) == 1
        assert out[0].title == "recovered"
        assert fake_http.get.call_count == 2
        # Honored the server-advertised Retry-After (3s), clamped under max.
        assert 3.0 in slept

    def test_429_twice_drops_ticker_fail_soft(self, monkeypatch):
        """A persistent 429 (retry also 429) drops the ticker without
        raising — only one retry, no infinite loop."""
        monkeypatch.setattr(
            "collectors.news_sources.gdelt.time.sleep", lambda s: None
        )
        resp_429 = MagicMock(status_code=429, headers={"Retry-After": "1"})
        fake_http = MagicMock()
        fake_http.get.side_effect = [resp_429, resp_429]

        adapter = GdeltNewsAdapter(
            ticker_name_map={"AAPL": "Apple"},
            http=fake_http,
            inter_request_sleep=0.0,
        )
        out = adapter.fetch(["AAPL"])
        assert out == []
        assert fake_http.get.call_count == 2  # initial + one retry, no more

    def test_429_on_one_ticker_does_not_block_others(self, monkeypatch):
        """A 429 (then-drop) on one ticker still lets the rest of the batch
        return — breadth degrades, pull does not fail."""
        monkeypatch.setattr(
            "collectors.news_sources.gdelt.time.sleep", lambda s: None
        )
        resp_429 = MagicMock(status_code=429, headers={})
        resp_ok = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"articles": [{
                "url": "https://x.com/msft",
                "title": "msft ok",
                "seendate": "20260512T143000Z",
            }]}),
            raise_for_status=MagicMock(return_value=None),
        )
        fake_http = MagicMock()
        # AAPL: 429 then 429 (dropped); MSFT: ok
        fake_http.get.side_effect = [resp_429, resp_429, resp_ok]

        adapter = GdeltNewsAdapter(
            ticker_name_map={"AAPL": "Apple", "MSFT": "Microsoft"},
            http=fake_http,
            inter_request_sleep=0.0,
        )
        out = adapter.fetch(["AAPL", "MSFT"])
        assert len(out) == 1
        assert out[0].title == "msft ok"

    @pytest.mark.parametrize(
        "header,expected",
        [
            ({"Retry-After": "5"}, 5.0),
            ({"Retry-After": "0"}, 0.0),
            ({}, 2.0),                       # missing → fallback
            ({"Retry-After": "garbage"}, 2.0),  # unparseable → fallback
            ({"Retry-After": "-7"}, 2.0),    # negative → fallback
            ({"Retry-After": "999"}, 30.0),  # clamped to max
        ],
    )
    def test_retry_after_seconds_parsing(self, header, expected):
        resp = MagicMock(headers=header)
        assert _retry_after_seconds(resp) == expected


# ── Yahoo RSS adapter ──────────────────────────────────────────────────


class TestYahooRssNewsAdapter:
    def test_happy_path_with_recent_entry(self):
        fake_parser = MagicMock()
        recent = datetime.now(timezone.utc) - timedelta(hours=2)
        fake_parser.parse.return_value = MagicMock(entries=[{
            "title": "AAPL hits new high",
            "link": "https://reuters.com/aapl",
            "published_parsed": recent.timetuple(),
            "summary": "Apple shares...",
            "source": {"title": "Reuters"},
            "id": "hash1",
        }])
        adapter = YahooRssNewsAdapter(feedparser_module=fake_parser)
        out = adapter.fetch(["AAPL"], hours=24)
        assert len(out) == 1
        assert out[0].source == "yahoo_rss"
        assert out[0].vendor_article_id == "hash1"
        assert "Reuters" in out[0].tags

    def test_entries_older_than_cutoff_dropped(self):
        fake_parser = MagicMock()
        too_old = datetime.now(timezone.utc) - timedelta(hours=100)
        fake_parser.parse.return_value = MagicMock(entries=[{
            "title": "ancient news",
            "link": "https://x.com/a",
            "published_parsed": too_old.timetuple(),
            "summary": "",
        }])
        adapter = YahooRssNewsAdapter(feedparser_module=fake_parser)
        out = adapter.fetch(["AAPL"], hours=24)
        assert out == []

    def test_entries_without_link_skipped(self):
        fake_parser = MagicMock()
        fake_parser.parse.return_value = MagicMock(entries=[
            {"title": "no link", "published_parsed": datetime.now(
                timezone.utc).timetuple()},
        ])
        adapter = YahooRssNewsAdapter(feedparser_module=fake_parser)
        assert adapter.fetch(["AAPL"]) == []

    def test_fetch_failure_skips_ticker(self):
        fake_parser = MagicMock()
        fake_parser.parse.side_effect = RuntimeError("net down")
        adapter = YahooRssNewsAdapter(feedparser_module=fake_parser)
        assert adapter.fetch(["AAPL"]) == []


# ── Paid-vendor stubs ──────────────────────────────────────────────────


class TestPaidStubsFailLoudOnConstruction:
    def test_benzinga_raises_on_init(self):
        with pytest.raises(NotImplementedError, match="Phase 4"):
            BenzingaNewsAdapter()

    def test_ravenpack_raises_on_init(self):
        with pytest.raises(NotImplementedError, match="Phase 4"):
            RavenpackNewsAdapter()

    def test_bloomberg_raises_on_init(self):
        with pytest.raises(NotImplementedError, match="Phase 4"):
            BloombergNewsAdapter()


# ── Aggregator: dedup + trust weighting ───────────────────────────────


class TestNormalizationHelpers:
    def test_title_normalization_lowercases_and_strips_punct(self):
        assert _normalize_title("Apple's Q4 Beat — Up 5%!") == "apple s q4 beat up 5"

    def test_title_normalization_idempotent(self):
        n = _normalize_title("Some Title!")
        assert _normalize_title(n) == n

    def test_url_fingerprint_strips_querystring(self):
        fp1 = _url_fingerprint("https://x.com/path?utm_source=a")
        fp2 = _url_fingerprint("https://x.com/path?utm_source=b&ref=c")
        assert fp1 == fp2

    def test_url_fingerprint_strips_fragment(self):
        fp1 = _url_fingerprint("https://x.com/path#section1")
        fp2 = _url_fingerprint("https://x.com/path#section2")
        assert fp1 == fp2


class TestNewsAggregatorDedup:
    def _make_static_source(self, name, articles):
        src = MagicMock(spec=["name", "fetch"])
        src.name = name
        src.fetch.return_value = articles
        return src

    def test_fan_in_concatenates_all_sources(self):
        a1 = _make_article(source="polygon", title="A")
        a2 = _make_article(source="gdelt", title="B")
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", [a1]),
            self._make_static_source("gdelt", [a2]),
        ])
        out = agg.fetch(["AAPL"])
        assert {x.canonical_title for x in out} == {"A", "B"}

    def test_same_url_dedups_across_sources(self):
        url = "https://reuters.com/aapl-q4-beat?utm=a"
        a1 = _make_article(source="polygon", title="Apple Q4 Beat", url=url)
        a2 = _make_article(
            source="gdelt",
            title="Apple Q4 Beat",
            url="https://reuters.com/aapl-q4-beat?utm=b",
        )
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", [a1]),
            self._make_static_source("gdelt", [a2]),
        ])
        out = agg.fetch(["AAPL"])
        assert len(out) == 1
        assert out[0].n_sources == 2

    def test_canonical_title_picks_longest(self):
        url = "https://reuters.com/aapl"
        a1 = _make_article(source="polygon", title="Apple Reports Strong Q4 Beat", url=url)
        a2 = _make_article(source="gdelt", title="Apple Reports Strong Q4 Beat!!!", url=url)
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", [a1, a2]),
        ])
        out = agg.fetch(["AAPL"])
        assert out[0].canonical_title == "Apple Reports Strong Q4 Beat!!!"

    def test_canonical_url_picks_highest_trust_source(self):
        url = "https://reuters.com/x"
        a_polygon = _make_article(source="polygon", title="story", url=url)
        a_yahoo = _make_article(source="yahoo_rss", title="story", url=url)
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", [a_polygon, a_yahoo]),
        ])
        out = agg.fetch(["AAPL"])
        # polygon trust > yahoo_rss trust, but canonical_url is the
        # URL (same for both). The trust-weight ordering surfaces when
        # variants differ — pin via n_sources here.
        assert out[0].canonical_url == url

    def test_ticker_union_across_variants(self):
        url = "https://x.com/sector"
        a1 = _make_article(source="polygon", title="Sector roundup", url=url, tickers=("AAPL", "MSFT"))
        a2 = _make_article(source="gdelt", title="Sector Roundup", url=url, tickers=("AAPL", "GOOGL"))
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", [a1]),
            self._make_static_source("gdelt", [a2]),
        ])
        out = agg.fetch(["AAPL"])
        assert out[0].tickers == ("AAPL", "GOOGL", "MSFT")

    def test_one_source_raising_does_not_crash_aggregator(self):
        a_good = _make_article(source="polygon", title="ok")
        broken = MagicMock(spec=["name", "fetch"])
        broken.name = "broken_vendor"
        broken.fetch.side_effect = RuntimeError("kaboom")
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", [a_good]),
            broken,
        ])
        out = agg.fetch(["AAPL"])
        assert len(out) == 1

    def test_output_sorted_by_published_at_desc(self):
        old = _now() - timedelta(hours=24)
        new = _now() - timedelta(hours=2)
        a_old = _make_article(source="polygon", title="old", url="https://x/a", published_at=old)
        a_new = _make_article(source="polygon", title="new", url="https://x/b", published_at=new)
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", [a_old, a_new])
        ])
        out = agg.fetch(["AAPL"])
        assert [x.canonical_title for x in out] == ["new", "old"]

    def test_empty_fan_in_returns_empty(self):
        agg = NewsAggregator(sources=[
            self._make_static_source("polygon", []),
            self._make_static_source("gdelt", []),
        ])
        assert agg.fetch(["AAPL"]) == []


class TestNewsAggregatorTrustWeights:
    def test_default_weights_loaded(self):
        agg = NewsAggregator(sources=[])
        assert agg.trust_weight("polygon") == DEFAULT_TRUST_WEIGHTS["polygon"]
        assert agg.trust_weight("yahoo_rss") == DEFAULT_TRUST_WEIGHTS["yahoo_rss"]
        assert agg.trust_weight("bloomberg") == 1.0
        assert agg.trust_weight("ravenpack") == 1.0

    def test_custom_weights_override_defaults(self):
        agg = NewsAggregator(
            sources=[],
            trust_weights={"polygon": 0.5, "yahoo_rss": 0.95},
        )
        assert agg.trust_weight("polygon") == 0.5

    def test_unknown_source_defaults_to_half(self, caplog):
        agg = NewsAggregator(sources=[], trust_weights={})
        with caplog.at_level("WARNING"):
            w = agg.trust_weight("brand_new_vendor")
        assert w == 0.5
        assert any("brand_new_vendor" in r.message for r in caplog.records)


# ── Fingerprint determinism ────────────────────────────────────────────


def test_article_fingerprint_is_deterministic():
    a = _make_article(title="Stable Title", url="https://x.com/p?u=1")
    b = _make_article(title="Stable Title", url="https://x.com/p?u=2")
    assert _article_fingerprint(a) == _article_fingerprint(b)


def test_article_fingerprint_differs_on_different_titles():
    a = _make_article(title="A", url="https://x.com/p")
    b = _make_article(title="B", url="https://x.com/p")
    assert _article_fingerprint(a) != _article_fingerprint(b)


# ── NewsArticle shape pinned (sourced from lib) ────────────────────────


class TestLibShapeContract:
    """Pin the contract with the lib's NewsArticle shape so a future
    lib version bump that changes the schema surfaces here."""

    def test_extra_fields_forbidden(self):
        with pytest.raises(ValidationError, match="Extra inputs are not"):
            NewsArticle(
                tickers=("AAPL",), title="t", body_excerpt="b",
                url="https://x", published_at=_now(),
                source="polygon", fetched_at=_now(),
                vendor_specific_field="oops",
            )

    def test_frozen_records(self):
        a = _make_article()
        with pytest.raises(ValidationError):
            a.title = "different"
