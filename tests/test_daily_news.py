"""Tests for collectors/daily_news.py — the weekday daily news producer.

Heavier integration of NewsAggregator + NLP + parquet writer is covered in their
own Wave 1 PRs; here we test the daily orchestrator shape: universe assembly
(holdings ∪ signals, fail-soft) and the collect() control flow with the network
+ S3 layers mocked.
"""

from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

from collectors import daily_news


def _mock_s3(holdings=None, signals_universe=None):
    """S3 mock serving the Metron holdings_universe.json key and a signals.json.

    The holdings object mirrors Metron's REAL published payload
    (``metron/holdings_universe.json`` — config#1506): the symbols-only ``tickers``
    slice the union consumes, alongside the ``holdings``/``currencies`` fields the
    market-data producer reads. daily_news must read ONLY ``tickers`` and ignore the rest.
    """
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "signals/2026-06-05/"}],
    }

    def _get_object(Bucket, Key):
        if Key == daily_news.HOLDINGS_UNIVERSE_KEY:
            if holdings is None:
                raise RuntimeError("NoSuchKey")
            payload = {
                "schema_version": 2,
                "as_of": "2026-07-02",
                "source": "metron",
                # Full Metron shape — daily_news must pick out `tickers` and ignore these.
                "holdings": [{"yf_symbol": t, "currency": "USD"} for t in holdings],
                "currencies": [],
                "tickers": holdings,
            }
            return {"Body": BytesIO(json.dumps(payload).encode())}
        if Key.endswith("signals.json"):
            uni = [{"ticker": t} for t in (signals_universe or [])]
            return {"Body": BytesIO(json.dumps({"universe": uni}).encode())}
        raise RuntimeError(f"unexpected key {Key}")

    s3.get_object.side_effect = _get_object
    return s3


def test_holdings_universe_key_points_at_metron():
    # config#1506: the held-ticker source is Metron's snapshot, NOT the retired
    # robodashboard producer (nousergon/metron-ops#119).
    assert daily_news.HOLDINGS_UNIVERSE_KEY == "metron/holdings_universe.json"


def test_assemble_universe_unions_dedupes_sorts():
    s3 = _mock_s3(holdings=["AAPL", "tsla"], signals_universe=["AAPL", "MSFT"])
    assert daily_news.assemble_universe("b", s3) == ["AAPL", "MSFT", "TSLA"]


def test_load_holdings_reads_tickers_slice_of_metron_payload():
    # daily_news consumes ONLY the symbols-only `tickers` slice of Metron's payload —
    # the `holdings`/`currencies` (yf-priced) fields are the market-data producer's view.
    s3 = _mock_s3(holdings=["AAPL", "nvda"], signals_universe=[])
    assert daily_news._load_holdings_universe("b", s3) == ["AAPL", "NVDA"]


def test_assemble_universe_fail_soft_no_holdings():
    # Missing holdings_universe.json → AE signals universe only (not an error).
    s3 = _mock_s3(holdings=None, signals_universe=["MSFT", "NVDA"])
    assert daily_news.assemble_universe("b", s3) == ["MSFT", "NVDA"]


def test_collect_skips_on_empty_universe():
    s3 = _mock_s3(holdings=[], signals_universe=[])
    out = daily_news.collect("b", s3_client=s3)
    assert out["status"] == "skipped"
    assert out["tickers"] == 0


def _fake_aggregator():
    # AsyncNewsAggregator.fetch is a coroutine — collect() drives it via
    # anyio.run, so the fetch seam must be awaitable.
    agg = MagicMock()
    agg.fetch = AsyncMock(return_value=[])
    return agg


def test_build_aggregator_is_async_concurrent_fanin():
    """The producer must use the concurrent AsyncNewsAggregator (3 sources
    overlapping), not the sequential sync aggregator — the L4567 timeout fix.
    """
    from collectors.news_aggregator_async import AsyncNewsAggregator

    with patch(
        "rag.pipelines.run_news_pipeline._load_ticker_name_map", return_value={}
    ):
        agg = daily_news._build_aggregator()
    assert isinstance(agg, AsyncNewsAggregator)
    assert set(agg.source_names) == {"polygon", "gdelt", "yahoo_rss"}


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_dry_run_does_not_write(mock_agg, mock_nlp, mock_ensure):
    mock_agg.return_value = _fake_aggregator()
    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3, dry_run=True)
    assert out["status"] == "ok_dry_run"
    assert out["tickers"] == 2


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_writes_to_daily_prefix(mock_agg, mock_nlp, mock_write, mock_ensure):
    mock_agg.return_value = _fake_aggregator()
    fake_df = MagicMock()
    fake_df.__len__ = lambda self: 7
    mock_write.return_value = ("data/news_aggregates_daily/run/result.parquet", fake_df)

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3)

    assert out["status"] == "ok"
    # Wrote to the DAILY prefix, NOT the Saturday data/news_aggregates prefix.
    assert mock_write.call_args.kwargs["prefix"] == daily_news.DAILY_PREFIX
    assert out["rows"] == 7


@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
@patch("collectors.daily_news.ensure_lm_master_dict")
def test_collect_fails_loud_when_lm_dict_unavailable(
    mock_ensure, mock_agg, mock_nlp, mock_write
):
    """A missing LM dict must NOT write an all-zero-sentiment artifact — the
    producer returns an error status and never reaches the writer (L4575)."""
    from collectors.nlp.loughran_mcdonald import LmDictUnavailable

    mock_ensure.side_effect = LmDictUnavailable("missing + S3 fetch failed")
    mock_agg.return_value = _fake_aggregator()
    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])

    out = daily_news.collect("b", s3_client=s3)

    assert out["status"] == "error"
    assert out["reason"] == "lm_dict_unavailable"
    mock_write.assert_not_called()  # no degraded artifact written
    mock_agg.assert_not_called()  # failed fast, before the ~17-min news pull


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_writes_raw_article_companion(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_ensure
):
    """The additive raw-article companion is written to the articles prefix
    alongside the aggregate, surfacing its key/rows on the status dict."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock()
    agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("data/news_aggregates_daily/run/result.parquet", agg_df)
    art_df = MagicMock()
    art_df.__len__ = lambda self: 12
    mock_articles.return_value = (
        "data/news_articles_daily/run/articles.parquet", art_df,
    )

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3)

    assert out["status"] == "ok"
    assert mock_articles.call_args.kwargs["prefix"] == daily_news.ARTICLES_PREFIX
    assert out["articles_status"] == "ok"
    assert out["articles_rows"] == 12
    assert out["articles_key"] == "data/news_articles_daily/run/articles.parquet"


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_article_companion_failure_is_fail_soft(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_ensure
):
    """A failure writing the raw-article companion must NOT fail the run —
    the aggregate (primary) already landed; status stays ok with the failure
    recorded on articles_status (per the fail-soft secondary-artifact rule)."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock()
    agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("data/news_aggregates_daily/run/result.parquet", agg_df)
    mock_articles.side_effect = RuntimeError("S3 hiccup")

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3)

    assert out["status"] == "ok"            # primary deliverable survived
    assert out["rows"] == 7
    assert out["articles_status"] == "error"  # failure recorded, not silent
    assert out["articles_key"] is None


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_digest.write_digest")
@patch("data.derived.news_digest.build_digest")
@patch("collectors.topic_news.fetch_topics")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_writes_combined_digest(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_topics,
    mock_build, mock_write_digest, mock_ensure,
):
    """collect() also produces the podcast-ready digest (portfolio + macro +
    tech) from the already-fetched article records, in the same flow."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock(); agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("data/news_aggregates_daily/run/result.parquet", agg_df)
    art_df = MagicMock(); art_df.__len__ = lambda self: 12
    mock_articles.return_value = ("data/news_articles_daily/run/articles.parquet", art_df)
    mock_topics.return_value = {"macro": [{"title": "m"}], "tech": [{"title": "t"}]}
    mock_build.return_value = {
        "sections": {"portfolio": [], "macro": [{"title": "m"}], "tech": [{"title": "t"}]}
    }
    mock_write_digest.return_value = "data/news_digest_daily/run/digest.json"

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3)

    assert out["status"] == "ok"
    assert out["digest_status"] == "ok"
    assert out["topic_status"] == "ok"
    assert out["digest_key"] == "data/news_digest_daily/run/digest.json"
    # The digest builder received the in-memory article DataFrame + topics.
    assert mock_build.call_args.kwargs["articles_df"] is art_df
    assert mock_build.call_args.kwargs["topics"]["macro"] == [{"title": "m"}]
    assert mock_write_digest.call_args.kwargs["prefix"] == daily_news.DIGEST_PREFIX


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_digest.write_digest")
@patch("data.derived.news_digest.build_digest")
@patch("collectors.topic_news.fetch_topics")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_digest_topic_failure_is_fail_soft(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_topics,
    mock_build, mock_write_digest, mock_ensure,
):
    """A topic-RSS failure must NOT block the digest: it's still written with
    the portfolio section populated and empty topics — recorded on
    topic_status, never silent, never fatal."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock(); agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("k/result.parquet", agg_df)
    art_df = MagicMock(); art_df.__len__ = lambda self: 12
    mock_articles.return_value = ("k/articles.parquet", art_df)
    mock_topics.side_effect = RuntimeError("RSS down")
    mock_build.return_value = {"sections": {"portfolio": [{}], "macro": [], "tech": []}}
    mock_write_digest.return_value = "data/news_digest_daily/run/digest.json"

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3)

    assert out["status"] == "ok"             # primary deliverable survived
    assert out["topic_status"] == "error"     # topic failure recorded
    assert out["digest_status"] == "ok"       # digest STILL written
    # Digest was built with empty topics despite the RSS failure.
    assert mock_build.call_args.kwargs["topics"] == {}
    mock_write_digest.assert_called_once()


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_digest.build_digest")
@patch("collectors.topic_news.fetch_topics")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_digest_write_failure_is_fail_soft(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_topics,
    mock_build, mock_ensure,
):
    """A failure building/writing the digest must NOT fail the run — the
    aggregate + article artifacts already landed; recorded on digest_status."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock(); agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("k/result.parquet", agg_df)
    art_df = MagicMock(); art_df.__len__ = lambda self: 12
    mock_articles.return_value = ("k/articles.parquet", art_df)
    mock_topics.return_value = {"macro": [], "tech": []}
    mock_build.side_effect = RuntimeError("schema bug")

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3)

    assert out["status"] == "ok"
    assert out["digest_status"] == "error"
    assert out["digest_key"] is None


# ── require_digest: digest promoted to a hard requirement of the run ──────────


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_digest.build_digest")
@patch("collectors.topic_news.fetch_topics")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_require_digest_fails_run_when_digest_write_errors(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_topics,
    mock_build, mock_ensure,
):
    """With require_digest, a digest build/write failure fails the whole run
    (so daily-news.service exits non-zero and morning-signal's Requires=
    blocks the pod) — unlike the default fail-soft path."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock(); agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("k/result.parquet", agg_df)
    art_df = MagicMock(); art_df.__len__ = lambda self: 12
    mock_articles.return_value = ("k/articles.parquet", art_df)
    mock_topics.return_value = {"macro": [], "tech": []}
    mock_build.side_effect = RuntimeError("schema bug")

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3, require_digest=True)

    assert out["status"] == "error"        # run fails → main() exits 1
    assert out["digest_status"] == "error"
    assert out["rows"] == 7                 # aggregate still landed for the dashboard


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_digest.write_digest")
@patch("data.derived.news_digest.build_digest")
@patch("collectors.topic_news.fetch_topics")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_require_digest_fails_run_when_digest_empty(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_topics,
    mock_build, mock_write_digest, mock_ensure,
):
    """An empty digest (zero items across all sections) fails the run under
    require_digest, even though the write itself succeeded."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock(); agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("k/result.parquet", agg_df)
    art_df = MagicMock(); art_df.__len__ = lambda self: 12
    mock_articles.return_value = ("k/articles.parquet", art_df)
    mock_topics.return_value = {"macro": [], "tech": []}
    mock_build.return_value = {"sections": {"portfolio": [], "macro": [], "tech": []}}
    mock_write_digest.return_value = "data/news_digest_daily/run/digest.json"

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3, require_digest=True)

    assert out["status"] == "error"
    assert out["digest_status"] == "ok"    # write succeeded; emptiness is the failure
    assert out["digest_total"] == 0


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_digest.write_digest")
@patch("data.derived.news_digest.build_digest")
@patch("collectors.topic_news.fetch_topics")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_require_digest_passes_when_digest_nonempty(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_topics,
    mock_build, mock_write_digest, mock_ensure,
):
    """A fresh, non-empty digest satisfies require_digest → run is ok."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock(); agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("k/result.parquet", agg_df)
    art_df = MagicMock(); art_df.__len__ = lambda self: 12
    mock_articles.return_value = ("k/articles.parquet", art_df)
    mock_topics.return_value = {"macro": [{"title": "m"}], "tech": []}
    mock_build.return_value = {"sections": {"portfolio": [{"ticker": "AAPL"}], "macro": [{"title": "m"}], "tech": []}}
    mock_write_digest.return_value = "data/news_digest_daily/run/digest.json"

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3, require_digest=True)

    assert out["status"] == "ok"
    assert out["digest_total"] == 2


@patch("collectors.daily_news.ensure_lm_master_dict")
@patch("data.derived.news_digest.write_digest")
@patch("data.derived.news_digest.build_digest")
@patch("collectors.topic_news.fetch_topics")
@patch("data.derived.news_articles.articles_build_and_write")
@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_default_stays_fail_soft_on_empty_digest(
    mock_agg, mock_nlp, mock_write, mock_articles, mock_topics,
    mock_build, mock_write_digest, mock_ensure,
):
    """Without require_digest (the SF path), an empty digest is still a soft
    degrade — status stays ok so the weekday SF isn't regressed."""
    mock_agg.return_value = _fake_aggregator()
    agg_df = MagicMock(); agg_df.__len__ = lambda self: 7
    mock_write.return_value = ("k/result.parquet", agg_df)
    art_df = MagicMock(); art_df.__len__ = lambda self: 12
    mock_articles.return_value = ("k/articles.parquet", art_df)
    mock_topics.return_value = {"macro": [], "tech": []}
    mock_build.return_value = {"sections": {"portfolio": [], "macro": [], "tech": []}}
    mock_write_digest.return_value = "data/news_digest_daily/run/digest.json"

    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3)  # require_digest defaults False

    assert out["status"] == "ok"
    assert out["digest_total"] == 0
