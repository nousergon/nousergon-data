"""Tests for the news → RAG ingest pipeline (Wave 1 PR A.3).

Covers:
  - One document per (ticker, article) — multi-ticker articles emit
    one doc per ticker
  - Idempotency: document_exists short-circuits the embed + ingest call
  - Empty / too-short bodies skipped
  - Chunk text = title + longest body excerpt across variants
  - Canonical source picks deterministically (alphabetical fallback)
  - RAG `source` field prefixed with 'news_'
  - dry_run mode skips embed/ingest
  - sector lookup uses optional ticker_to_sector map
  - Failures isolated per-document, batch continues
  - Stats dict shape
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from nousergon_lib.sources import NewsArticle

from collectors.news_aggregator import AggregatedNewsArticle
from rag.pipelines.ingest_news import (
    _canonical_source,
    _chunk_text,
    _rag_source,
    ingest_articles,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_variant(source: str = "polygon", body: str = "lead paragraph") -> NewsArticle:
    return NewsArticle(
        tickers=("AAPL",),
        title="t",
        body_excerpt=body,
        url="https://x.com/a",
        published_at=_now(),
        source=source,
        fetched_at=_now(),
    )


def _make_article(
    *,
    fingerprint: str = "fp1",
    title: str = "Apple Q4 Beat",
    tickers: tuple[str, ...] = ("AAPL",),
    variants: tuple[NewsArticle, ...] | None = None,
    body: str = "Strong quarterly results across all segments.",
) -> AggregatedNewsArticle:
    if variants is None:
        variants = (_make_variant(body=body),)
    return AggregatedNewsArticle(
        canonical_title=title,
        canonical_url="https://x.com/a",
        tickers=tickers,
        earliest_published_at=_now(),
        variants=variants,
        canonical_fingerprint=fingerprint,
    )


# ── Helpers ────────────────────────────────────────────────────────────


class TestHelpers:
    def test_rag_source_prefixed(self):
        assert _rag_source("polygon") == "news_polygon"
        assert _rag_source("gdelt") == "news_gdelt"
        assert _rag_source("yahoo_rss") == "news_yahoo_rss"

    def test_chunk_text_combines_title_and_longest_body(self):
        article = _make_article(
            title="Apple beats",
            variants=(
                _make_variant(source="polygon", body="short"),
                _make_variant(source="gdelt", body="much longer body with more semantic context"),
            ),
        )
        text = _chunk_text(article)
        assert "Apple beats" in text
        assert "much longer body with more semantic context" in text
        # Picks longest, drops shorter
        assert "short" not in text

    def test_chunk_text_handles_missing_title(self):
        article = AggregatedNewsArticle(
            canonical_title="",
            canonical_url="https://x",
            tickers=("AAPL",),
            earliest_published_at=_now(),
            variants=(_make_variant(body="body"),),
            canonical_fingerprint="fp",
        )
        assert _chunk_text(article) == "body"

    def test_chunk_text_empty_when_all_empty(self):
        article = AggregatedNewsArticle(
            canonical_title="",
            canonical_url="https://x",
            tickers=("AAPL",),
            earliest_published_at=_now(),
            variants=(_make_variant(body=""),),
            canonical_fingerprint="fp",
        )
        assert _chunk_text(article) == ""

    def test_canonical_source_deterministic_alphabetical(self):
        """Re-ingesting the same article on different runs must
        produce the same source — pick alphabetically across variants."""
        article = _make_article(
            variants=(
                _make_variant(source="polygon"),
                _make_variant(source="gdelt"),
                _make_variant(source="yahoo_rss"),
            ),
        )
        assert _canonical_source(article) == "gdelt"

    def test_canonical_source_single_variant(self):
        article = _make_article(variants=(_make_variant(source="polygon"),))
        assert _canonical_source(article) == "polygon"


# ── Single-ticker happy path ───────────────────────────────────────────


class TestSingleTickerIngest:
    def test_one_article_one_ticker_one_document(self):
        article = _make_article(tickers=("AAPL",))
        embed = MagicMock(return_value=[[0.1, 0.2, 0.3]])
        exists = MagicMock(return_value=False)
        ingest = MagicMock(return_value="doc-id-1")

        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=ingest,
        )

        assert stats["n_documents_ingested"] == 1
        assert stats["n_failures"] == 0
        # Embedder called once with one chunk's content
        embed.assert_called_once()
        # ingest_document called with the expected shape
        ingest.assert_called_once()
        kwargs = ingest.call_args.kwargs
        assert kwargs["ticker"] == "AAPL"
        assert kwargs["doc_type"] == "news"
        assert kwargs["source"] == "news_polygon"
        assert kwargs["filed_date"] == date(2026, 5, 13)
        assert len(kwargs["chunks"]) == 1
        assert kwargs["chunks"][0]["embedding"] == [0.1, 0.2, 0.3]

    def test_idempotency_via_document_exists(self):
        """document_exists returning True short-circuits both the
        embedder + ingest_document — saves vector-API cost on re-runs."""
        article = _make_article()
        embed = MagicMock()
        exists = MagicMock(return_value=True)
        ingest = MagicMock()

        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=ingest,
        )

        assert stats["n_documents_skipped_exists"] == 1
        assert stats["n_documents_ingested"] == 0
        # Embedder NOT called (cost-saving)
        embed.assert_not_called()
        ingest.assert_not_called()

    def test_empty_body_skipped(self):
        article = AggregatedNewsArticle(
            canonical_title="",
            canonical_url="https://x",
            tickers=("AAPL",),
            earliest_published_at=_now(),
            variants=(_make_variant(body=""),),
            canonical_fingerprint="empty",
        )
        embed = MagicMock()
        ingest = MagicMock()

        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_skipped_empty_text"] == 1
        assert stats["n_documents_ingested"] == 0
        embed.assert_not_called()

    def test_too_short_body_skipped(self):
        article = _make_article(title="", body="hi")
        embed = MagicMock()
        ingest = MagicMock()
        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        # Title="" + body="hi" → text < 20 chars → skipped
        assert stats["n_documents_skipped_empty_text"] == 1
        embed.assert_not_called()


# ── external_id dedup (config#2957) ─────────────────────────────────────


class TestExternalIdDedup:
    def test_document_exists_called_with_external_id(self):
        """config#2957: document_exists_fn must be called with the
        article's canonical_fingerprint as external_id so per-article
        (not just per-day) dedup is possible."""
        article = _make_article(fingerprint="fp-xyz", tickers=("AAPL",))
        exists = MagicMock(return_value=False)
        ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=exists,
            ingest_document_fn=MagicMock(return_value="doc"),
        )
        exists.assert_called_once_with("AAPL", "news", date(2026, 5, 13), "news_polygon", "fp-xyz")

    def test_ingest_document_called_with_external_id(self):
        """config#2957: ingest_document_fn must receive external_id so
        the new per-article partial unique index actually dedups."""
        article = _make_article(fingerprint="fp-abc", tickers=("AAPL",))
        ingest = MagicMock(return_value="doc")
        ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert ingest.call_args.kwargs["external_id"] == "fp-abc"

    def test_two_distinct_same_day_articles_both_attempted_with_different_external_id(self):
        """config#2957 acceptance: two distinct same-day articles for one
        ticker/source must each get their own dedup identity — a fake
        document_exists keyed on (ticker, external_id) (mirroring the new
        partial unique index) lets BOTH persist instead of the second
        silently colliding on the old (ticker, source, day)-only key."""
        a1 = _make_article(fingerprint="fp-1", tickers=("AAPL",), title="Apple headline 1")
        a2 = _make_article(fingerprint="fp-2", tickers=("AAPL",), title="Apple headline 2")

        seen_external_ids: set[str] = set()

        def fake_exists(ticker, doc_type, filed_date, source, external_id=None):
            return external_id in seen_external_ids

        def fake_ingest(*, external_id, **kw):
            seen_external_ids.add(external_id)
            return f"doc-{external_id}"

        stats = ingest_articles(
            [a1, a2],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=fake_exists,
            ingest_document_fn=fake_ingest,
        )
        assert stats["n_documents_ingested"] == 2
        assert stats["n_documents_skipped_exists"] == 0

    def test_same_article_reingested_dedups_via_external_id(self):
        """config#2957 acceptance: re-ingesting the SAME article (same
        fingerprint) still dedups, even against a fake keyed on
        (ticker, external_id) rather than just (ticker, source, day)."""
        article = _make_article(fingerprint="fp-same", tickers=("AAPL",))
        seen_external_ids = {"fp-same"}  # already ingested in a prior run
        exists = MagicMock(side_effect=lambda t, dt, fd, s, external_id=None: external_id in seen_external_ids)
        embed = MagicMock()
        ingest = MagicMock()

        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_skipped_exists"] == 1
        assert stats["n_documents_ingested"] == 0
        embed.assert_not_called()
        ingest.assert_not_called()


# ── Multi-ticker article ────────────────────────────────────────────────


class TestMultiTickerArticle:
    def test_indexes_once_per_ticker(self):
        """Sector-piece concerning AAPL + MSFT + GOOGL produces 3
        documents (one per ticker)."""
        article = _make_article(
            title="Tech earnings season recap",
            tickers=("AAPL", "MSFT", "GOOGL"),
        )
        embed = MagicMock(return_value=[[0.0, 0.0, 0.0]])
        ingest = MagicMock(return_value="doc-id")

        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_attempted"] == 3
        assert stats["n_documents_ingested"] == 3
        # ingest_document called 3× with different tickers
        tickers_called = {
            call.kwargs["ticker"] for call in ingest.call_args_list
        }
        assert tickers_called == {"AAPL", "MSFT", "GOOGL"}

    def test_per_ticker_existence_check(self):
        """If AAPL is already indexed but MSFT isn't, only MSFT gets
        embedded + ingested."""
        article = _make_article(tickers=("AAPL", "MSFT"))
        existing = {"AAPL"}
        exists = MagicMock(side_effect=lambda t, *a, **k: t in existing)
        embed = MagicMock(return_value=[[0.1, 0.2]])
        ingest = MagicMock(return_value="doc")

        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_skipped_exists"] == 1
        assert stats["n_documents_ingested"] == 1
        # Only MSFT got ingested
        ingest.assert_called_once()
        assert ingest.call_args.kwargs["ticker"] == "MSFT"


# ── Sector lookup ──────────────────────────────────────────────────────


class TestSectorLookup:
    def test_ticker_to_sector_passed_through(self):
        article = _make_article(tickers=("AAPL",))
        ingest = MagicMock(return_value="doc")
        ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            ticker_to_sector={"AAPL": "Technology"},
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert ingest.call_args.kwargs["sector"] == "Technology"

    def test_missing_sector_passes_none(self):
        article = _make_article(tickers=("UNKNOWN",))
        ingest = MagicMock(return_value="doc")
        ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            ticker_to_sector={"AAPL": "Technology"},  # UNKNOWN not in map
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert ingest.call_args.kwargs["sector"] is None


# ── dry_run ────────────────────────────────────────────────────────────


class TestDryRun:
    def test_dry_run_skips_embed_and_ingest_but_counts(self):
        article = _make_article(tickers=("AAPL", "MSFT"))
        embed = MagicMock()
        ingest = MagicMock()
        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
            dry_run=True,
        )
        embed.assert_not_called()
        ingest.assert_not_called()
        # Counters still increment so dry-run shows what would happen
        assert stats["n_documents_ingested"] == 2


# ── Failure isolation ──────────────────────────────────────────────────


class TestFailureIsolation:
    def test_per_document_failure_continues_batch(self):
        """One article failing in ingest_document doesn't stop the rest."""
        a1 = _make_article(fingerprint="a", tickers=("AAPL",))
        a2 = _make_article(fingerprint="b", tickers=("MSFT",))
        a3 = _make_article(fingerprint="c", tickers=("GOOGL",))

        def ingest_side_effect(*, ticker, **kw):
            if ticker == "MSFT":
                raise RuntimeError("pgvector temporary failure")
            return f"doc-{ticker}"

        embed = MagicMock(return_value=[[0.0]])
        ingest = MagicMock(side_effect=ingest_side_effect)

        stats = ingest_articles(
            [a1, a2, a3],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_ingested"] == 2
        assert stats["n_failures"] == 1

    def test_ingest_returning_none_counts_as_failure(self):
        """If ingest_document returns None (lib's failure signal),
        count as failure without crashing the batch."""
        article = _make_article(tickers=("AAPL",))
        ingest = MagicMock(return_value=None)
        stats = ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_ingested"] == 0
        assert stats["n_failures"] == 1


# ── Stats shape ────────────────────────────────────────────────────────


def test_stats_shape_has_canonical_keys():
    stats = ingest_articles(
        [],
        filed_date=date(2026, 5, 13),
        embed_texts_fn=MagicMock(),
        document_exists_fn=MagicMock(),
        ingest_document_fn=MagicMock(),
    )
    expected_keys = {
        "n_articles_input",
        "n_documents_attempted",
        "n_documents_skipped_exists",
        "n_documents_skipped_empty_text",
        "n_documents_ingested",
        "n_failures",
    }
    assert set(stats.keys()) == expected_keys
    # Empty input → all zero
    assert all(v == 0 for v in stats.values())


# ── Batched embeddings (config#2956 deliverable 3) ──────────────────────


class TestBatchedEmbeddings:
    def test_one_embed_call_for_multiple_pending_articles(self):
        """The N-article batch must call embed_texts_fn ONCE with all N
        chunk bodies, not once per article (the previous shape)."""
        a1 = _make_article(fingerprint="a", tickers=("AAPL",), body="Apple body text here.")
        a2 = _make_article(fingerprint="b", tickers=("MSFT",), body="Microsoft body text here.")
        a3 = _make_article(fingerprint="c", tickers=("GOOGL",), body="Google body text here.")
        embed = MagicMock(return_value=[[0.1], [0.2], [0.3]])
        ingest = MagicMock(side_effect=lambda **kw: f"doc-{kw['ticker']}")

        stats = ingest_articles(
            [a1, a2, a3],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )

        embed.assert_called_once()
        (texts_arg,), _ = embed.call_args
        assert len(texts_arg) == 3
        assert stats["n_documents_ingested"] == 3
        assert ingest.call_count == 3

    def test_each_document_gets_its_own_embedding_by_position(self):
        """Batched embeddings must map back to the correct document by
        list position, not get mixed up across articles."""
        a1 = _make_article(fingerprint="a", tickers=("AAPL",), body="Apple body text here.")
        a2 = _make_article(fingerprint="b", tickers=("MSFT",), body="Microsoft body text here.")
        embed = MagicMock(return_value=[["embA"], ["embB"]])
        ingest = MagicMock(return_value="doc-id")

        ingest_articles(
            [a1, a2],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )

        calls_by_ticker = {c.kwargs["ticker"]: c.kwargs["chunks"][0]["embedding"] for c in ingest.call_args_list}
        assert calls_by_ticker["AAPL"] == ["embA"]
        assert calls_by_ticker["MSFT"] == ["embB"]

    def test_skipped_existing_documents_excluded_from_embed_batch(self):
        """document_exists-skipped articles must not be embedded at all —
        the batch only covers documents that will actually be ingested."""
        a1 = _make_article(fingerprint="a", tickers=("AAPL",), body="Apple body text here.")
        a2 = _make_article(fingerprint="b", tickers=("MSFT",), body="Microsoft body text here.")
        embed = MagicMock(return_value=[["embA"]])
        exists = MagicMock(side_effect=lambda ticker, *a: ticker == "MSFT")

        stats = ingest_articles(
            [a1, a2],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=MagicMock(return_value="doc"),
        )

        (texts_arg,), _ = embed.call_args
        assert len(texts_arg) == 1
        assert stats["n_documents_skipped_exists"] == 1
        assert stats["n_documents_ingested"] == 1

    def test_no_embed_call_when_nothing_pending(self):
        """dry_run / all-skipped batches must not call the embedder at
        all (empty batch short-circuit)."""
        article = _make_article(tickers=("AAPL",))
        embed = MagicMock()

        ingest_articles(
            [article],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=True),  # already exists
            ingest_document_fn=MagicMock(),
        )

        embed.assert_not_called()

    def test_batch_level_embed_failure_counts_all_pending_as_failures(self):
        """A single batched embed call failing (e.g. Voyage API outage)
        must fail soft — count every pending document as a failure and
        NOT raise, rather than crashing the whole ingest run."""
        a1 = _make_article(fingerprint="a", tickers=("AAPL",), body="Apple body text here.")
        a2 = _make_article(fingerprint="b", tickers=("MSFT",), body="Microsoft body text here.")
        embed = MagicMock(side_effect=RuntimeError("voyage API down"))
        ingest = MagicMock()

        stats = ingest_articles(
            [a1, a2],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )

        assert stats["n_failures"] == 2
        assert stats["n_documents_ingested"] == 0
        ingest.assert_not_called()

    def test_per_document_ingest_failure_still_isolated_after_batching(self):
        """Batching the embed call must not weaken per-document ingest
        failure isolation — one bad ingest_document call still doesn't
        drop the rest of the batch."""
        a1 = _make_article(fingerprint="a", tickers=("AAPL",), body="Apple body text here.")
        a2 = _make_article(fingerprint="b", tickers=("MSFT",), body="Microsoft body text here.")
        a3 = _make_article(fingerprint="c", tickers=("GOOGL",), body="Google body text here.")

        def ingest_side_effect(*, ticker, **kw):
            if ticker == "MSFT":
                raise RuntimeError("pgvector temporary failure")
            return f"doc-{ticker}"

        embed = MagicMock(return_value=[[0.0], [0.0], [0.0]])
        ingest = MagicMock(side_effect=ingest_side_effect)

        stats = ingest_articles(
            [a1, a2, a3],
            filed_date=date(2026, 5, 13),
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_ingested"] == 2
        assert stats["n_failures"] == 1
        embed.assert_called_once()
