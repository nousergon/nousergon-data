"""Tests for the news NLP pipeline (Wave 1 PR A.1).

Covers:
  - Pydantic shapes (frozen + extra='forbid')
  - Protocol structural-subtyping for SentimentScorer / EntityExtractor / EventExtractor
  - Loughran-McDonald scorer (tokenization + composite formula + edge cases)
  - load_lm_master_dict CSV parser
  - Anthropic event extractor (tool_use parsing + transient failure + malformed entries)
  - NewsNLPPipeline orchestrator (composition + graceful degrade)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from alpha_engine_lib.sources import NewsArticle

from collectors.news_aggregator import AggregatedNewsArticle
from collectors.nlp.event_extraction import (
    DEFAULT_EVENT_CATEGORIES,
    AnthropicEventExtractor,
    _build_tool_spec,
)
from collectors.nlp.loughran_mcdonald import (
    LoughranMcDonaldScorer,
    _tokenize,
    _truthy,
    load_lm_master_dict,
)
from collectors.nlp.pipeline import NewsNLPPipeline, NewsNLPOutput
from collectors.nlp.protocols import (
    EntityExtractor,
    EntityMention,
    EventExtractor,
    EventFlag,
    SentimentScore,
    SentimentScorer,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Pydantic shapes ────────────────────────────────────────────────────


class TestSentimentScoreShape:
    def test_construction(self):
        s = SentimentScore(
            scorer="loughran_mcdonald",
            article_fingerprint="abc123",
            composite=0.25,
            positive_word_count=4,
            negative_word_count=2,
            total_token_count=80,
        )
        assert s.composite == 0.25
        assert s.uncertainty_word_count is None

    def test_frozen(self):
        s = SentimentScore(
            scorer="x", article_fingerprint="abc", composite=0.0,
        )
        with pytest.raises(ValidationError):
            s.composite = 0.5  # type: ignore[misc]

    def test_extra_forbidden(self):
        with pytest.raises(ValidationError, match="Extra inputs are not"):
            SentimentScore(
                scorer="x", article_fingerprint="abc", composite=0.0,
                some_unknown_field="oops",  # type: ignore[call-arg]
            )


class TestEntityMentionShape:
    def test_construction(self):
        e = EntityMention(
            extractor="regex_ticker",
            article_fingerprint="abc",
            text="NVDA",
            label="TICKER",
            canonical_ticker="NVDA",
        )
        assert e.canonical_ticker == "NVDA"


class TestEventFlagShape:
    def test_construction(self):
        f = EventFlag(
            extractor="anthropic_haiku",
            article_fingerprint="abc",
            category="merger_or_acquisition",
            description="Acquirer announces all-stock deal.",
            tickers=("AAPL",),
            severity=0.9,
            extracted_at=_now(),
        )
        assert f.severity == 0.9
        assert f.tickers == ("AAPL",)

    def test_severity_default(self):
        f = EventFlag(
            extractor="x", article_fingerprint="a",
            category="other", description="d",
            extracted_at=_now(),
        )
        assert f.severity == 0.5


# ── Protocol structural subtyping ──────────────────────────────────────


class TestProtocolSubtyping:
    def test_lm_scorer_satisfies_sentiment_protocol(self):
        assert isinstance(LoughranMcDonaldScorer(lm_dict={}), SentimentScorer)

    def test_anthropic_extractor_satisfies_event_protocol(self):
        extractor = AnthropicEventExtractor(client=MagicMock())
        assert isinstance(extractor, EventExtractor)

    def test_entity_protocol_structural_match(self):
        class FakeExtractor:
            name = "fake"

            def extract(self, *, text, article_fingerprint):
                return []

        assert isinstance(FakeExtractor(), EntityExtractor)


# ── LM tokenization + composite ────────────────────────────────────────


class TestTokenization:
    def test_alphabetic_only_lowercased(self):
        assert _tokenize("The Stock Rose 5% in Q4!") == [
            "the", "stock", "rose", "in", "q",
        ]

    def test_empty_text(self):
        assert _tokenize("") == []

    def test_dropped_punctuation_and_digits(self):
        # Numbers + currency drop out
        toks = _tokenize("revenue $1.2B beat estimates")
        assert toks == ["revenue", "b", "beat", "estimates"]


class TestTruthyHelper:
    def test_year_stamp_truthy(self):
        assert _truthy("2009") is True

    def test_zero_falsy(self):
        assert _truthy("0") is False

    def test_empty_string_falsy(self):
        assert _truthy("") is False

    def test_none_falsy(self):
        assert _truthy(None) is False

    def test_non_numeric_falsy(self):
        assert _truthy("abc") is False


class TestLoughranMcDonaldScorer:
    SYNTHETIC_DICT = {
        "good":     {"positive": True,  "negative": False, "uncertainty": False},
        "great":    {"positive": True,  "negative": False, "uncertainty": False},
        "bad":      {"positive": False, "negative": True,  "uncertainty": False},
        "loss":     {"positive": False, "negative": True,  "uncertainty": False},
        "approximately": {"positive": False, "negative": False, "uncertainty": True},
    }

    def test_pure_positive(self):
        s = LoughranMcDonaldScorer(lm_dict=self.SYNTHETIC_DICT).score(
            text="good good great", article_fingerprint="fp1",
        )
        assert s.positive_word_count == 3
        assert s.negative_word_count == 0
        assert s.composite == pytest.approx(1.0)

    def test_pure_negative(self):
        s = LoughranMcDonaldScorer(lm_dict=self.SYNTHETIC_DICT).score(
            text="bad bad loss", article_fingerprint="fp1",
        )
        assert s.negative_word_count == 3
        assert s.composite == pytest.approx(-1.0)

    def test_balanced_yields_zero(self):
        s = LoughranMcDonaldScorer(lm_dict=self.SYNTHETIC_DICT).score(
            text="good bad", article_fingerprint="fp1",
        )
        assert s.composite == 0.0

    def test_dilution_by_neutral_words(self):
        # 1 positive in 5 tokens — composite = 1/5 = 0.2
        s = LoughranMcDonaldScorer(lm_dict=self.SYNTHETIC_DICT).score(
            text="the quarterly report was good overall",
            article_fingerprint="fp1",
        )
        assert s.positive_word_count == 1
        assert s.total_token_count == 6
        assert s.composite == pytest.approx(1 / 6)

    def test_uncertainty_counted_separately_from_polarity(self):
        s = LoughranMcDonaldScorer(lm_dict=self.SYNTHETIC_DICT).score(
            text="results approximately matched estimates",
            article_fingerprint="fp1",
        )
        assert s.uncertainty_word_count == 1
        assert s.positive_word_count == 0
        assert s.negative_word_count == 0
        assert s.composite == 0.0

    def test_empty_text_returns_zero(self):
        s = LoughranMcDonaldScorer(lm_dict=self.SYNTHETIC_DICT).score(
            text="", article_fingerprint="fp1",
        )
        assert s.composite == 0.0
        assert s.total_token_count == 0

    def test_clipped_to_range(self):
        # With clip=0.5, all-positive text caps at 0.5
        s = LoughranMcDonaldScorer(
            lm_dict=self.SYNTHETIC_DICT, composite_clip=0.5,
        ).score(text="good great", article_fingerprint="fp1")
        assert s.composite == 0.5

    def test_empty_dict_warns_and_yields_zero(self, caplog):
        with caplog.at_level("WARNING"):
            scorer = LoughranMcDonaldScorer(lm_dict={})
        assert any("empty dict" in r.message for r in caplog.records)
        s = scorer.score(text="good great", article_fingerprint="fp1")
        assert s.composite == 0.0


# ── load_lm_master_dict CSV parsing ────────────────────────────────────


class TestLoadLmMasterDict:
    def test_parses_canonical_csv_format(self, tmp_path: Path):
        csv = tmp_path / "lm.csv"
        csv.write_text(
            "Word,Sequence Number,Negative,Positive,Uncertainty,"
            "Litigious,Strong_Modal,Weak_Modal,Constraining,"
            "Syllables,Source\n"
            "good,1,0,2009,0,0,0,0,0,1,LM\n"
            "bad,2,2009,0,0,0,0,0,0,1,LM\n"
            "approximately,3,0,0,2009,0,0,0,0,4,LM\n"
        )
        out = load_lm_master_dict(csv)
        assert out["good"]["positive"] is True
        assert out["good"]["negative"] is False
        assert out["bad"]["negative"] is True
        assert out["approximately"]["uncertainty"] is True

    def test_missing_file_returns_empty_dict_with_warning(self, tmp_path, caplog):
        with caplog.at_level("WARNING"):
            out = load_lm_master_dict(tmp_path / "missing.csv")
        assert out == {}
        assert any("not found" in r.message for r in caplog.records)

    def test_blank_rows_skipped(self, tmp_path: Path):
        csv = tmp_path / "lm.csv"
        csv.write_text(
            "Word,Negative,Positive\n"
            ",0,0\n"  # blank word
            "good,0,2009\n"
        )
        out = load_lm_master_dict(csv)
        assert "good" in out
        assert "" not in out


# ── Anthropic event extractor ──────────────────────────────────────────


def _make_tool_use_response(events: list[dict]) -> object:
    """Build a mock Anthropic response with a tool_use content block."""
    block = MagicMock()
    block.type = "tool_use"
    block.name = "EmitEventFlags"
    block.input = {"events": events}
    response = MagicMock()
    response.content = [block]
    return response


class TestAnthropicEventExtractor:
    def test_tool_spec_built_with_default_categories(self):
        spec = _build_tool_spec()
        cats = spec["input_schema"]["properties"]["events"]["items"][
            "properties"
        ]["category"]["enum"]
        # All categories present in the enum
        for cat in DEFAULT_EVENT_CATEGORIES:
            assert cat in cats

    def test_happy_path_parses_events(self):
        client = MagicMock()
        client.messages.create.return_value = _make_tool_use_response([
            {
                "category": "merger_or_acquisition",
                "description": "Acquirer announces all-stock deal for X.",
                "tickers": ["AAPL"],
                "severity": 0.9,
            },
        ])
        extractor = AnthropicEventExtractor(client=client)
        out = extractor.extract(
            text="Apple announces acquisition...",
            article_fingerprint="fp1",
            article_tickers=("AAPL",),
        )
        assert len(out) == 1
        assert out[0].category == "merger_or_acquisition"
        assert out[0].severity == 0.9

    def test_empty_text_short_circuits_without_calling_llm(self):
        client = MagicMock()
        extractor = AnthropicEventExtractor(client=client)
        out = extractor.extract(
            text="", article_fingerprint="fp1", article_tickers=(),
        )
        assert out == []
        client.messages.create.assert_not_called()

    def test_transient_llm_failure_returns_empty(self):
        client = MagicMock()
        client.messages.create.side_effect = RuntimeError("anthropic 500")
        extractor = AnthropicEventExtractor(client=client)
        out = extractor.extract(
            text="some text", article_fingerprint="fp1", article_tickers=(),
        )
        assert out == []

    def test_malformed_event_entry_dropped_others_kept(self):
        client = MagicMock()
        client.messages.create.return_value = _make_tool_use_response([
            {"category": "earnings_release"},  # missing required description
            {
                "category": "earnings_release",
                "description": "Q4 results released.",
                "tickers": ["AAPL"],
                "severity": 0.6,
            },
        ])
        extractor = AnthropicEventExtractor(client=client)
        out = extractor.extract(
            text="x", article_fingerprint="fp1", article_tickers=("AAPL",),
        )
        # Malformed entry dropped; good entry kept
        assert len(out) == 1
        assert out[0].description == "Q4 results released."

    def test_tool_use_input_as_json_string(self):
        """Anthropic SDK can return tool_use.input as either a dict or
        a JSON string depending on stream-vs-message mode. Tolerate
        both."""
        client = MagicMock()
        block = MagicMock()
        block.type = "tool_use"
        block.name = "EmitEventFlags"
        block.input = json.dumps({"events": [{
            "category": "other", "description": "x",
            "tickers": [], "severity": 0.1,
        }]})
        response = MagicMock()
        response.content = [block]
        client.messages.create.return_value = response
        extractor = AnthropicEventExtractor(client=client)
        out = extractor.extract(
            text="x", article_fingerprint="fp1", article_tickers=(),
        )
        assert len(out) == 1
        assert out[0].category == "other"

    def test_no_tool_use_block_returns_empty(self):
        client = MagicMock()
        response = MagicMock()
        text_block = MagicMock()
        text_block.type = "text"
        response.content = [text_block]
        client.messages.create.return_value = response
        extractor = AnthropicEventExtractor(client=client)
        out = extractor.extract(
            text="x", article_fingerprint="fp1", article_tickers=(),
        )
        assert out == []


# ── Pipeline orchestrator ──────────────────────────────────────────────


def _make_aggregated(
    fingerprint: str = "fp1",
    title: str = "Apple Q4 results",
    body: str = "Apple reported strong results.",
    tickers: tuple[str, ...] = ("AAPL",),
) -> AggregatedNewsArticle:
    variant = NewsArticle(
        tickers=tickers, title=title, body_excerpt=body,
        url="https://x.com/a", published_at=_now(),
        source="polygon", fetched_at=_now(),
    )
    return AggregatedNewsArticle(
        canonical_title=title,
        canonical_url="https://x.com/a",
        tickers=tickers,
        earliest_published_at=_now(),
        variants=(variant,),
        canonical_fingerprint=fingerprint,
    )


class TestPipelineOrchestrator:
    def _make_lm_scorer(self):
        return LoughranMcDonaldScorer(lm_dict={
            "strong":   {"positive": True, "negative": False, "uncertainty": False},
            "results":  {"positive": False, "negative": False, "uncertainty": False},
            "weak":     {"positive": False, "negative": True, "uncertainty": False},
        })

    def test_empty_pipeline_returns_empty_output(self):
        pipeline = NewsNLPPipeline()
        out = pipeline.process([_make_aggregated()])
        assert isinstance(out, NewsNLPOutput)
        assert out.sentiment_scores == []
        assert out.entity_mentions == []
        assert out.event_flags == []
        assert out.n_articles_processed == 1

    def test_sentiment_scorer_runs_per_article(self):
        pipeline = NewsNLPPipeline(
            sentiment_scorers=[self._make_lm_scorer()],
        )
        out = pipeline.process([
            _make_aggregated(
                fingerprint="a", title="strong results",
                body="firm reported strong execution",
            ),
            _make_aggregated(
                fingerprint="b", title="weak results",
                body="firm reported weak execution",
            ),
        ])
        assert len(out.sentiment_scores) == 2
        by_fp = {s.article_fingerprint: s for s in out.sentiment_scores}
        assert by_fp["a"].composite > 0
        assert by_fp["b"].composite < 0

    def test_multiple_scorers_emit_per_scorer_per_article(self):
        scorer1 = self._make_lm_scorer()
        scorer2 = MagicMock(spec=["name", "score"])
        scorer2.name = "fake_other"
        scorer2.score.return_value = SentimentScore(
            scorer="fake_other", article_fingerprint="dummy",
            composite=0.42,
        )
        pipeline = NewsNLPPipeline(sentiment_scorers=[scorer1, scorer2])
        out = pipeline.process([_make_aggregated(fingerprint="a")])
        # 1 article × 2 scorers = 2 scores
        assert len(out.sentiment_scores) == 2
        names = {s.scorer for s in out.sentiment_scores}
        assert names == {"loughran_mcdonald", "fake_other"}

    def test_scorer_exception_skips_that_score_but_continues(self):
        bad_scorer = MagicMock(spec=["name", "score"])
        bad_scorer.name = "broken"
        bad_scorer.score.side_effect = RuntimeError("boom")
        pipeline = NewsNLPPipeline(
            sentiment_scorers=[bad_scorer, self._make_lm_scorer()],
        )
        out = pipeline.process([_make_aggregated(fingerprint="a", title="strong")])
        # Only the LM score makes it through
        assert len(out.sentiment_scores) == 1
        assert out.sentiment_scores[0].scorer == "loughran_mcdonald"

    def test_event_extractor_receives_article_tickers(self):
        extractor = MagicMock(spec=["name", "extract"])
        extractor.name = "fake_event"
        extractor.extract.return_value = [EventFlag(
            extractor="fake_event", article_fingerprint="a",
            category="earnings_release", description="d",
            tickers=("AAPL",), severity=0.5, extracted_at=_now(),
        )]
        pipeline = NewsNLPPipeline(event_extractors=[extractor])
        out = pipeline.process([
            _make_aggregated(fingerprint="a", tickers=("AAPL", "NVDA")),
        ])
        # Pipeline passes article tickers through verbatim
        kwargs = extractor.extract.call_args.kwargs
        assert kwargs["article_tickers"] == ("AAPL", "NVDA")
        assert kwargs["article_fingerprint"] == "a"
        assert len(out.event_flags) == 1

    def test_empty_article_text_skipped_as_failed(self):
        # Construct an aggregated article with no title or body
        variant = NewsArticle(
            tickers=("AAPL",), title="", body_excerpt="",
            url="https://x.com/a", published_at=_now(),
            source="polygon", fetched_at=_now(),
        )
        agg = AggregatedNewsArticle(
            canonical_title="",
            canonical_url="https://x.com/a",
            tickers=("AAPL",),
            earliest_published_at=_now(),
            variants=(variant,),
            canonical_fingerprint="empty",
        )
        pipeline = NewsNLPPipeline(
            sentiment_scorers=[self._make_lm_scorer()],
        )
        out = pipeline.process([agg])
        assert out.n_articles_failed == 1
        assert out.n_articles_processed == 0
        assert out.sentiment_scores == []

    def test_pipeline_uses_longest_excerpt_across_variants(self):
        short = NewsArticle(
            tickers=("AAPL",), title="t", body_excerpt="brief",
            url="https://x.com/a", published_at=_now(),
            source="polygon", fetched_at=_now(),
        )
        long = NewsArticle(
            tickers=("AAPL",), title="t",
            body_excerpt="a much longer body containing strong words",
            url="https://x.com/a", published_at=_now(),
            source="gdelt", fetched_at=_now(),
        )
        agg = AggregatedNewsArticle(
            canonical_title="t",
            canonical_url="https://x.com/a",
            tickers=("AAPL",),
            earliest_published_at=_now(),
            variants=(short, long),
            canonical_fingerprint="fp",
        )
        pipeline = NewsNLPPipeline(
            sentiment_scorers=[self._make_lm_scorer()],
        )
        out = pipeline.process([agg])
        # The longer body has "strong" — should produce positive composite
        assert out.sentiment_scores[0].composite > 0
