"""Tests for Gate A orchestrators (run_news_pipeline + run_analyst_pipeline)
+ the shared --from-signals helper.

Covers the per-CLI orchestration shape — heavier integration of the
underlying modules (NewsAggregator + NLP pipeline + parquet writer +
RAG ingest + analyst snapshotter + revisions computer) is already
tested in their respective Wave 1 PRs.
"""

from __future__ import annotations

import json
import sys
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest


# ── _signals_universe ─────────────────────────────────────────────────


class TestLoadSignalsTickers:
    def _mock_s3_with_signals(self, universe):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "CommonPrefixes": [{"Prefix": "signals/2026-05-13/"}],
        }
        s3.get_object.return_value = {
            "Body": BytesIO(json.dumps({"universe": universe}).encode()),
        }
        return s3

    def test_loads_universe_dict_shape(self):
        from rag.pipelines._signals_universe import load_signals_tickers

        s3 = self._mock_s3_with_signals([
            {"ticker": "AAPL", "sector": "Technology"},
            {"ticker": "MSFT", "sector": "Technology"},
        ])
        tickers = load_signals_tickers(s3_client=s3)
        assert tickers == ["AAPL", "MSFT"]

    def test_loads_universe_flat_shape_backward_compat(self):
        from rag.pipelines._signals_universe import load_signals_tickers

        s3 = self._mock_s3_with_signals(["AAPL", "MSFT"])
        tickers = load_signals_tickers(s3_client=s3)
        assert tickers == ["AAPL", "MSFT"]

    def test_uppercases_tickers(self):
        from rag.pipelines._signals_universe import load_signals_tickers

        s3 = self._mock_s3_with_signals([{"ticker": "aapl"}])
        assert load_signals_tickers(s3_client=s3) == ["AAPL"]

    def test_picks_most_recent_prefix(self):
        from rag.pipelines._signals_universe import load_signals_tickers

        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "signals/2026-05-06/"},
                {"Prefix": "signals/2026-05-13/"},
                {"Prefix": "signals/2026-04-29/"},
            ],
        }
        s3.get_object.return_value = {
            "Body": BytesIO(json.dumps({"universe": [{"ticker": "X"}]}).encode()),
        }
        load_signals_tickers(s3_client=s3)
        # Sorted prefixes pick the lexicographically-last (most recent)
        s3.get_object.assert_called_with(
            Bucket="alpha-engine-research",
            Key="signals/2026-05-13/signals.json",
        )

    def test_no_signals_returns_empty_with_error_log(self, caplog):
        from rag.pipelines._signals_universe import load_signals_tickers

        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"CommonPrefixes": []}
        with caplog.at_level("ERROR"):
            tickers = load_signals_tickers(s3_client=s3)
        assert tickers == []
        assert any("no signals/ prefix" in r.message for r in caplog.records)

    def test_s3_read_failure_returns_empty(self, caplog):
        from rag.pipelines._signals_universe import load_signals_tickers

        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "CommonPrefixes": [{"Prefix": "signals/2026-05-13/"}],
        }
        s3.get_object.side_effect = RuntimeError("net down")
        with caplog.at_level("ERROR"):
            tickers = load_signals_tickers(s3_client=s3)
        assert tickers == []


# ── run_news_pipeline CLI ──────────────────────────────────────────────


class TestRunNewsPipelineCli:
    def test_explicit_tickers_path(self, capsys, monkeypatch):
        """`--tickers` path skips signals.json load and runs through
        with mocks. We verify the orchestration shape, not the
        individual module behavior."""
        from rag.pipelines import run_news_pipeline

        # Mock all 4 downstream modules
        with patch.object(
            run_news_pipeline, "_run_nlp",
            return_value=_empty_nlp_output(),
        ), patch(
            "rag.pipelines.run_news_pipeline._load_ticker_name_map",
            return_value={},
        ), patch(
            "collectors.news_aggregator.NewsAggregator"
        ) as mock_agg_cls, patch(
            "data.derived.news_aggregates.aggregate_and_write"
        ) as mock_aaw, patch(
            "rag.pipelines.ingest_news.ingest_articles"
        ) as mock_rag_ingest, patch(
            "boto3.client"
        ) as mock_boto:
            # Configure: aggregator returns empty list (no articles)
            mock_agg_cls.return_value.fetch.return_value = []
            mock_aaw.return_value = (
                "data/news_aggregates/2026-05-17.parquet",
                _empty_df(),
            )
            mock_rag_ingest.return_value = {
                "n_articles_input": 0,
                "n_documents_attempted": 0,
                "n_documents_skipped_exists": 0,
                "n_documents_skipped_empty_text": 0,
                "n_documents_ingested": 0,
                "n_failures": 0,
            }

            monkeypatch.setattr(
                sys, "argv",
                ["run_news_pipeline", "--tickers", "AAPL,MSFT",
                 "--aggregate-date", "2026-05-17"],
            )
            rc = run_news_pipeline.main()
        assert rc == 0
        # Aggregator was constructed with 3 free-tier adapters
        assert mock_agg_cls.called
        # Parquet writer called
        assert mock_aaw.called
        # RAG ingest called
        assert mock_rag_ingest.called

    def test_dry_run_skips_writes(self, monkeypatch):
        from rag.pipelines import run_news_pipeline

        with patch.object(
            run_news_pipeline, "_run_nlp",
            return_value=_empty_nlp_output(),
        ), patch(
            "rag.pipelines.run_news_pipeline._load_ticker_name_map",
            return_value={},
        ), patch(
            "collectors.news_aggregator.NewsAggregator"
        ) as mock_agg_cls, patch(
            "data.derived.news_aggregates.aggregate_and_write"
        ) as mock_aaw, patch(
            "rag.pipelines.ingest_news.ingest_articles"
        ) as mock_rag_ingest:
            mock_agg_cls.return_value.fetch.return_value = []
            monkeypatch.setattr(
                sys, "argv",
                ["run_news_pipeline", "--tickers", "AAPL", "--dry-run"],
            )
            run_news_pipeline.main()
        # Dry-run skips both the parquet write AND the RAG ingest
        assert not mock_aaw.called
        assert not mock_rag_ingest.called

    def test_skip_rag_runs_aggregates_but_not_rag(self, monkeypatch):
        from rag.pipelines import run_news_pipeline

        with patch.object(
            run_news_pipeline, "_run_nlp",
            return_value=_empty_nlp_output(),
        ), patch(
            "rag.pipelines.run_news_pipeline._load_ticker_name_map",
            return_value={},
        ), patch(
            "collectors.news_aggregator.NewsAggregator"
        ) as mock_agg_cls, patch(
            "data.derived.news_aggregates.aggregate_and_write"
        ) as mock_aaw, patch(
            "rag.pipelines.ingest_news.ingest_articles"
        ) as mock_rag_ingest, patch("boto3.client"):
            mock_agg_cls.return_value.fetch.return_value = []
            mock_aaw.return_value = (
                "data/news_aggregates/x.parquet", _empty_df(),
            )
            monkeypatch.setattr(
                sys, "argv",
                ["run_news_pipeline", "--tickers", "AAPL", "--skip-rag"],
            )
            run_news_pipeline.main()
        assert mock_aaw.called
        assert not mock_rag_ingest.called

    def test_empty_tickers_returns_nonzero(self, monkeypatch):
        from rag.pipelines import run_news_pipeline

        with patch(
            "rag.pipelines._signals_universe.load_signals_tickers",
            return_value=[],
        ):
            monkeypatch.setattr(
                sys, "argv",
                ["run_news_pipeline", "--from-signals"],
            )
            rc = run_news_pipeline.main()
        assert rc == 1

    def test_required_args_mutually_exclusive(self, monkeypatch, capsys):
        from rag.pipelines import run_news_pipeline

        monkeypatch.setattr(
            sys, "argv",
            ["run_news_pipeline"],  # neither --tickers nor --from-signals
        )
        with pytest.raises(SystemExit):
            run_news_pipeline.main()


# ── run_analyst_pipeline CLI ───────────────────────────────────────────


class TestRunAnalystPipelineCli:
    def test_explicit_tickers_path(self, monkeypatch):
        from rag.pipelines import run_analyst_pipeline

        with patch(
            "data.snapshotter.analyst_daily.snapshot_universe",
            return_value={
                "n_tickers": 1, "n_documents_written": 1,
                "n_source_calls_attempted": 2,
                "n_source_calls_succeeded": 2,
                "n_tickers_with_zero_coverage": 0,
            },
        ) as mock_snap, patch(
            "data.derived.analyst_revisions.compute_and_write_revisions",
            return_value=("data/analyst_revisions/2026-05-17.parquet", []),
        ) as mock_rev, patch("boto3.client"):
            monkeypatch.setattr(
                sys, "argv",
                ["run_analyst_pipeline", "--tickers", "AAPL",
                 "--snapshot-date", "2026-05-17"],
            )
            rc = run_analyst_pipeline.main()
        assert rc == 0
        assert mock_snap.called
        assert mock_rev.called

    def test_skip_revisions_runs_snapshot_only(self, monkeypatch):
        from rag.pipelines import run_analyst_pipeline

        with patch(
            "data.snapshotter.analyst_daily.snapshot_universe",
            return_value={
                "n_tickers": 1, "n_documents_written": 1,
                "n_source_calls_attempted": 2,
                "n_source_calls_succeeded": 2,
                "n_tickers_with_zero_coverage": 0,
            },
        ) as mock_snap, patch(
            "data.derived.analyst_revisions.compute_and_write_revisions",
        ) as mock_rev, patch("boto3.client"):
            monkeypatch.setattr(
                sys, "argv",
                ["run_analyst_pipeline", "--tickers", "AAPL",
                 "--skip-revisions"],
            )
            rc = run_analyst_pipeline.main()
        assert rc == 0
        assert mock_snap.called
        assert not mock_rev.called

    def test_dry_run_skips_revisions(self, monkeypatch):
        from rag.pipelines import run_analyst_pipeline

        with patch(
            "data.snapshotter.analyst_daily.snapshot_universe",
            return_value={
                "n_tickers": 1, "n_documents_written": 1,
                "n_source_calls_attempted": 2,
                "n_source_calls_succeeded": 2,
                "n_tickers_with_zero_coverage": 0,
            },
        ) as mock_snap, patch(
            "data.derived.analyst_revisions.compute_and_write_revisions",
        ) as mock_rev, patch("boto3.client"):
            monkeypatch.setattr(
                sys, "argv",
                ["run_analyst_pipeline", "--tickers", "AAPL", "--dry-run"],
            )
            rc = run_analyst_pipeline.main()
        assert rc == 0
        # Snapshot called with dry_run=True (no S3 write)
        assert mock_snap.call_args.kwargs.get("dry_run") is True
        # Revisions skipped under --dry-run
        assert not mock_rev.called

    def test_empty_tickers_returns_nonzero(self, monkeypatch):
        from rag.pipelines import run_analyst_pipeline

        with patch(
            "rag.pipelines._signals_universe.load_signals_tickers",
            return_value=[],
        ):
            monkeypatch.setattr(
                sys, "argv",
                ["run_analyst_pipeline", "--from-signals"],
            )
            rc = run_analyst_pipeline.main()
        assert rc == 1


# ── ingest_form4 --from-signals integration ────────────────────────────


class TestIngestForm4FromSignals:
    def test_from_signals_loads_tickers_then_runs(self, monkeypatch):
        """The --from-signals flag now wraps the shared helper. Just
        verify the integration shape — underlying ingest_for_tickers
        is covered by test_ingest_form4."""
        from rag.pipelines import ingest_form4

        with patch(
            "rag.pipelines._signals_universe.load_signals_tickers",
            return_value=["AAPL", "MSFT"],
        ), patch.object(
            ingest_form4, "ingest_for_tickers",
            return_value={
                "n_tickers": 2, "n_filings_discovered": 0,
                "n_filings_downloaded": 0, "n_transactions_parsed": 0,
                "n_parquet_writes": 0, "n_failures": 0,
            },
        ) as mock_ingest, patch("boto3.client"):
            monkeypatch.setattr(
                sys, "argv",
                ["ingest_form4", "--from-signals"],
            )
            ingest_form4.main()
        assert mock_ingest.called
        assert mock_ingest.call_args.args[0] == ["AAPL", "MSFT"]


# ── Helpers ────────────────────────────────────────────────────────────


def _empty_nlp_output():
    from collectors.nlp.pipeline import NewsNLPOutput
    return NewsNLPOutput()


def _empty_df():
    import pandas as pd
    return pd.DataFrame()
