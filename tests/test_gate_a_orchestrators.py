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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── _rag_scope ────────────────────────────────────────────────────────
#
# config-I5700: the corpus fetch set is the scanner DECISION SET, resolved
# from universe_membership/{date}/membership.json — NOT signals.json::universe,
# which is a 903-row sizing envelope. The old TestLoadSignalsTickers class
# pinned the defective behaviour (including that `sorted(prefixes)[-1]`
# list_objects_v2 scan, which silently returns the 1000th-oldest date once the
# partition count crosses a page) and is replaced wholesale rather than
# adapted.
#
# config-I6630: the cut is ``attractiveness_top_60``, not
# ``scanner_candidates``. Both are 60 wide, but the latter is a tech_score
# momentum GATE and the former is the attractiveness RANK the predictor's
# scored cut is the head of. Scoping to the gate cut meant the corpus covered
# 2 of the predictor's 20 names (measured live 2026-08-07).


class TestLoadRagScope:
    def _s3(self, *, cuts=None, holdings=("HELD",), membership=True,
            predictor_cut="attractiveness_top_20"):
        s3 = MagicMock()
        payloads = {}
        if membership:
            payloads["universe_membership/latest.json"] = {
                "run_date": "2026-07-29",
                "predictor_universe_cut": predictor_cut,
                "cuts": cuts if cuts is not None else {
                    "attractiveness_top_60": {
                        "basis": "attractiveness_rank", "size": 2,
                        "tickers": ["msft", "AAPL"],
                        "source": "scanner/universe/2026-07-29/universe.json::attractiveness_score",
                    },
                    # The predictor's scored cut — nested, as the funnel requires.
                    "attractiveness_top_20": {
                        "basis": "attractiveness_rank", "size": 1,
                        "tickers": ["AAPL"],
                    },
                    # The momentum GATE cut is present and must NOT be picked up.
                    "scanner_candidates": {
                        "basis": "scanner_gate", "size": 3,
                        "tickers": ["A", "B", "C"],
                        "source": "candidates/2026-07-29/candidates.json::scanner_tickers",
                    },
                },
            }
        if holdings is not None:
            payloads["metron/holdings_universe.json"] = {
                "as_of": "2026-07-29", "tickers": list(holdings),
            }

        def _get(Bucket, Key):
            if Key not in payloads:
                raise RuntimeError(f"NoSuchKey {Key}")
            return {"Body": BytesIO(json.dumps(payloads[Key]).encode())}

        s3.get_object.side_effect = _get
        return s3

    def test_resolves_feed_cut_union_holdings_uppercased_and_sorted(self):
        from rag.pipelines._rag_scope import load_rag_scope_tickers

        assert load_rag_scope_tickers(s3_client=self._s3()) == ["AAPL", "HELD", "MSFT"]

    def test_reads_the_pointer_not_a_prefix_listing(self):
        # The pagination bomb: list_objects_v2 caps CommonPrefixes at 1000, so
        # a scan-and-sort silently goes STALE rather than failing. The resolver
        # must never list.
        from rag.pipelines._rag_scope import load_rag_scope_tickers

        s3 = self._s3()
        load_rag_scope_tickers(s3_client=s3)
        s3.list_objects_v2.assert_not_called()
        assert any(
            c.kwargs.get("Key") == "universe_membership/latest.json"
            for c in s3.get_object.call_args_list
        )

    def test_run_date_pins_the_dated_artifact(self):
        from rag.pipelines._rag_scope import RagScopeUnavailable, load_rag_scope_tickers

        s3 = self._s3(membership=False)
        with pytest.raises(RagScopeUnavailable):
            load_rag_scope_tickers(s3_client=s3, run_date="2026-07-29")
        assert any(
            c.kwargs.get("Key") == "universe_membership/2026-07-29/membership.json"
            for c in s3.get_object.call_args_list
        )

    def test_takes_the_attractiveness_feed_cut_not_the_momentum_gate_cut(self):
        # config-I6630. Both cuts are present and both are "the 60"; only the
        # attractiveness rank cut is the decision set.
        from rag.pipelines._rag_scope import load_rag_scope

        scope = load_rag_scope(s3_client=self._s3())
        assert scope["counts"]["attractiveness_top_60"] == 2
        assert "scanner_candidates" not in scope["counts"]
        assert not {"A", "B", "C"} & set(scope["tickers"])

    def test_scope_contains_the_cut_the_predictor_scores(self):
        # The funnel invariant, consumer side. The corpus exists to give the
        # SCORED names evidence; a scope that does not contain them is filling
        # the wrong 60.
        from rag.pipelines._rag_scope import load_rag_scope

        scope = load_rag_scope(s3_client=self._s3())
        assert {"AAPL"} <= set(scope["tickers"])

    def test_scope_not_covering_the_predictor_cut_raises(self):
        # The exact live 2026-08-07 shape: scored cut and scope cut drawn from
        # different rankings. Ingestion is off every decision pipeline's
        # critical path, so raising costs a corpus fill, never a trading day.
        from rag.pipelines._rag_scope import RagScopeUnavailable, load_rag_scope_tickers

        s3 = self._s3(cuts={
            "attractiveness_top_60": {"tickers": ["ANF", "SN"]},
            "attractiveness_top_20": {"tickers": ["ANF", "LULU", "ROKU"]},
        })
        with pytest.raises(RagScopeUnavailable, match="funnel invariant"):
            load_rag_scope_tickers(s3_client=s3)

    def test_an_arm_promotion_to_an_uncovered_cut_raises(self):
        # A champion change that moves the decision set must move the corpus
        # scope in the same change. This is the coupling that was missing.
        from rag.pipelines._rag_scope import RagScopeUnavailable, load_rag_scope_tickers

        s3 = self._s3(predictor_cut="thinktank_top_20", cuts={
            "attractiveness_top_60": {"tickers": ["AAPL", "MSFT"]},
            "thinktank_top_20": {"tickers": ["NVDA"]},
        })
        with pytest.raises(RagScopeUnavailable, match="funnel invariant"):
            load_rag_scope_tickers(s3_client=s3)

    def test_membership_without_a_named_predictor_cut_raises(self):
        # A consumer guessing which cut is scored is how a cut change becomes
        # invisible to half the fleet.
        from rag.pipelines._rag_scope import RagScopeUnavailable, load_rag_scope_tickers

        with pytest.raises(RagScopeUnavailable, match="predictor_universe_cut"):
            load_rag_scope_tickers(s3_client=self._s3(predictor_cut=None))

    def test_missing_membership_raises_never_widens(self):
        from rag.pipelines._rag_scope import RagScopeUnavailable, load_rag_scope_tickers

        with pytest.raises(RagScopeUnavailable, match="903-ticker"):
            load_rag_scope_tickers(s3_client=self._s3(membership=False))

    def test_empty_feed_cut_raises_and_names_the_available_cuts(self):
        from rag.pipelines._rag_scope import RagScopeUnavailable, load_rag_scope_tickers

        s3 = self._s3(cuts={"scanner_candidates": {"tickers": ["X"]}})
        with pytest.raises(RagScopeUnavailable, match="scanner_candidates"):
            load_rag_scope_tickers(s3_client=s3)

    def test_missing_holdings_degrades_loudly_but_does_not_fail(self, caplog):
        # Held names losing fresh evidence narrows coverage; it cannot corrupt
        # it. Blocking the whole corpus fill on a cross-product artifact is the
        # worse failure — but the gap must be stated, not swallowed.
        from rag.pipelines._rag_scope import load_rag_scope_tickers

        with caplog.at_level("WARNING"):
            tickers = load_rag_scope_tickers(s3_client=self._s3(holdings=None))
        assert tickers == ["AAPL", "MSFT"]
        assert any("HELD NAMES WILL NOT BE COVERED" in r.message for r in caplog.records)

    def test_non_equity_identifiers_are_dropped_loudly(self, caplog):
        # Measured live 2026-07-30: the held set carried 912828YK0, a US
        # Treasury CUSIP. Every ingestion source here is equity-only, so it is
        # a wasted request per source per run, forever — and a permanent
        # corpus gap no watermark can close. Dropped, but NAMED.
        from rag.pipelines._rag_scope import load_rag_scope

        s3 = self._s3(holdings=("912828YK0", "HELD"))
        with caplog.at_level("WARNING"):
            scope = load_rag_scope(s3_client=s3)
        assert "912828YK0" not in scope["tickers"]
        assert scope["counts"]["rejected_non_equity"] == 1
        assert any("912828YK0" in r.message for r in caplog.records)

    def test_share_class_tickers_survive_the_filter(self):
        # BRK.B / BF-B are real, resolvable tickers — the filter must not be
        # so strict that it silently drops legitimate holdings.
        from rag.pipelines._rag_scope import load_rag_scope

        scope = load_rag_scope(s3_client=self._s3(holdings=("BRK.B", "BF-B")))
        assert {"BRK.B", "BF-B"} <= set(scope["tickers"])
        assert scope["counts"]["rejected_non_equity"] == 0

    def test_scope_is_far_narrower_than_the_sizing_envelope(self):
        # The regression guard. 903 -> ~68 is the whole point; a resolver that
        # silently returns board-scale output is the defect returning.
        from rag.pipelines._rag_scope import load_rag_scope

        board_scale = 903
        scope = load_rag_scope(s3_client=self._s3())
        assert scope["counts"]["total"] < board_scale / 5


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
            # config-I5703: the weekly path uses the ASYNC aggregator now.
            # Patching the sync class here left the REAL aggregator running and
            # reaching the network — which is why this test took ~90s.
            "collectors.news_aggregator_async.AsyncNewsAggregator"
        ) as mock_agg_cls, patch(
            "data.derived.news_aggregates.aggregate_and_write"
        ) as mock_aaw, patch(
            "rag.pipelines.ingest_news.ingest_articles"
        ) as mock_rag_ingest, patch(
            "boto3.client"
        ) as mock_boto:
            # Configure: aggregator returns empty list (no articles)
            # .fetch is a coroutine driven by anyio.run — an AsyncMock,
            # not a plain return_value, or main() awaits a MagicMock.
            mock_agg_cls.return_value.fetch = AsyncMock(return_value=[])
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
            "collectors.news_aggregator_async.AsyncNewsAggregator"
        ) as mock_agg_cls, patch(
            "data.derived.news_aggregates.aggregate_and_write"
        ) as mock_aaw, patch(
            "rag.pipelines.ingest_news.ingest_articles"
        ) as mock_rag_ingest:
            mock_agg_cls.return_value.fetch = AsyncMock(return_value=[])
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
            "collectors.news_aggregator_async.AsyncNewsAggregator"
        ) as mock_agg_cls, patch(
            "data.derived.news_aggregates.aggregate_and_write"
        ) as mock_aaw, patch(
            "rag.pipelines.ingest_news.ingest_articles"
        ) as mock_rag_ingest, patch("boto3.client"):
            mock_agg_cls.return_value.fetch = AsyncMock(return_value=[])
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
            "rag.pipelines._rag_scope.load_rag_scope_tickers",
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
            "rag.pipelines._rag_scope.load_rag_scope_tickers",
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
            "rag.pipelines._rag_scope.load_rag_scope_tickers",
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
