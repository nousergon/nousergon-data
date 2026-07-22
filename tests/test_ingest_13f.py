"""Tests for the 13F institutional-ownership → RAG ingest pipeline.

config#2428. Covers:
  - Chunk text formatting (row_to_chunk_text) — narrative summary shape,
    signed vs. unsigned share formatting, missing-QoQ-baseline handling
  - Quarter-string → filed_date conversion
  - Idempotency: document_exists short-circuits embed + ingest
  - Rows with zero holders / missing ticker-or-quarter skipped
  - Optional ticker allowlist (mirrors --tickers / --from-signals)
  - dry_run mode skips embed/ingest
  - Failures isolated per-document, batch continues
  - Empty/None inst_ownership_df handled gracefully (no crash)
  - Stats dict shape
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rag.pipelines.ingest_13f import (
    _fmt_pct,
    _fmt_shares,
    _quarter_end_date,
    ingest_inst_ownership,
    row_to_chunk_text,
)


def _make_row(**overrides) -> pd.Series:
    base = {
        "ticker": "AAPL",
        "quarter": "2026Q2",
        "n_funds_holding": 18,
        "total_shares_held": 450_200_000.0,
        "total_value_usd": 90_000_000_000.0,
        "shares_qoq_change": 2_100_000.0,
        "value_qoq_change": 500_000_000.0,
        "top5_concentration_pct": 8.2,
        "n_funds_increasing": 12,
        "n_funds_decreasing": 3,
        "n_funds_new": 1,
        "n_funds_exited": 0,
    }
    base.update(overrides)
    return pd.Series(base)


# ── Formatting helpers ──────────────────────────────────────────────────


class TestFormatHelpers:
    def test_quarter_end_date_q2(self):
        assert _quarter_end_date("2026Q2") == date(2026, 6, 30)

    def test_quarter_end_date_q4(self):
        assert _quarter_end_date("2026Q4") == date(2026, 12, 31)

    def test_quarter_end_date_q1(self):
        assert _quarter_end_date("2026Q1") == date(2026, 3, 31)

    def test_fmt_shares_unsigned_for_totals(self):
        assert _fmt_shares(450_200_000.0) == "450.2M"

    def test_fmt_shares_signed_for_deltas(self):
        assert _fmt_shares(2_100_000.0, signed=True) == "+2.1M"
        assert _fmt_shares(-1_500_000.0, signed=True) == "-1.5M"

    def test_fmt_shares_thousands_scale(self):
        assert _fmt_shares(2_500.0) == "2.5K"

    def test_fmt_shares_none_or_nan(self):
        assert _fmt_shares(None) == "an unknown number of"
        assert _fmt_shares(float("nan")) == "an unknown number of"

    def test_fmt_pct_signed(self):
        assert _fmt_pct(4.3) == "+4.3%"
        assert _fmt_pct(-2.1) == "-2.1%"

    def test_fmt_pct_none(self):
        assert _fmt_pct(None) is None
        assert _fmt_pct(float("nan")) is None


# ── row_to_chunk_text ────────────────────────────────────────────────────


class TestRowToChunkText:
    def test_full_row_produces_expected_narrative(self):
        row = _make_row()
        text = row_to_chunk_text(row)
        assert "Q2 2026" in text
        assert "AAPL" in text
        assert "12 funds increased holdings" in text
        assert "3 decreased" in text
        assert "1 opened new positions" in text
        assert "0 fully exited" in text
        assert "+2.1M shares" in text
        assert "18 funds hold a combined 450.2M shares" in text
        assert "Top-5 funds hold 8.2%" in text

    def test_qoq_pct_change_computed_from_shares_and_total(self):
        row = _make_row(shares_qoq_change=2_100_000.0, total_shares_held=450_200_000.0)
        text = row_to_chunk_text(row)
        # prior = 450.2M - 2.1M = 448.1M; pct = 2.1M / 448.1M ~ 0.47%
        assert "QoQ)" in text

    def test_missing_qoq_baseline_omits_net_change_sentence(self):
        """First observed quarter for a ticker: shares_qoq_change is None
        (no prior-quarter baseline). Chunk still produces a valid
        narrative, just without the 'Net change' sentence."""
        row = _make_row(shares_qoq_change=None, value_qoq_change=None)
        text = row_to_chunk_text(row)
        assert "Net change" not in text
        assert "AAPL" in text

    def test_missing_top5_concentration_omitted(self):
        row = _make_row(top5_concentration_pct=None)
        text = row_to_chunk_text(row)
        assert "Top-5" not in text

    def test_negative_net_accumulation_renders_negative_sign(self):
        row = _make_row(
            n_funds_increasing=2, n_funds_decreasing=9,
            shares_qoq_change=-1_500_000.0,
        )
        text = row_to_chunk_text(row)
        assert "-1.5M shares" in text


# ── ingest_inst_ownership ────────────────────────────────────────────────


class TestIngestInstOwnership:
    def test_empty_dataframe_returns_zero_stats_no_crash(self):
        stats = ingest_inst_ownership(
            pd.DataFrame(),
            embed_texts_fn=MagicMock(),
            document_exists_fn=MagicMock(),
            ingest_document_fn=MagicMock(),
        )
        assert stats["n_rows_input"] == 0
        assert stats["n_documents_ingested"] == 0

    def test_none_dataframe_returns_zero_stats_no_crash(self):
        stats = ingest_inst_ownership(
            None,
            embed_texts_fn=MagicMock(),
            document_exists_fn=MagicMock(),
            ingest_document_fn=MagicMock(),
        )
        assert stats["n_rows_input"] == 0
        assert stats["n_documents_ingested"] == 0

    def test_one_row_one_document(self):
        df = pd.DataFrame([_make_row()])
        embed = MagicMock(return_value=[[0.1, 0.2, 0.3]])
        exists = MagicMock(return_value=False)
        ingest = MagicMock(return_value="doc-id-1")

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=ingest,
        )

        assert stats["n_documents_ingested"] == 1
        assert stats["n_failures"] == 0
        ingest.assert_called_once()
        kwargs = ingest.call_args.kwargs
        assert kwargs["ticker"] == "AAPL"
        assert kwargs["doc_type"] == "13F"
        assert kwargs["source"] == "sec_13f_bulk"
        assert kwargs["filed_date"] == date(2026, 6, 30)
        assert len(kwargs["chunks"]) == 1
        assert kwargs["chunks"][0]["embedding"] == [0.1, 0.2, 0.3]

    def test_idempotency_via_document_exists(self):
        df = pd.DataFrame([_make_row()])
        embed = MagicMock()
        exists = MagicMock(return_value=True)
        ingest = MagicMock()

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=ingest,
        )

        assert stats["n_documents_skipped_exists"] == 1
        assert stats["n_documents_ingested"] == 0
        embed.assert_not_called()
        ingest.assert_not_called()

    def test_zero_funds_holding_row_skipped(self):
        """A ticker resolved from the CUSIP crosswalk but with 0 funds
        holding it this quarter (edge case in the derived table) has
        nothing to narrate — skip rather than emit an empty summary."""
        df = pd.DataFrame([_make_row(n_funds_holding=0)])
        embed = MagicMock()
        ingest = MagicMock()

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_skipped_no_data"] == 1
        assert stats["n_documents_ingested"] == 0
        embed.assert_not_called()

    def test_missing_ticker_or_quarter_skipped(self):
        df = pd.DataFrame([
            _make_row(ticker=""),
            _make_row(quarter=None),
        ])
        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=MagicMock(),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=MagicMock(),
        )
        assert stats["n_documents_skipped_no_data"] == 2
        assert stats["n_documents_ingested"] == 0

    def test_ticker_allowlist_filters_rows(self):
        df = pd.DataFrame([_make_row(ticker="AAPL"), _make_row(ticker="MSFT")])
        ingest = MagicMock(return_value="doc-id")
        stats = ingest_inst_ownership(
            df,
            tickers=["MSFT"],
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_ingested"] == 1
        assert ingest.call_args.kwargs["ticker"] == "MSFT"

    def test_dry_run_skips_embed_and_ingest(self):
        df = pd.DataFrame([_make_row()])
        embed = MagicMock()
        ingest = MagicMock()
        stats = ingest_inst_ownership(
            df,
            dry_run=True,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_ingested"] == 1
        embed.assert_not_called()
        ingest.assert_not_called()

    def test_failure_isolated_per_row_batch_continues(self):
        df = pd.DataFrame([_make_row(ticker="AAPL"), _make_row(ticker="MSFT")])
        embed = MagicMock(return_value=[[0.0]])
        ingest = MagicMock(side_effect=[RuntimeError("db down"), "doc-id-2"])

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_failures"] == 1
        assert stats["n_documents_ingested"] == 1

    def test_sector_lookup_passed_through(self):
        df = pd.DataFrame([_make_row(ticker="AAPL")])
        ingest = MagicMock(return_value="doc-id")
        ingest_inst_ownership(
            df,
            ticker_to_sector={"AAPL": "Information Technology"},
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert ingest.call_args.kwargs["sector"] == "Information Technology"

    def test_stats_dict_shape(self):
        df = pd.DataFrame([_make_row()])
        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=MagicMock(return_value=[[0.0]]),
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=MagicMock(return_value="doc-id"),
        )
        assert set(stats.keys()) == {
            "n_rows_input",
            "n_documents_attempted",
            "n_documents_skipped_exists",
            "n_documents_skipped_no_data",
            "n_documents_ingested",
            "n_failures",
        }


# ── Batched embeddings (config#2956 deliverable 3) ──────────────────────


class TestBatchedEmbeddings:
    def test_one_embed_call_for_multiple_pending_rows(self):
        """The N-row batch must call embed_texts_fn ONCE with all N chunk
        bodies, not once per row (the previous shape)."""
        df = pd.DataFrame([
            _make_row(ticker="AAPL"),
            _make_row(ticker="MSFT"),
            _make_row(ticker="GOOGL"),
        ])
        embed = MagicMock(return_value=[[0.1], [0.2], [0.3]])
        ingest = MagicMock(side_effect=lambda **kw: f"doc-{kw['ticker']}")

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )

        embed.assert_called_once()
        (texts_arg,), _ = embed.call_args
        assert len(texts_arg) == 3
        assert stats["n_documents_ingested"] == 3

    def test_each_row_gets_its_own_embedding_by_position(self):
        df = pd.DataFrame([
            _make_row(ticker="AAPL"),
            _make_row(ticker="MSFT"),
        ])
        embed = MagicMock(return_value=[["embA"], ["embB"]])
        ingest = MagicMock(return_value="doc-id")

        ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )

        calls_by_ticker = {c.kwargs["ticker"]: c.kwargs["chunks"][0]["embedding"] for c in ingest.call_args_list}
        assert calls_by_ticker["AAPL"] == ["embA"]
        assert calls_by_ticker["MSFT"] == ["embB"]

    def test_skipped_existing_rows_excluded_from_embed_batch(self):
        df = pd.DataFrame([
            _make_row(ticker="AAPL"),
            _make_row(ticker="MSFT"),
        ])
        embed = MagicMock(return_value=[["embA"]])
        exists = MagicMock(side_effect=lambda ticker, *a: ticker == "MSFT")

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=exists,
            ingest_document_fn=MagicMock(return_value="doc"),
        )

        (texts_arg,), _ = embed.call_args
        assert len(texts_arg) == 1
        assert stats["n_documents_skipped_exists"] == 1
        assert stats["n_documents_ingested"] == 1

    def test_no_embed_call_when_nothing_pending(self):
        df = pd.DataFrame([_make_row(ticker="AAPL")])
        embed = MagicMock()

        ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=True),
            ingest_document_fn=MagicMock(),
        )

        embed.assert_not_called()

    def test_batch_level_embed_failure_counts_all_pending_as_failures(self):
        df = pd.DataFrame([
            _make_row(ticker="AAPL"),
            _make_row(ticker="MSFT"),
        ])
        embed = MagicMock(side_effect=RuntimeError("voyage API down"))
        ingest = MagicMock()

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )

        assert stats["n_failures"] == 2
        assert stats["n_documents_ingested"] == 0
        ingest.assert_not_called()

    def test_per_document_ingest_failure_still_isolated_after_batching(self):
        df = pd.DataFrame([
            _make_row(ticker="AAPL"),
            _make_row(ticker="MSFT"),
            _make_row(ticker="GOOGL"),
        ])

        def ingest_side_effect(*, ticker, **kw):
            if ticker == "MSFT":
                raise RuntimeError("pgvector temporary failure")
            return f"doc-{ticker}"

        embed = MagicMock(return_value=[[0.0], [0.0], [0.0]])
        ingest = MagicMock(side_effect=ingest_side_effect)

        stats = ingest_inst_ownership(
            df,
            embed_texts_fn=embed,
            document_exists_fn=MagicMock(return_value=False),
            ingest_document_fn=ingest,
        )
        assert stats["n_documents_ingested"] == 2
        assert stats["n_failures"] == 1
        embed.assert_called_once()
