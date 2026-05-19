"""Tests for the analyst substrate (Wave 1 PR C).

Covers:
  - YfinanceAnalystAdapter (rating normalization, missing fields, fetch failures)
  - FinnhubAnalystAdapter (bucket → consensus rating ladder, totals=0 returns None)
  - Paid stubs raise NotImplementedError
  - Protocol structural subtyping
  - Daily snapshotter (per-ticker write + multi-source merge + round-trip)
  - Self-derived revisions (deltas / missing history → None / weekend gap walk-back)
  - End-to-end compute_and_write_revisions
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest

from alpha_engine_lib.sources import AnalystSnapshot, AnalystSource

from collectors.analyst_sources.finnhub import FinnhubAnalystAdapter
from collectors.analyst_sources.ibes import IbesAnalystAdapter
from collectors.analyst_sources.visible_alpha import VisibleAlphaAnalystAdapter
from collectors.analyst_sources.yfinance import YfinanceAnalystAdapter
from data.derived.analyst_revisions import (
    SCHEMA_VERSION as REVISIONS_SCHEMA_VERSION,
    AnalystRevisionRow,
    build_revision_row,
    compute_and_write_revisions,
    rows_to_dataframe,
)
from data.snapshotter.analyst_daily import (
    SCHEMA_VERSION as SNAPSHOT_SCHEMA_VERSION,
    read_snapshot_document,
    snapshot_one_ticker,
    snapshot_universe,
    write_snapshot_document,
)


# ── In-memory S3 mock (reused pattern) ─────────────────────────────────


class _InMemoryS3:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise RuntimeError("NoSuchKey")
        return {"Body": BytesIO(self.store[(Bucket, Key)])}

    def list_objects_v2(self, *, Bucket, Prefix="", **kwargs):
        contents = [
            {"Key": key} for (b, key) in self.store
            if b == Bucket and key.startswith(Prefix)
        ]
        return {"Contents": contents}


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── yfinance adapter ──────────────────────────────────────────────────


class TestYfinanceAdapter:
    def test_normalizes_rating_strings(self):
        fake_yf = MagicMock()
        fake_yf.Ticker.return_value.info = {
            "recommendationKey": "strong_buy",
            "targetMeanPrice": 250.0,
            "targetMedianPrice": 248.5,
            "numberOfAnalystOpinions": 18,
        }
        adapter = YfinanceAnalystAdapter(yf_module=fake_yf)
        snap = adapter.fetch("AAPL")
        assert snap.ticker == "AAPL"
        assert snap.source == "yfinance"
        assert snap.consensus_rating == "strongBuy"
        assert snap.mean_target == 250.0
        assert snap.median_target == 248.5
        assert snap.num_analysts == 18

    def test_handles_buy_underperform_sell_strong_sell(self):
        cases = [
            ("buy", "buy"),
            ("hold", "hold"),
            ("underperform", "sell"),
            ("sell", "sell"),
            ("strong_sell", "strongSell"),
        ]
        for yf_key, canonical in cases:
            fake_yf = MagicMock()
            fake_yf.Ticker.return_value.info = {
                "recommendationKey": yf_key,
                "numberOfAnalystOpinions": 10,
            }
            adapter = YfinanceAnalystAdapter(yf_module=fake_yf)
            snap = adapter.fetch("X")
            assert snap.consensus_rating == canonical

    def test_unrecognized_rating_returns_none(self):
        fake_yf = MagicMock()
        fake_yf.Ticker.return_value.info = {
            "recommendationKey": "completely-unknown",
            "numberOfAnalystOpinions": 5,
        }
        adapter = YfinanceAnalystAdapter(yf_module=fake_yf)
        snap = adapter.fetch("X")
        assert snap.consensus_rating is None
        assert snap.num_analysts == 5

    def test_nan_target_handled_as_none(self):
        fake_yf = MagicMock()
        fake_yf.Ticker.return_value.info = {
            "targetMeanPrice": float("nan"),
            "numberOfAnalystOpinions": 1,
        }
        adapter = YfinanceAnalystAdapter(yf_module=fake_yf)
        snap = adapter.fetch("X")
        assert snap.mean_target is None

    def test_fetch_failure_returns_none(self):
        fake_yf = MagicMock()
        fake_yf.Ticker.side_effect = RuntimeError("yfinance down")
        adapter = YfinanceAnalystAdapter(yf_module=fake_yf)
        assert adapter.fetch("X") is None

    def test_empty_info_returns_none(self):
        fake_yf = MagicMock()
        fake_yf.Ticker.return_value.info = {}
        adapter = YfinanceAnalystAdapter(yf_module=fake_yf)
        assert adapter.fetch("X") is None


# ── Finnhub adapter ────────────────────────────────────────────────────


class TestFinnhubAdapter:
    def test_bullish_recommendation_returns_buy(self):
        fetcher = MagicMock(return_value=[{
            "strongBuy": 3, "buy": 8, "hold": 2, "sell": 1, "strongSell": 0,
        }])
        adapter = FinnhubAnalystAdapter(finnhub_get_fn=fetcher)
        snap = adapter.fetch("AAPL")
        assert snap.consensus_rating == "buy"
        assert snap.num_analysts == 14
        assert snap.mean_target is None  # paid-tier only

    def test_bearish_returns_sell(self):
        fetcher = MagicMock(return_value=[{
            "strongBuy": 0, "buy": 1, "hold": 3, "sell": 5, "strongSell": 2,
        }])
        adapter = FinnhubAnalystAdapter(finnhub_get_fn=fetcher)
        assert adapter.fetch("X").consensus_rating == "sell"

    def test_balanced_returns_hold(self):
        fetcher = MagicMock(return_value=[{
            "strongBuy": 1, "buy": 2, "hold": 5, "sell": 2, "strongSell": 1,
        }])
        adapter = FinnhubAnalystAdapter(finnhub_get_fn=fetcher)
        assert adapter.fetch("X").consensus_rating == "hold"

    def test_zero_coverage_returns_none(self):
        fetcher = MagicMock(return_value=[{
            "strongBuy": 0, "buy": 0, "hold": 0, "sell": 0, "strongSell": 0,
        }])
        adapter = FinnhubAnalystAdapter(finnhub_get_fn=fetcher)
        assert adapter.fetch("X") is None

    def test_empty_response_returns_none(self):
        fetcher = MagicMock(return_value=[])
        adapter = FinnhubAnalystAdapter(finnhub_get_fn=fetcher)
        assert adapter.fetch("X") is None

    def test_fetch_failure_returns_none(self):
        fetcher = MagicMock(side_effect=RuntimeError("finnhub 429"))
        adapter = FinnhubAnalystAdapter(finnhub_get_fn=fetcher)
        assert adapter.fetch("X") is None


# ── Paid stubs ─────────────────────────────────────────────────────────


class TestPaidStubs:
    def test_ibes_stub_raises_on_init(self):
        with pytest.raises(NotImplementedError, match="Phase 4"):
            IbesAnalystAdapter()

    def test_visible_alpha_stub_raises_on_init(self):
        with pytest.raises(NotImplementedError, match="Phase 4"):
            VisibleAlphaAnalystAdapter()


# ── Protocol structural subtyping ──────────────────────────────────────


def test_yfinance_satisfies_analyst_source():
    assert isinstance(
        YfinanceAnalystAdapter(yf_module=MagicMock()), AnalystSource,
    )


def test_finnhub_satisfies_analyst_source():
    assert isinstance(
        FinnhubAnalystAdapter(finnhub_get_fn=MagicMock()), AnalystSource,
    )


# ── Daily snapshotter ──────────────────────────────────────────────────


class _StaticSource:
    """Test helper: an AnalystSource that returns a pre-built snapshot."""
    def __init__(self, name: str, snapshot: AnalystSnapshot | None) -> None:
        self.name = name
        self._snapshot = snapshot

    def fetch(self, ticker: str) -> AnalystSnapshot | None:
        return self._snapshot


def _make_snapshot(
    *, source: str = "yfinance", ticker: str = "AAPL",
    rating: str | None = "buy", target: float | None = 250.0,
    num: int | None = 12,
) -> AnalystSnapshot:
    return AnalystSnapshot(
        ticker=ticker, source=source, fetched_at=_now(),
        consensus_rating=rating, mean_target=target,
        num_analysts=num,
    )


class TestSnapshotter:
    def test_snapshot_one_ticker_calls_each_source(self):
        snap_a = _make_snapshot(source="yfinance", target=250.0)
        snap_b = _make_snapshot(source="finnhub", rating="hold", target=None)
        sources = [_StaticSource("yfinance", snap_a), _StaticSource("finnhub", snap_b)]
        result = snapshot_one_ticker("AAPL", sources)
        assert set(result.keys()) == {"yfinance", "finnhub"}

    def test_snapshot_skips_sources_that_return_none(self):
        sources = [
            _StaticSource("yfinance", _make_snapshot()),
            _StaticSource("missing", None),
        ]
        result = snapshot_one_ticker("AAPL", sources)
        assert "yfinance" in result
        assert "missing" not in result

    def test_snapshot_isolated_per_source_exception(self):
        good = _StaticSource("yfinance", _make_snapshot())

        class _Broken:
            name = "broken"

            def fetch(self, ticker):
                raise RuntimeError("kaboom")

        result = snapshot_one_ticker("AAPL", [good, _Broken()])
        assert "yfinance" in result
        assert "broken" not in result

    def test_write_and_read_snapshot_document(self):
        s3 = _InMemoryS3()
        snap = _make_snapshot()
        key = write_snapshot_document(
            ticker="AAPL",
            snapshot_date=date(2026, 5, 13),
            snapshots={"yfinance": snap},
            s3_client=s3,
            run_id="2605131944",
        )
        # Canonical shape: YYMMDDHHMM-encoded artifact key.
        # The lib's eval_artifact_key shortens basename='result.json' to
        # just {run_id}.json (default-case shortcut).
        assert key == "data/analyst_snapshots/AAPL/2605131944.json"
        # latest.json sidecar written alongside
        assert ("alpha-engine-research",
                "data/analyst_snapshots/AAPL/latest.json") in s3.store

        doc = read_snapshot_document(
            "AAPL", date(2026, 5, 13), s3_client=s3,
        )
        assert doc is not None
        assert doc["ticker"] == "AAPL"
        assert doc["schema_version"] == SNAPSHOT_SCHEMA_VERSION
        assert "yfinance" in doc["snapshots_by_source"]
        assert doc["snapshots_by_source"]["yfinance"]["mean_target"] == 250.0

    def test_read_missing_document_returns_none(self):
        s3 = _InMemoryS3()
        assert read_snapshot_document("X", date(2026, 1, 1), s3_client=s3) is None

    def test_legacy_date_keyed_json_is_ignored(self):
        """Regression guard: post-#234 canonical-key migration, a
        bare ``{prefix}/{ticker}/{date}.json`` file (the pre-migration
        shape) must NOT be read by the canonical lister. The list
        prefix is ``{ticker}/{YYMMDD}`` so an ``YYYY-MM-DD.json``
        file does not match it.
        """
        s3 = _InMemoryS3()
        legacy_body = json.dumps({
            "ticker": "AAPL",
            "snapshot_date": "2026-05-13",
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "snapshots_by_source": {},
        }).encode("utf-8")
        s3.put_object(
            Bucket="alpha-engine-research",
            Key="data/analyst_snapshots/AAPL/2026-05-13.json",
            Body=legacy_body,
        )
        # Canonical reader lists by YYMMDD prefix; legacy YYYY-MM-DD key
        # does not match → None.
        doc = read_snapshot_document(
            "AAPL", date(2026, 5, 13), s3_client=s3,
        )
        assert doc is None

    def test_universe_orchestrator(self):
        s3 = _InMemoryS3()
        sources = [_StaticSource("yfinance", _make_snapshot())]
        stats = snapshot_universe(
            ["AAPL", "MSFT"], sources,
            snapshot_date=date(2026, 5, 13), s3_client=s3,
        )
        assert stats["n_documents_written"] == 2
        # Canonical shape: per-ticker latest.json sidecar present for each.
        # Artifact keys carry YYMMDDHHMM run_id (varies per run) so just
        # check the sidecar exists for both tickers.
        keys = {key for (_b, key) in s3.store}
        assert "data/analyst_snapshots/AAPL/latest.json" in keys
        assert "data/analyst_snapshots/MSFT/latest.json" in keys

    def test_universe_orchestrator_dry_run_skips_writes(self):
        s3 = _InMemoryS3()
        stats = snapshot_universe(
            ["AAPL"], [_StaticSource("yfinance", _make_snapshot())],
            snapshot_date=date(2026, 5, 13), s3_client=s3, dry_run=True,
        )
        assert stats["n_documents_written"] == 1
        assert len(s3.store) == 0

    def test_universe_orchestrator_counts_zero_coverage(self):
        s3 = _InMemoryS3()
        sources = [_StaticSource("yfinance", None)]
        stats = snapshot_universe(
            ["X"], sources,
            snapshot_date=date(2026, 5, 13), s3_client=s3,
        )
        assert stats["n_tickers_with_zero_coverage"] == 1


# ── Self-derived revisions ────────────────────────────────────────────


def _doc(target: float | None, rating: str | None = "buy", num: int | None = 12) -> dict:
    """Build a snapshot document as the snapshotter would write."""
    snaps: dict[str, dict] = {}
    if target is not None or rating is not None or num is not None:
        snaps["yfinance"] = {
            "ticker": "AAPL", "source": "yfinance",
            "consensus_rating": rating, "mean_target": target,
            "median_target": None, "num_analysts": num,
            "rating_changes_30d": [], "fetched_at": _now().isoformat(),
        }
    return {
        "ticker": "AAPL", "snapshot_date": "x",
        "schema_version": 1, "fetched_at": "x",
        "snapshots_by_source": snaps,
    }


class TestRevisionRowBuilder:
    def test_30d_revision_computes_deltas(self):
        as_of = date(2026, 5, 13)
        docs = {
            as_of: _doc(target=260.0, num=14),
            as_of - timedelta(days=30): _doc(target=240.0, num=12),
        }
        row = build_revision_row(
            ticker="AAPL", as_of_date=as_of,
            snapshot_documents_by_date=docs,
        )
        assert row.mean_target_current == 260.0
        assert row.mean_target_30d_ago == 240.0
        assert row.mean_target_delta_30d == 20.0
        assert row.mean_target_pct_change_30d == pytest.approx(20.0 / 240.0, rel=1e-3)
        assert row.num_analysts_delta_30d == 2

    def test_no_history_yields_none_deltas(self):
        """First day of snapshotter operation: only today's doc exists."""
        as_of = date(2026, 5, 13)
        docs = {as_of: _doc(target=250.0)}
        row = build_revision_row(
            ticker="AAPL", as_of_date=as_of,
            snapshot_documents_by_date=docs,
        )
        assert row.mean_target_current == 250.0
        assert row.mean_target_30d_ago is None
        assert row.mean_target_delta_30d is None
        assert row.mean_target_pct_change_30d is None

    def test_weekend_gap_walks_back(self):
        """30 days ago was a Sunday; the snapshotter has a doc 31 days
        ago — walk back to find a usable historical anchor."""
        as_of = date(2026, 5, 13)
        docs = {
            as_of: _doc(target=260.0),
            as_of - timedelta(days=31): _doc(target=240.0),
            # day -30 explicitly missing
        }
        row = build_revision_row(
            ticker="AAPL", as_of_date=as_of,
            snapshot_documents_by_date=docs,
        )
        assert row.mean_target_30d_ago == 240.0
        assert row.mean_target_delta_30d == 20.0

    def test_rating_changed_30d_flag(self):
        as_of = date(2026, 5, 13)
        docs = {
            as_of: _doc(target=260.0, rating="buy"),
            as_of - timedelta(days=30): _doc(target=240.0, rating="hold"),
        }
        row = build_revision_row(
            ticker="AAPL", as_of_date=as_of,
            snapshot_documents_by_date=docs,
        )
        assert row.consensus_rating_current == "buy"
        assert row.consensus_rating_30d_ago == "hold"
        assert row.rating_changed_30d is True

    def test_same_rating_no_change_flag(self):
        as_of = date(2026, 5, 13)
        docs = {
            as_of: _doc(target=260.0, rating="buy"),
            as_of - timedelta(days=30): _doc(target=250.0, rating="buy"),
        }
        row = build_revision_row(
            ticker="AAPL", as_of_date=as_of,
            snapshot_documents_by_date=docs,
        )
        assert row.rating_changed_30d is False

    def test_n_snapshot_days_observed(self):
        as_of = date(2026, 5, 13)
        docs = {
            as_of: _doc(target=260.0),
            as_of - timedelta(days=5): _doc(target=255.0),
            as_of - timedelta(days=10): _doc(target=250.0),
        }
        row = build_revision_row(
            ticker="AAPL", as_of_date=as_of,
            snapshot_documents_by_date=docs,
        )
        assert row.n_snapshot_days_observed == 3

    def test_schema_version_pinned(self):
        as_of = date(2026, 5, 13)
        row = build_revision_row(
            ticker="X", as_of_date=as_of, snapshot_documents_by_date={},
        )
        assert row.schema_version == REVISIONS_SCHEMA_VERSION


class TestRevisionsDataFrame:
    def test_empty_yields_empty_df_with_schema(self):
        df = rows_to_dataframe([])
        assert len(df) == 0
        for col in AnalystRevisionRow.__dataclass_fields__:
            assert col in df.columns

    def test_round_trip(self):
        row = AnalystRevisionRow(
            ticker="AAPL", as_of_date=date(2026, 5, 13),
            schema_version=1, primary_source="yfinance",
            mean_target_current=260.0, mean_target_7d_ago=255.0,
            mean_target_30d_ago=240.0, mean_target_delta_7d=5.0,
            mean_target_delta_30d=20.0, mean_target_pct_change_30d=0.083,
            num_analysts_current=14, num_analysts_30d_ago=12,
            num_analysts_delta_30d=2,
            consensus_rating_current="buy",
            consensus_rating_30d_ago="hold",
            rating_changed_30d=True, n_snapshot_days_observed=15,
        )
        df = rows_to_dataframe([row])
        assert len(df) == 1
        assert df.iloc[0]["mean_target_delta_30d"] == 20.0


# ── End-to-end orchestrator ───────────────────────────────────────────


class TestComputeAndWriteRevisions:
    def test_end_to_end(self):
        s3 = _InMemoryS3()
        # Seed S3 with two days of snapshots for AAPL
        as_of = date(2026, 5, 13)
        for d, target in [
            (as_of, 260.0),
            (as_of - timedelta(days=30), 240.0),
        ]:
            doc = _doc(target=target)
            doc["snapshot_date"] = d.isoformat()
            s3.store[(
                "alpha-engine-research",
                f"data/analyst_snapshots/AAPL/{d.isoformat()}.json",
            )] = json.dumps(doc).encode("utf-8")

        key, rows = compute_and_write_revisions(
            ["AAPL"],
            as_of_date=as_of,
            s3_client=s3,
        )
        # Canonical shape: artifact key uses YYMMDDHHMM run_id
        assert key.startswith("data/analyst_revisions/")
        assert key.endswith("_result.parquet")
        assert len(rows) == 1
        assert rows[0].mean_target_delta_30d == 20.0
        # Parquet + latest.json sidecar both written
        assert ("alpha-engine-research", key) in s3.store
        assert ("alpha-engine-research",
                "data/analyst_revisions/latest.json") in s3.store


def test_canonical_artifact_key_shape():
    """Canonical key shape (post-PR-1 migration) — YYMMDDHHMM run_id
    + flat layout per the alpha_engine_lib.eval_artifacts module."""
    from alpha_engine_lib.eval_artifacts import (
        eval_artifact_key, eval_latest_key, new_eval_run_id,
    )
    run_id = new_eval_run_id()
    assert len(run_id) == 10
    assert (
        eval_artifact_key("data/analyst_revisions", run_id, basename="result.parquet")
        == f"data/analyst_revisions/{run_id}_result.parquet"
    )
    assert (
        eval_latest_key("data/analyst_snapshots/AAPL")
        == "data/analyst_snapshots/AAPL/latest.json"
    )
