"""Tests for collectors/daily_news.py — the weekday daily news producer.

Heavier integration of NewsAggregator + NLP + parquet writer is covered in their
own Wave 1 PRs; here we test the daily orchestrator shape: universe assembly
(holdings ∪ signals, fail-soft) and the collect() control flow with the network
+ S3 layers mocked.
"""

from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

from collectors import daily_news


def _mock_s3(holdings=None, signals_universe=None):
    """S3 mock serving the holdings_universe.json key and a signals.json."""
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "signals/2026-06-05/"}],
    }

    def _get_object(Bucket, Key):
        if Key == daily_news.HOLDINGS_UNIVERSE_KEY:
            if holdings is None:
                raise RuntimeError("NoSuchKey")
            return {"Body": BytesIO(json.dumps({"tickers": holdings}).encode())}
        if Key.endswith("signals.json"):
            uni = [{"ticker": t} for t in (signals_universe or [])]
            return {"Body": BytesIO(json.dumps({"universe": uni}).encode())}
        raise RuntimeError(f"unexpected key {Key}")

    s3.get_object.side_effect = _get_object
    return s3


def test_assemble_universe_unions_dedupes_sorts():
    s3 = _mock_s3(holdings=["AAPL", "tsla"], signals_universe=["AAPL", "MSFT"])
    assert daily_news.assemble_universe("b", s3) == ["AAPL", "MSFT", "TSLA"]


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
    agg = MagicMock()
    agg.fetch.return_value = []
    return agg


@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_dry_run_does_not_write(mock_agg, mock_nlp):
    mock_agg.return_value = _fake_aggregator()
    s3 = _mock_s3(holdings=["AAPL"], signals_universe=["MSFT"])
    out = daily_news.collect("b", s3_client=s3, dry_run=True)
    assert out["status"] == "ok_dry_run"
    assert out["tickers"] == 2


@patch("data.derived.news_aggregates.aggregate_and_write")
@patch("collectors.daily_news._build_nlp_pipeline")
@patch("collectors.daily_news._build_aggregator")
def test_collect_writes_to_daily_prefix(mock_agg, mock_nlp, mock_write):
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
