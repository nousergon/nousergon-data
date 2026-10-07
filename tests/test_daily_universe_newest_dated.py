"""The weekday EOD universe follows this morning's constituents refresh.

2026-10-07: MorningEnrich wrote ``weekly/2026-10-06/constituents.json`` without
WBD/PSKY (and with SKYD), but the EOD collection read the Saturday-only
``latest_weekly.json`` pointer (2026-10-02), still refreshed both dead tickers,
and hard-failed D03.
"""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

import weekly_collector
from builders._constituents_loader import load_newest_dated_constituents

BUCKET = "alpha-engine-research"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        def put(key, body):
            client.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(body).encode())

        put("market_data/weekly/2026-10-02/constituents.json", {"tickers": ["AAPL", "WBD", "PSKY"]})
        put("market_data/weekly/2026-10-06/constituents.json", {"tickers": ["AAPL", "SKYD"]})
        put("market_data/weekly/2026-10-09/constituents.json", {"tickers": ["AAPL", "FUTURE"]})
        put("market_data/latest_weekly.json", {"date": "2026-10-02", "s3_prefix": "market_data/weekly/2026-10-02/"})
        yield client


def test_newest_dated_on_or_before_wins(s3):
    tickers, dated = load_newest_dated_constituents(s3, BUCKET, "2026-10-07")
    assert dated == "2026-10-06"
    assert tickers == {"AAPL", "SKYD"}


def test_a_partition_without_a_constituents_file_is_skipped(s3):
    s3.put_object(Bucket=BUCKET, Key="market_data/weekly/2026-10-07/other.json", Body=b"{}")
    _, dated = load_newest_dated_constituents(s3, BUCKET, "2026-10-07")
    assert dated == "2026-10-06"


def test_nothing_on_or_before_raises(s3):
    with pytest.raises(RuntimeError):
        load_newest_dated_constituents(s3, BUCKET, "2026-01-01")


def test_eod_universe_drops_a_midweek_delisting(s3):
    tickers = weekly_collector._load_daily_universe_tickers({"bucket": BUCKET}, "2026-10-07")
    assert "SKYD" in tickers and "WBD" not in tickers and "PSKY" not in tickers
    assert "SPY" in tickers  # macro union still applied


def test_without_run_date_the_pointer_still_answers(s3):
    tickers = weekly_collector._load_daily_universe_tickers({"bucket": BUCKET})
    assert "WBD" in tickers
