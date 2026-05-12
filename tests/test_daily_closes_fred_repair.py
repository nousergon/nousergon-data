"""Tests for the FRED-repair one-shot script.

Locks the repair output to be byte-identical to a fresh windowed run of
the fixed ``_fetch_fred_closes``: per-date FRED value taken from the
most-recent-on-or-before observation, OHLC + Adj_Close all set to that
value, Volume=0 / VWAP=None / source=fred.
"""

from __future__ import annotations

import io
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors import daily_closes_fred_repair as repair_mod


@pytest.fixture
def fred_api_key(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "test-key-xyz")


def _existing_parquet(closes: dict[str, dict]) -> bytes:
    """Build a daily_closes-shape parquet (index=ticker)."""
    rows = []
    idx = []
    for ticker, fields in closes.items():
        idx.append(ticker)
        rows.append(fields)
    df = pd.DataFrame(rows, index=pd.Index(idx, name="ticker"))
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    return buf.getvalue()


def _s3_with_parquet_bodies(bodies: dict[str, bytes]) -> MagicMock:
    """S3 mock that returns parquet bodies by key, 404 for unknown keys."""
    s3 = MagicMock()

    def fake_get_object(Bucket=None, Key=None):
        if Key not in bodies:
            err = {"Error": {"Code": "404", "Message": "Not Found"}}
            raise ClientError(err, "GetObject")
        return {"Body": MagicMock(read=lambda body=bodies[Key]: body)}

    def fake_put_object(Bucket=None, Key=None, Body=None, **_):
        bodies[Key] = Body  # capture writes back into the same dict

    s3.get_object.side_effect = fake_get_object
    s3.put_object.side_effect = fake_put_object
    return s3


# ── business-day enumeration ────────────────────────────────────────────────


def test_business_days_inclusive_window():
    # 2026-04-22 is a Wed; 2026-04-28 is a Tue. The window covers
    # Wed 4/22, Thu 4/23, Fri 4/24, (skip Sat/Sun), Mon 4/27, Tue 4/28.
    days = repair_mod._business_days("2026-04-22", "2026-04-28")
    assert days == ["2026-04-22", "2026-04-23", "2026-04-24", "2026-04-27", "2026-04-28"]


def test_business_days_rejects_inverted_range():
    with pytest.raises(ValueError, match="must be <="):
        repair_mod._business_days("2026-04-28", "2026-04-22")


# ── on-or-before lookup ─────────────────────────────────────────────────────


def test_value_on_or_before_picks_exact_date_when_present():
    fred = {"2026-04-22": 18.36, "2026-04-23": 18.10, "2026-04-24": 17.95}
    sorted_dates = sorted(fred.keys())
    obs = repair_mod._value_on_or_before(fred, "2026-04-23", sorted_dates)
    assert obs == ("2026-04-23", 18.10)


def test_value_on_or_before_falls_back_to_prior_date_when_missing():
    # FRED has no observation for 2026-04-22 (weekend / holiday); the
    # most recent on or before is 2026-04-21.
    fred = {"2026-04-21": 18.30, "2026-04-23": 18.10}
    sorted_dates = sorted(fred.keys())
    obs = repair_mod._value_on_or_before(fred, "2026-04-22", sorted_dates)
    assert obs == ("2026-04-21", 18.30)


def test_value_on_or_before_returns_none_when_target_predates_all_data():
    fred = {"2026-04-22": 18.36}
    sorted_dates = sorted(fred.keys())
    assert repair_mod._value_on_or_before(fred, "2026-04-21", sorted_dates) is None


# ── end-to-end repair ──────────────────────────────────────────────────────


def test_repair_overwrites_clobbered_vix_rows(fred_api_key, monkeypatch):
    """The repair script should rewrite each affected parquet's VIX row
    with the correct per-date FRED value rather than today's latest."""
    # Two parquets, each with VIX clobbered to 17.19 (today's value at
    # the time of the bad windowed run) plus an unrelated AAPL row that
    # must be left untouched.
    bodies = {
        "staging/daily_closes/2026-04-22.parquet": _existing_parquet({
            "VIX": {
                "date": "2026-04-22",
                "Open": 17.19, "High": 17.19, "Low": 17.19, "Close": 17.19,
                "Adj_Close": 17.19, "Volume": 0, "VWAP": None, "source": "fred",
            },
            "AAPL": {
                "date": "2026-04-22",
                "Open": 230.10, "High": 232.40, "Low": 229.50, "Close": 231.20,
                "Adj_Close": 231.20, "Volume": 50_000_000, "VWAP": 231.05, "source": "polygon",
            },
        }),
        "staging/daily_closes/2026-04-28.parquet": _existing_parquet({
            "VIX": {
                "date": "2026-04-28",
                "Open": 17.19, "High": 17.19, "Low": 17.19, "Close": 17.19,
                "Adj_Close": 17.19, "Volume": 0, "VWAP": None, "source": "fred",
            },
        }),
    }
    s3 = _s3_with_parquet_bodies(bodies)

    def fake_fetch_range(series_id, start, end, api_key):
        assert series_id == "VIXCLS"
        return {"2026-04-22": 18.36, "2026-04-23": 18.10, "2026-04-28": 19.50}

    with patch("collectors.daily_closes_fred_repair.boto3.client", return_value=s3), \
         patch("collectors.daily_closes_fred_repair._fetch_fred_range", side_effect=fake_fetch_range):
        result = repair_mod.repair(
            bucket="b",
            start="2026-04-22",
            end="2026-04-28",
            tickers=["VIX"],
        )

    assert result["status"] == "ok"
    assert result["parquets_rewritten"] == 2
    assert result["rows_repaired"] == 2

    fixed_22 = pd.read_parquet(io.BytesIO(bodies["staging/daily_closes/2026-04-22.parquet"]))
    assert fixed_22.at["VIX", "Close"] == pytest.approx(18.36)
    assert fixed_22.at["VIX", "Open"] == pytest.approx(18.36)
    assert fixed_22.at["VIX", "Adj_Close"] == pytest.approx(18.36)
    # AAPL untouched (polygon source not in scope for FRED repair).
    assert fixed_22.at["AAPL", "Close"] == pytest.approx(231.20)
    assert fixed_22.at["AAPL", "source"] == "polygon"

    fixed_28 = pd.read_parquet(io.BytesIO(bodies["staging/daily_closes/2026-04-28.parquet"]))
    assert fixed_28.at["VIX", "Close"] == pytest.approx(19.50)


def test_repair_idempotent_skips_already_correct_rows(fred_api_key):
    """Re-running the repair on an already-repaired parquet is a no-op."""
    bodies = {
        "staging/daily_closes/2026-04-22.parquet": _existing_parquet({
            "VIX": {
                "date": "2026-04-22",
                "Open": 18.36, "High": 18.36, "Low": 18.36, "Close": 18.36,
                "Adj_Close": 18.36, "Volume": 0, "VWAP": None, "source": "fred",
            },
        }),
    }
    s3 = _s3_with_parquet_bodies(bodies)
    pre_body = bodies["staging/daily_closes/2026-04-22.parquet"]

    def fake_fetch_range(series_id, start, end, api_key):
        return {"2026-04-22": 18.36}

    with patch("collectors.daily_closes_fred_repair.boto3.client", return_value=s3), \
         patch("collectors.daily_closes_fred_repair._fetch_fred_range", side_effect=fake_fetch_range):
        result = repair_mod.repair(
            bucket="b",
            start="2026-04-22",
            end="2026-04-22",
            tickers=["VIX"],
        )

    assert result["per_date"]["2026-04-22"]["status"] == "noop"
    assert result["parquets_rewritten"] == 0
    # S3 body unchanged since put_object was never invoked.
    assert bodies["staging/daily_closes/2026-04-22.parquet"] == pre_body


def test_repair_dry_run_does_not_write(fred_api_key):
    bodies = {
        "staging/daily_closes/2026-04-22.parquet": _existing_parquet({
            "VIX": {
                "date": "2026-04-22",
                "Open": 17.19, "High": 17.19, "Low": 17.19, "Close": 17.19,
                "Adj_Close": 17.19, "Volume": 0, "VWAP": None, "source": "fred",
            },
        }),
    }
    s3 = _s3_with_parquet_bodies(bodies)
    pre_body = bodies["staging/daily_closes/2026-04-22.parquet"]

    with patch("collectors.daily_closes_fred_repair.boto3.client", return_value=s3), \
         patch(
             "collectors.daily_closes_fred_repair._fetch_fred_range",
             return_value={"2026-04-22": 18.36},
         ):
        result = repair_mod.repair(
            bucket="b",
            start="2026-04-22",
            end="2026-04-22",
            tickers=["VIX"],
            dry_run=True,
        )

    assert result["per_date"]["2026-04-22"]["status"] == "would_rewrite"
    assert result["parquets_rewritten"] == 0
    s3.put_object.assert_not_called()
    assert bodies["staging/daily_closes/2026-04-22.parquet"] == pre_body


def test_repair_skips_missing_parquets_in_window(fred_api_key):
    """If a parquet doesn't exist (e.g., holiday), repair logs missing and
    continues — doesn't crash."""
    bodies: dict[str, bytes] = {}
    s3 = _s3_with_parquet_bodies(bodies)

    with patch("collectors.daily_closes_fred_repair.boto3.client", return_value=s3), \
         patch(
             "collectors.daily_closes_fred_repair._fetch_fred_range",
             return_value={"2026-04-22": 18.36, "2026-04-23": 18.10},
         ):
        result = repair_mod.repair(
            bucket="b",
            start="2026-04-22",
            end="2026-04-23",
            tickers=["VIX"],
        )

    assert result["status"] == "ok"
    assert result["parquets_missing"] == 2
    assert result["parquets_rewritten"] == 0
    assert result["per_date"]["2026-04-22"]["status"] == "missing"


def test_repair_rejects_unknown_ticker(fred_api_key):
    with pytest.raises(ValueError, match="Unknown FRED tickers"):
        repair_mod.repair(
            bucket="b",
            start="2026-04-22",
            end="2026-04-22",
            tickers=["NOTAFREDTICKER"],
        )


def test_repair_requires_api_key(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="FRED_API_KEY not set"):
        repair_mod.repair(bucket="b", start="2026-04-22", end="2026-04-22")
