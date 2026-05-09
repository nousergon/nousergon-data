"""Regression tests for `weekly_collector._self_heal_chronic_polygon_gaps`
and the chronic-polygon-gaps config loader.

Locks the 2026-05-09 weekly-SF DataPhase1 postflight failure: PSTG ended
at 5/5 in ArcticDB while SPY was at 5/8 (3d stale, > 2d threshold).
Polygon doesn't reliably serve PSTG (one of 4 known chronic gaps —
BF-B/BRK-B/MOG-A/PSTG), so MorningEnrich's polygon_only daily_append
left it stuck at whatever the prior EOD yfinance pass landed; on days
when EOD also dropped it the gap compounded. The self-heal step
yfinance-backfills any [last_date+1, target_date] gap for each chronic
ticker so postflight passes uniformly without ad-hoc allowlists.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ── Config loader ────────────────────────────────────────────────────────────


def test_load_chronic_polygon_gaps_returns_sorted_keys():
    from weekly_collector import _load_chronic_polygon_gaps

    config = {
        "chronic_polygon_gaps": {
            "tickers": {
                "PSTG": {"reason": "polygon coverage flaky"},
                "BF-B": {"reason": "class B share dot-vs-dash"},
                "MOG-A": {"reason": "class A share dot-vs-dash"},
                "BRK-B": {"reason": "class B share dot-vs-dash"},
            }
        }
    }
    assert _load_chronic_polygon_gaps(config) == ["BF-B", "BRK-B", "MOG-A", "PSTG"]


def test_load_chronic_polygon_gaps_empty_when_section_missing():
    """Missing config section means no self-heal — pre-PR strict
    polygon_only behavior preserved as the safe default."""
    from weekly_collector import _load_chronic_polygon_gaps

    assert _load_chronic_polygon_gaps({}) == []
    assert _load_chronic_polygon_gaps({"chronic_polygon_gaps": {}}) == []
    assert _load_chronic_polygon_gaps(
        {"chronic_polygon_gaps": {"tickers": None}}
    ) == []


def test_load_chronic_polygon_gaps_empty_when_section_malformed():
    """A list (instead of dict) under `tickers` is malformed config — be
    permissive (return empty) rather than crash MorningEnrich at boot."""
    from weekly_collector import _load_chronic_polygon_gaps

    assert _load_chronic_polygon_gaps(
        {"chronic_polygon_gaps": {"tickers": ["BF-B", "PSTG"]}}
    ) == []


# ── Self-heal helper ─────────────────────────────────────────────────────────


def _stub_universe_lib(ticker_to_last_date: dict[str, str | None]):
    """Build an ArcticDB stub whose ``tail(sym, n=1)`` returns a single-row
    frame ending on the given date (None → empty frame, simulating a
    ticker absent from the library)."""
    lib = MagicMock()

    def _tail(sym, n=1):
        last = ticker_to_last_date.get(sym)
        if last is None:
            empty = MagicMock()
            empty.data = pd.DataFrame()
            return empty
        df = pd.DataFrame(
            {"Close": [100.0]},
            index=pd.DatetimeIndex([pd.Timestamp(last)]),
        )
        result = MagicMock()
        result.data = df
        return result

    lib.tail.side_effect = _tail
    return lib


def _stub_s3_with_pcache(parquet_dfs_by_ticker: dict[str, pd.DataFrame]):
    """S3 stub that serves predictor/price_cache/{T}.parquet from a dict
    of in-memory DataFrames; raises NoSuchKey for absent tickers."""
    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey

    written: dict[str, bytes] = {}

    def _get_object(Bucket, Key):
        for ticker, df in parquet_dfs_by_ticker.items():
            if Key == f"predictor/price_cache/{ticker}.parquet":
                buf = io.BytesIO()
                df.to_parquet(buf, engine="pyarrow")
                buf.seek(0)
                return {"Body": MagicMock(read=lambda buf=buf: buf.read())}
        raise _NoSuchKey(f"key={Key} not stubbed")

    def _put_object(Bucket, Key, Body, **_kwargs):
        written[Key] = Body
        return {}

    s3.get_object.side_effect = _get_object
    s3.put_object.side_effect = _put_object
    s3.written = written  # accessible to assertions
    return s3


def _yf_df(start: str, n_business_days: int) -> pd.DataFrame:
    """Build a yfinance-shaped OHLCV frame (columns: Open/High/Low/Close/Volume)."""
    idx = pd.bdate_range(start=start, periods=n_business_days)
    return pd.DataFrame(
        {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0, "Volume": 1_000_000},
        index=idx,
    )


def test_self_heal_skips_already_fresh_tickers():
    """Idempotence: if ArcticDB last_date >= target_date the ticker is
    skipped — no yfinance fetch, no parquet write, no backfill call."""
    from weekly_collector import _self_heal_chronic_polygon_gaps

    universe_lib = _stub_universe_lib({"BF-B": "2026-05-08"})
    s3 = _stub_s3_with_pcache({})

    yf_download = MagicMock()
    backfill_mock = MagicMock()

    with patch("weekly_collector.boto3.client", return_value=s3), \
         patch("store.arctic_store.get_universe_lib", return_value=universe_lib), \
         patch("yfinance.download", yf_download), \
         patch("builders.backfill.backfill", backfill_mock):
        result = _self_heal_chronic_polygon_gaps(
            bucket="test-bucket",
            target_date="2026-05-08",
            chronic_tickers=["BF-B"],
        )

    assert result["checked"] == 1
    assert len(result["healed"]) == 0
    assert len(result["skipped_already_fresh"]) == 1
    assert result["skipped_already_fresh"][0] == {"ticker": "BF-B", "last_date": "2026-05-08"}
    yf_download.assert_not_called()
    backfill_mock.assert_not_called()
    assert s3.written == {}


def test_self_heal_backfills_pstg_via_yfinance_and_invokes_per_ticker_backfill():
    """The 2026-05-09 incident exact path: PSTG at 5/5 in ArcticDB,
    target=5/8, yfinance has 5/6 + 5/7 + 5/8. Self-heal must:
      - yfinance-fetch [5/6, 5/8]
      - patch predictor/price_cache/PSTG.parquet with new rows (deduped)
      - invoke builders.backfill(ticker_filter='PSTG') so the ArcticDB
        write goes through the same per-ticker compute_features path.
    """
    from weekly_collector import _self_heal_chronic_polygon_gaps

    universe_lib = _stub_universe_lib({"PSTG": "2026-05-05"})
    existing_pcache = pd.DataFrame(
        {"Open": 70.0, "High": 71.0, "Low": 69.0, "Close": 70.0, "Volume": 1_000},
        index=pd.bdate_range(end="2026-05-05", periods=10),
    )
    s3 = _stub_s3_with_pcache({"PSTG": existing_pcache})

    fresh_yf = pd.DataFrame(
        {"Open": [73.0, 74.0, 78.0], "High": [75.0, 78.0, 79.0],
         "Low":  [73.0, 74.0, 75.0], "Close": [74.5, 76.0, 78.2],
         "Volume": [1_500, 2_000, 3_000]},
        index=pd.DatetimeIndex(["2026-05-06", "2026-05-07", "2026-05-08"]),
    )

    backfill_mock = MagicMock()

    with patch("weekly_collector.boto3.client", return_value=s3), \
         patch("store.arctic_store.get_universe_lib", return_value=universe_lib), \
         patch("yfinance.download", return_value=fresh_yf), \
         patch("builders.backfill.backfill", backfill_mock):
        result = _self_heal_chronic_polygon_gaps(
            bucket="test-bucket",
            target_date="2026-05-08",
            chronic_tickers=["PSTG"],
        )

    assert result["checked"] == 1
    assert len(result["healed"]) == 1
    healed = result["healed"][0]
    assert healed["ticker"] == "PSTG"
    assert healed["previous_last_date"] == "2026-05-05"
    assert healed["rows_added"] == 3
    assert healed["new_last_date"] == "2026-05-08"
    assert len(result["errors"]) == 0

    # Patched parquet got written with the fresh rows merged in
    pcache_key = "predictor/price_cache/PSTG.parquet"
    assert pcache_key in s3.written
    written_df = pd.read_parquet(io.BytesIO(s3.written[pcache_key]))
    assert pd.Timestamp("2026-05-08") in written_df.index
    assert pd.Timestamp("2026-05-06") in written_df.index

    backfill_mock.assert_called_once_with(
        bucket="test-bucket", ticker_filter="PSTG", dry_run=False
    )


def test_self_heal_dry_run_skips_writes_and_backfill_invocation():
    """``dry_run=True`` exercises the read paths + computes the healed
    summary, but writes nothing to S3 and invokes no per-ticker backfill."""
    from weekly_collector import _self_heal_chronic_polygon_gaps

    universe_lib = _stub_universe_lib({"PSTG": "2026-05-05"})
    s3 = _stub_s3_with_pcache({
        "PSTG": pd.DataFrame(
            {"Open": 70.0, "Close": 70.0, "High": 71.0, "Low": 69.0, "Volume": 1_000},
            index=pd.bdate_range(end="2026-05-05", periods=5),
        )
    })

    backfill_mock = MagicMock()
    fresh_yf = _yf_df("2026-05-06", n_business_days=3)

    with patch("weekly_collector.boto3.client", return_value=s3), \
         patch("store.arctic_store.get_universe_lib", return_value=universe_lib), \
         patch("yfinance.download", return_value=fresh_yf), \
         patch("builders.backfill.backfill", backfill_mock):
        result = _self_heal_chronic_polygon_gaps(
            bucket="test-bucket",
            target_date="2026-05-08",
            chronic_tickers=["PSTG"],
            dry_run=True,
        )

    assert len(result["healed"]) == 1
    assert s3.written == {}  # no writes in dry-run
    backfill_mock.assert_not_called()


def test_self_heal_per_ticker_failure_is_isolated():
    """A yfinance hiccup on one chronic ticker logs an error for that
    ticker but does not block the others — postflight is the load-bearing
    gate, this step is best-effort."""
    from weekly_collector import _self_heal_chronic_polygon_gaps

    universe_lib = _stub_universe_lib({"PSTG": "2026-05-05", "BF-B": "2026-05-06"})
    existing_pcache = pd.DataFrame(
        {"Open": 70.0, "High": 71.0, "Low": 69.0, "Close": 70.0, "Volume": 1_000},
        index=pd.bdate_range(end="2026-05-06", periods=10),
    )
    s3 = _stub_s3_with_pcache({"PSTG": existing_pcache, "BF-B": existing_pcache})

    def _yf_side_effect(ticker, **_kwargs):
        if ticker == "PSTG":
            raise RuntimeError("yfinance: rate-limited (HTTP 429)")
        return pd.DataFrame(
            {"Open": [101.0, 102.0], "High": [102.0, 103.0],
             "Low":  [100.0, 101.0], "Close": [101.5, 102.5],
             "Volume": [1_000, 1_000]},
            index=pd.DatetimeIndex(["2026-05-07", "2026-05-08"]),
        )

    backfill_mock = MagicMock()

    with patch("weekly_collector.boto3.client", return_value=s3), \
         patch("store.arctic_store.get_universe_lib", return_value=universe_lib), \
         patch("yfinance.download", side_effect=_yf_side_effect), \
         patch("builders.backfill.backfill", backfill_mock):
        result = _self_heal_chronic_polygon_gaps(
            bucket="test-bucket",
            target_date="2026-05-08",
            chronic_tickers=["PSTG", "BF-B"],
        )

    assert result["checked"] == 2
    assert len(result["healed"]) == 1
    assert result["healed"][0]["ticker"] == "BF-B"
    assert len(result["errors"]) == 1
    assert result["errors"][0]["ticker"] == "PSTG"
    # BF-B's backfill was still invoked despite PSTG's failure
    backfill_mock.assert_called_once_with(
        bucket="test-bucket", ticker_filter="BF-B", dry_run=False
    )


def test_self_heal_empty_chronic_tickers_is_noop():
    """No chronic-gap config → empty list → no work done. Preserves the
    pre-PR behavior when the config section is absent."""
    from weekly_collector import _self_heal_chronic_polygon_gaps

    s3 = MagicMock()
    universe_lib = MagicMock()

    with patch("weekly_collector.boto3.client", return_value=s3), \
         patch("store.arctic_store.get_universe_lib", return_value=universe_lib):
        result = _self_heal_chronic_polygon_gaps(
            bucket="test-bucket",
            target_date="2026-05-08",
            chronic_tickers=[],
        )

    assert result == {
        "checked": 0,
        "healed": [],
        "skipped_already_fresh": [],
        "errors": [],
    }
    universe_lib.tail.assert_not_called()
    s3.get_object.assert_not_called()
