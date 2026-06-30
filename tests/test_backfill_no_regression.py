"""Regression tests for builders/backfill.py freshness preflight.

Locks the 2026-05-02 incident invariant: a full-universe backfill must
NOT silently regress ArcticDB macro/universe last_date when its source
data (predictor/price_cache + daily_closes delta) ends earlier than what
ArcticDB already has. Two layers gate this:

1. ``_apply_daily_delta`` runs against the loaded price cache so backfill
   sees the same delta-merged data that the feature snapshot does. Without
   this, the price cache mtime "current" check can leave the cache 1+ day
   behind MorningEnrich's polygon-T+1 fill.
2. ``_assert_no_arctic_regression`` reads SPY + a 20-symbol universe sample
   from ArcticDB and refuses to write if planned data is older. Cheap
   defense-in-depth: catches the regression class even when delta isn't
   enough (e.g. delta file missing, source cache regressed for some
   reason).

Both layers exist because the underlying full-series ``lib.write()`` calls
in backfill clobber every existing row — there is no path to recover from
a silent regression once the writes land.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _series(start: str, n: int = 5) -> pd.Series:
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.Series([100.0 + i for i in range(n)], index=idx)


def _ohlcv(start: str, n: int = 5) -> pd.DataFrame:
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.DataFrame(
        {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0, "Volume": 1_000_000},
        index=idx,
    )


def _arctic_lib_with(symbols_to_last_date: dict[str, str]):
    """Stub an ArcticDB library whose ``tail(sym, n=1)`` returns a single-row
    frame ending on the given date. ``list_symbols`` returns the keys."""
    lib = MagicMock()

    def _tail(sym, n=1):
        if sym not in symbols_to_last_date:
            raise RuntimeError(f"symbol {sym} not found")
        df = pd.DataFrame(
            {"Close": [100.0]},
            index=pd.DatetimeIndex([pd.Timestamp(symbols_to_last_date[sym])]),
        )
        result = MagicMock()
        result.data = df
        return result

    lib.tail.side_effect = _tail
    lib.list_symbols.return_value = list(symbols_to_last_date.keys())
    return lib


def test_assert_no_regression_passes_when_planned_matches_existing():
    """Equal last_date is fine: backfill rewrite preserves freshness."""
    from builders import backfill as _bf

    planned_macro = {"SPY": _series("2026-04-25"), "VIX": _series("2026-04-25")}
    planned_universe = {"AAPL": _ohlcv("2026-04-25")}

    macro_lib = _arctic_lib_with({"SPY": "2026-04-25", "VIX": "2026-04-25"})
    universe_lib = _arctic_lib_with({"AAPL": "2026-04-25"})

    with patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib):
        _bf._assert_no_arctic_regression(
            "test-bucket", planned_macro, planned_universe, run_date="2026-04-26"
        )  # must not raise


def test_assert_no_regression_passes_when_planned_is_newer():
    """Newer planned data is the happy path — strictly forward-progressing."""
    from builders import backfill as _bf

    planned_macro = {"SPY": _series("2026-04-29")}
    planned_universe = {"AAPL": _ohlcv("2026-04-29")}

    macro_lib = _arctic_lib_with({"SPY": "2026-04-25"})
    universe_lib = _arctic_lib_with({"AAPL": "2026-04-25"})

    with patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib):
        _bf._assert_no_arctic_regression(
            "test-bucket", planned_macro, planned_universe, run_date="2026-04-30"
        )


def test_assert_no_regression_raises_on_macro_regression():
    """The 2026-05-02 incident exact path: planned macro ends 4/30,
    ArcticDB has 5/1 from MorningEnrich. Must hard-fail BEFORE any
    feature compute or write runs.
    """
    from builders import backfill as _bf

    planned_macro = {"SPY": _series("2026-04-26", n=4)}  # ends 4/29 BDay
    planned_universe = {"AAPL": _ohlcv("2026-04-26", n=4)}

    macro_lib = _arctic_lib_with({"SPY": "2026-05-01"})
    universe_lib = _arctic_lib_with({"AAPL": "2026-05-01"})

    with patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib):
        with pytest.raises(RuntimeError, match="regression preflight failed"):
            _bf._assert_no_arctic_regression(
                "test-bucket", planned_macro, planned_universe, run_date="2026-05-02"
            )


def test_assert_no_regression_raises_on_universe_regression():
    """Universe regression alone (macro fresh) is also a hard-fail.

    Catches the case where macro got refreshed by some path but the
    universe writes would still clobber daily_append's appends.
    """
    from builders import backfill as _bf

    planned_macro = {"SPY": _series("2026-05-01")}  # fresh
    planned_universe = {"AAPL": _ohlcv("2026-04-26", n=4)}  # stale

    macro_lib = _arctic_lib_with({"SPY": "2026-05-01"})
    universe_lib = _arctic_lib_with({"AAPL": "2026-05-01"})

    with patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib):
        with pytest.raises(RuntimeError, match="regression preflight failed"):
            _bf._assert_no_arctic_regression(
                "test-bucket", planned_macro, planned_universe, run_date="2026-05-02"
            )


def test_assert_no_regression_skips_symbols_absent_from_arctic():
    """First-write case: a planned symbol with no existing ArcticDB row
    must not be treated as a regression (existing_last is None → skip).
    """
    from builders import backfill as _bf

    planned_macro = {"SPY": _series("2026-04-25")}
    planned_universe = {"NEWCO": _ohlcv("2026-04-25")}

    macro_lib = _arctic_lib_with({"SPY": "2026-04-25"})
    # NEWCO not in ArcticDB universe yet — list_symbols returns empty
    universe_lib = _arctic_lib_with({})

    with patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib):
        _bf._assert_no_arctic_regression(
            "test-bucket", planned_macro, planned_universe, run_date="2026-04-26"
        )  # must not raise


def test_full_backfill_calls_apply_daily_delta():
    """The 2026-05-02 fix: full-universe backfill must apply daily_closes
    delta on top of the 10y price cache so the source picks up
    MorningEnrich's polygon-T+1 fill. Skipping this leaves the source 1
    day behind ArcticDB whenever the price cache passes the mtime
    'current' check.
    """
    from builders import backfill as _bf

    price_data = {"AAPL": _ohlcv("2024-01-01", n=400)}
    macro: dict = {}
    universe_lib = MagicMock()
    macro_lib = MagicMock()

    delta_mock = MagicMock(side_effect=lambda s3, b, d, pd_, **_kw: (pd_, set()))

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_apply_daily_delta", delta_mock), \
         patch.object(_bf, "_assert_no_arctic_regression"), \
         patch.object(_bf, "_load_current_constituents", return_value=set(price_data.keys())), \
         patch.object(_bf, "_extract_macro_series", return_value=macro), \
         patch.object(_bf, "_load_sector_map", return_value={"AAPL": "XLK"}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "_build_macro_features_df", return_value=pd.DataFrame()), \
         patch.object(_bf, "compute_features", side_effect=lambda df, **_: df), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib), \
         patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "_scan_universe_and_emit_freshness_receipt",
                      return_value={"n_symbols_checked": 1, "stalest_symbol": "AAPL",
                                    "stalest_age_trading_days": 1, "all_fresh": True}), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        _bf.backfill(ticker_filter=None)

    delta_mock.assert_called_once()


def test_full_backfill_calls_regression_preflight():
    """The 2026-05-02 fix: full-universe backfill must run the regression
    preflight before any compute or write so a stale source aborts loudly
    instead of silently clobbering ArcticDB.
    """
    from builders import backfill as _bf

    price_data = {"AAPL": _ohlcv("2024-01-01", n=400)}
    universe_lib = MagicMock()
    macro_lib = MagicMock()
    regression_mock = MagicMock()

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_apply_daily_delta", side_effect=lambda s3, b, d, pd_, **_kw: (pd_, set())), \
         patch.object(_bf, "_assert_no_arctic_regression", regression_mock), \
         patch.object(_bf, "_load_current_constituents", return_value=set(price_data.keys())), \
         patch.object(_bf, "_extract_macro_series", return_value={}), \
         patch.object(_bf, "_load_sector_map", return_value={"AAPL": "XLK"}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "_build_macro_features_df", return_value=pd.DataFrame()), \
         patch.object(_bf, "compute_features", side_effect=lambda df, **_: df), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib), \
         patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "_scan_universe_and_emit_freshness_receipt",
                      return_value={"n_symbols_checked": 1, "stalest_symbol": "AAPL",
                                    "stalest_age_trading_days": 1, "all_fresh": True}), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        _bf.backfill(ticker_filter=None)

    regression_mock.assert_called_once()


def test_ticker_filter_skips_regression_preflight():
    """Per-ticker backfill (--ticker X) skips the regression preflight —
    it doesn't touch the macro library by default and only writes a single
    universe symbol, so the cross-symbol freshness check is moot. Avoids
    a spurious failure from a stale-cache --ticker patch when the rest of
    ArcticDB has moved forward.
    """
    from builders import backfill as _bf

    price_data = {"AAPL": _ohlcv("2024-01-01", n=400)}
    universe_lib = MagicMock()
    macro_lib = MagicMock()
    regression_mock = MagicMock()

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_apply_daily_delta", side_effect=lambda s3, b, d, pd_, **_kw: (pd_, set())), \
         patch.object(_bf, "_assert_no_arctic_regression", regression_mock), \
         patch.object(_bf, "_load_current_constituents", return_value=set(price_data.keys())), \
         patch.object(_bf, "_extract_macro_series", return_value={}), \
         patch.object(_bf, "_load_sector_map", return_value={"AAPL": "XLK"}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "_build_macro_features_df", return_value=pd.DataFrame()), \
         patch.object(_bf, "compute_features", side_effect=lambda df, **_: df), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib), \
         patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "_scan_universe_and_emit_freshness_receipt",
                      return_value={"n_symbols_checked": 1, "stalest_symbol": "AAPL",
                                    "stalest_age_trading_days": 1, "all_fresh": True}), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        _bf.backfill(ticker_filter="AAPL")

    regression_mock.assert_not_called()


def test_dry_run_skips_delta_and_preflight():
    """``--dry-run`` is a CI / local validation path: must not require S3
    daily_closes parquets to exist or ArcticDB to be reachable. Both the
    delta load and the preflight read are gated behind ``not dry_run``.
    """
    from builders import backfill as _bf

    price_data = {"AAPL": _ohlcv("2024-01-01", n=400)}
    delta_mock = MagicMock()
    regression_mock = MagicMock()

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_apply_daily_delta", delta_mock), \
         patch.object(_bf, "_assert_no_arctic_regression", regression_mock), \
         patch.object(_bf, "_load_current_constituents", return_value=set(price_data.keys())), \
         patch.object(_bf, "_extract_macro_series", return_value={}), \
         patch.object(_bf, "_load_sector_map", return_value={"AAPL": "XLK"}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "compute_features", side_effect=lambda df, **_: df), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        _bf.backfill(dry_run=True)

    delta_mock.assert_not_called()
    regression_mock.assert_not_called()


# ── constituents filter (PR closing the prune+backfill loop) ─────────────────


def test_backfill_skips_tickers_absent_from_constituents():
    """The 2026-05-02 SF redrive #6 root cause: pre-MorningEnrich prune
    drops 8 stragglers, then Phase 1 step 8 (this function) recreates
    them because their parquet files still exist in price_cache. With
    the constituents filter, backfill writes only tickers in the current
    investable universe. Stragglers stay pruned."""
    from builders import backfill as _bf

    price_data = {
        "AAPL": _ohlcv("2024-01-01", n=400),
        "MSFT": _ohlcv("2024-01-01", n=400),
        "STRAGGLER": _ohlcv("2024-01-01", n=400),  # parquet exists, not in constituents
    }
    universe_lib = MagicMock()
    macro_lib = MagicMock()

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_apply_daily_delta", side_effect=lambda s3, b, d, pd_, **_kw: (pd_, set())), \
         patch.object(_bf, "_assert_no_arctic_regression"), \
         patch.object(_bf, "_load_current_constituents",
                      return_value={"AAPL", "MSFT"}), \
         patch.object(_bf, "_extract_macro_series", return_value={}), \
         patch.object(_bf, "_load_sector_map", return_value={"AAPL": "XLK", "MSFT": "XLK"}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "_build_macro_features_df", return_value=pd.DataFrame()), \
         patch.object(_bf, "compute_features", side_effect=lambda df, **_: df), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib), \
         patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch.object(_bf, "_scan_universe_and_emit_freshness_receipt",
                      return_value={"n_symbols_checked": 1, "stalest_symbol": "AAPL",
                                    "stalest_age_trading_days": 1, "all_fresh": True}), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        result = _bf.backfill(ticker_filter=None)

    # universe_lib.write should be called for AAPL + MSFT only — NOT STRAGGLER.
    written_tickers = {
        call.args[0] for call in universe_lib.write.call_args_list
    }
    assert "AAPL" in written_tickers
    assert "MSFT" in written_tickers
    assert "STRAGGLER" not in written_tickers, (
        "STRAGGLER absent from constituents must NOT be written to arctic — "
        "would undo the prune and re-trip Backtester preflight"
    )
    assert result["status"] == "ok"


def test_backfill_hard_fails_when_constituents_load_fails():
    """Failing to load constituents must NOT silently proceed (which would
    write everything in price_cache and undo all prune work). Hard-fail
    per feedback_no_silent_fails."""
    from builders import backfill as _bf

    price_data = {"AAPL": _ohlcv("2024-01-01", n=400)}

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_apply_daily_delta", side_effect=lambda s3, b, d, pd_, **_kw: (pd_, set())), \
         patch.object(_bf, "_assert_no_arctic_regression"), \
         patch.object(_bf, "_load_current_constituents",
                      side_effect=RuntimeError("S3 503")), \
         patch.object(_bf, "_extract_macro_series", return_value={}), \
         patch.object(_bf, "_load_sector_map", return_value={}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        with pytest.raises(RuntimeError, match="could not load current constituents"):
            _bf.backfill(ticker_filter=None)


def test_backfill_dry_run_does_not_filter_by_constituents():
    """Dry-run must work even without S3 constituents access (CI / local
    smoke). Falls back to writing everything in price_cache (since dry-run
    doesn't actually write)."""
    from builders import backfill as _bf

    price_data = {"AAPL": _ohlcv("2024-01-01", n=400)}
    constituents_calls = MagicMock()

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_load_current_constituents", constituents_calls), \
         patch.object(_bf, "_extract_macro_series", return_value={}), \
         patch.object(_bf, "_load_sector_map", return_value={"AAPL": "XLK"}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "compute_features", side_effect=lambda df, **_: df), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        _bf.backfill(dry_run=True)

    # In dry_run, _load_current_constituents must NOT be called — we use
    # the price_data keys directly so dry-run works without S3.
    constituents_calls.assert_not_called()


def test_load_current_constituents_run_date_bypasses_pointer():
    """When run_date is provided, read constituents.json by the explicit
    weekly path rather than following ``latest_weekly.json``.

    The 2026-05-23 Saturday SF failure had Wikipedia adding 3 new entrants
    (BNY/P/SN) to that morning's constituents.json; Phase 1's backfill
    followed the still-stale pointer (last week's date), excluded those 3
    from the ArcticDB write, and Research's preflight then tripped the 5%
    per-ticker error threshold reading them from a library that didn't have
    them. Run-date threading bypasses the pointer for the in-Phase-1 read.
    """
    import json as _json
    from builders import backfill as _bf

    new_payload = _json.dumps(
        {"tickers": ["AAPL", "MSFT", "BNY", "P", "SN"]}
    ).encode()
    stale_payload = _json.dumps({"tickers": ["AAPL", "MSFT"]}).encode()

    def _mock_get(Bucket, Key):
        # The pointer must NOT be consulted on the run_date path; if the
        # test setup ever returns the stale pointer for the explicit
        # weekly key, the assertion below fails — making the regression
        # impossible to ship silently.
        if Key == "market_data/weekly/2026-05-23/constituents.json":
            body = MagicMock()
            body.read.return_value = new_payload
            return {"Body": body}
        if Key == "market_data/latest_weekly.json":
            raise AssertionError(
                "run_date path must not consult latest_weekly.json — "
                "that pointer hasn't been advanced yet when backfill runs."
            )
        if Key.endswith("constituents.json"):
            body = MagicMock()
            body.read.return_value = stale_payload
            return {"Body": body}
        raise AssertionError(f"unexpected S3 key: {Key}")

    s3 = MagicMock()
    s3.get_object.side_effect = _mock_get
    result = _bf._load_current_constituents(s3, "alpha-engine-research", run_date="2026-05-23")
    assert "BNY" in result and "P" in result and "SN" in result
    assert len(result) == 5


def test_load_current_constituents_falls_back_to_pointer_when_no_run_date():
    """Ad-hoc callers (CLI, per-ticker recovery) leave run_date=None and
    must still resolve via the pointer — the legacy behavior."""
    import json as _json
    from builders import backfill as _bf

    pointer = _json.dumps(
        {"date": "2026-05-23", "s3_prefix": "market_data/weekly/2026-05-23/"}
    ).encode()
    cons = _json.dumps({"tickers": ["AAPL", "MSFT"]}).encode()

    def _mock_get(Bucket, Key):
        body = MagicMock()
        if Key == "market_data/latest_weekly.json":
            body.read.return_value = pointer
        else:
            body.read.return_value = cons
        return {"Body": body}

    s3 = MagicMock()
    s3.get_object.side_effect = _mock_get
    result = _bf._load_current_constituents(s3, "alpha-engine-research")
    assert result == {"AAPL", "MSFT"}
