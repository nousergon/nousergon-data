"""alpha-engine-config-I11471 / I10068 — every FRED-sourced macro series must
reach the ArcticDB ``macro`` library through the weekly backfill.

Measured 2026-09-24: ``reference/price_cache/TWO.parquet`` (2503 rows) and
``BAA10Y.parquet`` (2498 rows) were healthy and refreshed every Saturday by
``collectors/fred_history.py``, but ``macro/TWO`` and ``macro/BAA10Y`` did not
exist in ArcticDB. ``_extract_macro_series`` and the raw-series write loop in
``backfill()`` each carried their own literal symbol list, and neither named
them. crucible-predictor's Stage-1b macro block therefore read both as absent
(rows=0) and graded ``finite_pct=0.00`` in rehearsal-2026-09-23-2.

These tests fail when a ``FRED_HISTORY_MAP`` key is added without reaching the
write loop — the class fix I10068 deliverable 4 asks for.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

import builders.backfill as _bf
from collectors.fred_history import FRED_HISTORY_MAP


def _close_frame(n: int) -> pd.DataFrame:
    idx = pd.bdate_range("2016-09-16", periods=n)
    return pd.DataFrame({"Close": [2.0 + i * 1e-3 for i in range(n)]}, index=idx)


def test_every_fred_history_key_is_a_declared_raw_macro_series():
    missing = set(FRED_HISTORY_MAP) - set(_bf._RAW_MACRO_SERIES)
    assert not missing, (
        f"FRED_HISTORY_MAP keys {sorted(missing)} are fetched into the price "
        "cache but never written to ArcticDB macro — add them to "
        "builders/backfill.py::_RAW_MACRO_SERIES."
    )


def test_extract_macro_series_carries_two_and_baa10y():
    price_data = {sym: _close_frame(2500) for sym in FRED_HISTORY_MAP}
    price_data["SPY"] = _close_frame(2500)

    macro = _bf._extract_macro_series(price_data)

    for sym in FRED_HISTORY_MAP:
        assert sym in macro, f"{sym} missing from _extract_macro_series output"
        assert len(macro[sym]) == 2500


def test_full_backfill_writes_every_fred_series_to_macro_lib():
    """Drive ``backfill()`` end to end with the S3/ArcticDB layer mocked and
    record which symbols reach the raw-series write boundary."""
    price_data = {
        "AAPL": pd.DataFrame(
            {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0,
             "Adj_Close": 100.0, "Volume": 1_000_000},
            index=pd.bdate_range("2024-01-01", periods=400),
        ),
        "SPY": _close_frame(400),
        **{sym: _close_frame(400) for sym in FRED_HISTORY_MAP},
    }

    written: list[str] = []

    def _record_write(lib, symbol, df, reference_date=None):
        written.append(symbol)

    def _fake_compute_features(df, **_):
        out = df.copy()
        for col in ("atr_14_pct", "rsi_14", "momentum_60d"):
            out[col] = 0.5
        return out

    fake_macro_df = pd.DataFrame(
        {"vix_level": [15.0, 16.0]}, index=pd.date_range("2024-01-01", periods=2),
    )

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_apply_daily_delta",
                      side_effect=lambda s3, b, d, pd_, **_kw: (pd_, set())), \
         patch.object(_bf, "_assert_no_arctic_regression"), \
         patch.object(_bf, "_load_current_constituents", return_value={"AAPL"}), \
         patch.object(_bf, "_load_sector_map", return_value={"AAPL": "XLK"}), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "_build_macro_features_df", return_value=fake_macro_df), \
         patch.object(_bf, "compute_features", side_effect=_fake_compute_features), \
         patch.object(_bf, "get_universe_lib", return_value=MagicMock()), \
         patch.object(_bf, "get_macro_lib", return_value=MagicMock()), \
         patch.object(_bf, "_write_macro_series_no_shrink", side_effect=_record_write), \
         patch.object(_bf, "_scan_universe_and_emit_freshness_receipt",
                      return_value={"n_symbols_checked": 1, "stalest_symbol": "AAPL",
                                    "stalest_age_trading_days": 1, "all_fresh": True}), \
         patch("builders.backfill.boto3.client", return_value=MagicMock()):
        result = _bf.backfill(ticker_filter=None, rebuild_macro=False)

    assert result["status"] == "ok"
    for sym in FRED_HISTORY_MAP:
        assert sym in written, (
            f"macro/{sym} was never written by the weekly backfill; "
            f"raw-series writes were {sorted(written)}"
        )
