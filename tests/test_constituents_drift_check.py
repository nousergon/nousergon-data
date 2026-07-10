"""Tests for the Friday-Preflight Wikipedia-vs-ArcticDB constituents
drift check (5/23-SF P0 (g))."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_check_drift_no_drift_returns_ok():
    """When Wikipedia ⊆ ArcticDB universe (modulo skip-list), status=ok."""
    from validators.constituents_drift_check import check_drift

    # Wikipedia returns a small known set; ArcticDB has the same + extras.
    fake_fetch = MagicMock(return_value=(
        ["AAPL", "MSFT", "NVDA"],  # tickers
        {}, {}, {},                   # sector_map, sector_etf_map, sub_industry_map
        2, 1,                        # sp500_count, sp400_count
    ))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL", "MSFT", "NVDA", "GOOG"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == []


def test_check_drift_missing_tickers_detected():
    """The canonical 5/23 scenario: Wikipedia lists BNY/P/SN, ArcticDB
    doesn't — drift_detected + missing list populated."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(
        ["AAPL", "MSFT", "BNY", "P", "SN"],
        {}, {}, {},
        3, 2,
    ))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL", "MSFT"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "drift_detected"
    assert set(result["missing_from_arctic"]) == {"BNY", "P", "SN"}
    assert result["within_threshold"] is False


def test_check_drift_under_threshold_passes():
    """max_stragglers tolerance: 1 missing with cap=2 → status=ok."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "BNY"], {}, {}, {}, 1, 1))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False, max_stragglers=2)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == ["BNY"]
    assert result["within_threshold"] is True


def test_check_drift_skip_list_excluded_from_diff():
    """SPY (in _SKIP_TICKERS) doesn't fire drift even if Wikipedia
    lists it but ArcticDB lacks the SKIP_TICKERS entry — _SKIP_TICKERS
    is stripped from BOTH sides of the comparison."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "SPY", "VIX"], {}, {}, {}, 1, 0))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == []


def test_check_drift_sector_etf_excluded():
    """XLK / XLF / XL* prefixes excluded from drift comparison."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "XLK", "XLF"], {}, {}, {}, 1, 0))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"


def test_check_drift_wikipedia_fetch_failure_returns_error():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(side_effect=Exception("Wikipedia 503"))
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch):
        result = check_drift(alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "wikipedia_fetch"


def test_check_drift_arctic_failure_returns_error():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL"], {}, {}, {}, 1, 0))
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               side_effect=Exception("ArcticDB unreachable")):
        result = check_drift(alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "arctic_list"


def test_main_exit_code_ok():
    from validators.constituents_drift_check import main

    fake_fetch = MagicMock(return_value=(["AAPL"], {}, {}, {}, 1, 0))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        rc = main(["--no-alert"])
    assert rc == 0


def test_main_exit_code_drift_detected():
    from validators.constituents_drift_check import main

    fake_fetch = MagicMock(return_value=(["AAPL", "BNY"], {}, {}, {}, 1, 1))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        rc = main(["--no-alert"])
    assert rc == 1
