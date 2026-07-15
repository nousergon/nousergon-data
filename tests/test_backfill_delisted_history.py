"""Tests for builders/backfill_delisted_history.py (config#1943 Leg 3 backfill).

The module imports light (pandas only); all heavy deps (arcticdb / boto3 /
yfinance) are behind module-level seams that these tests patch, so the suite
runs with no ArcticDB / network / AWS creds. The one exception is
``test_normalize_bars_writes_through_real_arcticdb`` below, which exercises a
real local (lmdb) ArcticDB instance to catch dtype/normalization regressions
the mocked orchestrator tests structurally cannot see (config#2676) — it
skips cleanly if ``arcticdb`` isn't installed.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from builders import backfill_delisted_history as _mod


# ── pure helpers ──────────────────────────────────────────────────────────────
def test_compute_membership_windows_first_last():
    membership = {
        "2020-01-01": ["AAA", "BBB"],
        "2021-06-01": ["BBB", "CCC"],
        "2019-03-15": ["AAA"],
    }
    windows = _mod.compute_membership_windows(membership)
    assert windows["AAA"] == ("2019-03-15", "2020-01-01")
    assert windows["BBB"] == ("2020-01-01", "2021-06-01")
    assert windows["CCC"] == ("2021-06-01", "2021-06-01")


def test_compute_membership_windows_normalizes_and_skips_blank():
    windows = _mod.compute_membership_windows({"2020-01-01": [" aaa ", "", "BBB"]})
    assert set(windows) == {"AAA", "BBB"}


def test_compute_backfill_targets_excludes_universe_and_retained():
    ever = {"AAA", "BBB", "CCC", "DDD"}
    universe = {"AAA"}          # still tradable
    retained = {"BBB"}          # already retained going-forward (#696)
    assert _mod.compute_backfill_targets(ever, universe, retained) == ["CCC", "DDD"]


def test_compute_backfill_targets_case_insensitive():
    assert _mod.compute_backfill_targets({"aaa", "BBB"}, {"AAA"}, set()) == ["BBB"]


# ── bar normalization ─────────────────────────────────────────────────────────
def _yf_frame(dates, closes):
    idx = pd.DatetimeIndex(dates)
    return pd.DataFrame(
        {
            "Open": closes, "High": closes, "Low": closes,
            "Close": closes, "Volume": [1000] * len(closes),
        },
        index=idx,
    )


def test_normalize_bars_schema_and_provenance():
    df = _mod._normalize_bars(_yf_frame(["2020-01-02", "2020-01-03"], [10.0, 11.0]))
    assert list(df.columns) == [*_mod.OHLCV_COLS, _mod.PROVENANCE_COL]
    assert df["VWAP"].isna().all()                      # yfinance has no VWAP
    assert (df[_mod.PROVENANCE_COL] == "yfinance-backfill").all()
    assert df.index.tz is None                          # naive dates


def test_normalize_bars_flattens_multiindex_columns():
    base = _yf_frame(["2020-01-02"], [10.0])
    base.columns = pd.MultiIndex.from_product([base.columns, ["ABMD"]])
    df = _mod._normalize_bars(base)
    assert df["Close"].iloc[0] == 10.0


def test_normalize_bars_drops_nan_close_rows():
    frame = _yf_frame(["2020-01-02", "2020-01-03"], [10.0, float("nan")])
    df = _mod._normalize_bars(frame)
    assert len(df) == 1


def test_normalize_bars_empty():
    assert _mod._normalize_bars(pd.DataFrame()).empty


def test_normalize_bars_writes_through_real_arcticdb(tmp_path):
    """Regression test for config#2676: a mocked ``delisted_lib.write()`` always
    "succeeds", so the mocked orchestrator tests above cannot catch ArcticDB's
    normalize-time dtype rejection. Write a real ``_normalize_bars`` output frame
    to a real (local lmdb) ArcticDB library and assert it succeeds and that
    VWAP round-trips as float64, not object dtype."""
    adb = pytest.importorskip("arcticdb")

    df = _mod._normalize_bars(_yf_frame(["2020-01-02", "2020-01-03"], [10.0, 11.0]))
    assert df["VWAP"].dtype == "float64"  # would be object dtype pre-fix

    ac = adb.Arctic(f"lmdb://{tmp_path}/arctic")
    lib = ac.get_library("test_delisted_history", create_if_missing=True)
    lib.write("TESTSYM", df)  # raises arcticdb.exceptions.NormalizationException pre-fix

    read_back = lib.read("TESTSYM").data
    assert read_back["VWAP"].dtype == "float64"
    assert read_back["VWAP"].isna().all()


# ── metadata contract ─────────────────────────────────────────────────────────
def test_build_metadata_contract():
    df = _mod._normalize_bars(_yf_frame(["2020-01-02", "2020-01-03"], [10.0, 11.0]))
    md = _mod._build_metadata(
        "ABMD", df, membership_first="2019-01-01", membership_last="2020-01-01",
    )
    assert md["schema_version"] == _mod.DELISTED_HISTORY_SCHEMA_VERSION == 1
    assert md["symbol"] == "ABMD"
    assert md["origin"] == "backfill_delisted_history"
    assert md["price_source"] == "yfinance"
    assert md["source"] == "yfinance-backfill"
    assert md["rows"] == 2
    assert md["first_active_date"] == "2020-01-02"
    assert md["last_active_date"] == "2020-01-03"
    assert md["membership_first_date"] == "2019-01-01"


# ── orchestrator ──────────────────────────────────────────────────────────────
def _patch_stack(*, membership, universe, retained, yf_side_effect):
    """Patch all seams; return (universe_lib, delisted_lib, patchers-as-context)."""
    universe_lib = MagicMock()
    universe_lib.list_symbols.return_value = list(universe)
    delisted_lib = MagicMock()
    delisted_lib.list_symbols.return_value = list(retained)
    return universe_lib, delisted_lib


def _run(*, membership, universe, retained, yf_side_effect, **kw):
    universe_lib = MagicMock()
    universe_lib.list_symbols.return_value = list(universe)
    delisted_lib = MagicMock()
    delisted_lib.list_symbols.return_value = list(retained)
    with patch.object(_mod, "_s3_client", return_value=MagicMock()), \
         patch.object(_mod, "_read_historical_membership", return_value=membership), \
         patch.object(_mod, "_get_universe_lib", return_value=universe_lib), \
         patch.object(_mod, "_get_delisted_history_lib", return_value=delisted_lib), \
         patch.object(_mod, "_yf_download", side_effect=yf_side_effect), \
         patch.object(_mod, "_write_audit"):
        summary = _mod.backfill_delisted_history(**kw)
    return summary, universe_lib, delisted_lib


def test_backfill_writes_recovered_and_reports_fraction():
    membership = {
        "2018-01-01": ["ABMD", "CTXS"],
        "2020-06-01": ["CTXS"],          # ABMD dropped -> delisted target
        "2021-01-01": [],
    }
    good = _yf_frame(pd.date_range("2018-01-02", periods=40, freq="D"), [10.0] * 40)
    summary, universe_lib, delisted_lib = _run(
        membership=membership,
        universe={"CTXS"},               # CTXS still tradable
        retained=set(),
        yf_side_effect=lambda t, s, e: good,
        apply=True,
    )
    assert summary["n_targets"] == 1                 # only ABMD
    assert summary["n_recovered"] == 1
    assert summary["recovered_fraction"] == 1.0
    delisted_lib.write.assert_called_once()
    _, kwargs = delisted_lib.write.call_args
    assert kwargs["prune_previous_versions"] is True
    assert kwargs["metadata"]["symbol"] == "ABMD"
    # NEVER touch the live universe library.
    universe_lib.delete.assert_not_called()
    universe_lib.write.assert_not_called()


def test_backfill_skips_already_retained():
    membership = {"2018-01-01": ["ABMD"], "2020-06-01": []}
    summary, _u, delisted_lib = _run(
        membership=membership,
        universe=set(),
        retained={"ABMD"},              # already in delisted_history
        yf_side_effect=lambda t, s, e: _yf_frame(["2018-01-02"], [10.0]),
        apply=True,
    )
    assert summary["n_targets"] == 0
    delisted_lib.write.assert_not_called()


def test_empty_yf_counts_no_data_not_written():
    membership = {"2018-01-01": ["OLDCO"], "2020-06-01": []}
    summary, _u, delisted_lib = _run(
        membership=membership, universe=set(), retained=set(),
        yf_side_effect=lambda t, s, e: pd.DataFrame(), apply=True,
    )
    assert summary["n_no_data"] == 1 and summary["n_recovered"] == 0
    delisted_lib.write.assert_not_called()


def test_min_rows_stub_is_no_data():
    membership = {"2018-01-01": ["STUB"], "2020-06-01": []}
    tiny = _yf_frame(pd.date_range("2018-01-02", periods=3, freq="D"), [1.0, 2.0, 3.0])
    summary, _u, delisted_lib = _run(
        membership=membership, universe=set(), retained=set(),
        yf_side_effect=lambda t, s, e: tiny, apply=True, min_rows=20,
    )
    assert summary["n_no_data"] == 1
    delisted_lib.write.assert_not_called()


def test_dry_run_does_not_write():
    membership = {"2018-01-01": ["ABMD"], "2020-06-01": []}
    good = _yf_frame(pd.date_range("2018-01-02", periods=40, freq="D"), [10.0] * 40)
    summary, _u, delisted_lib = _run(
        membership=membership, universe=set(), retained=set(),
        yf_side_effect=lambda t, s, e: good, apply=False,
    )
    assert summary["n_recovered"] == 1               # counted as would-recover
    delisted_lib.write.assert_not_called()           # but nothing written


def test_per_ticker_error_isolation():
    membership = {"2018-01-01": ["BOOM", "OKOK"], "2020-06-01": []}
    good = _yf_frame(pd.date_range("2018-01-02", periods=40, freq="D"), [10.0] * 40)

    def yf(t, s, e):
        if t == "BOOM":
            raise RuntimeError("yfinance blew up")
        return good

    summary, _u, delisted_lib = _run(
        membership=membership, universe=set(), retained=set(),
        yf_side_effect=yf, apply=True,
    )
    assert summary["n_errors"] == 1 and summary["n_recovered"] == 1
    assert summary["errors"][0]["ticker"] == "BOOM"
    delisted_lib.write.assert_called_once()          # OKOK still written


def test_limit_caps_targets():
    membership = {"2018-01-01": ["AAA", "BBB", "CCC"], "2020-06-01": []}
    good = _yf_frame(pd.date_range("2018-01-02", periods=40, freq="D"), [10.0] * 40)
    summary, _u, _d = _run(
        membership=membership, universe=set(), retained=set(),
        yf_side_effect=lambda t, s, e: good, apply=True, limit=2,
    )
    assert summary["n_targets"] == 2


def test_tickers_override_filters_to_real_targets():
    membership = {"2018-01-01": ["AAA", "BBB"], "2020-06-01": ["BBB"]}
    good = _yf_frame(pd.date_range("2018-01-02", periods=40, freq="D"), [10.0] * 40)
    # BBB still tradable; override asks for AAA (real target) + BBB (skipped).
    summary, _u, _d = _run(
        membership=membership, universe={"BBB"}, retained=set(),
        yf_side_effect=lambda t, s, e: good, apply=True,
        tickers_override=["AAA", "BBB"],
    )
    assert summary["n_targets"] == 1
    assert summary["recovered"][0]["symbol"] == "AAA"


def test_read_historical_membership_rejects_missing_map():
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: json.dumps({"x": 1}).encode())}
    with pytest.raises(ValueError, match="membership"):
        _mod._read_historical_membership(s3, "bucket")


def test_read_historical_membership_parses_producer_shape():
    payload = {"schema_version": 1, "membership": {"2020-01-01": ["AAA"]}}
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: json.dumps(payload).encode())}
    got = _mod._read_historical_membership(s3, "bucket")
    assert got == {"2020-01-01": ["AAA"]}
