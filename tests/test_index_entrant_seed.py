"""Index entrants get an ArcticDB universe symbol the evening they join
(alpha-engine-config-I11444).

BE joined the S&P 500 effective 2026-09-21. Its 10y price-cache parquet landed
that evening (`_run_daily`), but its ArcticDB symbol was first written
2026-09-23 01:17Z by a weekly backfill, so Crucible v2's `data.daily` failed
the 2026-09-21 and 2026-09-22 windows with `MissingSourceError: ['BE']`. The
EOD append now seeds such members before it appends.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

import weekly_collector
from weekly_collector import _seed_index_entrants as _real_seed  # before conftest's stub

BUCKET = "test-bucket"


def _lib(symbols):
    lib = MagicMock()
    lib.list_symbols.return_value = list(symbols)
    return lib


def _seed(expected, present, backfill_side_effect=None, dry_run=False):
    calls = []

    def _fake_backfill(**kwargs):
        calls.append(kwargs)
        if backfill_side_effect is not None:
            return backfill_side_effect(kwargs["ticker_filter"])
        return {"status": "ok"}

    with patch("store.arctic_store.get_universe_lib", return_value=_lib(present)), \
         patch("builders.backfill.backfill", side_effect=_fake_backfill):
        out = _real_seed(BUCKET, expected, dry_run=dry_run)
    return out, calls


def test_member_without_a_symbol_is_seeded_through_the_per_ticker_backfill():
    out, calls = _seed(
        ["AAPL", "BE", "SPY", "XLK", "^VIX"], present=["AAPL", "SPY", "XLK"],
    )
    assert out["status"] == "ok"
    assert out["entrants"] == ["BE"]
    assert out["seeded"] == ["BE"]
    assert calls == [{"bucket": BUCKET, "ticker_filter": "BE", "dry_run": False}]


def test_steady_state_has_no_entrants_and_never_backfills():
    out, calls = _seed(["AAPL", "MSFT", "SPY"], present=["AAPL", "MSFT", "SPY", "TTD"])
    assert out == {"status": "ok", "entrants": [], "seeded": [], "errors": []}
    assert calls == []


def test_macro_series_and_unpromoted_sector_etfs_are_never_entrants():
    # ^VIX / TNX are macro series and XLI is a sector ETF that is not a declared
    # benchmark proxy: none of them is ever written to `universe`.
    out, calls = _seed(["AAPL", "^VIX", "TNX", "XLI"], present=["AAPL"])
    assert out["entrants"] == []
    assert calls == []


def test_macro_daily_tickers_riding_in_the_expected_scope_are_never_entrants():
    # XLRE passes admits_universe_write but is never a universe symbol; it is in
    # every append's expected scope only via _MACRO_DAILY_TICKERS. Measured
    # 2026-09-23 it was the one name an unfiltered entrant set produced.
    assert "XLRE" in weekly_collector._MACRO_DAILY_TICKERS
    expected = weekly_collector._augment_with_macro_daily_tickers(["AAPL"])
    out, calls = _seed(expected, present=["AAPL", "SPY", "IWM", "XLK", "XLV", "XLF", "XLE"])
    assert out["entrants"] == []
    assert calls == []


def test_one_entrant_failing_does_not_stop_the_others_and_is_reported():
    def _outcome(ticker):
        if ticker == "BE":
            return {"status": "error", "error": "ticker_no_data: BE"}
        return {"status": "ok"}

    out, calls = _seed(["AAPL", "BE", "ILMN"], present=["AAPL"], backfill_side_effect=_outcome)
    assert [c["ticker_filter"] for c in calls] == ["BE", "ILMN"]
    assert out["status"] == "error"
    assert out["seeded"] == ["ILMN"]
    assert out["errors"] == [{"ticker": "BE", "reason": "ticker_no_data: BE"}]


def test_a_raising_backfill_is_recorded_per_ticker():
    def _raise(ticker):
        raise RuntimeError("s3 down")

    out, _ = _seed(["AAPL", "BE"], present=["AAPL"], backfill_side_effect=_raise)
    assert out["status"] == "error"
    assert out["errors"] == [{"ticker": "BE", "reason": "s3 down"}]


def test_a_mass_absence_is_refused_not_seeded():
    expected = [f"T{i:03d}" for i in range(weekly_collector._ENTRANT_SEED_MAX + 1)]
    out, calls = _seed(expected, present=[])
    assert out["status"] == "error"
    assert "outage or a wrong library" in out["error"]
    assert calls == []


def test_dry_run_names_entrants_without_writing():
    out, calls = _seed(["AAPL", "BE"], present=["AAPL"], dry_run=True)
    assert out["status"] == "ok_dry_run"
    assert out["entrants"] == ["BE"]
    assert calls == []


# ── wiring into the EOD append ──────────────────────────────────────────────


def _eod(seed, append=None):
    config = {"bucket": BUCKET, "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-09-21", dry_run=False)
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL", "BE"]}
    order = []

    def _seed(bucket, tickers, dry_run=False):
        order.append(("seed", sorted(tickers)))
        return seed(bucket, tickers, dry_run)

    def _append(**kwargs):
        order.append(("append", kwargs["date_str"]))
        return append(**kwargs) if append else {"status": "ok"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector._seed_index_entrants", side_effect=_seed), \
         patch("builders.daily_append.daily_append", side_effect=_append), \
         patch("weekly_collector._mark_pending_upgrade"):
        out = weekly_collector._run_daily_arctic_append(config, args)
    return out, order


def test_eod_append_seeds_entrants_before_appending_against_the_same_membership():
    out, order = _eod(lambda b, t, d: {"status": "ok", "entrants": ["BE"], "seeded": ["BE"], "errors": []})
    assert [step for step, _ in order] == ["seed", "append"]
    seeded_scope = order[0][1]
    assert "BE" in seeded_scope and "SPY" in seeded_scope, (
        "the seed must see the same expected_tickers the append (and data.daily) use"
    )
    assert out["status"] == "ok"
    assert out["collectors"]["entrant_seed"]["seeded"] == ["BE"]
    # The load-bearing verdict stays first, so a failed append's reason names it.
    assert list(out["collectors"]) == ["arcticdb", "entrant_seed"]


def test_a_failed_seed_never_fails_the_load_bearing_append():
    out, order = _eod(lambda b, t, d: {"status": "error", "entrants": ["BE"], "seeded": [],
                                       "errors": [{"ticker": "BE", "reason": "x"}]})
    assert [step for step, _ in order] == ["seed", "append"]
    assert out["status"] == "ok"
    assert out["collectors"]["entrant_seed"]["status"] == "error"


def test_a_raising_seed_is_contained():
    def _boom(b, t, d):
        raise RuntimeError("arctic unreachable")

    out, order = _eod(_boom)
    assert [step for step, _ in order] == ["seed", "append"]
    assert out["status"] == "ok"
    assert out["collectors"]["entrant_seed"] == {
        "status": "error", "error": "RuntimeError: arctic unreachable",
    }


def test_a_seed_timeout_is_contained():
    def _hang(b, t, d):
        raise weekly_collector._HardTimeout("index-entrant seed exceeded 600s")

    out, _ = _eod(_hang)
    assert out["status"] == "ok"
    assert out["collectors"]["entrant_seed"]["status"] == "error"
    assert "hard timeout" in out["collectors"]["entrant_seed"]["error"]


# ── the per-ticker backfill loads only what one symbol's write needs ────────


def test_load_full_cache_only_downloads_the_named_stems():
    from builders import backfill as _bf

    keys = [
        "reference/price_cache/AAPL.parquet",
        "reference/price_cache/BE.parquet",
        "reference/price_cache/MSFT.parquet",
        "reference/price_cache/SPY.parquet",
    ]
    frame = pd.DataFrame({"Close": [1.0]}, index=pd.DatetimeIndex(["2026-09-18"]))
    read = []
    with patch.object(_bf, "list_price_cache_keys", return_value=keys), \
         patch.object(_bf, "_load_parquet_from_s3",
                      side_effect=lambda s3, b, k: read.append(k) or frame):
        out = _bf._load_full_cache(MagicMock(), BUCKET, only=frozenset({"BE", "SPY"}))
    assert sorted(out) == ["BE", "SPY"]
    assert sorted(read) == ["reference/price_cache/BE.parquet", "reference/price_cache/SPY.parquet"]


def test_per_ticker_backfill_requests_the_ticker_plus_its_macro_context_only():
    from builders import backfill as _bf

    seen = {}

    def _load(s3, bucket, prefix=None, only=None):
        seen["only"] = only
        return {}  # -> no_price_data: stops before any write

    with patch.object(_bf, "_load_full_cache", side_effect=_load), \
         patch.object(_bf, "boto3", MagicMock()):
        out = _bf.backfill(bucket=BUCKET, ticker_filter="BE")
    assert out["status"] == "error"
    only = seen["only"]
    assert "BE" in only
    for needed in ("SPY", "VIX", "TNX", "IRX", "HYOAS", "XLK", "XLU", "SMH"):
        assert needed in only
    assert "AAPL" not in only


def test_full_backfill_still_loads_the_whole_cache():
    from builders import backfill as _bf

    seen = {}

    def _load(s3, bucket, prefix=None, only=None):
        seen["only"] = only
        return {}

    with patch.object(_bf, "_load_full_cache", side_effect=_load), \
         patch.object(_bf, "boto3", MagicMock()):
        _bf.backfill(bucket=BUCKET)
    assert seen["only"] is None
