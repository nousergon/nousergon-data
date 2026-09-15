"""Shadow ArcticDB libraries start from live state, read-only against live.

`alpha-engine-config-I10866`. The first real shadow run (2026-09-15, trading
day 2026-09-14) died at ``--morning-arctic-append``. ``ShadowRoot.arctic_library``
redirects reads as well as writes, so ``shadow_20260914_universe_schema_meta``
opened EMPTY and was read as schema v0 against an expected v2. Every test here
runs against a real LMDB-backed ArcticDB, because library options, the static
schema, ``date_range`` bounding and batch ``DataError`` results are native
ArcticDB behaviour that a mock would only restate.

The three properties the issue requires:

1. A shadow open of ``universe_schema_meta`` reads the LIVE stamp's version
   (``test_shadow_open_of_schema_meta_reads_the_live_stamp``).
2. A shadow ``daily_append`` on a seeded library computes the same feature
   values as the same append on the live fixture
   (``test_shadow_daily_append_on_a_seeded_library_matches_the_live_append``).
3. No write reaches a live library name
   (``test_no_write_reaches_a_live_library``).
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import arcticdb as adb

import store.arctic_store as arctic_store
from migrations import EXPECTED_SCHEMA_VERSION, assert_universe_schema_current
from shadow import arctic_seed
from shadow.arctic_seed import (
    MANIFEST_LIBRARY,
    SCHEMA_META_LIBRARY,
    SEEDED_DATA_LIBRARIES,
    ShadowSeedError,
    ensure_seeded,
    read_manifest,
)
from shadow.root import (
    LIVE_ARCTIC_LIBRARIES,
    ShadowGuardViolation,
    ShadowRoot,
    activate,
    deactivate,
)
from store.schema_version import read_schema_version, write_schema_version
from tests.conftest import recent_trading_day_str

TRADING_DAY = dt.date(2026, 9, 14)
ROOT = ShadowRoot(TRADING_DAY)


def _ohlcv(dates) -> pd.DataFrame:
    idx = pd.DatetimeIndex(dates, name="date")
    close = 100.0 + np.cumsum(np.linspace(-0.5, 0.7, len(idx)))
    return pd.DataFrame(
        {
            "Open": close - 0.2,
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
            "Volume": np.full(len(idx), 1_000_000.0),
            "VWAP": close + 0.1,
        },
        index=idx,
    )


@pytest.fixture
def arctic(tmp_path, monkeypatch):
    """A 'production' ArcticDB: live libraries populated THROUGH the trading
    day and past it, because live v1 has already appended those days by the
    time a shadow run for a past trading day starts."""
    ac = adb.Arctic(f"lmdb://{tmp_path / 'prod'}")
    universe = ac.get_library("universe", create_if_missing=True)
    # 10 business days before the trading day, the trading day, and 2 after.
    dates = pd.bdate_range(end=pd.Timestamp(TRADING_DAY) + pd.Timedelta(days=2), periods=13)
    universe.write("AAA", _ohlcv(dates), metadata={"ticker": "AAA"})
    universe.write("BBB", _ohlcv(dates[3:]))
    # Listed ON the trading day, so as of just before it, it did not exist.
    universe.write("NEW", _ohlcv(dates[dates >= pd.Timestamp(TRADING_DAY)]))
    macro = ac.get_library("macro", create_if_missing=True)
    macro.write("VIX", _ohlcv(dates)[["Close"]])
    delisted = ac.get_library("delisted_history", create_if_missing=True)
    delisted.write("OLD", _ohlcv(dates[:5]), metadata={"schema_version": 1})
    meta = ac.get_library(SCHEMA_META_LIBRARY, create_if_missing=True)
    write_schema_version(meta, EXPECTED_SCHEMA_VERSION, migration_number=EXPECTED_SCHEMA_VERSION, columns_after=["Close"])
    monkeypatch.setattr(arctic_store, "_arctic_instance", ac)
    yield ac
    deactivate()


def _live_fingerprint(ac) -> dict:
    """Every live library's symbols, versions and data — equal before and after
    means nothing was written, appended, pruned or deleted there."""
    out = {}
    for name in sorted(LIVE_ARCTIC_LIBRARIES):
        if not ac.has_library(name):
            out[name] = None
            continue
        lib = ac.get_library(name)
        out[name] = {
            sym: (sorted(v.version for v in lib.list_versions(sym)), lib.read(sym).data.to_json())
            for sym in sorted(lib.list_symbols())
        }
    return out


# ── the crash itself ────────────────────────────────────────────────────────


def test_shadow_open_of_schema_meta_reads_the_live_stamp(arctic):
    activate(ROOT)
    meta = arctic_store.get_schema_meta_lib()
    assert read_schema_version(meta) == EXPECTED_SCHEMA_VERSION
    # The exact call that raised SchemaVersionMismatch on 2026-09-15.
    assert assert_universe_schema_current(meta) == EXPECTED_SCHEMA_VERSION
    assert arctic.has_library(f"shadow_20260914_{SCHEMA_META_LIBRARY}")


def test_an_unstamped_live_plane_is_never_stamped_in_the_shadow(tmp_path, monkeypatch):
    """Never fake the stamp: if live has none, the shadow has none either,
    and both read as baseline through the same code path."""
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    for name in SEEDED_DATA_LIBRARIES:
        ac.get_library(name, create_if_missing=True).write(
            "X", _ohlcv(pd.bdate_range(end="2026-09-10", periods=3))
        )
    manifest = ensure_seeded(ac, ROOT)
    assert manifest["schema_version"] is None
    shadow_meta = ac.get_library(f"shadow_20260914_{SCHEMA_META_LIBRARY}")
    assert read_schema_version(shadow_meta) is None
    assert not ac.has_library(SCHEMA_META_LIBRARY), "seeding must not create a LIVE library"


# ── the seed's shape ────────────────────────────────────────────────────────


def test_seed_copies_live_state_bounded_to_before_the_trading_day(arctic):
    manifest = ensure_seeded(arctic, ROOT)
    shadow = arctic.get_library("shadow_20260914_universe")
    live = arctic.get_library("universe")
    assert sorted(shadow.list_symbols()) == ["AAA", "BBB"]
    for sym in ("AAA", "BBB"):
        got = shadow.read(sym)
        expected = live.read(sym).data
        expected = expected[expected.index < pd.Timestamp(TRADING_DAY)]
        pd.testing.assert_frame_equal(got.data, expected)
        assert got.data.index.max() < pd.Timestamp(TRADING_DAY)
    assert shadow.read("AAA").metadata == {"ticker": "AAA"}
    assert shadow.options() == live.options()
    assert manifest["libraries"]["universe"]["absent_before_cutoff"] == ["NEW"]
    assert manifest["libraries"]["universe"]["symbols"] == 2
    assert manifest["libraries"]["delisted_history"]["symbols"] == 1
    assert arctic.get_library("shadow_20260914_delisted_history").read("OLD").metadata == {"schema_version": 1}


def test_every_live_library_has_a_seeding_rule():
    assert set(SEEDED_DATA_LIBRARIES) | {SCHEMA_META_LIBRARY} == set(LIVE_ARCTIC_LIBRARIES)


def test_seed_is_idempotent_per_stamp(arctic):
    first = ensure_seeded(arctic, ROOT)
    version_before = arctic.get_library("shadow_20260914_universe").list_versions("AAA")
    second = ensure_seeded(arctic, ROOT)
    assert second == first
    assert arctic.get_library("shadow_20260914_universe").list_versions("AAA") == version_before


def test_unseeded_leftovers_under_the_stamp_are_discarded(arctic):
    """The 2026-09-15 run left EMPTY shadow libraries behind (created with
    create_if_missing). Without a manifest nothing under the stamp counts."""
    junk = arctic.get_library("shadow_20260914_universe", create_if_missing=True)
    junk.write("JUNK", _ohlcv(pd.bdate_range(end="2026-09-01", periods=2)))
    arctic.get_library(f"shadow_20260914_{SCHEMA_META_LIBRARY}", create_if_missing=True)
    ensure_seeded(arctic, ROOT)
    assert "JUNK" not in arctic.get_library("shadow_20260914_universe").list_symbols()


def test_a_crash_before_the_manifest_leaves_the_stamp_unseeded_and_reseedable(arctic, monkeypatch):
    real = arctic_seed._copy_schema_stamp

    def _crash(*a, **k):
        raise RuntimeError("box reclaimed mid-seed")

    monkeypatch.setattr(arctic_seed, "_copy_schema_stamp", _crash)
    with pytest.raises(RuntimeError, match="mid-seed"):
        ensure_seeded(arctic, ROOT)
    assert read_manifest(arctic, ROOT) is None
    monkeypatch.setattr(arctic_seed, "_copy_schema_stamp", real)
    assert ensure_seeded(arctic, ROOT)["schema_version"] == EXPECTED_SCHEMA_VERSION


def test_every_shadow_open_is_seeded_first(arctic):
    activate(ROOT)
    arctic_store.get_macro_lib()
    assert read_manifest(arctic, ROOT) is not None
    assert "VIX" in arctic_store.get_macro_lib().list_symbols()


# ── fail loud ───────────────────────────────────────────────────────────────


def test_a_missing_live_library_refuses_rather_than_seeding_empty(tmp_path):
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    ac.get_library("universe", create_if_missing=True)
    with pytest.raises(ShadowSeedError, match="'macro' does not exist"):
        ensure_seeded(ac, ROOT)
    assert read_manifest(ac, ROOT) is None


def test_a_symbol_that_cannot_be_bounded_to_the_trading_day_raises(arctic):
    arctic.get_library("universe").write("NOIDX", pd.DataFrame({"Close": [1.0, 2.0]}))
    with pytest.raises(ShadowSeedError, match="NOIDX"):
        ensure_seeded(arctic, ROOT)
    assert read_manifest(arctic, ROOT) is None


def test_an_exhausted_budget_raises_instead_of_committing_a_partial_seed(arctic):
    ticks = iter(range(0, 10_000, 100))
    with pytest.raises(ShadowSeedError, match="time budget"):
        ensure_seeded(
            arctic, ROOT, clock=lambda: float(next(ticks)),
            env={arctic_seed.ENV_SEED_BUDGET_SECONDS: "150"}, chunk_size=1,
        )
    assert read_manifest(arctic, ROOT) is None


def test_an_unparseable_budget_raises():
    with pytest.raises(ShadowSeedError):
        arctic_seed._budget_seconds({arctic_seed.ENV_SEED_BUDGET_SECONDS: "soon"})


# ── no write reaches a live library ─────────────────────────────────────────


def test_no_write_reaches_a_live_library(arctic):
    before = _live_fingerprint(arctic)
    activate(ROOT)
    universe = arctic_store.get_universe_lib()
    universe.write("AAA", _ohlcv(pd.bdate_range(end="2026-09-14", periods=4)), prune_previous_versions=True)
    universe.write("ZZZ", _ohlcv(pd.bdate_range(end="2026-09-14", periods=4)))
    arctic_store.get_macro_lib().write("VIX", pd.DataFrame({"Close": [0.0]}, index=pd.DatetimeIndex(["2026-09-14"])))
    arctic_store.get_delisted_history_lib().delete("OLD")
    write_schema_version(arctic_store.get_schema_meta_lib(), 99, migration_number=99, columns_after=[])
    deactivate()
    assert _live_fingerprint(arctic) == before
    assert universe.name == "shadow_20260914_universe"


def test_seeding_holds_only_read_only_handles_to_live(arctic, monkeypatch):
    """Structural, not careful: every live handle seeding obtains refuses
    anything but a read, so a future edit that writes through it raises."""
    handles = []
    real = arctic_seed._live_handle

    def _spy(*a, **k):
        h = real(*a, **k)
        handles.append(h)
        return h

    monkeypatch.setattr(arctic_seed, "_live_handle", _spy)
    ensure_seeded(arctic, ROOT)
    assert {h._name for h in handles if h is not None} == set(LIVE_ARCTIC_LIBRARIES)
    for h in handles:
        for method in ("write", "write_batch", "update", "append", "delete", "update_batch"):
            with pytest.raises(ShadowGuardViolation):
                getattr(h, method)


def test_the_live_name_collision_guard_still_holds():
    for live in LIVE_ARCTIC_LIBRARIES:
        assert ROOT.arctic_library(live).startswith("shadow_20260914_")
    assert ROOT.arctic_library(MANIFEST_LIBRARY) not in LIVE_ARCTIC_LIBRARIES
    with pytest.raises(ShadowGuardViolation):
        ensure_seeded(MagicMock(), None)


# ── a shadow daily_append computes what the live append computes ────────────


def _fake_compute_features(combined, **_):
    """History-dependent on purpose: feature 0 is the number of bars seen, and
    the rest are rolling means over growing windows. A one-bar (unseeded)
    history gives visibly different values."""
    from features.feature_engineer import FEATURES

    out = combined.copy()
    close = combined["Close"].astype("float64")
    for i, f in enumerate(FEATURES):
        if i == 0:
            out[f] = np.arange(1, len(combined) + 1, dtype="float64")
        else:
            out[f] = close.rolling(2 + i % 30, min_periods=1).mean()
    return out


def _universe_history(dates) -> pd.DataFrame:
    from features.feature_engineer import FEATURES

    frame = _ohlcv(dates)
    frame = _fake_compute_features(frame)
    frame["source"] = "polygon"
    return arctic_store.to_arctic_canonical(frame, features=FEATURES)


def _patch_daily_append(monkeypatch, today_ts, symbols):
    from builders import daily_append as _da

    macro_keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]
    sector_etfs = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"]
    closes = {
        t: {"Open": 150.0, "High": 152.0, "Low": 149.0, "Close": 151.0, "Volume": 2_000_000, "VWAP": 150.5}
        for t in set(symbols) | set(macro_keys) | set(sector_etfs)
    }
    macro_lib = MagicMock()
    macro_df = _ohlcv(pd.bdate_range(end=today_ts, periods=300))[["Close"]]
    macro_lib.read.return_value = MagicMock(data=macro_df)
    macro_lib.list_symbols.return_value = sector_etfs
    monkeypatch.setenv("FACTOR_MOMENTUM_DAILY_ENABLED", "false")
    monkeypatch.setenv("FACTOR_LOADING_ZSCORE_DAILY_ENABLED", "false")
    monkeypatch.setattr(_da, "_load_daily_closes", lambda *a, **k: closes)
    monkeypatch.setattr(_da, "_load_sector_map", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_fundamentals", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_alternative", lambda *a, **k: {})
    monkeypatch.setattr(_da, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_da, "_emit_missing_from_closes_metric", MagicMock())
    monkeypatch.setattr(_da, "_emit_quality_gate_metrics", MagicMock())
    monkeypatch.setattr(_da, "compute_features", _fake_compute_features)
    monkeypatch.setattr("builders.daily_append.boto3.client", lambda *a, **k: MagicMock())
    return _da


def test_shadow_daily_append_on_a_seeded_library_matches_the_live_append(tmp_path, monkeypatch):
    from features.compute import UNIVERSE_BENCHMARK_PROXIES
    from features.feature_engineer import FEATURES

    today_str = recent_trading_day_str()
    today_ts = pd.Timestamp(today_str)
    root = ShadowRoot(today_ts.date())
    # XLRE: daily_append requires its bar in closes AND reads it as a universe
    # ticker, so the fixture universe must carry it like production does.
    symbols = sorted({"AAA", "XLRE", *UNIVERSE_BENCHMARK_PROXIES})
    history_dates = pd.bdate_range(end=today_ts - pd.Timedelta(days=1), periods=280)

    # (1) The LIVE fixture: history up to the day before, appended normally.
    live_ac = adb.Arctic(f"lmdb://{tmp_path / 'live'}")
    live_universe = live_ac.get_library("universe", create_if_missing=True)
    for sym in symbols:
        live_universe.write(sym, _universe_history(history_dates))
    _da = _patch_daily_append(monkeypatch, today_ts, symbols)
    monkeypatch.setattr(_da, "get_universe_lib", lambda *a, **k: live_universe)
    live_result = _da.daily_append(date_str=today_str)
    assert live_result["status"] == "ok", live_result
    live_row = live_universe.read("AAA").data.loc[[today_ts]]

    # (2) PRODUCTION as a shadow run meets it: live has ALREADY appended the
    # trading day (with different bars), and the schema plane is stamped.
    prod_ac = adb.Arctic(f"lmdb://{tmp_path / 'prod'}")
    prod_universe = prod_ac.get_library("universe", create_if_missing=True)
    for sym in symbols:
        history = _universe_history(history_dates)
        appended = history.iloc[[-1]].copy()
        appended.index = pd.DatetimeIndex([today_ts], name=history.index.name)
        appended["Close"] = 999.0
        # Identical bars before the trading day, so any difference in the
        # shadow result can only come from the seed.
        prod_universe.write(sym, pd.concat([history, appended]))
    prod_ac.get_library("macro", create_if_missing=True).write("VIX", _ohlcv(history_dates)[["Close"]])
    prod_ac.get_library("delisted_history", create_if_missing=True)
    write_schema_version(
        prod_ac.get_library(SCHEMA_META_LIBRARY, create_if_missing=True),
        EXPECTED_SCHEMA_VERSION, migration_number=EXPECTED_SCHEMA_VERSION, columns_after=list(FEATURES),
    )
    before = _live_fingerprint(prod_ac)

    monkeypatch.setattr(arctic_store, "_arctic_instance", prod_ac)
    # The REAL open seams, so the redirect, the seed and the schema assert run.
    monkeypatch.setattr(_da, "get_universe_lib", arctic_store.get_universe_lib)
    monkeypatch.setattr(_da, "get_schema_meta_lib", arctic_store.get_schema_meta_lib)
    activate(root)
    try:
        shadow_result = _da.daily_append(date_str=today_str)
    finally:
        deactivate()
    assert shadow_result["status"] == "ok", shadow_result
    assert shadow_result["tickers_appended"] == live_result["tickers_appended"]

    shadow_universe = prod_ac.get_library(root.arctic_library("universe"))
    shadow_row = shadow_universe.read("AAA").data.loc[[today_ts]]
    pd.testing.assert_frame_equal(shadow_row, live_row)
    # The history really was there: the bar count includes all 280 seeded rows.
    assert shadow_row[FEATURES[0]].iloc[0] == 281.0
    # And the live libraries were not touched, including the already-appended day.
    assert _live_fingerprint(prod_ac) == before
    assert prod_universe.read("AAA").data.loc[today_ts, "Close"] == 999.0
