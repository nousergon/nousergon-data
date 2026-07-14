"""Tests for the bitemporal schema extension of ``store.arctic_store``
(market-value-integrity L0/L5, config#2459):

  1. ``to_arctic_canonical`` additively splices BITEMPORAL_COLS into the
     canonical column order (mirrors the ``total_return_close`` precedent
     — absent columns are a no-op, present columns land in a fixed slot).
  2. ``get_preliminary_lib`` opens a library structurally distinct from
     ``get_universe_lib`` (mirrors ``get_delisted_history_lib``).
  3. End-to-end against a REAL local ``lmdb://`` ArcticDB instance:
     additive round-trip (old rows w/o bitemporal cols still read fine,
     new rows carry all 6 fields), preliminary/settled physical
     separation via ``read_settled_only``, and correction-record
     versioning via ``write_correction``.

No S3/AWS credentials are used or required — every test here is local-only
(``lmdb://`` file backend) per this PR's validation discipline: this is a
live-production-store schema change and must NOT be exercised against the
real ``alpha-engine-research`` bucket from this test suite.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from nousergon_lib.arcticdb import (
    BITEMPORAL_COLS,
    KNOWLEDGE_TIME_COL,
    PRELIMINARY_LIB,
    SETTLED_COL,
    UNIVERSE_LIB,
    read_settled_only,
    write_correction,
)
from store.arctic_store import (
    OHLCV_COLS,
    PROVENANCE_COL,
    to_arctic_canonical,
)


# ── to_arctic_canonical: additive bitemporal splice ──────────────────────────


def _ohlcv_source(n=3, start="2026-07-01"):
    idx = pd.date_range(start, periods=n, freq="B", name="date")
    return pd.DataFrame(
        {
            "Open": [100.0 + i for i in range(n)],
            "High": [101.0 + i for i in range(n)],
            "Low": [99.0 + i for i in range(n)],
            "Close": [100.5 + i for i in range(n)],
            "Volume": [1_000_000] * n,
            "source": ["polygon"] * n,
        },
        index=idx,
    )


def test_to_arctic_canonical_no_bitemporal_cols_is_byte_identical_noop():
    """A live-universe-shaped frame with NONE of the 6 bitemporal columns
    must project to EXACTLY the pre-config#2459 column order — this is
    the additive/no-op guarantee every existing symbol depends on."""
    df = _ohlcv_source()
    out = to_arctic_canonical(df, features=[])
    assert list(out.columns) == ["Open", "High", "Low", "Close", "Volume", "source"]


def test_to_arctic_canonical_splices_all_six_bitemporal_cols_after_source():
    idx = pd.date_range("2026-07-01", periods=2, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "Close": [100.5, 101.5],
            "Volume": [1_000_000, 1_000_000],
            "source": ["polygon", "polygon"],
            "settled": [True, True],
            "as_of": pd.Timestamp("2026-07-01T20:00:00Z"),
            "source_tier": ["primary", "primary"],
            "valid_date": ["2026-07-01", "2026-07-02"],
            "knowledge_time": pd.Timestamp("2026-07-01T20:00:00Z"),
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=[])

    expected = [
        "Open", "Close", "Volume", "source",
        "settled", "as_of", "source_tier", "valid_date", "knowledge_time",
    ]
    assert list(out.columns) == expected


def test_to_arctic_canonical_bitemporal_cols_are_present_only_subset():
    """A producer that writes only SOME of the 6 fields (e.g. settled +
    as_of, no source_tier yet) must not be forced to backfill the rest —
    present-only splice, matching the issue's incremental-adoption need."""
    idx = pd.date_range("2026-07-01", periods=2, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Close": [100.5, 101.5],
            "source": ["polygon", "polygon"],
            "settled": [True, True],
            "as_of": pd.Timestamp("2026-07-01T20:00:00Z"),
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=[])

    assert list(out.columns) == ["Close", "source", "settled", "as_of"]


def test_to_arctic_canonical_bitemporal_cols_sit_before_features():
    idx = pd.date_range("2026-07-01", periods=2, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Close": [100.5, 101.5],
            "source": ["polygon", "polygon"],
            "settled": [True, True],
            "rsi_14": [55.0, 56.0],
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=["rsi_14"])

    assert list(out.columns) == ["Close", "source", "settled", "rsi_14"]


def test_to_arctic_canonical_coexists_with_total_return_close():
    """Bitemporal splice must not disturb the existing total_return_close
    (CRSP basis, PR7) splice — both additive migrations must compose."""
    idx = pd.date_range("2026-07-01", periods=2, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "Close": [100.5, 101.5],
            "total_return_close": [100.5, 101.5],
            "source": ["polygon", "polygon"],
            "settled": [True, True],
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=[])

    assert list(out.columns) == [
        "Open", "Close", "total_return_close", "source", "settled",
    ]


def test_bitemporal_cols_constant_matches_lib_source_of_truth():
    """store.arctic_store re-exports BITEMPORAL_COLS from
    nousergon_lib.arcticdb — pins there being exactly one source of
    truth, not a duplicated/possibly-drifting local copy."""
    from store import arctic_store

    assert arctic_store.BITEMPORAL_COLS == BITEMPORAL_COLS


# ── get_preliminary_lib: physical separation ─────────────────────────────────


def test_get_preliminary_lib_is_distinct_from_universe_and_macro(monkeypatch):
    from store import arctic_store

    assert arctic_store.PRELIMINARY_LIB not in ("universe", "macro", "delisted_history")
    assert arctic_store.PRELIMINARY_LIB == PRELIMINARY_LIB


def test_scratch_universe_lib_refuses_preliminary_lib_name(monkeypatch):
    """get_scratch_universe_lib's live-name refusal (config#804 pattern)
    must cover PRELIMINARY_LIB too — it is just as live/producer-owned as
    universe/macro, so a scratch migration must never target it either."""
    from store import arctic_store

    with pytest.raises(ValueError, match="LIVE"):
        arctic_store.get_scratch_universe_lib(PRELIMINARY_LIB)


# ── End-to-end against a REAL local lmdb ArcticDB instance ──────────────────


def _patch_arctic_singleton(monkeypatch, arctic):
    """Point store.arctic_store's connection singleton + nousergon_lib's
    open_arctic at a single shared local Arctic instance, so
    get_universe_lib / get_preliminary_lib / read_settled_only /
    write_correction all see the same on-disk libraries."""
    from store import arctic_store
    import nousergon_lib.arcticdb as lib_arctic

    monkeypatch.setattr(arctic_store, "_get_arctic", lambda bucket=None: arctic)
    monkeypatch.setattr(lib_arctic, "open_arctic", lambda bucket, region=None: arctic)


def test_additive_round_trip_old_rows_without_bitemporal_cols_still_read_fine(
    tmp_path, monkeypatch
):
    """A symbol written BEFORE this migration (no bitemporal columns at
    all) must round-trip through get_universe_lib / read_settled_only
    unchanged after the schema extension lands — the additive contract."""
    import arcticdb as adb
    from store import arctic_store

    arctic = adb.Arctic(f"lmdb://{tmp_path}")
    _patch_arctic_singleton(monkeypatch, arctic)

    old_row = to_arctic_canonical(_ohlcv_source(), features=[])
    universe_lib = arctic_store.get_universe_lib()
    universe_lib.write("AAPL", old_row)

    out = universe_lib.read("AAPL").data
    pd.testing.assert_frame_equal(out, old_row, check_freq=False)

    settled_out = read_settled_only("any-bucket", "AAPL")
    pd.testing.assert_frame_equal(settled_out, old_row, check_freq=False)


def test_additive_round_trip_new_rows_carry_all_six_bitemporal_fields(
    tmp_path, monkeypatch
):
    import arcticdb as adb
    from store import arctic_store

    arctic = adb.Arctic(f"lmdb://{tmp_path}")
    _patch_arctic_singleton(monkeypatch, arctic)

    idx = pd.date_range("2026-07-01", periods=2, freq="B", name="date")
    new_row = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "High": [101.0, 102.0],
            "Low": [99.0, 100.0],
            "Close": [100.5, 101.5],
            "Volume": [1_000_000, 1_000_000],
            "source": ["polygon", "polygon"],
            "settled": [True, True],
            "as_of": [pd.Timestamp("2026-07-01T20:00:00Z")] * 2,
            "source_tier": ["primary", "primary"],
            "valid_date": ["2026-07-01", "2026-07-02"],
            "knowledge_time": [pd.Timestamp("2026-07-01T20:00:00Z")] * 2,
        },
        index=idx,
    )
    canonical = to_arctic_canonical(new_row, features=[])
    for col in BITEMPORAL_COLS:
        assert col in canonical.columns

    universe_lib = arctic_store.get_universe_lib()
    universe_lib.write("MSFT", canonical)

    out = universe_lib.read("MSFT").data
    for col in BITEMPORAL_COLS:
        assert col in out.columns
    assert out[SETTLED_COL].all()


def test_preliminary_settled_physical_separation_via_read_settled_only(
    tmp_path, monkeypatch
):
    """A symbol written ONLY to the preliminary library must be
    unreachable through read_settled_only — the read-path chokepoint
    structurally cannot pull preliminary data into a settled read, even
    though both libraries live on the SAME underlying Arctic instance /
    bucket."""
    import arcticdb as adb
    from store import arctic_store

    arctic = adb.Arctic(f"lmdb://{tmp_path}")
    _patch_arctic_singleton(monkeypatch, arctic)

    prelim_row = to_arctic_canonical(_ohlcv_source(), features=[])
    prelim_row[SETTLED_COL] = False
    prelim_lib = arctic_store.get_preliminary_lib()
    prelim_lib.write("TSLA", prelim_row)

    assert prelim_lib.has_symbol("TSLA")
    with pytest.raises(Exception):
        read_settled_only("any-bucket", "TSLA")


def test_correction_record_versioning_v1_still_queryable_v2_is_head(
    tmp_path, monkeypatch
):
    """write v1 (published settled value), 'correct' it via
    write_correction, confirm v1 is still queryable as-of its own
    version/timestamp and v2 (the correction) is the new head — the
    bitemporal correction-audit-trail contract (scope item 3)."""
    import arcticdb as adb
    from store import arctic_store

    arctic = adb.Arctic(f"lmdb://{tmp_path}")
    _patch_arctic_singleton(monkeypatch, arctic)

    universe_lib = arctic_store.get_universe_lib()

    v1_row = to_arctic_canonical(_ohlcv_source(), features=[])
    v1_row[SETTLED_COL] = True
    vi0 = universe_lib.write("SPY", v1_row, prune_previous_versions=False)

    corrected = v1_row.copy()
    corrected.iloc[1, corrected.columns.get_loc("Close")] = 987.65
    vi1 = write_correction(
        universe_lib, "SPY", corrected,
        reason="exchange restated 2026-07-02 close", source="polygon",
    )

    assert vi1.version == vi0.version + 1

    still_v1 = universe_lib.read("SPY", as_of=vi0.version).data
    pd.testing.assert_frame_equal(still_v1, v1_row, check_freq=False)

    head = read_settled_only("any-bucket", "SPY")
    pd.testing.assert_frame_equal(head, corrected, check_freq=False)

    meta = universe_lib.read_metadata("SPY", as_of=vi1.version).metadata
    assert meta["reason"] == "exchange restated 2026-07-02 close"
    assert meta["source"] == "polygon"
    assert meta["correction"] is True
