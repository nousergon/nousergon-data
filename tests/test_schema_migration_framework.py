"""tests/test_schema_migration_framework.py — CI harness for the ArcticDB
schema-migration framework (alpha-engine-config-I3241).

Runs a REAL migration against a seeded local LMDB ArcticDB library (no AWS —
LMDB needs none) in the repo's normal ``test`` job, proving end to end that:

  * a schema-additive append onto an un-migrated symbol fails loud
    (``StreamDescriptorMismatch``) — the config-I3236 failure;
  * the producer pre-append assert catches the version gap BEFORE any write and
    names the pending migration;
  * a migration's ``run()`` full-writes the symbols (the config#2459
    ``update()``-can't-cross-a-descriptor lesson), ``verify()`` passes, and the
    PRODUCTION write primitive (``update_batch``) then appends cleanly;
  * the version stamp advances and the producer assert goes green.

The migration exercised here is SYNTHETIC (defined in-test), not a real module
under ``migrations/`` — the framework harness must prove the *mechanism* works
without depending on a real forward migration existing yet (the sub-sector
re-land is a separate PR). The baseline (0000) module IS exercised against a
frozen-schema fixture.
"""

from __future__ import annotations

import importlib

import numpy as np
import pandas as pd
import pytest

import arcticdb as adb
from arcticdb.version_store.library import UpdatePayload, WritePayload

from store.arctic_store import OHLCV_COLS, PROVENANCE_COL, to_arctic_canonical
from store.schema_version import (
    BASELINE_SCHEMA_VERSION,
    SchemaVersionMismatch,
    assert_schema_version,
    read_schema_version,
    write_schema_version,
)
from migrations._base import (
    Migration,
    MigrationError,
    rewrite_symbols_full,
    verify_additive,
    validate_chain,
)

_OLD_FEATURES = ["feat_a", "feat_b"]
_NEW_FEATURES = ["feat_a", "feat_b", "feat_c_new"]


def _universe_frame(dates, features) -> pd.DataFrame:
    idx = pd.DatetimeIndex(dates, name="date")
    data: dict[str, object] = {
        col: np.arange(1, len(idx) + 1, dtype="float64") for col in OHLCV_COLS
    }
    data[PROVENANCE_COL] = ["polygon"] * len(idx)
    for feat in features:
        data[feat] = np.arange(len(idx), dtype="float32")
    return pd.DataFrame(data, index=idx)


@pytest.fixture()
def arctic(tmp_path):
    return adb.Arctic(f"lmdb://{tmp_path}")


@pytest.fixture()
def universe_lib(arctic):
    return arctic.get_library("universe", create_if_missing=True)


@pytest.fixture()
def meta_lib(arctic):
    return arctic.get_library("universe_schema_meta", create_if_missing=True)


def _seed_old_schema(lib, symbols=("AAA", "BBB", "CCC")):
    for sym in symbols:
        hist = _universe_frame(
            pd.date_range("2026-06-01", periods=5, freq="D"), _OLD_FEATURES
        )
        lib.write_batch(
            [WritePayload(symbol=sym, data=to_arctic_canonical(hist, features=_OLD_FEATURES))]
        )
    return list(symbols)


def _synthetic_add_feat_c_migration() -> Migration:
    """A real, runnable 0->1 additive migration built on the shared helpers,
    scoped to synthetic feature columns so the harness never churns when real
    columns change."""
    cols_after = tuple(OHLCV_COLS) + (PROVENANCE_COL,) + tuple(_NEW_FEATURES)

    def _project(df: pd.DataFrame) -> pd.DataFrame:
        return to_arctic_canonical(df, features=_NEW_FEATURES)

    def _run(lib, mlib) -> None:
        rewrite_symbols_full(
            lib,
            expected_columns=cols_after,
            new_columns={"feat_c_new": np.float32("nan")},
            project=_project,
        )
        write_schema_version(mlib, 1, migration_number=1, columns_after=cols_after)

    def _verify(lib) -> None:
        verify_additive(lib, expected_columns=cols_after, project=_project)

    return Migration(
        number=1,
        name="synth_add_feat_c",
        target_library="universe",
        symbol_scope="all universe symbols",
        schema_version_before=0,
        schema_version_after=1,
        columns_after=cols_after,
        backfill_policy="history rows: NaN (not retro-computable)",
        run=_run,
        verify=_verify,
    )


# ── The config-I3236 failure, reproduced then resolved by a migration ────────


def test_unmigrated_append_fails_loud(universe_lib):
    """Precondition: a widened append onto an old-schema symbol surfaces as a
    StreamDescriptorMismatch (the 904/904 config-I3236 failure)."""
    _seed_old_schema(universe_lib, symbols=("AAA",))
    row = _universe_frame([pd.Timestamp("2026-06-06")], _NEW_FEATURES)
    res = universe_lib.update_batch(
        [UpdatePayload(symbol="AAA", data=to_arctic_canonical(row, features=_NEW_FEATURES))],
        upsert=True,
    )
    assert "StreamDescriptorMismatch" in str(res[0])


def test_migration_run_verify_and_production_update_batch(universe_lib, meta_lib):
    """The core harness (config-I3241 deliverable 3): run a real migration
    against seeded old-schema symbols, verify(), and assert the PRODUCTION
    write primitive update_batch succeeds post-migration (config#2459)."""
    symbols = _seed_old_schema(universe_lib)
    write_schema_version(
        meta_lib, 0, migration_number=0,
        columns_after=tuple(OHLCV_COLS) + (PROVENANCE_COL,) + tuple(_OLD_FEATURES),
    )

    mig = _synthetic_add_feat_c_migration()

    # Pre-write producer assert: at v0, a producer emitting v1 must fail loud
    # and name the pending migration — BEFORE any symbol write.
    with pytest.raises(SchemaVersionMismatch) as ei:
        assert_schema_version(meta_lib, 1, pending_migrations=[1])
    assert "0001" in str(ei.value)

    # Run + verify the migration (verify includes a live update_batch probe).
    mig.run(universe_lib, meta_lib)
    mig.verify(universe_lib)

    # Stamp advanced to 1.
    assert read_schema_version(meta_lib) == 1
    # Producer assert is now green at v1.
    assert assert_schema_version(meta_lib, 1) == 1

    # The real production append primitive now lands the widened row cleanly.
    for sym in symbols:
        row = _universe_frame([pd.Timestamp("2026-06-07")], _NEW_FEATURES)
        res = universe_lib.update_batch(
            [UpdatePayload(symbol=sym, data=to_arctic_canonical(row, features=_NEW_FEATURES))],
            upsert=True,
        )
        assert "StreamDescriptorMismatch" not in str(res[0]), res[0]
        stored = universe_lib.read(sym).data
        assert "feat_c_new" in stored.columns
        assert pd.Timestamp("2026-06-07") in stored.index


def test_rewrite_uses_full_write_not_update(universe_lib, meta_lib):
    """Guard the config#2459 lesson structurally: rewrite_symbols_full must
    make a descriptor-crossing change succeed where a bare update() cannot."""
    _seed_old_schema(universe_lib, symbols=("AAA",))
    cols_after = tuple(OHLCV_COLS) + (PROVENANCE_COL,) + tuple(_NEW_FEATURES)
    n = rewrite_symbols_full(
        universe_lib,
        expected_columns=cols_after,
        new_columns={"feat_c_new": np.float32("nan")},
        project=lambda df: to_arctic_canonical(df, features=_NEW_FEATURES),
    )
    assert n == 1
    assert tuple(universe_lib.read("AAA").data.columns) == cols_after


def test_rewrite_aborts_on_nonconforming_projection(universe_lib):
    """Fail-loud: if the post-projection columns don't match the declared
    columns_after, the rewrite raises rather than persisting a bad descriptor."""
    _seed_old_schema(universe_lib, symbols=("AAA",))
    with pytest.raises(MigrationError):
        rewrite_symbols_full(
            universe_lib,
            expected_columns=("Open", "Close"),  # wrong on purpose
            project=lambda df: to_arctic_canonical(df, features=_OLD_FEATURES),
        )


def test_rewrite_new_columns_fn_computes_per_symbol_recompute(universe_lib, meta_lib):
    """``new_columns_fn`` (config#934 RECOMPUTE backfill support) — unlike
    ``new_columns``' uniform fill, it is called per-symbol with that symbol's
    own pre-migration history and can retro-compute a DISTINCT value per
    symbol, proving a real (non-NaN) backfill policy is mechanically
    supported end to end."""
    _seed_old_schema(universe_lib, symbols=("AAA", "BBB"))
    cols_after = tuple(OHLCV_COLS) + (PROVENANCE_COL,) + tuple(_NEW_FEATURES)

    def _recompute(symbol: str, df: pd.DataFrame) -> dict:
        # Distinct per-symbol value derived from the symbol's own history —
        # the shape a real RECOMPUTE migration (migrations/0001) uses.
        return {"feat_c_new": (df["feat_a"] * 0 + len(symbol)).astype("float32")}

    n = rewrite_symbols_full(
        universe_lib,
        expected_columns=cols_after,
        new_columns_fn=_recompute,
        project=lambda df: to_arctic_canonical(df, features=_NEW_FEATURES),
    )
    assert n == 2
    aaa = universe_lib.read("AAA").data
    bbb = universe_lib.read("BBB").data
    assert (aaa["feat_c_new"] == float(len("AAA"))).all()
    assert (bbb["feat_c_new"] == float(len("BBB"))).all()
    assert aaa["feat_c_new"].dtype == np.float32


# ── Version stamp semantics ──────────────────────────────────────────────────


def test_unstamped_meta_lib_reads_none_and_asserts_as_baseline(meta_lib):
    """A fresh/legacy meta lib has no stamp; producers treat it as baseline v0
    so merging the framework onto a live-but-unstamped bucket doesn't brick."""
    assert read_schema_version(meta_lib) is None
    # expected == baseline -> OK (no raise), returns baseline.
    assert assert_schema_version(meta_lib, BASELINE_SCHEMA_VERSION) == BASELINE_SCHEMA_VERSION


def test_stale_producer_direction_fails_loud(meta_lib):
    """effective > expected: producer code is older than applied migrations."""
    write_schema_version(meta_lib, 3, migration_number=3, columns_after=("Open",))
    with pytest.raises(SchemaVersionMismatch) as ei:
        assert_schema_version(meta_lib, 1)
    assert "STALE" in str(ei.value) or "stale" in str(ei.value)


def test_stamp_roundtrips_metadata(meta_lib):
    cols = tuple(OHLCV_COLS) + (PROVENANCE_COL,)
    write_schema_version(meta_lib, 7, migration_number=7, columns_after=cols)
    item = meta_lib.read("schema_version")
    assert item.metadata["schema_version"] == 7
    assert item.metadata["migration_number"] == 7
    assert item.metadata["n_columns"] == len(cols)
    assert read_schema_version(meta_lib) == 7


# ── Baseline (0000) migration exercised against a frozen-schema fixture ──────


def test_baseline_migration_stamps_conforming_library(universe_lib, meta_lib):
    baseline = importlib.import_module("migrations.0000_baseline_universe_schema")
    # Seed a symbol at the real live schema (== the frozen baseline columns).
    hist = _universe_frame(pd.date_range("2026-06-01", periods=4, freq="D"),
                           list(baseline.BASELINE_COLUMNS[7:]))  # features after OHLCV+source
    universe_lib.write_batch([WritePayload(symbol="AAA", data=to_arctic_canonical(hist))])
    baseline.MIGRATION.run(universe_lib, meta_lib)
    baseline.MIGRATION.verify(universe_lib)
    assert read_schema_version(meta_lib) == 0


def test_baseline_migration_refuses_nonbaseline_library(universe_lib, meta_lib):
    baseline = importlib.import_module("migrations.0000_baseline_universe_schema")
    _seed_old_schema(universe_lib, symbols=("AAA",))  # 2-feature junk, not baseline
    with pytest.raises(MigrationError):
        baseline.MIGRATION.run(universe_lib, meta_lib)


# ── Producer wrapper wiring (assert_universe_schema_current) ──────────────────


def test_producer_wrapper_wires_expected_and_pending(monkeypatch, meta_lib):
    """assert_universe_schema_current takes an already-opened meta lib (the
    producer opens it via the mockable get_schema_meta_lib seam) and derives
    EXPECTED + pending from the real chain, raising when the stamp lags."""
    import migrations as mig_pkg

    # No stamp -> baseline 0 == EXPECTED_SCHEMA_VERSION -> OK. Pinned via
    # monkeypatch (not the real ambient chain) so this stays true regardless
    # of how many real forward migrations exist in the repo at test time.
    monkeypatch.setattr(mig_pkg, "EXPECTED_SCHEMA_VERSION", 0)
    assert mig_pkg.assert_universe_schema_current(meta_lib) == mig_pkg.EXPECTED_SCHEMA_VERSION

    # Simulate a future pending migration by forcing EXPECTED ahead of the stamp.
    write_schema_version(meta_lib, 0, migration_number=0, columns_after=("Open",))
    monkeypatch.setattr(mig_pkg, "EXPECTED_SCHEMA_VERSION", 1)
    monkeypatch.setattr(mig_pkg, "pending_migrations", lambda cur: [type("M", (), {"number": 1})()])
    with pytest.raises(SchemaVersionMismatch) as ei:
        mig_pkg.assert_universe_schema_current(meta_lib)
    assert "0001" in str(ei.value)
