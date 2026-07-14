"""config#2006 — the column-add / schema-evolution rollout contract.

nousergon-data PR688 (config#939) widened ``features.feature_engineer.FEATURES``
by three columns (``vwap_divergence_pct``, ``cmf_20_ratio``,
``hy_oas_credit_spread_pct``) but shipped NO migration for the ~900 EXISTING
ArcticDB universe symbols. The first live ``daily_append`` after the merge
(2026-07-08 EOD recovery) failed per-ticker: the universe library is
STATIC-schema, so an ``update_batch`` payload carrying the widened column set
(95 cols) does not match the stored 92-column descriptor.

``tests/test_daily_append_schema_drift.py`` (config#1150) already locks the
INSTRUMENTATION side — that a schema-drift error is COUNTED and re-raised /
routed to ``n_err`` rather than swallowed — using a mocked exception. What was
missing is a test at the DATA layer that reproduces the actual failure against a
REAL old-schema library through the real ``to_arctic_canonical`` chokepoint, and
pins the recovery contract: a **full-history restate** at the new schema is what
makes the append green again (deliverable 3 of config#2006).

This module is the CI tripwire for the column-add bug CLASS. It asserts:

  1. Appending a NEW-schema row onto an OLD-schema symbol is SURFACED as a
     per-symbol ``DataError`` naming ``StreamDescriptorMismatch`` — never a
     silent success and never a silent drop of the new column. (``daily_append``
     aggregates that DataError into ``n_err`` → non-zero exit, the fail-loud
     behaviour observed on 2026-07-08.)
  2. The NEW column is the sole cause — the identical append at the OLD schema
     succeeds.
  3. A full-history restate at the NEW schema (the ``builders/backfill.py`` /
     ``migrate_universe_*`` recovery path) makes the subsequent append succeed.

Because it drives a real LMDB ArcticDB via ``update_batch`` / ``write_batch``,
it also guards against a future "fix" that routes universe appends through a
schema-narrowing helper (e.g. ``_align_schema_for_update``, which DROPS extra
columns) — that would turn a loud, restate-forcing failure into a silent
partial-coverage write, exactly the regression this class must never allow.

Synthetic FEATURES lists are used (not the live ``FEATURES``) so the guard does
not churn every time a real column is added — the invariant under test is the
descriptor-set contract itself, independent of which columns exist today.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import arcticdb as adb
from arcticdb.version_store.library import UpdatePayload, WritePayload

from store.arctic_store import OHLCV_COLS, PROVENANCE_COL, to_arctic_canonical

# An OLD (pre-widening) feature schema and the NEW schema after a column-add PR.
# The new set is a strict superset — the additive-only widening that PR688 did.
_OLD_FEATURES = ["feat_a", "feat_b"]
_NEW_FEATURES = ["feat_a", "feat_b", "feat_c_new"]


def _universe_frame(dates, features) -> pd.DataFrame:
    """A universe-shaped frame: OHLCV + source provenance + the given features,
    laid out so ``to_arctic_canonical`` is the single ordering authority."""
    idx = pd.DatetimeIndex(dates, name="date")
    data: dict[str, object] = {
        col: np.arange(1, len(idx) + 1, dtype="float64") for col in OHLCV_COLS
    }
    data[PROVENANCE_COL] = ["polygon"] * len(idx)
    for feat in features:
        data[feat] = np.arange(len(idx), dtype="float32")
    return pd.DataFrame(data, index=idx)


@pytest.fixture()
def universe_lib(tmp_path):
    """A real LMDB-backed ArcticDB ``universe`` library (mirrors
    ``tests/test_split_restatement.py`` / ``test_migrate_universe_crsp_basis.py``
    — the static-schema descriptor behaviour under test is a native ArcticDB
    property, so it cannot be exercised with a mock)."""
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    return ac.get_library("universe", create_if_missing=True)


def _store_old_schema(lib, symbol="AAA"):
    hist = _universe_frame(
        pd.date_range("2026-06-01", periods=5, freq="D"), _OLD_FEATURES
    )
    lib.write_batch(
        [WritePayload(symbol=symbol, data=to_arctic_canonical(hist, features=_OLD_FEATURES))]
    )
    return symbol


def _append_row(lib, symbol, date, features):
    """Mirror the append-at-head path in ``daily_append`` exactly:
    ``to_arctic_canonical`` → ``UpdatePayload`` → ``update_batch(upsert=True)``."""
    row = _universe_frame([pd.Timestamp(date)], features)
    return lib.update_batch(
        [UpdatePayload(symbol=symbol, data=to_arctic_canonical(row, features=features))],
        upsert=True,
    )


def _is_descriptor_mismatch(result) -> bool:
    """A ``DataError`` result whose message names the schema mismatch."""
    # ArcticDB returns arcticdb_ext DataError objects (not exceptions) for
    # per-symbol failures in a batch. Match on the canonical error string
    # rather than the (version-varying) type.
    return "StreamDescriptorMismatch" in str(result)


class TestColumnAddWithoutRestateIsSurfaced:
    """A widened-schema append onto an un-migrated symbol must fail LOUD."""

    def test_new_column_append_returns_descriptor_mismatch(self, universe_lib):
        symbol = _store_old_schema(universe_lib)
        results = _append_row(universe_lib, symbol, "2026-06-06", _NEW_FEATURES)
        assert len(results) == 1
        assert _is_descriptor_mismatch(results[0]), (
            "appending a widened (new-column) row onto a static-schema symbol "
            "with no restate must surface as a per-symbol StreamDescriptorMismatch "
            f"DataError (daily_append aggregates it into n_err), got: {results[0]!r}"
        )

    def test_new_column_is_not_silently_dropped(self, universe_lib):
        """The stored series must be UNCHANGED after the failed append — the new
        column is never silently discarded to force a false-green write."""
        symbol = _store_old_schema(universe_lib)
        _append_row(universe_lib, symbol, "2026-06-06", _NEW_FEATURES)
        stored = universe_lib.read(symbol).data
        assert "feat_c_new" not in stored.columns
        assert len(stored) == 5, "the mismatched row must not have landed"

    def test_same_schema_append_succeeds(self, universe_lib):
        """Control: the identical append at the OLD schema is clean — proving
        the NEW column is the sole cause, not the append mechanics."""
        symbol = _store_old_schema(universe_lib)
        results = _append_row(universe_lib, symbol, "2026-06-06", _OLD_FEATURES)
        assert not _is_descriptor_mismatch(results[0])
        assert len(universe_lib.read(symbol).data) == 6


class TestFullHistoryRestateResolvesTheMismatch:
    """The documented recovery: restate the whole symbol at the new schema
    (``builders/backfill.py`` full-history rewrite), then appends are green."""

    def test_restate_then_append_is_green(self, universe_lib):
        symbol = _store_old_schema(universe_lib)

        # Precondition: the un-migrated append fails.
        pre = _append_row(universe_lib, symbol, "2026-06-06", _NEW_FEATURES)
        assert _is_descriptor_mismatch(pre[0])

        # Restate full history at the NEW schema (what backfill.py does).
        restated = _universe_frame(
            pd.date_range("2026-06-01", periods=6, freq="D"), _NEW_FEATURES
        )
        universe_lib.write_batch(
            [WritePayload(symbol=symbol, data=to_arctic_canonical(restated, features=_NEW_FEATURES))],
            prune_previous_versions=True,
        )

        # The same-shape append now lands cleanly.
        post = _append_row(universe_lib, symbol, "2026-06-07", _NEW_FEATURES)
        assert not _is_descriptor_mismatch(post[0]), (
            f"after a full-history restate at the new schema the append must "
            f"succeed, got: {post[0]!r}"
        )
        final = universe_lib.read(symbol).data
        assert "feat_c_new" in final.columns
        assert len(final) == 7
